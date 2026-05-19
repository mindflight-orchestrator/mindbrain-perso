# Plan: Typed Edge Properties for `graph_relation`

Date: 2026-05-19

## Why no overlap exists

**vs facets:** Facets (`facet_assignments_raw` → `facet_postings`) are bitmap-indexed
dimensions on *documents and chunks*. Edge properties are typed scalars on *relations*.
Separate tables, separate concern.

**vs node attributes:** `graph_entity` carries `entity_type`, `name`, `confidence`,
`metadata_json`. `graph_relation` already carries `relation_type`, `confidence`,
`valid_from_unix`, `valid_to_unix`, `metadata_json`. The new tables add nothing that
already exists on either side.

**Removed types (were redundant):**
- `date_range` — `valid_from_unix` / `valid_to_unix` on `graph_relation` already store
  possession periods; using them avoids a JOIN on every temporal query.
- `entity_ref` / `relation_ref` / `graph_ref` — cross-references are modeled as
  relations themselves (`source_id` / `target_id`); deferred to v2.

**Design challenges resolved:**
- Raw staging layer kept (`relation_properties_raw`) to match the established
  `*_raw → graph_*` projection pattern. Avoids JSON re-parsing on re-projection.
- Partial index form: `WHERE <column> IS NOT NULL` — direct SQLite predicate match,
  no planner inference required.
- Currency CHECK constraint: enforces `currency IS NULL OR value_type = 'money_minor'`
  at zero runtime cost.
- Batch loader: `loadRelationPropertiesBatch(ids[])` — single `WHERE relation_id IN (...)`
  query to avoid N+1, consistent with the complexity-optimization plan.
- `SchemaMode.import` skips all four `grp_*` indexes (same pattern as
  `graph_relation_*_idx`), then `CREATE INDEX` at end of bulk import.

---

## New tables

### Raw layer: `relation_properties_raw`

Workspace-scoped source of truth. One row per `(workspace_id, relation_id, property_key)`.

```sql
CREATE TABLE IF NOT EXISTS relation_properties_raw (
    workspace_id  TEXT    NOT NULL,
    relation_id   INTEGER NOT NULL,
    property_key  TEXT    NOT NULL,
    value_type    TEXT    NOT NULL CHECK(value_type IN (
                      'text', 'number', 'percentage_bp', 'money_minor',
                      'date_unix', 'doc_ref', 'uri')),
    value_text    TEXT,
    value_number  REAL,
    value_integer INTEGER,
    ref_doc_id    INTEGER,
    currency      TEXT,
    created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (currency IS NULL OR value_type = 'money_minor'),
    PRIMARY KEY (workspace_id, relation_id, property_key),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY (workspace_id, relation_id) REFERENCES relations_raw(workspace_id, relation_id)
);

CREATE INDEX IF NOT EXISTS relation_properties_raw_relation_idx
    ON relation_properties_raw(workspace_id, relation_id);
```

### Derived layer: `graph_relation_property`

Globally indexed projection. One row per `(relation_id, property_key)`.

```sql
CREATE TABLE IF NOT EXISTS graph_relation_property (
    relation_id   INTEGER NOT NULL REFERENCES graph_relation(relation_id),
    property_key  TEXT    NOT NULL,
    value_type    TEXT    NOT NULL CHECK(value_type IN (
                      'text', 'number', 'percentage_bp', 'money_minor',
                      'date_unix', 'doc_ref', 'uri')),
    value_text    TEXT,      -- text, uri
    value_number  REAL,      -- number (scores, weights, ratios)
    value_integer INTEGER,   -- percentage_bp (basis points, 10000=100%)
                             -- money_minor (cents, e.g. 25000000=250000.00 EUR)
                             -- date_unix (unix epoch)
    ref_doc_id    INTEGER,   -- doc_ref → documents_raw.doc_id
    currency      TEXT,      -- ISO 4217, only when value_type = money_minor
    CHECK (currency IS NULL OR value_type = 'money_minor'),
    PRIMARY KEY (relation_id, property_key)
);

-- filter by key+text value (e.g. ownership_kind = 'nue_propriete')
CREATE INDEX IF NOT EXISTS grp_key_text_idx
    ON graph_relation_property(property_key, value_text)
    WHERE value_text IS NOT NULL;

-- sort/range by key+integer (amounts, percentages, dates)
CREATE INDEX IF NOT EXISTS grp_key_int_idx
    ON graph_relation_property(property_key, value_integer)
    WHERE value_integer IS NOT NULL;

-- sort/range by key+real
CREATE INDEX IF NOT EXISTS grp_key_num_idx
    ON graph_relation_property(property_key, value_number)
    WHERE value_number IS NOT NULL;

-- find all edges referencing a document
CREATE INDEX IF NOT EXISTS grp_doc_ref_idx
    ON graph_relation_property(ref_doc_id)
    WHERE ref_doc_id IS NOT NULL;
```

---

## 7 value types (no redundancy)

| type | stored in | example use |
|------|-----------|-------------|
| `text` | `value_text` | `ownership_kind` = `pleine_propriete` / `nue_propriete` / `usufruit` |
| `number` | `value_number` | confidence weights, ratios, scores |
| `percentage_bp` | `value_integer` | `share_bp` = 5000 means 50% |
| `money_minor` | `value_integer` + `currency` | `purchase_price` = 25000000, currency = EUR = 250 000 € |
| `date_unix` | `value_integer` | `contract_signed_at` (unix epoch) |
| `doc_ref` | `ref_doc_id` | `purchase_contract` → `documents_raw.doc_id` |
| `uri` | `value_text` | external link to a justificatif not in documents_raw |

---

## Temporal ownership model

No new column needed. `graph_relation` already has:
- `valid_from_unix` INTEGER — start of possession (inclusive)
- `valid_to_unix` INTEGER — end of possession (exclusive); NULL = current owner

**Convention:** `valid_from_unix <= :at AND (valid_to_unix IS NULL OR valid_to_unix > :at)`

### Current owners query (uses existing `graph_relation_target_id_idx`)

```sql
SELECT gr.source_id, grp_kind.value_text AS ownership_kind, grp_share.value_integer AS share_bp
FROM graph_relation gr
LEFT JOIN graph_relation_property grp_kind
       ON grp_kind.relation_id = gr.relation_id AND grp_kind.property_key = 'ownership_kind'
LEFT JOIN graph_relation_property grp_share
       ON grp_share.relation_id = gr.relation_id AND grp_share.property_key = 'share_bp'
WHERE gr.relation_type  = 'POSSEDE'
  AND gr.target_id      = :bien_id
  AND gr.valid_from_unix <= :at
  AND (gr.valid_to_unix IS NULL OR gr.valid_to_unix > :at)
  AND gr.deprecated_at IS NULL;
```

### Historical owners query

```sql
SELECT gr.source_id, gr.valid_from_unix, gr.valid_to_unix
FROM graph_relation gr
WHERE gr.relation_type = 'POSSEDE'
  AND gr.target_id     = :bien_id
  AND gr.valid_to_unix IS NOT NULL   -- closed interval = past
  AND gr.deprecated_at IS NULL;
```

### On sale/transfer

1. Set `valid_to_unix = :sale_date` on the seller's `POSSEDE` edge.
2. Insert new `POSSEDE` edge for buyer with `valid_from_unix = :sale_date`,
   `valid_to_unix = NULL`.
3. Insert/update `relation_properties_raw` rows for both edges
   (ownership_kind, share_bp, contract doc_ref, etc.) → project to
   `graph_relation_property` in the same transaction.

**Co-ownership** = multiple `POSSEDE` edges with different `source_id` (each co-owner)
and `share_bp` properties summing to 10000.

---

## Implementation touchpoints

- `sql/sqlite_mindbrain--1.0.0.sql`: `relation_properties_raw` (1 index) and
  `graph_relation_property` (4 indexes). `grp_*_idx` excluded in `SchemaMode.import`.
- `src/standalone/sqlite_schema.zig`: import exclusions for 4 `grp_*` indexes;
  existence tests for both new tables.
- `src/standalone/collections_sqlite.zig`: `RelationPropertyValueType` enum,
  `RelationPropertyRawSpec` struct, `upsertRelationPropertyRaw`.
- `src/standalone/interfaces.zig`: `RelationProperty` record, `RelationPropertyPredicate`
  (with ops: eq, exists, lt, lte, gt, gte, between, in_text), extended `GraphEdgeFilter`
  with `property_predicates` and `sort_by_property`.
- `src/standalone/graph_sqlite.zig`: `upsertRelationProperty`, `loadRelationPropertiesBatch`
  (single IN query, no N+1), `projectRelationProperties` (INSERT ... SELECT from raw layer).
- `src/standalone/import_pipeline.zig`: `addRelationProperty` method (raw + derived in
  one call); `reindexGraphWithDocumentTable` calls `projectRelationProperties` after the
  relations loop.

---

## Validation

```bash
ZIG_LOCAL_CACHE_DIR=/tmp/zig-cache \
ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache \
.codex/toolchains/zig-x86_64-linux-0.16.0/zig build test-standalone
```

- Schema: `relation_properties_raw` (1 index) and `graph_relation_property` (4 indexes)
  exist in `SchemaMode.runtime`; `grp_*_idx` absent in `SchemaMode.import`.
- CHECK constraints: rejects `money_minor` row missing currency, rejects non-money row
  with currency set.
- CRUD: insert each of the 7 value types; read back; filter by eq; sort by integer/real.
- Ownership: current owners at a given date; all past owners; sale transfer (close old
  edge, open new edge); co-ownership shares sum to 10000 bp.
- Batch loader: `loadRelationPropertiesBatch` returns properties for N edges in a single
  SQL statement.
- Pipeline: `RelationPropertyRawSpec` roundtrips through
  `relation_properties_raw` → projection → `graph_relation_property`.
