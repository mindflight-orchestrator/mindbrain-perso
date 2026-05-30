# Taxonomy And APIs

The standalone HTTP server exposes ontology catalog, schema graph, type detail,
taxonomy, and schema write routes.

## Read Routes

| Route | Query | Purpose |
|-------|-------|---------|
| `GET /api/mindbrain/ontology/list` | optional `workspace_id` | Ontology catalog plus default ontology id. |
| `GET /api/mindbrain/ontology/graph` | optional `workspace_id`, optional `ontology_id` | Entity types, edge types, seed nodes, seed edges. |
| `GET /api/mindbrain/ontology/type` | `ontology_id`, `kind=entity|edge`, `type` | Type detail and related triples. |
| `GET /api/mindbrain/ontology/taxonomy` | `ontology_id`, optional `workspace_id` | Namespaced dimensions and values. |
| `GET /api/mindbrain/workspace/list` | none | Workspaces, entity counts, default ontology ids. |
| `GET /api/mindbrain/graph/type-counts` | `workspace_id` | Instance counts by entity type with ontology labels. |

The taxonomy route returns two arrays:

- `dimensions`: rows from `ontology_dimensions`;
- `values`: rows from `ontology_values`.

## Write Routes

All write routes use the serialized HTTP writer lane and reject frozen
ontologies with HTTP 409.

| Route | Body | Purpose |
|-------|------|---------|
| `POST /api/mindbrain/ontology/taxonomy/dimension` | `ontology_id`, `namespace`, `dimension`, optional type flags, metadata | Upsert a dimension. |
| `POST /api/mindbrain/ontology/taxonomy/value` | `ontology_id`, `namespace`, `dimension`, `value_id`, `value`, optional parent/label/metadata | Upsert a controlled value. |
| `POST /api/mindbrain/ontology/entity-type` | `ontology_id`, `entity_type`, optional label/metadata/parent | Upsert an entity type and optional subclass triple. |
| `POST /api/mindbrain/ontology/edge-type` | `ontology_id`, `edge_type`, optional source/target/directed/metadata | Upsert an edge type. |
| `POST /api/mindbrain/ontology/property` | `ontology_id`, `name`, `kind`, `domain`, `range`, optional metadata | Upsert object/datatype property. |
| `POST /api/mindbrain/ontology/triple` | full triple row | Upsert a preserved ontology triple. |

## Frozen Ontology Behavior

Before writing, handlers call `collections_sqlite.isOntologyFrozen`. If the
target ontology is frozen, the response body is:

```json
{"error":"ontology_frozen"}
```

Use frozen ontologies for imported or released vocabularies that should not be
changed by Studio/UI write surfaces.

## Source Modules

| Module | Route responsibility |
|--------|----------------------|
| `http_app.zig` | Route dispatch, JSON parsing, writer-lane calls. |
| `collections_io.zig` | Taxonomy selectors for dimensions and values. |
| `collections_sqlite.zig` | Upsert helpers and frozen checks. |
| `ontology_sqlite.zig` | Coverage and projection relevance helpers. |

## Studio / Browser Boundary

The browser-facing model should use these typed HTTP routes instead of opening
the SQLite file directly. Raw SQL endpoints exist for trusted local operators,
but taxonomy/schema editing should prefer the typed ontology routes.
