# Porting guide: collections raw layer + document chunking (SQLite → PostgreSQL / pg_mindbrain)

This document is the **single reference** for implementing in **pg_mindbrain** (PostgreSQL) the behavior and structure introduced in two development tracks:

1. **Generic collections: raw vs. index** — workspace → collections → documents/chunks → ontologies; canonical `*_raw` tables vs rebuildable facet/BM25/graph indexes. (Detailed planning lived in maintainer-local artifacts; not committed under `.cursor/`.)
2. **Document chunking, nanoids, `source.*` facets, external links** — URL-safe public ids, chunking strategies, auto-facets, and outbound URI edges. (Same: internal planning snapshots are local-only.)

**Mindbrain (this repo)** implements the reference behavior in **Zig + SQLite** under `src/standalone/`. **pg_mindbrain** should mirror **schema semantics** and **operational contracts**; implementation language differs, but **table names, keys, and invariants** should stay aligned so bundles and mental models transfer cleanly.

---

## 1. Design principles (both tracks)

### 1.1 Raw layer is source of truth

- Everything under `mb_collections.*_raw` (Postgres) or the equivalent SQLite tables is **durable** and **what you back up**.
- Facet postings, BM25 tables, graph adjacency (`lj_out` / `lj_in`), ANN indexes, etc. are **derived**. They may be dropped and rebuilt from raw data **per workspace** (optionally scoped to a collection).

### 1.2 Tenancy and grouping

| Concept | Meaning |
|--------|---------|
| **Workspace** | Tenant boundary. Owns collections, ontologies, entities/relations, cross-collection links. |
| **Collection** | Document bucket inside a workspace (`legal`, `technical`, `yago-core`, …). Owns `documents_raw` / `chunks_raw` for its `collection_id`. |
| **Ontology** | Workspace-scoped **schema + vocabulary graph**: namespaces, dimensions, values, entity types, edge types, optional seed entities/relations. |
| **Attachment** | `collection_ontologies` ties an ontology to one or more collections so the **same** ontology can span e.g. legal ↔ technical. |

### 1.3 Two kinds of “links”

| Mechanism | Table | Role |
|-----------|--------|------|
| **Internal / cross-collection** | `document_links_raw` | Doc-to-doc (or chunk) edges **inside the system**, typed by `ontology_id` + `edge_type`. Endpoints are `(collection_id, doc_id[, chunk_index])`. |
| **External / outbound URI** | `external_links_raw` | Edges from a document (or chunk) to an **external** `target_uri` (HTTP(S), etc.). Not the same as internal links. |

### 1.4 Public vs internal document identity (chunking track)

| Field | Exposure | Usage |
|-------|----------|--------|
| `(workspace_id, collection_id, doc_id)` | Internal only | All FKs, joins, BM25 parent rows, graph bridges. |
| `doc_nanoid` | **May appear in URLs / APIs** | Opaque, URL-safe; **chunks** addressed externally as `<doc_nanoid>#<chunk_index>` (no per-chunk nanoid). |

**Postgres:** partial unique index on `doc_nanoid` **where** `doc_nanoid <> ''`; assigned public nanoids are globally unique while empty string remains an internal "not supplied" sentinel.

### 1.5 Built-in `source.*` namespace

On workspace creation, the **default** ontology must expose namespace `source` with dimensions used for **auto-extracted** chunk metadata: `path`, `dir`, `filename`, `extension`, `ingested_at`, `chunk_index`, `chunk_count`, `strategy`. Assignments are stored in `facet_assignments_raw` with `target_kind = 'chunk'` (and the appropriate `chunk_index`).

**`dir` semantics:** cumulative path prefixes (e.g. `data`, `data/legal`, `data/legal/2025`), one facet row per prefix per chunk — not only single path segments.

---

## 2. Schema inventory (`mb_collections`)

All new collection/ontology/raw objects live in the **`mb_collections`** schema so they do not collide with existing `facets.*`, `graph.*`, etc.

### 2.1 Extension install script

The first public PostgreSQL schema lives directly in
`sql/sqlite_mindbrain--1.0.0.sql`. The `mb_collections` DDL includes the core
raw tables plus `doc_nanoid`, `summary`, chunking metadata, vectors,
`document_links_raw`, and `external_links_raw` as the initial 1.0.0 shape.

### 2.2 Workspace and collection scaffolding

**SQLite reference:** `sqlite_schema.zig` + `collections_sqlite.zig`.  
**Postgres:** `mb_collections.workspaces`, `mb_collections.collections`, `mb_collections.workspace_settings`.

Notable columns:

- `collections.chunk_bits` — `smallint` (PG); used when forming **synthetic BM25 document ids** for chunks: `(doc_id << chunk_bits) | chunk_index` (must match application convention).
- `collections.metadata` — `jsonb` (PG) vs JSON text in SQLite (conceptually the same).

**Ontologies:** `mb_collections.ontologies` with `workspace_id` nullable for future global ontologies; in practice workspace-scoped rows match the Zig model.

**Attachment:** `mb_collections.collection_ontologies` — PK `(workspace_id, collection_id, ontology_id)`.

### 2.3 Ontology graph tables

| Table | Purpose |
|-------|---------|
| `ontology_namespaces` | Namespace definitions per ontology. |
| `ontology_dimensions` | Dimensions under a namespace (`value_type`, `is_multi`, `hierarchy_kind`). |
| `ontology_values` | Optional enumerated/hierarchical values. |
| `ontology_entity_types` / `ontology_edge_types` | Graph schema for the **working graph** using this ontology. |
| `ontology_entities_raw` / `ontology_relations_raw` | **Seed** entities/relations shipped with the ontology (taxonomy roots, etc.). |

### 2.4 Documents and chunks (raw)

**`mb_collections.documents_raw`**

- PK: `(workspace_id, collection_id, doc_id)`.
- Core: `content`, `language`, `source_ref`, `metadata`, timestamps.
- `doc_nanoid text NOT NULL DEFAULT ''`, `summary text` (reserved, nullable).

**`mb_collections.chunks_raw`**

- PK: `(workspace_id, collection_id, doc_id, chunk_index)`.
- FK to parent document row.
- Core: `content`, `language`, `offset_start`, `offset_end`, `metadata`, timestamps.
- `strategy text`, `token_count bigint`, `parent_chunk_index int` (nullable; **late** chunking).

**Vectors:** `documents_raw_vector`, `chunks_raw_vector` — `bytea` embeddings keyed like their parent rows.

### 2.5 Facet assignments (raw)

**`mb_collections.facet_assignments_raw`**

- `target_kind` ∈ `('doc','chunk')`.
- `chunk_index` = `-1` or sentinel for doc-level rows (mirror SQLite convention).
- `ontology_id`, `namespace`, `dimension`, `value`, optional `value_id`, `weight`, `source`.

This is the **canonical** place for facet picks that must survive backup/restore, including auto-generated **`source.*`** rows.

### 2.6 Working graph (raw, workspace-scoped)

| Table | Purpose |
|-------|---------|
| `entities_raw` | PK `(workspace_id, entity_id)`; links to `ontology_id` for typing. |
| `entity_aliases_raw` | Surface forms. |
| `relations_raw` | Edges between workspace entity ids (cross-collection capable). |
| `entity_documents_raw` | Bridge entity ↔ document (same entity may attach to docs in **different** collections). |
| `entity_chunks_raw` | Bridge entity ↔ chunk. |

### 2.7 Cross-collection document links (raw, internal)

**`mb_collections.document_links_raw`**

- PK `(workspace_id, link_id)`.
- `ontology_id`, `edge_type`, source/target collection + doc + optional chunk index, `weight`, `source`, `metadata`.

Used for **legal ↔ technical** style mappings without forcing an entity node.

### 2.8 External links (raw, outbound URIs)

**`mb_collections.external_links_raw`**

- PK `(workspace_id, link_id)`.
- `source_collection_id`, `source_doc_id`, optional `source_chunk_index`.
- `target_uri` (not an internal doc id).
- `edge_type`, `weight`, `metadata`, `created_at`.

FK: `(workspace_id, source_collection_id, source_doc_id)` → `documents_raw`.

**Note:** Default `edge_type` in the PG schema is `external_link`; keep application code consistent across SQLite and PG.

### 2.9 Indexes (Postgres)

- `documents_raw_nanoid_uidx`: **partial unique** on `documents_raw(doc_nanoid)` **where** `doc_nanoid <> ''`.
- `external_links_raw_*_idx`: source doc lookup, `target_uri`, `edge_type`.

---

## 3. SQLite ↔ Postgres type mapping (reference)

| Logical role | SQLite (typical) | Postgres (`mb_collections`) |
|--------------|------------------|-----------------------------|
| ids / text | `TEXT` | `text` |
| integers | `INTEGER` | `bigint` or `int` (match migration) |
| booleans | `INTEGER` 0/1 | `boolean` |
| floats | `REAL` | `double precision` |
| JSON payload | `TEXT` JSON | `jsonb` |
| embeddings | `BLOB` | `bytea` |
| timestamps | `TEXT` / numeric | `timestamptz` |

The **names** of columns in `documents_raw` / `chunks_raw` / `facet_assignments_raw` should match between environments; only representation types differ.

---

## 4. Application behavior to port (no Zig required)

The **reference implementation** lives in:

| Area | Primary Zig modules |
|------|---------------------|
| Raw upserts / lookups | `collections_sqlite.zig` |
| Ingest + reindex orchestration | `import_pipeline.zig` |
| Backup/restore JSON bundle | `collections_io.zig` |
| Chunking + `source.*` derivation | `chunker.zig` |
| Nanoid generation | `nanoid.zig` |
| CLI | `tool.zig` |

### 4.1 Pipeline responsibilities (conceptual API)

When porting to pg_mindbrain, preserve these **operations** (names may become SQL functions or service methods):

**Lifecycle**

- Ensure workspace; ensure default ontology; ensure collection; attach ontology to collection.
- Load ontology bundle **or** incremental `ensureNamespace` / `ensureDimension` / …

**Raw ingest**

- `upsertDocumentRaw`, `upsertChunkRaw`, `upsertFacetAssignmentRaw`.
- `ingestDocumentChunked`: nanoid assign → document row → chunk rows → `deriveSourceFacets` equivalent → facet upserts; optional BM25 sync for parent + chunks using synthetic chunk doc ids.
- `linkExternal` → `upsertExternalLinkRaw`.
- Entity/relation/document link helpers as in the collections plan.

**Reindex (derived)**

- `reindexFacets` — from `facet_assignments_raw` into pg_facets / postings (PG-specific).
- `reindexBm25` — from `documents_raw` and optionally `chunks_raw` (chunk table id + `chunk_bits` for synthetic ids).
- `reindexGraph` — from `relations_raw` + adjacency implied by `document_links_raw` (and entity bridges), into `graph.*`.
- `reindexAll` — orchestrates the above.

**URL routing**

- `lookupDocByNanoid(nanoid)` → `(workspace_id, collection_id, doc_id)` for resolving `<doc_nanoid>#<chunk_index>`.

### 4.2 Chunking strategies (library contract)

Port the **semantics**, not necessarily Zig:

- **Deterministic:** `fixed_token`, `sentence`, `paragraph`, `recursive_character`, `structure_aware`.
- **Embedding-aware (callbacks):** `semantic` (`EmbedSentencesFn`), `late` (`EmbedFullDocFn`).

CLI in SQLite exposes only deterministic strategies; semantic/late are **library-only** in the reference design.

### 4.3 Nanoid

- Alphabet: URL-safe `0-9A-Za-z_-`, default length 21.
- Cryptographically secure RNG; rejection sampling if alphabet size is not power of two.
- Uniqueness: enforced by DB partial unique index on non-empty values.

### 4.4 Backup / restore

**SQLite:** `collections_io` exports a JSON bundle (schema version, scope, arrays of rows).  
**Postgres:** Either:

- reuse the **same JSON bundle format** for interchange, or
- use `COPY` / native dumps for PG-only ops.

Bundle must include: workspace/collection/ontology scaffolding, `documents_raw` (with `doc_nanoid`, `summary`), `chunks_raw` (with chunker columns), `facet_assignments_raw`, entity/relation/link tables, **`external_links_raw`**, and internal `document_links_raw` as applicable.

Import = replay upserts in FK-safe order, then **reindexAll**.

## 5. Derived layer bridge in pg_mindbrain

The generic plan states the direction of travel; exact PG function names evolve in the extension:

- **`facet_assignments_raw`** ↔ existing facet assignment / `pg_facets` pipeline: reindex must map `(ontology_id, namespace, dimension)` to registered facet definitions for the **collection’s** faceted table.
- **`documents_raw` / `chunks_raw`** ↔ BM25 / FTS tables: parent docs use real `doc_id`; chunks use **synthetic** ids unless you allocate a separate BM25 table per chunk stream.
- **`relations_raw` + `document_links_raw`** ↔ `graph.relation` and adjacency: reindex must project cross-collection adjacency consistently with SQLite’s `graph_lj_out` / `graph_lj_in` behavior.

**Action for porters:** extend **reindex** procedures to consume `mb_collections.*_raw` as the single source of truth.

---

## 6. Port checklist (pg_mindbrain)

1. **Schema:** Install `sql/sqlite_mindbrain--1.0.0.sql`. Confirm `mb_collections` exists and FK order matches.
2. **Defaults:** Align `external_links_raw.edge_type` default with application (`external_link` vs `reference`).
3. **Bootstrap:** On workspace create, insert default ontology + **`source`** namespace dimensions (mirror `ensureSourceNamespace` in Zig).
4. **Ingest:** Implement chunked ingest + nanoid + `source.*` facet writes in your server language; match **order** of operations in `Pipeline.ingestDocumentChunked`.
5. **Indexes:** Implement `reindexBm25` with optional **chunk** table + `chunk_bits`; verify synthetic id formula matches clients.
6. **Routing:** Implement `lookupDocByNanoid` (index-friendly: exact match on `doc_nanoid`).
7. **Security:** Never expose internal `doc_id` in public URLs; only `doc_nanoid` + chunk index.
8. **Tests:** Round-trip bundle import/export; cross-collection link + external link; reindex parity on facets/BM25/graph for a small fixture workspace.
9. **Docs:** Keep this file and `docs/collections.md`, `docs/chunking.md`, `docs/facets.md` aligned when behavior changes.

---

## 7. Source files in this repository (for implementers)

| Topic | Path |
|-------|------|
| SQLite schema | `src/standalone/sqlite_schema.zig` |
| Raw CRUD | `src/standalone/collections_sqlite.zig` |
| Pipeline | `src/standalone/import_pipeline.zig` |
| Bundle I/O | `src/standalone/collections_io.zig` |
| Chunker | `src/standalone/chunker.zig` |
| Nanoid | `src/standalone/nanoid.zig` |
| PG extension schema | `sql/sqlite_mindbrain--1.0.0.sql` |
| User docs | `docs/collections.md`, `docs/chunking.md`, `docs/facets.md` |

---

## 8. Plan traceability

| Plan file | This document section |
|-----------|------------------------|
| `generic_collections_raw-vs-index_0bd1f132.plan.md` | §1–2 (model + raw schema), §4.1–4.2 ingest/reindex, §5–6 |
| `document_chunking_nanoid_facets_a73e9eb3.plan.md` | §1.4–1.5, §2.4–2.8 (nanoid/chunks/external), §4.2–4.4, §6 |

Together, these two tracks define the **complete raw contract** you need to reproduce in **PostgreSQL** under **pg_mindbrain**.
