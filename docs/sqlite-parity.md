# SQLite-backed Mindbrain — API Parity Contract

`mindbrain` keeps the same public API surface as the PostgreSQL extension
[`pg_mindbrain`](../../pg_mindbrain) but runs on SQLite. This document is
the single page that describes how the parity is delivered, where the
two backends diverge, and how to validate the contract end-to-end.

For the per-function classification (which API maps to a SQLite view, a
Zig helper, or is intentionally skipped) see
[`dev/api-parity-inventory.md`](dev/api-parity-inventory.md).

## Contract summary

- **Source of truth**: `sql/sqlite_mindbrain--1.0.0.sql` defines the
  canonical SQLite storage schema. The Zig modules under `src/mb_*` and
  `src/standalone/*_sqlite.zig` define the public API names, argument lists,
  default values, and result shapes.
- **Implementation**: every API family is exposed under the same names in
  `mindbrain`. SQLite is the default backend. PostgreSQL-only logic is
  replaced by SQLite SQL, FTS5, JSON1, or Zig helpers.
- **Engine assets**: the durable engines under `src/standalone/*_sqlite.zig`
  back the parity surface. The PostgreSQL extension entry points in
  `src/main.zig`, `src/mb_facets/main.zig`, `src/mb_graph/main.zig`,
  `src/mb_pragma/main.zig`, and `src/mb_ontology/main.zig` are kept
  install-compatible so `CREATE EXTENSION pg_mindbrain` continues to
  succeed against a Postgres host while the same call shapes drive the
  SQLite engine in standalone mode.

## API families and where they live

| Family | Public surface | SQLite implementation |
| --- | --- | --- |
| `mb_pragma.*` | `mb_pragma.pragma_parse`, `pragma_rank_native`, `pragma_pack_context`, `pragma_pack_context_scoped`, `pragma_next_hops`, `pragma_projection_*`, `_zig` exports | `src/standalone/pragma_sqlite.zig` + `src/standalone/pragma_projection_types.zig` (uses `mb_pragma.projection_types` for alias / rank / pack / structured semantics) |
| `graph.*` | `graph.find_entity_by_name`, `find_entities_by_names`, `find_entities_by_metadata`, `marketplace_search`, `marketplace_search_by_domain`, `confidence_decay`, `deprecate_relation`, traversal helpers | `src/standalone/graph_sqlite.zig` + `src/standalone/ontology_sqlite.zig` |
| `mb_ontology.*` | `coverage`, `coverage_by_domain`, `coverage_toon`, `coverage_by_domain_toon`, `marketplace_search_by_domain`, taxonomy import | `src/standalone/ontology_sqlite.zig` |
| `mindbrain.*` / `mb_collections.*` | workspace registration, schema export, collection / chunk / entity / relation API, BM25 reindex | `src/standalone/workspace_sqlite.zig`, `src/standalone/collections_sqlite.zig`, `src/standalone/import_pipeline.zig` |
| `facets.*` | facet definitions, postings, deltas, top-N, counts, drop helpers, BM25 sync triggers, BM25 worker range | `src/standalone/facet_sqlite.zig`, `src/standalone/search_sqlite.zig` |

## Storage contract

The SQLite schema lives in
[`sql/sqlite_mindbrain--1.0.0.sql`](../sql/sqlite_mindbrain--1.0.0.sql), which
`src/standalone/sqlite_schema.zig` embeds for runtime bootstrap. That single
canonical file mirrors the durable PostgreSQL data contract:

- `facets`, `projections`, `projection_types` are the
  primary persistence layer. The `projection_types` seeds for
  `FACT`, `GOAL`, `CONSTRAINT`, `STEP`, `NOTE` match the PostgreSQL
  defaults (compatibility aliases, rank bias, pack priority, next-hop
  multiplier, structured flag).
- `collection_ontologies.role` defaults to `primary`,
  `external_links_raw.edge_type` defaults to `external_link`, and
  `documents_raw` carries an `updated_at` timestamp, all matching the
  PostgreSQL DDL.
- The bundled BM25 stopword seed populates `bm25_stopwords` during schema
  bootstrap so custom stopword filtering is available without a separate load
  step.
- Roaring bitmaps are persisted as portable `BLOB` columns and operated
  on through `src/standalone/roaring.zig`.

## What the SQLite backend deliberately does not do

The following PostgreSQL-only operations have no SQLite equivalent and
are documented as `n/a` in the inventory:

- `dblink` / parallel BM25 worker pool
  (`facets.bm25_index_documents_parallel`, `bm25_cleanup_dblinks`,
  `bm25_full_cleanup`).
- `pg_stat_activity` introspection (`facets.bm25_status`, `bm25_progress`,
  `bm25_active_processes`).
- `pg_terminate_backend` (`facets.bm25_kill_stuck`).
- `UNLOGGED` table tuning (`facets.set_table_unlogged`,
  `set_table_logged`, `bulk_load_with_unlogged`).

`facets.bm25_get_worker_range` is reproduced in
`src/standalone/search_sqlite.zig` so callers that fan work out across
threads or processes still get the same range arithmetic.

## Validation strategy

Three layers of validation back the parity contract:

1. **Engine unit tests** (`zig build test`) cover every
   `src/standalone/*_sqlite.zig` module with the same fixtures the
   PostgreSQL surface relies on.
2. **PostgreSQL parity SQL tests** (`test/sql/**/*.sql`) keep the
   PostgreSQL-facing behavior anchored when an actual Postgres host is
   available (`test/run_all_tests.sh`). They are the canonical
   behavioral truth.
3. **SQLite parity smoke** (`test/sqlite/run_parity_smoke.sh`) drives
   the standalone Zig API surface through `zig build test` and the
   pragma smoke tests, and is what runs in the no-Postgres
   environments.

## Working without a PostgreSQL host

`zig build test` exercises the entire SQLite engine surface, including
schema, pragma alias / rank / pack / next-hop semantics, graph parity
helpers, ontology coverage and TOON exports, facet counts / top values
/ drop helpers, and BM25 sync triggers. It is the primary signal that
the parity contract still holds when no Postgres instance is reachable.
