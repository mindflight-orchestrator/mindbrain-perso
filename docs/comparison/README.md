# MindBrain and pg_mindbrain Comparison

This folder compares the public API and SQL migration surfaces of this
repository and the sibling `../pg_mindbrain` checkout.

The comparison is intentionally not a one-to-one API mapping. The two projects
share concepts, but they expose them through different runtime models:

- **MindBrain here** is a SQLite runtime. Applications use the local
  `mindbrain-http` proxy, the `mindbrain-standalone-tool` CLI, and the
  SQLite bootstrap file `sql/sqlite_mindbrain--1.0.0.sql`.
- **`../pg_mindbrain`** is a PostgreSQL extension. Applications call SQL
  functions in schemas such as `facets`, `graph`, `mb_collections`,
  `mb_ontology`, `mb_pragma`, `mb_state_graph`, `mb_process`, and
  `mb_bg_worker`.

## Sources Reviewed

MindBrain:

- `docs/api-reference.md`
- `docs/standalone.md`
- `docs/facets.md`, `docs/graph.md`, `docs/pragma.md`
- `docs/workspace.md`, `docs/collections.md`, `docs/projections.md`
- `sql/sqlite_mindbrain--1.0.0.sql`

Sibling `../pg_mindbrain`:

- `../pg_mindbrain/docs/API_REFERENCE.md`
- `../pg_mindbrain/docs/SCHEMA_REFERENCE.md`
- `../pg_mindbrain/docs/BACKGROUND_WORKERS.md`
- `../pg_mindbrain/docs/OPERATIONS.md`
- `../pg_mindbrain/docs/MIGRATION_1.4_TO_1.5.md`
- `../pg_mindbrain/docs/MIGRATION_1.5.2_TO_1.5.3.md`
- `../pg_mindbrain/docs/MIGRATION_1.5.3_TO_1.5.4.md`
- `../pg_mindbrain/sql/migration.md`
- `../pg_mindbrain/sql/pg_mindbrain--*.sql`

Historical development-note folders are intentionally ignored as normative
references.

## Summary Matrix

| Domain | MindBrain SQLite runtime | `pg_mindbrain` PostgreSQL extension | Comparison |
| --- | --- | --- | --- |
| Runtime boundary | Local HTTP proxy plus CLI over one SQLite database. | SQL extension loaded into PostgreSQL databases. | Different abstraction. |
| Direct query/admin access | `POST /api/mindbrain/sql*` executes SQLite SQL and manages SQLite sessions. | Callers use SQL directly through PostgreSQL clients. | SQLite-only wrapper. |
| Facets and BM25 | SQLite tables such as `facet_tables`, `facet_postings`, `search_*`, plus CLI/HTTP read paths. | `facets.*` SQL API with generated per-table storage, roaring bitmaps, BM25 workers, typed filters. | Shared concept, different API. |
| Graph | HTTP/CLI routes for traversal, path, subgraph, graph search; SQLite tables `graph_entity`, `graph_relation`, adjacency tables. | `graph.*` SQL functions, graph tables, views, native traversal, embeddings, projection helpers. | Shared concept, different API depth. |
| Workspace and collections | CLI commands and SQLite raw tables without PostgreSQL schemas. | `mb_collections.*` SQL functions and schema-qualified raw tables. | Shared model, different surface. |
| Ontology and coverage | CLI/HTTP coverage and workspace export backed by SQLite helpers. | `mb_ontology.*` registry, coverage, marketplace/search, workspace comparison, conflict helpers. | Shared concept, Postgres is broader. |
| Pragma and projections | `pack` HTTP/CLI routes, SQLite `projections`, `projection_types`, `agent_state`. | `mb_pragma.*` and public wrappers for projection parsing, ranking, packing, plus `mb_pragma` tables. | Shared concept, different call path. |
| Workers and queues | SQLite queue commands and SSE demo event stream. | PostgreSQL background worker, external `pg-mindbrain-worker`, BM25 parallel workers, process outbox. | Postgres-only extension/runtime surface. |
| SQL migration model | Single SQLite bootstrap script, idempotent object creation. | Versioned extension install body plus additive deltas and `ALTER EXTENSION`. | Fundamentally different migration model. |

## Files

- [api-comparison.md](api-comparison.md) compares public capabilities and API
  shapes.
- [sql-migrations-comparison.md](sql-migrations-comparison.md) compares SQL
  installation, versioning, and migration rules.

## Practical Reading Rule

Use this repository's docs when the deployment embeds or proxies SQLite through
MindBrain. Use `../pg_mindbrain` docs when the deployment installs a PostgreSQL
extension and expects SQL functions, extension ownership, PostgreSQL worker
settings, and versioned upgrade scripts.
