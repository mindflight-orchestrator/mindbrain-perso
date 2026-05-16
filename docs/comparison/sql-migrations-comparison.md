# SQL and Migration Comparison

MindBrain and `pg_mindbrain` both ship SQL, but the SQL serves different
deployment models.

MindBrain ships a SQLite bootstrap/runtime schema. `pg_mindbrain` ships a
PostgreSQL extension install body plus versioned upgrade deltas.

## Install and Upgrade Model

| Topic | MindBrain SQLite | `pg_mindbrain` PostgreSQL |
| --- | --- | --- |
| Primary SQL file | `sql/sqlite_mindbrain--1.0.0.sql`. | `sql/pg_mindbrain--1.0.0.sql` plus later `sql/pg_mindbrain--<version>.sql` deltas. |
| Fresh install | Apply/bootstrap the SQLite schema into a SQLite database. | `CREATE EXTENSION pg_mindbrain` loads `1.0.0`, then `ALTER EXTENSION pg_mindbrain UPDATE TO '1.5.4'`. |
| Versioning | Single current SQLite schema file in this repo. | Extension version chain from `1.0.0` through `1.5.4`. |
| Upgrade mechanism | No extension catalog; schema creation is mostly idempotent. | PostgreSQL extension machinery and adjacent upgrade scripts. |
| Transaction semantics | SQLite statements and optional local session routes. | PostgreSQL transactions; `ALTER EXTENSION` runs upgrade scripts transactionally. |
| Rollback model | Recreate/restore the SQLite database or apply explicit local repair SQL. | No down-migrations; restore backup or keep catalog at previous version if upgrade transaction fails. |
| Generated install files | None documented for SQLite. | Build installs both `pg_mindbrain--<NEW>.sql` and `pg_mindbrain--<OLD>--<NEW>.sql` names for each delta. |

## MindBrain SQLite Schema Shape

`sql/sqlite_mindbrain--1.0.0.sql` is the single canonical current-state schema
file that creates the runtime tables with `CREATE TABLE IF NOT EXISTS`,
indexes, compatibility triggers, and seed rows. The standalone Zig runtime
embeds this file rather than maintaining a second DDL copy.

Major table groups:

| Group | Representative SQLite tables |
| --- | --- |
| Workspace metadata | `workspaces`, `table_semantics`, `column_semantics`, `relation_semantics`, `source_mappings`, `pending_migrations`. |
| Facets | `facet_tables`, `facet_definitions`, `facet_postings`, `facet_deltas`, `facet_value_nodes`, `facets`. |
| Graph | `graph_entity`, `graph_entity_alias`, `graph_relation`, `graph_entity_document`, `graph_lj_out`, `graph_lj_in`, `graph_execution_run`, `graph_knowledge_patch`, `graph_entity_degree`. |
| Search and BM25 | `search_documents`, `search_fts_docs`, `search_embeddings`, `search_document_stats`, `search_collection_stats`, `search_term_stats`, `search_term_frequencies`, `search_postings`, `bm25_sync_triggers`, `bm25_stopwords`. |
| Projections and memory | `projections`, `projection_types`, `agent_state`, `memory_items`, `memory_projections`, `memory_edges`. |
| Queue/events | `queue_registry`, `queue_messages`, plus signal/alert demo tables. |
| Collections and raw layer | `collections`, `ontologies`, `collection_ontologies`, `workspace_settings`, ontology vocabulary/type tables, `documents_raw`, `chunks_raw`, vector tables, raw facet/entity/relation/link tables. |

SQLite keeps these tables in one database namespace. The HTTP proxy and CLI
provide higher-level access patterns around that file.

## `pg_mindbrain` Extension Schema Shape

`../pg_mindbrain` separates objects into PostgreSQL schemas and versioned
release files.

Current version files documented by the sibling checkout:

| File | Adds |
| --- | --- |
| `pg_mindbrain--1.0.0.sql` | Base schemas: `facets`, `mb_collections`, `graph`, `mb_pragma`, `mindbrain`, `mb_ontology`, plus most public APIs. |
| `pg_mindbrain--1.1.0.sql` | Graph embeddings, entity chunks, collection typed facet assignments, graph projection/search additions. |
| `pg_mindbrain--1.2.0.sql` | `mb_state_graph`, `mb_process`, state graph APIs, process event outbox. |
| `pg_mindbrain--1.3.0.sql` | `mb_bg_worker` provider/job/attempt control plane. |
| `pg_mindbrain--1.3.1.sql` | Worker status helper. |
| `pg_mindbrain--1.3.2.sql` | Internal PostgreSQL background-worker heartbeat table and status extension. |
| `pg_mindbrain--1.4.0.sql` | Typed facet sidecars, range filters, histograms, upgrade-path support. |
| `pg_mindbrain--1.5.0.sql` | Unified facet constructor/filter/readers. |
| `pg_mindbrain--1.5.2.sql` | Document graph node/link extraction APIs and dedupe indexes. |
| `pg_mindbrain--1.5.3.sql` | Activable LLM-assisted Markdown link extraction planning and provider-readiness helpers. |
| `pg_mindbrain--1.5.4.sql` | Explicit state-graph transition executor and transition event type. |

PostgreSQL schema groups:

| Schema | Role |
| --- | --- |
| `facets` | Registered source tables, generated facet storage, BM25 tables, typed sidecars, search/count/filter/admin functions. |
| `mb_collections` | Workspace/collection raw layer, documents, chunks, raw facets, raw graph rows, typed facet assignments, document/external links. |
| `graph` | Entity/relation graph, aliases, adjacency, traversal/search, embeddings, projections and graph helper views. |
| `mb_state_graph` | State graphs, states, transitions, target bindings, transition outbox integration. |
| `mb_process` | Process registry, event types, external targets, event outbox lifecycle. |
| `mb_bg_worker` | Provider configs, credentials, document profile/link extraction jobs, attempts, internal heartbeat. |
| `mb_pragma` | Projection types, projections, agent state, projection/packing helpers. |
| `mindbrain` | Extension metadata, pending migrations, source mappings, table/column/relation semantics. |
| `mb_ontology` | Ontology registry, workspace bridges, entity/relation type registry, coverage/search/export/conflict helpers. |

## Migration Semantics

### MindBrain

MindBrain's SQLite SQL behaves like a runtime bootstrap:

- it enables SQLite foreign keys;
- it uses `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, and
  `CREATE TRIGGER IF NOT EXISTS`;
- it seeds projection types;
- it does not create PostgreSQL schemas, extension-owned objects, or extension
  upgrade aliases;
- it is consumed by the standalone runtime and tools, not by PostgreSQL's
  extension catalog.

The important compatibility concern is that local runtime code and the SQLite
file agree on table names and columns.

### `pg_mindbrain`

The sibling migration model is PostgreSQL-extension specific:

- `pg_mindbrain.control` keeps `default_version = '1.0.0'`.
- `CREATE EXTENSION pg_mindbrain` loads only the `1.0.0` install body.
- Later files are additive deltas and assume the previous version is present.
- Packaged builds install adjacent `OLD--NEW` aliases for
  `ALTER EXTENSION pg_mindbrain UPDATE TO '<NEW>'`.
- The test/development path can source the deltas in order with `\i`, but that
  path does not update `pg_extension.extversion`.
- Deltas must tolerate both extension-owned upgrade context and direct layered
  sourcing.
- Hard-cut releases can drop old public surface without shims; `1.5.0` does
  this for legacy facet constructors and typed filter objects.

The important compatibility concern is extension ownership: dropping, replacing
or moving objects must respect PostgreSQL's extension catalog.

## API Migration Differences

| Area | MindBrain SQLite migration concern | `pg_mindbrain` migration concern |
| --- | --- | --- |
| Facet API | Keep runtime helper code aligned with SQLite tables and HTTP/CLI response shapes. | Preserve/update SQL function signatures, generated storage, extension-owned objects, and migration guides. |
| Typed facets | SQLite has raw facet tables and helper code; no extension hard cut. | `1.5.0` unifies facet constructors and filter JSON; callers must rewrite old SQL calls before upgrade. |
| Document links | SQLite exposes CLI/import helpers and compatibility HTTP reads. | `1.5.2` adds deterministic extraction/apply/search; `1.5.3` adds LLM planning/provider readiness. |
| State graph | No first-class documented SQLite migration chain for state graph APIs here. | `1.2.0` adds state/process layers; `1.5.4` adds `mb_state_graph.transition_target`. |
| Workers | SQLite queue schema and CLI commands must stay runtime-compatible. | `1.3.x` adds provider/job tables, heartbeat, worker status, and PostgreSQL startup settings. |
| Operational install | Local file/bootstrap and executable build compatibility. | Extension files, shared library, PostgreSQL restart, dependencies, and `ALTER EXTENSION` chain. |

## Validation Implications

MindBrain validation should check:

- `mindbrain-http` and `mindbrain-standalone-tool` build with Zig 0.16.0;
- the SQLite schema initializes cleanly;
- documented HTTP routes and CLI commands match `src/standalone/http_server.zig`
  and `src/standalone/tool.zig`;
- SQLite table names used by docs match `sql/sqlite_mindbrain--1.0.0.sql`.

`pg_mindbrain` validation should check:

- packaged extension files include adjacent upgrade aliases;
- `CREATE EXTENSION pg_mindbrain` followed by
  `ALTER EXTENSION pg_mindbrain UPDATE TO '1.5.4'` succeeds;
- layered `\i sql/pg_mindbrain--*.sql` test path still converges;
- hard-cut migration guides describe required caller rewrites;
- extension-owned objects can be dropped/replaced only through safe migration
  patterns.

## Bottom Line

Treat MindBrain SQL as embedded SQLite runtime bootstrap SQL. Treat
`pg_mindbrain` SQL as PostgreSQL extension release SQL. They share ontology,
facet, graph, collection, and projection concepts, but their migration rules,
transaction boundaries, worker models, and public APIs are different.
