# API Comparison

MindBrain and `pg_mindbrain` expose similar knowledge-runtime concepts, but
their public API boundaries are not equivalent.

MindBrain is a SQLite service boundary. It exposes a local HTTP API, a CLI, and
SQLite tables. `pg_mindbrain` is a PostgreSQL extension boundary. It exposes
schema-qualified SQL functions, extension-owned objects, and PostgreSQL worker
surfaces.

## Capability Matrix

| Capability | MindBrain API | `pg_mindbrain` API | Status |
| --- | --- | --- | --- |
| Runtime health | `GET /health` | PostgreSQL connection plus extension/runtime checks in operations docs. | Different abstraction. |
| Arbitrary SQL | `POST /api/mindbrain/sql`, session open/query/close routes. | Direct SQL through PostgreSQL client; no HTTP SQL proxy in the extension. | SQLite-only wrapper. |
| Workspace export | `GET /api/mindbrain/workspace-export`, `workspace-export` CLI. | `mb_ontology.export_workspace_model`, `mindbrain.*` metadata, `mb_collections.ensure_workspace`. | Shared concept. |
| Coverage | `GET /api/mindbrain/coverage`, `coverage` CLI. | `mb_ontology.coverage`, `coverage_by_domain`, TOON variants. | Shared concept. |
| Graph traversal | `GET /api/mindbrain/traverse`, `graph-path`, `graph/subgraph`, CLI equivalents. | `graph.k_hops_filtered`, `shortest_path_filtered`, `stream_subgraph`, `entity_neighborhood`, graph helper functions. | Shared concept, different API. |
| Graph search | `GET /api/mindbrain/ghostcrab/graph-search`. | `graph.entity_fts_search`, `graph.marketplace_search`, `mb_ontology.marketplace_search`, embedding search helpers. | Shared concept, Postgres broader. |
| Faceted search | `search-compact-info`, SQLite search/facet stores, raw SQL through proxy. | `facets.search_documents`, `search_documents_with_facets`, filter/count/top/histogram APIs. | Shared concept, different API. |
| BM25 operations | SQLite search tables and CLI indexing paths. | `facets.bm25_*` SQL API, status/progress/cleanup, parallel workers. | Postgres broader. |
| Collections | `workspace-create`, `collection-create`, `collection-export/import`, raw SQLite tables. | `mb_collections.ensure_*`, raw tables, chunk ingest, document-link extraction and search. | Shared data model. |
| Document normalization/profile | CLI document commands and SQLite queue. | `mb_bg_worker` provider/job SQL API plus `pg-mindbrain-worker`. | Shared workflow, different ownership. |
| Projections/context packing | `GET /api/mindbrain/pack`, `ghostcrab/pack-projections`, `projection-get`, CLI `pack`. | `mb_pragma.pragma_pack_context*`, projection tables, public pragma wrappers. | Shared concept. |
| Queue/process events | `queue-*` CLI and `GET /api/events` demo stream. | `mb_bg_worker.*`, `mb_process.*`, optional internal bgworker. | Different abstraction. |
| State graph | No first-class HTTP API documented here beyond graph/coverage/projection reads. | `mb_state_graph.*`, including `transition_target` in 1.5.4. | Postgres-only extension API. |

## MindBrain HTTP Surface

MindBrain's stable application-facing HTTP routes are grouped as:

- SQL/admin: `/api/mindbrain/sql`, `/api/mindbrain/sql/session/open`,
  `/api/mindbrain/sql/session/query`, `/api/mindbrain/sql/session/close`.
- Health and events: `/health`, `/api/events`, `/api/mindbrain/events`.
- Workspace/coverage: `/api/mindbrain/coverage`,
  `/api/mindbrain/coverage-by-domain`, `/api/mindbrain/workspace-export`,
  `/api/mindbrain/workspace-export-by-domain`.
- Graph/search/context: `/api/mindbrain/search-compact-info`,
  `/api/mindbrain/graph-path`, `/api/mindbrain/graph/subgraph`,
  `/api/mindbrain/traverse`, `/api/mindbrain/pack`.
- Compatibility reads for downstream GhostCrab SQLite integrations:
  `/api/mindbrain/ghostcrab/pack-projections`,
  `/api/mindbrain/ghostcrab/projection-get`,
  `/api/mindbrain/ghostcrab/graph-search`.

These routes are trusted-local admin/runtime routes. The SQL routes are
unauthenticated and intentionally documented as local-only unless protected by
an external proxy.

## MindBrain CLI Surface

The CLI is the operational and import surface around the SQLite runtime:

- Workspace and collections: `workspace-create`, `workspace-export`,
  `workspace-export-by-domain`, `collection-create`, `collection-export`,
  `collection-import`.
- Ontology and coverage: `ontology-register`, `ontology-attach`, `coverage`,
  `coverage-by-domain`.
- Documents and chunks: `document-ingest`, `document-by-nanoid`,
  `document-normalize`, `external-link-add`.
- LLM profile and retrieval: `document-profile`, `document-profile-enqueue`,
  `document-profile-worker`, `contextual-search`.
- Graph and context: `traverse`, `graph-path`, `search-compact-info`, `pack`.
- Queue and maintenance: `queue-send`, `queue-read`, `queue-archive`,
  `queue-delete`, `seed-demo`, `bootstrap-from-sql`, `benchmark-db`,
  `corpus-eval`, `simulate`.

The CLI maps to SQLite files. It is not a PostgreSQL extension API and does not
own PostgreSQL catalog state.

## `pg_mindbrain` SQL Surface

The sibling extension's public API is organized by PostgreSQL schemas:

- `facets`: facet construction, generated facet storage, filtering, counts,
  BM25 indexing/search, typed filters, operational BM25 cleanup and status.
- `mb_collections`: workspace, collection, ontology attachment, raw
  documents/chunks, raw graph rows, document-link extraction, typed facet
  assignments, reindex helpers.
- `graph`: entity/relation storage, aliases, traversal, shortest paths,
  subgraph streaming, graph search, graph embeddings and similarity.
- `mb_state_graph`: state graph metadata, state/transition/binding APIs,
  explicit transition execution in 1.5.4.
- `mb_process`: registered processes, event types, external targets, outbox
  lifecycle and event consumers.
- `mb_bg_worker`: provider configuration, credentials, profile jobs, document
  link extraction jobs, worker claims/completion/failure, internal heartbeat.
- `mb_ontology`: ontology registry, coverage, domain search, workspace export,
  conflict detection, workspace bridges, TOON output helpers.
- `mb_pragma` and public wrappers: projection parsing, candidate/ranking
  helpers, context packing, projection tables.
- `mindbrain`: metadata, source mappings, pending migrations, table/column and
  relation semantics.

Unlike MindBrain, callers invoke this surface inside PostgreSQL transactions.
There is no HTTP proxy layer in the extension itself.

## Important Non-Equivalences

| Topic | Why it matters |
| --- | --- |
| HTTP route vs SQL function | A MindBrain endpoint may combine SQLite SQL, TOON encoding, JSON response shaping, and compatibility behavior. A `pg_mindbrain` function is a database function with PostgreSQL typing and extension ownership. |
| SQLite session vs PostgreSQL transaction | MindBrain SQL sessions wrap SQLite `BEGIN IMMEDIATE`; `pg_mindbrain` relies on PostgreSQL transactions and `ALTER EXTENSION` transactional upgrades. |
| Queue model | MindBrain has a simple SQLite queue CLI and SSE demo stream. `pg_mindbrain` has provider/job SQL tables, worker leases, attempts, and process outbox semantics. |
| Worker ownership | MindBrain document profile work is CLI/runtime driven. `pg_mindbrain` owns worker control-plane SQL plus an executable worker. |
| State graph | `pg_mindbrain` exposes state-graph transitions and process events; MindBrain has no equivalent first-class documented HTTP state-graph API. |
| Migration ownership | MindBrain initializes a SQLite runtime. `pg_mindbrain` must preserve PostgreSQL extension membership and adjacent upgrade scripts. |

## Porting Guidance

When translating behavior from MindBrain to `pg_mindbrain`, port the capability,
not the endpoint name. For example:

- `GET /api/mindbrain/coverage` maps conceptually to `mb_ontology.coverage`,
  but response shape, transaction context, and access path differ.
- `mindbrain-standalone-tool document-ingest` maps conceptually to
  `mb_collections.ingest_document_chunked` plus related raw-table helpers.
- `GET /api/mindbrain/graph/subgraph` maps conceptually to
  `graph.stream_subgraph`, but the SQLite route returns an SSE-formatted body
  while the extension exposes SQL rows/functions.
- `queue-*` commands do not map directly to `mb_bg_worker`; the Postgres worker
  path has provider readiness, credentials, claims, leases, attempts, and
  completion/failure semantics.

When translating from `pg_mindbrain` back to MindBrain, check whether the
feature exists as a local HTTP route, a CLI command, a SQLite helper, or only as
a sibling extension capability.
