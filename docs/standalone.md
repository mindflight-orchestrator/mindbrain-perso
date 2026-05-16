# Standalone (SQLite) engine and CLI

This repository builds a **standalone** stack backed by **SQLite**: facet/graph/pragma/workspace logic used for portability, tests, tooling, and the local HTTP dashboard/API.

Sources live under [src/standalone/](../src/standalone/). The reusable library surface is exported from [src/standalone/lib.zig](../src/standalone/lib.zig). The entry binaries kept in this repo are **`mindbrain-standalone-tool`**, defined in [src/standalone/tool.zig](../src/standalone/tool.zig), and **`mindbrain-http`**, defined in [src/standalone/http_server.zig](../src/standalone/http_server.zig) and wired in [build.zig](../build.zig).

For **LLM-assisted document profiling**, durable queue jobs, and offline corpus evaluation, see [document-profile.md](document-profile.md).

## Build targets

| Command | Output |
|---------|--------|
| `zig build` | Shared library + default artifacts (requires **Zig 0.16.0**; see [build.zig](../build.zig)) |
| `zig build test` | Unit tests: standalone engine + BM25 tests |
| `zig build standalone-tool` | Installs **`mindbrain-standalone-tool`** |
| `zig build standalone-http` | Installs **`mindbrain-http`** |
| `zig build benchmark-tool` | Installs **`mindbrain-benchmark-tool`** |
| `zig build bench-standalone` | Runs **`standalone-bench`** (linked with SQLite) |
| `scripts/test-standalone-http-contract.sh` | Builds `mindbrain-http`, starts it on a temporary SQLite DB, and verifies the fact-write and SQL-session HTTP contract |
| `./scripts/prebuilds-all.sh` | Cross-compiles standalone binaries into **`prebuilds/{platform-arch}/bin/`** with **Zig 0.16.x** |

Linking requires **libsqlite3** on the system for standalone binaries and tests.
Cross-compiling the standalone binaries also requires target-compatible SQLite development/runtime libraries for each target triple.

## HTTP server: `mindbrain-http`

`mindbrain-http` serves the SQLite-backed API and static dashboard assets from the standalone layer. The route-by-route contract is maintained in [api-reference.md](api-reference.md).

### Security model

Treat `mindbrain-http` as a **trusted-local admin surface**, not a public API. The SQL routes (`POST /api/mindbrain/sql`, `POST /api/mindbrain/sql/session/open`, `POST /api/mindbrain/sql/session/query`, `POST /api/mindbrain/sql/session/close`) are **unauthenticated** and can execute arbitrary SQLite statements. The fact-write route (`POST /api/mindbrain/facts/write`) can persist durable rows into the standalone fact store. By default the server should stay on **loopback** (`127.0.0.1:8091`). Only bind to `0.0.0.0` or another non-loopback IP when the deployment is on a trusted network segment or behind your own auth/reverse-proxy layer.

### Bind configuration

- `MINDBRAIN_HTTP_ADDR` sets the listen address from the environment, including both IP and port, for example `127.0.0.1:8091`, `0.0.0.0:8091`, or `[::1]:8091`.
- `MINDBRAIN_SQLITE_BUSY_TIMEOUT_MS` or `--sqlite-busy-timeout-ms` sets the SQLite busy timeout. The default is `1000`, so HTTP writer/session contention fails quickly instead of tying up requests for a full test timeout.
- `--addr` overrides `MINDBRAIN_HTTP_ADDR` when you need a one-off CLI override.
- `MINDBRAIN_HTTP_MAX_BODY_BYTES` caps SQL JSON request bodies.
- `MINDBRAIN_HTTP_MAX_CONNS` caps concurrent client connections.

### Route sensitivity

| Route group | Sensitivity | Notes |
|-------------|-------------|-------|
| `POST /api/mindbrain/sql*` | High | Arbitrary SQL and transaction/session control. Trusted operators only. |
| `POST /api/mindbrain/facts/write` | High | Durable fact-store mutation. Allocates `doc_id` and writes `facets` rows for downstream retrieval/packing clients. |
| `GET /api/mindbrain/sql/write-status` | Medium | Reports serialized writer-lane status and counters. |
| `GET /api/mindbrain/workspace-export*` | High | Full workspace model export. |
| `GET /api/mindbrain/pack`, `GET /api/mindbrain/ghostcrab/pack-projections`, `GET /api/mindbrain/ghostcrab/projection-get` | High | Retrieval/projection output can expose packed context, evidence, or operational projection rows. |
| `GET /api/mindbrain/coverage*`, `GET /api/mindbrain/graph-*`, `GET /api/mindbrain/traverse`, `GET /api/mindbrain/ghostcrab/graph-search`, `GET /api/events`, `GET /api/mindbrain/search-compact-info`, `GET /api/mindbrain/simulate` | Medium | Read-heavy operational and graph/search surfaces; still avoid exposing to untrusted callers. |
| `GET /health`, static assets | Low | Basic liveness/static serving only. |

## CLI: `mindbrain-standalone-tool`

Usage strings from the tool:

```text
mindbrain-standalone-tool traverse --db <sqlite_path> --start <node_id> [--direction outbound|inbound] [--depth <n>] [--target <node_id>] [--edge-label <label> ...]
mindbrain-standalone-tool workspace-export --db <sqlite_path> --workspace-id <id>
mindbrain-standalone-tool workspace-create --db <sqlite_path> --workspace-id <id> [--label <text>] [--description <text>] [--profile <name>]
mindbrain-standalone-tool collection-create --db <sqlite_path> --workspace-id <id> --collection-id <id> --name <name> [--chunk-bits <n>] [--language <lang>]
mindbrain-standalone-tool ontology-register --db <sqlite_path> --workspace-id <id> --ontology-id <id> --name <name> [--version <v>] [--source-kind <kind>]
mindbrain-standalone-tool ontology-attach --db <sqlite_path> --workspace-id <id> --collection-id <id> --ontology-id <id> [--role <role>]
mindbrain-standalone-tool collection-export --db <sqlite_path> --workspace-id <id> [--collection-id <id>] [--output <file>]
mindbrain-standalone-tool collection-import --db <sqlite_path> --bundle <file>
mindbrain-standalone-tool document-ingest --db <sqlite_path> --workspace-id <id> --collection-id <id> --doc-id <n> [--nanoid <id>] [--source-ref <uri>] [--language <lang>] [--ingested-at <iso>] [--ontology-id <id>] [--strategy fixed_token|sentence|paragraph|recursive_character|structure_aware] [--target-tokens <n>] [--overlap-tokens <n>] [--max-chars <n>] [--min-chars <n>] (--content <text> | --content-file <path>)
mindbrain-standalone-tool document-by-nanoid --db <sqlite_path> --nanoid <id>
mindbrain-standalone-tool document-normalize --input <path> --output-dir <dir> [--languages fr,nl] [--split-by-language] [--pdf-backend auto|pdftotext|ocrmypdf|deepseek|none] [--html-backend pandoc|builtin-strip] [--deepseek-command <template>]
mindbrain-standalone-tool document-profile (--content <text> | --content-file <path> | --content-dir <path>) (--base-url <url> --model <name> | --mock-profile-json <path> | --dry-run) [--api-key <key>] [--source-ref <ref>]
mindbrain-standalone-tool document-profile-enqueue --db <sqlite_path> (--content-file <path> | --content-dir <path>) [--queue <name>] [--include-ext md,txt] [--workspace-id <id> --collection-id <id> (--doc-id <n> | --doc-id-start <n>)] [--language <lang>]
mindbrain-standalone-tool document-profile-worker --db <sqlite_path> (--base-url <url> --model <name> | --mock-profile-json <path>) [--queue <name>] [--vt <sec>] [--limit <n>] [--api-key <key>] [--archive-failures] [--contextual-retrieval] [--contextual-doc-chars <n>] [--contextual-max-tokens <n>] [--contextual-search-table-id <n>] [--embedding-base-url <url>] [--embedding-api-key <key>] [--embedding-model <name>]
mindbrain-standalone-tool contextual-search --db <sqlite_path> --table-id <n> --query <text> [--base-url <url> --embedding-model <name> [--api-key <key>]] [--limit <n>] [--vector-weight <0..1>] [--rerank --rerank-base-url <url> --rerank-model <name> [--rerank-api-key <key>] [--rerank-candidates <n>] [--rerank-max-doc-chars <n>]]
mindbrain-standalone-tool search-embedding-batch --db <sqlite_path> --table-id <n> --embedding-base-url <url> --embedding-model <name> [--embedding-api-key <key>] [--limit <n>] [--missing-only]
mindbrain-standalone-tool corpus-eval [--fixtures <dir>] [--case <name>]
mindbrain-standalone-tool external-link-add --db <sqlite_path> --workspace-id <id> --source-collection-id <id> --source-doc-id <n> --target-uri <uri> [--source-chunk-index <n>] [--edge-type <name>] [--weight <float>] [--link-id <n>] [--metadata-json <json>]
mindbrain-standalone-tool graph-path --db <sqlite_path> --source <name> --target <name> [--edge-label <label> ...] [--max-depth <n>]
mindbrain-standalone-tool search-compact-info --db <sqlite_path>
mindbrain-standalone-tool benchmark-db [--db <sqlite_path>] [--query-iterations <n>] [--mutation-iterations <n>]
mindbrain-standalone-tool seed-demo --db <sqlite_path>
mindbrain-standalone-tool bootstrap-from-sql --db <sqlite_path> --sql-file <path>
mindbrain-standalone-tool workspace-export-by-domain --db <sqlite_path> --domain-or-workspace <id>
mindbrain-standalone-tool coverage --db <sqlite_path> --workspace-id <id> [--entity-type <type> ...]
mindbrain-standalone-tool coverage-by-domain --db <sqlite_path> --domain-or-workspace <id> [--entity-type <type> ...]
mindbrain-standalone-tool pack --db <sqlite_path> --user-id <id> --query <text> [--scope <scope>] [--limit <n>]
mindbrain-standalone-tool queue-send --db <sqlite_path> --queue <name> --message <text>
mindbrain-standalone-tool queue-read --db <sqlite_path> --queue <name> [--vt <seconds>] [--limit <n>]
mindbrain-standalone-tool queue-archive --db <sqlite_path> --queue <name> --msg-id <id>
mindbrain-standalone-tool queue-delete --db <sqlite_path> --queue <name> --msg-id <id>
mindbrain-standalone-tool simulate
```

Run `mindbrain-standalone-tool` with no arguments (or with an unknown first argument) to print the full usage list to stderr, matching [tool.zig](../src/standalone/tool.zig) `printUsage`.

- **`traverse`** — Graph walk from a start node; prints JSON (`target_found`, `rows`).
- **`workspace-export`** — Emits **TOON** workspace model export to stdout.
- **`workspace-create` / `collection-create` / `collection-export` / `collection-import`** — Workspace and collection lifecycle plus portable JSON bundle export/import.
- **`ontology-register` / `ontology-attach`** — Register workspace-scoped ontologies and attach them to collections.
- **`graph-path`** — Path finding between named nodes.
- **`benchmark-db`** — Runs facet and graph query/mutation benchmarks against a SQLite database and returns JSON with embedded TOON payloads for the facet and graph query results.
- **`graph/subgraph`** — SSE graph stream for browser clients (`seed_node`, `node`, `edge`, `done`).
- **`coverage` / `coverage-by-domain`** — Emits a TOON `coverage_report` with a summary and per-gap rows for ontology or taxonomy nodes that are not currently covered by the graph.
- **`pack`** — Context packing-style retrieval for a user/query (SQLite implementation).
- **`document-ingest` / `document-by-nanoid` / `external-link-add`** — Raw document/chunk ingestion, public document-id lookup, and cross-document link insertion.
- **`document-normalize`** — Orchestrates external PDF/HTML extraction tools (`pdftotext`, `ocrmypdf`, `pandoc`, or a DeepSeek command template) and writes normalized `.txt` / `.md` plus sidecar metadata.
- **`document-profile` / `document-profile-enqueue` / `document-profile-worker`** — LLM document “semantic id card” JSON, optional SQLite queue, optional persist to `documents_raw` and `chunks_raw` (see [document-profile.md](document-profile.md)).
- **`contextual-search`** — BM25 search by default. When embedding provider flags are present and `search_embeddings` has rows for the table, it embeds the live query and adds vector fusion. If embedding flags are omitted, or indexed embeddings are absent, it stays BM25-only and reports `semantic_status` in JSON. Optional `--rerank` performs non-default LLM score reranking over retrieved candidates.
- **`search-embedding-batch`** — Populates `search_embeddings` for existing `search_documents` rows outside the live search request path.
- **`POST /api/mindbrain/search-embedding-upsert`** — HTTP JSON write path for clients that already have embedding arrays; stores them as packed `f32` blobs in `search_embeddings`.
- **`seed-demo` / `bootstrap-from-sql`** — Seed the built-in demo dataset or execute a SQL bootstrap file against a SQLite database.
- **`corpus-eval`** — Deterministic checks against [fixtures/](../fixtures/corpus_eval/) (no network).
- **`queue-*`** — Lightweight message queue operations on the SQLite runtime (`queue_messages` and helpers in [`queue_sqlite.zig`](../src/standalone/queue_sqlite.zig)).
- **`simulate`** — Internal simulation entrypoint (see source for behavior).

Dataset-specific demo and benchmark commands live in [demo-benchmark.md](demo-benchmark.md) and are built as the separate `mindbrain-benchmark-tool`.

## Coverage report shape

The `coverage` and `coverage-by-domain` commands emit a TOON `coverage_report` with:

- `summary`: workspace-level counters such as `covered_nodes`, `total_nodes`, `graph_entities`, `facet_rows`, `projection_rows`, and `coverage_ratio`.
- `gaps`: one row per uncovered ontology or taxonomy node.

Current `gaps` columns are:

| Column | Meaning |
|--------|---------|
| `id` | Canonical node identity derived from facet JSON (`node_id`, `entity_id`, `name`, or `label`). |
| `label` | Human-readable label for the missing node. |
| `entity_type` | Facet entity type for the node. |
| `criticality` | Optional facet criticality, defaulting to `normal`. |
| `decayed_confidence` | Secondary confidence metric for the gap node, derived from the graph confidence-decay helper when a related graph entity can be identified. This is not the same thing as coverage ratio or raw node confidence. It can be `null` when no matching graph entity exists to decay against. |

## Library surface

External Zig code can import the standalone library module and reuse the SQLite-backed storage helpers, graph traversal, queue, and search helpers without taking the CLI or HTTP server entrypoints:

```zig
const mindbrain = @import("mindbrain");
const db = try mindbrain.facet_sqlite.Database.open("data/mindbrain.sqlite");
```

## Schema

SQLite DDL and migrations are maintained in Zig modules such as [src/standalone/sqlite_schema.zig](../src/standalone/sqlite_schema.zig) and applied via helpers like `applyStandaloneSchema()` where used.

The standalone fact-store contract uses the `facets` table as the durable fact
row table. MindBrain allocates `facets.doc_id` transactionally in
`POST /api/mindbrain/facts/write`, appends rows when `source_ref` is absent, and
upserts synchronized rows by `(workspace_id, source_ref)` when `source_ref` is
present. Fresh schemas also keep a compatibility trigger that assigns `doc_id`
for legacy raw SQL inserts that omit it, but new clients should use the HTTP
write API instead of allocating or omitting `doc_id` themselves.

## Relationship to the source tree

The standalone layer **mirrors concepts** from the graph, workspace, and search code paths in this repository. Use the standalone runtime for embedded or CI-style scenarios.
