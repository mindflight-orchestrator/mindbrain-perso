# API reference

This page documents the current public API surface exposed by this checkout.
It is derived from the active entrypoints:

- HTTP server: [`src/standalone/http_server.zig`](../src/standalone/http_server.zig)
- CLI: [`src/standalone/tool.zig`](../src/standalone/tool.zig)
- Build wiring: [`build.zig`](../build.zig)

MindBrain is SQLite-first in this repository. The native shared library is
still built as `pg_mindbrain`, but the operational API that applications call in
this checkout is the standalone SQLite CLI plus the local HTTP server.

## HTTP server

Build and run:

```bash
/opt/zig/zig-x86_64-linux-0.16.0/zig build standalone-http
./zig-out/bin/mindbrain-http --addr 127.0.0.1:8091 --db data/mindbrain.sqlite
```

Use any equivalent Zig 0.16.0 binary if your local path differs.

Environment and CLI options:

| Option | Meaning | Default |
|--------|---------|---------|
| `--addr <ip:port>` / `MINDBRAIN_HTTP_ADDR` | Listen address. Use bracketed IPv6, for example `[::1]:8091`. | `127.0.0.1:8091` |
| `--db <sqlite_path>` / `MINDBRAIN_DB_PATH` | SQLite database path. Parent directories are created. | `data/mindbrain.sqlite` |
| `--static-dir <dir>` / `MINDBRAIN_STATIC_DIR` | Directory for static assets. Unknown GET/HEAD paths fall through to this directory. | `dashboard/dist` |
| `--init-only` | Initialize the database schema and exit without listening. | off |
| `MINDBRAIN_HTTP_MAX_BODY_BYTES` | Maximum JSON body size for SQL requests. | `1048576` |
| `MINDBRAIN_HTTP_MAX_CONNS` | Maximum concurrent accepted connections. | `128` |
| `--sqlite-busy-timeout-ms <n>` / `MINDBRAIN_SQLITE_BUSY_TIMEOUT_MS` | SQLite busy timeout for backend connections. | `1000` |

### Security

`mindbrain-http` is a trusted-local admin surface. It has no built-in
authentication. The SQL endpoints execute arbitrary SQLite statements, so keep
the server bound to loopback or place it behind an external auth/proxy layer.

### Response rules

- `GET` and `HEAD` are accepted for read routes and static assets.
- SQL and fact-write routes accept `POST` only.
- Unsupported methods return `405`.
- Missing or invalid required parameters return `400`.
- Missing sessions or unknown domain/workspace lookups return `404` where the
  handler can distinguish that condition.
- Successful JSON endpoints use `application/json; charset=utf-8`.
- TOON/text endpoints use `text/plain; charset=utf-8`.
- Event streams use `text/event-stream; charset=utf-8`.

### SQL endpoints

| Method | Path | Body | Response |
|--------|------|------|----------|
| `POST` | `/api/mindbrain/sql` | `{ "sql": "...", "params": [...] }` | `{ ok, columns, rows, changes, last_insert_rowid }` |
| `POST` | `/api/mindbrain/sql/session/open` | empty body | `{ ok, session_id }` and starts `BEGIN IMMEDIATE` |
| `POST` | `/api/mindbrain/sql/session/query` | `{ "session_id": 1, "sql": "...", "params": [...] }` | Same shape as `/sql`; session is required |
| `POST` | `/api/mindbrain/sql/session/close` | `{ "session_id": 1, "commit": true }` | `{ ok, session_id, committed }` |
| `GET` | `/api/mindbrain/sql/write-status` | none | `{ ok, mode, active_session_id, completed, failed, busy_timeout_ms, last_error }` |

`params` is an array of JSON values. Unknown JSON fields are rejected. If a
non-session SQL request has no parameters and contains multiple statements, the
server executes it through SQLite `exec` and returns only mutation metadata.

Standalone HTTP serializes writes through one writer connection using WAL,
`busy_timeout`, and `synchronous=NORMAL`. SQL writes, SQL sessions, fact writes,
and `/api/mindbrain/simulate` use that writer lane; read-only SQL uses separate
read connections. `/api/mindbrain/sql/write-status` reports the current writer
mode, active SQL session if any, and completed/failed writer operation counters.

SQL execution failures return JSON with `ok:false` and an `error` object that
contains the MindBrain operation name, SQLite primary and extended result codes,
and the raw SQLite error message:

```json
{
  "ok": false,
  "error": {
    "kind": "StepFailed",
    "operation": "step",
    "sqlite_code": 19,
    "sqlite_extended_code": 2067,
    "sqlite_message": "UNIQUE constraint failed: facets.doc_id"
  }
}
```

Session close always closes the SQLite handle after attempting the requested
`COMMIT` or `ROLLBACK`; if `COMMIT` fails, the server attempts a rollback before
returning the error payload.

### Fact write endpoint

| Method | Path | Body | Response |
|--------|------|------|----------|
| `POST` | `/api/mindbrain/facts/write` | `{ "workspace_id": "default", "schema_id": "...", "content": "...", "facets_json": "{}", "source_ref": "...", "created_by": "...", "valid_from_unix": 0, "valid_until_unix": 0 }` | `{ ok, id, doc_id, created, updated }` |

Required fields are `schema_id` and `content`; both must be non-empty.
`workspace_id` defaults to `default`. `facets_json` defaults to `{}` and must
parse as a JSON object text. `source_ref` is optional; an empty string is
normalized to null by the HTTP layer.

The endpoint is the standalone durable fact-store write API. It writes to the
`facets` table and keeps the legacy `facets` text column and `facets_json`
column in sync. `doc_id` is allocated by MindBrain inside the same SQLite write
transaction as the insert, using the next integer above the current maximum
`facets.doc_id`.

Write behavior:

- When `source_ref` is absent, each request appends a new fact row and returns a
  new `id` and `doc_id`.
- When `source_ref` is present, rows are upserted by `(workspace_id,
  source_ref)`. Existing rows keep their original `id` and `doc_id` and return
  `updated:true`; new rows return `created:true`.
- Fresh schemas keep `facets.doc_id` unique and use a compatibility trigger to
  allocate it for legacy raw SQL inserts that omit the value. Downstream
  search/pack clients should still treat non-null `doc_id` as the durable
  integer document key, and new clients should prefer this endpoint instead of
  writing `facets` directly.

### Read endpoints

| Method | Path | Query | Response |
|--------|------|-------|----------|
| `GET`/`HEAD` | `/health` | none | `ok\n` |
| `GET`/`HEAD` | `/api/mindbrain/simulate` | none | JSON simulation event summary; also writes to `demo_firehose` |
| `GET` | `/api/events` | none | Long-lived SSE stream reading `demo_firehose` |
| `GET` | `/api/mindbrain/events` | none | Alias of `/api/events` |
| `GET`/`HEAD` | `/api/mindbrain/search-compact-info` | none | TOON compact search snapshot |
| `GET`/`HEAD` | `/api/mindbrain/coverage` | `workspace_id`, repeated `entity_type` optional | TOON coverage report |
| `GET`/`HEAD` | `/api/mindbrain/coverage-by-domain` | `domain_or_workspace`, repeated `entity_type` optional | TOON coverage report after workspace resolution |
| `GET`/`HEAD` | `/api/mindbrain/workspace-export` | `workspace_id` | TOON workspace model export |
| `GET`/`HEAD` | `/api/mindbrain/workspace-export-by-domain` | `domain_or_workspace` | TOON workspace export after workspace resolution |
| `GET`/`HEAD` | `/api/mindbrain/graph-path` | `source`, `target`, repeated `edge_label` optional, `max_depth` optional | TOON shortest path |
| `GET`/`HEAD` | `/api/mindbrain/graph/subgraph` | `seed_ids=1,2`, `hops` optional, `edge_types=a,b` optional | SSE-formatted subgraph body |
| `GET`/`HEAD` | `/api/mindbrain/traverse` | `start`, `direction`, `depth`, `target` optional, repeated `edge_label` optional | JSON graph traversal result |
| `GET`/`HEAD` | `/api/mindbrain/pack` | `user_id`, `query`, `scope` optional, `limit` optional | TOON packed context |

### GhostCrab compatibility endpoints

These endpoints are implemented in the standalone HTTP server so downstream
GhostCrab SQLite integrations can consume MindBrain-owned read behavior.

| Method | Path | Query | Response |
|--------|------|-------|----------|
| `GET`/`HEAD` | `/api/mindbrain/ghostcrab/pack-projections` | `agent_id`, `query`, `scope` optional, `limit` optional | JSON projection rows for packed context |
| `GET`/`HEAD` | `/api/mindbrain/ghostcrab/projection-get` | `workspace_id`, `projection_id`, `collection_id` optional, `include_evidence` optional, `include_deltas` optional | JSON projection result, linked evidence, deltas, and report |
| `GET`/`HEAD` | `/api/mindbrain/ghostcrab/graph-search` | `workspace_id`, `query` optional, `collection_id` optional, repeated `entity_type` optional, `metadata_filters` optional, `limit` optional | JSON graph entity search result |

## CLI

Build:

```bash
/opt/zig/zig-x86_64-linux-0.16.0/zig build standalone-tool
```

Use any equivalent Zig 0.16.0 binary if your local path differs.

Run with no arguments to print the source-defined usage. Current commands:

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
mindbrain-standalone-tool contextual-search --db <sqlite_path> --table-id <n> --query <text> --base-url <url> --embedding-model <name> [--api-key <key>] [--limit <n>] [--vector-weight <0..1>]
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

Command families:

| Family | Commands |
|--------|----------|
| Workspace and collections | `workspace-create`, `workspace-export`, `workspace-export-by-domain`, `collection-create`, `collection-export`, `collection-import` |
| Ontology | `ontology-register`, `ontology-attach`, `coverage`, `coverage-by-domain` |
| Documents and chunks | `document-ingest`, `document-by-nanoid`, `document-normalize`, `external-link-add` |
| LLM profile and retrieval | `document-profile`, `document-profile-enqueue`, `document-profile-worker`, `contextual-search` |
| Graph | `traverse`, `graph-path` |
| Search and context | `search-compact-info`, `pack` |
| Queue | `queue-send`, `queue-read`, `queue-archive`, `queue-delete` |
| Demo and maintenance | `seed-demo`, `bootstrap-from-sql`, `benchmark-db`, `corpus-eval`, `simulate` |

## Native and SQL surfaces

The Zig build also creates a dynamic library named `pg_mindbrain` from
[`src/main.zig`](../src/main.zig). The SQLite install script in this repository
is [`sql/sqlite_mindbrain--1.0.0.sql`](../sql/sqlite_mindbrain--1.0.0.sql).

Function-level SQL behavior is documented in the topic pages:

- [facets.md](facets.md)
- [graph.md](graph.md)
- [pragma.md](pragma.md)
- [workspace.md](workspace.md)
- [collections.md](collections.md)
- [projections.md](projections.md)
- [sqlite-parity.md](sqlite-parity.md)
