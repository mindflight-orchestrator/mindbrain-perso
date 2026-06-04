# Pragma Queries And APIs

Pragma is available through native SQL symbols, SQLite standalone helpers, the
CLI, and the local HTTP server.

## SQL / Native Symbols

The native PostgreSQL-facing surface is in [src/mb_pragma/main.zig](../../src/mb_pragma/main.zig).

| Function | Role | Current status |
|----------|------|----------------|
| `pragma_parse_proposition_line_native(text)` | Parse one DSL line and return JSON text. | Implemented |
| `pragma_parse_proposition_line(text)` | SQL wrapper that may cast native output to JSON/JSONB. | Wrapper-level |
| `pragma_candidate_bitmap(user_id, query, projection_types)` | SQL prefilter over memory projections. | SQL-side |
| `pragma_pack_context(user_id, query, limit_n)` | Unscoped context pack over memory projections. | SQL-side / standalone parity |
| `pragma_pack_context_scoped(user_id, query, scope, limit_n)` | Scoped context pack. | SQL-side / standalone parity |
| `pragma_rank_native(...)` | Native rank function. | Stub |
| `pragma_next_hops_native(...)` | Native next-hop function. | Stub |

The SQL tests under [test/sql/pragma](../../test/sql/pragma) show the expected
PostgreSQL fixture shape, including the `"memory-server"` schema pattern.

## Standalone Zig Helpers

| Helper | File | Role |
|--------|------|------|
| `insertMemoryItem` | `pragma_sqlite.zig` | Upsert a memory item. |
| `insertMemoryProjection` | `pragma_sqlite.zig` | Upsert a memory projection row. |
| `insertMemoryEdge` | `pragma_sqlite.zig` | Upsert a memory edge. |
| `rankNative` | `pragma_sqlite.zig` | SQLite rank implementation. |
| `packContext` | `pragma_sqlite.zig` | Unscoped pack. |
| `packContextScoped` | `pragma_sqlite.zig` | Scoped pack. |
| `nextHops` | `pragma_sqlite.zig` | Suggested next nodes. |
| `projectionTypeMatch` | `pragma_sqlite.zig` | Alias matching against `projection_types`. |

## CLI

```bash
mindbrain-standalone-tool pack \
  --db <sqlite_path> \
  --user-id <id> \
  --query <text> \
  [--scope <scope>] \
  [--limit <n>]
```

The CLI prints a TOON `pack_context` payload.

## HTTP

| Route | Query | Response |
|-------|-------|----------|
| `GET /api/mindbrain/pack` | `user_id`, `query`, optional `scope`, optional `limit` | TOON packed context from `memory_projections`. |
| `GET /api/mindbrain/ghostcrab/pack-projections` | `agent_id`, optional `query`, optional `scope`, optional `limit` | JSON rows from durable `projections`, with additive `analysis_plan` artifact compatibility fields. |
| `GET /api/mindbrain/ghostcrab/projections/relevance` | `agent_id`, `entity_name`, optional `query`, optional `scope`, optional `limit` | JSON durable projections ranked for graph context. |
| `GET /api/mindbrain/ghostcrab/projection-get` | `workspace_id`, `projection_id`, optional `collection_id`, evidence/delta flags | JSON materialized graph projection bundle with additive `answer_snapshot` artifact compatibility fields. |
| `GET /api/mindbrain/ghostcrab/artifact/{artifact_id}` | none | JSON answer artifact registry row. |
| `POST /api/mindbrain/ghostcrab/artifact/{artifact_id}/refresh` | none | Explicitly refresh a `live_answer_view`, incrementing version and writing one update event. |
| `GET /api/mindbrain/ghostcrab/artifact/{artifact_id}/events` | optional `limit` | JSON retained `answer_update_event` rows. |

`/api/mindbrain/pack` and `/ghostcrab/pack-projections` are intentionally not
the same route. The first is the memory-table pack path. The second is a
GhostCrab compatibility route over durable `projections`.

Answer artifact registry rows live outside both `memory_projections` and
durable `projections`. They provide stable artifact ids, versions, lifecycle
state, and retained update events for backend-owned answer surfaces.

## Useful Inspection Queries

```sql
SELECT type_name, compatibility_aliases, rank_bias, pack_priority
FROM projection_types
ORDER BY pack_priority, type_name;
```

```sql
SELECT projection_type, COUNT(*)
FROM memory_projections
GROUP BY projection_type
ORDER BY projection_type;
```

```sql
SELECT proj_type, status, COUNT(*)
FROM projections
GROUP BY proj_type, status
ORDER BY proj_type, status;
```
