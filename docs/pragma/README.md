# Pragma

Pragma is MindBrain's context-packing layer for agent memory. It ranks compact
memory projections, parses structured proposition lines, suggests next hops,
and exposes packed context through SQL, the standalone CLI, and the HTTP API.

The main rule: pragma rows are retrieval material, not the primary source of
truth. They should point back to raw documents, graph relations, ontology
nodes, facts, or agent decisions when those sources exist.

## What It Can Do

| Capability | Surface | Status |
|------------|---------|--------|
| Parse proposition DSL lines | `src/mb_pragma/dsl_parser.zig`, `src/standalone/pragma_dsl.zig` | Implemented |
| Rank SQLite memory projections | `src/standalone/pragma_sqlite.zig` | Implemented |
| Pack scoped SQLite context | `packContextScoped`, CLI `pack`, HTTP `/api/mindbrain/pack` | Implemented |
| Configure projection type aliases and priorities | `projection_types`, `pragma_projection_types.zig` | Implemented |
| Suggest next hops from propositions and memory edges | `pragma_sqlite.nextHops` | Implemented in standalone |
| PostgreSQL native rank / next-hop symbols | `src/mb_pragma/main.zig` | Stub, empty result |
| GhostCrab projection pack compatibility | `/api/mindbrain/ghostcrab/pack-projections` | Implemented over durable `projections` |

## Layers

| Layer | Tables / code | Role |
|-------|---------------|------|
| Memory raw rows | `memory_items`, `memory_projections`, `memory_edges` | Legacy memory-style rows used by standalone pragma ranking and packing. |
| Durable agent projections | `projections`, `projection_types`, `agent_state` | Agent-facing facts, goals, steps, constraints, and type policy. |
| Native extension shim | `src/mb_pragma` | PostgreSQL exported symbols and parser wiring. |
| Standalone implementation | `src/standalone/pragma_*` | SQLite rank, pack, next-hop, DSL, and type matching. |
| HTTP / CLI | `src/standalone/http_app.zig`, `tool.zig` | Operational endpoints and `mindbrain-standalone-tool pack`. |

## Documentation Map

| Document | Contents |
|----------|----------|
| [model-and-storage.md](model-and-storage.md) | Tables and row contracts. |
| [raw-layer.md](raw-layer.md) | Source-of-truth boundaries and projection meanings. |
| [proposition-dsl.md](proposition-dsl.md) | Structured `fact|...` line format and parser behavior. |
| [context-packing.md](context-packing.md) | Ranking, scoped matching, pack order, and next-hop semantics. |
| [queries-and-apis.md](queries-and-apis.md) | SQL, CLI, HTTP, and GhostCrab-compatible routes. |
| [examples-immeuble-demo.md](examples-immeuble-demo.md) | Current snapshot from `data/immeuble-demo.sqlite`. |

## Source Anchors

- Native C/PostgreSQL symbols: [src/mb_pragma/main.zig](../../src/mb_pragma/main.zig)
- Native parser: [src/mb_pragma/dsl_parser.zig](../../src/mb_pragma/dsl_parser.zig)
- Standalone rank/pack/next-hop: [src/standalone/pragma_sqlite.zig](../../src/standalone/pragma_sqlite.zig)
- Standalone DSL parser: [src/standalone/pragma_dsl.zig](../../src/standalone/pragma_dsl.zig)
- Projection type policy: [src/standalone/pragma_projection_types.zig](../../src/standalone/pragma_projection_types.zig)
- SQLite schema: [sql/sqlite_mindbrain--1.0.0.sql](../../sql/sqlite_mindbrain--1.0.0.sql)
