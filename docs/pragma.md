# Pragma

The canonical Pragma documentation now lives in [pragma/README.md](pragma/README.md).

Pragma is MindBrain's context-packing layer for agent memory and projections. It
covers:

- memory raw rows: `memory_items`, `memory_projections`, `memory_edges`;
- durable projection rows: `projections`, `projection_types`, `agent_state`;
- proposition DSL parsing;
- scoped context packing;
- next-hop suggestions;
- CLI and HTTP pack routes.

## Start Here

| Document | Contents |
|----------|----------|
| [pragma/README.md](pragma/README.md) | Overview and source anchors. |
| [pragma/model-and-storage.md](pragma/model-and-storage.md) | Tables and row contracts. |
| [pragma/raw-layer.md](pragma/raw-layer.md) | Source-of-truth boundaries and projection meanings. |
| [pragma/proposition-dsl.md](pragma/proposition-dsl.md) | Structured `fact|...` line format. |
| [pragma/context-packing.md](pragma/context-packing.md) | Ranking, scope, pack priority, next hops. |
| [pragma/queries-and-apis.md](pragma/queries-and-apis.md) | SQL, CLI, HTTP, and GhostCrab-compatible routes. |
| [pragma/examples-immeuble-demo.md](pragma/examples-immeuble-demo.md) | Current `immeuble-demo` snapshot. |

## Native Status

| Symbol | Status |
|--------|--------|
| `pragma_parse_proposition_line` | Implemented in Zig. |
| `pragma_rank_native` / `pragma_rank_zig` | Stub, returns empty set. |
| `pragma_next_hops_native` / `pragma_next_hops_zig` | Stub, returns empty set. |

See [native-reference.md](native-reference.md) for exported native symbols.
