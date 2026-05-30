# Context Packing

Context packing selects a small set of projection rows that an agent can use in
a prompt or decision step.

## Standalone Flow

`src/standalone/pragma_sqlite.zig` implements the SQLite path:

1. Load `memory_projections` for a `user_id`.
2. Load `projection_types` into an in-memory index.
3. Filter by requested type aliases, query text, and optional scope.
4. Score rows from rank hints, confidence, type bias, and content matches.
5. Sort by pack priority and score.
6. Return the requested limit.

The CLI exposes this as:

```bash
mindbrain-standalone-tool pack \
  --db data/mindbrain.sqlite \
  --user-id user_test \
  --query offline \
  --scope player:player_123 \
  --limit 10
```

## Scope Matching

`packContextScoped` accepts a nullable scope. When scope is provided, the helper
matches it against row metadata/facets and common prefixed forms such as
`player:<id>`.

Use the narrowest useful scope:

| Scope | Use |
|-------|-----|
| workspace id | Shared workspace context. |
| collection id | Corpus-specific context. |
| `player:<id>` | Per-player or per-user operational memory. |
| entity / project key | Local entity or workflow context. |
| null | Only for global context that is safe across the caller boundary. |

## Pack Priority

The `projection_types` table controls pack order for canonical semantic types.
Legacy memory rows still use direct aliases:

| Legacy row type | Default meaning |
|-----------------|-----------------|
| `canonical` | Cleaned summary. |
| `proposition` | Structured DSL content. |
| `raw` | Less-processed source text. |

If `projection_types` is missing or does not contain a legacy alias, the
standalone code keeps compatibility defaults for these three types.

## Next Hops

`pragma_sqlite.nextHops` suggests nearby nodes from two sources:

- structured proposition fields such as `subject`, `object`, `from`, `to`, and
  `id`;
- explicit `memory_edges`.

Structured projections use `next_hop_multiplier` from `projection_types` when
available. Otherwise the parser-level type priority is used.

## Native Extension Status

`src/mb_pragma/main.zig` currently exposes:

| Symbol | Status |
|--------|--------|
| `pragma_parse_proposition_line` | Implemented. |
| `pragma_rank_native` / `pragma_rank_zig` | Stub, returns empty set. |
| `pragma_next_hops_native` / `pragma_next_hops_zig` | Stub, returns empty set. |

Do not assume PostgreSQL native rank and next-hop behavior matches the
standalone SQLite implementation until those stubs are replaced.
