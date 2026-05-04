# Pragma (memory projections)

The pragma section of this repository adds SQL functions for retrieving and ranking **memory projections** in applications that use the **`memory-server`** schema pattern (tables such as `memory_items`, `memory_projections`, `memory_edges`).

Everything below is defined in the SQL install script (search for the pragma comments).

## Assumed tables

The SQL references qualified names such as **`"memory-server".memory_projections`**. Your database must create those tables (and indexes, `tsvector` columns, etc.) according to your application migrations; the runtime supplies **functions**, not the full memory-server schema.

## Proposition DSL

Structured proposition text should follow [dsl-rules.md](dsl-rules.md). Parse a single line in SQL:

```sql
SELECT pragma_parse_proposition_line('fact|subject=a|predicate=p|object=o|conf=0.9');
```

- **`pragma_parse_proposition_line_native(text)`** returns JSON as **text** (C/Zig).
- **`pragma_parse_proposition_line(text)`** wraps it as **jsonb**.

## Candidate sets and ranking

| Function | Role |
|----------|------|
| **`pragma_candidate_bitmap(user_id, query, projection_types)`** | SQL-only prefilter using `tsvector` and `ILIKE`; returns a **`roaringbitmap`** of candidate row ordinals |
| **`pragma_rank_native(...)`** | Intended native scorer over candidates — **stub: returns empty set** in current Zig ([src/mb_pragma/main.zig](../src/mb_pragma/main.zig)) |
| **`pragma_next_hops_native(...)`** | Intended next-hop helper from propositions / edges — **stub: returns empty set** |

## Context packing

| Function | Role |
|----------|------|
| **`pragma_pack_context(user_id, query, limit_n)`** | Returns a ranked mix of `canonical`, `proposition`, and `raw` projections |
| **`pragma_pack_context_scoped(user_id, query, scope, limit_n)`** | Same with **scope** filtering (`player:...` prefix and JSON `metadata` / `facets` keys) |

## Native implementation status

| Symbol | Status |
|--------|--------|
| `pragma_parse_proposition_line` | Implemented (Zig) |
| `pragma_rank_native` | Stub |
| `pragma_next_hops_native` | Stub |

See [native-reference.md](native-reference.md) for the full wiring table.
