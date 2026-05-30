# Proposition DSL

Structured memory projections can use a compact pipe-delimited DSL. The parser
is implemented twice:

- [src/mb_pragma/dsl_parser.zig](../../src/mb_pragma/dsl_parser.zig) for the
  native PostgreSQL symbol;
- [src/standalone/pragma_dsl.zig](../../src/standalone/pragma_dsl.zig) for the
  SQLite standalone runtime.

The canonical rules live in [docs/dsl-rules.md](../dsl-rules.md).

## Shape

Each proposition is one line:

```text
fact|id=f42|subject=offline_sync|predicate=blocked_by|object=ios_background_limits|conf=0.91
```

Common record types:

| Type | Typical keys |
|------|--------------|
| `fact` | `subject`, `predicate`, `object`, `conf` |
| `constraint` | `scope`, `rule` |
| `step` | `process`, `order`, `action` |
| `goal` | `subject`, `target`, `conf` |

Unknown keys are preserved by the parser as key/value pairs. This makes the DSL
usable for structured retrieval without requiring a rigid schema for every
application.

## Native SQL Parser

The native symbol parses one line and returns JSON text. SQL wrappers may cast
that text to JSON/JSONB.

```sql
SELECT pragma_parse_proposition_line(
  'fact|subject=offline_sync|predicate=blocked_by|object=ios_background_limits|conf=0.91'
);
```

Unparseable lines return JSON `null` text in the native implementation. A SQL
NULL input returns SQL NULL.

## Standalone Parser

`pragma_dsl.parseLine` parses one line. `parseFirstRecord` scans a larger
content string and returns the first parseable proposition record. The
standalone pack and next-hop helpers use this to interpret `proposition` rows.

## Type Priority

`typePrior` gives structured rows a lightweight ranking signal:

| Type | Purpose |
|------|---------|
| `fact` | Strong reusable context. |
| `goal` | Planning context. |
| `constraint` | Guardrail or blocker context. |
| `step` | Process/action context. |

`projection_types` can override broader ranking and pack behavior at the
semantic projection-type level.
