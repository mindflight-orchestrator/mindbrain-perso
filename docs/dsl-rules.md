# Proposition DSL rules

The **Proposition DSL** is a compact, line-oriented text format for storing structured memory in `memory_projections` rows with `projection_type = 'proposition'`. It is designed for:

- BM25-friendly lexical search
- Easy parsing in Zig, Go, Rust, Python
- Model portability across LLM vendors
- Natural bridge into `memory_edges` for graph expansion

## Format

Each line is a single proposition record:

```
<record_type>|<key>=<value>|<key>=<value>|...
```

- **Record type**: One word before the first `|` (e.g. `fact`, `constraint`, `step`, `goal`)
- **Key-value pairs**: Separated by `|`, each pair is `key=value`
- **Encoding**: UTF-8 plain text
- **Line separator**: Newline (`\n`) between records

## Rules for creating DSL lines

### 1. Record type (required)

The first token before `|` must be a known type. Use lowercase.

| Type | Purpose | Example use |
|------|---------|-------------|
| `fact` | Atomic fact, subject-predicate-object | `fact|subject=X|predicate=Y|object=Z|conf=0.9` |
| `constraint` | Rule or limitation | `constraint|scope=memory|rule=keep_context_small` |
| `step` | Process step or action | `step|process=context_pack|order=1|action=retrieve` |
| `goal` | User or agent goal | `goal|actor=user|wants=offline_sync` |
| `edge` | Graph edge (subject→object) | `edge|from=node_a|to=node_b|type=depends_on` |

### 2. Key-value pairs

- Format: `key=value`
- Keys: lowercase, alphanumeric + underscore (e.g. `subject`, `object`, `conf`)
- Values: no `|` or newline; use `_` for spaces if needed
- Optional: use `conf` or `confidence` for 0.0–1.0 scores

### 3. Common keys by type

**fact**

- `id` — unique id for this fact
- `subject` — subject entity
- `predicate` — relation
- `object` — object entity
- `conf` — confidence 0.0–1.0

**constraint**

- `id`, `scope`, `rule`

**step**

- `id`, `process`, `order`, `action`

**goal**

- `id`, `actor`, `wants`, `status`

**edge**

- `from`, `to`, `type`, `weight`

### 4. Multi-line content

One `memory_projections.content` field can hold multiple DSL lines. Separate with newlines:

```
fact|id=f42|subject=offline_sync|predicate=blocked_by|object=ios_background_limits|conf=0.91
constraint|id=c9|scope=memory|rule=keep_context_small
step|id=s3|process=context_pack|order=1|action=retrieve_candidates
```

### 5. Parsing behavior

- Empty lines: ignored
- Malformed lines: skipped (no `|`, or invalid structure)
- Unknown record types: accepted; parser preserves type as string
- Duplicate keys: last value wins
- Missing `conf`: treated as 1.0 for scoring

## Examples

```
fact|id=f1|subject=user|predicate=wants|object=offline_sync|conf=0.95
fact|id=f2|subject=offline_sync|predicate=blocked_by|object=ios_background_limits|conf=0.91
constraint|id=c1|scope=memory|rule=keep_context_small
goal|id=g1|actor=user|wants=dynamic_memory_for_long_agents
step|id=s1|process=context_pack|order=1|action=retrieve_candidates
edge|from=evt_123|to=skill_456|type=triggers|weight=0.8
```

## Integration with the runtime

- **`pragma_parse_proposition_line(line)`** — Parses one line, returns **JSONB** (implemented in Zig; see [pragma.md](pragma.md)).
- **`pragma_rank_native`** — Reserved for native scoring over candidates; **currently a stub** returning no rows until implemented.
- **`pragma_next_hops_native`** — Reserved for next-hop suggestions from propositions and `memory_edges`; **currently a stub** returning no rows until implemented.
