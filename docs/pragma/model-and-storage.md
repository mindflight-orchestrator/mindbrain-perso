# Pragma Model And Storage

Pragma uses two related storage families.

The `memory_*` tables are the legacy memory retrieval model used by
`src/standalone/pragma_sqlite.zig`. The `projections` tables are the durable
agent-facing model used by ontology coverage, GhostCrab-compatible pack routes,
and projection relevance.

## `memory_items`

`memory_items` identifies source memory records for a user.

| Column | Meaning |
|--------|---------|
| `id` | Stable memory item id. |
| `user_id` | User / agent namespace for retrieval. |
| `source_type` | Optional source family, such as event, document, or system. |
| `source_ref` | Optional pointer back to a source object. |
| `content` | Optional raw content in the SQLite schema. |
| `created_at_unix` | Creation timestamp. |

## `memory_projections`

`memory_projections` stores searchable views derived from memory items.

| Column | Meaning |
|--------|---------|
| `id` | Projection id. |
| `item_id` | Parent `memory_items.id`. |
| `user_id` | Retrieval namespace. |
| `projection_type` | Legacy type such as `canonical`, `proposition`, or `raw`. |
| `content` | Text or proposition DSL content used for ranking and packing. |
| `rank_hint` | Optional ranking hint. |
| `confidence` | Confidence multiplier. |
| `metadata_json` | JSON metadata text. |
| `facets_json` | JSON facet/scope metadata text. |
| `created_at_unix` | Creation timestamp. |

## `memory_edges`

`memory_edges` links memory nodes for next-hop suggestions.

| Column | Meaning |
|--------|---------|
| `id` | Edge id. |
| `user_id` | Retrieval namespace. |
| `node_from` | Source node key. |
| `node_to` | Target node key. |
| `edge_type` | Relation type. |
| `weight` | Edge weight. |
| `created_at_unix` | Creation timestamp. |

## Durable `projections`

The durable projection table is broader than legacy `memory_projections`.
It stores agent-ready operational context.

| Column | Meaning |
|--------|---------|
| `id` | Projection id. |
| `agent_id` | Agent or process that owns the projection. |
| `scope` | Workspace, collection, entity, player, or domain boundary. |
| `proj_type` | Semantic type such as `FACT`, `GOAL`, `STEP`, `CONSTRAINT`, or `NOTE`. |
| `content` | Agent-readable statement or compact structured content. |
| `weight` | Retrieval importance. |
| `source_ref` | Optional grounding pointer. |
| `source_type` | Optional grounding type. |
| `status` | Lifecycle state; active and blocking rows participate in pack routes. |
| `created_at_unix` | Creation timestamp. |
| `expires_at_unix` | Optional expiry timestamp. |

## `projection_types`

`projection_types` is the compatibility and scoring policy table.

| Column | Meaning |
|--------|---------|
| `type_name` | Canonical semantic type. |
| `compatibility_aliases` | JSON array of legacy aliases. |
| `rank_bias` | Ranking multiplier. |
| `pack_priority` | Lower values are packed first. |
| `next_hop_multiplier` | Multiplier for next-hop expansion. |
| `structured` | Whether rows should be interpreted as structured proposition content. |

The default seed maps:

| Type | Aliases | Pack priority |
|------|---------|---------------|
| `FACT` | `canonical`, `proposition` | 1 |
| `GOAL` | `canonical`, `proposition` | 1 |
| `CONSTRAINT` | `proposition` | 2 |
| `STEP` | `proposition` | 2 |
| `NOTE` | `raw` | 3 |

## `agent_state`

`agent_state` stores lightweight operational state by `agent_id`:
`health`, `state`, JSON metrics, and update time. It is adjacent to
projections but is not itself part of ranking.
