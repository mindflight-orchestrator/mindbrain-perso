# Graph Storage Audit

This audit reviews the current SQLite-backed graph traversal path used by `mindbrain-standalone-tool benchmark-db` and evaluates whether a different graph storage/parcours format would be materially more efficient.

## Scope

The benchmark run against `data/imdb-full.sqlite` showed:

- facet query time around sub-millisecond
- graph mutation times in the sub-millisecond to low-millisecond range
- graph traversal around 10.45 seconds on average before graph indexes were added

The question is whether the traversal cost is caused by the current storage layout, the query shape, or the lack of modern graph-specific indexing/storage.

## Findings

### 1. The current traversal path is index-sensitive

The graph traversal code resolves the start node by name:

- [`loadTraverseEntityByName()`](../src/standalone/graph_sqlite.zig#L2969) executes `SELECT entity_id FROM graph_entity WHERE name = ?1 LIMIT 1`

Then it expands neighbors by scanning `graph_relation` rows for:

- `r.source_id = ?1` in outbound traversal
- `r.target_id = ?1` in inbound traversal

See [`loadTraverseNeighbors()`](../src/standalone/graph_sqlite.zig#L2977).

That means the traversal is only fast if the relevant predicates are backed by good indexes.

### 2. The schema does not appear to index the hot traversal predicates directly

In the standalone schema:

- `graph_entity` has `UNIQUE(entity_type, name)`
- `graph_entity` has `INDEX graph_entity_workspace_id_idx ON graph_entity(workspace_id)`
- `graph_relation` has `INDEX graph_relation_workspace_id_idx ON graph_relation(workspace_id)`

See [`src/standalone/sqlite_schema.zig`](../src/standalone/sqlite_schema.zig#L230).

But the traversal uses:

- `name = ?1`
- `source_id = ?1`
- `target_id = ?1`

There is no direct index on `graph_entity(name)` and no direct index on `graph_relation(source_id)` or `graph_relation(target_id)` in the current schema excerpt.

### 3. The benchmark result does not prove the graph format is the limiting factor

The current implementation stores graph data as SQLite tables with adjacency materialized in `graph_lj_out` / `graph_lj_in`.

That is a valid storage model. The 10.45 second traversal cost is more consistent with a poor access path than with a fundamentally wrong format.

In other words:

- this is likely an indexing and query-planning problem first
- it is only secondarily a storage-format problem

### 4. Modern graph engines do better because they combine adjacency storage with optimized indexes

Two relevant design patterns show up in modern graph systems:

- **Index-free adjacency**: indexes are used to find the start node, then traversal follows direct adjacency links
- **CSR / columnar adjacency + join indices**: graph data is stored in traversal-friendly structures and optimized for analytical joins

Neo4j documents the first approach as index-free adjacency and notes that indexes are still used to find the anchor node.

Kuzu documents a columnar storage engine with CSR adjacency lists and join indices, aimed at join-heavy analytical workloads.

### 5. The graph indexes eliminated the hotspot

After adding:

- `graph_entity_name_idx`
- `graph_relation_source_id_idx`
- `graph_relation_target_id_idx`

the same benchmark changed from roughly `10.45 s` to roughly `2.21 ms` for the graph traversal path.

That is about a `4,700x` improvement.

| Metric | Before indexes | After indexes |
|---|---:|---:|
| `graph_query.mean_ns` | `10,448,566,845` | `2,214,445` |
| Approx. wall time | `10.45 s` | `2.21 ms` |
| Change | - | `~4,700x faster` |

This means the previous bottleneck was not the graph storage format itself. The main issue was the missing access path for:

- anchor lookup by name
- adjacency expansion by source or target node

## Recommendation

### Short term: optimize the current SQLite path

Before changing engines, add the missing access paths:

- index `graph_entity(name)`
- index `graph_relation(source_id)`
- index `graph_relation(target_id)`
- consider composite indexes if filters regularly combine `workspace_id`, `relation_type`, and endpoints

Also split the benchmark so that:

- anchor lookup
- neighbor expansion
- path reconstruction

are measured separately.

That will show whether the 10.45 second cost is:

- lookup-heavy
- expansion-heavy
- or reconstruction-heavy

### Medium term: benchmark a graph-native engine

If you want a more modern graph storage format for large traversals, the most relevant candidates are:

- **Kuzu** for embedded graph analytics
- **Neo4j** for native graph traversal with index-free adjacency

Both are closer to the traversal patterns you care about than a plain SQL table implementation.

## Practical conclusion

At the moment, the best reading of the benchmark is:

- facet workloads are already in good shape
- mutation workloads are acceptable for a SQLite-backed implementation
- graph traversal was the hotspot before indexing
- the first optimization target was indexing and query shape, and that fix already produced a massive win

The remaining question is now about trade-offs and feature set, not raw feasibility:

- keep the SQLite stack and continue tuning it
- or move to a graph-native engine if you want a more specialized storage/query model

If after indexing the hotspot remains large, then a graph-native engine such as Kuzu is the next realistic comparison point. Based on the current numbers, that comparison is now optional rather than urgent.

## References

- [SQLite Query Planner](https://www.sqlite.org/queryplanner.html)
- [SQLite Optimizer Overview](https://www.sqlite.org/optoverview.html)
- [Neo4j Indexes](https://neo4j.com/docs/cypher-manual/current/indexes/)
- [Neo4j Graph Platform Overview](https://neo4j.com/graphacademy/training-overview-40/02-overview40-neo4j-graph-platform/)
- [Kuzu Documentation](https://kuzudb.com/docs)
