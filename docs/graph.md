# Graph

The active implementation in this repo is **SQLite-first** and lives in [src/standalone/graph_sqlite.zig](../src/standalone/graph_sqlite.zig). The graph model still mirrors the same conceptual entity/relation tables, but the runtime surface is exposed through Zig helpers.

The graph stores a typed knowledge graph alongside facet-indexed documents. Native traversal uses **Roaring Bitmaps** of relation IDs per entity (`graph.lj_out` / `graph.lj_in`), rebuilt from `graph.relation`.

> **Where the data comes from.** Entities, aliases and relations are
> ingested through the raw layer described in
> [collections.md](./collections.md) (`entities_raw`,
> `entity_aliases_raw`, `relations_raw`, `entity_documents_raw`,
> `entity_chunks_raw`, `document_links_raw`). The tables documented
> below are the *derived* graph index built by
> `Pipeline.reindexGraph(...)` and can be rebuilt at any time from the
> raw rows.

## Core tables

| Table | Role |
|-------|------|
| **`graph.entity`** | Entities: `type`, `name`, `metadata`, timestamps; optional **`workspace_id`** for multi-tenant isolation |
| **`graph.entity_alias`** | Maps text terms → `entity_id` with confidence |
| **`graph.relation`** | Directed edges: `type`, `source_id`, `target_id`, optional validity dates and confidence |
| **`graph.entity_document`** | Links entities to facet document rows (`doc_id`, `table_oid`, `role`) |
| **`graph.entity_chunk`** | Links entities to raw collection chunks (`workspace_id`, `collection_id`, `doc_id`, `chunk_index`, `role`) |

Additional tables (execution runs, learning pipeline, etc.) exist in the same schema for higher-level workflows; inspect the SQL file for the full set.

## Bitmap adjacency

- **`graph.lj_out`** / **`graph.lj_in`** — For each `entity_id`, a **`roaringbitmap`** of incident **`graph.relation.id`** values.
- **`graph.rebuild_lj_relations()`** — Recomputes both tables from **`graph.relation`**. Run after bulk loads or large graph changes.

Compatibility views **`lj_o`** and **`lj_i`** expose the same data under names expected by the Zig BFS engine.

## Native traversal (Zig)

| Function | Returns | Notes |
|----------|---------|--------|
| **`k_hops_filtered(seed_nodes, max_hops, ...)`** | `roaringbitmap` of reachable **entity** IDs (as ints) | Uses `allowed_edges` / `filter_edges_meta` for optional filters |
| **`shortest_path_filtered(src, dest, ...)`** | `int` path length or **-1** if none | `src` / `dest` are integer node IDs matching the graph’s int view |

Both are implemented in C/Zig.

## SQLite helper surface

The standalone Zig module exposes a richer direct API for application code:

| Zig helper | Purpose |
|------------|---------|
| `getEntity(...)` / `getEntityByName(...)` | Load full entity records for UI, import, or debugging flows |
| `setEntityWorkspaceId(...)` | Reassign an entity to a workspace |
| `countEntitiesByWorkspace(...)` | Count active entities in a workspace |
| `findRelationByIds(...)` / `findRelationByEndpoints(...)` | Load a single relation by IDs or entity names |
| `getRelationsFrom(...)` / `getRelationsTo(...)` | Enumerate relations around a node |
| `cleanupTestData(...)` | Remove prefixed test fixtures from the graph tables |
| `streamEntityNeighborhood(...)` | Emit neighborhood events as JSON payloads |
| `streamSubgraph(...)` | Emit subgraph expansion events as JSON payloads |
| `marketplaceSearchToon(...)` / `entityFtsSearchToon(...)` / `skillDependenciesToon(...)` | TOON exports for the graph search helpers |

## SQL helpers

- **`graph.resolve_terms(text[], real)`** — Terms → seed bitmap via **`graph.entity_alias`**.
- **`graph.entity_docs(roaringbitmap, regclass)`** — Document rows linked to entities.

Commented examples in the SQL file (search for `Example usage`) show insert → rebuild → `k_hops_filtered` → `graph.entity_docs`.

## Workspace isolation

`graph.entity` and `graph.relation` include **`workspace_id`** (default `'default'`) aligned with **`mindbrain.workspaces`**. Filter by workspace in application queries when serving multiple tenants.

## Client examples

[examples/javascript/graph/README.md](../examples/javascript/graph/README.md) documents a browser SSE consumer for the standalone graph stream.
[examples/golang/graph/README.md](../examples/golang/graph/README.md) documents a Go client against the SQL-oriented APIs. For this repo, treat it as conceptual reference rather than an installation target.
