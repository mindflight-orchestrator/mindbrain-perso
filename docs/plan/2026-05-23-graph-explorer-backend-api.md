# Graph Explorer Backend API Plan

Date: 2026-05-23
Scope: `mindbrain` standalone/SQLite HTTP API.

## Summary

This plan adds the read-only backend surface needed by the dual-view graph
explorer in `../sqlite-sigma-graphology`.

The viewer must be able to show two linked layers:

- the ontology model: entity types, edge types, triples, rules, labels, and
  definitions;
- the instance graph: `graph_entity`, `graph_relation`, relation properties,
  facets, provenance, and projection links.

The backend remains the source of truth. The browser-facing viewer must not call
`/api/mindbrain/sql`; it should consume narrow JSON endpoints.

## Current State

- Existing HTTP routes already cover health, workspace export, graph search,
  BM25/facet search, projections, and graph subgraph.
- `GET /api/mindbrain/graph/subgraph` currently accepts seeds, hops, edge types,
  and `format=json`, but it does not expose a workspace filter.
- The ontology import layer stores model data in `ontologies`,
  `workspace_settings`, `ontology_entity_types`, `ontology_edge_types`, and
  `ontology_triples_raw`.
- `ontology_entities_raw` and `ontology_relations_raw` may contain imported
  ontology individuals or seed graph rows. They are useful context, but they are
  not the primary schema graph.
- Instance details are available in `graph_entity`, `graph_relation`,
  `graph_relation_property`, facets/search tables, and graph evidence tables,
  but there are no narrow detail endpoints for the viewer.

## Required API Additions

### `GET /api/mindbrain/ontology/list?workspace_id=`

Return ontology choices for a workspace.

Response shape:

```json
{
  "workspace_id": "immeuble-demo",
  "default_ontology_id": "immeuble-demo::core",
  "ontologies": [
    {
      "ontology_id": "immeuble-demo::core",
      "name": "core",
      "version": "1.0.0",
      "source_kind": "constructed",
      "frozen": false,
      "metadata": {}
    }
  ]
}
```

Implementation reads `workspace_settings.default_ontology_id` and `ontologies`.
If `workspace_id` is omitted, return all ontologies plus nullable defaults.

### `GET /api/mindbrain/ontology/graph?workspace_id=&ontology_id=`

Return the schema graph for the **Modèle** tab.

Primary nodes come from `ontology_entity_types`. Primary edges come from
`ontology_edge_types`. Preserve `ontology_entities_raw` and
`ontology_relations_raw` as optional `seed_nodes` / `seed_edges`, not as the
main model layer.

Response shape:

```json
{
  "ontology_id": "immeuble-demo::core",
  "nodes": [
    {
      "id": "entity_type:person",
      "kind": "entity_type",
      "type": "person",
      "label": "Personne",
      "metadata": {}
    }
  ],
  "edges": [
    {
      "id": "edge_type:owns",
      "kind": "edge_type",
      "type": "owns",
      "source_type": "person",
      "target_type": "unit",
      "directed": true,
      "metadata": {}
    }
  ],
  "seed_nodes": [],
  "seed_edges": []
}
```

When an edge type has no `source_entity_type` or `target_entity_type`, still
return it with null endpoints so the viewer can list it in the inspector.

### `GET /api/mindbrain/ontology/type?ontology_id=&kind=entity|edge&type=`

Return the definition panel for a model node or edge.

For `kind=entity`, read `ontology_entity_types`. For `kind=edge`, read
`ontology_edge_types`. Include matching triples from `ontology_triples_raw`
where the subject or predicate identifies the type. Include labels,
definitions, domain/range, controlled vocabulary metadata, and raw triples.

Response shape:

```json
{
  "ontology_id": "immeuble-demo::core",
  "kind": "edge",
  "type": "owns",
  "label": "possede",
  "source_type": "person",
  "target_type": "unit",
  "metadata": {},
  "triples": []
}
```

Do not implement OWL/SHACL reasoning in v1. The endpoint only exposes stored
metadata and preserved triples.

### `GET /api/mindbrain/graph/entity?workspace_id=&entity_id=`

Return one instance node for the inspector.

Include:

- `graph_entity` row with parsed `metadata_json`;
- best matching facet rows for the same workspace and record/source reference;
- incident relations, workspace scoped;
- optional evidence links from graph document/chunk tables when present.

The endpoint must require or infer `workspace_id` before fetching incident
relations. If `workspace_id` is omitted, read the entity first and use its
stored `workspace_id`.

### `GET /api/mindbrain/graph/relation?workspace_id=&relation_id=`

Return one graph relation for the inspector.

Include:

- `graph_relation` row with parsed `metadata_json`;
- source and target compact entity records;
- typed properties from `graph_relation_property`;
- temporal fields `valid_from_unix` / `valid_to_unix`;
- provenance fields such as `ref_doc_id` properties when present.

The relation must be workspace checked. If `workspace_id` is provided and does
not match the relation row, return 404.

### Extend `GET /api/mindbrain/graph/subgraph`

Add `workspace_id` as an optional query parameter:

```text
GET /api/mindbrain/graph/subgraph?workspace_id=...&seed_ids=1,2&hops=2&edge_types=requires&format=json
```

The workspace filter must apply to seed validation and relation expansion. This
is required before the viewer can safely use subgraph focus in a multi-workspace
database.

## Implemented API Reference

### Ontology model endpoints

The model view should use:

```text
GET /api/mindbrain/ontology/list?workspace_id=<workspace_id>
GET /api/mindbrain/ontology/graph?workspace_id=<workspace_id>
GET /api/mindbrain/ontology/graph?ontology_id=<ontology_id>
GET /api/mindbrain/ontology/type?ontology_id=<ontology_id>&kind=entity&type=<entity_type>
GET /api/mindbrain/ontology/type?ontology_id=<ontology_id>&kind=edge&type=<edge_type>
```

`ontology/list` returns the workspace default ontology and the ontology choices.
When `workspace_id` is omitted it returns all ontologies and
`default_ontology_id: null`.

`ontology/graph` resolves `ontology_id` directly when provided, otherwise it
uses `workspace_settings.default_ontology_id` for `workspace_id`. Its `nodes`
array is built from `ontology_entity_types`, its `edges` array is built from
`ontology_edge_types`, and imported raw ontology rows are exposed separately as
`seed_nodes` and `seed_edges`.

`ontology/type` returns one schema element for the inspector. For entity kinds it
reads `ontology_entity_types`; for edge kinds it reads `ontology_edge_types`.
It also returns preserved rows from `ontology_triples_raw` where the stored
subject or predicate matches, or contains, the requested type string. It does not
run inference.

### Instance detail endpoints

The instance inspector should use:

```text
GET /api/mindbrain/graph/entity?workspace_id=<workspace_id>&entity_id=<node_id>
GET /api/mindbrain/graph/relation?workspace_id=<workspace_id>&relation_id=<relation_id>
```

`graph/entity` can infer the workspace from the `graph_entity` row when
`workspace_id` is omitted. If a `workspace_id` is provided and does not match the
row, the endpoint returns 404. The response contains:

- `entity`: the selected `graph_entity` row with parsed metadata;
- `facets`: up to ten matching `facets` rows in the same workspace;
- `incident_relations`: direct incoming/outgoing `graph_relation` rows scoped to
  the entity workspace;
- `evidence_links`: `graph_entity_chunk` provenance links when available.

`graph/relation` returns a single workspace-checked relation. The response
contains the selected `graph_relation`, compact `source` and `target` nodes from
the same workspace, temporal fields, metadata, and typed
`graph_relation_property` rows. Properties with `ref_doc_id` expose document
provenance for the relation.

### Node-centered subgraph endpoint

The graph canvas should use `graph/subgraph` when it needs a node and all related
edges/nodes around it:

```text
GET /api/mindbrain/graph/subgraph?workspace_id=<workspace_id>&seed_ids=<node_id>&hops=1&format=json
```

`seed_ids` is the node id input. A single node id is enough:

```text
GET /api/mindbrain/graph/subgraph?workspace_id=immeuble-demo&seed_ids=123&hops=1&format=json
```

Use `hops=1` for direct neighbors and incident edges. Increase `hops` to expand
to neighbors of neighbors. `edge_types` can restrict traversal to one or more
comma-separated relation types:

```text
GET /api/mindbrain/graph/subgraph?workspace_id=immeuble-demo&seed_ids=123&hops=2&edge_types=owns,requires&format=json
```

With `workspace_id`, seed validation and every expansion step are workspace
scoped. Seeds outside the requested workspace are ignored, and relations or
neighbor nodes from other workspaces are not emitted.

The response is a JSON array of graph stream events when `format=json` is set.
Each item has `seq`, `kind`, and a JSON `payload`. The relevant event kinds are:

- `seed_node`: the starting node;
- `edge`: an emitted relation plus compact source and target nodes;
- `node`: a newly discovered neighbor node;
- `done`: traversal summary with seed, hop, node, and edge counts.

Without `format=json`, the same events are returned as server-sent-event frames.

## Implementation Touchpoints

- `src/standalone/http_app.zig`: route dispatch and JSON response handlers.
- `src/standalone/collections_sqlite.zig` or a small ontology read helper:
  ontology list, graph, and type lookups.
- `src/standalone/graph_sqlite.zig`: entity detail, relation detail, relation
  property batch loading, and workspace-scoped subgraph expansion.
- `sql/sqlite_mindbrain--1.0.0.sql`: no required schema change for v1.

Reuse existing JSON encoding conventions and allocator ownership patterns from
the current HTTP handlers.

## Validation

Run the standalone Zig tests:

```sh
ZIG_LOCAL_CACHE_DIR=/tmp/zig-cache \
ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache \
zig build test-standalone
```

Add tests that verify:

- ontology list returns the default ontology for a workspace;
- ontology graph is built from entity/edge type tables;
- ontology type returns metadata plus matching raw triples;
- graph entity detail includes facets and incident relations;
- graph relation detail includes typed `graph_relation_property` rows;
- subgraph expansion does not leak entities or relations across workspaces.

## Out Of Scope

- Mutating ontology or graph data from the viewer.
- Full OWL2, SHACL, or inference support.
- Browser access to SQL endpoints.
- Frontend components, SvelteKit proxy, or Sigma rendering.
