# Queries and APIs

MindBrain exposes graph data through direct Zig helpers, HTTP routes, CLI
commands, and Graph Pattern Query.

## Graph Pattern Query

GPQ is a small read-only graph query language documented in
[`docs/queries/graph-pattern-query.md`](../queries/graph-pattern-query.md).
The implementation is in
[`src/standalone/graph_pattern.zig`](../../src/standalone/graph_pattern.zig).

The syntax is shared across backends:

| Backend | Execution |
| --- | --- |
| SQLite/proxy | Parse, validate, and execute through SQLite SQL or traversal helpers. |
| PostgreSQL extension | Parse in MindBrain, serialize JSON AST, execute `graph.pattern_query_ast(jsonb, jsonb)`. |

PostgreSQL does not own the GPQ text parser in v1.

### Supported clauses

| Clause | Notes |
| --- | --- |
| `WORKSPACE id` | Required. |
| `MATCH` | Node-only or one directed edge pattern. |
| `WHERE` | Node fields, relation fields, metadata keys, typed relation properties. |
| `PROJECT` | Stable list of output fields. |
| `PROJECT BUNDLE projection_get` | Type B `ProjectionResult` bundle preset. |
| `LIMIT n` | Default 100, maximum 1000. |
| `HOPS n` / `HOPS 1..n` | Multi-hop traversal style queries. |

### Supported paths

Node paths:

- `<var>.entity_id`
- `<var>.entity_type`
- `<var>.name`
- `<var>.confidence`
- `<var>.metadata`
- `<var>.metadata.<key>`

Relation paths:

- `<rel>.relation_id`
- `<rel>.relation_type`
- `<rel>.confidence`
- `<rel>.metadata`
- `<rel>.metadata.<key>`
- `<rel>.prop.<key>`

Supported predicate operators are `=`, `>`, `>=`, `<`, `<=`, and `IN` for the
supported list cases. Identifiers and dynamic metadata/property path fragments
are validated before SQL is built.

### Examples

Node query:

```text
WORKSPACE immeuble-demo
MATCH (u:unit)
WHERE u.metadata.building_id = 1
PROJECT u.entity_id, u.name, u.metadata.lot, u.metadata.usage_status
LIMIT 20
```

One-edge relation-property query:

```text
WORKSPACE immeuble-demo
MATCH (p:person)-[o:owns]->(u:unit)
WHERE o.prop.quote_part >= 0.5
PROJECT p.name, u.name, o.relation_id, o.prop.quote_part
LIMIT 20
```

Traversal query:

```text
WORKSPACE immeuble-demo
MATCH (b:building {name: 'Residence Les Tilleuls'})-[r:contains]->(x:unit)
HOPS 1..2
WHERE r.relation_type IN ('contains')
PROJECT x.entity_id, x.entity_type, x.name
LIMIT 50
```

## HTTP routes

| Method | Route | Purpose |
| --- | --- | --- |
| `POST` | `/api/mindbrain/graph/pattern-query` | Execute GPQ with body `{"query":"...","backend":"sqlite"|"postgres","options":{},"debug":false}`. |
| `GET` | `/api/mindbrain/graph/type-counts?workspace_id=...` | Count graph entities by type, joined to ontology labels when available. |
| `GET` | `/api/mindbrain/graph/diagnostics?workspace_id=...` | Build diagnostics report. Optional `ontology_id`, `limit`, `component_small_max`. |
| `GET` | `/api/mindbrain/graph/gap-rules?workspace_id=...` | List configured closed-world gap rules. |
| `POST` | `/api/mindbrain/graph/gap-rules/import` | Import gap rules JSON. |
| `POST` | `/api/mindbrain/graph/gap-rules/delete` | Delete selected gap rules. |
| `GET` | `/api/mindbrain/graph/entity?entity_id=...` | Entity detail, facets, incident relations, evidence links. Optional `workspace_id`. |
| `GET` | `/api/mindbrain/graph/relation?relation_id=...` | Relation detail, endpoints, typed properties. Optional `workspace_id`. |
| `GET` | `/api/mindbrain/graph-path?source=...&target=...` | TOON shortest path by entity names. Optional repeated `edge_label`, `max_depth`. |
| `GET` | `/api/mindbrain/graph/subgraph?seed_ids=1,2` | SSE subgraph events, or JSON when `format=json`. Optional `hops`, `edge_types`, `workspace_id`. |
| `GET` | `/api/mindbrain/traverse?start=...` | JSON traversal rows. Optional `workspace_id`, `direction`, `depth`, `target`, repeated `edge_label`. |

## Query strategy

Use the cheapest surface that matches the question:

| Need | Preferred path |
| --- | --- |
| Entity by id/name/type | Direct graph helpers or indexed SQL. |
| One directed edge pattern | GPQ one-edge query or direct SQL join. |
| Relation property filtering | GPQ or SQL join against `graph_relation_property`. |
| Multi-hop expansion | `graph/subgraph`, `traverse`, or `HOPS`. |
| Shortest path | `graph-path` / `shortestPathToon`. |
| Quality report | `graph/diagnostics`. |
| Closed-world rule management | Gap-rules routes. |

Node-only GPQ and one-edge GPQ deliberately stay SQL-first. The traversal
runtime is reserved for `HOPS`, subgraph expansion, shortest paths, and related
graph-walk APIs.

## SQL snippets

Find entities by type:

```sql
SELECT entity_id, entity_type, name, confidence, metadata_json
FROM graph_entity
WHERE workspace_id = 'immeuble-demo'
  AND entity_type = 'unit'
  AND deprecated_at IS NULL
ORDER BY confidence DESC, entity_id;
```

Find ownership relations with typed properties:

```sql
SELECT src.name AS owner,
       dst.name AS unit,
       r.relation_id,
       quote.value_number AS quote_part,
       right_type.value_text AS right_type
FROM graph_relation r
JOIN graph_entity src ON src.entity_id = r.source_id
JOIN graph_entity dst ON dst.entity_id = r.target_id
LEFT JOIN graph_relation_property quote
  ON quote.relation_id = r.relation_id
 AND quote.property_key = 'quote_part'
LEFT JOIN graph_relation_property right_type
  ON right_type.relation_id = r.relation_id
 AND right_type.property_key = 'right_type'
WHERE r.workspace_id = 'immeuble-demo'
  AND r.relation_type = 'owns'
  AND r.deprecated_at IS NULL;
```

Find relation evidence gaps:

```sql
SELECT r.relation_id, r.relation_type, src.name AS source, dst.name AS target
FROM graph_relation r
JOIN graph_entity src ON src.entity_id = r.source_id
JOIN graph_entity dst ON dst.entity_id = r.target_id
WHERE r.workspace_id = 'immeuble-demo'
  AND r.deprecated_at IS NULL
  AND NOT EXISTS (
    SELECT 1
    FROM graph_relation_property p
    WHERE p.relation_id = r.relation_id
      AND p.ref_doc_id IS NOT NULL
  );
```
