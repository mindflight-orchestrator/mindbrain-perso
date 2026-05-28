# Graph Pattern Query

Graph Pattern Query (GPQ) is a small read-only query language for MindBrain graph data. It uses one shared syntax across:

| Backend | Runtime | Storage |
|---|---|---|
| proxy+SQLite | MindBrain standalone/proxy | `graph_entity`, `graph_relation`, `graph_relation_property` |
| postgres_extension+postgres | `pg_mindbrain` | `graph.entity`, `graph.relation`, `graph.relation_property` |

The syntax is shared; each backend compiles it to the most efficient native execution path.

## What GPQ Queries

GPQ queries graph entities, directed graph relations, metadata, and typed relation properties.

It is not the same as GhostCrab projections:

| Concept | Storage | Read surface |
|---|---|---|
| GPQ graph query | graph entity/relation tables | graph-query route/function |
| Type A projection | `projections` working-memory table | `ghostcrab_pack` |
| Type B projection | `ProjectionResult` graph entities | `ghostcrab_projection_get` |

GPQ may locate Type B `ProjectionResult` graph entities, but bundle-shaped projection output is delegated to `projection_get`.

## Syntax

```text
WORKSPACE immeuble-demo
MATCH (u:unit)-[o:owns]->(p:person)
WHERE u.metadata.building_id = '1'
  AND o.prop.quote_part >= 0.5
PROJECT u.entity_id, u.name, p.entity_id, p.name, o.relation_id, o.prop.quote_part
LIMIT 20
```

Supported v1 clauses:

| Clause | Purpose |
|---|---|
| `WORKSPACE id` | Required workspace scope |
| `MATCH` | Node-only or one directed edge chain |
| `WHERE` | Predicates over node fields, relation fields, metadata, and typed relation properties |
| `PROJECT` | Output fields in stable order |
| `PROJECT BUNDLE projection_get` | Type B projection bundle preset |
| `LIMIT n` | Result cap |
| `HOPS n` / `HOPS 1..n` | Multi-hop traversal |

GPQ is Cypher-shaped for readability, but it is not Neo4j/Cypher compatibility.

## Node Patterns

Node syntax:

```text
(p:person)
(u:unit {name: 'Lot A3'})
```

Supported node paths:

| Path | Meaning |
|---|---|
| `p.entity_id` | Entity identifier |
| `p.entity_type` | Entity type |
| `p.name` | Entity name |
| `p.confidence` | Confidence score |
| `p.metadata` | Full metadata object |
| `p.metadata.<key>` | Metadata key lookup |

Example:

```text
WORKSPACE immeuble-demo
MATCH (p:person)
WHERE p.metadata.building_id = '1'
PROJECT p.entity_id, p.name, p.metadata
LIMIT 20
```

## Edge Patterns

Directed edge syntax:

```text
(u:unit)-[o:owns]->(p:person)
```

Supported relation paths:

| Path | Meaning |
|---|---|
| `o.relation_id` | Relation identifier |
| `o.relation_type` | Relation type |
| `o.confidence` | Confidence score |
| `o.valid_from` | Start of validity interval |
| `o.valid_to` | End of validity interval |
| `o.metadata` | Full relation metadata |
| `o.metadata.<key>` | Relation metadata key lookup |
| `o.prop.<key>` | Typed relation property |

Typed relation properties use the existing value columns/indexes for text, numeric, integer, timestamp/date, and document references.

Example:

```text
WORKSPACE immeuble-demo
MATCH (u:unit)-[o:owns]->(p:person)
WHERE o.prop.quote_part >= 0.5
PROJECT u.name, p.name, o.relation_id, o.prop.quote_part
LIMIT 20
```

See [graph-pattern-query-examples.md](graph-pattern-query-examples.md) for the
tested example catalogue. The real `immeuble-demo` fixture uses `quote_part`,
`right_type`, `monthly_rent`, `amount`, and `payment_status` as relation
properties.

## HOPS

Use `HOPS` for real traversal, not for one-edge patterns.

```text
WORKSPACE immeuble-demo
MATCH (b:building {name: 'Les Tilleuls'})-[r:contains]->(x)
HOPS 1..3
WHERE r.relation_type IN ('contains', 'owns')
PROJECT x.entity_id, x.entity_type, x.name
LIMIT 50
```

Backends execute this differently:

- SQLite uses the standalone in-memory traversal runtime.
- PostgreSQL delegates to existing graph traversal functions such as `graph.k_hops_filtered` or `graph.bfs_hop` where possible.

## ProjectionResult Preset

Type B materialized projections can be selected with normal graph syntax:

```text
WORKSPACE seo-audit
MATCH (pr:ProjectionResult)
WHERE pr.metadata.projection_id = 'proj_keyword_opportunities'
PROJECT BUNDLE projection_get
LIMIT 1
```

`PROJECT BUNDLE projection_get` returns the projection-get bundle shape instead of a flat row list. It is only for Type B materialized graph projections, not Type A working-memory projections.

## Response Shape

Flat query response:

```json
{
  "workspace_id": "immeuble-demo",
  "backend": "sqlite",
  "limit": 20,
  "returned": 1,
  "columns": ["u.name", "p.name", "o.prop.quote_part"],
  "rows": [
    {
      "u.name": "Lot A3",
      "p.name": "Nicolas Dupont",
      "o.prop.quote_part": 0.5
    }
  ]
}
```

Debug responses may include a backend-specific plan summary:

```json
{
  "plan": {
    "strategy": "sql_one_edge_join",
    "uses_traversal_runtime": false
  }
}
```

## Backend Efficiency Rules

Both implementations must use existing indexes first:

- Node-only patterns compile to indexed entity-table queries.
- One-edge patterns compile to SQL joins.
- Relation-property predicates use typed property indexes.
- Full traversal runtimes are reserved for `HOPS`.
- Projection hydration is batched, not one query per returned row.

If a metadata key becomes a hot filter, add a deliberate expression/generated index later. Do not add broad indexes for every possible metadata path in v1.
