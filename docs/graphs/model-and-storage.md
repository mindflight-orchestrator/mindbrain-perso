# Graph model and storage

MindBrain separates the ontology/model graph from the instance/data graph.

| Graph | Tables | Meaning |
| --- | --- | --- |
| Ontology graph | `ontology_entity_types`, `ontology_edge_types`, `ontology_dimensions`, `ontology_values`, `ontology_triples_raw`, optional ontology seed rows | What may exist, how types are defined, and how vocabularies map to standards. |
| Instance graph | `entities_raw`, `relations_raw`, derived `graph_entity`, `graph_relation`, `graph_relation_property` | What was observed or imported for a workspace. |

The UI rule from the graphing methodology still applies: ontology explains
meaning, the graph shows facts, and the inspector links instances to types,
rules, and evidence.

## Derived serving tables

The current SQLite table names are flat, not schema-qualified.

| Table | Purpose |
| --- | --- |
| `graph_entity` | Workspace-scoped entity nodes with `entity_type`, `name`, confidence, metadata, deprecation time, and creation time. |
| `graph_entity_alias` | Search/resolve aliases keyed by `(term, entity_id)`. |
| `graph_relation` | Directed edges with `relation_type`, `source_id`, `target_id`, validity interval, confidence, run/patch ids, metadata, and timestamps. |
| `graph_relation_property` | Typed edge attributes such as text, numbers, money minor units, dates, document references, and URIs. |
| `graph_entity_document` | Derived entity-to-facet-document grounding. |
| `graph_entity_chunk` | Derived entity-to-raw-chunk grounding by workspace, collection, doc id, and chunk index. |
| `graph_lj_out` / `graph_lj_in` | Roaring bitmap adjacency caches keyed by entity id. |
| `graph_entity_degree` | Cached in/out/total degree statistics used by discovery and analytics helpers. |
| `graph_execution_run` / `graph_knowledge_patch` | Learning and patch bookkeeping. |
| `graph_gap_rules` | Closed-world quality rules used by diagnostics. |

`graph_entity` has `UNIQUE(workspace_id, entity_type, name)`. The migration in
`sql/migrations/2026-05-23-graph-entity-workspace-unique.sql` rebuilds legacy
databases that still used global `UNIQUE(entity_type, name)`.

## Workspace isolation

Graph queries that serve more than one tenant must scope by `workspace_id`.
The derived tables default to `workspace_id = 'default'`, but application paths
should pass the real workspace id explicitly.

Important indexes in the canonical schema:

| Index | Access path |
| --- | --- |
| `graph_entity_name_idx` | Anchor lookup by name. |
| `graph_entity_workspace_type_name_idx` | Workspace/type/name lookups and GPQ node patterns. |
| `graph_entity_workspace_id_idx` | Workspace scans and counts. |
| `graph_relation_workspace_id_idx` | Workspace relation scans. |
| `graph_relation_source_id_idx` | Outbound neighbor expansion. |
| `graph_relation_target_id_idx` | Inbound neighbor expansion. |
| `grp_key_text_idx`, `grp_key_int_idx`, `grp_key_num_idx`, `grp_doc_ref_idx` | Typed relation-property predicates and evidence lookup. |

These indexes are part of the design. Prior graph storage audits showed that
the traversal hotspot was missing access paths, not the basic table model.

## Relation properties

`graph_relation_property` mirrors `relation_properties_raw` into the serving
graph. Supported `value_type` values are:

- `text`
- `number`
- `percentage_bp`
- `money_minor`
- `date_unix`
- `doc_ref`
- `uri`

Numeric GPQ predicates use `value_number` or `value_integer`. Text predicates
use `value_text`. Document evidence checks look at `ref_doc_id`.

## Adjacency and traversal runtime

The serving graph has both relational edge rows and Roaring bitmap adjacency:

- `graph_lj_out(entity_id, relation_ids_blob)`
- `graph_lj_in(entity_id, relation_ids_blob)`

`graph_sqlite.rebuildLjRelations` rebuilds both caches from active relations.
`graph_sqlite.rebuildLjForEntities` updates a narrower set after targeted graph
patches. Bulk imports can rebuild the full graph; incremental learning paths
track touched entities and refresh only those rows.

For simple node and one-edge queries, use indexed SQL joins. Use the traversal
runtime for actual multi-hop expansion and shortest path work.

## Compatibility notes

The sibling PostgreSQL extension uses schema-qualified names such as
`graph.entity`, `graph.relation`, and `graph.relation_property`. This SQLite
repo keeps the same conceptual model but uses flat table names such as
`graph_entity`, `graph_relation`, and `graph_relation_property`.

`src/mb_graph` and the sibling `pg_mindbrain/src/mb_graph` are not assumed to be
identical. Shared syntax, especially for GPQ, does not imply shared executors.
