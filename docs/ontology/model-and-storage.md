# Ontology Model And Storage

Ontology storage is workspace-scoped and lives in the canonical SQLite schema
[sql/sqlite_mindbrain--1.0.0.sql](../../sql/sqlite_mindbrain--1.0.0.sql).

## Catalog And Attachment

| Table | Role |
|-------|------|
| `ontologies` | Ontology header: id, workspace, name, version, source kind, frozen flag, metadata. |
| `workspace_settings` | Default ontology per workspace. |
| `collection_ontologies` | Attachment between workspace collections and ontologies. |

`ontologies.workspace_id` ties an ontology to a workspace. `workspace_settings`
selects the default ontology for APIs that accept only a workspace id.

`collection_ontologies` lets one ontology span multiple collections, and lets a
collection choose a primary ontology.

## Namespaces And Taxonomy Values

| Table | Role |
|-------|------|
| `ontology_namespaces` | Namespace definitions such as `source`, `domain`, or domain-specific prefixes. |
| `ontology_dimensions` | Facet/taxonomy dimensions inside a namespace. |
| `ontology_values` | Controlled or hierarchical values for a dimension. |

Dimensions define value type, multi-value behavior, hierarchy kind, and
metadata. Values carry stable integer ids, labels, and optional parent ids.

These tables are the vocabulary backing ontology-aware facet assignment. Raw
facet decisions are still stored in `facet_assignments_raw`.

## Graph Schema Types

| Table | Role |
|-------|------|
| `ontology_entity_types` | Class/node types such as `building`, `unit`, or `person`. |
| `ontology_edge_types` | Directed edge/property types with optional source and target type constraints. |

These tables define what may exist. Instance facts live in `entities_raw`,
`relations_raw`, and the derived `graph_*` tables.

## Seed Ontology Instances

| Table | Role |
|-------|------|
| `ontology_entities_raw` | Seed nodes shipped with the ontology. |
| `ontology_relations_raw` | Seed edges shipped with the ontology. |

Seed rows are ontology-owned. They are useful for taxonomy roots, reference
entities, examples, and schema graph display. They are not automatically the
same thing as workspace instance facts.

## Preserved RDF

| Table | Role |
|-------|------|
| `ontology_triples_raw` | Lossless preservation of imported/generated RDF triples. |

Every imported N-Triples row can be preserved here, even if it cannot be
projected into `ontology_entity_types`, `ontology_edge_types`, dimensions, or
seed instance rows.

## Frozen Ontologies

HTTP write routes reject changes when `ontologies.frozen = true`. This protects
taxonomy, entity type, edge type, property, and triple writes from mutating a
locked ontology.

## Code Ownership

| Module | Responsibility |
|--------|----------------|
| `collections_sqlite.zig` | Upsert/ensure helpers for ontology catalog, taxonomy, types, seed rows, and triples. |
| `collections_io.zig` | Bundle import/export and taxonomy selectors. |
| `ontology_sqlite.zig` | Coverage, taxonomy projections, projection relevance, workspace resolution. |
| `owl2_import.zig` | N-Triples parser/importer/exporter. |
| `linkml_interchange.zig` | LinkML compile/export and native bundle import. |
| `http_app.zig` | HTTP taxonomy/schema/coverage routes. |
| `tool.zig` | CLI commands. |
