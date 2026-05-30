# Ontology Raw Layer

Ontology raw data is the durable definition layer. Derived graph, facet, search,
and projection surfaces can be rebuilt or rematerialized from it, but only if
the raw definition rows are preserved.

## Definition Vs Instance Data

| Kind | Tables | Meaning |
|------|--------|---------|
| Ontology definition | `ontologies`, `ontology_namespaces`, `ontology_dimensions`, `ontology_values`, `ontology_entity_types`, `ontology_edge_types` | Vocabulary and schema: what may exist. |
| Preserved RDF | `ontology_triples_raw` | Source triples or generated triples that should round-trip. |
| Ontology seed rows | `ontology_entities_raw`, `ontology_relations_raw` | Nodes/edges owned by the ontology itself. |
| Workspace instance facts | `entities_raw`, `relations_raw`, `facet_assignments_raw`, document/chunk raw tables | What does exist in a workspace or corpus. |
| Derived serving indexes | `graph_entity`, `graph_relation`, `facet_postings`, `search_*` | Query acceleration and serving structures. |

Do not collapse ontology definition and example instance data into the same
concept. A LinkML schema or OWL vocabulary defines allowed classes and slots; an
immeuble example graph instantiates buildings, units, people, payments, and
relations.

## `ontology_triples_raw`

`ontology_triples_raw` is the preservation pivot for standards fidelity.

| Column | Meaning |
|--------|---------|
| `ontology_id` | Owning ontology. |
| `triple_index` | Stable row order within the ontology. |
| `subject_kind` | `iri` or `blank`. |
| `subject` | Subject value. |
| `predicate` | Predicate IRI/value. |
| `object_kind` | `iri`, `blank`, or `literal`. |
| `object_value` | Object value. |
| `object_datatype` | Optional datatype for literals. |
| `object_language` | Optional language tag. |
| `source_line` | Original or generated N-Triples line. |
| `metadata_json` | JSON metadata text. |

Complex OWL expressions, unsupported predicates, and unprojected triples should
remain here instead of being silently dropped.

## Seed Rows

`ontology_entities_raw` and `ontology_relations_raw` store ontology-owned seed
graph rows. They can be exported in ontology graph APIs and bundles. They are
not the same as document-extracted or user-imported workspace facts.

## Materialization Boundary

When `ontology-import --materialize-graph` is used, object triples can be
mirrored into `entities_raw` and `relations_raw`. That is an explicit
materialization step. Without it, preserved ontology triples and projected type
tables remain definition data only.

## Backup Guidance

For ontology portability, preserve:

- `ontologies`;
- `workspace_settings`;
- `collection_ontologies`;
- all `ontology_*` body tables;
- raw documents, chunks, facets, entities, and relations if the ontology is
  shipped with example or production instance data.

Derived graph/facet/search tables are useful for fast serving but should be
treated as rebuildable indexes.
