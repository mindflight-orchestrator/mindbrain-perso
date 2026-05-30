# Ontology

The ontology layer defines workspace-scoped vocabulary: entity types, edge
types, namespaces, facet dimensions, controlled values, seed nodes, seed edges,
and preserved RDF triples.

In this checkout, the rich ontology implementation is primarily in the
standalone SQLite runtime:

- [src/standalone/collections_sqlite.zig](../../src/standalone/collections_sqlite.zig)
- [src/standalone/collections_io.zig](../../src/standalone/collections_io.zig)
- [src/standalone/ontology_sqlite.zig](../../src/standalone/ontology_sqlite.zig)
- [src/standalone/owl2_import.zig](../../src/standalone/owl2_import.zig)
- [src/standalone/linkml_interchange.zig](../../src/standalone/linkml_interchange.zig)
- [src/standalone/http_app.zig](../../src/standalone/http_app.zig)
- [src/standalone/tool.zig](../../src/standalone/tool.zig)

`src/mb_ontology/main.zig` is currently a small native symbol surface for JSON
to TOON conversion. It is not where the SQLite ontology tables or import/export
logic live.

## What It Can Do

| Capability | Surface | Status |
|------------|---------|--------|
| Register and attach ontologies | CLI, `collections_sqlite.ensureOntology`, `collection_ontologies` | Implemented |
| Store taxonomy namespaces, dimensions, values | `ontology_namespaces`, `ontology_dimensions`, `ontology_values` | Implemented |
| Store graph schema types | `ontology_entity_types`, `ontology_edge_types` | Implemented |
| Preserve RDF/N-Triples | `ontology_triples_raw` | Implemented |
| Store ontology seed instances | `ontology_entities_raw`, `ontology_relations_raw` | Implemented |
| Import/export N-Triples | `owl2_import.zig`, CLI `ontology-import/export` | Implemented |
| Compile/export LinkML | `linkml_interchange.zig`, CLI `ontology-compile-linkml/export-linkml` | Implemented |
| Expose taxonomy/schema HTTP APIs | `http_app.zig` | Implemented |
| Coverage and projection relevance | `ontology_sqlite.zig`, HTTP/CLI | Implemented |
| Full OWL2 reasoning | Out of scope | Not implemented |

## Documentation Map

| Document | Contents |
|----------|----------|
| [model-and-storage.md](model-and-storage.md) | Schema inventory and ownership. |
| [raw-layer.md](raw-layer.md) | Raw triples, seed rows, and source-of-truth boundaries. |
| [import-export.md](import-export.md) | CLI import/export, bundles, and materialization. |
| [linkml-and-owl2.md](linkml-and-owl2.md) | LinkML profile, OWL2/N-Triples preservation, limits. |
| [taxonomy-and-apis.md](taxonomy-and-apis.md) | HTTP taxonomy/type/list/graph routes and write routes. |
| [coverage-and-projections.md](coverage-and-projections.md) | Coverage reports and ontology-derived projections. |
| [examples-immeuble-demo.md](examples-immeuble-demo.md) | Current snapshot from `data/immeuble-demo.sqlite`. |

## Relationship To Other Layers

| Layer | Relationship |
|-------|--------------|
| Collections | Own documents/chunks and attach collections to ontologies. |
| Facets | Use ontology dimensions and values as controlled vocabularies. |
| Graph | Uses ontology entity and edge types as schema; instance rows live separately. |
| Pragma/projections | Can materialize ontology or taxonomy context for agents. |
| LinkML/OWL | Authoring/interchange inputs compiled or imported into native tables. |
