# Ontology Import And Export

MindBrain supports a first standalone ontology import/export path for
normalized RDF/N-Triples and native JSON bundles.

## Register And Attach

Register an ontology in a workspace:

```bash
mindbrain-standalone-tool ontology-register \
  --db data/mindbrain.sqlite \
  --workspace-id immeuble-demo \
  --ontology-id immeuble-demo::core \
  --name core \
  --version 2.1.0 \
  --source-kind linkml
```

Attach it to a collection:

```bash
mindbrain-standalone-tool ontology-attach \
  --db data/mindbrain.sqlite \
  --workspace-id immeuble-demo \
  --collection-id immeuble-demo::docs \
  --ontology-id immeuble-demo::core
```

## Import N-Triples

```bash
mindbrain-standalone-tool ontology-import \
  --db data/mindbrain.sqlite \
  --workspace-id immeuble-demo \
  --ontology-id immeuble-demo::core \
  --input ontology.nt \
  --name core \
  --materialize-graph
```

Import behavior:

- every parsed triple is preserved in `ontology_triples_raw`;
- simple OWL/RDFS class declarations project into `ontology_entity_types`;
- object properties project into `ontology_edge_types`;
- selected datatype/property shapes can project into dimensions or metadata;
- with `--materialize-graph`, object triples can also become raw graph rows.

Full OWL2 DL reasoning is not implemented. RDF/XML, Turtle, Manchester Syntax,
and OWL Functional Syntax should be normalized upstream before this importer.

## Export

Export preserved triples:

```bash
mindbrain-standalone-tool ontology-export \
  --db data/mindbrain.sqlite \
  --ontology-id immeuble-demo::core \
  --format ntriples \
  --output ontology.nt
```

Export a native bundle:

```bash
mindbrain-standalone-tool ontology-export \
  --db data/mindbrain.sqlite \
  --workspace-id immeuble-demo \
  --ontology-id immeuble-demo::core \
  --format bundle \
  --output ontology-bundle.json
```

## Bundle Import / Export

`collections_io.zig` handles portable JSON bundles. Bundles can include
workspace, collection, ontology, document, facet, graph, and raw ontology body
blocks.

Use `collection-export`, `collection-import`, or `backup-load` when the goal is
to move a complete corpus, not only a vocabulary.

## Reindexing After Import

Ontology import changes definitions and optionally raw graph rows. Derived
indexes are separate:

| Command | Purpose |
|---------|---------|
| `backup-load --reindex graph` | Rebuild graph-derived structures. |
| `backup-load --reindex all` | Rebuild graph, facets, and BM25 for the loaded bundle. |
| HTTP `POST /api/mindbrain/reindex/graph` | Rebuild graph for a workspace. |
| HTTP `POST /api/mindbrain/reindex/all` | Rebuild graph/facet/BM25 for a collection/table. |

## Test Fixtures

Small OWL2/RDF fixtures live under [docs/source](../source/). They are used to
verify preservation and projection behavior without relying on network fetches.
