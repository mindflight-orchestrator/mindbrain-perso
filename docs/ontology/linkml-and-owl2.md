# LinkML And OWL2

MindBrain treats LinkML as a practical authoring format and RDF/N-Triples as
the preservation format for standards fidelity.

The detailed interchange contract is documented in
[docs/methodology/graphing/linkml-interchange.md](../methodology/graphing/linkml-interchange.md).
This page summarizes how it fits into the ontology directory.

## LinkML Compile

Compile LinkML into a native bundle:

```bash
mindbrain-standalone-tool ontology-compile-linkml \
  --input ontologies/immeuble-demo/core.yaml \
  --workspace-id immeuble-demo \
  --ontology-id immeuble-demo::core \
  --output /tmp/immeuble-demo/ontology-compiled.json \
  --ntriples /tmp/immeuble-demo/ontology.nt
```

With `--db`, the compiled bundle is imported into SQLite.

## LinkML Export

Export a native ontology back to LinkML:

```bash
mindbrain-standalone-tool ontology-export-linkml \
  --db data/immeuble-demo.sqlite \
  --ontology-id immeuble-demo::core \
  --output exported.yaml
```

Or export from a compiled bundle:

```bash
mindbrain-standalone-tool ontology-export-linkml \
  --input-bundle /tmp/immeuble-demo/ontology-compiled.json \
  --ontology-id immeuble-demo::core \
  --output exported.yaml
```

## Native Mapping

| LinkML | Native ontology table |
|--------|-----------------------|
| `classes` | `ontology_entity_types` |
| class URI / mappings | `metadata_json`, generated triples |
| `slots` with class ranges | `ontology_edge_types` |
| slot domain/range | `source_entity_type`, `target_entity_type` |
| `enums` | `ontology_dimensions`, `ontology_values` |
| generated RDF | `ontology_triples_raw` |

## Interchange Profile

The native profile is intentionally bounded:

- classes;
- slots;
- enums;
- cardinality and range metadata where represented by the compiler;
- URI mappings;
- GhostCrab/MindBrain pattern annotations;
- generated RDF preservation in `ontology_triples_raw`.

Anything outside that subset should still be preserved as raw triples when RDF
is available, but it may not round-trip as a first-class native typed table row.

## OWL2 Boundary

`owl2_import.zig` imports normalized triples. It is not a full OWL reasoner.

| Supported MVP behavior | Out of scope |
|------------------------|--------------|
| Preserve triples. | OWL2 DL reasoning. |
| Project simple classes and properties. | Remote ontology fetching. |
| Export preserved N-Triples. | RDF/XML/Turtle parser inside the binary. |
| Optional graph materialization for object triples. | Manchester/Functional Syntax parser. |

Use upstream tools to convert Turtle, RDF/XML, or richer OWL source files to
N-Triples before importing into MindBrain.
