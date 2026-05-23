# LinkML ↔ GhostCrab interchange

Defines bidirectional conversion between LinkML YAML (authoring) and GhostCrab native ontology storage (runtime). RDF N-Triples in `ontology_triples_raw` is the canonical preservation pivot for standards fidelity.

## What LinkML is

[LinkML](https://linkml.io/) is a YAML-first schema language for linked data.

| LinkML | Semantic web |
|--------|----------------|
| `classes:` | OWL / RDFS classes |
| `slots:` | Object and datatype properties |
| `enums:` | SKOS concept schemes |
| Cardinality, `pattern`, `range` | SHACL (via `gen-shacl`) |
| `class_uri`, `slot_uri`, `exact_mappings` | Alignment to PROV-O, OWL-Time, external vocabs |

Generators used by GhostCrab compile:

- `gen-rdf` → Turtle / JSON-LD
- `gen-owl` → OWL Turtle
- `gen-shacl` → SHACL shapes
- `linkml-convert` → instance JSON/YAML ↔ RDF

Reverse (OWL → LinkML): Schema Automator `schemauto import-owl` — lossy on complex axioms.

## GhostCrab native model

Native interchange is the SQLite / `ghostcrab_backup_bundle` ontology blocks defined in [`src/standalone/collections_io.zig`](../../src/standalone/collections_io.zig).

| Native block | Role |
|--------------|------|
| `ontology_entity_types` | Node / class types |
| `ontology_edge_types` | Directed edge types + optional domain/range |
| `ontology_dimensions` + `ontology_values` | Hierarchical facet vocabulary |
| `ontology_triples_raw` | Full RDF preservation |
| `ontology_entities_raw` / `ontology_relations_raw` | Seed ontology individuals |
| `relation_properties_raw` | Qualified relation attributes on instances |

Ontology **loadouts** in GhostCrab are agent modeling recipes, not part of core interchange unless merged explicitly.

## Mapping: LinkML → GhostCrab

| LinkML | GhostCrab native |
|--------|------------------|
| `classes.<name>` | `ontology_entity_types.entity_type = <name>` |
| `class_uri` | `metadata_json.class_uri` |
| `is_a` / mixins | `metadata_json.parents[]` + triples via gen-rdf |
| `slots.<name>` (range = class) | `ontology_edge_types.edge_type = <name>` |
| slot `domain` / `range` | `source_entity_type` / `target_entity_type` |
| `slot_uri` | `metadata_json.slot_uri` |
| `enums.<name>` | `ontology_dimensions` + `ontology_values` |
| `annotations.ghostcrab.pattern` | `metadata_json.ghostcrab.pattern` |
| `description`, `comments` | `metadata_json` + SKOS triples |
| Generated RDF | `ontology_triples_raw` via `ontology-import` |

## Mapping: GhostCrab → LinkML

| GhostCrab native | LinkML |
|------------------|--------|
| `entity_type` | `classes.<entity_type>` |
| `metadata_json.class_uri` | `class_uri:` |
| `edge_type` + domain/range | `slots.<edge_type>` with `domain` / `range` |
| `ontology_values` tree | `enums.<dimension>` |
| Triples (`rdfs:subClassOf`, `skos:definition`, …) | class/slot annotations |
| Unprojected triples | preserved in `ontology_triples_raw`; schema notes `ghostcrab.preserved_triples_ref` |

## Round-trip contract

| Path | Guarantee |
|------|-----------|
| LinkML → N-Triples → `ontology_triples_raw` | Lossless for generated RDF |
| Native typed tables → LinkML → native tables | Lossless for **interchange profile** subset |
| LinkML → OWL → LinkML | Lossy |
| Full OWL 2 DL reasoning | Out of scope (mindbrain OWL MVP) |

**Interchange profile** = classes, slots, enums, cardinality, URI mappings, GhostCrab pattern annotations. Everything else remains in `ontology_triples_raw`.

## CLI

The canonical runtime is native Zig in this repository:

```bash
# Compile LinkML → native bundle + optional SQLite import
mindbrain-standalone-tool ontology-compile-linkml \
  --input ontologies/immeuble-demo/core.yaml \
  --workspace-id immeuble-demo \
  --ontology-id immeuble-demo::core \
  --output /tmp/immeuble-demo/ontology-compiled.json

# Export native ontology → LinkML
mindbrain-standalone-tool ontology-export-linkml \
  --input-bundle /tmp/immeuble-demo/ontology-compiled.json \
  --ontology-id immeuble-demo::core \
  --output ontologies/immeuble-demo/exported.yaml
```

With SQLite:

```bash
mindbrain-standalone-tool ontology-export-linkml \
  --db data/mindbrain.sqlite \
  --ontology-id immeuble-demo::core \
  --output ontologies/immeuble-demo/exported.yaml
```

GhostCrab downstream wrappers (`gcp brain ontology compile`, `gcp brain ontology export-linkml`) spawn these native commands; they do not define the interchange contract.

## Pipeline diagram

```
LinkML YAML
  ├─ gen-rdf / gen-owl / gen-shacl
  ├─ ontology-import (N-Triples)
  └─ project → ontology_entity_types, ontology_edge_types, ontology_dimensions/values
       ↓
GhostCrab native (bundle or SQLite)
       ↓
mindbrain-standalone-tool ontology-export-linkml → LinkML YAML (+ preserved_triples_ref when needed)
```

## Existing tools (not replaced)

| Tool | Use |
|------|-----|
| LinkML | Authoring + standard generators |
| W3C yml2vocab | Simple RDFS vocabs only |
| `mindbrain-standalone-tool ontology-import/export` | N-Triples ↔ SQLite |
| `mindbrain-standalone-tool backup-load` | Full backup bundle |
| `gcp brain ontology compile/export-linkml` | GhostCrab CLI wrappers over native commands |
| Legacy TS scripts under `ghostcrab-personal-mcp/scripts/ontology/` | Compatibility tests only; not the source of truth |

## Dependencies

- Python: `linkml`, `linkml-runtime` (optional `schema-automator` for OWL recovery)
- Native: `mindbrain-standalone-tool ontology-import`, `ontology-compile-linkml`, `ontology-export-linkml`
