# OWL2 Source Fixtures

This directory holds official OWL2 reference links and small local fixtures for
standalone `ontology-import` tests.

The local `.nt` files are intentionally tiny N-Triples fixtures. They are not a
copy of the W3C specifications or test suite; they are regression inputs built
from standard OWL/RDF vocabulary terms defined by W3C. Keep larger conformance
coverage in a generated/downloaded test workspace, not in the documentation
tree.

## Official Sources

| Source | Use in MindBrain |
|--------|------------------|
| <https://www.w3.org/TR/owl-overview/> | High-level OWL2 roadmap and terminology. |
| <https://www.w3.org/TR/owl2-syntax/> | Structural model and Functional-Style Syntax reference. |
| <https://www.w3.org/TR/owl2-mapping-to-rdf/> | RDF graph mapping used to justify N-Triples as the first import target. |
| <https://www.w3.org/TR/owl2-profiles/> | OWL2 EL/QL/RL profile reference; the Postgres reasoner plan targets an OWL-RL core first. |
| <https://www.w3.org/TR/owl2-test/> | W3C conformance and test-case reference for later importer/reasoner validation. |
| <https://www.w3.org/2009/owl-test-cases> | Archived OWL2 test-case repository entrypoint. |

The same list is available in [official-owl2-sources.json](official-owl2-sources.json)
for future scripted checks.

## Local Fixtures

| Fixture | Purpose |
|---------|---------|
| [owl2-core.nt](owl2-core.nt) | Basic ontology declarations, classes, object/data properties, labels, domain/range, and one individual assertion. |
| [owl2-rl-relations.nt](owl2-rl-relations.nt) | OWL-RL-oriented graph rules: subclass, subproperty, inverse, symmetric, transitive, equivalent, sameAs, and disjoint declarations. |
| [owl2-restrictions.nt](owl2-restrictions.nt) | Blank-node restrictions and RDF lists that the first importer must preserve even before it can reason over them. |

## Expected Test Behavior

The standalone `ontology-import` tests should:

1. Load each fixture as N-Triples.
2. Preserve every triple, including blank-node structures.
3. Materialize simple vocabulary into ontology/entity/relation raw tables.
4. Leave unsupported OWL constructs as raw triples plus metadata instead of
   dropping them.
5. Re-export the bundle and compare stable counts for classes, properties,
   individuals, relations, and preserved triples.
