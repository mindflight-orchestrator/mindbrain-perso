# Diagnostics and quality

Graph diagnostics combine closed-world business rules with native graph quality
checks. The implementation is
[`src/standalone/graph_diagnostics.zig`](../../src/standalone/graph_diagnostics.zig).
These reports and gap rules are not answer artifacts; see
[../artifacts/non-artifact-gaps-and-reports.md](../artifacts/non-artifact-gaps-and-reports.md)
for the boundary.

## Open world vs closed world

Ontology and LinkML describe what may exist. Missing facts are not automatically
false in an open-world model.

Operational domains often need closed-world checks: a unit must have a cellar,
a rented unit must have a lease, or a relation must have evidence. MindBrain
stores those expectations in `graph_gap_rules`.

## Gap rules

`graph_gap_rules` defines required or bounded relation counts for an entity
type in a workspace/ontology scope.

Important fields:

| Field | Meaning |
| --- | --- |
| `rule_id` | Stable rule identity. |
| `ontology_id` | Ontology that owns the rule. |
| `workspace_id` | Optional workspace specialization. |
| `entity_type` | Entity type being checked. |
| `relation_type` | Relation type to count. |
| `direction` | `out`, `in`, or `either`. |
| `target_entity_type` | Optional endpoint type constraint. |
| `min_count` / `max_count` | Cardinality bounds. |
| `severity` | Report severity, usually `info`, `warning`, or `error`. |
| `metadata_json` | Optional filters, such as entity metadata filters. |

Import route:

```bash
curl -fsS -X POST 'http://127.0.0.1:8092/api/mindbrain/graph/gap-rules/import' \
  -H 'Content-Type: application/json' \
  -d @gap-rules.demo.json
```

Read route:

```bash
curl -fsS 'http://127.0.0.1:8092/api/mindbrain/graph/gap-rules?workspace_id=immeuble-demo'
```

## Diagnostics report

HTTP:

```bash
curl -fsS 'http://127.0.0.1:8092/api/mindbrain/graph/diagnostics?workspace_id=immeuble-demo&limit=200'
```

The JSON response has:

- `kind: "graph_diagnostics_report"`
- `summary`
- `issues[]`

Issue rows include `kind`, `severity`, `label`, `suggested_action`, and
optional `entity_id`, `relation_id`, `rule_id`, `observed_count`,
`expected_min`, and `expected_max`.

## Issue kinds

| Kind | Source | Meaning |
| --- | --- | --- |
| `missing_required_relation` | Gap rule | Count is below `min_count`. |
| `too_many_relations` | Gap rule | Count is above `max_count`. |
| `relation_type_mismatch` | Native check | Instance relation endpoints disagree with `ontology_edge_types`. |
| `isolated_entity` | Native check | Entity has no active incoming or outgoing relations. |
| `small_component` | Native check | Weakly connected component is at or below the configured threshold. |
| `entity_without_evidence` | Native check | Entity has no document or chunk grounding. |
| `relation_without_evidence` | Native check | Relation has no property with `ref_doc_id`. |
| `ontology_coverage_gap` | Coverage bridge | Ontology/taxonomy node is not represented in graph usage. |

## Coverage vs diagnostics

Coverage asks: which ontology or taxonomy nodes are not instantiated?

Diagnostics asks: which graph facts violate rules, topology expectations,
endpoint typing, or evidence expectations?

Use both. Coverage finds unused model surface. Diagnostics finds actionable
problems in current instance data.

## Remediation workflow

1. Read `summary` and choose one dominant issue family.
2. Pick one `issues[]` row with an entity id or relation id.
3. Inspect detail with `/api/mindbrain/graph/entity` or
   `/api/mindbrain/graph/relation`.
4. Traverse around the node with `/api/mindbrain/traverse`,
   `/api/mindbrain/graph-path`, or `/api/mindbrain/graph/subgraph`.
5. Decide whether the source of truth is a document, a manual fact, or a bad
   rule.
6. Write or correct raw rows.
7. Reindex the graph when raw graph rows changed.
8. Re-run diagnostics and confirm the counter moved.

## Common fixes

| Issue | First fix to consider |
| --- | --- |
| Missing required relation | Re-extract from source document or add the missing raw relation. |
| Too many relations | Review duplicate/conflicting relations and deprecate or merge. |
| Relation type mismatch | Align instance `relation_type` with ontology edge type, or update ontology edge definition if the rule is too narrow. |
| Entity without evidence | Link entity to document/chunk evidence. |
| Relation without evidence | Add a `doc_ref` relation property or re-run extraction with provenance. |
| Isolated entity | Add missing bridge relation or mark the entity deprecated/intentional. |
| Small component | Add the missing connector edge or accept it as an isolated subdomain. |
| Ontology coverage gap | Instantiate the type from data or demote/remove unused model surface. |

## Test coverage

Standalone tests cover:

- importing gap rules;
- deleting selected rules;
- cardinality violations;
- relation type mismatches;
- isolated entities;
- small components;
- evidence gaps;
- entity metadata filters in rule metadata.

The tests are imported through `src/standalone/tests.zig`.
