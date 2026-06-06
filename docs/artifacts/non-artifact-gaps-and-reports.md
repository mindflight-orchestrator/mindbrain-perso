# Non-Artifact Gaps and Reports

This page records the current gap/report surfaces that intentionally live
outside `mindbrain_answer_artifacts`.

Answer artifacts are answer-facing registry rows. Gaps, validation rules,
diagnostics reports, coverage reports, answerability categories, and MECE/doc
validation findings are not answer artifacts and must not receive
`artifact_kind` fields.

## Boundary

| Meaning | Preferred code | Current MindBrain surface | Stored? |
|---------|----------------|---------------------------|---------|
| Graph data gap | `graph_data_gap` | `graph_diagnostics` issue rows | No dedicated table; computed report output. |
| Graph fact conflict | `graph_conflict` | Planned `graph_diagnostics` conflict kinds + optional `graph_knowledge_patch` proposals | Rules in `graph_conflict_rules` (planned); findings computed on read. |
| Graph validation rule | `graph_gap_rule` | `graph_gap_rules` table and gap-rules routes | Yes, in `graph_gap_rules`. |
| Ontology coverage gap | `coverage_gap` | `coverage_report.gaps` and diagnostics `ontology_coverage_gap` bridge | No dedicated table; computed report output. |
| Answerability gap | `answerability_gap` | GhostCrab gap-auditor categories | Not in this MindBrain runtime. |
| MECE/doc gap | `mece_gap` | Docs / ontology validation workflow | Not in this MindBrain runtime. |

Frozen diagnostics or coverage outputs are not stored in
`mindbrain_answer_artifacts`. If a frozen report/export registry is needed
later, create a separate report-export registry instead of widening answer
artifacts.

## Existing MindBrain Runtime Surfaces

### Graph gap rules

`graph_gap_rules` is the only persisted gap-related table in the current
runtime. It stores closed-world validation rules:

- `rule_id`, `ontology_id`, optional `workspace_id`;
- entity and relation shape: `entity_type`, `relation_type`, `direction`,
  optional `target_entity_type`;
- cardinality: `min_count`, optional `max_count`;
- reporting fields: `severity`, `label`, `enabled`, `metadata_json`.

Routes:

- `GET /api/mindbrain/graph/gap-rules`
- `POST /api/mindbrain/graph/gap-rules/import`
- `POST /api/mindbrain/graph/gap-rules/delete`

CLI:

- `mindbrain-standalone-tool graph-gap-rules-import --db <sqlite_path> --input <rules.json>`

### Graph diagnostics

Graph diagnostics are computed on read. The report shape is:

- `kind: "graph_diagnostics_report"`;
- `summary` counters;
- `issues[]` rows.

Current issue kinds are:

| Issue kind | Source | Meaning |
|------------|--------|---------|
| `missing_required_relation` | `graph_gap_rules` | Observed relation count is below `min_count`. |
| `too_many_relations` | `graph_gap_rules` | Observed relation count is above `max_count`. |
| `relation_type_mismatch` | Native check | Relation endpoints do not match `ontology_edge_types`. |
| `isolated_entity` | Native check | Entity has no active incoming or outgoing relation. |
| `small_component` | Native check | Weakly connected component is at or below the configured threshold. |
| `entity_without_evidence` | Native check | Entity has no linked document or chunk evidence. |
| `relation_without_evidence` | Native check | Relation has no `graph_relation_property.ref_doc_id`. |
| `ontology_coverage_gap` | Coverage bridge | Ontology/taxonomy node is not represented by graph usage. |

Planned conflict issue kinds (`graph_conflict`, not `graph_data_gap`):

| Issue kind | Meaning |
|------------|---------|
| `mutually_exclusive_facts` | Incompatible active targets for the same relation slot. |
| `temporal_conflict` | Overlapping or invalid validity intervals. |
| `granularity_conflict` | Coarse vs fine endpoint specificity clash. |
| `redundant_fact` | Duplicate active edges or claims. |

Spec: [../graphs/graph-conflict-taxonomy.md](../graphs/graph-conflict-taxonomy.md).

Routes:

- `GET /api/mindbrain/graph/diagnostics`

CLI:

- `mindbrain-standalone-tool graph-diagnostics --db <sqlite_path> --workspace-id <id> [--ontology-id <id>] [--limit <n>] [--component-small-max <n>] [--format json|toon]`

### Coverage

Coverage reports are computed from ontology/taxonomy facet rows, graph
entities, and projection counts. The report shape is:

- `kind: coverage_report` in TOON output;
- `summary` counters: `covered_nodes`, `total_nodes`, `graph_entities`,
  `facet_rows`, `projection_rows`, `coverage_ratio`;
- `gaps[]`: `id`, `label`, `entity_type`, `criticality`, optional
  `decayed_confidence`.

Routes:

- `GET /api/mindbrain/coverage`
- `GET /api/mindbrain/coverage-by-domain`

CLI:

- `mindbrain-standalone-tool coverage --db <sqlite_path> --workspace-id <id> [--entity-type <type> ...]`
- `mindbrain-standalone-tool coverage-by-domain --db <sqlite_path> --domain-or-workspace <id> [--entity-type <type> ...]`

## What Not To Do

- Do not backfill diagnostics, coverage, graph gap rules, answerability gaps,
  or MECE/doc gaps into `mindbrain_answer_artifacts`.
- Do not add `artifact_kind` to diagnostics, coverage, graph search, or
  gap-rules responses.
- Do not use `answer_snapshot` for frozen diagnostics or coverage outputs.
- Do not create synthetic answer artifact ids for graph validation findings.

## Related Docs

- [artifact-model.md](artifact-model.md)
- [../graphs/diagnostics-and-quality.md](../graphs/diagnostics-and-quality.md)
- [../graphs/graph-conflict-taxonomy.md](../graphs/graph-conflict-taxonomy.md)
- [../graphs/schema-pattern-frequency.md](../graphs/schema-pattern-frequency.md)
- [../graphs/memory-guided-recall.md](../graphs/memory-guided-recall.md)
- [../ontology/coverage-and-projections.md](../ontology/coverage-and-projections.md)
- [../methodology/graphing/immeuble-gap-diagnostics-demo.md](../methodology/graphing/immeuble-gap-diagnostics-demo.md)
