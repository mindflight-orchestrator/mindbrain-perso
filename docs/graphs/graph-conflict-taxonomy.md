# Graph conflict taxonomy

This page defines the target **graph conflict** vocabulary for MindBrain graph
quality control. It is inspired by MemGraphRAG conflict families but mapped onto
MindBrain's raw-first graph, evidence model, and existing gap surfaces.

Implementation belongs in the MindBrain backend (`mindbrain-perso`). GhostCrab
MCP wraps the HTTP contract after submodule bump. See companion specs:

- [graph-conflict-diagnostics-queries.md](graph-conflict-diagnostics-queries.md)
- [knowledge-patch-proposal-pipeline.md](knowledge-patch-proposal-pipeline.md)
- [schema-pattern-frequency.md](schema-pattern-frequency.md)
- [memory-guided-recall.md](memory-guided-recall.md)

## Three naming layers (do not mix)

| Layer | Code prefix | Persisted? | Meaning |
| --- | --- | --- | --- |
| Validation rule | `graph_gap_rule` | Yes (`graph_gap_rules`) | Closed-world expectation an operator declares (cardinality, required edge). |
| Data gap | `graph_data_gap` | No (diagnostics issue row) | Instance violates a rule, topology, typing, or evidence expectation. |
| Fact conflict | `graph_conflict` | No (diagnostics issue row); optional `graph_knowledge_patch` proposal | Two or more active facts cannot all be true under ontology + evidence constraints. |

Conflicts are **not** answer artifacts. Do not add `artifact_kind` to conflict
findings. See [../artifacts/non-artifact-gaps-and-reports.md](../artifacts/non-artifact-gaps-and-reports.md).

## `graph_data_gap` vs `graph_conflict`

| Question | `graph_data_gap` | `graph_conflict` |
| --- | --- | --- |
| Is something missing? | Often yes (`missing_required_relation`, evidence gaps) | No — extra incompatible facts |
| Is something duplicated? | Sometimes (`too_many_relations` as cardinality) | Yes — mutually exclusive or redundant claims |
| Is typing wrong? | `relation_type_mismatch` | Rarely — conflicts assume typed edges |
| Is provenance missing? | `entity_without_evidence`, `relation_without_evidence` | Evidence may exist on **both** sides |
| Typical fix | Add fact, link evidence, fix rule | Deprecate loser, narrow validity, merge entities |

`too_many_relations` remains a **graph_data_gap** when driven by `graph_gap_rules`
cardinality. It becomes a **graph_conflict** when two active relations share the
same semantic slot (same source, same `relation_type`, incompatible targets)
without a declared `max_count` rule.

## Conflict families (`graph_conflict_*`)

Diagnostics issue kinds use the `kind` field. Proposed values:

| Issue kind | MemGraphRAG analogue | Definition |
| --- | --- | --- |
| `mutually_exclusive_facts` | Mutually exclusive conflict | Same workspace, same source entity, same `relation_type`, multiple active targets that ontology or rule marks as exclusive. |
| `temporal_conflict` | Temporal conflict | Same relation slot, overlapping or ambiguous `valid_from_unix` / `valid_to_unix` intervals. |
| `granularity_conflict` | Granularity conflict | Same fact family at incompatible specificity (city vs country, unit vs building, fine vs coarse type). |
| `redundant_fact` | Redundancy (detection pass) | Near-duplicate relations: same endpoints and type, or duplicate evidence for one claim. |

### `mutually_exclusive_facts`

**Detect when:**

- Two+ active `graph_relation` rows share `(workspace_id, source_id, relation_type)` with different `target_id`, and
- Any of:
  - a `graph_gap_rules` row sets `max_count = 1` for that slot (upgrade from `too_many_relations` to conflict when targets disagree semantically);
  - `ontology_edge_functional` metadata marks the edge functional (future ontology flag);
  - configured `graph_conflict_rules` row (see below) declares exclusivity.

**Issue row fields (extensions):**

- `conflict_kind`: `mutually_exclusive`
- `relation_ids`: array of conflicting relation ids
- `entity_id`: source entity
- `suggested_action`: `propose_knowledge_patch` | `review_extraction`

### `temporal_conflict`

**Detect when:**

- Same `(source_id, relation_type, target_id)` or same exclusive slot has multiple rows with intervals that overlap without merger semantics, or
- One row has `valid_from_unix > valid_to_unix`, or
- Both intervals open-ended (`valid_to_unix IS NULL`) for the same exclusive slot.

Uses existing columns on `graph_relation`:

- `valid_from_unix`, `valid_to_unix`, `deprecated_at`

**Issue row fields:**

- `conflict_kind`: `temporal`
- `relation_ids`: conflicting pair or set
- `observed_count`: number of overlapping intervals
- `suggested_action`: `narrow_validity` | `deprecate_weaker_evidence`

### `granularity_conflict`

**Detect when:**

- Two active relations describe the same semantic predicate at different granularities, e.g.:
  - `(person)-[born_in]->(city)` and `(person)-[born_in]->(country)`;
  - `(unit)-[located_in]->(floor)` and `(unit)-[located_in]->(building)`.
- Detection uses `ontology_edge_types` plus optional `ontology_entity_types.parent_type` or configured `granularity_pairs` in `graph_conflict_rules`.

**Issue row fields:**

- `conflict_kind`: `granularity`
- `relation_ids`: coarse vs fine relation ids
- `label`: human-readable specificity mismatch
- `suggested_action`: `keep_finer_grain` | `normalize_to_ontology`

### `redundant_fact`

**Detect when:**

- Duplicate active edges: same `(source_id, target_id, relation_type)` from different `run_id` / extraction passes, or
- Same claim with multiple `ref_doc_id` entries but identical property payload.

Lower severity than exclusivity conflicts. Often resolved by deprecating the
lower-confidence duplicate rather than adjudicating semantics.

## Optional persisted rules: `graph_conflict_rules`

Complement `graph_gap_rules` (cardinality) with declarative conflict expectations:

```sql
CREATE TABLE IF NOT EXISTS graph_conflict_rules (
    rule_id TEXT PRIMARY KEY,
    ontology_id TEXT NOT NULL,
    workspace_id TEXT,
    conflict_kind TEXT NOT NULL CHECK(conflict_kind IN (
        'mutually_exclusive', 'temporal', 'granularity', 'redundant'
    )),
    entity_type TEXT,
    relation_type TEXT NOT NULL,
    direction TEXT NOT NULL DEFAULT 'out' CHECK(direction IN ('out', 'in', 'either')),
    target_entity_type TEXT,
    coarser_entity_type TEXT,
    finer_entity_type TEXT,
    severity TEXT NOT NULL DEFAULT 'warning',
    label TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(ontology_id) REFERENCES ontologies(ontology_id),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id)
);
```

`graph_gap_rules` answers "how many edges should exist?". `graph_conflict_rules`
answers "which combinations of active edges are incompatible?".

## Diagnostics report extensions

`graph_diagnostics_report.summary` gains counters:

| Counter | Meaning |
| --- | --- |
| `mutually_exclusive_conflicts` | Count of `mutually_exclusive_facts` issues |
| `temporal_conflicts` | Count of `temporal_conflict` issues |
| `granularity_conflicts` | Count of `granularity_conflict` issues |
| `redundant_facts` | Count of `redundant_fact` issues |
| `conflict_proposals_pending` | Patches in `graph_knowledge_patch` with `status = 'pending'` tied to conflicts |

Existing counters (`missing_required_relations`, `evidence_gaps`, etc.) are
unchanged.

## Mapping MemGraphRAG agents

| MemGraphRAG agent | MindBrain target surface |
| --- | --- |
| Extraction Agent | `business-extract`, `document-qualify` (existing) |
| Conflict Detection Agent | `evaluateGraphConflicts` in `graph_diagnostics.zig` (new) |
| Conflict Resolution Agent | `graph_knowledge_patch` proposal pipeline (see knowledge-patch spec) |

## MCP and HTTP (planned)

| Route | Role |
| --- | --- |
| `GET /api/mindbrain/graph/diagnostics` | Includes conflict issue kinds when enabled |
| `GET /api/mindbrain/graph/conflict-rules` | List `graph_conflict_rules` |
| `POST /api/mindbrain/graph/conflict-rules/import` | Import rules JSON |
| `POST /api/mindbrain/graph/conflicts/propose-patches` | Run detection + emit pending patches |
| `GET /api/mindbrain/graph/knowledge-patches?status=pending` | Review proposals |

GhostCrab MCP mirrors: `ghostcrab_graph_conflict_rules`,
`ghostcrab_graph_conflicts_propose`, extend `ghostcrab_graph_diagnostics`.

## Remediation decision table

| Issue kind | First action | If documented in sources |
| --- | --- | --- |
| `mutually_exclusive_facts` | Open pending patch; compare evidence scores | Re-run extraction on both source docs |
| `temporal_conflict` | Narrow `valid_*` on relations | Parse dates from source passages |
| `granularity_conflict` | Keep finer-grained entity; deprecate coarse edge | Fix ontology mapping in extractor |
| `redundant_fact` | Deprecate lower-confidence duplicate | Deduplicate in extraction pass |

## Related docs

- [diagnostics-and-quality.md](diagnostics-and-quality.md) — current issue kinds
- [graph-conflict-diagnostics-queries.md](graph-conflict-diagnostics-queries.md) — SQL detection queries
- [knowledge-patch-proposal-pipeline.md](knowledge-patch-proposal-pipeline.md) — adjudication flow
