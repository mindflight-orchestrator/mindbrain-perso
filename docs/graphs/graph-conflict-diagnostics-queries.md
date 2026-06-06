# Graph conflict diagnostics queries

Reference SQL and Zig integration notes for detecting `graph_conflict_*` issue
kinds defined in [graph-conflict-taxonomy.md](graph-conflict-taxonomy.md).

These queries run inside `evaluateGraphConflicts` (new function in
`graph_diagnostics.zig`), called from `buildReport` after existing evaluators.

Parameters:

- `?1` = `workspace_id`
- `?2` = `ontology_id`
- `?3` = `limit` (max issues per kind)

All queries filter `r.deprecated_at IS NULL` unless noted.

## 1. Mutually exclusive facts

### 1a. Gap-rule-driven exclusivity (upgrade from `too_many_relations`)

When `graph_gap_rules.max_count = 1` and multiple distinct targets exist:

```sql
SELECT
    r.source_id AS entity_id,
    gr.rule_id,
    r.relation_type,
    GROUP_CONCAT(r.relation_id) AS relation_ids,
    COUNT(DISTINCT r.target_id) AS observed_count
FROM graph_relation r
JOIN graph_entity s ON s.entity_id = r.source_id
JOIN graph_gap_rules gr
  ON gr.ontology_id = ?2
 AND gr.enabled != 0
 AND gr.max_count = 1
 AND gr.entity_type = s.entity_type
 AND gr.relation_type = r.relation_type
 AND (gr.workspace_id IS NULL OR gr.workspace_id = ?1)
WHERE r.workspace_id = ?1
  AND r.deprecated_at IS NULL
  AND (
    (gr.direction = 'out' AND r.source_id = s.entity_id)
    OR (gr.direction = 'in' AND r.target_id = s.entity_id)
    OR (gr.direction = 'either' AND (r.source_id = s.entity_id OR r.target_id = s.entity_id))
  )
GROUP BY r.source_id, gr.rule_id, r.relation_type
HAVING COUNT(DISTINCT r.target_id) > 1
ORDER BY r.source_id
LIMIT ?3;
```

Emit kind `mutually_exclusive_facts` (not `too_many_relations`) when targets
are semantically distinct entities.

### 1b. Undeclared exclusive slot (same source + relation_type, different targets)

```sql
SELECT
    r.source_id AS entity_id,
    r.relation_type,
    GROUP_CONCAT(r.relation_id) AS relation_ids,
    COUNT(DISTINCT r.target_id) AS observed_count
FROM graph_relation r
WHERE r.workspace_id = ?1
  AND r.deprecated_at IS NULL
GROUP BY r.source_id, r.relation_type
HAVING COUNT(DISTINCT r.target_id) > 1
   AND COUNT(*) > 1
ORDER BY observed_count DESC, r.source_id
LIMIT ?3;
```

Gate with `graph_conflict_rules` where `conflict_kind = 'mutually_exclusive'`
or ontology metadata `edge_functional = true` to avoid noise on legitimately
multi-valued predicates (e.g. `published`).

## 2. Temporal conflicts

### 2a. Invalid interval (from > to)

```sql
SELECT
    r.relation_id,
    r.source_id AS entity_id,
    r.relation_type,
    r.valid_from_unix,
    r.valid_to_unix
FROM graph_relation r
WHERE r.workspace_id = ?1
  AND r.deprecated_at IS NULL
  AND r.valid_from_unix IS NOT NULL
  AND r.valid_to_unix IS NOT NULL
  AND r.valid_from_unix > r.valid_to_unix
ORDER BY r.relation_id
LIMIT ?3;
```

Kind: `temporal_conflict`, sub-reason `invalid_interval`.

### 2b. Overlapping intervals on exclusive slot

```sql
SELECT
    a.relation_id AS relation_id_a,
    b.relation_id AS relation_id_b,
    a.source_id AS entity_id,
    a.relation_type,
    a.target_id AS target_a,
    b.target_id AS target_b
FROM graph_relation a
JOIN graph_relation b
  ON a.workspace_id = b.workspace_id
 AND a.source_id = b.source_id
 AND a.relation_type = b.relation_type
 AND a.relation_id < b.relation_id
 AND a.deprecated_at IS NULL
 AND b.deprecated_at IS NULL
WHERE a.workspace_id = ?1
  AND (
    (a.target_id = b.target_id)
    OR (
      /* exclusive slot: different targets, both intervals open-ended */
      a.target_id != b.target_id
      AND a.valid_to_unix IS NULL
      AND b.valid_to_unix IS NULL
    )
  )
  AND intervals_overlap(
    a.valid_from_unix, a.valid_to_unix,
    b.valid_from_unix, b.valid_to_unix
  )
ORDER BY a.source_id, a.relation_type
LIMIT ?3;
```

`intervals_overlap` is implemented in Zig:

```zig
fn intervalsOverlap(a0: ?i64, a1: ?i64, b0: ?i64, b1: ?i64) bool {
    const start_a = a0 orelse std.math.minInt(i64);
    const end_a = a1 orelse std.math.maxInt(i64);
    const start_b = b0 orelse std.math.minInt(i64);
    const end_b = b1 orelse std.math.maxInt(i64);
    return start_a <= end_b and start_b <= end_a;
}
```

Kind: `temporal_conflict`, sub-reason `overlapping_validity`.

### 2c. Ambiguous open-ended duplicate

Same `(source_id, relation_type, target_id)` with two active rows and NULL
validity on both:

```sql
SELECT
    r.source_id AS entity_id,
    r.relation_type,
    r.target_id,
    GROUP_CONCAT(r.relation_id) AS relation_ids,
    COUNT(*) AS observed_count
FROM graph_relation r
WHERE r.workspace_id = ?1
  AND r.deprecated_at IS NULL
GROUP BY r.source_id, r.relation_type, r.target_id
HAVING COUNT(*) > 1
ORDER BY observed_count DESC
LIMIT ?3;
```

Kind: `redundant_fact` if intervals agree; `temporal_conflict` if mixed
`valid_*` present.

## 3. Granularity conflicts

Requires ontology parent links. Use `ontology_entity_types.metadata_json`:

```json
{ "parent_entity_type": "country" }
```

### 3a. Same predicate, mixed endpoint specificity

```sql
SELECT
    coarse.relation_id AS coarse_relation_id,
    fine.relation_id AS fine_relation_id,
    coarse.source_id AS entity_id,
    coarse.relation_type,
    coarse_tgt.entity_type AS coarse_target_type,
    fine_tgt.entity_type AS fine_target_type
FROM graph_relation coarse
JOIN graph_entity coarse_tgt ON coarse_tgt.entity_id = coarse.target_id
JOIN graph_relation fine
  ON fine.workspace_id = coarse.workspace_id
 AND fine.source_id = coarse.source_id
 AND fine.relation_type = coarse.relation_type
 AND fine.relation_id != coarse.relation_id
 AND fine.deprecated_at IS NULL
JOIN graph_entity fine_tgt ON fine_tgt.entity_id = fine.target_id
JOIN ontology_entity_types coarse_ont
  ON coarse_ont.ontology_id = ?2
 AND coarse_ont.entity_type = coarse_tgt.entity_type
JOIN ontology_entity_types fine_ont
  ON fine_ont.ontology_id = ?2
 AND fine_ont.entity_type = fine_tgt.entity_type
WHERE coarse.workspace_id = ?1
  AND coarse.deprecated_at IS NULL
  AND json_extract(fine_ont.metadata_json, '$.parent_entity_type') = coarse_tgt.entity_type
ORDER BY coarse.source_id
LIMIT ?3;
```

Kind: `granularity_conflict`.

### 3b. Rule-driven granularity pairs

When `graph_conflict_rules.conflict_kind = 'granularity'`:

```sql
SELECT
    r_coarse.relation_id AS coarse_relation_id,
    r_fine.relation_id AS fine_relation_id,
    r_coarse.source_id AS entity_id,
    cr.coarser_entity_type,
    cr.finer_entity_type,
    cr.rule_id
FROM graph_conflict_rules cr
JOIN graph_relation r_coarse
  ON r_coarse.workspace_id = ?1
 AND r_coarse.relation_type = cr.relation_type
 AND r_coarse.deprecated_at IS NULL
JOIN graph_entity t_coarse ON t_coarse.entity_id = r_coarse.target_id
 AND t_coarse.entity_type = cr.coarser_entity_type
JOIN graph_relation r_fine
  ON r_fine.workspace_id = ?1
 AND r_fine.source_id = r_coarse.source_id
 AND r_fine.relation_type = cr.relation_type
 AND r_fine.relation_id != r_coarse.relation_id
 AND r_fine.deprecated_at IS NULL
JOIN graph_entity t_fine ON t_fine.entity_id = r_fine.target_id
 AND t_fine.entity_type = cr.finer_entity_type
WHERE cr.ontology_id = ?2
  AND cr.conflict_kind = 'granularity'
  AND cr.enabled != 0
  AND (cr.workspace_id IS NULL OR cr.workspace_id = ?1)
ORDER BY r_coarse.source_id
LIMIT ?3;
```

## 4. Redundant facts

### 4a. Exact duplicate edges

```sql
SELECT
    MIN(r.relation_id) AS keep_relation_id,
    r.source_id AS entity_id,
    r.target_id,
    r.relation_type,
    GROUP_CONCAT(r.relation_id) AS relation_ids,
    COUNT(*) AS observed_count,
    MIN(r.confidence) AS min_confidence,
    MAX(r.confidence) AS max_confidence
FROM graph_relation r
WHERE r.workspace_id = ?1
  AND r.deprecated_at IS NULL
GROUP BY r.source_id, r.target_id, r.relation_type
HAVING COUNT(*) > 1
ORDER BY observed_count DESC
LIMIT ?3;
```

Kind: `redundant_fact`, `suggested_action`: `deprecate_lower_confidence_duplicates`.

## Evidence attachment for conflict rows

Join evidence counts when emitting issues:

```sql
SELECT
    r.relation_id,
    COUNT(DISTINCT p.ref_doc_id) AS doc_evidence_count,
    COUNT(DISTINCT c.chunk_index) AS chunk_evidence_count
FROM graph_relation r
LEFT JOIN graph_relation_property p
  ON p.relation_id = r.relation_id AND p.ref_doc_id IS NOT NULL
LEFT JOIN graph_entity_chunk c
  ON c.entity_id IN (r.source_id, r.target_id)
 AND c.workspace_id = r.workspace_id
WHERE r.relation_id = ?1
GROUP BY r.relation_id;
```

Store in issue `metadata_json`:

```json
{
  "relation_ids": [10, 11],
  "evidence_by_relation": {
    "10": { "doc_count": 2, "chunk_count": 1, "score": 0.82 },
    "11": { "doc_count": 1, "chunk_count": 0, "score": 0.41 }
  }
}
```

## Zig integration sketch

```zig
pub fn evaluateGraphConflicts(
    db: Database,
    allocator: std.mem.Allocator,
    options: DiagnosticsOptions,
    ontology_id: []const u8,
    issues: *std.ArrayList(Issue),
    summary: *Summary,
) !void {
    try evaluateMutuallyExclusiveFacts(db, allocator, options, ontology_id, issues, summary);
    try evaluateTemporalConflicts(db, allocator, options, issues, summary);
    try evaluateGranularityConflicts(db, allocator, options, ontology_id, issues, summary);
    try evaluateRedundantFacts(db, allocator, options, issues, summary);
}
```

Call from `buildReport` after `evaluateEvidenceGaps` and before
`appendCoverageGaps`.

## Performance notes

- Index: `(workspace_id, source_id, relation_type, deprecated_at)` on `graph_relation`.
- Run conflict detection on demand or after `business-extract` / `reindexGraph`.
- Default `limit` per kind: 50; share global `options.limit` budget.

## Related docs

- [graph-conflict-taxonomy.md](graph-conflict-taxonomy.md)
- [knowledge-patch-proposal-pipeline.md](knowledge-patch-proposal-pipeline.md)
