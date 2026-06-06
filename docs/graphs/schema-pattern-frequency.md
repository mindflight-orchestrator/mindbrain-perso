# Schema pattern frequency

MemGraphRAG maintains an **ontology layer** of relation schemas `(source_type,
relation_type, target_type)` with corpus frequency counts. MindBrain today has a
**prescriptive** ontology (`ontology_entity_types`, `ontology_edge_types`) but
no aggregate of **observed** schema patterns from extraction.

This spec defines `graph_schema_pattern_frequency` and hooks for import,
diagnostics, and memory-guided retrieval.

## Goals

1. **Construction filter** — drop or flag schemas that appear once in a noisy LLM pass.
2. **Retrieval penalty** — down-rank entity types and edge patterns that are too generic or weakly supported.
3. **Audit** — compare declared ontology edges vs observed instance patterns.

## Table schema (proposed migration)

```sql
CREATE TABLE IF NOT EXISTS graph_schema_pattern_frequency (
    pattern_id INTEGER PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    ontology_id TEXT NOT NULL,
    source_entity_type TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    target_entity_type TEXT NOT NULL,
    observation_count INTEGER NOT NULL DEFAULT 0,
    distinct_source_entities INTEGER NOT NULL DEFAULT 0,
    distinct_target_entities INTEGER NOT NULL DEFAULT 0,
    last_observed_at_unix INTEGER,
    corpus_scope TEXT NOT NULL DEFAULT 'workspace',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(workspace_id, ontology_id, source_entity_type, relation_type, target_entity_type),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY(ontology_id) REFERENCES ontologies(ontology_id)
);

CREATE INDEX IF NOT EXISTS graph_schema_pattern_freq_lookup_idx
    ON graph_schema_pattern_frequency(workspace_id, ontology_id, observation_count DESC);
```

| Column | Meaning |
| --- | --- |
| `observation_count` | Total active relation rows matching the pattern |
| `distinct_source_entities` | Unique `source_id` entities using the pattern |
| `distinct_target_entities` | Unique `target_id` entities |
| `corpus_scope` | `workspace`, `collection:<id>`, or `extraction_run:<run_key>` |
| `metadata_json` | Optional `min_confidence_avg`, `extraction_run_ids[]` |

## Increment path: post `business-extract`

After `applyBusinessExtractionEnvelope` commits `relations_raw` and
`reindexGraph` projects `graph_relation`:

1. Run `refreshSchemaPatternFrequency(workspace_id, ontology_id)`.
2. Upsert from active relations joined to entity types:

```sql
INSERT INTO graph_schema_pattern_frequency (
    workspace_id, ontology_id,
    source_entity_type, relation_type, target_entity_type,
    observation_count, distinct_source_entities, distinct_target_entities,
    last_observed_at_unix
)
SELECT
    r.workspace_id,
    ? AS ontology_id,
    s.entity_type,
    r.relation_type,
    t.entity_type,
    COUNT(*) AS observation_count,
    COUNT(DISTINCT r.source_id) AS distinct_source_entities,
    COUNT(DISTINCT r.target_id) AS distinct_target_entities,
    MAX(r.created_at_unix) AS last_observed_at_unix
FROM graph_relation r
JOIN graph_entity s ON s.entity_id = r.source_id
JOIN graph_entity t ON t.entity_id = r.target_id
WHERE r.workspace_id = ?1
  AND r.deprecated_at IS NULL
GROUP BY r.workspace_id, s.entity_type, r.relation_type, t.entity_type
ON CONFLICT(workspace_id, ontology_id, source_entity_type, relation_type, target_entity_type)
DO UPDATE SET
    observation_count = excluded.observation_count,
    distinct_source_entities = excluded.distinct_source_entities,
    distinct_target_entities = excluded.distinct_target_entities,
    last_observed_at_unix = excluded.last_observed_at_unix;
```

Hook location: `import_pipeline.zig` after successful `reindexGraph`, or
`tool.zig` business-extract completion handler.

## Construction-time filtering

During extraction apply (optional flag `--filter-low-frequency-schemas`):

| Threshold | Action |
| --- | --- |
| `observation_count < min_schema_count` (default 2) | Flag relation in `metadata_json.flagged_low_frequency`; do not block import in v1 |
| Pattern not in `ontology_edge_types` | Existing `relation_type_mismatch` diagnostic |
| Pattern in `ontology_edge_types` but `observation_count = 1` and confidence < 0.6 | Emit diagnostics issue `schema_pattern_weak_support` (severity `info`) |

## Generic type penalty configuration

Store per-ontology genericity in `ontology_entity_types.metadata_json` or a small
side table `ontology_type_genericity`:

```json
{
  "genericity_score": 0.9,
  "genericity_reason": "root_type"
}
```

Default scores (when unset):

| Entity type pattern | Default `genericity_score` |
| --- | --- |
| `Thing`, `Entity`, `Resource` | 1.0 (maximum penalty) |
| `Person`, `Organization`, `Event` | 0.5 |
| Domain-specific types (e.g. `unit`, `lease`) | 0.0 |

Edge types `related_to`, `associated_with`, `mentions` default to 0.8.

## Retrieval penalty (memory-guided recall)

When scoring schema activation for a query (see [memory-guided-recall.md](memory-guided-recall.md)):

```
schema_activation_score =
    text_match(query, pattern) * log1p(observation_count) * (1 - genericity_penalty)
```

Where:

```
genericity_penalty = clamp(
    0.5 * source_genericity + 0.5 * target_genericity + edge_genericity * 0.3,
    0, 0.95
)
```

Patterns with `observation_count = 0` in frequency table but present in
`ontology_edge_types` get `log1p(0) = 0` unless explicitly boosted by query
match on relation type name.

## Diagnostics: schema frequency issues

New optional diagnostics kinds (informational, not blocking):

| Issue kind | Condition |
| --- | --- |
| `schema_pattern_weak_support` | Pattern in ontology, `observation_count < min_schema_count` |
| `schema_pattern_undeclared` | High `observation_count`, no matching `ontology_edge_types` row |
| `schema_pattern_over_generic` | Top matched pattern has `genericity_score >= 0.8` and drives retrieval |

## HTTP / CLI (planned)

| Surface | Role |
| --- | --- |
| `GET /api/mindbrain/graph/schema-patterns?workspace_id=&ontology_id=` | List frequency rows |
| `POST /api/mindbrain/graph/schema-patterns/refresh` | Recompute from `graph_relation` |
| `mindbrain-standalone-tool schema-patterns-refresh` | CLI refresh |

## Relationship to existing surfaces

| Existing | Role vs frequency |
| --- | --- |
| `ontology_edge_types` | Whitelist of allowed patterns |
| `ontology_coverage_gap` | Ontology node not instantiated |
| `graph_entity_degree` | Per-entity connectivity, not schema tuple |
| BM25 term/document frequency | Passage layer only |

## Implementation order

1. Migration + refresh SQL + post-reindex hook.
2. Diagnostics issues for weak/undeclared patterns.
3. Wire genericity penalty into `memory-guided-recall` endpoint.
