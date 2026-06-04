# Coverage And Projections

Ontology coverage answers a different question than graph traversal:

- graph traversal asks what relationships are currently materialized;
- ontology coverage asks which ontology or taxonomy nodes are reflected by the
  current graph/facet/projection state.

Coverage gaps are report rows, not answer artifacts. See
[../artifacts/non-artifact-gaps-and-reports.md](../artifacts/non-artifact-gaps-and-reports.md)
for the gap/report boundary.

## Coverage CLI

```bash
mindbrain-standalone-tool coverage \
  --db data/immeuble-demo.sqlite \
  --workspace-id immeuble-demo
```

```bash
mindbrain-standalone-tool coverage-by-domain \
  --db data/immeuble-demo.sqlite \
  --domain-or-workspace immeuble-demo
```

The output is TOON by default and contains:

| Block | Meaning |
|-------|---------|
| `summary` | Workspace counters: covered nodes, total nodes, graph entities, facet rows, projection rows, ratio. |
| `gaps` | Ontology/taxonomy nodes not currently covered. |

## Coverage HTTP

| Route | Query | Response |
|-------|-------|----------|
| `GET /api/mindbrain/coverage` | `workspace_id`, repeated `entity_type` optional | TOON coverage report. |
| `GET /api/mindbrain/coverage-by-domain` | `domain_or_workspace`, repeated `entity_type` optional | TOON coverage report after workspace resolution. |

## Projection Helpers

`src/standalone/ontology_sqlite.zig` also manages durable projection helpers:

| Helper | Role |
|--------|------|
| `insertProjection` | Upsert one `projections` row. |
| `loadAgentProjections` | Load rows by `agent_id`. |
| `materializePackProjections` | Pack durable `projections` for GhostCrab compatibility. |
| `projectionRelevance` | Score projections against a graph/entity context. |
| `materializeRelevanceProjections` | Return ranked durable projections. |
| `materializeTaxonomyProjections` | Create projection rows from taxonomy/facet rows. |

These are agent-facing views. They do not replace ontology definitions or raw
document/graph/facet evidence.

## GhostCrab-Compatible Routes

| Route | Meaning |
|-------|---------|
| `/api/mindbrain/ghostcrab/pack-projections` | Durable `projections` rows selected for packed context. |
| `/api/mindbrain/ghostcrab/projections/relevance` | Durable projections ranked by selected graph/entity context. |
| `/api/mindbrain/ghostcrab/projection-get` | Materialized graph projection bundle with optional evidence/deltas. |

`projection-get` is a graph-bundle style output. Do not confuse it with
working-memory pack rows.

## Interpreting Gaps

An ontology coverage gap does not always mean data is wrong. Common causes:

| Cause | Fix |
|-------|-----|
| The ontology defines a class that is not present in the corpus. | Add source data or demote/remove the unused class. |
| A graph relation uses a legacy/generic edge type. | Align the reindexer or add an ontology edge alias. |
| Evidence exists only in raw documents. | Extract entities/relations/facets and reindex. |
| Projection rows are missing. | Materialize projections if an agent needs compact context. |

Use graph diagnostics for closed-world validation and required-relation checks.
Use coverage to detect unused or uninstantiated ontology vocabulary.
