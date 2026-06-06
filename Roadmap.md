# MindBrain Roadmap

This roadmap summarizes the post-`v1.4.4` work now grouped on the
`rewrite-post-v1.4.4-roadmap` branch. It is organized by delivered product
capability rather than by the original exploratory commit sequence.

## Current Baseline

- Baseline tag: `v1.4.4`
- Rewrite branch: `rewrite-post-v1.4.4-roadmap`
- Preserved backup branch: `backup-main-before-post-v1.4.4-roadmap`
- Validation: the rewrite branch has the same final tree as the original
  `main` head before the squash.

## Roadmap Themes

### 1. Graph Explorer Backend Surface

- Add backend HTTP routes for graph explorer traversal and lookup workflows.
- Wire SQLite graph repository helpers so clients can inspect entities,
  relations, paths, and neighborhoods through API routes.
- Keep the graph explorer implementation native to the standalone server while
  preserving the detailed implementation plan in `docs/plan/`.

### 2. Native LinkML Ontology Interchange

- Add native LinkML compile and export support.
- Add sample ontology profiles and CLI entrypoints for ontology graph workflows.
- Harden import traversal with cycle detection, duplicate preservation, and
  indexed class/enum lookup paths.
- Keep API and methodology documentation aligned with the native ontology graph
  route and CLI surface.

### 3. Studio Taxonomy API Surface

- Add HTTP endpoints for Studio taxonomy and projection workflows.
- Back the taxonomy API with standalone SQLite collection and ontology helpers.
- Document the new API surface and expose the required local environment
  defaults.

### 4. LLM Document Qualification And Extraction

- Add document qualification import support.
- Add native business extraction commands for ontology-backed workflows.
- Fix OpenAI reasoning chat payloads and include ontology vocabulary in LLM
  qualification prompts.
- Preserve and expose full LLM HTTP error bodies, stderr diagnostics,
  empty-response detail, and invalid assignment logs.

### 5. Raw Graph Retry Idempotence

- Add autoincrement raw graph IDs and persistence helpers for repeatable LLM
  graph application.
- Make raw graph retries idempotent.
- Tolerate common LLM property type aliases.
- Create placeholder relation endpoints so missing LLM relation targets do not
  break the extraction pipeline.

## Next Roadmap Priorities

### 6. Move Qualification Ontologies Into System Prompts

- Move qualification taxonomies and ontology vocabulary into the system prompt
  instead of embedding them in the user prompt body.
- Keep source documents and extraction requests in the user prompt so the model
  receives a stable instruction contract plus variable evidence.
- Use this split to improve cross-provider prompt comparison, reduce prompt
  drift from noisy documents, and make the LLM contract easier to audit.
- Current anchor: `buildBusinessExtractionPrompt(...)` still appends ontology
  vocabulary into the user-content prompt, so the prompt builder should be split
  before wider provider validation.

### 7. Live Provider Validation Beyond OpenAI

- Add opt-in live tests for OpenRouter and Anthropic with the same rigor as the
  current OpenAI path.
- Gate live tests with environment variables and classify missing keys,
  quota/billing failures, and provider-side JSON incompatibility as explicit
  skips rather than code regressions.
- Capture request JSON, raw provider response, parsed JSON content, and the
  final database application result for every provider/model run.
- Keep provider-specific adapters honest by checking both transport success and
  semantic import success.

### 8. Audit Import Behavior Across Alternative Models

- Retest the import and business-extraction flow across multiple providers and
  models, not only the model that first passed.
- Compare ontology/taxonomy adherence, missing relation endpoints, property type
  aliases, placeholder creation, retry idempotence, parsing failures, and
  database application results.
- Produce a compact compatibility report for each provider/model with the
  command used, outcome, anomalies, and status: `supported`, `needs adapter`, or
  `unsupported`.
- Use the audit results to decide which models are safe defaults for document
  qualification and which require provider-specific prompt or response handling.

### 9. Graph Gap Diagnostics For Studio And MCP

- Keep the implementation in MindBrain first: Studio and GhostCrab MCP should
  consume one shared backend contract instead of duplicating graph analysis.
- Add a minimal `graph_gap_rules` table for closed-world business constraints
  such as required relations and cardinality. This covers the practical gap
  checks that ontology-only OWL semantics do not express safely.
- Expose diagnostics for missing required relations, excessive relations,
  isolated entities, small connected components, ontology edge-type mismatches,
  missing graph evidence, and existing ontology coverage gaps.
- Provide both HTTP and CLI entrypoints so Studio can build UI panels later and
  MCP can wrap the same data without reaching into SQLite internals.
- Defer heavier layers such as SHACL import/export, OWL reasoning, centrality,
  community detection, graph embeddings, and link prediction until the basic
  diagnostic loop is useful on real workspaces.

**Status: delivered** — MindBrain standalone HTTP/CLI, `graph_gap_rules` table,
and GhostCrab MCP tools `ghostcrab_graph_diagnostics`, `ghostcrab_graph_gap_rules`,
`ghostcrab_graph_gap_rules_import`.

Cross-cutting rules for graph intelligence work after §9:

- MindBrain implements analysis; GhostCrab MCP wraps HTTP; Studio consumes the
  same JSON contract.
- **Violations** (rules, topology, motifs) belong in diagnostics reports.
- **Suggestions** (link prediction) use separate read-only endpoints; writes stay
  on `learn` / explicit upsert paths.
- **Closed world** is explicit via `graph_gap_rules`; LinkML/OWL remain open world.
- Validate each phase on the `immeuble-demo` workspace before generalizing.

Recommended priority after §9: **§9c** (MemGraph QC bridge), **§9b** (demo +
Studio panel), **§11** (motifs), **§10** (topology), **§12** (SHACL compile),
**§13–§14** (structure + suggest), **§15–§16** (batch ML and optional OWL
bridge).

### 9c. MemGraphRAG-Aligned Graph Quality Control (spec delivered)

Spec docs in `docs/graphs/`:

- `graph-conflict-taxonomy.md` — `graph_conflict_*` vs `graph_data_gap`
- `graph-conflict-diagnostics-queries.md` — SQL for exclusive/temporal/granularity/redundant
- `schema-pattern-frequency.md` — `graph_schema_pattern_frequency` + genericity penalty
- `knowledge-patch-proposal-pipeline.md` — pending patches with evidence scoring
- `memory-guided-recall.md` — unified `POST /ghostcrab/recall` pipeline

Migration draft: `sql/migrations/2026-06-05-graph-quality-control.sql`.

Implementation phases:

1. Migration + `evaluateGraphConflicts` in `graph_diagnostics.zig`.
2. `refreshSchemaPatternFrequency` hook after `business-extract` / reindex.
3. `graph-conflicts-propose` + patch approve/reject routes.
4. `memory_recall.zig` + `ghostcrab_recall` MCP wrapper.
5. PPR expansion mode when §13 PageRank ships.

**Done when:** immeuble-demo shows at least one synthetic conflict detected,
one pending patch proposed with evidence scores, and `recall` returns
`memory_hit: true` for a graph-grounded query.

### 9b. Immeuble Gap Demo And Studio Diagnostics Panel

- Ship `examples/immeuble-demo/gap-rules.demo.json` and
  `scripts/demo-immeuble-gaps.sh` for a reproducible baseline → rules → anomaly loop.
- Document the demo in `examples/immeuble-demo/README.md`.
- Add a Studio diagnostics panel that renders `GET /api/mindbrain/graph/diagnostics`
  (`summary` counters, `issues` table, filters on `kind` / `severity` / `rule_id`).
- Reuse existing MCP tools; no new graph analysis in TypeScript.

**Done when:** a 15-minute immeuble demo passes on golden data with
`rules_evaluated >= 3` and zero rule violations; Studio shows the same report as curl.

### 10. Graph Topology Diagnostics

- Add `GET /api/mindbrain/graph/degree-stats` and
  `GET /api/mindbrain/graph/components` in MindBrain standalone.
- Wrap with MCP read tools `ghostcrab_graph_degree_stats` and
  `ghostcrab_graph_components`.
- Optionally extend `graph_diagnostics` with `leaf_entity` and `hub_outlier` issues.
- Validate on immeuble: 13 `unit` rows with coherent degree stats; finance/CODA
  subgraphs identifiable as separate components when isolated.

### 11. Business Motif And Path Rules

- Add motif rule storage/import and `GET /api/mindbrain/graph/motif-diagnostics`.
- Detect broken business chains (building → unit → occupant, lease → unit).
- MCP: `ghostcrab_graph_motif_rules`, `ghostcrab_graph_motif_rules_import`,
  `ghostcrab_graph_motif_diagnostics`.
- Keep unary rules in `graph_gap_rules`; motifs express multi-hop paths only.

### 12. SHACL Interchange And Rule Compilation

- Import SHACL (LinkML `gen-shacl` pipeline) and compile to `graph_gap_rules`.
- Export SHACL from stored rules + ontology edge metadata.
- MCP: `ghostcrab_shacl_import`, `ghostcrab_shacl_export`.
- Defer embedded OWL reasoners; optional external bridge lands in §16.

### 13. Graph Centrality And Community Structure

- Add centrality and community endpoints (`degree`, `betweenness`, `pagerank`;
  `louvain` / `leiden`).
- MCP: `ghostcrab_graph_centrality`, `ghostcrab_graph_communities`.
- Accept batch or short sync compute on immeuble-scale graphs.

### 14. Link Suggestion And Missing-Relation Heuristics

- Add read-only `GET /api/mindbrain/graph/link-suggest` with topological scores
  (common neighbors, Adamic-Adar, Jaccard).
- MCP: `ghostcrab_link_suggest`.
- Treat suggestions as hypotheses, not confirmed gaps.

### 15. Graph Embeddings And Anomaly Scores (Batch)

- Offline jobs write `graph_link_scores` / `graph_anomaly_scores` into SQLite.
- MindBrain serves cached reads; MCP: `ghostcrab_link_scores`,
  `ghostcrab_anomaly_scores`.
- Defer until §9b–§11 are in regular use and graph volume justifies ML.

### 16. OWL Reasoning Bridge (Optional)

- Export RDF/TTL from the MindBrain ontology graph; call an external reasoner.
- Surface `ontology_inconsistency` issues via MCP `ghostcrab_ontology_reason`.
- Keep reasoning out of the embedded standalone hot path.

## Validation Commands

```sh
git diff --stat backup-main-before-post-v1.4.4-roadmap..HEAD
git diff --check
ZIG_LOCAL_CACHE_DIR=/tmp/zig-cache ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache /usr/local/bin/zig-0.16 build test-standalone
```
