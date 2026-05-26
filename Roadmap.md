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

## Validation Commands

```sh
git diff --stat backup-main-before-post-v1.4.4-roadmap..HEAD
git diff --check
ZIG_LOCAL_CACHE_DIR=/tmp/zig-cache ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache /usr/local/bin/zig-0.16 build test-standalone
```
