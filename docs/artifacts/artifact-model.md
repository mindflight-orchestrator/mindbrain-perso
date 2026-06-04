# MindBrain Answer Artifact Model

This ADR fixes the backend contract for answer artifacts in the SQLite/Personal
runtime. GhostCrab may add client-facing MCP and CLI naming later, but the
authoritative registry lives in MindBrain.

## Vocabulary

`mindbrain_answer_artifacts.artifact_kind` is limited to:

- `analysis_plan`
- `live_answer_view`
- `answer_snapshot`
- `evidence_pack`

Answer update events are rows in `mindbrain_answer_events` with
`event_kind = 'answer_update_event'`. Gap rules, graph gaps, coverage gaps,
answerability gaps, MECE gaps, diagnostics reports, and coverage reports are
not answer artifacts.

For the existing non-artifact gap and report surfaces, see
[non-artifact-gaps-and-reports.md](non-artifact-gaps-and-reports.md).

## Identity

Artifact ids are version-less. The current mutable version is stored in
`current_version`, starting at `1`. Public ids use the `kind__slug` shape, for
example `analysis_plan__pilotage_hebdomadaire`.

Slugs are deterministic ASCII labels generated from label-like input, scope, or
legacy refs. Non-ASCII and punctuation collapse to `_`; duplicate slugs receive
`__2`, `__3`, and so on within the relevant scope shape.

## Scope

- `analysis_plan`: agent and scope scoped unless promoted later.
- `live_answer_view`: workspace scoped; `payload_json` may contain a source plan ref.
- `answer_snapshot`: workspace scoped and frozen.
- `evidence_pack`: tied to a parent artifact/version using `payload_json`.

SQLite constraints enforce the supported scope shapes. Workspace-scoped rows
must have `workspace_id`. Agent-scoped plans must have `agent_id` and `scope`.
Evidence packs must reference a parent in `payload_json.parent_artifact_id`.

## State

The authoritative state field is `state`. `lifecycle` records broad mutability:
`draft`, `active`, `frozen`, `stale`, `archived`, or `deleted`.

Legacy `projections.status` maps into `state` during backfill. Terminal and
frozen booleans are derived at serialization time rather than persisted as
independent fields.

## Legacy Surfaces

Durable `projections` remain the working-memory write surface. The
`/api/mindbrain/ghostcrab/pack-projections` response adds compatibility fields
for `analysis_plan` without changing the stored projection row.

Graph `ProjectionResult` entities remain readable through
`/api/mindbrain/ghostcrab/projection-get`. That bundle is exposed as an
`answer_snapshot` compatibility view. Gap, coverage, diagnostics, and graph
quality routes do not get `artifact_kind`.

Legacy `memory_projections` stays out of the registry for now. It remains the
input to `GET /api/mindbrain/pack` TOON output and is not backfilled.

## Refresh

The Personal/SQLite default is explicit refresh. Writes may mark live views
stale when their dependency can be resolved, but the runtime does not
automatically recompute payloads.

Refreshing a `live_answer_view` increments `current_version` and writes one
`answer_update_event` in the same transaction. If no live recompute engine is
available, refresh records a small signal and preserves the existing payload.

## Edition Policy

This implementation is SQLite-first. Postgres parity is a sibling contract and
must be implemented separately when the plan is widened.
