# Governed `live_answer_view` creation

## Goal

Add one authoritative MindBrain HTTP write path for creating workspace-scoped
`live_answer_view` registry rows. GhostCrab consumers must never insert these
rows through the SQL API, synthesize an artifact, rename a conflicting slug, or
report success when the native path is unavailable.

The effective workspace is mandatory. MCP callers may omit an explicit
`workspace_id` only because GhostCrab always resolves the active session
workspace; MindBrain still receives and validates a concrete workspace id.

## Backend contract

`POST /api/mindbrain/ghostcrab/artifact` accepts a strict JSON object:

```json
{
  "workspace_id": "immeuble",
  "slug": "annuaire_coproprietes",
  "public_label": "Annuaire immeubles gérés",
  "definition": {
    "source_plan_id": "analysis_plan__immeuble_competency_questions",
    "business_question": "Quels immeubles sont gérés ?"
  }
}
```

- The workspace must exist.
- `slug` is an explicit canonical ASCII slug, 1-80 bytes, with lower-case
  alphanumeric groups separated by one or more underscores. It is never
  normalized or renamed.
- `public_label` is trimmed, non-empty, and at most 200 bytes.
- `definition` is a non-empty JSON object. Top-level `materialized` is reserved
  for the backend.
- MindBrain generates `live_answer_view__<slug>` and fixes kind, lifecycle,
  state and version server-side.
- A new row starts at version 1 with lifecycle `stale`, state `dirty`, and no
  answer update event.
- An identical replay returns the existing row without mutation. Object key
  order does not affect equality; array order and scalar types/values do.
- A conflicting identity, workspace, label or definition returns HTTP 409.
  There is no suffix allocation on this explicit public surface.

Responses are HTTP 201 for a new row and HTTP 200 for an identical replay,
with `created` and `idempotent` booleans. Validation is HTTP 400, an unknown
workspace is HTTP 404, and an artifact conflict is HTTP 409.

`GET /api/mindbrain/capabilities` exposes
`features.live_answer_view_create = true` only when the create handler exists.

## Refresh compatibility

Creation stores the definition at the top level of `payload_json` so existing
Studio and GhostCrab readers keep seeing `source_plan_id`, `business_question`,
`summary`, and `refresh_checks`.

Explicit refresh preserves that definition and replaces only the server-owned
`materialized` object containing workspace/entity/relation/fact counts. It
increments the version and writes one `answer_update_event` in the same
transaction. A historical live payload that is not a JSON object fails without
changing the row or events.

This delivery does not add a business-query/view-spec execution engine.

## Verification

- Domain tests: valid create, validation, missing workspace, identical replay,
  replay after refresh, conflicts and concurrent requests.
- Refresh tests: definition preservation, materialized count replacement,
  version/event atomicity and invalid historical payload rollback.
- HTTP tests: 201/200/400/404/409, method guard, serialized row and capability.
- Full Zig 0.16 standalone suite with local/global caches under `/tmp`.

## Integration and publication

MindBrain is committed and pushed first. GhostCrab then pins the published SHA
as its `vendor/mindbrain` gitlink and exposes the MCP/CLI surfaces. No tag is
created or pushed as part of this implementation.
