# Quality Convergence Pipeline

This document defines the native MindBrain layer used to compare registry schemas,
native ontology definitions, graph data, coverage, diagnostics, and projection
state before proposing remediation actions.

## Native Storage

The pipeline persists two native tables:

- `quality_convergence_run`: one immutable run envelope per analysis execution.
- `quality_remediation_action`: proposed actions derived from a run.

A run stores workspace and ontology scope, run kind, canonical layer, status,
input fingerprint, summary JSON, and the full report JSON.

An action stores issue type, severity, confidence, evidence JSON, proposed MCP
tool arguments, execution mode, and decision state.

This state is deliberately not an answer artifact. It belongs to the runtime
quality and remediation loop, not to user-facing generated answers.

## Native Surfaces

HTTP endpoints:

| Method | Route | Contract |
|--------|-------|----------|
| `POST` | `/api/mindbrain/quality/convergence/run` | Body: `workspace_id`, optional `ontology_id`, optional `persist` defaulting true, optional `limit`, optional `component_small_max`. |
| `GET` | `/api/mindbrain/quality/convergence/runs` | Query: required `workspace_id`, optional `limit`; returns persisted run summaries. |
| `GET` | `/api/mindbrain/quality/convergence/run` | Query: required `run_id`; returns one persisted run. |
| `GET` | `/api/mindbrain/quality/remediation/actions` | Query: required `run_id`, optional `status`; returns proposed actions. |
| `POST` | `/api/mindbrain/quality/remediation/decision` | Body: `action_id`, `decision=approved|rejected`, optional `actor`, optional `note`. |
| `POST` | `/api/mindbrain/quality/remediation/status` | Body: `action_id`, `status=proposed|approved|rejected|applied|failed|skipped`, optional `result_json`. |

Standalone CLI commands:

```text
mindbrain-standalone-tool quality-convergence --db <sqlite_path> --workspace-id <id> [--ontology-id <id>] [--no-persist] [--limit <n>]
mindbrain-standalone-tool quality-remediation-list --db <sqlite_path> --run-id <id> [--status <status>]
mindbrain-standalone-tool quality-remediation-decision --db <sqlite_path> --action-id <id> --decision approved|rejected [--actor <id>] [--note <text>]
mindbrain-standalone-tool quality-remediation-status --db <sqlite_path> --action-id <id> --status proposed|approved|rejected|applied|failed|skipped [--result-json <json>]
```

## Analysis Contract

The first implementation compares five layers:

- schema registry facts and facet definitions
- native ontology tables
- graph entities and relations
- ontology coverage report
- graph diagnostics report

The canonical layer defaults to `native_ontology`. Registry schemas and
graph/runtime data are measured against the ontology where possible, instead of
treating every layer as independently authoritative.

## Remediation Policy

The native pipeline only proposes actions. Execution remains separate and must
go through a decision state first. This gives the assistant or operator a stable
review point before any mutation-capable MCP tool is called.

Current action classes include registry/native ontology mismatches, missing
relation materialization, ontology coverage gaps, and graph diagnostics issues.
