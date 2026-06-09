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

- `POST /api/mindbrain/quality/convergence/run`
- `GET /api/mindbrain/quality/convergence/runs?workspace_id=...`
- `GET /api/mindbrain/quality/convergence/run?run_id=...`
- `GET /api/mindbrain/quality/remediation/actions?run_id=...`
- `POST /api/mindbrain/quality/remediation/decision`
- `POST /api/mindbrain/quality/remediation/status`

Standalone CLI commands:

- `quality-convergence`
- `quality-remediation-list`
- `quality-remediation-decision`
- `quality-remediation-status`

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
