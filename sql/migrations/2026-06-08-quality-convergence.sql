-- Quality convergence reports and remediation action queue.
-- These rows are operational quality surfaces, not answer artifacts.

CREATE TABLE IF NOT EXISTS quality_convergence_run (
    run_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    ontology_id TEXT,
    run_kind TEXT NOT NULL DEFAULT 'convergence',
    status TEXT NOT NULL DEFAULT 'completed'
        CHECK(status IN ('running', 'completed', 'failed')),
    canonical_layer TEXT NOT NULL DEFAULT 'ghostcrab_runtime_registry',
    input_fingerprint TEXT NOT NULL,
    summary_json TEXT NOT NULL DEFAULT '{}',
    report_json TEXT NOT NULL DEFAULT '{}',
    created_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY(ontology_id) REFERENCES ontologies(ontology_id)
);

CREATE INDEX IF NOT EXISTS quality_convergence_run_workspace_idx
    ON quality_convergence_run(workspace_id, created_at_unix DESC);

CREATE TABLE IF NOT EXISTS quality_remediation_action (
    action_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    ontology_id TEXT,
    issue_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 0.5,
    reason TEXT NOT NULL,
    schema_id TEXT,
    entity_type TEXT,
    projection_id TEXT,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    mcp_tool TEXT,
    tool_args_json TEXT NOT NULL DEFAULT '{}',
    execution_mode TEXT NOT NULL
        CHECK(execution_mode IN ('diagnostic_only', 'manual', 'auto_safe', 'auto_allowed')),
    idempotency_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'proposed'
        CHECK(status IN ('proposed', 'approved', 'rejected', 'applied', 'failed', 'skipped')),
    decision_actor TEXT,
    decision_note TEXT,
    result_json TEXT NOT NULL DEFAULT '{}',
    created_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
    decided_at_unix INTEGER,
    applied_at_unix INTEGER,
    FOREIGN KEY(run_id) REFERENCES quality_convergence_run(run_id),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY(ontology_id) REFERENCES ontologies(ontology_id)
);

CREATE INDEX IF NOT EXISTS quality_remediation_action_run_idx
    ON quality_remediation_action(run_id, status, severity);

CREATE UNIQUE INDEX IF NOT EXISTS quality_remediation_action_idempotency_idx
    ON quality_remediation_action(workspace_id, idempotency_key);
