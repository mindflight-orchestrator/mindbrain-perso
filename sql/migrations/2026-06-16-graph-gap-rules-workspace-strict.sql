-- Migration: make graph gap rules workspace-owned.
--
-- Older builds allowed graph_gap_rules.workspace_id to be NULL for global
-- rules. Personal runtime now requires every rule to be scoped by workspace.
-- Rows that cannot be scoped are rejected instead of guessed.

CREATE TEMP TABLE IF NOT EXISTS graph_gap_rules_workspace_guard (
    must_be_zero INTEGER NOT NULL CHECK (must_be_zero = 0)
);

INSERT INTO graph_gap_rules_workspace_guard(must_be_zero)
SELECT 1
WHERE EXISTS (
    SELECT 1
    FROM graph_gap_rules
    WHERE workspace_id IS NULL
);

DROP TABLE graph_gap_rules_workspace_guard;

DROP INDEX IF EXISTS graph_gap_rules_lookup_idx;
ALTER TABLE graph_gap_rules RENAME TO graph_gap_rules__legacy_workspace_nullable;

CREATE TABLE graph_gap_rules (
    rule_id TEXT PRIMARY KEY,
    ontology_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    direction TEXT NOT NULL CHECK(direction IN ('out', 'in', 'either')),
    target_entity_type TEXT,
    min_count INTEGER NOT NULL DEFAULT 1,
    max_count INTEGER,
    severity TEXT NOT NULL DEFAULT 'warning',
    label TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(ontology_id) REFERENCES ontologies(ontology_id),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id)
);

INSERT INTO graph_gap_rules(
    rule_id, ontology_id, workspace_id, entity_type, relation_type,
    direction, target_entity_type, min_count, max_count, severity,
    label, enabled, metadata_json, created_at, updated_at
)
SELECT
    rule_id, ontology_id, workspace_id, entity_type, relation_type,
    direction, target_entity_type, min_count, max_count, severity,
    label, enabled, metadata_json, created_at, updated_at
FROM graph_gap_rules__legacy_workspace_nullable;

DROP TABLE graph_gap_rules__legacy_workspace_nullable;

CREATE INDEX IF NOT EXISTS graph_gap_rules_lookup_idx
    ON graph_gap_rules(ontology_id, workspace_id, enabled);
