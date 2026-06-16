-- Answer artifact registry and retained answer-update events.
-- Keep this migration aligned with sql/sqlite_mindbrain--1.0.0.sql.

CREATE TABLE IF NOT EXISTS mindbrain_answer_artifacts (
    artifact_id TEXT PRIMARY KEY,
    slug TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    agent_id TEXT,
    scope TEXT,
    artifact_kind TEXT NOT NULL CHECK (artifact_kind IN ('analysis_plan', 'live_answer_view', 'answer_snapshot', 'evidence_pack')),
    public_label_key TEXT,
    public_label TEXT NOT NULL,
    lifecycle TEXT NOT NULL CHECK (lifecycle IN ('draft', 'active', 'frozen', 'stale', 'archived', 'deleted')),
    state TEXT NOT NULL,
    current_version INTEGER NOT NULL DEFAULT 1 CHECK (current_version >= 1),
    payload_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(payload_json)),
    legacy_ref TEXT,
    created_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
    CHECK (
        (artifact_kind = 'analysis_plan' AND agent_id IS NOT NULL AND scope IS NOT NULL) OR
        (artifact_kind IN ('live_answer_view', 'answer_snapshot')) OR
        (artifact_kind = 'evidence_pack' AND json_extract(payload_json, '$.parent_artifact_id') IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS mindbrain_answer_artifacts_workspace_uidx
    ON mindbrain_answer_artifacts(workspace_id, artifact_kind, slug)
    WHERE artifact_kind IN ('analysis_plan', 'live_answer_view', 'answer_snapshot');

CREATE UNIQUE INDEX IF NOT EXISTS mindbrain_answer_artifacts_agent_uidx
    ON mindbrain_answer_artifacts(workspace_id, agent_id, scope, artifact_kind, slug)
    WHERE artifact_kind = 'analysis_plan';

CREATE UNIQUE INDEX IF NOT EXISTS mindbrain_answer_artifacts_legacy_uidx
    ON mindbrain_answer_artifacts(legacy_ref)
    WHERE legacy_ref IS NOT NULL;

CREATE INDEX IF NOT EXISTS mindbrain_answer_artifacts_workspace_idx
    ON mindbrain_answer_artifacts(workspace_id, artifact_kind, lifecycle);

CREATE TABLE IF NOT EXISTS mindbrain_answer_events (
    event_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL,
    event_kind TEXT NOT NULL CHECK (event_kind = 'answer_update_event'),
    from_version INTEGER,
    to_version INTEGER,
    signal_json TEXT NOT NULL CHECK (json_valid(signal_json)),
    created_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
    FOREIGN KEY(artifact_id) REFERENCES mindbrain_answer_artifacts(artifact_id)
);

CREATE INDEX IF NOT EXISTS mindbrain_answer_events_artifact_idx
    ON mindbrain_answer_events(artifact_id, created_at_unix DESC);
