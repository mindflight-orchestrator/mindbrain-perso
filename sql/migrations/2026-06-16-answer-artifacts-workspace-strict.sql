-- Migration: make analysis_plan answer artifacts workspace-owned.
--
-- Older Personal SQLite builds stored analysis_plan rows with workspace_id NULL
-- and carried the workspace only by convention in scope. This migration accepts
-- only rows whose scope maps to exactly one registered workspace_id.

CREATE TEMP TABLE IF NOT EXISTS answer_artifact_workspace_guard (
    must_be_zero INTEGER NOT NULL CHECK (must_be_zero = 0)
);

INSERT INTO answer_artifact_workspace_guard(must_be_zero)
SELECT 1
WHERE EXISTS (
    SELECT 1
    FROM mindbrain_answer_artifacts a
    WHERE a.artifact_kind = 'analysis_plan'
      AND a.workspace_id IS NULL
      AND (
          a.scope IS NULL
          OR (
              SELECT COUNT(*)
              FROM workspaces w
              WHERE a.scope = w.workspace_id
                 OR a.scope LIKE w.workspace_id || ':%'
          ) != 1
      )
);

DROP TABLE answer_artifact_workspace_guard;

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

DROP INDEX IF EXISTS mindbrain_answer_events_artifact_idx;
DROP INDEX IF EXISTS mindbrain_answer_artifacts_workspace_uidx;
DROP INDEX IF EXISTS mindbrain_answer_artifacts_agent_uidx;
DROP INDEX IF EXISTS mindbrain_answer_artifacts_legacy_uidx;
DROP INDEX IF EXISTS mindbrain_answer_artifacts_workspace_idx;

ALTER TABLE mindbrain_answer_events RENAME TO mindbrain_answer_events__artifact_workspace_strict;
ALTER TABLE mindbrain_answer_artifacts RENAME TO mindbrain_answer_artifacts__legacy_workspace_nullable;

CREATE TABLE mindbrain_answer_artifacts (
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

INSERT INTO mindbrain_answer_artifacts(
    artifact_id, slug, workspace_id, agent_id, scope, artifact_kind,
    public_label_key, public_label, lifecycle, state, current_version,
    payload_json, legacy_ref, created_at_unix, updated_at_unix
)
SELECT
    artifact_id,
    slug,
    COALESCE(
        workspace_id,
        (
            SELECT w.workspace_id
            FROM workspaces w
            WHERE mindbrain_answer_artifacts__legacy_workspace_nullable.scope = w.workspace_id
               OR mindbrain_answer_artifacts__legacy_workspace_nullable.scope LIKE w.workspace_id || ':%'
            ORDER BY length(w.workspace_id) DESC
            LIMIT 1
        )
    ),
    agent_id, scope, artifact_kind,
    public_label_key, public_label, lifecycle, state, current_version,
    payload_json, legacy_ref, created_at_unix, updated_at_unix
FROM mindbrain_answer_artifacts__legacy_workspace_nullable;

DROP TABLE mindbrain_answer_artifacts__legacy_workspace_nullable;

CREATE TABLE mindbrain_answer_events (
    event_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL,
    event_kind TEXT NOT NULL CHECK (event_kind = 'answer_update_event'),
    from_version INTEGER,
    to_version INTEGER,
    signal_json TEXT NOT NULL CHECK (json_valid(signal_json)),
    created_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
    FOREIGN KEY(artifact_id) REFERENCES mindbrain_answer_artifacts(artifact_id)
);

INSERT INTO mindbrain_answer_events(
    event_id, artifact_id, event_kind, from_version, to_version, signal_json, created_at_unix
)
SELECT
    event_id, artifact_id, event_kind, from_version, to_version, signal_json, created_at_unix
FROM mindbrain_answer_events__artifact_workspace_strict;

DROP TABLE mindbrain_answer_events__artifact_workspace_strict;

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

CREATE INDEX IF NOT EXISTS mindbrain_answer_events_artifact_idx
    ON mindbrain_answer_events(artifact_id, created_at_unix DESC);
