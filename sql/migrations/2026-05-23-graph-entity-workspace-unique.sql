-- Migration: rebuild graph_entity so entity natural keys are scoped by workspace.
-- Intended for legacy databases whose graph_entity table still has
-- UNIQUE(entity_type, name). The runtime path applies the same rebuild from
-- Database.applyStandaloneSchema().

CREATE TABLE IF NOT EXISTS mindbrain_schema_migrations (
    id TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

UPDATE graph_entity
SET workspace_id = COALESCE(
    NULLIF(json_extract(metadata_json, '$.workspace_id'), ''),
    workspace_id
)
WHERE workspace_id = 'default'
  AND json_extract(metadata_json, '$.workspace_id') IS NOT NULL
  AND json_extract(metadata_json, '$.workspace_id') != '';

PRAGMA foreign_keys = OFF;

DROP TABLE IF EXISTS graph_entity__ws_unique_new;

CREATE TABLE graph_entity__ws_unique_new (
    entity_id INTEGER PRIMARY KEY,
    workspace_id TEXT NOT NULL DEFAULT 'default',
    entity_type TEXT NOT NULL,
    name TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 1.0,
    deprecated_at INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
    UNIQUE(workspace_id, entity_type, name)
);

INSERT INTO graph_entity__ws_unique_new (
    entity_id,
    workspace_id,
    entity_type,
    name,
    confidence,
    deprecated_at,
    metadata_json,
    created_at_unix
)
SELECT
    entity_id,
    workspace_id,
    entity_type,
    name,
    confidence,
    deprecated_at,
    metadata_json,
    created_at_unix
FROM graph_entity;

DROP TABLE graph_entity;
ALTER TABLE graph_entity__ws_unique_new RENAME TO graph_entity;

CREATE INDEX IF NOT EXISTS graph_entity_name_idx ON graph_entity(name);
CREATE INDEX IF NOT EXISTS graph_entity_workspace_type_name_idx
    ON graph_entity(workspace_id, entity_type, name);
CREATE INDEX IF NOT EXISTS graph_entity_workspace_id_idx ON graph_entity(workspace_id);

PRAGMA foreign_keys = ON;

INSERT OR IGNORE INTO mindbrain_schema_migrations (id)
VALUES ('2026-05-23-graph-entity-workspace-unique-applied');
