-- Proposed migration: MemGraphRAG-aligned graph quality control surfaces.
-- Spec: vendor/mindbrain/docs/graphs/graph-conflict-taxonomy.md
-- Implement in mindbrain-perso before bumping vendor/mindbrain submodule.

-- Observed schema pattern frequencies (post business-extract / reindex refresh).
CREATE TABLE IF NOT EXISTS graph_schema_pattern_frequency (
    pattern_id INTEGER PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    ontology_id TEXT NOT NULL,
    source_entity_type TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    target_entity_type TEXT NOT NULL,
    observation_count INTEGER NOT NULL DEFAULT 0,
    distinct_source_entities INTEGER NOT NULL DEFAULT 0,
    distinct_target_entities INTEGER NOT NULL DEFAULT 0,
    last_observed_at_unix INTEGER,
    corpus_scope TEXT NOT NULL DEFAULT 'workspace',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(workspace_id, ontology_id, source_entity_type, relation_type, target_entity_type),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY(ontology_id) REFERENCES ontologies(ontology_id)
);

CREATE INDEX IF NOT EXISTS graph_schema_pattern_freq_lookup_idx
    ON graph_schema_pattern_frequency(workspace_id, ontology_id, observation_count DESC);

-- Declarative conflict expectations (complement graph_gap_rules cardinality).
CREATE TABLE IF NOT EXISTS graph_conflict_rules (
    rule_id TEXT PRIMARY KEY,
    ontology_id TEXT NOT NULL,
    workspace_id TEXT,
    conflict_kind TEXT NOT NULL CHECK(conflict_kind IN (
        'mutually_exclusive', 'temporal', 'granularity', 'redundant'
    )),
    entity_type TEXT,
    relation_type TEXT NOT NULL,
    direction TEXT NOT NULL DEFAULT 'out' CHECK(direction IN ('out', 'in', 'either')),
    target_entity_type TEXT,
    coarser_entity_type TEXT,
    finer_entity_type TEXT,
    severity TEXT NOT NULL DEFAULT 'warning',
    label TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(ontology_id) REFERENCES ontologies(ontology_id),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id)
);

CREATE INDEX IF NOT EXISTS graph_conflict_rules_lookup_idx
    ON graph_conflict_rules(ontology_id, workspace_id, enabled);

-- graph_knowledge_patch extensions (idempotent ALTER — SQLite may require rebuild in app).
-- Apply via migration runner that checks column presence.
