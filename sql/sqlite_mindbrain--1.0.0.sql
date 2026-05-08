-- MindBrain SQLite install schema.
-- this file is equivalent to  src/standalone/sqlite_schema.zig
-- This file is intentionally SQLite-only. It does not use PostgreSQL
-- extensions, namespaces, procedural functions, server catalog types,
-- specialized index methods, custom data types, or migration-style table
-- alterations.
--
-- Design notes:
-- - Logical PostgreSQL schemas are represented as unprefixed / prefixed table
--   names, matching the standalone SQLite engine.
-- - JSON values are stored as TEXT.
-- - Embeddings and bitmap/posting payloads are stored as BLOB/TEXT payloads and
--   interpreted by the application/native layer.
-- - This is a fresh-install schema. Future migrations should live in separate
--   migration files rather than being folded into this base definition.

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS workspaces (
    id TEXT PRIMARY KEY,
    workspace_id TEXT UNIQUE,
    label TEXT NOT NULL DEFAULT 'GhostCrab Operating Model',
    pg_schema TEXT NOT NULL DEFAULT 'main',
    description TEXT,
    created_by TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    domain_profile TEXT,
    domain_profile_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS table_semantics (
    table_id INTEGER PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    table_schema TEXT NOT NULL DEFAULT 'public',
    table_name TEXT NOT NULL,
    business_role TEXT,
    generation_strategy TEXT NOT NULL DEFAULT 'unknown',
    emit_facets INTEGER NOT NULL DEFAULT 1,
    emit_graph_entity INTEGER NOT NULL DEFAULT 0,
    emit_graph_relation INTEGER NOT NULL DEFAULT 0,
    notes TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    schema_name TEXT,
    key_column TEXT,
    content_column TEXT,
    metadata_column TEXT,
    vector_column TEXT,
    created_at_column TEXT,
    updated_at_column TEXT,
    language TEXT NOT NULL DEFAULT 'english',
    rich_meta TEXT NOT NULL DEFAULT '{}',
    UNIQUE(workspace_id, table_schema, table_name),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id)
);

CREATE TABLE IF NOT EXISTS column_semantics (
    column_semantic_id INTEGER PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    table_id INTEGER,
    table_schema TEXT NOT NULL DEFAULT 'public',
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    column_role TEXT NOT NULL DEFAULT 'unknown',
    semantic_role TEXT,
    data_type TEXT,
    is_nullable INTEGER NOT NULL DEFAULT 1,
    rich_meta TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY(table_id) REFERENCES table_semantics(table_id),
    UNIQUE(workspace_id, table_schema, table_name, column_name)
);

CREATE TABLE IF NOT EXISTS relation_semantics (
    relation_semantic_id INTEGER PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    from_schema TEXT NOT NULL DEFAULT 'public',
    from_table TEXT NOT NULL,
    to_schema TEXT NOT NULL DEFAULT 'public',
    to_table TEXT NOT NULL,
    fk_column TEXT NOT NULL DEFAULT '',
    relation_kind TEXT NOT NULL DEFAULT 'unknown',
    relation_name TEXT,
    source_table_id INTEGER,
    target_table_id INTEGER,
    edge_type TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    rich_meta TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY(source_table_id) REFERENCES table_semantics(table_id),
    FOREIGN KEY(target_table_id) REFERENCES table_semantics(table_id),
    UNIQUE(workspace_id, from_schema, from_table, to_schema, to_table, fk_column)
);

CREATE TABLE IF NOT EXISTS source_mappings (
    source_mapping_id INTEGER PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    source_key TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    target_table_id INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY(target_table_id) REFERENCES table_semantics(table_id),
    UNIQUE(workspace_id, source_key)
);

CREATE TABLE IF NOT EXISTS facet_tables (
    table_id INTEGER PRIMARY KEY,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    chunk_bits INTEGER NOT NULL,
    FOREIGN KEY(table_id) REFERENCES table_semantics(table_id),
    UNIQUE(schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS facet_definitions (
    table_id INTEGER NOT NULL,
    facet_id INTEGER NOT NULL,
    facet_name TEXT NOT NULL,
    PRIMARY KEY(table_id, facet_id),
    FOREIGN KEY(table_id) REFERENCES facet_tables(table_id),
    UNIQUE(table_id, facet_name)
);

CREATE TABLE IF NOT EXISTS facet_postings (
    table_id INTEGER NOT NULL,
    facet_id INTEGER NOT NULL,
    facet_value TEXT NOT NULL,
    chunk_id INTEGER NOT NULL,
    posting_blob BLOB NOT NULL,
    PRIMARY KEY(table_id, facet_id, facet_value, chunk_id),
    FOREIGN KEY(table_id, facet_id) REFERENCES facet_definitions(table_id, facet_id)
);

CREATE TABLE IF NOT EXISTS facet_deltas (
    table_id INTEGER NOT NULL,
    facet_id INTEGER NOT NULL,
    facet_value TEXT NOT NULL,
    posting INTEGER NOT NULL,
    delta INTEGER NOT NULL,
    PRIMARY KEY(table_id, facet_id, facet_value, posting),
    FOREIGN KEY(table_id, facet_id) REFERENCES facet_definitions(table_id, facet_id)
);

CREATE TABLE IF NOT EXISTS facet_value_nodes (
    table_id INTEGER NOT NULL,
    value_id INTEGER NOT NULL,
    facet_id INTEGER NOT NULL,
    facet_value TEXT NOT NULL,
    children_blob BLOB,
    PRIMARY KEY(table_id, value_id),
    FOREIGN KEY(table_id, facet_id) REFERENCES facet_definitions(table_id, facet_id)
);

CREATE TABLE IF NOT EXISTS graph_entity (
    entity_id INTEGER PRIMARY KEY,
    workspace_id TEXT NOT NULL DEFAULT 'default',
    entity_type TEXT NOT NULL,
    name TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 1.0,
    deprecated_at INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
    UNIQUE(entity_type, name)
);

CREATE INDEX IF NOT EXISTS graph_entity_name_idx
    ON graph_entity(name);

CREATE INDEX IF NOT EXISTS graph_entity_workspace_id_idx
    ON graph_entity(workspace_id);

CREATE TABLE IF NOT EXISTS graph_entity_alias (
    term TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    confidence REAL NOT NULL DEFAULT 1.0,
    PRIMARY KEY(term, entity_id),
    FOREIGN KEY(entity_id) REFERENCES graph_entity(entity_id)
);

CREATE TABLE IF NOT EXISTS graph_relation (
    relation_id INTEGER PRIMARY KEY,
    workspace_id TEXT NOT NULL DEFAULT 'default',
    relation_type TEXT NOT NULL,
    source_id INTEGER NOT NULL,
    target_id INTEGER NOT NULL,
    valid_from_unix INTEGER,
    valid_to_unix INTEGER,
    confidence REAL NOT NULL DEFAULT 1.0,
    deprecated_at INTEGER,
    run_id INTEGER,
    patch_id INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
    FOREIGN KEY(source_id) REFERENCES graph_entity(entity_id),
    FOREIGN KEY(target_id) REFERENCES graph_entity(entity_id)
);

CREATE INDEX IF NOT EXISTS graph_relation_workspace_id_idx
    ON graph_relation(workspace_id);

CREATE INDEX IF NOT EXISTS graph_relation_source_id_idx
    ON graph_relation(source_id, relation_id);

CREATE INDEX IF NOT EXISTS graph_relation_target_id_idx
    ON graph_relation(target_id, relation_id);

CREATE TABLE IF NOT EXISTS graph_entity_document (
    entity_id INTEGER NOT NULL,
    doc_id INTEGER NOT NULL,
    table_id INTEGER NOT NULL,
    role TEXT,
    confidence REAL NOT NULL DEFAULT 1.0,
    PRIMARY KEY(entity_id, doc_id, table_id),
    FOREIGN KEY(entity_id) REFERENCES graph_entity(entity_id)
);

CREATE TABLE IF NOT EXISTS graph_lj_out (
    entity_id INTEGER PRIMARY KEY,
    relation_ids_blob BLOB NOT NULL,
    FOREIGN KEY(entity_id) REFERENCES graph_entity(entity_id)
);

CREATE TABLE IF NOT EXISTS graph_lj_in (
    entity_id INTEGER PRIMARY KEY,
    relation_ids_blob BLOB NOT NULL,
    FOREIGN KEY(entity_id) REFERENCES graph_entity(entity_id)
);

CREATE TABLE IF NOT EXISTS graph_execution_run (
    id INTEGER PRIMARY KEY,
    run_key TEXT NOT NULL UNIQUE,
    domain TEXT NOT NULL,
    outcome TEXT NOT NULL DEFAULT 'success',
    confidence REAL NOT NULL DEFAULT 1.0,
    transcript TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at_unix INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE TABLE IF NOT EXISTS graph_knowledge_patch (
    id INTEGER PRIMARY KEY,
    patch_key TEXT NOT NULL UNIQUE,
    domain TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    confidence REAL NOT NULL DEFAULT 0.5,
    artifacts_json TEXT NOT NULL DEFAULT '[]',
    source_run TEXT,
    applied_by TEXT,
    created_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
    applied_at_unix INTEGER,
    FOREIGN KEY(source_run) REFERENCES graph_execution_run(run_key)
);

CREATE TABLE IF NOT EXISTS graph_entity_degree (
    entity_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    confidence REAL NOT NULL,
    out_degree INTEGER NOT NULL,
    in_degree INTEGER NOT NULL,
    total_degree INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS facets (
    id TEXT PRIMARY KEY,
    schema_id TEXT NOT NULL,
    content TEXT NOT NULL,
    facets TEXT NOT NULL DEFAULT '{}',
    facets_json TEXT NOT NULL DEFAULT '{}',
    embedding_blob BLOB,
    embedding TEXT,
    created_by TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at_unix INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at_unix INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 1,
    supersedes TEXT,
    valid_from_unix INTEGER,
    valid_until_unix INTEGER,
    workspace_id TEXT NOT NULL DEFAULT 'default',
    source_ref TEXT,
    doc_id INTEGER UNIQUE
);

CREATE INDEX IF NOT EXISTS facets_workspace_id_idx
    ON facets(workspace_id);

CREATE INDEX IF NOT EXISTS facets_source_ref_idx
    ON facets(source_ref)
    WHERE source_ref IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS facets_source_ref_workspace_uniq
    ON facets(source_ref, workspace_id)
    WHERE source_ref IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_facets_source_ref_workspace
    ON facets(source_ref, workspace_id)
    WHERE source_ref IS NOT NULL;

CREATE TRIGGER IF NOT EXISTS trg_sync_workspace_compat_after_insert
AFTER INSERT ON workspaces
BEGIN
    UPDATE workspaces
    SET workspace_id = COALESCE(NEW.workspace_id, NEW.id),
        domain_profile_json = COALESCE(NULLIF(NEW.domain_profile_json, '{}'), NEW.domain_profile, '{}'),
        domain_profile = COALESCE(NEW.domain_profile, NULLIF(NEW.domain_profile_json, '{}')),
        updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_sync_workspace_compat_after_update
AFTER UPDATE ON workspaces
BEGIN
    UPDATE workspaces
    SET workspace_id = COALESCE(NEW.workspace_id, NEW.id),
        domain_profile_json = COALESCE(NULLIF(NEW.domain_profile_json, '{}'), NEW.domain_profile, '{}'),
        domain_profile = COALESCE(NEW.domain_profile, NULLIF(NEW.domain_profile_json, '{}')),
        updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_sync_facets_compat_after_insert
AFTER INSERT ON facets
BEGIN
    UPDATE facets
    SET facets = COALESCE(NULLIF(NEW.facets, '{}'), NEW.facets_json, '{}'),
        facets_json = COALESCE(NULLIF(NEW.facets_json, '{}'), NEW.facets, '{}'),
        embedding = COALESCE(NEW.embedding, embedding),
        updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_sync_facets_compat_after_update
AFTER UPDATE ON facets
BEGIN
    UPDATE facets
    SET facets = COALESCE(NULLIF(NEW.facets, '{}'), NEW.facets_json, '{}'),
        facets_json = COALESCE(NULLIF(NEW.facets_json, '{}'), NEW.facets, '{}'),
        embedding = COALESCE(NEW.embedding, embedding),
        updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.id;
END;

CREATE TABLE IF NOT EXISTS pending_migrations (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    sql TEXT NOT NULL,
    sync_spec TEXT,
    rationale TEXT,
    preview_trigger TEXT,
    proposed_by TEXT,
    approved_by TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    proposed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    approved_at TEXT,
    executed_at TEXT,
    semantic_spec TEXT,
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id)
);

CREATE TABLE IF NOT EXISTS projections (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    scope TEXT,
    proj_type TEXT NOT NULL,
    content TEXT NOT NULL,
    weight REAL NOT NULL DEFAULT 0.5,
    source_ref TEXT,
    source_type TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at_unix INTEGER NOT NULL DEFAULT 0,
    expires_at_unix INTEGER
);

CREATE INDEX IF NOT EXISTS idx_proj_agent
    ON projections(agent_id, status);

CREATE INDEX IF NOT EXISTS idx_proj_scope
    ON projections(scope) WHERE scope IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_proj_type_weight
    ON projections(proj_type, weight DESC);

CREATE INDEX IF NOT EXISTS idx_proj_expires
    ON projections(expires_at_unix) WHERE expires_at_unix IS NOT NULL;

CREATE TABLE IF NOT EXISTS agent_state (
    agent_id TEXT PRIMARY KEY,
    health TEXT NOT NULL DEFAULT 'GREEN',
    state TEXT NOT NULL DEFAULT 'IDLE',
    metrics_json TEXT NOT NULL DEFAULT '{}',
    updated_at_unix INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS projection_types (
    type_name TEXT PRIMARY KEY,
    compatibility_aliases TEXT NOT NULL DEFAULT '[]',
    rank_bias REAL NOT NULL DEFAULT 0.9,
    pack_priority INTEGER NOT NULL DEFAULT 100,
    next_hop_multiplier REAL NOT NULL DEFAULT 0.8,
    structured INTEGER NOT NULL DEFAULT 0
);

INSERT INTO projection_types (
    type_name, compatibility_aliases, rank_bias,
    pack_priority, next_hop_multiplier, structured
) VALUES
    ('FACT', '["canonical","proposition"]', 1.3, 1, 1.2, 1),
    ('GOAL', '["canonical","proposition"]', 1.2, 1, 1.1, 1),
    ('CONSTRAINT', '["proposition"]', 1.0, 2, 1.0, 1),
    ('STEP', '["proposition"]', 0.9, 2, 0.9, 1),
    ('NOTE', '["raw"]', 0.7, 3, 0.6, 0)
ON CONFLICT(type_name) DO UPDATE SET
    compatibility_aliases = excluded.compatibility_aliases,
    rank_bias = excluded.rank_bias,
    pack_priority = excluded.pack_priority,
    next_hop_multiplier = excluded.next_hop_multiplier,
    structured = excluded.structured;

CREATE TABLE IF NOT EXISTS queue_registry (
    queue_name TEXT PRIMARY KEY,
    created_at_unix INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE TABLE IF NOT EXISTS queue_messages (
    msg_id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_name TEXT NOT NULL,
    read_ct INTEGER NOT NULL DEFAULT 0,
    enqueued_at_unix INTEGER NOT NULL,
    vt_until_unix INTEGER,
    archived INTEGER NOT NULL DEFAULT 0,
    archived_at_unix INTEGER,
    deleted INTEGER NOT NULL DEFAULT 0,
    deleted_at_unix INTEGER,
    message TEXT NOT NULL,
    FOREIGN KEY(queue_name) REFERENCES queue_registry(queue_name)
);

CREATE INDEX IF NOT EXISTS idx_queue_messages_active
    ON queue_messages(queue_name, archived, deleted, vt_until_unix, msg_id);

CREATE TABLE IF NOT EXISTS memory_items (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    source_type TEXT,
    source_ref TEXT,
    content TEXT,
    created_at_unix INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS memory_projections (
    id TEXT PRIMARY KEY,
    item_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    projection_type TEXT NOT NULL,
    content TEXT NOT NULL,
    rank_hint REAL,
    confidence REAL NOT NULL DEFAULT 1.0,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    facets_json TEXT NOT NULL DEFAULT '{}',
    created_at_unix INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY(item_id) REFERENCES memory_items(id)
);

CREATE TABLE IF NOT EXISTS memory_edges (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    node_from TEXT NOT NULL,
    node_to TEXT NOT NULL,
    edge_type TEXT NOT NULL,
    weight REAL NOT NULL DEFAULT 1.0,
    created_at_unix INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS search_documents (
    table_id INTEGER NOT NULL,
    doc_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    language TEXT NOT NULL DEFAULT 'english',
    PRIMARY KEY(table_id, doc_id)
);

CREATE TABLE IF NOT EXISTS search_fts_docs (
    fts_rowid INTEGER PRIMARY KEY,
    table_id INTEGER NOT NULL,
    doc_id INTEGER NOT NULL,
    UNIQUE(table_id, doc_id)
);

CREATE VIRTUAL TABLE IF NOT EXISTS search_fts USING fts5(content);

CREATE TABLE IF NOT EXISTS search_embeddings (
    table_id INTEGER NOT NULL,
    doc_id INTEGER NOT NULL,
    dimensions INTEGER NOT NULL,
    embedding_blob BLOB NOT NULL,
    PRIMARY KEY(table_id, doc_id),
    FOREIGN KEY(table_id, doc_id) REFERENCES search_documents(table_id, doc_id)
);

CREATE TABLE IF NOT EXISTS search_document_stats (
    table_id INTEGER NOT NULL,
    doc_id INTEGER NOT NULL,
    document_length INTEGER NOT NULL,
    unique_terms INTEGER NOT NULL,
    PRIMARY KEY(table_id, doc_id)
);

CREATE TABLE IF NOT EXISTS search_collection_stats (
    table_id INTEGER PRIMARY KEY,
    total_documents INTEGER NOT NULL,
    total_document_length INTEGER NOT NULL DEFAULT 0,
    avg_document_length REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS search_term_stats (
    table_id INTEGER NOT NULL,
    term_hash INTEGER NOT NULL,
    document_frequency INTEGER NOT NULL,
    PRIMARY KEY(table_id, term_hash)
);

CREATE TABLE IF NOT EXISTS search_term_frequencies (
    table_id INTEGER NOT NULL,
    doc_id INTEGER NOT NULL,
    term_hash INTEGER NOT NULL,
    frequency INTEGER NOT NULL,
    PRIMARY KEY(table_id, doc_id, term_hash)
);

CREATE TABLE IF NOT EXISTS search_postings (
    table_id INTEGER NOT NULL,
    term_hash INTEGER NOT NULL,
    posting_blob BLOB NOT NULL,
    PRIMARY KEY(table_id, term_hash)
);

CREATE TABLE IF NOT EXISTS bm25_sync_triggers (
    table_id INTEGER PRIMARY KEY,
    id_column TEXT NOT NULL,
    content_column TEXT NOT NULL,
    language TEXT NOT NULL DEFAULT 'english',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bm25_stopwords (
    language TEXT NOT NULL,
    word TEXT NOT NULL,
    normalized_word TEXT NOT NULL,
    source TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(language, normalized_word)
);

CREATE INDEX IF NOT EXISTS bm25_stopwords_source_idx
    ON bm25_stopwords(language, source);

CREATE TABLE IF NOT EXISTS signal_raw (
    id TEXT PRIMARY KEY,
    emitter_id TEXT NOT NULL,
    asset_ref TEXT,
    platform_ref TEXT,
    event_type TEXT,
    severity_raw INTEGER NOT NULL DEFAULT 1,
    ttp_hint_json TEXT,
    scenario_id TEXT,
    raw_payload_json TEXT,
    status TEXT NOT NULL DEFAULT 'PENDING',
    ts_unix INTEGER NOT NULL,
    promoted INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_signal_raw_ts ON signal_raw (ts_unix DESC);
CREATE INDEX IF NOT EXISTS idx_signal_raw_event_type ON signal_raw (event_type);
CREATE INDEX IF NOT EXISTS idx_signal_raw_severity_raw ON signal_raw (severity_raw);
CREATE INDEX IF NOT EXISTS idx_signal_raw_emitter_id ON signal_raw (emitter_id);
CREATE INDEX IF NOT EXISTS idx_signal_raw_asset_ref ON signal_raw (asset_ref);
CREATE INDEX IF NOT EXISTS idx_signal_raw_scenario_id ON signal_raw (scenario_id);
CREATE INDEX IF NOT EXISTS idx_signal_raw_status ON signal_raw (status);

CREATE TABLE IF NOT EXISTS alert_enriched (
    id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL,
    ttp_ids_json TEXT,
    kill_chain_pos TEXT,
    severity_ai TEXT NOT NULL DEFAULT 'LOW',
    blast_radius_json TEXT,
    confidence REAL NOT NULL DEFAULT 0,
    llm_summary TEXT,
    scenario_id TEXT,
    status TEXT NOT NULL DEFAULT 'TRIAGED',
    ts_unix INTEGER NOT NULL,
    escalated INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (signal_id) REFERENCES signal_raw(id)
);

CREATE INDEX IF NOT EXISTS idx_alert_enriched_ts ON alert_enriched (ts_unix DESC);
CREATE INDEX IF NOT EXISTS idx_alert_enriched_severity_ai ON alert_enriched (severity_ai);
CREATE INDEX IF NOT EXISTS idx_alert_enriched_kill_chain ON alert_enriched (kill_chain_pos);
CREATE INDEX IF NOT EXISTS idx_alert_enriched_signal_id ON alert_enriched (signal_id);
CREATE INDEX IF NOT EXISTS idx_alert_enriched_scenario_id ON alert_enriched (scenario_id);

CREATE TABLE IF NOT EXISTS alert_escalated (
    id TEXT PRIMARY KEY,
    enriched_id TEXT NOT NULL,
    title TEXT NOT NULL,
    severity TEXT NOT NULL,
    recommended_action TEXT,
    status TEXT NOT NULL DEFAULT 'OPEN',
    operator_notes TEXT,
    scenario_id TEXT,
    ts_created_unix INTEGER NOT NULL,
    ts_closed_unix INTEGER,
    FOREIGN KEY (enriched_id) REFERENCES alert_enriched(id)
);

CREATE INDEX IF NOT EXISTS idx_alert_escalated_ts_created ON alert_escalated (ts_created_unix DESC);
CREATE INDEX IF NOT EXISTS idx_alert_escalated_severity ON alert_escalated (severity);
CREATE INDEX IF NOT EXISTS idx_alert_escalated_status ON alert_escalated (status);
CREATE INDEX IF NOT EXISTS idx_alert_escalated_scenario_id ON alert_escalated (scenario_id);

CREATE TABLE IF NOT EXISTS collections (
    collection_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    name TEXT NOT NULL,
    key_kind TEXT NOT NULL DEFAULT 'integer',
    chunk_bits INTEGER NOT NULL DEFAULT 8,
    default_language TEXT NOT NULL DEFAULT 'english',
    status TEXT NOT NULL DEFAULT 'active',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(workspace_id, name),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id)
);

CREATE INDEX IF NOT EXISTS collections_workspace_idx
    ON collections(workspace_id);

CREATE TABLE IF NOT EXISTS ontologies (
    ontology_id TEXT PRIMARY KEY,
    workspace_id TEXT,
    name TEXT NOT NULL,
    version TEXT NOT NULL DEFAULT '1.0.0',
    frozen INTEGER NOT NULL DEFAULT 0,
    source_kind TEXT NOT NULL DEFAULT 'constructed',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(workspace_id, name),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id)
);

CREATE INDEX IF NOT EXISTS ontologies_workspace_idx
    ON ontologies(workspace_id);

CREATE TABLE IF NOT EXISTS collection_ontologies (
    workspace_id TEXT NOT NULL,
    collection_id TEXT NOT NULL,
    ontology_id TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'primary',
    attached_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(workspace_id, collection_id, ontology_id),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY(collection_id) REFERENCES collections(collection_id),
    FOREIGN KEY(ontology_id) REFERENCES ontologies(ontology_id)
);

CREATE INDEX IF NOT EXISTS collection_ontologies_ontology_idx
    ON collection_ontologies(ontology_id);

CREATE TABLE IF NOT EXISTS workspace_settings (
    workspace_id TEXT PRIMARY KEY,
    default_ontology_id TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY(default_ontology_id) REFERENCES ontologies(ontology_id)
);

CREATE TABLE IF NOT EXISTS ontology_namespaces (
    ontology_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    label TEXT,
    parent_namespace TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY(ontology_id, namespace),
    FOREIGN KEY(ontology_id) REFERENCES ontologies(ontology_id)
);

CREATE TABLE IF NOT EXISTS ontology_dimensions (
    ontology_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    dimension TEXT NOT NULL,
    value_type TEXT NOT NULL DEFAULT 'text',
    is_multi INTEGER NOT NULL DEFAULT 0,
    hierarchy_kind TEXT NOT NULL DEFAULT 'flat',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY(ontology_id, namespace, dimension),
    FOREIGN KEY(ontology_id, namespace) REFERENCES ontology_namespaces(ontology_id, namespace)
);

CREATE TABLE IF NOT EXISTS ontology_values (
    ontology_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    dimension TEXT NOT NULL,
    value_id INTEGER NOT NULL,
    value TEXT NOT NULL,
    parent_value_id INTEGER,
    label TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY(ontology_id, namespace, dimension, value_id),
    UNIQUE(ontology_id, namespace, dimension, value),
    FOREIGN KEY(ontology_id, namespace, dimension) REFERENCES ontology_dimensions(ontology_id, namespace, dimension)
);

CREATE INDEX IF NOT EXISTS ontology_values_value_idx
    ON ontology_values(ontology_id, namespace, dimension, value);

CREATE TABLE IF NOT EXISTS ontology_entity_types (
    ontology_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    label TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY(ontology_id, entity_type),
    FOREIGN KEY(ontology_id) REFERENCES ontologies(ontology_id)
);

CREATE TABLE IF NOT EXISTS ontology_edge_types (
    ontology_id TEXT NOT NULL,
    edge_type TEXT NOT NULL,
    directed INTEGER NOT NULL DEFAULT 1,
    source_entity_type TEXT,
    target_entity_type TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY(ontology_id, edge_type),
    FOREIGN KEY(ontology_id) REFERENCES ontologies(ontology_id)
);

CREATE TABLE IF NOT EXISTS ontology_entities_raw (
    ontology_id TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    entity_type TEXT NOT NULL,
    name TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY(ontology_id, entity_id),
    UNIQUE(ontology_id, entity_type, name),
    FOREIGN KEY(ontology_id) REFERENCES ontologies(ontology_id)
);

CREATE TABLE IF NOT EXISTS ontology_relations_raw (
    ontology_id TEXT NOT NULL,
    relation_id INTEGER NOT NULL,
    edge_type TEXT NOT NULL,
    source_entity_id INTEGER NOT NULL,
    target_entity_id INTEGER NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY(ontology_id, relation_id),
    FOREIGN KEY(ontology_id) REFERENCES ontologies(ontology_id)
);

CREATE INDEX IF NOT EXISTS ontology_relations_raw_source_idx
    ON ontology_relations_raw(ontology_id, source_entity_id);

CREATE INDEX IF NOT EXISTS ontology_relations_raw_target_idx
    ON ontology_relations_raw(ontology_id, target_entity_id);

CREATE TABLE IF NOT EXISTS documents_raw (
    workspace_id TEXT NOT NULL,
    collection_id TEXT NOT NULL,
    doc_id INTEGER NOT NULL,
    doc_nanoid TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL DEFAULT '',
    language TEXT,
    source_ref TEXT,
    summary TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(workspace_id, collection_id, doc_id),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY(collection_id) REFERENCES collections(collection_id)
);

CREATE INDEX IF NOT EXISTS documents_raw_collection_idx
    ON documents_raw(collection_id);

CREATE UNIQUE INDEX IF NOT EXISTS documents_raw_nanoid_uidx
    ON documents_raw(doc_nanoid)
    WHERE doc_nanoid <> '';

CREATE TABLE IF NOT EXISTS chunks_raw (
    workspace_id TEXT NOT NULL,
    collection_id TEXT NOT NULL,
    doc_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    language TEXT,
    offset_start INTEGER,
    offset_end INTEGER,
    strategy TEXT,
    token_count INTEGER,
    parent_chunk_index INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY(workspace_id, collection_id, doc_id, chunk_index),
    FOREIGN KEY(workspace_id, collection_id, doc_id) REFERENCES documents_raw(workspace_id, collection_id, doc_id)
);

CREATE TABLE IF NOT EXISTS documents_raw_vector (
    workspace_id TEXT NOT NULL,
    collection_id TEXT NOT NULL,
    doc_id INTEGER NOT NULL,
    dim INTEGER NOT NULL,
    embedding_blob BLOB NOT NULL,
    PRIMARY KEY(workspace_id, collection_id, doc_id),
    FOREIGN KEY(workspace_id, collection_id, doc_id) REFERENCES documents_raw(workspace_id, collection_id, doc_id)
);

CREATE TABLE IF NOT EXISTS chunks_raw_vector (
    workspace_id TEXT NOT NULL,
    collection_id TEXT NOT NULL,
    doc_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    dim INTEGER NOT NULL,
    embedding_blob BLOB NOT NULL,
    PRIMARY KEY(workspace_id, collection_id, doc_id, chunk_index),
    FOREIGN KEY(workspace_id, collection_id, doc_id, chunk_index) REFERENCES chunks_raw(workspace_id, collection_id, doc_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS facet_assignments_raw (
    workspace_id TEXT NOT NULL,
    collection_id TEXT NOT NULL,
    target_kind TEXT NOT NULL CHECK(target_kind IN ('doc','chunk')),
    doc_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL DEFAULT -1,
    ontology_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    dimension TEXT NOT NULL,
    value TEXT NOT NULL,
    value_id INTEGER,
    weight REAL NOT NULL DEFAULT 1.0,
    source TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(workspace_id, collection_id, target_kind, doc_id, chunk_index, ontology_id, namespace, dimension, value),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY(collection_id) REFERENCES collections(collection_id),
    FOREIGN KEY(ontology_id) REFERENCES ontologies(ontology_id)
);

CREATE INDEX IF NOT EXISTS facet_assignments_raw_doc_idx
    ON facet_assignments_raw(workspace_id, collection_id, doc_id);

CREATE INDEX IF NOT EXISTS facet_assignments_raw_lookup_idx
    ON facet_assignments_raw(workspace_id, collection_id, ontology_id, namespace, dimension, value);

CREATE TABLE IF NOT EXISTS entities_raw (
    workspace_id TEXT NOT NULL,
    ontology_id TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    entity_type TEXT NOT NULL,
    name TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 1.0,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(workspace_id, entity_id),
    UNIQUE(workspace_id, entity_type, name),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY(ontology_id) REFERENCES ontologies(ontology_id)
);

CREATE INDEX IF NOT EXISTS entities_raw_ontology_idx
    ON entities_raw(workspace_id, ontology_id);

CREATE TABLE IF NOT EXISTS entity_aliases_raw (
    workspace_id TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    term TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 1.0,
    PRIMARY KEY(workspace_id, entity_id, term),
    FOREIGN KEY(workspace_id, entity_id) REFERENCES entities_raw(workspace_id, entity_id)
);

CREATE INDEX IF NOT EXISTS entity_aliases_raw_term_idx
    ON entity_aliases_raw(workspace_id, term);

CREATE TABLE IF NOT EXISTS relations_raw (
    workspace_id TEXT NOT NULL,
    ontology_id TEXT NOT NULL,
    relation_id INTEGER NOT NULL,
    edge_type TEXT NOT NULL,
    source_entity_id INTEGER NOT NULL,
    target_entity_id INTEGER NOT NULL,
    valid_from TEXT,
    valid_to TEXT,
    confidence REAL NOT NULL DEFAULT 1.0,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(workspace_id, relation_id),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY(ontology_id) REFERENCES ontologies(ontology_id),
    FOREIGN KEY(workspace_id, source_entity_id) REFERENCES entities_raw(workspace_id, entity_id),
    FOREIGN KEY(workspace_id, target_entity_id) REFERENCES entities_raw(workspace_id, entity_id)
);

CREATE INDEX IF NOT EXISTS relations_raw_source_idx
    ON relations_raw(workspace_id, source_entity_id);

CREATE INDEX IF NOT EXISTS relations_raw_target_idx
    ON relations_raw(workspace_id, target_entity_id);

CREATE INDEX IF NOT EXISTS relations_raw_edge_type_idx
    ON relations_raw(workspace_id, edge_type);

CREATE TABLE IF NOT EXISTS entity_documents_raw (
    workspace_id TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    collection_id TEXT NOT NULL,
    doc_id INTEGER NOT NULL,
    role TEXT,
    confidence REAL NOT NULL DEFAULT 1.0,
    PRIMARY KEY(workspace_id, entity_id, collection_id, doc_id),
    FOREIGN KEY(workspace_id, entity_id) REFERENCES entities_raw(workspace_id, entity_id),
    FOREIGN KEY(workspace_id, collection_id, doc_id) REFERENCES documents_raw(workspace_id, collection_id, doc_id)
);

CREATE INDEX IF NOT EXISTS entity_documents_raw_doc_idx
    ON entity_documents_raw(workspace_id, collection_id, doc_id);

CREATE TABLE IF NOT EXISTS entity_chunks_raw (
    workspace_id TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    collection_id TEXT NOT NULL,
    doc_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    role TEXT,
    confidence REAL NOT NULL DEFAULT 1.0,
    PRIMARY KEY(workspace_id, entity_id, collection_id, doc_id, chunk_index),
    FOREIGN KEY(workspace_id, entity_id) REFERENCES entities_raw(workspace_id, entity_id),
    FOREIGN KEY(workspace_id, collection_id, doc_id, chunk_index) REFERENCES chunks_raw(workspace_id, collection_id, doc_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS entity_chunks_raw_chunk_idx
    ON entity_chunks_raw(workspace_id, collection_id, doc_id, chunk_index);

CREATE TABLE IF NOT EXISTS document_links_raw (
    workspace_id TEXT NOT NULL,
    link_id INTEGER NOT NULL,
    ontology_id TEXT NOT NULL,
    edge_type TEXT NOT NULL,
    source_collection_id TEXT NOT NULL,
    source_doc_id INTEGER NOT NULL,
    source_chunk_index INTEGER,
    target_collection_id TEXT NOT NULL,
    target_doc_id INTEGER NOT NULL,
    target_chunk_index INTEGER,
    weight REAL NOT NULL DEFAULT 1.0,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    source TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(workspace_id, link_id),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY(ontology_id) REFERENCES ontologies(ontology_id),
    FOREIGN KEY(workspace_id, source_collection_id, source_doc_id) REFERENCES documents_raw(workspace_id, collection_id, doc_id),
    FOREIGN KEY(workspace_id, target_collection_id, target_doc_id) REFERENCES documents_raw(workspace_id, collection_id, doc_id)
);

CREATE INDEX IF NOT EXISTS document_links_raw_source_idx
    ON document_links_raw(workspace_id, source_collection_id, source_doc_id);

CREATE INDEX IF NOT EXISTS document_links_raw_target_idx
    ON document_links_raw(workspace_id, target_collection_id, target_doc_id);

CREATE INDEX IF NOT EXISTS document_links_raw_edge_type_idx
    ON document_links_raw(workspace_id, ontology_id, edge_type);

CREATE TABLE IF NOT EXISTS external_links_raw (
    workspace_id TEXT NOT NULL,
    link_id INTEGER NOT NULL,
    source_collection_id TEXT NOT NULL,
    source_doc_id INTEGER NOT NULL,
    source_chunk_index INTEGER,
    target_uri TEXT NOT NULL,
    edge_type TEXT NOT NULL DEFAULT 'external_link',
    weight REAL NOT NULL DEFAULT 1.0,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(workspace_id, link_id),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY(workspace_id, source_collection_id, source_doc_id)
        REFERENCES documents_raw(workspace_id, collection_id, doc_id)
);

CREATE INDEX IF NOT EXISTS external_links_raw_doc_idx
    ON external_links_raw(workspace_id, source_collection_id, source_doc_id);

CREATE INDEX IF NOT EXISTS external_links_raw_uri_idx
    ON external_links_raw(target_uri);
