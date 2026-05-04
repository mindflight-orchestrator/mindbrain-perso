//! Helpers that persist the workspace -> collection -> document/chunk hierarchy
//! and the workspace-scoped ontology graph in SQLite. These are the canonical
//! "raw" tables described in docs/collections.md; derived facet/graph indexes
//! are rebuilt from them by import_pipeline.zig.

const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");

pub const Database = facet_sqlite.Database;
pub const Error = facet_sqlite.Error;
pub const c = facet_sqlite.c;

pub const TargetKind = enum {
    doc,
    chunk,

    pub fn label(self: TargetKind) []const u8 {
        return switch (self) {
            .doc => "doc",
            .chunk => "chunk",
        };
    }
};

pub const WorkspaceSpec = struct {
    workspace_id: []const u8,
    label: ?[]const u8 = null,
    description: ?[]const u8 = null,
    domain_profile: ?[]const u8 = null,
};

pub const CollectionSpec = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    name: []const u8,
    key_kind: []const u8 = "integer",
    chunk_bits: u8 = 8,
    default_language: []const u8 = "english",
    metadata_json: []const u8 = "{}",
};

pub const OntologySpec = struct {
    ontology_id: []const u8,
    workspace_id: ?[]const u8,
    name: []const u8,
    version: []const u8 = "1.0.0",
    frozen: bool = false,
    source_kind: []const u8 = "constructed",
    metadata_json: []const u8 = "{}",
};

pub const NamespaceSpec = struct {
    ontology_id: []const u8,
    namespace: []const u8,
    label: ?[]const u8 = null,
    parent_namespace: ?[]const u8 = null,
    metadata_json: []const u8 = "{}",
};

pub const DimensionSpec = struct {
    ontology_id: []const u8,
    namespace: []const u8,
    dimension: []const u8,
    value_type: []const u8 = "text",
    is_multi: bool = false,
    hierarchy_kind: []const u8 = "flat",
    metadata_json: []const u8 = "{}",
};

pub const ValueSpec = struct {
    ontology_id: []const u8,
    namespace: []const u8,
    dimension: []const u8,
    value_id: u32,
    value: []const u8,
    parent_value_id: ?u32 = null,
    label: ?[]const u8 = null,
    metadata_json: []const u8 = "{}",
};

pub const EntityTypeSpec = struct {
    ontology_id: []const u8,
    entity_type: []const u8,
    label: ?[]const u8 = null,
    metadata_json: []const u8 = "{}",
};

pub const EdgeTypeSpec = struct {
    ontology_id: []const u8,
    edge_type: []const u8,
    directed: bool = true,
    source_entity_type: ?[]const u8 = null,
    target_entity_type: ?[]const u8 = null,
    metadata_json: []const u8 = "{}",
};

pub const OntologyEntitySpec = struct {
    ontology_id: []const u8,
    entity_id: u64,
    entity_type: []const u8,
    name: []const u8,
    metadata_json: []const u8 = "{}",
};

pub const OntologyRelationSpec = struct {
    ontology_id: []const u8,
    relation_id: u64,
    edge_type: []const u8,
    source_entity_id: u64,
    target_entity_id: u64,
    metadata_json: []const u8 = "{}",
};

pub const DocumentRawSpec = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    doc_id: u64,
    /// URL-safe public identifier. Empty string means "do not change" on
    /// upsert (the existing value, if any, is preserved); a non-empty value
    /// is enforced as globally unique by `documents_raw_nanoid_uidx`.
    doc_nanoid: []const u8 = "",
    content: []const u8 = "",
    language: ?[]const u8 = null,
    source_ref: ?[]const u8 = null,
    /// Reserved for a follow-up summarization plan. NULL means "not yet
    /// summarized"; the column is preserved on upsert when this field is
    /// `null`.
    summary: ?[]const u8 = null,
    metadata_json: []const u8 = "{}",
};

pub const ChunkRawSpec = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    doc_id: u64,
    chunk_index: u32,
    content: []const u8 = "",
    language: ?[]const u8 = null,
    offset_start: ?u64 = null,
    offset_end: ?u64 = null,
    /// Name of the chunking strategy that produced this chunk (e.g.
    /// "fixed_token", "semantic", "late"). NULL for legacy / external rows.
    strategy: ?[]const u8 = null,
    /// Token count under the standalone tokenizer; used by the pipeline to
    /// budget LLM contexts without re-tokenizing.
    token_count: ?u64 = null,
    /// For hierarchical strategies (e.g. late chunking that records both a
    /// document-level and slice-level row), the parent slice's chunk_index.
    parent_chunk_index: ?u32 = null,
    metadata_json: []const u8 = "{}",
};

pub const ExternalLinkRawSpec = struct {
    workspace_id: []const u8,
    link_id: u64,
    source_collection_id: []const u8,
    source_doc_id: u64,
    source_chunk_index: ?u32 = null,
    target_uri: []const u8,
    edge_type: []const u8 = "reference",
    weight: f64 = 1.0,
    metadata_json: []const u8 = "{}",
};

pub const VectorSpec = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    doc_id: u64,
    chunk_index: ?u32 = null,
    dim: u32,
    embedding_blob: []const u8,
};

pub const FacetAssignmentRawSpec = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    target_kind: TargetKind,
    doc_id: u64,
    chunk_index: ?u32 = null,
    ontology_id: []const u8,
    namespace: []const u8,
    dimension: []const u8,
    value: []const u8,
    value_id: ?u32 = null,
    weight: f64 = 1.0,
    source: ?[]const u8 = null,
};

pub const EntityRawSpec = struct {
    workspace_id: []const u8,
    ontology_id: []const u8,
    entity_id: u64,
    entity_type: []const u8,
    name: []const u8,
    confidence: f64 = 1.0,
    metadata_json: []const u8 = "{}",
};

pub const EntityAliasRawSpec = struct {
    workspace_id: []const u8,
    entity_id: u64,
    term: []const u8,
    confidence: f64 = 1.0,
};

pub const RelationRawSpec = struct {
    workspace_id: []const u8,
    ontology_id: []const u8,
    relation_id: u64,
    edge_type: []const u8,
    source_entity_id: u64,
    target_entity_id: u64,
    valid_from: ?[]const u8 = null,
    valid_to: ?[]const u8 = null,
    confidence: f64 = 1.0,
    metadata_json: []const u8 = "{}",
};

pub const EntityDocumentRawSpec = struct {
    workspace_id: []const u8,
    entity_id: u64,
    collection_id: []const u8,
    doc_id: u64,
    role: ?[]const u8 = null,
    confidence: f64 = 1.0,
};

pub const EntityChunkRawSpec = struct {
    workspace_id: []const u8,
    entity_id: u64,
    collection_id: []const u8,
    doc_id: u64,
    chunk_index: u32,
    role: ?[]const u8 = null,
    confidence: f64 = 1.0,
};

pub const DocumentLinkRawSpec = struct {
    workspace_id: []const u8,
    link_id: u64,
    ontology_id: []const u8,
    edge_type: []const u8,
    source_collection_id: []const u8,
    source_doc_id: u64,
    source_chunk_index: ?u32 = null,
    target_collection_id: []const u8,
    target_doc_id: u64,
    target_chunk_index: ?u32 = null,
    weight: f64 = 1.0,
    source: ?[]const u8 = null,
    metadata_json: []const u8 = "{}",
};

// ---- Workspaces / collections / settings ----------------------------------

pub fn ensureWorkspace(db: Database, spec: WorkspaceSpec) !void {
    const sql =
        \\INSERT INTO workspaces(id, workspace_id, label, description, domain_profile, status)
        \\VALUES(?1, ?1, COALESCE(?2, ?1), COALESCE(?3, ''), COALESCE(?4, 'generic'), 'active')
        \\ON CONFLICT(workspace_id) DO UPDATE SET
        \\    label = COALESCE(excluded.label, workspaces.label),
        \\    description = COALESCE(excluded.description, workspaces.description),
        \\    domain_profile = COALESCE(excluded.domain_profile, workspaces.domain_profile),
        \\    updated_at = CURRENT_TIMESTAMP
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);

    try facet_sqlite.bindText(stmt, 1, spec.workspace_id);
    if (spec.label) |label| try facet_sqlite.bindText(stmt, 2, label) else try facet_sqlite.bindNull(stmt, 2);
    if (spec.description) |desc| try facet_sqlite.bindText(stmt, 3, desc) else try facet_sqlite.bindNull(stmt, 3);
    if (spec.domain_profile) |profile| try facet_sqlite.bindText(stmt, 4, profile) else try facet_sqlite.bindNull(stmt, 4);
    try facet_sqlite.stepDone(stmt);

    const settings_sql =
        \\INSERT INTO workspace_settings(workspace_id) VALUES(?1)
        \\ON CONFLICT(workspace_id) DO NOTHING
    ;
    const settings_stmt = try facet_sqlite.prepare(db, settings_sql);
    defer facet_sqlite.finalize(settings_stmt);
    try facet_sqlite.bindText(settings_stmt, 1, spec.workspace_id);
    try facet_sqlite.stepDone(settings_stmt);

    // Bootstrap the per-workspace default ontology so that the built-in
    // `source.*` namespace is always available, even before any explicit
    // ontology has been declared. Idempotent.
    var default_id_buf: [256]u8 = undefined;
    const default_id_slice = try formatDefaultOntologyId(&default_id_buf, spec.workspace_id);
    try ensureOntology(db, .{
        .ontology_id = default_id_slice,
        .workspace_id = spec.workspace_id,
        .name = "default",
        .source_kind = "auto",
    });
    try setDefaultOntology(db, spec.workspace_id, default_id_slice);
    try ensureSourceNamespace(db, default_id_slice);
}

pub fn setDefaultOntology(db: Database, workspace_id: []const u8, ontology_id: []const u8) !void {
    const sql =
        \\INSERT INTO workspace_settings(workspace_id, default_ontology_id)
        \\VALUES(?1, ?2)
        \\ON CONFLICT(workspace_id) DO UPDATE SET
        \\    default_ontology_id = excluded.default_ontology_id,
        \\    updated_at = CURRENT_TIMESTAMP
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, ontology_id);
    try facet_sqlite.stepDone(stmt);
}

pub fn defaultOntology(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8) !?[]const u8 {
    const sql = "SELECT default_ontology_id FROM workspace_settings WHERE workspace_id = ?1";
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    const status = c.sqlite3_step(stmt);
    if (status == c.SQLITE_DONE) return null;
    if (status != c.SQLITE_ROW) return error.StepFailed;
    if (c.sqlite3_column_type(stmt, 0) == c.SQLITE_NULL) return null;
    return try facet_sqlite.dupeColumnText(allocator, stmt, 0);
}

pub fn ensureCollection(db: Database, spec: CollectionSpec) !void {
    const sql =
        \\INSERT INTO collections(collection_id, workspace_id, name, key_kind, chunk_bits, default_language, metadata_json)
        \\VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
        \\ON CONFLICT(collection_id) DO UPDATE SET
        \\    name = excluded.name,
        \\    key_kind = excluded.key_kind,
        \\    chunk_bits = excluded.chunk_bits,
        \\    default_language = excluded.default_language,
        \\    metadata_json = excluded.metadata_json,
        \\    updated_at = CURRENT_TIMESTAMP
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.collection_id);
    try facet_sqlite.bindText(stmt, 2, spec.workspace_id);
    try facet_sqlite.bindText(stmt, 3, spec.name);
    try facet_sqlite.bindText(stmt, 4, spec.key_kind);
    try facet_sqlite.bindInt64(stmt, 5, spec.chunk_bits);
    try facet_sqlite.bindText(stmt, 6, spec.default_language);
    try facet_sqlite.bindText(stmt, 7, spec.metadata_json);
    try facet_sqlite.stepDone(stmt);
}

// ---- Ontology header + attachment -----------------------------------------

pub fn ensureOntology(db: Database, spec: OntologySpec) !void {
    const sql =
        \\INSERT INTO ontologies(ontology_id, workspace_id, name, version, frozen, source_kind, metadata_json)
        \\VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
        \\ON CONFLICT(ontology_id) DO UPDATE SET
        \\    name = excluded.name,
        \\    version = excluded.version,
        \\    frozen = excluded.frozen,
        \\    source_kind = excluded.source_kind,
        \\    metadata_json = excluded.metadata_json
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.ontology_id);
    if (spec.workspace_id) |ws| try facet_sqlite.bindText(stmt, 2, ws) else try facet_sqlite.bindNull(stmt, 2);
    try facet_sqlite.bindText(stmt, 3, spec.name);
    try facet_sqlite.bindText(stmt, 4, spec.version);
    try facet_sqlite.bindInt64(stmt, 5, @as(i64, if (spec.frozen) 1 else 0));
    try facet_sqlite.bindText(stmt, 6, spec.source_kind);
    try facet_sqlite.bindText(stmt, 7, spec.metadata_json);
    try facet_sqlite.stepDone(stmt);
}

pub fn attachOntologyToCollection(
    db: Database,
    workspace_id: []const u8,
    collection_id: []const u8,
    ontology_id: []const u8,
    role: []const u8,
) !void {
    const sql =
        \\INSERT INTO collection_ontologies(workspace_id, collection_id, ontology_id, role)
        \\VALUES(?1, ?2, ?3, ?4)
        \\ON CONFLICT(workspace_id, collection_id, ontology_id) DO UPDATE SET role = excluded.role
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, collection_id);
    try facet_sqlite.bindText(stmt, 3, ontology_id);
    try facet_sqlite.bindText(stmt, 4, role);
    try facet_sqlite.stepDone(stmt);
}

// ---- Ontology body --------------------------------------------------------

pub fn ensureNamespace(db: Database, spec: NamespaceSpec) !void {
    const sql =
        \\INSERT INTO ontology_namespaces(ontology_id, namespace, label, parent_namespace, metadata_json)
        \\VALUES(?1, ?2, ?3, ?4, ?5)
        \\ON CONFLICT(ontology_id, namespace) DO UPDATE SET
        \\    label = COALESCE(excluded.label, ontology_namespaces.label),
        \\    parent_namespace = COALESCE(excluded.parent_namespace, ontology_namespaces.parent_namespace),
        \\    metadata_json = excluded.metadata_json
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.ontology_id);
    try facet_sqlite.bindText(stmt, 2, spec.namespace);
    if (spec.label) |label| try facet_sqlite.bindText(stmt, 3, label) else try facet_sqlite.bindNull(stmt, 3);
    if (spec.parent_namespace) |p| try facet_sqlite.bindText(stmt, 4, p) else try facet_sqlite.bindNull(stmt, 4);
    try facet_sqlite.bindText(stmt, 5, spec.metadata_json);
    try facet_sqlite.stepDone(stmt);
}

pub fn ensureDimension(db: Database, spec: DimensionSpec) !void {
    const sql =
        \\INSERT INTO ontology_dimensions(ontology_id, namespace, dimension, value_type, is_multi, hierarchy_kind, metadata_json)
        \\VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
        \\ON CONFLICT(ontology_id, namespace, dimension) DO UPDATE SET
        \\    value_type = excluded.value_type,
        \\    is_multi = excluded.is_multi,
        \\    hierarchy_kind = excluded.hierarchy_kind,
        \\    metadata_json = excluded.metadata_json
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.ontology_id);
    try facet_sqlite.bindText(stmt, 2, spec.namespace);
    try facet_sqlite.bindText(stmt, 3, spec.dimension);
    try facet_sqlite.bindText(stmt, 4, spec.value_type);
    try facet_sqlite.bindInt64(stmt, 5, @as(i64, if (spec.is_multi) 1 else 0));
    try facet_sqlite.bindText(stmt, 6, spec.hierarchy_kind);
    try facet_sqlite.bindText(stmt, 7, spec.metadata_json);
    try facet_sqlite.stepDone(stmt);
}

pub fn ensureValue(db: Database, spec: ValueSpec) !void {
    const sql =
        \\INSERT INTO ontology_values(ontology_id, namespace, dimension, value_id, value, parent_value_id, label, metadata_json)
        \\VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        \\ON CONFLICT(ontology_id, namespace, dimension, value_id) DO UPDATE SET
        \\    value = excluded.value,
        \\    parent_value_id = excluded.parent_value_id,
        \\    label = excluded.label,
        \\    metadata_json = excluded.metadata_json
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.ontology_id);
    try facet_sqlite.bindText(stmt, 2, spec.namespace);
    try facet_sqlite.bindText(stmt, 3, spec.dimension);
    try facet_sqlite.bindInt64(stmt, 4, spec.value_id);
    try facet_sqlite.bindText(stmt, 5, spec.value);
    if (spec.parent_value_id) |p| try facet_sqlite.bindInt64(stmt, 6, p) else try facet_sqlite.bindNull(stmt, 6);
    if (spec.label) |label| try facet_sqlite.bindText(stmt, 7, label) else try facet_sqlite.bindNull(stmt, 7);
    try facet_sqlite.bindText(stmt, 8, spec.metadata_json);
    try facet_sqlite.stepDone(stmt);
}

pub fn ensureEntityType(db: Database, spec: EntityTypeSpec) !void {
    const sql =
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label, metadata_json)
        \\VALUES(?1, ?2, ?3, ?4)
        \\ON CONFLICT(ontology_id, entity_type) DO UPDATE SET
        \\    label = COALESCE(excluded.label, ontology_entity_types.label),
        \\    metadata_json = excluded.metadata_json
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.ontology_id);
    try facet_sqlite.bindText(stmt, 2, spec.entity_type);
    if (spec.label) |label| try facet_sqlite.bindText(stmt, 3, label) else try facet_sqlite.bindNull(stmt, 3);
    try facet_sqlite.bindText(stmt, 4, spec.metadata_json);
    try facet_sqlite.stepDone(stmt);
}

pub fn ensureEdgeType(db: Database, spec: EdgeTypeSpec) !void {
    const sql =
        \\INSERT INTO ontology_edge_types(ontology_id, edge_type, directed, source_entity_type, target_entity_type, metadata_json)
        \\VALUES(?1, ?2, ?3, ?4, ?5, ?6)
        \\ON CONFLICT(ontology_id, edge_type) DO UPDATE SET
        \\    directed = excluded.directed,
        \\    source_entity_type = excluded.source_entity_type,
        \\    target_entity_type = excluded.target_entity_type,
        \\    metadata_json = excluded.metadata_json
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.ontology_id);
    try facet_sqlite.bindText(stmt, 2, spec.edge_type);
    try facet_sqlite.bindInt64(stmt, 3, @as(i64, if (spec.directed) 1 else 0));
    if (spec.source_entity_type) |t| try facet_sqlite.bindText(stmt, 4, t) else try facet_sqlite.bindNull(stmt, 4);
    if (spec.target_entity_type) |t| try facet_sqlite.bindText(stmt, 5, t) else try facet_sqlite.bindNull(stmt, 5);
    try facet_sqlite.bindText(stmt, 6, spec.metadata_json);
    try facet_sqlite.stepDone(stmt);
}

pub fn upsertOntologyEntity(db: Database, spec: OntologyEntitySpec) !void {
    const sql =
        \\INSERT INTO ontology_entities_raw(ontology_id, entity_id, entity_type, name, metadata_json)
        \\VALUES(?1, ?2, ?3, ?4, ?5)
        \\ON CONFLICT(ontology_id, entity_id) DO UPDATE SET
        \\    entity_type = excluded.entity_type,
        \\    name = excluded.name,
        \\    metadata_json = excluded.metadata_json
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.ontology_id);
    try facet_sqlite.bindInt64(stmt, 2, spec.entity_id);
    try facet_sqlite.bindText(stmt, 3, spec.entity_type);
    try facet_sqlite.bindText(stmt, 4, spec.name);
    try facet_sqlite.bindText(stmt, 5, spec.metadata_json);
    try facet_sqlite.stepDone(stmt);
}

pub fn upsertOntologyRelation(db: Database, spec: OntologyRelationSpec) !void {
    const sql =
        \\INSERT INTO ontology_relations_raw(ontology_id, relation_id, edge_type, source_entity_id, target_entity_id, metadata_json)
        \\VALUES(?1, ?2, ?3, ?4, ?5, ?6)
        \\ON CONFLICT(ontology_id, relation_id) DO UPDATE SET
        \\    edge_type = excluded.edge_type,
        \\    source_entity_id = excluded.source_entity_id,
        \\    target_entity_id = excluded.target_entity_id,
        \\    metadata_json = excluded.metadata_json
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.ontology_id);
    try facet_sqlite.bindInt64(stmt, 2, spec.relation_id);
    try facet_sqlite.bindText(stmt, 3, spec.edge_type);
    try facet_sqlite.bindInt64(stmt, 4, spec.source_entity_id);
    try facet_sqlite.bindInt64(stmt, 5, spec.target_entity_id);
    try facet_sqlite.bindText(stmt, 6, spec.metadata_json);
    try facet_sqlite.stepDone(stmt);
}

// ---- Ontology bundle ------------------------------------------------------

pub const OntologyBundle = struct {
    header: OntologySpec,
    namespaces: []const NamespaceSpec = &.{},
    dimensions: []const DimensionSpec = &.{},
    values: []const ValueSpec = &.{},
    entity_types: []const EntityTypeSpec = &.{},
    edge_types: []const EdgeTypeSpec = &.{},
    entities: []const OntologyEntitySpec = &.{},
    relations: []const OntologyRelationSpec = &.{},
};

pub fn loadOntologyBundle(db: Database, bundle: OntologyBundle) !void {
    try ensureOntology(db, bundle.header);
    for (bundle.namespaces) |ns| try ensureNamespace(db, ns);
    for (bundle.dimensions) |dim| try ensureDimension(db, dim);
    for (bundle.values) |value| try ensureValue(db, value);
    for (bundle.entity_types) |et| try ensureEntityType(db, et);
    for (bundle.edge_types) |et| try ensureEdgeType(db, et);
    for (bundle.entities) |e| try upsertOntologyEntity(db, e);
    for (bundle.relations) |r| try upsertOntologyRelation(db, r);
}

// ---- Documents / chunks ---------------------------------------------------

pub fn upsertDocumentRaw(db: Database, spec: DocumentRawSpec) !void {
    const sql =
        \\INSERT INTO documents_raw(workspace_id, collection_id, doc_id, doc_nanoid, content, language, source_ref, summary, metadata_json)
        \\VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        \\ON CONFLICT(workspace_id, collection_id, doc_id) DO UPDATE SET
        \\    doc_nanoid = CASE WHEN excluded.doc_nanoid = '' THEN documents_raw.doc_nanoid ELSE excluded.doc_nanoid END,
        \\    content = excluded.content,
        \\    language = excluded.language,
        \\    source_ref = excluded.source_ref,
        \\    summary = COALESCE(excluded.summary, documents_raw.summary),
        \\    metadata_json = excluded.metadata_json
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.workspace_id);
    try facet_sqlite.bindText(stmt, 2, spec.collection_id);
    try facet_sqlite.bindInt64(stmt, 3, spec.doc_id);
    try facet_sqlite.bindText(stmt, 4, spec.doc_nanoid);
    try facet_sqlite.bindText(stmt, 5, spec.content);
    if (spec.language) |lang| try facet_sqlite.bindText(stmt, 6, lang) else try facet_sqlite.bindNull(stmt, 6);
    if (spec.source_ref) |s| try facet_sqlite.bindText(stmt, 7, s) else try facet_sqlite.bindNull(stmt, 7);
    if (spec.summary) |s| try facet_sqlite.bindText(stmt, 8, s) else try facet_sqlite.bindNull(stmt, 8);
    try facet_sqlite.bindText(stmt, 9, spec.metadata_json);
    try facet_sqlite.stepDone(stmt);
}

pub fn upsertChunkRaw(db: Database, spec: ChunkRawSpec) !void {
    const sql =
        \\INSERT INTO chunks_raw(workspace_id, collection_id, doc_id, chunk_index, content, language, offset_start, offset_end, strategy, token_count, parent_chunk_index, metadata_json)
        \\VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        \\ON CONFLICT(workspace_id, collection_id, doc_id, chunk_index) DO UPDATE SET
        \\    content = excluded.content,
        \\    language = excluded.language,
        \\    offset_start = excluded.offset_start,
        \\    offset_end = excluded.offset_end,
        \\    strategy = excluded.strategy,
        \\    token_count = excluded.token_count,
        \\    parent_chunk_index = excluded.parent_chunk_index,
        \\    metadata_json = excluded.metadata_json
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.workspace_id);
    try facet_sqlite.bindText(stmt, 2, spec.collection_id);
    try facet_sqlite.bindInt64(stmt, 3, spec.doc_id);
    try facet_sqlite.bindInt64(stmt, 4, spec.chunk_index);
    try facet_sqlite.bindText(stmt, 5, spec.content);
    if (spec.language) |lang| try facet_sqlite.bindText(stmt, 6, lang) else try facet_sqlite.bindNull(stmt, 6);
    if (spec.offset_start) |o| try facet_sqlite.bindInt64(stmt, 7, o) else try facet_sqlite.bindNull(stmt, 7);
    if (spec.offset_end) |o| try facet_sqlite.bindInt64(stmt, 8, o) else try facet_sqlite.bindNull(stmt, 8);
    if (spec.strategy) |s| try facet_sqlite.bindText(stmt, 9, s) else try facet_sqlite.bindNull(stmt, 9);
    if (spec.token_count) |tc| try facet_sqlite.bindInt64(stmt, 10, tc) else try facet_sqlite.bindNull(stmt, 10);
    if (spec.parent_chunk_index) |pi| try facet_sqlite.bindInt64(stmt, 11, pi) else try facet_sqlite.bindNull(stmt, 11);
    try facet_sqlite.bindText(stmt, 12, spec.metadata_json);
    try facet_sqlite.stepDone(stmt);
}

pub fn upsertDocumentVector(db: Database, spec: VectorSpec) !void {
    std.debug.assert(spec.chunk_index == null);
    const sql =
        \\INSERT INTO documents_raw_vector(workspace_id, collection_id, doc_id, dim, embedding_blob)
        \\VALUES(?1, ?2, ?3, ?4, ?5)
        \\ON CONFLICT(workspace_id, collection_id, doc_id) DO UPDATE SET
        \\    dim = excluded.dim,
        \\    embedding_blob = excluded.embedding_blob
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.workspace_id);
    try facet_sqlite.bindText(stmt, 2, spec.collection_id);
    try facet_sqlite.bindInt64(stmt, 3, spec.doc_id);
    try facet_sqlite.bindInt64(stmt, 4, spec.dim);
    try facet_sqlite.bindBlob(stmt, 5, spec.embedding_blob);
    try facet_sqlite.stepDone(stmt);
}

pub fn upsertChunkVector(db: Database, spec: VectorSpec) !void {
    const chunk_index = spec.chunk_index orelse return error.BindFailed;
    const sql =
        \\INSERT INTO chunks_raw_vector(workspace_id, collection_id, doc_id, chunk_index, dim, embedding_blob)
        \\VALUES(?1, ?2, ?3, ?4, ?5, ?6)
        \\ON CONFLICT(workspace_id, collection_id, doc_id, chunk_index) DO UPDATE SET
        \\    dim = excluded.dim,
        \\    embedding_blob = excluded.embedding_blob
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.workspace_id);
    try facet_sqlite.bindText(stmt, 2, spec.collection_id);
    try facet_sqlite.bindInt64(stmt, 3, spec.doc_id);
    try facet_sqlite.bindInt64(stmt, 4, chunk_index);
    try facet_sqlite.bindInt64(stmt, 5, spec.dim);
    try facet_sqlite.bindBlob(stmt, 6, spec.embedding_blob);
    try facet_sqlite.stepDone(stmt);
}

// ---- Facet assignments raw ------------------------------------------------

pub fn upsertFacetAssignmentRaw(db: Database, spec: FacetAssignmentRawSpec) !void {
    const sql =
        \\INSERT INTO facet_assignments_raw(workspace_id, collection_id, target_kind, doc_id, chunk_index, ontology_id, namespace, dimension, value, value_id, weight, source)
        \\VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        \\ON CONFLICT(workspace_id, collection_id, target_kind, doc_id, chunk_index, ontology_id, namespace, dimension, value) DO UPDATE SET
        \\    value_id = excluded.value_id,
        \\    weight = excluded.weight,
        \\    source = excluded.source
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.workspace_id);
    try facet_sqlite.bindText(stmt, 2, spec.collection_id);
    try facet_sqlite.bindText(stmt, 3, spec.target_kind.label());
    try facet_sqlite.bindInt64(stmt, 4, spec.doc_id);
    const chunk_index_value: i64 = if (spec.chunk_index) |ci| @intCast(ci) else -1;
    try facet_sqlite.bindInt64(stmt, 5, chunk_index_value);
    try facet_sqlite.bindText(stmt, 6, spec.ontology_id);
    try facet_sqlite.bindText(stmt, 7, spec.namespace);
    try facet_sqlite.bindText(stmt, 8, spec.dimension);
    try facet_sqlite.bindText(stmt, 9, spec.value);
    if (spec.value_id) |vid| try facet_sqlite.bindInt64(stmt, 10, vid) else try facet_sqlite.bindNull(stmt, 10);
    if (c.sqlite3_bind_double(stmt, 11, spec.weight) != c.SQLITE_OK) return error.BindFailed;
    if (spec.source) |s| try facet_sqlite.bindText(stmt, 12, s) else try facet_sqlite.bindNull(stmt, 12);
    try facet_sqlite.stepDone(stmt);
}

// ---- Entities / relations / linkage --------------------------------------

pub fn upsertEntityRaw(db: Database, spec: EntityRawSpec) !void {
    const sql =
        \\INSERT INTO entities_raw(workspace_id, ontology_id, entity_id, entity_type, name, confidence, metadata_json)
        \\VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
        \\ON CONFLICT(workspace_id, entity_id) DO UPDATE SET
        \\    ontology_id = excluded.ontology_id,
        \\    entity_type = excluded.entity_type,
        \\    name = excluded.name,
        \\    confidence = excluded.confidence,
        \\    metadata_json = excluded.metadata_json
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.workspace_id);
    try facet_sqlite.bindText(stmt, 2, spec.ontology_id);
    try facet_sqlite.bindInt64(stmt, 3, spec.entity_id);
    try facet_sqlite.bindText(stmt, 4, spec.entity_type);
    try facet_sqlite.bindText(stmt, 5, spec.name);
    if (c.sqlite3_bind_double(stmt, 6, spec.confidence) != c.SQLITE_OK) return error.BindFailed;
    try facet_sqlite.bindText(stmt, 7, spec.metadata_json);
    try facet_sqlite.stepDone(stmt);
}

pub fn upsertEntityAliasRaw(db: Database, spec: EntityAliasRawSpec) !void {
    const sql =
        \\INSERT INTO entity_aliases_raw(workspace_id, entity_id, term, confidence)
        \\VALUES(?1, ?2, ?3, ?4)
        \\ON CONFLICT(workspace_id, entity_id, term) DO UPDATE SET confidence = excluded.confidence
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.workspace_id);
    try facet_sqlite.bindInt64(stmt, 2, spec.entity_id);
    try facet_sqlite.bindText(stmt, 3, spec.term);
    if (c.sqlite3_bind_double(stmt, 4, spec.confidence) != c.SQLITE_OK) return error.BindFailed;
    try facet_sqlite.stepDone(stmt);
}

pub fn upsertRelationRaw(db: Database, spec: RelationRawSpec) !void {
    const sql =
        \\INSERT INTO relations_raw(workspace_id, ontology_id, relation_id, edge_type, source_entity_id, target_entity_id, valid_from, valid_to, confidence, metadata_json)
        \\VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        \\ON CONFLICT(workspace_id, relation_id) DO UPDATE SET
        \\    ontology_id = excluded.ontology_id,
        \\    edge_type = excluded.edge_type,
        \\    source_entity_id = excluded.source_entity_id,
        \\    target_entity_id = excluded.target_entity_id,
        \\    valid_from = excluded.valid_from,
        \\    valid_to = excluded.valid_to,
        \\    confidence = excluded.confidence,
        \\    metadata_json = excluded.metadata_json
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.workspace_id);
    try facet_sqlite.bindText(stmt, 2, spec.ontology_id);
    try facet_sqlite.bindInt64(stmt, 3, spec.relation_id);
    try facet_sqlite.bindText(stmt, 4, spec.edge_type);
    try facet_sqlite.bindInt64(stmt, 5, spec.source_entity_id);
    try facet_sqlite.bindInt64(stmt, 6, spec.target_entity_id);
    if (spec.valid_from) |v| try facet_sqlite.bindText(stmt, 7, v) else try facet_sqlite.bindNull(stmt, 7);
    if (spec.valid_to) |v| try facet_sqlite.bindText(stmt, 8, v) else try facet_sqlite.bindNull(stmt, 8);
    if (c.sqlite3_bind_double(stmt, 9, spec.confidence) != c.SQLITE_OK) return error.BindFailed;
    try facet_sqlite.bindText(stmt, 10, spec.metadata_json);
    try facet_sqlite.stepDone(stmt);
}

pub fn linkEntityDocumentRaw(db: Database, spec: EntityDocumentRawSpec) !void {
    const sql =
        \\INSERT INTO entity_documents_raw(workspace_id, entity_id, collection_id, doc_id, role, confidence)
        \\VALUES(?1, ?2, ?3, ?4, ?5, ?6)
        \\ON CONFLICT(workspace_id, entity_id, collection_id, doc_id) DO UPDATE SET
        \\    role = excluded.role,
        \\    confidence = excluded.confidence
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.workspace_id);
    try facet_sqlite.bindInt64(stmt, 2, spec.entity_id);
    try facet_sqlite.bindText(stmt, 3, spec.collection_id);
    try facet_sqlite.bindInt64(stmt, 4, spec.doc_id);
    if (spec.role) |r| try facet_sqlite.bindText(stmt, 5, r) else try facet_sqlite.bindNull(stmt, 5);
    if (c.sqlite3_bind_double(stmt, 6, spec.confidence) != c.SQLITE_OK) return error.BindFailed;
    try facet_sqlite.stepDone(stmt);
}

pub fn linkEntityChunkRaw(db: Database, spec: EntityChunkRawSpec) !void {
    const sql =
        \\INSERT INTO entity_chunks_raw(workspace_id, entity_id, collection_id, doc_id, chunk_index, role, confidence)
        \\VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
        \\ON CONFLICT(workspace_id, entity_id, collection_id, doc_id, chunk_index) DO UPDATE SET
        \\    role = excluded.role,
        \\    confidence = excluded.confidence
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.workspace_id);
    try facet_sqlite.bindInt64(stmt, 2, spec.entity_id);
    try facet_sqlite.bindText(stmt, 3, spec.collection_id);
    try facet_sqlite.bindInt64(stmt, 4, spec.doc_id);
    try facet_sqlite.bindInt64(stmt, 5, spec.chunk_index);
    if (spec.role) |r| try facet_sqlite.bindText(stmt, 6, r) else try facet_sqlite.bindNull(stmt, 6);
    if (c.sqlite3_bind_double(stmt, 7, spec.confidence) != c.SQLITE_OK) return error.BindFailed;
    try facet_sqlite.stepDone(stmt);
}

pub fn upsertDocumentLinkRaw(db: Database, spec: DocumentLinkRawSpec) !void {
    const sql =
        \\INSERT INTO document_links_raw(workspace_id, link_id, ontology_id, edge_type, source_collection_id, source_doc_id, source_chunk_index, target_collection_id, target_doc_id, target_chunk_index, weight, metadata_json, source)
        \\VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
        \\ON CONFLICT(workspace_id, link_id) DO UPDATE SET
        \\    ontology_id = excluded.ontology_id,
        \\    edge_type = excluded.edge_type,
        \\    source_collection_id = excluded.source_collection_id,
        \\    source_doc_id = excluded.source_doc_id,
        \\    source_chunk_index = excluded.source_chunk_index,
        \\    target_collection_id = excluded.target_collection_id,
        \\    target_doc_id = excluded.target_doc_id,
        \\    target_chunk_index = excluded.target_chunk_index,
        \\    weight = excluded.weight,
        \\    metadata_json = excluded.metadata_json,
        \\    source = excluded.source
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.workspace_id);
    try facet_sqlite.bindInt64(stmt, 2, spec.link_id);
    try facet_sqlite.bindText(stmt, 3, spec.ontology_id);
    try facet_sqlite.bindText(stmt, 4, spec.edge_type);
    try facet_sqlite.bindText(stmt, 5, spec.source_collection_id);
    try facet_sqlite.bindInt64(stmt, 6, spec.source_doc_id);
    if (spec.source_chunk_index) |ci| try facet_sqlite.bindInt64(stmt, 7, ci) else try facet_sqlite.bindNull(stmt, 7);
    try facet_sqlite.bindText(stmt, 8, spec.target_collection_id);
    try facet_sqlite.bindInt64(stmt, 9, spec.target_doc_id);
    if (spec.target_chunk_index) |ci| try facet_sqlite.bindInt64(stmt, 10, ci) else try facet_sqlite.bindNull(stmt, 10);
    if (c.sqlite3_bind_double(stmt, 11, spec.weight) != c.SQLITE_OK) return error.BindFailed;
    try facet_sqlite.bindText(stmt, 12, spec.metadata_json);
    if (spec.source) |s| try facet_sqlite.bindText(stmt, 13, s) else try facet_sqlite.bindNull(stmt, 13);
    try facet_sqlite.stepDone(stmt);
}

pub fn upsertExternalLinkRaw(db: Database, spec: ExternalLinkRawSpec) !void {
    const sql =
        \\INSERT INTO external_links_raw(workspace_id, link_id, source_collection_id, source_doc_id, source_chunk_index, target_uri, edge_type, weight, metadata_json)
        \\VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        \\ON CONFLICT(workspace_id, link_id) DO UPDATE SET
        \\    source_collection_id = excluded.source_collection_id,
        \\    source_doc_id = excluded.source_doc_id,
        \\    source_chunk_index = excluded.source_chunk_index,
        \\    target_uri = excluded.target_uri,
        \\    edge_type = excluded.edge_type,
        \\    weight = excluded.weight,
        \\    metadata_json = excluded.metadata_json
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, spec.workspace_id);
    try facet_sqlite.bindInt64(stmt, 2, spec.link_id);
    try facet_sqlite.bindText(stmt, 3, spec.source_collection_id);
    try facet_sqlite.bindInt64(stmt, 4, spec.source_doc_id);
    if (spec.source_chunk_index) |ci| try facet_sqlite.bindInt64(stmt, 5, ci) else try facet_sqlite.bindNull(stmt, 5);
    try facet_sqlite.bindText(stmt, 6, spec.target_uri);
    try facet_sqlite.bindText(stmt, 7, spec.edge_type);
    if (c.sqlite3_bind_double(stmt, 8, spec.weight) != c.SQLITE_OK) return error.BindFailed;
    try facet_sqlite.bindText(stmt, 9, spec.metadata_json);
    try facet_sqlite.stepDone(stmt);
}

// ---- Read helpers (for backup/migrate/CLI) --------------------------------

pub fn workspaceExists(db: Database, workspace_id: []const u8) !bool {
    const sql = "SELECT 1 FROM workspaces WHERE workspace_id = ?1";
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    return switch (c.sqlite3_step(stmt)) {
        c.SQLITE_ROW => true,
        c.SQLITE_DONE => false,
        else => error.StepFailed,
    };
}

pub fn collectionExists(db: Database, collection_id: []const u8) !bool {
    const sql = "SELECT 1 FROM collections WHERE collection_id = ?1";
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, collection_id);
    return switch (c.sqlite3_step(stmt)) {
        c.SQLITE_ROW => true,
        c.SQLITE_DONE => false,
        else => error.StepFailed,
    };
}

pub fn ontologyExists(db: Database, ontology_id: []const u8) !bool {
    const sql = "SELECT 1 FROM ontologies WHERE ontology_id = ?1";
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, ontology_id);
    return switch (c.sqlite3_step(stmt)) {
        c.SQLITE_ROW => true,
        c.SQLITE_DONE => false,
        else => error.StepFailed,
    };
}

pub const DocLookup = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    doc_id: u64,

    pub fn deinit(self: DocLookup, allocator: std.mem.Allocator) void {
        allocator.free(self.workspace_id);
        allocator.free(self.collection_id);
    }
};

/// Resolves a public `doc_nanoid` back to the (workspace, collection, doc_id)
/// triple used for internal joins. Returns `null` when no row matches; the
/// caller owns the duplicated strings on the returned struct.
pub fn lookupDocByNanoid(
    db: Database,
    allocator: std.mem.Allocator,
    nanoid_value: []const u8,
) !?DocLookup {
    if (nanoid_value.len == 0) return null;
    const sql =
        \\SELECT workspace_id, collection_id, doc_id
        \\FROM documents_raw
        \\WHERE doc_nanoid = ?1
        \\LIMIT 1
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, nanoid_value);
    const status = c.sqlite3_step(stmt);
    if (status == c.SQLITE_DONE) return null;
    if (status != c.SQLITE_ROW) return error.StepFailed;

    const ws = try facet_sqlite.dupeColumnText(allocator, stmt, 0);
    errdefer allocator.free(ws);
    const cid = try facet_sqlite.dupeColumnText(allocator, stmt, 1);
    errdefer allocator.free(cid);
    const did: i64 = c.sqlite3_column_int64(stmt, 2);
    return .{
        .workspace_id = ws,
        .collection_id = cid,
        .doc_id = @intCast(did),
    };
}

/// Built-in namespace used by the chunker / pipeline to publish
/// auto-extracted, source-derived facets (file path components, ingest
/// timestamps, chunk strategy, …). Always provisioned on the per-workspace
/// default ontology so callers can attach `source.*` facets without first
/// having to declare a custom ontology.
pub const source_namespace: []const u8 = "source";

pub const SourceDimension = struct {
    name: []const u8,
    hierarchy_kind: []const u8 = "flat",
    is_multi: bool = false,
};

pub const source_dimensions = [_]SourceDimension{
    .{ .name = "path" },
    .{ .name = "dir", .hierarchy_kind = "tree" },
    .{ .name = "filename" },
    .{ .name = "extension" },
    .{ .name = "ingested_at" },
    .{ .name = "chunk_index" },
    .{ .name = "chunk_count" },
    .{ .name = "strategy" },
};

/// Provisions the canonical `source.*` namespace + dimensions on `ontology_id`.
/// Idempotent; safe to call from every ingest path.
pub fn ensureSourceNamespace(db: Database, ontology_id: []const u8) !void {
    try ensureNamespace(db, .{
        .ontology_id = ontology_id,
        .namespace = source_namespace,
        .label = "Source-derived facets",
    });
    for (source_dimensions) |dim| {
        try ensureDimension(db, .{
            .ontology_id = ontology_id,
            .namespace = source_namespace,
            .dimension = dim.name,
            .value_type = "text",
            .is_multi = dim.is_multi,
            .hierarchy_kind = dim.hierarchy_kind,
        });
    }
}

/// Builds the canonical default ontology id for a workspace
/// (`<workspace_id>::default`) into `out_buf`. Returns the slice of `out_buf`
/// holding the id.
pub fn formatDefaultOntologyId(out_buf: []u8, workspace_id: []const u8) ![]const u8 {
    return std.fmt.bufPrint(out_buf, "{s}::default", .{workspace_id}) catch error.ValueOutOfRange;
}

/// Ensures the per-workspace default ontology exists, wires it as the
/// workspace default, and bootstraps the built-in `source.*` namespace.
/// The returned slice is owned by `allocator`; the caller frees it.
pub fn ensureDefaultOntology(
    db: Database,
    allocator: std.mem.Allocator,
    workspace_id: []const u8,
) ![]const u8 {
    var stack_buf: [256]u8 = undefined;
    const default_id_slice = try formatDefaultOntologyId(&stack_buf, workspace_id);

    try ensureOntology(db, .{
        .ontology_id = default_id_slice,
        .workspace_id = workspace_id,
        .name = "default",
        .source_kind = "auto",
    });
    try setDefaultOntology(db, workspace_id, default_id_slice);
    try ensureSourceNamespace(db, default_id_slice);

    return allocator.dupe(u8, default_id_slice);
}

// ---- Tests ---------------------------------------------------------------

test "ensureWorkspace bootstraps the default ontology with the source.* namespace" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try ensureWorkspace(db, .{ .workspace_id = "wsX" });
    try ensureWorkspace(db, .{ .workspace_id = "wsX" });

    const default_id = (try defaultOntology(db, std.testing.allocator, "wsX")) orelse
        return error.MissingDefaultOntology;
    defer std.testing.allocator.free(default_id);
    try std.testing.expectEqualStrings("wsX::default", default_id);

    const sql_dims =
        \\SELECT COUNT(*) FROM ontology_dimensions
        \\WHERE ontology_id = ?1 AND namespace = 'source'
    ;
    const stmt_dims = try facet_sqlite.prepare(db, sql_dims);
    defer facet_sqlite.finalize(stmt_dims);
    try facet_sqlite.bindText(stmt_dims, 1, default_id);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt_dims));
    try std.testing.expectEqual(@as(i64, source_dimensions.len), c.sqlite3_column_int64(stmt_dims, 0));
}

test "ensureDefaultOntology is idempotent and returns an owned id" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try ensureWorkspace(db, .{ .workspace_id = "wsY" });
    const first = try ensureDefaultOntology(db, std.testing.allocator, "wsY");
    defer std.testing.allocator.free(first);
    try std.testing.expectEqualStrings("wsY::default", first);

    const second = try ensureDefaultOntology(db, std.testing.allocator, "wsY");
    defer std.testing.allocator.free(second);
    try std.testing.expectEqualStrings(first, second);

    try ensureSourceNamespace(db, first);
}

test "ensureWorkspace and ensureCollection are idempotent" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try ensureWorkspace(db, .{ .workspace_id = "ws1", .label = "Workspace one" });
    try ensureWorkspace(db, .{ .workspace_id = "ws1", .description = "updated" });
    try std.testing.expect(try workspaceExists(db, "ws1"));

    try ensureCollection(db, .{
        .workspace_id = "ws1",
        .collection_id = "ws1::legal",
        .name = "legal",
        .chunk_bits = 6,
    });
    try ensureCollection(db, .{
        .workspace_id = "ws1",
        .collection_id = "ws1::legal",
        .name = "legal",
        .chunk_bits = 8,
    });
    try std.testing.expect(try collectionExists(db, "ws1::legal"));
}

test "loadOntologyBundle persists namespaces, dimensions, values, types" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try ensureWorkspace(db, .{ .workspace_id = "ws1" });

    try loadOntologyBundle(db, .{
        .header = .{
            .ontology_id = "ws1::core",
            .workspace_id = "ws1",
            .name = "core",
        },
        .namespaces = &.{
            .{ .ontology_id = "ws1::core", .namespace = "topic" },
        },
        .dimensions = &.{
            .{ .ontology_id = "ws1::core", .namespace = "topic", .dimension = "category", .hierarchy_kind = "tree" },
        },
        .values = &.{
            .{ .ontology_id = "ws1::core", .namespace = "topic", .dimension = "category", .value_id = 1, .value = "science" },
            .{ .ontology_id = "ws1::core", .namespace = "topic", .dimension = "category", .value_id = 2, .value = "physics", .parent_value_id = 1 },
        },
        .entity_types = &.{
            .{ .ontology_id = "ws1::core", .entity_type = "person" },
            .{ .ontology_id = "ws1::core", .entity_type = "company" },
        },
        .edge_types = &.{
            .{ .ontology_id = "ws1::core", .edge_type = "works_for", .source_entity_type = "person", .target_entity_type = "company" },
        },
        .entities = &.{
            .{ .ontology_id = "ws1::core", .entity_id = 1, .entity_type = "person", .name = "Ada" },
        },
        .relations = &.{},
    });

    try std.testing.expect(try ontologyExists(db, "ws1::core"));

    const sql = "SELECT COUNT(*) FROM ontology_values WHERE ontology_id = 'ws1::core'";
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt));
    try std.testing.expectEqual(@as(i64, 2), c.sqlite3_column_int64(stmt, 0));
}

test "raw document, chunk, facet assignment, and cross-collection link round-trip" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try ensureWorkspace(db, .{ .workspace_id = "ws1" });
    try ensureCollection(db, .{ .workspace_id = "ws1", .collection_id = "ws1::legal", .name = "legal" });
    try ensureCollection(db, .{ .workspace_id = "ws1", .collection_id = "ws1::tech", .name = "tech" });
    try ensureOntology(db, .{ .ontology_id = "ws1::core", .workspace_id = "ws1", .name = "core" });
    try attachOntologyToCollection(db, "ws1", "ws1::legal", "ws1::core", "primary");
    try attachOntologyToCollection(db, "ws1", "ws1::tech", "ws1::core", "primary");

    try upsertDocumentRaw(db, .{
        .workspace_id = "ws1",
        .collection_id = "ws1::legal",
        .doc_id = 100,
        .doc_nanoid = "legal-nano-001",
        .content = "Contract about graph systems",
    });
    try upsertChunkRaw(db, .{
        .workspace_id = "ws1",
        .collection_id = "ws1::legal",
        .doc_id = 100,
        .chunk_index = 0,
        .content = "graph systems",
        .strategy = "fixed_token",
        .token_count = 2,
    });

    try upsertDocumentRaw(db, .{
        .workspace_id = "ws1",
        .collection_id = "ws1::tech",
        .doc_id = 200,
        .doc_nanoid = "tech-nano-200",
        .content = "RFC describing graph indexes",
    });

    try upsertExternalLinkRaw(db, .{
        .workspace_id = "ws1",
        .link_id = 7,
        .source_collection_id = "ws1::legal",
        .source_doc_id = 100,
        .target_uri = "https://example.com/spec",
    });

    try ensureNamespace(db, .{ .ontology_id = "ws1::core", .namespace = "topic" });
    try ensureDimension(db, .{ .ontology_id = "ws1::core", .namespace = "topic", .dimension = "category" });
    try upsertFacetAssignmentRaw(db, .{
        .workspace_id = "ws1",
        .collection_id = "ws1::legal",
        .target_kind = .doc,
        .doc_id = 100,
        .ontology_id = "ws1::core",
        .namespace = "topic",
        .dimension = "category",
        .value = "graph",
    });
    try upsertFacetAssignmentRaw(db, .{
        .workspace_id = "ws1",
        .collection_id = "ws1::legal",
        .target_kind = .chunk,
        .doc_id = 100,
        .chunk_index = 0,
        .ontology_id = "ws1::core",
        .namespace = "topic",
        .dimension = "category",
        .value = "graph-chunk",
    });

    try upsertEntityRaw(db, .{
        .workspace_id = "ws1",
        .ontology_id = "ws1::core",
        .entity_id = 1,
        .entity_type = "concept",
        .name = "graph systems",
    });
    try upsertEntityAliasRaw(db, .{
        .workspace_id = "ws1",
        .entity_id = 1,
        .term = "graphs",
    });
    try linkEntityDocumentRaw(db, .{
        .workspace_id = "ws1",
        .entity_id = 1,
        .collection_id = "ws1::legal",
        .doc_id = 100,
    });
    try linkEntityDocumentRaw(db, .{
        .workspace_id = "ws1",
        .entity_id = 1,
        .collection_id = "ws1::tech",
        .doc_id = 200,
    });

    try upsertDocumentLinkRaw(db, .{
        .workspace_id = "ws1",
        .link_id = 1,
        .ontology_id = "ws1::core",
        .edge_type = "explains",
        .source_collection_id = "ws1::tech",
        .source_doc_id = 200,
        .target_collection_id = "ws1::legal",
        .target_doc_id = 100,
    });

    const sql_facets =
        \\SELECT COUNT(*) FROM facet_assignments_raw
        \\WHERE workspace_id = 'ws1' AND collection_id = 'ws1::legal'
    ;
    const stmt_facets = try facet_sqlite.prepare(db, sql_facets);
    defer facet_sqlite.finalize(stmt_facets);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt_facets));
    try std.testing.expectEqual(@as(i64, 2), c.sqlite3_column_int64(stmt_facets, 0));

    const sql_entities =
        \\SELECT COUNT(*) FROM entity_documents_raw
        \\WHERE workspace_id = 'ws1' AND entity_id = 1
    ;
    const stmt_entities = try facet_sqlite.prepare(db, sql_entities);
    defer facet_sqlite.finalize(stmt_entities);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt_entities));
    try std.testing.expectEqual(@as(i64, 2), c.sqlite3_column_int64(stmt_entities, 0));

    const sql_links =
        \\SELECT COUNT(*) FROM document_links_raw
        \\WHERE workspace_id = 'ws1' AND source_collection_id = 'ws1::tech'
    ;
    const stmt_links = try facet_sqlite.prepare(db, sql_links);
    defer facet_sqlite.finalize(stmt_links);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt_links));
    try std.testing.expectEqual(@as(i64, 1), c.sqlite3_column_int64(stmt_links, 0));

    const sql_external =
        \\SELECT target_uri, edge_type FROM external_links_raw
        \\WHERE workspace_id = 'ws1' AND link_id = 7
    ;
    const stmt_external = try facet_sqlite.prepare(db, sql_external);
    defer facet_sqlite.finalize(stmt_external);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt_external));
    const target_uri = std.mem.span(@as([*c]const u8, c.sqlite3_column_text(stmt_external, 0)));
    try std.testing.expectEqualStrings("https://example.com/spec", target_uri);
    const edge_type = std.mem.span(@as([*c]const u8, c.sqlite3_column_text(stmt_external, 1)));
    try std.testing.expectEqualStrings("reference", edge_type);

    const sql_chunk =
        \\SELECT strategy, token_count FROM chunks_raw
        \\WHERE workspace_id = 'ws1' AND collection_id = 'ws1::legal' AND doc_id = 100 AND chunk_index = 0
    ;
    const stmt_chunk = try facet_sqlite.prepare(db, sql_chunk);
    defer facet_sqlite.finalize(stmt_chunk);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt_chunk));
    const strategy_value = std.mem.span(@as([*c]const u8, c.sqlite3_column_text(stmt_chunk, 0)));
    try std.testing.expectEqualStrings("fixed_token", strategy_value);
    try std.testing.expectEqual(@as(i64, 2), c.sqlite3_column_int64(stmt_chunk, 1));

    const lookup = (try lookupDocByNanoid(db, std.testing.allocator, "tech-nano-200")) orelse return error.MissingNanoid;
    defer lookup.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("ws1", lookup.workspace_id);
    try std.testing.expectEqualStrings("ws1::tech", lookup.collection_id);
    try std.testing.expectEqual(@as(u64, 200), lookup.doc_id);

    try std.testing.expect((try lookupDocByNanoid(db, std.testing.allocator, "missing")) == null);
}

test "documents_raw enforces nanoid uniqueness only for non-empty values" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try ensureWorkspace(db, .{ .workspace_id = "ws1" });
    try ensureCollection(db, .{ .workspace_id = "ws1", .collection_id = "ws1::a", .name = "a" });
    try ensureCollection(db, .{ .workspace_id = "ws1", .collection_id = "ws1::b", .name = "b" });

    try upsertDocumentRaw(db, .{
        .workspace_id = "ws1",
        .collection_id = "ws1::a",
        .doc_id = 1,
        .doc_nanoid = "shared-nano",
    });
    try std.testing.expectError(
        error.StepFailed,
        upsertDocumentRaw(db, .{
            .workspace_id = "ws1",
            .collection_id = "ws1::b",
            .doc_id = 2,
            .doc_nanoid = "shared-nano",
        }),
    );

    try upsertDocumentRaw(db, .{ .workspace_id = "ws1", .collection_id = "ws1::a", .doc_id = 10 });
    try upsertDocumentRaw(db, .{ .workspace_id = "ws1", .collection_id = "ws1::b", .doc_id = 11 });

    try upsertDocumentRaw(db, .{
        .workspace_id = "ws1",
        .collection_id = "ws1::a",
        .doc_id = 1,
        .content = "updated",
    });
    const lookup = (try lookupDocByNanoid(db, std.testing.allocator, "shared-nano")) orelse return error.MissingNanoid;
    defer lookup.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 1), lookup.doc_id);
}
