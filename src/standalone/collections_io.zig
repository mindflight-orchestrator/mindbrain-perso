//! Backup/restore helpers for the workspace -> collection -> document/chunk
//! raw layer defined in collections_sqlite.zig. Exports produce a self-
//! contained JSON bundle; imports replay it through the canonical raw
//! upsert helpers so derived indexes can be rebuilt with reindexAll.

const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const collections_sqlite = @import("collections_sqlite.zig");

const Allocator = std.mem.Allocator;
const Database = facet_sqlite.Database;
const c = facet_sqlite.c;

pub const Scope = union(enum) {
    workspace: []const u8,
    collection: struct { workspace_id: []const u8, collection_id: []const u8 },
};

const json_writer_capacity: usize = 8 * 1024;

const Bundle = struct {
    schema_version: []const u8 = "1",
    scope: ScopeJson,
    workspaces: []WorkspaceRow,
    collections: []CollectionRow,
    ontologies: []OntologyRow,
    collection_ontologies: []CollectionOntologyRow,
    workspace_settings: []WorkspaceSettingsRow,
    documents_raw: []DocumentRow,
    chunks_raw: []ChunkRow,
    facet_assignments_raw: []FacetAssignmentRow,
    entities_raw: []EntityRow,
    entity_aliases_raw: []EntityAliasRow,
    relations_raw: []RelationRow,
    entity_documents_raw: []EntityDocumentRow,
    entity_chunks_raw: []EntityChunkRow,
    document_links_raw: []DocumentLinkRow,
    external_links_raw: []ExternalLinkRow = &.{},
};

const ScopeJson = struct {
    kind: []const u8,
    workspace_id: []const u8,
    collection_id: ?[]const u8,
};

const WorkspaceRow = struct {
    workspace_id: []const u8,
    label: ?[]const u8,
    description: ?[]const u8,
    domain_profile: ?[]const u8,
};

const CollectionRow = struct {
    collection_id: []const u8,
    workspace_id: []const u8,
    name: []const u8,
    key_kind: []const u8,
    chunk_bits: i64,
    default_language: []const u8,
    metadata_json: []const u8,
};

const OntologyRow = struct {
    ontology_id: []const u8,
    workspace_id: ?[]const u8,
    name: []const u8,
    version: []const u8,
    frozen: bool,
    source_kind: []const u8,
    metadata_json: []const u8,
};

const CollectionOntologyRow = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    ontology_id: []const u8,
    role: []const u8,
};

const WorkspaceSettingsRow = struct {
    workspace_id: []const u8,
    default_ontology_id: ?[]const u8,
};

const DocumentRow = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    doc_id: i64,
    doc_nanoid: []const u8 = "",
    content: []const u8,
    language: ?[]const u8,
    source_ref: ?[]const u8,
    summary: ?[]const u8 = null,
    metadata_json: []const u8,
};

const ChunkRow = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    doc_id: i64,
    chunk_index: i64,
    content: []const u8,
    language: ?[]const u8,
    offset_start: ?i64,
    offset_end: ?i64,
    strategy: ?[]const u8 = null,
    token_count: ?i64 = null,
    parent_chunk_index: ?i64 = null,
    metadata_json: []const u8,
};

const FacetAssignmentRow = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    target_kind: []const u8,
    doc_id: i64,
    chunk_index: i64,
    ontology_id: []const u8,
    namespace: []const u8,
    dimension: []const u8,
    value: []const u8,
    value_id: ?i64,
    weight: f64,
    source: ?[]const u8,
};

const EntityRow = struct {
    workspace_id: []const u8,
    ontology_id: []const u8,
    entity_id: i64,
    entity_type: []const u8,
    name: []const u8,
    confidence: f64,
    metadata_json: []const u8,
};

const EntityAliasRow = struct {
    workspace_id: []const u8,
    entity_id: i64,
    term: []const u8,
    confidence: f64,
};

const RelationRow = struct {
    workspace_id: []const u8,
    ontology_id: []const u8,
    relation_id: i64,
    edge_type: []const u8,
    source_entity_id: i64,
    target_entity_id: i64,
    valid_from: ?[]const u8,
    valid_to: ?[]const u8,
    confidence: f64,
    metadata_json: []const u8,
};

const EntityDocumentRow = struct {
    workspace_id: []const u8,
    entity_id: i64,
    collection_id: []const u8,
    doc_id: i64,
    role: ?[]const u8,
    confidence: f64,
};

const EntityChunkRow = struct {
    workspace_id: []const u8,
    entity_id: i64,
    collection_id: []const u8,
    doc_id: i64,
    chunk_index: i64,
    role: ?[]const u8,
    confidence: f64,
};

const ExternalLinkRow = struct {
    workspace_id: []const u8,
    link_id: i64,
    source_collection_id: []const u8,
    source_doc_id: i64,
    source_chunk_index: ?i64,
    target_uri: []const u8,
    edge_type: []const u8,
    weight: f64,
    metadata_json: []const u8,
};

const DocumentLinkRow = struct {
    workspace_id: []const u8,
    link_id: i64,
    ontology_id: []const u8,
    edge_type: []const u8,
    source_collection_id: []const u8,
    source_doc_id: i64,
    source_chunk_index: ?i64,
    target_collection_id: []const u8,
    target_doc_id: i64,
    target_chunk_index: ?i64,
    weight: f64,
    metadata_json: []const u8,
    source: ?[]const u8,
};

// ---- Export ---------------------------------------------------------------

pub fn exportToJson(allocator: Allocator, db: Database, scope: Scope) ![]const u8 {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const workspace_id = switch (scope) {
        .workspace => |ws| ws,
        .collection => |coll| coll.workspace_id,
    };
    const collection_filter: ?[]const u8 = switch (scope) {
        .workspace => null,
        .collection => |coll| coll.collection_id,
    };

    const bundle = Bundle{
        .scope = .{
            .kind = if (collection_filter == null) "workspace" else "collection",
            .workspace_id = workspace_id,
            .collection_id = collection_filter,
        },
        .workspaces = try selectWorkspaces(arena_allocator, db, workspace_id),
        .collections = try selectCollections(arena_allocator, db, workspace_id, collection_filter),
        .ontologies = try selectOntologies(arena_allocator, db, workspace_id, collection_filter),
        .collection_ontologies = try selectCollectionOntologies(arena_allocator, db, workspace_id, collection_filter),
        .workspace_settings = try selectWorkspaceSettings(arena_allocator, db, workspace_id),
        .documents_raw = try selectDocuments(arena_allocator, db, workspace_id, collection_filter),
        .chunks_raw = try selectChunks(arena_allocator, db, workspace_id, collection_filter),
        .facet_assignments_raw = try selectFacetAssignments(arena_allocator, db, workspace_id, collection_filter),
        .entities_raw = try selectEntities(arena_allocator, db, workspace_id),
        .entity_aliases_raw = try selectEntityAliases(arena_allocator, db, workspace_id),
        .relations_raw = try selectRelations(arena_allocator, db, workspace_id),
        .entity_documents_raw = try selectEntityDocuments(arena_allocator, db, workspace_id, collection_filter),
        .entity_chunks_raw = try selectEntityChunks(arena_allocator, db, workspace_id, collection_filter),
        .document_links_raw = try selectDocumentLinks(arena_allocator, db, workspace_id, collection_filter),
        .external_links_raw = try selectExternalLinks(arena_allocator, db, workspace_id, collection_filter),
    };

    return try std.json.Stringify.valueAlloc(allocator, bundle, .{ .whitespace = .indent_2 });
}

// ---- Import ---------------------------------------------------------------

pub fn importBundleJson(db: Database, allocator: Allocator, json_bytes: []const u8) !void {
    const parsed = try std.json.parseFromSlice(Bundle, allocator, json_bytes, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
    defer parsed.deinit();
    const bundle = parsed.value;

    try db.exec("BEGIN");
    errdefer db.exec("ROLLBACK") catch {};

    for (bundle.workspaces) |row| {
        try collections_sqlite.ensureWorkspace(db, .{
            .workspace_id = row.workspace_id,
            .label = row.label,
            .description = row.description,
            .domain_profile = row.domain_profile,
        });
    }

    for (bundle.workspace_settings) |row| {
        if (row.default_ontology_id) |oid| {
            try collections_sqlite.setDefaultOntology(db, row.workspace_id, oid);
        }
    }

    for (bundle.collections) |row| {
        try collections_sqlite.ensureCollection(db, .{
            .workspace_id = row.workspace_id,
            .collection_id = row.collection_id,
            .name = row.name,
            .key_kind = row.key_kind,
            .chunk_bits = std.math.cast(u8, row.chunk_bits) orelse return error.ValueOutOfRange,
            .default_language = row.default_language,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.ontologies) |row| {
        try collections_sqlite.ensureOntology(db, .{
            .ontology_id = row.ontology_id,
            .workspace_id = row.workspace_id,
            .name = row.name,
            .version = row.version,
            .frozen = row.frozen,
            .source_kind = row.source_kind,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.collection_ontologies) |row| {
        try collections_sqlite.attachOntologyToCollection(db, row.workspace_id, row.collection_id, row.ontology_id, row.role);
    }

    for (bundle.documents_raw) |row| {
        try collections_sqlite.upsertDocumentRaw(db, .{
            .workspace_id = row.workspace_id,
            .collection_id = row.collection_id,
            .doc_id = @intCast(row.doc_id),
            .doc_nanoid = row.doc_nanoid,
            .content = row.content,
            .language = row.language,
            .source_ref = row.source_ref,
            .summary = row.summary,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.chunks_raw) |row| {
        try collections_sqlite.upsertChunkRaw(db, .{
            .workspace_id = row.workspace_id,
            .collection_id = row.collection_id,
            .doc_id = @intCast(row.doc_id),
            .chunk_index = std.math.cast(u32, row.chunk_index) orelse return error.ValueOutOfRange,
            .content = row.content,
            .language = row.language,
            .offset_start = if (row.offset_start) |o| @intCast(o) else null,
            .offset_end = if (row.offset_end) |o| @intCast(o) else null,
            .strategy = row.strategy,
            .token_count = if (row.token_count) |tc| @intCast(tc) else null,
            .parent_chunk_index = if (row.parent_chunk_index) |pi| std.math.cast(u32, pi) orelse return error.ValueOutOfRange else null,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.facet_assignments_raw) |row| {
        const target_kind: collections_sqlite.TargetKind = if (std.mem.eql(u8, row.target_kind, "doc")) .doc else .chunk;
        const chunk_index_opt: ?u32 = if (row.chunk_index < 0)
            null
        else
            (std.math.cast(u32, row.chunk_index) orelse return error.ValueOutOfRange);
        try collections_sqlite.upsertFacetAssignmentRaw(db, .{
            .workspace_id = row.workspace_id,
            .collection_id = row.collection_id,
            .target_kind = target_kind,
            .doc_id = @intCast(row.doc_id),
            .chunk_index = chunk_index_opt,
            .ontology_id = row.ontology_id,
            .namespace = row.namespace,
            .dimension = row.dimension,
            .value = row.value,
            .value_id = if (row.value_id) |v| std.math.cast(u32, v) orelse return error.ValueOutOfRange else null,
            .weight = row.weight,
            .source = row.source,
        });
    }

    for (bundle.entities_raw) |row| {
        try collections_sqlite.upsertEntityRaw(db, .{
            .workspace_id = row.workspace_id,
            .ontology_id = row.ontology_id,
            .entity_id = @intCast(row.entity_id),
            .entity_type = row.entity_type,
            .name = row.name,
            .confidence = row.confidence,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.entity_aliases_raw) |row| {
        try collections_sqlite.upsertEntityAliasRaw(db, .{
            .workspace_id = row.workspace_id,
            .entity_id = @intCast(row.entity_id),
            .term = row.term,
            .confidence = row.confidence,
        });
    }

    for (bundle.relations_raw) |row| {
        try collections_sqlite.upsertRelationRaw(db, .{
            .workspace_id = row.workspace_id,
            .ontology_id = row.ontology_id,
            .relation_id = @intCast(row.relation_id),
            .edge_type = row.edge_type,
            .source_entity_id = @intCast(row.source_entity_id),
            .target_entity_id = @intCast(row.target_entity_id),
            .valid_from = row.valid_from,
            .valid_to = row.valid_to,
            .confidence = row.confidence,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.entity_documents_raw) |row| {
        try collections_sqlite.linkEntityDocumentRaw(db, .{
            .workspace_id = row.workspace_id,
            .entity_id = @intCast(row.entity_id),
            .collection_id = row.collection_id,
            .doc_id = @intCast(row.doc_id),
            .role = row.role,
            .confidence = row.confidence,
        });
    }

    for (bundle.entity_chunks_raw) |row| {
        try collections_sqlite.linkEntityChunkRaw(db, .{
            .workspace_id = row.workspace_id,
            .entity_id = @intCast(row.entity_id),
            .collection_id = row.collection_id,
            .doc_id = @intCast(row.doc_id),
            .chunk_index = std.math.cast(u32, row.chunk_index) orelse return error.ValueOutOfRange,
            .role = row.role,
            .confidence = row.confidence,
        });
    }

    for (bundle.document_links_raw) |row| {
        try collections_sqlite.upsertDocumentLinkRaw(db, .{
            .workspace_id = row.workspace_id,
            .link_id = @intCast(row.link_id),
            .ontology_id = row.ontology_id,
            .edge_type = row.edge_type,
            .source_collection_id = row.source_collection_id,
            .source_doc_id = @intCast(row.source_doc_id),
            .source_chunk_index = if (row.source_chunk_index) |v| std.math.cast(u32, v) orelse return error.ValueOutOfRange else null,
            .target_collection_id = row.target_collection_id,
            .target_doc_id = @intCast(row.target_doc_id),
            .target_chunk_index = if (row.target_chunk_index) |v| std.math.cast(u32, v) orelse return error.ValueOutOfRange else null,
            .weight = row.weight,
            .source = row.source,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.external_links_raw) |row| {
        try collections_sqlite.upsertExternalLinkRaw(db, .{
            .workspace_id = row.workspace_id,
            .link_id = @intCast(row.link_id),
            .source_collection_id = row.source_collection_id,
            .source_doc_id = @intCast(row.source_doc_id),
            .source_chunk_index = if (row.source_chunk_index) |v| std.math.cast(u32, v) orelse return error.ValueOutOfRange else null,
            .target_uri = row.target_uri,
            .edge_type = row.edge_type,
            .weight = row.weight,
            .metadata_json = row.metadata_json,
        });
    }

    try db.exec("COMMIT");
}

// ---- Internal selectors ---------------------------------------------------

fn selectWorkspaces(arena: Allocator, db: Database, workspace_id: []const u8) ![]WorkspaceRow {
    const sql =
        \\SELECT workspace_id, label, description, domain_profile
        \\FROM workspaces WHERE workspace_id = ?1
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(WorkspaceRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .label = try maybeColText(arena, stmt, 1),
            .description = try maybeColText(arena, stmt, 2),
            .domain_profile = try maybeColText(arena, stmt, 3),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectCollections(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]CollectionRow {
    const sql_all =
        \\SELECT collection_id, workspace_id, name, key_kind, chunk_bits, default_language, metadata_json
        \\FROM collections WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT collection_id, workspace_id, name, key_kind, chunk_bits, default_language, metadata_json
        \\FROM collections WHERE workspace_id = ?1 AND collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(CollectionRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .name = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .key_kind = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .chunk_bits = c.sqlite3_column_int64(stmt, 4),
            .default_language = try facet_sqlite.dupeColumnText(arena, stmt, 5),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 6),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectOntologies(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]OntologyRow {
    const sql_all =
        \\SELECT ontology_id, workspace_id, name, version, frozen, source_kind, metadata_json
        \\FROM ontologies WHERE workspace_id = ?1 OR workspace_id IS NULL
    ;
    const sql_attached =
        \\SELECT o.ontology_id, o.workspace_id, o.name, o.version, o.frozen, o.source_kind, o.metadata_json
        \\FROM ontologies o
        \\JOIN collection_ontologies co ON co.ontology_id = o.ontology_id
        \\WHERE co.workspace_id = ?1 AND co.collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_attached);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(OntologyRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .workspace_id = try maybeColText(arena, stmt, 1),
            .name = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .version = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .frozen = c.sqlite3_column_int64(stmt, 4) != 0,
            .source_kind = try facet_sqlite.dupeColumnText(arena, stmt, 5),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 6),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectCollectionOntologies(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]CollectionOntologyRow {
    const sql_all =
        \\SELECT workspace_id, collection_id, ontology_id, role
        \\FROM collection_ontologies WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, collection_id, ontology_id, role
        \\FROM collection_ontologies WHERE workspace_id = ?1 AND collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(CollectionOntologyRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .role = try facet_sqlite.dupeColumnText(arena, stmt, 3),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectWorkspaceSettings(arena: Allocator, db: Database, workspace_id: []const u8) ![]WorkspaceSettingsRow {
    const sql = "SELECT workspace_id, default_ontology_id FROM workspace_settings WHERE workspace_id = ?1";
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(WorkspaceSettingsRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .default_ontology_id = try maybeColText(arena, stmt, 1),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectDocuments(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]DocumentRow {
    const sql_all =
        \\SELECT workspace_id, collection_id, doc_id, doc_nanoid, content, language, source_ref, summary, metadata_json
        \\FROM documents_raw WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, collection_id, doc_id, doc_nanoid, content, language, source_ref, summary, metadata_json
        \\FROM documents_raw WHERE workspace_id = ?1 AND collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(DocumentRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .doc_id = c.sqlite3_column_int64(stmt, 2),
            .doc_nanoid = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .content = try facet_sqlite.dupeColumnText(arena, stmt, 4),
            .language = try maybeColText(arena, stmt, 5),
            .source_ref = try maybeColText(arena, stmt, 6),
            .summary = try maybeColText(arena, stmt, 7),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 8),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectChunks(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]ChunkRow {
    const sql_all =
        \\SELECT workspace_id, collection_id, doc_id, chunk_index, content, language, offset_start, offset_end, strategy, token_count, parent_chunk_index, metadata_json
        \\FROM chunks_raw WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, collection_id, doc_id, chunk_index, content, language, offset_start, offset_end, strategy, token_count, parent_chunk_index, metadata_json
        \\FROM chunks_raw WHERE workspace_id = ?1 AND collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(ChunkRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .doc_id = c.sqlite3_column_int64(stmt, 2),
            .chunk_index = c.sqlite3_column_int64(stmt, 3),
            .content = try facet_sqlite.dupeColumnText(arena, stmt, 4),
            .language = try maybeColText(arena, stmt, 5),
            .offset_start = if (c.sqlite3_column_type(stmt, 6) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 6),
            .offset_end = if (c.sqlite3_column_type(stmt, 7) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 7),
            .strategy = try maybeColText(arena, stmt, 8),
            .token_count = if (c.sqlite3_column_type(stmt, 9) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 9),
            .parent_chunk_index = if (c.sqlite3_column_type(stmt, 10) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 10),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 11),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectExternalLinks(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]ExternalLinkRow {
    const sql_all =
        \\SELECT workspace_id, link_id, source_collection_id, source_doc_id, source_chunk_index, target_uri, edge_type, weight, metadata_json
        \\FROM external_links_raw WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, link_id, source_collection_id, source_doc_id, source_chunk_index, target_uri, edge_type, weight, metadata_json
        \\FROM external_links_raw WHERE workspace_id = ?1 AND source_collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(ExternalLinkRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .link_id = c.sqlite3_column_int64(stmt, 1),
            .source_collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .source_doc_id = c.sqlite3_column_int64(stmt, 3),
            .source_chunk_index = if (c.sqlite3_column_type(stmt, 4) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 4),
            .target_uri = try facet_sqlite.dupeColumnText(arena, stmt, 5),
            .edge_type = try facet_sqlite.dupeColumnText(arena, stmt, 6),
            .weight = c.sqlite3_column_double(stmt, 7),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 8),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectFacetAssignments(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]FacetAssignmentRow {
    const sql_all =
        \\SELECT workspace_id, collection_id, target_kind, doc_id, chunk_index, ontology_id, namespace, dimension, value, value_id, weight, source
        \\FROM facet_assignments_raw WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, collection_id, target_kind, doc_id, chunk_index, ontology_id, namespace, dimension, value, value_id, weight, source
        \\FROM facet_assignments_raw WHERE workspace_id = ?1 AND collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(FacetAssignmentRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .target_kind = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .doc_id = c.sqlite3_column_int64(stmt, 3),
            .chunk_index = c.sqlite3_column_int64(stmt, 4),
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 5),
            .namespace = try facet_sqlite.dupeColumnText(arena, stmt, 6),
            .dimension = try facet_sqlite.dupeColumnText(arena, stmt, 7),
            .value = try facet_sqlite.dupeColumnText(arena, stmt, 8),
            .value_id = if (c.sqlite3_column_type(stmt, 9) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 9),
            .weight = c.sqlite3_column_double(stmt, 10),
            .source = try maybeColText(arena, stmt, 11),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectEntities(arena: Allocator, db: Database, workspace_id: []const u8) ![]EntityRow {
    const sql =
        \\SELECT workspace_id, ontology_id, entity_id, entity_type, name, confidence, metadata_json
        \\FROM entities_raw WHERE workspace_id = ?1
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(EntityRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .entity_id = c.sqlite3_column_int64(stmt, 2),
            .entity_type = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .name = try facet_sqlite.dupeColumnText(arena, stmt, 4),
            .confidence = c.sqlite3_column_double(stmt, 5),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 6),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectEntityAliases(arena: Allocator, db: Database, workspace_id: []const u8) ![]EntityAliasRow {
    const sql =
        \\SELECT workspace_id, entity_id, term, confidence
        \\FROM entity_aliases_raw WHERE workspace_id = ?1
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(EntityAliasRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .entity_id = c.sqlite3_column_int64(stmt, 1),
            .term = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .confidence = c.sqlite3_column_double(stmt, 3),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectRelations(arena: Allocator, db: Database, workspace_id: []const u8) ![]RelationRow {
    const sql =
        \\SELECT workspace_id, ontology_id, relation_id, edge_type, source_entity_id, target_entity_id, valid_from, valid_to, confidence, metadata_json
        \\FROM relations_raw WHERE workspace_id = ?1
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(RelationRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .relation_id = c.sqlite3_column_int64(stmt, 2),
            .edge_type = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .source_entity_id = c.sqlite3_column_int64(stmt, 4),
            .target_entity_id = c.sqlite3_column_int64(stmt, 5),
            .valid_from = try maybeColText(arena, stmt, 6),
            .valid_to = try maybeColText(arena, stmt, 7),
            .confidence = c.sqlite3_column_double(stmt, 8),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 9),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectEntityDocuments(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]EntityDocumentRow {
    const sql_all =
        \\SELECT workspace_id, entity_id, collection_id, doc_id, role, confidence
        \\FROM entity_documents_raw WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, entity_id, collection_id, doc_id, role, confidence
        \\FROM entity_documents_raw WHERE workspace_id = ?1 AND collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(EntityDocumentRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .entity_id = c.sqlite3_column_int64(stmt, 1),
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .doc_id = c.sqlite3_column_int64(stmt, 3),
            .role = try maybeColText(arena, stmt, 4),
            .confidence = c.sqlite3_column_double(stmt, 5),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectEntityChunks(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]EntityChunkRow {
    const sql_all =
        \\SELECT workspace_id, entity_id, collection_id, doc_id, chunk_index, role, confidence
        \\FROM entity_chunks_raw WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, entity_id, collection_id, doc_id, chunk_index, role, confidence
        \\FROM entity_chunks_raw WHERE workspace_id = ?1 AND collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(EntityChunkRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .entity_id = c.sqlite3_column_int64(stmt, 1),
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .doc_id = c.sqlite3_column_int64(stmt, 3),
            .chunk_index = c.sqlite3_column_int64(stmt, 4),
            .role = try maybeColText(arena, stmt, 5),
            .confidence = c.sqlite3_column_double(stmt, 6),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectDocumentLinks(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]DocumentLinkRow {
    const sql_all =
        \\SELECT workspace_id, link_id, ontology_id, edge_type, source_collection_id, source_doc_id, source_chunk_index, target_collection_id, target_doc_id, target_chunk_index, weight, metadata_json, source
        \\FROM document_links_raw WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, link_id, ontology_id, edge_type, source_collection_id, source_doc_id, source_chunk_index, target_collection_id, target_doc_id, target_chunk_index, weight, metadata_json, source
        \\FROM document_links_raw
        \\WHERE workspace_id = ?1 AND (source_collection_id = ?2 OR target_collection_id = ?2)
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(DocumentLinkRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .link_id = c.sqlite3_column_int64(stmt, 1),
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .edge_type = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .source_collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 4),
            .source_doc_id = c.sqlite3_column_int64(stmt, 5),
            .source_chunk_index = if (c.sqlite3_column_type(stmt, 6) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 6),
            .target_collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 7),
            .target_doc_id = c.sqlite3_column_int64(stmt, 8),
            .target_chunk_index = if (c.sqlite3_column_type(stmt, 9) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 9),
            .weight = c.sqlite3_column_double(stmt, 10),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 11),
            .source = try maybeColText(arena, stmt, 12),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn maybeColText(arena: Allocator, stmt: *c.sqlite3_stmt, index: c_int) !?[]const u8 {
    if (c.sqlite3_column_type(stmt, index) == c.SQLITE_NULL) return null;
    return try facet_sqlite.dupeColumnText(arena, stmt, index);
}

// ---- Tests ---------------------------------------------------------------

test "export+import bundle round-trips workspace, collection, raw rows" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws_round", .label = "Round" });
    try collections_sqlite.ensureCollection(db, .{
        .workspace_id = "ws_round",
        .collection_id = "ws_round::docs",
        .name = "docs",
    });
    try collections_sqlite.ensureOntology(db, .{
        .ontology_id = "ws_round::core",
        .workspace_id = "ws_round",
        .name = "core",
    });
    try collections_sqlite.attachOntologyToCollection(db, "ws_round", "ws_round::docs", "ws_round::core", "primary");
    try collections_sqlite.upsertDocumentRaw(db, .{
        .workspace_id = "ws_round",
        .collection_id = "ws_round::docs",
        .doc_id = 1,
        .content = "alpha",
    });
    try collections_sqlite.upsertChunkRaw(db, .{
        .workspace_id = "ws_round",
        .collection_id = "ws_round::docs",
        .doc_id = 1,
        .chunk_index = 0,
        .content = "alpha",
    });
    try collections_sqlite.ensureNamespace(db, .{ .ontology_id = "ws_round::core", .namespace = "topic" });
    try collections_sqlite.ensureDimension(db, .{ .ontology_id = "ws_round::core", .namespace = "topic", .dimension = "category" });
    try collections_sqlite.upsertFacetAssignmentRaw(db, .{
        .workspace_id = "ws_round",
        .collection_id = "ws_round::docs",
        .target_kind = .doc,
        .doc_id = 1,
        .ontology_id = "ws_round::core",
        .namespace = "topic",
        .dimension = "category",
        .value = "alpha",
    });

    const bundle = try exportToJson(std.testing.allocator, db, .{ .workspace = "ws_round" });
    defer std.testing.allocator.free(bundle);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "ws_round::docs") != null);

    var dest = try Database.openInMemory();
    defer dest.close();
    try dest.applyStandaloneSchema();

    try importBundleJson(dest, std.testing.allocator, bundle);

    const sql = "SELECT COUNT(*) FROM documents_raw WHERE workspace_id = 'ws_round'";
    const stmt = try facet_sqlite.prepare(dest, sql);
    defer facet_sqlite.finalize(stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt));
    try std.testing.expectEqual(@as(i64, 1), c.sqlite3_column_int64(stmt, 0));
}

test "export+import bundle round-trips chunks, cross-collection links and entity bindings" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws_xc", .label = "Cross" });
    try collections_sqlite.ensureCollection(db, .{
        .workspace_id = "ws_xc",
        .collection_id = "ws_xc::legal",
        .name = "legal",
    });
    try collections_sqlite.ensureCollection(db, .{
        .workspace_id = "ws_xc",
        .collection_id = "ws_xc::technical",
        .name = "technical",
    });
    try collections_sqlite.ensureOntology(db, .{
        .ontology_id = "ws_xc::core",
        .workspace_id = "ws_xc",
        .name = "core",
    });
    try collections_sqlite.attachOntologyToCollection(db, "ws_xc", "ws_xc::legal", "ws_xc::core", "primary");
    try collections_sqlite.attachOntologyToCollection(db, "ws_xc", "ws_xc::technical", "ws_xc::core", "primary");

    try collections_sqlite.upsertDocumentRaw(db, .{
        .workspace_id = "ws_xc",
        .collection_id = "ws_xc::legal",
        .doc_id = 100,
        .content = "Contract clause about retention.",
    });
    try collections_sqlite.upsertChunkRaw(db, .{
        .workspace_id = "ws_xc",
        .collection_id = "ws_xc::legal",
        .doc_id = 100,
        .chunk_index = 0,
        .content = "Retention clause",
    });
    try collections_sqlite.upsertDocumentRaw(db, .{
        .workspace_id = "ws_xc",
        .collection_id = "ws_xc::technical",
        .doc_id = 200,
        .content = "Storage retention spec.",
    });
    try collections_sqlite.upsertChunkRaw(db, .{
        .workspace_id = "ws_xc",
        .collection_id = "ws_xc::technical",
        .doc_id = 200,
        .chunk_index = 0,
        .content = "Storage spec",
    });

    try collections_sqlite.upsertEntityRaw(db, .{
        .workspace_id = "ws_xc",
        .ontology_id = "ws_xc::core",
        .entity_id = 5000,
        .entity_type = "concept",
        .name = "data_retention",
    });
    try collections_sqlite.linkEntityDocumentRaw(db, .{
        .workspace_id = "ws_xc",
        .entity_id = 5000,
        .collection_id = "ws_xc::legal",
        .doc_id = 100,
        .role = "subject",
    });
    try collections_sqlite.linkEntityDocumentRaw(db, .{
        .workspace_id = "ws_xc",
        .entity_id = 5000,
        .collection_id = "ws_xc::technical",
        .doc_id = 200,
        .role = "subject",
    });
    try collections_sqlite.linkEntityChunkRaw(db, .{
        .workspace_id = "ws_xc",
        .entity_id = 5000,
        .collection_id = "ws_xc::legal",
        .doc_id = 100,
        .chunk_index = 0,
        .role = "subject",
    });

    try collections_sqlite.upsertDocumentLinkRaw(db, .{
        .workspace_id = "ws_xc",
        .link_id = 1,
        .ontology_id = "ws_xc::core",
        .edge_type = "implements",
        .source_collection_id = "ws_xc::legal",
        .source_doc_id = 100,
        .target_collection_id = "ws_xc::technical",
        .target_doc_id = 200,
        .weight = 0.8,
    });

    // Upsert one document with the new doc_nanoid + summary fields and a chunk
    // carrying the chunker's strategy/token_count/parent_chunk_index columns
    // so the round-trip exercises every newly introduced raw column.
    try collections_sqlite.upsertDocumentRaw(db, .{
        .workspace_id = "ws_xc",
        .collection_id = "ws_xc::legal",
        .doc_id = 300,
        .doc_nanoid = "nano_round_trip_300",
        .content = "Annexed clause about retention.",
        .summary = "Short summary",
    });
    try collections_sqlite.upsertChunkRaw(db, .{
        .workspace_id = "ws_xc",
        .collection_id = "ws_xc::legal",
        .doc_id = 300,
        .chunk_index = 0,
        .content = "Annexed clause",
        .strategy = "fixed_token",
        .token_count = 3,
        .parent_chunk_index = null,
    });

    try collections_sqlite.upsertExternalLinkRaw(db, .{
        .workspace_id = "ws_xc",
        .link_id = 42,
        .source_collection_id = "ws_xc::legal",
        .source_doc_id = 100,
        .source_chunk_index = 0,
        .target_uri = "https://example.com/policy",
        .edge_type = "cites",
        .weight = 0.5,
        .metadata_json = "{\"note\":\"external\"}",
    });

    const bundle = try exportToJson(std.testing.allocator, db, .{ .workspace = "ws_xc" });
    defer std.testing.allocator.free(bundle);

    // Bundle must carry the freshly added columns + table verbatim so a fresh
    // import can rehydrate them.
    try std.testing.expect(std.mem.indexOf(u8, bundle, "nano_round_trip_300") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "fixed_token") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "external_links_raw") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "https://example.com/policy") != null);

    var dest = try Database.openInMemory();
    defer dest.close();
    try dest.applyStandaloneSchema();
    try importBundleJson(dest, std.testing.allocator, bundle);

    const Counts = struct {
        fn one(d: Database, sql: []const u8) !i64 {
            const stmt = try facet_sqlite.prepare(d, sql);
            defer facet_sqlite.finalize(stmt);
            try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt));
            return c.sqlite3_column_int64(stmt, 0);
        }
    };

    try std.testing.expectEqual(@as(i64, 2), try Counts.one(dest, "SELECT COUNT(*) FROM collections WHERE workspace_id = 'ws_xc'"));
    try std.testing.expectEqual(@as(i64, 3), try Counts.one(dest, "SELECT COUNT(*) FROM documents_raw WHERE workspace_id = 'ws_xc'"));
    try std.testing.expectEqual(@as(i64, 3), try Counts.one(dest, "SELECT COUNT(*) FROM chunks_raw WHERE workspace_id = 'ws_xc'"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM entities_raw WHERE workspace_id = 'ws_xc'"));
    try std.testing.expectEqual(@as(i64, 2), try Counts.one(dest, "SELECT COUNT(*) FROM entity_documents_raw WHERE workspace_id = 'ws_xc'"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM entity_chunks_raw WHERE workspace_id = 'ws_xc'"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM document_links_raw WHERE workspace_id = 'ws_xc'"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM external_links_raw WHERE workspace_id = 'ws_xc'"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM documents_raw WHERE doc_nanoid = 'nano_round_trip_300'"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM chunks_raw WHERE strategy = 'fixed_token' AND token_count = 3"));

    // Collection-scoped export should keep the cross-collection link because at
    // least one endpoint matches the requested collection, but should drop
    // documents that live exclusively in the other collection.
    const partial = try exportToJson(std.testing.allocator, db, .{
        .collection = .{ .workspace_id = "ws_xc", .collection_id = "ws_xc::legal" },
    });
    defer std.testing.allocator.free(partial);
    try std.testing.expect(std.mem.indexOf(u8, partial, "ws_xc::legal") != null);
    try std.testing.expect(std.mem.indexOf(u8, partial, "implements") != null);
    try std.testing.expect(std.mem.indexOf(u8, partial, "\"doc_id\": 100") != null);
    // Documents that belong only to the other collection must not leak.
    try std.testing.expect(std.mem.indexOf(u8, partial, "Storage retention spec") == null);
}
