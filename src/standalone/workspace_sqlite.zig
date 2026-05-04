const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const ontology_sqlite = @import("ontology_sqlite.zig");
const toon_exports = @import("toon_exports.zig");
const ztoon = @import("ztoon");

pub const Database = facet_sqlite.Database;
pub const c = facet_sqlite.c;
const Error = facet_sqlite.Error;
const Field = ztoon.Field;
const Value = ztoon.Value;

pub const WorkspaceExport = struct {
    schema_version: []const u8,
    workspace: ?WorkspaceMetadata,
    tables: []TableExport,
    columns: []ColumnExport,
    relations: []RelationExport,
    source_mappings: []SourceMappingExport,
    generation_hints: GenerationHints,
    validation_warnings: []const []const u8,
};

pub const WorkspaceMetadata = struct {
    workspace_id: []const u8,
    label: []const u8,
    description: []const u8,
    domain_profile: []const u8,
    schema_name: []const u8,
};

pub const TableExport = struct {
    schema_name: []const u8,
    table_name: []const u8,
    table_role: []const u8,
    entity_family: ?[]const u8,
    primary_time_column: ?[]const u8,
    volume_driver: []const u8,
    generation_strategy: []const u8,
    emit_facets: bool,
    emit_graph_entities: bool,
    emit_graph_relations: bool,
    emit_projections: bool,
    notes_json: []const u8,
};

pub const ColumnExport = struct {
    schema_name: []const u8,
    table_name: []const u8,
    column_name: []const u8,
    column_role: []const u8,
    semantic_type: ?[]const u8,
    facet_key: ?[]const u8,
    graph_usage: ?[]const u8,
    projection_signal: ?[]const u8,
    is_nullable: bool,
};

pub const RelationExport = struct {
    source_schema: []const u8,
    source_table: []const u8,
    source_column: []const u8,
    target_schema: []const u8,
    target_table: []const u8,
    target_column: []const u8,
    relation_role: ?[]const u8,
    hierarchical: bool,
    graph_label: ?[]const u8,
    cardinality: []const u8,
};

pub const SourceMappingExport = struct {
    source_key: []const u8,
    source_kind: []const u8,
    target_table: ?[]const u8,
    metadata_json: []const u8,
};

pub const GenerationHints = struct {
    table_order: []const []const u8,
    estimated_total_rows: usize,
    domain_profile: []const u8,
    time_window_days: u32,
};

pub fn upsertWorkspace(
    db: Database,
    workspace_id: []const u8,
    domain_profile_json: []const u8,
) !void {
    const stmt = try prepare(db, "INSERT OR REPLACE INTO workspaces(id, workspace_id, domain_profile, domain_profile_json) VALUES (?1, ?2, ?3, ?4)");
    defer finalize(stmt);
    try bindText(stmt, 1, workspace_id);
    try bindText(stmt, 2, workspace_id);
    try bindText(stmt, 3, domain_profile_json);
    try bindText(stmt, 4, domain_profile_json);
    try stepDone(stmt);
}

pub fn upsertTableSemantic(
    db: Database,
    table_id: u64,
    workspace_id: []const u8,
    schema_name: []const u8,
    table_name: []const u8,
    key_column: []const u8,
    content_column: []const u8,
    metadata_column: []const u8,
    vector_column: ?[]const u8,
    language: []const u8,
) !void {
    const stmt = try prepare(db, "INSERT OR REPLACE INTO table_semantics(table_id, workspace_id, table_schema, table_name, business_role, generation_strategy, emit_facets, emit_graph_entity, emit_graph_relation, notes, schema_name, key_column, content_column, metadata_column, vector_column, language) VALUES (?1, ?2, ?3, ?4, NULL, 'unknown', 1, 0, 0, NULL, ?3, ?5, ?6, ?7, ?8, ?9)");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindText(stmt, 2, workspace_id);
    try bindText(stmt, 3, schema_name);
    try bindText(stmt, 4, table_name);
    try bindText(stmt, 5, key_column);
    try bindText(stmt, 6, content_column);
    try bindText(stmt, 7, metadata_column);
    if (vector_column) |value| try bindText(stmt, 8, value) else try bindNull(stmt, 8);
    try bindText(stmt, 9, language);
    try stepDone(stmt);
}

pub fn upsertColumnSemantic(
    db: Database,
    column_semantic_id: u64,
    table_id: u64,
    column_name: []const u8,
    semantic_role: []const u8,
    data_type: ?[]const u8,
    is_nullable: bool,
) !void {
    const lookup = try prepare(db, "SELECT workspace_id, table_schema, table_name FROM table_semantics WHERE table_id = ?1");
    defer finalize(lookup);
    try bindInt64(lookup, 1, table_id);
    if (c.sqlite3_step(lookup) == c.SQLITE_ROW) {
        const workspace_ptr = c.sqlite3_column_text(lookup, 0) orelse return error.MissingRow;
        const workspace_len: usize = @intCast(c.sqlite3_column_bytes(lookup, 0));
        const schema_ptr = c.sqlite3_column_text(lookup, 1) orelse return error.MissingRow;
        const schema_len: usize = @intCast(c.sqlite3_column_bytes(lookup, 1));
        const table_ptr = c.sqlite3_column_text(lookup, 2) orelse return error.MissingRow;
        const table_len: usize = @intCast(c.sqlite3_column_bytes(lookup, 2));
        const workspace_value: []const u8 = @as([*]const u8, @ptrCast(workspace_ptr))[0..workspace_len];
        const schema_value: []const u8 = @as([*]const u8, @ptrCast(schema_ptr))[0..schema_len];
        const table_value: []const u8 = @as([*]const u8, @ptrCast(table_ptr))[0..table_len];

        const stmt = try prepare(db, "INSERT OR REPLACE INTO column_semantics(column_semantic_id, workspace_id, table_id, table_schema, table_name, column_name, column_role, semantic_role, data_type, is_nullable) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, ?8, ?9)");
        defer finalize(stmt);
        try bindInt64(stmt, 1, column_semantic_id);
        try bindText(stmt, 2, workspace_value);
        try bindInt64(stmt, 3, table_id);
        try bindText(stmt, 4, schema_value);
        try bindText(stmt, 5, table_value);
        try bindText(stmt, 6, column_name);
        try bindText(stmt, 7, semantic_role);
        if (data_type) |value| try bindText(stmt, 8, value) else try bindNull(stmt, 8);
        const nullable_flag: i64 = if (is_nullable) 1 else 0;
        try bindInt64(stmt, 9, nullable_flag);
        try stepDone(stmt);
        return;
    }
    return error.NotFound;
}

pub fn upsertRelationSemantic(
    db: Database,
    relation_semantic_id: u64,
    workspace_id: []const u8,
    relation_name: []const u8,
    source_table_id: u64,
    target_table_id: u64,
    edge_type: ?[]const u8,
    metadata_json: []const u8,
) !void {
    const source_lookup = try prepare(db, "SELECT workspace_id, table_schema, table_name FROM table_semantics WHERE table_id = ?1");
    defer finalize(source_lookup);
    try bindInt64(source_lookup, 1, source_table_id);
    if (c.sqlite3_step(source_lookup) != c.SQLITE_ROW) return error.NotFound;
    const source_schema_ptr = c.sqlite3_column_text(source_lookup, 1) orelse return error.MissingRow;
    const source_schema_len: usize = @intCast(c.sqlite3_column_bytes(source_lookup, 1));
    const source_table_ptr = c.sqlite3_column_text(source_lookup, 2) orelse return error.MissingRow;
    const source_table_len: usize = @intCast(c.sqlite3_column_bytes(source_lookup, 2));
    const source_schema_value: []const u8 = @as([*]const u8, @ptrCast(source_schema_ptr))[0..source_schema_len];
    const source_table_value: []const u8 = @as([*]const u8, @ptrCast(source_table_ptr))[0..source_table_len];

    const target_lookup = try prepare(db, "SELECT workspace_id, table_schema, table_name FROM table_semantics WHERE table_id = ?1");
    defer finalize(target_lookup);
    try bindInt64(target_lookup, 1, target_table_id);
    if (c.sqlite3_step(target_lookup) != c.SQLITE_ROW) return error.NotFound;
    const target_schema_ptr = c.sqlite3_column_text(target_lookup, 1) orelse return error.MissingRow;
    const target_schema_len: usize = @intCast(c.sqlite3_column_bytes(target_lookup, 1));
    const target_table_ptr = c.sqlite3_column_text(target_lookup, 2) orelse return error.MissingRow;
    const target_table_len: usize = @intCast(c.sqlite3_column_bytes(target_lookup, 2));
    const target_schema_value: []const u8 = @as([*]const u8, @ptrCast(target_schema_ptr))[0..target_schema_len];
    const target_table_value: []const u8 = @as([*]const u8, @ptrCast(target_table_ptr))[0..target_table_len];

    const stmt = try prepare(db, "INSERT OR REPLACE INTO relation_semantics(relation_semantic_id, workspace_id, from_schema, from_table, to_schema, to_table, fk_column, relation_kind, relation_name, source_table_id, target_table_id, edge_type, metadata_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, '', ?7, ?8, ?9, ?10, ?11, ?12)");
    defer finalize(stmt);
    try bindInt64(stmt, 1, relation_semantic_id);
    try bindText(stmt, 2, workspace_id);
    try bindText(stmt, 3, source_schema_value);
    try bindText(stmt, 4, source_table_value);
    try bindText(stmt, 5, target_schema_value);
    try bindText(stmt, 6, target_table_value);
    try bindText(stmt, 7, edge_type orelse "unknown");
    try bindText(stmt, 8, relation_name);
    try bindInt64(stmt, 9, source_table_id);
    try bindInt64(stmt, 10, target_table_id);
    if (edge_type) |value| try bindText(stmt, 11, value) else try bindNull(stmt, 11);
    try bindText(stmt, 12, metadata_json);
    try stepDone(stmt);
}

pub fn upsertSourceMapping(
    db: Database,
    source_mapping_id: u64,
    workspace_id: []const u8,
    source_key: []const u8,
    source_kind: []const u8,
    target_table_id: ?u64,
    metadata_json: []const u8,
) !void {
    const stmt = try prepare(db, "INSERT OR REPLACE INTO source_mappings(source_mapping_id, workspace_id, source_key, source_kind, target_table_id, metadata_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6)");
    defer finalize(stmt);
    try bindInt64(stmt, 1, source_mapping_id);
    try bindText(stmt, 2, workspace_id);
    try bindText(stmt, 3, source_key);
    try bindText(stmt, 4, source_kind);
    if (target_table_id) |value| try bindInt64(stmt, 5, value) else try bindNull(stmt, 5);
    try bindText(stmt, 6, metadata_json);
    try stepDone(stmt);
}

pub fn exportWorkspaceModel(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8) !WorkspaceExport {
    const workspace = try loadWorkspace(db, allocator, workspace_id);
    if (workspace == null) {
        const warning = try std.fmt.allocPrint(allocator, "Workspace {s} does not exist", .{workspace_id});
        errdefer allocator.free(warning);
        const warnings = try allocator.alloc([]const u8, 1);
        warnings[0] = warning;
        return .{
            .schema_version = "1.0.0",
            .workspace = null,
            .tables = &.{},
            .columns = &.{},
            .relations = &.{},
            .source_mappings = &.{},
            .generation_hints = .{
                .table_order = &.{},
                .estimated_total_rows = 0,
                .domain_profile = "",
                .time_window_days = 90,
            },
            .validation_warnings = warnings,
        };
    }

    const tables = try loadTables(db, allocator, workspace_id);
    errdefer deinitTableExports(allocator, tables);
    const columns = try loadColumns(db, allocator, workspace_id);
    errdefer deinitColumnExports(allocator, columns);
    const relations = try loadRelations(db, allocator, workspace_id);
    errdefer deinitRelationExports(allocator, relations);
    const source_mappings = try loadSourceMappings(db, allocator, workspace_id);
    errdefer deinitSourceMappingExports(allocator, source_mappings);
    const hints = try buildGenerationHints(allocator, workspace.?, tables);
    errdefer deinitGenerationHints(allocator, hints);

    return .{
        .schema_version = "1.0.0",
        .workspace = workspace,
        .tables = tables,
        .columns = columns,
        .relations = relations,
        .source_mappings = source_mappings,
        .generation_hints = hints,
        .validation_warnings = &.{},
    };
}

pub fn exportWorkspaceModelToon(
    db: Database,
    allocator: std.mem.Allocator,
    workspace_id: []const u8,
) ![]u8 {
    var model = try exportWorkspaceModel(db, allocator, workspace_id);
    defer deinitWorkspaceExport(allocator, &model);

    const root = try buildWorkspaceExportValue(allocator, model);
    defer toon_exports.deinitOwnedValue(allocator, root);

    return try toon_exports.encodeValueAlloc(
        allocator,
        root,
        toon_exports.default_options,
    );
}

pub fn exportWorkspaceModelByDomain(
    db: Database,
    allocator: std.mem.Allocator,
    domain_or_workspace: []const u8,
) !?WorkspaceExport {
    const resolved = try ontology_sqlite.resolveWorkspace(db, allocator, domain_or_workspace);
    defer if (resolved) |value| allocator.free(value);
    if (resolved == null) return null;
    return try exportWorkspaceModel(db, allocator, resolved.?);
}

pub fn exportWorkspaceModelToonByDomain(
    db: Database,
    allocator: std.mem.Allocator,
    domain_or_workspace: []const u8,
) !?[]u8 {
    const model = try exportWorkspaceModelByDomain(db, allocator, domain_or_workspace);
    if (model == null) return null;
    var owned = model.?;
    defer deinitWorkspaceExport(allocator, &owned);

    const root = try buildWorkspaceExportValue(allocator, owned);
    defer toon_exports.deinitOwnedValue(allocator, root);

    return try toon_exports.encodeValueAlloc(
        allocator,
        root,
        toon_exports.default_options,
    );
}

fn buildWorkspaceExportValue(
    allocator: std.mem.Allocator,
    model: WorkspaceExport,
) !Value {
    const fields = try allocator.alloc(Field, 9);
    fields[0] = .{ .key = "kind", .value = .{ .string = "workspace_export" } };
    fields[1] = .{ .key = "schema_version", .value = .{ .string = model.schema_version } };
    fields[2] = .{ .key = "workspace", .value = try buildWorkspaceMetadataValue(allocator, model.workspace) };
    fields[3] = .{ .key = "tables", .value = .{ .array = try buildTableExports(allocator, model.tables) } };
    fields[4] = .{ .key = "columns", .value = .{ .array = try buildColumnExports(allocator, model.columns) } };
    fields[5] = .{ .key = "relations", .value = .{ .array = try buildRelationExports(allocator, model.relations) } };
    fields[6] = .{ .key = "source_mappings", .value = .{ .array = try buildSourceMappingExports(allocator, model.source_mappings) } };
    fields[7] = .{ .key = "generation_hints", .value = try buildGenerationHintsValue(allocator, model.generation_hints) };
    fields[8] = .{ .key = "validation_warnings", .value = .{ .array = try toon_exports.buildStringArray(allocator, model.validation_warnings) } };
    return .{ .object = fields };
}

fn buildWorkspaceMetadataValue(
    allocator: std.mem.Allocator,
    maybe_workspace: ?WorkspaceMetadata,
) !Value {
    const workspace = maybe_workspace orelse return .null;
    const fields = try allocator.alloc(Field, 5);
    fields[0] = .{ .key = "workspace_id", .value = .{ .string = workspace.workspace_id } };
    fields[1] = .{ .key = "label", .value = .{ .string = workspace.label } };
    fields[2] = .{ .key = "description", .value = .{ .string = workspace.description } };
    fields[3] = .{ .key = "domain_profile", .value = .{ .string = workspace.domain_profile } };
    fields[4] = .{ .key = "schema_name", .value = .{ .string = workspace.schema_name } };
    return .{ .object = fields };
}

fn buildTableExports(allocator: std.mem.Allocator, tables: []const TableExport) ![]Value {
    const values = try allocator.alloc(Value, tables.len);
    errdefer allocator.free(values);

    for (tables, 0..) |table, index| {
        const fields = try allocator.alloc(Field, 12);
        fields[0] = .{ .key = "schema_name", .value = .{ .string = table.schema_name } };
        fields[1] = .{ .key = "table_name", .value = .{ .string = table.table_name } };
        fields[2] = .{ .key = "table_role", .value = .{ .string = table.table_role } };
        fields[3] = .{ .key = "entity_family", .value = toon_exports.optionalStringValue(table.entity_family) };
        fields[4] = .{ .key = "primary_time_column", .value = toon_exports.optionalStringValue(table.primary_time_column) };
        fields[5] = .{ .key = "volume_driver", .value = .{ .string = table.volume_driver } };
        fields[6] = .{ .key = "generation_strategy", .value = .{ .string = table.generation_strategy } };
        fields[7] = .{ .key = "emit_facets", .value = .{ .bool = table.emit_facets } };
        fields[8] = .{ .key = "emit_graph_entities", .value = .{ .bool = table.emit_graph_entities } };
        fields[9] = .{ .key = "emit_graph_relations", .value = .{ .bool = table.emit_graph_relations } };
        fields[10] = .{ .key = "emit_projections", .value = .{ .bool = table.emit_projections } };
        fields[11] = .{ .key = "notes_json", .value = .{ .string = table.notes_json } };
        values[index] = .{ .object = fields };
    }

    return values;
}

fn buildColumnExports(allocator: std.mem.Allocator, columns: []const ColumnExport) ![]Value {
    const values = try allocator.alloc(Value, columns.len);
    errdefer allocator.free(values);

    for (columns, 0..) |column, index| {
        const fields = try allocator.alloc(Field, 9);
        fields[0] = .{ .key = "schema_name", .value = .{ .string = column.schema_name } };
        fields[1] = .{ .key = "table_name", .value = .{ .string = column.table_name } };
        fields[2] = .{ .key = "column_name", .value = .{ .string = column.column_name } };
        fields[3] = .{ .key = "column_role", .value = .{ .string = column.column_role } };
        fields[4] = .{ .key = "semantic_type", .value = toon_exports.optionalStringValue(column.semantic_type) };
        fields[5] = .{ .key = "facet_key", .value = toon_exports.optionalStringValue(column.facet_key) };
        fields[6] = .{ .key = "graph_usage", .value = toon_exports.optionalStringValue(column.graph_usage) };
        fields[7] = .{ .key = "projection_signal", .value = toon_exports.optionalStringValue(column.projection_signal) };
        fields[8] = .{ .key = "is_nullable", .value = .{ .bool = column.is_nullable } };
        values[index] = .{ .object = fields };
    }

    return values;
}

fn buildRelationExports(allocator: std.mem.Allocator, relations: []const RelationExport) ![]Value {
    const values = try allocator.alloc(Value, relations.len);
    errdefer allocator.free(values);

    for (relations, 0..) |relation, index| {
        const fields = try allocator.alloc(Field, 10);
        fields[0] = .{ .key = "source_schema", .value = .{ .string = relation.source_schema } };
        fields[1] = .{ .key = "source_table", .value = .{ .string = relation.source_table } };
        fields[2] = .{ .key = "source_column", .value = .{ .string = relation.source_column } };
        fields[3] = .{ .key = "target_schema", .value = .{ .string = relation.target_schema } };
        fields[4] = .{ .key = "target_table", .value = .{ .string = relation.target_table } };
        fields[5] = .{ .key = "target_column", .value = .{ .string = relation.target_column } };
        fields[6] = .{ .key = "relation_role", .value = toon_exports.optionalStringValue(relation.relation_role) };
        fields[7] = .{ .key = "hierarchical", .value = .{ .bool = relation.hierarchical } };
        fields[8] = .{ .key = "graph_label", .value = toon_exports.optionalStringValue(relation.graph_label) };
        fields[9] = .{ .key = "cardinality", .value = .{ .string = relation.cardinality } };
        values[index] = .{ .object = fields };
    }

    return values;
}

fn buildSourceMappingExports(allocator: std.mem.Allocator, rows: []const SourceMappingExport) ![]Value {
    const values = try allocator.alloc(Value, rows.len);
    errdefer allocator.free(values);

    for (rows, 0..) |row, index| {
        const fields = try allocator.alloc(Field, 4);
        fields[0] = .{ .key = "source_key", .value = .{ .string = row.source_key } };
        fields[1] = .{ .key = "source_kind", .value = .{ .string = row.source_kind } };
        fields[2] = .{ .key = "target_table", .value = toon_exports.optionalStringValue(row.target_table) };
        fields[3] = .{ .key = "metadata_json", .value = .{ .string = row.metadata_json } };
        values[index] = .{ .object = fields };
    }

    return values;
}

fn buildGenerationHintsValue(allocator: std.mem.Allocator, hints: GenerationHints) !Value {
    const fields = try allocator.alloc(Field, 4);
    fields[0] = .{ .key = "table_order", .value = .{ .array = try toon_exports.buildStringArray(allocator, hints.table_order) } };
    fields[1] = .{ .key = "estimated_total_rows", .value = toon_exports.intValue(hints.estimated_total_rows) };
    fields[2] = .{ .key = "domain_profile", .value = .{ .string = hints.domain_profile } };
    fields[3] = .{ .key = "time_window_days", .value = toon_exports.intValue(hints.time_window_days) };
    return .{ .object = fields };
}

pub fn deinitWorkspaceExport(allocator: std.mem.Allocator, value: *WorkspaceExport) void {
    if (value.workspace) |*workspace| deinitWorkspaceMetadata(allocator, workspace);
    deinitTableExports(allocator, value.tables);
    deinitColumnExports(allocator, value.columns);
    deinitRelationExports(allocator, value.relations);
    deinitSourceMappingExports(allocator, value.source_mappings);
    deinitGenerationHints(allocator, value.generation_hints);
    for (value.validation_warnings) |warning| allocator.free(warning);
    allocator.free(value.validation_warnings);
    value.* = undefined;
}

fn loadWorkspace(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8) !?WorkspaceMetadata {
    const stmt = try prepare(db, "SELECT workspace_id, domain_profile_json FROM workspaces WHERE workspace_id = ?1");
    defer finalize(stmt);
    try bindText(stmt, 1, workspace_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return null;

    const id = try dupeColumnText(allocator, stmt, 0);
    errdefer allocator.free(id);
    const domain_profile = try dupeColumnText(allocator, stmt, 1);
    errdefer allocator.free(domain_profile);

    return .{
        .workspace_id = id,
        .label = try allocator.dupe(u8, id),
        .description = try allocator.dupe(u8, ""),
        .domain_profile = domain_profile,
        .schema_name = try allocator.dupe(u8, "main"),
    };
}

fn loadTables(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8) ![]TableExport {
    const stmt = try prepare(db, "SELECT table_id, schema_name, table_name, key_column, content_column, language, vector_column FROM table_semantics WHERE workspace_id = ?1 ORDER BY schema_name, table_name");
    defer finalize(stmt);
    try bindText(stmt, 1, workspace_id);

    var rows = std.ArrayList(TableExport).empty;
    defer {
        for (rows.items) |row| deinitTableExport(allocator, row);
        rows.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const table_id = try columnU64(stmt, 0);
        const key_column = try dupeColumnText(allocator, stmt, 3);
        defer allocator.free(key_column);
        const content_column = try dupeColumnText(allocator, stmt, 4);
        defer allocator.free(content_column);
        const language = try dupeColumnText(allocator, stmt, 5);
        defer allocator.free(language);
        const signals = try loadTableSignals(db, allocator, table_id, content_column, c.sqlite3_column_type(stmt, 6) != c.SQLITE_NULL);
        defer deinitTableSignals(allocator, signals);

        try rows.append(allocator, .{
            .schema_name = try dupeColumnText(allocator, stmt, 1),
            .table_name = try dupeColumnText(allocator, stmt, 2),
            .table_role = try allocator.dupe(u8, inferTableRole(key_column)),
            .entity_family = null,
            .primary_time_column = null,
            .volume_driver = try allocator.dupe(u8, signals.volume_driver),
            .generation_strategy = try allocator.dupe(u8, signals.generation_strategy),
            .emit_facets = signals.emit_facets,
            .emit_graph_entities = signals.emit_graph_entities,
            .emit_graph_relations = signals.emit_graph_relations,
            .emit_projections = signals.emit_projections,
            .notes_json = try allocator.dupe(u8, signals.notes_json),
        });
    }

    return rows.toOwnedSlice(allocator);
}

fn loadColumns(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8) ![]ColumnExport {
    const stmt = try prepare(db, "SELECT ts.schema_name, ts.table_name, cs.column_name, cs.semantic_role, cs.data_type, cs.is_nullable FROM column_semantics cs JOIN table_semantics ts ON ts.table_id = cs.table_id WHERE ts.workspace_id = ?1 ORDER BY ts.schema_name, ts.table_name, cs.column_name");
    defer finalize(stmt);
    try bindText(stmt, 1, workspace_id);

    var rows = std.ArrayList(ColumnExport).empty;
    defer {
        for (rows.items) |row| deinitColumnExport(allocator, row);
        rows.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const semantic_role = try dupeColumnText(allocator, stmt, 3);
        defer allocator.free(semantic_role);
        try rows.append(allocator, .{
            .schema_name = try dupeColumnText(allocator, stmt, 0),
            .table_name = try dupeColumnText(allocator, stmt, 1),
            .column_name = try dupeColumnText(allocator, stmt, 2),
            .column_role = try allocator.dupe(u8, mapColumnRole(semantic_role)),
            .semantic_type = if (c.sqlite3_column_type(stmt, 4) == c.SQLITE_NULL) null else try dupeColumnText(allocator, stmt, 4),
            .facet_key = null,
            .graph_usage = null,
            .projection_signal = null,
            .is_nullable = c.sqlite3_column_int64(stmt, 5) != 0,
        });
    }

    return rows.toOwnedSlice(allocator);
}

fn loadRelations(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8) ![]RelationExport {
    const stmt = try prepare(db, "SELECT src.schema_name, src.table_name, dst.schema_name, dst.table_name, rs.relation_name, rs.edge_type FROM relation_semantics rs JOIN table_semantics src ON src.table_id = rs.source_table_id JOIN table_semantics dst ON dst.table_id = rs.target_table_id WHERE rs.workspace_id = ?1 ORDER BY src.schema_name, src.table_name, dst.schema_name, dst.table_name, rs.relation_name");
    defer finalize(stmt);
    try bindText(stmt, 1, workspace_id);

    var rows = std.ArrayList(RelationExport).empty;
    defer {
        for (rows.items) |row| deinitRelationExport(allocator, row);
        rows.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        try rows.append(allocator, .{
            .source_schema = try dupeColumnText(allocator, stmt, 0),
            .source_table = try dupeColumnText(allocator, stmt, 1),
            .source_column = try allocator.dupe(u8, "id"),
            .target_schema = try dupeColumnText(allocator, stmt, 2),
            .target_table = try dupeColumnText(allocator, stmt, 3),
            .target_column = try allocator.dupe(u8, "id"),
            .relation_role = try dupeColumnText(allocator, stmt, 4),
            .hierarchical = false,
            .graph_label = if (c.sqlite3_column_type(stmt, 5) == c.SQLITE_NULL) null else try dupeColumnText(allocator, stmt, 5),
            .cardinality = try allocator.dupe(u8, "1:n"),
        });
    }

    return rows.toOwnedSlice(allocator);
}

const TableSignals = struct {
    emit_facets: bool,
    emit_graph_entities: bool,
    emit_graph_relations: bool,
    emit_projections: bool,
    volume_driver: []const u8,
    generation_strategy: []const u8,
    notes_json: []const u8,
};

fn loadTableSignals(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    content_column: []const u8,
    has_vector_column: bool,
) !TableSignals {
    const SignalAccumulator = struct {
        label_or_status_columns: usize = 0,
        fk_columns: usize = 0,
        status_columns: usize = 0,
        relation_count: usize = 0,
        source_mapping_count: usize = 0,
        wants_facets: bool = false,
        wants_graph: bool = false,
        wants_projection: bool = false,
    };

    var signals = SignalAccumulator{};

    const column_stmt = try prepare(db, "SELECT semantic_role FROM column_semantics WHERE table_id = ?1");
    defer finalize(column_stmt);
    try bindInt64(column_stmt, 1, table_id);
    while (true) {
        const rc = c.sqlite3_step(column_stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        const role = try dupeColumnText(allocator, column_stmt, 0);
        defer allocator.free(role);

        if (std.mem.eql(u8, role, "label") or std.mem.eql(u8, role, "status")) {
            signals.label_or_status_columns += 1;
        }
        if (std.mem.eql(u8, role, "status")) signals.status_columns += 1;
        if (std.mem.eql(u8, role, "fk")) signals.fk_columns += 1;
    }

    const relation_stmt = try prepare(db, "SELECT COUNT(*) FROM relation_semantics WHERE source_table_id = ?1 OR target_table_id = ?1");
    defer finalize(relation_stmt);
    try bindInt64(relation_stmt, 1, table_id);
    if (c.sqlite3_step(relation_stmt) == c.SQLITE_ROW) {
        signals.relation_count = @intCast(c.sqlite3_column_int64(relation_stmt, 0));
    }

    const mapping_stmt = try prepare(db, "SELECT source_kind FROM source_mappings WHERE target_table_id = ?1");
    defer finalize(mapping_stmt);
    try bindInt64(mapping_stmt, 1, table_id);
    while (true) {
        const rc = c.sqlite3_step(mapping_stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        signals.source_mapping_count += 1;
        const source_kind = try dupeColumnText(allocator, mapping_stmt, 0);
        defer allocator.free(source_kind);

        if (std.mem.indexOf(u8, source_kind, "facet") != null) signals.wants_facets = true;
        if (std.mem.indexOf(u8, source_kind, "graph") != null) signals.wants_graph = true;
        if (std.mem.indexOf(u8, source_kind, "projection") != null) signals.wants_projection = true;
    }

    const emit_facets = signals.wants_facets or signals.label_or_status_columns > 0;
    const emit_graph_relations = signals.wants_graph or signals.relation_count > 0 or signals.fk_columns > 0;
    const emit_graph_entities = emit_graph_relations or signals.wants_graph;
    const emit_projections = signals.wants_projection or has_vector_column or content_column.len > 0;
    const volume_driver = if (signals.source_mapping_count > 1 or signals.relation_count > 1)
        "high"
    else if (signals.source_mapping_count == 1 or signals.label_or_status_columns > 0)
        "medium"
    else
        "low";
    const generation_strategy = if (signals.source_mapping_count > 0)
        "source_mapped"
    else if (signals.relation_count > 0)
        "relational"
    else
        "static_ref";
    const notes_json = try std.fmt.allocPrint(
        allocator,
        "{{\"source_mapping_count\":{d},\"relation_count\":{d},\"label_or_status_columns\":{d},\"status_columns\":{d}}}",
        .{
            signals.source_mapping_count,
            signals.relation_count,
            signals.label_or_status_columns,
            signals.status_columns,
        },
    );

    return .{
        .emit_facets = emit_facets,
        .emit_graph_entities = emit_graph_entities,
        .emit_graph_relations = emit_graph_relations,
        .emit_projections = emit_projections,
        .volume_driver = try allocator.dupe(u8, volume_driver),
        .generation_strategy = try allocator.dupe(u8, generation_strategy),
        .notes_json = notes_json,
    };
}

fn deinitTableSignals(allocator: std.mem.Allocator, signals: TableSignals) void {
    allocator.free(signals.volume_driver);
    allocator.free(signals.generation_strategy);
    allocator.free(signals.notes_json);
}

fn loadSourceMappings(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8) ![]SourceMappingExport {
    const stmt = try prepare(db, "SELECT sm.source_key, sm.source_kind, ts.schema_name, ts.table_name, sm.metadata_json FROM source_mappings sm LEFT JOIN table_semantics ts ON ts.table_id = sm.target_table_id WHERE sm.workspace_id = ?1 ORDER BY sm.source_key");
    defer finalize(stmt);
    try bindText(stmt, 1, workspace_id);

    var rows = std.ArrayList(SourceMappingExport).empty;
    defer {
        for (rows.items) |row| deinitSourceMappingExport(allocator, row);
        rows.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        var target_table: ?[]const u8 = null;
        if (c.sqlite3_column_type(stmt, 2) != c.SQLITE_NULL and c.sqlite3_column_type(stmt, 3) != c.SQLITE_NULL) {
            const schema_name = try dupeColumnText(allocator, stmt, 2);
            defer allocator.free(schema_name);
            const table_name = try dupeColumnText(allocator, stmt, 3);
            defer allocator.free(table_name);
            target_table = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ schema_name, table_name });
        }
        errdefer if (target_table) |value| allocator.free(value);

        try rows.append(allocator, .{
            .source_key = try dupeColumnText(allocator, stmt, 0),
            .source_kind = try dupeColumnText(allocator, stmt, 1),
            .target_table = target_table,
            .metadata_json = try dupeColumnText(allocator, stmt, 4),
        });
    }

    return rows.toOwnedSlice(allocator);
}

fn buildGenerationHints(
    allocator: std.mem.Allocator,
    workspace: WorkspaceMetadata,
    tables: []const TableExport,
) !GenerationHints {
    var order = std.ArrayList([]const u8).empty;
    defer {
        for (order.items) |item| allocator.free(item);
        order.deinit(allocator);
    }

    var estimated_total_rows: usize = 0;
    for (tables) |table| {
        try order.append(allocator, try std.fmt.allocPrint(allocator, "{s}.{s}", .{ table.schema_name, table.table_name }));
        estimated_total_rows += switch (classifyVolumeDriver(table.volume_driver)) {
            .tiny => 20,
            .low => 200,
            .medium => 2_000,
            .high => 10_000,
        };
    }

    return .{
        .table_order = try order.toOwnedSlice(allocator),
        .estimated_total_rows = estimated_total_rows,
        .domain_profile = try allocator.dupe(u8, workspace.domain_profile),
        .time_window_days = 90,
    };
}

const VolumeClass = enum { tiny, low, medium, high };

fn classifyVolumeDriver(driver: []const u8) VolumeClass {
    if (std.mem.eql(u8, driver, "tiny")) return .tiny;
    if (std.mem.eql(u8, driver, "low")) return .low;
    if (std.mem.eql(u8, driver, "high")) return .high;
    return .medium;
}

fn inferTableRole(key_column: []const u8) []const u8 {
    if (std.mem.eql(u8, key_column, "id")) return "reference";
    return "association";
}

fn mapColumnRole(role: []const u8) []const u8 {
    if (std.mem.eql(u8, role, "id")) return "id";
    if (std.mem.eql(u8, role, "fk")) return "fk";
    if (std.mem.eql(u8, role, "status")) return "status";
    if (std.mem.eql(u8, role, "timestamp")) return "timestamp";
    return "label";
}

fn deinitWorkspaceMetadata(allocator: std.mem.Allocator, workspace: *WorkspaceMetadata) void {
    allocator.free(workspace.workspace_id);
    allocator.free(workspace.label);
    allocator.free(workspace.description);
    allocator.free(workspace.domain_profile);
    allocator.free(workspace.schema_name);
}

fn deinitTableExport(allocator: std.mem.Allocator, row: TableExport) void {
    allocator.free(row.schema_name);
    allocator.free(row.table_name);
    allocator.free(row.table_role);
    if (row.entity_family) |value| allocator.free(value);
    if (row.primary_time_column) |value| allocator.free(value);
    allocator.free(row.volume_driver);
    allocator.free(row.generation_strategy);
    allocator.free(row.notes_json);
}

fn deinitTableExports(allocator: std.mem.Allocator, rows: []TableExport) void {
    for (rows) |row| deinitTableExport(allocator, row);
    allocator.free(rows);
}

fn deinitColumnExport(allocator: std.mem.Allocator, row: ColumnExport) void {
    allocator.free(row.schema_name);
    allocator.free(row.table_name);
    allocator.free(row.column_name);
    allocator.free(row.column_role);
    if (row.semantic_type) |value| allocator.free(value);
    if (row.facet_key) |value| allocator.free(value);
    if (row.graph_usage) |value| allocator.free(value);
    if (row.projection_signal) |value| allocator.free(value);
}

fn deinitColumnExports(allocator: std.mem.Allocator, rows: []ColumnExport) void {
    for (rows) |row| deinitColumnExport(allocator, row);
    allocator.free(rows);
}

fn deinitRelationExport(allocator: std.mem.Allocator, row: RelationExport) void {
    allocator.free(row.source_schema);
    allocator.free(row.source_table);
    allocator.free(row.source_column);
    allocator.free(row.target_schema);
    allocator.free(row.target_table);
    allocator.free(row.target_column);
    if (row.relation_role) |value| allocator.free(value);
    if (row.graph_label) |value| allocator.free(value);
    allocator.free(row.cardinality);
}

fn deinitRelationExports(allocator: std.mem.Allocator, rows: []RelationExport) void {
    for (rows) |row| deinitRelationExport(allocator, row);
    allocator.free(rows);
}

fn deinitSourceMappingExport(allocator: std.mem.Allocator, row: SourceMappingExport) void {
    allocator.free(row.source_key);
    allocator.free(row.source_kind);
    if (row.target_table) |value| allocator.free(value);
    allocator.free(row.metadata_json);
}

fn deinitSourceMappingExports(allocator: std.mem.Allocator, rows: []SourceMappingExport) void {
    for (rows) |row| deinitSourceMappingExport(allocator, row);
    allocator.free(rows);
}

fn deinitGenerationHints(allocator: std.mem.Allocator, hints: GenerationHints) void {
    for (hints.table_order) |item| allocator.free(item);
    allocator.free(hints.table_order);
    allocator.free(hints.domain_profile);
}

fn prepare(db: Database, sql: []const u8) Error!*c.sqlite3_stmt {
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
        return error.PrepareFailed;
    }
    return stmt.?;
}

fn finalize(stmt: *c.sqlite3_stmt) void {
    _ = c.sqlite3_finalize(stmt);
}

fn stepDone(stmt: *c.sqlite3_stmt) Error!void {
    if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
}

fn bindInt64(stmt: *c.sqlite3_stmt, index: c_int, value: anytype) Error!void {
    if (c.sqlite3_bind_int64(stmt, index, @intCast(value)) != c.SQLITE_OK) return error.BindFailed;
}

fn bindText(stmt: *c.sqlite3_stmt, index: c_int, value: []const u8) Error!void {
    if (c.sqlite3_bind_text(stmt, index, value.ptr, @intCast(value.len), sqliteTransient()) != c.SQLITE_OK) return error.BindFailed;
}

// Zig 0.15 cross-compilation workaround: c.SQLITE_TRANSIENT is ((sqlite3_destructor_type)(-1)),
// but Zig's comptime pointer-alignment check rejects the -1 sentinel on aarch64.
// A volatile load makes the expression runtime-only, bypassing the comptime check.
fn sqliteTransient() c.sqlite3_destructor_type {
    var v: isize = -1;
    return @ptrFromInt(@as(usize, @bitCast((@as(*volatile isize, &v)).*)));
}

fn bindNull(stmt: *c.sqlite3_stmt, index: c_int) Error!void {
    if (c.sqlite3_bind_null(stmt, index) != c.SQLITE_OK) return error.BindFailed;
}

fn columnU64(stmt: *c.sqlite3_stmt, index: c_int) Error!u64 {
    const value = c.sqlite3_column_int64(stmt, index);
    return std.math.cast(u64, value) orelse error.ValueOutOfRange;
}

fn dupeColumnText(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, index: c_int) ![]const u8 {
    const len = c.sqlite3_column_bytes(stmt, index);
    const ptr = c.sqlite3_column_text(stmt, index) orelse return error.MissingRow;
    const bytes: []const u8 = @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
    return allocator.dupe(u8, bytes);
}

test "workspace export returns standalone workspace metadata and semantics" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try upsertWorkspace(db, "default", "{\"domain\":\"ghostcrab\"}");
    try upsertTableSemantic(db, 1, "default", "public", "documents", "id", "content", "metadata", "embedding", "english");
    try upsertColumnSemantic(db, 1, 1, "id", "id", "uuid", false);
    try upsertColumnSemantic(db, 2, 1, "content", "label", "text", true);
    try upsertColumnSemantic(db, 3, 1, "status", "status", "text", true);
    try upsertRelationSemantic(db, 1, "default", "documents_to_documents", 1, 1, "related_to", "{}");
    try upsertSourceMapping(db, 1, "default", "vault:documents", "facet_projection_graph", 1, "{\"source\":\"vault\"}");

    var model = try exportWorkspaceModel(db, std.testing.allocator, "default");
    defer deinitWorkspaceExport(std.testing.allocator, &model);

    try std.testing.expect(model.workspace != null);
    try std.testing.expectEqualStrings("default", model.workspace.?.workspace_id);
    try std.testing.expectEqual(@as(usize, 1), model.tables.len);
    try std.testing.expectEqualStrings("documents", model.tables[0].table_name);
    try std.testing.expect(model.tables[0].emit_facets);
    try std.testing.expect(model.tables[0].emit_graph_entities);
    try std.testing.expect(model.tables[0].emit_graph_relations);
    try std.testing.expect(model.tables[0].emit_projections);
    try std.testing.expect(std.mem.indexOf(u8, model.tables[0].notes_json, "\"source_mapping_count\":1") != null);
    try std.testing.expectEqual(@as(usize, 3), model.columns.len);
    try std.testing.expectEqual(@as(usize, 1), model.relations.len);
    try std.testing.expectEqual(@as(usize, 1), model.source_mappings.len);
    try std.testing.expectEqualStrings("vault:documents", model.source_mappings[0].source_key);
    try std.testing.expectEqualStrings("facet_projection_graph", model.source_mappings[0].source_kind);
    try std.testing.expectEqualStrings("public.documents", model.source_mappings[0].target_table.?);
    try std.testing.expectEqual(@as(usize, 1), model.generation_hints.table_order.len);
    try std.testing.expectEqualStrings("public.documents", model.generation_hints.table_order[0]);
}

test "workspace export reports missing workspaces" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    var model = try exportWorkspaceModel(db, std.testing.allocator, "missing");
    defer deinitWorkspaceExport(std.testing.allocator, &model);

    try std.testing.expect(model.workspace == null);
    try std.testing.expectEqual(@as(usize, 1), model.validation_warnings.len);
    try std.testing.expectEqual(@as(usize, 0), model.source_mappings.len);
}

test "workspace export has a TOON variant" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try upsertWorkspace(db, "default", "{\"domain\":\"ghostcrab\"}");
    try upsertTableSemantic(db, 1, "default", "public", "documents", "id", "content", "metadata", "embedding", "english");
    try upsertColumnSemantic(db, 1, 1, "id", "id", "uuid", false);
    try upsertColumnSemantic(db, 2, 1, "content", "label", "text", true);
    try upsertRelationSemantic(db, 1, "default", "documents_to_documents", 1, 1, "related_to", "{}");
    try upsertSourceMapping(db, 1, "default", "vault:documents", "facet_projection_graph", 1, "{\"source\":\"vault\"}");

    const toon = try exportWorkspaceModelToon(db, std.testing.allocator, "default");
    defer std.testing.allocator.free(toon);

    try std.testing.expect(std.mem.indexOf(u8, toon, "kind: workspace_export") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "schema_version: 1.0.0") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "workspace:") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "tables[1\t]{schema_name\ttable_name\ttable_role\tentity_family\tprimary_time_column\tvolume_driver\tgeneration_strategy\temit_facets\temit_graph_entities\temit_graph_relations\temit_projections\tnotes_json}:") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "public\tdocuments\treference") != null);
}

test "workspace export resolves by domain" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try upsertWorkspace(db, "default", "{\"domain\":\"ghostcrab\"}");
    try upsertTableSemantic(db, 1, "default", "public", "documents", "id", "content", "metadata", "embedding", "english");

    const model = try exportWorkspaceModelByDomain(db, std.testing.allocator, "ghostcrab");
    try std.testing.expect(model != null);
    var owned = model.?;
    defer deinitWorkspaceExport(std.testing.allocator, &owned);
    try std.testing.expectEqualStrings("default", owned.workspace.?.workspace_id);

    const toon = try exportWorkspaceModelToonByDomain(db, std.testing.allocator, "ghostcrab");
    try std.testing.expect(toon != null);
    defer std.testing.allocator.free(toon.?);
    try std.testing.expect(std.mem.indexOf(u8, toon.?, "kind: workspace_export") != null);
}
