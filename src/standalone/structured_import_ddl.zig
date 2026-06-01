//! Physical ws_* DDL for structured import (Phase 3D).
const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const search_sqlite = @import("search_sqlite.zig");
const structured_import = @import("structured_import.zig");
const workspace_sqlite = @import("workspace_sqlite.zig");

pub const DdlProposeReport = struct {
    sql: []const u8,
    tables: u64 = 0,
    indexes: u64 = 0,
};

pub const DdlExecuteReport = struct {
    statements_executed: u64 = 0,
};

pub const LoadWsReport = struct {
    rows_loaded: u64 = 0,
    tables_loaded: u64 = 0,
};

pub fn ddlPropose(allocator: std.mem.Allocator, db: facet_sqlite.Database, workspace_id: []const u8) !DdlProposeReport {
    const sql =
        \\SELECT table_id, table_name, key_column
        \\FROM table_semantics
        \\WHERE workspace_id = ?1 AND table_schema = 'structured'
        \\ORDER BY table_name
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    var tables: u64 = 0;
    var indexes: u64 = 0;

    while (true) {
        const rc = facet_sqlite.c.sqlite3_step(stmt);
        if (rc == facet_sqlite.c.SQLITE_DONE) break;
        if (rc != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;
        const table_id = try facet_sqlite.columnU64(stmt, 0);
        const table_name = try facet_sqlite.dupeColumnText(allocator, stmt, 1);
        defer allocator.free(table_name);
        const key_column = try facet_sqlite.dupeColumnText(allocator, stmt, 2);
        defer allocator.free(key_column);

        const ws_name = try wsTableName(allocator, table_name);
        defer allocator.free(ws_name);

        if (tables > 0) try out.append(allocator, '\n');
        try out.appendSlice(allocator, "CREATE TABLE IF NOT EXISTS ");
        try out.appendSlice(allocator, ws_name);
        try out.appendSlice(allocator, " (\n  ");
        try out.appendSlice(allocator, key_column);
        try out.appendSlice(allocator, " TEXT PRIMARY KEY");

        const columns = try loadColumnSemantics(allocator, db, workspace_id, table_name);
        defer {
            for (columns) |col| allocator.free(col.name);
            allocator.free(columns);
        }
        for (columns) |col| {
            if (std.mem.eql(u8, col.name, key_column)) continue;
            try out.appendSlice(allocator, ",\n  ");
            try out.appendSlice(allocator, col.name);
            try out.appendSlice(allocator, " TEXT");
        }
        try out.appendSlice(allocator, "\n);\n");
        try out.appendSlice(allocator, "CREATE INDEX IF NOT EXISTS idx_");
        try out.appendSlice(allocator, ws_name[4..]);
        try out.appendSlice(allocator, "_pk ON ");
        try out.appendSlice(allocator, ws_name);
        try out.appendSlice(allocator, "(");
        try out.appendSlice(allocator, key_column);
        try out.appendSlice(allocator, ");");
        tables += 1;
        indexes += 1;

        for (columns) |col| {
            if (!std.mem.eql(u8, col.role, "fk")) continue;
            try out.append(allocator, '\n');
            try out.appendSlice(allocator, "CREATE INDEX IF NOT EXISTS idx_");
            try out.appendSlice(allocator, ws_name[4..]);
            try out.appendSlice(allocator, "_");
            try out.appendSlice(allocator, col.name);
            try out.appendSlice(allocator, " ON ");
            try out.appendSlice(allocator, ws_name);
            try out.appendSlice(allocator, "(");
            try out.appendSlice(allocator, col.name);
            try out.appendSlice(allocator, ");");
            indexes += 1;
        }
        _ = table_id;
    }

    return .{
        .sql = try out.toOwnedSlice(allocator),
        .tables = tables,
        .indexes = indexes,
    };
}

const ColumnSpec = struct {
    name: []const u8,
    role: []const u8,
};

fn loadColumnSemantics(allocator: std.mem.Allocator, db: facet_sqlite.Database, workspace_id: []const u8, table_name: []const u8) ![]ColumnSpec {
    const sql =
        \\SELECT column_name, column_role
        \\FROM column_semantics
        \\WHERE workspace_id = ?1 AND table_schema = 'structured' AND table_name = ?2
        \\ORDER BY column_name
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, table_name);
    var out = std.ArrayList(ColumnSpec).empty;
    errdefer {
        for (out.items) |col| allocator.free(col.name);
        out.deinit(allocator);
    }
    while (true) {
        const rc = facet_sqlite.c.sqlite3_step(stmt);
        if (rc == facet_sqlite.c.SQLITE_DONE) break;
        if (rc != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;
        try out.append(allocator, .{
            .name = try facet_sqlite.dupeColumnText(allocator, stmt, 0),
            .role = try facet_sqlite.dupeColumnText(allocator, stmt, 1),
        });
    }
    return try out.toOwnedSlice(allocator);
}

pub fn ddlExecute(_: std.mem.Allocator, db: facet_sqlite.Database, sql: []const u8) !DdlExecuteReport {
    var report = DdlExecuteReport{};
    var iter = std.mem.splitSequence(u8, sql, ";\n");
    while (iter.next()) |chunk| {
        const trimmed = std.mem.trim(u8, chunk, " \t\r\n");
        if (trimmed.len == 0) continue;
        var lower = std.ArrayList(u8).empty;
        defer lower.deinit(std.heap.page_allocator);
        for (trimmed) |c| try lower.append(std.heap.page_allocator, std.ascii.toLower(c));
        if (std.mem.indexOf(u8, lower.items, "drop ") != null) return error.DestructiveSqlRejected;
        if (std.mem.indexOf(u8, lower.items, "truncate ") != null) return error.DestructiveSqlRejected;
        try db.exec(trimmed);
        report.statements_executed += 1;
    }
    return report;
}

pub fn loadWsFromBundle(
    allocator: std.mem.Allocator,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    bundle: structured_import.TabularBundle,
    mode: structured_import.ImportMode,
) !LoadWsReport {
    var report = LoadWsReport{};
    for (bundle.tables) |named| {
        const ws_name = try wsTableName(allocator, named.name);
        defer allocator.free(ws_name);
        if (mode == .reset) {
            const drop_sql = try std.fmt.allocPrint(allocator, "DELETE FROM {s}", .{ws_name});
            defer allocator.free(drop_sql);
            db.exec(drop_sql) catch {};
        }
        const key_column_owned = try lookupKeyColumn(allocator, db, workspace_id, named.name);
        defer if (key_column_owned) |k| allocator.free(k);
        const key_column = key_column_owned orelse "record_id";
        for (named.table.rows) |row| {
            try upsertWsRow(allocator, db, ws_name, key_column, named.table.headers, row);
            report.rows_loaded += 1;
        }
        report.tables_loaded += 1;
    }
    return report;
}

pub fn loadWsFromPath(
    allocator: std.mem.Allocator,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    mapping_path: []const u8,
    input_dir: ?[]const u8,
    mode: structured_import.ImportMode,
) !LoadWsReport {
    const mapping_text = try structured_import.readJsonFile(allocator, mapping_path);
    defer allocator.free(mapping_text);
    var mapping = try std.json.parseFromSlice(std.json.Value, allocator, mapping_text, .{});
    defer mapping.deinit();
    const base_dir = input_dir orelse (std.fs.path.dirname(mapping_path) orelse ".");
    var bundle = try readTabularBundleFromMappingBase(allocator, base_dir, mapping.value);
    defer bundle.deinit(allocator);
    return try loadWsFromBundle(allocator, db, workspace_id, bundle, mode);
}

fn readTabularBundleFromMappingBase(
    allocator: std.mem.Allocator,
    base_dir: []const u8,
    mapping: std.json.Value,
) !structured_import.TabularBundle {
    if (mapping != .object) return error.InvalidMapping;
    const entities = mapping.object.get("entities") orelse return error.InvalidMapping;
    if (entities != .object) return error.InvalidMapping;
    var tables = std.ArrayList(structured_import.NamedTable).empty;
    errdefer {
        for (tables.items) |named| named.deinit(allocator);
        tables.deinit(allocator);
    }
    for (entities.object.keys()) |entity_name| {
        const entity_val = entities.object.get(entity_name) orelse continue;
        if (entity_val != .object) continue;
        const csv_rel = entity_val.object.get("csv") orelse continue;
        if (csv_rel != .string) continue;
        const csv_path = try std.fs.path.join(allocator, &.{ base_dir, csv_rel.string });
        defer allocator.free(csv_path);
        const table = try structured_import.readTableFile(allocator, csv_path);
        try tables.append(allocator, .{
            .name = try allocator.dupe(u8, entity_name),
            .table = table,
        });
    }
    if (tables.items.len == 0) return error.InvalidMapping;
    return .{ .tables = try tables.toOwnedSlice(allocator) };
}

pub fn setupWsSearchTables(allocator: std.mem.Allocator, db: facet_sqlite.Database, workspace_id: []const u8) !u64 {
    const sql =
        \\SELECT table_id, table_name, key_column, content_column, metadata_column, language, emit_facets
        \\FROM table_semantics
        \\WHERE workspace_id = ?1 AND table_schema = 'structured' AND emit_facets = 1
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var configured: u64 = 0;
    while (true) {
        const rc = facet_sqlite.c.sqlite3_step(stmt);
        if (rc == facet_sqlite.c.SQLITE_DONE) break;
        if (rc != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;
        const table_id = try facet_sqlite.columnU64(stmt, 0);
        const table_name = try facet_sqlite.dupeColumnText(allocator, stmt, 1);
        defer allocator.free(table_name);
        const key_column = try facet_sqlite.dupeColumnText(allocator, stmt, 2);
        defer allocator.free(key_column);
        const content_column = try facet_sqlite.dupeColumnText(allocator, stmt, 3);
        defer allocator.free(content_column);
        const metadata_column = try facet_sqlite.dupeColumnText(allocator, stmt, 4);
        defer allocator.free(metadata_column);
        const language = try facet_sqlite.dupeColumnText(allocator, stmt, 5);
        defer allocator.free(language);
        const ws_name = try wsTableName(allocator, table_name);
        defer allocator.free(ws_name);
        try search_sqlite.setupSearchTable(db, allocator, .{
            .table_id = table_id,
            .workspace_id = workspace_id,
            .schema_name = "ws",
            .table_name = ws_name,
            .key_column = key_column,
            .content_column = content_column,
            .metadata_column = metadata_column,
            .language = language,
            .populate = false,
        });
        configured += 1;
    }
    return configured;
}

fn lookupKeyColumn(allocator: std.mem.Allocator, db: facet_sqlite.Database, workspace_id: []const u8, table_name: []const u8) !?[]const u8 {
    const sql = "SELECT key_column FROM table_semantics WHERE workspace_id = ?1 AND table_schema = 'structured' AND table_name = ?2 LIMIT 1";
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, table_name);
    if (facet_sqlite.c.sqlite3_step(stmt) != facet_sqlite.c.SQLITE_ROW) return null;
    return try facet_sqlite.dupeColumnText(allocator, stmt, 0);
}

fn upsertWsRow(
    allocator: std.mem.Allocator,
    db: facet_sqlite.Database,
    ws_name: []const u8,
    key_column: []const u8,
    headers: []const []const u8,
    row: []const []const u8,
) !void {
    const key_idx = blk: {
        for (headers, 0..) |header, idx| {
            if (std.mem.eql(u8, header, key_column)) break :blk idx;
        }
        return error.MissingPrimaryKey;
    };
    if (key_idx >= row.len or row[key_idx].len == 0) return error.MissingPrimaryKey;

    var columns = std.ArrayList([]const u8).empty;
    defer columns.deinit(allocator);
    var values = std.ArrayList([]const u8).empty;
    defer values.deinit(allocator);
    for (headers, 0..) |header, idx| {
        if (idx >= row.len) continue;
        try columns.append(allocator, header);
        try values.append(allocator, row[idx]);
    }

    var sql = std.ArrayList(u8).empty;
    defer sql.deinit(allocator);
    try sql.appendSlice(allocator, "INSERT OR REPLACE INTO ");
    try sql.appendSlice(allocator, ws_name);
    try sql.append(allocator, '(');
    for (columns.items, 0..) |col, idx| {
        if (idx > 0) try sql.append(allocator, ',');
        try sql.appendSlice(allocator, col);
    }
    try sql.appendSlice(allocator, ") VALUES (");
    for (0..columns.items.len) |idx| {
        if (idx > 0) try sql.append(allocator, ',');
        try sql.appendSlice(allocator, "?");
    }
    try sql.append(allocator, ')');

    const stmt = try facet_sqlite.prepare(db, sql.items);
    defer facet_sqlite.finalize(stmt);
    for (values.items, 0..) |value, idx| {
        try facet_sqlite.bindText(stmt, @intCast(idx + 1), value);
    }
    if (facet_sqlite.c.sqlite3_step(stmt) != facet_sqlite.c.SQLITE_DONE) return error.StepFailed;
}

fn wsTableName(allocator: std.mem.Allocator, entity_name: []const u8) ![]const u8 {
    return try std.fmt.allocPrint(allocator, "ws_{s}", .{entity_name});
}

pub fn readDataPlane(mapping_path: []const u8, allocator: std.mem.Allocator) ![]const u8 {
    return structured_import.readDataPlane(mapping_path, allocator);
}
