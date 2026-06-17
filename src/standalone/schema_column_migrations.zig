const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const manifest = @import("schema_column_manifest.generated.zig");

pub const mindbrain_version = "1.7.3";

const Database = facet_sqlite.Database;
const c = facet_sqlite.c;
const Error = facet_sqlite.Error;

pub const MissingColumn = struct {
    table: []const u8,
    column: []const u8,
};

pub fn applyAdditiveColumnMigrations(db: Database) Error!void {
    try db.exec(
        \\CREATE TABLE IF NOT EXISTS mindbrain_schema_migrations (
        \\    id TEXT PRIMARY KEY,
        \\    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        \\);
    );

    for (manifest.column_manifest) |entry| {
        if (!(try tableExists(db, entry.table))) continue;
        if (try columnExists(db, entry.table, entry.column)) continue;
        if (try migrationApplied(db, entry.migration_id)) continue;

        const sql = std.fmt.allocPrint(
            std.heap.page_allocator,
            "ALTER TABLE {s} ADD COLUMN {s} {s}",
            .{ entry.table, entry.column, entry.ddl },
        ) catch return error.ExecFailed;
        defer std.heap.page_allocator.free(sql);

        try db.exec(sql);
        try markMigrationApplied(db, entry.migration_id);
        std.debug.print("[mindbrain] schema column added: {s}.{s}\n", .{
            entry.table,
            entry.column,
        });
    }
}

pub fn findMissingColumns(allocator: std.mem.Allocator, db: Database) Error![]MissingColumn {
    var missing = std.ArrayList(MissingColumn).empty;
    errdefer {
        for (missing.items) |item| {
            allocator.free(item.table);
            allocator.free(item.column);
        }
        missing.deinit(allocator);
    }

    for (manifest.column_manifest) |entry| {
        if (!(try tableExists(db, entry.table))) continue;
        if (try columnExists(db, entry.table, entry.column)) continue;
        missing.append(allocator, .{
            .table = allocator.dupe(u8, entry.table) catch return error.ExecFailed,
            .column = allocator.dupe(u8, entry.column) catch return error.ExecFailed,
        }) catch return error.ExecFailed;
    }

    return missing.toOwnedSlice(allocator) catch return error.ExecFailed;
}

pub fn schemaTablesCount(db: Database) Error!u64 {
    const stmt = try facet_sqlite.prepare(
        db,
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    );
    defer facet_sqlite.finalize(stmt);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StepFailed;
    return @intCast(c.sqlite3_column_int64(stmt, 0));
}

pub fn listAppliedMigrations(allocator: std.mem.Allocator, db: Database) Error![]const []const u8 {
    const stmt = try facet_sqlite.prepare(
        db,
        "SELECT id FROM mindbrain_schema_migrations ORDER BY applied_at, id",
    );
    defer facet_sqlite.finalize(stmt);

    var ids = std.ArrayList([]const u8).empty;
    errdefer {
        for (ids.items) |id| allocator.free(id);
        ids.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        const id = facet_sqlite.dupeColumnText(allocator, stmt, 0) catch return error.ExecFailed;
        ids.append(allocator, id) catch return error.ExecFailed;
    }

    return ids.toOwnedSlice(allocator) catch return error.ExecFailed;
}

fn tableExists(db: Database, table_name: []const u8) Error!bool {
    const stmt = try facet_sqlite.prepare(
        db,
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1 LIMIT 1",
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, table_name);
    return c.sqlite3_step(stmt) == c.SQLITE_ROW;
}

pub fn columnExists(db: Database, table_name: []const u8, column_name: []const u8) Error!bool {
    const sql = std.fmt.allocPrint(
        std.heap.page_allocator,
        "PRAGMA table_info({s})",
        .{table_name},
    ) catch return error.ExecFailed;
    defer std.heap.page_allocator.free(sql);

    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        const name = std.mem.span(c.sqlite3_column_text(stmt, 1) orelse continue);
        if (std.mem.eql(u8, name, column_name)) return true;
    }
    return false;
}

fn migrationApplied(db: Database, migration_id: []const u8) Error!bool {
    const stmt = try facet_sqlite.prepare(
        db,
        "SELECT 1 FROM mindbrain_schema_migrations WHERE id = ?1 LIMIT 1",
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, migration_id);
    return c.sqlite3_step(stmt) == c.SQLITE_ROW;
}

fn markMigrationApplied(db: Database, migration_id: []const u8) Error!void {
    const stmt = try facet_sqlite.prepare(
        db,
        "INSERT OR IGNORE INTO mindbrain_schema_migrations (id) VALUES (?1)",
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, migration_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
}

test "additive column migration adds missing documents_raw.summary" {
    var db = try Database.openInMemory();
    defer db.close();

    try db.exec(
        \\CREATE TABLE documents_raw (
        \\    workspace_id TEXT NOT NULL,
        \\    collection_id TEXT NOT NULL,
        \\    doc_id INTEGER NOT NULL,
        \\    doc_nanoid TEXT NOT NULL DEFAULT '',
        \\    content TEXT NOT NULL DEFAULT '',
        \\    language TEXT,
        \\    source_ref TEXT,
        \\    metadata_json TEXT NOT NULL DEFAULT '{}',
        \\    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        \\    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        \\    PRIMARY KEY(workspace_id, collection_id, doc_id)
        \\);
    );

    try std.testing.expect(!try columnExists(db, "documents_raw", "summary"));
    try applyAdditiveColumnMigrations(db);
    try std.testing.expect(try columnExists(db, "documents_raw", "summary"));
}

test "findMissingColumns reports absent columns on legacy tables" {
    var db = try Database.openInMemory();
    defer db.close();

    try db.exec(
        \\CREATE TABLE documents_raw (
        \\    workspace_id TEXT NOT NULL,
        \\    collection_id TEXT NOT NULL,
        \\    doc_id INTEGER NOT NULL,
        \\    content TEXT NOT NULL DEFAULT '{}',
        \\    PRIMARY KEY(workspace_id, collection_id, doc_id)
        \\);
    );

    const missing = try findMissingColumns(std.testing.allocator, db);
    defer {
        for (missing) |item| {
            std.testing.allocator.free(item.table);
            std.testing.allocator.free(item.column);
        }
        std.testing.allocator.free(missing);
    }

    var found_summary = false;
    for (missing) |item| {
        if (std.mem.eql(u8, item.table, "documents_raw") and std.mem.eql(u8, item.column, "summary")) {
            found_summary = true;
        }
    }
    try std.testing.expect(found_summary);
}
