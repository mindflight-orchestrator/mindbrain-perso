const std = @import("std");

const facet_sqlite = @import("facet_sqlite.zig");
const nanoid = @import("nanoid.zig");

const c = facet_sqlite.c;

pub const FactWrite = struct {
    id: ?[]const u8 = null,
    workspace_id: []const u8 = "default",
    schema_id: []const u8,
    content: []const u8,
    facets_json: []const u8 = "{}",
    embedding_blob: ?[]const u8 = null,
    created_by: ?[]const u8 = null,
    valid_from_unix: ?i64 = null,
    valid_until_unix: ?i64 = null,
    source_ref: ?[]const u8 = null,
};

pub const FactWriteResult = struct {
    id: []const u8,
    doc_id: u64,
    created: bool,
    updated: bool,
};

pub fn deinitFactWriteResult(allocator: std.mem.Allocator, result: FactWriteResult) void {
    allocator.free(result.id);
}

pub fn validateFacetsJsonObject(allocator: std.mem.Allocator, text: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, text, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidFacetsJson;
}

pub fn writeFact(
    db: facet_sqlite.Database,
    allocator: std.mem.Allocator,
    write: FactWrite,
) !FactWriteResult {
    if (write.schema_id.len == 0 or write.content.len == 0) return error.InvalidFactWrite;
    if (write.workspace_id.len == 0) return error.InvalidFactWrite;
    if (write.source_ref) |source_ref| {
        if (source_ref.len == 0) return error.InvalidFactWrite;
    }
    try validateFacetsJsonObject(allocator, write.facets_json);

    try db.exec("BEGIN IMMEDIATE");
    var transaction_active = true;
    errdefer if (transaction_active) {
        db.exec("ROLLBACK") catch {};
    };

    if (write.source_ref) |source_ref| {
        const updated = try updateFactBySourceRef(db, write, source_ref);
        if (updated) {
            const selected = try selectFactBySourceRef(db, allocator, write.workspace_id, source_ref);
            try commitTransaction(db, &transaction_active);
            return .{
                .id = selected.id,
                .doc_id = selected.doc_id,
                .created = false,
                .updated = true,
            };
        }
    }

    const id = if (write.id) |provided|
        try allocator.dupe(u8, provided)
    else
        try nanoid.generateDefault(allocator);
    errdefer allocator.free(id);

    try insertFact(db, write, id);
    const doc_id = try selectDocIdById(db, id);
    try commitTransaction(db, &transaction_active);
    return .{
        .id = id,
        .doc_id = doc_id,
        .created = true,
        .updated = false,
    };
}

fn commitTransaction(db: facet_sqlite.Database, transaction_active: *bool) !void {
    db.exec("COMMIT") catch |commit_err| {
        db.exec("ROLLBACK") catch {};
        transaction_active.* = false;
        return commit_err;
    };
    transaction_active.* = false;
}

fn updateFactBySourceRef(db: facet_sqlite.Database, write: FactWrite, source_ref: []const u8) !bool {
    const sql =
        \\UPDATE facets
        \\SET schema_id = ?1,
        \\    content = ?2,
        \\    facets = ?3,
        \\    facets_json = ?3,
        \\    embedding_blob = ?4,
        \\    created_by = COALESCE(?5, created_by),
        \\    updated_at = CURRENT_TIMESTAMP,
        \\    updated_at_unix = unixepoch(),
        \\    version = version + 1,
        \\    valid_from_unix = ?6,
        \\    valid_until_unix = ?7
        \\WHERE workspace_id = ?8 AND source_ref = ?9
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);

    try bindFactPayload(stmt, write);
    try facet_sqlite.bindText(stmt, 8, write.workspace_id);
    try facet_sqlite.bindText(stmt, 9, source_ref);
    try facet_sqlite.stepDone(stmt);
    return c.sqlite3_changes(db.handle) > 0;
}

fn insertFact(db: facet_sqlite.Database, write: FactWrite, id: []const u8) !void {
    const sql =
        \\INSERT INTO facets(
        \\    id,
        \\    schema_id,
        \\    content,
        \\    facets,
        \\    facets_json,
        \\    embedding_blob,
        \\    created_by,
        \\    created_at_unix,
        \\    updated_at_unix,
        \\    valid_from_unix,
        \\    valid_until_unix,
        \\    workspace_id,
        \\    source_ref,
        \\    doc_id
        \\)
        \\VALUES (
        \\    ?1,
        \\    ?2,
        \\    ?3,
        \\    ?4,
        \\    ?4,
        \\    ?5,
        \\    ?6,
        \\    unixepoch(),
        \\    unixepoch(),
        \\    ?7,
        \\    ?8,
        \\    ?9,
        \\    ?10,
        \\    (SELECT COALESCE(MAX(doc_id), 0) + 1 FROM facets)
        \\)
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);

    try facet_sqlite.bindText(stmt, 1, id);
    try facet_sqlite.bindText(stmt, 2, write.schema_id);
    try facet_sqlite.bindText(stmt, 3, write.content);
    try facet_sqlite.bindText(stmt, 4, write.facets_json);
    try bindOptionalBlob(stmt, 5, write.embedding_blob);
    try bindOptionalText(stmt, 6, write.created_by);
    try bindOptionalInt64(stmt, 7, write.valid_from_unix);
    try bindOptionalInt64(stmt, 8, write.valid_until_unix);
    try facet_sqlite.bindText(stmt, 9, write.workspace_id);
    try bindOptionalText(stmt, 10, write.source_ref);
    try facet_sqlite.stepDone(stmt);
}

fn bindFactPayload(stmt: *c.sqlite3_stmt, write: FactWrite) !void {
    try facet_sqlite.bindText(stmt, 1, write.schema_id);
    try facet_sqlite.bindText(stmt, 2, write.content);
    try facet_sqlite.bindText(stmt, 3, write.facets_json);
    try bindOptionalBlob(stmt, 4, write.embedding_blob);
    try bindOptionalText(stmt, 5, write.created_by);
    try bindOptionalInt64(stmt, 6, write.valid_from_unix);
    try bindOptionalInt64(stmt, 7, write.valid_until_unix);
}

fn bindOptionalText(stmt: *c.sqlite3_stmt, index: c_int, value: ?[]const u8) !void {
    if (value) |text| {
        try facet_sqlite.bindText(stmt, index, text);
    } else {
        try facet_sqlite.bindNull(stmt, index);
    }
}

fn bindOptionalBlob(stmt: *c.sqlite3_stmt, index: c_int, value: ?[]const u8) !void {
    if (value) |bytes| {
        try facet_sqlite.bindBlob(stmt, index, bytes);
    } else {
        try facet_sqlite.bindNull(stmt, index);
    }
}

fn bindOptionalInt64(stmt: *c.sqlite3_stmt, index: c_int, value: ?i64) !void {
    if (value) |number| {
        try facet_sqlite.bindInt64(stmt, index, number);
    } else {
        try facet_sqlite.bindNull(stmt, index);
    }
}

const SelectedFact = struct {
    id: []const u8,
    doc_id: u64,
};

fn selectFactBySourceRef(
    db: facet_sqlite.Database,
    allocator: std.mem.Allocator,
    workspace_id: []const u8,
    source_ref: []const u8,
) !SelectedFact {
    const sql =
        \\SELECT id, doc_id
        \\FROM facets
        \\WHERE workspace_id = ?1 AND source_ref = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);

    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, source_ref);

    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;
    return .{
        .id = try facet_sqlite.dupeColumnText(allocator, stmt, 0),
        .doc_id = try facet_sqlite.columnU64(stmt, 1),
    };
}

fn selectDocIdById(db: facet_sqlite.Database, id: []const u8) !u64 {
    const sql = "SELECT doc_id FROM facets WHERE id = ?1";
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);

    try facet_sqlite.bindText(stmt, 1, id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;
    return try facet_sqlite.columnU64(stmt, 0);
}

test "writeFact appends unsourced facts with backend doc_id allocation" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    const first = try writeFact(db, std.testing.allocator, .{
        .schema_id = "ghostcrab.fact",
        .content = "first",
        .facets_json = "{\"kind\":\"note\"}",
    });
    defer deinitFactWriteResult(std.testing.allocator, first);

    const second = try writeFact(db, std.testing.allocator, .{
        .schema_id = "ghostcrab.fact",
        .content = "second",
        .facets_json = "{\"kind\":\"note\"}",
    });
    defer deinitFactWriteResult(std.testing.allocator, second);

    try std.testing.expect(first.created);
    try std.testing.expect(!first.updated);
    try std.testing.expect(second.created);
    try std.testing.expectEqual(first.doc_id + 1, second.doc_id);
}

test "writeFact upserts sourced facts by workspace and source_ref" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    const first = try writeFact(db, std.testing.allocator, .{
        .workspace_id = "ws",
        .schema_id = "ghostcrab.fact",
        .content = "first",
        .facets_json = "{\"kind\":\"state\"}",
        .source_ref = "sync:state:1",
    });
    defer deinitFactWriteResult(std.testing.allocator, first);

    const second = try writeFact(db, std.testing.allocator, .{
        .workspace_id = "ws",
        .schema_id = "ghostcrab.fact",
        .content = "updated",
        .facets_json = "{\"kind\":\"state\",\"status\":\"new\"}",
        .source_ref = "sync:state:1",
    });
    defer deinitFactWriteResult(std.testing.allocator, second);

    try std.testing.expect(first.created);
    try std.testing.expect(second.updated);
    try std.testing.expectEqual(first.doc_id, second.doc_id);
    try std.testing.expectEqualStrings(first.id, second.id);
}

test "writeFact rejects non-object facets_json" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try std.testing.expectError(error.InvalidFacetsJson, writeFact(db, std.testing.allocator, .{
        .schema_id = "ghostcrab.fact",
        .content = "invalid",
        .facets_json = "[]",
    }));
}

test "writeFact allows same source_ref in separate workspaces" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    const first = try writeFact(db, std.testing.allocator, .{
        .workspace_id = "ws-a",
        .schema_id = "ghostcrab.fact",
        .content = "workspace a",
        .source_ref = "sync:shared",
    });
    defer deinitFactWriteResult(std.testing.allocator, first);

    const second = try writeFact(db, std.testing.allocator, .{
        .workspace_id = "ws-b",
        .schema_id = "ghostcrab.fact",
        .content = "workspace b",
        .source_ref = "sync:shared",
    });
    defer deinitFactWriteResult(std.testing.allocator, second);

    try std.testing.expect(first.created);
    try std.testing.expect(second.created);
    try std.testing.expect(!std.mem.eql(u8, first.id, second.id));
    try std.testing.expect(first.doc_id != second.doc_id);
}

test "writeFact rapid sequential inserts never reuse doc_id" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    var previous_doc_id: u64 = 0;
    for (0..32) |index| {
        const content = try std.fmt.allocPrint(std.testing.allocator, "fact {}", .{index});
        defer std.testing.allocator.free(content);

        const result = try writeFact(db, std.testing.allocator, .{
            .schema_id = "ghostcrab.fact",
            .content = content,
        });
        defer deinitFactWriteResult(std.testing.allocator, result);

        try std.testing.expect(result.created);
        try std.testing.expect(result.doc_id > previous_doc_id);
        previous_doc_id = result.doc_id;
    }
}

test "writeFact preserves provided id and keeps sourced id on update" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    const created = try writeFact(db, std.testing.allocator, .{
        .id = "fact:provided",
        .workspace_id = "ws",
        .schema_id = "ghostcrab.fact",
        .content = "created",
        .source_ref = "sync:provided",
    });
    defer deinitFactWriteResult(std.testing.allocator, created);

    const updated = try writeFact(db, std.testing.allocator, .{
        .id = "fact:ignored-on-update",
        .workspace_id = "ws",
        .schema_id = "ghostcrab.fact",
        .content = "updated",
        .source_ref = "sync:provided",
    });
    defer deinitFactWriteResult(std.testing.allocator, updated);

    try std.testing.expectEqualStrings("fact:provided", created.id);
    try std.testing.expectEqualStrings("fact:provided", updated.id);
    try std.testing.expectEqual(created.doc_id, updated.doc_id);
    try std.testing.expect(updated.updated);
}

test "standalone schema allocates legacy facets doc_id and has source_ref indexes" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    {
        try db.exec("INSERT INTO facets (schema_id, content, facets, workspace_id) VALUES ('legacy:test', 'legacy raw insert', '{}', 'default')");
        const stmt = try facet_sqlite.prepare(db, "SELECT COUNT(*) FROM facets WHERE schema_id = 'legacy:test' AND doc_id IS NOT NULL");
        defer facet_sqlite.finalize(stmt);

        if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StepFailed;
        try std.testing.expectEqual(@as(i64, 1), c.sqlite3_column_int64(stmt, 0));
    }

    {
        const stmt = try facet_sqlite.prepare(db, "PRAGMA index_list(facets)");
        defer facet_sqlite.finalize(stmt);

        var found_canonical = false;
        var found_legacy_compat = false;
        while (true) {
            const rc = c.sqlite3_step(stmt);
            if (rc == c.SQLITE_DONE) break;
            if (rc != c.SQLITE_ROW) return error.StepFailed;

            const name = try facet_sqlite.dupeColumnText(std.testing.allocator, stmt, 1);
            defer std.testing.allocator.free(name);
            if (std.mem.eql(u8, name, "facets_source_ref_workspace_uniq")) found_canonical = true;
            if (std.mem.eql(u8, name, "idx_facets_source_ref_workspace")) found_legacy_compat = true;
        }
        try std.testing.expect(found_canonical);
        try std.testing.expect(found_legacy_compat);
    }
}
