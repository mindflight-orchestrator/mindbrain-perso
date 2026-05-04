const std = @import("std");
const interfaces = @import("interfaces.zig");
const roaring = @import("roaring.zig");
const sqlite_schema = @import("sqlite_schema.zig");

const bm25_stopwords_seed_sql = @embedFile("seed_bm25_stopwords.sql");

pub const c = @cImport({
    @cInclude("sqlite3.h");
});

pub const Error = error{
    OpenFailed,
    ExecFailed,
    PrepareFailed,
    StepFailed,
    BindFailed,
    MissingRow,
    ValueOutOfRange,
};

pub const FacetDefinitionSpec = struct {
    facet_id: u32,
    facet_name: []const u8,
};

pub const FacetDefinitionRecord = struct {
    facet_id: u32,
    facet_name: []const u8,
};

pub const FacetDeltaRecord = struct {
    table_id: u64,
    facet_id: u32,
    facet_value: []const u8,
    posting: u64,
    delta: i64,
};

pub const FacetDocumentAssignment = struct {
    facet_id: u32,
    facet_value: []const u8,
};

pub const FacetTableSummary = struct {
    table_id: u64,
    schema_name: []const u8,
    table_name: []const u8,
    chunk_bits: u8,
    facet_count: u64,
    posting_count: u64,
    value_node_count: u64,
};

const DeltaKey = struct {
    facet_id: u32,
    chunk_id: u32,
    facet_value: []const u8,
};

const DeltaValue = struct {
    add: ?roaring.Bitmap = null,
    remove: ?roaring.Bitmap = null,
};

const DeltaKeyContext = struct {
    pub fn hash(_: @This(), key: DeltaKey) u64 {
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(std.mem.asBytes(&key.facet_id));
        hasher.update(std.mem.asBytes(&key.chunk_id));
        hasher.update(key.facet_value);
        return hasher.final();
    }

    pub fn eql(_: @This(), a: DeltaKey, b: DeltaKey) bool {
        return a.facet_id == b.facet_id and a.chunk_id == b.chunk_id and std.mem.eql(u8, a.facet_value, b.facet_value);
    }
};

const DeltaMap = std.HashMap(DeltaKey, DeltaValue, DeltaKeyContext, 80);

pub const Database = struct {
    handle: *c.sqlite3,

    pub fn open(path: []const u8) Error!Database {
        const path_z = std.heap.c_allocator.dupeZ(u8, path) catch return error.OpenFailed;
        defer std.heap.c_allocator.free(path_z);

        var db_handle: ?*c.sqlite3 = null;
        if (c.sqlite3_open(path_z, &db_handle) != c.SQLITE_OK or db_handle == null) {
            if (db_handle) |handle| {
                _ = c.sqlite3_close(handle);
            }
            return error.OpenFailed;
        }
        return .{ .handle = db_handle.? };
    }

    pub fn openInMemory() Error!Database {
        var db_handle: ?*c.sqlite3 = null;
        if (c.sqlite3_open(":memory:", &db_handle) != c.SQLITE_OK or db_handle == null) {
            if (db_handle) |handle| {
                _ = c.sqlite3_close(handle);
            }
            return error.OpenFailed;
        }
        return .{ .handle = db_handle.? };
    }

    pub fn close(self: *Database) void {
        // Use the v2 close path so any delayed SQLite cleanup can finish safely
        // even if a caller still has internal statements or caches pending.
        _ = c.sqlite3_close_v2(self.handle);
        self.* = undefined;
    }

    pub fn exec(self: Database, sql: []const u8) Error!void {
        const sql_z = std.heap.c_allocator.dupeZ(u8, sql) catch return error.ExecFailed;
        defer std.heap.c_allocator.free(sql_z);

        var err_msg: [*c]u8 = null;
        defer if (err_msg) |msg| c.sqlite3_free(msg);

        if (c.sqlite3_exec(self.handle, sql_z.ptr, null, null, &err_msg) != c.SQLITE_OK) {
            if (err_msg) |msg| {
                std.debug.print("sqlite exec failed: {s}\n", .{std.mem.span(msg)});
            }
            return error.ExecFailed;
        }
    }

    pub fn applyStandaloneSchema(self: Database) !void {
        const schema = try sqlite_schema.renderMetadataSchema(std.heap.page_allocator);
        defer std.heap.page_allocator.free(schema);
        try self.exec(schema);
        try self.applyStandaloneStopwordsSeed();
    }

    pub fn applyStandaloneImportSchema(self: Database) !void {
        const schema = try sqlite_schema.renderMetadataSchemaWithMode(std.heap.page_allocator, .import);
        defer std.heap.page_allocator.free(schema);
        try self.exec(schema);
        try self.applyStandaloneStopwordsSeed();
    }

    pub fn applyStandaloneStopwordsSeed(self: Database) !void {
        try self.exec(bm25_stopwords_seed_sql);
    }
};

pub const Repository = struct {
    db: *const Database,

    pub fn asFacetRepository(self: *const Repository) interfaces.FacetRepository {
        return .{
            .ctx = @ptrCast(@constCast(self)),
            .getTableConfigFn = getTableConfig,
            .getFacetIdFn = getFacetId,
            .listFacetValuesFn = listFacetValues,
            .getFacetValueNodeFn = getFacetValueNode,
            .getFacetChildrenFn = getFacetChildren,
            .getPostingsFn = getPostings,
        };
    }

    fn getTableConfig(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        table_name: []const u8,
    ) anyerror!interfaces.FacetTableConfig {
        _ = allocator;
        const self: *const Repository = @ptrCast(@alignCast(ctx));
        return try loadFacetTableRuntime(self.db.*, table_name);
    }

    fn getFacetId(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        table_id: u64,
        facet_name: []const u8,
    ) anyerror!?u32 {
        _ = allocator;
        const self: *const Repository = @ptrCast(@alignCast(ctx));
        return loadFacetId(self.db.*, table_id, facet_name) catch |err| switch (err) {
            error.MissingRow => null,
            else => err,
        };
    }

    fn getPostings(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        table_id: u64,
        facet_id: u32,
        values: []const []const u8,
    ) anyerror![]interfaces.FacetPosting {
        const self: *const Repository = @ptrCast(@alignCast(ctx));
        var postings = std.ArrayList(interfaces.FacetPosting).empty;
        defer postings.deinit(allocator);

        for (values) |value| {
            try appendPostingsForValue(self.db.*, allocator, &postings, table_id, facet_id, value);
        }

        return postings.toOwnedSlice(allocator);
    }

    fn listFacetValues(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        table_id: u64,
        facet_id: u32,
    ) anyerror![][]const u8 {
        const self: *const Repository = @ptrCast(@alignCast(ctx));
        return try loadFacetValues(self.db.*, allocator, table_id, facet_id);
    }

    fn getFacetValueNode(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        table_id: u64,
        facet_id: u32,
        facet_value: []const u8,
    ) anyerror!?interfaces.FacetValueNode {
        const self: *const Repository = @ptrCast(@alignCast(ctx));
        return loadFacetValueNode(self.db.*, allocator, table_id, facet_id, facet_value) catch |err| switch (err) {
            error.MissingRow => null,
            else => err,
        };
    }

    fn getFacetChildren(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        table_id: u64,
        value_id: u32,
    ) anyerror![]interfaces.FacetValueNode {
        const self: *const Repository = @ptrCast(@alignCast(ctx));
        return try loadFacetChildren(self.db.*, allocator, table_id, value_id);
    }
};

pub fn insertFacetTable(
    db: Database,
    table_id: u64,
    schema_name: []const u8,
    table_name: []const u8,
    chunk_bits: u8,
) !void {
    const sql =
        "INSERT INTO facet_tables(table_id, schema_name, table_name, chunk_bits) VALUES (?1, ?2, ?3, ?4)";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    try bindText(stmt, 2, schema_name);
    try bindText(stmt, 3, table_name);
    try bindInt64(stmt, 4, chunk_bits);
    try stepDone(stmt);
}

pub fn upsertFacetTable(
    db: Database,
    table_id: u64,
    schema_name: []const u8,
    table_name: []const u8,
    chunk_bits: u8,
) !void {
    const sql =
        "INSERT OR REPLACE INTO facet_tables(table_id, schema_name, table_name, chunk_bits) VALUES (?1, ?2, ?3, ?4)";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    try bindText(stmt, 2, schema_name);
    try bindText(stmt, 3, table_name);
    try bindInt64(stmt, 4, chunk_bits);
    try stepDone(stmt);
}

pub fn insertFacetDefinition(
    db: Database,
    table_id: u64,
    facet_id: u32,
    facet_name: []const u8,
) !void {
    const sql =
        "INSERT INTO facet_definitions(table_id, facet_id, facet_name) VALUES (?1, ?2, ?3)";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, facet_id);
    try bindText(stmt, 3, facet_name);
    try stepDone(stmt);
}

pub fn upsertFacetDefinition(
    db: Database,
    table_id: u64,
    facet_id: u32,
    facet_name: []const u8,
) !void {
    const sql =
        "INSERT OR REPLACE INTO facet_definitions(table_id, facet_id, facet_name) VALUES (?1, ?2, ?3)";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, facet_id);
    try bindText(stmt, 3, facet_name);
    try stepDone(stmt);
}

pub fn setupFacetTable(
    db: Database,
    table_id: u64,
    schema_name: []const u8,
    table_name: []const u8,
    chunk_bits: u8,
    definitions: []const FacetDefinitionSpec,
) !void {
    try upsertFacetTable(db, table_id, schema_name, table_name, chunk_bits);
    for (definitions) |definition| {
        try upsertFacetDefinition(db, table_id, definition.facet_id, definition.facet_name);
    }
}

pub fn queueFacetDelta(
    db: Database,
    table_id: u64,
    facet_id: u32,
    facet_value: []const u8,
    posting: u64,
    delta: i64,
) !void {
    if (delta == 0) return;

    const sql =
        "INSERT INTO facet_deltas(table_id, facet_id, facet_value, posting, delta) VALUES (?1, ?2, ?3, ?4, ?5) " ++ "ON CONFLICT(table_id, facet_id, facet_value, posting) DO UPDATE SET delta = facet_deltas.delta + excluded.delta";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    const posting_i64 = std.math.cast(i64, posting) orelse return error.ValueOutOfRange;
    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, facet_id);
    try bindText(stmt, 3, facet_value);
    try bindInt64(stmt, 4, posting_i64);
    try bindInt64(stmt, 5, delta);
    try stepDone(stmt);
}

pub fn syncFacetAssignments(
    db: Database,
    table_id: u64,
    doc_id: u64,
    assignments: []const FacetDocumentAssignment,
) !u64 {
    if (assignments.len == 0) return 0;

    const table = try loadFacetTableConfigById(db, std.heap.page_allocator, table_id);
    defer {
        std.heap.page_allocator.free(table.schema_name);
        std.heap.page_allocator.free(table.table_name);
    }

    for (assignments) |assignment| {
        try queueFacetDelta(db, table_id, assignment.facet_id, assignment.facet_value, doc_id, 1);
    }

    return try mergeDeltasSafe(db, table_id, null);
}

pub fn countFacetDeltas(
    db: Database,
    table_id: u64,
    facet_id: ?u32,
) !u64 {
    const sql =
        if (facet_id) |_|
            "SELECT COUNT(*) FROM facet_deltas WHERE table_id = ?1 AND facet_id = ?2 AND delta <> 0"
        else
            "SELECT COUNT(*) FROM facet_deltas WHERE table_id = ?1 AND delta <> 0";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    if (facet_id) |value| try bindInt64(stmt, 2, value);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;
    return try columnU64(stmt, 0);
}

pub fn applyDeltas(
    db: Database,
    table_id: u64,
    facet_id: ?u32,
) !u64 {
    const table = try loadFacetTableConfigById(db, std.heap.page_allocator, table_id);
    defer {
        std.heap.page_allocator.free(table.schema_name);
        std.heap.page_allocator.free(table.table_name);
    }

    const delta_sql = if (facet_id != null)
        "SELECT facet_id, facet_value, posting, delta FROM facet_deltas WHERE table_id = ?1 AND facet_id = ?2 AND delta <> 0"
    else
        "SELECT facet_id, facet_value, posting, delta FROM facet_deltas WHERE table_id = ?1 AND delta <> 0";

    const stmt = try prepare(db, delta_sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    if (facet_id) |value| try bindInt64(stmt, 2, value);

    var grouped = DeltaMap.init(std.heap.page_allocator);
    errdefer freeDeltaMap(std.heap.page_allocator, &grouped);

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const row_facet_id = try columnU32(stmt, 0);
        const facet_value = try dupeColumnText(std.heap.page_allocator, stmt, 1);
        var released = false;
        defer if (!released) std.heap.page_allocator.free(facet_value);

        const posting_i64 = try columnI64(stmt, 2);
        const posting_u64 = std.math.cast(u64, posting_i64) orelse return error.ValueOutOfRange;
        const shift: u6 = @intCast(table.chunk_bits);
        const chunk_id_u64 = posting_u64 >> shift;
        const in_chunk_mask: u64 = (@as(u64, 1) << shift) - 1;
        const in_chunk_id = std.math.cast(u32, posting_u64 & in_chunk_mask) orelse return error.ValueOutOfRange;
        const chunk_id = std.math.cast(u32, chunk_id_u64) orelse return error.ValueOutOfRange;
        const delta = try columnI64(stmt, 3);

        const owned_key = DeltaKey{
            .facet_id = row_facet_id,
            .chunk_id = chunk_id,
            .facet_value = facet_value,
        };
        const gop = try grouped.getOrPut(owned_key);
        if (gop.found_existing) {
            released = true;
            std.heap.page_allocator.free(facet_value);
        } else {
            gop.key_ptr.* = owned_key;
            gop.value_ptr.* = .{};
            released = true;
        }

        if (delta > 0) {
            if (gop.value_ptr.add == null) {
                gop.value_ptr.add = try roaring.Bitmap.empty();
            }
            gop.value_ptr.add.?.add(in_chunk_id);
        } else {
            if (gop.value_ptr.remove == null) {
                gop.value_ptr.remove = try roaring.Bitmap.empty();
            }
            gop.value_ptr.remove.?.add(in_chunk_id);
        }
    }

    const select_stmt = try prepare(
        db,
        "SELECT posting_blob FROM facet_postings WHERE table_id = ?1 AND facet_id = ?2 AND facet_value = ?3 AND chunk_id = ?4",
    );
    defer finalize(select_stmt);

    const upsert_stmt = try prepare(
        db,
        "INSERT INTO facet_postings(table_id, facet_id, facet_value, chunk_id, posting_blob) VALUES (?1, ?2, ?3, ?4, ?5) " ++ "ON CONFLICT(table_id, facet_id, facet_value, chunk_id) DO UPDATE SET posting_blob = excluded.posting_blob",
    );
    defer finalize(upsert_stmt);

    const delete_stmt = try prepare(
        db,
        "DELETE FROM facet_postings WHERE table_id = ?1 AND facet_id = ?2 AND facet_value = ?3 AND chunk_id = ?4",
    );
    defer finalize(delete_stmt);

    var affected: u64 = 0;
    var it = grouped.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const value = entry.value_ptr.*;

        try resetStatement(select_stmt);
        try bindInt64(select_stmt, 1, table_id);
        try bindInt64(select_stmt, 2, key.facet_id);
        try bindText(select_stmt, 3, key.facet_value);
        try bindInt64(select_stmt, 4, key.chunk_id);

        const select_rc = c.sqlite3_step(select_stmt);
        var current: roaring.Bitmap = if (select_rc == c.SQLITE_ROW) blk: {
            const blob_len = c.sqlite3_column_bytes(select_stmt, 0);
            const blob_ptr = c.sqlite3_column_blob(select_stmt, 0) orelse return error.MissingRow;
            const blob: []const u8 = @as([*]const u8, @ptrCast(blob_ptr))[0..@intCast(blob_len)];
            break :blk try roaring.Bitmap.deserializePortable(blob);
        } else if (select_rc == c.SQLITE_DONE) try roaring.Bitmap.empty() else return error.StepFailed;
        defer current.deinit();

        if (value.remove) |remove| {
            const next = try current.andNotNew(remove);
            current.deinit();
            current = next;
        }
        if (value.add) |add| current.orInPlace(add);

        if (current.isEmpty()) {
            try resetStatement(delete_stmt);
            try bindInt64(delete_stmt, 1, table_id);
            try bindInt64(delete_stmt, 2, key.facet_id);
            try bindText(delete_stmt, 3, key.facet_value);
            try bindInt64(delete_stmt, 4, key.chunk_id);
            if (c.sqlite3_step(delete_stmt) != c.SQLITE_DONE) return error.StepFailed;
            if (c.sqlite3_changes(db.handle) > 0) affected += 1;
            continue;
        }

        const bytes = try current.serializePortableStable(std.heap.page_allocator);
        defer std.heap.page_allocator.free(bytes);

        try resetStatement(upsert_stmt);
        try bindInt64(upsert_stmt, 1, table_id);
        try bindInt64(upsert_stmt, 2, key.facet_id);
        try bindText(upsert_stmt, 3, key.facet_value);
        try bindInt64(upsert_stmt, 4, key.chunk_id);
        try bindBlob(upsert_stmt, 5, bytes);
        if (c.sqlite3_step(upsert_stmt) != c.SQLITE_DONE) return error.StepFailed;
        affected += 1;
    }

    try clearFacetDeltas(db, table_id, facet_id);
    freeDeltaMap(std.heap.page_allocator, &grouped);
    return affected;
}

pub fn mergeDeltasSafe(
    db: Database,
    table_id: u64,
    facet_id: ?u32,
) !u64 {
    try db.exec("SAVEPOINT facet_merge_deltas_safe");

    const result = applyDeltas(db, table_id, facet_id) catch |err| {
        _ = db.exec("ROLLBACK TO SAVEPOINT facet_merge_deltas_safe") catch {};
        _ = db.exec("RELEASE SAVEPOINT facet_merge_deltas_safe") catch {};
        return err;
    };

    if (db.exec("RELEASE SAVEPOINT facet_merge_deltas_safe")) |_| {
        return result;
    } else |_| {
        _ = db.exec("ROLLBACK TO SAVEPOINT facet_merge_deltas_safe") catch {};
        return error.ExecFailed;
    }
}

pub fn mergeDeltas(
    db: Database,
    table_id: u64,
) !u64 {
    return try applyDeltas(db, table_id, null);
}

pub fn insertPostingBitmap(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    facet_id: u32,
    facet_value: []const u8,
    chunk_id: u32,
    bitmap: roaring.Bitmap,
) !void {
    const sql =
        "INSERT INTO facet_postings(table_id, facet_id, facet_value, chunk_id, posting_blob) VALUES (?1, ?2, ?3, ?4, ?5)";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    const bytes = try bitmap.serializePortableStable(allocator);
    defer allocator.free(bytes);

    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, facet_id);
    try bindText(stmt, 3, facet_value);
    try bindInt64(stmt, 4, chunk_id);
    try bindBlob(stmt, 5, bytes);
    try stepDone(stmt);
}

pub fn upsertPostingBitmap(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    facet_id: u32,
    facet_value: []const u8,
    chunk_id: u32,
    bitmap: roaring.Bitmap,
) !void {
    const sql =
        "INSERT OR REPLACE INTO facet_postings(table_id, facet_id, facet_value, chunk_id, posting_blob) VALUES (?1, ?2, ?3, ?4, ?5)";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    const bytes = try bitmap.serializePortableStable(allocator);
    defer allocator.free(bytes);

    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, facet_id);
    try bindText(stmt, 3, facet_value);
    try bindInt64(stmt, 4, chunk_id);
    try bindBlob(stmt, 5, bytes);
    try stepDone(stmt);
}

pub fn deletePostingBitmap(
    db: Database,
    table_id: u64,
    facet_id: u32,
    facet_value: []const u8,
    chunk_id: u32,
) !void {
    const sql =
        "DELETE FROM facet_postings WHERE table_id = ?1 AND facet_id = ?2 AND facet_value = ?3 AND chunk_id = ?4";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, facet_id);
    try bindText(stmt, 3, facet_value);
    try bindInt64(stmt, 4, chunk_id);
    try stepDone(stmt);
}

pub fn insertFacetValueNode(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    value_id: u32,
    facet_id: u32,
    facet_value: []const u8,
    child_ids: ?[]const u32,
) !void {
    const sql =
        "INSERT INTO facet_value_nodes(table_id, value_id, facet_id, facet_value, children_blob) VALUES (?1, ?2, ?3, ?4, ?5)";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, value_id);
    try bindInt64(stmt, 3, facet_id);
    try bindText(stmt, 4, facet_value);
    if (child_ids) |ids| {
        var bitmap = try roaring.Bitmap.fromSlice(ids);
        defer bitmap.deinit();
        const bytes = try bitmap.serializePortableStable(allocator);
        defer allocator.free(bytes);
        try bindBlob(stmt, 5, bytes);
    } else {
        try bindNull(stmt, 5);
    }
    try stepDone(stmt);
}

pub fn upsertFacetValueNode(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    value_id: u32,
    facet_id: u32,
    facet_value: []const u8,
    child_ids: ?[]const u32,
) !void {
    const sql =
        "INSERT OR REPLACE INTO facet_value_nodes(table_id, value_id, facet_id, facet_value, children_blob) VALUES (?1, ?2, ?3, ?4, ?5)";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, value_id);
    try bindInt64(stmt, 3, facet_id);
    try bindText(stmt, 4, facet_value);
    if (child_ids) |ids| {
        var bitmap = try roaring.Bitmap.fromSlice(ids);
        defer bitmap.deinit();
        const bytes = try bitmap.serializePortableStable(allocator);
        defer allocator.free(bytes);
        try bindBlob(stmt, 5, bytes);
    } else {
        try bindNull(stmt, 5);
    }
    try stepDone(stmt);
}

pub fn loadFacetTableConfig(
    db: Database,
    allocator: std.mem.Allocator,
    table_name: []const u8,
) !interfaces.FacetTableConfig {
    const sql =
        "SELECT table_id, chunk_bits, schema_name, table_name FROM facet_tables WHERE table_name = ?1";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindText(stmt, 1, table_name);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;

    const table_id = try columnU64(stmt, 0);
    const chunk_bits = try columnU8(stmt, 1);
    const schema_name_slice = try dupeColumnText(allocator, stmt, 2);
    errdefer allocator.free(schema_name_slice);
    const table_name_slice = try dupeColumnText(allocator, stmt, 3);
    errdefer allocator.free(table_name_slice);

    return .{
        .table_id = table_id,
        .chunk_bits = chunk_bits,
        .schema_name = schema_name_slice,
        .table_name = table_name_slice,
    };
}

fn loadFacetTableConfigById(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
) !interfaces.FacetTableConfig {
    const sql =
        "SELECT chunk_bits, schema_name, table_name FROM facet_tables WHERE table_id = ?1";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;

    const chunk_bits = try columnU8(stmt, 0);
    const schema_name_slice = try dupeColumnText(allocator, stmt, 1);
    errdefer allocator.free(schema_name_slice);
    const table_name_slice = try dupeColumnText(allocator, stmt, 2);
    errdefer allocator.free(table_name_slice);

    return .{
        .table_id = table_id,
        .chunk_bits = chunk_bits,
        .schema_name = schema_name_slice,
        .table_name = table_name_slice,
    };
}

fn loadFacetTableRuntime(
    db: Database,
    table_name: []const u8,
) !interfaces.FacetTableConfig {
    const sql =
        "SELECT table_id, chunk_bits FROM facet_tables WHERE table_name = ?1";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindText(stmt, 1, table_name);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;

    return .{
        .table_id = try columnU64(stmt, 0),
        .chunk_bits = try columnU8(stmt, 1),
        .schema_name = "",
        .table_name = table_name,
    };
}

pub fn loadFacetId(
    db: Database,
    table_id: u64,
    facet_name: []const u8,
) !u32 {
    const sql =
        "SELECT facet_id FROM facet_definitions WHERE table_id = ?1 AND facet_name = ?2";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    try bindText(stmt, 2, facet_name);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;
    return try columnU32(stmt, 0);
}

pub fn listFacetDefinitions(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
) ![]FacetDefinitionRecord {
    const sql =
        "SELECT facet_id, facet_name FROM facet_definitions WHERE table_id = ?1 ORDER BY facet_id";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);

    var defs = std.ArrayList(FacetDefinitionRecord).empty;
    errdefer {
        for (defs.items) |def| allocator.free(def.facet_name);
        defs.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        try defs.append(allocator, .{
            .facet_id = try columnU32(stmt, 0),
            .facet_name = try dupeColumnText(allocator, stmt, 1),
        });
    }

    return defs.toOwnedSlice(allocator);
}

pub fn listTableFacets(
    db: Database,
    allocator: std.mem.Allocator,
    table_name: []const u8,
) ![]FacetDefinitionRecord {
    const table = try loadFacetTableConfig(db, allocator, table_name);
    defer {
        allocator.free(table.schema_name);
        allocator.free(table.table_name);
    }
    return try listFacetDefinitions(db, allocator, table.table_id);
}

pub fn listTableFacetNames(
    db: Database,
    allocator: std.mem.Allocator,
    table_name: []const u8,
) ![][]const u8 {
    const defs = try listTableFacets(db, allocator, table_name);

    var names = std.ArrayList([]const u8).empty;
    errdefer {
        for (names.items) |name| allocator.free(name);
        names.deinit(allocator);
    }

    for (defs) |def| {
        try names.append(allocator, try allocator.dupe(u8, def.facet_name));
    }

    for (defs) |def| allocator.free(def.facet_name);
    allocator.free(defs);

    return names.toOwnedSlice(allocator);
}

pub fn describeFacetTable(
    db: Database,
    allocator: std.mem.Allocator,
    table_name: []const u8,
) !FacetTableSummary {
    const table = try loadFacetTableConfig(db, allocator, table_name);
    defer {
        allocator.free(table.schema_name);
        allocator.free(table.table_name);
    }

    const facet_sql =
        "SELECT COUNT(*) FROM facet_definitions WHERE table_id = ?1";
    const posting_sql =
        "SELECT COUNT(*) FROM facet_postings WHERE table_id = ?1";
    const node_sql =
        "SELECT COUNT(*) FROM facet_value_nodes WHERE table_id = ?1";

    const facet_stmt = try prepare(db, facet_sql);
    defer finalize(facet_stmt);
    try bindInt64(facet_stmt, 1, table.table_id);
    if (c.sqlite3_step(facet_stmt) != c.SQLITE_ROW) return error.MissingRow;
    const facet_count = try columnU64(facet_stmt, 0);

    const posting_stmt = try prepare(db, posting_sql);
    defer finalize(posting_stmt);
    try bindInt64(posting_stmt, 1, table.table_id);
    if (c.sqlite3_step(posting_stmt) != c.SQLITE_ROW) return error.MissingRow;
    const posting_count = try columnU64(posting_stmt, 0);

    const node_stmt = try prepare(db, node_sql);
    defer finalize(node_stmt);
    try bindInt64(node_stmt, 1, table.table_id);
    if (c.sqlite3_step(node_stmt) != c.SQLITE_ROW) return error.MissingRow;
    const value_node_count = try columnU64(node_stmt, 0);

    return .{
        .table_id = table.table_id,
        .schema_name = try allocator.dupe(u8, table.schema_name),
        .table_name = try allocator.dupe(u8, table.table_name),
        .chunk_bits = table.chunk_bits,
        .facet_count = facet_count,
        .posting_count = posting_count,
        .value_node_count = value_node_count,
    };
}

pub const FacetValueCount = struct {
    facet_value: []const u8,
    doc_count: u64,
};

pub const FacetFilter = struct {
    facet_id: u32,
    facet_value: []const u8,
};

/// Choose a safe default for `chunk_bits`, parity for
/// `facets.optimal_chunk_bits`. The PostgreSQL implementation grows the
/// chunk size with the projected document count to balance bitmap size
/// against fan-out; the same heuristic applies to the SQLite engine.
pub fn optimalChunkBits(approx_doc_count: u64) u8 {
    if (approx_doc_count <= 1024) return 10;
    if (approx_doc_count <= 65_536) return 14;
    if (approx_doc_count <= 1_048_576) return 16;
    return 18;
}

/// Cardinality of a roaring bitmap, parity for `facets.count_results`.
pub fn countResults(bitmap: roaring.Bitmap) u64 {
    return bitmap.cardinality();
}

/// Refresh all merged facet postings from queued deltas, parity for
/// `facets.refresh_facets`. Returns the number of (facet, value, chunk)
/// rows touched.
pub fn refreshFacets(db: Database, table_id: u64) !u64 {
    return try mergeDeltas(db, table_id);
}

/// Drop a set of facet definitions and any postings/value nodes/deltas
/// associated with them, parity for `facets.drop_facets`.
pub fn dropFacets(db: Database, table_id: u64, facet_ids: []const u32) !void {
    if (facet_ids.len == 0) return;
    const stmts = [_][]const u8{
        "DELETE FROM facet_postings WHERE table_id = ?1 AND facet_id = ?2",
        "DELETE FROM facet_value_nodes WHERE table_id = ?1 AND facet_id = ?2",
        "DELETE FROM facet_deltas WHERE table_id = ?1 AND facet_id = ?2",
        "DELETE FROM facet_definitions WHERE table_id = ?1 AND facet_id = ?2",
    };
    for (stmts) |sql| {
        const stmt = try prepare(db, sql);
        defer finalize(stmt);
        for (facet_ids) |facet_id| {
            try resetStatement(stmt);
            try bindInt64(stmt, 1, table_id);
            try bindInt64(stmt, 2, facet_id);
            if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
        }
    }
}

/// Tear down all facet-related rows for a table, parity for
/// `facets.drop_faceting`.
pub fn dropFaceting(db: Database, table_id: u64) !void {
    const stmts = [_][]const u8{
        "DELETE FROM facet_postings WHERE table_id = ?1",
        "DELETE FROM facet_value_nodes WHERE table_id = ?1",
        "DELETE FROM facet_deltas WHERE table_id = ?1",
        "DELETE FROM facet_definitions WHERE table_id = ?1",
        "DELETE FROM facet_tables WHERE table_id = ?1",
    };
    for (stmts) |sql| {
        const stmt = try prepare(db, sql);
        defer finalize(stmt);
        try bindInt64(stmt, 1, table_id);
        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
    }
}

/// Document count for every value of a facet, parity for
/// `facets.get_facet_counts`. Counts are summed across chunks.
pub fn getFacetCounts(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    facet_id: u32,
) ![]FacetValueCount {
    const sql =
        "SELECT facet_value, posting_blob FROM facet_postings WHERE table_id = ?1 AND facet_id = ?2 ORDER BY facet_value, chunk_id";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, facet_id);

    var results = std.ArrayList(FacetValueCount).empty;
    errdefer {
        for (results.items) |entry| allocator.free(entry.facet_value);
        results.deinit(allocator);
    }

    var current_value: ?[]u8 = null;
    var current_count: u64 = 0;
    errdefer if (current_value) |value| allocator.free(value);

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const value_bytes = try dupeColumnText(allocator, stmt, 0);
        defer allocator.free(value_bytes);
        const blob_len = c.sqlite3_column_bytes(stmt, 1);
        const blob_ptr = c.sqlite3_column_blob(stmt, 1) orelse return error.MissingRow;
        const blob: []const u8 = @as([*]const u8, @ptrCast(blob_ptr))[0..@intCast(blob_len)];
        var bitmap = try roaring.Bitmap.deserializePortable(blob);
        defer bitmap.deinit();
        const chunk_count = bitmap.cardinality();

        const same_value = current_value != null and std.mem.eql(u8, current_value.?, value_bytes);
        if (!same_value) {
            if (current_value) |existing| {
                try results.append(allocator, .{
                    .facet_value = existing,
                    .doc_count = current_count,
                });
            }
            current_value = try allocator.dupe(u8, value_bytes);
            current_count = chunk_count;
        } else {
            current_count += chunk_count;
        }
    }
    if (current_value) |existing| {
        try results.append(allocator, .{
            .facet_value = existing,
            .doc_count = current_count,
        });
        current_value = null;
    }

    return results.toOwnedSlice(allocator);
}

/// Top-N facet values by document count, parity for
/// `facets.top_values`. The result is sorted by count descending and the
/// caller owns the returned slice and each `facet_value` string.
pub fn topValues(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    facet_id: u32,
    limit: usize,
) ![]FacetValueCount {
    const all = try getFacetCounts(db, allocator, table_id, facet_id);
    errdefer {
        for (all) |entry| allocator.free(entry.facet_value);
        allocator.free(all);
    }
    std.mem.sort(FacetValueCount, all, {}, struct {
        fn lessThan(_: void, lhs: FacetValueCount, rhs: FacetValueCount) bool {
            if (lhs.doc_count == rhs.doc_count) {
                return std.mem.order(u8, lhs.facet_value, rhs.facet_value) == .lt;
            }
            return lhs.doc_count > rhs.doc_count;
        }
    }.lessThan);
    if (all.len <= limit) return all;
    for (all[limit..]) |entry| allocator.free(entry.facet_value);
    return try allocator.realloc(all, limit);
}

/// Intersect the document postings of every supplied filter, parity for
/// `facets.filter_documents_by_facets_bitmap`. The caller owns the
/// returned bitmap.
pub fn filterDocumentsByFacetsBitmap(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    filters: []const FacetFilter,
) !roaring.Bitmap {
    if (filters.len == 0) return try roaring.Bitmap.empty();

    var combined: ?roaring.Bitmap = null;
    errdefer if (combined) |*bitmap| bitmap.deinit();

    for (filters) |filter| {
        var postings = std.ArrayList(interfaces.FacetPosting).empty;
        defer {
            for (postings.items) |*posting| {
                var owned = posting.bitmap;
                owned.deinit();
            }
            postings.deinit(allocator);
        }
        try appendPostingsForValue(db, allocator, &postings, table_id, filter.facet_id, filter.facet_value);

        var union_bm = try roaring.Bitmap.empty();
        defer union_bm.deinit();
        for (postings.items) |posting| {
            const shift: u6 = @intCast(@as(u8, 16));
            _ = shift;
            // Postings are already in dense, chunk-local IDs; combine across chunks
            // by treating each posting's chunk_id as a high-order offset to keep
            // results unique. Compose into a single bitmap so AND across filters works.
            const ids = try posting.bitmap.toArray(allocator);
            defer allocator.free(ids);
            const chunk_offset: u32 = posting.chunk_id;
            for (ids) |id| {
                const composed: u64 = (@as(u64, chunk_offset) << 16) | @as(u64, id);
                if (composed > std.math.maxInt(u32)) continue;
                union_bm.add(@intCast(composed));
            }
        }

        if (combined == null) {
            combined = union_bm;
            union_bm = try roaring.Bitmap.empty();
        } else {
            combined.?.andInPlace(union_bm);
        }
    }

    return combined orelse try roaring.Bitmap.empty();
}

/// Document IDs that satisfy every facet filter, parity for
/// `facets.filter_documents_by_facets`.
pub fn filterDocumentsByFacets(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    filters: []const FacetFilter,
) ![]roaring.DenseId {
    var bitmap = try filterDocumentsByFacetsBitmap(db, allocator, table_id, filters);
    defer bitmap.deinit();
    return try bitmap.toArray(allocator);
}

pub fn loadPostingBitmap(
    db: Database,
    table_id: u64,
    facet_id: u32,
    facet_value: []const u8,
    chunk_id: u32,
) !roaring.Bitmap {
    const sql =
        "SELECT posting_blob FROM facet_postings WHERE table_id = ?1 AND facet_id = ?2 AND facet_value = ?3 AND chunk_id = ?4";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, facet_id);
    try bindText(stmt, 3, facet_value);
    try bindInt64(stmt, 4, chunk_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;

    const len = c.sqlite3_column_bytes(stmt, 0);
    const ptr = c.sqlite3_column_blob(stmt, 0) orelse return error.MissingRow;
    const bytes: []const u8 = @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
    return try roaring.Bitmap.deserializePortable(bytes);
}

fn appendPostingsForValue(
    db: Database,
    allocator: std.mem.Allocator,
    postings: *std.ArrayList(interfaces.FacetPosting),
    table_id: u64,
    facet_id: u32,
    facet_value: []const u8,
) !void {
    const sql =
        "SELECT chunk_id, posting_blob FROM facet_postings WHERE table_id = ?1 AND facet_id = ?2 AND facet_value = ?3 ORDER BY chunk_id";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, facet_id);
    try bindText(stmt, 3, facet_value);

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const chunk_id = try columnU32(stmt, 0);
        const blob_len = c.sqlite3_column_bytes(stmt, 1);
        const blob_ptr = c.sqlite3_column_blob(stmt, 1) orelse return error.MissingRow;
        const blob: []const u8 = @as([*]const u8, @ptrCast(blob_ptr))[0..@intCast(blob_len)];

        try postings.append(allocator, .{
            .chunk_id = chunk_id,
            .bitmap = try roaring.Bitmap.deserializePortable(blob),
        });
    }
}

fn loadFacetValues(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    facet_id: u32,
) ![][]const u8 {
    const sql =
        "SELECT DISTINCT facet_value FROM facet_postings WHERE table_id = ?1 AND facet_id = ?2 ORDER BY facet_value";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, facet_id);

    var values = std.ArrayList([]const u8).empty;
    defer {
        for (values.items) |value| allocator.free(value);
        values.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        try values.append(allocator, try dupeColumnText(allocator, stmt, 0));
    }

    return values.toOwnedSlice(allocator);
}

fn loadFacetValueNode(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    facet_id: u32,
    facet_value: []const u8,
) !interfaces.FacetValueNode {
    const sql =
        "SELECT value_id, facet_id, facet_value, children_blob FROM facet_value_nodes WHERE table_id = ?1 AND facet_id = ?2 AND facet_value = ?3";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, facet_id);
    try bindText(stmt, 3, facet_value);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;

    return try valueNodeFromRow(allocator, stmt);
}

fn loadFacetChildren(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    value_id: u32,
) ![]interfaces.FacetValueNode {
    const sql =
        "SELECT children_blob FROM facet_value_nodes WHERE table_id = ?1 AND value_id = ?2";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, value_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return &.{};

    if (c.sqlite3_column_type(stmt, 0) == c.SQLITE_NULL) return &.{};
    const blob_len = c.sqlite3_column_bytes(stmt, 0);
    const blob_ptr = c.sqlite3_column_blob(stmt, 0) orelse return &.{};
    const blob: []const u8 = @as([*]const u8, @ptrCast(blob_ptr))[0..@intCast(blob_len)];
    var child_bitmap = try roaring.Bitmap.deserializePortable(blob);
    defer child_bitmap.deinit();

    const child_ids = try child_bitmap.toArray(allocator);
    defer allocator.free(child_ids);

    var children = std.ArrayList(interfaces.FacetValueNode).empty;
    defer {
        for (children.items) |*child| deinitValueNode(allocator, child);
        children.deinit(allocator);
    }

    for (child_ids) |child_id| {
        const child = try loadFacetValueNodeById(db, allocator, table_id, child_id);
        try children.append(allocator, child);
    }

    return children.toOwnedSlice(allocator);
}

fn loadFacetValueNodeById(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    value_id: u32,
) !interfaces.FacetValueNode {
    const sql =
        "SELECT value_id, facet_id, facet_value, children_blob FROM facet_value_nodes WHERE table_id = ?1 AND value_id = ?2";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, value_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;

    return try valueNodeFromRow(allocator, stmt);
}

fn valueNodeFromRow(
    allocator: std.mem.Allocator,
    stmt: *c.sqlite3_stmt,
) !interfaces.FacetValueNode {
    const value_id = try columnU32(stmt, 0);
    const facet_id = try columnU32(stmt, 1);
    const facet_value = try dupeColumnText(allocator, stmt, 2);
    errdefer allocator.free(facet_value);

    var children_bitmap: ?roaring.Bitmap = null;
    errdefer if (children_bitmap) |*bitmap| bitmap.deinit();

    if (c.sqlite3_column_type(stmt, 3) != c.SQLITE_NULL) {
        const blob_len = c.sqlite3_column_bytes(stmt, 3);
        const blob_ptr = c.sqlite3_column_blob(stmt, 3) orelse return error.MissingRow;
        const blob: []const u8 = @as([*]const u8, @ptrCast(blob_ptr))[0..@intCast(blob_len)];
        children_bitmap = try roaring.Bitmap.deserializePortable(blob);
    }

    return .{
        .value_id = value_id,
        .facet_id = facet_id,
        .facet_value = facet_value,
        .children_bitmap = children_bitmap,
    };
}

fn clearFacetDeltas(
    db: Database,
    table_id: u64,
    facet_id: ?u32,
) !void {
    const sql =
        if (facet_id) |_|
            "DELETE FROM facet_deltas WHERE table_id = ?1 AND facet_id = ?2 AND delta <> 0"
        else
            "DELETE FROM facet_deltas WHERE table_id = ?1 AND delta <> 0";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    if (facet_id) |value| try bindInt64(stmt, 2, value);
    if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
}

fn freeDeltaMap(allocator: std.mem.Allocator, map: *DeltaMap) void {
    var it = map.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const value = entry.value_ptr.*;
        allocator.free(key.facet_value);
        if (value.add) |bitmap| {
            var owned = bitmap;
            owned.deinit();
        }
        if (value.remove) |bitmap| {
            var owned = bitmap;
            owned.deinit();
        }
    }
    map.deinit();
}

pub fn prepare(db: Database, sql: []const u8) Error!*c.sqlite3_stmt {
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
        return error.PrepareFailed;
    }
    return stmt.?;
}

pub fn finalize(stmt: *c.sqlite3_stmt) void {
    _ = c.sqlite3_finalize(stmt);
}

pub fn resetStatement(stmt: *c.sqlite3_stmt) Error!void {
    if (c.sqlite3_reset(stmt) != c.SQLITE_OK) return error.StepFailed;
    if (c.sqlite3_clear_bindings(stmt) != c.SQLITE_OK) return error.BindFailed;
}

pub fn stepDone(stmt: *c.sqlite3_stmt) Error!void {
    if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
}

pub fn bindInt64(stmt: *c.sqlite3_stmt, index: c_int, value: anytype) Error!void {
    if (c.sqlite3_bind_int64(stmt, index, @intCast(value)) != c.SQLITE_OK) return error.BindFailed;
}

pub fn bindText(stmt: *c.sqlite3_stmt, index: c_int, value: []const u8) Error!void {
    if (c.sqlite3_bind_text(stmt, index, value.ptr, @intCast(value.len), sqliteTransient()) != c.SQLITE_OK) {
        return error.BindFailed;
    }
}

pub fn bindBlob(stmt: *c.sqlite3_stmt, index: c_int, value: []const u8) Error!void {
    if (c.sqlite3_bind_blob(stmt, index, value.ptr, @intCast(value.len), sqliteTransient()) != c.SQLITE_OK) {
        return error.BindFailed;
    }
}

// Zig 0.15 cross-compilation workaround: c.SQLITE_TRANSIENT is ((sqlite3_destructor_type)(-1)),
// but Zig's comptime pointer-alignment check rejects the -1 sentinel on aarch64.
// A volatile load makes the expression runtime-only, bypassing the comptime check.
pub fn sqliteTransient() c.sqlite3_destructor_type {
    var v: isize = -1;
    return @ptrFromInt(@as(usize, @bitCast((@as(*volatile isize, &v)).*)));
}

pub fn columnI64(stmt: *c.sqlite3_stmt, index: c_int) Error!i64 {
    return c.sqlite3_column_int64(stmt, index);
}

pub fn bindNull(stmt: *c.sqlite3_stmt, index: c_int) Error!void {
    if (c.sqlite3_bind_null(stmt, index) != c.SQLITE_OK) return error.BindFailed;
}

pub fn columnU64(stmt: *c.sqlite3_stmt, index: c_int) Error!u64 {
    const value = c.sqlite3_column_int64(stmt, index);
    return std.math.cast(u64, value) orelse error.ValueOutOfRange;
}

pub fn columnU32(stmt: *c.sqlite3_stmt, index: c_int) Error!u32 {
    const value = c.sqlite3_column_int64(stmt, index);
    return std.math.cast(u32, value) orelse error.ValueOutOfRange;
}

pub fn columnU8(stmt: *c.sqlite3_stmt, index: c_int) Error!u8 {
    const value = c.sqlite3_column_int64(stmt, index);
    return std.math.cast(u8, value) orelse error.ValueOutOfRange;
}

pub fn dupeColumnText(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, index: c_int) ![]const u8 {
    const len = c.sqlite3_column_bytes(stmt, index);
    const ptr = c.sqlite3_column_text(stmt, index) orelse return error.MissingRow;
    const bytes: []const u8 = @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
    return try allocator.dupe(u8, bytes);
}

fn deinitValueNode(allocator: std.mem.Allocator, node: *interfaces.FacetValueNode) void {
    allocator.free(node.facet_value);
    if (node.children_bitmap) |*bitmap| bitmap.deinit();
    node.* = undefined;
}

test "sqlite facet registry stores and resolves table config and facet ids" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertFacetTable(db, 41, "public", "docs_fixture", 8);
    try insertFacetDefinition(db, 41, 3, "category");

    const config = try loadFacetTableConfig(db, std.testing.allocator, "docs_fixture");
    defer {
        std.testing.allocator.free(config.schema_name);
        std.testing.allocator.free(config.table_name);
    }

    try std.testing.expectEqual(@as(u64, 41), config.table_id);
    try std.testing.expectEqual(@as(u8, 8), config.chunk_bits);
    try std.testing.expectEqualStrings("public", config.schema_name);
    try std.testing.expectEqualStrings("docs_fixture", config.table_name);

    const facet_id = try loadFacetId(db, 41, "category");
    try std.testing.expectEqual(@as(u32, 3), facet_id);
}

test "sqlite bm25 stopword seed is loaded by default" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    const sql = "SELECT language, normalized_word, source FROM bm25_stopwords WHERE language = 'english' AND normalized_word = 'the' LIMIT 1";
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
        return error.PrepareFailed;
    }
    defer _ = c.sqlite3_finalize(stmt.?);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt.?));

    const language = try dupeColumnText(std.testing.allocator, stmt.?, 0);
    defer std.testing.allocator.free(language);
    const normalized_word = try dupeColumnText(std.testing.allocator, stmt.?, 1);
    defer std.testing.allocator.free(normalized_word);
    const source = try dupeColumnText(std.testing.allocator, stmt.?, 2);
    defer std.testing.allocator.free(source);

    try std.testing.expectEqualStrings("english", language);
    try std.testing.expectEqualStrings("the", normalized_word);
    try std.testing.expect(std.mem.startsWith(u8, source, "bundled:"));
}

test "sqlite stores roaring bitmaps as blobs and loads them back" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertFacetTable(db, 9, "public", "docs_fixture", 4);
    try insertFacetDefinition(db, 9, 1, "region");

    var bitmap = try roaring.Bitmap.fromSlice(&.{ 1, 4, 7 });
    defer bitmap.deinit();

    try insertPostingBitmap(db, std.testing.allocator, 9, 1, "eu", 0, bitmap);

    var restored = try loadPostingBitmap(db, 9, 1, "eu", 0);
    defer restored.deinit();

    const ids = try restored.toArray(std.testing.allocator);
    defer std.testing.allocator.free(ids);

    try std.testing.expectEqualSlices(u32, &.{ 1, 4, 7 }, ids);
}

test "sqlite-backed facet repository drives standalone facet filtering" {
    const facet_store = @import("facet_store.zig");

    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertFacetTable(db, 1, "public", "docs_fixture", 4);
    try insertFacetDefinition(db, 1, 1, "category");
    try insertFacetDefinition(db, 1, 2, "region");

    var search_bitmap = try roaring.Bitmap.fromSlice(&.{ 1, 2, 4 });
    defer search_bitmap.deinit();
    try insertPostingBitmap(db, std.testing.allocator, 1, 1, "search", 0, search_bitmap);

    var filtering_bitmap = try roaring.Bitmap.fromSlice(&.{3});
    defer filtering_bitmap.deinit();
    try insertPostingBitmap(db, std.testing.allocator, 1, 1, "filtering", 0, filtering_bitmap);

    var eu_bitmap = try roaring.Bitmap.fromSlice(&.{ 1, 3, 4 });
    defer eu_bitmap.deinit();
    try insertPostingBitmap(db, std.testing.allocator, 1, 2, "eu", 0, eu_bitmap);

    var us_bitmap = try roaring.Bitmap.fromSlice(&.{2});
    defer us_bitmap.deinit();
    try insertPostingBitmap(db, std.testing.allocator, 1, 2, "us", 0, us_bitmap);

    const repository = Repository{ .db = &db };
    var result = (try facet_store.filterDocuments(
        std.testing.allocator,
        repository.asFacetRepository(),
        "docs_fixture",
        &.{
            .{ .facet_name = "category", .values = &.{ "search", "filtering" } },
            .{ .facet_name = "region", .values = &.{"eu"} },
        },
    )).?;
    defer result.deinit();

    const ids = try result.toArray(std.testing.allocator);
    defer std.testing.allocator.free(ids);

    try std.testing.expectEqualSlices(u32, &.{ 1, 3, 4 }, ids);
}

test "sqlite-backed facet repository counts facet values" {
    const facet_store = @import("facet_store.zig");

    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertFacetTable(db, 1, "public", "docs_fixture", 4);
    try insertFacetDefinition(db, 1, 1, "category");

    var search_bitmap = try roaring.Bitmap.fromSlice(&.{ 1, 2, 4 });
    defer search_bitmap.deinit();
    try insertPostingBitmap(db, std.testing.allocator, 1, 1, "search", 0, search_bitmap);

    var filtering_bitmap = try roaring.Bitmap.fromSlice(&.{3});
    defer filtering_bitmap.deinit();
    try insertPostingBitmap(db, std.testing.allocator, 1, 1, "filtering", 0, filtering_bitmap);

    var filter_bitmap = try roaring.Bitmap.fromSlice(&.{ 1, 3, 4 });
    defer filter_bitmap.deinit();

    const repository = Repository{ .db = &db };
    const counts = try facet_store.countFacetValues(
        std.testing.allocator,
        repository.asFacetRepository(),
        "docs_fixture",
        "category",
        filter_bitmap,
    );
    defer {
        for (counts) |count| std.testing.allocator.free(count.facet_value);
        std.testing.allocator.free(counts);
    }

    try std.testing.expectEqual(@as(usize, 2), counts.len);
    try std.testing.expectEqualStrings("search", counts[0].facet_value);
    try std.testing.expectEqual(@as(u64, 2), counts[0].cardinality);
    try std.testing.expectEqualStrings("filtering", counts[1].facet_value);
    try std.testing.expectEqual(@as(u64, 1), counts[1].cardinality);
}

test "facet parity helpers expose counts, top values, and drop helpers" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertFacetTable(db, 7, "public", "docs_fixture", 4);
    try insertFacetDefinition(db, 7, 1, "category");
    try insertFacetDefinition(db, 7, 2, "region");

    var search_chunk0 = try roaring.Bitmap.fromSlice(&.{ 1, 2, 4 });
    defer search_chunk0.deinit();
    try insertPostingBitmap(db, std.testing.allocator, 7, 1, "search", 0, search_chunk0);
    var search_chunk1 = try roaring.Bitmap.fromSlice(&.{ 5, 6 });
    defer search_chunk1.deinit();
    try insertPostingBitmap(db, std.testing.allocator, 7, 1, "search", 1, search_chunk1);

    var filtering_chunk0 = try roaring.Bitmap.fromSlice(&.{3});
    defer filtering_chunk0.deinit();
    try insertPostingBitmap(db, std.testing.allocator, 7, 1, "filtering", 0, filtering_chunk0);

    var eu_chunk0 = try roaring.Bitmap.fromSlice(&.{ 1, 4 });
    defer eu_chunk0.deinit();
    try insertPostingBitmap(db, std.testing.allocator, 7, 2, "eu", 0, eu_chunk0);

    const counts = try getFacetCounts(db, std.testing.allocator, 7, 1);
    defer {
        for (counts) |row| std.testing.allocator.free(row.facet_value);
        std.testing.allocator.free(counts);
    }
    try std.testing.expectEqual(@as(usize, 2), counts.len);

    const top = try topValues(db, std.testing.allocator, 7, 1, 1);
    defer {
        for (top) |row| std.testing.allocator.free(row.facet_value);
        std.testing.allocator.free(top);
    }
    try std.testing.expectEqual(@as(usize, 1), top.len);
    try std.testing.expectEqualStrings("search", top[0].facet_value);
    try std.testing.expectEqual(@as(u64, 5), top[0].doc_count);

    try std.testing.expectEqual(@as(u8, 14), optimalChunkBits(50_000));
    try std.testing.expectEqual(@as(u8, 16), optimalChunkBits(500_000));

    try dropFacets(db, 7, &.{1});
    const remaining = try listFacetDefinitions(db, std.testing.allocator, 7);
    defer {
        for (remaining) |entry| std.testing.allocator.free(entry.facet_name);
        std.testing.allocator.free(remaining);
    }
    try std.testing.expectEqual(@as(usize, 1), remaining.len);
    try std.testing.expectEqualStrings("region", remaining[0].facet_name);

    try dropFaceting(db, 7);
    const after = try listFacetDefinitions(db, std.testing.allocator, 7);
    defer std.testing.allocator.free(after);
    try std.testing.expectEqual(@as(usize, 0), after.len);
}

test "sqlite-backed facet repository traverses hierarchy children" {
    const facet_store = @import("facet_store.zig");

    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertFacetTable(db, 1, "public", "docs_fixture", 4);
    try insertFacetDefinition(db, 1, 1, "main_category");
    try insertFacetDefinition(db, 1, 2, "category");
    try insertFacetValueNode(db, std.testing.allocator, 1, 10, 1, "science", &.{ 20, 21 });
    try insertFacetValueNode(db, std.testing.allocator, 1, 20, 2, "physics", null);
    try insertFacetValueNode(db, std.testing.allocator, 1, 21, 2, "chemistry", null);

    const repository = Repository{ .db = &db };
    const children = try facet_store.listHierarchyChildren(
        std.testing.allocator,
        repository.asFacetRepository(),
        "docs_fixture",
        "main_category",
        "science",
    );
    defer {
        for (children) |*child| deinitValueNode(std.testing.allocator, child);
        std.testing.allocator.free(children);
    }

    try std.testing.expectEqual(@as(usize, 2), children.len);
    try std.testing.expectEqualStrings("physics", children[0].facet_value);
    try std.testing.expectEqualStrings("chemistry", children[1].facet_value);
}
