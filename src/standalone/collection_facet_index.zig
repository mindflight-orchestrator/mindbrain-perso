//! Exact collection facet projection. Bitmap IDs identify targets, never
//! external document IDs. Raw rows supply provenance within the same snapshot.
const std = @import("std");
const sqlite = @import("facet_sqlite.zig");
const roaring = @import("roaring.zig");
const Allocator = std.mem.Allocator;

/// Caller owns the write transaction. Publishing clean state is atomic with
/// replacement of both the bitmap and its identity mapping.
pub fn rebuild(allocator: Allocator, db: sqlite.Database, ws: []const u8, collection: []const u8) !u64 {
    inline for (.{ "collection_facet_targets", "collection_facet_postings" }) |table| {
        const stmt = try sqlite.prepare(db, "DELETE FROM " ++ table ++ " WHERE workspace_id=?1 AND collection_id=?2");
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, ws);
        try sqlite.bindText(stmt, 2, collection);
        try sqlite.stepDone(stmt);
    }
    const targets = try sqlite.prepare(db,
        \\INSERT INTO collection_facet_targets
        \\SELECT ?1,?2,ROW_NUMBER() OVER (ORDER BY target_kind,doc_id,chunk_index)-1,target_kind,doc_id,chunk_index
        \\FROM facet_assignments_raw WHERE workspace_id=?1 AND collection_id=?2
        \\GROUP BY target_kind,doc_id,chunk_index
    );
    defer sqlite.finalize(targets);
    try sqlite.bindText(targets, 1, ws);
    try sqlite.bindText(targets, 2, collection);
    try sqlite.stepDone(targets);
    const groups = try sqlite.prepare(db,
        \\SELECT ontology_id,namespace,dimension,value FROM facet_assignments_raw
        \\WHERE workspace_id=?1 AND collection_id=?2
        \\GROUP BY ontology_id,namespace,dimension,value
    );
    defer sqlite.finalize(groups);
    try sqlite.bindText(groups, 1, ws);
    try sqlite.bindText(groups, 2, collection);
    const members = try sqlite.prepare(db,
        \\SELECT t.dense_id FROM facet_assignments_raw a JOIN collection_facet_targets t
        \\ON t.workspace_id=a.workspace_id AND t.collection_id=a.collection_id
        \\AND t.target_kind=a.target_kind AND t.doc_id=a.doc_id AND t.chunk_index=a.chunk_index
        \\WHERE a.workspace_id=?1 AND a.collection_id=?2 AND a.ontology_id=?3
        \\AND a.namespace=?4 AND a.dimension=?5 AND a.value=?6 ORDER BY t.dense_id
    );
    defer sqlite.finalize(members);
    const posting = try sqlite.prepare(db, "INSERT INTO collection_facet_postings VALUES (?1,?2,?3,?4,?5,?6,?7)");
    defer sqlite.finalize(posting);
    var total: u64 = 0;
    while (try next(groups)) {
        try sqlite.resetStatement(members);
        try sqlite.resetStatement(posting);
        for ([_]*sqlite.c.sqlite3_stmt{ members, posting }) |stmt| {
            try sqlite.bindText(stmt, 1, ws);
            try sqlite.bindText(stmt, 2, collection);
            for (0..4) |i| {
                const text = try sqlite.dupeColumnText(allocator, groups, @intCast(i));
                defer allocator.free(text);
                try sqlite.bindText(stmt, @intCast(i + 3), text);
            }
        }
        var bitmap = try roaring.Bitmap.empty();
        defer bitmap.deinit();
        while (try next(members)) {
            const id = try sqlite.columnU32(members, 0);
            bitmap.add(id);
            total += 1;
        }
        const blob = try bitmap.serializePortable(allocator);
        defer allocator.free(blob);
        try sqlite.bindBlob(posting, 7, blob);
        try sqlite.stepDone(posting);
    }
    const state = try sqlite.prepare(db,
        \\INSERT INTO collection_facet_index_state VALUES (?1,?2,0)
        \\ON CONFLICT(workspace_id,collection_id) DO UPDATE SET dirty=0
    );
    defer sqlite.finalize(state);
    try sqlite.bindText(state, 1, ws);
    try sqlite.bindText(state, 2, collection);
    try sqlite.stepDone(state);
    return total;
}

/// Null means missing/stale projection; an empty bitmap is a valid indexed
/// answer. Caller must retain a read snapshot through identity decoding.
pub fn matching(allocator: Allocator, db: sqlite.Database, ws: []const u8, collection: []const u8, namespace: ?[]const u8, dimension: ?[]const u8, value: ?[]const u8) !?roaring.Bitmap {
    const state = try sqlite.prepare(db, "SELECT dirty FROM collection_facet_index_state WHERE workspace_id=?1 AND collection_id=?2");
    defer sqlite.finalize(state);
    try sqlite.bindText(state, 1, ws);
    try sqlite.bindText(state, 2, collection);
    if (!try next(state) or try sqlite.columnI64(state, 0) != 0) return null;
    const stmt = try sqlite.prepare(db,
        \\SELECT posting_blob FROM collection_facet_postings
        \\WHERE workspace_id=?1 AND collection_id=?2
        \\AND (?3 IS NULL OR namespace=?3) AND (?4 IS NULL OR dimension=?4)
        \\AND (?5 IS NULL OR value LIKE '%' || ?5 || '%')
    );
    defer sqlite.finalize(stmt);
    try sqlite.bindText(stmt, 1, ws);
    try sqlite.bindText(stmt, 2, collection);
    for ([_]?[]const u8{ namespace, dimension, value }, 3..) |param, i| {
        if (param) |text| try sqlite.bindText(stmt, @intCast(i), text) else try sqlite.bindNull(stmt, @intCast(i));
    }
    var result = try roaring.Bitmap.empty();
    errdefer result.deinit();
    while (try next(stmt)) {
        const len = sqlite.c.sqlite3_column_bytes(stmt, 0);
        const ptr = sqlite.c.sqlite3_column_blob(stmt, 0) orelse return error.MissingRow;
        const blob = @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
        var bitmap = try roaring.Bitmap.deserializePortable(blob);
        defer bitmap.deinit();
        result.orInPlace(bitmap);
    }
    _ = allocator;
    return result;
}

fn next(stmt: *sqlite.c.sqlite3_stmt) !bool {
    return switch (sqlite.c.sqlite3_step(stmt)) {
        sqlite.c.SQLITE_ROW => true,
        sqlite.c.SQLITE_DONE => false,
        else => error.StepFailed,
    };
}
