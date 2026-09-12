//! Explicit, workspace-scoped graph identity resolution. No text fallback.
const std = @import("std");
const sql = @import("facet_sqlite.zig");
const facts = @import("facts_sqlite.zig");

pub const Reference = struct {
    kind: enum { external_id, fact_id, entity_id, name },
    value: std.json.Value,
    ontology_id: ?[]const u8 = null,
    entity_type: ?[]const u8 = null,
};

pub fn resolve(db: sql.Database, workspace: []const u8, ref: Reference) !u32 {
    if (workspace.len == 0) return error.BadRequest;
    const selector: []const u8 = switch (ref.kind) {
        .external_id => "r.external_id=?2",
        .name => "g.name=?2",
        .entity_id => "g.entity_id=?2",
        .fact_id => comptime "EXISTS (SELECT 1 FROM structured_import_provenance p JOIN agent_facts a ON a.id=p.fact_id AND a.workspace_id=p.workspace_id WHERE p.workspace_id=g.workspace_id AND p.entity_external_id=r.external_id AND a.id=?2 AND " ++ facts.activeWindowSql("a.") ++ ")",
    };
    var buffer: [2048]u8 = undefined;
    const query = try std.fmt.bufPrint(&buffer, "SELECT DISTINCT g.entity_id FROM graph_entity g LEFT JOIN entities_raw r ON r.entity_id=g.entity_id AND r.workspace_id=g.workspace_id " ++
        "WHERE g.workspace_id=?1 AND g.deprecated_at IS NULL AND (r.entity_id IS NULL OR (r.name=g.name AND r.entity_type=g.entity_type AND r.metadata_json=g.metadata_json)) AND {s} AND (?3 IS NULL OR r.ontology_id=?3) AND (?4 IS NULL OR g.entity_type=?4) ORDER BY g.entity_id LIMIT 2", .{selector});
    const stmt = try sql.prepare(db, query);
    defer sql.finalize(stmt);
    try sql.bindText(stmt, 1, workspace);
    if (ref.kind == .entity_id) {
        if (ref.value != .integer or ref.value.integer <= 0 or ref.value.integer > std.math.maxInt(u32)) return error.BadRequest;
        try sql.bindInt64(stmt, 2, ref.value.integer);
    } else {
        if (ref.value != .string or ref.value.string.len == 0) return error.BadRequest;
        try sql.bindText(stmt, 2, ref.value.string);
    }
    if (ref.ontology_id) |value| try sql.bindText(stmt, 3, value) else try sql.bindNull(stmt, 3);
    if (ref.entity_type) |value| try sql.bindText(stmt, 4, value) else try sql.bindNull(stmt, 4);
    const step = sql.c.sqlite3_step(stmt);
    if (step == sql.c.SQLITE_DONE) return error.NotFound;
    if (step != sql.c.SQLITE_ROW) return error.StepFailed;
    const id = std.math.cast(u32, try sql.columnU64(stmt, 0)) orelse return error.ValueOutOfRange;
    const next = sql.c.sqlite3_step(stmt);
    if (next == sql.c.SQLITE_ROW) return error.AmbiguousReference;
    if (next != sql.c.SQLITE_DONE) return error.StepFailed;
    return id;
}

test "external identity differs from display name; scopes and kinds fail closed" {
    const c = @import("collections_sqlite.zig");
    const reindex = @import("reindex_http.zig");
    var db = try sql.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    const a = std.testing.allocator;
    try c.ensureWorkspace(db, .{ .workspace_id = "b1", .label = "b1" });
    try c.ensureOntology(db, .{ .workspace_id = "b1", .ontology_id = "b1:legal", .name = "legal" });
    const id = try c.upsertEntityRawAuto(db, .{ .workspace_id = "b1", .ontology_id = "b1:legal", .external_id = "001", .entity_type = "Assertion", .name = "Une assertion" });
    _ = try reindex.reindexGraph(a, &db, "b1", null);
    const ref: Reference = .{ .kind = .external_id, .value = .{ .string = "001" } };
    try std.testing.expectEqual(id, try resolve(db, "b1", ref));
    try std.testing.expectError(error.NotFound, resolve(db, "other", ref));
    try std.testing.expectError(error.NotFound, resolve(db, "b1", .{ .kind = .name, .value = .{ .string = "001" } }));
    try std.testing.expectError(error.BadRequest, resolve(db, "b1", .{ .kind = .entity_id, .value = .{ .string = "001" } }));
    try std.testing.expectError(error.NotFound, resolve(db, "b1", .{ .kind = .external_id, .value = .{ .string = "001" }, .ontology_id = "wrong" }));
    _ = try c.upsertEntityRawAuto(db, .{ .workspace_id = "b1", .ontology_id = "b1:legal", .external_id = "002", .entity_type = "Document", .name = "Une assertion" });
    _ = try reindex.reindexGraph(a, &db, "b1", null);
    try std.testing.expectError(error.AmbiguousReference, resolve(db, "b1", .{ .kind = .name, .value = .{ .string = "Une assertion" } }));
}
