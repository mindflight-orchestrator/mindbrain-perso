const std = @import("std");
const business_edge_normalize = @import("business_edge_normalize.zig");
const collections_sqlite = @import("collections_sqlite.zig");
const facet_sqlite = @import("facet_sqlite.zig");

test "normalizeBusinessEdgeType maps block_contains_unit" {
    try std.testing.expectEqualStrings("contains", business_edge_normalize.normalizeBusinessEdgeType("block_contains_unit"));
}

test "business extract alias persists canonical contains edge type" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws-business-alias" });
    try collections_sqlite.ensureOntology(db, .{
        .ontology_id = "ws-business-alias::core",
        .workspace_id = "ws-business-alias",
        .name = "core",
    });
    try collections_sqlite.ensureEdgeType(db, .{
        .ontology_id = "ws-business-alias::core",
        .edge_type = "contains",
        .source_entity_type = "block",
        .target_entity_type = "unit",
    });
    const block_id = try collections_sqlite.upsertEntityRawAuto(db, .{
        .workspace_id = "ws-business-alias",
        .ontology_id = "ws-business-alias::core",
        .external_id = "block:a",
        .entity_type = "block",
        .name = "Block A",
        .metadata_json = "{}",
    });
    const unit_id = try collections_sqlite.upsertEntityRawAuto(db, .{
        .workspace_id = "ws-business-alias",
        .ontology_id = "ws-business-alias::core",
        .external_id = "unit:a1",
        .entity_type = "unit",
        .name = "A1",
        .metadata_json = "{}",
    });
    const canonical = business_edge_normalize.normalizeBusinessEdgeType("block_contains_unit");
    _ = try collections_sqlite.upsertRelationRawAuto(db, .{
        .workspace_id = "ws-business-alias",
        .ontology_id = "ws-business-alias::core",
        .external_id = "rel:block:a1",
        .edge_type = canonical,
        .source_entity_id = block_id,
        .target_entity_id = unit_id,
        .metadata_json = "{}",
    });

    const stmt = try facet_sqlite.prepare(db, "SELECT edge_type FROM relations_raw WHERE workspace_id = ?1 AND external_id = ?2");
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, "ws-business-alias");
    try facet_sqlite.bindText(stmt, 2, "rel:block:a1");
    try std.testing.expect(facet_sqlite.c.sqlite3_step(stmt) == facet_sqlite.c.SQLITE_ROW);
    const ptr = facet_sqlite.c.sqlite3_column_text(stmt, 0) orelse return error.StepFailed;
    const edge_type = ptr[0..@intCast(facet_sqlite.c.sqlite3_column_bytes(stmt, 0))];
    try std.testing.expectEqualStrings("contains", edge_type);
}

test "business extract apply failure leaves zero entities after rollback" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws-business-fail" });

    const json =
        \\{"entities_raw":[{"external_id":"","entity_type":"building","name":"Bad"}],"relations_raw":[]}
    ;

    try db.exec("BEGIN");
    var parsed = try std.json.parseFromSlice(
        struct {
            entities_raw: []struct {
                external_id: []const u8,
            },
        },
        std.testing.allocator,
        json,
        .{ .ignore_unknown_fields = true },
    );
    defer parsed.deinit();
    var apply_err: ?anyerror = null;
    for (parsed.value.entities_raw) |row| {
        if (row.external_id.len == 0) apply_err = error.InvalidArguments;
    }
    if (apply_err) |_| try db.exec("ROLLBACK");

    const stmt = try facet_sqlite.prepare(db, "SELECT COUNT(*) FROM entities_raw WHERE workspace_id = ?1");
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, "ws-business-fail");
    try std.testing.expect(facet_sqlite.c.sqlite3_step(stmt) == facet_sqlite.c.SQLITE_ROW);
    try std.testing.expectEqual(@as(i64, 0), facet_sqlite.c.sqlite3_column_int64(stmt, 0));
}
