const std = @import("std");
const collections_sqlite = @import("collections_sqlite.zig");
const facet_sqlite = @import("facet_sqlite.zig");
const linkml_interchange = @import("linkml_interchange.zig");
const syndic_profile_seed = @import("syndic_profile_seed.zig");

test "syndic compile profile leaves core as default after ingest bootstrap" {
    var result = try linkml_interchange.compileLinkmlToBundle(std.testing.allocator, .{
        .input_path = "ontologies/immeuble-demo/core.yaml",
        .workspace_id = "ws-syndic-lab",
        .ontology_id = "ws-syndic-lab::core",
    });
    defer result.deinit(std.testing.allocator);

    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try collections_sqlite.ensureWorkspace(db, .{
        .workspace_id = "ws-syndic-lab",
        .domain_profile = "syndic",
    });
    try linkml_interchange.importCompiledBundle(db, std.testing.allocator, result.bundle_json);
    try syndic_profile_seed.seedSyndicProfile(db, "ws-syndic-lab", "ws-syndic-lab::core");
    try collections_sqlite.setDefaultOntology(db, "ws-syndic-lab", "ws-syndic-lab::core");

    // document-ingest / persistProfiledDocument calls ensureWorkspace again.
    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws-syndic-lab" });

    const default_id = try collections_sqlite.defaultOntology(db, std.testing.allocator, "ws-syndic-lab");
    defer if (default_id) |id| std.testing.allocator.free(id);
    try std.testing.expect(default_id != null);
    try std.testing.expectEqualStrings("ws-syndic-lab::core", default_id.?);
}

test "syndic profile seeds dimensions on target ontology only" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try syndic_profile_seed.seedSyndicProfile(db, "ws-syndic-a", "ws-syndic-a::core");
    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws-syndic-b" });
    try collections_sqlite.ensureOntology(db, .{
        .ontology_id = "ws-syndic-b::core",
        .workspace_id = "ws-syndic-b",
        .name = "core",
    });

    const countForOntology = struct {
        fn run(db_handle: facet_sqlite.Database, ontology_id: []const u8) !i64 {
            const stmt = try facet_sqlite.prepare(
                db_handle,
                "SELECT COUNT(*) FROM ontology_dimensions WHERE ontology_id = ?1",
            );
            defer facet_sqlite.finalize(stmt);
            try facet_sqlite.bindText(stmt, 1, ontology_id);
            try std.testing.expect(facet_sqlite.c.sqlite3_step(stmt) == facet_sqlite.c.SQLITE_ROW);
            return facet_sqlite.c.sqlite3_column_int64(stmt, 0);
        }
    };

    try std.testing.expect(try countForOntology.run(db, "ws-syndic-a::core") >= syndic_profile_seed.syndicDimensionCount());
    try std.testing.expectEqual(@as(i64, 0), try countForOntology.run(db, "ws-syndic-b::core"));
}
