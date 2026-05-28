const std = @import("std");
const collections_sqlite = @import("collections_sqlite.zig");
const facet_sqlite = @import("facet_sqlite.zig");
const qualification_apply = @import("qualification_apply.zig");
const qualification_normalize = @import("qualification_normalize.zig");
const zig16_compat = @import("zig16_compat.zig");

const facet_filter = "domain.building,domain.decision,domain.role,domain.scenario,domain.unit,finance.payment_status,source.document_type";

fn seedQualifyWorkspace(db: facet_sqlite.Database) !void {
    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws-qualify" });
    try collections_sqlite.ensureCollection(db, .{
        .workspace_id = "ws-qualify",
        .collection_id = "ws-qualify::docs",
        .name = "docs",
    });
    try collections_sqlite.ensureOntology(db, .{
        .ontology_id = "ws-qualify::core",
        .workspace_id = "ws-qualify",
        .name = "core",
    });
    for (1..10) |doc_id| {
        try collections_sqlite.upsertDocumentRaw(db, .{
            .workspace_id = "ws-qualify",
            .collection_id = "ws-qualify::docs",
            .doc_id = doc_id,
            .source_ref = "doc.md",
            .content = "fixture",
        });
    }
}

test "normalizeQualificationFacet splits ontology_id namespace drift" {
    const normalized = qualification_normalize.normalizeQualificationFacet(
        "ws::core",
        "domain.building",
        facet_filter,
    );
    try std.testing.expectEqualStrings("domain", normalized.namespace);
    try std.testing.expectEqualStrings("building", normalized.dimension);
}

test "applyQualificationEnvelope accepts syndic drift envelope" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try seedQualifyWorkspace(db);

    const json = try std.Io.Dir.cwd().readFileAlloc(
        zig16_compat.io(),
        "fixtures/corpus_eval/syndic/qualification_llm_drift.json",
        std.testing.allocator,
        .limited(1024 * 1024),
    );
    defer std.testing.allocator.free(json);
    const accepted = try qualification_apply.applyQualificationEnvelope(std.testing.allocator, db, .{
        .workspace_id = "ws-qualify",
        .collection_id = "ws-qualify::docs",
        .ontology_id = "ws-qualify::core",
        .facet_filter = facet_filter,
        .source = "test-fixture",
        .dry_run = false,
        .json = json,
    });
    try std.testing.expect(accepted >= 24);

    const stmt = try facet_sqlite.prepare(db, "SELECT COUNT(*) FROM facet_assignments_raw WHERE workspace_id = ?1");
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, "ws-qualify");
    try std.testing.expect(facet_sqlite.c.sqlite3_step(stmt) == facet_sqlite.c.SQLITE_ROW);
    try std.testing.expect(facet_sqlite.c.sqlite3_column_int64(stmt, 0) >= 24);
}

test "applyQualificationEnvelope rejects unknown facet" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try seedQualifyWorkspace(db);

    const json =
        \\{"assignments":[{"target_kind":"doc","doc_id":1,"namespace":"foo","dimension":"bar","value":"x","weight":1}]}
    ;
    const result = qualification_apply.applyQualificationEnvelope(std.testing.allocator, db, .{
        .workspace_id = "ws-qualify",
        .collection_id = "ws-qualify::docs",
        .ontology_id = "ws-qualify::core",
        .facet_filter = facet_filter,
        .source = "test-fixture",
        .dry_run = false,
        .json = json,
    });
    try std.testing.expectError(qualification_apply.CliError.InvalidArguments, result);

    const stmt = try facet_sqlite.prepare(db, "SELECT COUNT(*) FROM facet_assignments_raw WHERE workspace_id = ?1");
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, "ws-qualify");
    try std.testing.expect(facet_sqlite.c.sqlite3_step(stmt) == facet_sqlite.c.SQLITE_ROW);
    try std.testing.expectEqual(@as(i64, 0), facet_sqlite.c.sqlite3_column_int64(stmt, 0));
}

test "facetCsvContains parses domain.building filter" {
    try std.testing.expect(qualification_normalize.facetCsvContains(facet_filter, "domain", "building"));
    try std.testing.expect(!qualification_normalize.facetCsvContains(facet_filter, "ws::core", "domain.building"));
}
