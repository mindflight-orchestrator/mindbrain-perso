//! Integration tests that read fixtures from `data/sources/<family>/` and
//! exercise `Pipeline.ingestDocumentChunked` against realistic legal-style
//! markdown (EU AI Act–like structure).

const std = @import("std");
const collections_sqlite = @import("collections_sqlite.zig");
const facet_sqlite = @import("facet_sqlite.zig");
const facet_store = @import("facet_store.zig");
const graph_store = @import("graph_store.zig");
const import_pipeline = @import("import_pipeline.zig");
const search_store = @import("search_store.zig");

const Pipeline = import_pipeline.Pipeline;

/// Path relative to the repository root (where `zig build test` runs).
pub const iac_act_sample_path = "data/sources/iac_act/eu_ai_act_sample.md";

test "data/sources iac_act sample ingests with nanoid, chunks, and source.* facets" {
    const allocator = std.testing.allocator;

    const content = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, iac_act_sample_path, allocator, .limited(10 * 1024 * 1024));
    defer allocator.free(content);

    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    var search = search_store.Store.init(allocator);
    defer search.deinit();
    var facets = facet_store.Store.init(allocator);
    defer facets.deinit();
    var graph = graph_store.Store.init(allocator);
    defer graph.deinit();

    var pipeline = Pipeline{
        .allocator = allocator,
        .db = &db,
        .search = &search,
        .facets = &facets,
        .graph = &graph,
    };

    try pipeline.createWorkspace(.{ .workspace_id = "ws_iac", .label = "IAC fixture" });
    try pipeline.createCollection(.{
        .workspace_id = "ws_iac",
        .collection_id = "ws_iac::legal",
        .name = "legal",
    });

    const source_ref = iac_act_sample_path;

    var result = try pipeline.ingestDocumentChunked(.{
        .workspace_id = "ws_iac",
        .collection_id = "ws_iac::legal",
        .doc_id = 1,
        .content = content,
        .language = "english",
        .source_ref = source_ref,
        .ingested_at = "2026-04-21T12:00:00Z",
        .chunker_options = .{ .strategy = .structure_aware },
    });
    defer result.deinit(allocator);

    try std.testing.expect(result.doc_nanoid.len > 0);
    try std.testing.expect(result.chunk_count >= 2);

    const Counts = struct {
        fn one(d: facet_sqlite.Database, sql: []const u8) !i64 {
            const stmt = try facet_sqlite.prepare(d, sql);
            defer facet_sqlite.finalize(stmt);
            try std.testing.expectEqual(facet_sqlite.c.SQLITE_ROW, facet_sqlite.c.sqlite3_step(stmt));
            return facet_sqlite.c.sqlite3_column_int64(stmt, 0);
        }
    };

    try std.testing.expectEqual(@as(i64, 1), try Counts.one(db, "SELECT COUNT(*) FROM documents_raw WHERE workspace_id = 'ws_iac' AND doc_nanoid <> ''"));
    try std.testing.expectEqual(
        @as(i64, @intCast(result.chunk_count)),
        try Counts.one(db, "SELECT COUNT(*) FROM chunks_raw WHERE workspace_id = 'ws_iac'"),
    );
    try std.testing.expectEqual(
        @as(i64, @intCast(result.chunk_count)),
        try Counts.one(db, "SELECT COUNT(*) FROM chunks_raw WHERE strategy = 'structure_aware' AND token_count > 0"),
    );

    try std.testing.expectEqual(
        @as(i64, @intCast(result.chunk_count)),
        try Counts.one(db, "SELECT COUNT(*) FROM facet_assignments_raw WHERE namespace = 'source' AND dimension = 'filename' AND value = 'eu_ai_act_sample.md'"),
    );
    try std.testing.expectEqual(
        @as(i64, @intCast(result.chunk_count)),
        try Counts.one(db, "SELECT COUNT(*) FROM facet_assignments_raw WHERE namespace = 'source' AND dimension = 'extension' AND value = 'md'"),
    );

    const lookup = try collections_sqlite.lookupDocByNanoid(db, allocator, result.doc_nanoid);
    try std.testing.expect(lookup != null);
    if (lookup) |found| {
        defer found.deinit(allocator);
        try std.testing.expectEqualStrings("ws_iac", found.workspace_id);
        try std.testing.expectEqualStrings("ws_iac::legal", found.collection_id);
        try std.testing.expectEqual(@as(u64, 1), found.doc_id);
    }

    try pipeline.linkExternal(.{
        .workspace_id = "ws_iac",
        .link_id = 1,
        .source_collection_id = "ws_iac::legal",
        .source_doc_id = 1,
        .target_uri = "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689",
        .edge_type = "official_text",
    });
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(db, "SELECT COUNT(*) FROM external_links_raw WHERE target_uri LIKE 'https://eur-lex.europa.eu%'"));
}
