//! HTTP helpers for workspace-scoped reindex and collection facet search.

const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const facet_store = @import("facet_store.zig");
const graph_store = @import("graph_store.zig");
const import_pipeline = @import("import_pipeline.zig");
const interfaces = @import("interfaces.zig");
const search_sqlite = @import("search_sqlite.zig");
const search_store = @import("search_store.zig");
const roaring = @import("roaring.zig");

const Allocator = std.mem.Allocator;
const Database = facet_sqlite.Database;

pub const ReindexGraphResult = struct {
    projected_count: u64,
    document_table_id: ?u64 = null,
    /// True when Roaring adjacency (graph_lj_out/in) was rebuilt as part of this
    /// reindex. The native pipeline always rebuilds it; only the GhostCrab SQL
    /// fallback path leaves it stale.
    adjacency_rebuilt: bool = true,
};

pub const ReindexAllResult = struct {
    graph_projected: u64,
    facet_assignments: u64,
    bm25_documents: u64,
};

pub const CollectionFacetMatch = struct {
    doc_id: u64,
    chunk_index: ?u32,
    namespace: []const u8,
    dimension: []const u8,
    value: []const u8,
    weight: f32,

    pub fn deinit(self: CollectionFacetMatch, allocator: Allocator) void {
        allocator.free(self.namespace);
        allocator.free(self.dimension);
        allocator.free(self.value);
    }
};

pub const SearchCollectionFacetsResult = struct {
    matches: []CollectionFacetMatch,
    source: []const u8,
};

fn initPipeline(allocator: Allocator, db: *const Database, workspace_id: []const u8) struct {
    search: search_store.Store,
    facets: facet_store.Store,
    graph: graph_store.Store,
    pipeline: import_pipeline.Pipeline,
} {
    return .{
        .search = search_store.Store.init(allocator),
        .facets = facet_store.Store.init(allocator),
        .graph = graph_store.Store.init(allocator),
        .pipeline = .{
            .allocator = allocator,
            .db = db,
            .search = undefined,
            .facets = undefined,
            .graph = undefined,
            .workspace_id = workspace_id,
        },
    };
}

pub fn reindexGraph(
    allocator: Allocator,
    db: *const Database,
    workspace_id: []const u8,
    document_table_id: ?u64,
) !ReindexGraphResult {
    var bundle = initPipeline(allocator, db, workspace_id);
    defer bundle.search.deinit();
    defer bundle.facets.deinit();
    defer bundle.graph.deinit();
    bundle.pipeline.search = &bundle.search;
    bundle.pipeline.facets = &bundle.facets;
    bundle.pipeline.graph = &bundle.graph;

    const projected = try bundle.pipeline.reindexGraphWithDocumentTable(workspace_id, document_table_id);
    return .{
        .projected_count = projected,
        .document_table_id = document_table_id,
        .adjacency_rebuilt = true,
    };
}

pub fn reindexAll(
    allocator: Allocator,
    db: *const Database,
    workspace_id: []const u8,
    collection_id: []const u8,
    table_id: u64,
) !ReindexAllResult {
    var bundle = initPipeline(allocator, db, workspace_id);
    defer bundle.search.deinit();
    defer bundle.facets.deinit();
    defer bundle.graph.deinit();
    bundle.pipeline.search = &bundle.search;
    bundle.pipeline.facets = &bundle.facets;
    bundle.pipeline.graph = &bundle.graph;
    bundle.pipeline.collection_id = collection_id;

    const table_config = try facet_sqlite.loadFacetTableConfigByTableId(db.*, allocator, table_id);
    defer {
        allocator.free(table_config.schema_name);
        allocator.free(table_config.table_name);
    }
    if (!std.mem.eql(u8, table_config.table_name, collection_id)) return error.TableNotFound;
    try bundle.facets.registerTable(table_config);

    const bm25 = try bundle.pipeline.reindexBm25(workspace_id, collection_id, .{ .table_id = table_id });
    const facets = try bundle.pipeline.reindexFacets(workspace_id, collection_id, table_id);
    const graph = try bundle.pipeline.reindexGraphWithDocumentTable(workspace_id, table_id);
    return .{
        .graph_projected = graph,
        .facet_assignments = facets,
        .bm25_documents = bm25.documents,
    };
}

fn valueMatchesQuery(facet_value: []const u8, value_query: ?[]const u8) bool {
    const query = value_query orelse return true;
    if (query.len == 0) return true;
    if (query.len > facet_value.len) return false;
    var i: usize = 0;
    while (i + query.len <= facet_value.len) : (i += 1) {
        if (std.ascii.eqlIgnoreCase(facet_value[i .. i + query.len], query)) return true;
    }
    return false;
}

fn appendFmt(
    allocator: Allocator,
    list: *std.ArrayList(u8),
    comptime fmt: []const u8,
    args: anytype,
) !void {
    const rendered = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(rendered);
    try list.appendSlice(allocator, rendered);
}

fn tryResolveFacetTable(
    db: Database,
    allocator: Allocator,
    collection_id: []const u8,
    table_id_param: ?u64,
) !?interfaces.FacetTableConfig {
    if (table_id_param) |table_id| {
        // Only a missing registration demotes to the raw fallback; real DB
        // errors must surface instead of silently degrading the search path.
        const config = facet_sqlite.loadFacetTableConfigByTableId(db, allocator, table_id) catch |err| switch (err) {
            error.MissingRow => return null,
            else => return err,
        };
        if (!std.mem.eql(u8, config.table_name, collection_id)) {
            allocator.free(config.schema_name);
            allocator.free(config.table_name);
            return error.TableNotFound;
        }
        return config;
    }
    return facet_sqlite.loadFacetTableConfig(db, allocator, collection_id) catch |err| switch (err) {
        error.MissingRow => null,
        else => err,
    };
}

fn searchCollectionFacetsRaw(
    allocator: Allocator,
    db: Database,
    workspace_id: []const u8,
    collection_id: []const u8,
    namespace: ?[]const u8,
    dimension: ?[]const u8,
    value_query: ?[]const u8,
    limit: usize,
) ![]CollectionFacetMatch {
    var sql = std.ArrayList(u8).empty;
    defer sql.deinit(allocator);
    try sql.appendSlice(allocator,
        \\SELECT doc_id, chunk_index, namespace, dimension, value, weight
        \\FROM facet_assignments_raw
        \\WHERE workspace_id = ?1 AND collection_id = ?2
    );

    var params = std.ArrayList([]const u8).empty;
    defer params.deinit(allocator);
    try params.append(allocator, workspace_id);
    try params.append(allocator, collection_id);

    var value_pattern: ?[]u8 = null;
    defer if (value_pattern) |pattern| allocator.free(pattern);

    if (namespace) |ns| {
        try appendFmt(allocator, &sql, " AND namespace = ?{d}", .{params.items.len + 1});
        try params.append(allocator, ns);
    }
    if (dimension) |dim| {
        try appendFmt(allocator, &sql, " AND dimension = ?{d}", .{params.items.len + 1});
        try params.append(allocator, dim);
    }
    if (value_query) |vq| {
        try appendFmt(allocator, &sql, " AND value LIKE ?{d}", .{params.items.len + 1});
        value_pattern = try std.fmt.allocPrint(allocator, "%{s}%", .{vq});
        try params.append(allocator, value_pattern.?);
    }
    try appendFmt(allocator, &sql, " ORDER BY weight DESC, doc_id ASC LIMIT {d}", .{limit});

    const stmt = try facet_sqlite.prepare(db, sql.items);
    defer facet_sqlite.finalize(stmt);
    for (params.items, 0..) |param, index| {
        try facet_sqlite.bindText(stmt, @intCast(index + 1), param);
    }

    var rows = std.ArrayList(CollectionFacetMatch).empty;
    errdefer {
        for (rows.items) |row| row.deinit(allocator);
        rows.deinit(allocator);
    }

    const c = facet_sqlite.c;
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const chunk_index_raw = c.sqlite3_column_int64(stmt, 1);
        const chunk_index: ?u32 = if (chunk_index_raw < 0 or chunk_index_raw > std.math.maxInt(u32))
            null
        else
            @intCast(chunk_index_raw);

        try rows.append(allocator, .{
            .doc_id = @bitCast(c.sqlite3_column_int64(stmt, 0)),
            .chunk_index = chunk_index,
            .namespace = try facet_sqlite.dupeColumnText(allocator, stmt, 2),
            .dimension = try facet_sqlite.dupeColumnText(allocator, stmt, 3),
            .value = try facet_sqlite.dupeColumnText(allocator, stmt, 4),
            .weight = @floatCast(c.sqlite3_column_double(stmt, 5)),
        });
    }

    return rows.toOwnedSlice(allocator);
}

fn searchCollectionFacetsPostings(
    allocator: Allocator,
    db: Database,
    table_config: interfaces.FacetTableConfig,
    namespace: []const u8,
    dimension: []const u8,
    value_query: ?[]const u8,
    limit: usize,
) ![]CollectionFacetMatch {
    const facet_name = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ namespace, dimension });
    defer allocator.free(facet_name);

    const facet_id = facet_sqlite.loadFacetId(db, table_config.table_id, facet_name) catch |err| switch (err) {
        error.MissingRow => return &.{},
        else => return err,
    };

    var rows = std.ArrayList(CollectionFacetMatch).empty;
    errdefer {
        for (rows.items) |row| row.deinit(allocator);
        rows.deinit(allocator);
    }

    var seen_docs = try roaring.Bitmap.empty();
    defer seen_docs.deinit();

    // Single scan over the facet's postings ordered by value: group the chunk
    // blobs per facet_value in-stream instead of issuing one postings query
    // per distinct value (which was O(values) statements per request).
    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT facet_value, chunk_id, posting_blob FROM facet_postings WHERE table_id = {d} AND facet_id = {d} ORDER BY facet_value, chunk_id",
        .{ table_config.table_id, facet_id },
    );
    defer allocator.free(sql);
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);

    const c = facet_sqlite.c;

    var group_value: ?[]const u8 = null;
    defer if (group_value) |value| allocator.free(value);
    var group_postings = std.ArrayList(interfaces.FacetPosting).empty;
    defer {
        for (group_postings.items) |posting| {
            var bitmap = posting.bitmap;
            bitmap.deinit();
        }
        group_postings.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_ROW and rc != c.SQLITE_DONE) return error.StepFailed;
        const end_of_rows = rc == c.SQLITE_DONE;

        var row_value: ?[]const u8 = null;
        errdefer if (row_value) |value| allocator.free(value);
        if (!end_of_rows) row_value = try facet_sqlite.dupeColumnText(allocator, stmt, 0);

        const same_group = group_value != null and row_value != null and
            std.mem.eql(u8, group_value.?, row_value.?);

        var collect = same_group;
        if (!same_group) {
            // Flush the completed group before starting the next one.
            if (group_value) |value| {
                if (try facet_sqlite.reconstructFacetBitmapFromPostings(
                    allocator,
                    table_config.chunk_bits,
                    group_postings.items,
                )) |reconstructed| {
                    var facet_bitmap = reconstructed;
                    defer facet_bitmap.deinit();
                    var iter = roaring.Bitmap.UInt32Iterator.init(facet_bitmap);
                    while (iter.hasValue()) {
                        if (rows.items.len >= limit) break;
                        const doc_id = iter.currentValue();
                        iter.advance();
                        if (seen_docs.contains(doc_id)) continue;
                        seen_docs.add(doc_id);

                        try rows.append(allocator, .{
                            .doc_id = doc_id,
                            .chunk_index = null,
                            .namespace = try allocator.dupe(u8, namespace),
                            .dimension = try allocator.dupe(u8, dimension),
                            .value = try allocator.dupe(u8, value),
                            .weight = 1.0,
                        });
                    }
                }
                for (group_postings.items) |posting| {
                    var bitmap = posting.bitmap;
                    bitmap.deinit();
                }
                group_postings.clearRetainingCapacity();
                allocator.free(value);
                group_value = null;
            }

            if (end_of_rows or rows.items.len >= limit) {
                if (row_value) |value| allocator.free(value);
                break;
            }

            if (valueMatchesQuery(row_value.?, value_query)) {
                group_value = row_value;
                row_value = null; // ownership moved to group_value
                collect = true;
            }
        }

        if (collect) {
            const chunk_id_raw = c.sqlite3_column_int64(stmt, 1);
            if (chunk_id_raw < 0 or chunk_id_raw > std.math.maxInt(u32)) return error.StepFailed;
            const blob_len = c.sqlite3_column_bytes(stmt, 2);
            const blob_ptr = c.sqlite3_column_blob(stmt, 2) orelse return error.MissingRow;
            const blob: []const u8 = @as([*]const u8, @ptrCast(blob_ptr))[0..@intCast(blob_len)];
            try group_postings.append(allocator, .{
                .chunk_id = @intCast(chunk_id_raw),
                .bitmap = try roaring.Bitmap.deserializePortable(blob),
            });
        }

        if (row_value) |value| allocator.free(value);
    }

    return rows.toOwnedSlice(allocator);
}

pub fn searchCollectionFacets(
    allocator: Allocator,
    db: Database,
    workspace_id: []const u8,
    collection_id: []const u8,
    table_id_param: ?u64,
    namespace: ?[]const u8,
    dimension: ?[]const u8,
    value_query: ?[]const u8,
    limit: usize,
) !SearchCollectionFacetsResult {
    if (try tryResolveFacetTable(db, allocator, collection_id, table_id_param)) |table_config| {
        defer {
            allocator.free(table_config.schema_name);
            allocator.free(table_config.table_name);
        }

        const posting_count = try facet_sqlite.countFacetPostingsForTable(db, table_config.table_id);
        if (posting_count > 0 and namespace != null and dimension != null) {
            const matches = try searchCollectionFacetsPostings(
                allocator,
                db,
                table_config,
                namespace.?,
                dimension.?,
                value_query,
                limit,
            );
            return .{
                .matches = matches,
                .source = "facet_postings",
            };
        }
    }

    const matches = try searchCollectionFacetsRaw(
        allocator,
        db,
        workspace_id,
        collection_id,
        namespace,
        dimension,
        value_query,
        limit,
    );
    return .{
        .matches = matches,
        .source = "facet_assignments_raw",
    };
}

test "searchCollectionFacets decodes facet_postings Roaring bitmaps after reindex" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try db.exec(
        \\INSERT INTO workspaces (id, workspace_id, label) VALUES ('ws1', 'ws1', 'ws1');
        \\INSERT INTO collections (collection_id, workspace_id, name, key_kind, chunk_bits)
        \\  VALUES ('ws1::main', 'ws1', 'main', 'integer', 4);
        \\INSERT INTO ontologies (ontology_id, workspace_id, name)
        \\  VALUES ('ws1::core', 'ws1', 'core');
    );

    try facet_sqlite.setupFacetTable(db, 7, "public", "ws1::main", 4, &.{
        .{ .facet_id = 1, .facet_name = "topic.category" },
    });

    try db.exec(
        \\INSERT INTO facet_assignments_raw (
        \\  workspace_id, collection_id, target_kind, doc_id, chunk_index,
        \\  ontology_id, namespace, dimension, value, weight, source
        \\) VALUES (
        \\  'ws1', 'ws1::main', 'doc', 42, -1,
        \\  'ws1::core', 'topic', 'category', 'legal', 1.0, 'test'
        \\);
    );

    var search = search_store.Store.init(std.testing.allocator);
    defer search.deinit();
    var facets = facet_store.Store.init(std.testing.allocator);
    defer facets.deinit();
    var graph = graph_store.Store.init(std.testing.allocator);
    defer graph.deinit();

    var pipeline = import_pipeline.Pipeline{
        .allocator = std.testing.allocator,
        .db = &db,
        .search = &search,
        .facets = &facets,
        .graph = &graph,
        .workspace_id = "ws1",
        .collection_id = "ws1::main",
    };
    try facets.registerTable(.{
        .table_id = 7,
        .chunk_bits = 4,
        .schema_name = "public",
        .table_name = "ws1::main",
    });

    const indexed = try pipeline.reindexFacets("ws1", "ws1::main", 7);
    try std.testing.expectEqual(@as(u64, 1), indexed);
    try std.testing.expectEqual(@as(u64, 1), try facet_sqlite.countFacetPostingsForTable(db, 7));

    const result = try searchCollectionFacets(
        std.testing.allocator,
        db,
        "ws1",
        "ws1::main",
        7,
        "topic",
        "category",
        "leg",
        10,
    );
    defer {
        for (result.matches) |row| row.deinit(std.testing.allocator);
        std.testing.allocator.free(result.matches);
    }

    try std.testing.expectEqualStrings("facet_postings", result.source);
    try std.testing.expectEqual(@as(usize, 1), result.matches.len);
    try std.testing.expectEqual(@as(u64, 42), result.matches[0].doc_id);
    try std.testing.expectEqualStrings("legal", result.matches[0].value);
}

test "searchCollectionFacets falls back to facet_assignments_raw without postings" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try db.exec(
        \\INSERT INTO workspaces (id, workspace_id, label) VALUES ('ws1', 'ws1', 'ws1');
        \\INSERT INTO collections (collection_id, workspace_id, name, key_kind, chunk_bits)
        \\  VALUES ('ws1::main', 'ws1', 'main', 'integer', 4);
        \\INSERT INTO ontologies (ontology_id, workspace_id, name)
        \\  VALUES ('ws1::core', 'ws1', 'core');
        \\INSERT INTO facet_assignments_raw (
        \\  workspace_id, collection_id, target_kind, doc_id, chunk_index,
        \\  ontology_id, namespace, dimension, value, weight, source
        \\) VALUES (
        \\  'ws1', 'ws1::main', 'doc', 1, -1,
        \\  'ws1::core', 'topic', 'category', 'legal', 1.0, 'test'
        \\);
    );

    const result = try searchCollectionFacets(
        std.testing.allocator,
        db,
        "ws1",
        "ws1::main",
        null,
        null,
        null,
        "leg",
        10,
    );
    defer {
        for (result.matches) |row| row.deinit(std.testing.allocator);
        std.testing.allocator.free(result.matches);
    }

    try std.testing.expectEqualStrings("facet_assignments_raw", result.source);
    try std.testing.expectEqual(@as(usize, 1), result.matches.len);
    try std.testing.expectEqualStrings("legal", result.matches[0].value);
}

test "searchCollectionFacets rejects mismatched table id and collection" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try facet_sqlite.setupFacetTable(db, 7, "public", "ws1::main", 4, &.{
        .{ .facet_id = 1, .facet_name = "topic.category" },
    });

    try std.testing.expectError(
        error.TableNotFound,
        searchCollectionFacets(
            std.testing.allocator,
            db,
            "ws1",
            "ws1::other",
            7,
            "topic",
            "category",
            "leg",
            10,
        ),
    );
}

test "reindexAll registers facet table config and rebuilds derived indexes" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try db.exec(
        \\INSERT INTO workspaces (id, workspace_id, label) VALUES ('ws1', 'ws1', 'ws1');
        \\INSERT INTO collections (collection_id, workspace_id, name, key_kind, chunk_bits)
        \\  VALUES ('ws1::main', 'ws1', 'main', 'integer', 4);
        \\INSERT INTO ontologies (ontology_id, workspace_id, name)
        \\  VALUES ('ws1::core', 'ws1', 'core');
        \\INSERT INTO documents_raw (
        \\  workspace_id, collection_id, doc_id, content, language
        \\) VALUES (
        \\  'ws1', 'ws1::main', 42, 'graph legal document', 'english'
        \\);
        \\INSERT INTO entities_raw (
        \\  workspace_id, ontology_id, entity_id, entity_type, name, confidence
        \\) VALUES (
        \\  'ws1', 'ws1::core', 1, 'concept', 'Graph Law', 0.9
        \\);
        \\INSERT INTO entity_documents_raw (
        \\  workspace_id, entity_id, collection_id, doc_id, role, confidence
        \\) VALUES (
        \\  'ws1', 1, 'ws1::main', 42, 'mentions', 0.8
        \\);
        \\INSERT INTO facet_assignments_raw (
        \\  workspace_id, collection_id, target_kind, doc_id, chunk_index,
        \\  ontology_id, namespace, dimension, value, weight, source
        \\) VALUES (
        \\  'ws1', 'ws1::main', 'doc', 42, -1,
        \\  'ws1::core', 'topic', 'category', 'legal', 1.0, 'test'
        \\);
    );

    try facet_sqlite.setupFacetTable(db, 7, "public", "ws1::main", 4, &.{});
    try search_sqlite.setupSearchTable(db, std.testing.allocator, .{
        .table_id = 7,
        .workspace_id = "ws1",
        .schema_name = "public",
        .table_name = "ws1::main",
        .key_column = "doc_id",
        .content_column = "content",
        .metadata_column = "metadata",
        .language = "english",
        .populate = false,
    });
    try search_sqlite.bm25CreateSyncTrigger(db, .{
        .table_id = 7,
        .id_column = "doc_id",
        .content_column = "content",
        .language = "english",
    });

    const result = try reindexAll(std.testing.allocator, &db, "ws1", "ws1::main", 7);
    try std.testing.expectEqual(@as(u64, 1), result.bm25_documents);
    try std.testing.expectEqual(@as(u64, 1), result.facet_assignments);
    try std.testing.expectEqual(@as(u64, 2), result.graph_projected);
    try std.testing.expectEqual(@as(u64, 1), try facet_sqlite.countFacetPostingsForTable(db, 7));

    {
        const stmt = try facet_sqlite.prepare(db, "SELECT COUNT(*) FROM search_documents WHERE table_id = 7");
        defer facet_sqlite.finalize(stmt);
        try std.testing.expectEqual(facet_sqlite.c.SQLITE_ROW, facet_sqlite.c.sqlite3_step(stmt));
        try std.testing.expectEqual(@as(i64, 1), facet_sqlite.c.sqlite3_column_int64(stmt, 0));
    }
    {
        const stmt = try facet_sqlite.prepare(db, "SELECT COUNT(*) FROM graph_entity WHERE workspace_id = 'ws1'");
        defer facet_sqlite.finalize(stmt);
        try std.testing.expectEqual(facet_sqlite.c.SQLITE_ROW, facet_sqlite.c.sqlite3_step(stmt));
        try std.testing.expectEqual(@as(i64, 1), facet_sqlite.c.sqlite3_column_int64(stmt, 0));
    }
}
