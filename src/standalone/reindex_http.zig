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
const collection_facet_index = @import("collection_facet_index.zig");

const Allocator = std.mem.Allocator;
const Database = facet_sqlite.Database;

pub const ReindexGraphResult = struct {
    projected_count: u64,
    document_table_id: ?u64 = null,
    /// True when Roaring adjacency (graph_lj_out/in) was rebuilt as part of this
    /// reindex. The native pipeline always rebuilds it; only the GhostCrab SQL
    /// fallback path leaves it stale.
    adjacency_rebuilt: bool = true,
    /// Relations skipped because an endpoint was missing from the workspace.
    /// Non-zero means the raw layer holds dangling edges: the projection is
    /// complete for everything else, but those edges are absent from the graph.
    skipped_cross_workspace_relations: u64 = 0,
};

pub const ReindexAllResult = struct {
    graph_projected: u64,
    facet_assignments: u64,
    bm25_documents: u64,
    skipped_cross_workspace_relations: u64 = 0,
};

pub const CollectionFacetMatch = struct {
    doc_id: u64,
    chunk_index: ?u32,
    namespace: []const u8,
    dimension: []const u8,
    value: []const u8,
    weight: f32,
    target_kind: []const u8,
    ontology_id: []const u8,
    assignment_source: ?[]const u8,

    pub fn jsonStringify(self: CollectionFacetMatch, jw: anytype) !void {
        try jw.beginObject();
        inline for (@typeInfo(CollectionFacetMatch).@"struct".fields) |field| {
            try jw.objectField(field.name);
            if (comptime std.mem.eql(u8, field.name, "doc_id")) {
                if (self.doc_id > 9007199254740991) {
                    var buffer: [20]u8 = undefined;
                    try jw.write(std.fmt.bufPrint(&buffer, "{d}", .{self.doc_id}) catch unreachable);
                } else try jw.write(self.doc_id);
            } else try jw.write(@field(self, field.name));
        }
        try jw.endObject();
    }

    pub fn deinit(self: CollectionFacetMatch, allocator: Allocator) void {
        allocator.free(self.namespace);
        allocator.free(self.dimension);
        allocator.free(self.value);
        allocator.free(self.target_kind);
        allocator.free(self.ontology_id);
        if (self.assignment_source) |source| allocator.free(source);
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
        .skipped_cross_workspace_relations = bundle.pipeline.skipped_cross_workspace_relations,
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
        .skipped_cross_workspace_relations = bundle.pipeline.skipped_cross_workspace_relations,
    };
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

pub const CollectionFacetFilters = struct {
    target_kind: ?[]const u8 = null,
    doc_id: ?u64 = null,
    chunk_index: ?u32 = null,
    ontology_id: ?[]const u8 = null,
};

fn searchCollectionFacetsRaw(
    allocator: Allocator,
    db: Database,
    workspace_id: []const u8,
    collection_id: []const u8,
    namespace: ?[]const u8,
    dimension: ?[]const u8,
    value_query: ?[]const u8,
    limit: usize,
    bitmap: ?roaring.Bitmap,
    filters: CollectionFacetFilters,
) ![]CollectionFacetMatch {
    var sql = std.ArrayList(u8).empty;
    defer sql.deinit(allocator);
    try sql.appendSlice(allocator, "SELECT a.doc_id,a.chunk_index,a.namespace,a.dimension,a.value,a.weight,a.target_kind,a.ontology_id,a.source");
    if (bitmap != null) try sql.appendSlice(allocator, ", t.dense_id");
    try sql.appendSlice(allocator, " FROM facet_assignments_raw a");
    if (bitmap != null) try sql.appendSlice(allocator, " JOIN collection_facet_targets t ON t.workspace_id=a.workspace_id AND t.collection_id=a.collection_id AND t.target_kind=a.target_kind AND t.doc_id=a.doc_id AND t.chunk_index=a.chunk_index");
    try sql.appendSlice(allocator, " WHERE a.workspace_id=?1 AND a.collection_id=?2");

    var params = std.ArrayList([]const u8).empty;
    defer params.deinit(allocator);
    try params.append(allocator, workspace_id);
    try params.append(allocator, collection_id);

    var value_pattern: ?[]u8 = null;
    defer if (value_pattern) |pattern| allocator.free(pattern);

    if (namespace) |ns| {
        try appendFmt(allocator, &sql, " AND a.namespace = ?{d}", .{params.items.len + 1});
        try params.append(allocator, ns);
    }
    if (dimension) |dim| {
        try appendFmt(allocator, &sql, " AND a.dimension = ?{d}", .{params.items.len + 1});
        try params.append(allocator, dim);
    }
    if (value_query) |vq| {
        try appendFmt(allocator, &sql, " AND a.value LIKE ?{d}", .{params.items.len + 1});
        value_pattern = try std.fmt.allocPrint(allocator, "%{s}%", .{vq});
        try params.append(allocator, value_pattern.?);
    }
    var doc_buffer: [21]u8 = undefined;
    var chunk_buffer: [10]u8 = undefined;
    const doc_text: ?[]const u8 = if (filters.doc_id) |id| try std.fmt.bufPrint(&doc_buffer, "{d}", .{@as(i64, @bitCast(id))}) else null;
    const chunk_text: ?[]const u8 = if (filters.chunk_index) |index| try std.fmt.bufPrint(&chunk_buffer, "{d}", .{index}) else null;
    const columns = [_][]const u8{ "target_kind", "doc_id", "chunk_index", "ontology_id" };
    for ([_]?[]const u8{ filters.target_kind, doc_text, chunk_text, filters.ontology_id }, columns) |param, column| {
        if (param) |value| {
            try appendFmt(allocator, &sql, " AND a.{s}=?{d}", .{ column, params.items.len + 1 });
            try params.append(allocator, value);
        }
    }
    if (filters.chunk_index != null) try sql.appendSlice(allocator, " AND a.target_kind='chunk'");
    try sql.appendSlice(allocator, " ORDER BY a.weight DESC,a.doc_id,a.chunk_index,a.ontology_id,a.namespace,a.dimension,a.value");
    if (bitmap == null) try appendFmt(allocator, &sql, " LIMIT {d}", .{limit});

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
    while (rows.items.len < limit) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        if (bitmap) |allowed| {
            if (!allowed.contains(try facet_sqlite.columnU32(stmt, 9))) continue;
        }
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
            .target_kind = try facet_sqlite.dupeColumnText(allocator, stmt, 6),
            .ontology_id = try facet_sqlite.dupeColumnText(allocator, stmt, 7),
            .assignment_source = if (c.sqlite3_column_type(stmt, 8) == c.SQLITE_NULL) null else try facet_sqlite.dupeColumnText(allocator, stmt, 8),
        });
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
    return searchCollectionFacetsFiltered(allocator, db, workspace_id, collection_id, table_id_param, namespace, dimension, value_query, limit, .{});
}

pub fn searchCollectionFacetsFiltered(
    allocator: Allocator,
    db: Database,
    workspace_id: []const u8,
    collection_id: []const u8,
    table_id_param: ?u64,
    namespace: ?[]const u8,
    dimension: ?[]const u8,
    value_query: ?[]const u8,
    limit: usize,
    filters: CollectionFacetFilters,
) !SearchCollectionFacetsResult {
    if (filters.target_kind) |kind| {
        if (!std.mem.eql(u8, kind, "doc") and !std.mem.eql(u8, kind, "chunk")) return error.BadRequest;
        if (std.mem.eql(u8, kind, "doc") and filters.chunk_index != null) return error.BadRequest;
    }
    // State check, bitmap read and identity decoding share one SQLite snapshot.
    try db.exec("SAVEPOINT collection_facet_read");
    errdefer {
        db.exec("ROLLBACK TO collection_facet_read") catch {};
        db.exec("RELEASE collection_facet_read") catch {};
    }
    if (try tryResolveFacetTable(db, allocator, collection_id, table_id_param)) |config| {
        allocator.free(config.schema_name);
        allocator.free(config.table_name);
    }
    var bitmap = try collection_facet_index.matching(allocator, db, workspace_id, collection_id, namespace, dimension, value_query);
    defer if (bitmap) |*value| value.deinit();
    const matches = try searchCollectionFacetsRaw(allocator, db, workspace_id, collection_id, namespace, dimension, value_query, limit, bitmap, filters);
    errdefer {
        for (matches) |row| row.deinit(allocator);
        allocator.free(matches);
    }
    try db.exec("RELEASE collection_facet_read");
    return .{ .matches = matches, .source = if (bitmap != null) "facet_postings" else "facet_assignments_raw" };
}

test "searchCollectionFacets preserves chunk identities in Roaring bitmaps after reindex" {
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
        \\INSERT INTO facet_assignments_raw (workspace_id, collection_id, target_kind, doc_id, chunk_index, ontology_id, namespace, dimension, value, weight, source)
        \\VALUES ('ws1', 'ws1::main', 'chunk', 42, 0, 'ws1::core', 'topic', 'category', 'legal', 0.75, 'chunk-test');
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
    try std.testing.expectEqual(@as(u64, 2), indexed);
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
    try std.testing.expectEqual(@as(usize, 2), result.matches.len);
    try std.testing.expectEqual(@as(u64, 42), result.matches[0].doc_id);
    try std.testing.expectEqualStrings("legal", result.matches[0].value);
    try std.testing.expectEqual(@as(?u32, 0), result.matches[1].chunk_index);
    try std.testing.expectEqual(@as(f32, 0.75), result.matches[1].weight);
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

fn facetTestDatabase() !Database {
    var db = try Database.openInMemory();
    errdefer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id,workspace_id,label) VALUES ('ws','ws','ws'),('neighbor','neighbor','neighbor');
        \\INSERT INTO collections(collection_id,workspace_id,name,key_kind,chunk_bits)
        \\VALUES ('ws::docs','ws','docs','integer',4),('neighbor::docs','neighbor','docs','integer',4);
        \\INSERT INTO ontologies(ontology_id,workspace_id,name) VALUES ('a','ws','a'),('b','ws','b'),('neighbor','neighbor','n');
        \\INSERT INTO facet_assignments_raw(workspace_id,collection_id,target_kind,doc_id,chunk_index,ontology_id,namespace,dimension,value,weight,source) VALUES
        \\('ws','ws::docs','chunk',9007199254740993,0,'a','topic','category','shared',0.4,'first'),
        \\('ws','ws::docs','chunk',9007199254740993,0,'b','topic','category','shared',0.8,'second'),
        \\('ws','ws::docs','chunk',9007199254740993,1,'a','topic','category','different',0.6,NULL),
        \\('ws','ws::docs','doc',-1,-1,'a','topic','category','unsigned',0.2,'max-u64'),
        \\('neighbor','neighbor::docs','chunk',9007199254740993,0,'neighbor','topic','category','shared',1,'neighbor');
    );
    return db;
}

fn rebuildTestFacets(db: Database) !void {
    var tx = try facet_sqlite.Transaction.begin(db);
    defer tx.deinit();
    _ = try collection_facet_index.rebuild(std.testing.allocator, db, "ws", "ws::docs");
    try tx.commit();
}

fn freeFacetTestResult(result: SearchCollectionFacetsResult) void {
    for (result.matches) |row| row.deinit(std.testing.allocator);
    std.testing.allocator.free(result.matches);
}

fn findTestFacets(db: Database, value: ?[]const u8, limit: usize) !SearchCollectionFacetsResult {
    return searchCollectionFacets(std.testing.allocator, db, "ws", "ws::docs", null, "topic", "category", value, limit);
}

test "collection facet index preserves taxonomy weights provenance and full width IDs through vacuum" {
    var db = try facetTestDatabase();
    defer db.close();
    try rebuildTestFacets(db);
    // Rowids may change during VACUUM. The projection uses explicit target keys.
    try db.exec("VACUUM");
    const result = try findTestFacets(db, null, 10);
    defer freeFacetTestResult(result);
    try std.testing.expectEqualStrings("facet_postings", result.source);
    try std.testing.expectEqual(@as(usize, 4), result.matches.len);
    try std.testing.expectEqual(@as(u64, 9007199254740993), result.matches[0].doc_id);
    try std.testing.expectEqual(@as(?u32, 0), result.matches[0].chunk_index);
    try std.testing.expectEqualStrings("b", result.matches[0].ontology_id);
    try std.testing.expectEqualStrings("second", result.matches[0].assignment_source.?);
    try std.testing.expectEqualStrings("chunk", result.matches[0].target_kind);
    try std.testing.expectEqual(@as(?u32, 1), result.matches[1].chunk_index);
    try std.testing.expectEqualStrings("a", result.matches[2].ontology_id);
    try std.testing.expectEqual(@as(u64, std.math.maxInt(u64)), result.matches[3].doc_id);
    try std.testing.expectEqual(@as(?u32, null), result.matches[3].chunk_index);
    const json = try std.json.Stringify.valueAlloc(std.testing.allocator, result.matches, .{});
    defer std.testing.allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"doc_id\":\"9007199254740993\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"doc_id\":\"18446744073709551615\"") != null);
    const limited = try findTestFacets(db, "shared", 1);
    defer freeFacetTestResult(limited);
    try std.testing.expectEqual(@as(usize, 1), limited.matches.len);
    try std.testing.expectEqualStrings("b", limited.matches[0].ontology_id);
}

test "collection facet index invalidates late insert update and delete and rebuilds empty scopes" {
    var db = try facetTestDatabase();
    defer db.close();
    try rebuildTestFacets(db);
    // A neighboring workspace must not invalidate this collection.
    try db.exec("UPDATE facet_assignments_raw SET weight=0.5 WHERE workspace_id='neighbor'");
    const unaffected = try findTestFacets(db, "shared", 10);
    defer freeFacetTestResult(unaffected);
    try std.testing.expectEqualStrings("facet_postings", unaffected.source);
    try db.exec(
        \\INSERT INTO facet_assignments_raw(workspace_id,collection_id,target_kind,doc_id,chunk_index,ontology_id,namespace,dimension,value,weight)
        \\VALUES ('ws','ws::docs','chunk',7,0,'a','topic','category','late',0.9);
    );
    const late = try findTestFacets(db, "late", 10);
    defer freeFacetTestResult(late);
    try std.testing.expectEqualStrings("facet_assignments_raw", late.source);
    try std.testing.expectEqual(@as(usize, 1), late.matches.len);
    try rebuildTestFacets(db);
    try db.exec("UPDATE facet_assignments_raw SET value='edited',weight=0.3 WHERE workspace_id='ws' AND value='late'");
    const edited = try findTestFacets(db, "edited", 10);
    defer freeFacetTestResult(edited);
    try std.testing.expectEqualStrings("facet_assignments_raw", edited.source);
    try std.testing.expectEqual(@as(f32, 0.3), edited.matches[0].weight);
    try rebuildTestFacets(db);
    try db.exec("DELETE FROM facet_assignments_raw WHERE workspace_id='ws'");
    const deleted = try findTestFacets(db, null, 10);
    defer freeFacetTestResult(deleted);
    try std.testing.expectEqualStrings("facet_assignments_raw", deleted.source);
    try std.testing.expectEqual(@as(usize, 0), deleted.matches.len);
    try rebuildTestFacets(db);
    const empty = try findTestFacets(db, null, 10);
    defer freeFacetTestResult(empty);
    try std.testing.expectEqualStrings("facet_postings", empty.source);
    try std.testing.expectEqual(@as(usize, 0), empty.matches.len);
}

test "collection facet bitmap is decoded and failed rebuild publication rolls back" {
    var db = try facetTestDatabase();
    defer db.close();
    try rebuildTestFacets(db);
    {
        var tx = try facet_sqlite.Transaction.begin(db);
        defer tx.deinit(); // Simulate interruption before commit.
        try db.exec("DELETE FROM facet_assignments_raw WHERE workspace_id='ws'");
        try std.testing.expectEqual(@as(u64, 0), try collection_facet_index.rebuild(std.testing.allocator, db, "ws", "ws::docs"));
    }
    const restored = try findTestFacets(db, null, 10);
    defer freeFacetTestResult(restored);
    try std.testing.expectEqualStrings("facet_postings", restored.source);
    try std.testing.expectEqual(@as(usize, 4), restored.matches.len);
    // Prove that this path consumes actual bitmap bytes, not just the raw rows.
    try db.exec("UPDATE collection_facet_postings SET posting_blob=x'00' WHERE workspace_id='ws'");
    try std.testing.expectError(error.DeserializeFailed, findTestFacets(db, null, 10));
    // The failed read must release its savepoint and leave the connection usable.
    try rebuildTestFacets(db);
    const recovered = try findTestFacets(db, "shared", 10);
    defer freeFacetTestResult(recovered);
    try std.testing.expectEqual(@as(usize, 2), recovered.matches.len);
}

test "collection facet exact filters run before ranking and limits" {
    var db = try facetTestDatabase();
    defer db.close();
    try rebuildTestFacets(db);
    const exact = try searchCollectionFacetsFiltered(std.testing.allocator, db, "ws", "ws::docs", null, "topic", "category", "shared", 1, .{
        .target_kind = "chunk",
        .doc_id = 9007199254740993,
        .chunk_index = 0,
        .ontology_id = "a",
    });
    defer freeFacetTestResult(exact);
    try std.testing.expectEqualStrings("facet_postings", exact.source);
    try std.testing.expectEqual(@as(usize, 1), exact.matches.len);
    try std.testing.expectEqualStrings("a", exact.matches[0].ontology_id);
    try std.testing.expectEqual(@as(f32, 0.4), exact.matches[0].weight);
    try std.testing.expectError(error.BadRequest, searchCollectionFacetsFiltered(std.testing.allocator, db, "ws", "ws::docs", null, null, null, null, 10, .{ .target_kind = "doc", .chunk_index = 0 }));
    try std.testing.expectError(error.BadRequest, searchCollectionFacetsFiltered(std.testing.allocator, db, "ws", "ws::docs", null, null, null, null, 10, .{ .target_kind = "invalid" }));
}
