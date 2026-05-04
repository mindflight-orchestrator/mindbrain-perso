const std = @import("std");
const collections_sqlite = @import("collections_sqlite.zig");
const facet_sqlite = @import("facet_sqlite.zig");
const facet_store = @import("facet_store.zig");
const graph_sqlite = @import("graph_sqlite.zig");
const interfaces = @import("interfaces.zig");
const ontology_sqlite = @import("ontology_sqlite.zig");
const search_store = @import("search_store.zig");
const vector_blob = @import("vector_blob.zig");
const vector_sqlite_exact = @import("vector_sqlite_exact.zig");
const roaring = @import("roaring.zig");

pub const Database = facet_sqlite.Database;
pub const c = facet_sqlite.c;

pub const GraphQuery = struct {
    start_nodes: []const roaring.EntityId = &.{},
    max_hops: usize = 1,
    filter: interfaces.GraphEdgeFilter = .{},
};

pub const ProjectionQuery = struct {
    agent_id: []const u8,
    query: []const u8 = "",
    limit: usize = 10,
};

pub const Query = struct {
    workspace_id: []const u8 = "default",
    table_name: ?[]const u8 = null,
    facet_filters: []const interfaces.FacetValueFilter = &.{},
    count_facets: []const []const u8 = &.{},
    graph: ?GraphQuery = null,
    projection: ?ProjectionQuery = null,
};

pub const FacetCountGroup = struct {
    facet_name: []const u8,
    counts: []interfaces.FacetCount,
};

pub const Result = struct {
    workspace_id: []const u8,
    documents: ?roaring.Bitmap = null,
    reachable_nodes: ?roaring.Bitmap = null,
    facet_counts: []FacetCountGroup = &.{},
    projections: []ontology_sqlite.ProjectionRecord = &.{},
    source_refs: [][]const u8 = &.{},

    pub fn deinit(self: *Result, allocator: std.mem.Allocator) void {
        allocator.free(self.workspace_id);
        if (self.documents) |*bitmap| bitmap.deinit();
        if (self.reachable_nodes) |*bitmap| bitmap.deinit();

        for (self.facet_counts) |group| {
            allocator.free(group.facet_name);
            for (group.counts) |count| allocator.free(count.facet_value);
            allocator.free(group.counts);
        }
        allocator.free(self.facet_counts);

        ontology_sqlite.deinitProjectionRows(allocator, self.projections);

        for (self.source_refs) |source_ref| allocator.free(source_ref);
        allocator.free(self.source_refs);

        self.* = undefined;
    }
};

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    db: *const Database,
    graph_runtime: graph_sqlite.InMemoryRuntime,

    pub fn init(db: *const Database, allocator: std.mem.Allocator) !Runtime {
        return .{
            .allocator = allocator,
            .db = db,
            .graph_runtime = try graph_sqlite.loadRuntime(db.*, allocator),
        };
    }

    pub fn deinit(self: *Runtime) void {
        self.graph_runtime.deinit();
        self.* = undefined;
    }

    pub fn vectorRepositoryForCollection(
        self: *const Runtime,
        workspace_id: []const u8,
        collection_id: []const u8,
        scope: vector_sqlite_exact.Scope,
    ) vector_sqlite_exact.Repository {
        return .{
            .db = self.db,
            .workspace_id = workspace_id,
            .collection_id = collection_id,
            .scope = scope,
        };
    }

    pub fn execute(self: *const Runtime, allocator: std.mem.Allocator, query: Query) !Result {
        var arena_state = std.heap.ArenaAllocator.init(self.allocator);
        defer arena_state.deinit();
        const scratch = arena_state.allocator();

        var result = Result{
            .workspace_id = try allocator.dupe(u8, query.workspace_id),
            .facet_counts = try allocator.alloc(FacetCountGroup, query.count_facets.len),
            .projections = try allocator.alloc(ontology_sqlite.ProjectionRecord, 0),
            .source_refs = try allocator.alloc([]const u8, 0),
        };
        errdefer result.deinit(allocator);

        for (query.count_facets, 0..) |facet_name, index| {
            result.facet_counts[index] = .{
                .facet_name = try allocator.dupe(u8, facet_name),
                .counts = try allocator.alloc(interfaces.FacetCount, 0),
            };
        }

        if (query.table_name) |table_name| {
            const repository = facet_sqlite.Repository{ .db = self.db };
            if (query.facet_filters.len > 0) {
                result.documents = try facet_store.filterDocuments(
                    scratch,
                    repository.asFacetRepository(),
                    table_name,
                    query.facet_filters,
                );
            }

            for (query.count_facets, 0..) |facet_name, index| {
                allocator.free(result.facet_counts[index].counts);
                result.facet_counts[index].counts = try facet_store.countFacetValues(
                    allocator,
                    repository.asFacetRepository(),
                    table_name,
                    facet_name,
                    if (result.documents) |bitmap| bitmap else null,
                );
            }

            if (result.documents) |bitmap| {
                result.source_refs = try loadSourceRefsForDocs(self.db.*, allocator, query.workspace_id, bitmap);
            }
        }

        if (query.graph) |graph_query| {
            if (graph_query.start_nodes.len > 0 and graph_query.max_hops > 0) {
                result.reachable_nodes = try self.graph_runtime.kHops(
                    scratch,
                    graph_query.start_nodes,
                    graph_query.max_hops,
                    graph_query.filter,
                );
            }
        }

        if (query.projection) |projection_query| {
            result.projections = try ontology_sqlite.materializeProjections(
                self.db.*,
                allocator,
                projection_query.agent_id,
                query.workspace_id,
                projection_query.query,
                if (result.source_refs.len > 0) result.source_refs else null,
                projection_query.limit,
            );
        }

        return result;
    }
};

pub fn countFacetValuesWithVectorToon(
    allocator: std.mem.Allocator,
    facet_repository: interfaces.FacetRepository,
    vector_repository: interfaces.VectorRepository,
    table_name: []const u8,
    facet_name: []const u8,
    vector_request: interfaces.VectorSearchRequest,
    filter_bitmap: ?roaring.Bitmap,
) ![]u8 {
    const nearest = try vector_repository.searchNearestFn(vector_repository.ctx, allocator, vector_request);
    defer allocator.free(nearest);

    var vector_bitmap = try roaring.Bitmap.empty();
    defer vector_bitmap.deinit();
    for (nearest) |match| {
        vector_bitmap.add(@intCast(match.doc_id));
    }

    if (filter_bitmap) |bitmap| {
        var intersected = try vector_bitmap.andNew(bitmap);
        defer intersected.deinit();
        return try facet_store.countFacetValuesToon(
            allocator,
            facet_repository,
            table_name,
            facet_name,
            intersected,
        );
    }

    return try facet_store.countFacetValuesToon(
        allocator,
        facet_repository,
        table_name,
        facet_name,
        vector_bitmap,
    );
}

fn loadSourceRefsForDocs(
    db: Database,
    allocator: std.mem.Allocator,
    workspace_id: []const u8,
    documents: roaring.Bitmap,
) ![][]const u8 {
    const doc_ids = try documents.toArray(allocator);
    defer allocator.free(doc_ids);

    var dedupe = std.StringHashMap(void).init(allocator);
    defer {
        var it = dedupe.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        dedupe.deinit();
    }

    const stmt = try prepare(db, "SELECT COALESCE(source_ref, id) FROM facets WHERE workspace_id = ?1 AND doc_id = ?2");
    defer finalize(stmt);

    for (doc_ids) |doc_id| {
        try resetStatement(stmt);
        try bindText(stmt, 1, workspace_id);
        try bindInt64(stmt, 2, doc_id);
        while (true) {
            const rc = c.sqlite3_step(stmt);
            if (rc == c.SQLITE_DONE) break;
            if (rc != c.SQLITE_ROW) return error.StepFailed;

            const source_ref = try dupeColumnText(allocator, stmt, 0);
            errdefer allocator.free(source_ref);
            const gop = try dedupe.getOrPut(source_ref);
            if (gop.found_existing) {
                allocator.free(source_ref);
            } else {
                gop.value_ptr.* = {};
            }
        }
    }

    var source_refs = std.ArrayList([]const u8).empty;
    defer source_refs.deinit(allocator);

    var it = dedupe.iterator();
    while (it.next()) |entry| {
        try source_refs.append(allocator, try allocator.dupe(u8, entry.key_ptr.*));
    }

    std.mem.sort([]const u8, source_refs.items, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.order(u8, lhs, rhs) == .lt;
        }
    }.lessThan);

    return source_refs.toOwnedSlice(allocator);
}

fn prepare(db: Database, sql: []const u8) !*c.sqlite3_stmt {
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
        return error.PrepareFailed;
    }
    return stmt.?;
}

fn finalize(stmt: *c.sqlite3_stmt) void {
    _ = c.sqlite3_finalize(stmt);
}

fn resetStatement(stmt: *c.sqlite3_stmt) !void {
    if (c.sqlite3_reset(stmt) != c.SQLITE_OK) return error.StepFailed;
    if (c.sqlite3_clear_bindings(stmt) != c.SQLITE_OK) return error.BindFailed;
}

fn bindInt64(stmt: *c.sqlite3_stmt, index: c_int, value: anytype) !void {
    if (c.sqlite3_bind_int64(stmt, index, @intCast(value)) != c.SQLITE_OK) return error.BindFailed;
}

fn bindText(stmt: *c.sqlite3_stmt, index: c_int, value: []const u8) !void {
    if (c.sqlite3_bind_text(stmt, index, value.ptr, @intCast(value.len), facet_sqlite.sqliteTransient()) != c.SQLITE_OK) {
        return error.BindFailed;
    }
}

test "query executor can combine vector nearest neighbors with facet counts" {
    var facet_store_runtime = facet_store.Store.init(std.testing.allocator);
    defer facet_store_runtime.deinit();
    try facet_store_runtime.registerTable(.{
        .table_id = 1,
        .chunk_bits = 4,
        .schema_name = "public",
        .table_name = "docs_fixture",
    });
    try facet_store_runtime.registerFacet(1, 1, "category");
    try facet_store_runtime.addPosting(1, 1, "Technology", 0, &.{ 7, 9 });
    try facet_store_runtime.addPosting(1, 1, "Cooking", 0, &.{11});

    var search_runtime = search_store.Store.init(std.testing.allocator);
    defer search_runtime.deinit();
    try search_runtime.upsertEmbedding(.{ .table_id = 1, .doc_id = 7, .values = &.{ 1.0, 0.0 } });
    try search_runtime.upsertEmbedding(.{ .table_id = 1, .doc_id = 9, .values = &.{ 0.9, 0.1 } });
    try search_runtime.upsertEmbedding(.{ .table_id = 1, .doc_id = 11, .values = &.{ 0.0, 1.0 } });

    const toon = try countFacetValuesWithVectorToon(
        std.testing.allocator,
        facet_store_runtime.asRepository(),
        search_runtime.vectorRepository(),
        "docs_fixture",
        "category",
        .{
            .table_name = "docs_fixture",
            .key_column = "doc_id",
            .vector_column = "embedding",
            .query_vector = &.{ 0.95, 0.05 },
            .limit = 2,
        },
        null,
    );
    defer std.testing.allocator.free(toon);

    try std.testing.expect(std.mem.indexOf(u8, toon, "kind: facet_counts") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "table_name: docs_fixture") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "facet_name: category") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "Technology") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "Cooking") == null);
}

test "query executor can create sqlite exact vector repository for collection vectors" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws_qv" });
    try collections_sqlite.ensureCollection(db, .{ .workspace_id = "ws_qv", .collection_id = "ws_qv::docs", .name = "docs" });
    try collections_sqlite.upsertDocumentRaw(db, .{ .workspace_id = "ws_qv", .collection_id = "ws_qv::docs", .doc_id = 5 });

    const bytes = try vector_blob.encodeF32Le(std.testing.allocator, &.{ 0.0, 1.0 });
    defer std.testing.allocator.free(bytes);
    try collections_sqlite.upsertDocumentVector(db, .{
        .workspace_id = "ws_qv",
        .collection_id = "ws_qv::docs",
        .doc_id = 5,
        .dim = 2,
        .embedding_blob = bytes,
    });

    var runtime = try Runtime.init(&db, std.testing.allocator);
    defer runtime.deinit();

    const exact = runtime.vectorRepositoryForCollection("ws_qv", "ws_qv::docs", .documents);
    const vector_repo = exact.asVectorRepository();
    const matches = try vector_repo.searchNearestFn(vector_repo.ctx, std.testing.allocator, .{
        .table_name = "documents_raw_vector",
        .key_column = "doc_id",
        .vector_column = "embedding_blob",
        .query_vector = &.{ 0.0, 1.0 },
        .limit = 1,
    });
    defer std.testing.allocator.free(matches);

    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqual(@as(interfaces.DocId, 5), matches[0].doc_id);
}

fn dupeColumnText(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, index: c_int) ![]const u8 {
    const len = c.sqlite3_column_bytes(stmt, index);
    const ptr = c.sqlite3_column_text(stmt, index) orelse return error.MissingRow;
    const bytes: []const u8 = @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
    return allocator.dupe(u8, bytes);
}

test "query executor composes facet filters, graph traversal, and projection materialization" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try facet_sqlite.insertFacetTable(db, 1, "public", "docs_fixture", 4);
    try facet_sqlite.insertFacetDefinition(db, 1, 1, "category");
    try facet_sqlite.insertFacetDefinition(db, 1, 2, "region");

    var category_search = try roaring.Bitmap.fromSlice(&.{ 1, 3 });
    defer category_search.deinit();
    try facet_sqlite.insertPostingBitmap(db, std.testing.allocator, 1, 1, "search", 0, category_search);

    var region_eu = try roaring.Bitmap.fromSlice(&.{ 1, 2, 3 });
    defer region_eu.deinit();
    try facet_sqlite.insertPostingBitmap(db, std.testing.allocator, 1, 2, "eu", 0, region_eu);

    try ontology_sqlite.upsertFacet(db, .{
        .id = "row-1",
        .schema_id = "ghostcrab:taxonomy",
        .content = "Ada doc",
        .facets_json = "{\"category\":\"search\"}",
        .workspace_id = "default",
        .doc_id = 1,
        .source_ref = "facet-doc-1",
    });
    try ontology_sqlite.upsertFacet(db, .{
        .id = "row-3",
        .schema_id = "ghostcrab:taxonomy",
        .content = "Graph doc",
        .facets_json = "{\"category\":\"search\"}",
        .workspace_id = "default",
        .doc_id = 3,
        .source_ref = "facet-doc-3",
    });

    try graph_sqlite.insertEntity(db, 1, "person", "Ada");
    try graph_sqlite.insertEntity(db, 2, "company", "Acme");
    try graph_sqlite.insertEntity(db, 3, "company", "Labs");
    try graph_sqlite.insertRelation(db, .{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "works_for", .confidence = 0.9 });
    try graph_sqlite.insertRelation(db, .{ .relation_id = 11, .source_id = 2, .target_id = 3, .relation_type = "works_for", .confidence = 0.8 });
    try graph_sqlite.insertAdjacency(db, "graph_lj_out", 1, &.{10}, std.testing.allocator);
    try graph_sqlite.insertAdjacency(db, "graph_lj_out", 2, &.{11}, std.testing.allocator);
    try graph_sqlite.insertAdjacency(db, "graph_lj_in", 2, &.{10}, std.testing.allocator);
    try graph_sqlite.insertAdjacency(db, "graph_lj_in", 3, &.{11}, std.testing.allocator);

    try ontology_sqlite.insertProjection(db, .{
        .id = "proj-1",
        .agent_id = "agent-q",
        .scope = "default",
        .proj_type = "FACT",
        .content = "Ada works for Acme",
        .weight = 1.0,
        .source_ref = "facet-doc-1",
        .source_type = "taxonomy",
        .status = "active",
    });
    try ontology_sqlite.insertProjection(db, .{
        .id = "proj-2",
        .agent_id = "agent-q",
        .scope = "default",
        .proj_type = "FACT",
        .content = "Other context",
        .weight = 0.5,
        .source_ref = "facet-doc-999",
        .source_type = "taxonomy",
        .status = "active",
    });

    var runtime = try Runtime.init(&db, std.testing.allocator);
    defer runtime.deinit();

    var result = try runtime.execute(std.testing.allocator, .{
        .workspace_id = "default",
        .table_name = "docs_fixture",
        .facet_filters = &.{
            .{ .facet_name = "category", .values = &.{"search"} },
            .{ .facet_name = "region", .values = &.{"eu"} },
        },
        .count_facets = &.{"category"},
        .graph = .{
            .start_nodes = &.{1},
            .max_hops = 2,
            .filter = .{ .edge_types = &.{"works_for"} },
        },
        .projection = .{
            .agent_id = "agent-q",
            .query = "Ada",
            .limit = 5,
        },
    });
    defer result.deinit(std.testing.allocator);

    try std.testing.expect(result.documents != null);
    const docs = try result.documents.?.toArray(std.testing.allocator);
    defer std.testing.allocator.free(docs);
    try std.testing.expectEqualSlices(u32, &.{ 1, 3 }, docs);

    try std.testing.expect(result.reachable_nodes != null);
    const nodes = try result.reachable_nodes.?.toArray(std.testing.allocator);
    defer std.testing.allocator.free(nodes);
    try std.testing.expectEqualSlices(u32, &.{ 1, 2, 3 }, nodes);

    try std.testing.expectEqual(@as(usize, 1), result.facet_counts.len);
    try std.testing.expectEqualStrings("category", result.facet_counts[0].facet_name);
    try std.testing.expectEqual(@as(usize, 1), result.facet_counts[0].counts.len);
    try std.testing.expectEqualStrings("search", result.facet_counts[0].counts[0].facet_value);
    try std.testing.expectEqual(@as(u64, 2), result.facet_counts[0].counts[0].cardinality);

    try std.testing.expectEqual(@as(usize, 2), result.source_refs.len);
    try std.testing.expectEqualStrings("facet-doc-1", result.source_refs[0]);
    try std.testing.expectEqualStrings("facet-doc-3", result.source_refs[1]);

    try std.testing.expectEqual(@as(usize, 1), result.projections.len);
    try std.testing.expectEqualStrings("proj-1", result.projections[0].id);
}

test "query executor supports facet count requests without a table selection" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    var runtime = try Runtime.init(&db, std.testing.allocator);
    defer runtime.deinit();

    var result = try runtime.execute(std.testing.allocator, .{
        .workspace_id = "default",
        .count_facets = &.{"category"},
    });
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), result.facet_counts.len);
    try std.testing.expectEqualStrings("category", result.facet_counts[0].facet_name);
    try std.testing.expectEqual(@as(usize, 0), result.facet_counts[0].counts.len);
    try std.testing.expect(result.documents == null);
}
