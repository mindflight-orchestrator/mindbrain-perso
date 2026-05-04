const std = @import("std");
const collections_sqlite = @import("collections_sqlite.zig");
const facet_sqlite = @import("facet_sqlite.zig");
const hybrid_search = @import("hybrid_search.zig");
const interfaces = @import("interfaces.zig");
const search_store = @import("search_store.zig");
const vector_blob = @import("vector_blob.zig");
const vector_distance = @import("vector_distance.zig");
const tokenization_sqlite = @import("tokenization_sqlite.zig");

const c = facet_sqlite.c;
pub const Database = facet_sqlite.Database;

pub const BackendKind = enum {
    exact_sqlite,
    sqlite_vec,
    vectorlite,
    usearch,
};

pub const Scope = enum {
    documents,
    chunks,
};

pub const ChunkVectorSearchRequest = struct {
    query_vector: []const f32,
    limit: usize,
    metric: interfaces.VectorDistanceMetric = .cosine,
};

pub const ChunkVectorSearchMatch = struct {
    doc_id: interfaces.DocId,
    chunk_index: u32,
    distance: f64,
    similarity: f64,
};

pub const Repository = struct {
    db: *const Database,
    workspace_id: []const u8,
    collection_id: []const u8,
    scope: Scope = .documents,

    pub fn asVectorRepository(self: *const Repository) interfaces.VectorRepository {
        return .{
            .ctx = @ptrCast(@constCast(self)),
            .getEmbeddingDimensionsFn = getEmbeddingDimensionsViaInterface,
            .searchNearestFn = searchNearestViaInterface,
            .upsertEmbeddingFn = upsertEmbeddingViaInterface,
            .deleteEmbeddingFn = deleteEmbeddingViaInterface,
        };
    }

    pub fn getEmbeddingDimensions(self: *const Repository) !?usize {
        const sql = switch (self.scope) {
            .documents =>
            \\SELECT dim FROM documents_raw_vector
            \\WHERE workspace_id = ?1 AND collection_id = ?2
            \\ORDER BY doc_id
            \\LIMIT 1
            ,
            .chunks =>
            \\SELECT dim FROM chunks_raw_vector
            \\WHERE workspace_id = ?1 AND collection_id = ?2
            \\ORDER BY doc_id, chunk_index
            \\LIMIT 1
            ,
        };
        const stmt = try facet_sqlite.prepare(self.db.*, sql);
        defer facet_sqlite.finalize(stmt);
        try facet_sqlite.bindText(stmt, 1, self.workspace_id);
        try facet_sqlite.bindText(stmt, 2, self.collection_id);

        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) return null;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        return @intCast(try facet_sqlite.columnU64(stmt, 0));
    }

    pub fn searchNearest(self: *const Repository, allocator: std.mem.Allocator, request: interfaces.VectorSearchRequest) ![]interfaces.VectorSearchMatch {
        if (request.query_vector.len == 0 or request.limit == 0) {
            return allocator.alloc(interfaces.VectorSearchMatch, 0);
        }

        return switch (self.scope) {
            .documents => self.searchDocumentVectors(allocator, request),
            .chunks => self.searchChunkVectorsAsDocuments(allocator, request),
        };
    }

    pub fn searchNearestChunks(self: *const Repository, allocator: std.mem.Allocator, request: ChunkVectorSearchRequest) ![]ChunkVectorSearchMatch {
        if (request.query_vector.len == 0 or request.limit == 0) {
            return allocator.alloc(ChunkVectorSearchMatch, 0);
        }

        const sql =
            \\SELECT doc_id, chunk_index, dim, embedding_blob
            \\FROM chunks_raw_vector
            \\WHERE workspace_id = ?1 AND collection_id = ?2 AND dim = ?3
            \\ORDER BY doc_id, chunk_index
        ;
        const stmt = try facet_sqlite.prepare(self.db.*, sql);
        defer facet_sqlite.finalize(stmt);
        try facet_sqlite.bindText(stmt, 1, self.workspace_id);
        try facet_sqlite.bindText(stmt, 2, self.collection_id);
        try facet_sqlite.bindInt64(stmt, 3, request.query_vector.len);

        var matches = std.ArrayList(ChunkVectorSearchMatch).empty;
        defer matches.deinit(allocator);

        while (true) {
            const rc = c.sqlite3_step(stmt);
            if (rc == c.SQLITE_DONE) break;
            if (rc != c.SQLITE_ROW) return error.StepFailed;

            const doc_id = try facet_sqlite.columnU64(stmt, 0);
            const chunk_index = try facet_sqlite.columnU32(stmt, 1);
            const dim: usize = @intCast(try facet_sqlite.columnU64(stmt, 2));
            const values = try decodeColumnEmbedding(allocator, stmt, 3, dim);
            defer allocator.free(values);

            const vector_score = vector_distance.score(request.metric, request.query_vector, values);
            try insertTopChunkMatch(allocator, &matches, .{
                .doc_id = doc_id,
                .chunk_index = chunk_index,
                .distance = vector_score.distance,
                .similarity = vector_score.similarity,
            }, request.limit);
        }

        return matches.toOwnedSlice(allocator);
    }

    fn searchDocumentVectors(self: *const Repository, allocator: std.mem.Allocator, request: interfaces.VectorSearchRequest) ![]interfaces.VectorSearchMatch {
        const sql =
            \\SELECT doc_id, dim, embedding_blob
            \\FROM documents_raw_vector
            \\WHERE workspace_id = ?1 AND collection_id = ?2 AND dim = ?3
            \\ORDER BY doc_id
        ;
        const stmt = try facet_sqlite.prepare(self.db.*, sql);
        defer facet_sqlite.finalize(stmt);
        try facet_sqlite.bindText(stmt, 1, self.workspace_id);
        try facet_sqlite.bindText(stmt, 2, self.collection_id);
        try facet_sqlite.bindInt64(stmt, 3, request.query_vector.len);

        var matches = std.ArrayList(interfaces.VectorSearchMatch).empty;
        defer matches.deinit(allocator);

        while (true) {
            const rc = c.sqlite3_step(stmt);
            if (rc == c.SQLITE_DONE) break;
            if (rc != c.SQLITE_ROW) return error.StepFailed;

            const doc_id = try facet_sqlite.columnU64(stmt, 0);
            const dim: usize = @intCast(try facet_sqlite.columnU64(stmt, 1));
            const values = try decodeColumnEmbedding(allocator, stmt, 2, dim);
            defer allocator.free(values);

            const vector_score = vector_distance.score(request.metric, request.query_vector, values);
            try insertTopDocumentMatch(allocator, &matches, .{
                .doc_id = doc_id,
                .distance = vector_score.distance,
                .similarity = vector_score.similarity,
            }, request.limit);
        }

        return matches.toOwnedSlice(allocator);
    }

    fn searchChunkVectorsAsDocuments(self: *const Repository, allocator: std.mem.Allocator, request: interfaces.VectorSearchRequest) ![]interfaces.VectorSearchMatch {
        const chunk_matches = try self.searchNearestChunks(allocator, .{
            .query_vector = request.query_vector,
            .limit = request.limit,
            .metric = request.metric,
        });
        defer allocator.free(chunk_matches);

        var matches = std.ArrayList(interfaces.VectorSearchMatch).empty;
        defer matches.deinit(allocator);
        for (chunk_matches) |match| {
            try insertTopDocumentMatch(allocator, &matches, .{
                .doc_id = match.doc_id,
                .distance = match.distance,
                .similarity = match.similarity,
            }, request.limit);
        }
        return matches.toOwnedSlice(allocator);
    }
};

fn getEmbeddingDimensionsViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, table_name: []const u8, column_name: []const u8) anyerror!?usize {
    _ = allocator;
    _ = table_name;
    _ = column_name;
    const self: *const Repository = @ptrCast(@alignCast(ctx));
    return self.getEmbeddingDimensions();
}

fn searchNearestViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, request: interfaces.VectorSearchRequest) anyerror![]interfaces.VectorSearchMatch {
    const self: *const Repository = @ptrCast(@alignCast(ctx));
    return self.searchNearest(allocator, request);
}

fn upsertEmbeddingViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, request: interfaces.UpsertEmbeddingRequest) anyerror!void {
    _ = ctx;
    _ = allocator;
    _ = request;
    return error.UnsupportedOperation;
}

fn deleteEmbeddingViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId) anyerror!void {
    _ = ctx;
    _ = allocator;
    _ = table_id;
    _ = doc_id;
    return error.UnsupportedOperation;
}

fn decodeColumnEmbedding(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, index: c_int, dim: usize) ![]f32 {
    const blob_ptr = c.sqlite3_column_blob(stmt, index) orelse return error.MissingRow;
    const blob_size = c.sqlite3_column_bytes(stmt, index);
    if (blob_size < 0) return error.ValueOutOfRange;
    const blob: []const u8 = @as([*]const u8, @ptrCast(blob_ptr))[0..@intCast(blob_size)];
    return vector_blob.decodeF32Le(allocator, blob, dim) catch |err| switch (err) {
        error.InvalidEmbeddingBlob => error.ValueOutOfRange,
        else => err,
    };
}

fn insertTopDocumentMatch(
    allocator: std.mem.Allocator,
    matches: *std.ArrayList(interfaces.VectorSearchMatch),
    candidate: interfaces.VectorSearchMatch,
    limit: usize,
) !void {
    if (limit == 0) return;
    if (matches.items.len < limit) {
        try matches.append(allocator, candidate);
    } else if (vector_distance.lessThan({}, candidate, matches.items[matches.items.len - 1])) {
        matches.items[matches.items.len - 1] = candidate;
    } else {
        return;
    }
    bubbleLastDocumentMatch(matches.items);
}

fn bubbleLastDocumentMatch(items: []interfaces.VectorSearchMatch) void {
    if (items.len < 2) return;
    var index = items.len - 1;
    while (index > 0 and vector_distance.lessThan({}, items[index], items[index - 1])) : (index -= 1) {
        std.mem.swap(interfaces.VectorSearchMatch, &items[index], &items[index - 1]);
    }
}

fn insertTopChunkMatch(
    allocator: std.mem.Allocator,
    matches: *std.ArrayList(ChunkVectorSearchMatch),
    candidate: ChunkVectorSearchMatch,
    limit: usize,
) !void {
    if (limit == 0) return;
    if (matches.items.len < limit) {
        try matches.append(allocator, candidate);
    } else if (chunkLessThan(candidate, matches.items[matches.items.len - 1])) {
        matches.items[matches.items.len - 1] = candidate;
    } else {
        return;
    }
    bubbleLastChunkMatch(matches.items);
}

fn bubbleLastChunkMatch(items: []ChunkVectorSearchMatch) void {
    if (items.len < 2) return;
    var index = items.len - 1;
    while (index > 0 and chunkLessThan(items[index], items[index - 1])) : (index -= 1) {
        std.mem.swap(ChunkVectorSearchMatch, &items[index], &items[index - 1]);
    }
}

fn chunkLessThan(lhs: ChunkVectorSearchMatch, rhs: ChunkVectorSearchMatch) bool {
    if (lhs.similarity != rhs.similarity) return lhs.similarity > rhs.similarity;
    if (lhs.doc_id != rhs.doc_id) return lhs.doc_id < rhs.doc_id;
    return lhs.chunk_index < rhs.chunk_index;
}

test "sqlite exact vector repository searches raw document vectors" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws_vec" });
    try collections_sqlite.ensureCollection(db, .{ .workspace_id = "ws_vec", .collection_id = "ws_vec::docs", .name = "docs" });
    try collections_sqlite.upsertDocumentRaw(db, .{ .workspace_id = "ws_vec", .collection_id = "ws_vec::docs", .doc_id = 1 });
    try collections_sqlite.upsertDocumentRaw(db, .{ .workspace_id = "ws_vec", .collection_id = "ws_vec::docs", .doc_id = 2 });

    const one = try vector_blob.encodeF32Le(std.testing.allocator, &.{ 1.0, 0.0 });
    defer std.testing.allocator.free(one);
    const two = try vector_blob.encodeF32Le(std.testing.allocator, &.{ 0.0, 1.0 });
    defer std.testing.allocator.free(two);

    try collections_sqlite.upsertDocumentVector(db, .{ .workspace_id = "ws_vec", .collection_id = "ws_vec::docs", .doc_id = 1, .dim = 2, .embedding_blob = one });
    try collections_sqlite.upsertDocumentVector(db, .{ .workspace_id = "ws_vec", .collection_id = "ws_vec::docs", .doc_id = 2, .dim = 2, .embedding_blob = two });

    const repo = Repository{ .db = &db, .workspace_id = "ws_vec", .collection_id = "ws_vec::docs" };
    const vector_repo = repo.asVectorRepository();
    const matches = try vector_repo.searchNearestFn(vector_repo.ctx, std.testing.allocator, .{
        .table_name = "documents_raw_vector",
        .key_column = "doc_id",
        .vector_column = "embedding_blob",
        .query_vector = &.{ 0.9, 0.1 },
        .limit = 2,
    });
    defer std.testing.allocator.free(matches);

    try std.testing.expectEqual(@as(usize, 2), matches.len);
    try std.testing.expectEqual(@as(interfaces.DocId, 1), matches[0].doc_id);
    try std.testing.expect(matches[0].similarity > matches[1].similarity);
}

test "sqlite exact vector repository searches raw chunk vectors" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws_chunk_vec" });
    try collections_sqlite.ensureCollection(db, .{ .workspace_id = "ws_chunk_vec", .collection_id = "ws_chunk_vec::docs", .name = "docs" });
    try collections_sqlite.upsertDocumentRaw(db, .{ .workspace_id = "ws_chunk_vec", .collection_id = "ws_chunk_vec::docs", .doc_id = 7 });
    try collections_sqlite.upsertChunkRaw(db, .{ .workspace_id = "ws_chunk_vec", .collection_id = "ws_chunk_vec::docs", .doc_id = 7, .chunk_index = 0 });
    try collections_sqlite.upsertChunkRaw(db, .{ .workspace_id = "ws_chunk_vec", .collection_id = "ws_chunk_vec::docs", .doc_id = 7, .chunk_index = 1 });

    const near = try vector_blob.encodeF32Le(std.testing.allocator, &.{ 0.2, 0.8 });
    defer std.testing.allocator.free(near);
    const far = try vector_blob.encodeF32Le(std.testing.allocator, &.{ 1.0, 0.0 });
    defer std.testing.allocator.free(far);
    try collections_sqlite.upsertChunkVector(db, .{ .workspace_id = "ws_chunk_vec", .collection_id = "ws_chunk_vec::docs", .doc_id = 7, .chunk_index = 0, .dim = 2, .embedding_blob = far });
    try collections_sqlite.upsertChunkVector(db, .{ .workspace_id = "ws_chunk_vec", .collection_id = "ws_chunk_vec::docs", .doc_id = 7, .chunk_index = 1, .dim = 2, .embedding_blob = near });

    const repo = Repository{ .db = &db, .workspace_id = "ws_chunk_vec", .collection_id = "ws_chunk_vec::docs", .scope = .chunks };
    const matches = try repo.searchNearestChunks(std.testing.allocator, .{
        .query_vector = &.{ 0.0, 1.0 },
        .limit = 1,
    });
    defer std.testing.allocator.free(matches);

    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqual(@as(u64, 7), matches[0].doc_id);
    try std.testing.expectEqual(@as(u32, 1), matches[0].chunk_index);
}

test "sqlite exact vector repository can feed hybrid search" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws_hybrid_vec" });
    try collections_sqlite.ensureCollection(db, .{ .workspace_id = "ws_hybrid_vec", .collection_id = "ws_hybrid_vec::docs", .name = "docs" });
    try collections_sqlite.upsertDocumentRaw(db, .{ .workspace_id = "ws_hybrid_vec", .collection_id = "ws_hybrid_vec::docs", .doc_id = 1 });
    try collections_sqlite.upsertDocumentRaw(db, .{ .workspace_id = "ws_hybrid_vec", .collection_id = "ws_hybrid_vec::docs", .doc_id = 2 });

    const one = try vector_blob.encodeF32Le(std.testing.allocator, &.{ 1.0, 0.0 });
    defer std.testing.allocator.free(one);
    const two = try vector_blob.encodeF32Le(std.testing.allocator, &.{ 0.0, 1.0 });
    defer std.testing.allocator.free(two);
    try collections_sqlite.upsertDocumentVector(db, .{ .workspace_id = "ws_hybrid_vec", .collection_id = "ws_hybrid_vec::docs", .doc_id = 1, .dim = 2, .embedding_blob = one });
    try collections_sqlite.upsertDocumentVector(db, .{ .workspace_id = "ws_hybrid_vec", .collection_id = "ws_hybrid_vec::docs", .doc_id = 2, .dim = 2, .embedding_blob = two });

    var search = search_store.Store.init(std.testing.allocator);
    defer search.deinit();
    try search.upsertDocument(.{ .table_id = 9, .doc_id = 1, .content = "alpha beta", .language = "english" });
    try search.upsertDocument(.{ .table_id = 9, .doc_id = 2, .content = "alpha", .language = "english" });

    const repo = Repository{ .db = &db, .workspace_id = "ws_hybrid_vec", .collection_id = "ws_hybrid_vec::docs" };
    const vector_repo = repo.asVectorRepository();
    const term_hash = tokenization_sqlite.hashTermLexeme("alpha");
    const results = try hybrid_search.search(std.testing.allocator, search.bm25Repository(), vector_repo, .{
        .bm25_table_id = 9,
        .bm25_term_hashes = &.{term_hash},
        .vector = .{
            .table_name = "documents_raw_vector",
            .key_column = "doc_id",
            .vector_column = "embedding_blob",
            .query_vector = &.{ 0.0, 1.0 },
            .limit = 2,
        },
        .vector_weight = 0.9,
        .limit = 2,
    });
    defer std.testing.allocator.free(results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqual(@as(interfaces.DocId, 2), results[0].doc_id);
}
