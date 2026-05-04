const std = @import("std");
const fixture_loader = @import("fixture_loader.zig");
const interfaces = @import("interfaces.zig");
const roaring = @import("roaring.zig");
const hybrid_search = @import("hybrid_search.zig");
const vector_distance = @import("vector_distance.zig");
const tokenization_sqlite = @import("tokenization_sqlite.zig");

pub const table_id: u64 = 1;

pub const Store = struct {
    fixture: *const fixture_loader.LoadedFixture,

    pub fn init(fixture: *const fixture_loader.LoadedFixture) Store {
        return .{ .fixture = fixture };
    }

    pub fn bm25Repository(self: *Store) interfaces.Bm25Repository {
        return .{
            .ctx = @ptrCast(self),
            .getCollectionStatsFn = bm25GetCollectionStats,
            .getDocumentStatsFn = bm25GetDocumentStats,
            .getTermStatsFn = bm25GetTermStats,
            .getTermFrequenciesFn = bm25GetTermFrequencies,
            .getPostingBitmapFn = bm25GetPostingBitmap,
            .upsertDocumentFn = bm25UpsertDocument,
            .deleteDocumentFn = bm25DeleteDocument,
        };
    }

    pub fn vectorRepository(self: *Store) interfaces.VectorRepository {
        return .{
            .ctx = @ptrCast(self),
            .getEmbeddingDimensionsFn = vectorGetEmbeddingDimensions,
            .searchNearestFn = vectorSearchNearest,
            .upsertEmbeddingFn = vectorUpsertEmbedding,
            .deleteEmbeddingFn = vectorDeleteEmbedding,
        };
    }
};

pub fn tokenizeQueryHashes(allocator: std.mem.Allocator, query: []const u8) ![]u64 {
    return tokenizeHashes(allocator, query);
}

fn bm25GetCollectionStats(ctx: *anyopaque, allocator: std.mem.Allocator, requested_table_id: u64) anyerror!interfaces.CollectionStats {
    _ = allocator;
    if (requested_table_id != table_id) return .{ .total_documents = 0, .avg_document_length = 0.0 };

    const store: *Store = @ptrCast(@alignCast(ctx));
    const docs = store.fixture.fixture.documents;

    var total_length: u64 = 0;
    for (docs) |doc| {
        const tokens = try countTokens(doc.content);
        total_length += tokens;
    }

    return .{
        .total_documents = docs.len,
        .avg_document_length = if (docs.len == 0) 0.0 else @as(f64, @floatFromInt(total_length)) / @as(f64, @floatFromInt(docs.len)),
    };
}

fn bm25GetDocumentStats(ctx: *anyopaque, allocator: std.mem.Allocator, requested_table_id: u64, doc_id: interfaces.DocId) anyerror!?interfaces.DocumentStats {
    _ = allocator;
    if (requested_table_id != table_id) return null;

    const store: *Store = @ptrCast(@alignCast(ctx));
    for (store.fixture.fixture.documents) |doc| {
        if (doc.doc_id != doc_id) continue;

        const doc_tokens = try tokenizeHashes(std.heap.page_allocator, doc.content);
        defer std.heap.page_allocator.free(doc_tokens);

        var unique = std.AutoHashMap(u64, void).init(std.heap.page_allocator);
        defer unique.deinit();
        for (doc_tokens) |token| {
            try unique.put(token, {});
        }

        return .{
            .doc_id = doc.doc_id,
            .document_length = @intCast(doc_tokens.len),
            .unique_terms = @intCast(unique.count()),
        };
    }

    return null;
}

fn bm25GetTermStats(ctx: *anyopaque, allocator: std.mem.Allocator, requested_table_id: u64, term_hashes: []const u64) anyerror![]interfaces.TermStat {
    if (requested_table_id != table_id) return allocator.alloc(interfaces.TermStat, 0);

    const store: *Store = @ptrCast(@alignCast(ctx));
    var stats = try allocator.alloc(interfaces.TermStat, term_hashes.len);

    for (term_hashes, 0..) |term_hash, i| {
        var doc_freq: u64 = 0;
        for (store.fixture.fixture.documents) |doc| {
            const doc_tokens = try tokenizeHashes(std.heap.page_allocator, doc.content);
            defer std.heap.page_allocator.free(doc_tokens);
            if (containsToken(doc_tokens, term_hash)) doc_freq += 1;
        }

        stats[i] = .{
            .term_hash = term_hash,
            .document_frequency = doc_freq,
        };
    }

    return stats;
}

fn bm25GetTermFrequencies(ctx: *anyopaque, allocator: std.mem.Allocator, requested_table_id: u64, doc_id: interfaces.DocId, term_hashes: []const u64) anyerror![]interfaces.TermFrequency {
    if (requested_table_id != table_id) return allocator.alloc(interfaces.TermFrequency, 0);

    const store: *Store = @ptrCast(@alignCast(ctx));
    for (store.fixture.fixture.documents) |doc| {
        if (doc.doc_id != doc_id) continue;

        const doc_tokens = try tokenizeHashes(std.heap.page_allocator, doc.content);
        defer std.heap.page_allocator.free(doc_tokens);

        var freqs = std.ArrayList(interfaces.TermFrequency).empty;
        defer freqs.deinit(allocator);

        for (term_hashes) |term_hash| {
            const count = countTokenFrequency(doc_tokens, term_hash);
            if (count == 0) continue;
            try freqs.append(allocator, .{
                .term_hash = term_hash,
                .frequency = count,
            });
        }

        return freqs.toOwnedSlice(allocator);
    }

    return allocator.alloc(interfaces.TermFrequency, 0);
}

fn bm25GetPostingBitmap(ctx: *anyopaque, allocator: std.mem.Allocator, requested_table_id: u64, term_hash: u64) anyerror!?roaring.Bitmap {
    _ = allocator;
    if (requested_table_id != table_id) return null;

    const store: *Store = @ptrCast(@alignCast(ctx));
    var doc_ids = std.ArrayList(u32).empty;
    defer doc_ids.deinit(std.heap.page_allocator);

    for (store.fixture.fixture.documents) |doc| {
        const doc_tokens = try tokenizeHashes(std.heap.page_allocator, doc.content);
        defer std.heap.page_allocator.free(doc_tokens);

        if (containsToken(doc_tokens, term_hash)) {
            try doc_ids.append(std.heap.page_allocator, @intCast(doc.doc_id));
        }
    }

    if (doc_ids.items.len == 0) return null;
    return try roaring.Bitmap.fromSlice(doc_ids.items);
}

fn bm25UpsertDocument(ctx: *anyopaque, allocator: std.mem.Allocator, request: interfaces.UpsertDocumentRequest) anyerror!void {
    _ = ctx;
    _ = allocator;
    _ = request;
    return error.NotImplemented;
}

fn bm25DeleteDocument(ctx: *anyopaque, allocator: std.mem.Allocator, requested_table_id: u64, doc_id: interfaces.DocId) anyerror!void {
    _ = ctx;
    _ = allocator;
    _ = requested_table_id;
    _ = doc_id;
    return error.NotImplemented;
}

fn vectorGetEmbeddingDimensions(ctx: *anyopaque, allocator: std.mem.Allocator, table_name: []const u8, column_name: []const u8) anyerror!?usize {
    _ = allocator;
    const store: *Store = @ptrCast(@alignCast(ctx));
    if (!std.mem.eql(u8, table_name, store.fixture.fixture.schema.table_name)) return null;
    if (store.fixture.fixture.schema.vector_column == null) return null;
    if (!std.mem.eql(u8, column_name, store.fixture.fixture.schema.vector_column.?)) return null;
    if (store.fixture.fixture.embeddings.len == 0) return 0;
    return store.fixture.fixture.embeddings[0].values.len;
}

fn vectorSearchNearest(ctx: *anyopaque, allocator: std.mem.Allocator, request: interfaces.VectorSearchRequest) anyerror![]interfaces.VectorSearchMatch {
    const store: *Store = @ptrCast(@alignCast(ctx));

    var matches = std.ArrayList(interfaces.VectorSearchMatch).empty;
    defer matches.deinit(allocator);

    for (store.fixture.fixture.embeddings) |embedding| {
        const vector_score = vector_distance.score(request.metric, request.query_vector, embedding.values);
        try matches.append(allocator, .{
            .doc_id = embedding.doc_id,
            .distance = vector_score.distance,
            .similarity = vector_score.similarity,
        });
    }

    std.mem.sort(interfaces.VectorSearchMatch, matches.items, {}, vector_distance.lessThan);

    if (matches.items.len > request.limit) {
        matches.shrinkRetainingCapacity(request.limit);
    }

    return matches.toOwnedSlice(allocator);
}

fn vectorUpsertEmbedding(ctx: *anyopaque, allocator: std.mem.Allocator, request: interfaces.UpsertEmbeddingRequest) anyerror!void {
    _ = ctx;
    _ = allocator;
    _ = request;
    return error.NotImplemented;
}

fn vectorDeleteEmbedding(ctx: *anyopaque, allocator: std.mem.Allocator, requested_table_id: u64, doc_id: interfaces.DocId) anyerror!void {
    _ = ctx;
    _ = allocator;
    _ = requested_table_id;
    _ = doc_id;
    return error.NotImplemented;
}

fn tokenizeHashes(allocator: std.mem.Allocator, text: []const u8) ![]u64 {
    return tokenization_sqlite.tokenizeSearchHashes(allocator, text, null);
}

fn countTokens(text: []const u8) !u64 {
    const tokens = try tokenizeHashes(std.heap.page_allocator, text);
    defer std.heap.page_allocator.free(tokens);
    return tokens.len;
}

fn containsToken(tokens: []const u64, target: u64) bool {
    for (tokens) |token| {
        if (token == target) return true;
    }
    return false;
}

fn countTokenFrequency(tokens: []const u64, target: u64) u32 {
    var count: u32 = 0;
    for (tokens) |token| {
        if (token == target) count += 1;
    }
    return count;
}

test "fixture-backed repositories drive hybrid search from the golden fixture" {
    var loaded = try fixture_loader.loadFromFile(std.testing.allocator, "docs/fixtures/hybrid_search_baseline_fixture.json");
    defer loaded.deinit();

    var store = Store.init(&loaded);
    const query = try loaded.findQuery("hybrid-search-01");
    const term_hashes = try tokenizeQueryHashes(std.testing.allocator, query.params.query);
    defer std.testing.allocator.free(term_hashes);

    const results = try hybrid_search.search(
        std.testing.allocator,
        store.bm25Repository(),
        store.vectorRepository(),
        .{
            .bm25_table_id = table_id,
            .bm25_term_hashes = term_hashes,
            .vector = .{
                .table_name = query.params.table_name,
                .key_column = loaded.fixture.schema.key_column,
                .vector_column = query.params.vector_column.?,
                .query_vector = query.params.query_vector.?,
                .limit = query.params.limit,
            },
            .vector_weight = query.params.vector_weight,
            .limit = query.params.limit,
        },
    );
    defer std.testing.allocator.free(results);

    try std.testing.expectEqual(@as(usize, 3), results.len);
    const expected = loaded.fixture.expected.object.get("hybrid-search-01").?;
    const ordered_doc_ids = expected.object.get("ordered_doc_ids").?.array.items;
    const scores = expected.object.get("scores").?.object;
    const tolerance = loaded.fixture.comparison.score_tolerance;
    for (results, 0..) |result, index| {
        try std.testing.expectEqual(
            @as(interfaces.DocId, @intCast(ordered_doc_ids[index].integer)),
            result.doc_id,
        );

        var score_key_buf: [32]u8 = undefined;
        const score_key = try std.fmt.bufPrint(&score_key_buf, "{d}", .{result.doc_id});
        const expected_score = scores.get(score_key).?.float;
        try std.testing.expectApproxEqAbs(expected_score, result.combined_score, tolerance);
    }
}
