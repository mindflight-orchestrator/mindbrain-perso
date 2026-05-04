const std = @import("std");
const interfaces = @import("interfaces.zig");
const roaring = @import("roaring.zig");
pub const Error = error{
    InvalidWeight,
};

const CandidateScore = struct {
    bm25_score: f64 = 0.0,
    vector_score: f64 = 0.0,
};

pub fn search(
    allocator: std.mem.Allocator,
    bm25_repository: interfaces.Bm25Repository,
    vector_repository: ?interfaces.VectorRepository,
    request: interfaces.HybridSearchRequest,
) ![]interfaces.HybridSearchMatch {
    if (request.vector_weight < 0.0 or request.vector_weight > 1.0) {
        return error.InvalidWeight;
    }

    const collection_stats = try bm25_repository.getCollectionStatsFn(
        bm25_repository.ctx,
        allocator,
        request.bm25_table_id,
    );
    const term_stats = try bm25_repository.getTermStatsFn(
        bm25_repository.ctx,
        allocator,
        request.bm25_table_id,
        request.bm25_term_hashes,
    );
    defer allocator.free(term_stats);

    var term_df = std.AutoHashMap(u64, u64).init(allocator);
    defer term_df.deinit();
    for (term_stats) |term_stat| {
        try term_df.put(term_stat.term_hash, term_stat.document_frequency);
    }

    var candidate_scores = std.AutoHashMap(interfaces.DocId, CandidateScore).init(allocator);
    defer candidate_scores.deinit();

    for (request.bm25_term_hashes) |term_hash| {
        const maybe_bitmap = try bm25_repository.getPostingBitmapFn(
            bm25_repository.ctx,
            allocator,
            request.bm25_table_id,
            term_hash,
        );

        if (maybe_bitmap) |bitmap| {
            defer {
                var bm = bitmap;
                bm.deinit();
            }

            const doc_ids = try bitmap.toArray(allocator);
            defer allocator.free(doc_ids);

            for (doc_ids) |doc_id| {
                _ = try getOrPutCandidate(&candidate_scores, doc_id);
            }
        }
    }

    if (request.vector) |vector_request| {
        if (vector_repository) |repo| {
            const vector_matches = try repo.searchNearestFn(repo.ctx, allocator, vector_request);
            defer allocator.free(vector_matches);

            for (vector_matches) |match| {
                const entry = try getOrPutCandidate(&candidate_scores, match.doc_id);
                if (match.similarity > entry.vector_score) {
                    entry.vector_score = match.similarity;
                }
            }
        }
    }

    var candidate_iter = candidate_scores.iterator();
    while (candidate_iter.next()) |entry| {
        const doc_id = entry.key_ptr.*;
        const maybe_doc_stats = try bm25_repository.getDocumentStatsFn(
            bm25_repository.ctx,
            allocator,
            request.bm25_table_id,
            doc_id,
        );
        if (maybe_doc_stats == null) continue;

        const term_freqs = try bm25_repository.getTermFrequenciesFn(
            bm25_repository.ctx,
            allocator,
            request.bm25_table_id,
            doc_id,
            request.bm25_term_hashes,
        );
        defer allocator.free(term_freqs);

        entry.value_ptr.bm25_score = calculateBm25Score(
            request.bm25_term_hashes,
            term_freqs,
            maybe_doc_stats.?,
            collection_stats,
            &term_df,
        );
    }

    var results = std.ArrayList(interfaces.HybridSearchMatch).empty;
    defer results.deinit(allocator);

    var iter = candidate_scores.iterator();
    while (iter.next()) |entry| {
        var bm25_score = entry.value_ptr.bm25_score;
        var vector_score = entry.value_ptr.vector_score;
        if (!std.math.isFinite(bm25_score)) {
            bm25_score = 0.0;
        }
        if (!std.math.isFinite(vector_score)) {
            vector_score = 0.0;
        }
        var combined_score = bm25_score * (1.0 - request.vector_weight) +
            vector_score * request.vector_weight;
        if (!std.math.isFinite(combined_score)) {
            combined_score = 0.0;
        }

        try results.append(allocator, .{
            .doc_id = entry.key_ptr.*,
            .bm25_score = bm25_score,
            .vector_score = vector_score,
            .combined_score = combined_score,
        });
    }

    std.mem.sort(interfaces.HybridSearchMatch, results.items, {}, struct {
        fn lessThan(_: void, a: interfaces.HybridSearchMatch, b: interfaces.HybridSearchMatch) bool {
            return a.combined_score > b.combined_score;
        }
    }.lessThan);

    if (results.items.len > request.limit) {
        results.shrinkRetainingCapacity(request.limit);
    }

    return results.toOwnedSlice(allocator);
}

fn calculateBm25Score(
    term_hashes: []const u64,
    term_freqs: []const interfaces.TermFrequency,
    doc_stats: interfaces.DocumentStats,
    collection_stats: interfaces.CollectionStats,
    term_df: *const std.AutoHashMap(u64, u64),
) f64 {
    if (collection_stats.total_documents <= 0) {
        return 0.0;
    }

    // Match search_native: invalid avg document length must not reach the BM25 ratio.
    var avgdl: f64 = collection_stats.avg_document_length;
    if (!(avgdl > 0.0) or std.math.isNan(avgdl) or std.math.isInf(avgdl)) {
        avgdl = 1.0;
    }

    const k1 = 1.2;
    const b = 0.75;
    const doc_len = @as(f64, @floatFromInt(doc_stats.document_length));
    const total_docs = @as(f64, @floatFromInt(collection_stats.total_documents));

    var score: f64 = 0.0;
    for (term_hashes) |term_hash| {
        const tf = lookupFrequency(term_freqs, term_hash);
        if (tf == 0) continue;

        const df = term_df.get(term_hash) orelse 0;
        if (df == 0) continue;

        const idf = @log((total_docs + 1.0) / (@as(f64, @floatFromInt(df)) + 0.5));
        const tf_f = @as(f64, @floatFromInt(tf));
        const numerator = tf_f * (k1 + 1.0);
        const denominator = tf_f + k1 * (1.0 - b + b * (doc_len / avgdl));
        score += idf * (numerator / denominator);
    }

    if (!std.math.isFinite(score)) {
        return 0.0;
    }
    return score;
}

fn lookupFrequency(term_freqs: []const interfaces.TermFrequency, term_hash: u64) u32 {
    for (term_freqs) |term_freq| {
        if (term_freq.term_hash == term_hash) return term_freq.frequency;
    }
    return 0;
}

fn getOrPutCandidate(
    map: *std.AutoHashMap(interfaces.DocId, CandidateScore),
    doc_id: interfaces.DocId,
) !*CandidateScore {
    const gop = try map.getOrPut(doc_id);
    if (!gop.found_existing) {
        gop.value_ptr.* = .{};
    }
    return gop.value_ptr;
}

test "hybrid search ranks BM25-only candidates by exact BM25 score" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var fixture = TestFixture.init();
    const bm25_repo = fixture.bm25Repository();

    const results = try search(allocator, bm25_repo, null, .{
        .bm25_table_id = 42,
        .bm25_term_hashes = &.{ 11, 22 },
        .limit = 10,
        .vector_weight = 0.0,
    });

    try std.testing.expectEqual(@as(usize, 3), results.len);
    try std.testing.expectEqual(@as(interfaces.DocId, 1), results[0].doc_id);
    try std.testing.expectEqual(@as(interfaces.DocId, 3), results[1].doc_id);
    try std.testing.expectEqual(@as(interfaces.DocId, 2), results[2].doc_id);
    try std.testing.expect(results[0].bm25_score > results[1].bm25_score);
    try std.testing.expect(results[1].bm25_score > results[2].bm25_score);
    try std.testing.expectEqual(@as(f64, 0.0), results[0].vector_score);
}

test "hybrid search blends vector and BM25 scores" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var fixture = TestFixture.init();
    const bm25_repo = fixture.bm25Repository();
    const vector_repo = fixture.vectorRepository(&.{
        .{ .doc_id = 2, .distance = 0.1, .similarity = 0.95 },
        .{ .doc_id = 1, .distance = 0.2, .similarity = 0.70 },
        .{ .doc_id = 4, .distance = 0.3, .similarity = 0.60 },
    });

    const results = try search(allocator, bm25_repo, vector_repo, .{
        .bm25_table_id = 42,
        .bm25_term_hashes = &.{ 11, 22 },
        .vector = .{
            .table_name = "docs",
            .key_column = "doc_id",
            .vector_column = "embedding",
            .query_vector = &.{ 0.1, 0.2, 0.3 },
            .limit = 5,
        },
        .limit = 10,
        .vector_weight = 0.85,
    });

    try std.testing.expectEqual(@as(usize, 4), results.len);
    try std.testing.expectEqual(@as(interfaces.DocId, 2), results[0].doc_id);
    try std.testing.expectEqual(@as(interfaces.DocId, 1), results[1].doc_id);
    try std.testing.expectEqual(@as(interfaces.DocId, 4), results[2].doc_id);
    try std.testing.expectEqual(@as(interfaces.DocId, 3), results[3].doc_id);
    try std.testing.expect(results[0].vector_score > results[1].vector_score);
    try std.testing.expect(results[1].bm25_score > results[2].bm25_score);
}

test "hybrid search rejects invalid vector weights" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var fixture = TestFixture.init();

    try std.testing.expectError(error.InvalidWeight, search(
        allocator,
        fixture.bm25Repository(),
        null,
        .{
            .bm25_table_id = 42,
            .bm25_term_hashes = &.{11},
            .vector_weight = 1.5,
        },
    ));
}

test "calculateBm25Score normalizes non-finite avg_document_length" {
    var term_df = std.AutoHashMap(u64, u64).init(std.testing.allocator);
    defer term_df.deinit();
    try term_df.put(11, 1);

    const term_freqs = [_]interfaces.TermFrequency{
        .{ .term_hash = 11, .frequency = 2 },
    };
    const doc_stats: interfaces.DocumentStats = .{ .doc_id = 1, .document_length = 10, .unique_terms = 1 };
    const hashes = [_]u64{11};

    const inf_stats: interfaces.CollectionStats = .{
        .total_documents = 2,
        .avg_document_length = std.math.inf(f64),
    };
    const s_inf = calculateBm25Score(&hashes, &term_freqs, doc_stats, inf_stats, &term_df);
    try std.testing.expect(std.math.isFinite(s_inf));
    try std.testing.expect(s_inf > 0.0);

    const nan_stats: interfaces.CollectionStats = .{
        .total_documents = 2,
        .avg_document_length = std.math.nan(f64),
    };
    const s_nan = calculateBm25Score(&hashes, &term_freqs, doc_stats, nan_stats, &term_df);
    try std.testing.expect(std.math.isFinite(s_nan));
    try std.testing.expect(s_nan > 0.0);
}

const TestFixture = struct {
    vector_matches: []const interfaces.VectorSearchMatch = &.{},

    const doc_stats = [_]interfaces.DocumentStats{
        .{ .doc_id = 1, .document_length = 100, .unique_terms = 2 },
        .{ .doc_id = 2, .document_length = 80, .unique_terms = 1 },
        .{ .doc_id = 3, .document_length = 120, .unique_terms = 1 },
        .{ .doc_id = 4, .document_length = 90, .unique_terms = 0 },
    };

    const term_stats = [_]interfaces.TermStat{
        .{ .term_hash = 11, .document_frequency = 2 },
        .{ .term_hash = 22, .document_frequency = 2 },
    };

    const doc1_term_freqs = [_]interfaces.TermFrequency{
        .{ .term_hash = 11, .frequency = 3 },
        .{ .term_hash = 22, .frequency = 1 },
    };

    const doc2_term_freqs = [_]interfaces.TermFrequency{
        .{ .term_hash = 11, .frequency = 1 },
    };

    const doc3_term_freqs = [_]interfaces.TermFrequency{
        .{ .term_hash = 22, .frequency = 2 },
    };

    pub fn init() TestFixture {
        return .{};
    }

    pub fn bm25Repository(self: *TestFixture) interfaces.Bm25Repository {
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

    pub fn vectorRepository(self: *TestFixture, matches: []const interfaces.VectorSearchMatch) interfaces.VectorRepository {
        self.vector_matches = matches;
        return .{
            .ctx = @ptrCast(self),
            .getEmbeddingDimensionsFn = vectorGetEmbeddingDimensions,
            .searchNearestFn = vectorSearchNearest,
            .upsertEmbeddingFn = vectorUpsertEmbedding,
            .deleteEmbeddingFn = vectorDeleteEmbedding,
        };
    }
};

fn bm25GetCollectionStats(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64) anyerror!interfaces.CollectionStats {
    _ = ctx;
    _ = allocator;
    try std.testing.expectEqual(@as(u64, 42), table_id);
    return .{
        .total_documents = 3,
        .avg_document_length = 100.0,
    };
}

fn bm25GetDocumentStats(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId) anyerror!?interfaces.DocumentStats {
    _ = allocator;
    try std.testing.expectEqual(@as(u64, 42), table_id);
    const fixture: *TestFixture = @ptrCast(@alignCast(ctx));
    _ = fixture;

    for (TestFixture.doc_stats) |doc_stats| {
        if (doc_stats.doc_id == doc_id) return doc_stats;
    }
    return null;
}

fn bm25GetTermStats(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, term_hashes: []const u64) anyerror![]interfaces.TermStat {
    _ = ctx;
    try std.testing.expectEqual(@as(u64, 42), table_id);

    var stats = try allocator.alloc(interfaces.TermStat, term_hashes.len);
    for (term_hashes, 0..) |term_hash, i| {
        stats[i] = findTermStat(term_hash) orelse .{
            .term_hash = term_hash,
            .document_frequency = 0,
        };
    }
    return stats;
}

fn bm25GetTermFrequencies(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId, term_hashes: []const u64) anyerror![]interfaces.TermFrequency {
    _ = ctx;
    try std.testing.expectEqual(@as(u64, 42), table_id);

    const source = switch (doc_id) {
        1 => TestFixture.doc1_term_freqs[0..],
        2 => TestFixture.doc2_term_freqs[0..],
        3 => TestFixture.doc3_term_freqs[0..],
        else => &.{},
    };

    var freqs = std.ArrayList(interfaces.TermFrequency).empty;
    defer freqs.deinit(allocator);

    for (term_hashes) |term_hash| {
        for (source) |term_freq| {
            if (term_freq.term_hash == term_hash) {
                try freqs.append(allocator, term_freq);
                break;
            }
        }
    }

    return freqs.toOwnedSlice(allocator);
}

fn bm25GetPostingBitmap(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, term_hash: u64) anyerror!?roaring.Bitmap {
    _ = ctx;
    _ = allocator;
    try std.testing.expectEqual(@as(u64, 42), table_id);

    return switch (term_hash) {
        11 => try roaring.Bitmap.fromSlice(&.{ 1, 2 }),
        22 => try roaring.Bitmap.fromSlice(&.{ 1, 3 }),
        else => null,
    };
}

fn bm25UpsertDocument(ctx: *anyopaque, allocator: std.mem.Allocator, request: interfaces.UpsertDocumentRequest) anyerror!void {
    _ = ctx;
    _ = allocator;
    _ = request;
    return error.TestUnexpectedCall;
}

fn bm25DeleteDocument(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId) anyerror!void {
    _ = ctx;
    _ = allocator;
    _ = table_id;
    _ = doc_id;
    return error.TestUnexpectedCall;
}

fn vectorGetEmbeddingDimensions(ctx: *anyopaque, allocator: std.mem.Allocator, table_name: []const u8, column_name: []const u8) anyerror!?usize {
    _ = ctx;
    _ = allocator;
    _ = table_name;
    _ = column_name;
    return 3;
}

fn vectorSearchNearest(ctx: *anyopaque, allocator: std.mem.Allocator, request: interfaces.VectorSearchRequest) anyerror![]interfaces.VectorSearchMatch {
    try std.testing.expectEqual(@as(usize, 5), request.limit);
    const fixture: *TestFixture = @ptrCast(@alignCast(ctx));
    const matches = try allocator.alloc(interfaces.VectorSearchMatch, fixture.vector_matches.len);
    @memcpy(matches, fixture.vector_matches);
    return matches;
}

fn vectorUpsertEmbedding(ctx: *anyopaque, allocator: std.mem.Allocator, request: interfaces.UpsertEmbeddingRequest) anyerror!void {
    _ = ctx;
    _ = allocator;
    _ = request;
    return error.TestUnexpectedCall;
}

fn vectorDeleteEmbedding(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId) anyerror!void {
    _ = ctx;
    _ = allocator;
    _ = table_id;
    _ = doc_id;
    return error.TestUnexpectedCall;
}

fn findTermStat(term_hash: u64) ?interfaces.TermStat {
    for (TestFixture.term_stats) |term_stat| {
        if (term_stat.term_hash == term_hash) return term_stat;
    }
    return null;
}
