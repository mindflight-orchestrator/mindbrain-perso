const std = @import("std");
const interfaces = @import("interfaces.zig");
const roaring = @import("roaring.zig");
pub const Error = error{
    InvalidWeight,
};

/// Sentinel for "candidate has no score in this channel". Both channels use
/// the same sentinel so BM25-only and vector-only candidates are treated
/// symmetrically: a candidate is "present" in a channel iff its score is
/// finite, and only present candidates receive a rank in that channel.
const score_missing = -std.math.inf(f64);

const CandidateScore = struct {
    bm25_score: f64 = score_missing,
    vector_score: f64 = score_missing,
};

/// Reciprocal Rank Fusion (RRF) damping constant.
///
/// rrf(rank) = 1 / (RRF_K + rank). A larger K flattens the head of each
/// channel's ranking (the gap between rank 1 and rank 2 shrinks), so no single
/// channel's top hit can dominate the fused order on its own. K = 60 is the
/// widely-used default from the original RRF work (Cormack, Clarke & Buettcher,
/// SIGIR 2009) and TREC practice, and is what we adopt here.
const RRF_K: f64 = 60.0;

/// One candidate's score within a single channel, used only to compute ranks.
const RankEntry = struct {
    doc_id: interfaces.DocId,
    score: f64,
};

/// Rank ordering within a channel: higher score first; ties break on doc_id
/// ascending so rank assignment is fully deterministic across runs.
fn rankEntryBefore(_: void, a: RankEntry, b: RankEntry) bool {
    if (a.score != b.score) return a.score > b.score;
    return a.doc_id < b.doc_id;
}

const Channel = enum { bm25, vector };

/// Builds a doc_id -> 1-based rank map for one channel. Only candidates whose
/// score in that channel is finite (i.e. present) are ranked; absent
/// candidates are omitted from the map entirely and contribute 0 via
/// `rrfContribution` (no worst-rank penalty).
fn buildChannelRanks(
    allocator: std.mem.Allocator,
    candidate_scores: *const std.AutoHashMap(interfaces.DocId, CandidateScore),
    channel: Channel,
) !std.AutoHashMap(interfaces.DocId, usize) {
    var entries = std.ArrayList(RankEntry).empty;
    defer entries.deinit(allocator);

    var it = candidate_scores.iterator();
    while (it.next()) |entry| {
        const score = switch (channel) {
            .bm25 => entry.value_ptr.bm25_score,
            .vector => entry.value_ptr.vector_score,
        };
        if (!std.math.isFinite(score)) continue;
        try entries.append(allocator, .{ .doc_id = entry.key_ptr.*, .score = score });
    }

    std.mem.sort(RankEntry, entries.items, {}, rankEntryBefore);

    var ranks = std.AutoHashMap(interfaces.DocId, usize).init(allocator);
    errdefer ranks.deinit();
    for (entries.items, 0..) |entry, index| {
        try ranks.put(entry.doc_id, index + 1);
    }
    return ranks;
}

/// RRF contribution of one channel for a doc: 1/(RRF_K + rank) when the doc is
/// present in that channel (has a rank), otherwise 0.
fn rrfContribution(ranks: *const std.AutoHashMap(interfaces.DocId, usize), doc_id: interfaces.DocId) f64 {
    const rank = ranks.get(doc_id) orelse return 0.0;
    return 1.0 / (RRF_K + @as(f64, @floatFromInt(rank)));
}

/// Reciprocal Rank Fusion of the two channels, shared by both fusion sites so
/// their ranking contract cannot drift.
///
///     combined = (1 - w) * rrf(rank_bm25) + w * rrf(rank_vector)
///     rrf(rank) = 1 / (RRF_K + rank),  and 0 for a channel the doc is absent from
///
/// We fuse ranks rather than raw scores because RRF is magnitude-agnostic: it
/// is robust to the BM25-vs-cosine scale mismatch that raw score blending
/// suffered from, where an unbounded BM25 score could drown out bounded cosine
/// similarity (or vice versa). At w = 0 the order is pure BM25 rank; at w = 1
/// it is pure vector rank. The per-channel raw scores are still reported on
/// each result unchanged; only the combined score and ordering derive from
/// ranks. Results are pushed into `results` via the bounded top-k heap.
fn fuseCandidatesRrf(
    allocator: std.mem.Allocator,
    candidate_scores: *const std.AutoHashMap(interfaces.DocId, CandidateScore),
    vector_weight: f64,
    limit: usize,
    results: *std.ArrayList(interfaces.HybridSearchMatch),
) !void {
    var bm25_ranks = try buildChannelRanks(allocator, candidate_scores, .bm25);
    defer bm25_ranks.deinit();
    var vector_ranks = try buildChannelRanks(allocator, candidate_scores, .vector);
    defer vector_ranks.deinit();

    var iter = candidate_scores.iterator();
    while (iter.next()) |entry| {
        const doc_id = entry.key_ptr.*;
        var bm25_score = entry.value_ptr.bm25_score;
        var vector_score = entry.value_ptr.vector_score;

        var combined_score = (1.0 - vector_weight) * rrfContribution(&bm25_ranks, doc_id) +
            vector_weight * rrfContribution(&vector_ranks, doc_id);
        if (!std.math.isFinite(combined_score)) combined_score = 0.0;
        // Reported per-channel scores stay raw; map the -inf "missing" sentinel
        // to 0.0 for reporting only (ranking already handled absence above).
        if (!std.math.isFinite(bm25_score)) bm25_score = 0.0;
        if (!std.math.isFinite(vector_score)) vector_score = 0.0;

        try insertTopHybridMatch(allocator, results, .{
            .doc_id = doc_id,
            .bm25_score = bm25_score,
            .vector_score = vector_score,
            .combined_score = combined_score,
        }, limit);
    }
}

pub fn search(
    allocator: std.mem.Allocator,
    bm25_repository: interfaces.Bm25Repository,
    vector_repository: ?interfaces.VectorRepository,
    request: interfaces.HybridSearchRequest,
) ![]interfaces.HybridSearchMatch {
    if (request.vector_weight < 0.0 or request.vector_weight > 1.0) {
        return error.InvalidWeight;
    }
    if (request.limit == 0) return allocator.alloc(interfaces.HybridSearchMatch, 0);

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

    var bm25_candidates = try roaring.Bitmap.empty();
    defer bm25_candidates.deinit();

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
            bm25_candidates.orInPlace(bitmap);
        }
    }

    if (!bm25_candidates.isEmpty()) {
        const doc_ids = try bm25_candidates.toArray(allocator);
        defer allocator.free(doc_ids);
        for (doc_ids) |doc_id| {
            _ = try getOrPutCandidate(&candidate_scores, doc_id);
        }
    }

    if (request.vector) |vector_request| {
        if (vector_repository) |repo| {
            const vector_matches = try repo.searchNearestFn(repo.ctx, allocator, vector_request);
            defer allocator.free(vector_matches);

            for (vector_matches) |match| {
                const entry = try getOrPutCandidate(&candidate_scores, match.doc_id);
                // score_missing sentinel: negative similarities (anti-correlated
                // docs) must not be silently collapsed to a 0.0 default.
                if (entry.vector_score == score_missing or match.similarity > entry.vector_score) {
                    entry.vector_score = match.similarity;
                }
            }
        }
    }

    const candidate_doc_ids = try collectCandidateDocIds(allocator, &candidate_scores);
    defer allocator.free(candidate_doc_ids);

    const doc_stats_rows = try bm25_repository.getDocumentStatsBatchFn(
        bm25_repository.ctx,
        allocator,
        request.bm25_table_id,
        candidate_doc_ids,
    );
    defer allocator.free(doc_stats_rows);

    const term_frequency_rows = try bm25_repository.getTermFrequenciesBatchFn(
        bm25_repository.ctx,
        allocator,
        request.bm25_table_id,
        candidate_doc_ids,
        request.bm25_term_hashes,
    );
    defer allocator.free(term_frequency_rows);

    var doc_stats_by_id = std.AutoHashMap(interfaces.DocId, interfaces.DocumentStats).init(allocator);
    defer doc_stats_by_id.deinit();
    for (doc_stats_rows) |row| {
        try doc_stats_by_id.put(row.doc_id, row);
    }

    var frequencies_by_doc_term = std.AutoHashMap(u128, u32).init(allocator);
    defer frequencies_by_doc_term.deinit();
    for (term_frequency_rows) |row| {
        try frequencies_by_doc_term.put(packDocTermKey(row.doc_id, row.term_hash), row.frequency);
    }

    var candidate_iter = candidate_scores.iterator();
    while (candidate_iter.next()) |entry| {
        const doc_stats = doc_stats_by_id.get(entry.key_ptr.*) orelse continue;
        entry.value_ptr.bm25_score = calculateBm25ScoreFromMap(
            request.bm25_term_hashes,
            entry.key_ptr.*,
            &frequencies_by_doc_term,
            doc_stats,
            collection_stats,
            &term_df,
        );
    }

    var results = std.ArrayList(interfaces.HybridSearchMatch).empty;
    defer results.deinit(allocator);

    // Fuse the two channels with Reciprocal Rank Fusion (see fuseCandidatesRrf).
    try fuseCandidatesRrf(allocator, &candidate_scores, request.vector_weight, request.limit, &results);

    sortHybridMatches(results.items);

    return results.toOwnedSlice(allocator);
}

pub fn fusePreScored(
    allocator: std.mem.Allocator,
    bm25_matches: []const interfaces.DocScore,
    vector_matches: []const interfaces.VectorSearchMatch,
    vector_weight: f64,
    limit: usize,
) ![]interfaces.HybridSearchMatch {
    if (vector_weight < 0.0 or vector_weight > 1.0) return error.InvalidWeight;
    if (limit == 0) return allocator.alloc(interfaces.HybridSearchMatch, 0);

    var candidate_scores = std.AutoHashMap(interfaces.DocId, CandidateScore).init(allocator);
    defer candidate_scores.deinit();

    // Both channels use the score_missing sentinel: a candidate absent from
    // the BM25 result set is "missing", not "score 0", exactly like the
    // vector channel (previously negative BM25 scores were clamped to the
    // 0.0 default while vector used a -inf sentinel).
    for (bm25_matches) |match| {
        const entry = try getOrPutCandidate(&candidate_scores, match.doc_id);
        if (entry.bm25_score == score_missing or match.score > entry.bm25_score) entry.bm25_score = match.score;
    }

    for (vector_matches) |match| {
        const entry = try getOrPutCandidate(&candidate_scores, match.doc_id);
        if (entry.vector_score == score_missing or match.similarity > entry.vector_score) entry.vector_score = match.similarity;
    }

    var results = std.ArrayList(interfaces.HybridSearchMatch).empty;
    defer results.deinit(allocator);

    // Same Reciprocal Rank Fusion as `search` (see fuseCandidatesRrf).
    try fuseCandidatesRrf(allocator, &candidate_scores, vector_weight, limit, &results);

    sortHybridMatches(results.items);
    return results.toOwnedSlice(allocator);
}

fn collectCandidateDocIds(allocator: std.mem.Allocator, candidate_scores: *std.AutoHashMap(interfaces.DocId, CandidateScore)) ![]interfaces.DocId {
    var doc_ids = try std.ArrayList(interfaces.DocId).initCapacity(allocator, candidate_scores.count());
    defer doc_ids.deinit(allocator);
    var it = candidate_scores.iterator();
    while (it.next()) |entry| {
        try doc_ids.append(allocator, entry.key_ptr.*);
    }
    return doc_ids.toOwnedSlice(allocator);
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

fn calculateBm25ScoreFromMap(
    term_hashes: []const u64,
    doc_id: interfaces.DocId,
    frequencies_by_doc_term: *const std.AutoHashMap(u128, u32),
    doc_stats: interfaces.DocumentStats,
    collection_stats: interfaces.CollectionStats,
    term_df: *const std.AutoHashMap(u64, u64),
) f64 {
    if (collection_stats.total_documents <= 0) return 0.0;

    var avgdl: f64 = collection_stats.avg_document_length;
    if (!(avgdl > 0.0) or std.math.isNan(avgdl) or std.math.isInf(avgdl)) avgdl = 1.0;

    const k1 = 1.2;
    const b = 0.75;
    const doc_len = @as(f64, @floatFromInt(doc_stats.document_length));
    const total_docs = @as(f64, @floatFromInt(collection_stats.total_documents));

    var score: f64 = 0.0;
    for (term_hashes) |term_hash| {
        const tf = frequencies_by_doc_term.get(packDocTermKey(doc_id, term_hash)) orelse 0;
        if (tf == 0) continue;

        const df = term_df.get(term_hash) orelse 0;
        if (df == 0) continue;

        const idf = @log((total_docs + 1.0) / (@as(f64, @floatFromInt(df)) + 0.5));
        const tf_f = @as(f64, @floatFromInt(tf));
        const numerator = tf_f * (k1 + 1.0);
        const denominator = tf_f + k1 * (1.0 - b + b * (doc_len / avgdl));
        score += idf * (numerator / denominator);
    }

    if (!std.math.isFinite(score)) return 0.0;
    return score;
}

fn packDocTermKey(doc_id: interfaces.DocId, term_hash: u64) u128 {
    return (@as(u128, doc_id) << 64) | @as(u128, term_hash);
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

fn hybridBetter(lhs: interfaces.HybridSearchMatch, rhs: interfaces.HybridSearchMatch) bool {
    if (lhs.combined_score != rhs.combined_score) return lhs.combined_score > rhs.combined_score;
    return lhs.doc_id < rhs.doc_id;
}

fn hybridWorse(lhs: interfaces.HybridSearchMatch, rhs: interfaces.HybridSearchMatch) bool {
    return hybridBetter(rhs, lhs);
}

fn insertTopHybridMatch(
    allocator: std.mem.Allocator,
    matches: *std.ArrayList(interfaces.HybridSearchMatch),
    candidate: interfaces.HybridSearchMatch,
    limit: usize,
) !void {
    if (limit == 0) return;
    if (matches.items.len < limit) {
        try matches.append(allocator, candidate);
        siftUpWorstHybrid(matches.items, matches.items.len - 1);
    } else if (hybridBetter(candidate, matches.items[0])) {
        matches.items[0] = candidate;
        siftDownWorstHybrid(matches.items, 0);
    }
}

fn siftUpWorstHybrid(items: []interfaces.HybridSearchMatch, start_index: usize) void {
    var index = start_index;
    while (index > 0) {
        const parent = (index - 1) / 2;
        if (!hybridWorse(items[index], items[parent])) break;
        std.mem.swap(interfaces.HybridSearchMatch, &items[index], &items[parent]);
        index = parent;
    }
}

fn siftDownWorstHybrid(items: []interfaces.HybridSearchMatch, start_index: usize) void {
    var index = start_index;
    while (true) {
        const left = index * 2 + 1;
        const right = left + 1;
        var worst = index;
        if (left < items.len and hybridWorse(items[left], items[worst])) worst = left;
        if (right < items.len and hybridWorse(items[right], items[worst])) worst = right;
        if (worst == index) break;
        std.mem.swap(interfaces.HybridSearchMatch, &items[index], &items[worst]);
        index = worst;
    }
}

fn sortHybridMatches(items: []interfaces.HybridSearchMatch) void {
    std.mem.sort(interfaces.HybridSearchMatch, items, {}, struct {
        fn lessThan(_: void, a: interfaces.HybridSearchMatch, b: interfaces.HybridSearchMatch) bool {
            return hybridBetter(a, b);
        }
    }.lessThan);
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

    // RRF contract: each channel is ranked independently (rank 1 = best) and
    // fused as (1-w)*1/(K+rank_bm25) + w*1/(K+rank_vector), K = 60. BM25 ranks
    // over {1,2,3}: 1,3,2; vector ranks over {2,1,4}: 2,1,4; docs absent from a
    // channel contribute 0. At w = 0.85 the vector channel dominates, so doc 4
    // (vector rank 3, no BM25) outranks doc 3 (BM25 rank 2, no vector) — the
    // reverse of the old min-max order, because RRF is magnitude-agnostic.
    try std.testing.expectEqual(@as(usize, 4), results.len);
    try std.testing.expectEqual(@as(interfaces.DocId, 2), results[0].doc_id);
    try std.testing.expectEqual(@as(interfaces.DocId, 1), results[1].doc_id);
    try std.testing.expectEqual(@as(interfaces.DocId, 4), results[2].doc_id);
    try std.testing.expectEqual(@as(interfaces.DocId, 3), results[3].doc_id);
    try std.testing.expect(results[0].vector_score > results[1].vector_score);
    // doc 1 carries a BM25 score; doc 4 has none (reported as 0.0).
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

test "fuse pre-scored matches is bounded and deterministic" {
    const bm25_matches = [_]interfaces.DocScore{
        .{ .doc_id = 10, .score = 0.5 },
        .{ .doc_id = 20, .score = 0.5 },
        .{ .doc_id = 30, .score = 0.1 },
    };
    const vector_matches = [_]interfaces.VectorSearchMatch{
        .{ .doc_id = 20, .distance = 0.0, .similarity = 0.5 },
        .{ .doc_id = 10, .distance = 0.0, .similarity = 0.5 },
        .{ .doc_id = 40, .distance = 0.0, .similarity = 0.2 },
    };

    const results = try fusePreScored(
        std.testing.allocator,
        &bm25_matches,
        &vector_matches,
        0.5,
        2,
    );
    defer std.testing.allocator.free(results);

    // RRF contract: within each channel the (0.5, 0.5) tie between docs 10 and
    // 20 breaks on doc_id ascending, so doc 10 takes rank 1 and doc 20 rank 2
    // in both channels. combined = 0.5/(K+r_bm25) + 0.5/(K+r_vec) with K = 60:
    // doc 10 = 1/61, doc 20 = 1/62. Order is deterministic and 10 leads.
    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqual(@as(interfaces.DocId, 10), results[0].doc_id);
    try std.testing.expectEqual(@as(interfaces.DocId, 20), results[1].doc_id);
    try std.testing.expectApproxEqAbs(1.0 / 61.0, results[0].combined_score, 1e-9);
    try std.testing.expectApproxEqAbs(1.0 / 62.0, results[1].combined_score, 1e-9);
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
            .getDocumentStatsBatchFn = bm25GetDocumentStatsBatch,
            .getTermStatsFn = bm25GetTermStats,
            .getTermFrequenciesFn = bm25GetTermFrequencies,
            .getTermFrequenciesBatchFn = bm25GetTermFrequenciesBatch,
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

fn bm25GetDocumentStatsBatch(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_ids: []const interfaces.DocId) anyerror![]interfaces.DocumentStats {
    var rows = std.ArrayList(interfaces.DocumentStats).empty;
    defer rows.deinit(allocator);
    for (doc_ids) |doc_id| {
        if (try bm25GetDocumentStats(ctx, allocator, table_id, doc_id)) |stats| {
            try rows.append(allocator, stats);
        }
    }
    return rows.toOwnedSlice(allocator);
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

fn bm25GetTermFrequenciesBatch(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_ids: []const interfaces.DocId, term_hashes: []const u64) anyerror![]interfaces.DocumentTermFrequency {
    var rows = std.ArrayList(interfaces.DocumentTermFrequency).empty;
    defer rows.deinit(allocator);
    for (doc_ids) |doc_id| {
        const freqs = try bm25GetTermFrequencies(ctx, allocator, table_id, doc_id, term_hashes);
        defer allocator.free(freqs);
        for (freqs) |freq| {
            try rows.append(allocator, .{
                .doc_id = doc_id,
                .term_hash = freq.term_hash,
                .frequency = freq.frequency,
            });
        }
    }
    return rows.toOwnedSlice(allocator);
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
