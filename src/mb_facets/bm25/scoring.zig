const std = @import("std");
const utils = @import("../utils.zig");
const c = utils.c;
const tokenizer = @import("tokenizer.zig");

/// Collection statistics for BM25 calculation
/// Uses term_hash (i64) as key to avoid string memory management issues
pub const CollectionStats = struct {
    total_documents: i64,      // N
    avg_document_length: f64,  // avgdl
    doc_frequencies: std.AutoHashMap(i64, i64), // term_hash -> number of documents containing it
    
    pub fn init(allocator: std.mem.Allocator) CollectionStats {
        return CollectionStats{
            .total_documents = 0,
            .avg_document_length = 0.0,
            .doc_frequencies = std.AutoHashMap(i64, i64).init(allocator),
        };
    }
    
    pub fn deinit(self: *CollectionStats) void {
        // AutoHashMap with i64 keys doesn't need special cleanup
        // PostgreSQL will clean up the memory context
        _ = self;
    }
};

/// Calculate IDF (Inverse Document Frequency) for a term using hash
/// Uses BM25+ variant: IDF(qi) = log((N + 1) / (n(qi) + 0.5))
/// This ensures IDF is always positive (unlike the original Robertson-Sparck Jones formula)
pub fn calculateIDFByHash(
    term_hash: i64,
    stats: *CollectionStats
) f64 {
    const n_qi = stats.doc_frequencies.get(term_hash) orelse 0;
    const N = @as(f64, @floatFromInt(stats.total_documents));
    const n = @as(f64, @floatFromInt(n_qi));
    
    if (n == 0) {
        // Term not found in any document - return 0
        return 0.0;
    }
    
    // BM25+ formula ensures positive IDF even for common terms
    const numerator = N + 1.0;
    const denominator = n + 0.5;
    const ratio = numerator / denominator;
    
    return @log(ratio);
}

/// Calculate BM25 score for a document given query term hashes
/// BM25(q, d) = Σ IDF(qi) × (f(qi, d) × (k1 + 1)) / (f(qi, d) + k1 × (1 - b + b × |d| / avgdl))
pub fn calculateBM25ByHash(
    query_term_hashes: []const i64,
    term_frequencies: std.AutoHashMap(i64, i32), // term_hash -> frequency in this document
    doc_length: i32,
    stats: *CollectionStats,
    k1: f64,
    b: f64
) f64 {
    var score: f64 = 0.0;
    // Guard against a missing/empty statistics row: avgdl == 0 turns the
    // denominator into inf (score 0) and doc_len == 0 into 0/0 == NaN.
    var avgdl = stats.avg_document_length;
    if (!(avgdl > 0.0)) avgdl = 1.0;
    const doc_len = @as(f64, @floatFromInt(doc_length));
    
    for (query_term_hashes) |term_hash| {
        // Get IDF
        const idf = calculateIDFByHash(term_hash, stats);
        if (idf == 0.0) continue; // Skip terms not in collection
        
        // Get term frequency in document
        const tf = @as(f64, @floatFromInt(term_frequencies.get(term_hash) orelse 0));
        if (tf == 0.0) continue; // Skip terms not in document
        
        // Calculate BM25 component
        const numerator = tf * (k1 + 1.0);
        const denominator = tf + k1 * (1.0 - b + b * (doc_len / avgdl));
        
        score += idf * (numerator / denominator);
    }
    
    return score;
}

/// Hash a term string to i64 (same algorithm as tokenizer)
pub fn hashTerm(term: []const u8) i64 {
    return tokenizer.hashLexeme(term);
}

/// Load collection statistics from database
/// This version avoids all dynamic allocations during SPI operations
pub fn loadStatistics(
    table_id: c.Oid,
    allocator: std.mem.Allocator,
) !CollectionStats {
    return loadStatisticsForTerms(table_id, null, allocator);
}

/// Scoring only ever looks up the query's own term hashes; loading the
/// document frequency of the entire vocabulary materialized every posting
/// bitmap's cardinality on every search/score call.
pub fn loadStatisticsForTerms(
    table_id: c.Oid,
    term_hashes: ?[]const i64,
    allocator: std.mem.Allocator,
) !CollectionStats {
    var stats = CollectionStats.init(allocator);

    // Pre-size the map BEFORE any SPI session: allocations made while SPI is
    // connected can land in the SPI memory context and vanish at SPI_finish.
    // With an explicit term list the df row count is bounded by the number of
    // query terms, which removes the COUNT(*) pre-pass (and the SPI
    // finish/reconnect dance) that previously ran on every score/search call.
    if (term_hashes) |hashes| {
        if (hashes.len > 0) {
            try stats.doc_frequencies.ensureTotalCapacity(@intCast(hashes.len));
        }
    }

    // Try to connect - may already be connected from caller
    const conn_result = c.SPI_connect();
    const need_finish = (conn_result == c.SPI_OK_CONNECT);
    if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
        utils.elog(c.ERROR, "SPI_connect failed");
        return error.SPIConnectFailed;
    }
    
    // Load basic statistics
    const query = try std.fmt.allocPrintSentinel(
        allocator,
        "SELECT total_documents, avg_document_length FROM facets.bm25_statistics WHERE table_id = {d}",
        .{table_id}, 0);
    defer allocator.free(query);
    
    const ret = c.SPI_execute(query.ptr, true, 1);
    if (ret != c.SPI_OK_SELECT) {
        if (need_finish) {
            _ = c.SPI_finish();
        }
        utils.elog(c.ERROR, "Failed to load statistics");
        return error.QueryFailed;
    }
    
    if (c.SPI_processed > 0) {
        const tuple = c.SPI_tuptable.*.vals[0];
        const tupdesc = c.SPI_tuptable.*.tupdesc;
        
        var isnull1: bool = false;
        var isnull2: bool = false;
        const total_docs_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull1);
        const avg_len_datum = c.SPI_getbinval(tuple, tupdesc, 2, &isnull2);
        
        if (!isnull1) {
            stats.total_documents = c.DatumGetInt64(total_docs_datum);
        }
        if (!isnull2) {
            stats.avg_document_length = c.DatumGetFloat8(avg_len_datum);
        }
    }
    
    // Build the optional term filter for the df query.
    var term_filter_buf = std.ArrayList(u8).empty;
    defer term_filter_buf.deinit(allocator);
    if (term_hashes) |hashes| {
        try term_filter_buf.appendSlice(allocator, " AND term_hash IN (");
        for (hashes, 0..) |hash, index| {
            if (index != 0) try term_filter_buf.append(allocator, ',');
            var num_buf: [24]u8 = undefined;
            const text = try std.fmt.bufPrint(&num_buf, "{d}", .{hash});
            try term_filter_buf.appendSlice(allocator, text);
        }
        try term_filter_buf.append(allocator, ')');
    }

    if (term_hashes) |hashes| {
        // Term-scoped path: the map is already sized (see above), so the
        // document frequencies load in the same SPI session.
        if (hashes.len > 0) {
            const freq_query = try std.fmt.allocPrintSentinel(
                allocator,
                "SELECT term_hash, rb_cardinality(doc_ids)::bigint AS doc_count FROM facets.bm25_index WHERE table_id = {d}{s}",
                .{ table_id, term_filter_buf.items }, 0);
            defer allocator.free(freq_query);

            const freq_ret = c.SPI_execute(freq_query.ptr, true, 0);
            if (freq_ret == c.SPI_OK_SELECT) {
                fillDocFrequenciesFromSpi(&stats);
            }
        }
        if (need_finish) {
            _ = c.SPI_finish();
        }
        return stats;
    }

    // Full-vocabulary path (loadStatistics): count first so the map can be
    // sized outside the SPI session, then reconnect and fill.
    const count_query = try std.fmt.allocPrintSentinel(
        allocator,
        "SELECT COUNT(*) FROM facets.bm25_index WHERE table_id = {d}",
        .{table_id}, 0);
    defer allocator.free(count_query);

    const count_ret = c.SPI_execute(count_query.ptr, true, 1);
    var num_terms: u64 = 0;
    if (count_ret == c.SPI_OK_SELECT and c.SPI_processed > 0) {
        var isnull: bool = false;
        const count_datum = c.SPI_getbinval(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 1, &isnull);
        if (!isnull) {
            num_terms = @intCast(c.DatumGetInt64(count_datum));
        }
    }

    // Finish first SPI session if we created it
    if (need_finish) {
        _ = c.SPI_finish();
    }

    if (num_terms == 0) {
        return stats;
    }

    // Pre-allocate the HashMap with expected capacity
    try stats.doc_frequencies.ensureTotalCapacity(@intCast(num_terms));

    // Re-connect to SPI to fetch the actual data
    const conn_result2 = c.SPI_connect();
    const need_finish2 = (conn_result2 == c.SPI_OK_CONNECT);
    if (conn_result2 != c.SPI_OK_CONNECT and conn_result2 != c.SPI_ERROR_CONNECT) {
        utils.elog(c.ERROR, "SPI_connect failed");
        return error.SPIConnectFailed;
    }
    defer if (need_finish2) {
        _ = c.SPI_finish();
    };

    // Load document frequencies
    const freq_query = try std.fmt.allocPrintSentinel(
        allocator,
        "SELECT term_hash, rb_cardinality(doc_ids)::bigint AS doc_count FROM facets.bm25_index WHERE table_id = {d}",
        .{table_id}, 0);
    defer allocator.free(freq_query);

    const freq_ret = c.SPI_execute(freq_query.ptr, true, 0);
    if (freq_ret == c.SPI_OK_SELECT) {
        fillDocFrequenciesFromSpi(&stats);
    }

    return stats;
}

/// Copies (term_hash, doc_count) rows of the just-executed SPI SELECT into
/// stats.doc_frequencies. The map must already have capacity for every row
/// (putAssumeCapacity: no allocation may happen inside an SPI session).
fn fillDocFrequenciesFromSpi(stats: *CollectionStats) void {
    if (c.SPI_tuptable == null) return;
    var i: u64 = 0;
    while (i < c.SPI_processed) : (i += 1) {
        const tuple = c.SPI_tuptable.*.vals[@intCast(i)];
        const tupdesc = c.SPI_tuptable.*.tupdesc;

        var isnull_hash: bool = false;
        var isnull_count: bool = false;
        const hash_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull_hash);
        const count_datum = c.SPI_getbinval(tuple, tupdesc, 2, &isnull_count);

        if (!isnull_hash and !isnull_count) {
            stats.doc_frequencies.putAssumeCapacity(c.DatumGetInt64(hash_datum), c.DatumGetInt64(count_datum));
        }
    }
}
