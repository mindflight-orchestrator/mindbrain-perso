const std = @import("std");
const utils = @import("../utils.zig");
const c = utils.c;
const tokenizer = @import("tokenizer.zig");
const tokenizer_pure = @import("tokenizer_pure.zig");
const scoring = @import("scoring.zig");
const roaring_index = @import("roaring_index.zig");
const stopwords = @import("stopwords.zig");

/// Search options
pub const SearchOptions = struct {
    prefix_match: bool = false,
    fuzzy_match: bool = false,
    fuzzy_threshold: f64 = 0.3,
    k1: f64 = 1.2,
    b: f64 = 0.75,
};

// NOTE: the former `pub fn search()` (and its helpers tokenizeQuery,
// expandQueryTerms, calculateScores, calculateDocumentScore, rankResults,
// SearchResult) was removed: it had no callers left (main.zig routes ranked
// search through search_native.searchNative) and scored documents with one
// SPI SELECT per (document, term) pair, i.e. O(docs x terms) round trips.

/// Get roaring bitmap of documents matching query
/// If SPI is already connected, use getMatchesBitmapWithExistingConnection instead
pub fn getMatchesBitmap(table_id: c.Oid, query_text: []const u8, config_name: []const u8, options: SearchOptions, allocator: std.mem.Allocator) !?*c.roaring_bitmap_t {
    utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: Starting, table_id={d}, query_len={d}, config={s}", .{ table_id, query_text.len, config_name });

    // Connect to SPI first, then tokenize to avoid nested connections
    utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: Connecting to SPI", .{});
    const conn_result = c.SPI_connect();
    const need_finish = (conn_result == c.SPI_OK_CONNECT);
    utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: SPI_connect result={d}, need_finish={}", .{ conn_result, need_finish });
    if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
        utils.elog(c.ERROR, "[TRACE] getMatchesBitmap: SPI_connect failed unexpectedly");
        return error.SPIConnectFailed;
    }
    defer if (need_finish) {
        utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: About to call SPI_finish (need_finish={})", .{need_finish});
        _ = c.SPI_finish();
        utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: SPI_finish completed", .{});
    };

    // Phase 1: Tokenize query (assumes SPI is already connected)
    utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: Phase 1 - Tokenizing query", .{});
    var query_tokens = try tokenizer.tokenizeWithExistingConnection(query_text, config_name, allocator);
    defer {
        for (query_tokens.items) |token| {
            allocator.free(token);
        }
        query_tokens.deinit(allocator);
    }
    utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: Phase 1 - Tokenized, got {d} tokens", .{query_tokens.items.len});

    if (query_tokens.items.len == 0) {
        utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: No tokens, returning null", .{});
        return null;
    }

    var custom_stopwords = try stopwords.loadWithExistingConnection(config_name, allocator);
    defer custom_stopwords.deinit();

    // Phase 2: Expand query terms and get document sets
    const MAX_QUERY_HASHES = 64;
    var expanded_hashes_arr: [MAX_QUERY_HASHES]i64 = undefined;
    var expanded_hashes_count: usize = 0;

    var combined_bitmap: ?*c.roaring_bitmap_t = null;

    // Expand query terms to hashes. Deduplicate: repeated query terms (or
    // prefix/fuzzy expansions converging on the same term) would otherwise
    // burn MAX_QUERY_HASHES slots and re-fetch the same posting bitmap.
    utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: Phase 2 - Expanding query terms to hashes", .{});
    var truncated = false;
    expand: for (query_tokens.items) |query_term| {
        if (custom_stopwords.contains(query_term)) continue;

        utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: Processing token: {s}", .{query_term});
        var matching_hashes = try findMatchingTermHashesInternal(table_id, query_term, options, allocator);
        defer matching_hashes.deinit(allocator);

        for (matching_hashes.items) |hash| {
            if (containsHash(expanded_hashes_arr[0..expanded_hashes_count], hash)) continue;
            if (expanded_hashes_count >= MAX_QUERY_HASHES) {
                truncated = true;
                break :expand;
            }
            expanded_hashes_arr[expanded_hashes_count] = hash;
            expanded_hashes_count += 1;
        }
    }
    if (truncated) {
        // Previously silent: extra terms simply vanished from the match set.
        utils.elogFmt(c.WARNING, "getMatchesBitmap: query expansion truncated at {d} distinct term hashes; remaining terms ignored", .{MAX_QUERY_HASHES});
    }
    utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: Phase 2 - Expanded to {d} hashes", .{expanded_hashes_count});

    if (expanded_hashes_count == 0) {
        utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: No matching hashes, returning null", .{});
        return null;
    }

    // Get document sets for all term hashes and combine (OR operation)
    // IMPORTANT: Do this while SPI is still connected, then copy the result
    utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: Phase 3 - Getting document sets for {d} term hashes", .{expanded_hashes_count});
    for (expanded_hashes_arr[0..expanded_hashes_count], 0..) |term_hash, idx| {
        const i = idx + 1;
        utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: Phase 3.{d} - Getting doc_set for hash={d}", .{ i, term_hash });
        const doc_set = try getDocumentSetByHashInternal(table_id, term_hash, allocator);

        if (doc_set) |ds| {
            defer {
                utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: Phase 3.{d} - Freeing doc_set={*}", .{ i, ds });
                roaring_index.free(ds);
            }
            utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: Phase 3.{d} - Got doc_set={*}, cardinality={d}", .{ i, ds, roaring_index.cardinality(ds) });
            if (combined_bitmap) |combined| {
                utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: Phase 3.{d} - ORing with existing bitmap", .{i});
                roaring_index.orInPlace(combined, ds);
            } else {
                utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: Phase 3.{d} - Copying first bitmap", .{i});
                combined_bitmap = roaring_index.copy(ds);
                utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: Phase 3.{d} - Copied bitmap={*}, cardinality={d}", .{ i, combined_bitmap.?, roaring_index.cardinality(combined_bitmap.?) });
            }
        } else {
            utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: Phase 3.{d} - No doc_set for hash={d}", .{ i, term_hash });
        }
    }

    // SPI_finish will be called here via defer, but combined_bitmap is already copied
    // and allocated with malloc, so it will persist after SPI_finish
    if (combined_bitmap) |bm| {
        utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: Returning bitmap={*}, cardinality={d}", .{ bm, roaring_index.cardinality(bm) });
    } else {
        utils.elogFmt(c.DEBUG3, "[TRACE] getMatchesBitmap: Returning null (no bitmap)", .{});
    }
    return combined_bitmap;
}

fn containsHash(hashes: []const i64, candidate: i64) bool {
    for (hashes) |hash| {
        if (hash == candidate) return true;
    }
    return false;
}

/// Internal version of findMatchingTermHashes (assumes SPI is connected)
fn findMatchingTermHashesInternal(table_id: c.Oid, query_term: []const u8, options: SearchOptions, allocator: std.mem.Allocator) !std.ArrayList(i64) {
    var matching_hashes = std.ArrayList(i64).empty;

    if (options.fuzzy_match) {
        const query = try std.fmt.allocPrintSentinel(allocator, "SELECT term_hash FROM facets.bm25_index WHERE table_id = {d} AND term_text % $1 AND similarity(term_text, $1) >= {d}", .{ table_id, options.fuzzy_threshold }, 0);
        defer allocator.free(query);

        var argtypes = [_]c.Oid{c.TEXTOID};
        const term_datum = c.PointerGetDatum(c.cstring_to_text_with_len(query_term.ptr, @intCast(query_term.len)));
        var argvalues = [_]c.Datum{term_datum};
        var argnulls = [_]u8{' '};

        const ret = c.SPI_execute_with_args(query.ptr, 1, &argtypes, &argvalues, &argnulls, true, 0);
        if (ret == c.SPI_OK_SELECT) {
            var i: u64 = 0;
            while (i < c.SPI_processed) : (i += 1) {
                const tuple = c.SPI_tuptable.*.vals[@intCast(i)];
                const tupdesc = c.SPI_tuptable.*.tupdesc;
                var isnull: bool = false;
                const hash_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);
                if (!isnull) {
                    try matching_hashes.append(allocator, c.DatumGetInt64(hash_datum));
                }
            }
        }
    } else if (options.prefix_match) {
        const query = try std.fmt.allocPrintSentinel(allocator, "SELECT term_hash FROM facets.bm25_index WHERE table_id = {d} AND term_text LIKE $1 || '%'", .{table_id}, 0);
        defer allocator.free(query);

        var argtypes = [_]c.Oid{c.TEXTOID};
        const term_datum = c.PointerGetDatum(c.cstring_to_text_with_len(query_term.ptr, @intCast(query_term.len)));
        var argvalues = [_]c.Datum{term_datum};
        var argnulls = [_]u8{' '};

        const ret = c.SPI_execute_with_args(query.ptr, 1, &argtypes, &argvalues, &argnulls, true, 0);
        if (ret == c.SPI_OK_SELECT) {
            var i: u64 = 0;
            while (i < c.SPI_processed) : (i += 1) {
                const tuple = c.SPI_tuptable.*.vals[@intCast(i)];
                const tupdesc = c.SPI_tuptable.*.tupdesc;
                var isnull: bool = false;
                const hash_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);
                if (!isnull) {
                    try matching_hashes.append(allocator, c.DatumGetInt64(hash_datum));
                }
            }
        }
    } else {
        // Exact match: hash the query term directly
        try matching_hashes.append(allocator, tokenizer.hashLexeme(query_term));
    }

    return matching_hashes;
}

/// Internal version of getDocumentSetByHash (assumes SPI is connected)
fn getDocumentSetByHashInternal(table_id: c.Oid, term_hash: i64, allocator: std.mem.Allocator) !?*c.roaring_bitmap_t {
    utils.elogFmt(c.DEBUG3, "[TRACE] getDocumentSetByHashInternal: Starting, table_id={d}, term_hash={d}", .{ table_id, term_hash });

    const query = try std.fmt.allocPrintSentinel(allocator, "SELECT doc_ids FROM facets.bm25_index WHERE table_id = {d} AND term_hash = {d}", .{ table_id, term_hash }, 0);
    defer allocator.free(query);

    utils.elogFmt(c.DEBUG3, "[TRACE] getDocumentSetByHashInternal: Executing query: {s}", .{query});
    const ret = c.SPI_execute(query.ptr, true, 1);
    utils.elogFmt(c.DEBUG3, "[TRACE] getDocumentSetByHashInternal: SPI_execute result={d}, processed={d}", .{ ret, c.SPI_processed });
    if (ret != c.SPI_OK_SELECT or c.SPI_processed == 0) {
        utils.elogFmt(c.DEBUG3, "[TRACE] getDocumentSetByHashInternal: No results, returning null", .{});
        return null;
    }

    utils.elogFmt(c.DEBUG3, "[TRACE] getDocumentSetByHashInternal: Got {d} rows, getting tuple", .{c.SPI_processed});
    const tuple = c.SPI_tuptable.*.vals[0];
    const tupdesc = c.SPI_tuptable.*.tupdesc;

    var isnull: bool = false;
    utils.elogFmt(c.DEBUG3, "[TRACE] getDocumentSetByHashInternal: Getting doc_ids_datum from tuple", .{});
    const doc_ids_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);
    utils.elogFmt(c.DEBUG3, "[TRACE] getDocumentSetByHashInternal: Got doc_ids_datum, isnull={}", .{isnull});

    if (isnull) {
        utils.elogFmt(c.DEBUG3, "[TRACE] getDocumentSetByHashInternal: doc_ids_datum is null, returning null", .{});
        return null;
    }

    utils.elogFmt(c.DEBUG3, "[TRACE] getDocumentSetByHashInternal: Calling datumToRoaringBitmap", .{});
    const bitmap = try roaring_index.datumToRoaringBitmap(doc_ids_datum);
    utils.elogFmt(c.DEBUG3, "[TRACE] getDocumentSetByHashInternal: datumToRoaringBitmap returned bitmap={*}, cardinality={d}", .{ bitmap, roaring_index.cardinality(bitmap) });

    return bitmap;
}

/// Calculate BM25 score for a single document
/// IMPORTANT: This function uses a PURE ZIG tokenizer to avoid SPI issues when called
/// from within PL/pgSQL EXECUTE statements. The pure tokenizer does simple word splitting
/// without PostgreSQL's stemming, so results may differ slightly from to_tsvector.
pub fn calculateScore(table_id: c.Oid, query_text: []const u8, doc_id: i64, config_name: []const u8, options: SearchOptions, allocator: std.mem.Allocator) !f64 {
    _ = config_name; // Not used with pure tokenizer

    utils.elogFmt(c.DEBUG3, "[TRACE] calculateScore: Starting for doc_id={d}", .{doc_id});

    // Phase 1: Tokenize query using PURE ZIG tokenizer (NO SPI!)
    // This is critical to avoid crashes when called from within EXECUTE statements
    var query_tokens = try tokenizer_pure.tokenizePure(query_text, allocator);
    defer {
        for (query_tokens.items) |token| {
            allocator.free(token);
        }
        query_tokens.deinit(allocator);
    }

    utils.elogFmt(c.DEBUG3, "[TRACE] calculateScore: Tokenized query into {d} tokens", .{query_tokens.items.len});

    if (query_tokens.items.len == 0) {
        return 0.0;
    }

    // Convert query tokens to hashes (no SPI needed)
    var query_hashes = std.ArrayList(i64).empty;
    defer query_hashes.deinit(allocator);

    for (query_tokens.items) |term| {
        try query_hashes.append(allocator, tokenizer_pure.hashLexeme(term));
    }

    utils.elogFmt(c.DEBUG3, "[TRACE] calculateScore: Generated {d} hashes", .{query_hashes.items.len});

    // Phase 2: Load statistics (uses SPI but handles connection safely)
    utils.elogFmt(c.DEBUG3, "[TRACE] calculateScore: Loading statistics", .{});
    var stats = try scoring.loadStatisticsForTerms(table_id, query_hashes.items, allocator);
    defer stats.deinit();
    utils.elogFmt(c.DEBUG3, "[TRACE] calculateScore: Statistics loaded, total_docs={d}", .{stats.total_documents});

    // Phase 3: Get term frequencies and document length (single SPI connection)
    var term_freqs = std.AutoHashMap(i64, i32).init(allocator);
    defer term_freqs.deinit();
    var doc_length: i32 = 0;

    // Pre-allocate term_freqs
    try term_freqs.ensureTotalCapacity(@intCast(query_hashes.items.len));

    {
        utils.elogFmt(c.DEBUG3, "[TRACE] calculateScore: Phase 3 - SPI_connect", .{});
        // Try to connect - may already be connected from caller
        const conn_result3 = c.SPI_connect();
        const need_finish3 = (conn_result3 == c.SPI_OK_CONNECT);
        utils.elogFmt(c.DEBUG3, "[TRACE] calculateScore: SPI_connect result={d}, need_finish={}", .{ conn_result3, need_finish3 });
        if (conn_result3 != c.SPI_OK_CONNECT and conn_result3 != c.SPI_ERROR_CONNECT) {
            utils.elog(c.ERROR, "SPI_connect failed");
            return error.SPIConnectFailed;
        }
        defer if (need_finish3) {
            utils.elogFmt(c.DEBUG3, "[TRACE] calculateScore: Calling SPI_finish", .{});
            _ = c.SPI_finish();
        };

        // Get term frequencies for this document in one statement (was one
        // SELECT per query term).
        utils.elogFmt(c.DEBUG3, "[TRACE] calculateScore: Getting term frequencies", .{});
        try loadTermFrequenciesForDocInternal(table_id, doc_id, query_hashes.items, &term_freqs, allocator);

        // Get document length
        utils.elogFmt(c.DEBUG3, "[TRACE] calculateScore: Getting document length", .{});
        doc_length = try getDocumentLengthInternal(table_id, doc_id, allocator);
        utils.elogFmt(c.DEBUG3, "[TRACE] calculateScore: doc_length={d}", .{doc_length});
    }

    if (term_freqs.count() == 0) {
        utils.elogFmt(c.DEBUG3, "[TRACE] calculateScore: No matching terms, returning 0", .{});
        return 0.0;
    }

    // Phase 4: Calculate BM25 score (no SPI needed)
    utils.elogFmt(c.DEBUG3, "[TRACE] calculateScore: Calculating BM25 score", .{});
    return scoring.calculateBM25ByHash(query_hashes.items, term_freqs, doc_length, &stats, options.k1, options.b);
}

/// Loads the frequencies of `term_hashes` for one document in a single
/// SELECT ... IN (...) statement (assumes SPI is connected). The map must be
/// pre-sized to at least `term_hashes.len` entries; the row count is bounded
/// by the number of distinct hashes.
fn loadTermFrequenciesForDocInternal(
    table_id: c.Oid,
    doc_id: i64,
    term_hashes: []const i64,
    term_freqs: *std.AutoHashMap(i64, i32),
    allocator: std.mem.Allocator,
) !void {
    if (term_hashes.len == 0) return;

    var in_list = std.ArrayList(u8).empty;
    defer in_list.deinit(allocator);
    for (term_hashes, 0..) |hash, index| {
        if (index != 0) try in_list.append(allocator, ',');
        var num_buf: [24]u8 = undefined;
        const text = std.fmt.bufPrint(&num_buf, "{d}", .{hash}) catch unreachable;
        try in_list.appendSlice(allocator, text);
    }

    const query = try std.fmt.allocPrintSentinel(
        allocator,
        "SELECT term_hash, frequency FROM facets.bm25_term_frequencies WHERE table_id = {d} AND doc_id = {d} AND term_hash IN ({s})",
        .{ table_id, doc_id, in_list.items },
        0,
    );
    defer allocator.free(query);

    const ret = c.SPI_execute(query.ptr, true, 0);
    if (ret != c.SPI_OK_SELECT or c.SPI_tuptable == null) return;

    var i: u64 = 0;
    while (i < c.SPI_processed) : (i += 1) {
        const tuple = c.SPI_tuptable.*.vals[@intCast(i)];
        const tupdesc = c.SPI_tuptable.*.tupdesc;

        var isnull_hash: bool = false;
        var isnull_freq: bool = false;
        const hash_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull_hash);
        const freq_datum = c.SPI_getbinval(tuple, tupdesc, 2, &isnull_freq);
        if (isnull_hash or isnull_freq) continue;

        const freq: i32 = @intCast(c.DatumGetInt32(freq_datum));
        if (freq > 0) {
            term_freqs.putAssumeCapacity(c.DatumGetInt64(hash_datum), freq);
        }
    }
}

/// Internal version of getDocumentLength (assumes SPI is connected)
fn getDocumentLengthInternal(table_id: c.Oid, doc_id: i64, allocator: std.mem.Allocator) !i32 {
    _ = allocator; // Not needed - use stack buffer

    // Use stack-allocated buffer to avoid palloc issues during SPI
    var query_buf: [256]u8 = undefined;
    const query = std.fmt.bufPrintZ(&query_buf, "SELECT doc_length FROM facets.bm25_documents WHERE table_id = {d} AND doc_id = {d}", .{ table_id, doc_id }) catch {
        utils.elog(c.ERROR, "Query buffer too small");
        return 0;
    };

    const ret = c.SPI_execute(query.ptr, true, 1);
    if (ret != c.SPI_OK_SELECT or c.SPI_processed == 0) {
        return 0;
    }

    const tuple = c.SPI_tuptable.*.vals[0];
    const tupdesc = c.SPI_tuptable.*.tupdesc;

    var isnull: bool = false;
    const length_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);

    if (isnull) {
        return 0;
    }

    return @intCast(c.DatumGetInt32(length_datum));
}
