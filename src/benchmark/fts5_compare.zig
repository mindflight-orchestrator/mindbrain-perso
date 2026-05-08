const std = @import("std");
const mindbrain = @import("mindbrain");
const compat = @import("zig16_compat.zig");

const facet_sqlite = mindbrain.facet_sqlite;
const search_sqlite = mindbrain.search_sqlite;

const c = facet_sqlite.c;

const table_id: u64 = 1;
const language = "english";

pub const Options = struct {
    db_path: ?[]const u8 = null,
    limit: usize = 10,
    queries: []const []const u8 = &.{},
};

pub const EngineResult = struct {
    doc_id: u64,
    score: f64,
};

pub const QueryComparison = struct {
    query: []const u8,
    fts5_match: []const u8,
    custom_query_ns: u64,
    fts5_query_ns: u64,
    top_k_overlap: usize,
    top_k_overlap_ratio: f64,
    first_rank_same: bool,
    custom_results: []EngineResult,
    fts5_results: []EngineResult,
};

pub const Summary = struct {
    db_path: []const u8,
    doc_count: usize,
    query_count: usize,
    limit: usize,
    custom_index_ns: u64,
    custom_incremental_index_ns: u64,
    custom_bulk_index_ns: u64,
    fts5_index_ns: u64,
    comparisons: []QueryComparison,
};

const SampleDoc = struct {
    id: u64,
    title: []const u8,
    body: []const u8,
};

const sample_docs = [_]SampleDoc{
    .{ .id = 1, .title = "SQLite full text search", .body = "SQLite FTS5 provides tokenized full text search with a built in bm25 ranking function." },
    .{ .id = 2, .title = "Custom BM25 ranking", .body = "MindBrain keeps a custom BM25 index with term hashes, posting bitmaps, document statistics, and stopword filtering." },
    .{ .id = 3, .title = "Hybrid semantic retrieval", .body = "Hybrid retrieval combines BM25 lexical score with vector similarity for contextual search over document chunks." },
    .{ .id = 4, .title = "Roaring bitmap facets", .body = "Faceted filtering stores postings as roaring bitmaps and intersects candidate document sets quickly." },
    .{ .id = 5, .title = "PostgreSQL extension parity", .body = "The PostgreSQL extension exposes facets, graph search, and native BM25 functions with matching public API names." },
    .{ .id = 6, .title = "SQLite standalone backend", .body = "The standalone backend uses SQLite tables, JSON helpers, Zig engines, and embedded stopword bootstrap data." },
    .{ .id = 7, .title = "Document chunk indexing", .body = "Large documents are split into chunks so lexical search and vector embeddings can share deterministic chunk identifiers." },
    .{ .id = 8, .title = "Prefix and fuzzy matching", .body = "Search systems often add prefix matching, fuzzy term expansion, language filters, and custom tokenizer rules." },
    .{ .id = 9, .title = "Database migration validation", .body = "Schema bootstrap must create tables, load seeds, preserve stopwords, and make migrations reproducible." },
    .{ .id = 10, .title = "SQLite phrase search", .body = "FTS5 supports phrase queries, NEAR operators, snippets, highlights, and optimized inverted indexes." },
    .{ .id = 11, .title = "Vector exact search", .body = "Exact vector search scans embedding rows and orders candidates by cosine, l2, or inner product metrics." },
    .{ .id = 12, .title = "Benchmark methodology", .body = "A useful benchmark compares top k overlap, first rank agreement, indexing time, and query latency." },
};

const default_queries = [_][]const u8{
    "sqlite bm25 search",
    "custom bm25 index",
    "hybrid vector search",
    "roaring bitmap facets",
    "stopword bootstrap migration",
    "phrase query snippets",
};

pub fn run(allocator: std.mem.Allocator, options: Options) !Summary {
    if (options.limit == 0) return error.InvalidLimit;

    const custom_incremental_index_ns = try measureCustomIncrementalIndex(allocator);

    var db = if (options.db_path) |path| try facet_sqlite.Database.open(path) else try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try db.exec(
        \\DROP TABLE IF EXISTS bench_docs_fts;
        \\CREATE VIRTUAL TABLE bench_docs_fts USING fts5(title, body);
    );

    var custom_index_timer = try compat.Timer.start();
    try insertSearchDocuments(db, allocator, sample_docs[0..]);
    const custom_bulk_index_ns = custom_index_timer.read();

    var fts5_index_timer = try compat.Timer.start();
    try insertFts5Docs(db, sample_docs[0..]);
    const fts5_index_ns = fts5_index_timer.read();

    const queries = if (options.queries.len > 0) options.queries else default_queries[0..];
    var comparisons = std.ArrayList(QueryComparison).empty;
    defer comparisons.deinit(allocator);

    for (queries) |query| {
        const fts5_match = try buildFts5OrQuery(allocator, query);
        var custom_query_timer = try compat.Timer.start();
        const custom = try runCustomQuery(allocator, db, query, options.limit);
        const custom_query_ns = custom_query_timer.read();

        var fts5_query_timer = try compat.Timer.start();
        const fts5 = try runFts5Query(allocator, db, fts5_match, options.limit);
        const fts5_query_ns = fts5_query_timer.read();

        try comparisons.append(allocator, .{
            .query = query,
            .fts5_match = fts5_match,
            .custom_query_ns = custom_query_ns,
            .fts5_query_ns = fts5_query_ns,
            .top_k_overlap = countOverlap(custom, fts5),
            .top_k_overlap_ratio = overlapRatio(custom, fts5),
            .first_rank_same = custom.len > 0 and fts5.len > 0 and custom[0].doc_id == fts5[0].doc_id,
            .custom_results = custom,
            .fts5_results = fts5,
        });
    }

    return .{
        .db_path = options.db_path orelse ":memory:",
        .doc_count = sample_docs.len,
        .query_count = queries.len,
        .limit = options.limit,
        .custom_index_ns = custom_bulk_index_ns,
        .custom_incremental_index_ns = custom_incremental_index_ns,
        .custom_bulk_index_ns = custom_bulk_index_ns,
        .fts5_index_ns = fts5_index_ns,
        .comparisons = try comparisons.toOwnedSlice(allocator),
    };
}

fn measureCustomIncrementalIndex(allocator: std.mem.Allocator) !u64 {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    var timer = try compat.Timer.start();
    for (sample_docs) |doc| {
        const content = try std.fmt.allocPrint(allocator, "{s} {s}", .{ doc.title, doc.body });
        defer allocator.free(content);
        try search_sqlite.syncSearchDocument(db, allocator, table_id, doc.id, content, language);
    }
    return timer.read();
}

pub fn deinitSummary(allocator: std.mem.Allocator, summary: Summary) void {
    for (summary.comparisons) |comparison| {
        allocator.free(comparison.fts5_match);
        allocator.free(comparison.custom_results);
        allocator.free(comparison.fts5_results);
    }
    allocator.free(summary.comparisons);
}

fn runCustomQuery(allocator: std.mem.Allocator, db: facet_sqlite.Database, query: []const u8, limit: usize) ![]EngineResult {
    const matches = try search_sqlite.searchFts5Bm25(db, allocator, table_id, query, limit);
    defer allocator.free(matches);

    var results = try allocator.alloc(EngineResult, matches.len);
    for (matches, 0..) |match, index| {
        results[index] = .{
            .doc_id = match.doc_id,
            .score = match.score,
        };
    }
    return results;
}

fn runFts5Query(allocator: std.mem.Allocator, db: facet_sqlite.Database, query: []const u8, limit: usize) ![]EngineResult {
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT rowid, bm25(bench_docs_fts) AS score
        \\FROM bench_docs_fts
        \\WHERE bench_docs_fts MATCH ?1
        \\ORDER BY score ASC
        \\LIMIT ?2
    );
    defer facet_sqlite.finalize(stmt);

    try facet_sqlite.bindText(stmt, 1, query);
    try facet_sqlite.bindInt64(stmt, 2, @as(i64, @intCast(limit)));

    var results = std.ArrayList(EngineResult).empty;
    defer results.deinit(allocator);

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try results.append(allocator, .{
            .doc_id = @intCast(c.sqlite3_column_int64(stmt, 0)),
            .score = c.sqlite3_column_double(stmt, 1),
        });
    }

    return results.toOwnedSlice(allocator);
}

fn insertSearchDocuments(db: facet_sqlite.Database, allocator: std.mem.Allocator, docs: []const SampleDoc) !void {
    for (docs) |doc| {
        const content = try std.fmt.allocPrint(allocator, "{s} {s}", .{ doc.title, doc.body });
        defer allocator.free(content);
        try search_sqlite.upsertSearchDocument(db, table_id, doc.id, content, language);
    }
}

fn buildFts5OrQuery(allocator: std.mem.Allocator, query: []const u8) ![]const u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);

    var token_count: usize = 0;
    var i: usize = 0;
    while (i < query.len) {
        while (i < query.len and !isFtsTokenChar(query[i])) : (i += 1) {}
        if (i >= query.len) break;

        const start = i;
        while (i < query.len and isFtsTokenChar(query[i])) : (i += 1) {}
        if (start == i) continue;

        if (token_count > 0) try out.appendSlice(allocator, " OR ");
        try out.append(allocator, '"');
        for (query[start..i]) |char| {
            try out.append(allocator, std.ascii.toLower(char));
        }
        try out.append(allocator, '"');
        token_count += 1;
    }

    if (token_count == 0) {
        return try allocator.dupe(u8, query);
    }
    return out.toOwnedSlice(allocator);
}

fn isFtsTokenChar(char: u8) bool {
    return std.ascii.isAlphanumeric(char) or char >= 0x80;
}

fn insertFts5Docs(db: facet_sqlite.Database, docs: []const SampleDoc) !void {
    const stmt = try facet_sqlite.prepare(db, "INSERT INTO bench_docs_fts(rowid, title, body) VALUES (?1, ?2, ?3)");
    defer facet_sqlite.finalize(stmt);

    for (docs) |doc| {
        try facet_sqlite.bindInt64(stmt, 1, @as(i64, @intCast(doc.id)));
        try facet_sqlite.bindText(stmt, 2, doc.title);
        try facet_sqlite.bindText(stmt, 3, doc.body);
        try facet_sqlite.stepDone(stmt);
        _ = c.sqlite3_reset(stmt);
        _ = c.sqlite3_clear_bindings(stmt);
    }
}

fn countOverlap(a: []const EngineResult, b: []const EngineResult) usize {
    var count: usize = 0;
    for (a) |left| {
        for (b) |right| {
            if (left.doc_id == right.doc_id) {
                count += 1;
                break;
            }
        }
    }
    return count;
}

fn overlapRatio(a: []const EngineResult, b: []const EngineResult) f64 {
    const denominator = @max(a.len, b.len);
    if (denominator == 0) return 1.0;
    return @as(f64, @floatFromInt(countOverlap(a, b))) / @as(f64, @floatFromInt(denominator));
}

test "fts5 compare produces one comparison per query" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const queries = [_][]const u8{"sqlite search"};
    const summary = try run(arena.allocator(), .{
        .limit = 5,
        .queries = queries[0..],
    });

    try std.testing.expectEqual(@as(usize, sample_docs.len), summary.doc_count);
    try std.testing.expectEqual(@as(usize, 1), summary.query_count);
    try std.testing.expect(summary.comparisons[0].custom_results.len > 0);
    try std.testing.expect(summary.comparisons[0].fts5_results.len > 0);
}
