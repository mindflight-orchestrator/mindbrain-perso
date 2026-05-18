const std = @import("std");
const interfaces = @import("interfaces.zig");
const roaring = @import("roaring.zig");
const vector_distance = @import("vector_distance.zig");
const facet_sqlite = @import("facet_sqlite.zig");
const bm25_stopwords_sqlite = @import("bm25_stopwords_sqlite.zig");
const tokenization_sqlite = @import("tokenization_sqlite.zig");

pub const DocumentRecord = struct {
    table_id: u64,
    doc_id: interfaces.DocId,
    content: []const u8,
    language: []const u8,
};

const EmbeddingEntry = struct {
    table_id: u64,
    doc_id: interfaces.DocId,
    values: []const f32,
};

const DocumentStatEntry = struct {
    table_id: u64,
    stats: interfaces.DocumentStats,
};

const TermStatEntry = struct {
    table_id: u64,
    stat: interfaces.TermStat,
};

const TermFrequencyEntry = struct {
    table_id: u64,
    doc_id: interfaces.DocId,
    frequency: interfaces.TermFrequency,
};

const PostingEntry = struct {
    table_id: u64,
    term_hash: u64,
    bitmap: roaring.Bitmap,
};

const TableDocKey = struct {
    table_id: u64,
    doc_id: interfaces.DocId,
};

const TableDocTermKey = struct {
    table_id: u64,
    doc_id: interfaces.DocId,
    term_hash: u64,
};

pub const Store = struct {
    allocator: std.mem.Allocator,
    documents: std.ArrayList(DocumentRecord),
    embeddings: std.ArrayList(EmbeddingEntry),
    collection_stats: std.ArrayList(interfaces.CollectionStatsEntry),
    document_stats: std.ArrayList(DocumentStatEntry),
    term_stats: std.ArrayList(TermStatEntry),
    term_frequencies: std.ArrayList(TermFrequencyEntry),
    postings: std.ArrayList(PostingEntry),
    collection_stats_by_table: std.AutoHashMap(u64, interfaces.CollectionStats),
    document_stats_by_doc: std.AutoHashMap(TableDocKey, interfaces.DocumentStats),
    term_stats_by_term: std.AutoHashMap(u128, interfaces.TermStat),
    term_frequencies_by_doc_term: std.AutoHashMap(TableDocTermKey, u32),
    posting_index_by_term: std.AutoHashMap(u128, usize),
    bm25_stopwords: bm25_stopwords_sqlite.StopwordCache,
    /// When set (e.g. by `loadSearchStore`), BM25 tokenization loads `bm25_stopwords` for each document language.
    bm25_db: ?facet_sqlite.Database = null,

    pub fn init(allocator: std.mem.Allocator) Store {
        return .{
            .allocator = allocator,
            .documents = .empty,
            .embeddings = .empty,
            .collection_stats = .empty,
            .document_stats = .empty,
            .term_stats = .empty,
            .term_frequencies = .empty,
            .postings = .empty,
            .collection_stats_by_table = std.AutoHashMap(u64, interfaces.CollectionStats).init(allocator),
            .document_stats_by_doc = std.AutoHashMap(TableDocKey, interfaces.DocumentStats).init(allocator),
            .term_stats_by_term = std.AutoHashMap(u128, interfaces.TermStat).init(allocator),
            .term_frequencies_by_doc_term = std.AutoHashMap(TableDocTermKey, u32).init(allocator),
            .posting_index_by_term = std.AutoHashMap(u128, usize).init(allocator),
            .bm25_stopwords = bm25_stopwords_sqlite.StopwordCache.init(allocator),
            .bm25_db = null,
        };
    }

    pub fn deinit(self: *Store) void {
        self.clearBm25Index();
        self.collection_stats.deinit(self.allocator);
        self.document_stats.deinit(self.allocator);
        self.term_stats.deinit(self.allocator);
        self.term_frequencies.deinit(self.allocator);
        self.postings.deinit(self.allocator);
        self.collection_stats_by_table.deinit();
        self.document_stats_by_doc.deinit();
        self.term_stats_by_term.deinit();
        self.term_frequencies_by_doc_term.deinit();
        self.posting_index_by_term.deinit();
        self.bm25_stopwords.deinit();
        for (self.documents.items) |doc| {
            self.allocator.free(doc.content);
            self.allocator.free(doc.language);
        }
        self.documents.deinit(self.allocator);

        for (self.embeddings.items) |entry| self.allocator.free(entry.values);
        self.embeddings.deinit(self.allocator);
        self.* = undefined;
    }

    fn clearBm25Index(self: *Store) void {
        self.collection_stats.clearRetainingCapacity();
        self.document_stats.clearRetainingCapacity();
        self.term_stats.clearRetainingCapacity();
        self.term_frequencies.clearRetainingCapacity();
        for (self.postings.items) |*entry| entry.bitmap.deinit();
        self.postings.clearRetainingCapacity();
        self.collection_stats_by_table.clearRetainingCapacity();
        self.document_stats_by_doc.clearRetainingCapacity();
        self.term_stats_by_term.clearRetainingCapacity();
        self.term_frequencies_by_doc_term.clearRetainingCapacity();
        self.posting_index_by_term.clearRetainingCapacity();
    }

    pub fn bm25Repository(self: *Store) interfaces.Bm25Repository {
        return .{
            .ctx = @ptrCast(self),
            .getCollectionStatsFn = getCollectionStats,
            .getDocumentStatsFn = getDocumentStats,
            .getDocumentStatsBatchFn = getDocumentStatsBatch,
            .getTermStatsFn = getTermStats,
            .getTermFrequenciesFn = getTermFrequencies,
            .getTermFrequenciesBatchFn = getTermFrequenciesBatch,
            .getPostingBitmapFn = getPostingBitmap,
            .upsertDocumentFn = upsertDocumentViaInterface,
            .deleteDocumentFn = deleteDocumentViaInterface,
        };
    }

    pub fn vectorRepository(self: *Store) interfaces.VectorRepository {
        return .{
            .ctx = @ptrCast(self),
            .getEmbeddingDimensionsFn = getEmbeddingDimensions,
            .searchNearestFn = searchNearest,
            .upsertEmbeddingFn = upsertEmbeddingViaInterface,
            .deleteEmbeddingFn = deleteEmbeddingViaInterface,
        };
    }

    pub fn upsertDocument(self: *Store, request: interfaces.UpsertDocumentRequest) !void {
        for (self.documents.items) |*doc| {
            if (doc.table_id == request.table_id and doc.doc_id == request.doc_id) {
                self.allocator.free(doc.content);
                self.allocator.free(doc.language);
                doc.content = try self.allocator.dupe(u8, request.content);
                doc.language = try self.allocator.dupe(u8, request.language);
                try self.rebuildBm25Index();
                return;
            }
        }

        try self.documents.append(self.allocator, .{
            .table_id = request.table_id,
            .doc_id = request.doc_id,
            .content = try self.allocator.dupe(u8, request.content),
            .language = try self.allocator.dupe(u8, request.language),
        });
        try self.rebuildBm25Index();
    }

    pub fn deleteDocument(self: *Store, table_id: u64, doc_id: interfaces.DocId) !void {
        var i: usize = 0;
        while (i < self.documents.items.len) : (i += 1) {
            if (self.documents.items[i].table_id != table_id or self.documents.items[i].doc_id != doc_id) continue;
            self.allocator.free(self.documents.items[i].content);
            self.allocator.free(self.documents.items[i].language);
            _ = self.documents.swapRemove(i);
            try self.rebuildBm25Index();
            return;
        }
    }

    fn rebuildBm25Index(self: *Store) !void {
        self.clearBm25Index();

        var doc_lengths = std.AutoHashMap(u64, struct { total_docs: u64, total_length: u64 }).init(self.allocator);
        defer doc_lengths.deinit();
        var term_doc_freq = std.AutoHashMap(u128, u64).init(self.allocator);
        defer term_doc_freq.deinit();
        var posting_docs = std.AutoHashMap(u128, std.ArrayList(u32)).init(self.allocator);
        defer {
            var it = posting_docs.iterator();
            while (it.next()) |entry| entry.value_ptr.deinit(self.allocator);
            posting_docs.deinit();
        }

        for (self.documents.items) |doc| {
            const tokens = try tokenizeForBm25(self.allocator, self, doc.content, doc.language);
            defer self.allocator.free(tokens);

            var local_counts = std.AutoHashMap(u64, u32).init(self.allocator);
            defer local_counts.deinit();
            for (tokens) |token| {
                const entry = try local_counts.getOrPut(token);
                if (entry.found_existing) {
                    entry.value_ptr.* += 1;
                } else {
                    entry.value_ptr.* = 1;
                }
            }

            const doc_stats: interfaces.DocumentStats = .{
                .doc_id = doc.doc_id,
                .document_length = @intCast(tokens.len),
                .unique_terms = @intCast(local_counts.count()),
            };
            try self.document_stats.append(self.allocator, .{
                .table_id = doc.table_id,
                .stats = doc_stats,
            });
            try self.document_stats_by_doc.put(.{
                .table_id = doc.table_id,
                .doc_id = doc.doc_id,
            }, doc_stats);

            const length_entry = try doc_lengths.getOrPut(doc.table_id);
            if (!length_entry.found_existing) length_entry.value_ptr.* = .{ .total_docs = 0, .total_length = 0 };
            length_entry.value_ptr.total_docs += 1;
            length_entry.value_ptr.total_length += tokens.len;

            var local_it = local_counts.iterator();
            while (local_it.next()) |entry| {
                const term_hash = entry.key_ptr.*;
                const frequency: interfaces.TermFrequency = .{
                    .term_hash = term_hash,
                    .frequency = entry.value_ptr.*,
                };
                try self.term_frequencies.append(self.allocator, .{
                    .table_id = doc.table_id,
                    .doc_id = doc.doc_id,
                    .frequency = frequency,
                });
                try self.term_frequencies_by_doc_term.put(.{
                    .table_id = doc.table_id,
                    .doc_id = doc.doc_id,
                    .term_hash = term_hash,
                }, entry.value_ptr.*);

                const key = packTableTermKey(doc.table_id, term_hash);
                const df_entry = try term_doc_freq.getOrPut(key);
                if (!df_entry.found_existing) df_entry.value_ptr.* = 0;
                df_entry.value_ptr.* += 1;

                const posting_entry = try posting_docs.getOrPut(key);
                if (!posting_entry.found_existing) posting_entry.value_ptr.* = .empty;
                try posting_entry.value_ptr.append(self.allocator, @intCast(doc.doc_id));
            }
        }

        var length_it = doc_lengths.iterator();
        while (length_it.next()) |entry| {
            const total_docs = entry.value_ptr.total_docs;
            const avg = if (total_docs == 0) 0.0 else @as(f64, @floatFromInt(entry.value_ptr.total_length)) / @as(f64, @floatFromInt(total_docs));
            const stats: interfaces.CollectionStats = .{
                .total_documents = total_docs,
                .avg_document_length = avg,
            };
            try self.collection_stats.append(self.allocator, .{
                .table_id = entry.key_ptr.*,
                .stats = stats,
            });
            try self.collection_stats_by_table.put(entry.key_ptr.*, stats);
        }

        var df_it = term_doc_freq.iterator();
        while (df_it.next()) |entry| {
            const unpacked = unpackTableTermKey(entry.key_ptr.*);
            const stat: interfaces.TermStat = .{
                .term_hash = unpacked.term_hash,
                .document_frequency = entry.value_ptr.*,
            };
            try self.term_stats.append(self.allocator, .{
                .table_id = unpacked.table_id,
                .stat = stat,
            });
            try self.term_stats_by_term.put(entry.key_ptr.*, stat);
        }

        var posting_it = posting_docs.iterator();
        while (posting_it.next()) |entry| {
            const unpacked = unpackTableTermKey(entry.key_ptr.*);
            const posting_index = self.postings.items.len;
            try self.postings.append(self.allocator, .{
                .table_id = unpacked.table_id,
                .term_hash = unpacked.term_hash,
                .bitmap = try roaring.Bitmap.fromSlice(entry.value_ptr.items),
            });
            try self.posting_index_by_term.put(entry.key_ptr.*, posting_index);
        }
    }

    pub fn upsertEmbedding(self: *Store, request: interfaces.UpsertEmbeddingRequest) !void {
        for (self.embeddings.items) |*entry| {
            if (entry.table_id == request.table_id and entry.doc_id == request.doc_id) {
                self.allocator.free(entry.values);
                entry.values = try self.allocator.dupe(f32, request.values);
                return;
            }
        }

        try self.embeddings.append(self.allocator, .{
            .table_id = request.table_id,
            .doc_id = request.doc_id,
            .values = try self.allocator.dupe(f32, request.values),
        });
    }

    pub fn deleteEmbedding(self: *Store, table_id: u64, doc_id: interfaces.DocId) !void {
        var i: usize = 0;
        while (i < self.embeddings.items.len) : (i += 1) {
            if (self.embeddings.items[i].table_id != table_id or self.embeddings.items[i].doc_id != doc_id) continue;
            self.allocator.free(self.embeddings.items[i].values);
            _ = self.embeddings.swapRemove(i);
            return;
        }
    }
};

fn getCollectionStats(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64) anyerror!interfaces.CollectionStats {
    _ = allocator;
    const self: *Store = @ptrCast(@alignCast(ctx));
    if (self.collection_stats_by_table.get(table_id)) |stats| return stats;
    return .{ .total_documents = 0, .avg_document_length = 0.0 };
}

fn getDocumentStats(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId) anyerror!?interfaces.DocumentStats {
    _ = allocator;
    const self: *Store = @ptrCast(@alignCast(ctx));
    return self.document_stats_by_doc.get(.{
        .table_id = table_id,
        .doc_id = doc_id,
    });
}

fn getDocumentStatsBatch(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_ids: []const interfaces.DocId) anyerror![]interfaces.DocumentStats {
    var rows = std.ArrayList(interfaces.DocumentStats).empty;
    defer rows.deinit(allocator);
    for (doc_ids) |doc_id| {
        if (try getDocumentStats(ctx, allocator, table_id, doc_id)) |stats| {
            try rows.append(allocator, stats);
        }
    }
    return rows.toOwnedSlice(allocator);
}

fn getTermStats(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, term_hashes: []const u64) anyerror![]interfaces.TermStat {
    const self: *Store = @ptrCast(@alignCast(ctx));
    const stats = try allocator.alloc(interfaces.TermStat, term_hashes.len);

    for (term_hashes, 0..) |term_hash, index| {
        stats[index] = self.term_stats_by_term.get(packTableTermKey(table_id, term_hash)) orelse .{
            .term_hash = term_hash,
            .document_frequency = 0,
        };
    }

    return stats;
}

fn getTermFrequencies(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId, term_hashes: []const u64) anyerror![]interfaces.TermFrequency {
    var freqs = try std.ArrayList(interfaces.TermFrequency).initCapacity(allocator, term_hashes.len);
    defer freqs.deinit(allocator);

    const self: *Store = @ptrCast(@alignCast(ctx));
    for (term_hashes) |term_hash| {
        if (self.term_frequencies_by_doc_term.get(.{
            .table_id = table_id,
            .doc_id = doc_id,
            .term_hash = term_hash,
        })) |frequency| {
            try freqs.append(allocator, .{
                .term_hash = term_hash,
                .frequency = frequency,
            });
        }
    }
    return freqs.toOwnedSlice(allocator);
}

fn getTermFrequenciesBatch(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_ids: []const interfaces.DocId, term_hashes: []const u64) anyerror![]interfaces.DocumentTermFrequency {
    const self: *Store = @ptrCast(@alignCast(ctx));
    var rows = std.ArrayList(interfaces.DocumentTermFrequency).empty;
    defer rows.deinit(allocator);
    for (doc_ids) |doc_id| {
        for (term_hashes) |term_hash| {
            if (self.term_frequencies_by_doc_term.get(.{
                .table_id = table_id,
                .doc_id = doc_id,
                .term_hash = term_hash,
            })) |frequency| {
                try rows.append(allocator, .{
                    .doc_id = doc_id,
                    .term_hash = term_hash,
                    .frequency = frequency,
                });
            }
        }
    }
    return rows.toOwnedSlice(allocator);
}

fn getPostingBitmap(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, term_hash: u64) anyerror!?roaring.Bitmap {
    _ = allocator;
    const self: *Store = @ptrCast(@alignCast(ctx));
    if (self.posting_index_by_term.get(packTableTermKey(table_id, term_hash))) |index| {
        if (index < self.postings.items.len) return try self.postings.items[index].bitmap.clone();
    }
    return null;
}

fn upsertDocumentViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, request: interfaces.UpsertDocumentRequest) anyerror!void {
    _ = allocator;
    const self: *Store = @ptrCast(@alignCast(ctx));
    try self.upsertDocument(request);
}

fn deleteDocumentViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId) anyerror!void {
    _ = allocator;
    const self: *Store = @ptrCast(@alignCast(ctx));
    try self.deleteDocument(table_id, doc_id);
}

fn getEmbeddingDimensions(ctx: *anyopaque, allocator: std.mem.Allocator, table_name: []const u8, column_name: []const u8) anyerror!?usize {
    _ = allocator;
    _ = table_name;
    _ = column_name;
    const self: *Store = @ptrCast(@alignCast(ctx));
    if (self.embeddings.items.len == 0) return null;
    return self.embeddings.items[0].values.len;
}

fn searchNearest(ctx: *anyopaque, allocator: std.mem.Allocator, request: interfaces.VectorSearchRequest) anyerror![]interfaces.VectorSearchMatch {
    const self: *Store = @ptrCast(@alignCast(ctx));
    var matches = std.ArrayList(interfaces.VectorSearchMatch).empty;
    defer matches.deinit(allocator);

    for (self.embeddings.items) |entry| {
        if (entry.table_id != 1 and request.table_name.len > 0) {}
        const vector_score = vector_distance.score(request.metric, request.query_vector, entry.values);
        try matches.append(allocator, .{
            .doc_id = entry.doc_id,
            .distance = vector_score.distance,
            .similarity = vector_score.similarity,
        });
    }

    std.mem.sort(interfaces.VectorSearchMatch, matches.items, {}, vector_distance.lessThan);

    if (matches.items.len > request.limit) matches.shrinkRetainingCapacity(request.limit);
    return matches.toOwnedSlice(allocator);
}

fn upsertEmbeddingViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, request: interfaces.UpsertEmbeddingRequest) anyerror!void {
    _ = allocator;
    const self: *Store = @ptrCast(@alignCast(ctx));
    try self.upsertEmbedding(request);
}

fn deleteEmbeddingViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId) anyerror!void {
    _ = allocator;
    const self: *Store = @ptrCast(@alignCast(ctx));
    try self.deleteEmbedding(table_id, doc_id);
}

fn tokenizeForBm25(allocator: std.mem.Allocator, store: *Store, content: []const u8, language: []const u8) ![]u64 {
    const sw = try store.bm25_stopwords.sliceFor(store.bm25_db, language);
    return tokenization_sqlite.tokenizeSearchHashes(allocator, content, sw);
}

fn countTokens(store: *Store, text: []const u8, language: []const u8) !u64 {
    const tokens = try tokenizeForBm25(std.heap.page_allocator, store, text, language);
    defer std.heap.page_allocator.free(tokens);
    return tokens.len;
}

fn packTableTermKey(table_id: u64, term_hash: u64) u128 {
    return (@as(u128, table_id) << 64) | @as(u128, term_hash);
}

fn unpackTableTermKey(key: u128) struct { table_id: u64, term_hash: u64 } {
    return .{
        .table_id = @intCast(key >> 64),
        .term_hash = @intCast(key & 0xffff_ffff_ffff_ffff),
    };
}

fn containsToken(tokens: []const u64, target: u64) bool {
    for (tokens) |token| if (token == target) return true;
    return false;
}

fn countTokenFrequency(tokens: []const u64, target: u64) u32 {
    var count: u32 = 0;
    for (tokens) |token| {
        if (token == target) count += 1;
    }
    return count;
}

test "search store maintains indexed BM25 lookup maps across updates" {
    var store = Store.init(std.testing.allocator);
    defer store.deinit();

    try store.upsertDocument(.{
        .table_id = 1,
        .doc_id = 10,
        .content = "zig zig sqlite",
        .language = "english",
    });
    try store.upsertDocument(.{
        .table_id = 1,
        .doc_id = 20,
        .content = "sqlite graph",
        .language = "english",
    });

    const repo = store.bm25Repository();
    const zig_hash = tokenization_sqlite.hashTermLexeme("zig");
    const sqlite_hash = tokenization_sqlite.hashTermLexeme("sqlite");

    const collection = try repo.getCollectionStatsFn(repo.ctx, std.testing.allocator, 1);
    try std.testing.expectEqual(@as(u64, 2), collection.total_documents);

    const term_stats = try repo.getTermStatsFn(repo.ctx, std.testing.allocator, 1, &.{ zig_hash, sqlite_hash });
    defer std.testing.allocator.free(term_stats);
    try std.testing.expectEqual(@as(u64, 1), term_stats[0].document_frequency);
    try std.testing.expectEqual(@as(u64, 2), term_stats[1].document_frequency);

    const doc_stats = try repo.getDocumentStatsBatchFn(repo.ctx, std.testing.allocator, 1, &.{ 10, 20 });
    defer std.testing.allocator.free(doc_stats);
    try std.testing.expectEqual(@as(usize, 2), doc_stats.len);

    const freqs = try repo.getTermFrequenciesBatchFn(repo.ctx, std.testing.allocator, 1, &.{ 10, 20 }, &.{ zig_hash, sqlite_hash });
    defer std.testing.allocator.free(freqs);
    try std.testing.expectEqual(@as(usize, 3), freqs.len);

    var sqlite_posting = (try repo.getPostingBitmapFn(repo.ctx, std.testing.allocator, 1, sqlite_hash)).?;
    defer sqlite_posting.deinit();
    try std.testing.expect(sqlite_posting.contains(10));
    try std.testing.expect(sqlite_posting.contains(20));

    try store.deleteDocument(1, 10);
    try std.testing.expect((try repo.getDocumentStatsFn(repo.ctx, std.testing.allocator, 1, 10)) == null);
    const updated_collection = try repo.getCollectionStatsFn(repo.ctx, std.testing.allocator, 1);
    try std.testing.expectEqual(@as(u64, 1), updated_collection.total_documents);
    try std.testing.expect((try repo.getPostingBitmapFn(repo.ctx, std.testing.allocator, 1, zig_hash)) == null);
}
