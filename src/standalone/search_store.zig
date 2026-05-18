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

pub const Store = struct {
    allocator: std.mem.Allocator,
    documents: std.ArrayList(DocumentRecord),
    embeddings: std.ArrayList(EmbeddingEntry),
    collection_stats: std.ArrayList(interfaces.CollectionStatsEntry),
    document_stats: std.ArrayList(DocumentStatEntry),
    term_stats: std.ArrayList(TermStatEntry),
    term_frequencies: std.ArrayList(TermFrequencyEntry),
    postings: std.ArrayList(PostingEntry),
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

            try self.document_stats.append(self.allocator, .{
                .table_id = doc.table_id,
                .stats = .{
                    .doc_id = doc.doc_id,
                    .document_length = @intCast(tokens.len),
                    .unique_terms = @intCast(local_counts.count()),
                },
            });

            const length_entry = try doc_lengths.getOrPut(doc.table_id);
            if (!length_entry.found_existing) length_entry.value_ptr.* = .{ .total_docs = 0, .total_length = 0 };
            length_entry.value_ptr.total_docs += 1;
            length_entry.value_ptr.total_length += tokens.len;

            var local_it = local_counts.iterator();
            while (local_it.next()) |entry| {
                const term_hash = entry.key_ptr.*;
                try self.term_frequencies.append(self.allocator, .{
                    .table_id = doc.table_id,
                    .doc_id = doc.doc_id,
                    .frequency = .{
                        .term_hash = term_hash,
                        .frequency = entry.value_ptr.*,
                    },
                });

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
            try self.collection_stats.append(self.allocator, .{
                .table_id = entry.key_ptr.*,
                .stats = .{
                    .total_documents = total_docs,
                    .avg_document_length = avg,
                },
            });
        }

        var df_it = term_doc_freq.iterator();
        while (df_it.next()) |entry| {
            const unpacked = unpackTableTermKey(entry.key_ptr.*);
            try self.term_stats.append(self.allocator, .{
                .table_id = unpacked.table_id,
                .stat = .{
                    .term_hash = unpacked.term_hash,
                    .document_frequency = entry.value_ptr.*,
                },
            });
        }

        var posting_it = posting_docs.iterator();
        while (posting_it.next()) |entry| {
            const unpacked = unpackTableTermKey(entry.key_ptr.*);
            try self.postings.append(self.allocator, .{
                .table_id = unpacked.table_id,
                .term_hash = unpacked.term_hash,
                .bitmap = try roaring.Bitmap.fromSlice(entry.value_ptr.items),
            });
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
    for (self.collection_stats.items) |entry| {
        if (entry.table_id == table_id) return entry.stats;
    }
    return .{ .total_documents = 0, .avg_document_length = 0.0 };
}

fn getDocumentStats(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId) anyerror!?interfaces.DocumentStats {
    _ = allocator;
    const self: *Store = @ptrCast(@alignCast(ctx));
    for (self.document_stats.items) |entry| {
        if (entry.table_id == table_id and entry.stats.doc_id == doc_id) return entry.stats;
    }
    return null;
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
        stats[index] = .{ .term_hash = term_hash, .document_frequency = 0 };
        for (self.term_stats.items) |entry| {
            if (entry.table_id == table_id and entry.stat.term_hash == term_hash) {
                stats[index] = entry.stat;
                break;
            }
        }
    }

    return stats;
}

fn getTermFrequencies(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId, term_hashes: []const u64) anyerror![]interfaces.TermFrequency {
    const self: *Store = @ptrCast(@alignCast(ctx));
    var wanted_terms = std.AutoHashMap(u64, void).init(allocator);
    defer wanted_terms.deinit();
    for (term_hashes) |term_hash| {
        _ = try wanted_terms.getOrPut(term_hash);
    }

    var freqs = std.ArrayList(interfaces.TermFrequency).empty;
    defer freqs.deinit(allocator);
    for (self.term_frequencies.items) |entry| {
        if (entry.table_id == table_id and entry.doc_id == doc_id and wanted_terms.contains(entry.frequency.term_hash)) {
            try freqs.append(allocator, entry.frequency);
        }
    }
    return freqs.toOwnedSlice(allocator);
}

fn getTermFrequenciesBatch(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_ids: []const interfaces.DocId, term_hashes: []const u64) anyerror![]interfaces.DocumentTermFrequency {
    var rows = std.ArrayList(interfaces.DocumentTermFrequency).empty;
    defer rows.deinit(allocator);
    for (doc_ids) |doc_id| {
        const freqs = try getTermFrequencies(ctx, allocator, table_id, doc_id, term_hashes);
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

fn getPostingBitmap(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, term_hash: u64) anyerror!?roaring.Bitmap {
    _ = allocator;
    const self: *Store = @ptrCast(@alignCast(ctx));
    for (self.postings.items) |entry| {
        if (entry.table_id == table_id and entry.term_hash == term_hash) return try entry.bitmap.clone();
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
