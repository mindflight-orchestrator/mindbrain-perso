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
    // Keyed maps for O(1) BM25 reads.
    collection_stats_by_table: std.AutoHashMap(u64, interfaces.CollectionStats),
    document_stats_by_doc: std.AutoHashMap(TableDocKey, interfaces.DocumentStats),
    term_stats_by_term: std.AutoHashMap(u128, interfaces.TermStat),
    term_frequencies_by_doc_term: std.AutoHashMap(TableDocTermKey, u32),
    posting_index_by_term: std.AutoHashMap(u128, usize),
    // Position maps for O(1) existence checks and delta maintenance.
    document_position_map: std.AutoHashMap(TableDocKey, usize),
    embedding_position_map: std.AutoHashMap(TableDocKey, usize),
    // Tracks total corpus length per table for correct avg delta computation.
    collection_total_length_by_table: std.AutoHashMap(u64, u64),
    bm25_stopwords: bm25_stopwords_sqlite.StopwordCache,
    /// When set (e.g. by `loadSearchStore`), BM25 tokenization uses the DB for stopwords.
    bm25_db: ?facet_sqlite.Database = null,
    /// Set by beginBulkLoad; suppresses per-document BM25 rebuilds until endBulkLoad.
    bulk_loading: bool = false,

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
            .document_position_map = std.AutoHashMap(TableDocKey, usize).init(allocator),
            .embedding_position_map = std.AutoHashMap(TableDocKey, usize).init(allocator),
            .collection_total_length_by_table = std.AutoHashMap(u64, u64).init(allocator),
            .bm25_stopwords = bm25_stopwords_sqlite.StopwordCache.init(allocator),
            .bm25_db = null,
            .bulk_loading = false,
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
        self.document_position_map.deinit();
        self.embedding_position_map.deinit();
        self.collection_total_length_by_table.deinit();
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
        self.collection_total_length_by_table.clearRetainingCapacity();
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

    /// Enter bulk-load mode: upsertDocument calls append without duplicate checks
    /// or BM25 rebuilds until endBulkLoad is called.
    pub fn beginBulkLoad(self: *Store) void {
        self.bulk_loading = true;
    }

    /// Exit bulk-load mode: rebuild position maps and the BM25 index exactly once.
    pub fn endBulkLoad(self: *Store) !void {
        self.bulk_loading = false;
        self.document_position_map.clearRetainingCapacity();
        for (self.documents.items, 0..) |doc, i| {
            try self.document_position_map.put(.{ .table_id = doc.table_id, .doc_id = doc.doc_id }, i);
        }
        self.embedding_position_map.clearRetainingCapacity();
        for (self.embeddings.items, 0..) |emb, i| {
            try self.embedding_position_map.put(.{ .table_id = emb.table_id, .doc_id = emb.doc_id }, i);
        }
        try self.rebuildBm25Index();
    }

    pub fn upsertDocument(self: *Store, request: interfaces.UpsertDocumentRequest) !void {
        const key = TableDocKey{ .table_id = request.table_id, .doc_id = request.doc_id };

        if (self.bulk_loading) {
            // Fast path: append directly. endBulkLoad rebuilds position maps and BM25 index.
            try self.documents.append(self.allocator, .{
                .table_id = request.table_id,
                .doc_id = request.doc_id,
                .content = try self.allocator.dupe(u8, request.content),
                .language = try self.allocator.dupe(u8, request.language),
            });
            return;
        }

        if (self.document_position_map.get(key)) |index| {
            // Update existing document with delta maintenance.
            const old_doc = self.documents.items[index];
            const old_tokens = try tokenizeForBm25(self.allocator, self, old_doc.content, old_doc.language);
            defer self.allocator.free(old_tokens);
            var old_counts = std.AutoHashMap(u64, u32).init(self.allocator);
            defer old_counts.deinit();
            try buildTokenCounts(&old_counts, old_tokens);

            const new_content = try self.allocator.dupe(u8, request.content);
            errdefer self.allocator.free(new_content);
            const new_language = try self.allocator.dupe(u8, request.language);
            errdefer self.allocator.free(new_language);
            const new_tokens = try tokenizeForBm25(self.allocator, self, new_content, new_language);
            defer self.allocator.free(new_tokens);
            var new_counts = std.AutoHashMap(u64, u32).init(self.allocator);
            defer new_counts.deinit();
            try buildTokenCounts(&new_counts, new_tokens);

            try self.removeDocContributions(request.table_id, request.doc_id, old_tokens.len, &old_counts);
            self.allocator.free(self.documents.items[index].content);
            self.allocator.free(self.documents.items[index].language);
            self.documents.items[index].content = new_content;
            self.documents.items[index].language = new_language;
            try self.addDocContributions(request.table_id, request.doc_id, new_tokens.len, &new_counts);
        } else {
            // New document: append and add contributions.
            const doc_index = self.documents.items.len;
            const new_content = try self.allocator.dupe(u8, request.content);
            errdefer self.allocator.free(new_content);
            const new_language = try self.allocator.dupe(u8, request.language);
            errdefer self.allocator.free(new_language);
            try self.documents.append(self.allocator, .{
                .table_id = request.table_id,
                .doc_id = request.doc_id,
                .content = new_content,
                .language = new_language,
            });
            try self.document_position_map.put(key, doc_index);
            const new_tokens = try tokenizeForBm25(self.allocator, self, new_content, new_language);
            defer self.allocator.free(new_tokens);
            var new_counts = std.AutoHashMap(u64, u32).init(self.allocator);
            defer new_counts.deinit();
            try buildTokenCounts(&new_counts, new_tokens);
            try self.addDocContributions(request.table_id, request.doc_id, new_tokens.len, &new_counts);
        }
    }

    pub fn deleteDocument(self: *Store, table_id: u64, doc_id: interfaces.DocId) !void {
        const key = TableDocKey{ .table_id = table_id, .doc_id = doc_id };
        const index = self.document_position_map.get(key) orelse return;

        const doc = self.documents.items[index];
        const old_tokens = try tokenizeForBm25(self.allocator, self, doc.content, doc.language);
        defer self.allocator.free(old_tokens);
        var old_counts = std.AutoHashMap(u64, u32).init(self.allocator);
        defer old_counts.deinit();
        try buildTokenCounts(&old_counts, old_tokens);

        try self.removeDocContributions(table_id, doc_id, old_tokens.len, &old_counts);
        self.allocator.free(self.documents.items[index].content);
        self.allocator.free(self.documents.items[index].language);
        _ = self.documents.swapRemove(index);
        _ = self.document_position_map.remove(key);

        // swapRemove moves the last element to `index`; update its position entry.
        if (index < self.documents.items.len) {
            const moved = self.documents.items[index];
            try self.document_position_map.put(
                .{ .table_id = moved.table_id, .doc_id = moved.doc_id },
                index,
            );
        }
    }

    pub fn upsertEmbedding(self: *Store, request: interfaces.UpsertEmbeddingRequest) !void {
        const key = TableDocKey{ .table_id = request.table_id, .doc_id = request.doc_id };
        if (!self.bulk_loading) {
            if (self.embedding_position_map.get(key)) |index| {
                self.allocator.free(self.embeddings.items[index].values);
                self.embeddings.items[index].values = try self.allocator.dupe(f32, request.values);
                return;
            }
            const emb_index = self.embeddings.items.len;
            try self.embeddings.append(self.allocator, .{
                .table_id = request.table_id,
                .doc_id = request.doc_id,
                .values = try self.allocator.dupe(f32, request.values),
            });
            try self.embedding_position_map.put(key, emb_index);
        } else {
            // Bulk path: append; endBulkLoad rebuilds the position map.
            try self.embeddings.append(self.allocator, .{
                .table_id = request.table_id,
                .doc_id = request.doc_id,
                .values = try self.allocator.dupe(f32, request.values),
            });
        }
    }

    pub fn deleteEmbedding(self: *Store, table_id: u64, doc_id: interfaces.DocId) !void {
        const key = TableDocKey{ .table_id = table_id, .doc_id = doc_id };
        const index = self.embedding_position_map.get(key) orelse return;
        self.allocator.free(self.embeddings.items[index].values);
        _ = self.embeddings.swapRemove(index);
        _ = self.embedding_position_map.remove(key);
        if (index < self.embeddings.items.len) {
            const moved = self.embeddings.items[index];
            try self.embedding_position_map.put(
                .{ .table_id = moved.table_id, .doc_id = moved.doc_id },
                index,
            );
        }
    }

    // ------------------------------------------------------------------
    // Delta maintenance helpers (O(tokens_in_doc) per call)
    // ------------------------------------------------------------------

    fn addDocContributions(
        self: *Store,
        table_id: u64,
        doc_id: interfaces.DocId,
        token_count: usize,
        counts: *const std.AutoHashMap(u64, u32),
    ) !void {
        // Collection stats.
        const total_len_entry = try self.collection_total_length_by_table.getOrPut(table_id);
        if (!total_len_entry.found_existing) total_len_entry.value_ptr.* = 0;
        total_len_entry.value_ptr.* += token_count;

        const col_entry = try self.collection_stats_by_table.getOrPut(table_id);
        if (!col_entry.found_existing) col_entry.value_ptr.* = .{ .total_documents = 0, .avg_document_length = 0.0 };
        col_entry.value_ptr.total_documents += 1;
        const new_total = col_entry.value_ptr.total_documents;
        col_entry.value_ptr.avg_document_length = @as(f64, @floatFromInt(total_len_entry.value_ptr.*)) / @as(f64, @floatFromInt(new_total));

        // Document stats.
        try self.document_stats_by_doc.put(
            .{ .table_id = table_id, .doc_id = doc_id },
            .{
                .doc_id = doc_id,
                .document_length = @intCast(token_count),
                .unique_terms = @intCast(counts.count()),
            },
        );

        // Per-term contributions.
        var it = counts.iterator();
        while (it.next()) |entry| {
            const term_hash = entry.key_ptr.*;
            const freq = entry.value_ptr.*;
            const term_key = packTableTermKey(table_id, term_hash);

            try self.term_frequencies_by_doc_term.put(
                .{ .table_id = table_id, .doc_id = doc_id, .term_hash = term_hash },
                freq,
            );

            const ts_entry = try self.term_stats_by_term.getOrPut(term_key);
            if (!ts_entry.found_existing) {
                ts_entry.value_ptr.* = .{ .term_hash = term_hash, .document_frequency = 0 };
            }
            ts_entry.value_ptr.document_frequency += 1;

            if (self.posting_index_by_term.get(term_key)) |posting_idx| {
                self.postings.items[posting_idx].bitmap.add(@intCast(doc_id));
            } else {
                const posting_idx = self.postings.items.len;
                var bitmap = try roaring.Bitmap.empty();
                bitmap.add(@intCast(doc_id));
                try self.postings.append(self.allocator, .{
                    .table_id = table_id,
                    .term_hash = term_hash,
                    .bitmap = bitmap,
                });
                try self.posting_index_by_term.put(term_key, posting_idx);
            }
        }
    }

    fn removeDocContributions(
        self: *Store,
        table_id: u64,
        doc_id: interfaces.DocId,
        token_count: usize,
        counts: *const std.AutoHashMap(u64, u32),
    ) !void {
        // Collection stats.
        if (self.collection_total_length_by_table.getPtr(table_id)) |total_len| {
            total_len.* = if (total_len.* >= token_count) total_len.* - token_count else 0;
        }
        if (self.collection_stats_by_table.getPtr(table_id)) |stat| {
            if (stat.total_documents > 0) {
                stat.total_documents -= 1;
                if (stat.total_documents == 0) {
                    _ = self.collection_stats_by_table.remove(table_id);
                    _ = self.collection_total_length_by_table.remove(table_id);
                } else {
                    const remaining = self.collection_total_length_by_table.get(table_id) orelse 0;
                    stat.avg_document_length = @as(f64, @floatFromInt(remaining)) / @as(f64, @floatFromInt(stat.total_documents));
                }
            }
        }

        // Document stats.
        _ = self.document_stats_by_doc.remove(.{ .table_id = table_id, .doc_id = doc_id });

        // Per-term contributions.
        var it = counts.iterator();
        while (it.next()) |entry| {
            const term_hash = entry.key_ptr.*;
            const term_key = packTableTermKey(table_id, term_hash);

            _ = self.term_frequencies_by_doc_term.remove(
                .{ .table_id = table_id, .doc_id = doc_id, .term_hash = term_hash },
            );

            if (self.term_stats_by_term.getPtr(term_key)) |ts| {
                if (ts.document_frequency > 0) ts.document_frequency -= 1;
            }

            if (self.posting_index_by_term.get(term_key)) |posting_idx| {
                self.postings.items[posting_idx].bitmap.remove(@intCast(doc_id));
            }
        }
    }

    // ------------------------------------------------------------------

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
                const count_entry = try local_counts.getOrPut(token);
                if (count_entry.found_existing) {
                    count_entry.value_ptr.* += 1;
                } else {
                    count_entry.value_ptr.* = 1;
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
            const total_length = entry.value_ptr.total_length;
            const avg = if (total_docs == 0) 0.0 else @as(f64, @floatFromInt(total_length)) / @as(f64, @floatFromInt(total_docs));
            const stats: interfaces.CollectionStats = .{
                .total_documents = total_docs,
                .avg_document_length = avg,
            };
            try self.collection_stats.append(self.allocator, .{
                .table_id = entry.key_ptr.*,
                .stats = stats,
            });
            try self.collection_stats_by_table.put(entry.key_ptr.*, stats);
            try self.collection_total_length_by_table.put(entry.key_ptr.*, total_length);
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
        if (index < self.postings.items.len) {
            const bitmap = &self.postings.items[index].bitmap;
            // Return null rather than an empty bitmap so callers see consistent
            // null-means-absent semantics regardless of whether the term was
            // removed via delta or via a full rebuild.
            if (bitmap.isEmpty()) return null;
            return try bitmap.clone();
        }
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

/// Bounded heap top-k: O(N * D + N log K) instead of O(N * D + N log N).
fn searchNearest(ctx: *anyopaque, allocator: std.mem.Allocator, request: interfaces.VectorSearchRequest) anyerror![]interfaces.VectorSearchMatch {
    const self: *Store = @ptrCast(@alignCast(ctx));
    if (request.limit == 0) return allocator.alloc(interfaces.VectorSearchMatch, 0);

    var matches = try std.ArrayList(interfaces.VectorSearchMatch).initCapacity(allocator, @min(request.limit, self.embeddings.items.len));
    defer matches.deinit(allocator);

    for (self.embeddings.items) |entry| {
        if (entry.table_id != 1 and request.table_name.len > 0) {}
        const vector_score = vector_distance.score(request.metric, request.query_vector, entry.values);
        try vector_distance.insertTopMatch(allocator, &matches, .{
            .doc_id = entry.doc_id,
            .distance = vector_score.distance,
            .similarity = vector_score.similarity,
        }, request.limit);
    }

    std.mem.sort(interfaces.VectorSearchMatch, matches.items, {}, vector_distance.lessThan);
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

fn buildTokenCounts(counts: *std.AutoHashMap(u64, u32), tokens: []const u64) !void {
    for (tokens) |token| {
        const entry = try counts.getOrPut(token);
        if (entry.found_existing) {
            entry.value_ptr.* += 1;
        } else {
            entry.value_ptr.* = 1;
        }
    }
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

test "search store bulk load matches incremental load" {
    const content_a = "zig sqlite roaring";
    const content_b = "graph traversal roaring";

    var incremental = Store.init(std.testing.allocator);
    defer incremental.deinit();
    try incremental.upsertDocument(.{ .table_id = 1, .doc_id = 1, .content = content_a, .language = "english" });
    try incremental.upsertDocument(.{ .table_id = 1, .doc_id = 2, .content = content_b, .language = "english" });

    var bulk = Store.init(std.testing.allocator);
    defer bulk.deinit();
    bulk.beginBulkLoad();
    try bulk.upsertDocument(.{ .table_id = 1, .doc_id = 1, .content = content_a, .language = "english" });
    try bulk.upsertDocument(.{ .table_id = 1, .doc_id = 2, .content = content_b, .language = "english" });
    try bulk.endBulkLoad();

    const inc_repo = incremental.bm25Repository();
    const bulk_repo = bulk.bm25Repository();

    const inc_col = try inc_repo.getCollectionStatsFn(inc_repo.ctx, std.testing.allocator, 1);
    const bulk_col = try bulk_repo.getCollectionStatsFn(bulk_repo.ctx, std.testing.allocator, 1);
    try std.testing.expectEqual(inc_col.total_documents, bulk_col.total_documents);

    const roaring_hash = tokenization_sqlite.hashTermLexeme("roaring");
    var inc_posting = (try inc_repo.getPostingBitmapFn(inc_repo.ctx, std.testing.allocator, 1, roaring_hash)).?;
    defer inc_posting.deinit();
    var bulk_posting = (try bulk_repo.getPostingBitmapFn(bulk_repo.ctx, std.testing.allocator, 1, roaring_hash)).?;
    defer bulk_posting.deinit();
    try std.testing.expectEqual(inc_posting.cardinality(), bulk_posting.cardinality());
    try std.testing.expect(bulk_posting.contains(1));
    try std.testing.expect(bulk_posting.contains(2));
}
