const std = @import("std");
const interfaces = @import("interfaces.zig");
const roaring = @import("roaring.zig");
const vector_distance = @import("vector_distance.zig");

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

const EmbeddingEntry = struct {
    table_id: u64,
    doc_id: interfaces.DocId,
    values: []const f32,
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
    collection_stats: std.ArrayList(interfaces.CollectionStatsEntry),
    document_stats: std.ArrayList(DocumentStatEntry),
    term_stats: std.ArrayList(TermStatEntry),
    term_frequencies: std.ArrayList(TermFrequencyEntry),
    postings: std.ArrayList(PostingEntry),
    embeddings: std.ArrayList(EmbeddingEntry),
    // Keyed maps for O(1) reads; built by buildIndexes() after all rows are loaded.
    collection_stats_by_table: std.AutoHashMap(u64, interfaces.CollectionStats),
    document_stats_by_doc: std.AutoHashMap(TableDocKey, interfaces.DocumentStats),
    term_stats_by_term: std.AutoHashMap(u128, interfaces.TermStat),
    term_frequencies_by_doc_term: std.AutoHashMap(TableDocTermKey, u32),
    posting_index_by_term: std.AutoHashMap(u128, usize),

    pub fn init(allocator: std.mem.Allocator) Store {
        return .{
            .allocator = allocator,
            .collection_stats = .empty,
            .document_stats = .empty,
            .term_stats = .empty,
            .term_frequencies = .empty,
            .postings = .empty,
            .embeddings = .empty,
            .collection_stats_by_table = std.AutoHashMap(u64, interfaces.CollectionStats).init(allocator),
            .document_stats_by_doc = std.AutoHashMap(TableDocKey, interfaces.DocumentStats).init(allocator),
            .term_stats_by_term = std.AutoHashMap(u128, interfaces.TermStat).init(allocator),
            .term_frequencies_by_doc_term = std.AutoHashMap(TableDocTermKey, u32).init(allocator),
            .posting_index_by_term = std.AutoHashMap(u128, usize).init(allocator),
        };
    }

    pub fn deinit(self: *Store) void {
        self.collection_stats.deinit(self.allocator);
        self.document_stats.deinit(self.allocator);
        self.term_stats.deinit(self.allocator);
        self.term_frequencies.deinit(self.allocator);
        for (self.postings.items) |*entry| entry.bitmap.deinit();
        self.postings.deinit(self.allocator);
        for (self.embeddings.items) |entry| self.allocator.free(entry.values);
        self.embeddings.deinit(self.allocator);
        self.collection_stats_by_table.deinit();
        self.document_stats_by_doc.deinit();
        self.term_stats_by_term.deinit();
        self.term_frequencies_by_doc_term.deinit();
        self.posting_index_by_term.deinit();
        self.* = undefined;
    }

    /// Build keyed lookup maps from the existing ArrayList rows.
    /// Call once after all rows have been appended (e.g. at the end of loadCompactSearchStore).
    pub fn buildIndexes(self: *Store) !void {
        self.collection_stats_by_table.clearRetainingCapacity();
        for (self.collection_stats.items) |entry| {
            try self.collection_stats_by_table.put(entry.table_id, entry.stats);
        }

        self.document_stats_by_doc.clearRetainingCapacity();
        for (self.document_stats.items) |entry| {
            try self.document_stats_by_doc.put(
                .{ .table_id = entry.table_id, .doc_id = entry.stats.doc_id },
                entry.stats,
            );
        }

        self.term_stats_by_term.clearRetainingCapacity();
        for (self.term_stats.items) |entry| {
            try self.term_stats_by_term.put(
                packTableTermKey(entry.table_id, entry.stat.term_hash),
                entry.stat,
            );
        }

        self.term_frequencies_by_doc_term.clearRetainingCapacity();
        for (self.term_frequencies.items) |entry| {
            try self.term_frequencies_by_doc_term.put(
                .{
                    .table_id = entry.table_id,
                    .doc_id = entry.doc_id,
                    .term_hash = entry.frequency.term_hash,
                },
                entry.frequency.frequency,
            );
        }

        self.posting_index_by_term.clearRetainingCapacity();
        for (self.postings.items, 0..) |entry, i| {
            try self.posting_index_by_term.put(
                packTableTermKey(entry.table_id, entry.term_hash),
                i,
            );
        }
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
            .upsertDocumentFn = unsupportedUpsertDocument,
            .deleteDocumentFn = unsupportedDeleteDocument,
        };
    }

    pub fn vectorRepository(self: *Store) interfaces.VectorRepository {
        return .{
            .ctx = @ptrCast(self),
            .getEmbeddingDimensionsFn = getEmbeddingDimensions,
            .searchNearestFn = searchNearest,
            .upsertEmbeddingFn = unsupportedUpsertEmbedding,
            .deleteEmbeddingFn = unsupportedDeleteEmbedding,
        };
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
    return self.document_stats_by_doc.get(.{ .table_id = table_id, .doc_id = doc_id });
}

fn getDocumentStatsBatch(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_ids: []const interfaces.DocId) anyerror![]interfaces.DocumentStats {
    const self: *Store = @ptrCast(@alignCast(ctx));
    var rows = std.ArrayList(interfaces.DocumentStats).empty;
    defer rows.deinit(allocator);
    for (doc_ids) |doc_id| {
        if (self.document_stats_by_doc.get(.{ .table_id = table_id, .doc_id = doc_id })) |stats| {
            try rows.append(allocator, stats);
        }
    }
    return rows.toOwnedSlice(allocator);
}

fn getTermStats(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, term_hashes: []const u64) anyerror![]interfaces.TermStat {
    const self: *Store = @ptrCast(@alignCast(ctx));
    const rows = try allocator.alloc(interfaces.TermStat, term_hashes.len);
    for (term_hashes, 0..) |term_hash, index| {
        rows[index] = self.term_stats_by_term.get(packTableTermKey(table_id, term_hash)) orelse .{
            .term_hash = term_hash,
            .document_frequency = 0,
        };
    }
    return rows;
}

fn getTermFrequencies(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId, term_hashes: []const u64) anyerror![]interfaces.TermFrequency {
    const self: *Store = @ptrCast(@alignCast(ctx));
    var rows = std.ArrayList(interfaces.TermFrequency).empty;
    defer rows.deinit(allocator);
    for (term_hashes) |term_hash| {
        if (self.term_frequencies_by_doc_term.get(.{
            .table_id = table_id,
            .doc_id = doc_id,
            .term_hash = term_hash,
        })) |frequency| {
            try rows.append(allocator, .{ .term_hash = term_hash, .frequency = frequency });
        }
    }
    return rows.toOwnedSlice(allocator);
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
        if (request.table_id) |tid| {
            if (entry.table_id != tid) continue;
        }
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

fn unsupportedUpsertDocument(ctx: *anyopaque, allocator: std.mem.Allocator, request: interfaces.UpsertDocumentRequest) anyerror!void {
    _ = ctx;
    _ = allocator;
    _ = request;
    return error.UnsupportedOperation;
}

fn unsupportedDeleteDocument(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId) anyerror!void {
    _ = ctx;
    _ = allocator;
    _ = table_id;
    _ = doc_id;
    return error.UnsupportedOperation;
}

fn unsupportedUpsertEmbedding(ctx: *anyopaque, allocator: std.mem.Allocator, request: interfaces.UpsertEmbeddingRequest) anyerror!void {
    _ = ctx;
    _ = allocator;
    _ = request;
    return error.UnsupportedOperation;
}

fn unsupportedDeleteEmbedding(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId) anyerror!void {
    _ = ctx;
    _ = allocator;
    _ = table_id;
    _ = doc_id;
    return error.UnsupportedOperation;
}

fn packTableTermKey(table_id: u64, term_hash: u64) u128 {
    return (@as(u128, table_id) << 64) | @as(u128, term_hash);
}
