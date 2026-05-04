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

pub const Store = struct {
    allocator: std.mem.Allocator,
    collection_stats: std.ArrayList(interfaces.CollectionStatsEntry),
    document_stats: std.ArrayList(DocumentStatEntry),
    term_stats: std.ArrayList(TermStatEntry),
    term_frequencies: std.ArrayList(TermFrequencyEntry),
    postings: std.ArrayList(PostingEntry),
    embeddings: std.ArrayList(EmbeddingEntry),

    pub fn init(allocator: std.mem.Allocator) Store {
        return .{
            .allocator = allocator,
            .collection_stats = .empty,
            .document_stats = .empty,
            .term_stats = .empty,
            .term_frequencies = .empty,
            .postings = .empty,
            .embeddings = .empty,
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
        self.* = undefined;
    }

    pub fn bm25Repository(self: *Store) interfaces.Bm25Repository {
        return .{
            .ctx = @ptrCast(self),
            .getCollectionStatsFn = getCollectionStats,
            .getDocumentStatsFn = getDocumentStats,
            .getTermStatsFn = getTermStats,
            .getTermFrequenciesFn = getTermFrequencies,
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

fn getTermStats(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, term_hashes: []const u64) anyerror![]interfaces.TermStat {
    const self: *Store = @ptrCast(@alignCast(ctx));
    const rows = try allocator.alloc(interfaces.TermStat, term_hashes.len);
    for (term_hashes, 0..) |term_hash, index| {
        rows[index] = .{ .term_hash = term_hash, .document_frequency = 0 };
        for (self.term_stats.items) |entry| {
            if (entry.table_id == table_id and entry.stat.term_hash == term_hash) {
                rows[index] = entry.stat;
                break;
            }
        }
    }
    return rows;
}

fn getTermFrequencies(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId, term_hashes: []const u64) anyerror![]interfaces.TermFrequency {
    const self: *Store = @ptrCast(@alignCast(ctx));
    var rows = std.ArrayList(interfaces.TermFrequency).empty;
    defer rows.deinit(allocator);
    for (term_hashes) |term_hash| {
        for (self.term_frequencies.items) |entry| {
            if (entry.table_id == table_id and entry.doc_id == doc_id and entry.frequency.term_hash == term_hash) {
                try rows.append(allocator, entry.frequency);
                break;
            }
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
