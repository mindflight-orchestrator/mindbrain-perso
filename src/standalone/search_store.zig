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

pub const Store = struct {
    allocator: std.mem.Allocator,
    documents: std.ArrayList(DocumentRecord),
    embeddings: std.ArrayList(EmbeddingEntry),
    bm25_stopwords: bm25_stopwords_sqlite.StopwordCache,
    /// When set (e.g. by `loadSearchStore`), BM25 tokenization loads `bm25_stopwords` for each document language.
    bm25_db: ?facet_sqlite.Database = null,

    pub fn init(allocator: std.mem.Allocator) Store {
        return .{
            .allocator = allocator,
            .documents = .empty,
            .embeddings = .empty,
            .bm25_stopwords = bm25_stopwords_sqlite.StopwordCache.init(allocator),
            .bm25_db = null,
        };
    }

    pub fn deinit(self: *Store) void {
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

    pub fn bm25Repository(self: *Store) interfaces.Bm25Repository {
        return .{
            .ctx = @ptrCast(self),
            .getCollectionStatsFn = getCollectionStats,
            .getDocumentStatsFn = getDocumentStats,
            .getTermStatsFn = getTermStats,
            .getTermFrequenciesFn = getTermFrequencies,
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
                return;
            }
        }

        try self.documents.append(self.allocator, .{
            .table_id = request.table_id,
            .doc_id = request.doc_id,
            .content = try self.allocator.dupe(u8, request.content),
            .language = try self.allocator.dupe(u8, request.language),
        });
    }

    pub fn deleteDocument(self: *Store, table_id: u64, doc_id: interfaces.DocId) !void {
        var i: usize = 0;
        while (i < self.documents.items.len) : (i += 1) {
            if (self.documents.items[i].table_id != table_id or self.documents.items[i].doc_id != doc_id) continue;
            self.allocator.free(self.documents.items[i].content);
            self.allocator.free(self.documents.items[i].language);
            _ = self.documents.swapRemove(i);
            return;
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

    var total_documents: u64 = 0;
    var total_length: u64 = 0;
    for (self.documents.items) |doc| {
        if (doc.table_id != table_id) continue;
        total_documents += 1;
        total_length += try countTokens(self, doc.content, doc.language);
    }

    return .{
        .total_documents = total_documents,
        .avg_document_length = if (total_documents == 0) 0.0 else @as(f64, @floatFromInt(total_length)) / @as(f64, @floatFromInt(total_documents)),
    };
}

fn getDocumentStats(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId) anyerror!?interfaces.DocumentStats {
    _ = allocator;
    const self: *Store = @ptrCast(@alignCast(ctx));
    for (self.documents.items) |doc| {
        if (doc.table_id != table_id or doc.doc_id != doc_id) continue;

        const tokens = try tokenizeForBm25(std.heap.page_allocator, self, doc.content, doc.language);
        defer std.heap.page_allocator.free(tokens);

        var unique = std.AutoHashMap(u64, void).init(std.heap.page_allocator);
        defer unique.deinit();
        for (tokens) |token| try unique.put(token, {});

        return .{
            .doc_id = doc_id,
            .document_length = @intCast(tokens.len),
            .unique_terms = @intCast(unique.count()),
        };
    }
    return null;
}

fn getTermStats(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, term_hashes: []const u64) anyerror![]interfaces.TermStat {
    const self: *Store = @ptrCast(@alignCast(ctx));
    const stats = try allocator.alloc(interfaces.TermStat, term_hashes.len);

    for (term_hashes, 0..) |term_hash, index| {
        var document_frequency: u64 = 0;
        for (self.documents.items) |doc| {
            if (doc.table_id != table_id) continue;
            const tokens = try tokenizeForBm25(std.heap.page_allocator, self, doc.content, doc.language);
            defer std.heap.page_allocator.free(tokens);
            if (containsToken(tokens, term_hash)) document_frequency += 1;
        }

        stats[index] = .{
            .term_hash = term_hash,
            .document_frequency = document_frequency,
        };
    }

    return stats;
}

fn getTermFrequencies(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId, term_hashes: []const u64) anyerror![]interfaces.TermFrequency {
    const self: *Store = @ptrCast(@alignCast(ctx));
    for (self.documents.items) |doc| {
        if (doc.table_id != table_id or doc.doc_id != doc_id) continue;
        const tokens = try tokenizeForBm25(std.heap.page_allocator, self, doc.content, doc.language);
        defer std.heap.page_allocator.free(tokens);

        var freqs = std.ArrayList(interfaces.TermFrequency).empty;
        defer freqs.deinit(allocator);
        for (term_hashes) |term_hash| {
            const count = countTokenFrequency(tokens, term_hash);
            if (count == 0) continue;
            try freqs.append(allocator, .{ .term_hash = term_hash, .frequency = count });
        }
        return freqs.toOwnedSlice(allocator);
    }
    return allocator.alloc(interfaces.TermFrequency, 0);
}

fn getPostingBitmap(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, term_hash: u64) anyerror!?roaring.Bitmap {
    _ = allocator;
    const self: *Store = @ptrCast(@alignCast(ctx));
    var matches = std.ArrayList(u32).empty;
    defer matches.deinit(std.heap.page_allocator);

    for (self.documents.items) |doc| {
        if (doc.table_id != table_id) continue;
        const tokens = try tokenizeForBm25(std.heap.page_allocator, self, doc.content, doc.language);
        defer std.heap.page_allocator.free(tokens);
        if (containsToken(tokens, term_hash)) try matches.append(std.heap.page_allocator, @intCast(doc.doc_id));
    }

    if (matches.items.len == 0) return null;
    return try roaring.Bitmap.fromSlice(matches.items);
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
