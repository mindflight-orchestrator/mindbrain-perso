const std = @import("std");
const interfaces = @import("interfaces.zig");
const facet_sqlite = @import("facet_sqlite.zig");
const search_store = @import("search_store.zig");
const search_compact_store = @import("search_compact_store.zig");
const vector_blob = @import("vector_blob.zig");
const workspace_sqlite = @import("workspace_sqlite.zig");
const toon_exports = @import("toon_exports.zig");
const roaring = @import("roaring.zig");
const bm25_stopwords_sqlite = @import("bm25_stopwords_sqlite.zig");
const tokenization_sqlite = @import("tokenization_sqlite.zig");

const c = facet_sqlite.c;
const Database = facet_sqlite.Database;

pub const CompactSearchSnapshot = struct {
    collection_stats: usize,
    document_stats: usize,
    term_stats: usize,
    term_frequencies: usize,
    postings: usize,
    embeddings: usize,
};

pub const Bm25Match = struct {
    doc_id: interfaces.DocId,
    score: f64,
};

pub const SearchDocumentRow = struct {
    table_id: u64,
    doc_id: interfaces.DocId,
    content: []u8,
    language: []u8,

    pub fn deinit(self: SearchDocumentRow, allocator: std.mem.Allocator) void {
        allocator.free(self.content);
        allocator.free(self.language);
    }
};

pub const SearchTableSetupSpec = struct {
    table_id: u64,
    workspace_id: []const u8 = "default",
    schema_name: []const u8,
    table_name: []const u8,
    key_column: []const u8 = "id",
    content_column: []const u8 = "content",
    metadata_column: []const u8 = "metadata",
    language: []const u8 = "english",
    populate: bool = false,
};

pub const Bm25SyncTriggerSpec = struct {
    table_id: u64,
    id_column: []const u8 = "id",
    content_column: []const u8 = "content",
    language: []const u8 = "english",
};

pub fn setupSearchTable(
    db: Database,
    allocator: std.mem.Allocator,
    spec: SearchTableSetupSpec,
) !void {
    try workspace_sqlite.upsertTableSemantic(
        db,
        spec.table_id,
        spec.workspace_id,
        spec.schema_name,
        spec.table_name,
        spec.key_column,
        spec.content_column,
        spec.metadata_column,
        null,
        spec.language,
    );

    if (spec.populate) {
        try rebuildSearchArtifacts(db, allocator);
    }
}

pub fn bm25CreateSyncTrigger(
    db: Database,
    spec: Bm25SyncTriggerSpec,
) !void {
    const stmt = try prepare(db, "INSERT OR REPLACE INTO bm25_sync_triggers(table_id, id_column, content_column, language) VALUES (?1, ?2, ?3, ?4)");
    defer finalize(stmt);
    try bindInt64(stmt, 1, spec.table_id);
    try bindText(stmt, 2, spec.id_column);
    try bindText(stmt, 3, spec.content_column);
    try bindText(stmt, 4, spec.language);
    try stepDone(stmt);
}

pub fn bm25DropSyncTrigger(db: Database, table_id: u64) !void {
    const stmt = try prepare(db, "DELETE FROM bm25_sync_triggers WHERE table_id = ?1");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try stepDone(stmt);
}

pub fn hasBm25SyncTrigger(db: Database, table_id: u64) !bool {
    const stmt = try prepare(db, "SELECT 1 FROM bm25_sync_triggers WHERE table_id = ?1 LIMIT 1");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    return c.sqlite3_step(stmt) == c.SQLITE_ROW;
}

pub const Bm25WorkerRange = struct {
    start_id: u64,
    end_id: u64,
};

/// Compute an evenly partitioned worker range for a logical worker, parity
/// for `facets.bm25_get_worker_range`. SQLite has no parallel worker
/// pool, but the helper is reused by callers that fan work out across
/// processes or threads. The semantics match the PostgreSQL function:
/// the range is half-open `[start_id, end_id)`, with the last worker
/// absorbing any remainder.
pub fn bm25GetWorkerRange(
    total: u64,
    worker_id: u64,
    worker_count: u64,
) !Bm25WorkerRange {
    if (worker_count == 0) return error.ValueOutOfRange;
    if (worker_id >= worker_count) return error.ValueOutOfRange;

    const base = total / worker_count;
    const remainder = total % worker_count;
    const start_id = base * worker_id + @min(worker_id, remainder);
    const this_size = base + (if (worker_id < remainder) @as(u64, 1) else 0);
    return .{
        .start_id = start_id,
        .end_id = start_id + this_size,
    };
}

pub fn syncSearchDocument(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    doc_id: u64,
    content: []const u8,
    language: []const u8,
) !void {
    try upsertSearchDocument(db, table_id, doc_id, content, language);
    _ = allocator;
}

pub fn syncSearchDocumentIfTriggered(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    doc_id: u64,
    content: []const u8,
    language: []const u8,
) !void {
    if (!try hasBm25SyncTrigger(db, table_id)) return;
    try syncSearchDocument(db, allocator, table_id, doc_id, content, language);
}

pub fn syncDeleteSearchDocument(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    doc_id: u64,
) !void {
    try deleteSearchDocument(db, table_id, doc_id);
    try rebuildSearchArtifacts(db, allocator);
}

pub fn syncDeleteSearchDocumentIfTriggered(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    doc_id: u64,
) !void {
    if (!try hasBm25SyncTrigger(db, table_id)) return;
    try syncDeleteSearchDocument(db, allocator, table_id, doc_id);
}

pub fn upsertSearchDocument(
    db: Database,
    table_id: u64,
    doc_id: u64,
    content: []const u8,
    language: []const u8,
) !void {
    const sql =
        "INSERT OR REPLACE INTO search_documents(table_id, doc_id, content, language) VALUES (?1, ?2, ?3, ?4)";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, doc_id);
    try bindText(stmt, 3, content);
    try bindText(stmt, 4, language);
    try stepDone(stmt);

    try upsertSearchFtsDocument(db, table_id, doc_id, content);
}

pub fn upsertSearchEmbedding(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    doc_id: u64,
    values: []const f32,
) !void {
    const sql =
        "INSERT OR REPLACE INTO search_embeddings(table_id, doc_id, dimensions, embedding_blob) VALUES (?1, ?2, ?3, ?4)";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    const bytes = try encodeEmbedding(allocator, values);
    defer allocator.free(bytes);

    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, doc_id);
    try bindInt64(stmt, 3, values.len);
    try bindBlob(stmt, 4, bytes);
    try stepDone(stmt);
}

pub fn searchFts5Bm25(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    query: []const u8,
    limit: usize,
) ![]Bm25Match {
    if (limit == 0) return allocator.alloc(Bm25Match, 0);

    const match_query = try buildFts5OrQuery(allocator, query);
    defer allocator.free(match_query);
    if (match_query.len == 0) return allocator.alloc(Bm25Match, 0);

    const sql =
        \\SELECT d.doc_id, -bm25(search_fts) AS score
        \\FROM search_fts
        \\JOIN search_fts_docs d ON d.fts_rowid = search_fts.rowid
        \\WHERE d.table_id = ?1 AND search_fts MATCH ?2
        \\ORDER BY bm25(search_fts) ASC
        \\LIMIT ?3
    ;
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, table_id);
    try bindText(stmt, 2, match_query);
    try bindInt64(stmt, 3, limit);

    var matches = std.ArrayList(Bm25Match).empty;
    defer matches.deinit(allocator);

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try matches.append(allocator, .{
            .doc_id = try columnDocId(stmt, 0),
            .score = c.sqlite3_column_double(stmt, 1),
        });
    }

    return matches.toOwnedSlice(allocator);
}

pub fn countSearchEmbeddings(db: Database, table_id: u64) !u64 {
    const stmt = try prepare(db, "SELECT COUNT(*) FROM search_embeddings WHERE table_id = ?1");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StepFailed;
    return try columnU64(stmt, 0);
}

pub fn loadSearchDocumentsForEmbeddingBatch(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    limit: usize,
    missing_only: bool,
) ![]SearchDocumentRow {
    if (limit == 0) return allocator.alloc(SearchDocumentRow, 0);

    const sql =
        \\SELECT d.table_id, d.doc_id, d.content, d.language
        \\FROM search_documents d
        \\WHERE d.table_id = ?1
        \\  AND (?2 = 0 OR NOT EXISTS (
        \\      SELECT 1 FROM search_embeddings e
        \\      WHERE e.table_id = d.table_id AND e.doc_id = d.doc_id
        \\  ))
        \\ORDER BY d.doc_id
        \\LIMIT ?3
    ;
    const stmt = try prepare(db, sql);
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, @as(i64, if (missing_only) 1 else 0));
    try bindInt64(stmt, 3, limit);

    var rows = std.ArrayList(SearchDocumentRow).empty;
    errdefer {
        for (rows.items) |row| row.deinit(allocator);
        rows.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        {
            const row_table_id = try columnU64(stmt, 0);
            const row_doc_id = try columnDocId(stmt, 1);
            const row_content = try columnTextOwned(allocator, stmt, 2);
            errdefer allocator.free(row_content);
            const row_language = try columnTextOwned(allocator, stmt, 3);
            var row = SearchDocumentRow{
                .table_id = row_table_id,
                .doc_id = row_doc_id,
                .content = row_content,
                .language = row_language,
            };
            errdefer row.deinit(allocator);
            try rows.append(allocator, row);
        }
    }

    return rows.toOwnedSlice(allocator);
}

pub fn deleteSearchDocument(db: Database, table_id: u64, doc_id: u64) !void {
    try deleteSearchFtsDocument(db, table_id, doc_id);

    const embedding_sql = "DELETE FROM search_embeddings WHERE table_id = ?1 AND doc_id = ?2";
    const embedding_stmt = try prepare(db, embedding_sql);
    defer finalize(embedding_stmt);
    try bindInt64(embedding_stmt, 1, table_id);
    try bindInt64(embedding_stmt, 2, doc_id);
    try stepDone(embedding_stmt);

    const doc_sql = "DELETE FROM search_documents WHERE table_id = ?1 AND doc_id = ?2";
    const doc_stmt = try prepare(db, doc_sql);
    defer finalize(doc_stmt);
    try bindInt64(doc_stmt, 1, table_id);
    try bindInt64(doc_stmt, 2, doc_id);
    try stepDone(doc_stmt);
}

fn upsertSearchFtsDocument(db: Database, table_id: u64, doc_id: u64, content: []const u8) !void {
    const fts_rowid = try ensureFtsRowid(db, table_id, doc_id);
    try deleteSearchFtsRow(db, fts_rowid);

    const stmt = try prepare(db, "INSERT INTO search_fts(rowid, content) VALUES (?1, ?2)");
    defer finalize(stmt);
    try bindInt64(stmt, 1, fts_rowid);
    try bindText(stmt, 2, content);
    try stepDone(stmt);
}

fn deleteSearchFtsDocument(db: Database, table_id: u64, doc_id: u64) !void {
    const maybe_rowid = try loadFtsRowid(db, table_id, doc_id);
    if (maybe_rowid) |fts_rowid| {
        try deleteSearchFtsRow(db, fts_rowid);
    }

    const stmt = try prepare(db, "DELETE FROM search_fts_docs WHERE table_id = ?1 AND doc_id = ?2");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, doc_id);
    try stepDone(stmt);
}

fn deleteSearchFtsRow(db: Database, fts_rowid: u64) !void {
    const stmt = try prepare(db, "DELETE FROM search_fts WHERE rowid = ?1");
    defer finalize(stmt);
    try bindInt64(stmt, 1, fts_rowid);
    try stepDone(stmt);
}

fn ensureFtsRowid(db: Database, table_id: u64, doc_id: u64) !u64 {
    if (try loadFtsRowid(db, table_id, doc_id)) |fts_rowid| return fts_rowid;

    const insert_stmt = try prepare(db, "INSERT INTO search_fts_docs(table_id, doc_id) VALUES (?1, ?2)");
    defer finalize(insert_stmt);
    try bindInt64(insert_stmt, 1, table_id);
    try bindInt64(insert_stmt, 2, doc_id);
    try stepDone(insert_stmt);

    const rowid = c.sqlite3_last_insert_rowid(db.handle);
    if (rowid < 0) return error.ValueOutOfRange;
    return @intCast(rowid);
}

fn loadFtsRowid(db: Database, table_id: u64, doc_id: u64) !?u64 {
    const stmt = try prepare(db, "SELECT fts_rowid FROM search_fts_docs WHERE table_id = ?1 AND doc_id = ?2");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, doc_id);

    const rc = c.sqlite3_step(stmt);
    if (rc == c.SQLITE_DONE) return null;
    if (rc != c.SQLITE_ROW) return error.StepFailed;
    return try columnU64(stmt, 0);
}

pub fn loadSearchStore(db: Database, allocator: std.mem.Allocator) !search_store.Store {
    var store = search_store.Store.init(allocator);
    errdefer store.deinit();
    store.bm25_db = db;

    const doc_sql = "SELECT table_id, doc_id, content, language FROM search_documents ORDER BY table_id, doc_id";
    const doc_stmt = try prepare(db, doc_sql);
    defer finalize(doc_stmt);

    while (true) {
        const rc = c.sqlite3_step(doc_stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const content = try columnTextOwned(allocator, doc_stmt, 2);
        defer allocator.free(content);
        const language = try columnTextOwned(allocator, doc_stmt, 3);
        defer allocator.free(language);

        try store.upsertDocument(.{
            .table_id = try columnU64(doc_stmt, 0),
            .doc_id = try columnDocId(doc_stmt, 1),
            .content = content,
            .language = language,
        });
    }

    const embedding_sql = "SELECT table_id, doc_id, dimensions, embedding_blob FROM search_embeddings ORDER BY table_id, doc_id";
    const embedding_stmt = try prepare(db, embedding_sql);
    defer finalize(embedding_stmt);

    while (true) {
        const rc = c.sqlite3_step(embedding_stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const dimensions = try columnUsize(embedding_stmt, 2);
        const values = try decodeEmbedding(allocator, embedding_stmt, 3, dimensions);
        defer allocator.free(values);

        try store.upsertEmbedding(.{
            .table_id = try columnU64(embedding_stmt, 0),
            .doc_id = try columnDocId(embedding_stmt, 1),
            .values = values,
        });
    }

    return store;
}

pub fn rebuildSearchArtifacts(db: Database, allocator: std.mem.Allocator) !void {
    try db.exec("DELETE FROM search_collection_stats");
    try db.exec("DELETE FROM search_document_stats");
    try db.exec("DELETE FROM search_term_stats");
    try db.exec("DELETE FROM search_term_frequencies");
    try db.exec("DELETE FROM search_postings");
    try db.exec("DELETE FROM search_fts");
    try db.exec("DELETE FROM search_fts_docs");

    var writer = try ArtifactWriter.init(db);
    defer writer.deinit();

    var stop_cache = bm25_stopwords_sqlite.StopwordCache.init(allocator);
    defer stop_cache.deinit();

    const doc_sql = "SELECT table_id, doc_id, content, language FROM search_documents ORDER BY table_id, doc_id";
    const doc_stmt = try prepare(db, doc_sql);
    defer finalize(doc_stmt);

    var doc_lengths = std.AutoHashMap(u64, struct { total_docs: u64, total_length: u64 }).init(allocator);
    defer doc_lengths.deinit();
    var term_doc_freq = std.AutoHashMap(u128, u64).init(allocator);
    defer term_doc_freq.deinit();

    while (true) {
        const rc = c.sqlite3_step(doc_stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const table_id = try columnU64(doc_stmt, 0);
        const doc_id = try columnDocId(doc_stmt, 1);
        const content = try columnTextOwned(allocator, doc_stmt, 2);
        defer allocator.free(content);
        const language = try columnTextOwned(allocator, doc_stmt, 3);
        defer allocator.free(language);

        try upsertSearchFtsDocument(db, table_id, doc_id, content);

        const sw = try stop_cache.sliceFor(db, language);
        const tokens = try tokenization_sqlite.tokenizeSearchHashes(allocator, content, sw);
        defer allocator.free(tokens);

        var local_counts = std.AutoHashMap(u64, u32).init(allocator);
        defer local_counts.deinit();
        for (tokens) |token| {
            const entry = try local_counts.getOrPut(token);
            if (entry.found_existing) {
                entry.value_ptr.* += 1;
            } else {
                entry.value_ptr.* = 1;
            }
        }

        try writer.upsertDocumentStat(table_id, doc_id, tokens.len, local_counts.count());

        const doc_length_entry = try doc_lengths.getOrPut(table_id);
        if (doc_length_entry.found_existing) {
            doc_length_entry.value_ptr.total_docs += 1;
            doc_length_entry.value_ptr.total_length += tokens.len;
        } else {
            doc_length_entry.value_ptr.* = .{ .total_docs = 1, .total_length = tokens.len };
        }

        var local_it = local_counts.iterator();
        while (local_it.next()) |entry| {
            try writer.upsertTermFrequency(table_id, doc_id, entry.key_ptr.*, entry.value_ptr.*);

            const key = makeArtifactKey(table_id, entry.key_ptr.*);
            const df_entry = try term_doc_freq.getOrPut(key);
            if (df_entry.found_existing) {
                df_entry.value_ptr.* += 1;
            } else {
                df_entry.value_ptr.* = 1;
            }
        }
    }

    var doc_length_it = doc_lengths.iterator();
    while (doc_length_it.next()) |entry| {
        const avg_length = if (entry.value_ptr.total_docs == 0) 0.0 else @as(f64, @floatFromInt(entry.value_ptr.total_length)) / @as(f64, @floatFromInt(entry.value_ptr.total_docs));
        try writer.upsertCollectionStat(entry.key_ptr.*, entry.value_ptr.total_docs, entry.value_ptr.total_length, avg_length);
    }

    var df_it = term_doc_freq.iterator();
    while (df_it.next()) |entry| {
        const unpacked = unpackArtifactKey(entry.key_ptr.*);
        try writer.upsertTermStat(unpacked.table_id, unpacked.term_hash, entry.value_ptr.*);
    }
}

pub fn upsertSearchArtifactsForDocument(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    doc_id: u64,
) !void {
    const row = try loadSearchDocumentContentAndLanguage(db, allocator, table_id, doc_id);
    defer allocator.free(row.content);
    defer allocator.free(row.language);
    try upsertSearchFtsDocument(db, table_id, doc_id, row.content);
    try rebuildSearchArtifacts(db, allocator);
}

pub fn deleteSearchArtifactsForDocument(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    doc_id: u64,
) !void {
    try deleteSearchFtsDocument(db, table_id, doc_id);
    try rebuildSearchArtifacts(db, allocator);
}

pub fn loadCompactSearchStore(db: Database, allocator: std.mem.Allocator) !search_compact_store.Store {
    var store = search_compact_store.Store.init(allocator);
    errdefer store.deinit();

    const collection_stmt = try prepare(db, "SELECT table_id, total_documents, avg_document_length FROM search_collection_stats ORDER BY table_id");
    defer finalize(collection_stmt);
    while (true) {
        const rc = c.sqlite3_step(collection_stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try store.collection_stats.append(allocator, .{
            .table_id = try columnU64(collection_stmt, 0),
            .stats = .{
                .total_documents = try columnU64(collection_stmt, 1),
                .avg_document_length = c.sqlite3_column_double(collection_stmt, 2),
            },
        });
    }

    const doc_stmt = try prepare(db, "SELECT table_id, doc_id, document_length, unique_terms FROM search_document_stats ORDER BY table_id, doc_id");
    defer finalize(doc_stmt);
    while (true) {
        const rc = c.sqlite3_step(doc_stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try store.document_stats.append(allocator, .{
            .table_id = try columnU64(doc_stmt, 0),
            .stats = .{
                .doc_id = try columnDocId(doc_stmt, 1),
                .document_length = @intCast(try columnU64(doc_stmt, 2)),
                .unique_terms = @intCast(try columnU64(doc_stmt, 3)),
            },
        });
    }

    const term_stmt = try prepare(db, "SELECT table_id, term_hash, document_frequency FROM search_term_stats ORDER BY table_id, term_hash");
    defer finalize(term_stmt);
    while (true) {
        const rc = c.sqlite3_step(term_stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try store.term_stats.append(allocator, .{
            .table_id = try columnU64(term_stmt, 0),
            .stat = .{
                .term_hash = columnHashU64(term_stmt, 1),
                .document_frequency = try columnU64(term_stmt, 2),
            },
        });
    }

    const freq_stmt = try prepare(db, "SELECT table_id, doc_id, term_hash, frequency FROM search_term_frequencies ORDER BY table_id, doc_id, term_hash");
    defer finalize(freq_stmt);
    while (true) {
        const rc = c.sqlite3_step(freq_stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try store.term_frequencies.append(allocator, .{
            .table_id = try columnU64(freq_stmt, 0),
            .doc_id = try columnDocId(freq_stmt, 1),
            .frequency = .{
                .term_hash = columnHashU64(freq_stmt, 2),
                .frequency = @intCast(try columnU64(freq_stmt, 3)),
            },
        });
    }

    const posting_stmt = try prepare(db, "SELECT table_id, term_hash, posting_blob FROM search_postings ORDER BY table_id, term_hash");
    defer finalize(posting_stmt);
    while (true) {
        const rc = c.sqlite3_step(posting_stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        const blob_ptr = c.sqlite3_column_blob(posting_stmt, 2) orelse return error.MissingRow;
        const blob_len = c.sqlite3_column_bytes(posting_stmt, 2);
        const blob: []const u8 = @as([*]const u8, @ptrCast(blob_ptr))[0..@intCast(blob_len)];
        try store.postings.append(allocator, .{
            .table_id = try columnU64(posting_stmt, 0),
            .term_hash = columnHashU64(posting_stmt, 1),
            .bitmap = try roaring.Bitmap.deserializePortable(blob),
        });
    }

    const embedding_sql = "SELECT table_id, doc_id, dimensions, embedding_blob FROM search_embeddings ORDER BY table_id, doc_id";
    const embedding_stmt = try prepare(db, embedding_sql);
    defer finalize(embedding_stmt);
    while (true) {
        const rc = c.sqlite3_step(embedding_stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        const dimensions = try columnUsize(embedding_stmt, 2);
        try store.embeddings.append(allocator, .{
            .table_id = try columnU64(embedding_stmt, 0),
            .doc_id = try columnDocId(embedding_stmt, 1),
            .values = try decodeEmbedding(allocator, embedding_stmt, 3, dimensions),
        });
    }

    return store;
}

pub fn compactSearchSnapshot(db: Database, allocator: std.mem.Allocator) !CompactSearchSnapshot {
    var store = try loadCompactSearchStore(db, allocator);
    defer store.deinit();

    return .{
        .collection_stats = store.collection_stats.items.len,
        .document_stats = store.document_stats.items.len,
        .term_stats = store.term_stats.items.len,
        .term_frequencies = store.term_frequencies.items.len,
        .postings = store.postings.items.len,
        .embeddings = store.embeddings.items.len,
    };
}

pub fn compactSearchSnapshotToon(db: Database, allocator: std.mem.Allocator) ![]u8 {
    const snapshot = try compactSearchSnapshot(db, allocator);
    return try toon_exports.encodeSearchCompactSnapshotAlloc(allocator, snapshot, toon_exports.default_options);
}

test "search sqlite setup registers table semantics and syncs artifacts" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try workspace_sqlite.upsertWorkspace(db, "default", "{}");
    try setupSearchTable(db, std.testing.allocator, .{
        .table_id = 31,
        .workspace_id = "default",
        .schema_name = "public",
        .table_name = "documents",
        .key_column = "id",
        .content_column = "content",
        .metadata_column = "metadata",
        .language = "english",
        .populate = false,
    });

    const toon = try workspace_sqlite.exportWorkspaceModelToon(db, std.testing.allocator, "default");
    defer std.testing.allocator.free(toon);
    try std.testing.expect(std.mem.indexOf(u8, toon, "documents") != null);

    try syncSearchDocument(db, std.testing.allocator, 31, 7, "zig sqlite roaring", "english");
    try syncSearchDocument(db, std.testing.allocator, 31, 9, "graph traversal with roaring", "english");

    var store = try loadSearchStore(db, std.testing.allocator);
    defer store.deinit();

    const bm25_repo = store.bm25Repository();
    var bitmap = (try bm25_repo.getPostingBitmapFn(bm25_repo.ctx, std.testing.allocator, 31, tokenization_sqlite.hashTermLexeme("roaring"))).?;
    defer bitmap.deinit();
    try std.testing.expect(bitmap.contains(7));
    try std.testing.expect(bitmap.contains(9));

    try syncDeleteSearchDocument(db, std.testing.allocator, 31, 7);

    var reloaded = try loadSearchStore(db, std.testing.allocator);
    defer reloaded.deinit();
    const reloaded_repo = reloaded.bm25Repository();
    var remaining = (try reloaded_repo.getPostingBitmapFn(reloaded_repo.ctx, std.testing.allocator, 31, tokenization_sqlite.hashTermLexeme("roaring"))).?;
    defer remaining.deinit();
    try std.testing.expect(!remaining.contains(7));
    try std.testing.expect(remaining.contains(9));
}

fn upsertCollectionStat(db: Database, table_id: u64, total_documents: u64, total_document_length: u64, avg_document_length: f64) !void {
    const stmt = try prepare(db, "INSERT OR REPLACE INTO search_collection_stats(table_id, total_documents, total_document_length, avg_document_length) VALUES (?1, ?2, ?3, ?4)");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, total_documents);
    try bindInt64(stmt, 3, total_document_length);
    if (c.sqlite3_bind_double(stmt, 4, avg_document_length) != c.SQLITE_OK) return error.BindFailed;
    try stepDone(stmt);
}

fn deleteCollectionStat(db: Database, table_id: u64) !void {
    const stmt = try prepare(db, "DELETE FROM search_collection_stats WHERE table_id = ?1");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try stepDone(stmt);
}

const ArtifactWriter = struct {
    document_stat_stmt: *c.sqlite3_stmt,
    term_frequency_stmt: *c.sqlite3_stmt,
    collection_stat_stmt: *c.sqlite3_stmt,
    term_stat_stmt: *c.sqlite3_stmt,

    fn init(db: Database) !ArtifactWriter {
        return .{
            .document_stat_stmt = try prepare(db, "INSERT OR REPLACE INTO search_document_stats(table_id, doc_id, document_length, unique_terms) VALUES (?1, ?2, ?3, ?4)"),
            .term_frequency_stmt = try prepare(db, "INSERT OR REPLACE INTO search_term_frequencies(table_id, doc_id, term_hash, frequency) VALUES (?1, ?2, ?3, ?4)"),
            .collection_stat_stmt = try prepare(db, "INSERT OR REPLACE INTO search_collection_stats(table_id, total_documents, total_document_length, avg_document_length) VALUES (?1, ?2, ?3, ?4)"),
            .term_stat_stmt = try prepare(db, "INSERT OR REPLACE INTO search_term_stats(table_id, term_hash, document_frequency) VALUES (?1, ?2, ?3)"),
        };
    }

    fn deinit(self: *ArtifactWriter) void {
        finalize(self.document_stat_stmt);
        finalize(self.term_frequency_stmt);
        finalize(self.collection_stat_stmt);
        finalize(self.term_stat_stmt);
    }

    fn reset(stmt: *c.sqlite3_stmt) !void {
        if (c.sqlite3_reset(stmt) != c.SQLITE_OK) return error.ResetFailed;
        if (c.sqlite3_clear_bindings(stmt) != c.SQLITE_OK) return error.BindFailed;
    }

    fn upsertDocumentStat(self: *ArtifactWriter, table_id: u64, doc_id: u64, document_length: usize, unique_terms: usize) !void {
        try reset(self.document_stat_stmt);
        try bindInt64(self.document_stat_stmt, 1, table_id);
        try bindInt64(self.document_stat_stmt, 2, doc_id);
        try bindInt64(self.document_stat_stmt, 3, document_length);
        try bindInt64(self.document_stat_stmt, 4, unique_terms);
        try stepDone(self.document_stat_stmt);
    }

    fn upsertTermFrequency(self: *ArtifactWriter, table_id: u64, doc_id: u64, term_hash: u64, frequency: u32) !void {
        try reset(self.term_frequency_stmt);
        try bindInt64(self.term_frequency_stmt, 1, table_id);
        try bindInt64(self.term_frequency_stmt, 2, doc_id);
        try bindHashU64(self.term_frequency_stmt, 3, term_hash);
        try bindInt64(self.term_frequency_stmt, 4, frequency);
        try stepDone(self.term_frequency_stmt);
    }

    fn upsertCollectionStat(self: *ArtifactWriter, table_id: u64, total_documents: u64, total_document_length: u64, avg_document_length: f64) !void {
        try reset(self.collection_stat_stmt);
        try bindInt64(self.collection_stat_stmt, 1, table_id);
        try bindInt64(self.collection_stat_stmt, 2, total_documents);
        try bindInt64(self.collection_stat_stmt, 3, total_document_length);
        if (c.sqlite3_bind_double(self.collection_stat_stmt, 4, avg_document_length) != c.SQLITE_OK) return error.BindFailed;
        try stepDone(self.collection_stat_stmt);
    }

    fn upsertTermStat(self: *ArtifactWriter, table_id: u64, term_hash: u64, document_frequency: u64) !void {
        try reset(self.term_stat_stmt);
        try bindInt64(self.term_stat_stmt, 1, table_id);
        try bindHashU64(self.term_stat_stmt, 2, term_hash);
        try bindInt64(self.term_stat_stmt, 3, document_frequency);
        try stepDone(self.term_stat_stmt);
    }
};

fn upsertDocumentStat(db: Database, table_id: u64, doc_id: u64, document_length: usize, unique_terms: usize) !void {
    const stmt = try prepare(db, "INSERT OR REPLACE INTO search_document_stats(table_id, doc_id, document_length, unique_terms) VALUES (?1, ?2, ?3, ?4)");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, doc_id);
    try bindInt64(stmt, 3, document_length);
    try bindInt64(stmt, 4, unique_terms);
    try stepDone(stmt);
}

fn deleteDocumentStat(db: Database, table_id: u64, doc_id: u64) !void {
    const stmt = try prepare(db, "DELETE FROM search_document_stats WHERE table_id = ?1 AND doc_id = ?2");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, doc_id);
    try stepDone(stmt);
}

fn upsertTermStat(db: Database, table_id: u64, term_hash: u64, document_frequency: u64) !void {
    const stmt = try prepare(db, "INSERT OR REPLACE INTO search_term_stats(table_id, term_hash, document_frequency) VALUES (?1, ?2, ?3)");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindHashU64(stmt, 2, term_hash);
    try bindInt64(stmt, 3, document_frequency);
    try stepDone(stmt);
}

fn deleteTermStat(db: Database, table_id: u64, term_hash: u64) !void {
    const stmt = try prepare(db, "DELETE FROM search_term_stats WHERE table_id = ?1 AND term_hash = ?2");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindHashU64(stmt, 2, term_hash);
    try stepDone(stmt);
}

fn upsertTermFrequency(db: Database, table_id: u64, doc_id: u64, term_hash: u64, frequency: u32) !void {
    const stmt = try prepare(db, "INSERT OR REPLACE INTO search_term_frequencies(table_id, doc_id, term_hash, frequency) VALUES (?1, ?2, ?3, ?4)");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, doc_id);
    try bindHashU64(stmt, 3, term_hash);
    try bindInt64(stmt, 4, frequency);
    try stepDone(stmt);
}

fn deleteTermFrequency(db: Database, table_id: u64, doc_id: u64, term_hash: u64) !void {
    const stmt = try prepare(db, "DELETE FROM search_term_frequencies WHERE table_id = ?1 AND doc_id = ?2 AND term_hash = ?3");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, doc_id);
    try bindHashU64(stmt, 3, term_hash);
    try stepDone(stmt);
}

fn upsertPosting(db: Database, allocator: std.mem.Allocator, table_id: u64, term_hash: u64, bitmap: roaring.Bitmap) !void {
    const stmt = try prepare(db, "INSERT OR REPLACE INTO search_postings(table_id, term_hash, posting_blob) VALUES (?1, ?2, ?3)");
    defer finalize(stmt);
    const bytes = try bitmap.serializePortableStable(allocator);
    defer allocator.free(bytes);
    try bindInt64(stmt, 1, table_id);
    try bindHashU64(stmt, 2, term_hash);
    try bindBlob(stmt, 3, bytes);
    try stepDone(stmt);
}

fn deletePosting(db: Database, table_id: u64, term_hash: u64) !void {
    const stmt = try prepare(db, "DELETE FROM search_postings WHERE table_id = ?1 AND term_hash = ?2");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindHashU64(stmt, 2, term_hash);
    try stepDone(stmt);
}

fn reconcileTermArtifact(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    doc_id: u64,
    term_hash: u64,
    old_frequency: u32,
    new_frequency: u32,
) !void {
    if (new_frequency == 0) {
        try deleteTermFrequency(db, table_id, doc_id, term_hash);
    } else {
        try upsertTermFrequency(db, table_id, doc_id, term_hash, new_frequency);
    }

    const maybe_old_df = try loadTermStat(db, table_id, term_hash);
    const old_df = if (maybe_old_df) |term_stat| term_stat.document_frequency else 0;
    var new_df = old_df;
    if (old_frequency == 0 and new_frequency > 0) new_df += 1;
    if (old_frequency > 0 and new_frequency == 0) new_df -= 1;

    if (new_df == 0) {
        try deleteTermStat(db, table_id, term_hash);
    } else {
        try upsertTermStat(db, table_id, term_hash, new_df);
    }

    var posting = loadPosting(db, table_id, term_hash) catch |err| switch (err) {
        error.MissingRow => null,
        else => return err,
    };
    defer if (posting) |*bitmap| bitmap.deinit();

    if (old_frequency == 0 and new_frequency > 0) {
        if (posting == null) posting = try roaring.Bitmap.empty();
        posting.?.add(@intCast(doc_id));
    } else if (old_frequency > 0 and new_frequency == 0) {
        if (posting) |*bitmap| bitmap.remove(@intCast(doc_id));
    }

    if (posting) |bitmap| {
        if (bitmap.isEmpty()) {
            try deletePosting(db, table_id, term_hash);
        } else {
            try upsertPosting(db, allocator, table_id, term_hash, bitmap);
        }
    } else if (new_frequency == 0) {
        try deletePosting(db, table_id, term_hash);
    }
}

const CollectionStatRecord = struct {
    total_documents: u64,
    total_length: u64,
};

fn loadCollectionStat(db: Database, table_id: u64) !CollectionStatRecord {
    const stmt = try prepare(db, "SELECT total_documents, total_document_length, avg_document_length FROM search_collection_stats WHERE table_id = ?1");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) {
        return .{ .total_documents = 0, .total_length = 0 };
    }

    const total_documents = try columnU64(stmt, 0);
    const total_document_length = try columnU64(stmt, 1);
    return .{
        .total_documents = total_documents,
        .total_length = total_document_length,
    };
}

fn loadDocumentStat(db: Database, table_id: u64, doc_id: u64) !?interfaces.DocumentStats {
    const stmt = try prepare(db, "SELECT document_length, unique_terms FROM search_document_stats WHERE table_id = ?1 AND doc_id = ?2");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, doc_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return null;
    return .{
        .doc_id = doc_id,
        .document_length = @intCast(try columnU64(stmt, 0)),
        .unique_terms = @intCast(try columnU64(stmt, 1)),
    };
}

fn loadTermStat(db: Database, table_id: u64, term_hash: u64) !?interfaces.TermStat {
    const stmt = try prepare(db, "SELECT document_frequency FROM search_term_stats WHERE table_id = ?1 AND term_hash = ?2");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindHashU64(stmt, 2, term_hash);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return null;
    return .{
        .term_hash = term_hash,
        .document_frequency = try columnU64(stmt, 0),
    };
}

fn loadPosting(db: Database, table_id: u64, term_hash: u64) !roaring.Bitmap {
    const stmt = try prepare(db, "SELECT posting_blob FROM search_postings WHERE table_id = ?1 AND term_hash = ?2");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindHashU64(stmt, 2, term_hash);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;
    const blob_ptr = c.sqlite3_column_blob(stmt, 0) orelse return error.MissingRow;
    const blob_len = c.sqlite3_column_bytes(stmt, 0);
    const blob: []const u8 = @as([*]const u8, @ptrCast(blob_ptr))[0..@intCast(blob_len)];
    return try roaring.Bitmap.deserializePortable(blob);
}

fn loadTermFrequencyMapForDoc(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    doc_id: u64,
) !std.AutoHashMap(u64, u32) {
    var counts = std.AutoHashMap(u64, u32).init(allocator);
    errdefer counts.deinit();

    const stmt = try prepare(db, "SELECT term_hash, frequency FROM search_term_frequencies WHERE table_id = ?1 AND doc_id = ?2");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, doc_id);

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try counts.put(columnHashU64(stmt, 0), @intCast(try columnU64(stmt, 1)));
    }

    return counts;
}

pub fn loadSearchDocumentContent(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    doc_id: u64,
) ![]u8 {
    const stmt = try prepare(db, "SELECT content FROM search_documents WHERE table_id = ?1 AND doc_id = ?2");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, doc_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;
    return try columnTextOwned(allocator, stmt, 0);
}

fn loadSearchDocumentContentAndLanguage(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    doc_id: u64,
) !struct { content: []u8, language: []u8 } {
    const stmt = try prepare(db, "SELECT content, language FROM search_documents WHERE table_id = ?1 AND doc_id = ?2");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, doc_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;
    const content = try columnTextOwned(allocator, stmt, 0);
    errdefer allocator.free(content);
    const language = try columnTextOwned(allocator, stmt, 1);
    return .{ .content = content, .language = language };
}

fn makeArtifactKey(table_id: u64, term_hash: u64) u128 {
    return (@as(u128, table_id) << 64) | term_hash;
}

fn unpackArtifactKey(key: u128) struct { table_id: u64, term_hash: u64 } {
    return .{
        .table_id = @intCast(key >> 64),
        .term_hash = @intCast(key & std.math.maxInt(u64)),
    };
}

fn encodeEmbedding(allocator: std.mem.Allocator, values: []const f32) ![]u8 {
    return vector_blob.encodeF32Le(allocator, values);
}

fn decodeEmbedding(
    allocator: std.mem.Allocator,
    stmt: *c.sqlite3_stmt,
    index: c_int,
    dimensions: usize,
) ![]f32 {
    const blob_ptr = c.sqlite3_column_blob(stmt, index) orelse return allocator.alloc(f32, 0);
    const blob_size = c.sqlite3_column_bytes(stmt, index);
    if (blob_size < 0) return error.ValueOutOfRange;

    const blob_len: usize = @intCast(blob_size);
    const bytes: []const u8 = @as([*]const u8, @ptrCast(blob_ptr))[0..blob_len];
    return vector_blob.decodeF32Le(allocator, bytes, dimensions) catch |err| switch (err) {
        error.InvalidEmbeddingBlob => error.ValueOutOfRange,
        else => err,
    };
}

fn prepare(db: Database, sql: []const u8) !*c.sqlite3_stmt {
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
        return error.PrepareFailed;
    }
    return stmt.?;
}

fn finalize(stmt: *c.sqlite3_stmt) void {
    _ = c.sqlite3_finalize(stmt);
}

fn bindInt64(stmt: *c.sqlite3_stmt, index: c_int, value: anytype) !void {
    if (c.sqlite3_bind_int64(stmt, index, @intCast(value)) != c.SQLITE_OK) {
        return error.BindFailed;
    }
}

fn bindText(stmt: *c.sqlite3_stmt, index: c_int, value: []const u8) !void {
    if (c.sqlite3_bind_text(stmt, index, value.ptr, @intCast(value.len), facet_sqlite.sqliteTransient()) != c.SQLITE_OK) {
        return error.BindFailed;
    }
}

fn bindHashU64(stmt: *c.sqlite3_stmt, index: c_int, value: u64) !void {
    const signed: i64 = @bitCast(value);
    if (c.sqlite3_bind_int64(stmt, index, signed) != c.SQLITE_OK) {
        return error.BindFailed;
    }
}

fn bindBlob(stmt: *c.sqlite3_stmt, index: c_int, value: []const u8) !void {
    if (c.sqlite3_bind_blob(stmt, index, value.ptr, @intCast(value.len), facet_sqlite.sqliteTransient()) != c.SQLITE_OK) {
        return error.BindFailed;
    }
}

fn stepDone(stmt: *c.sqlite3_stmt) !void {
    if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
}

fn columnU64(stmt: *c.sqlite3_stmt, index: c_int) !u64 {
    const value = c.sqlite3_column_int64(stmt, index);
    if (value < 0) return error.ValueOutOfRange;
    return @intCast(value);
}

fn columnUsize(stmt: *c.sqlite3_stmt, index: c_int) !usize {
    return try columnU64(stmt, index);
}

fn columnDocId(stmt: *c.sqlite3_stmt, index: c_int) !interfaces.DocId {
    return @intCast(try columnU64(stmt, index));
}

fn columnHashU64(stmt: *c.sqlite3_stmt, index: c_int) u64 {
    return @bitCast(c.sqlite3_column_int64(stmt, index));
}

fn columnTextOwned(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, index: c_int) ![]u8 {
    const ptr = c.sqlite3_column_text(stmt, index) orelse return allocator.dupe(u8, "");
    const len = c.sqlite3_column_bytes(stmt, index);
    if (len < 0) return error.ValueOutOfRange;
    const slice = @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
    return allocator.dupe(u8, slice);
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
        if (isEnglishStopWordAscii(query[start..i])) continue;

        if (token_count > 0) try out.appendSlice(allocator, " OR ");
        try out.append(allocator, '"');
        for (query[start..i]) |char| {
            try out.append(allocator, std.ascii.toLower(char));
        }
        try out.append(allocator, '"');
        token_count += 1;
    }

    if (token_count == 0) return try allocator.dupe(u8, "");
    return out.toOwnedSlice(allocator);
}

fn isFtsTokenChar(char: u8) bool {
    return std.ascii.isAlphanumeric(char) or char >= 0x80;
}

fn isEnglishStopWordAscii(word: []const u8) bool {
    const stop_words = [_][]const u8{
        "a",    "an",   "and",  "are", "as",  "at",   "be",   "by",   "for", "from",
        "has",  "have", "he",   "in",  "is",  "it",   "its",  "of",   "on",  "or",
        "that", "the",  "this", "to",  "was", "were", "will", "with",
    };
    for (stop_words) |stop| {
        if (word.len != stop.len) continue;
        var matches = true;
        for (word, 0..) |char, index| {
            if (std.ascii.toLower(char) != stop[index]) {
                matches = false;
                break;
            }
        }
        if (matches) return true;
    }
    return false;
}

test "search sqlite persists and reloads standalone search state" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try upsertSearchDocument(db, 1, 7, "zig sqlite roaring", "english");
    try upsertSearchDocument(db, 1, 9, "graph traversal with roaring", "english");
    try upsertSearchEmbedding(db, std.testing.allocator, 1, 7, &.{ 0.8, 0.2 });
    try upsertSearchEmbedding(db, std.testing.allocator, 1, 9, &.{ 0.4, 0.9 });

    var store = try loadSearchStore(db, std.testing.allocator);
    defer store.deinit();

    const bm25_repo = store.bm25Repository();
    var bitmap = (try bm25_repo.getPostingBitmapFn(bm25_repo.ctx, std.testing.allocator, 1, tokenization_sqlite.hashTermLexeme("roaring"))).?;
    defer bitmap.deinit();
    try std.testing.expect(bitmap.contains(7));
    try std.testing.expect(bitmap.contains(9));

    const vector_repo = store.vectorRepository();
    const nearest = try vector_repo.searchNearestFn(vector_repo.ctx, std.testing.allocator, .{
        .table_name = "docs_fixture",
        .key_column = "doc_id",
        .vector_column = "embedding",
        .query_vector = &.{ 0.75, 0.25 },
        .limit = 1,
    });
    defer std.testing.allocator.free(nearest);
    try std.testing.expectEqual(@as(u64, 7), nearest[0].doc_id);
}

test "search sqlite counts embeddings and selects missing embedding batch rows" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try upsertSearchDocument(db, 1, 7, "zig sqlite roaring", "english");
    try upsertSearchDocument(db, 1, 9, "graph traversal", "english");
    try upsertSearchEmbedding(db, std.testing.allocator, 1, 7, &.{ 0.8, 0.2 });

    try std.testing.expectEqual(@as(u64, 1), try countSearchEmbeddings(db, 1));

    const missing_rows = try loadSearchDocumentsForEmbeddingBatch(db, std.testing.allocator, 1, 10, true);
    defer {
        for (missing_rows) |row| row.deinit(std.testing.allocator);
        std.testing.allocator.free(missing_rows);
    }
    try std.testing.expectEqual(@as(usize, 1), missing_rows.len);
    try std.testing.expectEqual(@as(u64, 9), missing_rows[0].doc_id);

    const all_rows = try loadSearchDocumentsForEmbeddingBatch(db, std.testing.allocator, 1, 10, false);
    defer {
        for (all_rows) |row| row.deinit(std.testing.allocator);
        std.testing.allocator.free(all_rows);
    }
    try std.testing.expectEqual(@as(usize, 2), all_rows.len);
}

test "search sqlite stores search embeddings as packed f32 blobs" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try upsertSearchEmbedding(db, std.testing.allocator, 1, 7, &.{ 0.25, -0.5, 1.0 });

    const stmt = try prepare(db, "SELECT dimensions, length(embedding_blob), typeof(embedding_blob) FROM search_embeddings WHERE table_id = ?1 AND doc_id = ?2");
    defer finalize(stmt);
    try bindInt64(stmt, 1, 1);
    try bindInt64(stmt, 2, 7);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;

    try std.testing.expectEqual(@as(u64, 3), try columnU64(stmt, 0));
    try std.testing.expectEqual(@as(u64, 12), try columnU64(stmt, 1));
    const storage_type = try columnTextOwned(std.testing.allocator, stmt, 2);
    defer std.testing.allocator.free(storage_type);
    try std.testing.expectEqualStrings("blob", storage_type);
}

test "search sqlite rebuilds compact artifacts and reloads compact store" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try upsertSearchDocument(db, 1, 7, "zig sqlite roaring", "english");
    try upsertSearchDocument(db, 1, 9, "graph traversal with roaring", "english");
    try upsertSearchEmbedding(db, std.testing.allocator, 1, 7, &.{ 0.8, 0.2 });
    try upsertSearchEmbedding(db, std.testing.allocator, 1, 9, &.{ 0.4, 0.9 });
    try rebuildSearchArtifacts(db, std.testing.allocator);

    var store = try loadCompactSearchStore(db, std.testing.allocator);
    defer store.deinit();

    const bm25_matches = try searchFts5Bm25(db, std.testing.allocator, 1, "roaring", 10);
    defer std.testing.allocator.free(bm25_matches);
    try std.testing.expectEqual(@as(usize, 2), bm25_matches.len);

    const bm25_repo = store.bm25Repository();
    const doc_stats = (try bm25_repo.getDocumentStatsFn(bm25_repo.ctx, std.testing.allocator, 1, 7)).?;
    try std.testing.expect(doc_stats.document_length > 0);

    const vector_repo = store.vectorRepository();
    const nearest = try vector_repo.searchNearestFn(vector_repo.ctx, std.testing.allocator, .{
        .table_name = "docs_fixture",
        .key_column = "doc_id",
        .vector_column = "embedding",
        .query_vector = &.{ 0.75, 0.25 },
        .limit = 1,
    });
    defer std.testing.allocator.free(nearest);
    try std.testing.expectEqual(@as(u64, 7), nearest[0].doc_id);
}

test "search sqlite rebuild clears stale collection stats for removed tables" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try upsertSearchDocument(db, 1, 7, "zig sqlite roaring", "english");
    try rebuildSearchArtifacts(db, std.testing.allocator);

    try deleteSearchDocument(db, 1, 7);
    try rebuildSearchArtifacts(db, std.testing.allocator);

    var store = try loadCompactSearchStore(db, std.testing.allocator);
    defer store.deinit();

    try std.testing.expectEqual(@as(usize, 0), store.collection_stats.items.len);
    try std.testing.expectEqual(@as(usize, 0), store.document_stats.items.len);
    try std.testing.expectEqual(@as(usize, 0), store.term_stats.items.len);
    try std.testing.expectEqual(@as(usize, 0), store.term_frequencies.items.len);
    try std.testing.expectEqual(@as(usize, 0), store.postings.items.len);
}

test "search sqlite fts upsert updates bm25 results" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try upsertSearchDocument(db, 1, 7, "zig sqlite roaring", "english");
    try rebuildSearchArtifacts(db, std.testing.allocator);

    try upsertSearchDocument(db, 1, 7, "graph traversal with roaring", "english");
    try upsertSearchArtifactsForDocument(db, std.testing.allocator, 1, 7);

    const zig_matches = try searchFts5Bm25(db, std.testing.allocator, 1, "zig", 10);
    defer std.testing.allocator.free(zig_matches);
    try std.testing.expectEqual(@as(usize, 0), zig_matches.len);

    const graph_matches = try searchFts5Bm25(db, std.testing.allocator, 1, "graph", 10);
    defer std.testing.allocator.free(graph_matches);
    try std.testing.expectEqual(@as(usize, 1), graph_matches.len);
    try std.testing.expectEqual(@as(u64, 7), graph_matches[0].doc_id);

    var store = try loadCompactSearchStore(db, std.testing.allocator);
    defer store.deinit();
    const bm25_repo = store.bm25Repository();
    const doc_stats = (try bm25_repo.getDocumentStatsFn(bm25_repo.ctx, std.testing.allocator, 1, 7)).?;
    try std.testing.expect(doc_stats.document_length > 0);

    const collection_stats = try bm25_repo.getCollectionStatsFn(bm25_repo.ctx, std.testing.allocator, 1);
    try std.testing.expectEqual(@as(u64, 1), collection_stats.total_documents);
}

test "search sqlite delete removes fts bm25 rows" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try upsertSearchDocument(db, 1, 7, "zig sqlite roaring", "english");
    try upsertSearchDocument(db, 1, 9, "graph traversal with roaring", "english");
    try rebuildSearchArtifacts(db, std.testing.allocator);

    try syncDeleteSearchDocument(db, std.testing.allocator, 1, 7);

    var store = try loadCompactSearchStore(db, std.testing.allocator);
    defer store.deinit();

    const roaring_matches = try searchFts5Bm25(db, std.testing.allocator, 1, "roaring", 10);
    defer std.testing.allocator.free(roaring_matches);
    try std.testing.expectEqual(@as(usize, 1), roaring_matches.len);
    try std.testing.expectEqual(@as(u64, 9), roaring_matches[0].doc_id);

    const bm25_repo = store.bm25Repository();
    try std.testing.expect((try bm25_repo.getDocumentStatsFn(bm25_repo.ctx, std.testing.allocator, 1, 7)) == null);

    const collection_stats = try bm25_repo.getCollectionStatsFn(bm25_repo.ctx, std.testing.allocator, 1);
    try std.testing.expectEqual(@as(u64, 1), collection_stats.total_documents);
}

test "search sqlite bm25 rebuild applies bm25_stopwords for document language" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try upsertSearchDocument(db, 1, 1, "the graph test", "english");
    try rebuildSearchArtifacts(db, std.testing.allocator);

    const stopword_matches = try searchFts5Bm25(db, std.testing.allocator, 1, "the", 10);
    defer std.testing.allocator.free(stopword_matches);
    try std.testing.expectEqual(@as(usize, 0), stopword_matches.len);

    const graph_matches = try searchFts5Bm25(db, std.testing.allocator, 1, "graph", 10);
    defer std.testing.allocator.free(graph_matches);
    try std.testing.expectEqual(@as(usize, 1), graph_matches.len);
    try std.testing.expectEqual(@as(u64, 1), graph_matches[0].doc_id);
}

test "search sqlite compact snapshot reports persisted artifact counts" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try upsertSearchDocument(db, 1, 7, "zig sqlite roaring", "english");
    try upsertSearchDocument(db, 1, 9, "graph traversal with roaring", "english");
    try upsertSearchEmbedding(db, std.testing.allocator, 1, 7, &.{ 0.8, 0.2 });
    try upsertSearchEmbedding(db, std.testing.allocator, 1, 9, &.{ 0.4, 0.9 });
    try rebuildSearchArtifacts(db, std.testing.allocator);

    const snapshot = try compactSearchSnapshot(db, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), snapshot.collection_stats);
    try std.testing.expectEqual(@as(usize, 2), snapshot.document_stats);
    try std.testing.expectEqual(@as(usize, 5), snapshot.term_stats);
    try std.testing.expectEqual(@as(usize, 6), snapshot.term_frequencies);
    try std.testing.expectEqual(@as(usize, 0), snapshot.postings);
    try std.testing.expectEqual(@as(usize, 2), snapshot.embeddings);

    const toon = try compactSearchSnapshotToon(db, std.testing.allocator);
    defer std.testing.allocator.free(toon);
    try std.testing.expect(std.mem.indexOf(u8, toon, "kind: search_compact_snapshot") != null);
}
