const std = @import("std");
const interfaces = @import("interfaces.zig");
const roaring = @import("roaring.zig");
const utils = @import("../mb_facets/utils.zig");

const c = utils.c;

pub const Error = error{
    QueryFailed,
    NotImplemented,
};

var adapter_ctx: u8 = 0;

pub const Adapter = struct {
    pub fn asRepository() interfaces.Bm25Repository {
        return .{
            .ctx = @ptrCast(&adapter_ctx),
            .getCollectionStatsFn = getCollectionStatsViaInterface,
            .getDocumentStatsFn = getDocumentStatsViaInterface,
            .getTermStatsFn = getTermStatsViaInterface,
            .getTermFrequenciesFn = getTermFrequenciesViaInterface,
            .getPostingBitmapFn = getPostingBitmapViaInterface,
            .upsertDocumentFn = upsertDocumentViaInterface,
            .deleteDocumentFn = deleteDocumentViaInterface,
        };
    }

    pub fn getCollectionStats(table_id: u64, allocator: std.mem.Allocator) !interfaces.CollectionStats {
        _ = allocator;
        const conn_result = c.SPI_connect();
        const need_finish = (conn_result == c.SPI_OK_CONNECT);
        if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
            return error.QueryFailed;
        }
        defer if (need_finish) {
            _ = c.SPI_finish();
        };

        var query_buf: [256]u8 = undefined;
        const query = std.fmt.bufPrintZ(
            &query_buf,
            "SELECT total_documents, avg_document_length FROM facets.bm25_statistics WHERE table_id = {d}",
            .{table_id},
        ) catch return error.QueryFailed;

        const ret = c.SPI_execute(query.ptr, true, 1);
        if (ret != c.SPI_OK_SELECT) return error.QueryFailed;

        var stats = interfaces.CollectionStats{
            .total_documents = 0,
            .avg_document_length = 0.0,
        };

        if (c.SPI_processed == 0) return stats;
        if (c.SPI_tuptable == null) return stats;

        const tuple = c.SPI_tuptable.*.vals[0];
        const tupdesc = c.SPI_tuptable.*.tupdesc;
        var isnull1: bool = false;
        var isnull2: bool = false;

        const total_docs_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull1);
        const avg_len_datum = c.SPI_getbinval(tuple, tupdesc, 2, &isnull2);

        if (!isnull1) {
            stats.total_documents = @intCast(c.DatumGetInt64(total_docs_datum));
        }
        if (!isnull2) {
            stats.avg_document_length = c.DatumGetFloat8(avg_len_datum);
        }

        return stats;
    }

    pub fn getPostingBitmap(table_id: u64, term_hash: u64, allocator: std.mem.Allocator) !?roaring.Bitmap {
        const conn_result = c.SPI_connect();
        const need_finish = (conn_result == c.SPI_OK_CONNECT);
        if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
            return error.QueryFailed;
        }
        defer if (need_finish) {
            _ = c.SPI_finish();
        };

        const query = try std.fmt.allocPrintSentinel(
            allocator,
            "SELECT doc_ids FROM facets.bm25_index WHERE table_id = {d} AND term_hash = {d}",
            .{ table_id, term_hash },
            0,
        );
        defer allocator.free(query);

        const ret = c.SPI_execute(query.ptr, true, 1);
        if (ret != c.SPI_OK_SELECT or c.SPI_processed == 0 or c.SPI_tuptable == null) {
            return null;
        }

        const tuple = c.SPI_tuptable.*.vals[0];
        const tupdesc = c.SPI_tuptable.*.tupdesc;
        var isnull: bool = false;
        const doc_ids_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);
        if (isnull) return null;

        return try deserializeBitmapDatum(doc_ids_datum);
    }

    fn deserializeBitmapDatum(datum: c.Datum) !roaring.Bitmap {
        const varlena = utils.detoast_datum(datum);
        const total_size: usize = @intCast(utils.varsize(varlena));
        const payload_size = total_size - @as(usize, @intCast(utils.varhdrsz()));
        const payload = utils.vardata(varlena)[0..payload_size];
        return roaring.Bitmap.deserializePortable(payload);
    }

    fn getCollectionStatsViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64) anyerror!interfaces.CollectionStats {
        _ = ctx;
        return getCollectionStats(table_id, allocator);
    }

    fn getDocumentStatsViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId) anyerror!?interfaces.DocumentStats {
        _ = ctx;
        const conn_result = c.SPI_connect();
        const need_finish = (conn_result == c.SPI_OK_CONNECT);
        if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
            return error.QueryFailed;
        }
        defer if (need_finish) {
            _ = c.SPI_finish();
        };

        const query = try std.fmt.allocPrintSentinel(
            allocator,
            \\SELECT
            \\    d.doc_id,
            \\    d.doc_length,
            \\    (SELECT COUNT(DISTINCT term_hash) FROM facets.bm25_term_frequencies tf
            \\      WHERE tf.table_id = d.table_id AND tf.doc_id = d.doc_id)::int AS unique_terms
            \\FROM facets.bm25_documents d
            \\WHERE d.table_id = {d} AND d.doc_id = {d}
        ,
            .{ table_id, doc_id },
            0,
        );
        defer allocator.free(query);

        const ret = c.SPI_execute(query.ptr, true, 1);
        if (ret != c.SPI_OK_SELECT or c.SPI_processed == 0 or c.SPI_tuptable == null) return null;

        const tuple = c.SPI_tuptable.*.vals[0];
        const tupdesc = c.SPI_tuptable.*.tupdesc;
        var isnull1: bool = false;
        var isnull2: bool = false;
        var isnull3: bool = false;
        const doc_id_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull1);
        const doc_len_datum = c.SPI_getbinval(tuple, tupdesc, 2, &isnull2);
        const unique_terms_datum = c.SPI_getbinval(tuple, tupdesc, 3, &isnull3);

        return .{
            .doc_id = if (isnull1) doc_id else @intCast(c.DatumGetInt64(doc_id_datum)),
            .document_length = if (isnull2) 0 else @intCast(c.DatumGetInt32(doc_len_datum)),
            .unique_terms = if (isnull3) 0 else @intCast(c.DatumGetInt32(unique_terms_datum)),
        };
    }

    fn getTermStatsViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, term_hashes: []const u64) anyerror![]interfaces.TermStat {
        _ = ctx;
        if (term_hashes.len == 0) return allocator.alloc(interfaces.TermStat, 0);

        const conn_result = c.SPI_connect();
        const need_finish = (conn_result == c.SPI_OK_CONNECT);
        if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
            return error.QueryFailed;
        }
        defer if (need_finish) {
            _ = c.SPI_finish();
        };

        const hash_arr = try formatHashArray(term_hashes, allocator);
        defer allocator.free(hash_arr);

        const query = try std.fmt.allocPrintSentinel(
            allocator,
            "SELECT term_hash, rb_cardinality(doc_ids)::bigint FROM facets.bm25_index WHERE table_id = {d} AND term_hash = ANY({s})",
            .{ table_id, hash_arr },
            0,
        );
        defer allocator.free(query);

        const ret = c.SPI_execute(query.ptr, true, 0);
        if (ret != c.SPI_OK_SELECT) return error.QueryFailed;

        const rows: usize = @intCast(c.SPI_processed);
        if (rows > 0 and c.SPI_tuptable == null) return error.QueryFailed;

        const stats = try allocator.alloc(interfaces.TermStat, rows);

        var i: usize = 0;
        while (i < rows) : (i += 1) {
            const tuple = c.SPI_tuptable.*.vals[@intCast(i)];
            const tupdesc = c.SPI_tuptable.*.tupdesc;
            var isnull1: bool = false;
            var isnull2: bool = false;
            const hash_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull1);
            const df_datum = c.SPI_getbinval(tuple, tupdesc, 2, &isnull2);

            stats[i] = .{
                .term_hash = if (isnull1) 0 else @intCast(c.DatumGetInt64(hash_datum)),
                .document_frequency = if (isnull2) 0 else @intCast(c.DatumGetInt64(df_datum)),
            };
        }

        return stats;
    }

    fn getTermFrequenciesViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId, term_hashes: []const u64) anyerror![]interfaces.TermFrequency {
        _ = ctx;
        if (term_hashes.len == 0) return allocator.alloc(interfaces.TermFrequency, 0);

        const conn_result = c.SPI_connect();
        const need_finish = (conn_result == c.SPI_OK_CONNECT);
        if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
            return error.QueryFailed;
        }
        defer if (need_finish) {
            _ = c.SPI_finish();
        };

        const hash_arr = try formatHashArray(term_hashes, allocator);
        defer allocator.free(hash_arr);

        const query = try std.fmt.allocPrintSentinel(
            allocator,
            \\SELECT term_hash, frequency AS freq
            \\FROM facets.bm25_term_frequencies
            \\WHERE table_id = {d}
            \\  AND term_hash = ANY({s})
            \\  AND doc_id = {d}
        ,
            .{ table_id, hash_arr, doc_id },
            0,
        );
        defer allocator.free(query);

        const ret = c.SPI_execute(query.ptr, true, 0);
        if (ret != c.SPI_OK_SELECT) return error.QueryFailed;

        const rows: usize = @intCast(c.SPI_processed);
        if (rows > 0 and c.SPI_tuptable == null) return error.QueryFailed;

        const freqs = try allocator.alloc(interfaces.TermFrequency, rows);

        var i: usize = 0;
        while (i < rows) : (i += 1) {
            const tuple = c.SPI_tuptable.*.vals[@intCast(i)];
            const tupdesc = c.SPI_tuptable.*.tupdesc;
            var isnull1: bool = false;
            var isnull2: bool = false;
            const hash_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull1);
            const freq_datum = c.SPI_getbinval(tuple, tupdesc, 2, &isnull2);

            freqs[i] = .{
                .term_hash = if (isnull1) 0 else @intCast(c.DatumGetInt64(hash_datum)),
                .frequency = if (isnull2) 0 else @intCast(c.DatumGetInt32(freq_datum)),
            };
        }

        return freqs;
    }

    fn getPostingBitmapViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, term_hash: u64) anyerror!?roaring.Bitmap {
        _ = ctx;
        return getPostingBitmap(table_id, term_hash, allocator);
    }

    fn upsertDocumentViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, request: interfaces.UpsertDocumentRequest) anyerror!void {
        _ = ctx;
        _ = allocator;
        _ = request;
        return error.NotImplemented;
    }

    fn deleteDocumentViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId) anyerror!void {
        _ = ctx;
        _ = allocator;
        _ = table_id;
        _ = doc_id;
        return error.NotImplemented;
    }

    fn formatHashArray(term_hashes: []const u64, allocator: std.mem.Allocator) ![]u8 {
        var hash_arr = std.ArrayList(u8).empty;
        defer hash_arr.deinit(allocator);

        try hash_arr.appendSlice(allocator, "ARRAY[");
        for (term_hashes, 0..) |hash, i| {
            if (i > 0) try hash_arr.appendSlice(allocator, ",");
            const part = try std.fmt.allocPrint(allocator, "{d}", .{hash});
            defer allocator.free(part);
            try hash_arr.appendSlice(allocator, part);
        }
        try hash_arr.appendSlice(allocator, "]::bigint[]");
        return hash_arr.toOwnedSlice(allocator);
    }
};
