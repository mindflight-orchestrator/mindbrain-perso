const std = @import("std");
const interfaces = @import("interfaces.zig");
const utils = @import("../mb_facets/utils.zig");

const c = utils.c;

pub const Error = error{
    QueryFailed,
    UnsupportedMetric,
    InvalidQueryVector,
    NotImplemented,
};

var adapter_ctx: u8 = 0;

pub const Adapter = struct {
    pub fn asRepository() interfaces.VectorRepository {
        return .{
            .ctx = @ptrCast(&adapter_ctx),
            .getEmbeddingDimensionsFn = getEmbeddingDimensionsViaInterface,
            .searchNearestFn = searchNearestViaInterface,
            .upsertEmbeddingFn = upsertEmbeddingViaInterface,
            .deleteEmbeddingFn = deleteEmbeddingViaInterface,
        };
    }

    pub fn getEmbeddingDimensions(table_name: []const u8, column_name: []const u8, allocator: std.mem.Allocator) !?usize {
        const conn_result = c.SPI_connect();
        const need_finish = conn_result == c.SPI_OK_CONNECT;
        if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
            return error.QueryFailed;
        }
        defer if (need_finish) {
            _ = c.SPI_finish();
        };

        const query = try std.fmt.allocPrintSentinel(
            allocator,
            \\SELECT
            \\    regexp_replace(format_type(a.atttypid, a.atttypmod), '^vector\(([0-9]+)\)$', '\1')::int
            \\FROM pg_attribute a
            \\JOIN pg_class c ON c.oid = a.attrelid
            \\JOIN pg_namespace n ON n.oid = c.relnamespace
            \\WHERE c.relname = '{s}'
            \\  AND a.attname = '{s}'
            \\  AND a.attnum > 0
            \\  AND NOT a.attisdropped
        ,
            .{ table_name, column_name },
            0,
        );
        defer allocator.free(query);

        const ret = c.SPI_execute(query.ptr, true, 1);
        if (ret != c.SPI_OK_SELECT or c.SPI_processed == 0) return null;

        const tuple = c.SPI_tuptable.*.vals[0];
        const tupdesc = c.SPI_tuptable.*.tupdesc;
        var isnull: bool = false;
        const dim_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);
        if (isnull) return null;

        return @intCast(c.DatumGetInt32(dim_datum));
    }

    pub fn searchNearest(request: interfaces.VectorSearchRequest, allocator: std.mem.Allocator) ![]interfaces.VectorSearchMatch {
        if (request.query_vector.len == 0) return allocator.alloc(interfaces.VectorSearchMatch, 0);

        const operator = switch (request.metric) {
            .cosine => "<=>",
            .l2 => "<->",
            .inner_product => "<#>",
        };

        const conn_result = c.SPI_connect();
        const need_finish = conn_result == c.SPI_OK_CONNECT;
        if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
            return error.QueryFailed;
        }
        defer if (need_finish) {
            _ = c.SPI_finish();
        };

        const vector_literal = try formatVectorLiteral(request.query_vector, allocator);
        defer allocator.free(vector_literal);

        const query = try std.fmt.allocPrintSentinel(
            allocator,
            \\SELECT {s}, ({s} {s} '{s}'::vector) AS distance
            \\FROM {s}
            \\WHERE {s} IS NOT NULL
            \\ORDER BY {s} {s} '{s}'::vector
            \\LIMIT {d}
        ,
            .{
                request.key_column,
                request.vector_column,
                operator,
                vector_literal,
                request.table_name,
                request.vector_column,
                request.vector_column,
                operator,
                vector_literal,
                request.limit,
            },
            0,
        );
        defer allocator.free(query);

        const ret = c.SPI_execute(query.ptr, true, @intCast(request.limit));
        if (ret != c.SPI_OK_SELECT) return error.QueryFailed;

        const rows: usize = @intCast(c.SPI_processed);
        const matches = try allocator.alloc(interfaces.VectorSearchMatch, rows);

        var i: usize = 0;
        while (i < rows) : (i += 1) {
            const tuple = c.SPI_tuptable.*.vals[@intCast(i)];
            const tupdesc = c.SPI_tuptable.*.tupdesc;
            var isnull1: bool = false;
            var isnull2: bool = false;
            const id_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull1);
            const distance_datum = c.SPI_getbinval(tuple, tupdesc, 2, &isnull2);

            const distance = if (isnull2) 0.0 else c.DatumGetFloat8(distance_datum);
            matches[i] = .{
                .doc_id = if (isnull1) 0 else @intCast(c.DatumGetInt64(id_datum)),
                .distance = distance,
                .similarity = similarityFromDistance(request.metric, distance),
            };
        }

        return matches;
    }

    fn similarityFromDistance(metric: interfaces.VectorDistanceMetric, distance: f64) f64 {
        return switch (metric) {
            .cosine => 1.0 - (distance / 2.0),
            .l2 => 1.0 / (1.0 + distance),
            .inner_product => -distance,
        };
    }

    fn formatVectorLiteral(values: []const f32, allocator: std.mem.Allocator) ![]u8 {
        var buf = std.ArrayList(u8).empty;
        defer buf.deinit(allocator);

        try buf.append(allocator, '[');
        for (values, 0..) |value, i| {
            if (i > 0) try buf.append(allocator, ',');
            const part = try std.fmt.allocPrint(allocator, "{d}", .{value});
            defer allocator.free(part);
            try buf.appendSlice(allocator, part);
        }
        try buf.append(allocator, ']');
        return buf.toOwnedSlice(allocator);
    }

    fn getEmbeddingDimensionsViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, table_name: []const u8, column_name: []const u8) anyerror!?usize {
        _ = ctx;
        return getEmbeddingDimensions(table_name, column_name, allocator);
    }

    fn searchNearestViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, request: interfaces.VectorSearchRequest) anyerror![]interfaces.VectorSearchMatch {
        _ = ctx;
        return searchNearest(request, allocator);
    }

    fn upsertEmbeddingViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, request: interfaces.UpsertEmbeddingRequest) anyerror!void {
        _ = ctx;
        _ = allocator;
        _ = request;
        return error.NotImplemented;
    }

    fn deleteEmbeddingViaInterface(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: interfaces.DocId) anyerror!void {
        _ = ctx;
        _ = allocator;
        _ = table_id;
        _ = doc_id;
        return error.NotImplemented;
    }
};
