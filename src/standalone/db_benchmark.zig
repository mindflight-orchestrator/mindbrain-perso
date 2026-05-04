const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const facet_store = @import("facet_store.zig");
const graph_sqlite = @import("graph_sqlite.zig");
const roaring = @import("roaring.zig");
const toon_exports = @import("toon_exports.zig");
const workspace_sqlite = @import("workspace_sqlite.zig");
const compat = @import("zig16_compat.zig");

pub const Options = struct {
    workspace_id: []const u8 = "imdb-benchmark",
    facet_table_id: u64 = 88_001,
    facet_table_name: []const u8 = "imdb_benchmark_facets",
    facet_doc_count: usize = 4096,
    facet_batch_size: usize = 128,
    graph_batch_size: usize = 8,
    query_iterations: usize = 25,
    mutation_iterations: usize = 25,
};

pub const TimingStats = struct {
    min_ns: u64,
    p50_ns: u64,
    p95_ns: u64,
    max_ns: u64,
    mean_ns: u64,
};

pub const Report = struct {
    graph_entity_count: u64,
    graph_relation_count: u64,
    graph_alias_count: u64,
    facet_query: TimingStats,
    graph_query: TimingStats,
    facet_single_add: TimingStats,
    facet_single_remove: TimingStats,
    facet_single_update: TimingStats,
    facet_batch_update: TimingStats,
    graph_single_add: TimingStats,
    graph_single_remove: TimingStats,
    graph_single_update: TimingStats,
    graph_batch_update: TimingStats,
    facet_toon: []u8,
    graph_toon: []u8,

    pub fn deinit(self: *Report, allocator: std.mem.Allocator) void {
        allocator.free(self.facet_toon);
        allocator.free(self.graph_toon);
        self.* = undefined;
    }
};

const BenchmarkContext = struct {
    db: facet_sqlite.Database,
    allocator: std.mem.Allocator,
    opts: Options,
    bench_entity_ids: [4]u32 = .{ 0, 0, 0, 0 },
    bench_relation_ids: [3]u32 = .{ 0, 0, 0 },
    facet_filter: ?roaring.Bitmap = null,

    fn deinit(self: *BenchmarkContext) void {
        if (self.facet_filter) |*bitmap| bitmap.deinit();
        self.* = undefined;
    }
};

pub fn run(db: facet_sqlite.Database, allocator: std.mem.Allocator, opts: Options) !Report {
    try db.applyStandaloneSchema();

    try db.exec("SAVEPOINT imdb_full_db_benchmark");
    errdefer {
        _ = db.exec("ROLLBACK TO SAVEPOINT imdb_full_db_benchmark") catch {};
        _ = db.exec("RELEASE SAVEPOINT imdb_full_db_benchmark") catch {};
    }

    var ctx = try seedBenchmarkState(db, allocator, opts);
    defer ctx.deinit();

    const facet_toon = try benchFacetQuery(ctx);
    const graph_toon = try benchGraphQuery(ctx);

    const report = Report{
        .graph_entity_count = try countRows(db, "graph_entity"),
        .graph_relation_count = try countRows(db, "graph_relation"),
        .graph_alias_count = try countRows(db, "graph_entity_alias"),
        .facet_query = try measureFacetQuery(ctx, opts.query_iterations),
        .graph_query = try measureGraphQuery(ctx, opts.query_iterations),
        .facet_single_add = try measureFacetMutation(ctx, opts.mutation_iterations, facetSingleAdd),
        .facet_single_remove = try measureFacetMutation(ctx, opts.mutation_iterations, facetSingleRemove),
        .facet_single_update = try measureFacetMutation(ctx, opts.mutation_iterations, facetSingleUpdate),
        .facet_batch_update = try measureFacetMutation(ctx, opts.mutation_iterations, facetBatchUpdate),
        .graph_single_add = try measureGraphMutation(ctx, opts.mutation_iterations, graphSingleAdd),
        .graph_single_remove = try measureGraphMutation(ctx, opts.mutation_iterations, graphSingleRemove),
        .graph_single_update = try measureGraphMutation(ctx, opts.mutation_iterations, graphSingleUpdate),
        .graph_batch_update = try measureGraphMutation(ctx, opts.mutation_iterations, graphBatchUpdate),
        .facet_toon = facet_toon,
        .graph_toon = graph_toon,
    };

    _ = db.exec("ROLLBACK TO SAVEPOINT imdb_full_db_benchmark") catch return error.ExecFailed;
    _ = db.exec("RELEASE SAVEPOINT imdb_full_db_benchmark") catch return error.ExecFailed;
    return report;
}

fn seedBenchmarkState(db: facet_sqlite.Database, allocator: std.mem.Allocator, opts: Options) !BenchmarkContext {
    try workspace_sqlite.upsertWorkspace(db, opts.workspace_id, "{\"domain\":\"imdb-benchmark\"}");
    try workspace_sqlite.upsertTableSemantic(
        db,
        opts.facet_table_id,
        opts.workspace_id,
        "public",
        opts.facet_table_name,
        "doc_id",
        "content",
        "metadata_json",
        null,
        "english",
    );
    try facet_sqlite.setupFacetTable(
        db,
        opts.facet_table_id,
        "public",
        opts.facet_table_name,
        8,
        &.{
            .{ .facet_id = 1, .facet_name = "category" },
            .{ .facet_id = 2, .facet_name = "region" },
        },
    );

    var category_alpha = try roaring.Bitmap.empty();
    defer category_alpha.deinit();
    var category_beta = try roaring.Bitmap.empty();
    defer category_beta.deinit();
    var region_eu = try roaring.Bitmap.empty();
    defer region_eu.deinit();
    var region_us = try roaring.Bitmap.empty();
    defer region_us.deinit();

    for (0..opts.facet_doc_count) |index| {
        const doc_id: u32 = @intCast(index);
        if ((index & 1) == 0) category_alpha.add(doc_id) else category_beta.add(doc_id);
        if ((index % 3) == 0) region_eu.add(doc_id) else region_us.add(doc_id);
    }

    try facet_sqlite.upsertPostingBitmap(db, allocator, opts.facet_table_id, 1, "alpha", 0, category_alpha);
    try facet_sqlite.upsertPostingBitmap(db, allocator, opts.facet_table_id, 1, "beta", 0, category_beta);
    try facet_sqlite.upsertPostingBitmap(db, allocator, opts.facet_table_id, 2, "eu", 0, region_eu);
    try facet_sqlite.upsertPostingBitmap(db, allocator, opts.facet_table_id, 2, "us", 0, region_us);

    const entity_ids = .{
        try graph_sqlite.upsertEntityNatural(db, allocator, "benchmark_node", "bench-start", 1.0, "{}"),
        try graph_sqlite.upsertEntityNatural(db, allocator, "benchmark_node", "bench-mid-a", 1.0, "{}"),
        try graph_sqlite.upsertEntityNatural(db, allocator, "benchmark_node", "bench-mid-b", 1.0, "{}"),
        try graph_sqlite.upsertEntityNatural(db, allocator, "benchmark_node", "bench-end", 1.0, "{}"),
    };

    const relation_ids = .{ 88_101, 88_102, 88_103 };
    try graph_sqlite.upsertRelation(db, .{
        .relation_id = relation_ids[0],
        .source_id = entity_ids[0],
        .target_id = entity_ids[1],
        .relation_type = "bench_link",
        .confidence = 1.0,
    });
    try graph_sqlite.upsertRelation(db, .{
        .relation_id = relation_ids[1],
        .source_id = entity_ids[1],
        .target_id = entity_ids[2],
        .relation_type = "bench_link",
        .confidence = 1.0,
    });
    try graph_sqlite.upsertRelation(db, .{
        .relation_id = relation_ids[2],
        .source_id = entity_ids[2],
        .target_id = entity_ids[3],
        .relation_type = "bench_link",
        .confidence = 1.0,
    });
    try graph_sqlite.upsertAdjacency(db, "graph_lj_out", entity_ids[0], &.{relation_ids[0]}, allocator);
    try graph_sqlite.upsertAdjacency(db, "graph_lj_out", entity_ids[1], &.{relation_ids[1]}, allocator);
    try graph_sqlite.upsertAdjacency(db, "graph_lj_out", entity_ids[2], &.{relation_ids[2]}, allocator);
    try graph_sqlite.upsertAdjacency(db, "graph_lj_in", entity_ids[1], &.{relation_ids[0]}, allocator);
    try graph_sqlite.upsertAdjacency(db, "graph_lj_in", entity_ids[2], &.{relation_ids[1]}, allocator);
    try graph_sqlite.upsertAdjacency(db, "graph_lj_in", entity_ids[3], &.{relation_ids[2]}, allocator);

    var filter = try roaring.Bitmap.empty();
    errdefer filter.deinit();
    for (0..opts.facet_doc_count) |index| {
        if ((index % 3) == 0) filter.add(@intCast(index));
    }

    return .{
        .db = db,
        .allocator = allocator,
        .opts = opts,
        .bench_entity_ids = entity_ids,
        .bench_relation_ids = relation_ids,
        .facet_filter = filter,
    };
}

fn benchFacetQuery(ctx: BenchmarkContext) ![]u8 {
    const repo = facet_sqlite.Repository{ .db = &ctx.db };
    return try facetCountsToon(ctx.allocator, repo, ctx.opts.facet_table_name, ctx.facet_filter);
}

fn benchGraphQuery(ctx: BenchmarkContext) ![]u8 {
    return try graph_sqlite.traverseToon(
        ctx.db,
        ctx.allocator,
        "bench-start",
        .outbound,
        &.{"bench_link"},
        4,
        "bench-end",
    );
}

fn measureFacetQuery(ctx: BenchmarkContext, iterations: usize) !TimingStats {
    return try measure(iterations, ctx, struct {
        fn run(local: *const BenchmarkContext) !void {
            const toon = try benchFacetQuery(local.*);
            defer local.allocator.free(toon);
            std.mem.doNotOptimizeAway(toon.len);
        }
    }.run);
}

fn measureGraphQuery(ctx: BenchmarkContext, iterations: usize) !TimingStats {
    return try measure(iterations, ctx, struct {
        fn run(local: *const BenchmarkContext) !void {
            const toon = try benchGraphQuery(local.*);
            defer local.allocator.free(toon);
            std.mem.doNotOptimizeAway(toon.len);
        }
    }.run);
}

fn measureFacetMutation(ctx: BenchmarkContext, iterations: usize, comptime op: anytype) !TimingStats {
    return try measure(iterations, ctx, struct {
        fn run(local: *const BenchmarkContext) !void {
            try runSavepointScoped(local.db, "facet_benchmark_mutation", op, local);
        }
    }.run);
}

fn measureGraphMutation(ctx: BenchmarkContext, iterations: usize, comptime op: anytype) !TimingStats {
    return try measure(iterations, ctx, struct {
        fn run(local: *const BenchmarkContext) !void {
            try runSavepointScoped(local.db, "graph_benchmark_mutation", op, local);
        }
    }.run);
}

fn facetSingleAdd(ctx: *const BenchmarkContext) !void {
    try facet_sqlite.queueFacetDelta(ctx.db, ctx.opts.facet_table_id, 1, "alpha", 8_000, 1);
    _ = try facet_sqlite.mergeDeltasSafe(ctx.db, ctx.opts.facet_table_id, null);
}

fn facetSingleRemove(ctx: *const BenchmarkContext) !void {
    try facet_sqlite.queueFacetDelta(ctx.db, ctx.opts.facet_table_id, 1, "alpha", 2, -1);
    _ = try facet_sqlite.mergeDeltasSafe(ctx.db, ctx.opts.facet_table_id, null);
}

fn facetSingleUpdate(ctx: *const BenchmarkContext) !void {
    try facet_sqlite.queueFacetDelta(ctx.db, ctx.opts.facet_table_id, 1, "alpha", 4, -1);
    try facet_sqlite.queueFacetDelta(ctx.db, ctx.opts.facet_table_id, 1, "beta", 4, 1);
    _ = try facet_sqlite.mergeDeltasSafe(ctx.db, ctx.opts.facet_table_id, null);
}

fn facetBatchUpdate(ctx: *const BenchmarkContext) !void {
    var doc_id: u64 = 8_100;
    const end = doc_id + ctx.opts.facet_batch_size;
    while (doc_id < end) : (doc_id += 1) {
        try facet_sqlite.queueFacetDelta(ctx.db, ctx.opts.facet_table_id, 1, "alpha", doc_id, 1);
        try facet_sqlite.queueFacetDelta(ctx.db, ctx.opts.facet_table_id, 1, "beta", doc_id, -1);
    }
    _ = try facet_sqlite.mergeDeltasSafe(ctx.db, ctx.opts.facet_table_id, null);
}

fn graphSingleAdd(ctx: *const BenchmarkContext) !void {
    const relation_id: u32 = 88_201;
    const source_id = ctx.bench_entity_ids[3];
    const target_id = try graph_sqlite.upsertEntityNatural(ctx.db, ctx.allocator, "benchmark_node", "bench-extra-a", 1.0, "{}");
    try graph_sqlite.upsertRelation(ctx.db, .{
        .relation_id = relation_id,
        .source_id = source_id,
        .target_id = target_id,
        .relation_type = "bench_link",
        .confidence = 1.0,
    });
    try graph_sqlite.upsertAdjacency(ctx.db, "graph_lj_out", source_id, &.{relation_id}, ctx.allocator);
    try graph_sqlite.upsertAdjacency(ctx.db, "graph_lj_in", target_id, &.{relation_id}, ctx.allocator);
}

fn graphSingleRemove(ctx: *const BenchmarkContext) !void {
    const relation_id = ctx.bench_relation_ids[2];
    const source_id = ctx.bench_entity_ids[2];
    const target_id = ctx.bench_entity_ids[3];
    try graph_sqlite.deleteRelation(ctx.db, relation_id);
    try graph_sqlite.deleteAdjacency(ctx.db, "graph_lj_out", source_id, ctx.allocator);
    try graph_sqlite.deleteAdjacency(ctx.db, "graph_lj_in", target_id, ctx.allocator);
}

fn graphSingleUpdate(ctx: *const BenchmarkContext) !void {
    const relation_id = ctx.bench_relation_ids[1];
    try graph_sqlite.upsertRelation(ctx.db, .{
        .relation_id = relation_id,
        .source_id = ctx.bench_entity_ids[1],
        .target_id = ctx.bench_entity_ids[2],
        .relation_type = "bench_link",
        .confidence = 0.95,
    });
}

fn graphBatchUpdate(ctx: *const BenchmarkContext) !void {
    const extra_a = try graph_sqlite.upsertEntityNatural(ctx.db, ctx.allocator, "benchmark_node", "bench-extra-b", 1.0, "{}");
    const extra_b = try graph_sqlite.upsertEntityNatural(ctx.db, ctx.allocator, "benchmark_node", "bench-extra-c", 1.0, "{}");
    const batch_source = ctx.bench_entity_ids[0];
    const relation_a: u32 = 88_202;
    const relation_b: u32 = 88_203;
    try graph_sqlite.upsertRelation(ctx.db, .{
        .relation_id = relation_a,
        .source_id = batch_source,
        .target_id = extra_a,
        .relation_type = "bench_link",
        .confidence = 1.0,
    });
    try graph_sqlite.upsertRelation(ctx.db, .{
        .relation_id = relation_b,
        .source_id = extra_a,
        .target_id = extra_b,
        .relation_type = "bench_link",
        .confidence = 1.0,
    });
    try graph_sqlite.upsertAdjacency(ctx.db, "graph_lj_out", batch_source, &.{relation_a}, ctx.allocator);
    try graph_sqlite.upsertAdjacency(ctx.db, "graph_lj_out", extra_a, &.{relation_b}, ctx.allocator);
    try graph_sqlite.upsertAdjacency(ctx.db, "graph_lj_in", extra_a, &.{relation_a}, ctx.allocator);
    try graph_sqlite.upsertAdjacency(ctx.db, "graph_lj_in", extra_b, &.{relation_b}, ctx.allocator);

    try graph_sqlite.upsertRelation(ctx.db, .{
        .relation_id = ctx.bench_relation_ids[0],
        .source_id = ctx.bench_entity_ids[0],
        .target_id = ctx.bench_entity_ids[1],
        .relation_type = "bench_link",
        .confidence = 0.9,
    });
    try graph_sqlite.deleteRelation(ctx.db, ctx.bench_relation_ids[1]);
    try graph_sqlite.deleteAdjacency(ctx.db, "graph_lj_out", ctx.bench_entity_ids[1], ctx.allocator);
    try graph_sqlite.deleteAdjacency(ctx.db, "graph_lj_in", ctx.bench_entity_ids[2], ctx.allocator);
}

fn facetCountsToon(
    allocator: std.mem.Allocator,
    repo: facet_sqlite.Repository,
    table_name: []const u8,
    filter: ?roaring.Bitmap,
) ![]u8 {
    const docs = if (filter) |bitmap| bitmap else null;
    return try facet_store.countFacetValuesToon(
        allocator,
        repo.asFacetRepository(),
        table_name,
        "category",
        docs,
    );
}

fn measure(iterations: usize, ctx: BenchmarkContext, comptime func: anytype) !TimingStats {
    var times = std.ArrayList(u64).empty;
    defer times.deinit(ctx.allocator);

    var index: usize = 0;
    while (index < iterations) : (index += 1) {
        const start = std.Io.Timestamp.now(compat.io(), .awake);
        try @call(.auto, func, .{&ctx});
        const elapsed = start.durationTo(std.Io.Timestamp.now(compat.io(), .awake)).toNanoseconds();
        try times.append(ctx.allocator, @intCast(elapsed));
    }

    if (times.items.len > 1) {
        std.sort.block(u64, times.items, {}, struct {
            fn lessThan(_: void, lhs: u64, rhs: u64) bool {
                return lhs < rhs;
            }
        }.lessThan);
    }

    return .{
        .min_ns = if (times.items.len == 0) 0 else times.items[0],
        .p50_ns = percentileNearestRank(times.items, 50),
        .p95_ns = percentileNearestRank(times.items, 95),
        .max_ns = if (times.items.len == 0) 0 else times.items[times.items.len - 1],
        .mean_ns = if (iterations == 0) 0 else @divTrunc(sumTimes(times.items), @as(u64, @intCast(iterations))),
    };
}

fn percentileNearestRank(times: []const u64, percentile: usize) u64 {
    if (times.len == 0) return 0;
    const rank = (@as(usize, percentile) * times.len + 99) / 100;
    const index = if (rank == 0) 0 else @min(times.len - 1, rank - 1);
    return times[index];
}

fn sumTimes(times: []const u64) u64 {
    var total: u64 = 0;
    for (times) |time| total += time;
    return total;
}

fn runSavepointScoped(db: facet_sqlite.Database, comptime savepoint_name: []const u8, comptime func: anytype, ctx: anytype) !void {
    try db.exec("SAVEPOINT " ++ savepoint_name);
    errdefer {
        _ = db.exec("ROLLBACK TO SAVEPOINT " ++ savepoint_name) catch {};
        _ = db.exec("RELEASE SAVEPOINT " ++ savepoint_name) catch {};
    }

    try @call(.auto, func, .{ctx});

    try db.exec("ROLLBACK TO SAVEPOINT " ++ savepoint_name);
    try db.exec("RELEASE SAVEPOINT " ++ savepoint_name);
}

fn countRows(db: facet_sqlite.Database, table_name: []const u8) !u64 {
    const sql = try std.fmt.allocPrint(std.heap.page_allocator, "SELECT COUNT(*) FROM {s}", .{table_name});
    defer std.heap.page_allocator.free(sql);

    const stmt = try prepare(db, sql);
    defer finalize(stmt);
    if (facet_sqlite.c.sqlite3_step(stmt) != facet_sqlite.c.SQLITE_ROW) return error.MissingRow;
    return @intCast(facet_sqlite.c.sqlite3_column_int64(stmt, 0));
}

fn prepare(db: facet_sqlite.Database, sql: []const u8) !*facet_sqlite.c.sqlite3_stmt {
    var stmt: ?*facet_sqlite.c.sqlite3_stmt = null;
    if (facet_sqlite.c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != facet_sqlite.c.SQLITE_OK or stmt == null) {
        return error.PrepareFailed;
    }
    return stmt.?;
}

fn finalize(stmt: *facet_sqlite.c.sqlite3_stmt) void {
    _ = facet_sqlite.c.sqlite3_finalize(stmt);
}

test "database benchmark helper emits TOON payloads" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();

    var report = try run(db, std.testing.allocator, .{ .query_iterations = 4, .mutation_iterations = 2, .facet_doc_count = 64, .facet_batch_size = 8 });
    defer report.deinit(std.testing.allocator);

    try std.testing.expect(report.graph_entity_count > 0);
    try std.testing.expect(report.graph_relation_count > 0);
    try std.testing.expect(report.graph_alias_count == 0);
    try std.testing.expect(std.mem.indexOf(u8, report.facet_toon, "kind: facet_counts") != null);
    try std.testing.expect(std.mem.indexOf(u8, report.graph_toon, "kind: graph_traverse") != null);
    try std.testing.expect(report.facet_query.mean_ns > 0);
    try std.testing.expect(report.graph_query.mean_ns > 0);
}
