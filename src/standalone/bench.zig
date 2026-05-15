const std = @import("std");
const builtin = @import("builtin");
const facet_sqlite = @import("facet_sqlite.zig");
const facet_store = @import("facet_store.zig");
const graph_sqlite = @import("graph_sqlite.zig");
const search_sqlite = @import("search_sqlite.zig");
const roaring = @import("roaring.zig");
const ontology_sqlite = @import("ontology_sqlite.zig");
const query_executor = @import("query_executor.zig");
const compat = @import("zig16_compat.zig");
const tokenization_sqlite = @import("tokenization_sqlite.zig");

const DatasetConfig = struct {
    search_docs: usize = 2_000,
    facet_docs: usize = 8_000,
    graph_nodes: usize = 2_000,
};

const IterationConfig = struct {
    bitmap_ops: usize = 2_000,
    search_upserts: usize = 500,
    compact_lookups: usize = 2_000,
    compact_vector_queries: usize = 1_000,
    facet_filters: usize = 1_000,
    graph_paths: usize = 250,
    composite_queries: usize = 250,
    facet_single_mutations: usize = 50,
    facet_batch_mutations: usize = 20,
    graph_mutation_nodes: usize = 256,
    graph_single_mutations: usize = 8,
    graph_batch_mutations: usize = 5,
};

const LargeScaleConfig = struct {
    full_docs: usize = 100_000,
    mutation_docs: usize = 2_000,
    chunk_bits: u8 = 8,
    table_id: u64 = 7_701,
    table_name: []const u8 = "bench_large_docs",
};

const SyntheticFacetSpec = struct {
    facet_id: u32,
    facet_name: []const u8,
    values: []const []const u8,
};

const category_values = [_][]const u8{ "search", "graph", "systems", "ml", "ops", "finance", "science", "media" };
const region_values = [_][]const u8{ "eu", "us", "apac", "latam", "mea", "global" };
const language_values = [_][]const u8{ "english", "french", "german", "spanish", "dutch" };
const source_values = [_][]const u8{ "blog", "paper", "forum", "ticket" };
const year_bucket_values = [_][]const u8{ "2020", "2021", "2022", "2023", "2024", "2025" };

const synthetic_facet_specs = [_]SyntheticFacetSpec{
    .{ .facet_id = 1, .facet_name = "category", .values = category_values[0..] },
    .{ .facet_id = 2, .facet_name = "region", .values = region_values[0..] },
    .{ .facet_id = 3, .facet_name = "language", .values = language_values[0..] },
    .{ .facet_id = 4, .facet_name = "source", .values = source_values[0..] },
    .{ .facet_id = 5, .facet_name = "year_bucket", .values = year_bucket_values[0..] },
};

const FacetPostingCollector = struct {
    facet_id: u32,
    facet_value: []const u8,
    doc_ids: std.ArrayList(u32),
};

const FacetCollectorSet = struct {
    allocator: std.mem.Allocator,
    collectors: []FacetPostingCollector,
    offsets: []usize,

    fn init(allocator: std.mem.Allocator) !FacetCollectorSet {
        const offsets = try allocator.alloc(usize, synthetic_facet_specs.len + 1);
        var total_values: usize = 0;
        for (synthetic_facet_specs, 0..) |spec, index| {
            offsets[index] = total_values;
            total_values += spec.values.len;
        }
        offsets[synthetic_facet_specs.len] = total_values;

        const collectors = try allocator.alloc(FacetPostingCollector, total_values);
        errdefer allocator.free(collectors);

        var cursor: usize = 0;
        for (synthetic_facet_specs) |spec| {
            for (spec.values) |value| {
                collectors[cursor] = .{
                    .facet_id = spec.facet_id,
                    .facet_value = value,
                    .doc_ids = .empty,
                };
                cursor += 1;
            }
        }

        return .{
            .allocator = allocator,
            .collectors = collectors,
            .offsets = offsets,
        };
    }

    fn deinit(self: *FacetCollectorSet) void {
        for (self.collectors) |*collector| collector.doc_ids.deinit(self.allocator);
        self.allocator.free(self.collectors);
        self.allocator.free(self.offsets);
        self.* = undefined;
    }

    fn appendDoc(self: *FacetCollectorSet, doc_id: u32) !void {
        for (synthetic_facet_specs, 0..) |spec, spec_index| {
            const value_index = syntheticValueIndex(spec_index, doc_id, spec.values.len);
            try self.collectors[self.offsets[spec_index] + value_index].doc_ids.append(self.allocator, doc_id);
        }
    }

    fn persist(
        self: *FacetCollectorSet,
        db: facet_sqlite.Database,
        table_id: u64,
        table_name: []const u8,
        chunk_bits: u8,
    ) !void {
        try facet_sqlite.upsertFacetTable(db, table_id, "public", table_name, chunk_bits);
        for (synthetic_facet_specs) |spec| {
            try facet_sqlite.upsertFacetDefinition(db, table_id, spec.facet_id, spec.facet_name);
        }

        const shift_bits_u32: u5 = @intCast(chunk_bits);
        const shift_bits_u64: u6 = @intCast(chunk_bits);
        const chunk_mask: u32 = @intCast((@as(u64, 1) << shift_bits_u64) - 1);

        for (self.collectors) |*collector| {
            var index: usize = 0;
            while (index < collector.doc_ids.items.len) {
                const chunk_id: u32 = collector.doc_ids.items[index] >> shift_bits_u32;
                var bitmap = try roaring.Bitmap.empty();
                defer bitmap.deinit();

                while (index < collector.doc_ids.items.len and (collector.doc_ids.items[index] >> shift_bits_u32) == chunk_id) : (index += 1) {
                    bitmap.add(collector.doc_ids.items[index] & chunk_mask);
                }

                try facet_sqlite.upsertPostingBitmap(
                    db,
                    self.allocator,
                    table_id,
                    collector.facet_id,
                    collector.facet_value,
                    chunk_id,
                    bitmap,
                );
            }
        }
    }

    fn deletePersisted(
        self: *FacetCollectorSet,
        db: facet_sqlite.Database,
        table_id: u64,
        chunk_bits: u8,
    ) !void {
        const shift_bits: u5 = @intCast(chunk_bits);

        for (self.collectors) |*collector| {
            var index: usize = 0;
            while (index < collector.doc_ids.items.len) {
                const chunk_id: u32 = collector.doc_ids.items[index] >> shift_bits;
                while (index < collector.doc_ids.items.len and (collector.doc_ids.items[index] >> shift_bits) == chunk_id) : (index += 1) {}
                try facet_sqlite.deletePostingBitmap(db, table_id, collector.facet_id, collector.facet_value, chunk_id);
            }
        }
    }
};

const ColdStat = struct {
    name: []const u8,
    total_ns: u64,
    unit_count: usize,

    fn print(self: ColdStat) void {
        std.debug.print(
            "{s}_total_ms={d:.3} {s}_per_unit_us={d:.3} units={d}\n",
            .{
                self.name,
                nsToMs(self.total_ns),
                self.name,
                if (self.unit_count == 0) 0.0 else nsToUs(self.total_ns) / @as(f64, @floatFromInt(self.unit_count)),
                self.unit_count,
            },
        );
    }
};

const MicroStat = struct {
    name: []const u8,
    iterations: usize,
    min_ns: u64,
    p50_ns: u64,
    p95_ns: u64,
    max_ns: u64,
    mean_ns: u64,

    fn print(self: MicroStat) void {
        std.debug.print(
            "{s}_min_ns={d} {s}_p50_ns={d} {s}_p95_ns={d} {s}_max_ns={d} {s}_mean_ns={d} iterations={d}\n",
            .{
                self.name,
                self.min_ns,
                self.name,
                self.p50_ns,
                self.name,
                self.p95_ns,
                self.name,
                self.max_ns,
                self.name,
                self.mean_ns,
                self.iterations,
            },
        );
    }
};

const SimdReport = struct {
    os: []const u8,
    arch: []const u8,
    backend: []const u8,
    runtime_source: []const u8,
    avx2: bool,
    avx512: bool,
    avx512_compile_disabled: bool,

    fn print(self: SimdReport) void {
        std.debug.print(
            "simd os={s} arch={s} backend={s} runtime_source={s} avx2={} avx512={} avx512_compile_disabled={}\n",
            .{
                self.os,
                self.arch,
                self.backend,
                self.runtime_source,
                self.avx2,
                self.avx512,
                self.avx512_compile_disabled,
            },
        );
    }
};

const RoaringSupport = struct {
    avx2: bool,
    avx512: bool,
};

pub fn main(init: std.process.Init) !void {
    compat.setIo(init.io);
    var gpa_state = std.heap.DebugAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();

    const dataset = DatasetConfig{};
    const iters = IterationConfig{};

    std.debug.print("standalone benchmark\n", .{});
    detectSimdReport().print();
    std.debug.print(
        "dataset search_docs={d} facet_docs={d} graph_nodes={d}\n",
        .{ dataset.search_docs, dataset.facet_docs, dataset.graph_nodes },
    );
    std.debug.print(
        "iterations bitmap_ops={d} search_upserts={d} compact_lookups={d} compact_vector_queries={d} facet_filters={d} graph_paths={d} composite_queries={d} facet_single_mutations={d} facet_batch_mutations={d} graph_mutation_nodes={d} graph_single_mutations={d} graph_batch_mutations={d}\n",
        .{
            iters.bitmap_ops,
            iters.search_upserts,
            iters.compact_lookups,
            iters.compact_vector_queries,
            iters.facet_filters,
            iters.graph_paths,
            iters.composite_queries,
            iters.facet_single_mutations,
            iters.facet_batch_mutations,
            iters.graph_mutation_nodes,
            iters.graph_single_mutations,
            iters.graph_batch_mutations,
        },
    );

    std.debug.print("cold benchmarks\n", .{});
    (try coldSearchBulkInsert(allocator, dataset.search_docs)).print();
    (try coldSearchArtifactBuild(allocator, dataset.search_docs)).print();
    (try coldSearchReload(allocator, dataset.search_docs)).print();
    (try coldCompactReload(allocator, dataset.search_docs)).print();

    std.debug.print("micro benchmarks\n", .{});
    (try microBitmapBuildDense(allocator, iters.bitmap_ops)).print();
    (try microBitmapBuildSparse(allocator, iters.bitmap_ops)).print();
    (try microBitmapSerialize(allocator, iters.bitmap_ops)).print();
    (try microBitmapAnd(allocator, iters.bitmap_ops)).print();
    (try microBitmapOr(allocator, iters.bitmap_ops)).print();
    (try microBitmapDenseCardinality(allocator, iters.bitmap_ops)).print();
    (try microBitmapDenseToArray(allocator, iters.bitmap_ops)).print();
    (try microBitmapDenseDeserializeAnd(allocator, iters.bitmap_ops)).print();
    (try microSearchSingleDocUpsert(allocator, iters.search_upserts)).print();
    (try microCompactPostingLookup(allocator, dataset.search_docs, iters.compact_lookups)).print();
    (try microCompactVectorSearch(allocator, dataset.search_docs, iters.compact_vector_queries)).print();
    (try microFacetFilter(allocator, dataset.facet_docs, iters.facet_filters)).print();
    (try microGraphShortestPath(allocator, dataset.graph_nodes, iters.graph_paths)).print();
    (try microCompositeQuery(allocator, iters.composite_queries)).print();
    (try microFacetSingleAdd(allocator, dataset.facet_docs, iters.facet_single_mutations)).print();
    (try microFacetSingleRemove(allocator, dataset.facet_docs, iters.facet_single_mutations)).print();
    (try microFacetSingleUpdate(allocator, dataset.facet_docs, iters.facet_single_mutations)).print();
    (try microFacetBatchUpdate(allocator, dataset.facet_docs, iters.facet_batch_mutations)).print();
    (try microGraphSingleAdd(allocator, iters.graph_mutation_nodes, iters.graph_single_mutations)).print();
    (try microGraphSingleUpdate(allocator, iters.graph_mutation_nodes, iters.graph_single_mutations)).print();
    (try microGraphSingleRemove(allocator, iters.graph_mutation_nodes, iters.graph_single_mutations)).print();
    (try microGraphBatchPatch(allocator, iters.graph_mutation_nodes, iters.graph_batch_mutations)).print();

    std.debug.print("large-scale benchmarks\n", .{});
    try runLargeScaleBenchmarks(allocator);
}

fn detectSimdReport() SimdReport {
    const os = @tagName(builtin.os.tag);
    const arch = @tagName(builtin.cpu.arch);
    const avx512_compile_disabled = (builtin.cpu.arch != .x86_64);

    switch (builtin.cpu.arch) {
        .x86_64 => {
            const support = detectRoaringSupportX86();
            return .{
                .os = os,
                .arch = arch,
                .backend = if (support.avx512) "x86_avx512" else if (support.avx2) "x86_avx2" else "x86_scalar",
                .runtime_source = "croaring_runtime_detected",
                .avx2 = support.avx2,
                .avx512 = support.avx512,
                .avx512_compile_disabled = avx512_compile_disabled,
            };
        },
        .aarch64 => {
            return .{
                .os = os,
                .arch = arch,
                .backend = if (builtin.os.tag == .macos) "arm64_neon_apple_silicon" else "arm64_neon",
                .runtime_source = "arch_inferred",
                .avx2 = false,
                .avx512 = false,
                .avx512_compile_disabled = avx512_compile_disabled,
            };
        },
        else => {
            return .{
                .os = os,
                .arch = arch,
                .backend = "unknown_or_scalar",
                .runtime_source = "arch_inferred",
                .avx2 = false,
                .avx512 = false,
                .avx512_compile_disabled = avx512_compile_disabled,
            };
        },
    }
}

fn detectRoaringSupportX86() RoaringSupport {
    if (comptime builtin.cpu.arch != .x86_64) {
        return .{ .avx2 = false, .avx512 = false };
    }
    const support = (struct {
        extern fn croaring_hardware_support() c_int;
    }).croaring_hardware_support();
    return .{
        .avx2 = (support & 1) != 0,
        .avx512 = (support & 2) != 0,
    };
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

fn coldSearchBulkInsert(allocator: std.mem.Allocator, doc_count: usize) !ColdStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    const total_ns = try bulkInsertSearchDocs(db, allocator, doc_count);
    return .{ .name = "cold_search_bulk_insert", .total_ns = total_ns, .unit_count = doc_count };
}

fn coldSearchArtifactBuild(allocator: std.mem.Allocator, doc_count: usize) !ColdStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    _ = try bulkInsertSearchDocs(db, allocator, doc_count);

    var timer = try compat.Timer.start();
    try search_sqlite.rebuildSearchArtifacts(db, allocator);
    return .{ .name = "cold_search_artifact_build", .total_ns = timer.read(), .unit_count = doc_count };
}

fn coldSearchReload(allocator: std.mem.Allocator, doc_count: usize) !ColdStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    _ = try bulkInsertSearchDocs(db, allocator, doc_count);

    var timer = try compat.Timer.start();
    var store = try search_sqlite.loadSearchStore(db, allocator);
    defer store.deinit();
    return .{ .name = "cold_search_reload", .total_ns = timer.read(), .unit_count = doc_count };
}

fn coldCompactReload(allocator: std.mem.Allocator, doc_count: usize) !ColdStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    _ = try bulkInsertSearchDocs(db, allocator, doc_count);
    try search_sqlite.rebuildSearchArtifacts(db, allocator);

    var timer = try compat.Timer.start();
    var store = try search_sqlite.loadCompactSearchStore(db, allocator);
    defer store.deinit();
    return .{ .name = "cold_compact_reload", .total_ns = timer.read(), .unit_count = doc_count };
}

fn microBitmapBuildDense(allocator: std.mem.Allocator, iterations: usize) !MicroStat {
    const values = try buildValueArray(allocator, 20_000, 1);
    defer allocator.free(values);

    const Ctx = struct { values: []const u32 };
    var ctx = Ctx{ .values = values };
    return measure("micro_bitmap_build_dense", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            var bitmap = try roaring.Bitmap.fromSlice(local.values);
            defer bitmap.deinit();
            bitmap.optimizeForStorage();
            std.mem.doNotOptimizeAway(bitmap.cardinality());
        }
    }.run);
}

fn microBitmapBuildSparse(allocator: std.mem.Allocator, iterations: usize) !MicroStat {
    const values = try buildValueArray(allocator, 20_000, 13);
    defer allocator.free(values);

    const Ctx = struct { values: []const u32 };
    var ctx = Ctx{ .values = values };
    return measure("micro_bitmap_build_sparse", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            var bitmap = try roaring.Bitmap.fromSlice(local.values);
            defer bitmap.deinit();
            bitmap.optimizeForStorage();
            std.mem.doNotOptimizeAway(bitmap.cardinality());
        }
    }.run);
}

fn microBitmapSerialize(allocator: std.mem.Allocator, iterations: usize) !MicroStat {
    const values = try buildValueArray(allocator, 20_000, 2);
    defer allocator.free(values);

    var bitmap = try roaring.Bitmap.fromSlice(values);
    defer bitmap.deinit();

    const Ctx = struct {
        allocator: std.mem.Allocator,
        bitmap: *roaring.Bitmap,
    };
    var ctx = Ctx{ .allocator = allocator, .bitmap = &bitmap };
    return measure("micro_bitmap_serialize", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            const payload = try local.bitmap.serializePortableStable(local.allocator);
            defer local.allocator.free(payload);
            std.mem.doNotOptimizeAway(payload.len);
        }
    }.run);
}

fn microBitmapDenseCardinality(allocator: std.mem.Allocator, iterations: usize) !MicroStat {
    const values = try buildValueArray(allocator, 65_536, 1);
    defer allocator.free(values);

    var bitmap = try roaring.Bitmap.fromSlice(values);
    defer bitmap.deinit();

    const Ctx = struct { bitmap: *roaring.Bitmap };
    var ctx = Ctx{ .bitmap = &bitmap };
    return measure("micro_bitmap_dense_cardinality", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            std.mem.doNotOptimizeAway(local.bitmap.cardinality());
        }
    }.run);
}

fn microBitmapDenseToArray(allocator: std.mem.Allocator, iterations: usize) !MicroStat {
    const values = try buildValueArray(allocator, 65_536, 1);
    defer allocator.free(values);

    var bitmap = try roaring.Bitmap.fromSlice(values);
    defer bitmap.deinit();

    const Ctx = struct {
        allocator: std.mem.Allocator,
        bitmap: *roaring.Bitmap,
    };
    var ctx = Ctx{ .allocator = allocator, .bitmap = &bitmap };
    return measure("micro_bitmap_dense_to_array", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            const items = try local.bitmap.toArray(local.allocator);
            defer local.allocator.free(items);
            std.mem.doNotOptimizeAway(items.len);
        }
    }.run);
}

fn microBitmapDenseDeserializeAnd(allocator: std.mem.Allocator, iterations: usize) !MicroStat {
    const left_values = try buildValueArray(allocator, 65_536, 1);
    defer allocator.free(left_values);
    const right_values = try buildValueArray(allocator, 65_536, 2);
    defer allocator.free(right_values);

    var left = try roaring.Bitmap.fromSlice(left_values);
    defer left.deinit();
    var right = try roaring.Bitmap.fromSlice(right_values);
    defer right.deinit();

    const left_payload = try left.serializePortableStable(allocator);
    defer allocator.free(left_payload);
    const right_payload = try right.serializePortableStable(allocator);
    defer allocator.free(right_payload);

    const Ctx = struct {
        allocator: std.mem.Allocator,
        left_payload: []const u8,
        right_payload: []const u8,
    };
    var ctx = Ctx{
        .allocator = allocator,
        .left_payload = left_payload,
        .right_payload = right_payload,
    };
    return measure("micro_bitmap_dense_deserialize_and", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            var left_bitmap = try roaring.Bitmap.deserializePortable(local.left_payload);
            defer left_bitmap.deinit();
            var right_bitmap = try roaring.Bitmap.deserializePortable(local.right_payload);
            defer right_bitmap.deinit();
            var result = try left_bitmap.andNew(right_bitmap);
            defer result.deinit();
            std.mem.doNotOptimizeAway(result.cardinality());
        }
    }.run);
}

fn microBitmapAnd(allocator: std.mem.Allocator, iterations: usize) !MicroStat {
    const left_values = try buildValueArray(allocator, 20_000, 2);
    defer allocator.free(left_values);
    const right_values = try allocator.alloc(u32, 20_000);
    defer allocator.free(right_values);
    for (right_values, 0..) |*value, index| value.* = @intCast(index * 2 + 1);

    var left = try roaring.Bitmap.fromSlice(left_values);
    defer left.deinit();
    var right = try roaring.Bitmap.fromSlice(right_values);
    defer right.deinit();

    const Ctx = struct {
        left: *roaring.Bitmap,
        right: *roaring.Bitmap,
    };
    var ctx = Ctx{ .left = &left, .right = &right };
    return measure("micro_bitmap_and", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            var result = try local.left.andNew(local.right.*);
            defer result.deinit();
            std.mem.doNotOptimizeAway(result.cardinality());
        }
    }.run);
}

fn microBitmapOr(allocator: std.mem.Allocator, iterations: usize) !MicroStat {
    const left_values = try buildValueArray(allocator, 20_000, 2);
    defer allocator.free(left_values);
    const right_values = try allocator.alloc(u32, 20_000);
    defer allocator.free(right_values);
    for (right_values, 0..) |*value, index| value.* = @intCast(index * 2 + 1);

    var left = try roaring.Bitmap.fromSlice(left_values);
    defer left.deinit();
    var right = try roaring.Bitmap.fromSlice(right_values);
    defer right.deinit();

    const Ctx = struct {
        left: *roaring.Bitmap,
        right: *roaring.Bitmap,
    };
    var ctx = Ctx{ .left = &left, .right = &right };
    return measure("micro_bitmap_or", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            var result = try local.left.orNew(local.right.*);
            defer result.deinit();
            std.mem.doNotOptimizeAway(result.cardinality());
        }
    }.run);
}

fn microSearchSingleDocUpsert(allocator: std.mem.Allocator, iterations: usize) !MicroStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    const content = "single doc zig sqlite roaring benchmark";
    const embedding = [_]f32{ 0.7, 0.2, 0.1 };
    const Ctx = struct {
        allocator: std.mem.Allocator,
        db: facet_sqlite.Database,
        content: []const u8,
        embedding: []const f32,
    };
    var ctx = Ctx{
        .allocator = allocator,
        .db = db,
        .content = content,
        .embedding = &embedding,
    };
    return measure("micro_search_single_doc_upsert", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            try search_sqlite.upsertSearchDocument(local.db, 1, 1, local.content, "english");
            try search_sqlite.upsertSearchEmbedding(local.db, local.allocator, 1, 1, local.embedding);
        }
    }.run);
}

fn microCompactPostingLookup(allocator: std.mem.Allocator, doc_count: usize, iterations: usize) !MicroStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    _ = try bulkInsertSearchDocs(db, allocator, doc_count);
    try search_sqlite.rebuildSearchArtifacts(db, allocator);

    var store = try search_sqlite.loadCompactSearchStore(db, allocator);
    defer store.deinit();
    const repo = store.bm25Repository();
    const term_hash = tokenization_sqlite.hashTermLexeme("roaring");

    const Ctx = struct {
        allocator: std.mem.Allocator,
        repo: @TypeOf(repo),
        term_hash: u64,
    };
    var ctx = Ctx{ .allocator = allocator, .repo = repo, .term_hash = term_hash };
    return measure("micro_compact_posting_lookup", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            var bitmap = (try local.repo.getPostingBitmapFn(local.repo.ctx, local.allocator, 1, local.term_hash)).?;
            defer bitmap.deinit();
            std.mem.doNotOptimizeAway(bitmap.cardinality());
        }
    }.run);
}

fn microCompactVectorSearch(allocator: std.mem.Allocator, doc_count: usize, iterations: usize) !MicroStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    _ = try bulkInsertSearchDocs(db, allocator, doc_count);
    try search_sqlite.rebuildSearchArtifacts(db, allocator);

    var store = try search_sqlite.loadCompactSearchStore(db, allocator);
    defer store.deinit();
    const repo = store.vectorRepository();
    const query_vector = [_]f32{ 0.8, 0.2, 0.1 };

    const Ctx = struct {
        allocator: std.mem.Allocator,
        repo: @TypeOf(repo),
        query_vector: []const f32,
    };
    var ctx = Ctx{
        .allocator = allocator,
        .repo = repo,
        .query_vector = &query_vector,
    };
    return measure("micro_compact_vector_search", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            const nearest = try local.repo.searchNearestFn(local.repo.ctx, local.allocator, .{
                .table_name = "docs_fixture",
                .key_column = "doc_id",
                .vector_column = "embedding",
                .query_vector = local.query_vector,
                .limit = 5,
            });
            defer local.allocator.free(nearest);
            if (nearest.len > 0) std.mem.doNotOptimizeAway(nearest[0].doc_id);
        }
    }.run);
}

fn microFacetFilter(allocator: std.mem.Allocator, doc_count: usize, iterations: usize) !MicroStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try seedFacetBenchmarkData(db, allocator, doc_count);
    const repo = facet_sqlite.Repository{ .db = &db };

    const Ctx = struct {
        allocator: std.mem.Allocator,
        repo: @TypeOf(repo),
    };
    var ctx = Ctx{ .allocator = allocator, .repo = repo };
    return measure("micro_facet_filter", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            var result = (try facet_store.filterDocuments(
                local.allocator,
                local.repo.asFacetRepository(),
                "bench_facets",
                &.{
                    .{ .facet_name = "domain", .values = &.{"alpha"} },
                    .{ .facet_name = "region", .values = &.{"eu"} },
                },
            )).?;
            defer result.deinit();
            std.mem.doNotOptimizeAway(result.cardinality());
        }
    }.run);
}

fn microGraphShortestPath(allocator: std.mem.Allocator, node_count: usize, iterations: usize) !MicroStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try seedGraphChain(db, allocator, node_count);

    var runtime = try graph_sqlite.loadRuntime(db, allocator);
    defer runtime.deinit();

    const Ctx = struct {
        allocator: std.mem.Allocator,
        runtime: *graph_sqlite.InMemoryRuntime,
        target_id: u32,
        max_depth: usize,
    };
    var ctx = Ctx{
        .allocator = allocator,
        .runtime = &runtime,
        .target_id = @intCast(node_count),
        .max_depth = node_count,
    };
    return measure("micro_graph_shortest_path", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            const hops = try local.runtime.shortestPathHops(local.allocator, 1, local.target_id, .{}, local.max_depth);
            if (hops == null) return error.MissingRow;
            std.mem.doNotOptimizeAway(hops.?);
        }
    }.run);
}

fn microCompositeQuery(allocator: std.mem.Allocator, iterations: usize) !MicroStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try seedCompositeQueryData(db, allocator);

    var runtime = try query_executor.Runtime.init(&db, allocator);
    defer runtime.deinit();

    const Ctx = struct {
        allocator: std.mem.Allocator,
        runtime: *query_executor.Runtime,
    };
    var ctx = Ctx{ .allocator = allocator, .runtime = &runtime };
    return measure("micro_composite_query", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            var result = try local.runtime.execute(local.allocator, .{
                .workspace_id = "default",
                .table_name = "bench_query",
                .facet_filters = &.{
                    .{ .facet_name = "category", .values = &.{"search"} },
                    .{ .facet_name = "region", .values = &.{"eu"} },
                },
                .count_facets = &.{"category"},
                .graph = .{
                    .start_nodes = &.{10_000},
                    .max_hops = 2,
                    .filter = .{ .edge_types = &.{"works_for"} },
                },
                .projection = .{
                    .agent_id = "bench-agent",
                    .query = "Ada",
                    .limit = 5,
                },
            });
            defer result.deinit(local.allocator);
            if (result.documents) |bitmap| std.mem.doNotOptimizeAway(bitmap.cardinality());
            std.mem.doNotOptimizeAway(result.projections.len);
        }
    }.run);
}

fn microFacetSingleAdd(allocator: std.mem.Allocator, doc_count: usize, iterations: usize) !MicroStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try seedFacetBenchmarkData(db, allocator, doc_count);

    const Ctx = struct {
        db: facet_sqlite.Database,
    };
    var ctx = Ctx{ .db = db };
    return measure("micro_facet_single_add", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            try runSavepointScoped(local.db, "facet_single_add", struct {
                fn op(inner: *Ctx) !void {
                    try facet_sqlite.queueFacetDelta(inner.db, 900, 1, "alpha", 8_123, 1);
                    _ = try facet_sqlite.mergeDeltasSafe(inner.db, 900, null);
                }
            }.op, local);
        }
    }.run);
}

fn microFacetSingleRemove(allocator: std.mem.Allocator, doc_count: usize, iterations: usize) !MicroStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try seedFacetBenchmarkData(db, allocator, doc_count);

    const Ctx = struct {
        db: facet_sqlite.Database,
    };
    var ctx = Ctx{ .db = db };
    return measure("micro_facet_single_remove", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            try runSavepointScoped(local.db, "facet_single_remove", struct {
                fn op(inner: *Ctx) !void {
                    try facet_sqlite.queueFacetDelta(inner.db, 900, 1, "alpha", 42, -1);
                    _ = try facet_sqlite.mergeDeltasSafe(inner.db, 900, null);
                }
            }.op, local);
        }
    }.run);
}

fn microFacetSingleUpdate(allocator: std.mem.Allocator, doc_count: usize, iterations: usize) !MicroStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try seedFacetBenchmarkData(db, allocator, doc_count);

    const Ctx = struct {
        db: facet_sqlite.Database,
    };
    var ctx = Ctx{ .db = db };
    return measure("micro_facet_single_update", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            try runSavepointScoped(local.db, "facet_single_update", struct {
                fn op(inner: *Ctx) !void {
                    try facet_sqlite.queueFacetDelta(inner.db, 900, 1, "alpha", 42, -1);
                    try facet_sqlite.queueFacetDelta(inner.db, 900, 1, "beta", 42, 1);
                    _ = try facet_sqlite.mergeDeltasSafe(inner.db, 900, null);
                }
            }.op, local);
        }
    }.run);
}

fn microFacetBatchUpdate(allocator: std.mem.Allocator, doc_count: usize, iterations: usize) !MicroStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try seedFacetBenchmarkData(db, allocator, doc_count);

    const Ctx = struct {
        db: facet_sqlite.Database,
    };
    var ctx = Ctx{ .db = db };
    return measure("micro_facet_batch_update", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            try runSavepointScoped(local.db, "facet_batch_update", struct {
                fn op(inner: *Ctx) !void {
                    var doc_id: u64 = 1_000;
                    while (doc_id < 1_128) : (doc_id += 2) {
                        try facet_sqlite.queueFacetDelta(inner.db, 900, 1, "alpha", doc_id, -1);
                        try facet_sqlite.queueFacetDelta(inner.db, 900, 1, "beta", doc_id, 1);
                    }
                    _ = try facet_sqlite.mergeDeltasSafe(inner.db, 900, null);
                }
            }.op, local);
        }
    }.run);
}

fn microGraphSingleAdd(allocator: std.mem.Allocator, node_count: usize, iterations: usize) !MicroStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try seedGraphChain(db, allocator, node_count);

    const Ctx = struct {
        db: facet_sqlite.Database,
        allocator: std.mem.Allocator,
    };
    var ctx = Ctx{ .db = db, .allocator = allocator };
    return measure("micro_graph_single_add", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            try runSavepointScoped(local.db, "graph_single_add", struct {
                fn op(inner: *Ctx) !void {
                    _ = try graph_sqlite.upsertRelationNatural(
                        inner.db,
                        "skip",
                        1,
                        3,
                        0.7,
                        null,
                        null,
                        "{}",
                        null,
                        null,
                    );
                    try graph_sqlite.refreshEntityDegree(inner.db);
                    try graph_sqlite.rebuildAdjacency(inner.db, inner.allocator);
                }
            }.op, local);
        }
    }.run);
}

fn microGraphSingleUpdate(allocator: std.mem.Allocator, node_count: usize, iterations: usize) !MicroStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try seedGraphChain(db, allocator, node_count);

    const Ctx = struct {
        db: facet_sqlite.Database,
        allocator: std.mem.Allocator,
    };
    var ctx = Ctx{ .db = db, .allocator = allocator };
    return measure("micro_graph_single_update", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            try runSavepointScoped(local.db, "graph_single_update", struct {
                fn op(inner: *Ctx) !void {
                    _ = try graph_sqlite.upsertRelationNatural(
                        inner.db,
                        "next",
                        1,
                        2,
                        0.95,
                        null,
                        null,
                        "{\"bench\":\"single_update\"}",
                        null,
                        null,
                    );
                    try graph_sqlite.refreshEntityDegree(inner.db);
                    try graph_sqlite.rebuildAdjacency(inner.db, inner.allocator);
                }
            }.op, local);
        }
    }.run);
}

fn microGraphSingleRemove(allocator: std.mem.Allocator, node_count: usize, iterations: usize) !MicroStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try seedGraphChain(db, allocator, node_count);

    const Ctx = struct {
        db: facet_sqlite.Database,
        allocator: std.mem.Allocator,
    };
    var ctx = Ctx{ .db = db, .allocator = allocator };
    return measure("micro_graph_single_remove", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            try runSavepointScoped(local.db, "graph_single_remove", struct {
                fn op(inner: *Ctx) !void {
                    const artifacts = [_]graph_sqlite.KnowledgePatchArtifact{
                        .{
                            .action = "deprecate",
                            .relation_type = "next",
                            .source = "node-2",
                            .target = "node-3",
                        },
                    };
                    _ = try graph_sqlite.applyKnowledgePatch(
                        inner.db,
                        inner.allocator,
                        "bench-graph-single-remove",
                        "bench",
                        0.8,
                        artifacts[0..],
                        "bench",
                    );
                }
            }.op, local);
        }
    }.run);
}

fn microGraphBatchPatch(allocator: std.mem.Allocator, node_count: usize, iterations: usize) !MicroStat {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try seedGraphChain(db, allocator, node_count);

    const Ctx = struct {
        db: facet_sqlite.Database,
        allocator: std.mem.Allocator,
    };
    var ctx = Ctx{ .db = db, .allocator = allocator };
    return measure("micro_graph_batch_patch", iterations, &ctx, struct {
        fn run(local: *Ctx) !void {
            try runSavepointScoped(local.db, "graph_batch_patch", struct {
                fn op(inner: *Ctx) !void {
                    const artifacts = [_]graph_sqlite.KnowledgePatchArtifact{
                        .{
                            .action = "upsert_relation",
                            .relation_type = "skip",
                            .source = "node-1",
                            .target = "node-4",
                            .confidence = 0.7,
                            .metadata_json = "{}",
                        },
                        .{
                            .action = "upsert_relation",
                            .relation_type = "skip",
                            .source = "node-2",
                            .target = "node-5",
                            .confidence = 0.7,
                            .metadata_json = "{}",
                        },
                        .{
                            .action = "upsert_relation",
                            .relation_type = "next",
                            .source = "node-3",
                            .target = "node-4",
                            .confidence = 0.96,
                            .metadata_json = "{\"bench\":\"batch\"}",
                        },
                        .{
                            .action = "deprecate",
                            .relation_type = "next",
                            .source = "node-4",
                            .target = "node-5",
                        },
                        .{
                            .action = "deprecate",
                            .relation_type = "next",
                            .source = "node-5",
                            .target = "node-6",
                        },
                    };
                    _ = try graph_sqlite.applyKnowledgePatch(
                        inner.db,
                        inner.allocator,
                        "bench-graph-batch",
                        "bench",
                        0.9,
                        artifacts[0..],
                        "bench",
                    );
                }
            }.op, local);
        }
    }.run);
}

fn measure(comptime name: []const u8, iterations: usize, ctx: anytype, comptime func: anytype) !MicroStat {
    var times = std.ArrayList(u64).empty;
    defer times.deinit(std.heap.page_allocator);

    var index: usize = 0;
    while (index < iterations) : (index += 1) {
        var timer = try compat.Timer.start();
        try @call(.auto, func, .{ctx});
        const elapsed = timer.read();
        try times.append(std.heap.page_allocator, elapsed);
    }

    if (times.items.len > 1) {
        std.sort.block(u64, times.items, {}, struct {
            fn lessThan(_: void, lhs: u64, rhs: u64) bool {
                return lhs < rhs;
            }
        }.lessThan);
    }

    const min_ns = if (times.items.len == 0) 0 else times.items[0];
    const max_ns = if (times.items.len == 0) 0 else times.items[times.items.len - 1];
    const mean_ns = if (iterations == 0) 0 else @divTrunc(sumTimes(times.items), @as(u64, @intCast(iterations)));
    const p50_ns = percentileNearestRank(times.items, 50);
    const p95_ns = percentileNearestRank(times.items, 95);

    return .{
        .name = name,
        .iterations = iterations,
        .min_ns = if (iterations == 0) 0 else min_ns,
        .p50_ns = p50_ns,
        .p95_ns = p95_ns,
        .max_ns = max_ns,
        .mean_ns = mean_ns,
    };
}

fn sumTimes(times: []const u64) u64 {
    var total: u64 = 0;
    for (times) |time| total += time;
    return total;
}

fn percentileNearestRank(times: []const u64, percentile: usize) u64 {
    if (times.len == 0) return 0;
    const rank = (@as(usize, percentile) * times.len + 99) / 100;
    const index = if (rank == 0) 0 else @min(times.len - 1, rank - 1);
    return times[index];
}

fn bulkInsertSearchDocs(db: facet_sqlite.Database, allocator: std.mem.Allocator, doc_count: usize) !u64 {
    var timer = try compat.Timer.start();
    for (0..doc_count) |index| {
        const doc_id: u64 = @intCast(index + 1);
        const content = try std.fmt.allocPrint(allocator, "doc {d} zig sqlite roaring benchmark", .{doc_id});
        defer allocator.free(content);
        const embedding = [_]f32{
            @as(f32, @floatFromInt((index % 17) + 1)) / 17.0,
            @as(f32, @floatFromInt((index % 13) + 1)) / 13.0,
            @as(f32, @floatFromInt((index % 11) + 1)) / 11.0,
        };
        try search_sqlite.upsertSearchDocument(db, 1, doc_id, content, "english");
        try search_sqlite.upsertSearchEmbedding(db, allocator, 1, doc_id, &embedding);
    }
    return timer.read();
}

fn seedFacetBenchmarkData(db: facet_sqlite.Database, allocator: std.mem.Allocator, doc_count: usize) !void {
    try facet_sqlite.upsertFacetTable(db, 900, "public", "bench_facets", 8);
    try facet_sqlite.upsertFacetDefinition(db, 900, 1, "domain");
    try facet_sqlite.upsertFacetDefinition(db, 900, 2, "region");

    var domain_alpha = try roaring.Bitmap.empty();
    defer domain_alpha.deinit();
    var domain_beta = try roaring.Bitmap.empty();
    defer domain_beta.deinit();
    var region_eu = try roaring.Bitmap.empty();
    defer region_eu.deinit();
    var region_us = try roaring.Bitmap.empty();
    defer region_us.deinit();

    for (0..doc_count) |index| {
        const doc_id: u32 = @intCast(index);
        if (index % 2 == 0) domain_alpha.add(doc_id) else domain_beta.add(doc_id);
        if (index % 3 == 0) region_eu.add(doc_id) else region_us.add(doc_id);
    }

    try facet_sqlite.upsertPostingBitmap(db, allocator, 900, 1, "alpha", 0, domain_alpha);
    try facet_sqlite.upsertPostingBitmap(db, allocator, 900, 1, "beta", 0, domain_beta);
    try facet_sqlite.upsertPostingBitmap(db, allocator, 900, 2, "eu", 0, region_eu);
    try facet_sqlite.upsertPostingBitmap(db, allocator, 900, 2, "us", 0, region_us);
}

fn seedGraphChain(db: facet_sqlite.Database, allocator: std.mem.Allocator, node_count: usize) !void {
    for (0..node_count) |index| {
        const entity_id: u32 = @intCast(index + 1);
        const name = try std.fmt.allocPrint(allocator, "node-{d}", .{entity_id});
        defer allocator.free(name);
        try graph_sqlite.upsertEntity(db, entity_id, "concept", name);
    }

    for (0..node_count - 1) |index| {
        const relation_id: u32 = @intCast(index + 1);
        const source_id: u32 = @intCast(index + 1);
        const target_id: u32 = @intCast(index + 2);
        try graph_sqlite.upsertRelation(db, .{
            .relation_id = relation_id,
            .source_id = source_id,
            .target_id = target_id,
            .relation_type = "next",
            .confidence = 1.0,
        });
    }

    for (0..node_count) |index| {
        const entity_id: u32 = @intCast(index + 1);

        var outgoing = std.ArrayList(u32).empty;
        defer outgoing.deinit(allocator);
        if (index + 1 < node_count) try outgoing.append(allocator, @intCast(index + 1));
        try graph_sqlite.upsertAdjacency(db, "graph_lj_out", entity_id, outgoing.items, allocator);

        var incoming = std.ArrayList(u32).empty;
        defer incoming.deinit(allocator);
        if (index > 0) try incoming.append(allocator, @intCast(index));
        try graph_sqlite.upsertAdjacency(db, "graph_lj_in", entity_id, incoming.items, allocator);
    }
}

fn seedCompositeQueryData(db: facet_sqlite.Database, allocator: std.mem.Allocator) !void {
    try facet_sqlite.upsertFacetTable(db, 901, "public", "bench_query", 4);
    try facet_sqlite.upsertFacetDefinition(db, 901, 1, "category");
    try facet_sqlite.upsertFacetDefinition(db, 901, 2, "region");

    var category_search = try roaring.Bitmap.fromSlice(&.{ 1, 3, 5, 7 });
    defer category_search.deinit();
    try facet_sqlite.upsertPostingBitmap(db, allocator, 901, 1, "search", 0, category_search);

    var region_eu = try roaring.Bitmap.fromSlice(&.{ 1, 3, 4, 7 });
    defer region_eu.deinit();
    try facet_sqlite.upsertPostingBitmap(db, allocator, 901, 2, "eu", 0, region_eu);

    try ontology_sqlite.upsertFacet(db, .{
        .id = "bench-ref-1",
        .schema_id = "ghostcrab:taxonomy",
        .content = "Ada benchmark row",
        .facets_json = "{\"category\":\"search\"}",
        .workspace_id = "default",
        .doc_id = 1,
    });
    try ontology_sqlite.upsertFacet(db, .{
        .id = "bench-ref-3",
        .schema_id = "ghostcrab:taxonomy",
        .content = "Acme benchmark row",
        .facets_json = "{\"category\":\"search\"}",
        .workspace_id = "default",
        .doc_id = 3,
    });

    try graph_sqlite.upsertEntity(db, 10_000, "person", "Ada");
    try graph_sqlite.upsertEntity(db, 10_001, "company", "Acme");
    try graph_sqlite.upsertEntity(db, 10_002, "company", "Labs");
    try graph_sqlite.upsertRelation(db, .{ .relation_id = 20_000, .source_id = 10_000, .target_id = 10_001, .relation_type = "works_for", .confidence = 0.9 });
    try graph_sqlite.upsertRelation(db, .{ .relation_id = 20_001, .source_id = 10_001, .target_id = 10_002, .relation_type = "works_for", .confidence = 0.8 });
    try graph_sqlite.upsertAdjacency(db, "graph_lj_out", 10_000, &.{20_000}, allocator);
    try graph_sqlite.upsertAdjacency(db, "graph_lj_out", 10_001, &.{20_001}, allocator);
    try graph_sqlite.upsertAdjacency(db, "graph_lj_in", 10_001, &.{20_000}, allocator);
    try graph_sqlite.upsertAdjacency(db, "graph_lj_in", 10_002, &.{20_001}, allocator);

    try ontology_sqlite.insertProjection(db, .{
        .id = "bench-proj-1",
        .agent_id = "bench-agent",
        .scope = "default",
        .proj_type = "FACT",
        .content = "Ada works for Acme",
        .weight = 1.0,
        .source_ref = "bench-ref-1",
        .source_type = "taxonomy",
        .status = "active",
    });
}

fn buildValueArray(allocator: std.mem.Allocator, value_count: usize, stride: usize) ![]u32 {
    const values = try allocator.alloc(u32, value_count);
    for (values, 0..) |*value, index| value.* = @intCast(index * stride);
    return values;
}

fn avgNs(total_ns: u64, iterations: usize) u64 {
    if (iterations == 0) return 0;
    return @intCast(@divTrunc(total_ns, iterations));
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000_000.0;
}

fn nsToUs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000.0;
}

fn runLargeScaleBenchmarks(allocator: std.mem.Allocator) !void {
    const cfg = LargeScaleConfig{};
    const mutation_start_doc = alignedMutationStartDoc(cfg.full_docs, cfg.chunk_bits);

    std.debug.print(
        "large dataset full_docs={d} mutation_docs={d} mutation_start_doc={d} chunk_bits={d}\n",
        .{ cfg.full_docs, cfg.mutation_docs, mutation_start_doc, cfg.chunk_bits },
    );

    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    const full_search_sqlite_ns = try persistSyntheticSearchDocs(db, allocator, 1, cfg.full_docs);
    const full_facet_sqlite_ns = try persistSyntheticFacetBatch(
        db,
        allocator,
        cfg.table_id,
        cfg.table_name,
        cfg.chunk_bits,
        1,
        cfg.full_docs,
    );

    var timer = try compat.Timer.start();
    try search_sqlite.rebuildSearchArtifacts(db, allocator);
    const full_zig_artifact_ns = timer.read();

    timer = try compat.Timer.start();
    var compact_store = try search_sqlite.loadCompactSearchStore(db, allocator);
    compact_store.deinit();
    const full_zig_compact_reload_ns = timer.read();

    (ColdStat{ .name = "large_full_sqlite_search_persist", .total_ns = full_search_sqlite_ns, .unit_count = cfg.full_docs }).print();
    (ColdStat{ .name = "large_full_sqlite_facet_persist", .total_ns = full_facet_sqlite_ns, .unit_count = cfg.full_docs }).print();
    (ColdStat{ .name = "large_full_zig_artifact_build", .total_ns = full_zig_artifact_ns, .unit_count = cfg.full_docs }).print();
    (ColdStat{ .name = "large_full_zig_compact_reload", .total_ns = full_zig_compact_reload_ns, .unit_count = cfg.full_docs }).print();

    const add_search_sqlite_ns = try persistSyntheticSearchDocs(db, allocator, mutation_start_doc, cfg.mutation_docs);
    const add_facet_sqlite_ns = try persistSyntheticFacetBatch(
        db,
        allocator,
        cfg.table_id,
        cfg.table_name,
        cfg.chunk_bits,
        mutation_start_doc,
        cfg.mutation_docs,
    );

    const add_zig_artifact_ns = try upsertSyntheticSearchArtifactsIncremental(
        db,
        allocator,
        mutation_start_doc,
        cfg.mutation_docs,
    );

    timer = try compat.Timer.start();
    compact_store = try search_sqlite.loadCompactSearchStore(db, allocator);
    compact_store.deinit();
    const add_zig_compact_reload_ns = timer.read();

    (ColdStat{ .name = "large_add_sqlite_search_persist", .total_ns = add_search_sqlite_ns, .unit_count = cfg.mutation_docs }).print();
    (ColdStat{ .name = "large_add_sqlite_facet_persist", .total_ns = add_facet_sqlite_ns, .unit_count = cfg.mutation_docs }).print();
    (ColdStat{ .name = "large_add_zig_incremental_artifact_update", .total_ns = add_zig_artifact_ns, .unit_count = cfg.mutation_docs }).print();
    (ColdStat{ .name = "large_add_zig_compact_reload", .total_ns = add_zig_compact_reload_ns, .unit_count = cfg.full_docs + cfg.mutation_docs }).print();

    const remove_search_sqlite_ns = try deleteSyntheticSearchDocs(db, mutation_start_doc, cfg.mutation_docs);
    const remove_facet_sqlite_ns = try deleteSyntheticFacetBatch(
        db,
        allocator,
        cfg.table_id,
        cfg.chunk_bits,
        mutation_start_doc,
        cfg.mutation_docs,
    );

    const remove_zig_artifact_ns = try deleteSyntheticSearchArtifactsIncremental(
        db,
        allocator,
        mutation_start_doc,
        cfg.mutation_docs,
    );

    timer = try compat.Timer.start();
    compact_store = try search_sqlite.loadCompactSearchStore(db, allocator);
    defer compact_store.deinit();
    const remove_zig_compact_reload_ns = timer.read();

    (ColdStat{ .name = "large_remove_sqlite_search_delete", .total_ns = remove_search_sqlite_ns, .unit_count = cfg.mutation_docs }).print();
    (ColdStat{ .name = "large_remove_sqlite_facet_delete", .total_ns = remove_facet_sqlite_ns, .unit_count = cfg.mutation_docs }).print();
    (ColdStat{ .name = "large_remove_zig_incremental_artifact_delete", .total_ns = remove_zig_artifact_ns, .unit_count = cfg.mutation_docs }).print();
    (ColdStat{ .name = "large_remove_zig_compact_reload", .total_ns = remove_zig_compact_reload_ns, .unit_count = cfg.full_docs }).print();
}

fn alignedMutationStartDoc(full_docs: usize, chunk_bits: u8) u64 {
    const shift_bits: u6 = @intCast(chunk_bits);
    return @as(u64, (@as(u64, @intCast(full_docs)) >> shift_bits) + 1) << shift_bits;
}

fn persistSyntheticSearchDocs(
    db: facet_sqlite.Database,
    allocator: std.mem.Allocator,
    start_doc_id: u64,
    doc_count: usize,
) !u64 {
    var timer = try compat.Timer.start();
    for (0..doc_count) |index| {
        const doc_id = start_doc_id + index;
        const content = try renderSyntheticDocumentContent(allocator, @intCast(doc_id));
        defer allocator.free(content);
        try search_sqlite.upsertSearchDocument(db, 1, doc_id, content, "english");
    }
    return timer.read();
}

fn deleteSyntheticSearchDocs(
    db: facet_sqlite.Database,
    start_doc_id: u64,
    doc_count: usize,
) !u64 {
    var timer = try compat.Timer.start();
    for (0..doc_count) |index| {
        try search_sqlite.deleteSearchDocument(db, 1, start_doc_id + index);
    }
    return timer.read();
}

fn upsertSyntheticSearchArtifactsIncremental(
    db: facet_sqlite.Database,
    allocator: std.mem.Allocator,
    start_doc_id: u64,
    doc_count: usize,
) !u64 {
    var timer = try compat.Timer.start();
    for (0..doc_count) |index| {
        try search_sqlite.upsertSearchArtifactsForDocument(db, allocator, 1, start_doc_id + index);
    }
    return timer.read();
}

fn deleteSyntheticSearchArtifactsIncremental(
    db: facet_sqlite.Database,
    allocator: std.mem.Allocator,
    start_doc_id: u64,
    doc_count: usize,
) !u64 {
    var timer = try compat.Timer.start();
    for (0..doc_count) |index| {
        try search_sqlite.deleteSearchArtifactsForDocument(db, allocator, 1, start_doc_id + index);
    }
    return timer.read();
}

fn persistSyntheticFacetBatch(
    db: facet_sqlite.Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    table_name: []const u8,
    chunk_bits: u8,
    start_doc_id: u64,
    doc_count: usize,
) !u64 {
    var collectors = try FacetCollectorSet.init(allocator);
    defer collectors.deinit();

    for (0..doc_count) |index| {
        try collectors.appendDoc(@intCast(start_doc_id + index));
    }

    var timer = try compat.Timer.start();
    try collectors.persist(db, table_id, table_name, chunk_bits);
    return timer.read();
}

fn deleteSyntheticFacetBatch(
    db: facet_sqlite.Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    chunk_bits: u8,
    start_doc_id: u64,
    doc_count: usize,
) !u64 {
    var collectors = try FacetCollectorSet.init(allocator);
    defer collectors.deinit();

    for (0..doc_count) |index| {
        try collectors.appendDoc(@intCast(start_doc_id + index));
    }

    var timer = try compat.Timer.start();
    try collectors.deletePersisted(db, table_id, chunk_bits);
    return timer.read();
}

fn syntheticValueIndex(spec_index: usize, doc_id: u32, value_count: usize) usize {
    const mixed = @as(usize, doc_id) * 131 + spec_index * 17 + @as(usize, doc_id >> 3);
    return @mod(mixed, value_count);
}

fn syntheticFacetValue(spec_index: usize, doc_id: u32) []const u8 {
    const spec = synthetic_facet_specs[spec_index];
    return spec.values[syntheticValueIndex(spec_index, doc_id, spec.values.len)];
}

fn renderSyntheticDocumentContent(allocator: std.mem.Allocator, doc_id: u32) ![]u8 {
    const category = syntheticFacetValue(0, doc_id);
    const region = syntheticFacetValue(1, doc_id);
    const language = syntheticFacetValue(2, doc_id);
    const source = syntheticFacetValue(3, doc_id);
    const year_bucket = syntheticFacetValue(4, doc_id);

    return try std.fmt.allocPrint(
        allocator,
        "{s} {s} {s} {s} {s} doc{d} zig bm25",
        .{ category, region, language, source, year_bucket, doc_id },
    );
}
