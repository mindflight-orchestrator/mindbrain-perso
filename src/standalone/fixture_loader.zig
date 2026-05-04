const std = @import("std");

pub const Error = error{
    QueryNotFound,
};

pub const Fixture = struct {
    meta: Meta,
    schema: Schema,
    documents: []Document,
    embeddings: []Embedding = &.{},
    queries: []Query,
    expected: std.json.Value,
    comparison: Comparison,
};

pub const Meta = struct {
    fixture_id: []const u8,
    title: []const u8,
    purpose: []const u8,
    primary_entrypoint: []const u8,
    target_engine_api: []const u8,
    tags: ?[]const []const u8 = null,
};

pub const Schema = struct {
    schema_name: []const u8,
    table_name: []const u8,
    key_column: []const u8,
    content_column: []const u8,
    metadata_column: []const u8,
    vector_column: ?[]const u8 = null,
    created_at_column: ?[]const u8 = null,
    updated_at_column: ?[]const u8 = null,
    language: []const u8,
};

pub const Document = struct {
    doc_id: u64,
    content: []const u8,
    metadata: std.json.Value,
};

pub const Embedding = struct {
    doc_id: u64,
    values: []const f32,
};

pub const Query = struct {
    query_id: []const u8,
    kind: []const u8,
    entrypoint: []const u8,
    engine_api: []const u8,
    params: QueryParams,
};

pub const QueryParams = struct {
    schema_name: []const u8,
    table_name: []const u8,
    query: []const u8,
    vector_column: ?[]const u8 = null,
    content_column: []const u8,
    metadata_column: []const u8,
    limit: usize,
    offset: usize = 0,
    min_score: f64 = 0.0,
    vector_weight: f64 = 0.5,
    language: []const u8,
    query_vector: ?[]const f32 = null,
};

pub const Comparison = struct {
    score_tolerance: f64 = 0.0,
    allow_extra_fields: bool = false,
    order_required: bool = true,
    compare_total_found: bool = true,
    notes: ?[]const u8 = null,
};

pub const LoadedFixture = struct {
    arena: std.heap.ArenaAllocator,
    fixture: Fixture,

    pub fn deinit(self: *LoadedFixture) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn findQuery(self: *const LoadedFixture, query_id: []const u8) Error!Query {
        for (self.fixture.queries) |query| {
            if (std.mem.eql(u8, query.query_id, query_id)) return query;
        }
        return error.QueryNotFound;
    }
};

pub fn loadFromSlice(allocator: std.mem.Allocator, json_bytes: []const u8) !LoadedFixture {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();

    const parsed = try std.json.parseFromSliceLeaky(
        Fixture,
        arena.allocator(),
        json_bytes,
        .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = false,
        },
    );

    return .{
        .arena = arena,
        .fixture = parsed,
    };
}

pub fn loadFromFile(allocator: std.mem.Allocator, path: []const u8) !LoadedFixture {
    const json_bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, allocator, .limited(1024 * 1024));
    defer allocator.free(json_bytes);
    return loadFromSlice(allocator, json_bytes);
}

test "fixture loader parses the first hybrid fixture" {
    var loaded = try loadFromFile(std.testing.allocator, "docs/fixtures/hybrid_search_baseline_fixture.json");
    defer loaded.deinit();

    try std.testing.expectEqualStrings("hybrid-search-baseline-v1", loaded.fixture.meta.fixture_id);
    try std.testing.expectEqualStrings("docs_fixture", loaded.fixture.schema.table_name);
    try std.testing.expectEqual(@as(usize, 4), loaded.fixture.documents.len);
    try std.testing.expectEqual(@as(usize, 4), loaded.fixture.embeddings.len);
    try std.testing.expectEqual(@as(usize, 1), loaded.fixture.queries.len);
}

test "fixture loader finds the hybrid query definition" {
    var loaded = try loadFromFile(std.testing.allocator, "docs/fixtures/hybrid_search_baseline_fixture.json");
    defer loaded.deinit();

    const query = try loaded.findQuery("hybrid-search-01");
    try std.testing.expectEqualStrings("hybrid_search", query.kind);
    try std.testing.expectEqualStrings("standalone.hybrid_search", query.entrypoint);
    try std.testing.expectEqual(@as(usize, 3), query.params.limit);
    try std.testing.expectEqual(@as(f64, 0.85), query.params.vector_weight);
    try std.testing.expect(query.params.query_vector != null);
    try std.testing.expectEqual(@as(usize, 3), query.params.query_vector.?.len);
}
