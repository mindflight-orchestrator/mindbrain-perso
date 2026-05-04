const std = @import("std");
const interfaces = @import("interfaces.zig");
const roaring = @import("roaring.zig");
const toon_exports = @import("toon_exports.zig");

pub const Error = error{
    TableNotFound,
    FacetNotFound,
};

const PostingKey = struct {
    table_id: u64,
    facet_id: u32,
    facet_value: []const u8,
};

const ValueNodeKey = struct {
    table_id: u64,
    facet_id: u32,
    value_id: u32,
};

pub const Store = struct {
    allocator: std.mem.Allocator,
    table_configs: std.ArrayList(interfaces.FacetTableConfig),
    facet_ids: std.ArrayList(FacetIdEntry),
    postings: std.ArrayList(PostingEntry),
    value_nodes: std.ArrayList(ValueNodeEntry),

    pub fn init(allocator: std.mem.Allocator) Store {
        return .{
            .allocator = allocator,
            .table_configs = std.ArrayList(interfaces.FacetTableConfig).empty,
            .facet_ids = std.ArrayList(FacetIdEntry).empty,
            .postings = std.ArrayList(PostingEntry).empty,
            .value_nodes = std.ArrayList(ValueNodeEntry).empty,
        };
    }

    pub fn deinit(self: *Store) void {
        for (self.table_configs.items) |config| {
            self.allocator.free(config.schema_name);
            self.allocator.free(config.table_name);
        }
        self.table_configs.deinit(self.allocator);

        for (self.facet_ids.items) |entry| {
            self.allocator.free(entry.facet_name);
        }
        self.facet_ids.deinit(self.allocator);

        for (self.postings.items) |entry| {
            self.allocator.free(entry.key.facet_value);
            var posting = entry.posting;
            posting.bitmap.deinit();
        }
        self.postings.deinit(self.allocator);

        for (self.value_nodes.items) |*entry| {
            self.allocator.free(entry.node.facet_value);
            if (entry.node.children_bitmap) |*bitmap| bitmap.deinit();
        }
        self.value_nodes.deinit(self.allocator);

        self.* = undefined;
    }

    pub fn asRepository(self: *Store) interfaces.FacetRepository {
        return .{
            .ctx = @ptrCast(self),
            .getTableConfigFn = getTableConfig,
            .getFacetIdFn = getFacetId,
            .listFacetValuesFn = listFacetValues,
            .getFacetValueNodeFn = getFacetValueNode,
            .getFacetChildrenFn = getFacetChildren,
            .getPostingsFn = getPostings,
        };
    }

    pub fn registerTable(self: *Store, config: interfaces.FacetTableConfig) !void {
        try self.table_configs.append(self.allocator, .{
            .table_id = config.table_id,
            .chunk_bits = config.chunk_bits,
            .schema_name = try self.allocator.dupe(u8, config.schema_name),
            .table_name = try self.allocator.dupe(u8, config.table_name),
        });
    }

    pub fn registerFacet(self: *Store, table_id: u64, facet_id: u32, facet_name: []const u8) !void {
        try self.facet_ids.append(self.allocator, .{
            .table_id = table_id,
            .facet_id = facet_id,
            .facet_name = try self.allocator.dupe(u8, facet_name),
        });
    }

    pub fn addPosting(
        self: *Store,
        table_id: u64,
        facet_id: u32,
        facet_value: []const u8,
        chunk_id: u32,
        doc_ids_in_chunk: []const u32,
    ) !void {
        try self.postings.append(self.allocator, .{
            .key = .{
                .table_id = table_id,
                .facet_id = facet_id,
                .facet_value = try self.allocator.dupe(u8, facet_value),
            },
            .posting = .{
                .chunk_id = chunk_id,
                .bitmap = try roaring.Bitmap.fromSlice(doc_ids_in_chunk),
            },
        });
    }

    pub fn registerFacetValueNode(
        self: *Store,
        table_id: u64,
        facet_id: u32,
        value_id: u32,
        facet_value: []const u8,
        child_ids: ?[]const u32,
    ) !void {
        try self.value_nodes.append(self.allocator, .{
            .key = .{
                .table_id = table_id,
                .facet_id = facet_id,
                .value_id = value_id,
            },
            .node = .{
                .value_id = value_id,
                .facet_id = facet_id,
                .facet_value = try self.allocator.dupe(u8, facet_value),
                .children_bitmap = if (child_ids) |ids| try roaring.Bitmap.fromSlice(ids) else null,
            },
        });
    }

    fn getTableConfig(ctx: *anyopaque, allocator: std.mem.Allocator, table_name: []const u8) anyerror!interfaces.FacetTableConfig {
        _ = allocator;
        const self: *Store = @ptrCast(@alignCast(ctx));
        for (self.table_configs.items) |config| {
            if (std.mem.eql(u8, config.table_name, table_name)) return config;
        }
        return error.TableNotFound;
    }

    fn getFacetId(ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, facet_name: []const u8) anyerror!?u32 {
        _ = allocator;
        const self: *Store = @ptrCast(@alignCast(ctx));
        for (self.facet_ids.items) |entry| {
            if (entry.table_id == table_id and std.mem.eql(u8, entry.facet_name, facet_name)) {
                return entry.facet_id;
            }
        }
        return null;
    }

    fn getPostings(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        table_id: u64,
        facet_id: u32,
        values: []const []const u8,
    ) anyerror![]interfaces.FacetPosting {
        const self: *Store = @ptrCast(@alignCast(ctx));
        var results = std.ArrayList(interfaces.FacetPosting).empty;
        defer results.deinit(allocator);

        for (self.postings.items) |entry| {
            if (entry.key.table_id != table_id or entry.key.facet_id != facet_id) continue;
            if (!containsValue(values, entry.key.facet_value)) continue;

            try results.append(allocator, .{
                .chunk_id = entry.posting.chunk_id,
                .bitmap = try entry.posting.bitmap.clone(),
            });
        }

        return results.toOwnedSlice(allocator);
    }

    fn listFacetValues(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        table_id: u64,
        facet_id: u32,
    ) anyerror![][]const u8 {
        const self: *Store = @ptrCast(@alignCast(ctx));
        var values = std.ArrayList([]const u8).empty;
        defer {
            for (values.items) |value| allocator.free(value);
            values.deinit(allocator);
        }

        for (self.postings.items) |entry| {
            if (entry.key.table_id != table_id or entry.key.facet_id != facet_id) continue;
            if (containsValue(values.items, entry.key.facet_value)) continue;

            try values.append(allocator, try allocator.dupe(u8, entry.key.facet_value));
        }

        return values.toOwnedSlice(allocator);
    }

    fn getFacetValueNode(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        table_id: u64,
        facet_id: u32,
        facet_value: []const u8,
    ) anyerror!?interfaces.FacetValueNode {
        const self: *Store = @ptrCast(@alignCast(ctx));
        for (self.value_nodes.items) |entry| {
            if (entry.key.table_id != table_id or entry.key.facet_id != facet_id) continue;
            if (!std.mem.eql(u8, entry.node.facet_value, facet_value)) continue;
            return try cloneValueNode(allocator, entry.node);
        }
        return null;
    }

    fn getFacetChildren(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        table_id: u64,
        value_id: u32,
    ) anyerror![]interfaces.FacetValueNode {
        const self: *Store = @ptrCast(@alignCast(ctx));
        const parent = findValueNode(self, table_id, value_id) orelse return &.{};
        if (parent.node.children_bitmap == null) return &.{};

        const child_ids = try parent.node.children_bitmap.?.toArray(allocator);
        defer allocator.free(child_ids);

        var children = std.ArrayList(interfaces.FacetValueNode).empty;
        defer {
            for (children.items) |*node| deinitValueNode(allocator, node);
            children.deinit(allocator);
        }

        for (child_ids) |child_id| {
            const child = findValueNode(self, table_id, child_id) orelse continue;
            try children.append(allocator, try cloneValueNode(allocator, child.node));
        }

        return children.toOwnedSlice(allocator);
    }
};

const FacetIdEntry = struct {
    table_id: u64,
    facet_id: u32,
    facet_name: []const u8,
};

const PostingEntry = struct {
    key: PostingKey,
    posting: interfaces.FacetPosting,
};

const ValueNodeEntry = struct {
    key: ValueNodeKey,
    node: interfaces.FacetValueNode,
};

pub fn filterDocuments(
    allocator: std.mem.Allocator,
    repository: interfaces.FacetRepository,
    table_name: []const u8,
    filters: []const interfaces.FacetValueFilter,
) !?roaring.Bitmap {
    if (filters.len == 0) return null;

    const table_config = try repository.getTableConfigFn(repository.ctx, allocator, table_name);
    var final_bitmap: ?roaring.Bitmap = null;
    errdefer if (final_bitmap) |*bm| bm.deinit();

    for (filters) |filter| {
        const maybe_facet_id = try repository.getFacetIdFn(
            repository.ctx,
            allocator,
            table_config.table_id,
            filter.facet_name,
        );
        if (maybe_facet_id == null) return null;

        const postings = try repository.getPostingsFn(
            repository.ctx,
            allocator,
            table_config.table_id,
            maybe_facet_id.?,
            filter.values,
        );
        defer {
            for (postings) |posting| {
                var bitmap = posting.bitmap;
                bitmap.deinit();
            }
            allocator.free(postings);
        }

        const facet_bitmap = try reconstructFacetBitmap(allocator, table_config.chunk_bits, postings);
        if (facet_bitmap == null) {
            if (final_bitmap) |*bm| bm.deinit();
            return null;
        }

        if (final_bitmap == null) {
            final_bitmap = facet_bitmap.?;
        } else {
            final_bitmap.?.andInPlace(facet_bitmap.?);
            var current = facet_bitmap.?;
            current.deinit();

            if (final_bitmap.?.isEmpty()) {
                final_bitmap.?.deinit();
                return null;
            }
        }
    }

    return final_bitmap;
}

pub fn countFacetValues(
    allocator: std.mem.Allocator,
    repository: interfaces.FacetRepository,
    table_name: []const u8,
    facet_name: []const u8,
    filter_bitmap: ?roaring.Bitmap,
) ![]interfaces.FacetCount {
    const table_config = try repository.getTableConfigFn(repository.ctx, allocator, table_name);
    const maybe_facet_id = try repository.getFacetIdFn(repository.ctx, allocator, table_config.table_id, facet_name);
    if (maybe_facet_id == null) return &.{};

    const facet_id = maybe_facet_id.?;
    const values = try repository.listFacetValuesFn(repository.ctx, allocator, table_config.table_id, facet_id);
    defer {
        for (values) |value| allocator.free(value);
        allocator.free(values);
    }

    var counts = std.ArrayList(interfaces.FacetCount).empty;
    defer {
        for (counts.items) |count| allocator.free(count.facet_value);
        counts.deinit(allocator);
    }

    for (values) |facet_value| {
        const postings = try repository.getPostingsFn(repository.ctx, allocator, table_config.table_id, facet_id, &.{facet_value});
        defer {
            for (postings) |posting| {
                var bitmap = posting.bitmap;
                bitmap.deinit();
            }
            allocator.free(postings);
        }

        const maybe_bitmap = try reconstructFacetBitmap(allocator, table_config.chunk_bits, postings);
        if (maybe_bitmap == null) continue;

        var facet_bitmap = maybe_bitmap.?;
        defer facet_bitmap.deinit();

        const cardinality = if (filter_bitmap) |filter| blk: {
            var intersection = try facet_bitmap.andNew(filter);
            defer intersection.deinit();
            break :blk intersection.cardinality();
        } else facet_bitmap.cardinality();

        if (cardinality == 0) continue;

        try counts.append(allocator, .{
            .facet_name = facet_name,
            .facet_value = try allocator.dupe(u8, facet_value),
            .cardinality = cardinality,
            .facet_id = facet_id,
        });
    }

    std.sort.block(interfaces.FacetCount, counts.items, {}, struct {
        fn lessThan(_: void, lhs: interfaces.FacetCount, rhs: interfaces.FacetCount) bool {
            if (lhs.cardinality != rhs.cardinality) return lhs.cardinality > rhs.cardinality;
            return std.mem.order(u8, lhs.facet_value, rhs.facet_value) == .lt;
        }
    }.lessThan);

    return counts.toOwnedSlice(allocator);
}

pub fn countFacetValuesToon(
    allocator: std.mem.Allocator,
    repository: interfaces.FacetRepository,
    table_name: []const u8,
    facet_name: []const u8,
    filter_bitmap: ?roaring.Bitmap,
) ![]u8 {
    const counts = try countFacetValues(allocator, repository, table_name, facet_name, filter_bitmap);
    defer {
        for (counts) |count| allocator.free(count.facet_value);
        allocator.free(counts);
    }

    return try toon_exports.encodeFacetCountsAlloc(
        allocator,
        table_name,
        facet_name,
        counts,
        toon_exports.default_options,
    );
}

pub fn listHierarchyChildren(
    allocator: std.mem.Allocator,
    repository: interfaces.FacetRepository,
    table_name: []const u8,
    facet_name: []const u8,
    facet_value: []const u8,
) ![]interfaces.FacetValueNode {
    const table_config = try repository.getTableConfigFn(repository.ctx, allocator, table_name);
    const maybe_facet_id = try repository.getFacetIdFn(repository.ctx, allocator, table_config.table_id, facet_name);
    if (maybe_facet_id == null) return &.{};

    const maybe_node = try repository.getFacetValueNodeFn(
        repository.ctx,
        allocator,
        table_config.table_id,
        maybe_facet_id.?,
        facet_value,
    );
    if (maybe_node == null) return &.{};
    defer {
        var node = maybe_node.?;
        deinitValueNode(allocator, &node);
    }

    return try repository.getFacetChildrenFn(
        repository.ctx,
        allocator,
        table_config.table_id,
        maybe_node.?.value_id,
    );
}

fn reconstructFacetBitmap(
    allocator: std.mem.Allocator,
    chunk_bits: u8,
    postings: []const interfaces.FacetPosting,
) !?roaring.Bitmap {
    var facet_bitmap: ?roaring.Bitmap = null;
    errdefer if (facet_bitmap) |*bm| bm.deinit();

    for (postings) |posting| {
        const in_chunk = try posting.bitmap.toArray(allocator);
        defer allocator.free(in_chunk);

        for (in_chunk) |doc_id_in_chunk| {
            const original_id: u32 = (posting.chunk_id << @intCast(chunk_bits)) | doc_id_in_chunk;
            if (facet_bitmap == null) facet_bitmap = try roaring.Bitmap.empty();
            facet_bitmap.?.add(original_id);
        }
    }

    return facet_bitmap;
}

fn containsValue(values: []const []const u8, candidate: []const u8) bool {
    for (values) |value| {
        if (std.mem.eql(u8, value, candidate)) return true;
    }
    return false;
}

fn findValueNode(self: *Store, table_id: u64, value_id: u32) ?*const ValueNodeEntry {
    for (self.value_nodes.items) |*entry| {
        if (entry.key.table_id == table_id and entry.key.value_id == value_id) return entry;
    }
    return null;
}

fn cloneValueNode(allocator: std.mem.Allocator, node: interfaces.FacetValueNode) !interfaces.FacetValueNode {
    return .{
        .value_id = node.value_id,
        .facet_id = node.facet_id,
        .facet_value = try allocator.dupe(u8, node.facet_value),
        .children_bitmap = if (node.children_bitmap) |bitmap| try bitmap.clone() else null,
    };
}

fn deinitValueNode(allocator: std.mem.Allocator, node: *interfaces.FacetValueNode) void {
    allocator.free(node.facet_value);
    if (node.children_bitmap) |*bitmap| bitmap.deinit();
    node.* = undefined;
}

test "facet store filters documents with OR within a facet and AND across facets" {
    var store = Store.init(std.testing.allocator);
    defer store.deinit();

    try store.registerTable(.{
        .table_id = 1,
        .chunk_bits = 4,
        .schema_name = "public",
        .table_name = "docs_fixture",
    });
    try store.registerFacet(1, 1, "category");
    try store.registerFacet(1, 2, "region");

    try store.addPosting(1, 1, "search", 0, &.{ 1, 2, 4 });
    try store.addPosting(1, 1, "filtering", 0, &.{3});
    try store.addPosting(1, 2, "eu", 0, &.{ 1, 3, 4 });
    try store.addPosting(1, 2, "us", 0, &.{2});

    var result = (try filterDocuments(
        std.testing.allocator,
        store.asRepository(),
        "docs_fixture",
        &.{
            .{ .facet_name = "category", .values = &.{ "search", "filtering" } },
            .{ .facet_name = "region", .values = &.{"eu"} },
        },
    )).?;
    defer result.deinit();

    const ids = try result.toArray(std.testing.allocator);
    defer std.testing.allocator.free(ids);

    try std.testing.expectEqualSlices(u32, &.{ 1, 3, 4 }, ids);
}

test "facet store reconstructs original ids from chunked postings" {
    var store = Store.init(std.testing.allocator);
    defer store.deinit();

    try store.registerTable(.{
        .table_id = 7,
        .chunk_bits = 4,
        .schema_name = "public",
        .table_name = "chunked_docs",
    });
    try store.registerFacet(7, 1, "tag");
    try store.addPosting(7, 1, "alpha", 0, &.{2});
    try store.addPosting(7, 1, "alpha", 1, &.{1});

    var result = (try filterDocuments(
        std.testing.allocator,
        store.asRepository(),
        "chunked_docs",
        &.{
            .{ .facet_name = "tag", .values = &.{"alpha"} },
        },
    )).?;
    defer result.deinit();

    const ids = try result.toArray(std.testing.allocator);
    defer std.testing.allocator.free(ids);

    try std.testing.expectEqualSlices(u32, &.{ 2, 17 }, ids);
}

test "facet store counts facet values with optional filter bitmap" {
    var store = Store.init(std.testing.allocator);
    defer store.deinit();

    try store.registerTable(.{
        .table_id = 1,
        .chunk_bits = 4,
        .schema_name = "public",
        .table_name = "docs_fixture",
    });
    try store.registerFacet(1, 1, "category");
    try store.addPosting(1, 1, "search", 0, &.{ 1, 2, 4 });
    try store.addPosting(1, 1, "filtering", 0, &.{3});

    var filter_bitmap = try roaring.Bitmap.fromSlice(&.{ 1, 3, 4 });
    defer filter_bitmap.deinit();

    const counts = try countFacetValues(
        std.testing.allocator,
        store.asRepository(),
        "docs_fixture",
        "category",
        filter_bitmap,
    );
    defer {
        for (counts) |count| std.testing.allocator.free(count.facet_value);
        std.testing.allocator.free(counts);
    }

    try std.testing.expectEqual(@as(usize, 2), counts.len);
    try std.testing.expectEqualStrings("search", counts[0].facet_value);
    try std.testing.expectEqual(@as(u64, 2), counts[0].cardinality);
    try std.testing.expectEqualStrings("filtering", counts[1].facet_value);
    try std.testing.expectEqual(@as(u64, 1), counts[1].cardinality);
}

test "facet store aggregates chunked postings for the same facet value" {
    var store = Store.init(std.testing.allocator);
    defer store.deinit();

    try store.registerTable(.{
        .table_id = 1,
        .chunk_bits = 4,
        .schema_name = "public",
        .table_name = "docs_fixture",
    });
    try store.registerFacet(1, 1, "category");

    try store.addPosting(1, 1, "search", 0, &.{ 1, 2 });
    try store.addPosting(1, 1, "search", 1, &.{ 0, 3 });
    try store.addPosting(1, 1, "filtering", 0, &.{4});

    const counts = try countFacetValues(
        std.testing.allocator,
        store.asRepository(),
        "docs_fixture",
        "category",
        null,
    );
    defer {
        for (counts) |count| std.testing.allocator.free(count.facet_value);
        std.testing.allocator.free(counts);
    }

    try std.testing.expectEqual(@as(usize, 2), counts.len);
    try std.testing.expectEqualStrings("search", counts[0].facet_value);
    try std.testing.expectEqual(@as(u64, 4), counts[0].cardinality);
    try std.testing.expectEqualStrings("filtering", counts[1].facet_value);
    try std.testing.expectEqual(@as(u64, 1), counts[1].cardinality);
}

test "facet store filters chunked postings using reconstructed document ids" {
    var store = Store.init(std.testing.allocator);
    defer store.deinit();

    try store.registerTable(.{
        .table_id = 1,
        .chunk_bits = 4,
        .schema_name = "public",
        .table_name = "docs_fixture",
    });
    try store.registerFacet(1, 1, "category");

    try store.addPosting(1, 1, "search", 0, &.{ 1, 2 });
    try store.addPosting(1, 1, "search", 1, &.{3});
    try store.addPosting(1, 1, "filtering", 0, &.{2});
    try store.addPosting(1, 1, "filtering", 1, &.{1});

    var filter_bitmap = try roaring.Bitmap.fromSlice(&.{ 2, 19 });
    defer filter_bitmap.deinit();

    const counts = try countFacetValues(
        std.testing.allocator,
        store.asRepository(),
        "docs_fixture",
        "category",
        filter_bitmap,
    );
    defer {
        for (counts) |count| std.testing.allocator.free(count.facet_value);
        std.testing.allocator.free(counts);
    }

    try std.testing.expectEqual(@as(usize, 2), counts.len);
    try std.testing.expectEqualStrings("search", counts[0].facet_value);
    try std.testing.expectEqual(@as(u64, 2), counts[0].cardinality);
    try std.testing.expectEqualStrings("filtering", counts[1].facet_value);
    try std.testing.expectEqual(@as(u64, 1), counts[1].cardinality);
}

test "facet store has a TOON export variant for counts" {
    var store = Store.init(std.testing.allocator);
    defer store.deinit();

    try store.registerTable(.{
        .table_id = 1,
        .chunk_bits = 4,
        .schema_name = "public",
        .table_name = "docs_fixture",
    });
    try store.registerFacet(1, 1, "category");
    try store.addPosting(1, 1, "search", 0, &.{ 1, 2, 4 });
    try store.addPosting(1, 1, "filtering", 0, &.{3});

    const toon = try countFacetValuesToon(
        std.testing.allocator,
        store.asRepository(),
        "docs_fixture",
        "category",
        null,
    );
    defer std.testing.allocator.free(toon);

    try std.testing.expect(std.mem.indexOf(u8, toon, "kind: facet_counts") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "table_name: docs_fixture") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "counts[2\t]{facet_name\tfacet_value\tcardinality\tfacet_id}:") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "category\tsearch\t3\t1") != null);
}

test "facet store lists hierarchy children via children bitmap" {
    var store = Store.init(std.testing.allocator);
    defer store.deinit();

    try store.registerTable(.{
        .table_id = 1,
        .chunk_bits = 4,
        .schema_name = "public",
        .table_name = "docs_fixture",
    });
    try store.registerFacet(1, 1, "main_category");
    try store.registerFacet(1, 2, "category");
    try store.registerFacetValueNode(1, 1, 10, "science", &.{ 20, 21 });
    try store.registerFacetValueNode(1, 2, 20, "physics", null);
    try store.registerFacetValueNode(1, 2, 21, "chemistry", null);

    const children = try listHierarchyChildren(
        std.testing.allocator,
        store.asRepository(),
        "docs_fixture",
        "main_category",
        "science",
    );
    defer {
        for (children) |*child| deinitValueNode(std.testing.allocator, child);
        std.testing.allocator.free(children);
    }

    try std.testing.expectEqual(@as(usize, 2), children.len);
    try std.testing.expectEqualStrings("physics", children[0].facet_value);
    try std.testing.expectEqualStrings("chemistry", children[1].facet_value);
}
