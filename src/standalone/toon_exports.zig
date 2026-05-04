const std = @import("std");
const ztoon = @import("ztoon");
const interfaces = @import("interfaces.zig");

const Field = ztoon.Field;
const Value = ztoon.Value;

pub const default_options = ztoon.EncodeOptions{
    .delimiter = .tab,
    .indent_width = 2,
};

pub fn encodeFacetCountsAlloc(
    allocator: std.mem.Allocator,
    table_name: []const u8,
    facet_name: []const u8,
    counts: []const interfaces.FacetCount,
    options: ztoon.EncodeOptions,
) ![]u8 {
    const root = try buildFacetCountsValue(allocator, table_name, facet_name, counts);
    defer deinitOwnedValue(allocator, root);
    return try ztoon.encodeAlloc(allocator, root, options);
}

pub fn encodeValueAlloc(
    allocator: std.mem.Allocator,
    value: Value,
    options: ztoon.EncodeOptions,
) ![]u8 {
    return try ztoon.encodeAlloc(allocator, value, options);
}

pub fn encodePackContextAlloc(
    allocator: std.mem.Allocator,
    user_id: []const u8,
    query: []const u8,
    scope: ?[]const u8,
    rows: anytype,
    options: ztoon.EncodeOptions,
) ![]u8 {
    const root = try buildPackContextValue(allocator, user_id, query, scope, rows);
    defer deinitOwnedValue(allocator, root);
    return try ztoon.encodeAlloc(allocator, root, options);
}

pub fn encodeCoverageReportAlloc(
    allocator: std.mem.Allocator,
    report: anytype,
    options: ztoon.EncodeOptions,
) ![]u8 {
    const root = try buildCoverageReportValue(allocator, report);
    defer deinitOwnedValue(allocator, root);
    return try ztoon.encodeAlloc(allocator, root, options);
}

pub fn encodeSearchCompactSnapshotAlloc(
    allocator: std.mem.Allocator,
    snapshot: anytype,
    options: ztoon.EncodeOptions,
) ![]u8 {
    const root = try buildSearchCompactSnapshotValue(allocator, snapshot);
    defer deinitOwnedValue(allocator, root);
    return try ztoon.encodeAlloc(allocator, root, options);
}

fn buildFacetCountsValue(
    allocator: std.mem.Allocator,
    table_name: []const u8,
    facet_name: []const u8,
    counts: []const interfaces.FacetCount,
) !Value {
    const fields = try allocator.alloc(Field, 4);
    fields[0] = .{ .key = "kind", .value = .{ .string = "facet_counts" } };
    fields[1] = .{ .key = "table_name", .value = .{ .string = table_name } };
    fields[2] = .{ .key = "facet_name", .value = .{ .string = facet_name } };
    fields[3] = .{ .key = "counts", .value = .{ .array = try buildFacetCountRows(allocator, counts) } };
    return .{ .object = fields };
}

fn buildFacetCountRows(allocator: std.mem.Allocator, counts: []const interfaces.FacetCount) ![]Value {
    const rows = try allocator.alloc(Value, counts.len);
    errdefer allocator.free(rows);

    for (counts, 0..) |count, index| {
        const fields = try allocator.alloc(Field, 4);
        fields[0] = .{ .key = "facet_name", .value = .{ .string = count.facet_name } };
        fields[1] = .{ .key = "facet_value", .value = .{ .string = count.facet_value } };
        fields[2] = .{ .key = "cardinality", .value = intValue(count.cardinality) };
        fields[3] = .{ .key = "facet_id", .value = intValue(count.facet_id) };
        rows[index] = .{ .object = fields };
    }

    return rows;
}

fn buildPackContextValue(
    allocator: std.mem.Allocator,
    user_id: []const u8,
    query: []const u8,
    scope: ?[]const u8,
    rows: anytype,
) !Value {
    const fields = try allocator.alloc(Field, 5);
    fields[0] = .{ .key = "kind", .value = .{ .string = "pack_context" } };
    fields[1] = .{ .key = "user_id", .value = .{ .string = user_id } };
    fields[2] = .{ .key = "query", .value = .{ .string = query } };
    fields[3] = .{ .key = "scope", .value = optionalStringValue(scope) };
    fields[4] = .{ .key = "rows", .value = .{ .array = try buildPackRows(allocator, rows) } };
    return .{ .object = fields };
}

fn buildPackRows(
    allocator: std.mem.Allocator,
    rows: anytype,
) ![]Value {
    const result = try allocator.alloc(Value, rows.len);
    errdefer allocator.free(result);

    for (rows, 0..) |row, index| {
        const fields = try allocator.alloc(Field, 6);
        fields[0] = .{ .key = "id", .value = .{ .string = row.id } };
        fields[1] = .{ .key = "item_id", .value = .{ .string = row.item_id } };
        fields[2] = .{ .key = "projection_type", .value = .{ .string = row.projection_type } };
        fields[3] = .{ .key = "content", .value = .{ .string = row.content } };
        fields[4] = .{ .key = "rank_hint", .value = optionalFloatValue(row.rank_hint) };
        fields[5] = .{ .key = "confidence", .value = .{ .float = row.confidence } };
        result[index] = .{ .object = fields };
    }

    return result;
}

fn buildCoverageReportValue(allocator: std.mem.Allocator, report: anytype) !Value {
    const fields = try allocator.alloc(Field, 3);
    fields[0] = .{ .key = "kind", .value = .{ .string = "coverage_report" } };
    fields[1] = .{ .key = "summary", .value = try buildCoverageSummaryValue(allocator, report.summary) };
    fields[2] = .{ .key = "gaps", .value = .{ .array = try buildCoverageGaps(allocator, report.gaps) } };
    return .{ .object = fields };
}

fn buildCoverageSummaryValue(allocator: std.mem.Allocator, summary: anytype) !Value {
    const fields = try allocator.alloc(Field, 7);
    fields[0] = .{ .key = "workspace_id", .value = .{ .string = summary.workspace_id } };
    fields[1] = .{ .key = "covered_nodes", .value = intValue(summary.covered_nodes) };
    fields[2] = .{ .key = "total_nodes", .value = intValue(summary.total_nodes) };
    fields[3] = .{ .key = "graph_entities", .value = intValue(summary.graph_entities) };
    fields[4] = .{ .key = "facet_rows", .value = intValue(summary.facet_rows) };
    fields[5] = .{ .key = "projection_rows", .value = intValue(summary.projection_rows) };
    fields[6] = .{ .key = "coverage_ratio", .value = optionalFloatValue(summary.coverage_ratio) };
    return .{ .object = fields };
}

fn buildCoverageGaps(allocator: std.mem.Allocator, gaps: anytype) ![]Value {
    const values = try allocator.alloc(Value, gaps.len);
    errdefer allocator.free(values);

    for (gaps, 0..) |gap, index| {
        const fields = try allocator.alloc(Field, 5);
        fields[0] = .{ .key = "id", .value = .{ .string = gap.id } };
        fields[1] = .{ .key = "label", .value = .{ .string = gap.label } };
        fields[2] = .{ .key = "entity_type", .value = .{ .string = gap.entity_type } };
        fields[3] = .{ .key = "criticality", .value = .{ .string = gap.criticality } };
        fields[4] = .{ .key = "decayed_confidence", .value = optionalFloatValue(gap.decayed_confidence) };
        values[index] = .{ .object = fields };
    }

    return values;
}

fn buildSearchCompactSnapshotValue(allocator: std.mem.Allocator, snapshot: anytype) !Value {
    const fields = try allocator.alloc(Field, 7);
    fields[0] = .{ .key = "kind", .value = .{ .string = "search_compact_snapshot" } };
    fields[1] = .{ .key = "collection_stats", .value = intValue(snapshot.collection_stats) };
    fields[2] = .{ .key = "document_stats", .value = intValue(snapshot.document_stats) };
    fields[3] = .{ .key = "term_stats", .value = intValue(snapshot.term_stats) };
    fields[4] = .{ .key = "term_frequencies", .value = intValue(snapshot.term_frequencies) };
    fields[5] = .{ .key = "postings", .value = intValue(snapshot.postings) };
    fields[6] = .{ .key = "embeddings", .value = intValue(snapshot.embeddings) };
    return .{ .object = fields };
}

pub fn buildStringArray(allocator: std.mem.Allocator, values: []const []const u8) ![]Value {
    const result = try allocator.alloc(Value, values.len);
    errdefer allocator.free(result);

    for (values, 0..) |value, index| {
        result[index] = .{ .string = value };
    }

    return result;
}

pub fn intValue(value: anytype) Value {
    return .{ .integer = @intCast(value) };
}

pub fn optionalStringValue(value: ?[]const u8) Value {
    if (value) |slice| return .{ .string = slice };
    return .null;
}

pub fn optionalFloatValue(value: ?f64) Value {
    if (value) |number| return .{ .float = number };
    return .null;
}

pub fn deinitOwnedValue(allocator: std.mem.Allocator, value: Value) void {
    switch (value) {
        .array => |items| {
            for (items) |item| deinitOwnedValue(allocator, item);
            allocator.free(items);
        },
        .object => |fields| {
            for (fields) |field| deinitOwnedValue(allocator, field.value);
            allocator.free(fields);
        },
        else => {},
    }
}

test "coverage report encoding renders summary and gaps" {
    const gaps = [_]struct {
        id: []const u8,
        label: []const u8,
        entity_type: []const u8,
        criticality: []const u8,
        decayed_confidence: ?f64,
    }{
        .{
            .id = "acme",
            .label = "Acme",
            .entity_type = "company",
            .criticality = "normal",
            .decayed_confidence = null,
        },
    };
    const report = .{
        .summary = .{
            .workspace_id = "default",
            .covered_nodes = 1,
            .total_nodes = 2,
            .graph_entities = 1,
            .facet_rows = 2,
            .projection_rows = 3,
            .coverage_ratio = 0.5,
        },
        .gaps = gaps[0..],
    };

    const toon = try encodeCoverageReportAlloc(std.testing.allocator, report, default_options);
    defer std.testing.allocator.free(toon);

    try std.testing.expect(std.mem.indexOf(u8, toon, "kind: coverage_report") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "workspace_id: default") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "coverage_ratio: 0.5") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "id\tlabel\tentity_type\tcriticality\tdecayed_confidence") != null);
}

test "search compact snapshot encoding renders counts" {
    const snapshot = .{
        .collection_stats = 1,
        .document_stats = 2,
        .term_stats = 3,
        .term_frequencies = 4,
        .postings = 5,
        .embeddings = 6,
    };

    const toon = try encodeSearchCompactSnapshotAlloc(std.testing.allocator, snapshot, default_options);
    defer std.testing.allocator.free(toon);

    try std.testing.expect(std.mem.indexOf(u8, toon, "kind: search_compact_snapshot") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "collection_stats: 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "embeddings: 6") != null);
}
