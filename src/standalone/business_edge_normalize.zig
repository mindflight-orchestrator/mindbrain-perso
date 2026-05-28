const std = @import("std");

const alias_table = std.StaticStringMap([]const u8).initComptime(&.{
    .{ "block_contains_unit", "contains" },
    .{ "building_contains_block", "contains" },
    .{ "building_contains_shared_space", "contains" },
    .{ "building_contains_shared_equipment", "contains" },
    .{ "building_contains_private_garden", "contains" },
    .{ "shared_space_contains_equipment", "contains" },
    .{ "block_contains_parking", "contains" },
});

/// Maps LinkML slot aliases to canonical ontology edge_type names used in gap rules.
pub fn normalizeBusinessEdgeType(edge_type: []const u8) []const u8 {
    return alias_table.get(edge_type) orelse edge_type;
}

test "normalizeBusinessEdgeType maps block_contains_unit" {
    try std.testing.expectEqualStrings("contains", normalizeBusinessEdgeType("block_contains_unit"));
    try std.testing.expectEqualStrings("owns", normalizeBusinessEdgeType("owns"));
}
