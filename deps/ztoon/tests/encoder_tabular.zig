const std = @import("std");
const fixtures = @import("fixtures");
const support = @import("support.zig");

test "encodes a uniform object array as tabular TOON" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const actual = try support.encodeJsonFixture(arena.allocator(), fixtures.tabular_people_json, .{});
    try std.testing.expectEqualStrings(fixtures.tabular_people_toon, actual);
}
