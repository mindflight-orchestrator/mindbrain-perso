const std = @import("std");
const fixtures = @import("fixtures");
const support = @import("support.zig");

test "encodes a basic object fixture" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const actual = try support.encodeJsonFixture(arena.allocator(), fixtures.basic_object_json, .{});
    try std.testing.expectEqualStrings(fixtures.basic_object_toon, actual);
}
