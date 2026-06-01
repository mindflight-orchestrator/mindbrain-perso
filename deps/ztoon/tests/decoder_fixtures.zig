const std = @import("std");
const fixtures = @import("fixtures");
const support = @import("support.zig");

test "decodes basic object fixtures to json" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const actual = try support.decodeToonFixture(arena.allocator(), fixtures.basic_object_toon, .{});
    try support.expectJsonEquivalent(arena.allocator(), fixtures.basic_object_json, actual);
}

test "decodes tabular array fixtures to json" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const actual = try support.decodeToonFixture(arena.allocator(), fixtures.tabular_people_toon, .{});
    try support.expectJsonEquivalent(arena.allocator(), fixtures.tabular_people_json, actual);
}

test "decodes empty array fixtures to json" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const actual = try support.decodeToonFixture(arena.allocator(), fixtures.empty_array_toon, .{});
    try support.expectJsonEquivalent(arena.allocator(), fixtures.empty_array_json, actual);
}

test "decodes pipe delimiter fixtures to json" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const actual = try support.decodeToonFixture(arena.allocator(), fixtures.pipe_delimiter_toon, .{});
    try support.expectJsonEquivalent(arena.allocator(), fixtures.pipe_delimiter_json, actual);
}
