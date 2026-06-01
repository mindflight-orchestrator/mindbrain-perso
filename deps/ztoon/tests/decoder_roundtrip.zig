const std = @import("std");
const fixtures = @import("fixtures");
const support = @import("support.zig");

test "roundtrips basic object fixtures" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const toon = try support.encodeJsonFixture(arena.allocator(), fixtures.basic_object_json, .{});
    const roundtrip = try support.decodeToonFixture(arena.allocator(), toon, .{});
    try support.expectJsonEquivalent(arena.allocator(), fixtures.basic_object_json, roundtrip);
}

test "roundtrips tabular array fixtures" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const toon = try support.encodeJsonFixture(arena.allocator(), fixtures.tabular_people_json, .{});
    const roundtrip = try support.decodeToonFixture(arena.allocator(), toon, .{});
    try support.expectJsonEquivalent(arena.allocator(), fixtures.tabular_people_json, roundtrip);
}

test "roundtrips empty array fixtures" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const toon = try support.encodeJsonFixture(arena.allocator(), fixtures.empty_array_json, .{});
    const roundtrip = try support.decodeToonFixture(arena.allocator(), toon, .{});
    try support.expectJsonEquivalent(arena.allocator(), fixtures.empty_array_json, roundtrip);
}

test "roundtrips pipe delimiter fixtures" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const toon = try support.encodeJsonFixture(arena.allocator(), fixtures.pipe_delimiter_json, .{
        .delimiter = .pipe,
    });
    const roundtrip = try support.decodeToonFixture(arena.allocator(), toon, .{});
    try support.expectJsonEquivalent(arena.allocator(), fixtures.pipe_delimiter_json, roundtrip);
}
