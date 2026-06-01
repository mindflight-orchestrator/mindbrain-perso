const std = @import("std");
const fixtures = @import("fixtures");
const support = @import("support.zig");
const ztoon = @import("ztoon");

test "encodes empty arrays using a counted header" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const actual = try support.encodeJsonFixture(arena.allocator(), fixtures.empty_array_json, .{});
    try std.testing.expectEqualStrings(fixtures.empty_array_toon, actual);
}

test "encodes tabular arrays with a pipe delimiter" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const actual = try support.encodeJsonFixture(arena.allocator(), fixtures.pipe_delimiter_json, .{
        .delimiter = .pipe,
    });
    try std.testing.expectEqualStrings(fixtures.pipe_delimiter_toon, actual);
}

test "rejects unsupported floating point values" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const value = ztoon.Value{
        .object = &.{
            .{ .key = "negative_zero", .value = .{ .float = -0.0 } },
            .{ .key = "infinity", .value = .{ .float = std.math.inf(f64) } },
        },
    };

    try std.testing.expectError(
        error.UnsupportedFloatValue,
        ztoon.encodeAlloc(arena.allocator(), value, .{}),
    );
}

test "rejects unsupported key folding option" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const value = ztoon.Value{
        .object = &.{
            .{ .key = "name", .value = .{ .string = "Ada" } },
        },
    };

    try std.testing.expectError(
        error.UnsupportedKeyFolding,
        ztoon.encodeAlloc(arena.allocator(), value, .{ .key_folding = true }),
    );
}
