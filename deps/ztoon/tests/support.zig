const std = @import("std");
const fixtures = @import("fixtures");
const ztoon = @import("ztoon");

pub fn encodeJsonFixture(
    allocator: std.mem.Allocator,
    json_bytes: []const u8,
    options: ztoon.EncodeOptions,
) ![]u8 {
    _ = fixtures;
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, allocator, json_bytes, .{});
    const value = try ztoon.Value.fromJsonValue(allocator, parsed);
    return ztoon.encodeAlloc(allocator, value, options);
}

pub fn decodeToonFixture(
    allocator: std.mem.Allocator,
    toon_bytes: []const u8,
    options: ztoon.DecodeOptions,
) ![]u8 {
    return ztoon.decodeToJsonAlloc(allocator, toon_bytes, options);
}

pub fn expectJsonEquivalent(allocator: std.mem.Allocator, expected_json: []const u8, actual_json: []const u8) !void {
    const expected = try std.json.parseFromSliceLeaky(std.json.Value, allocator, expected_json, .{});
    const actual = try std.json.parseFromSliceLeaky(std.json.Value, allocator, actual_json, .{});
    try std.testing.expect(jsonValuesEqual(expected, actual));
}

fn jsonValuesEqual(left: std.json.Value, right: std.json.Value) bool {
    if (@intFromEnum(left) != @intFromEnum(right)) return false;
    return switch (left) {
        .null => true,
        .bool => |value| right.bool == value,
        .integer => |value| right.integer == value,
        .float => |value| std.math.approxEqAbs(f64, right.float, value, 1e-9),
        .number_string => |value| std.mem.eql(u8, value, right.number_string),
        .string => |value| std.mem.eql(u8, value, right.string),
        .array => |items| {
            const other = right.array;
            if (items.items.len != other.items.len) return false;
            for (items.items, other.items) |entry, other_entry| {
                if (!jsonValuesEqual(entry, other_entry)) return false;
            }
            return true;
        },
        .object => |map| {
            const other = right.object;
            if (map.count() != other.count()) return false;
            var iterator = map.iterator();
            while (iterator.next()) |entry| {
                const other_value = other.get(entry.key_ptr.*) orelse return false;
                if (!jsonValuesEqual(entry.value_ptr.*, other_value)) return false;
            }
            return true;
        },
    };
}
