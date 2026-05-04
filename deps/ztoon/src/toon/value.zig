const std = @import("std");

pub const Value = union(enum) {
    null,
    bool: bool,
    string: []const u8,
    integer: i64,
    float: f64,
    array: []const Value,
    object: []const Field,

    pub fn isScalar(self: Value) bool {
        return switch (self) {
            .null, .bool, .string, .integer, .float => true,
            .array, .object => false,
        };
    }

    pub fn tagName(self: Value) []const u8 {
        return @tagName(self);
    }

    pub fn fromJsonValue(allocator: std.mem.Allocator, json_value: std.json.Value) !Value {
        return switch (json_value) {
            .null => .null,
            .bool => |value| .{ .bool = value },
            .integer => |value| .{ .integer = value },
            .float => |value| .{ .float = value },
            .number_string => |value| blk: {
                if (std.mem.indexOfScalar(u8, value, '.') != null or
                    std.mem.indexOfScalar(u8, value, 'e') != null or
                    std.mem.indexOfScalar(u8, value, 'E') != null)
                {
                    break :blk .{ .float = try std.fmt.parseFloat(f64, value) };
                }

                break :blk .{ .integer = try std.fmt.parseInt(i64, value, 10) };
            },
            .string => |value| .{ .string = try allocator.dupe(u8, value) },
            .array => |items| blk: {
                const converted = try allocator.alloc(Value, items.items.len);
                for (items.items, 0..) |entry, index| {
                    converted[index] = try fromJsonValue(allocator, entry);
                }
                break :blk .{ .array = converted };
            },
            .object => |object| blk: {
                const keys = object.keys();
                const values = object.values();
                const converted = try allocator.alloc(Field, keys.len);

                for (keys, values, 0..) |key, value, index| {
                    converted[index] = .{
                        .key = try allocator.dupe(u8, key),
                        .value = try fromJsonValue(allocator, value),
                    };
                }

                break :blk .{ .object = converted };
            },
        };
    }
};

pub const Field = struct {
    key: []const u8,
    value: Value,
};
