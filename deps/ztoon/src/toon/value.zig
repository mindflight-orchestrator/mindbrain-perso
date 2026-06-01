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

pub fn deinit(allocator: std.mem.Allocator, value: Value) void {
    switch (value) {
        .string => |text| allocator.free(text),
        .array => |items| {
            for (items) |item| deinit(allocator, item);
            allocator.free(items);
        },
        .object => |fields| {
            for (fields) |field| {
                allocator.free(field.key);
                deinit(allocator, field.value);
            }
            allocator.free(fields);
        },
        else => {},
    }
}

pub fn toJsonValue(allocator: std.mem.Allocator, value: Value) !std.json.Value {
    return switch (value) {
        .null => .null,
        .bool => |v| .{ .bool = v },
        .integer => |v| .{ .integer = v },
        .float => |v| .{ .float = v },
        .string => |v| .{ .string = try allocator.dupe(u8, v) },
        .array => |items| blk: {
            var out = std.json.Array.init(allocator);
            errdefer out.deinit();
            for (items) |item| try out.append(try toJsonValue(allocator, item));
            break :blk .{ .array = out };
        },
        .object => |fields| blk: {
            var out = std.json.ObjectMap.empty;
            errdefer out.deinit(allocator);
            for (fields) |field| {
                try out.put(allocator, try allocator.dupe(u8, field.key), try toJsonValue(allocator, field.value));
            }
            break :blk .{ .object = out };
        },
    };
}

pub fn deinitJsonValue(allocator: std.mem.Allocator, value: std.json.Value) void {
    switch (value) {
        .string => |text| allocator.free(text),
        .array => |items| {
            for (items.items) |item| deinitJsonValue(allocator, item);
            items.deinit();
        },
        .object => |map| {
            var copy = map;
            var it = copy.iterator();
            while (it.next()) |entry| {
                allocator.free(entry.key_ptr.*);
                deinitJsonValue(allocator, entry.value_ptr.*);
            }
            copy.deinit(allocator);
        },
        else => {},
    }
}
