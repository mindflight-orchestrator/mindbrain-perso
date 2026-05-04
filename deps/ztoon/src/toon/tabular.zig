const std = @import("std");
const value_mod = @import("value.zig");

const Field = value_mod.Field;
const Value = value_mod.Value;

pub fn isUniformObjectArray(items: []const Value) bool {
    return headerFields(items) != null;
}

pub fn headerFields(items: []const Value) ?[]const Field {
    if (items.len == 0) return null;

    const first = switch (items[0]) {
        .object => |fields| fields,
        else => return null,
    };

    if (first.len == 0) return null;
    for (first) |field| {
        if (!field.value.isScalar()) return null;
    }

    for (items[1..]) |item| {
        const fields = switch (item) {
            .object => |object_fields| object_fields,
            else => return null,
        };

        if (fields.len != first.len) return null;

        for (fields, first, 0..) |field, reference, index| {
            _ = index;
            if (!field.value.isScalar()) return null;
            if (!std.mem.eql(u8, field.key, reference.key)) return null;
        }
    }

    return first;
}
