const std = @import("std");
const tabular = @import("tabular.zig");
const value_mod = @import("value.zig");
const writer_mod = @import("writer.zig");

const Field = value_mod.Field;
const Value = value_mod.Value;
const Delimiter = writer_mod.Delimiter;
const TextWriter = writer_mod.TextWriter;

pub const EncodeOptions = struct {
    delimiter: Delimiter = .comma,
    indent_width: usize = 2,
    key_folding: bool = false,
};

pub const Encoder = struct {
    writer: TextWriter,
    options: EncodeOptions,

    pub fn init(allocator: std.mem.Allocator, options: EncodeOptions) Encoder {
        return .{
            .writer = TextWriter.init(allocator, options.indent_width),
            .options = options,
        };
    }

    pub fn deinit(self: *Encoder) void {
        self.writer.deinit();
    }

    pub fn finish(self: *Encoder) ![]u8 {
        return self.writer.finish();
    }

    pub fn encode(self: *Encoder, value: Value) !void {
        _ = self.options.key_folding;
        try self.writeNode(null, value);
    }

    fn writeNode(self: *Encoder, key: ?[]const u8, value: Value) anyerror!void {
        switch (value) {
            .object => |fields| try self.writeObject(key, fields),
            .array => |items| try self.writeArray(key, items),
            else => try self.writeScalarField(key, value),
        }
    }

    fn writeObject(self: *Encoder, key: ?[]const u8, fields: []const Field) anyerror!void {
        if (key) |field_key| {
            try self.writeKey(field_key);
            try self.writer.writeByte(':');
            if (fields.len == 0) return;
            try self.writer.newline();
            self.writer.pushIndent();
            defer self.writer.popIndent();
        }

        for (fields, 0..) |field, index| {
            if (index != 0) try self.writer.newline();
            try self.writeNode(field.key, field.value);
        }
    }

    fn writeArray(self: *Encoder, key: ?[]const u8, items: []const Value) anyerror!void {
        if (tabular.headerFields(items)) |fields| {
            try self.writeArrayHeader(key, items.len, fields);
            if (items.len == 0) return;

            try self.writer.newline();
            self.writer.pushIndent();
            defer self.writer.popIndent();

            for (items, 0..) |item, index| {
                if (index != 0) try self.writer.newline();
                try self.writeTabularRow(item.object);
            }
            return;
        }

        try self.writeArrayHeader(key, items.len, null);
        if (items.len == 0) return;

        try self.writer.newline();
        self.writer.pushIndent();
        defer self.writer.popIndent();

        for (items, 0..) |item, index| {
            if (index != 0) try self.writer.newline();
            try self.writeListItem(item);
        }
    }

    fn writeArrayHeader(self: *Encoder, key: ?[]const u8, len: usize, fields: ?[]const Field) anyerror!void {
        if (key) |field_key| {
            try self.writeKey(field_key);
        }

        try self.writer.writeByte('[');
        try self.writeUnsigned(len);
        try self.writer.writeAll(self.options.delimiter.headerSuffix());
        try self.writer.writeByte(']');

        if (fields) |header_fields| {
            try self.writer.writeByte('{');
            for (header_fields, 0..) |field, index| {
                if (index != 0) try self.writer.writeAll(self.options.delimiter.separator());
                try self.writeToken(field.key, true);
            }
            try self.writer.writeByte('}');
        }

        try self.writer.writeByte(':');
    }

    fn writeListItem(self: *Encoder, value: Value) anyerror!void {
        if (value.isScalar()) {
            try self.writer.writeAll("- ");
            try self.writeScalarInline(value, false);
            return;
        }

        try self.writer.writeByte('-');
        try self.writer.newline();
        self.writer.pushIndent();
        defer self.writer.popIndent();
        try self.writeNode(null, value);
    }

    fn writeTabularRow(self: *Encoder, fields: []const Field) anyerror!void {
        for (fields, 0..) |field, index| {
            if (index != 0) try self.writer.writeAll(self.options.delimiter.separator());
            try self.writeScalarInline(field.value, true);
        }
    }

    fn writeScalarField(self: *Encoder, key: ?[]const u8, value: Value) anyerror!void {
        if (key) |field_key| {
            try self.writeKey(field_key);
            try self.writer.writeAll(": ");
        }
        try self.writeScalarInline(value, false);
    }

    fn writeScalarInline(self: *Encoder, value: Value, delimiter_sensitive: bool) anyerror!void {
        switch (value) {
            .null => try self.writer.writeAll("null"),
            .bool => |boolean| try self.writer.writeAll(if (boolean) "true" else "false"),
            .integer => |integer| {
                var buffer: [64]u8 = undefined;
                const rendered = try std.fmt.bufPrint(&buffer, "{d}", .{integer});
                try self.writer.writeAll(rendered);
            },
            .float => |float_value| {
                if (!std.math.isFinite(float_value) or (float_value == 0 and std.math.signbit(float_value))) {
                    try self.writer.writeAll("null");
                    return;
                }

                var buffer: [128]u8 = undefined;
                const rendered = try std.fmt.bufPrint(&buffer, "{d}", .{float_value});
                try self.writer.writeAll(rendered);
            },
            .string => |string| try self.writeString(string, delimiter_sensitive),
            .array, .object => return error.ExpectedScalarValue,
        }
    }

    fn writeUnsigned(self: *Encoder, value: usize) anyerror!void {
        var buffer: [32]u8 = undefined;
        const rendered = try std.fmt.bufPrint(&buffer, "{d}", .{value});
        try self.writer.writeAll(rendered);
    }

    fn writeKey(self: *Encoder, key: []const u8) anyerror!void {
        try self.writeToken(key, true);
    }

    fn writeString(self: *Encoder, value: []const u8, delimiter_sensitive: bool) anyerror!void {
        if (!needsQuoting(value, self.options.delimiter, delimiter_sensitive, false)) {
            try self.writer.writeAll(value);
            return;
        }

        try self.writer.writeByte('"');
        for (value) |byte| {
            switch (byte) {
                '\\' => try self.writer.writeAll("\\\\"),
                '"' => try self.writer.writeAll("\\\""),
                '\n' => try self.writer.writeAll("\\n"),
                '\r' => try self.writer.writeAll("\\r"),
                '\t' => try self.writer.writeAll("\\t"),
                else => try self.writer.writeByte(byte),
            }
        }
        try self.writer.writeByte('"');
    }

    fn writeToken(self: *Encoder, value: []const u8, is_key: bool) anyerror!void {
        if (!needsQuoting(value, self.options.delimiter, true, is_key)) {
            try self.writer.writeAll(value);
            return;
        }

        try self.writeString(value, true);
    }
};

pub fn encodeAlloc(allocator: std.mem.Allocator, value: Value, options: EncodeOptions) ![]u8 {
    var encoder = Encoder.init(allocator, options);
    defer encoder.deinit();
    try encoder.encode(value);
    return encoder.finish();
}

fn needsQuoting(value: []const u8, delimiter: Delimiter, delimiter_sensitive: bool, is_key: bool) bool {
    if (value.len == 0) return true;
    if (std.ascii.isWhitespace(value[0]) or std.ascii.isWhitespace(value[value.len - 1])) return true;
    if (is_key and std.mem.eql(u8, value, "-")) return true;

    for (value) |byte| {
        if (byte < 0x20) return true;
        if (byte == '"' or byte == '\\' or byte == ':' or byte == '[' or byte == ']' or byte == '{' or byte == '}') return true;
        if (delimiter_sensitive and std.mem.indexOfScalar(u8, delimiter.separator(), byte) != null) return true;
        if (std.ascii.isWhitespace(byte) and byte != '\t') return true;
    }

    return false;
}
