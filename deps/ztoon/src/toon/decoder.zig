const std = @import("std");
const value_mod = @import("value.zig");
const writer_mod = @import("writer.zig");

const Field = value_mod.Field;
const Value = value_mod.Value;
const Delimiter = writer_mod.Delimiter;

pub const DecodeOptions = struct {
    indent_width: usize = 2,
};

pub const DecodeError = error{
    UnexpectedIndent,
    InvalidFieldLine,
    InvalidTabularRow,
    ArrayLengthMismatch,
    ExpectedListItem,
    InvalidListItem,
    InvalidArrayHeader,
    InvalidScalar,
    InvalidQuotedString,
    OutOfMemory,
};

const ArrayHeader = struct {
    key: []const u8,
    count: usize,
    delimiter: Delimiter,
    columns: ?[]const []const u8,
};

const LineView = struct {
    indent: usize,
    content: []const u8,
};

pub fn decodeAlloc(allocator: std.mem.Allocator, text: []const u8, options: DecodeOptions) DecodeError!Value {
    var lines = try splitLines(allocator, text);
    defer {
        for (lines.items) |line| allocator.free(line);
        lines.deinit(allocator);
    }

    if (lines.items.len == 0) {
        return .{ .object = try allocator.alloc(Field, 0) };
    }

    const fields = try parseObjectFields(allocator, lines.items, 0, lines.items.len, 0, options);
    return .{ .object = fields };
}

pub fn decodeToJsonAlloc(allocator: std.mem.Allocator, text: []const u8, options: DecodeOptions) DecodeError![]u8 {
    const value = try decodeAlloc(allocator, text, options);
    defer value_mod.deinit(allocator, value);
    const json_value = try value_mod.toJsonValue(allocator, value);
    defer value_mod.deinitJsonValue(allocator, json_value);
    return std.json.Stringify.valueAlloc(allocator, json_value, .{});
}

fn splitLines(allocator: std.mem.Allocator, text: []const u8) DecodeError!std.ArrayList([]const u8) {
    var out = std.ArrayList([]const u8).empty;
    var iter = std.mem.splitScalar(u8, text, '\n');
    while (iter.next()) |raw| {
        var line = raw;
        if (line.len > 0 and line[line.len - 1] == '\r') line = line[0 .. line.len - 1];
        if (line.len == 0 and out.items.len > 0 and out.items[out.items.len - 1].len == 0) continue;
        try out.append(allocator, try allocator.dupe(u8, line));
    }
    while (out.items.len > 0 and out.items[out.items.len - 1].len == 0) {
        const last = out.pop() orelse break;
        allocator.free(last);
    }
    return out;
}

fn lineView(line: []const u8, indent_width: usize) LineView {
    var indent: usize = 0;
    while (indent < line.len and line[indent] == ' ') indent += 1;
    return .{
        .indent = indent / indent_width,
        .content = line[indent..],
    };
}

fn parseObjectFields(
    allocator: std.mem.Allocator,
    lines: []const []const u8,
    start: usize,
    end: usize,
    depth: usize,
    options: DecodeOptions,
) DecodeError![]Field {
    var fields = std.ArrayList(Field).empty;
    errdefer {
        for (fields.items) |field| {
            allocator.free(field.key);
            value_mod.deinit(allocator, field.value);
        }
        fields.deinit(allocator);
    }

    var index = start;
    while (index < end) {
        const view = lineView(lines[index], options.indent_width);
        if (view.content.len == 0) {
            index += 1;
            continue;
        }
        if (view.indent < depth) break;
        if (view.indent > depth) return error.UnexpectedIndent;

        if (try parseArrayHeader(allocator, view.content)) |header| {
            defer if (header.columns) |cols| {
                for (cols) |column| allocator.free(column);
                allocator.free(cols);
            };
            const body_end = findBlockEnd(lines, index + 1, end, depth + 1, options.indent_width);
            const array_value = try parseArrayBody(allocator, lines, index + 1, body_end, depth + 1, header, options);
            try fields.append(allocator, .{ .key = header.key, .value = array_value });
            index = body_end;
            continue;
        }

        const colon = findFieldColon(view.content) orelse return error.InvalidFieldLine;
        const key = try parseToken(allocator, std.mem.trim(u8, view.content[0..colon], " "));
        const value_text = std.mem.trim(u8, view.content[colon + 1 ..], " ");

        if (value_text.len == 0) {
            try fields.append(allocator, .{ .key = key, .value = .{ .object = try allocator.alloc(Field, 0) } });
            index += 1;
            continue;
        }

        try fields.append(allocator, .{ .key = key, .value = try parseScalar(allocator, value_text) });
        index += 1;
    }

    return try fields.toOwnedSlice(allocator);
}

fn findBlockEnd(lines: []const []const u8, start: usize, end: usize, depth: usize, indent_width: usize) usize {
    var index = start;
    while (index < end) {
        const view = lineView(lines[index], indent_width);
        if (view.content.len == 0) {
            index += 1;
            continue;
        }
        if (view.indent < depth) break;
        index += 1;
    }
    return index;
}

fn parseArrayBody(
    allocator: std.mem.Allocator,
    lines: []const []const u8,
    start: usize,
    end: usize,
    depth: usize,
    header: ArrayHeader,
    options: DecodeOptions,
) DecodeError!Value {
    if (header.columns != null) {
        return try parseTabularArray(allocator, lines, start, end, depth, header, options);
    }
    return try parseListArray(allocator, lines, start, end, depth, header, options);
}

fn parseTabularArray(
    allocator: std.mem.Allocator,
    lines: []const []const u8,
    start: usize,
    end: usize,
    depth: usize,
    header: ArrayHeader,
    options: DecodeOptions,
) DecodeError!Value {
    const columns = header.columns.?;
    var items = std.ArrayList(Value).empty;
    errdefer {
        for (items.items) |item| value_mod.deinit(allocator, item);
        items.deinit(allocator);
    }

    var index = start;
    while (index < end) {
        const view = lineView(lines[index], options.indent_width);
        if (view.content.len == 0 or view.indent < depth) break;
        if (view.indent != depth) return error.UnexpectedIndent;

        var cells = try splitDelimited(allocator, view.content, header.delimiter);
        defer {
            for (cells.items) |cell| allocator.free(cell);
            cells.deinit(allocator);
        }
        if (cells.items.len != columns.len) return error.InvalidTabularRow;

        var object_fields = try allocator.alloc(Field, columns.len);
        var filled: usize = 0;
        errdefer {
            for (object_fields[0..filled]) |field| {
                allocator.free(field.key);
                value_mod.deinit(allocator, field.value);
            }
            allocator.free(object_fields);
        }

        for (columns, cells.items, 0..) |column, cell, idx| {
            object_fields[idx] = .{
                .key = try allocator.dupe(u8, column),
                .value = try parseScalar(allocator, cell),
            };
            filled += 1;
        }

        try items.append(allocator, .{ .object = object_fields });
        index += 1;
    }

    if (items.items.len != header.count) return error.ArrayLengthMismatch;
    return .{ .array = try items.toOwnedSlice(allocator) };
}

fn parseListArray(
    allocator: std.mem.Allocator,
    lines: []const []const u8,
    start: usize,
    end: usize,
    depth: usize,
    header: ArrayHeader,
    options: DecodeOptions,
) DecodeError!Value {
    var items = std.ArrayList(Value).empty;
    errdefer {
        for (items.items) |item| value_mod.deinit(allocator, item);
        items.deinit(allocator);
    }

    var index = start;
    while (index < end) {
        const view = lineView(lines[index], options.indent_width);
        if (view.content.len == 0 or view.indent < depth) break;
        if (view.indent != depth) return error.UnexpectedIndent;
        if (!std.mem.startsWith(u8, view.content, "-")) return error.ExpectedListItem;

        if (std.mem.eql(u8, view.content, "-")) {
            const object_end = findBlockEnd(lines, index + 1, end, depth + 1, options.indent_width);
            const object_fields = try parseObjectFields(allocator, lines, index + 1, object_end, depth + 1, options);
            try items.append(allocator, .{ .object = object_fields });
            index = object_end;
            continue;
        }

        if (view.content.len >= 2 and view.content[1] == ' ') {
            try items.append(allocator, try parseScalar(allocator, view.content[2..]));
            index += 1;
            continue;
        }

        return error.InvalidListItem;
    }

    if (items.items.len != header.count) return error.ArrayLengthMismatch;
    return .{ .array = try items.toOwnedSlice(allocator) };
}

fn parseArrayHeader(allocator: std.mem.Allocator, line: []const u8) DecodeError!?ArrayHeader {
    if (!std.mem.endsWith(u8, line, ":")) return null;
    const body = std.mem.trim(u8, line[0 .. line.len - 1], " ");
    const open = std.mem.indexOfScalar(u8, body, '[') orelse return null;
    const close = std.mem.indexOfScalar(u8, body[open..], ']') orelse return null;
    const close_index = open + close;

    const key = try parseToken(allocator, std.mem.trim(u8, body[0..open], " "));
    const bracket = body[open + 1 .. close_index];
    const delimiter = detectDelimiterFromCount(bracket);
    const count_text = trimDelimiterSuffix(bracket, delimiter);
    const count = std.fmt.parseInt(usize, count_text, 10) catch return error.InvalidArrayHeader;

    var columns: ?[]const []const u8 = null;
    if (close_index + 1 < body.len and body[close_index + 1] == '{') {
        const cols_close = std.mem.indexOfScalar(u8, body[close_index + 1 ..], '}') orelse return error.InvalidArrayHeader;
        const cols_text = body[close_index + 2 .. close_index + 1 + cols_close];
        columns = try splitDelimitedIntoSlice(allocator, cols_text, delimiter);
    }

    return .{
        .key = key,
        .count = count,
        .delimiter = delimiter,
        .columns = columns,
    };
}

fn detectDelimiterFromCount(text: []const u8) Delimiter {
    if (text.len == 0) return .comma;
    if (text[text.len - 1] == '|') return .pipe;
    if (text[text.len - 1] == '\t') return .tab;
    return .comma;
}

fn trimDelimiterSuffix(text: []const u8, delimiter: Delimiter) []const u8 {
    return switch (delimiter) {
        .comma => text,
        .pipe => if (text.len > 0 and text[text.len - 1] == '|') text[0 .. text.len - 1] else text,
        .tab => if (text.len > 0 and text[text.len - 1] == '\t') text[0 .. text.len - 1] else text,
    };
}

fn splitDelimited(allocator: std.mem.Allocator, text: []const u8, delimiter: Delimiter) DecodeError!std.ArrayList([]const u8) {
    var out = std.ArrayList([]const u8).empty;
    const sep = delimiter.separator();
    var start: usize = 0;
    var index: usize = 0;
    var in_quotes = false;

    while (index <= text.len) {
        if (index < text.len and text[index] == '"' and !in_quotes) {
            in_quotes = true;
            index += 1;
            continue;
        }
        if (in_quotes and index < text.len and text[index] == '"') {
            if (index + 1 < text.len and text[index + 1] == '"') {
                index += 2;
                continue;
            }
            in_quotes = false;
            index += 1;
            continue;
        }
        if (!in_quotes and ((index == text.len) or std.mem.startsWith(u8, text[index..], sep))) {
            try out.append(allocator, try parseToken(allocator, std.mem.trim(u8, text[start..index], " ")));
            if (index == text.len) break;
            index += sep.len;
            start = index;
            continue;
        }
        index += 1;
    }
    return out;
}

fn splitDelimitedIntoSlice(allocator: std.mem.Allocator, text: []const u8, delimiter: Delimiter) DecodeError![]const []const u8 {
    var cells = try splitDelimited(allocator, text, delimiter);
    defer cells.deinit(allocator);
    return try cells.toOwnedSlice(allocator);
}

fn findFieldColon(text: []const u8) ?usize {
    var index: usize = 0;
    while (index < text.len) {
        if (text[index] == '"') {
            var quote_index: usize = index + 1;
            while (quote_index < text.len) {
                if (text[quote_index] == '\\') {
                    quote_index += 2;
                    continue;
                }
                if (text[quote_index] == '"') {
                    quote_index += 1;
                    break;
                }
                quote_index += 1;
            }
            index = quote_index;
            continue;
        }
        if (text[index] == ':') return index;
        index += 1;
    }
    return null;
}

const ParsedQuoted = struct {
    value: []const u8,
    consumed: usize,
};

fn parseQuotedToken(allocator: std.mem.Allocator, text: []const u8) DecodeError!ParsedQuoted {
    if (text.len == 0 or text[0] != '"') return error.InvalidQuotedString;
    var index: usize = 1;
    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(allocator);

    while (index < text.len) {
        const byte = text[index];
        if (byte == '"') {
            return .{
                .value = try buf.toOwnedSlice(allocator),
                .consumed = index + 1,
            };
        }
        if (byte == '\\') {
            if (index + 1 >= text.len) return error.InvalidQuotedString;
            const escape = text[index + 1];
            const decoded: u8 = switch (escape) {
                '\\' => '\\',
                '"' => '"',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                else => return error.InvalidQuotedString,
            };
            try buf.append(allocator, decoded);
            index += 2;
            continue;
        }
        try buf.append(allocator, byte);
        index += 1;
    }
    return error.InvalidQuotedString;
}

fn parseToken(allocator: std.mem.Allocator, text: []const u8) DecodeError![]const u8 {
    if (text.len == 0) return try allocator.dupe(u8, "");
    if (text[0] == '"') {
        const parsed = try parseQuotedToken(allocator, text);
        defer allocator.free(parsed.value);
        return try allocator.dupe(u8, parsed.value);
    }
    return try allocator.dupe(u8, text);
}

fn parseScalar(allocator: std.mem.Allocator, text: []const u8) DecodeError!Value {
    const token = try parseToken(allocator, text);
    defer allocator.free(token);

    if (std.mem.eql(u8, token, "null")) return .null;
    if (std.mem.eql(u8, token, "true")) return .{ .bool = true };
    if (std.mem.eql(u8, token, "false")) return .{ .bool = false };

    if (std.fmt.parseInt(i64, token, 10)) |integer| {
        return .{ .integer = integer };
    } else |_| {}

    if (std.fmt.parseFloat(f64, token)) |float_value| {
        return .{ .float = float_value };
    } else |_| {}

    return .{ .string = try allocator.dupe(u8, token) };
}
