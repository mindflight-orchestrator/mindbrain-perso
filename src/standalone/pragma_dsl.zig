const std = @import("std");

pub const PropositionRecord = struct {
    record_type: []const u8,
    fields: std.StringHashMap([]const u8),

    pub fn deinit(self: *PropositionRecord, allocator: std.mem.Allocator) void {
        allocator.free(self.record_type);
        var it = self.fields.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            allocator.free(entry.value_ptr.*);
        }
        self.fields.deinit();
    }

    pub fn get(self: *const PropositionRecord, key: []const u8) ?[]const u8 {
        return self.fields.get(key);
    }

    pub fn getConfidence(self: *const PropositionRecord) f64 {
        if (self.fields.get("conf")) |value| {
            if (std.fmt.parseFloat(f64, value)) |parsed| return parsed else |_| {}
        }
        if (self.fields.get("confidence")) |value| {
            if (std.fmt.parseFloat(f64, value)) |parsed| return parsed else |_| {}
        }
        return 1.0;
    }

    pub fn getNodeIds(self: *const PropositionRecord, allocator: std.mem.Allocator) ![]const []const u8 {
        var list = std.ArrayList([]const u8).empty;
        const keys = [_][]const u8{ "subject", "object", "from", "to", "id" };
        for (keys) |key| {
            if (self.fields.get(key)) |value| {
                if (value.len > 0) try list.append(allocator, value);
            }
        }
        return list.toOwnedSlice(allocator);
    }

    pub fn toJson(self: *const PropositionRecord, allocator: std.mem.Allocator) ![]const u8 {
        var buf = std.ArrayList(u8).empty;
        try buf.appendSlice(allocator, "{\"type\":\"");
        try escapeJsonString(allocator, &buf, self.record_type);
        try buf.appendSlice(allocator, "\"");

        var it = self.fields.iterator();
        while (it.next()) |entry| {
            try buf.appendSlice(allocator, ",\"");
            try escapeJsonString(allocator, &buf, entry.key_ptr.*);
            try buf.appendSlice(allocator, "\":\"");
            try escapeJsonString(allocator, &buf, entry.value_ptr.*);
            try buf.appendSlice(allocator, "\"");
        }

        try buf.appendSlice(allocator, "}");
        return buf.toOwnedSlice(allocator);
    }
};

pub fn parseLine(allocator: std.mem.Allocator, line: []const u8) ?PropositionRecord {
    const trimmed = std.mem.trim(u8, line, " \t\r\n");
    if (trimmed.len == 0) return null;

    const first_bar = std.mem.indexOf(u8, trimmed, "|") orelse return null;
    const record_type_trimmed = std.mem.trim(u8, trimmed[0..first_bar], " \t");
    if (record_type_trimmed.len == 0) return null;

    const record_type = allocator.dupe(u8, record_type_trimmed) catch return null;
    errdefer allocator.free(record_type);

    var fields = std.StringHashMap([]const u8).init(allocator);
    errdefer {
        var it = fields.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            allocator.free(entry.value_ptr.*);
        }
        fields.deinit();
    }

    var rest = trimmed[first_bar + 1 ..];
    while (std.mem.indexOf(u8, rest, "|")) |bar_idx| {
        parseSegment(allocator, &fields, std.mem.trim(u8, rest[0..bar_idx], " \t"));
        rest = rest[bar_idx + 1 ..];
    }
    parseSegment(allocator, &fields, std.mem.trim(u8, rest, " \t"));

    return .{
        .record_type = record_type,
        .fields = fields,
    };
}

pub fn parseFirstRecord(allocator: std.mem.Allocator, content: []const u8) ?PropositionRecord {
    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line| {
        if (parseLine(allocator, line)) |record| return record;
    }
    return null;
}

pub fn typePrior(record_type: []const u8) f64 {
    if (std.mem.eql(u8, record_type, "fact")) return 1.2;
    if (std.mem.eql(u8, record_type, "goal")) return 1.1;
    if (std.mem.eql(u8, record_type, "constraint")) return 1.0;
    if (std.mem.eql(u8, record_type, "step")) return 0.9;
    if (std.mem.eql(u8, record_type, "edge")) return 0.95;
    return 0.8;
}

fn parseSegment(allocator: std.mem.Allocator, fields: *std.StringHashMap([]const u8), segment: []const u8) void {
    if (segment.len == 0) return;
    const eq_idx = std.mem.indexOf(u8, segment, "=") orelse return;
    const key = std.mem.trim(u8, segment[0..eq_idx], " \t");
    const value = std.mem.trim(u8, segment[eq_idx + 1 ..], " \t");
    if (key.len == 0) return;
    putField(allocator, fields, key, value) catch {};
}

fn putField(allocator: std.mem.Allocator, fields: *std.StringHashMap([]const u8), key: []const u8, value: []const u8) !void {
    const duped_key = try allocator.dupe(u8, key);
    errdefer allocator.free(duped_key);
    const duped_value = try allocator.dupe(u8, value);
    errdefer allocator.free(duped_value);
    try fields.put(duped_key, duped_value);
}

fn escapeJsonString(allocator: std.mem.Allocator, buf: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    for (value) |char| {
        switch (char) {
            '"' => try buf.appendSlice(allocator, "\\\""),
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            else => try buf.append(allocator, char),
        }
    }
}

test "pragma dsl parser parses proposition lines and exports json" {
    var record = parseLine(std.testing.allocator, "fact|subject=ada|object=acme|conf=0.8").?;
    defer record.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("fact", record.record_type);
    try std.testing.expectEqualStrings("ada", record.get("subject").?);
    try std.testing.expectEqual(@as(f64, 0.8), record.getConfidence());

    const json = try record.toJson(std.testing.allocator);
    defer std.testing.allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"type\":\"fact\"") != null);
}

test "pragma dsl parser extracts node ids and first record" {
    var record = parseFirstRecord(std.testing.allocator, "\n\nedge|from=n1|to=n2|id=e1").?;
    defer record.deinit(std.testing.allocator);

    const nodes = try record.getNodeIds(std.testing.allocator);
    defer std.testing.allocator.free(nodes);

    try std.testing.expectEqual(@as(usize, 3), nodes.len);
    try std.testing.expectEqualStrings("n1", nodes[0]);
    try std.testing.expectEqualStrings("n2", nodes[1]);
    try std.testing.expectEqualStrings("e1", nodes[2]);
}

test "pragma dsl parser falls back on malformed confidence" {
    var record = parseLine(std.testing.allocator, "fact|subject=ada|conf=not-a-number").?;
    defer record.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(f64, 1.0), record.getConfidence());
}
