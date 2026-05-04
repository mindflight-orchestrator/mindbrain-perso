// Proposition DSL parser for pg_pragma.
// See docs/DSL_RULES.md for format specification.

const std = @import("std");

/// Parsed proposition record: type + key-value pairs.
pub const PropositionRecord = struct {
    record_type: []const u8,
    fields: std.StringHashMap([]const u8),

    pub fn deinit(self: *PropositionRecord, allocator: std.mem.Allocator) void {
        allocator.free(self.record_type);
        var it = self.fields.iterator();
        while (it.next()) |e| {
            allocator.free(e.key_ptr.*);
            allocator.free(e.value_ptr.*);
        }
        self.fields.deinit();
    }

    /// Get string field or null.
    pub fn get(self: *const PropositionRecord, key: []const u8) ?[]const u8 {
        return self.fields.get(key);
    }

    /// Get confidence (conf or confidence) as f64, default 1.0.
    pub fn getConfidence(self: *const PropositionRecord) f64 {
        if (self.fields.get("conf")) |v| {
            if (std.fmt.parseFloat(f64, v)) |f| {
                return f;
            } else |_| {}
        }
        if (self.fields.get("confidence")) |v| {
            if (std.fmt.parseFloat(f64, v)) |f| {
                return f;
            } else |_| {}
        }
        return 1.0;
    }

    /// Node-like values for graph expansion: subject, object, from, to, id.
    pub fn getNodeIds(self: *const PropositionRecord, allocator: std.mem.Allocator) ![]const []const u8 {
        var list: std.ArrayListUnmanaged([]const u8) = .{};
        const keys = [_][]const u8{ "subject", "object", "from", "to", "id" };
        for (keys) |key| {
            if (self.fields.get(key)) |v| {
                if (v.len > 0) try list.append(allocator, v);
            }
        }
        return list.toOwnedSlice(allocator);
    }

    /// Format as JSON object string (caller frees).
    pub fn toJson(self: *const PropositionRecord, allocator: std.mem.Allocator) ![]const u8 {
        var buf: std.ArrayListUnmanaged(u8) = .{};
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

fn escapeJsonString(allocator: std.mem.Allocator, buf: *std.ArrayListUnmanaged(u8), s: []const u8) !void {
    for (s) |c| {
        switch (c) {
            '"' => try buf.appendSlice(allocator, "\\\""),
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            else => try buf.append(allocator, c),
        }
    }
}

fn putField(allocator: std.mem.Allocator, fields: *std.StringHashMap([]const u8), key: []const u8, val: []const u8) !void {
    const k = allocator.dupe(u8, key) catch return;
    const v = allocator.dupe(u8, val) catch {
        allocator.free(k);
        return;
    };
    fields.put(k, v) catch {
        allocator.free(k);
        allocator.free(v);
    };
}

/// Parse a single DSL line. Returns null if line is empty or malformed.
/// Caller owns the returned record; call deinit().
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
        while (it.next()) |e| {
            allocator.free(e.key_ptr.*);
            allocator.free(e.value_ptr.*);
        }
        fields.deinit();
    }

    var rest = trimmed[first_bar + 1 ..];
    while (std.mem.indexOf(u8, rest, "|")) |bar_idx| {
        const segment = std.mem.trim(u8, rest[0..bar_idx], " \t");
        if (segment.len > 0) {
            if (std.mem.indexOf(u8, segment, "=")) |eq_idx| {
                const key = std.mem.trim(u8, segment[0..eq_idx], " \t");
                const val = std.mem.trim(u8, segment[eq_idx + 1 ..], " \t");
                if (key.len > 0) putField(allocator, &fields, key, val) catch {};
            }
        }
        rest = rest[bar_idx + 1 ..];
    }
    const segment = std.mem.trim(u8, rest, " \t");
    if (segment.len > 0) {
        if (std.mem.indexOf(u8, segment, "=")) |eq_idx| {
            const key = std.mem.trim(u8, segment[0..eq_idx], " \t");
            const val = std.mem.trim(u8, segment[eq_idx + 1 ..], " \t");
            if (key.len > 0) putField(allocator, &fields, key, val) catch {};
        }
    }

    return PropositionRecord{
        .record_type = record_type,
        .fields = fields,
    };
}

/// Parse multiple lines (newline-separated). Returns first valid record or null.
pub fn parseFirstRecord(allocator: std.mem.Allocator, content: []const u8) ?PropositionRecord {
    var iter = std.mem.splitScalar(u8, content, '\n');
    while (iter.next()) |line| {
        if (parseLine(allocator, line)) |rec| {
            return rec;
        }
    }
    return null;
}

/// Type prior weight for ranking (higher = prefer).
pub fn typePrior(record_type: []const u8) f64 {
    if (std.mem.eql(u8, record_type, "fact")) return 1.2;
    if (std.mem.eql(u8, record_type, "goal")) return 1.1;
    if (std.mem.eql(u8, record_type, "constraint")) return 1.0;
    if (std.mem.eql(u8, record_type, "step")) return 0.9;
    if (std.mem.eql(u8, record_type, "edge")) return 0.95;
    return 0.8; // unknown type
}
