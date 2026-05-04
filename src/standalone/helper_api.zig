//! Shared SQLite helpers for the standalone/dashboard code paths.
const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");

const http = std.http;
const c = facet_sqlite.c;

pub const ApiResponse = struct {
    status: http.Status,
    body: []u8,

    pub fn deinit(self: ApiResponse, allocator: std.mem.Allocator) void {
        allocator.free(self.body);
    }
};

pub fn jsonResponse(allocator: std.mem.Allocator, value: anytype) !ApiResponse {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.print("{f}", .{std.json.fmt(value, .{})});
    const body = try out.toOwnedSlice();
    return .{ .status = .ok, .body = body };
}

pub fn isoUtcFromUnix(allocator: std.mem.Allocator, unix_seconds: i64) ![]const u8 {
    if (unix_seconds < 0) return try allocator.dupe(u8, "1970-01-01T00:00:00Z");
    const es = std.time.epoch.EpochSeconds{ .secs = @intCast(unix_seconds) };
    const ed = es.getEpochDay();
    const ys = ed.calculateYearDay();
    const md = ys.calculateMonthDay();
    const ds = es.getDaySeconds();
    return try std.fmt.allocPrint(allocator, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
        ys.year,
        @intFromEnum(md.month),
        md.day_index + 1,
        ds.getHoursIntoDay(),
        ds.getMinutesIntoHour(),
        ds.getSecondsIntoMinute(),
    });
}

pub fn dupeColText(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, col: c_int) ![]const u8 {
    const ptr = c.sqlite3_column_text(stmt, col);
    if (ptr == null) return allocator.dupe(u8, "");
    const len: usize = @intCast(c.sqlite3_column_bytes(stmt, col));
    const slice: []const u8 = ptr[0..len];
    return try allocator.dupe(u8, slice);
}

pub fn colTextOptional(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, col: c_int) !?[]const u8 {
    if (c.sqlite3_column_type(stmt, col) == c.SQLITE_NULL) return null;
    return try dupeColText(allocator, stmt, col);
}

pub fn colI64(stmt: *c.sqlite3_stmt, col: c_int) i64 {
    return c.sqlite3_column_int64(stmt, col);
}

pub fn colIntAsBool(stmt: *c.sqlite3_stmt, col: c_int) bool {
    return c.sqlite3_column_int64(stmt, col) != 0;
}

pub fn prepare(db: facet_sqlite.Database, sql: []const u8) !*c.sqlite3_stmt {
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
        return error.PrepareFailed;
    }
    return stmt.?;
}

pub fn finalize(stmt: *c.sqlite3_stmt) void {
    _ = c.sqlite3_finalize(stmt);
}

pub fn stepRow(stmt: *c.sqlite3_stmt) !bool {
    const rc = c.sqlite3_step(stmt);
    if (rc == c.SQLITE_ROW) return true;
    if (rc == c.SQLITE_DONE) return false;
    return error.StepFailed;
}

pub fn countTable(db: facet_sqlite.Database, table: []const u8) !i64 {
    const sql = try std.fmt.allocPrint(std.heap.page_allocator, "SELECT COUNT(*) FROM {s}", .{table});
    defer std.heap.page_allocator.free(sql);
    const stmt = try prepare(db, sql);
    defer finalize(stmt);
    if (!try stepRow(stmt)) return error.StepFailed;
    return colI64(stmt, 0);
}

pub fn countQueueVisible(db: facet_sqlite.Database) !i64 {
    const stmt = try prepare(db, "SELECT COUNT(*) FROM queue_messages WHERE archived = 0 AND deleted = 0");
    defer finalize(stmt);
    if (!try stepRow(stmt)) return error.StepFailed;
    return colI64(stmt, 0);
}

pub fn severityLabelFromRaw(raw: i64) []const u8 {
    if (raw >= 4) return "CRITICAL";
    if (raw >= 3) return "HIGH";
    if (raw >= 2) return "MEDIUM";
    return "LOW";
}

pub fn parseTtpIds(allocator: std.mem.Allocator, json_text: ?[]const u8) ![][]const u8 {
    const t = json_text orelse return try allocator.alloc([]const u8, 0);
    if (t.len == 0) return try allocator.alloc([]const u8, 0);
    var parsed = std.json.parseFromSlice([][]const u8, allocator, t, .{ .allocate = .alloc_always }) catch {
        return try allocator.alloc([]const u8, 0);
    };
    defer parsed.deinit();
    const out = try allocator.alloc([]const u8, parsed.value.len);
    for (parsed.value, 0..) |s, i| {
        out[i] = try allocator.dupe(u8, s);
    }
    return out;
}

pub fn parseBlastRadius(allocator: std.mem.Allocator, json_text: ?[]const u8) ![][]const u8 {
    return parseTtpIds(allocator, json_text);
}

pub fn freeStringSlice(allocator: std.mem.Allocator, items: [][]const u8) void {
    for (items) |s| allocator.free(s);
    allocator.free(items);
}

pub fn parseLimitOffset(query: []const u8, default_limit: usize) struct { limit: usize, offset: usize } {
    var limit = default_limit;
    var offset: usize = 0;
    var it = std.mem.splitScalar(u8, query, '&');
    while (it.next()) |pair| {
        if (pair.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, pair, '=') orelse pair.len;
        const k = pair[0..eq];
        const v = if (eq < pair.len) pair[eq + 1 ..] else "";
        if (std.mem.eql(u8, k, "limit")) {
            limit = std.fmt.parseInt(usize, v, 10) catch limit;
        } else if (std.mem.eql(u8, k, "offset")) {
            offset = std.fmt.parseInt(usize, v, 10) catch offset;
        }
    }
    if (limit > 500) limit = 500;
    return .{ .limit = limit, .offset = offset };
}

pub fn parseDepthQuery(query: []const u8, default_depth: usize) usize {
    var it = std.mem.splitScalar(u8, query, '&');
    while (it.next()) |pair| {
        if (pair.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, pair, '=') orelse pair.len;
        const k = pair[0..eq];
        const v = if (eq < pair.len) pair[eq + 1 ..] else "";
        if (std.mem.eql(u8, k, "depth")) {
            return std.fmt.parseInt(usize, v, 10) catch default_depth;
        }
    }
    return default_depth;
}

test "helper api parses and clamps limit offset" {
    const got = parseLimitOffset("limit=750&offset=12", 50);
    try std.testing.expectEqual(@as(usize, 500), got.limit);
    try std.testing.expectEqual(@as(usize, 12), got.offset);
}

test "helper api parses depth query and falls back" {
    try std.testing.expectEqual(@as(usize, 7), parseDepthQuery("depth=7", 3));
    try std.testing.expectEqual(@as(usize, 3), parseDepthQuery("depth=not-a-number", 3));
}

test "helper api severity labels map raw values" {
    try std.testing.expectEqualStrings("LOW", severityLabelFromRaw(0));
    try std.testing.expectEqualStrings("MEDIUM", severityLabelFromRaw(2));
    try std.testing.expectEqualStrings("HIGH", severityLabelFromRaw(3));
    try std.testing.expectEqualStrings("CRITICAL", severityLabelFromRaw(4));
}

test "helper api jsonResponse serializes compact json" {
    const payload = .{ .ok = true, .count = @as(u32, 3) };
    var response = try jsonResponse(std.testing.allocator, payload);
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("{\"ok\":true,\"count\":3}", response.body);
}

test "helper api formats unix epoch as utc" {
    const ts = try isoUtcFromUnix(std.testing.allocator, 0);
    defer std.testing.allocator.free(ts);
    try std.testing.expectEqualStrings("1970-01-01T00:00:00Z", ts);
}
