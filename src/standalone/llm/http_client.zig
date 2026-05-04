const std = @import("std");

pub const JsonResponse = struct {
    body: []u8,

    pub fn deinit(self: JsonResponse, allocator: std.mem.Allocator) void {
        allocator.free(self.body);
    }
};

pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

pub fn postJson(
    allocator: std.mem.Allocator,
    io: std.Io,
    url: []const u8,
    api_key: ?[]const u8,
    payload: []const u8,
) !JsonResponse {
    var headers_buf: [4]std.http.Header = undefined;
    var header_count: usize = 0;
    headers_buf[header_count] = .{ .name = "content-type", .value = "application/json" };
    header_count += 1;

    var auth_header: ?[]u8 = null;
    defer if (auth_header) |h| allocator.free(h);
    if (api_key) |key| {
        auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{key});
        headers_buf[header_count] = .{ .name = "authorization", .value = auth_header.? };
        header_count += 1;
    }

    return post(allocator, io, url, payload, headers_buf[0..header_count]);
}

pub fn postWithHeaders(
    allocator: std.mem.Allocator,
    io: std.Io,
    url: []const u8,
    payload: []const u8,
    headers: []const Header,
) !JsonResponse {
    var headers_buf: [16]std.http.Header = undefined;
    if (headers.len > headers_buf.len) return error.TooManyHeaders;
    for (headers, 0..) |header, i| {
        headers_buf[i] = .{ .name = header.name, .value = header.value };
    }
    return post(allocator, io, url, payload, headers_buf[0..headers.len]);
}

fn post(
    allocator: std.mem.Allocator,
    io: std.Io,
    url: []const u8,
    payload: []const u8,
    headers: []const std.http.Header,
) !JsonResponse {
    var response_body: std.Io.Writer.Allocating = .init(allocator);
    defer response_body.deinit();

    var client: std.http.Client = .{
        .allocator = allocator,
        .io = io,
    };
    defer client.deinit();

    const fetch_result = try client.fetch(.{
        .location = .{ .url = url },
        .method = .POST,
        .payload = payload,
        .response_writer = &response_body.writer,
        .extra_headers = headers,
        .keep_alive = false,
    });
    if (fetch_result.status.class() != .success) return error.HttpRequestFailed;

    return .{ .body = try response_body.toOwnedSlice() };
}

pub fn trimRight(text: []const u8, byte: u8) []const u8 {
    var end = text.len;
    while (end > 0 and text[end - 1] == byte) : (end -= 1) {}
    return text[0..end];
}
