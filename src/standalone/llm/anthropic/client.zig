const std = @import("std");
const types = @import("../types.zig");
const http_client = @import("../http_client.zig");

pub const Config = struct {
    base_url: []const u8 = "https://api.anthropic.com/v1",
    api_key: ?[]const u8 = null,
    model: []const u8,
    version: []const u8 = "2023-06-01",
};

pub fn chat(
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,
    request: types.ChatRequest,
) !types.ChatResponse {
    const url = try messagesUrl(allocator, config.base_url);
    defer allocator.free(url);

    const body = try renderMessagesRequest(allocator, config.model, request);
    defer allocator.free(body);

    var headers_buf: [4]http_client.Header = undefined;
    var header_count: usize = 0;
    headers_buf[header_count] = .{ .name = "content-type", .value = "application/json" };
    header_count += 1;
    headers_buf[header_count] = .{ .name = "anthropic-version", .value = config.version };
    header_count += 1;
    if (config.api_key) |key| {
        headers_buf[header_count] = .{ .name = "x-api-key", .value = key };
        header_count += 1;
    }

    const response = try http_client.postWithHeaders(allocator, io, url, body, headers_buf[0..header_count]);
    errdefer response.deinit(allocator);
    return parseMessagesResponse(allocator, response.body);
}

fn messagesUrl(allocator: std.mem.Allocator, base_url: []const u8) ![]u8 {
    const trimmed = http_client.trimRight(base_url, '/');
    if (std.mem.endsWith(u8, trimmed, "/messages")) return try allocator.dupe(u8, trimmed);
    if (std.mem.endsWith(u8, trimmed, "/v1")) return try std.fmt.allocPrint(allocator, "{s}/messages", .{trimmed});
    return try std.fmt.allocPrint(allocator, "{s}/v1/messages", .{trimmed});
}

pub fn renderMessagesRequest(
    allocator: std.mem.Allocator,
    model: []const u8,
    request: types.ChatRequest,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    try out.writer.writeAll("{\"model\":");
    try out.writer.print("{f}", .{std.json.fmt(model, .{})});
    try out.writer.print(",\"max_tokens\":{}", .{request.max_tokens orelse 1024});

    if (request.temperature) |temperature| {
        try out.writer.print(",\"temperature\":{d}", .{temperature});
    }

    const system_text = try collectSystemMessages(allocator, request.messages);
    defer allocator.free(system_text);
    if (system_text.len > 0) {
        try out.writer.writeAll(",\"system\":");
        try out.writer.print("{f}", .{std.json.fmt(system_text, .{})});
    }

    try out.writer.writeAll(",\"messages\":[");
    var wrote = false;
    for (request.messages) |message| {
        if (std.mem.eql(u8, message.role, "system")) continue;
        if (wrote) try out.writer.writeByte(',');
        try renderMessage(&out.writer, message);
        wrote = true;
    }
    if (!wrote) {
        try out.writer.writeAll("{\"role\":\"user\",\"content\":\"\"}");
    }
    try out.writer.writeByte(']');
    try out.writer.writeByte('}');
    return try out.toOwnedSlice();
}

fn collectSystemMessages(allocator: std.mem.Allocator, messages: []const types.Message) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    var wrote = false;
    for (messages) |message| {
        if (!std.mem.eql(u8, message.role, "system")) continue;
        if (wrote) try out.writer.writeAll("\n\n");
        if (message.parts.len == 0) {
            try out.writer.writeAll(message.content);
        } else {
            for (message.parts) |part| {
                if (part == .text) try out.writer.writeAll(part.text);
            }
        }
        wrote = true;
    }
    return try out.toOwnedSlice();
}

fn renderMessage(writer: *std.Io.Writer, message: types.Message) !void {
    const role = if (std.mem.eql(u8, message.role, "assistant")) "assistant" else "user";
    try writer.writeAll("{\"role\":");
    try writer.print("{f}", .{std.json.fmt(role, .{})});
    try writer.writeAll(",\"content\":");
    if (message.parts.len == 0) {
        try writer.print("{f}", .{std.json.fmt(message.content, .{})});
    } else {
        try writer.writeByte('[');
        for (message.parts, 0..) |part, i| {
            if (i > 0) try writer.writeByte(',');
            try renderContentPart(writer, part);
        }
        try writer.writeByte(']');
    }
    try writer.writeByte('}');
}

fn renderContentPart(writer: *std.Io.Writer, part: types.ContentPart) !void {
    switch (part) {
        .text => |text| {
            try writer.writeAll("{\"type\":\"text\",\"text\":");
            try writer.print("{f}", .{std.json.fmt(text, .{})});
            try writer.writeByte('}');
        },
        .image_base64 => |image| {
            try writer.writeAll("{\"type\":\"image\",\"source\":{\"type\":\"base64\",\"media_type\":");
            try writer.print("{f}", .{std.json.fmt(image.mime_type, .{})});
            try writer.writeAll(",\"data\":");
            try writer.print("{f}", .{std.json.fmt(image.data, .{})});
            try writer.writeAll("}}");
        },
        .image_url => |image| {
            try writer.writeAll("{\"type\":\"image\",\"source\":{\"type\":\"url\",\"url\":");
            try writer.print("{f}", .{std.json.fmt(image.url, .{})});
            try writer.writeAll("}}");
        },
        .audio_base64, .file_base64 => {
            try writer.writeAll("{\"type\":\"text\",\"text\":\"[unsupported media part omitted]\"}");
        },
    }
}

pub fn parseMessagesResponse(allocator: std.mem.Allocator, raw_json: []u8) !types.ChatResponse {
    errdefer allocator.free(raw_json);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw_json, .{});
    defer parsed.deinit();

    const content = parsed.value.object.get("content") orelse return error.InvalidResponse;
    if (content != .array) return error.InvalidResponse;
    var text = std.Io.Writer.Allocating.init(allocator);
    errdefer text.deinit();
    for (content.array.items) |part| {
        const kind = part.object.get("type") orelse continue;
        if (kind != .string or !std.mem.eql(u8, kind.string, "text")) continue;
        const value = part.object.get("text") orelse continue;
        if (value == .string) try text.writer.writeAll(value.string);
    }
    return .{ .content = try text.toOwnedSlice(), .raw_json = raw_json };
}

test "renderMessagesRequest maps system to top-level field" {
    const messages = [_]types.Message{
        .{ .role = "system", .content = "Return JSON." },
        .{ .role = "user", .content = "Classify this." },
    };
    const body = try renderMessagesRequest(std.testing.allocator, "claude-haiku-4-5-20251001", .{
        .messages = &messages,
        .temperature = 0.0,
        .max_tokens = 512,
        .json_mode = true,
    });
    defer std.testing.allocator.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "\"system\":\"Return JSON.\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"role\":\"system\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"max_tokens\":512") != null);
}

test "parseMessagesResponse extracts text blocks" {
    const raw = try std.testing.allocator.dupe(u8,
        \\{"content":[{"type":"text","text":"hello"},{"type":"text","text":" world"}]}
    );
    var response = try parseMessagesResponse(std.testing.allocator, raw);
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("hello world", response.content);
}
