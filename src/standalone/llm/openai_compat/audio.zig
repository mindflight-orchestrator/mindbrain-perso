const std = @import("std");
const types = @import("../types.zig");
const http_client = @import("../http_client.zig");
const endpoints = @import("endpoints.zig");

pub const Config = struct {
    base_url: []const u8,
    api_key: ?[]const u8 = null,
    model: []const u8,
};

pub fn transcribe(
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,
    request: types.AudioTranscriptionRequest,
) !types.AudioTranscriptionResponse {
    const url = try endpoints.audioTranscriptionsUrl(allocator, config.base_url);
    defer allocator.free(url);

    const boundary = "mindbrain-llm-boundary";
    const body = try renderMultipartRequest(allocator, boundary, request);
    defer allocator.free(body);

    var content_type_buf: [128]u8 = undefined;
    const content_type = try std.fmt.bufPrint(&content_type_buf, "multipart/form-data; boundary={s}", .{boundary});
    var auth_header: ?[]u8 = null;
    defer if (auth_header) |value| allocator.free(value);

    var headers_buf: [2]http_client.Header = undefined;
    var header_count: usize = 0;
    headers_buf[header_count] = .{ .name = "content-type", .value = content_type };
    header_count += 1;
    if (config.api_key) |key| {
        auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{key});
        headers_buf[header_count] = .{ .name = "authorization", .value = auth_header.? };
        header_count += 1;
    }

    const response = try http_client.postWithHeaders(allocator, io, url, body, headers_buf[0..header_count]);
    errdefer response.deinit(allocator);
    return parseResponse(allocator, response.body);
}

pub fn renderMultipartRequest(
    allocator: std.mem.Allocator,
    boundary: []const u8,
    request: types.AudioTranscriptionRequest,
) ![]u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);

    try appendField(allocator, &out, boundary, "model", request.model);
    if (request.language) |language| try appendField(allocator, &out, boundary, "language", language);
    if (request.prompt) |prompt| try appendField(allocator, &out, boundary, "prompt", prompt);
    if (request.response_format) |format| try appendField(allocator, &out, boundary, "response_format", format);

    try out.writer(allocator).print("--{s}\r\n", .{boundary});
    try out.writer(allocator).print(
        "Content-Disposition: form-data; name=\"file\"; filename=\"{s}\"\r\n",
        .{request.filename},
    );
    try out.writer(allocator).print("Content-Type: {s}\r\n\r\n", .{request.mime_type});
    try out.appendSlice(allocator, request.audio_bytes);
    try out.appendSlice(allocator, "\r\n");
    try out.writer(allocator).print("--{s}--\r\n", .{boundary});

    return try out.toOwnedSlice(allocator);
}

fn appendField(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(u8),
    boundary: []const u8,
    name: []const u8,
    value: []const u8,
) !void {
    try out.writer(allocator).print("--{s}\r\n", .{boundary});
    try out.writer(allocator).print("Content-Disposition: form-data; name=\"{s}\"\r\n\r\n", .{name});
    try out.appendSlice(allocator, value);
    try out.appendSlice(allocator, "\r\n");
}

pub fn parseResponse(allocator: std.mem.Allocator, raw_json: []u8) !types.AudioTranscriptionResponse {
    errdefer allocator.free(raw_json);
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, raw_json, .{}) catch {
        return .{ .text = try allocator.dupe(u8, raw_json), .raw_json = raw_json };
    };
    defer parsed.deinit();

    const text_value = parsed.value.object.get("text") orelse return error.InvalidResponse;
    return .{ .text = try allocator.dupe(u8, text_value.string), .raw_json = raw_json };
}

test "renderMultipartRequest includes model and file body" {
    const body = try renderMultipartRequest(std.testing.allocator, "boundary", .{
        .model = "whisper",
        .filename = "sample.wav",
        .mime_type = "audio/wav",
        .audio_bytes = "abc",
    });
    defer std.testing.allocator.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "name=\"model\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "filename=\"sample.wav\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "abc") != null);
}

test "parseResponse extracts JSON text" {
    const raw = try std.testing.allocator.dupe(u8, "{\"text\":\"hello\"}");
    var response = try parseResponse(std.testing.allocator, raw);
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("hello", response.text);
}
