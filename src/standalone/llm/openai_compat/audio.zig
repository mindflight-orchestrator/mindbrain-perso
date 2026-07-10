const std = @import("std");
const types = @import("../types.zig");
const http_client = @import("../http_client.zig");
const endpoints = @import("endpoints.zig");

pub const Config = struct {
    base_url: []const u8,
    api_key: ?[]const u8 = null,
    model: []const u8,
    max_response_bytes: usize = 4 * 1024 * 1024,
    retry: http_client.RetryPolicy = .{},
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

    const response = try http_client.postWithHeaders(allocator, io, url, body, headers_buf[0..header_count], .{
        .max_response_bytes = config.max_response_bytes,
        .retry = config.retry,
    });
    // parseResponse owns response.body from here on (success and failure).
    return parseResponse(allocator, response.body);
}

pub fn renderMultipartRequest(
    allocator: std.mem.Allocator,
    boundary: []const u8,
    request: types.AudioTranscriptionRequest,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    try appendField(&out.writer, boundary, "model", request.model);
    if (request.language) |language| try appendField(&out.writer, boundary, "language", language);
    if (request.prompt) |prompt| try appendField(&out.writer, boundary, "prompt", prompt);
    if (request.response_format) |format| try appendField(&out.writer, boundary, "response_format", format);

    try out.writer.print("--{s}\r\n", .{boundary});
    try out.writer.print(
        "Content-Disposition: form-data; name=\"file\"; filename=\"{s}\"\r\n",
        .{request.filename},
    );
    try out.writer.print("Content-Type: {s}\r\n\r\n", .{request.mime_type});
    try out.writer.writeAll(request.audio_bytes);
    try out.writer.writeAll("\r\n");
    try out.writer.print("--{s}--\r\n", .{boundary});

    return try out.toOwnedSlice();
}

fn appendField(
    writer: *std.Io.Writer,
    boundary: []const u8,
    name: []const u8,
    value: []const u8,
) !void {
    try writer.print("--{s}\r\n", .{boundary});
    try writer.print("Content-Disposition: form-data; name=\"{s}\"\r\n\r\n", .{name});
    try writer.writeAll(value);
    try writer.writeAll("\r\n");
}

/// Takes ownership of `raw_json`: on success it is stored in the returned
/// response (freed by its `deinit`); on failure it is freed here. Callers
/// must not free it themselves.
pub fn parseResponse(allocator: std.mem.Allocator, raw_json: []u8) !types.AudioTranscriptionResponse {
    errdefer allocator.free(raw_json);
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, raw_json, .{}) catch {
        return .{ .text = try allocator.dupe(u8, raw_json), .raw_json = raw_json };
    };
    defer parsed.deinit();

    if (parsed.value != .object) return error.InvalidResponse;
    const text_value = parsed.value.object.get("text") orelse return error.InvalidResponse;
    if (text_value != .string) return error.InvalidResponse;
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
