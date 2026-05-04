const std = @import("std");
const types = @import("../types.zig");
const http_client = @import("../http_client.zig");
const endpoints = @import("endpoints.zig");

pub const Config = struct {
    base_url: []const u8,
    api_key: ?[]const u8 = null,
    model: []const u8,
};

pub fn embedTexts(
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,
    inputs: []const []const u8,
) !types.EmbeddingResponse {
    const url = try endpoints.embeddingsUrl(allocator, config.base_url);
    defer allocator.free(url);

    const body = try renderRequest(allocator, config.model, inputs);
    defer allocator.free(body);

    const response = try http_client.postJson(allocator, io, url, config.api_key, body);
    errdefer response.deinit(allocator);
    return parseResponse(allocator, response.body);
}

pub fn renderRequest(
    allocator: std.mem.Allocator,
    model: []const u8,
    inputs: []const []const u8,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    try out.writer.writeAll("{\"model\":");
    try out.writer.print("{f}", .{std.json.fmt(model, .{})});
    try out.writer.writeAll(",\"input\":[");
    for (inputs, 0..) |input, i| {
        if (i > 0) try out.writer.writeByte(',');
        try out.writer.print("{f}", .{std.json.fmt(input, .{})});
    }
    try out.writer.writeAll("]}");
    return try out.toOwnedSlice();
}

pub fn parseResponse(allocator: std.mem.Allocator, raw_json: []u8) !types.EmbeddingResponse {
    errdefer allocator.free(raw_json);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw_json, .{});
    defer parsed.deinit();

    const data = parsed.value.object.get("data") orelse return error.InvalidResponse;
    if (data != .array) return error.InvalidResponse;
    const vectors = try allocator.alloc(types.EmbeddingVector, data.array.items.len);
    errdefer allocator.free(vectors);

    for (data.array.items, 0..) |item, i| {
        const embedding = item.object.get("embedding") orelse return error.InvalidResponse;
        if (embedding != .array) return error.InvalidResponse;
        const values = try allocator.alloc(f32, embedding.array.items.len);
        errdefer allocator.free(values);
        for (embedding.array.items, 0..) |value, j| {
            values[j] = @floatCast(value.float);
        }
        vectors[i] = .{ .values = values };
    }

    return .{ .vectors = vectors, .raw_json = raw_json };
}

pub fn vectorToLittleEndianBlob(allocator: std.mem.Allocator, values: []const f32) ![]u8 {
    const blob = try allocator.alloc(u8, values.len * @sizeOf(f32));
    for (values, 0..) |value, i| {
        std.mem.writeInt(u32, blob[i * 4 ..][0..4], @bitCast(value), .little);
    }
    return blob;
}

test "renderRequest emits embeddings payload" {
    const inputs = [_][]const u8{ "first", "second" };
    const body = try renderRequest(std.testing.allocator, "embed-model", &inputs);
    defer std.testing.allocator.free(body);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"model\":\"embed-model\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"first\"") != null);
}

test "parseResponse extracts embedding vectors" {
    const raw = try std.testing.allocator.dupe(u8,
        \\{"data":[{"embedding":[0.25,-1.5,2]}]}
    );
    var response = try parseResponse(std.testing.allocator, raw);
    defer response.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), response.vectors.len);
    try std.testing.expectEqual(@as(usize, 3), response.vectors[0].values.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.25), response.vectors[0].values[0], 0.0001);
}
