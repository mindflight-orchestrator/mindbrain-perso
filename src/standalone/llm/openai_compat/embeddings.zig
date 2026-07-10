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

    const response = try http_client.postJson(allocator, io, url, config.api_key, body, .{
        .max_response_bytes = config.max_response_bytes,
        .retry = config.retry,
    });
    // parseResponse owns response.body from here on (success and failure).
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

/// Takes ownership of `raw_json`: on success it is stored in the returned
/// response (freed by its `deinit`); on failure it is freed here. Callers
/// must not free it themselves.
pub fn parseResponse(allocator: std.mem.Allocator, raw_json: []u8) !types.EmbeddingResponse {
    errdefer allocator.free(raw_json);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw_json, .{});
    defer parsed.deinit();

    if (parsed.value != .object) return error.InvalidResponse;
    const data = parsed.value.object.get("data") orelse return error.InvalidResponse;
    if (data != .array) return error.InvalidResponse;
    const vectors = try allocator.alloc(types.EmbeddingVector, data.array.items.len);
    var filled: usize = 0;
    errdefer {
        for (vectors[0..filled]) |vector| allocator.free(vector.values);
        allocator.free(vectors);
    }

    for (data.array.items) |item| {
        if (item != .object) return error.InvalidResponse;
        const embedding = item.object.get("embedding") orelse return error.InvalidResponse;
        if (embedding != .array) return error.InvalidResponse;
        const values = try allocator.alloc(f32, embedding.array.items.len);
        errdefer allocator.free(values);
        for (embedding.array.items, 0..) |value, j| {
            values[j] = try jsonNumberToF32(value);
        }
        vectors[filled] = .{ .values = values };
        filled += 1;
    }

    return .{ .vectors = vectors, .raw_json = raw_json };
}

/// Embedding components arrive as JSON numbers; whole values (0, 2, -1) parse
/// as `.integer`, so accessing `.float` unconditionally would panic.
fn jsonNumberToF32(value: std.json.Value) !f32 {
    return switch (value) {
        .float => |v| @floatCast(v),
        .integer => |v| @floatFromInt(v),
        .number_string => |text| @floatCast(std.fmt.parseFloat(f64, text) catch return error.InvalidResponse),
        else => error.InvalidResponse,
    };
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

test "parseResponse accepts whole-number embedding components" {
    const raw = try std.testing.allocator.dupe(u8,
        \\{"data":[{"embedding":[0,2,-1,0.5]}]}
    );
    var response = try parseResponse(std.testing.allocator, raw);
    defer response.deinit(std.testing.allocator);

    try std.testing.expectApproxEqAbs(@as(f32, 0.0), response.vectors[0].values[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), response.vectors[0].values[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), response.vectors[0].values[2], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), response.vectors[0].values[3], 0.0001);
}

fn expectInvalidResponse(raw: []const u8) !void {
    const owned = try std.testing.allocator.dupe(u8, raw);
    try std.testing.expectError(error.InvalidResponse, parseResponse(std.testing.allocator, owned));
}

test "parseResponse rejects malformed payloads without crashing or leaking" {
    try expectInvalidResponse("[]");
    try expectInvalidResponse("{\"data\":42}");
    try expectInvalidResponse("{\"data\":[\"nope\"]}");
    try expectInvalidResponse("{\"data\":[{\"embedding\":true}]}");
    try expectInvalidResponse("{\"data\":[{\"embedding\":[\"x\"]}]}");
    // Later malformed element must free vectors parsed before it.
    try expectInvalidResponse("{\"data\":[{\"embedding\":[1.5]},{\"embedding\":[null]}]}");
}
