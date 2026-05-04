const std = @import("std");
const types = @import("../types.zig");
const http_client = @import("../http_client.zig");

pub const Config = struct {
    base_url: []const u8 = "https://generativelanguage.googleapis.com/v1beta",
    api_key: ?[]const u8 = null,
    model: []const u8,
};

pub fn chat(
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,
    request: types.ChatRequest,
) !types.ChatResponse {
    const url = try modelUrl(allocator, config.base_url, config.model, "generateContent", config.api_key);
    defer allocator.free(url);

    const body = try renderGenerateContentRequest(allocator, request);
    defer allocator.free(body);

    const response = try http_client.postJson(allocator, io, url, null, body);
    errdefer response.deinit(allocator);
    return parseGenerateContentResponse(allocator, response.body);
}

pub fn embedTexts(
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,
    inputs: []const []const u8,
) !types.EmbeddingResponse {
    const url = try modelUrl(allocator, config.base_url, config.model, "batchEmbedContents", config.api_key);
    defer allocator.free(url);

    const body = try renderBatchEmbedRequest(allocator, config.model, inputs);
    defer allocator.free(body);

    const response = try http_client.postJson(allocator, io, url, null, body);
    errdefer response.deinit(allocator);
    return parseBatchEmbedResponse(allocator, response.body);
}

pub fn respond(
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,
    request: types.ResponseRequest,
) !types.ResponseResult {
    const url = try modelUrl(allocator, config.base_url, config.model, "generateContent", config.api_key);
    defer allocator.free(url);

    const body = try renderGenerateContentResponseRequest(allocator, request);
    defer allocator.free(body);

    const response = try http_client.postJson(allocator, io, url, null, body);
    errdefer response.deinit(allocator);
    return parseGenerateContentResponseResult(allocator, response.body);
}

fn modelUrl(
    allocator: std.mem.Allocator,
    base_url: []const u8,
    model: []const u8,
    action: []const u8,
    api_key: ?[]const u8,
) ![]u8 {
    const trimmed = http_client.trimRight(base_url, '/');
    if (api_key) |key| {
        return try std.fmt.allocPrint(allocator, "{s}/models/{s}:{s}?key={s}", .{ trimmed, model, action, key });
    }
    return try std.fmt.allocPrint(allocator, "{s}/models/{s}:{s}", .{ trimmed, model, action });
}

pub fn renderGenerateContentResponseRequest(
    allocator: std.mem.Allocator,
    request: types.ResponseRequest,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    try out.writer.writeAll("{");
    var wrote_field = false;
    if (request.instructions) |instructions| {
        try out.writer.writeAll("\"systemInstruction\":{\"parts\":[{\"text\":");
        try out.writer.print("{f}", .{std.json.fmt(instructions, .{})});
        try out.writer.writeAll("}]}");
        wrote_field = true;
    }
    if (wrote_field) try out.writer.writeByte(',');
    try out.writer.writeAll("\"contents\":[");
    try renderResponseInput(&out.writer, request.input);
    try out.writer.writeByte(']');

    if (request.temperature != null or request.max_output_tokens != null or request.text_format_json != null) {
        try out.writer.writeAll(",\"generationConfig\":{");
        var wrote_config = false;
        if (request.temperature) |temperature| {
            try out.writer.print("\"temperature\":{d}", .{temperature});
            wrote_config = true;
        }
        if (request.max_output_tokens) |max_tokens| {
            if (wrote_config) try out.writer.writeByte(',');
            try out.writer.print("\"maxOutputTokens\":{d}", .{max_tokens});
            wrote_config = true;
        }
        if (request.text_format_json != null) {
            if (wrote_config) try out.writer.writeByte(',');
            try out.writer.writeAll("\"responseMimeType\":\"application/json\"");
        }
        try out.writer.writeByte('}');
    }

    try out.writer.writeByte('}');
    return try out.toOwnedSlice();
}

fn renderResponseInput(writer: *std.Io.Writer, input: types.ResponseInput) !void {
    switch (input) {
        .text => |text| {
            try writer.writeAll("{\"role\":\"user\",\"parts\":[{\"text\":");
            try writer.print("{f}", .{std.json.fmt(text, .{})});
            try writer.writeAll("}]}");
        },
        .items => |items| {
            var wrote: usize = 0;
            for (items) |item| {
                if (item == .function_call_output) continue;
                if (wrote > 0) try writer.writeByte(',');
                switch (item) {
                    .message => |message| try renderResponseMessage(writer, message),
                    .function_call_output => unreachable,
                }
                wrote += 1;
            }
            if (wrote == 0) {
                try writer.writeAll("{\"role\":\"user\",\"parts\":[{\"text\":\"\"}]}");
            }
        },
    }
}

fn renderResponseMessage(writer: *std.Io.Writer, message: types.Message) !void {
    try writer.writeAll("{\"role\":");
    const role = if (std.mem.eql(u8, message.role, "assistant")) "model" else "user";
    try writer.print("{f}", .{std.json.fmt(role, .{})});
    try writer.writeAll(",\"parts\":[");
    if (message.parts.len == 0) {
        try writer.writeAll("{\"text\":");
        try writer.print("{f}", .{std.json.fmt(message.content, .{})});
        try writer.writeByte('}');
    } else {
        for (message.parts, 0..) |part, i| {
            if (i > 0) try writer.writeByte(',');
            try renderPart(writer, part);
        }
    }
    try writer.writeAll("]}");
}

pub fn renderGenerateContentRequest(
    allocator: std.mem.Allocator,
    request: types.ChatRequest,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    try out.writer.writeAll("{\"contents\":[");
    for (request.messages, 0..) |message, i| {
        if (i > 0) try out.writer.writeByte(',');
        try out.writer.writeAll("{\"role\":");
        const role = if (std.mem.eql(u8, message.role, "assistant")) "model" else "user";
        try out.writer.print("{f}", .{std.json.fmt(role, .{})});
        try out.writer.writeAll(",\"parts\":[");
        if (message.parts.len == 0) {
            try out.writer.writeAll("{\"text\":");
            try out.writer.print("{f}", .{std.json.fmt(message.content, .{})});
            try out.writer.writeByte('}');
        } else {
            for (message.parts, 0..) |part, j| {
                if (j > 0) try out.writer.writeByte(',');
                try renderPart(&out.writer, part);
            }
        }
        try out.writer.writeAll("]}");
    }
    try out.writer.writeByte(']');
    if (request.temperature != null or request.max_tokens != null) {
        try out.writer.writeAll(",\"generationConfig\":{");
        var wrote = false;
        if (request.temperature) |temperature| {
            try out.writer.print("\"temperature\":{d}", .{temperature});
            wrote = true;
        }
        if (request.max_tokens) |max_tokens| {
            if (wrote) try out.writer.writeByte(',');
            try out.writer.print("\"maxOutputTokens\":{d}", .{max_tokens});
        }
        try out.writer.writeByte('}');
    }
    try out.writer.writeByte('}');
    return try out.toOwnedSlice();
}

fn renderPart(writer: *std.Io.Writer, part: types.ContentPart) !void {
    switch (part) {
        .text => |text| {
            try writer.writeAll("{\"text\":");
            try writer.print("{f}", .{std.json.fmt(text, .{})});
            try writer.writeByte('}');
        },
        .image_base64 => |image| try renderInlineData(writer, image),
        .audio_base64 => |audio| try renderInlineData(writer, audio),
        .file_base64 => |file| try renderInlineData(writer, file),
        .image_url => |image| {
            try writer.writeAll("{\"fileData\":{\"mimeType\":\"image/*\",\"fileUri\":");
            try writer.print("{f}", .{std.json.fmt(image.url, .{})});
            try writer.writeAll("}}");
        },
    }
}

fn renderInlineData(writer: *std.Io.Writer, data: types.InlineData) !void {
    try writer.writeAll("{\"inlineData\":{\"mimeType\":");
    try writer.print("{f}", .{std.json.fmt(data.mime_type, .{})});
    try writer.writeAll(",\"data\":");
    try writer.print("{f}", .{std.json.fmt(data.data, .{})});
    try writer.writeAll("}}");
}

pub fn parseGenerateContentResponse(allocator: std.mem.Allocator, raw_json: []u8) !types.ChatResponse {
    errdefer allocator.free(raw_json);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw_json, .{});
    defer parsed.deinit();

    const candidates = parsed.value.object.get("candidates") orelse return error.InvalidResponse;
    if (candidates.array.items.len == 0) return error.InvalidResponse;
    const content = candidates.array.items[0].object.get("content") orelse return error.InvalidResponse;
    const parts = content.object.get("parts") orelse return error.InvalidResponse;

    var text = std.Io.Writer.Allocating.init(allocator);
    errdefer text.deinit();
    for (parts.array.items) |part| {
        if (part.object.get("text")) |value| {
            try text.writer.writeAll(value.string);
        }
    }
    return .{ .content = try text.toOwnedSlice(), .raw_json = raw_json };
}

pub fn parseGenerateContentResponseResult(allocator: std.mem.Allocator, raw_json: []u8) !types.ResponseResult {
    errdefer allocator.free(raw_json);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw_json, .{});
    defer parsed.deinit();

    const response_id = if (parsed.value.object.get("responseId")) |value|
        if (value == .string) value.string else ""
    else
        "";
    const candidates = parsed.value.object.get("candidates") orelse return error.InvalidResponse;
    if (candidates.array.items.len == 0) return error.InvalidResponse;
    const content = candidates.array.items[0].object.get("content") orelse return error.InvalidResponse;
    const parts = content.object.get("parts") orelse return error.InvalidResponse;

    var text = std.Io.Writer.Allocating.init(allocator);
    errdefer text.deinit();
    for (parts.array.items) |part| {
        if (part.object.get("text")) |value| try text.writer.writeAll(value.string);
    }
    const output_text = try text.toOwnedSlice();
    errdefer allocator.free(output_text);

    const items = try allocator.alloc(types.ResponseOutputItem, 1);
    errdefer allocator.free(items);
    items[0] = .{ .message = .{
        .id = try allocator.dupe(u8, response_id),
        .role = try allocator.dupe(u8, "assistant"),
        .text = try allocator.dupe(u8, output_text),
    } };

    return .{
        .id = try allocator.dupe(u8, response_id),
        .status = try allocator.dupe(u8, "completed"),
        .output_text = output_text,
        .output_items = items,
        .raw_json = raw_json,
    };
}

pub fn renderBatchEmbedRequest(
    allocator: std.mem.Allocator,
    model: []const u8,
    inputs: []const []const u8,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.writeAll("{\"requests\":[");
    for (inputs, 0..) |input, i| {
        if (i > 0) try out.writer.writeByte(',');
        try out.writer.writeAll("{\"model\":");
        try out.writer.print("{f}", .{std.json.fmt(model, .{})});
        try out.writer.writeAll(",\"content\":{\"parts\":[{\"text\":");
        try out.writer.print("{f}", .{std.json.fmt(input, .{})});
        try out.writer.writeAll("}]}}");
    }
    try out.writer.writeAll("]}");
    return try out.toOwnedSlice();
}

pub fn parseBatchEmbedResponse(allocator: std.mem.Allocator, raw_json: []u8) !types.EmbeddingResponse {
    errdefer allocator.free(raw_json);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw_json, .{});
    defer parsed.deinit();

    const embeddings = parsed.value.object.get("embeddings") orelse return error.InvalidResponse;
    if (embeddings != .array) return error.InvalidResponse;
    const vectors = try allocator.alloc(types.EmbeddingVector, embeddings.array.items.len);
    errdefer allocator.free(vectors);

    for (embeddings.array.items, 0..) |embedding_item, i| {
        const values_json = embedding_item.object.get("values") orelse return error.InvalidResponse;
        const values = try allocator.alloc(f32, values_json.array.items.len);
        errdefer allocator.free(values);
        for (values_json.array.items, 0..) |value, j| values[j] = @floatCast(value.float);
        vectors[i] = .{ .values = values };
    }
    return .{ .vectors = vectors, .raw_json = raw_json };
}

test "renderGenerateContentRequest maps assistant to model and image data" {
    const parts = [_]types.ContentPart{
        .{ .text = "look" },
        .{ .image_base64 = .{ .mime_type = "image/png", .data = "abc" } },
    };
    const messages = [_]types.Message{.{ .role = "assistant", .parts = &parts }};
    const body = try renderGenerateContentRequest(std.testing.allocator, .{ .messages = &messages });
    defer std.testing.allocator.free(body);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"role\":\"model\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"inlineData\"") != null);
}

test "parseGenerateContentResponse extracts text parts" {
    const raw = try std.testing.allocator.dupe(u8,
        \\{"candidates":[{"content":{"parts":[{"text":"hello"},{"text":" world"}]}}]}
    );
    var response = try parseGenerateContentResponse(std.testing.allocator, raw);
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("hello world", response.content);
}

test "renderGenerateContentResponseRequest maps instructions and response input" {
    const items = [_]types.ResponseInputItem{.{ .message = .{ .role = "user", .content = "hello" } }};
    const body = try renderGenerateContentResponseRequest(std.testing.allocator, .{
        .input = .{ .items = &items },
        .instructions = "Be terse.",
        .text_format_json = "{\"type\":\"json_object\"}",
    });
    defer std.testing.allocator.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "\"systemInstruction\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"responseMimeType\":\"application/json\"") != null);
}

test "parseGenerateContentResponseResult normalizes to ResponseResult" {
    const raw = try std.testing.allocator.dupe(u8,
        \\{"responseId":"gem_1","candidates":[{"content":{"parts":[{"text":"hello"}]}}]}
    );
    var result = try parseGenerateContentResponseResult(std.testing.allocator, raw);
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("gem_1", result.id);
    try std.testing.expectEqualStrings("hello", result.output_text);
    try std.testing.expectEqual(@as(usize, 1), result.output_items.len);
}
