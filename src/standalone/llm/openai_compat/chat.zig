const std = @import("std");
const types = @import("../types.zig");
const http_client = @import("../http_client.zig");
const endpoints = @import("endpoints.zig");

pub const Config = struct {
    base_url: []const u8,
    api_key: ?[]const u8 = null,
    model: []const u8,
    max_response_bytes: usize = 4 * 1024 * 1024,
};

pub fn chat(
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,
    request: types.ChatRequest,
) !types.ChatResponse {
    const url = try endpoints.completionUrl(allocator, config.base_url);
    defer allocator.free(url);

    const body = try renderRequest(allocator, config.model, request);
    defer allocator.free(body);

    const response = try http_client.postJson(allocator, io, url, config.api_key, body);
    errdefer response.deinit(allocator);
    return parseResponse(allocator, response.body);
}

pub fn renderRequest(
    allocator: std.mem.Allocator,
    model: []const u8,
    request: types.ChatRequest,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    try out.writer.writeAll("{\"model\":");
    try out.writer.print("{f}", .{std.json.fmt(model, .{})});
    try out.writer.writeAll(",\"messages\":[");
    for (request.messages, 0..) |message, i| {
        if (i > 0) try out.writer.writeByte(',');
        try renderMessage(&out.writer, message);
    }
    try out.writer.writeByte(']');

    if (request.temperature) |temperature| {
        try out.writer.print(",\"temperature\":{d}", .{temperature});
    }
    if (request.max_tokens) |max_tokens| {
        try out.writer.print(",\"max_tokens\":{d}", .{max_tokens});
    }
    if (request.json_mode) {
        try out.writer.writeAll(",\"response_format\":{\"type\":\"json_object\"}");
    }
    if (request.tools.len > 0) {
        try renderTools(&out.writer, request.tools);
    }
    if (request.tool_choice) |choice| {
        try renderToolChoice(&out.writer, choice);
    }
    try out.writer.writeByte('}');

    return try out.toOwnedSlice();
}

fn renderMessage(writer: *std.Io.Writer, message: types.Message) !void {
    try writer.writeAll("{\"role\":");
    try writer.print("{f}", .{std.json.fmt(message.role, .{})});
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
        .image_url => |image| {
            try writer.writeAll("{\"type\":\"image_url\",\"image_url\":{\"url\":");
            try writer.print("{f}", .{std.json.fmt(image.url, .{})});
            if (image.detail) |detail| {
                try writer.writeAll(",\"detail\":");
                try writer.print("{f}", .{std.json.fmt(detail, .{})});
            }
            try writer.writeAll("}}");
        },
        .image_base64 => |image| {
            try writer.writeAll("{\"type\":\"image_url\",\"image_url\":{\"url\":");
            try renderDataUriString(writer, image.mime_type, image.data);
            if (image.detail) |detail| {
                try writer.writeAll(",\"detail\":");
                try writer.print("{f}", .{std.json.fmt(detail, .{})});
            }
            try writer.writeAll("}}");
        },
        .audio_base64 => |audio| {
            try writer.writeAll("{\"type\":\"input_audio\",\"input_audio\":{\"data\":");
            try writer.print("{f}", .{std.json.fmt(audio.data, .{})});
            try writer.writeAll(",\"format\":");
            try writer.print("{f}", .{std.json.fmt(audio.mime_type, .{})});
            try writer.writeAll("}}");
        },
        .file_base64 => |file| {
            try writer.writeAll("{\"type\":\"file\",\"file\":{\"file_data\":");
            try renderDataUriString(writer, file.mime_type, file.data);
            try writer.writeAll("}}");
        },
    }
}

fn renderDataUriString(writer: *std.Io.Writer, mime_type: []const u8, base64_data: []const u8) !void {
    try writer.writeByte('"');
    try writer.writeAll("data:");
    try writer.writeAll(mime_type);
    try writer.writeAll(";base64,");
    try writer.writeAll(base64_data);
    try writer.writeByte('"');
}

fn renderTools(writer: *std.Io.Writer, tools: []const types.Tool) !void {
    try writer.writeAll(",\"tools\":[");
    for (tools, 0..) |tool, i| {
        if (i > 0) try writer.writeByte(',');
        try writer.writeAll("{\"type\":\"function\",\"function\":{\"name\":");
        try writer.print("{f}", .{std.json.fmt(tool.name, .{})});
        if (tool.description) |description| {
            try writer.writeAll(",\"description\":");
            try writer.print("{f}", .{std.json.fmt(description, .{})});
        }
        try writer.writeAll(",\"parameters\":");
        try writer.writeAll(tool.parameters_json);
        try writer.writeAll("}}");
    }
    try writer.writeByte(']');
}

fn renderToolChoice(writer: *std.Io.Writer, choice: types.ToolChoice) !void {
    try writer.writeAll(",\"tool_choice\":");
    switch (choice) {
        .auto => try writer.writeAll("\"auto\""),
        .none => try writer.writeAll("\"none\""),
        .required => try writer.writeAll("\"required\""),
        .named => |name| {
            try writer.writeAll("{\"type\":\"function\",\"function\":{\"name\":");
            try writer.print("{f}", .{std.json.fmt(name, .{})});
            try writer.writeAll("}}");
        },
    }
}

pub fn parseResponse(allocator: std.mem.Allocator, raw_json: []u8) !types.ChatResponse {
    errdefer allocator.free(raw_json);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw_json, .{});
    defer parsed.deinit();

    const choices = parsed.value.object.get("choices") orelse return error.InvalidResponse;
    if (choices.array.items.len == 0) return error.InvalidResponse;
    const first = choices.array.items[0];
    const message = first.object.get("message") orelse return error.InvalidResponse;
    const content_value = message.object.get("content");
    const content = if (content_value) |value| switch (value) {
        .string => |text| try allocator.dupe(u8, text),
        else => try allocator.dupe(u8, ""),
    } else try allocator.dupe(u8, "");
    errdefer allocator.free(content);

    const tool_calls = try parseToolCalls(allocator, message);
    errdefer {
        for (tool_calls) |call| call.deinit(allocator);
        if (tool_calls.len > 0) allocator.free(tool_calls);
    }

    return .{ .content = content, .raw_json = raw_json, .tool_calls = tool_calls };
}

fn parseToolCalls(allocator: std.mem.Allocator, message: std.json.Value) ![]types.ToolCall {
    const calls_value = message.object.get("tool_calls") orelse return &.{};
    if (calls_value != .array) return error.InvalidResponse;
    const calls = try allocator.alloc(types.ToolCall, calls_value.array.items.len);
    errdefer allocator.free(calls);

    for (calls_value.array.items, 0..) |call_value, i| {
        const id_value = call_value.object.get("id") orelse return error.InvalidResponse;
        const function_value = call_value.object.get("function") orelse return error.InvalidResponse;
        const name_value = function_value.object.get("name") orelse return error.InvalidResponse;
        const arguments_value = function_value.object.get("arguments") orelse return error.InvalidResponse;
        calls[i] = .{
            .id = try allocator.dupe(u8, id_value.string),
            .name = try allocator.dupe(u8, name_value.string),
            .arguments_json = if (arguments_value == .string)
                try allocator.dupe(u8, arguments_value.string)
            else
                try std.json.Stringify.valueAlloc(allocator, arguments_value, .{}),
        };
    }
    return calls;
}

pub fn parseContent(allocator: std.mem.Allocator, raw_json: []const u8) ![]u8 {
    const owned_raw = try allocator.dupe(u8, raw_json);
    var response = try parseResponse(allocator, owned_raw);
    defer response.deinit(allocator);
    return try allocator.dupe(u8, response.content);
}

test "renderRequest emits legacy messages and json mode" {
    const messages = [_]types.Message{
        .{ .role = "system", .content = "Return JSON." },
        .{ .role = "user", .content = "Profile this document." },
    };
    const body = try renderRequest(std.testing.allocator, "local-model", .{
        .messages = &messages,
        .temperature = 0.1,
        .max_tokens = 512,
        .json_mode = true,
    });
    defer std.testing.allocator.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "\"model\":\"local-model\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"role\":\"system\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"response_format\":{\"type\":\"json_object\"}") != null);
}

test "renderRequest emits tool metadata and image parts" {
    const parts = [_]types.ContentPart{
        .{ .text = "Describe this." },
        .{ .image_url = .{ .url = "https://example.test/image.png", .detail = "low" } },
    };
    const messages = [_]types.Message{.{ .role = "user", .parts = &parts }};
    const tools = [_]types.Tool{.{
        .name = "lookup",
        .description = "Lookup a fact.",
        .parameters_json = "{\"type\":\"object\",\"properties\":{}}",
    }};
    const body = try renderRequest(std.testing.allocator, "model", .{
        .messages = &messages,
        .tools = &tools,
        .tool_choice = .{ .named = "lookup" },
    });
    defer std.testing.allocator.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "\"type\":\"image_url\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"tools\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"tool_choice\"") != null);
}

test "parseResponse extracts content and tool calls" {
    const raw = try std.testing.allocator.dupe(u8,
        \\{
        \\  "choices": [
        \\    {"message": {
        \\      "role": "assistant",
        \\      "content": "using a tool",
        \\      "tool_calls": [
        \\        {"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"zig\"}"}}
        \\      ]
        \\    }}
        \\  ]
        \\}
    );
    var response = try parseResponse(std.testing.allocator, raw);
    defer response.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("using a tool", response.content);
    try std.testing.expectEqual(@as(usize, 1), response.tool_calls.len);
    try std.testing.expectEqualStrings("lookup", response.tool_calls[0].name);
}
