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

pub const EventList = struct {
    events: []types.StreamEvent,

    pub fn deinit(self: EventList, allocator: std.mem.Allocator) void {
        for (self.events) |event| {
            if (event.text) |value| allocator.free(value);
            if (event.tool_id) |value| allocator.free(value);
            if (event.tool_name) |value| allocator.free(value);
            if (event.arguments_delta) |value| allocator.free(value);
            if (event.raw_json) |value| allocator.free(value);
        }
        allocator.free(self.events);
    }
};

pub fn respond(
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,
    request: types.ResponseRequest,
) !types.ResponseResult {
    const url = try endpoints.responsesUrl(allocator, config.base_url);
    defer allocator.free(url);

    const body = try renderRequest(allocator, config.model, request);
    defer allocator.free(body);

    const response = try http_client.postJson(allocator, io, url, config.api_key, body);
    errdefer response.deinit(allocator);
    return parseResponse(allocator, response.body);
}

pub fn streamRespond(
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,
    request: types.ResponseRequest,
) !EventList {
    const url = try endpoints.responsesUrl(allocator, config.base_url);
    defer allocator.free(url);

    var stream_request = request;
    stream_request.stream = true;
    const body = try renderRequest(allocator, config.model, stream_request);
    defer allocator.free(body);

    const response = try http_client.postJson(allocator, io, url, config.api_key, body);
    defer response.deinit(allocator);
    return parseSseEvents(allocator, response.body);
}

pub fn renderRequest(
    allocator: std.mem.Allocator,
    model: []const u8,
    request: types.ResponseRequest,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    try out.writer.writeAll("{\"model\":");
    try out.writer.print("{f}", .{std.json.fmt(model, .{})});
    if (request.instructions) |instructions| {
        try out.writer.writeAll(",\"instructions\":");
        try out.writer.print("{f}", .{std.json.fmt(instructions, .{})});
    }
    try out.writer.writeAll(",\"input\":");
    try renderInput(&out.writer, request.input);
    if (request.temperature) |temperature| try out.writer.print(",\"temperature\":{d}", .{temperature});
    if (request.max_output_tokens) |tokens| try out.writer.print(",\"max_output_tokens\":{d}", .{tokens});
    if (request.store) |store| try out.writer.print(",\"store\":{}", .{store});
    if (request.previous_response_id) |id| {
        try out.writer.writeAll(",\"previous_response_id\":");
        try out.writer.print("{f}", .{std.json.fmt(id, .{})});
    }
    if (request.text_format_json) |format| {
        try out.writer.writeAll(",\"text\":{\"format\":");
        try out.writer.writeAll(format);
        try out.writer.writeByte('}');
    }
    if (request.tools.len > 0) try renderTools(&out.writer, request.tools);
    if (request.tool_choice) |choice| try renderToolChoice(&out.writer, choice);
    if (request.stream) try out.writer.writeAll(",\"stream\":true");
    try out.writer.writeByte('}');
    return try out.toOwnedSlice();
}

fn renderInput(writer: *std.Io.Writer, input: types.ResponseInput) !void {
    switch (input) {
        .text => |text| try writer.print("{f}", .{std.json.fmt(text, .{})}),
        .items => |items| {
            try writer.writeByte('[');
            for (items, 0..) |item, i| {
                if (i > 0) try writer.writeByte(',');
                try renderInputItem(writer, item);
            }
            try writer.writeByte(']');
        },
    }
}

fn renderInputItem(writer: *std.Io.Writer, item: types.ResponseInputItem) !void {
    switch (item) {
        .message => |message| {
            try writer.writeAll("{\"type\":\"message\",\"role\":");
            try writer.print("{f}", .{std.json.fmt(message.role, .{})});
            try writer.writeAll(",\"content\":[");
            if (message.parts.len == 0) {
                try renderMessageTextPart(writer, message.role, message.content);
            } else {
                for (message.parts, 0..) |part, i| {
                    if (i > 0) try writer.writeByte(',');
                    try renderContentPart(writer, message.role, part);
                }
            }
            try writer.writeAll("]}");
        },
        .function_call_output => |output| {
            try writer.writeAll("{\"type\":\"function_call_output\",\"call_id\":");
            try writer.print("{f}", .{std.json.fmt(output.call_id, .{})});
            try writer.writeAll(",\"output\":");
            try writer.print("{f}", .{std.json.fmt(output.output, .{})});
            try writer.writeByte('}');
        },
    }
}

fn renderMessageTextPart(writer: *std.Io.Writer, role: []const u8, text: []const u8) !void {
    const part_type = if (std.mem.eql(u8, role, "assistant")) "output_text" else "input_text";
    try writer.writeAll("{\"type\":");
    try writer.print("{f}", .{std.json.fmt(part_type, .{})});
    try writer.writeAll(",\"text\":");
    try writer.print("{f}", .{std.json.fmt(text, .{})});
    try writer.writeByte('}');
}

fn renderContentPart(writer: *std.Io.Writer, role: []const u8, part: types.ContentPart) !void {
    switch (part) {
        .text => |text| try renderMessageTextPart(writer, role, text),
        .image_url => |image| {
            try writer.writeAll("{\"type\":\"input_image\",\"image_url\":");
            try writer.print("{f}", .{std.json.fmt(image.url, .{})});
            if (image.detail) |detail| {
                try writer.writeAll(",\"detail\":");
                try writer.print("{f}", .{std.json.fmt(detail, .{})});
            }
            try writer.writeByte('}');
        },
        .image_base64 => |image| {
            try writer.writeAll("{\"type\":\"input_image\",\"image_url\":");
            try renderDataUriString(writer, image.mime_type, image.data);
            if (image.detail) |detail| {
                try writer.writeAll(",\"detail\":");
                try writer.print("{f}", .{std.json.fmt(detail, .{})});
            }
            try writer.writeByte('}');
        },
        .audio_base64 => |audio| {
            try writer.writeAll("{\"type\":\"input_audio\",\"input_audio\":{\"data\":");
            try writer.print("{f}", .{std.json.fmt(audio.data, .{})});
            try writer.writeAll(",\"format\":");
            try writer.print("{f}", .{std.json.fmt(audio.mime_type, .{})});
            try writer.writeAll("}}");
        },
        .file_base64 => |file| {
            try writer.writeAll("{\"type\":\"input_file\",\"file_data\":");
            try renderDataUriString(writer, file.mime_type, file.data);
            try writer.writeByte('}');
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
        try writer.writeAll("{\"type\":\"function\",\"name\":");
        try writer.print("{f}", .{std.json.fmt(tool.name, .{})});
        if (tool.description) |description| {
            try writer.writeAll(",\"description\":");
            try writer.print("{f}", .{std.json.fmt(description, .{})});
        }
        try writer.writeAll(",\"parameters\":");
        try writer.writeAll(tool.parameters_json);
        try writer.writeByte('}');
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
            try writer.writeAll("{\"type\":\"function\",\"name\":");
            try writer.print("{f}", .{std.json.fmt(name, .{})});
            try writer.writeByte('}');
        },
    }
}

pub fn parseResponse(allocator: std.mem.Allocator, raw_json: []u8) !types.ResponseResult {
    errdefer allocator.free(raw_json);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw_json, .{});
    defer parsed.deinit();

    const id = try dupObjectString(allocator, parsed.value, "id", "");
    errdefer allocator.free(id);
    const status = try dupObjectString(allocator, parsed.value, "status", "");
    errdefer allocator.free(status);

    const output_value = parsed.value.object.get("output") orelse return error.InvalidResponse;
    if (output_value != .array) return error.InvalidResponse;
    var items = std.ArrayList(types.ResponseOutputItem).empty;
    errdefer deinitPartialOutputItems(allocator, items.items);

    var output_text = std.Io.Writer.Allocating.init(allocator);
    errdefer output_text.deinit();
    for (output_value.array.items) |item_value| {
        const item = try parseOutputItem(allocator, item_value, &output_text);
        try items.append(allocator, item);
    }

    return .{
        .id = id,
        .status = status,
        .output_text = try output_text.toOwnedSlice(),
        .output_items = try items.toOwnedSlice(allocator),
        .raw_json = raw_json,
    };
}

fn parseOutputItem(
    allocator: std.mem.Allocator,
    item_value: std.json.Value,
    output_text: *std.Io.Writer.Allocating,
) !types.ResponseOutputItem {
    const item_type = if (item_value.object.get("type")) |value| value.string else "unknown";
    if (std.mem.eql(u8, item_type, "message")) return parseMessageOutput(allocator, item_value, output_text);
    if (std.mem.eql(u8, item_type, "function_call")) return .{ .function_call = .{
        .id = try dupObjectString(allocator, item_value, "id", ""),
        .call_id = try dupObjectString(allocator, item_value, "call_id", ""),
        .name = try dupObjectString(allocator, item_value, "name", ""),
        .arguments_json = try dupObjectString(allocator, item_value, "arguments", "{}"),
    } };
    if (std.mem.eql(u8, item_type, "function_call_output")) return .{ .function_call_output = .{
        .id = try dupObjectString(allocator, item_value, "id", ""),
        .call_id = try dupObjectString(allocator, item_value, "call_id", ""),
        .output = try dupObjectString(allocator, item_value, "output", ""),
    } };
    if (std.mem.eql(u8, item_type, "reasoning")) return .{ .reasoning = .{
        .id = try dupObjectString(allocator, item_value, "id", ""),
        .summary = try dupSummary(allocator, item_value),
    } };
    return .{ .unknown = .{
        .item_type = try allocator.dupe(u8, item_type),
        .raw_json = try std.json.Stringify.valueAlloc(allocator, item_value, .{}),
    } };
}

fn parseMessageOutput(
    allocator: std.mem.Allocator,
    item_value: std.json.Value,
    output_text: *std.Io.Writer.Allocating,
) !types.ResponseOutputItem {
    const content = item_value.object.get("content") orelse return error.InvalidResponse;
    if (content != .array) return error.InvalidResponse;
    var text = std.Io.Writer.Allocating.init(allocator);
    errdefer text.deinit();
    for (content.array.items) |part| {
        const part_type = if (part.object.get("type")) |value| value.string else "";
        if (std.mem.eql(u8, part_type, "output_text")) {
            if (part.object.get("text")) |value| {
                try text.writer.writeAll(value.string);
                try output_text.writer.writeAll(value.string);
            }
        }
    }
    return .{ .message = .{
        .id = try dupObjectString(allocator, item_value, "id", ""),
        .role = try dupObjectString(allocator, item_value, "role", "assistant"),
        .text = try text.toOwnedSlice(),
    } };
}

fn dupSummary(allocator: std.mem.Allocator, item_value: std.json.Value) ![]u8 {
    const summary = item_value.object.get("summary") orelse return try allocator.dupe(u8, "");
    if (summary == .string) return try allocator.dupe(u8, summary.string);
    return try std.json.Stringify.valueAlloc(allocator, summary, .{});
}

fn dupObjectString(
    allocator: std.mem.Allocator,
    object: std.json.Value,
    key: []const u8,
    default: []const u8,
) ![]u8 {
    const value = object.object.get(key) orelse return try allocator.dupe(u8, default);
    return switch (value) {
        .string => |text| try allocator.dupe(u8, text),
        else => try std.json.Stringify.valueAlloc(allocator, value, .{}),
    };
}

fn deinitPartialOutputItems(allocator: std.mem.Allocator, items: []types.ResponseOutputItem) void {
    for (items) |item| item.deinit(allocator);
}

pub fn parseSseEvents(allocator: std.mem.Allocator, sse_body: []const u8) !EventList {
    var events = std.ArrayList(types.StreamEvent).empty;
    errdefer deinitPartialEvents(allocator, events.items);

    var lines = std.mem.splitScalar(u8, sse_body, '\n');
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \r");
        if (!std.mem.startsWith(u8, line, "data:")) continue;
        const data = std.mem.trim(u8, line["data:".len..], " ");
        if (data.len == 0) continue;
        if (std.mem.eql(u8, data, "[DONE]")) {
            try events.append(allocator, .{ .kind = .done });
            continue;
        }
        try appendEventForChunk(allocator, &events, data);
    }

    return .{ .events = try events.toOwnedSlice(allocator) };
}

fn appendEventForChunk(
    allocator: std.mem.Allocator,
    events: *std.ArrayList(types.StreamEvent),
    data: []const u8,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, data, .{});
    defer parsed.deinit();
    const event_type = if (parsed.value.object.get("type")) |value| value.string else "";

    if (std.mem.eql(u8, event_type, "response.created")) {
        try events.append(allocator, .{ .kind = .response_created, .raw_json = try allocator.dupe(u8, data) });
    } else if (std.mem.eql(u8, event_type, "response.output_item.added")) {
        try events.append(allocator, .{ .kind = .output_item_added, .raw_json = try allocator.dupe(u8, data) });
    } else if (std.mem.eql(u8, event_type, "response.output_text.delta")) {
        const delta = if (parsed.value.object.get("delta")) |value| if (value == .string) value.string else "" else "";
        try events.append(allocator, .{
            .kind = .output_text_delta,
            .text = try allocator.dupe(u8, delta),
            .raw_json = try allocator.dupe(u8, data),
        });
    } else if (std.mem.eql(u8, event_type, "response.function_call_arguments.delta")) {
        const delta = if (parsed.value.object.get("delta")) |value| if (value == .string) value.string else "" else "";
        const item_id = if (parsed.value.object.get("item_id")) |value| if (value == .string) value.string else "" else "";
        try events.append(allocator, .{
            .kind = .function_call_arguments_delta,
            .tool_id = try allocator.dupe(u8, item_id),
            .arguments_delta = try allocator.dupe(u8, delta),
            .raw_json = try allocator.dupe(u8, data),
        });
    } else if (std.mem.eql(u8, event_type, "response.completed")) {
        try events.append(allocator, .{ .kind = .completed, .raw_json = try allocator.dupe(u8, data) });
    } else if (std.mem.eql(u8, event_type, "error")) {
        try events.append(allocator, .{ .kind = .error_event, .raw_json = try allocator.dupe(u8, data) });
    }
}

fn deinitPartialEvents(allocator: std.mem.Allocator, events: []types.StreamEvent) void {
    for (events) |event| {
        if (event.text) |value| allocator.free(value);
        if (event.tool_id) |value| allocator.free(value);
        if (event.tool_name) |value| allocator.free(value);
        if (event.arguments_delta) |value| allocator.free(value);
        if (event.raw_json) |value| allocator.free(value);
    }
}

test "renderRequest emits string input and structured output format" {
    const body = try renderRequest(std.testing.allocator, "gpt", .{
        .input = .{ .text = "Return JSON." },
        .instructions = "Be terse.",
        .store = false,
        .text_format_json = "{\"type\":\"json_object\"}",
        .max_output_tokens = 128,
    });
    defer std.testing.allocator.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "\"input\":\"Return JSON.\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"store\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"text\":{\"format\":{\"type\":\"json_object\"}}") != null);
}

test "renderRequest emits message array, tools, and tool output" {
    const messages = [_]types.ResponseInputItem{
        .{ .message = .{ .role = "user", .content = "Weather?" } },
        .{ .function_call_output = .{ .call_id = "call_1", .output = "{\"ok\":true}" } },
    };
    const tools = [_]types.Tool{.{ .name = "get_weather", .parameters_json = "{\"type\":\"object\"}" }};
    const body = try renderRequest(std.testing.allocator, "gpt", .{
        .input = .{ .items = &messages },
        .tools = &tools,
        .tool_choice = .{ .named = "get_weather" },
    });
    defer std.testing.allocator.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "\"type\":\"message\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"type\":\"function_call_output\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"tools\"") != null);
}

test "parseResponse extracts output text and function call" {
    const raw = try std.testing.allocator.dupe(u8,
        \\{
        \\  "id": "resp_1",
        \\  "status": "completed",
        \\  "output": [
        \\    {"type":"message","id":"msg_1","role":"assistant","content":[{"type":"output_text","text":"hello"}]},
        \\    {"type":"function_call","id":"fc_1","call_id":"call_1","name":"lookup","arguments":"{\"q\":\"zig\"}"}
        \\  ]
        \\}
    );
    var result = try parseResponse(std.testing.allocator, raw);
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("resp_1", result.id);
    try std.testing.expectEqualStrings("hello", result.output_text);
    try std.testing.expectEqual(@as(usize, 2), result.output_items.len);
}

test "parseSseEvents normalizes response stream events" {
    const body =
        \\data: {"type":"response.created","response":{"id":"resp_1"}}
        \\
        \\data: {"type":"response.output_text.delta","delta":"Hel"}
        \\
        \\data: {"type":"response.function_call_arguments.delta","item_id":"fc_1","delta":"{\"q\""}
        \\
        \\data: {"type":"response.completed","response":{"id":"resp_1"}}
        \\
    ;
    const list = try parseSseEvents(std.testing.allocator, body);
    defer list.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 4), list.events.len);
    try std.testing.expectEqual(types.StreamEventKind.response_created, list.events[0].kind);
    try std.testing.expectEqual(types.StreamEventKind.output_text_delta, list.events[1].kind);
    try std.testing.expectEqualStrings("Hel", list.events[1].text.?);
    try std.testing.expectEqual(types.StreamEventKind.completed, list.events[3].kind);
}
