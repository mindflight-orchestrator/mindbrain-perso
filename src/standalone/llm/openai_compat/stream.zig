const std = @import("std");
const types = @import("../types.zig");
const http_client = @import("../http_client.zig");
const endpoints = @import("endpoints.zig");
const chat_mod = @import("chat.zig");

pub const Config = chat_mod.Config;

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

pub fn streamChat(
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,
    request: types.ChatRequest,
) !EventList {
    const url = try endpoints.completionUrl(allocator, config.base_url);
    defer allocator.free(url);

    const body = try renderStreamingRequest(allocator, config.model, request);
    defer allocator.free(body);

    const response = try http_client.postJson(allocator, io, url, config.api_key, body);
    defer response.deinit(allocator);
    return parseSseEvents(allocator, response.body);
}

pub fn renderStreamingRequest(
    allocator: std.mem.Allocator,
    model: []const u8,
    request: types.ChatRequest,
) ![]u8 {
    const base = try chat_mod.renderRequest(allocator, model, request);
    defer allocator.free(base);
    if (base.len == 0 or base[base.len - 1] != '}') return error.InvalidRequest;
    return try std.fmt.allocPrint(allocator, "{s},\"stream\":true}}", .{base[0 .. base.len - 1]});
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

    const choices = parsed.value.object.get("choices") orelse return;
    if (choices.array.items.len == 0) return;
    const first = choices.array.items[0];
    const delta = first.object.get("delta") orelse return;

    if (delta.object.get("content")) |content_value| {
        if (content_value == .string and content_value.string.len > 0) {
            try events.append(allocator, .{
                .kind = .text_delta,
                .text = try allocator.dupe(u8, content_value.string),
                .raw_json = try allocator.dupe(u8, data),
            });
        }
    }

    if (delta.object.get("tool_calls")) |tool_calls| {
        if (tool_calls == .array) {
            for (tool_calls.array.items) |call| {
                const id = if (call.object.get("id")) |value| if (value == .string) value.string else "" else "";
                const function = call.object.get("function");
                const name = if (function) |fn_value| if (fn_value.object.get("name")) |value| if (value == .string) value.string else "" else "" else "";
                const args = if (function) |fn_value| if (fn_value.object.get("arguments")) |value| if (value == .string) value.string else "" else "" else "";
                try events.append(allocator, .{
                    .kind = .tool_call_delta,
                    .tool_id = try allocator.dupe(u8, id),
                    .tool_name = try allocator.dupe(u8, name),
                    .arguments_delta = try allocator.dupe(u8, args),
                    .raw_json = try allocator.dupe(u8, data),
                });
            }
        }
    }

    if (first.object.get("finish_reason")) |finish| {
        if (finish != .null) try events.append(allocator, .{ .kind = .done });
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

test "renderStreamingRequest enables stream flag" {
    const messages = [_]types.Message{.{ .role = "user", .content = "hello" }};
    const body = try renderStreamingRequest(std.testing.allocator, "model", .{ .messages = &messages });
    defer std.testing.allocator.free(body);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"stream\":true") != null);
}

test "parseSseEvents normalizes text and tool deltas" {
    const body =
        \\data: {"choices":[{"delta":{"content":"Hel"}}]}
        \\
        \\data: {"choices":[{"delta":{"tool_calls":[{"id":"call_1","function":{"name":"lookup","arguments":"{\"q\""}}]}}]}
        \\
        \\data: [DONE]
        \\
    ;
    const list = try parseSseEvents(std.testing.allocator, body);
    defer list.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 3), list.events.len);
    try std.testing.expectEqual(types.StreamEventKind.text_delta, list.events[0].kind);
    try std.testing.expectEqualStrings("Hel", list.events[0].text.?);
    try std.testing.expectEqual(types.StreamEventKind.tool_call_delta, list.events[1].kind);
    try std.testing.expectEqual(types.StreamEventKind.done, list.events[2].kind);
}
