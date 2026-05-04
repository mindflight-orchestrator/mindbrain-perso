//! Compatibility wrapper for the OpenAI-compatible chat client.
//!
//! New code should prefer `llm/lib.zig`; this file preserves the existing
//! text-only API used by document profiling and older tests.

const std = @import("std");
const llm = @import("llm/lib.zig");
const chat_mod = llm.openai_compat.chat;
const endpoints = llm.openai_compat.endpoints;

pub const Provider = enum {
    openai_compatible,
};

pub const Config = struct {
    provider: Provider = .openai_compatible,
    base_url: []const u8,
    api_key: ?[]const u8 = null,
    model: []const u8,
    max_response_bytes: usize = 4 * 1024 * 1024,
};

pub const Message = llm.Message;
pub const ChatRequest = llm.ChatRequest;
pub const ChatResponse = llm.ChatResponse;

pub fn chat(
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,
    request: ChatRequest,
) !ChatResponse {
    return chat_mod.chat(allocator, io, .{
        .base_url = config.base_url,
        .api_key = config.api_key,
        .model = config.model,
        .max_response_bytes = config.max_response_bytes,
    }, request);
}

pub fn completionUrl(allocator: std.mem.Allocator, base_url: []const u8) ![]u8 {
    return endpoints.completionUrl(allocator, base_url);
}

pub fn renderOpenAiRequest(
    allocator: std.mem.Allocator,
    model: []const u8,
    request: ChatRequest,
) ![]u8 {
    return chat_mod.renderRequest(allocator, model, request);
}

pub fn parseOpenAiContent(allocator: std.mem.Allocator, raw_json: []const u8) ![]u8 {
    return chat_mod.parseContent(allocator, raw_json);
}

test "compatibility client keeps OpenAI request behavior" {
    const messages = [_]Message{
        .{ .role = "system", .content = "Return JSON." },
        .{ .role = "user", .content = "Profile this document." },
    };
    const body = try renderOpenAiRequest(std.testing.allocator, "local-model", .{
        .messages = &messages,
        .temperature = 0.1,
        .max_tokens = 512,
        .json_mode = true,
    });
    defer std.testing.allocator.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "\"model\":\"local-model\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"response_format\":{\"type\":\"json_object\"}") != null);
}
