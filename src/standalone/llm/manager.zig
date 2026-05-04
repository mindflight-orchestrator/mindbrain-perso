const std = @import("std");
const types = @import("types.zig");
const openai_chat = @import("openai_compat/chat.zig");
const openai_stream = @import("openai_compat/stream.zig");
const openai_responses = @import("openai_compat/responses.zig");
const openai_embeddings = @import("openai_compat/embeddings.zig");
const openai_audio = @import("openai_compat/audio.zig");
const gemini = @import("gemini/client.zig");

pub const Manager = struct {
    config: types.ManagerConfig,

    pub fn init(config: types.ManagerConfig) Manager {
        return .{ .config = config };
    }

    pub fn getProvider(self: Manager, name_opt: ?[]const u8) ?types.ProviderConfig {
        const name = name_opt orelse self.config.default_provider;
        for (self.config.providers) |provider| {
            if (std.mem.eql(u8, provider.name, name)) return provider;
        }
        return null;
    }

    pub fn chat(
        self: Manager,
        allocator: std.mem.Allocator,
        io: std.Io,
        messages: []const types.Message,
        options: types.ChatOptions,
    ) !types.ChatResponse {
        const provider = self.getProvider(options.provider) orelse return error.ProviderNotFound;
        if (!supports(provider, .chat)) return error.UnsupportedCapability;
        const request = types.ChatRequest{
            .messages = messages,
            .temperature = options.temperature,
            .max_tokens = options.max_tokens,
            .json_mode = options.json_mode,
            .tools = options.tools,
            .tool_choice = options.tool_choice,
        };
        return switch (provider.kind) {
            .gemini => gemini.chat(allocator, io, .{
                .base_url = provider.base_url,
                .api_key = provider.api_key,
                .model = provider.model,
            }, request),
            else => openai_chat.chat(allocator, io, .{
                .base_url = provider.base_url,
                .api_key = provider.api_key,
                .model = provider.model,
            }, request),
        };
    }

    pub fn streamChat(
        self: Manager,
        allocator: std.mem.Allocator,
        io: std.Io,
        messages: []const types.Message,
        options: types.ChatOptions,
    ) !openai_stream.EventList {
        const provider = self.getProvider(options.provider) orelse return error.ProviderNotFound;
        if (!supports(provider, .streaming)) return error.UnsupportedCapability;
        if (provider.kind == .gemini) return error.UnsupportedProvider;
        return openai_stream.streamChat(allocator, io, .{
            .base_url = provider.base_url,
            .api_key = provider.api_key,
            .model = provider.model,
        }, .{
            .messages = messages,
            .temperature = options.temperature,
            .max_tokens = options.max_tokens,
            .json_mode = options.json_mode,
            .tools = options.tools,
            .tool_choice = options.tool_choice,
        });
    }

    pub fn respond(
        self: Manager,
        allocator: std.mem.Allocator,
        io: std.Io,
        request: types.ResponseRequest,
        options: types.ResponseOptions,
    ) !types.ResponseResult {
        const provider = self.getProvider(options.provider) orelse return error.ProviderNotFound;
        if (!supports(provider, .responses)) return error.UnsupportedCapability;
        return switch (provider.kind) {
            .gemini => gemini.respond(allocator, io, .{
                .base_url = provider.base_url,
                .api_key = provider.api_key,
                .model = provider.model,
            }, request),
            else => openai_responses.respond(allocator, io, .{
                .base_url = provider.base_url,
                .api_key = provider.api_key,
                .model = provider.model,
            }, request),
        };
    }

    pub fn streamRespond(
        self: Manager,
        allocator: std.mem.Allocator,
        io: std.Io,
        request: types.ResponseRequest,
        options: types.ResponseOptions,
    ) !openai_responses.EventList {
        const provider = self.getProvider(options.provider) orelse return error.ProviderNotFound;
        if (!supports(provider, .responses) or !supports(provider, .streaming)) return error.UnsupportedCapability;
        if (provider.kind == .gemini) return error.UnsupportedProvider;
        return openai_responses.streamRespond(allocator, io, .{
            .base_url = provider.base_url,
            .api_key = provider.api_key,
            .model = provider.model,
        }, request);
    }

    pub fn embedTexts(
        self: Manager,
        allocator: std.mem.Allocator,
        io: std.Io,
        provider_name: ?[]const u8,
        inputs: []const []const u8,
    ) !types.EmbeddingResponse {
        const provider = self.getProvider(provider_name) orelse return error.ProviderNotFound;
        if (!supports(provider, .embeddings)) return error.UnsupportedCapability;
        const model = provider.embedding_model orelse provider.model;
        return switch (provider.kind) {
            .gemini => gemini.embedTexts(allocator, io, .{
                .base_url = provider.base_url,
                .api_key = provider.api_key,
                .model = model,
            }, inputs),
            else => openai_embeddings.embedTexts(allocator, io, .{
                .base_url = provider.base_url,
                .api_key = provider.api_key,
                .model = model,
            }, inputs),
        };
    }

    pub fn transcribe(
        self: Manager,
        allocator: std.mem.Allocator,
        io: std.Io,
        provider_name: ?[]const u8,
        request: types.AudioTranscriptionRequest,
    ) !types.AudioTranscriptionResponse {
        const provider = self.getProvider(provider_name) orelse return error.ProviderNotFound;
        if (!supports(provider, .audio)) return error.UnsupportedCapability;
        if (provider.kind == .gemini) return error.UnsupportedProvider;
        const effective = types.AudioTranscriptionRequest{
            .model = provider.audio_model orelse request.model,
            .filename = request.filename,
            .mime_type = request.mime_type,
            .audio_bytes = request.audio_bytes,
            .language = request.language,
            .prompt = request.prompt,
            .response_format = request.response_format,
        };
        return openai_audio.transcribe(allocator, io, .{
            .base_url = provider.base_url,
            .api_key = provider.api_key,
            .model = effective.model,
        }, effective);
    }
};

pub fn supports(provider: types.ProviderConfig, capability: types.Capability) bool {
    for (provider.capabilities) |item| {
        if (item == capability) return true;
    }
    return false;
}

pub fn sanitizeToolName(
    allocator: std.mem.Allocator,
    name: []const u8,
) ![]u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    for (name) |byte| {
        if (byte == '.') {
            try out.appendSlice(allocator, "__");
        } else {
            try out.append(allocator, byte);
        }
    }
    return out.toOwnedSlice(allocator);
}

pub fn restoreToolName(
    allocator: std.mem.Allocator,
    name: []const u8,
) ![]u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    var i: usize = 0;
    while (i < name.len) {
        if (i + 1 < name.len and name[i] == '_' and name[i + 1] == '_') {
            try out.append(allocator, '.');
            i += 2;
        } else {
            try out.append(allocator, name[i]);
            i += 1;
        }
    }
    return out.toOwnedSlice(allocator);
}

pub fn generateCacheKey(
    allocator: std.mem.Allocator,
    provider_name: []const u8,
    model: []const u8,
    messages: []const types.Message,
    options: types.ChatOptions,
) ![]u8 {
    var rendered = std.Io.Writer.Allocating.init(allocator);
    defer rendered.deinit();

    try rendered.writer.writeAll("{\"provider\":");
    try rendered.writer.print("{f}", .{std.json.fmt(provider_name, .{})});
    try rendered.writer.writeAll(",\"model\":");
    try rendered.writer.print("{f}", .{std.json.fmt(model, .{})});
    try rendered.writer.writeAll(",\"request\":");

    const request_body = try openai_chat.renderRequest(allocator, model, .{
        .messages = messages,
        .temperature = options.temperature,
        .max_tokens = options.max_tokens,
        .json_mode = options.json_mode,
        .tools = options.tools,
        .tool_choice = options.tool_choice,
    });
    defer allocator.free(request_body);
    try rendered.writer.writeAll(request_body);
    try rendered.writer.writeByte('}');

    const input = rendered.written();
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(input, &digest, .{});
    return hexDigest(allocator, &digest);
}

fn hexDigest(allocator: std.mem.Allocator, bytes: []const u8) ![]u8 {
    const chars = "0123456789abcdef";
    const out = try allocator.alloc(u8, bytes.len * 2);
    for (bytes, 0..) |byte, i| {
        out[i * 2] = chars[byte >> 4];
        out[i * 2 + 1] = chars[byte & 0x0f];
    }
    return out;
}

test "manager resolves default and named providers" {
    const providers = [_]types.ProviderConfig{
        .{ .name = "local", .kind = .ollama, .base_url = "http://localhost:11434", .model = "llama" },
        .{ .name = "remote", .base_url = "https://api.example.test", .model = "model" },
    };
    const manager = Manager.init(.{
        .providers = &providers,
        .default_provider = "local",
    });

    try std.testing.expectEqualStrings("local", manager.getProvider(null).?.name);
    try std.testing.expectEqualStrings("remote", manager.getProvider("remote").?.name);
    try std.testing.expect(manager.getProvider("missing") == null);
}

test "tool name sanitize and restore mirrors Go reference behavior" {
    const sanitized = try sanitizeToolName(std.testing.allocator, "provider.search.web");
    defer std.testing.allocator.free(sanitized);
    try std.testing.expectEqualStrings("provider__search__web", sanitized);

    const restored = try restoreToolName(std.testing.allocator, sanitized);
    defer std.testing.allocator.free(restored);
    try std.testing.expectEqualStrings("provider.search.web", restored);
}

test "generateCacheKey is stable for identical requests" {
    const messages = [_]types.Message{
        .{ .role = "system", .content = "Return JSON." },
        .{ .role = "user", .content = "Profile this." },
    };
    const options = types.ChatOptions{ .temperature = 0.0, .max_tokens = 128, .json_mode = true };

    const a = try generateCacheKey(std.testing.allocator, "local", "model-a", &messages, options);
    defer std.testing.allocator.free(a);
    const b = try generateCacheKey(std.testing.allocator, "local", "model-a", &messages, options);
    defer std.testing.allocator.free(b);

    try std.testing.expectEqualStrings(a, b);
    try std.testing.expectEqual(@as(usize, 64), a.len);
}
