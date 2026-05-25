const std = @import("std");
const llm = @import("llm/lib.zig");

pub const ProviderName = enum {
    openai,
    openrouter,
    anthropic,

    pub fn parse(value: []const u8) ?ProviderName {
        if (std.mem.eql(u8, value, "openai")) return .openai;
        if (std.mem.eql(u8, value, "openrouter")) return .openrouter;
        if (std.mem.eql(u8, value, "anthropic")) return .anthropic;
        return null;
    }
};

pub const Explicit = struct {
    provider: ?[]const u8 = null,
    base_url: ?[]const u8 = null,
    api_key: ?[]const u8 = null,
    model: ?[]const u8 = null,
};

pub const EnvValues = struct {
    mb_provider: ?[]const u8 = null,
    mb_base_url: ?[]const u8 = null,
    mb_api_key: ?[]const u8 = null,
    mb_model: ?[]const u8 = null,
    openai_api_key: ?[]const u8 = null,
    openrouter_base_url: ?[]const u8 = null,
    openrouter_api_key: ?[]const u8 = null,
    openrouter_chat_model: ?[]const u8 = null,
    openrouter_chat_fallback_model: ?[]const u8 = null,
    openrouter_chat_smoke_test: ?[]const u8 = null,
    anthropic_base_url: ?[]const u8 = null,
    anthropic_api_key: ?[]const u8 = null,
    anthropic_model: ?[]const u8 = null,
    anthropic_version: ?[]const u8 = null,
};

pub const LoadedEnvValues = struct {
    values: EnvValues,
    owned: [field_count]?[]u8 = [_]?[]u8{null} ** field_count,

    pub fn deinit(self: *LoadedEnvValues, allocator: std.mem.Allocator) void {
        for (&self.owned) |*item| {
            if (item.*) |value| allocator.free(value);
            item.* = null;
        }
    }
};

pub const Resolved = struct {
    provider_name: ProviderName,
    provider: llm.ProviderConfig,
};

const field_count = 14;
const Field = enum(usize) {
    mb_provider,
    mb_base_url,
    mb_api_key,
    mb_model,
    openai_api_key,
    openrouter_base_url,
    openrouter_api_key,
    openrouter_chat_model,
    openrouter_chat_fallback_model,
    openrouter_chat_smoke_test,
    anthropic_base_url,
    anthropic_api_key,
    anthropic_model,
    anthropic_version,
};

pub fn envValues(env: *const std.process.Environ.Map) EnvValues {
    return .{
        .mb_provider = env.get("MB_DOCUMENTS_LLM_PROVIDER"),
        .mb_base_url = env.get("MB_DOCUMENTS_LLM_BASE_URL"),
        .mb_api_key = env.get("MB_DOCUMENTS_LLM_API_KEY"),
        .mb_model = env.get("MB_DOCUMENTS_LLM_MODEL"),
        .openai_api_key = env.get("OPENAI_API_KEY"),
        .openrouter_base_url = env.get("OPENROUTER_BASE_URL"),
        .openrouter_api_key = env.get("OPENROUTER_API_KEY"),
        .openrouter_chat_model = env.get("OPENROUTER_CHAT_MODEL"),
        .openrouter_chat_fallback_model = env.get("OPENROUTER_CHAT_FALLBACK_MODEL"),
        .openrouter_chat_smoke_test = env.get("OPENROUTER_CHAT_SMOKE_TEST"),
        .anthropic_base_url = env.get("ANTHROPIC_BASE_URL"),
        .anthropic_api_key = env.get("ANTHROPIC_API_KEY"),
        .anthropic_model = env.get("ANTHROPIC_MODEL"),
        .anthropic_version = env.get("ANTHROPIC_VERSION"),
    };
}

pub fn loadEnvValues(allocator: std.mem.Allocator, io: std.Io, env: *const std.process.Environ.Map) !LoadedEnvValues {
    var loaded = LoadedEnvValues{ .values = envValues(env) };
    errdefer loaded.deinit(allocator);
    try loadDotEnvFile(allocator, io, &loaded, ".env");
    return loaded;
}

fn loadDotEnvFile(allocator: std.mem.Allocator, io: std.Io, loaded: *LoadedEnvValues, path: []const u8) !void {
    const content = std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(64 * 1024)) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer allocator.free(content);

    var it = std.mem.splitScalar(u8, content, '\n');
    while (it.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r\n");
        if (line.len == 0 or line[0] == '#') continue;
        const eq = std.mem.indexOfScalar(u8, line, '=') orelse continue;
        const key = std.mem.trim(u8, line[0..eq], " \t");
        const raw_value = std.mem.trim(u8, line[eq + 1 ..], " \t\r\n");
        const value = unquote(raw_value);
        if (fieldForKey(key)) |field| {
            try setDotEnvValue(allocator, loaded, field, value);
        }
    }
}

fn unquote(value: []const u8) []const u8 {
    if (value.len >= 2 and ((value[0] == '"' and value[value.len - 1] == '"') or (value[0] == '\'' and value[value.len - 1] == '\''))) {
        return value[1 .. value.len - 1];
    }
    return value;
}

fn fieldForKey(key: []const u8) ?Field {
    if (std.mem.eql(u8, key, "MB_DOCUMENTS_LLM_PROVIDER")) return .mb_provider;
    if (std.mem.eql(u8, key, "MB_DOCUMENTS_LLM_BASE_URL")) return .mb_base_url;
    if (std.mem.eql(u8, key, "MB_DOCUMENTS_LLM_API_KEY")) return .mb_api_key;
    if (std.mem.eql(u8, key, "MB_DOCUMENTS_LLM_MODEL")) return .mb_model;
    if (std.mem.eql(u8, key, "OPENAI_API_KEY")) return .openai_api_key;
    if (std.mem.eql(u8, key, "OPENROUTER_BASE_URL")) return .openrouter_base_url;
    if (std.mem.eql(u8, key, "OPENROUTER_API_KEY")) return .openrouter_api_key;
    if (std.mem.eql(u8, key, "OPENROUTER_CHAT_MODEL")) return .openrouter_chat_model;
    if (std.mem.eql(u8, key, "OPENROUTER_CHAT_FALLBACK_MODEL")) return .openrouter_chat_fallback_model;
    if (std.mem.eql(u8, key, "OPENROUTER_CHAT_SMOKE_TEST")) return .openrouter_chat_smoke_test;
    if (std.mem.eql(u8, key, "ANTHROPIC_BASE_URL")) return .anthropic_base_url;
    if (std.mem.eql(u8, key, "ANTHROPIC_API_KEY")) return .anthropic_api_key;
    if (std.mem.eql(u8, key, "ANTHROPIC_MODEL")) return .anthropic_model;
    if (std.mem.eql(u8, key, "ANTHROPIC_VERSION")) return .anthropic_version;
    return null;
}

fn setDotEnvValue(allocator: std.mem.Allocator, loaded: *LoadedEnvValues, field: Field, value: []const u8) !void {
    const slot = @intFromEnum(field);
    const current = valueForField(loaded.values, field);
    if (current != null and loaded.owned[slot] == null) return;

    if (loaded.owned[slot]) |owned| allocator.free(owned);
    loaded.owned[slot] = try allocator.dupe(u8, value);
    setField(&loaded.values, field, loaded.owned[slot].?);
}

fn valueForField(values: EnvValues, field: Field) ?[]const u8 {
    return switch (field) {
        .mb_provider => values.mb_provider,
        .mb_base_url => values.mb_base_url,
        .mb_api_key => values.mb_api_key,
        .mb_model => values.mb_model,
        .openai_api_key => values.openai_api_key,
        .openrouter_base_url => values.openrouter_base_url,
        .openrouter_api_key => values.openrouter_api_key,
        .openrouter_chat_model => values.openrouter_chat_model,
        .openrouter_chat_fallback_model => values.openrouter_chat_fallback_model,
        .openrouter_chat_smoke_test => values.openrouter_chat_smoke_test,
        .anthropic_base_url => values.anthropic_base_url,
        .anthropic_api_key => values.anthropic_api_key,
        .anthropic_model => values.anthropic_model,
        .anthropic_version => values.anthropic_version,
    };
}

fn setField(values: *EnvValues, field: Field, value: []const u8) void {
    switch (field) {
        .mb_provider => values.mb_provider = value,
        .mb_base_url => values.mb_base_url = value,
        .mb_api_key => values.mb_api_key = value,
        .mb_model => values.mb_model = value,
        .openai_api_key => values.openai_api_key = value,
        .openrouter_base_url => values.openrouter_base_url = value,
        .openrouter_api_key => values.openrouter_api_key = value,
        .openrouter_chat_model => values.openrouter_chat_model = value,
        .openrouter_chat_fallback_model => values.openrouter_chat_fallback_model = value,
        .openrouter_chat_smoke_test => values.openrouter_chat_smoke_test = value,
        .anthropic_base_url => values.anthropic_base_url = value,
        .anthropic_api_key => values.anthropic_api_key = value,
        .anthropic_model => values.anthropic_model = value,
        .anthropic_version => values.anthropic_version = value,
    }
}

pub fn resolve(explicit: Explicit, env: EnvValues) !Resolved {
    const name = if (explicit.provider) |provider|
        ProviderName.parse(provider) orelse return error.InvalidProvider
    else if (env.mb_provider) |provider|
        ProviderName.parse(provider) orelse return error.InvalidProvider
    else
        ProviderName.openai;

    return switch (name) {
        .openai => .{
            .provider_name = name,
            .provider = .{
                .name = "default",
                .kind = .openai,
                .base_url = explicit.base_url orelse env.mb_base_url orelse "https://api.openai.com/v1",
                .api_key = explicit.api_key orelse env.mb_api_key orelse env.openai_api_key,
                .model = explicit.model orelse env.mb_model orelse "gpt-4.1-mini",
            },
        },
        .openrouter => .{
            .provider_name = name,
            .provider = .{
                .name = "default",
                .kind = .openrouter,
                .base_url = explicit.base_url orelse env.openrouter_base_url orelse "https://openrouter.ai/api/v1",
                .api_key = explicit.api_key orelse env.openrouter_api_key orelse env.mb_api_key,
                .model = explicit.model orelse env.openrouter_chat_model orelse env.openrouter_chat_fallback_model orelse env.openrouter_chat_smoke_test orelse env.mb_model orelse "qwen/qwen3-coder-next",
            },
        },
        .anthropic => .{
            .provider_name = name,
            .provider = .{
                .name = "default",
                .kind = .anthropic,
                .base_url = explicit.base_url orelse env.anthropic_base_url orelse "https://api.anthropic.com/v1",
                .api_key = explicit.api_key orelse env.anthropic_api_key orelse env.mb_api_key,
                .model = explicit.model orelse env.anthropic_model orelse env.mb_model orelse "claude-haiku-4-5-20251001",
                .anthropic_version = env.anthropic_version orelse "2023-06-01",
            },
        },
    };
}

test "resolve defaults to OpenAI-compatible document settings" {
    const resolved = try resolve(.{}, .{});
    try std.testing.expectEqual(ProviderName.openai, resolved.provider_name);
    try std.testing.expectEqual(llm.ProviderKind.openai, resolved.provider.kind);
    try std.testing.expectEqualStrings("https://api.openai.com/v1", resolved.provider.base_url);
    try std.testing.expectEqualStrings("gpt-4.1-mini", resolved.provider.model);
}

test "resolve OpenRouter uses provider-specific values before generic model" {
    const resolved = try resolve(.{ .provider = "openrouter" }, .{
        .mb_model = "generic",
        .openrouter_api_key = "or-key",
        .openrouter_chat_model = "qwen/qwen3-coder-next",
    });
    try std.testing.expectEqual(ProviderName.openrouter, resolved.provider_name);
    try std.testing.expectEqual(llm.ProviderKind.openrouter, resolved.provider.kind);
    try std.testing.expectEqualStrings("https://openrouter.ai/api/v1", resolved.provider.base_url);
    try std.testing.expectEqualStrings("qwen/qwen3-coder-next", resolved.provider.model);
    try std.testing.expectEqualStrings("or-key", resolved.provider.api_key.?);
}

test "resolve Anthropic uses native defaults and version" {
    const resolved = try resolve(.{ .provider = "anthropic" }, .{
        .anthropic_api_key = "ant-key",
        .anthropic_model = "claude-haiku-4-5-20251001",
    });
    try std.testing.expectEqual(ProviderName.anthropic, resolved.provider_name);
    try std.testing.expectEqual(llm.ProviderKind.anthropic, resolved.provider.kind);
    try std.testing.expectEqualStrings("https://api.anthropic.com/v1", resolved.provider.base_url);
    try std.testing.expectEqualStrings("2023-06-01", resolved.provider.anthropic_version.?);
}

test "dotenv values fill missing env values and duplicate keys use last value" {
    var loaded = LoadedEnvValues{ .values = .{} };
    defer loaded.deinit(std.testing.allocator);
    try setDotEnvValue(std.testing.allocator, &loaded, .anthropic_model, "old");
    try setDotEnvValue(std.testing.allocator, &loaded, .anthropic_model, "new");
    try std.testing.expectEqualStrings("new", loaded.values.anthropic_model.?);
}
