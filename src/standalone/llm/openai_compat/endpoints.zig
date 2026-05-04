const std = @import("std");
const http_client = @import("../http_client.zig");

pub fn completionUrl(allocator: std.mem.Allocator, base_url: []const u8) ![]u8 {
    return pathUrl(allocator, base_url, "/chat/completions");
}

pub fn responsesUrl(allocator: std.mem.Allocator, base_url: []const u8) ![]u8 {
    return pathUrl(allocator, base_url, "/responses");
}

pub fn embeddingsUrl(allocator: std.mem.Allocator, base_url: []const u8) ![]u8 {
    return pathUrl(allocator, base_url, "/embeddings");
}

pub fn audioTranscriptionsUrl(allocator: std.mem.Allocator, base_url: []const u8) ![]u8 {
    return pathUrl(allocator, base_url, "/audio/transcriptions");
}

fn pathUrl(allocator: std.mem.Allocator, base_url: []const u8, path: []const u8) ![]u8 {
    const trimmed = http_client.trimRight(base_url, '/');
    if (std.mem.endsWith(u8, trimmed, path)) {
        return try allocator.dupe(u8, trimmed);
    }
    if (std.mem.endsWith(u8, trimmed, "/v1")) {
        return try std.fmt.allocPrint(allocator, "{s}{s}", .{ trimmed, path });
    }
    return try std.fmt.allocPrint(allocator, "{s}/v1{s}", .{ trimmed, path });
}

test "OpenAI-compatible endpoints normalize base URLs" {
    const allocator = std.testing.allocator;
    const a = try completionUrl(allocator, "http://localhost:11434");
    defer allocator.free(a);
    try std.testing.expectEqualStrings("http://localhost:11434/v1/chat/completions", a);

    const b = try embeddingsUrl(allocator, "http://localhost:11434/v1/");
    defer allocator.free(b);
    try std.testing.expectEqualStrings("http://localhost:11434/v1/embeddings", b);

    const c = try responsesUrl(allocator, "https://openrouter.ai/api/v1");
    defer allocator.free(c);
    try std.testing.expectEqualStrings("https://openrouter.ai/api/v1/responses", c);
}
