//! Deterministic legal/business splitters.
//!
//! These are intentionally conservative. The LLM can recommend a profile, but
//! legal chunk boundaries must be reproducible and evidence-friendly.

const std = @import("std");
const chunker = @import("chunker.zig");

pub const LegalProfile = enum {
    legal_article,
    legal_consolidated,
    legal_amendment,

    pub fn label(self: LegalProfile) []const u8 {
        return switch (self) {
            .legal_article => "legal_article",
            .legal_consolidated => "legal_consolidated",
            .legal_amendment => "legal_amendment",
        };
    }
};

pub const Options = struct {
    profile: LegalProfile = .legal_article,
    max_chars: usize = 4096,
    min_chars: usize = 64,
};

const Span = struct {
    start: usize,
    end: usize,
};

pub fn chunkLegal(
    allocator: std.mem.Allocator,
    text: []const u8,
    options: Options,
) ![]chunker.Chunk {
    if (text.len == 0) return allocator.alloc(chunker.Chunk, 0);

    const article_spans = try collectArticleSpans(allocator, text);
    defer allocator.free(article_spans);

    if (article_spans.len == 0) {
        return chunker.chunk(allocator, text, .{
            .strategy = .structure_aware,
            .max_chars = options.max_chars,
            .min_chars = options.min_chars,
        });
    }

    var chunks = std.ArrayList(chunker.Chunk).empty;
    errdefer chunks.deinit(allocator);

    for (article_spans) |span| {
        try appendLegalSpan(allocator, &chunks, text, span, options);
    }

    return chunks.toOwnedSlice(allocator);
}

fn appendLegalSpan(
    allocator: std.mem.Allocator,
    chunks: *std.ArrayList(chunker.Chunk),
    text: []const u8,
    span: Span,
    options: Options,
) !void {
    const trimmed = trimSpan(text, span);
    if (trimmed.end <= trimmed.start) return;

    if (trimmed.end - trimmed.start <= options.max_chars) {
        try chunks.append(allocator, .{
            .index = @intCast(chunks.items.len),
            .content = text[trimmed.start..trimmed.end],
            .offset_start = trimmed.start,
            .offset_end = trimmed.end,
            .token_count = chunker.countTokens(text[trimmed.start..trimmed.end]),
            .strategy = options.profile.label(),
        });
        return;
    }

    // Keep the article boundary as the primary unit, but fall back within very
    // large articles so context windows stay bounded.
    const subchunks = try chunker.chunk(allocator, text[trimmed.start..trimmed.end], .{
        .strategy = .paragraph,
        .max_chars = options.max_chars,
        .min_chars = options.min_chars,
    });
    defer chunker.freeChunks(allocator, subchunks);

    for (subchunks) |sub| {
        const start = trimmed.start + sub.offset_start;
        const end = trimmed.start + sub.offset_end;
        try chunks.append(allocator, .{
            .index = @intCast(chunks.items.len),
            .content = text[start..end],
            .offset_start = start,
            .offset_end = end,
            .token_count = sub.token_count,
            .strategy = options.profile.label(),
        });
    }
}

fn collectArticleSpans(allocator: std.mem.Allocator, text: []const u8) ![]Span {
    var starts = std.ArrayList(usize).empty;
    defer starts.deinit(allocator);

    var line_start: usize = 0;
    while (line_start < text.len) {
        const line_end = lineEnd(text, line_start);
        const line = text[line_start..line_end];
        if (isArticleHeading(line)) {
            try starts.append(allocator, line_start);
        }
        line_start = if (line_end < text.len) line_end + 1 else text.len;
    }

    if (starts.items.len == 0) return allocator.alloc(Span, 0);

    var spans = try allocator.alloc(Span, starts.items.len);
    for (starts.items, 0..) |start, i| {
        const end = if (i + 1 < starts.items.len) starts.items[i + 1] else text.len;
        spans[i] = .{ .start = start, .end = end };
    }
    return spans;
}

pub fn isArticleHeading(line: []const u8) bool {
    const trimmed = trimAscii(line);
    if (trimmed.len == 0) return false;

    if (startsWithWordIgnoreCase(trimmed, "article")) return true;
    if (startsWithWordIgnoreCase(trimmed, "art.")) return true;
    if (startsWithWordIgnoreCase(trimmed, "section")) return true;
    return false;
}

fn startsWithWordIgnoreCase(text: []const u8, prefix: []const u8) bool {
    if (text.len < prefix.len) return false;
    if (!std.ascii.eqlIgnoreCase(text[0..prefix.len], prefix)) return false;
    return text.len == prefix.len or isBoundary(text[prefix.len]);
}

fn isBoundary(b: u8) bool {
    return b == ' ' or b == '\t' or b == '.' or b == ':' or b == '-';
}

fn lineEnd(text: []const u8, start: usize) usize {
    var i = start;
    while (i < text.len and text[i] != '\n') : (i += 1) {}
    return i;
}

fn trimSpan(text: []const u8, span: Span) Span {
    var start = span.start;
    var end = span.end;
    while (start < end and isWhitespace(text[start])) : (start += 1) {}
    while (end > start and isWhitespace(text[end - 1])) : (end -= 1) {}
    return .{ .start = start, .end = end };
}

fn trimAscii(text: []const u8) []const u8 {
    var start: usize = 0;
    var end: usize = text.len;
    while (start < end and isWhitespace(text[start])) : (start += 1) {}
    while (end > start and isWhitespace(text[end - 1])) : (end -= 1) {}
    return text[start..end];
}

fn isWhitespace(b: u8) bool {
    return b == ' ' or b == '\t' or b == '\n' or b == '\r';
}

test "chunkLegal splits article-shaped legal text" {
    const text =
        "Title I - Safety\n\n" ++
        "Article 1. Operators must inspect the machine before use.\n\n" ++
        "Article 2. Operators shall report incidents within 24 hours.\n\n" ++
        "Article 3. This ordinance cites Decree 2024-10.";

    const chunks = try chunkLegal(std.testing.allocator, text, .{ .profile = .legal_article });
    defer chunker.freeChunks(std.testing.allocator, chunks);

    try std.testing.expectEqual(@as(usize, 3), chunks.len);
    try std.testing.expect(std.mem.startsWith(u8, chunks[0].content, "Article 1."));
    try std.testing.expect(std.mem.startsWith(u8, chunks[1].content, "Article 2."));
    try std.testing.expect(std.mem.startsWith(u8, chunks[2].content, "Article 3."));
    for (chunks) |ch| try std.testing.expectEqualStrings("legal_article", ch.strategy);
}

test "chunkLegal falls back for non-article text" {
    const text = "This policy has no legal article markers.\n\nIt still needs deterministic chunks.";
    const chunks = try chunkLegal(std.testing.allocator, text, .{});
    defer chunker.freeChunks(std.testing.allocator, chunks);
    try std.testing.expect(chunks.len > 0);
    try std.testing.expectEqualStrings("structure_aware", chunks[0].strategy);
}
