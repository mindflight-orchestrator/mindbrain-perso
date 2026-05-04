//! Deterministic reference extraction for corpus evaluation and import.
//!
//! This is not meant to replace LLM extraction. It gives the LLM a stable
//! baseline and captures obvious legal/internal/external references.

const std = @import("std");

pub const ReferenceKind = enum {
    internal,
    external,
    url,
};

pub const Reference = struct {
    kind: ReferenceKind,
    text: []const u8,
    offset_start: usize,
    offset_end: usize,
    resolved: bool = false,
};

pub fn extract(
    allocator: std.mem.Allocator,
    text: []const u8,
) ![]Reference {
    var refs = std.ArrayList(Reference).empty;
    errdefer refs.deinit(allocator);

    try extractUrls(allocator, text, &refs);
    try extractArticleRefs(allocator, text, &refs);
    try extractInstrumentRefs(allocator, text, &refs);

    return refs.toOwnedSlice(allocator);
}

pub fn freeReferences(allocator: std.mem.Allocator, refs: []Reference) void {
    allocator.free(refs);
}

fn extractUrls(
    allocator: std.mem.Allocator,
    text: []const u8,
    refs: *std.ArrayList(Reference),
) !void {
    var cursor: usize = 0;
    while (cursor < text.len) {
        const http_pos = std.mem.indexOfPos(u8, text, cursor, "http://");
        const https_pos = std.mem.indexOfPos(u8, text, cursor, "https://");
        const pos = minOptional(http_pos, https_pos) orelse break;
        var end = pos;
        while (end < text.len and !isReferenceTerminator(text[end])) : (end += 1) {}
        try appendUnique(allocator, refs, .{
            .kind = .url,
            .text = text[pos..end],
            .offset_start = pos,
            .offset_end = end,
        });
        cursor = end;
    }
}

fn extractArticleRefs(
    allocator: std.mem.Allocator,
    text: []const u8,
    refs: *std.ArrayList(Reference),
) !void {
    var cursor: usize = 0;
    while (cursor < text.len) {
        const pos = indexOfWordIgnoreCase(text, cursor, "article") orelse break;
        const end = referenceEnd(text, pos + "article".len);
        const ref_text = trimAscii(text[pos..end]);
        if (ref_text.len > "article".len) {
            try appendUnique(allocator, refs, .{
                .kind = .internal,
                .text = ref_text,
                .offset_start = pos,
                .offset_end = pos + ref_text.len,
            });
        }
        cursor = @max(end, pos + 1);
    }
}

fn extractInstrumentRefs(
    allocator: std.mem.Allocator,
    text: []const u8,
    refs: *std.ArrayList(Reference),
) !void {
    const prefixes = [_][]const u8{ "decree", "law", "ordinance", "regulation" };
    for (prefixes) |prefix| {
        var cursor: usize = 0;
        while (cursor < text.len) {
            const pos = indexOfWordIgnoreCase(text, cursor, prefix) orelse break;
            const end = referenceEnd(text, pos + prefix.len);
            const ref_text = trimAscii(text[pos..end]);
            if (ref_text.len > prefix.len) {
                try appendUnique(allocator, refs, .{
                    .kind = .external,
                    .text = ref_text,
                    .offset_start = pos,
                    .offset_end = pos + ref_text.len,
                });
            }
            cursor = @max(end, pos + 1);
        }
    }
}

fn appendUnique(
    allocator: std.mem.Allocator,
    refs: *std.ArrayList(Reference),
    ref_value: Reference,
) !void {
    for (refs.items) |existing| {
        if (existing.kind == ref_value.kind and
            existing.offset_start == ref_value.offset_start and
            existing.offset_end == ref_value.offset_end)
        {
            return;
        }
    }
    try refs.append(allocator, ref_value);
}

fn minOptional(a: ?usize, b: ?usize) ?usize {
    if (a) |av| {
        if (b) |bv| return @min(av, bv);
        return av;
    }
    return b;
}

fn indexOfWordIgnoreCase(haystack: []const u8, start: usize, needle: []const u8) ?usize {
    var cursor = start;
    while (cursor + needle.len <= haystack.len) : (cursor += 1) {
        if (!std.ascii.eqlIgnoreCase(haystack[cursor .. cursor + needle.len], needle)) continue;
        const before_ok = cursor == 0 or !isWordByte(haystack[cursor - 1]);
        const after = cursor + needle.len;
        const after_ok = after >= haystack.len or !isWordByte(haystack[after]);
        if (before_ok and after_ok) return cursor;
    }
    return null;
}

fn isReferenceBodyByte(b: u8) bool {
    return std.ascii.isAlphanumeric(b) or b == ' ' or b == '-' or b == '_' or b == '.';
}

fn referenceEnd(text: []const u8, after_prefix: usize) usize {
    var end = after_prefix;
    while (end < text.len and (text[end] == ' ' or text[end] == '\t')) : (end += 1) {}
    while (end < text.len and isReferenceCodeByte(text[end])) : (end += 1) {}

    if (startsWithIgnoreCase(text[end..], " of ")) {
        end += " of ".len;
        while (end < text.len and isReferenceBodyByte(text[end])) {
            if (startsWithIgnoreCase(text[end..], " and ")) break;
            end += 1;
        }
    }

    return end;
}

fn startsWithIgnoreCase(text: []const u8, prefix: []const u8) bool {
    return text.len >= prefix.len and std.ascii.eqlIgnoreCase(text[0..prefix.len], prefix);
}

fn isReferenceCodeByte(b: u8) bool {
    return std.ascii.isAlphanumeric(b) or b == '-' or b == '_' or b == '.';
}

fn isReferenceTerminator(b: u8) bool {
    return b == ' ' or b == '\n' or b == '\r' or b == '\t' or b == ')' or b == ']';
}

fn isWordByte(b: u8) bool {
    return std.ascii.isAlphanumeric(b) or b == '_';
}

fn trimAscii(text: []const u8) []const u8 {
    var start: usize = 0;
    var end: usize = text.len;
    while (start < end and isTrimByte(text[start])) : (start += 1) {}
    while (end > start and isTrimByte(text[end - 1])) : (end -= 1) {}
    return text[start..end];
}

fn isTrimByte(b: u8) bool {
    return b == ' ' or b == '\t' or b == '\n' or b == '\r' or b == '.' or b == ',' or b == ';' or b == ':';
}

fn countKind(refs: []const Reference, kind: ReferenceKind) usize {
    var count: usize = 0;
    for (refs) |ref_value| {
        if (ref_value.kind == kind) count += 1;
    }
    return count;
}

test "extract finds internal and external legal references" {
    const text = "Article 3 cites Decree 2024-10 and Article 2 of this ordinance.";
    const refs = try extract(std.testing.allocator, text);
    defer freeReferences(std.testing.allocator, refs);

    try std.testing.expectEqual(@as(usize, 2), countKind(refs, .internal));
    try std.testing.expectEqual(@as(usize, 1), countKind(refs, .external));
}

test "extract finds urls separately from legal instruments" {
    const text = "See https://example.test/spec and Law 2026-1.";
    const refs = try extract(std.testing.allocator, text);
    defer freeReferences(std.testing.allocator, refs);

    try std.testing.expectEqual(@as(usize, 1), countKind(refs, .url));
    try std.testing.expectEqual(@as(usize, 1), countKind(refs, .external));
}
