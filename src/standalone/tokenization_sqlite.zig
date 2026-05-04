const std = @import("std");

const ENGLISH_STOP_WORDS = [_][]const u8{
    "a",    "an",   "and",  "are", "as",  "at",   "be",   "by",   "for", "from",
    "has",  "have", "he",   "in",  "is",  "it",   "its",  "of",   "on",  "or",
    "that", "the",  "this", "to",  "was", "were", "will", "with",
};

pub fn tokenizePure(text: []const u8, allocator: std.mem.Allocator) !std.ArrayList([]const u8) {
    var tokens = std.ArrayList([]const u8).empty;
    errdefer {
        for (tokens.items) |token| allocator.free(token);
        tokens.deinit(allocator);
    }

    var i: usize = 0;
    while (i < text.len) {
        while (i < text.len and !isWordChar(text[i])) : (i += 1) {}
        if (i >= text.len) break;

        const start = i;
        while (i < text.len and isWordChar(text[i])) : (i += 1) {}
        const word = text[start..i];
        if (word.len == 0 or word.len > 100) continue;

        const lower = try allocator.alloc(u8, word.len);
        for (word, 0..) |char, index| {
            lower[index] = std.ascii.toLower(char);
        }

        if (lower.len <= 1 or isStopWord(lower)) {
            allocator.free(lower);
            continue;
        }

        try tokens.append(allocator, lower);
    }

    return tokens;
}

/// Wyhash of lowercased UTF-8 lexeme bytes (same algorithm as incremental BM25 token hashing).
pub fn hashTermLexeme(input: []const u8) u64 {
    var hasher = std.hash.Wyhash.init(0);
    var buf: [256]u8 = undefined;
    var offset: usize = 0;
    while (offset < input.len) {
        const chunk_len = @min(buf.len, input.len - offset);
        for (input[offset..][0..chunk_len], 0..) |char, index| {
            buf[index] = std.ascii.toLower(char);
        }
        hasher.update(buf[0..chunk_len]);
        offset += chunk_len;
    }
    return hasher.final();
}

pub fn hashLexeme(lexeme: []const u8) u64 {
    return hashTermLexeme(lexeme);
}

fn isSearchWordChar(char: u8) bool {
    return std.ascii.isAlphanumeric(char) or char >= 0x80;
}

/// BM25 / SQLite search tokenizer: word boundaries match `search_sqlite` / `search_store`.
/// When `stopwords` is non-null, skips tokens whose `hashTermLexeme` is in the set.
pub fn tokenizeSearchHashes(
    allocator: std.mem.Allocator,
    text: []const u8,
    stopwords: ?*const std.AutoHashMap(u64, void),
) ![]u64 {
    var tokens = std.ArrayList(u64).empty;
    defer tokens.deinit(allocator);

    var i: usize = 0;
    while (i < text.len) {
        while (i < text.len and !isSearchWordChar(text[i])) : (i += 1) {}
        if (i >= text.len) break;
        const start = i;
        while (i < text.len and isSearchWordChar(text[i])) : (i += 1) {}
        const word = text[start..i];
        if (word.len == 0) continue;
        const h = hashTermLexeme(word);
        if (stopwords) |sw| {
            if (sw.contains(h)) continue;
        }
        try tokens.append(allocator, h);
    }
    return tokens.toOwnedSlice(allocator);
}

pub fn tokenizeToHashes(text: []const u8, allocator: std.mem.Allocator) !std.ArrayList(u64) {
    var tokens = try tokenizePure(text, allocator);
    defer {
        for (tokens.items) |token| allocator.free(token);
        tokens.deinit(allocator);
    }

    var hashes = std.ArrayList(u64).empty;
    errdefer hashes.deinit(allocator);

    for (tokens.items) |token| {
        try hashes.append(allocator, hashLexeme(token));
    }

    return hashes;
}

fn isStopWord(word: []const u8) bool {
    for (ENGLISH_STOP_WORDS) |stop| {
        if (std.mem.eql(u8, word, stop)) return true;
    }
    return false;
}

fn isWordChar(char: u8) bool {
    return std.ascii.isAlphanumeric(char) or char >= 0x80;
}

test "tokenizePure basic" {
    var tokens = try tokenizePure("Hello World", std.testing.allocator);
    defer {
        for (tokens.items) |token| std.testing.allocator.free(token);
        tokens.deinit(std.testing.allocator);
    }

    try std.testing.expectEqual(@as(usize, 2), tokens.items.len);
    try std.testing.expectEqualStrings("hello", tokens.items[0]);
    try std.testing.expectEqualStrings("world", tokens.items[1]);
}

test "tokenizePure filters stop words" {
    var tokens = try tokenizePure("the quick brown fox", std.testing.allocator);
    defer {
        for (tokens.items) |token| std.testing.allocator.free(token);
        tokens.deinit(std.testing.allocator);
    }

    try std.testing.expectEqual(@as(usize, 3), tokens.items.len);
    try std.testing.expectEqualStrings("quick", tokens.items[0]);
    try std.testing.expectEqualStrings("brown", tokens.items[1]);
    try std.testing.expectEqualStrings("fox", tokens.items[2]);
}

test "tokenizeToHashes preserves order and hash stability" {
    var hashes = try tokenizeToHashes("laptop computer", std.testing.allocator);
    defer hashes.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), hashes.items.len);
    try std.testing.expectEqual(hashLexeme("laptop"), hashes.items[0]);
    try std.testing.expectEqual(hashLexeme("computer"), hashes.items[1]);
}

test "tokenizeSearchHashes skips tokens present in stopword set" {
    var sw = std.AutoHashMap(u64, void).init(std.testing.allocator);
    defer sw.deinit();
    try sw.put(hashTermLexeme("the"), {});

    const tokens = try tokenizeSearchHashes(std.testing.allocator, "the fox", &sw);
    defer std.testing.allocator.free(tokens);

    try std.testing.expectEqual(@as(usize, 1), tokens.len);
    try std.testing.expectEqual(hashTermLexeme("fox"), tokens[0]);
}
