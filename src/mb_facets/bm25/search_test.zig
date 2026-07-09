const std = @import("std");
const tokenizer_pure = @import("tokenizer_pure.zig");

// Tests for pure Zig components that don't require PostgreSQL

test "tokenizePure - basic tokenization" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tokens = try tokenizer_pure.tokenizePure("Hello World", allocator);
    defer {
        for (tokens.items) |token| {
            allocator.free(token);
        }
        tokens.deinit(allocator);
    }

    try std.testing.expectEqual(@as(usize, 2), tokens.items.len);
    try std.testing.expectEqualStrings("hello", tokens.items[0]);
    try std.testing.expectEqualStrings("world", tokens.items[1]);
}

test "tokenizePure - stop words filtered" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tokens = try tokenizer_pure.tokenizePure("the quick brown fox", allocator);
    defer {
        for (tokens.items) |token| {
            allocator.free(token);
        }
        tokens.deinit(allocator);
    }

    // "the" is a stop word, should be filtered
    try std.testing.expectEqual(@as(usize, 3), tokens.items.len);
    try std.testing.expectEqualStrings("quick", tokens.items[0]);
    try std.testing.expectEqualStrings("brown", tokens.items[1]);
    try std.testing.expectEqualStrings("fox", tokens.items[2]);
}

test "tokenizePure - laptop query" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tokens = try tokenizer_pure.tokenizePure("laptop", allocator);
    defer {
        for (tokens.items) |token| {
            allocator.free(token);
        }
        tokens.deinit(allocator);
    }

    try std.testing.expectEqual(@as(usize, 1), tokens.items.len);
    try std.testing.expectEqualStrings("laptop", tokens.items[0]);
}

test "hashLexeme - consistency" {
    const hash1 = tokenizer_pure.hashLexeme("laptop");
    const hash2 = tokenizer_pure.hashLexeme("laptop");
    try std.testing.expectEqual(hash1, hash2);

    // Different words should have different hashes
    const hash3 = tokenizer_pure.hashLexeme("computer");
    try std.testing.expect(hash1 != hash3);
}

test "hashLexeme - known value" {
    // This hash should match what tokenizer_native produces
    const hash = tokenizer_pure.hashLexeme("laptop");
    // The hash should be a stable value
    try std.testing.expect(hash != 0);
}

test "tokenizeToHashes - basic" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var hashes = try tokenizer_pure.tokenizeToHashes("laptop computer", allocator);
    defer hashes.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), hashes.items.len);
    try std.testing.expectEqual(tokenizer_pure.hashLexeme("laptop"), hashes.items[0]);
    try std.testing.expectEqual(tokenizer_pure.hashLexeme("computer"), hashes.items[1]);
}

test "ArrayList returned from function - memory safety" {
    // This test verifies that an ArrayList created in a function
    // and returned to the caller maintains its data
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var results = createTestResults(allocator);
    defer results.deinit(allocator);

    // Verify the results are intact after function return
    try std.testing.expectEqual(@as(usize, 3), results.items.len);
    try std.testing.expectEqual(@as(i64, 1), results.items[0].doc_id);
    try std.testing.expectEqual(@as(i64, 2), results.items[1].doc_id);
    try std.testing.expectEqual(@as(i64, 3), results.items[2].doc_id);

    // Check scores are reasonable
    try std.testing.expect(results.items[0].score > 0.0);
    try std.testing.expect(results.items[1].score > 0.0);
    try std.testing.expect(results.items[2].score > 0.0);
}

// Helper struct matching SearchResult
const TestSearchResult = struct {
    doc_id: i64,
    score: f64,
};

// Helper function to simulate searchNative's pattern
fn createTestResults(allocator: std.mem.Allocator) std.ArrayList(TestSearchResult) {
    var results = std.ArrayList(TestSearchResult).empty;

    // Simulate building results like searchNative does
    var doc_scores = std.AutoHashMap(i64, f64).init(allocator);
    defer doc_scores.deinit();

    // Add some scores
    doc_scores.put(1, 1.5) catch unreachable;
    doc_scores.put(2, 1.2) catch unreachable;
    doc_scores.put(3, 0.8) catch unreachable;

    // Convert to results array (same pattern as searchNative)
    var doc_iter = doc_scores.iterator();
    while (doc_iter.next()) |entry| {
        if (entry.value_ptr.* > 0.0) {
            results.append(allocator, TestSearchResult{
                .doc_id = entry.key_ptr.*,
                .score = entry.value_ptr.*,
            }) catch unreachable;
        }
    }

    // Sort by score descending
    std.mem.sort(TestSearchResult, results.items, {}, struct {
        fn lessThan(_: void, a: TestSearchResult, b: TestSearchResult) bool {
            return a.score > b.score;
        }
    }.lessThan);

    return results;
}

test "HashMap to ArrayList conversion preserves data" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var doc_scores = std.AutoHashMap(i64, f64).init(allocator);
    defer doc_scores.deinit();

    // Add scores
    try doc_scores.put(1, 1.5);
    try doc_scores.put(2, 1.2);
    try doc_scores.put(3, 0.8);

    // Count entries
    var count: usize = 0;
    var iter = doc_scores.iterator();
    while (iter.next()) |_| {
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), count);

    // Convert to ArrayList
    var results = std.ArrayList(TestSearchResult).empty;
    defer results.deinit(allocator);

    var iter2 = doc_scores.iterator();
    while (iter2.next()) |entry| {
        try results.append(allocator, TestSearchResult{
            .doc_id = entry.key_ptr.*,
            .score = entry.value_ptr.*,
        });
    }

    try std.testing.expectEqual(@as(usize, 3), results.items.len);
}

test "lexeme hash parity across tokenizer implementations for 1..64 char inputs" {
    // tokenizer.zig (SPI-coupled, not importable here) hashes with
    // std.hash.Fnv1a_64 masked into the signed bigint range; the native and
    // pure tokenizers route through avx2_utils. All indexing and query paths
    // must agree for every length or the term index silently splits.
    const avx2_utils = @import("avx2_utils.zig");

    var buf: [64]u8 = undefined;
    var len: usize = 1;
    while (len <= buf.len) : (len += 1) {
        for (0..len) |i| {
            buf[i] = 'a' + @as(u8, @intCast((i * 7 + len) % 26));
        }
        const lexeme = buf[0..len];

        const reference_u64 = std.hash.Fnv1a_64.hash(lexeme);
        const max_bigint: u64 = 0x7FFFFFFFFFFFFFFF;
        const reference: i64 = @intCast(reference_u64 % (max_bigint + 1));

        // tokenizer_native routes through avx2_utils.hashLexemeAVX2 as well,
        // but imports pg headers and cannot be compiled in this suite.
        try std.testing.expectEqual(reference, avx2_utils.hashLexemeAVX2(lexeme));
        try std.testing.expectEqual(reference, tokenizer_pure.hashLexeme(lexeme));
    }
}
