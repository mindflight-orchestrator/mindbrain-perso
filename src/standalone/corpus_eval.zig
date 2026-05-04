//! Fixed, no-network evaluator for corpus profiling and chunking experiments.
//!
//! This borrows the `autoresearch` idea of a stable harness: prompts and
//! splitters can change, but fixture expectations and scores stay comparable.

const std = @import("std");
const chunker = @import("chunker.zig");
const corpus_profile = @import("corpus_profile.zig");

pub const ProfileScore = struct {
    matched: u32,
    total: u32,

    pub fn ratio(self: ProfileScore) f64 {
        if (self.total == 0) return 0.0;
        return @as(f64, @floatFromInt(self.matched)) / @as(f64, @floatFromInt(self.total));
    }
};

pub const ChunkExpectation = struct {
    contains: []const u8,
    strategy: ?[]const u8 = null,
};

pub fn parseChunkExpectations(
    allocator: std.mem.Allocator,
    json: []const u8,
) !std.json.Parsed([]ChunkExpectation) {
    return try std.json.parseFromSlice([]ChunkExpectation, allocator, json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

pub const ChunkScore = struct {
    matched: u32,
    total: u32,

    pub fn ratio(self: ChunkScore) f64 {
        if (self.total == 0) return 0.0;
        return @as(f64, @floatFromInt(self.matched)) / @as(f64, @floatFromInt(self.total));
    }
};

pub const EvalSummary = struct {
    profile: ProfileScore,
    chunks: ChunkScore,

    pub fn overall(self: EvalSummary) f64 {
        var sum: f64 = 0.0;
        var count: f64 = 0.0;
        if (self.profile.total > 0) {
            sum += self.profile.ratio();
            count += 1.0;
        }
        if (self.chunks.total > 0) {
            sum += self.chunks.ratio();
            count += 1.0;
        }
        if (count == 0.0) return 0.0;
        return sum / count;
    }
};

pub fn scoreProfile(
    expected: corpus_profile.DocumentProfile,
    actual: corpus_profile.DocumentProfile,
) ProfileScore {
    var matched: u32 = 0;
    const total: u32 = 6;

    if (expected.document_kind == actual.document_kind) matched += 1;
    if (expected.reference_density == actual.reference_density) matched += 1;
    if (expected.temporal_model == actual.temporal_model) matched += 1;
    if (expected.recommended_splitter == actual.recommended_splitter) matched += 1;
    if (std.mem.eql(u8, expected.language, actual.language)) matched += 1;
    if (std.mem.eql(u8, expected.jurisdiction, actual.jurisdiction)) matched += 1;

    return .{ .matched = matched, .total = total };
}

pub fn scoreChunks(chunks: []const chunker.Chunk, expectations: []const ChunkExpectation) ChunkScore {
    var matched: u32 = 0;
    for (expectations) |expected| {
        if (findMatchingChunk(chunks, expected)) matched += 1;
    }
    return .{
        .matched = matched,
        .total = @intCast(expectations.len),
    };
}

pub fn summarize(
    expected_profile: corpus_profile.DocumentProfile,
    actual_profile: corpus_profile.DocumentProfile,
    chunks: []const chunker.Chunk,
    expected_chunks: []const ChunkExpectation,
) EvalSummary {
    return .{
        .profile = scoreProfile(expected_profile, actual_profile),
        .chunks = scoreChunks(chunks, expected_chunks),
    };
}

fn findMatchingChunk(chunks: []const chunker.Chunk, expected: ChunkExpectation) bool {
    for (chunks) |ch| {
        if (std.mem.indexOf(u8, ch.content, expected.contains) == null) continue;
        if (expected.strategy) |strategy| {
            if (!std.mem.eql(u8, ch.strategy, strategy)) continue;
        }
        return true;
    }
    return false;
}

test "scoreProfile rewards matching profile dimensions" {
    const expected = corpus_profile.DocumentProfile{
        .document_kind = .legal_text,
        .language = "fr",
        .jurisdiction = "FR",
        .structure_markers = &.{"article"},
        .reference_density = .high,
        .temporal_model = .amended,
        .recommended_splitter = .legal_article,
        .confidence = 0.9,
    };
    const actual = corpus_profile.DocumentProfile{
        .document_kind = .legal_text,
        .language = "fr",
        .jurisdiction = "FR",
        .structure_markers = &.{"article"},
        .reference_density = .medium,
        .temporal_model = .amended,
        .recommended_splitter = .legal_article,
        .confidence = 0.8,
    };

    const score = scoreProfile(expected, actual);
    try std.testing.expectEqual(@as(u32, 5), score.matched);
    try std.testing.expectEqual(@as(u32, 6), score.total);
}

test "scoreChunks matches expected semantic anchors" {
    const text =
        "# Safety Policy\n\n" ++
        "Article 1. Operators must inspect the machine before use.\n\n" ++
        "Article 2. This policy cites Decree 2024-10.";
    const chunks = try chunker.chunk(std.testing.allocator, text, .{
        .strategy = .paragraph,
    });
    defer chunker.freeChunks(std.testing.allocator, chunks);

    const expectations = [_]ChunkExpectation{
        .{ .contains = "Operators must inspect", .strategy = "paragraph" },
        .{ .contains = "cites Decree 2024-10", .strategy = "paragraph" },
    };
    const score = scoreChunks(chunks, &expectations);
    try std.testing.expectEqual(@as(u32, 2), score.matched);
    try std.testing.expectEqual(@as(u32, 2), score.total);
}

test "parseChunkExpectations accepts fixture-shaped JSON" {
    const json =
        \\[
        \\  {"contains": "Article 1.", "strategy": "legal_article"},
        \\  {"contains": "Article 2."}
        \\]
    ;
    var parsed = try parseChunkExpectations(std.testing.allocator, json);
    defer parsed.deinit();

    try std.testing.expectEqual(@as(usize, 2), parsed.value.len);
    try std.testing.expectEqualStrings("Article 1.", parsed.value[0].contains);
    try std.testing.expectEqualStrings("legal_article", parsed.value[0].strategy.?);
    try std.testing.expect(parsed.value[1].strategy == null);
}

test "representative fixture profile parses and scores against itself" {
    const json =
        \\{
        \\  "document_kind": "legal_text",
        \\  "language": "en",
        \\  "jurisdiction": "city",
        \\  "authority": "city authority",
        \\  "structure_markers": ["title", "article"],
        \\  "reference_density": "medium",
        \\  "temporal_model": "static",
        \\  "recommended_splitter": "legal_article",
        \\  "target_tokens": 384,
        \\  "max_chars": 4096,
        \\  "risks": ["internal reference", "external decree reference"],
        \\  "confidence": 0.9
        \\}
    ;
    var parsed = try corpus_profile.parseJson(std.testing.allocator, json);
    defer parsed.deinit();

    const score = scoreProfile(parsed.value, parsed.value);
    try std.testing.expectEqual(@as(u32, 6), score.matched);
    try std.testing.expectEqual(@as(u32, 6), score.total);
}
