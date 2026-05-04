//! Structured profile emitted by the LLM-assisted sample analysis step.
//!
//! The profile is intentionally small and strict: it chooses a corpus-aware
//! splitter before full import, but it does not perform extraction itself.

const std = @import("std");

pub const DocumentKind = enum {
    technical,
    legal_text,
    legal_consolidated,
    legal_amendment,
    business_rule,
    policy,
    scanned_ocr,
    unknown,
};

pub const ReferenceDensity = enum {
    none,
    low,
    medium,
    high,
    unknown,
};

pub const TemporalModel = enum {
    static,
    versioned,
    amended,
    consolidated,
    repealed,
    effective_date,
    unknown,
};

pub const SplitterProfile = enum {
    technical_structure,
    legal_article,
    legal_consolidated,
    legal_amendment,
    business_rule,
    fallback_recursive,
};

pub const DocumentProfile = struct {
    document_kind: DocumentKind,
    language: []const u8 = "unknown",
    jurisdiction: []const u8 = "unknown",
    authority: ?[]const u8 = null,
    structure_markers: []const []const u8 = &.{},
    reference_density: ReferenceDensity = .unknown,
    temporal_model: TemporalModel = .unknown,
    recommended_splitter: SplitterProfile,
    target_tokens: u32 = 256,
    max_chars: usize = 2048,
    risks: []const []const u8 = &.{},
    confidence: f32 = 0.0,
};

pub const ValidationError = error{
    InvalidConfidence,
    InvalidChunkBudget,
    MissingStructureMarkers,
};

pub fn parseJson(
    allocator: std.mem.Allocator,
    json: []const u8,
) !std.json.Parsed(DocumentProfile) {
    var parsed = try std.json.parseFromSlice(DocumentProfile, allocator, json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    errdefer parsed.deinit();
    try validate(parsed.value);
    return parsed;
}

pub fn validate(profile: DocumentProfile) ValidationError!void {
    if (profile.confidence < 0.0 or profile.confidence > 1.0) return error.InvalidConfidence;
    if (profile.target_tokens == 0 or profile.max_chars < 128) return error.InvalidChunkBudget;
    if (profile.document_kind != .unknown and profile.structure_markers.len == 0) {
        return error.MissingStructureMarkers;
    }
}

pub fn isLegalLike(profile: DocumentProfile) bool {
    return switch (profile.document_kind) {
        .legal_text, .legal_consolidated, .legal_amendment => true,
        else => false,
    };
}

test "parseJson accepts a strict legal profile" {
    const json =
        \\{
        \\  "document_kind": "legal_text",
        \\  "language": "fr",
        \\  "jurisdiction": "FR",
        \\  "authority": "City Council",
        \\  "structure_markers": ["title", "chapter", "article"],
        \\  "reference_density": "high",
        \\  "temporal_model": "amended",
        \\  "recommended_splitter": "legal_article",
        \\  "target_tokens": 384,
        \\  "max_chars": 4096,
        \\  "risks": ["amendments", "external references"],
        \\  "confidence": 0.87
        \\}
    ;

    var parsed = try parseJson(std.testing.allocator, json);
    defer parsed.deinit();

    try std.testing.expectEqual(DocumentKind.legal_text, parsed.value.document_kind);
    try std.testing.expectEqual(SplitterProfile.legal_article, parsed.value.recommended_splitter);
    try std.testing.expect(isLegalLike(parsed.value));
}

test "parseJson rejects invalid confidence" {
    try std.testing.expectError(error.InvalidConfidence, parseInvalidConfidenceFixture());
}

fn parseInvalidConfidenceFixture() !void {
    const json =
        \\{
        \\  "document_kind": "technical",
        \\  "structure_markers": ["heading"],
        \\  "recommended_splitter": "technical_structure",
        \\  "confidence": 1.7
        \\}
    ;

    var parsed = try parseJson(std.testing.allocator, json);
    defer parsed.deinit();
}
