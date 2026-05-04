//! Maps corpus profiles to deterministic chunker settings.
//!
//! LLMs recommend a `SplitterProfile`; this module turns that recommendation
//! into reproducible options for the import pipeline.

const std = @import("std");
const chunker = @import("chunker.zig");
const corpus_profile = @import("corpus_profile.zig");

pub const ChunkingDecision = struct {
    profile: corpus_profile.SplitterProfile,
    options: chunker.Options,
    /// True when the selected profile should eventually route through a
    /// domain-specific splitter instead of the generic chunker.
    requires_specialized_splitter: bool = false,
    rationale: []const u8,
};

pub fn decide(profile: corpus_profile.DocumentProfile) ChunkingDecision {
    return switch (profile.recommended_splitter) {
        .technical_structure => .{
            .profile = .technical_structure,
            .options = .{
                .strategy = .structure_aware,
                .target_tokens = nonZeroOr(profile.target_tokens, 384),
                .overlap_tokens = 32,
                .max_chars = atLeast(profile.max_chars, 2048),
                .min_chars = 64,
            },
            .rationale = "preserve headings, tables, code blocks, and lists",
        },
        .legal_article => .{
            .profile = .legal_article,
            .options = legalFallbackOptions(profile, 512, 4096),
            .requires_specialized_splitter = true,
            .rationale = "split on legal article/clause boundaries with heading context",
        },
        .legal_consolidated => .{
            .profile = .legal_consolidated,
            .options = legalFallbackOptions(profile, 512, 4096),
            .requires_specialized_splitter = true,
            .rationale = "preserve current article text plus temporal/version metadata",
        },
        .legal_amendment => .{
            .profile = .legal_amendment,
            .options = legalFallbackOptions(profile, 384, 3072),
            .requires_specialized_splitter = true,
            .rationale = "keep amendment/repeal/replace operations as coherent chunks",
        },
        .business_rule => .{
            .profile = .business_rule,
            .options = .{
                .strategy = .paragraph,
                .target_tokens = nonZeroOr(profile.target_tokens, 320),
                .overlap_tokens = 24,
                .max_chars = atLeast(profile.max_chars, 2048),
                .min_chars = 48,
            },
            .rationale = "split around actor, condition, action, exception, and deadline blocks",
        },
        .fallback_recursive => .{
            .profile = .fallback_recursive,
            .options = .{
                .strategy = .recursive_character,
                .target_tokens = nonZeroOr(profile.target_tokens, 256),
                .overlap_tokens = 32,
                .max_chars = atLeast(profile.max_chars, 2048),
                .min_chars = 32,
            },
            .rationale = "unknown structure; use deterministic recursive fallback",
        },
    };
}

fn legalFallbackOptions(profile: corpus_profile.DocumentProfile, default_tokens: u32, default_max_chars: usize) chunker.Options {
    return .{
        .strategy = .structure_aware,
        .target_tokens = nonZeroOr(profile.target_tokens, default_tokens),
        .overlap_tokens = 48,
        .max_chars = atLeast(profile.max_chars, default_max_chars),
        .min_chars = 64,
    };
}

fn nonZeroOr(value: u32, fallback: u32) u32 {
    return if (value == 0) fallback else value;
}

fn atLeast(value: usize, minimum: usize) usize {
    return if (value < minimum) minimum else value;
}

test "decide maps technical profile to structure-aware chunking" {
    const profile = corpus_profile.DocumentProfile{
        .document_kind = .technical,
        .structure_markers = &.{ "heading", "code_block" },
        .recommended_splitter = .technical_structure,
        .target_tokens = 300,
        .max_chars = 3000,
        .confidence = 0.9,
    };
    const decision = decide(profile);
    try std.testing.expectEqual(corpus_profile.SplitterProfile.technical_structure, decision.profile);
    try std.testing.expectEqual(chunker.Strategy.structure_aware, decision.options.strategy);
    try std.testing.expect(!decision.requires_specialized_splitter);
}

test "decide marks legal profiles as specialized" {
    const profile = corpus_profile.DocumentProfile{
        .document_kind = .legal_consolidated,
        .structure_markers = &.{ "article", "effective_date" },
        .reference_density = .high,
        .temporal_model = .consolidated,
        .recommended_splitter = .legal_consolidated,
        .confidence = 0.82,
    };
    const decision = decide(profile);
    try std.testing.expectEqual(chunker.Strategy.structure_aware, decision.options.strategy);
    try std.testing.expect(decision.requires_specialized_splitter);
    try std.testing.expect(decision.options.max_chars >= 4096);
}
