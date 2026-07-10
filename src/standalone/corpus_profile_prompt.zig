//! Prompt builder for LLM-assisted document profiling.
//!
//! The output profile chooses the splitter before full extraction. The prompt
//! asks for strict JSON so callers can validate it with `corpus_profile`.

const std = @import("std");
const llm_client = @import("llm_client.zig");

pub const Prompt = struct {
    system: []u8,
    user: []u8,

    pub fn deinit(self: Prompt, allocator: std.mem.Allocator) void {
        allocator.free(self.system);
        allocator.free(self.user);
    }

    pub fn messages(self: *const Prompt) [2]llm_client.Message {
        return .{
            .{ .role = "system", .content = self.system },
            .{ .role = "user", .content = self.user },
        };
    }
};

pub fn build(
    allocator: std.mem.Allocator,
    text: []const u8,
    source_ref: ?[]const u8,
    sample_chars: usize,
) !Prompt {
    const sample = try sampleText(allocator, text, sample_chars);
    defer allocator.free(sample);

    const system = try allocator.dupe(u8,
        \\You classify documents before ingestion.
        \\Return only valid JSON matching the requested schema.
        \\Do not invent jurisdiction, authority, dates, references, or structure markers.
        \\If uncertain, use "unknown" and explain the risk in the risks array.
        \\
        \\Return exactly one JSON object with these fields:
        \\- document_kind: one of technical, legal_text, legal_consolidated, legal_amendment, business_rule, policy, scanned_ocr, unknown
        \\- language: BCP-47-like language code or "unknown"
        \\- jurisdiction: country/city/domain or "none"/"unknown"
        \\- authority: issuing authority string or null
        \\- structure_markers: array of strings such as heading, title, chapter, article, paragraph, clause, table, annex, code_block, condition, exception, effective_date, historical_note
        \\- reference_density: one of none, low, medium, high, unknown
        \\- temporal_model: one of static, versioned, amended, consolidated, repealed, effective_date, unknown
        \\- recommended_splitter: one of technical_structure, legal_article, legal_consolidated, legal_amendment, business_rule, fallback_recursive
        \\- target_tokens: integer, normally 256-768; never return 0
        \\- max_chars: integer, must be at least 128; normally 2048-8192
        \\- risks: array of short strings
        \\- confidence: number between 0 and 1
        \\
        \\Rules:
        \\- Legal text should prefer legal_article unless it is primarily an amendment or consolidated text.
        \\- Technical markdown/code should prefer technical_structure.
        \\- Business rules should preserve condition/action/exception blocks.
        \\- The LLM recommends the splitter; deterministic code will do the actual split.
    );

    const user = try std.fmt.allocPrint(allocator,
        \\Analyze this document sample and choose the best splitter.
        \\
        \\Source reference: {s}
        \\
        \\Document sample:
        \\{s}
    , .{ source_ref orelse "unknown", sample });

    return .{ .system = system, .user = user };
}

pub fn sampleText(
    allocator: std.mem.Allocator,
    text: []const u8,
    max_chars: usize,
) ![]u8 {
    if (max_chars == 0 or text.len <= max_chars) return try allocator.dupe(u8, text);

    const section = max_chars / 3;
    if (section == 0) return try allocator.dupe(u8, text[0..snapToCodepointStart(text, @min(text.len, max_chars))]);

    // Every cut point is snapped to a UTF-8 codepoint boundary so accented
    // FR/NL corpora never produce invalid byte sequences in the LLM prompt.
    const head = text[0..snapToCodepointStart(text, @min(section, text.len))];
    const mid_start_raw = if (text.len > section) (text.len / 2) - @min(section / 2, text.len / 2) else 0;
    const mid_start = snapToCodepointStart(text, mid_start_raw);
    const mid_end = @max(mid_start, snapToCodepointStart(text, @min(text.len, mid_start_raw + section)));
    const tail_start = snapToCodepointStart(text, if (text.len > section) text.len - section else 0);
    const tail = text[tail_start..];

    return try std.fmt.allocPrint(allocator,
        \\[BEGINNING]
        \\{s}
        \\
        \\[MIDDLE]
        \\{s}
        \\
        \\[END]
        \\{s}
    , .{ head, text[mid_start..mid_end], tail });
}

/// Moves `index` back to the start of the UTF-8 codepoint it points into
/// (identity for indexes already on a boundary or at text.len).
fn snapToCodepointStart(text: []const u8, index: usize) usize {
    var i = @min(index, text.len);
    while (i > 0 and i < text.len and (text[i] & 0xC0) == 0x80) i -= 1;
    return i;
}

test "sampleText includes beginning middle and end for long input" {
    const text = "aaaabbbbccccddddeeeeffffgggghhhhiiii";
    const sample = try sampleText(std.testing.allocator, text, 12);
    defer std.testing.allocator.free(sample);

    try std.testing.expect(std.mem.indexOf(u8, sample, "[BEGINNING]") != null);
    try std.testing.expect(std.mem.indexOf(u8, sample, "[MIDDLE]") != null);
    try std.testing.expect(std.mem.indexOf(u8, sample, "[END]") != null);
}

test "sampleText never splits UTF-8 codepoints" {
    // 'é' is 2 bytes; sizes chosen so naive byte cuts land mid-codepoint.
    const text = "ééééééééééééééééééééééééé";
    var max: usize = 7;
    while (max <= 20) : (max += 1) {
        const sample = try sampleText(std.testing.allocator, text, max);
        defer std.testing.allocator.free(sample);
        try std.testing.expect(std.unicode.utf8ValidateSlice(sample));
    }
}

test "build emits strict schema prompt" {
    var prompt = try build(std.testing.allocator, "Article 1. Operators must register.", "law.txt", 1024);
    defer prompt.deinit(std.testing.allocator);

    try std.testing.expect(std.mem.indexOf(u8, prompt.system, "recommended_splitter") != null);
    try std.testing.expect(std.mem.indexOf(u8, prompt.user, "recommended_splitter") == null);
    try std.testing.expect(std.mem.indexOf(u8, prompt.user, "law.txt") != null);
    const messages = prompt.messages();
    try std.testing.expectEqualStrings("system", messages[0].role);
    try std.testing.expectEqualStrings("user", messages[1].role);
}
