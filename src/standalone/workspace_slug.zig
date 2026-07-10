const std = @import("std");

/// canonicalize folds a raw workspace_id into a stable, duplicate-free slug.
///
/// Rules:
///   * Common Latin accents (Latin-1 Supplement + a few Latin Extended-A code
///     points) are folded to ASCII: à â ä á ã -> a, è é ê ë -> e, etc., plus the
///     uppercase forms, and the ligatures œ -> oe, æ -> ae, ß -> ss.
///   * ASCII letters are lowercased. [a-z0-9] and '_' are kept verbatim.
///   * Any run of other bytes (spaces, punctuation, or un-foldable multibyte
///     sequences) collapses to a single '-'. Leading/trailing '-' are trimmed.
///
/// The mapping is idempotent: an already-clean slug maps to itself, so existing
/// clean ids (e.g. "ws_fix", "ws1") keep resolving unchanged.
///
/// Limitation: the transliteration table only covers Latin script. An input made
/// entirely of un-foldable multibyte text (CJK, Arabic, Cyrillic, ...) collapses
/// to nothing; when the result would be empty we return error.InvalidWorkspaceId
/// rather than a meaningless slug.
pub fn canonicalize(allocator: std.mem.Allocator, raw: []const u8) ![]u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);

    // Start "in a dash" so any leading separators are suppressed (leading trim).
    var previous_dash = true;

    var i: usize = 0;
    while (i < raw.len) {
        // Try to fold a two-byte accented sequence first.
        if (i + 1 < raw.len) {
            if (translit(raw[i], raw[i + 1])) |rep| {
                try out.appendSlice(allocator, rep);
                previous_dash = false;
                i += 2;
                continue;
            }
        }

        const ch = raw[i];
        if (isKept(ch)) {
            try out.append(allocator, std.ascii.toLower(ch));
            previous_dash = false;
            i += 1;
            continue;
        }

        // Separator byte: emit a single collapsed '-'.
        if (!previous_dash) {
            try out.append(allocator, '-');
            previous_dash = true;
        }
        i += 1;
    }

    // Trailing trim.
    while (out.items.len > 0 and out.items[out.items.len - 1] == '-') _ = out.pop();

    // errdefer above frees `out` on this error path.
    if (out.items.len == 0) return error.InvalidWorkspaceId;
    return out.toOwnedSlice(allocator);
}

/// Bytes that survive verbatim (after lowercasing): [a-z0-9_].
fn isKept(ch: u8) bool {
    return (ch >= 'a' and ch <= 'z') or
        (ch >= 'A' and ch <= 'Z') or
        (ch >= '0' and ch <= '9') or
        ch == '_';
}

/// Map a two-byte UTF-8 sequence to its ASCII transliteration, or null if the
/// pair is not a known accented Latin letter. Covers U+00C0..U+00FF (encoded as
/// 0xC3 0x80..0xBF) and the ligatures œ/Œ (0xC5 0x92/0x93) and Ÿ (0xC5 0xB8).
fn translit(b0: u8, b1: u8) ?[]const u8 {
    if (b0 == 0xC3) {
        return switch (b1) {
            // à á â ã ä / À Á Â Ã Ä
            0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0x80, 0x81, 0x82, 0x83, 0x84 => "a",
            // è é ê ë / È É Ê Ë
            0xA8, 0xA9, 0xAA, 0xAB, 0x88, 0x89, 0x8A, 0x8B => "e",
            // ì í î ï / Ì Í Î Ï
            0xAC, 0xAD, 0xAE, 0xAF, 0x8C, 0x8D, 0x8E, 0x8F => "i",
            // ò ó ô õ ö / Ò Ó Ô Õ Ö
            0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0x92, 0x93, 0x94, 0x95, 0x96 => "o",
            // ù ú û ü / Ù Ú Û Ü
            0xB9, 0xBA, 0xBB, 0xBC, 0x99, 0x9A, 0x9B, 0x9C => "u",
            // ç / Ç
            0xA7, 0x87 => "c",
            // ñ / Ñ
            0xB1, 0x91 => "n",
            // ÿ
            0xBF => "y",
            // æ / Æ
            0xA6, 0x86 => "ae",
            // ß
            0x9F => "ss",
            else => null,
        };
    }
    if (b0 == 0xC5) {
        return switch (b1) {
            // œ / Œ
            0x93, 0x92 => "oe",
            // Ÿ
            0xB8 => "y",
            else => null,
        };
    }
    return null;
}

const testing = std.testing;

fn expectCanon(raw: []const u8, want: []const u8) !void {
    const got = try canonicalize(testing.allocator, raw);
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(want, got);
}

test "canonicalize folds accents and lowercases" {
    try expectCanon("Café Immeuble", "cafe-immeuble");
    try expectCanon("Élysée", "elysee");
    try expectCanon("Straße", "strasse");
    try expectCanon("Œuvre", "oeuvre");
}

test "canonicalize lowercases ASCII and keeps underscores/digits" {
    try expectCanon("WS_Fix", "ws_fix");
    try expectCanon("ws1", "ws1");
}

test "canonicalize is idempotent on clean slugs" {
    try expectCanon("ws_fix", "ws_fix");
    try expectCanon("cafe-immeuble", "cafe-immeuble");

    // Feeding the output back in yields the same result.
    const once = try canonicalize(testing.allocator, "Café Immeuble!!");
    defer testing.allocator.free(once);
    const twice = try canonicalize(testing.allocator, once);
    defer testing.allocator.free(twice);
    try testing.expectEqualStrings(once, twice);
}

test "canonicalize collapses and trims separators" {
    try expectCanon("a  b--c", "a-b-c");
    try expectCanon("  padded  ", "padded");
    try expectCanon("--edges--", "edges");
}

test "canonicalize dedup guarantee: case and accent variants collide" {
    const a = try canonicalize(testing.allocator, "café");
    defer testing.allocator.free(a);
    const b = try canonicalize(testing.allocator, "CAFÉ");
    defer testing.allocator.free(b);
    try testing.expectEqualStrings("cafe", a);
    try testing.expectEqualStrings(a, b);
}

test "canonicalize rejects entirely un-foldable input" {
    // All-CJK: the translit table cannot handle these, so the slug is empty.
    try testing.expectError(error.InvalidWorkspaceId, canonicalize(testing.allocator, "日本語"));
    try testing.expectError(error.InvalidWorkspaceId, canonicalize(testing.allocator, "   "));
    try testing.expectError(error.InvalidWorkspaceId, canonicalize(testing.allocator, "---"));
}
