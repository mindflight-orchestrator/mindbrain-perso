const std = @import("std");
const builtin = @import("builtin");

/// Hash function for lexemes. Every indexing and query path must produce
/// the same term_hash for the same lexeme; the previous "vectorized"
/// variant folded 8-byte chunks, which is NOT byte-wise FNV-1a, so terms
/// of 16+ chars got a different hash than tokenizer.zig's — splitting the
/// index and silently losing recall. Databases indexed with the old
/// chunked hash must be fully reindexed.
pub fn hashLexemeAVX2(lexeme: []const u8) i64 {
    return hashLexemeStandard(lexeme);
}

/// Standard FNV-1a hash (fallback)
fn hashLexemeStandard(lexeme: []const u8) i64 {
    const hash_u64 = std.hash.Fnv1a_64.hash(lexeme);
    const max_bigint: u64 = 0x7FFFFFFFFFFFFFFF;
    const masked = hash_u64 % (max_bigint + 1);
    return @as(i64, @intCast(masked));
}

/// Fast memcpy using vectorized operations when possible
/// For large copies, this can be faster than standard memcpy
pub fn fastMemcpy(dest: []u8, src: []const u8) void {
    if (dest.len != src.len) {
        @panic("fastMemcpy: lengths must match");
    }
    
    // For small copies, standard memcpy is fine
    if (src.len < 64) {
        @memcpy(dest, src);
        return;
    }
    
    // For larger copies, process in 32-byte chunks
    // This allows the compiler to potentially vectorize
    var i: usize = 0;
    while (i + 32 <= src.len) : (i += 32) {
        @memcpy(dest[i..][0..32], src[i..][0..32]);
    }
    
    // Handle remaining bytes
    if (i < src.len) {
        @memcpy(dest[i..], src[i..]);
    }
}

/// Vectorized string comparison (optimized for equality checks)
pub fn fastStringEqual(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    if (a.ptr == b.ptr) return true;
    
    // For small strings, standard comparison is fine
    if (a.len < 16) {
        return std.mem.eql(u8, a, b);
    }
    
    // Process in 16-byte chunks (allows SIMD optimization)
    var i: usize = 0;
    while (i + 16 <= a.len) : (i += 16) {
        const chunk_a = std.mem.readInt(u128, a[i..][0..16], .little);
        const chunk_b = std.mem.readInt(u128, b[i..][0..16], .little);
        if (chunk_a != chunk_b) return false;
    }
    
    // Handle remaining bytes
    if (i < a.len) {
        return std.mem.eql(u8, a[i..], b[i..]);
    }
    
    return true;
}

