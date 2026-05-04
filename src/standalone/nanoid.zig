//! Crypto-secure URL-safe nanoid generator. Used as the public `doc_nanoid`
//! that is the only document identifier ever exposed in URLs (chunks are
//! addressed externally as `<doc_nanoid>#<chunk_index>`). Internal joins keep
//! using the integer `doc_id`.

const std = @import("std");
const compat = @import("zig16_compat.zig");

pub const default_alphabet: []const u8 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_-";
pub const default_size: usize = 21;

pub const Error = error{
    EmptyAlphabet,
    AlphabetTooLarge,
};

/// Fills `buffer` with characters drawn uniformly at random from `alphabet`.
/// Caller must guarantee `alphabet.len > 0` and `alphabet.len <= 256`.
pub fn generate(buffer: []u8, alphabet: []const u8, rng: std.Random) !void {
    if (alphabet.len == 0) return error.EmptyAlphabet;
    if (alphabet.len > 256) return error.AlphabetTooLarge;

    // Rejection sampling so every alphabet position has equal probability,
    // even when alphabet.len is not a power of two.
    const mask: u8 = nextPow2Mask(@intCast(alphabet.len));

    var written: usize = 0;
    while (written < buffer.len) {
        var byte: u8 = 0;
        rng.bytes(@as(*[1]u8, @ptrCast(&byte)));
        const candidate: u8 = byte & mask;
        if (candidate < alphabet.len) {
            buffer[written] = alphabet[candidate];
            written += 1;
        }
    }
}

/// Allocates a 21-char URL-safe nanoid using the active Zig I/O random source.
pub fn generateDefault(allocator: std.mem.Allocator) ![]u8 {
    const buf = try allocator.alloc(u8, default_size);
    errdefer allocator.free(buf);
    const rng_source: std.Random.IoSource = .{ .io = compat.io() };
    try generate(buf, default_alphabet, rng_source.interface());
    return buf;
}

/// Allocates a nanoid of the requested size using the default URL-safe
/// alphabet and the active Zig I/O random source.
pub fn generateSized(allocator: std.mem.Allocator, size: usize) ![]u8 {
    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);
    const rng_source: std.Random.IoSource = .{ .io = compat.io() };
    try generate(buf, default_alphabet, rng_source.interface());
    return buf;
}

fn nextPow2Mask(n: u9) u8 {
    // Smallest (2^k - 1) >= n - 1 fits in u8 since alphabet.len <= 256.
    var m: u9 = 1;
    while (m < n) m <<= 1;
    return @intCast(m - 1);
}

test "generateDefault returns a 21-char URL-safe id" {
    const id = try generateDefault(std.testing.allocator);
    defer std.testing.allocator.free(id);

    try std.testing.expectEqual(default_size, id.len);
    for (id) |ch| {
        try std.testing.expect(std.mem.indexOfScalar(u8, default_alphabet, ch) != null);
    }
}

test "generate honours custom alphabets and buffer sizes" {
    const alphabet = "abc";
    var buf: [32]u8 = undefined;
    const rng_source: std.Random.IoSource = .{ .io = std.testing.io };
    try generate(&buf, alphabet, rng_source.interface());
    for (buf) |ch| {
        try std.testing.expect(ch == 'a' or ch == 'b' or ch == 'c');
    }
}

test "generate rejects empty alphabets" {
    var buf: [4]u8 = undefined;
    const rng_source: std.Random.IoSource = .{ .io = std.testing.io };
    try std.testing.expectError(error.EmptyAlphabet, generate(&buf, &.{}, rng_source.interface()));
}

test "generateDefault has no collisions across many draws" {
    const draws: usize = 4096;
    var seen = std.StringHashMap(void).init(std.testing.allocator);
    defer {
        var it = seen.iterator();
        while (it.next()) |entry| std.testing.allocator.free(entry.key_ptr.*);
        seen.deinit();
    }

    var i: usize = 0;
    while (i < draws) : (i += 1) {
        const id = try generateDefault(std.testing.allocator);
        const gop = try seen.getOrPut(id);
        if (gop.found_existing) {
            std.testing.allocator.free(id);
            return error.UnexpectedCollision;
        }
        gop.key_ptr.* = id;
    }
    try std.testing.expectEqual(@as(usize, draws), seen.count());
}
