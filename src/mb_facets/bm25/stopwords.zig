const std = @import("std");
const utils = @import("../utils.zig");
const c = utils.c;

pub const StopwordSet = struct {
    allocator: std.mem.Allocator,
    words: std.StringHashMap(void),

    pub fn init(allocator: std.mem.Allocator) StopwordSet {
        return .{
            .allocator = allocator,
            .words = std.StringHashMap(void).init(allocator),
        };
    }

    pub fn deinit(self: *StopwordSet) void {
        var key_iter = self.words.keyIterator();
        while (key_iter.next()) |key| {
            self.allocator.free(@constCast(key.*));
        }
        self.words.deinit();
    }

    pub fn contains(self: *const StopwordSet, word: []const u8) bool {
        return self.words.contains(word);
    }
};

fn boundedCStringLen(value: [*:0]const u8, max_len: usize) ?usize {
    var len: usize = 0;
    while (len < max_len and value[len] != 0) {
        len += 1;
    }
    return if (len < max_len) len else null;
}

/// Load custom BM25 stopwords for a language using the caller's existing SPI
/// connection. The table stores already-normalized lexemes, so callers can
/// compare directly against tokenizer output.
pub fn loadWithExistingConnection(language: []const u8, allocator: std.mem.Allocator) !StopwordSet {
    var set = StopwordSet.init(allocator);
    errdefer set.deinit();

    const query = "SELECT normalized_word FROM facets.bm25_stopwords WHERE language = $1";
    var argtypes = [_]c.Oid{c.TEXTOID};
    const language_datum = c.PointerGetDatum(c.cstring_to_text_with_len(@ptrCast(language.ptr), @intCast(language.len)));
    var argvalues = [_]c.Datum{language_datum};
    var argnulls = [_]u8{' '};

    const ret = c.SPI_execute_with_args(
        query.ptr,
        1,
        &argtypes,
        &argvalues,
        &argnulls,
        true,
        0,
    );

    if (ret != c.SPI_OK_SELECT) {
        utils.elog(c.ERROR, "Failed to load BM25 stopwords");
        return error.QueryFailed;
    }

    if (c.SPI_processed == 0 or c.SPI_tuptable == null) {
        return set;
    }

    var row: u64 = 0;
    while (row < c.SPI_processed) : (row += 1) {
        const tuple = c.SPI_tuptable.*.vals[@intCast(row)];
        const tupdesc = c.SPI_tuptable.*.tupdesc;
        var isnull: bool = false;
        const word_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);
        if (isnull) continue;

        const word_cstr = utils.textToCstring(word_datum);
        if (word_cstr == null) continue;
        defer c.pfree(@ptrCast(word_cstr));

        const word_len = boundedCStringLen(word_cstr, 1024) orelse {
            utils.elog(c.WARNING, "BM25 stopword too long or not null-terminated, skipping");
            continue;
        };
        if (word_len == 0) continue;

        const word = word_cstr[0..word_len];
        if (set.words.contains(word)) continue;

        const word_copy = try allocator.alloc(u8, word.len);
        @memcpy(word_copy, word);
        try set.words.put(word_copy, {});
    }

    return set;
}

test "StopwordSet contains inserted normalized words" {
    var set = StopwordSet.init(std.testing.allocator);
    defer set.deinit();

    const word = try std.testing.allocator.dupe(u8, "postgresql");
    try set.words.put(word, {});

    try std.testing.expect(set.contains("postgresql"));
    try std.testing.expect(!set.contains("database"));
}
