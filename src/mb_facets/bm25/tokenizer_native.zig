const std = @import("std");
const utils = @import("../utils.zig");
const c = utils.c;
const PgAllocator = utils.PgAllocator;
const avx2_utils = @import("avx2_utils.zig");

/// Token with frequency information
pub const TokenWithFreq = struct {
    lexeme: []const u8,
    freq: i32, // Term frequency (number of occurrences)
};

/// Tokenize text using PostgreSQL's text search C API directly (no SPI)
/// This is much faster than the SQL-based tokenizer for parallel processing
/// If SPI is already connected, it will reuse the existing connection
pub fn tokenizeNative(
    text: []const u8,
    config_name: []const u8,
    allocator: std.mem.Allocator,
) !std.ArrayList(TokenWithFreq) {
    // Use SPI to call to_tsvector, but do it efficiently
    // We still use SPI but avoid the overhead of ts_stat() subqueries
    // Check if SPI is already connected (returns SPI_ERROR_CONNECT if already connected)
    const conn_result = c.SPI_connect();
    const need_finish = (conn_result == c.SPI_OK_CONNECT);
    if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
        utils.elog(c.ERROR, "SPI_connect failed");
        return error.SPIConnectFailed;
    }
    defer if (need_finish) {
        _ = c.SPI_finish();
    };

    return tokenizeNativeWithExistingConnection(text, config_name, allocator);
}

/// Tokenize text assuming SPI is already connected
/// Internal helper to avoid nested SPI connections
/// WARNING: Caller must ensure SPI is connected before calling this
pub fn tokenizeNativeWithExistingConnection(
    text: []const u8,
    config_name: []const u8,
    allocator: std.mem.Allocator,
) !std.ArrayList(TokenWithFreq) {

    // Build query with properly escaped config name and text
    // Escape config_name for SQL (single quotes)
    var config_escaped = std.ArrayList(u8).empty;
    defer config_escaped.deinit(allocator);
    config_escaped.append(allocator, '\'') catch return error.OutOfMemory;
    for (config_name) |byte| {
        if (byte == '\'') {
            config_escaped.appendSlice(allocator, "''") catch return error.OutOfMemory;
        } else {
            config_escaped.append(allocator, byte) catch return error.OutOfMemory;
        }
    }
    config_escaped.append(allocator, '\'') catch return error.OutOfMemory;

    // Escape text for SQL
    var text_escaped = std.ArrayList(u8).empty;
    defer text_escaped.deinit(allocator);
    text_escaped.append(allocator, '\'') catch return error.OutOfMemory;
    for (text) |byte| {
        if (byte == '\'') {
            text_escaped.appendSlice(allocator, "''") catch return error.OutOfMemory;
        } else if (byte == '\\') {
            text_escaped.appendSlice(allocator, "\\\\") catch return error.OutOfMemory;
        } else {
            text_escaped.append(allocator, byte) catch return error.OutOfMemory;
        }
    }
    text_escaped.append(allocator, '\'') catch return error.OutOfMemory;

    // Build query string
    const query_fmt = "SELECT to_tsvector({s}::regconfig, {s})";
    const query = try std.fmt.allocPrintSentinel(allocator, query_fmt, .{ config_escaped.items, text_escaped.items }, 0);
    defer allocator.free(query);

    const ret = c.SPI_execute(query.ptr, true, 1);

    if (ret != c.SPI_OK_SELECT or c.SPI_processed == 0 or c.SPI_tuptable == null) {
        return std.ArrayList(TokenWithFreq).empty;
    }

    const tuple = c.SPI_tuptable.*.vals[0];
    const tupdesc = c.SPI_tuptable.*.tupdesc;
    var isnull: bool = false;
    const tsvector_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);

    if (isnull) {
        return std.ArrayList(TokenWithFreq).empty;
    }

    // Extract lexemes and positions from tsvector
    var tokens = std.ArrayList(TokenWithFreq).empty;
    errdefer {
        for (tokens.items) |token| {
            allocator.free(@constCast(token.lexeme));
        }
        tokens.deinit(allocator);
    }

    // Detoast the tsvector if needed
    const tsvector_varlena = utils.detoast_datum(tsvector_datum);

    // Validate the structural layout against the real VARSIZE before any
    // pointer arithmetic. Anything inconsistent here (negative size, header
    // overflow, suspicious lexeme count) is treated as "no tokens" rather
    // than dereferenced - this is the primary defense against the
    // backend-crashing class of bug we observed in the wild.
    const bounds = utils.tsvectorBounds(tsvector_varlena) orelse {
        utils.elog(c.WARNING, "tokenizeNative: malformed tsvector layout, returning empty");
        return tokens;
    };

    if (bounds.num_lexemes == 0) {
        return tokens;
    }

    const tsvector_data = @as(*utils.TSVectorData, @alignCast(@ptrCast(tsvector_varlena)));
    const word_entries = utils.ARRPTR(tsvector_data);
    const str_ptr = utils.STRPTR(tsvector_data);

    var i: usize = 0;
    while (i < bounds.num_lexemes) : (i += 1) {
        const entry = &word_entries[i];

        const lexeme_pos: usize = @intCast(entry.pos);
        const lexeme_len: usize = @intCast(entry.len);

        // Refuse to construct an out-of-bounds slice. A single bad entry
        // shouldn't take down the backend; warn loudly and stop instead.
        if (lexeme_pos > bounds.str_area_len or
            lexeme_len > bounds.str_area_len - lexeme_pos)
        {
            utils.elogFmt(
                c.WARNING,
                "tokenizeNative: lexeme {d} out of bounds (pos={d}, len={d}, area={d})",
                .{ i, lexeme_pos, lexeme_len, bounds.str_area_len },
            );
            return tokens;
        }

        const lexeme_ptr = str_ptr + lexeme_pos;
        const lexeme_slice = lexeme_ptr[0..lexeme_len];

        var freq: i32 = 1;
        if (entry.haspos != 0) {
            const num_positions = utils.safePOSDATALEN(tsvector_varlena, entry, bounds.str_area_len);
            if (num_positions > 0) {
                freq = @intCast(num_positions);
            }
        }

        const lexeme_copy = try allocator.alloc(u8, lexeme_len);
        @memcpy(lexeme_copy, lexeme_slice);

        try tokens.append(allocator, .{
            .lexeme = lexeme_copy,
            .freq = freq,
        });
    }

    return tokens;
}

/// Calculate hash of a lexeme (for term_hash)
/// Returns a value that fits in PostgreSQL's signed bigint range
/// Uses AVX2 optimization when available
pub fn hashLexeme(lexeme: []const u8) i64 {
    return avx2_utils.hashLexemeAVX2(lexeme);
}
