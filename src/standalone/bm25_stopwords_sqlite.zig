const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const tokenization_sqlite = @import("tokenization_sqlite.zig");

const c = facet_sqlite.c;
const Database = facet_sqlite.Database;

pub const StopwordCache = struct {
    allocator: std.mem.Allocator,
    by_language: std.StringHashMap(std.AutoHashMap(u64, void)),
    empty: std.AutoHashMap(u64, void),

    pub fn init(allocator: std.mem.Allocator) StopwordCache {
        return .{
            .allocator = allocator,
            .by_language = std.StringHashMap(std.AutoHashMap(u64, void)).init(allocator),
            .empty = std.AutoHashMap(u64, void).init(allocator),
        };
    }

    pub fn deinit(self: *StopwordCache) void {
        var it = self.by_language.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit();
        }
        self.by_language.deinit();
        self.empty.deinit();
    }

    /// Read-only set of term hashes for `language`. When `db` is null, returns an empty set (no filtering).
    pub fn sliceFor(self: *StopwordCache, db: ?Database, language: []const u8) !*const std.AutoHashMap(u64, void) {
        if (db == null) return &self.empty;
        return self.ensureLoaded(db.?, language);
    }

    fn ensureLoaded(self: *StopwordCache, db: Database, language: []const u8) !*const std.AutoHashMap(u64, void) {
        if (self.by_language.getPtr(language)) |ptr| {
            return ptr;
        }

        var set = std.AutoHashMap(u64, void).init(self.allocator);
        errdefer set.deinit();

        const sql = "SELECT normalized_word FROM bm25_stopwords WHERE language = ?1";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
            return error.PrepareFailed;
        }
        defer _ = c.sqlite3_finalize(stmt.?);

        _ = c.sqlite3_bind_text(stmt.?, 1, language.ptr, @intCast(language.len), facet_sqlite.sqliteTransient());

        while (true) {
            const rc = c.sqlite3_step(stmt.?);
            if (rc == c.SQLITE_DONE) break;
            if (rc != c.SQLITE_ROW) return error.StepFailed;
            const text_ptr = c.sqlite3_column_text(stmt.?, 0) orelse continue;
            const text_len = c.sqlite3_column_bytes(stmt.?, 0);
            if (text_len < 0) return error.ValueOutOfRange;
            const nw = @as([*]const u8, @ptrCast(text_ptr))[0..@intCast(text_len)];
            try set.put(tokenization_sqlite.hashTermLexeme(nw), {});
        }

        const lang_key = try self.allocator.dupe(u8, language);
        errdefer self.allocator.free(lang_key);
        try self.by_language.put(lang_key, set);
        return self.by_language.getPtr(lang_key).?;
    }
};
