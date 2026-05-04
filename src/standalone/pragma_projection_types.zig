//! Projection-type configuration helpers, parity surface for `mb_pragma.pragma_projection_*`.
//!
//! The PostgreSQL contract drives type matching, rank bias, pack priority,
//! and next-hop multipliers from the `mb_pragma.projection_types`
//! table. This module exposes the same semantics on top of the SQLite
//! `projection_types` table, falling back to the same defaults the PG
//! SQL functions use when the table has no matching row.

const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");

pub const Database = facet_sqlite.Database;
pub const c = facet_sqlite.c;
const Error = facet_sqlite.Error;

pub const default_rank_bias: f64 = 0.9;
pub const default_pack_priority: i32 = 100;
pub const default_next_hop_multiplier: f64 = 0.8;

/// Mirror of one `mb_pragma.projection_types` row.
pub const ProjectionType = struct {
    type_name: []const u8,
    /// JSON-encoded list of compatibility aliases as stored in SQLite.
    compatibility_aliases_json: []const u8 = "[]",
    rank_bias: f64 = default_rank_bias,
    pack_priority: i32 = default_pack_priority,
    next_hop_multiplier: f64 = default_next_hop_multiplier,
    structured: bool = false,
};

/// In-memory cache of `projection_types` keyed by canonical type name.
///
/// The cache parses `compatibility_aliases_json` into a flat slice of
/// borrowed strings to keep alias matching cheap. All allocations are tied
/// to the `arena` field; call `deinit` to release.
pub const ProjectionTypeIndex = struct {
    arena: std.heap.ArenaAllocator,
    rows: std.StringHashMap(Entry),

    pub const Entry = struct {
        rank_bias: f64,
        pack_priority: i32,
        next_hop_multiplier: f64,
        structured: bool,
        aliases: []const []const u8,
    };

    pub fn deinit(self: *ProjectionTypeIndex) void {
        self.rows.deinit();
        self.arena.deinit();
    }

    /// True when the configuration table currently has no rows.
    pub fn isEmpty(self: *const ProjectionTypeIndex) bool {
        return self.rows.count() == 0;
    }

    pub fn lookup(self: *const ProjectionTypeIndex, type_name: []const u8) ?Entry {
        if (self.findExact(type_name)) |entry| return entry;
        return null;
    }

    fn findExact(self: *const ProjectionTypeIndex, type_name: []const u8) ?Entry {
        var it = self.rows.iterator();
        while (it.next()) |kv| {
            if (std.ascii.eqlIgnoreCase(kv.key_ptr.*, type_name)) return kv.value_ptr.*;
        }
        return null;
    }

    /// Implements the contract of
    /// `mb_pragma.pragma_projection_type_match(p_projection_types, p_proj_type)`.
    /// Empty/null requested set always matches; otherwise we accept either a
    /// case-insensitive direct match or any alias listed for the row.
    pub fn matches(
        self: *const ProjectionTypeIndex,
        requested: ?[]const []const u8,
        proj_type: []const u8,
    ) bool {
        const needles = requested orelse return true;
        if (needles.len == 0) return true;

        for (needles) |needle| {
            if (std.ascii.eqlIgnoreCase(needle, proj_type)) return true;
        }

        const entry = self.findExact(proj_type) orelse return false;
        for (needles) |needle| {
            for (entry.aliases) |alias| {
                if (std.ascii.eqlIgnoreCase(alias, needle)) return true;
            }
        }
        return false;
    }

    pub fn rankBias(self: *const ProjectionTypeIndex, proj_type: []const u8) f64 {
        if (self.findExact(proj_type)) |entry| return entry.rank_bias;
        return default_rank_bias;
    }

    pub fn packPriority(self: *const ProjectionTypeIndex, proj_type: []const u8) i32 {
        if (self.findExact(proj_type)) |entry| return entry.pack_priority;
        return default_pack_priority;
    }

    pub fn nextHopMultiplier(self: *const ProjectionTypeIndex, proj_type: []const u8) f64 {
        if (self.findExact(proj_type)) |entry| return entry.next_hop_multiplier;
        return default_next_hop_multiplier;
    }

    pub fn isStructured(self: *const ProjectionTypeIndex, proj_type: []const u8) bool {
        if (self.findExact(proj_type)) |entry| return entry.structured;
        return false;
    }
};

/// Load every row of `projection_types` into an in-memory index.
pub fn loadIndex(db: Database, parent_allocator: std.mem.Allocator) !ProjectionTypeIndex {
    var arena = std.heap.ArenaAllocator.init(parent_allocator);
    errdefer arena.deinit();
    var rows = std.StringHashMap(ProjectionTypeIndex.Entry).init(parent_allocator);
    errdefer rows.deinit();

    const allocator = arena.allocator();

    const stmt = try prepare(
        db,
        "SELECT type_name, compatibility_aliases, rank_bias, pack_priority, next_hop_multiplier, structured FROM projection_types",
    );
    defer finalize(stmt);

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const type_name = try dupeColumnText(allocator, stmt, 0);
        const aliases_json = try dupeColumnText(allocator, stmt, 1);
        const rank_bias = c.sqlite3_column_double(stmt, 2);
        const pack_priority = c.sqlite3_column_int(stmt, 3);
        const next_hop_mul = c.sqlite3_column_double(stmt, 4);
        const structured = c.sqlite3_column_int(stmt, 5) != 0;

        const aliases = try parseAliasArray(allocator, aliases_json);

        try rows.put(type_name, .{
            .rank_bias = rank_bias,
            .pack_priority = pack_priority,
            .next_hop_multiplier = next_hop_mul,
            .structured = structured,
            .aliases = aliases,
        });
    }

    return .{ .arena = arena, .rows = rows };
}

fn parseAliasArray(allocator: std.mem.Allocator, json_text: []const u8) ![]const []const u8 {
    if (json_text.len == 0) return &[_][]const u8{};
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, json_text, .{}) catch
        return &[_][]const u8{};
    defer parsed.deinit();

    if (parsed.value != .array) return &[_][]const u8{};
    var aliases = try allocator.alloc([]const u8, parsed.value.array.items.len);
    var written: usize = 0;
    for (parsed.value.array.items) |item| {
        if (item != .string) continue;
        aliases[written] = try allocator.dupe(u8, item.string);
        written += 1;
    }
    return aliases[0..written];
}

fn prepare(db: Database, sql: []const u8) Error!*c.sqlite3_stmt {
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) return error.PrepareFailed;
    return stmt.?;
}

fn finalize(stmt: *c.sqlite3_stmt) void {
    _ = c.sqlite3_finalize(stmt);
}

fn dupeColumnText(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, index: c_int) ![]const u8 {
    const len = c.sqlite3_column_bytes(stmt, index);
    const ptr = c.sqlite3_column_text(stmt, index) orelse return try allocator.dupe(u8, "");
    const bytes: []const u8 = @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
    return try allocator.dupe(u8, bytes);
}

test "projection types index respects compatibility aliases and biases" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    var index = try loadIndex(db, std.testing.allocator);
    defer index.deinit();

    try std.testing.expect(!index.isEmpty());

    // Direct request matches direct type name (case insensitive).
    const direct_request = [_][]const u8{"FACT"};
    try std.testing.expect(index.matches(&direct_request, "FACT"));

    // Alias request: 'canonical' matches FACT via compatibility_aliases.
    const alias_request = [_][]const u8{"canonical"};
    try std.testing.expect(index.matches(&alias_request, "FACT"));
    try std.testing.expect(index.matches(&alias_request, "GOAL"));
    try std.testing.expect(!index.matches(&alias_request, "NOTE"));

    // Empty / null requested set always matches.
    try std.testing.expect(index.matches(null, "FACT"));
    try std.testing.expect(index.matches(&[_][]const u8{}, "ANYTHING"));

    // Bias defaults from projection_types seeds.
    try std.testing.expectApproxEqAbs(@as(f64, 1.3), index.rankBias("FACT"), 1e-9);
    try std.testing.expectEqual(@as(i32, 1), index.packPriority("GOAL"));
    try std.testing.expectApproxEqAbs(@as(f64, 0.6), index.nextHopMultiplier("NOTE"), 1e-9);
    try std.testing.expect(index.isStructured("CONSTRAINT"));
    try std.testing.expect(!index.isStructured("NOTE"));

    // Unknown types fall back to PG defaults.
    try std.testing.expectApproxEqAbs(default_rank_bias, index.rankBias("UNKNOWN"), 1e-9);
    try std.testing.expectEqual(default_pack_priority, index.packPriority("UNKNOWN"));
    try std.testing.expectApproxEqAbs(default_next_hop_multiplier, index.nextHopMultiplier("UNKNOWN"), 1e-9);
}
