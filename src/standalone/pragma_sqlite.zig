const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const pragma_dsl = @import("pragma_dsl.zig");
const projection_types = @import("pragma_projection_types.zig");
const toon_exports = @import("toon_exports.zig");

pub const Database = facet_sqlite.Database;
pub const c = facet_sqlite.c;
const Error = facet_sqlite.Error;
pub const ProjectionTypeIndex = projection_types.ProjectionTypeIndex;

pub const MemoryItem = struct {
    id: []const u8,
    user_id: []const u8,
    source_type: ?[]const u8 = null,
    source_ref: ?[]const u8 = null,
    content: ?[]const u8 = null,
};

pub const MemoryProjection = struct {
    id: []const u8,
    item_id: []const u8,
    user_id: []const u8,
    projection_type: []const u8,
    content: []const u8,
    rank_hint: ?f64 = null,
    confidence: f64 = 1.0,
    metadata_json: []const u8 = "{}",
    facets_json: []const u8 = "{}",
};

pub const MemoryEdge = struct {
    id: []const u8,
    user_id: []const u8,
    node_from: []const u8,
    node_to: []const u8,
    edge_type: []const u8,
    weight: f64 = 1.0,
};

pub const RankedProjection = struct {
    id: []const u8,
    item_id: []const u8,
    score: f64,
    projection_type: []const u8,
};

pub const PackedContextRow = struct {
    id: []const u8,
    item_id: []const u8,
    projection_type: []const u8,
    content: []const u8,
    rank_hint: ?f64,
    confidence: f64,
};

pub const NextHop = struct {
    node_id: []const u8,
    score: f64,
};

pub fn insertMemoryItem(db: Database, item: MemoryItem) !void {
    const stmt = try prepare(db, "INSERT OR REPLACE INTO memory_items(id, user_id, source_type, source_ref, content) VALUES (?1, ?2, ?3, ?4, ?5)");
    defer finalize(stmt);
    try bindText(stmt, 1, item.id);
    try bindText(stmt, 2, item.user_id);
    if (item.source_type) |value| try bindText(stmt, 3, value) else try bindNull(stmt, 3);
    if (item.source_ref) |value| try bindText(stmt, 4, value) else try bindNull(stmt, 4);
    if (item.content) |value| try bindText(stmt, 5, value) else try bindNull(stmt, 5);
    try stepDone(stmt);
}

pub fn insertMemoryProjection(db: Database, projection: MemoryProjection) !void {
    const stmt = try prepare(db, "INSERT OR REPLACE INTO memory_projections(id, item_id, user_id, projection_type, content, rank_hint, confidence, metadata_json, facets_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)");
    defer finalize(stmt);
    try bindText(stmt, 1, projection.id);
    try bindText(stmt, 2, projection.item_id);
    try bindText(stmt, 3, projection.user_id);
    try bindText(stmt, 4, projection.projection_type);
    try bindText(stmt, 5, projection.content);
    if (projection.rank_hint) |value| {
        if (c.sqlite3_bind_double(stmt, 6, value) != c.SQLITE_OK) return error.BindFailed;
    } else try bindNull(stmt, 6);
    if (c.sqlite3_bind_double(stmt, 7, projection.confidence) != c.SQLITE_OK) return error.BindFailed;
    try bindText(stmt, 8, projection.metadata_json);
    try bindText(stmt, 9, projection.facets_json);
    try stepDone(stmt);
}

pub fn insertMemoryEdge(db: Database, edge: MemoryEdge) !void {
    const stmt = try prepare(db, "INSERT OR REPLACE INTO memory_edges(id, user_id, node_from, node_to, edge_type, weight) VALUES (?1, ?2, ?3, ?4, ?5, ?6)");
    defer finalize(stmt);
    try bindText(stmt, 1, edge.id);
    try bindText(stmt, 2, edge.user_id);
    try bindText(stmt, 3, edge.node_from);
    try bindText(stmt, 4, edge.node_to);
    try bindText(stmt, 5, edge.edge_type);
    if (c.sqlite3_bind_double(stmt, 6, edge.weight) != c.SQLITE_OK) return error.BindFailed;
    try stepDone(stmt);
}

pub fn rankNative(
    db: Database,
    allocator: std.mem.Allocator,
    user_id: []const u8,
    query: []const u8,
    requested_types: ?[]const []const u8,
    limit_n: usize,
) ![]RankedProjection {
    const all = try loadProjections(db, allocator, user_id);
    defer {
        for (all) |row| deinitProjectionRow(allocator, row);
        allocator.free(all);
    }

    var index = try projection_types.loadIndex(db, allocator);
    defer index.deinit();

    var ranked = std.ArrayList(RankedProjection).empty;
    defer {
        for (ranked.items) |row| {
            allocator.free(row.id);
            allocator.free(row.item_id);
            allocator.free(row.projection_type);
        }
        ranked.deinit(allocator);
    }

    for (all) |row| {
        if (!index.matches(requested_types, row.projection_type)) continue;
        const score = scoreProjection(row, query, &index);
        if (score <= 0) continue;
        try ranked.append(allocator, .{
            .id = try allocator.dupe(u8, row.id),
            .item_id = try allocator.dupe(u8, row.item_id),
            .score = score,
            .projection_type = try allocator.dupe(u8, row.projection_type),
        });
    }

    std.mem.sort(RankedProjection, ranked.items, {}, struct {
        fn lessThan(_: void, lhs: RankedProjection, rhs: RankedProjection) bool {
            if (lhs.score != rhs.score) return lhs.score > rhs.score;
            return std.mem.order(u8, lhs.id, rhs.id) == .lt;
        }
    }.lessThan);
    if (ranked.items.len > limit_n) ranked.shrinkRetainingCapacity(limit_n);
    return ranked.toOwnedSlice(allocator);
}

pub fn packContext(
    db: Database,
    allocator: std.mem.Allocator,
    user_id: []const u8,
    query: []const u8,
    limit_n: usize,
) ![]PackedContextRow {
    return packContextScoped(db, allocator, user_id, query, null, limit_n);
}

pub fn packContextScoped(
    db: Database,
    allocator: std.mem.Allocator,
    user_id: []const u8,
    query: []const u8,
    scope: ?[]const u8,
    limit_n: usize,
) ![]PackedContextRow {
    const all = try loadProjections(db, allocator, user_id);
    defer {
        for (all) |row| deinitProjectionRow(allocator, row);
        allocator.free(all);
    }

    var index = try projection_types.loadIndex(db, allocator);
    defer index.deinit();

    const PackEntry = struct {
        row: PackedContextRow,
        priority: i32,
    };

    var pack_entries = std.ArrayList(PackEntry).empty;
    defer {
        for (pack_entries.items) |entry| deinitPackedRow(allocator, entry.row);
        pack_entries.deinit(allocator);
    }

    for (all) |row| {
        if (!isPackable(&index, row.projection_type)) continue;
        if (!matchesScope(scope, row.metadata_json, row.facets_json)) continue;
        if (!matchesText(row.content, query)) continue;

        try pack_entries.append(allocator, .{
            .row = .{
                .id = try allocator.dupe(u8, row.id),
                .item_id = try allocator.dupe(u8, row.item_id),
                .projection_type = try allocator.dupe(u8, row.projection_type),
                .content = try allocator.dupe(u8, row.content),
                .rank_hint = row.rank_hint,
                .confidence = row.confidence,
            },
            .priority = packPriorityFor(&index, row.projection_type),
        });
    }

    std.mem.sort(PackEntry, pack_entries.items, {}, struct {
        fn lessThan(_: void, lhs: PackEntry, rhs: PackEntry) bool {
            if (lhs.priority != rhs.priority) return lhs.priority < rhs.priority;
            const lhs_hint = lhs.row.rank_hint orelse -1e9;
            const rhs_hint = rhs.row.rank_hint orelse -1e9;
            if (lhs_hint != rhs_hint) return lhs_hint > rhs_hint;
            return std.mem.order(u8, lhs.row.id, rhs.row.id) == .lt;
        }
    }.lessThan);
    if (pack_entries.items.len > limit_n) {
        for (pack_entries.items[limit_n..]) |entry| deinitPackedRow(allocator, entry.row);
        pack_entries.shrinkRetainingCapacity(limit_n);
    }

    var packed_rows = try allocator.alloc(PackedContextRow, pack_entries.items.len);
    for (pack_entries.items, 0..) |entry, i| packed_rows[i] = entry.row;
    pack_entries.clearRetainingCapacity();
    return packed_rows;
}

pub fn nextHops(
    db: Database,
    allocator: std.mem.Allocator,
    user_id: []const u8,
    seed_nodes: []const []const u8,
    limit_n: usize,
) ![]NextHop {
    const edges = try loadEdges(db, allocator, user_id);
    defer {
        for (edges) |edge| deinitEdgeRow(allocator, edge);
        allocator.free(edges);
    }

    var scores = std.StringHashMap(f64).init(allocator);
    defer {
        var it = scores.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        scores.deinit();
    }

    for (edges) |edge| {
        for (seed_nodes) |seed| {
            if (std.mem.eql(u8, edge.node_from, seed)) {
                try accumulateScore(allocator, &scores, edge.node_to, edge.weight);
            }
            if (std.mem.eql(u8, edge.node_to, seed)) {
                try accumulateScore(allocator, &scores, edge.node_from, edge.weight * 0.8);
            }
        }
    }

    const projections = try loadProjections(db, allocator, user_id);
    defer {
        for (projections) |row| deinitProjectionRow(allocator, row);
        allocator.free(projections);
    }

    var index = try projection_types.loadIndex(db, allocator);
    defer index.deinit();

    for (projections) |projection| {
        var rec = pragma_dsl.parseFirstRecord(allocator, projection.content);
        defer if (rec) |*record| record.deinit(allocator);
        if (rec == null) continue;
        const nodes = try collectSuggestionNodes(allocator, rec.?);
        defer {
            for (nodes) |node| allocator.free(node);
            allocator.free(nodes);
        }
        // Per the PG contract, structured projections expand seeds via the
        // type-specific next_hop_multiplier; non-structured rows fall back
        // to the DSL record-type prior so we don't regress legacy data.
        const multiplier = if (index.isStructured(projection.projection_type))
            index.nextHopMultiplier(projection.projection_type)
        else
            pragma_dsl.typePrior(rec.?.record_type);
        for (seed_nodes) |seed| {
            if (!containsString(nodes, seed)) continue;
            for (nodes) |node| {
                if (std.mem.eql(u8, node, seed)) continue;
                try accumulateScore(allocator, &scores, node, projection.confidence * multiplier);
            }
        }
    }

    var hops = std.ArrayList(NextHop).empty;
    defer {
        for (hops.items) |hop| allocator.free(hop.node_id);
        hops.deinit(allocator);
    }

    var it = scores.iterator();
    while (it.next()) |entry| {
        try hops.append(allocator, .{
            .node_id = try allocator.dupe(u8, entry.key_ptr.*),
            .score = entry.value_ptr.*,
        });
    }

    std.mem.sort(NextHop, hops.items, {}, struct {
        fn lessThan(_: void, lhs: NextHop, rhs: NextHop) bool {
            if (lhs.score != rhs.score) return lhs.score > rhs.score;
            return std.mem.order(u8, lhs.node_id, rhs.node_id) == .lt;
        }
    }.lessThan);
    if (hops.items.len > limit_n) hops.shrinkRetainingCapacity(limit_n);
    return hops.toOwnedSlice(allocator);
}

const ProjectionRow = struct {
    id: []const u8,
    item_id: []const u8,
    projection_type: []const u8,
    content: []const u8,
    rank_hint: ?f64,
    confidence: f64,
    metadata_json: []const u8,
    facets_json: []const u8,
};

const EdgeRow = struct {
    node_from: []const u8,
    node_to: []const u8,
    weight: f64,
};

fn loadProjections(db: Database, allocator: std.mem.Allocator, user_id: []const u8) ![]ProjectionRow {
    const stmt = try prepare(db, "SELECT id, item_id, projection_type, content, rank_hint, confidence, metadata_json, facets_json FROM memory_projections WHERE user_id = ?1");
    defer finalize(stmt);
    try bindText(stmt, 1, user_id);

    var rows = std.ArrayList(ProjectionRow).empty;
    defer {
        for (rows.items) |row| deinitProjectionRow(allocator, row);
        rows.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(allocator, .{
            .id = try dupeColumnText(allocator, stmt, 0),
            .item_id = try dupeColumnText(allocator, stmt, 1),
            .projection_type = try dupeColumnText(allocator, stmt, 2),
            .content = try dupeColumnText(allocator, stmt, 3),
            .rank_hint = if (c.sqlite3_column_type(stmt, 4) == c.SQLITE_NULL) null else c.sqlite3_column_double(stmt, 4),
            .confidence = c.sqlite3_column_double(stmt, 5),
            .metadata_json = try dupeColumnText(allocator, stmt, 6),
            .facets_json = try dupeColumnText(allocator, stmt, 7),
        });
    }

    return rows.toOwnedSlice(allocator);
}

fn loadEdges(db: Database, allocator: std.mem.Allocator, user_id: []const u8) ![]EdgeRow {
    const stmt = try prepare(db, "SELECT node_from, node_to, weight FROM memory_edges WHERE user_id = ?1");
    defer finalize(stmt);
    try bindText(stmt, 1, user_id);

    var rows = std.ArrayList(EdgeRow).empty;
    defer {
        for (rows.items) |row| deinitEdgeRow(allocator, row);
        rows.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(allocator, .{
            .node_from = try dupeColumnText(allocator, stmt, 0),
            .node_to = try dupeColumnText(allocator, stmt, 1),
            .weight = c.sqlite3_column_double(stmt, 2),
        });
    }

    return rows.toOwnedSlice(allocator);
}

fn scoreProjection(row: ProjectionRow, query: []const u8, index: *const ProjectionTypeIndex) f64 {
    const type_prior = index.rankBias(row.projection_type);
    if (query.len == 0) return type_prior + row.confidence + (row.rank_hint orelse 0);
    if (!matchesText(row.content, query)) return 0;
    return type_prior + row.confidence + (row.rank_hint orelse 0) * 0.5;
}

/// Public projection type matching helper, mirrors
/// `mb_pragma.pragma_projection_type_match` for callers outside this module.
pub fn projectionTypeMatch(
    index: *const ProjectionTypeIndex,
    requested: ?[]const []const u8,
    proj_type: []const u8,
) bool {
    return index.matches(requested, proj_type);
}

fn isPackable(index: *const ProjectionTypeIndex, projection_type: []const u8) bool {
    if (index.lookup(projection_type) != null) return true;
    // Legacy `memory_projections` rows use the alias-style direct types
    // ('canonical', 'proposition', 'raw'); accept them when the
    // projection_types table has not (yet) been seeded for them.
    return std.ascii.eqlIgnoreCase(projection_type, "canonical") or
        std.ascii.eqlIgnoreCase(projection_type, "proposition") or
        std.ascii.eqlIgnoreCase(projection_type, "raw");
}

fn packPriorityFor(index: *const ProjectionTypeIndex, projection_type: []const u8) i32 {
    if (index.lookup(projection_type) != null) return index.packPriority(projection_type);
    if (std.ascii.eqlIgnoreCase(projection_type, "canonical")) return 1;
    if (std.ascii.eqlIgnoreCase(projection_type, "proposition")) return 2;
    return 3;
}

fn matchesText(content: []const u8, query: []const u8) bool {
    if (query.len == 0) return true;
    const hay = lowerOwned(std.heap.page_allocator, content) catch return false;
    defer std.heap.page_allocator.free(hay);
    const needle = lowerOwned(std.heap.page_allocator, query) catch return false;
    defer std.heap.page_allocator.free(needle);
    return std.mem.indexOf(u8, hay, needle) != null;
}

fn matchesScope(scope: ?[]const u8, metadata_json: []const u8, facets_json: []const u8) bool {
    if (scope == null or scope.?.len == 0) return true;
    const normalized = if (std.mem.startsWith(u8, scope.?, "player:")) scope.?[7..] else scope.?;
    return std.mem.indexOf(u8, metadata_json, scope.?) != null or
        std.mem.indexOf(u8, metadata_json, normalized) != null or
        std.mem.indexOf(u8, facets_json, scope.?) != null or
        std.mem.indexOf(u8, facets_json, normalized) != null;
}

fn containsString(values: []const []const u8, target: []const u8) bool {
    for (values) |value| if (std.mem.eql(u8, value, target)) return true;
    return false;
}

fn collectSuggestionNodes(allocator: std.mem.Allocator, record: pragma_dsl.PropositionRecord) ![]const []const u8 {
    var nodes = std.ArrayList([]const u8).empty;
    defer {
        for (nodes.items) |node| allocator.free(node);
        nodes.deinit(allocator);
    }

    // Match PG's `pragma_next_hops_native` which unnests subject/object/from/to/id.
    const keys = [_][]const u8{ "subject", "object", "from", "to", "id" };
    for (keys) |key| {
        if (record.get(key)) |value| {
            try nodes.append(allocator, try allocator.dupe(u8, value));
        }
    }

    return nodes.toOwnedSlice(allocator);
}

fn accumulateScore(allocator: std.mem.Allocator, scores: *std.StringHashMap(f64), key: []const u8, delta: f64) !void {
    if (scores.getPtr(key)) |existing| {
        existing.* += delta;
        return;
    }
    try scores.put(try allocator.dupe(u8, key), delta);
}

fn deinitProjectionRow(allocator: std.mem.Allocator, row: ProjectionRow) void {
    allocator.free(row.id);
    allocator.free(row.item_id);
    allocator.free(row.projection_type);
    allocator.free(row.content);
    allocator.free(row.metadata_json);
    allocator.free(row.facets_json);
}

pub fn deinitPackedRow(allocator: std.mem.Allocator, row: PackedContextRow) void {
    allocator.free(row.id);
    allocator.free(row.item_id);
    allocator.free(row.projection_type);
    allocator.free(row.content);
}

fn deinitEdgeRow(allocator: std.mem.Allocator, row: EdgeRow) void {
    allocator.free(row.node_from);
    allocator.free(row.node_to);
}

fn lowerOwned(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    const output = try allocator.alloc(u8, input.len);
    for (input, 0..) |char, index| output[index] = std.ascii.toLower(char);
    return output;
}

fn prepare(db: Database, sql: []const u8) Error!*c.sqlite3_stmt {
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) return error.PrepareFailed;
    return stmt.?;
}

fn finalize(stmt: *c.sqlite3_stmt) void {
    _ = c.sqlite3_finalize(stmt);
}

fn stepDone(stmt: *c.sqlite3_stmt) Error!void {
    if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
}

fn bindText(stmt: *c.sqlite3_stmt, index: c_int, value: []const u8) Error!void {
    if (c.sqlite3_bind_text(stmt, index, value.ptr, @intCast(value.len), facet_sqlite.sqliteTransient()) != c.SQLITE_OK) return error.BindFailed;
}

fn bindNull(stmt: *c.sqlite3_stmt, index: c_int) Error!void {
    if (c.sqlite3_bind_null(stmt, index) != c.SQLITE_OK) return error.BindFailed;
}

fn dupeColumnText(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, index: c_int) ![]const u8 {
    const len = c.sqlite3_column_bytes(stmt, index);
    const ptr = c.sqlite3_column_text(stmt, index) orelse return error.MissingRow;
    const bytes: []const u8 = @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
    return try allocator.dupe(u8, bytes);
}

test "pragma sqlite ranks projections and packs scoped context" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertMemoryItem(db, .{ .id = "i1", .user_id = "u1" });
    try insertMemoryProjection(db, .{
        .id = "p1",
        .item_id = "i1",
        .user_id = "u1",
        .projection_type = "canonical",
        .content = "Ada works for Acme",
        .rank_hint = 5,
        .confidence = 0.9,
        .metadata_json = "{\"player_id\":\"7\"}",
    });
    try insertMemoryProjection(db, .{
        .id = "p2",
        .item_id = "i1",
        .user_id = "u1",
        .projection_type = "raw",
        .content = "misc raw context",
        .rank_hint = 1,
        .confidence = 0.5,
    });

    const ranked = try rankNative(db, std.testing.allocator, "u1", "Ada", &.{ "canonical", "raw" }, 5);
    defer {
        for (ranked) |row| {
            std.testing.allocator.free(row.id);
            std.testing.allocator.free(row.item_id);
            std.testing.allocator.free(row.projection_type);
        }
        std.testing.allocator.free(ranked);
    }
    try std.testing.expectEqual(@as(usize, 1), ranked.len);
    try std.testing.expectEqualStrings("p1", ranked[0].id);

    const packed_rows = try packContextScoped(db, std.testing.allocator, "u1", "Ada", "player:7", 5);
    defer {
        for (packed_rows) |row| deinitPackedRow(std.testing.allocator, row);
        std.testing.allocator.free(packed_rows);
    }
    try std.testing.expectEqual(@as(usize, 1), packed_rows.len);
    try std.testing.expectEqualStrings("canonical", packed_rows[0].projection_type);
}

test "pragma sqlite suggests next hops from projections and edges" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertMemoryItem(db, .{ .id = "i1", .user_id = "u1" });
    try insertMemoryProjection(db, .{
        .id = "p1",
        .item_id = "i1",
        .user_id = "u1",
        .projection_type = "proposition",
        .content = "edge|from=ada|to=acme|id=e1|conf=0.8",
        .confidence = 0.8,
    });
    try insertMemoryEdge(db, .{
        .id = "e-1",
        .user_id = "u1",
        .node_from = "ada",
        .node_to = "graph",
        .edge_type = "related_to",
        .weight = 0.6,
    });

    const hops = try nextHops(db, std.testing.allocator, "u1", &.{"ada"}, 5);
    defer {
        for (hops) |hop| std.testing.allocator.free(hop.node_id);
        std.testing.allocator.free(hops);
    }
    // Per PG `pragma_next_hops_native`, the proposition expands every
    // jsonb value (subject/object/from/to/id), so the DSL `id=e1` field
    // contributes a third candidate node alongside acme and the edge
    // target `graph`.
    try std.testing.expectEqual(@as(usize, 3), hops.len);
    var saw_acme = false;
    var saw_e1 = false;
    var saw_graph = false;
    for (hops) |hop| {
        if (std.mem.eql(u8, hop.node_id, "acme")) saw_acme = true;
        if (std.mem.eql(u8, hop.node_id, "e1")) saw_e1 = true;
        if (std.mem.eql(u8, hop.node_id, "graph")) saw_graph = true;
    }
    try std.testing.expect(saw_acme and saw_e1 and saw_graph);
}

test "pragma sqlite has a TOON pack variant" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertMemoryProjection(db, .{
        .id = "p1",
        .item_id = "i1",
        .user_id = "u1",
        .projection_type = "canonical",
        .content = "Ada works for Acme",
        .rank_hint = 5,
        .confidence = 0.9,
        .metadata_json = "{\"scope\":\"player:7\"}",
    });

    const packed_rows = try packContextScoped(db, std.testing.allocator, "u1", "Ada", "player:7", 5);
    defer {
        for (packed_rows) |row| deinitPackedRow(std.testing.allocator, row);
        std.testing.allocator.free(packed_rows);
    }

    const toon = try toon_exports.encodePackContextAlloc(
        std.testing.allocator,
        "u1",
        "Ada",
        "player:7",
        packed_rows,
        toon_exports.default_options,
    );
    defer std.testing.allocator.free(toon);

    try std.testing.expect(std.mem.indexOf(u8, toon, "kind: pack_context") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "user_id: u1") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "rows[1\t]{id\titem_id\tprojection_type\tcontent\trank_hint\tconfidence}:") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "p1\ti1\tcanonical\t\"Ada works for Acme\"\t5\t0.9") != null);
}
