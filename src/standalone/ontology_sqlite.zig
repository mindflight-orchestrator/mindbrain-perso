const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const facet_store = @import("facet_store.zig");
const graph_sqlite = @import("graph_sqlite.zig");
const roaring = @import("roaring.zig");
const toon_exports = @import("toon_exports.zig");

pub const Database = facet_sqlite.Database;
pub const c = facet_sqlite.c;
const Error = facet_sqlite.Error;

pub const FacetRecord = struct {
    id: []const u8,
    schema_id: []const u8,
    content: []const u8,
    facets_json: []const u8,
    workspace_id: []const u8,
    doc_id: u64,
    source_ref: ?[]const u8 = null,
};

pub const ProjectionRecord = struct {
    id: []const u8,
    agent_id: []const u8,
    scope: ?[]const u8 = null,
    proj_type: []const u8,
    content: []const u8,
    weight: f32 = 0.5,
    source_ref: ?[]const u8 = null,
    source_type: ?[]const u8 = null,
    status: []const u8 = "active",
};

pub const CoverageSummary = struct {
    workspace_id: []const u8,
    covered_nodes: usize,
    total_nodes: usize,
    graph_entities: usize,
    facet_rows: usize,
    projection_rows: usize,
    coverage_ratio: ?f64,
};

pub const CoverageGap = struct {
    id: []const u8,
    label: []const u8,
    entity_type: []const u8,
    criticality: []const u8,
    decayed_confidence: ?f64,
};

pub const CoverageReport = struct {
    summary: CoverageSummary,
    gaps: []CoverageGap,
};

pub const TaxonomyFacetLevel = struct {
    facet_id: u32,
    facet_name: []const u8,
    facet_value: []const u8,
};

pub const TaxonomyNodeImport = struct {
    id: []const u8,
    workspace_id: []const u8,
    doc_id: u64,
    node_id: []const u8,
    label: []const u8,
    levels: []const TaxonomyFacetLevel,
    source_ref: ?[]const u8 = null,
    schema_id: []const u8 = "ghostcrab:taxonomy",
    entity_type: []const u8 = "taxonomy_node",
};

pub fn upsertFacet(db: Database, record: FacetRecord) !void {
    // agent_facts carries three unique constraints (PK id, UNIQUE doc_id, and
    // the partial UNIQUE(source_ref, workspace_id)). INSERT OR REPLACE would
    // silently delete any unrelated fact that happens to collide on doc_id or
    // source_ref; upsert on the id only and surface the other collisions as
    // typed errors instead.
    const stmt = try prepare(db,
        \\INSERT INTO agent_facts(id, schema_id, content, facets_json, workspace_id, doc_id, source_ref)
        \\VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        \\ON CONFLICT(id) DO UPDATE SET
        \\    schema_id = excluded.schema_id,
        \\    content = excluded.content,
        \\    facets = excluded.facets_json,
        \\    facets_json = excluded.facets_json,
        \\    workspace_id = excluded.workspace_id,
        \\    doc_id = excluded.doc_id,
        \\    source_ref = excluded.source_ref
    );
    defer finalize(stmt);
    try bindText(stmt, 1, record.id);
    try bindText(stmt, 2, record.schema_id);
    try bindText(stmt, 3, record.content);
    try bindText(stmt, 4, record.facets_json);
    try bindText(stmt, 5, record.workspace_id);
    try bindInt64(stmt, 6, record.doc_id);
    if (record.source_ref) |source_ref| try bindText(stmt, 7, source_ref) else try bindNull(stmt, 7);

    const rc = c.sqlite3_step(stmt);
    if (rc == c.SQLITE_DONE) return;
    if ((rc & 0xff) == c.SQLITE_CONSTRAINT) {
        const msg = std.mem.span(c.sqlite3_errmsg(db.handle));
        if (std.mem.indexOf(u8, msg, "agent_facts.doc_id") != null) return error.FactDocIdConflict;
        if (std.mem.indexOf(u8, msg, "agent_facts.source_ref") != null) return error.FactSourceRefConflict;
    }
    return error.StepFailed;
}

pub fn importTaxonomyIntoFacets(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    schema_name: []const u8,
    table_name: []const u8,
    chunk_bits: u8,
    nodes: []const TaxonomyNodeImport,
) !void {
    // ~6 statements per node incl. read-modify-write of posting bitmaps;
    // run the import atomically instead of one fsync per statement.
    var tx = try facet_sqlite.Transaction.begin(db);
    defer tx.deinit();

    try facet_sqlite.upsertFacetTable(db, table_id, schema_name, table_name, chunk_bits);

    // (facet_id, value) -> value_id cache plus per-parent children
    // accumulation: without them the import re-queried facet_value_nodes per
    // level (O(n^2) without the natural-key index) and rewrote each hub
    // parent's children bitmap once per child (O(k^2) blob churn).
    var scratch_arena = std.heap.ArenaAllocator.init(allocator);
    defer scratch_arena.deinit();
    const scratch = scratch_arena.allocator();

    var value_cache = FacetValueNodeCache{};
    var children = std.AutoHashMapUnmanaged(u32, std.ArrayList(u32)){};

    for (nodes) |node| {
        const facets_json = try renderTaxonomyFacetsJson(allocator, node);
        defer allocator.free(facets_json);

        try upsertFacet(db, .{
            .id = node.id,
            .schema_id = node.schema_id,
            .content = node.label,
            .facets_json = facets_json,
            .workspace_id = node.workspace_id,
            .doc_id = node.doc_id,
            .source_ref = node.source_ref,
        });

        for (node.levels) |level| {
            try facet_sqlite.upsertFacetDefinition(db, table_id, level.facet_id, level.facet_name);
            try appendPostingDoc(db, allocator, table_id, chunk_bits, level, node.doc_id);
        }

        var level_index: usize = 0;
        while (level_index + 1 < node.levels.len) : (level_index += 1) {
            const parent = node.levels[level_index];
            const child = node.levels[level_index + 1];

            const parent_value_id = try ensureFacetValueNodeId(db, allocator, scratch, &value_cache, table_id, parent.facet_id, parent.facet_value);
            const child_value_id = try ensureFacetValueNodeId(db, allocator, scratch, &value_cache, table_id, child.facet_id, child.facet_value);
            const gop = try children.getOrPut(scratch, parent_value_id);
            if (!gop.found_existing) gop.value_ptr.* = .empty;
            try gop.value_ptr.append(scratch, child_value_id);
        }
    }

    // Flush each parent's accumulated children with a single bitmap
    // read-modify-write instead of one per edge.
    var it = children.iterator();
    while (it.next()) |entry| {
        try appendFacetChildLinks(db, allocator, table_id, entry.key_ptr.*, entry.value_ptr.items);
    }

    try tx.commit();
}

pub fn insertProjection(db: Database, record: ProjectionRecord) !void {
    const stmt = try prepare(db, "INSERT OR REPLACE INTO projections(id, agent_id, scope, proj_type, content, weight, source_ref, source_type, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)");
    defer finalize(stmt);
    try bindText(stmt, 1, record.id);
    try bindText(stmt, 2, record.agent_id);
    if (record.scope) |scope| try bindText(stmt, 3, scope) else try bindNull(stmt, 3);
    try bindText(stmt, 4, record.proj_type);
    try bindText(stmt, 5, record.content);
    if (c.sqlite3_bind_double(stmt, 6, record.weight) != c.SQLITE_OK) return error.BindFailed;
    if (record.source_ref) |source_ref| try bindText(stmt, 7, source_ref) else try bindNull(stmt, 7);
    if (record.source_type) |source_type| try bindText(stmt, 8, source_type) else try bindNull(stmt, 8);
    try bindText(stmt, 9, record.status);
    try stepDone(stmt);
}

pub fn loadTaxonomyFacetRows(
    db: Database,
    allocator: std.mem.Allocator,
    workspace_id: []const u8,
) ![]FacetRecord {
    const stmt = try prepare(db, "SELECT id, schema_id, content, facets_json, workspace_id, doc_id, source_ref FROM agent_facts WHERE workspace_id = ?1 AND schema_id = 'ghostcrab:taxonomy' ORDER BY doc_id");
    defer finalize(stmt);
    try bindText(stmt, 1, workspace_id);

    var rows = std.ArrayList(FacetRecord).empty;
    defer {
        for (rows.items) |row| deinitFacetRecord(allocator, row);
        rows.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(allocator, try facetFromRow(allocator, stmt));
    }

    return rows.toOwnedSlice(allocator);
}

pub fn loadAgentProjections(
    db: Database,
    allocator: std.mem.Allocator,
    agent_id: []const u8,
) ![]ProjectionRecord {
    const stmt = try prepare(db, "SELECT id, agent_id, scope, proj_type, content, weight, source_ref, source_type, status FROM projections WHERE agent_id = ?1 ORDER BY weight DESC, id");
    defer finalize(stmt);
    try bindText(stmt, 1, agent_id);

    var rows = std.ArrayList(ProjectionRecord).empty;
    defer {
        for (rows.items) |row| deinitProjectionRecord(allocator, row);
        rows.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(allocator, try projectionFromRow(allocator, stmt));
    }

    return rows.toOwnedSlice(allocator);
}

pub fn materializeProjections(
    db: Database,
    allocator: std.mem.Allocator,
    agent_id: []const u8,
    workspace_id: ?[]const u8,
    query: []const u8,
    source_refs: ?[]const []const u8,
    limit_n: usize,
) ![]ProjectionRecord {
    const rows = try loadAgentProjections(db, allocator, agent_id);
    defer deinitProjectionRows(allocator, rows);

    var materialized = std.ArrayList(ProjectionRecord).empty;
    defer {
        for (materialized.items) |row| deinitProjectionRecord(allocator, row);
        materialized.deinit(allocator);
    }

    for (rows) |row| {
        if (!std.mem.eql(u8, row.status, "active") and !std.mem.eql(u8, row.status, "blocking")) continue;
        if (!matchesProjectionScope(workspace_id, row.scope)) continue;
        if (!matchesProjectionQuery(row.content, query)) continue;
        if (!matchesSourceRefs(source_refs, row.source_ref)) continue;

        try materialized.append(allocator, .{
            .id = try allocator.dupe(u8, row.id),
            .agent_id = try allocator.dupe(u8, row.agent_id),
            .scope = if (row.scope) |scope| try allocator.dupe(u8, scope) else null,
            .proj_type = try allocator.dupe(u8, row.proj_type),
            .content = try allocator.dupe(u8, row.content),
            .weight = row.weight,
            .source_ref = if (row.source_ref) |source_ref| try allocator.dupe(u8, source_ref) else null,
            .source_type = if (row.source_type) |source_type| try allocator.dupe(u8, source_type) else null,
            .status = try allocator.dupe(u8, row.status),
        });
    }

    std.mem.sort(ProjectionRecord, materialized.items, {}, struct {
        fn lessThan(_: void, lhs: ProjectionRecord, rhs: ProjectionRecord) bool {
            if (lhs.weight != rhs.weight) return lhs.weight > rhs.weight;
            return std.mem.order(u8, lhs.id, rhs.id) == .lt;
        }
    }.lessThan);

    if (materialized.items.len > limit_n) {
        for (materialized.items[limit_n..]) |row| deinitProjectionRecord(allocator, row);
        materialized.shrinkRetainingCapacity(limit_n);
    }

    return materialized.toOwnedSlice(allocator);
}

/// Exact legacy plan selection: never substitutes a global scope. Two rows
/// intentionally signal ambiguity to the caller, regardless of its pack limit.
pub fn selectExactPackProjections(
    db: Database,
    allocator: std.mem.Allocator,
    agent_id: []const u8,
    workspace_id: []const u8,
    scope: []const u8,
    plan_id: ?[]const u8,
) ![]ProjectionRecord {
    if (workspace_id.len == 0 or scope.len == 0 or agent_id.len == 0) return error.BadRequest;
    if (!matchesProjectionScope(workspace_id, scope)) return allocator.alloc(ProjectionRecord, 0);
    const stmt = try prepare(db, "SELECT id, agent_id, scope, proj_type, content, weight, source_ref, source_type, status FROM projections " ++
        "WHERE agent_id = ?1 AND scope = ?2 AND (?3 IS NULL OR id = ?3) " ++
        "AND status IN ('active','blocking') AND (expires_at_unix IS NULL OR expires_at_unix > strftime('%s','now')) " ++
        "ORDER BY id LIMIT 2");
    defer finalize(stmt);
    try bindText(stmt, 1, agent_id);
    try bindText(stmt, 2, scope);
    if (plan_id) |id| try bindText(stmt, 3, id) else try bindNull(stmt, 3);
    var rows = std.ArrayList(ProjectionRecord).empty;
    errdefer {
        for (rows.items) |row| deinitProjectionRecord(allocator, row);
        rows.deinit(allocator);
    }
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(allocator, try projectionFromRow(allocator, stmt));
    }
    return rows.toOwnedSlice(allocator);
}

pub fn materializePackProjections(
    db: Database,
    allocator: std.mem.Allocator,
    agent_id: []const u8,
    workspace_id: ?[]const u8,
    scope: ?[]const u8,
    query: []const u8,
    limit_n: usize,
) ![]ProjectionRecord {
    const rows = try loadAgentProjections(db, allocator, agent_id);
    defer deinitProjectionRows(allocator, rows);

    var materialized = std.ArrayList(ProjectionRecord).empty;
    defer {
        for (materialized.items) |row| deinitProjectionRecord(allocator, row);
        materialized.deinit(allocator);
    }

    for (rows) |row| {
        if (!std.mem.eql(u8, row.status, "active") and !std.mem.eql(u8, row.status, "blocking")) continue;
        if (!matchesProjectionScope(workspace_id, row.scope)) continue;
        if (!matchesPackScope(scope, row.scope)) continue;
        if (!matchesProjectionQuery(row.content, query)) continue;

        try materialized.append(allocator, .{
            .id = try allocator.dupe(u8, row.id),
            .agent_id = try allocator.dupe(u8, row.agent_id),
            .scope = if (row.scope) |value| try allocator.dupe(u8, value) else null,
            .proj_type = try allocator.dupe(u8, row.proj_type),
            .content = try allocator.dupe(u8, row.content),
            .weight = row.weight,
            .source_ref = if (row.source_ref) |value| try allocator.dupe(u8, value) else null,
            .source_type = if (row.source_type) |value| try allocator.dupe(u8, value) else null,
            .status = try allocator.dupe(u8, row.status),
        });
    }

    std.mem.sort(ProjectionRecord, materialized.items, {}, struct {
        fn lessThan(_: void, lhs: ProjectionRecord, rhs: ProjectionRecord) bool {
            const lhs_constraint = std.mem.eql(u8, lhs.proj_type, "CONSTRAINT");
            const rhs_constraint = std.mem.eql(u8, rhs.proj_type, "CONSTRAINT");
            if (lhs_constraint != rhs_constraint) return lhs_constraint;
            if (lhs.weight != rhs.weight) return lhs.weight > rhs.weight;
            return std.mem.order(u8, lhs.id, rhs.id) == .lt;
        }
    }.lessThan);

    if (materialized.items.len > limit_n) {
        for (materialized.items[limit_n..]) |row| deinitProjectionRecord(allocator, row);
        materialized.shrinkRetainingCapacity(limit_n);
    }

    return materialized.toOwnedSlice(allocator);
}

pub fn projectionRelevance(
    db: Database,
    allocator: std.mem.Allocator,
    workspace_id: ?[]const u8,
    entity_name: []const u8,
    query: []const u8,
) !f32 {
    const projections = try loadAllProjections(db, allocator);
    defer {
        for (projections) |row| deinitProjectionRecord(allocator, row);
        allocator.free(projections);
    }

    var score: f32 = 0;
    for (projections) |row| {
        if (!std.mem.eql(u8, row.status, "active") and !std.mem.eql(u8, row.status, "blocking")) continue;
        if (workspace_id != null and row.scope != null and !std.mem.eql(u8, workspace_id.?, row.scope.?)) continue;
        if (matchesText(row.content, entity_name)) {
            score += row.weight;
        } else if (query.len > 0 and matchesText(row.content, query)) {
            score += row.weight * 0.6;
        }
    }
    return score;
}

fn scoreProjectionRow(row: ProjectionRecord, entity_name: []const u8, query: []const u8) f32 {
    if (matchesText(row.content, entity_name)) return row.weight;
    if (query.len > 0 and matchesText(row.content, query)) return row.weight * 0.6;
    return 0;
}

pub fn materializeRelevanceProjections(
    db: Database,
    allocator: std.mem.Allocator,
    agent_id: []const u8,
    scope: ?[]const u8,
    entity_name: []const u8,
    query: []const u8,
    limit_n: usize,
) ![]ProjectionRecord {
    const rows = try loadAgentProjections(db, allocator, agent_id);
    defer deinitProjectionRows(allocator, rows);

    const ScoredRow = struct {
        row: ProjectionRecord,
        score: f32,
    };

    var materialized = std.ArrayList(ScoredRow).empty;
    errdefer {
        for (materialized.items) |entry| deinitProjectionRecord(allocator, entry.row);
        materialized.deinit(allocator);
    }

    for (rows) |row| {
        if (!std.mem.eql(u8, row.status, "active") and !std.mem.eql(u8, row.status, "blocking")) continue;
        if (!matchesPackScope(scope, row.scope)) continue;

        const score = scoreProjectionRow(row, entity_name, query);
        if (score <= 0) continue;

        try materialized.append(allocator, .{
            .row = .{
                .id = try allocator.dupe(u8, row.id),
                .agent_id = try allocator.dupe(u8, row.agent_id),
                .scope = if (row.scope) |value| try allocator.dupe(u8, value) else null,
                .proj_type = try allocator.dupe(u8, row.proj_type),
                .content = try allocator.dupe(u8, row.content),
                .weight = row.weight,
                .source_ref = if (row.source_ref) |value| try allocator.dupe(u8, value) else null,
                .source_type = if (row.source_type) |value| try allocator.dupe(u8, value) else null,
                .status = try allocator.dupe(u8, row.status),
            },
            .score = score,
        });
    }

    std.mem.sort(ScoredRow, materialized.items, {}, struct {
        fn lessThan(_: void, lhs: ScoredRow, rhs: ScoredRow) bool {
            if (lhs.score != rhs.score) return lhs.score > rhs.score;
            if (lhs.row.weight != rhs.row.weight) return lhs.row.weight > rhs.row.weight;
            return std.mem.order(u8, lhs.row.id, rhs.row.id) == .lt;
        }
    }.lessThan);

    if (materialized.items.len > limit_n) {
        for (materialized.items[limit_n..]) |entry| deinitProjectionRecord(allocator, entry.row);
        materialized.shrinkRetainingCapacity(limit_n);
    }

    var out = try allocator.alloc(ProjectionRecord, materialized.items.len);
    for (materialized.items, 0..) |entry, index| {
        out[index] = entry.row;
    }
    materialized.clearRetainingCapacity();
    materialized.deinit(allocator);
    return out;
}

pub fn materializeTaxonomyProjections(
    db: Database,
    allocator: std.mem.Allocator,
    workspace_id: []const u8,
    agent_id: []const u8,
) !usize {
    const rows = try loadTaxonomyFacetRows(db, allocator, workspace_id);
    defer {
        for (rows) |row| deinitFacetRecord(allocator, row);
        allocator.free(rows);
    }

    var inserted: usize = 0;
    var tx = try facet_sqlite.Transaction.begin(db);
    defer tx.deinit();
    for (rows) |row| {
        const node_id = try extractFacetIdentity(allocator, row);
        defer allocator.free(node_id);
        const entity_type = try extractFacetJsonValue(allocator, row.facets_json, "entity_type");
        defer if (entity_type) |value| allocator.free(value);

        const content = try renderTaxonomyProjectionContent(allocator, row, node_id);
        defer allocator.free(content);
        const projection_id = try std.fmt.allocPrint(allocator, "taxonomy-proj:{s}", .{row.id});
        defer allocator.free(projection_id);

        try insertProjection(db, .{
            .id = projection_id,
            .agent_id = agent_id,
            .scope = workspace_id,
            .proj_type = "FACT",
            .content = content,
            .weight = 0.8,
            .source_ref = row.id,
            .source_type = entity_type orelse "taxonomy",
            .status = "active",
        });
        inserted += 1;
    }
    try tx.commit();

    return inserted;
}

pub fn coverageReport(
    db: Database,
    allocator: std.mem.Allocator,
    workspace_id: []const u8,
    entity_types: ?[]const []const u8,
) !CoverageReport {
    const facet_rows_total = try countWorkspaceFacetRows(db, workspace_id);
    const facets = try loadWorkspaceFacets(db, allocator, workspace_id);
    defer {
        for (facets) |row| deinitFacetRecord(allocator, row);
        allocator.free(facets);
    }
    const graph_entities = try loadWorkspaceGraphEntities(db, allocator, workspace_id, entity_types);
    defer {
        for (graph_entities) |entity| {
            allocator.free(entity.name);
            allocator.free(entity.entity_type);
        }
        allocator.free(graph_entities);
    }

    // Name -> record map: the per-facet linear scans over all entities made
    // the report O(facets x entities).
    var entities_by_name = std.StringHashMap(GraphEntityRecord).init(allocator);
    defer entities_by_name.deinit();
    for (graph_entities) |entity| {
        _ = try entities_by_name.put(entity.name, entity);
    }

    const projection_rows = try loadWorkspaceProjectionCount(db, workspace_id);
    var gap_rows = std.ArrayList(CoverageGap).empty;
    defer {
        for (gap_rows.items) |gap| deinitCoverageGap(allocator, gap);
        gap_rows.deinit(allocator);
    }

    var total_nodes: usize = 0;
    var covered: usize = 0;
    for (facets) |facet| {
        if (!isOntologyOrTaxonomy(facet.schema_id)) continue;

        const maybe_entity_type = try extractFacetJsonValue(allocator, facet.facets_json, "entity_type");
        defer if (maybe_entity_type) |value| allocator.free(value);
        if (maybe_entity_type) |entity_type| {
            if (!matchesEntityTypes(entity_types, entity_type)) continue;
        } else if (entity_types != null and entity_types.?.len > 0) {
            continue;
        }

        total_nodes += 1;
        const node_id = try extractFacetIdentity(allocator, facet);
        defer allocator.free(node_id);
        if (entities_by_name.contains(node_id)) {
            covered += 1;
            continue;
        }

        const label = try extractFacetJsonValue(allocator, facet.facets_json, "label");
        defer if (label) |value| allocator.free(value);
        const criticality = try extractFacetJsonValue(allocator, facet.facets_json, "criticality");
        defer if (criticality) |value| allocator.free(value);
        const gap_label = label orelse facet.content;
        const gap_entity_type = maybe_entity_type orelse "unknown";
        const decayed_confidence = if (findGraphEntityIdByName(&entities_by_name, node_id, gap_label, gap_entity_type)) |entity_id|
            @as(f64, @floatCast(try graph_sqlite.confidenceDecay(db, entity_id, 90)))
        else
            null;

        try gap_rows.append(allocator, .{
            .id = try allocator.dupe(u8, node_id),
            .label = try allocator.dupe(u8, gap_label),
            .entity_type = try allocator.dupe(u8, gap_entity_type),
            .criticality = try allocator.dupe(u8, criticality orelse "normal"),
            .decayed_confidence = decayed_confidence,
        });
    }

    std.mem.sort(CoverageGap, gap_rows.items, {}, struct {
        fn lessThan(_: void, lhs: CoverageGap, rhs: CoverageGap) bool {
            return std.mem.order(u8, lhs.id, rhs.id) == .lt;
        }
    }.lessThan);

    return .{
        .summary = .{
            .workspace_id = try allocator.dupe(u8, workspace_id),
            .covered_nodes = covered,
            .total_nodes = total_nodes,
            .graph_entities = graph_entities.len,
            .facet_rows = facet_rows_total,
            .projection_rows = projection_rows,
            .coverage_ratio = if (total_nodes == 0) null else @as(f64, @floatFromInt(covered)) / @as(f64, @floatFromInt(total_nodes)),
        },
        .gaps = try gap_rows.toOwnedSlice(allocator),
    };
}

pub fn coverage(
    db: Database,
    allocator: std.mem.Allocator,
    workspace_id: []const u8,
    entity_types: ?[]const []const u8,
) !CoverageSummary {
    const report = try coverageReport(db, allocator, workspace_id, entity_types);
    defer {
        for (report.gaps) |gap| deinitCoverageGap(allocator, gap);
        allocator.free(report.gaps);
    }
    return report.summary;
}

pub fn coverageByDomain(
    db: Database,
    allocator: std.mem.Allocator,
    domain_or_workspace: []const u8,
    entity_types: ?[]const []const u8,
) !?CoverageSummary {
    const resolved = try resolveWorkspace(db, allocator, domain_or_workspace);
    defer if (resolved) |value| allocator.free(value);
    if (resolved == null) return null;
    return try coverage(db, allocator, resolved.?, entity_types);
}

/// TOON encoding of `coverage`, parity for `mb_ontology.coverage_toon`.
pub fn coverageToon(
    db: Database,
    allocator: std.mem.Allocator,
    workspace_id: []const u8,
    entity_types: ?[]const []const u8,
) ![]u8 {
    const report = try coverageReport(db, allocator, workspace_id, entity_types);
    defer {
        for (report.gaps) |gap| deinitCoverageGap(allocator, gap);
        allocator.free(report.gaps);
        allocator.free(report.summary.workspace_id);
    }
    return try toon_exports.encodeCoverageReportAlloc(allocator, report, toon_exports.default_options);
}

/// TOON encoding of `coverageByDomain`, parity for
/// `mb_ontology.coverage_by_domain_toon`. Returns `null` when the domain
/// or workspace cannot be resolved.
pub fn coverageByDomainToon(
    db: Database,
    allocator: std.mem.Allocator,
    domain_or_workspace: []const u8,
    entity_types: ?[]const []const u8,
) !?[]u8 {
    const resolved = try resolveWorkspace(db, allocator, domain_or_workspace);
    defer if (resolved) |value| allocator.free(value);
    if (resolved == null) return null;
    return try coverageToon(db, allocator, resolved.?, entity_types);
}

/// Resolve a domain or workspace identifier and run `graph.marketplace_search`,
/// mirroring `mb_ontology.marketplace_search_by_domain`. Returns an empty
/// slice if the domain cannot be resolved (the PG SRF returns zero rows).
pub fn marketplaceSearchByDomain(
    db: Database,
    allocator: std.mem.Allocator,
    query: []const u8,
    domain_or_workspace: []const u8,
    min_confidence: f32,
    max_hops: usize,
    limit: usize,
) ![]graph_sqlite.MarketplaceResult {
    const resolved = try resolveWorkspace(db, allocator, domain_or_workspace);
    defer if (resolved) |value| allocator.free(value);
    const domain_filter: ?[]const u8 = if (resolved) |value| value else null;
    // Scope the traversal to the resolved workspace as well as filtering on it:
    // the unscoped entry point walks every workspace's entities.
    return try graph_sqlite.marketplaceSearchWorkspace(
        db,
        allocator,
        resolved,
        query,
        domain_filter,
        min_confidence,
        max_hops,
        limit,
    );
}

pub fn resolveWorkspace(
    db: Database,
    allocator: std.mem.Allocator,
    domain_or_workspace: []const u8,
) !?[]const u8 {
    if (domain_or_workspace.len == 0) return null;

    // Exact workspace id wins; otherwise match the declared domain by
    // equality with a deterministic tie-break. Substring matching against
    // the raw profile JSON resolved arbitrary workspaces for inputs like
    // "domain" or "{".
    {
        const stmt = try prepare(db, "SELECT workspace_id FROM workspaces WHERE workspace_id = ?1 LIMIT 1");
        defer finalize(stmt);
        try bindText(stmt, 1, domain_or_workspace);
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_ROW) return try dupeColumnText(allocator, stmt, 0);
        if (rc != c.SQLITE_DONE) return error.StepFailed;
    }

    const stmt = try prepare(
        db,
        "SELECT workspace_id FROM workspaces WHERE json_extract(domain_profile_json, '$.domain') = ?1 OR domain_profile = ?1 ORDER BY workspace_id ASC LIMIT 1",
    );
    defer finalize(stmt);
    try bindText(stmt, 1, domain_or_workspace);
    const rc = c.sqlite3_step(stmt);
    if (rc == c.SQLITE_ROW) return try dupeColumnText(allocator, stmt, 0);
    if (rc != c.SQLITE_DONE) return error.StepFailed;
    return null;
}

fn facetFromRow(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt) !FacetRecord {
    return .{
        .id = try dupeColumnText(allocator, stmt, 0),
        .schema_id = try dupeColumnText(allocator, stmt, 1),
        .content = try dupeColumnText(allocator, stmt, 2),
        .facets_json = try dupeColumnText(allocator, stmt, 3),
        .workspace_id = try dupeColumnText(allocator, stmt, 4),
        .doc_id = try columnU64(stmt, 5),
        .source_ref = if (c.sqlite3_column_type(stmt, 6) == c.SQLITE_NULL) null else try dupeColumnText(allocator, stmt, 6),
    };
}

fn projectionFromRow(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt) !ProjectionRecord {
    return .{
        .id = try dupeColumnText(allocator, stmt, 0),
        .agent_id = try dupeColumnText(allocator, stmt, 1),
        .scope = if (c.sqlite3_column_type(stmt, 2) == c.SQLITE_NULL) null else try dupeColumnText(allocator, stmt, 2),
        .proj_type = try dupeColumnText(allocator, stmt, 3),
        .content = try dupeColumnText(allocator, stmt, 4),
        .weight = @floatCast(c.sqlite3_column_double(stmt, 5)),
        .source_ref = if (c.sqlite3_column_type(stmt, 6) == c.SQLITE_NULL) null else try dupeColumnText(allocator, stmt, 6),
        .source_type = if (c.sqlite3_column_type(stmt, 7) == c.SQLITE_NULL) null else try dupeColumnText(allocator, stmt, 7),
        .status = try dupeColumnText(allocator, stmt, 8),
    };
}

fn loadAllProjections(db: Database, allocator: std.mem.Allocator) ![]ProjectionRecord {
    const stmt = try prepare(db, "SELECT id, agent_id, scope, proj_type, content, weight, source_ref, source_type, status FROM projections");
    defer finalize(stmt);
    var rows = std.ArrayList(ProjectionRecord).empty;
    defer {
        for (rows.items) |row| deinitProjectionRecord(allocator, row);
        rows.deinit(allocator);
    }
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(allocator, try projectionFromRow(allocator, stmt));
    }
    return rows.toOwnedSlice(allocator);
}

fn countWorkspaceFacetRows(db: Database, workspace_id: []const u8) !usize {
    const stmt = try prepare(db, "SELECT COUNT(*) FROM agent_facts WHERE workspace_id = ?1");
    defer finalize(stmt);
    try bindText(stmt, 1, workspace_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StepFailed;
    return @intCast(c.sqlite3_column_int64(stmt, 0));
}

fn loadWorkspaceFacets(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8) ![]FacetRecord {
    // Coverage only consumes ontology/taxonomy rows; filtering in SQL uses
    // the (workspace_id, schema_id) index instead of duping every fact.
    const stmt = try prepare(db, "SELECT id, schema_id, content, facets_json, workspace_id, doc_id, source_ref FROM agent_facts WHERE workspace_id = ?1 AND schema_id IN ('mindbrain:ontology', 'ghostcrab:ontology', 'ghostcrab:taxonomy')");
    defer finalize(stmt);
    try bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(FacetRecord).empty;
    defer {
        for (rows.items) |row| deinitFacetRecord(allocator, row);
        rows.deinit(allocator);
    }
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(allocator, try facetFromRow(allocator, stmt));
    }
    return rows.toOwnedSlice(allocator);
}

const GraphEntityRecord = struct {
    entity_id: u32,
    name: []const u8,
    entity_type: []const u8,
};

fn loadWorkspaceGraphEntities(
    db: Database,
    allocator: std.mem.Allocator,
    workspace_id: []const u8,
    entity_types: ?[]const []const u8,
) ![]GraphEntityRecord {
    const stmt = try prepare(db, "SELECT entity_id, name, entity_type FROM graph_entity WHERE workspace_id = ?1 AND deprecated_at IS NULL");
    defer finalize(stmt);
    try bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(GraphEntityRecord).empty;
    defer {
        for (rows.items) |row| {
            allocator.free(row.name);
            allocator.free(row.entity_type);
        }
        rows.deinit(allocator);
    }
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        const entity_type = try dupeColumnText(allocator, stmt, 2);
        errdefer allocator.free(entity_type);
        if (!matchesEntityTypes(entity_types, entity_type)) {
            allocator.free(entity_type);
            continue;
        }
        try rows.append(allocator, .{
            .entity_id = try columnU32(stmt, 0),
            .name = try dupeColumnText(allocator, stmt, 1),
            .entity_type = entity_type,
        });
    }
    return rows.toOwnedSlice(allocator);
}

fn findGraphEntityIdByName(
    entities_by_name: *const std.StringHashMap(GraphEntityRecord),
    node_id: []const u8,
    label: []const u8,
    entity_type: []const u8,
) ?u32 {
    if (entities_by_name.get(node_id)) |entity| {
        if (std.mem.eql(u8, entity.entity_type, entity_type)) return entity.entity_id;
    }
    if (entities_by_name.get(label)) |entity| {
        if (std.mem.eql(u8, entity.entity_type, entity_type)) return entity.entity_id;
    }
    return null;
}

fn findGraphEntityId(
    graph_entities: []const GraphEntityRecord,
    node_id: []const u8,
    label: []const u8,
    entity_type: []const u8,
) ?u32 {
    for (graph_entities) |entity| {
        if (!std.mem.eql(u8, entity.entity_type, entity_type)) continue;
        if (std.mem.eql(u8, entity.name, node_id) or std.mem.eql(u8, entity.name, label)) {
            return entity.entity_id;
        }
    }
    return null;
}

fn loadWorkspaceProjectionCount(db: Database, workspace_id: []const u8) !usize {
    // The single `scope = ?1 OR scope LIKE ?1 || ':%' OR scope IS NULL`
    // predicate was unindexable (expression LIKE plus a NULL arm the partial
    // idx_proj_scope cannot serve) and scanned all projections per coverage
    // report. Split into three disjoint, indexable counts: exact scope and
    // the half-open [scope ++ ':', scope ++ ';') range use idx_proj_scope
    // (';' is the code point after ':'), the NULL arm uses
    // idx_proj_scope_null.
    var total: usize = 0;
    total += try countProjectionsBound(db, "SELECT COUNT(*) FROM projections WHERE scope = ?1", workspace_id);
    total += try countProjectionsBound(db, "SELECT COUNT(*) FROM projections WHERE scope >= ?1 || ':' AND scope < ?1 || ';'", workspace_id);

    const null_stmt = try prepare(db, "SELECT COUNT(*) FROM projections WHERE scope IS NULL");
    defer finalize(null_stmt);
    if (c.sqlite3_step(null_stmt) != c.SQLITE_ROW) return total;
    total += @as(usize, @intCast(c.sqlite3_column_int64(null_stmt, 0)));
    return total;
}

fn countProjectionsBound(db: Database, sql: []const u8, workspace_id: []const u8) !usize {
    const stmt = try prepare(db, sql);
    defer finalize(stmt);
    try bindText(stmt, 1, workspace_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return 0;
    return @intCast(c.sqlite3_column_int64(stmt, 0));
}

fn matchesEntityTypes(entity_types: ?[]const []const u8, entity_type: []const u8) bool {
    if (entity_types == null or entity_types.?.len == 0) return true;
    for (entity_types.?) |candidate| if (std.mem.eql(u8, candidate, entity_type)) return true;
    return false;
}

fn matchesProjectionScope(workspace_id: ?[]const u8, scope: ?[]const u8) bool {
    if (workspace_id == null or workspace_id.?.len == 0) return true;
    if (scope == null) return true;
    if (std.mem.eql(u8, workspace_id.?, scope.?)) return true;
    return scope.?.len > workspace_id.?.len and
        scope.?[workspace_id.?.len] == ':' and
        std.mem.startsWith(u8, scope.?, workspace_id.?);
}

fn matchesPackScope(requested_scope: ?[]const u8, scope: ?[]const u8) bool {
    if (requested_scope == null or requested_scope.?.len == 0) return true;
    if (scope == null) return true;
    return std.mem.eql(u8, requested_scope.?, scope.?);
}

fn matchesProjectionQuery(content: []const u8, query: []const u8) bool {
    if (query.len == 0) return true;
    return matchesText(content, query);
}

fn matchesSourceRefs(source_refs: ?[]const []const u8, source_ref: ?[]const u8) bool {
    if (source_refs == null or source_refs.?.len == 0) return true;
    if (source_ref == null) return false;
    for (source_refs.?) |candidate| {
        if (std.mem.eql(u8, candidate, source_ref.?)) return true;
    }
    return false;
}

fn matchesText(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0) return false;
    return std.ascii.indexOfIgnoreCase(haystack, needle) != null;
}

fn isOntologyOrTaxonomy(schema_id: []const u8) bool {
    return std.mem.eql(u8, schema_id, "mindbrain:ontology") or
        std.mem.eql(u8, schema_id, "ghostcrab:ontology") or
        std.mem.eql(u8, schema_id, "ghostcrab:taxonomy");
}

fn countOntologyRows(facets: []const FacetRecord) usize {
    var count: usize = 0;
    for (facets) |facet| {
        if (isOntologyOrTaxonomy(facet.schema_id)) count += 1;
    }
    return count;
}

// Real JSON parsing: the previous substring scanning truncated values at
// escaped quotes, matched keys inside nested objects, and missed non-string
// values entirely.
fn extractFacetJsonValue(allocator: std.mem.Allocator, facets_json: []const u8, key_name: []const u8) !?[]const u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, facets_json, .{}) catch return null;
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const value = parsed.value.object.get(key_name) orelse return null;
    return try jsonScalarToOwnedString(allocator, value);
}

fn jsonScalarToOwnedString(allocator: std.mem.Allocator, value: std.json.Value) !?[]const u8 {
    return switch (value) {
        .string => |text| try allocator.dupe(u8, text),
        .integer => |number| try std.fmt.allocPrint(allocator, "{d}", .{number}),
        .float => |number| try std.fmt.allocPrint(allocator, "{d}", .{number}),
        .number_string => |text| try allocator.dupe(u8, text),
        .bool => |flag| try allocator.dupe(u8, if (flag) "true" else "false"),
        else => null,
    };
}

fn extractFacetIdentity(allocator: std.mem.Allocator, facet: FacetRecord) ![]const u8 {
    const keys = [_][]const u8{ "node_id", "entity_id", "name", "label" };
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, facet.facets_json, .{}) catch
        return try allocator.dupe(u8, facet.content);
    defer parsed.deinit();
    if (parsed.value == .object) {
        for (keys) |key| {
            const value = parsed.value.object.get(key) orelse continue;
            if (try jsonScalarToOwnedString(allocator, value)) |text| {
                if (text.len > 0) return text;
                allocator.free(text);
            }
        }
    }
    return try allocator.dupe(u8, facet.content);
}

fn renderTaxonomyFacetsJson(allocator: std.mem.Allocator, node: TaxonomyNodeImport) ![]u8 {
    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(allocator);

    try buf.appendSlice(allocator, "{\"node_id\":\"");
    try appendJsonEscaped(&buf, allocator, node.node_id);
    try buf.appendSlice(allocator, "\",\"label\":\"");
    try appendJsonEscaped(&buf, allocator, node.label);
    try buf.appendSlice(allocator, "\",\"entity_type\":\"");
    try appendJsonEscaped(&buf, allocator, node.entity_type);
    try buf.appendSlice(allocator, "\"");

    for (node.levels) |level| {
        try buf.append(allocator, ',');
        try buf.append(allocator, '"');
        try appendJsonEscaped(&buf, allocator, level.facet_name);
        try buf.appendSlice(allocator, "\":\"");
        try appendJsonEscaped(&buf, allocator, level.facet_value);
        try buf.append(allocator, '"');
    }

    try buf.append(allocator, '}');
    return buf.toOwnedSlice(allocator);
}

fn appendPostingDoc(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    chunk_bits: u8,
    level: TaxonomyFacetLevel,
    doc_id: u64,
) !void {
    // Guard the casts: chunk ids and in-chunk ids are u32 roaring values, so
    // chunk_bits above 32 or a doc_id at/above 2^(32 + chunk_bits) cannot be
    // represented and previously tripped @intCast safety panics.
    if (chunk_bits > 32) return error.ValueOutOfRange;
    const shift_bits: u6 = @intCast(chunk_bits);
    const chunk_id = std.math.cast(u32, doc_id >> shift_bits) orelse return error.ValueOutOfRange;
    const chunk_mask: u64 = (@as(u64, 1) << shift_bits) - 1;
    const in_chunk_id: u32 = @intCast(doc_id & chunk_mask);

    var merged = facet_sqlite.loadPostingBitmap(db, table_id, level.facet_id, level.facet_value, chunk_id) catch |err| switch (err) {
        error.MissingRow => try roaring.Bitmap.empty(),
        else => return err,
    };
    defer merged.deinit();
    merged.add(in_chunk_id);

    try facet_sqlite.upsertPostingBitmap(db, allocator, table_id, level.facet_id, level.facet_value, chunk_id, merged);
}

const FacetValueNodeCache = struct {
    // Keys are "{facet_id}\x00{facet_value}", allocated in the caller's
    // scratch arena alongside the map storage.
    map: std.StringHashMapUnmanaged(u32) = .{},
    next_value_id: ?u32 = null,
};

fn ensureFacetValueNodeId(
    db: Database,
    allocator: std.mem.Allocator,
    scratch: std.mem.Allocator,
    cache: *FacetValueNodeCache,
    table_id: u64,
    facet_id: u32,
    facet_value: []const u8,
) !u32 {
    const key = try std.fmt.allocPrint(scratch, "{d}\x00{s}", .{ facet_id, facet_value });
    if (cache.map.get(key)) |value_id| return value_id;

    const existing_stmt = try prepare(db, "SELECT value_id FROM facet_value_nodes WHERE table_id = ?1 AND facet_id = ?2 AND facet_value = ?3");
    defer finalize(existing_stmt);
    try bindInt64(existing_stmt, 1, table_id);
    try bindInt64(existing_stmt, 2, facet_id);
    try bindText(existing_stmt, 3, facet_value);
    if (c.sqlite3_step(existing_stmt) == c.SQLITE_ROW) {
        const value_id = try columnU32(existing_stmt, 0);
        try cache.map.put(scratch, key, value_id);
        return value_id;
    }

    if (cache.next_value_id == null) {
        const next_stmt = try prepare(db, "SELECT COALESCE(MAX(value_id), 0) + 1 FROM facet_value_nodes WHERE table_id = ?1");
        defer finalize(next_stmt);
        try bindInt64(next_stmt, 1, table_id);
        if (c.sqlite3_step(next_stmt) != c.SQLITE_ROW) return error.MissingRow;
        cache.next_value_id = try columnU32(next_stmt, 0);
    }
    const value_id = cache.next_value_id.?;
    cache.next_value_id = value_id + 1;
    try facet_sqlite.upsertFacetValueNode(db, allocator, table_id, value_id, facet_id, facet_value, null);
    try cache.map.put(scratch, key, value_id);
    return value_id;
}

fn appendFacetChildLinks(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    parent_value_id: u32,
    child_value_ids: []const u32,
) !void {
    if (child_value_ids.len == 0) return;

    const stmt = try prepare(db, "SELECT facet_id, facet_value, children_blob FROM facet_value_nodes WHERE table_id = ?1 AND value_id = ?2");
    defer finalize(stmt);
    try bindInt64(stmt, 1, table_id);
    try bindInt64(stmt, 2, parent_value_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;

    const facet_id = try columnU32(stmt, 0);
    const facet_value = try dupeColumnText(allocator, stmt, 1);
    defer allocator.free(facet_value);

    var child_bitmap = if (c.sqlite3_column_type(stmt, 2) == c.SQLITE_NULL)
        try roaring.Bitmap.empty()
    else blk: {
        const blob_len = c.sqlite3_column_bytes(stmt, 2);
        const blob_ptr = c.sqlite3_column_blob(stmt, 2) orelse return error.MissingRow;
        const blob: []const u8 = @as([*]const u8, @ptrCast(blob_ptr))[0..@intCast(blob_len)];
        break :blk try roaring.Bitmap.deserializePortable(blob);
    };
    defer child_bitmap.deinit();

    for (child_value_ids) |child_value_id| child_bitmap.add(child_value_id);
    const child_ids = try child_bitmap.toArray(allocator);
    defer allocator.free(child_ids);

    try facet_sqlite.upsertFacetValueNode(db, allocator, table_id, parent_value_id, facet_id, facet_value, child_ids);
}

fn appendJsonEscaped(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, value: []const u8) !void {
    for (value) |char| switch (char) {
        '\\' => try buf.appendSlice(allocator, "\\\\"),
        '"' => try buf.appendSlice(allocator, "\\\""),
        '\n' => try buf.appendSlice(allocator, "\\n"),
        '\r' => try buf.appendSlice(allocator, "\\r"),
        '\t' => try buf.appendSlice(allocator, "\\t"),
        else => try buf.append(allocator, char),
    };
}

fn renderTaxonomyProjectionContent(allocator: std.mem.Allocator, row: FacetRecord, node_id: []const u8) ![]u8 {
    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(allocator);

    try appendPrint(&buf, allocator, "Taxonomy node {s}", .{node_id});

    const label = try extractFacetJsonValue(allocator, row.facets_json, "label");
    defer if (label) |value| allocator.free(value);
    if (label) |value| {
        if (!std.mem.eql(u8, value, node_id)) {
            try appendPrint(&buf, allocator, " labeled {s}", .{value});
        }
    }

    const domain = try extractFacetJsonValue(allocator, row.facets_json, "domain");
    defer if (domain) |value| allocator.free(value);
    if (domain) |value| {
        try appendPrint(&buf, allocator, " in domain {s}", .{value});
    }

    const category = try extractFacetJsonValue(allocator, row.facets_json, "category");
    defer if (category) |value| allocator.free(value);
    if (category) |value| {
        try appendPrint(&buf, allocator, " under category {s}", .{value});
    }

    const main_category = try extractFacetJsonValue(allocator, row.facets_json, "main_category");
    defer if (main_category) |value| allocator.free(value);
    if (main_category) |value| {
        try appendPrint(&buf, allocator, " with main category {s}", .{value});
    }

    return buf.toOwnedSlice(allocator);
}

fn appendPrint(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    comptime fmt: []const u8,
    args: anytype,
) !void {
    const text = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(text);
    try buf.appendSlice(allocator, text);
}

fn containsGraphEntity(entities: []const GraphEntityRecord, name: []const u8) bool {
    for (entities) |entity| if (std.mem.eql(u8, entity.name, name)) return true;
    return false;
}

fn lowerOwned(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    const output = try allocator.alloc(u8, input.len);
    for (input, 0..) |char, index| output[index] = std.ascii.toLower(char);
    return output;
}

fn deinitFacetRecord(allocator: std.mem.Allocator, record: FacetRecord) void {
    allocator.free(record.id);
    allocator.free(record.schema_id);
    allocator.free(record.content);
    allocator.free(record.facets_json);
    allocator.free(record.workspace_id);
    if (record.source_ref) |source_ref| allocator.free(source_ref);
}

pub fn deinitProjectionRecord(allocator: std.mem.Allocator, record: ProjectionRecord) void {
    allocator.free(record.id);
    allocator.free(record.agent_id);
    if (record.scope) |scope| allocator.free(scope);
    allocator.free(record.proj_type);
    allocator.free(record.content);
    if (record.source_ref) |source_ref| allocator.free(source_ref);
    if (record.source_type) |source_type| allocator.free(source_type);
    allocator.free(record.status);
}

pub fn deinitProjectionRows(allocator: std.mem.Allocator, rows: []ProjectionRecord) void {
    for (rows) |row| deinitProjectionRecord(allocator, row);
    allocator.free(rows);
}

fn deinitCoverageGap(allocator: std.mem.Allocator, gap: CoverageGap) void {
    allocator.free(gap.id);
    allocator.free(gap.label);
    allocator.free(gap.entity_type);
    allocator.free(gap.criticality);
}

fn prepare(db: Database, sql: []const u8) Error!*c.sqlite3_stmt {
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
        return error.PrepareFailed;
    }
    return stmt.?;
}

fn finalize(stmt: *c.sqlite3_stmt) void {
    _ = c.sqlite3_finalize(stmt);
}

fn stepDone(stmt: *c.sqlite3_stmt) Error!void {
    if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
}

fn bindInt64(stmt: *c.sqlite3_stmt, index: c_int, value: anytype) Error!void {
    if (c.sqlite3_bind_int64(stmt, index, @intCast(value)) != c.SQLITE_OK) return error.BindFailed;
}

fn bindText(stmt: *c.sqlite3_stmt, index: c_int, value: []const u8) Error!void {
    if (c.sqlite3_bind_text(stmt, index, value.ptr, @intCast(value.len), facet_sqlite.sqliteTransient()) != c.SQLITE_OK) {
        return error.BindFailed;
    }
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

fn columnU64(stmt: *c.sqlite3_stmt, index: c_int) Error!u64 {
    const value = c.sqlite3_column_int64(stmt, index);
    return std.math.cast(u64, value) orelse error.ValueOutOfRange;
}

fn columnU32(stmt: *c.sqlite3_stmt, index: c_int) Error!u32 {
    const value = c.sqlite3_column_int64(stmt, index);
    return std.math.cast(u32, value) orelse error.ValueOutOfRange;
}

test "ontology sqlite stores taxonomy facet rows and projections" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try upsertFacet(db, .{
        .id = "facet-taxonomy-1",
        .schema_id = "ghostcrab:taxonomy",
        .content = "Physics",
        .facets_json = "{\"entity_type\":\"taxonomy_node\"}",
        .workspace_id = "default",
        .doc_id = 101,
        .source_ref = "taxonomy:physics",
    });
    try insertProjection(db, .{
        .id = "proj-1",
        .agent_id = "agent-a",
        .scope = "default",
        .proj_type = "FACT",
        .content = "Physics is a taxonomy node",
        .weight = 0.9,
        .source_ref = "facet-taxonomy-1",
        .source_type = "taxonomy",
        .status = "active",
    });

    const taxonomy_rows = try loadTaxonomyFacetRows(db, std.testing.allocator, "default");
    defer {
        for (taxonomy_rows) |row| deinitFacetRecord(std.testing.allocator, row);
        std.testing.allocator.free(taxonomy_rows);
    }
    try std.testing.expectEqual(@as(usize, 1), taxonomy_rows.len);
    try std.testing.expectEqualStrings("Physics", taxonomy_rows[0].content);

    const projections = try loadAgentProjections(db, std.testing.allocator, "agent-a");
    defer {
        for (projections) |row| deinitProjectionRecord(std.testing.allocator, row);
        std.testing.allocator.free(projections);
    }
    try std.testing.expectEqual(@as(usize, 1), projections.len);
    try std.testing.expectEqualStrings("FACT", projections[0].proj_type);
    try std.testing.expectEqualStrings("facet-taxonomy-1", projections[0].source_ref.?);
}

test "ontology sqlite resolves workspace, projection relevance, and coverage" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec("INSERT INTO workspaces(workspace_id, domain_profile_json) VALUES ('default', '{\"domain\":\"ghostcrab\"}')");

    try upsertFacet(db, .{
        .id = "facet-1",
        .schema_id = "ghostcrab:taxonomy",
        .content = "Ada",
        .facets_json = "{\"node_id\":\"Ada\",\"entity_type\":\"person\"}",
        .workspace_id = "default",
        .doc_id = 1,
    });
    try upsertFacet(db, .{
        .id = "facet-2",
        .schema_id = "mindbrain:ontology",
        .content = "Acme",
        .facets_json = "{\"node_id\":\"Acme\",\"entity_type\":\"company\"}",
        .workspace_id = "default",
        .doc_id = 2,
    });
    try insertProjection(db, .{
        .id = "proj-1",
        .agent_id = "agent-a",
        .scope = "default",
        .proj_type = "FACT",
        .content = "Ada works for Acme",
        .weight = 1.0,
        .status = "active",
    });
    try db.exec("INSERT INTO graph_entity(entity_id, entity_type, name, metadata_json) VALUES (1, 'person', 'Ada', '{\"workspace_id\":\"default\"}')");

    const resolved = (try resolveWorkspace(db, std.testing.allocator, "ghostcrab")).?;
    defer std.testing.allocator.free(resolved);
    try std.testing.expectEqualStrings("default", resolved);

    const relevance = try projectionRelevance(db, std.testing.allocator, "default", "Ada", "Acme");
    try std.testing.expect(relevance > 0);

    const summary = try coverage(db, std.testing.allocator, "default", null);
    defer std.testing.allocator.free(summary.workspace_id);
    try std.testing.expectEqual(@as(usize, 2), summary.total_nodes);
    try std.testing.expectEqual(@as(usize, 1), summary.covered_nodes);
    try std.testing.expect(summary.coverage_ratio.? > 0.4);
}

test "taxonomy import populates facets and standalone facet hierarchy" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try importTaxonomyIntoFacets(
        db,
        std.testing.allocator,
        500,
        "public",
        "taxonomy",
        4,
        &.{
            .{
                .id = "tax-1",
                .workspace_id = "default",
                .doc_id = 1,
                .node_id = "physics",
                .label = "Physics",
                .source_ref = "taxonomy:physics",
                .levels = &.{
                    .{ .facet_id = 1, .facet_name = "domain", .facet_value = "science" },
                    .{ .facet_id = 2, .facet_name = "category", .facet_value = "physics" },
                },
            },
            .{
                .id = "tax-2",
                .workspace_id = "default",
                .doc_id = 2,
                .node_id = "chemistry",
                .label = "Chemistry",
                .source_ref = "taxonomy:chemistry",
                .levels = &.{
                    .{ .facet_id = 1, .facet_name = "domain", .facet_value = "science" },
                    .{ .facet_id = 2, .facet_name = "category", .facet_value = "chemistry" },
                },
            },
        },
    );

    const taxonomy_rows = try loadTaxonomyFacetRows(db, std.testing.allocator, "default");
    defer {
        for (taxonomy_rows) |row| deinitFacetRecord(std.testing.allocator, row);
        std.testing.allocator.free(taxonomy_rows);
    }
    try std.testing.expectEqual(@as(usize, 2), taxonomy_rows.len);
    try std.testing.expectEqualStrings("Physics", taxonomy_rows[0].content);

    const repository = facet_sqlite.Repository{ .db = &db };
    const counts = try facet_store.countFacetValues(
        std.testing.allocator,
        repository.asFacetRepository(),
        "taxonomy",
        "category",
        null,
    );
    defer {
        for (counts) |count| std.testing.allocator.free(count.facet_value);
        std.testing.allocator.free(counts);
    }
    try std.testing.expectEqual(@as(usize, 2), counts.len);

    const children = try facet_store.listHierarchyChildren(
        std.testing.allocator,
        repository.asFacetRepository(),
        "taxonomy",
        "domain",
        "science",
    );
    defer {
        for (children) |*child| {
            std.testing.allocator.free(child.facet_value);
            if (child.children_bitmap) |*bitmap| bitmap.deinit();
        }
        std.testing.allocator.free(children);
    }

    try std.testing.expectEqual(@as(usize, 2), children.len);
    try std.testing.expectEqualStrings("physics", children[0].facet_value);
    try std.testing.expectEqualStrings("chemistry", children[1].facet_value);
}

test "taxonomy projections and coverage report derive from imported taxonomy rows" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try importTaxonomyIntoFacets(
        db,
        std.testing.allocator,
        500,
        "public",
        "taxonomy",
        4,
        &.{
            .{
                .id = "tax-1",
                .workspace_id = "default",
                .doc_id = 1,
                .node_id = "ada",
                .label = "Ada",
                .schema_id = "ghostcrab:taxonomy",
                .entity_type = "person",
                .levels = &.{
                    .{ .facet_id = 1, .facet_name = "domain", .facet_value = "science" },
                    .{ .facet_id = 2, .facet_name = "category", .facet_value = "physics" },
                },
            },
            .{
                .id = "tax-2",
                .workspace_id = "default",
                .doc_id = 2,
                .node_id = "acme",
                .label = "Acme",
                .schema_id = "ghostcrab:taxonomy",
                .entity_type = "company",
                .levels = &.{
                    .{ .facet_id = 1, .facet_name = "domain", .facet_value = "industry" },
                    .{ .facet_id = 2, .facet_name = "category", .facet_value = "manufacturing" },
                },
            },
        },
    );

    const inserted = try materializeTaxonomyProjections(db, std.testing.allocator, "default", "agent-taxonomy");
    try std.testing.expectEqual(@as(usize, 2), inserted);

    const projections = try loadAgentProjections(db, std.testing.allocator, "agent-taxonomy");
    defer {
        for (projections) |row| deinitProjectionRecord(std.testing.allocator, row);
        std.testing.allocator.free(projections);
    }
    try std.testing.expectEqual(@as(usize, 2), projections.len);
    try std.testing.expect(std.mem.indexOf(u8, projections[0].content, "Taxonomy node") != null);

    try db.exec("INSERT INTO graph_entity(entity_id, entity_type, name, metadata_json) VALUES (1, 'person', 'ada', '{\"workspace_id\":\"default\"}')");

    const report = try coverageReport(db, std.testing.allocator, "default", null);
    defer {
        std.testing.allocator.free(report.summary.workspace_id);
        for (report.gaps) |gap| deinitCoverageGap(std.testing.allocator, gap);
        std.testing.allocator.free(report.gaps);
    }

    try std.testing.expectEqual(@as(usize, 2), report.summary.total_nodes);
    try std.testing.expectEqual(@as(usize, 1), report.summary.covered_nodes);
    try std.testing.expectEqual(@as(usize, 1), report.gaps.len);
    try std.testing.expectEqualStrings("acme", report.gaps[0].id);
    try std.testing.expect(report.gaps[0].decayed_confidence == null);
    try std.testing.expectEqual(@as(usize, 2), report.summary.projection_rows);
}

test "coverage report entity type filter matches graph entities by type" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try importTaxonomyIntoFacets(
        db,
        std.testing.allocator,
        500,
        "public",
        "taxonomy",
        4,
        &.{
            .{
                .id = "tax-1",
                .workspace_id = "default",
                .doc_id = 1,
                .node_id = "ada",
                .label = "Ada",
                .schema_id = "ghostcrab:taxonomy",
                .entity_type = "person",
                .levels = &.{
                    .{ .facet_id = 1, .facet_name = "domain", .facet_value = "science" },
                },
            },
            .{
                .id = "tax-2",
                .workspace_id = "default",
                .doc_id = 2,
                .node_id = "acme",
                .label = "Acme",
                .schema_id = "ghostcrab:taxonomy",
                .entity_type = "company",
                .levels = &.{
                    .{ .facet_id = 1, .facet_name = "domain", .facet_value = "industry" },
                },
            },
        },
    );

    try db.exec("INSERT INTO graph_entity(entity_id, entity_type, name, metadata_json) VALUES (1, 'person', 'ada', '{\"workspace_id\":\"default\"}')");

    // Filtering on the graph side must compare against entity_type, not the
    // entity name: 'ada' is a person, so it stays and covers the tax-1 node.
    const report = try coverageReport(db, std.testing.allocator, "default", &.{"person"});
    defer {
        std.testing.allocator.free(report.summary.workspace_id);
        for (report.gaps) |gap| deinitCoverageGap(std.testing.allocator, gap);
        std.testing.allocator.free(report.gaps);
    }

    try std.testing.expectEqual(@as(usize, 1), report.summary.total_nodes);
    try std.testing.expectEqual(@as(usize, 1), report.summary.covered_nodes);
    try std.testing.expectEqual(@as(usize, 0), report.gaps.len);
    try std.testing.expectEqual(@as(usize, 1), report.summary.graph_entities);
}

test "materializeRelevanceProjections ranks entity matches above query-only matches" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertProjection(db, .{
        .id = "proj-entity",
        .agent_id = "agent-rel",
        .scope = "default",
        .proj_type = "FACT",
        .content = "Ada owns Unit 1",
        .weight = 1.0,
        .status = "active",
    });
    try insertProjection(db, .{
        .id = "proj-query",
        .agent_id = "agent-rel",
        .scope = "default",
        .proj_type = "FACT",
        .content = "Acme subsidiary details",
        .weight = 0.5,
        .status = "active",
    });

    const rows = try materializeRelevanceProjections(
        db,
        std.testing.allocator,
        "agent-rel",
        null,
        "Ada",
        "Acme",
        10,
    );
    defer deinitProjectionRows(std.testing.allocator, rows);

    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqualStrings("proj-entity", rows[0].id);
}

test "upsertFacet updates by id and surfaces doc_id/source_ref conflicts as typed errors" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try upsertFacet(db, .{
        .id = "fact-a",
        .schema_id = "ghostcrab:taxonomy",
        .content = "Original",
        .facets_json = "{}",
        .workspace_id = "default",
        .doc_id = 1,
        .source_ref = "ref:a",
    });
    // Same id: plain update, no constraint noise.
    try upsertFacet(db, .{
        .id = "fact-a",
        .schema_id = "ghostcrab:taxonomy",
        .content = "Updated",
        .facets_json = "{}",
        .workspace_id = "default",
        .doc_id = 1,
        .source_ref = "ref:a",
    });

    // Different id colliding on doc_id: typed error, existing fact untouched.
    try std.testing.expectError(error.FactDocIdConflict, upsertFacet(db, .{
        .id = "fact-b",
        .schema_id = "ghostcrab:taxonomy",
        .content = "Intruder",
        .facets_json = "{}",
        .workspace_id = "default",
        .doc_id = 1,
        .source_ref = "ref:b",
    }));

    // Different id colliding on (source_ref, workspace): typed error.
    try std.testing.expectError(error.FactSourceRefConflict, upsertFacet(db, .{
        .id = "fact-c",
        .schema_id = "ghostcrab:taxonomy",
        .content = "Intruder",
        .facets_json = "{}",
        .workspace_id = "default",
        .doc_id = 2,
        .source_ref = "ref:a",
    }));

    const rows = try loadTaxonomyFacetRows(db, std.testing.allocator, "default");
    defer {
        for (rows) |row| deinitFacetRecord(std.testing.allocator, row);
        std.testing.allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings("fact-a", rows[0].id);
    try std.testing.expectEqualStrings("Updated", rows[0].content);
}

test "facet json extraction survives escaped quotes, nested objects, and non-strings" {
    const allocator = std.testing.allocator;

    // Escaped quote inside the label must not truncate the value.
    const escaped = "{\"node_id\":\"n1\",\"label\":\"say \\\"hi\\\" now\"}";
    const label = (try extractFacetJsonValue(allocator, escaped, "label")).?;
    defer allocator.free(label);
    try std.testing.expectEqualStrings("say \"hi\" now", label);

    // A key inside a nested object must not match at the top level.
    const nested = "{\"meta\":{\"label\":\"inner\"},\"node_id\":\"n2\"}";
    try std.testing.expect(try extractFacetJsonValue(allocator, nested, "label") == null);

    // Non-string scalar values are stringified instead of missed.
    const numeric = "{\"entity_id\":42}";
    const entity_id = (try extractFacetJsonValue(allocator, numeric, "entity_id")).?;
    defer allocator.free(entity_id);
    try std.testing.expectEqualStrings("42", entity_id);

    // Identity falls back through node_id/entity_id/name/label and handles
    // escaped quotes; invalid JSON falls back to the content column.
    const identity = try extractFacetIdentity(allocator, .{
        .id = "f",
        .schema_id = "ghostcrab:taxonomy",
        .content = "Fallback",
        .facets_json = "{\"name\":\"Ada \\\"the first\\\"\"}",
        .workspace_id = "default",
        .doc_id = 1,
    });
    defer allocator.free(identity);
    try std.testing.expectEqualStrings("Ada \"the first\"", identity);

    const fallback = try extractFacetIdentity(allocator, .{
        .id = "f2",
        .schema_id = "ghostcrab:taxonomy",
        .content = "Fallback",
        .facets_json = "not json",
        .workspace_id = "default",
        .doc_id = 2,
    });
    defer allocator.free(fallback);
    try std.testing.expectEqualStrings("Fallback", fallback);
}

test "taxonomy import rejects chunk_bits and doc_id outside posting range" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    const levels = [_]TaxonomyFacetLevel{
        .{ .facet_id = 1, .facet_name = "domain", .facet_value = "science" },
    };
    const node = TaxonomyNodeImport{
        .id = "tax-guard",
        .workspace_id = "default",
        .doc_id = 1,
        .node_id = "physics",
        .label = "Physics",
        .levels = &levels,
    };

    // chunk_bits beyond the 32-bit in-chunk id space.
    try std.testing.expectError(error.ValueOutOfRange, importTaxonomyIntoFacets(
        db,
        std.testing.allocator,
        600,
        "public",
        "taxonomy_guard",
        40,
        &.{node},
    ));

    // doc_id beyond 2^(32 + chunk_bits) overflows the u32 chunk id.
    var big_node = node;
    big_node.doc_id = @as(u64, 1) << 40;
    try std.testing.expectError(error.ValueOutOfRange, importTaxonomyIntoFacets(
        db,
        std.testing.allocator,
        601,
        "public",
        "taxonomy_guard2",
        4,
        &.{big_node},
    ));
}

test "exact pack selects identity and rejects foreign global expired and ambiguous plans" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec("INSERT INTO projections(id,agent_id,scope,proj_type,content,status) VALUES ('p1','agent','ws:plan','GOAL','Launch method','active'), ('global','agent',NULL,'GOAL','Global method','active'), ('foreign','agent','other:plan','GOAL','Foreign','active'), ('expired','agent','ws:expired','GOAL','Old','active')");
    try db.exec("UPDATE projections SET expires_at_unix=1 WHERE id='expired'");
    const a = std.testing.allocator;
    const exact = try selectExactPackProjections(db, a, "agent", "ws", "ws:plan", null);
    defer deinitProjectionRows(a, exact);
    try std.testing.expectEqual(@as(usize, 1), exact.len);
    try std.testing.expectEqualStrings("p1", exact[0].id);
    for ([_][]const u8{ "ws:absent", "ws:expired", "other:plan" }) |scope| {
        const absent = try selectExactPackProjections(db, a, "agent", "ws", scope, null);
        defer deinitProjectionRows(a, absent);
        try std.testing.expectEqual(@as(usize, 0), absent.len);
    }
    const wrong_agent = try selectExactPackProjections(db, a, "other", "ws", "ws:plan", null);
    defer deinitProjectionRows(a, wrong_agent);
    try std.testing.expectEqual(@as(usize, 0), wrong_agent.len);
    try db.exec("INSERT INTO projections(id,agent_id,scope,proj_type,content,status) VALUES ('p2','agent','ws:plan','GOAL','Second','active')");
    const ambiguous = try selectExactPackProjections(db, a, "agent", "ws", "ws:plan", null);
    defer deinitProjectionRows(a, ambiguous);
    try std.testing.expectEqual(@as(usize, 2), ambiguous.len);
    const by_id = try selectExactPackProjections(db, a, "agent", "ws", "ws:plan", "p2");
    defer deinitProjectionRows(a, by_id);
    try std.testing.expectEqual(@as(usize, 1), by_id.len);
    try std.testing.expectEqualStrings("p2", by_id[0].id);
    const wrong_id = try selectExactPackProjections(db, a, "agent", "ws", "ws:plan", "foreign");
    defer deinitProjectionRows(a, wrong_id);
    try std.testing.expectEqual(@as(usize, 0), wrong_id.len);
}
