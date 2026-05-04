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
    const stmt = try prepare(db, "INSERT OR REPLACE INTO facets(id, schema_id, content, facets_json, workspace_id, doc_id, source_ref) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)");
    defer finalize(stmt);
    try bindText(stmt, 1, record.id);
    try bindText(stmt, 2, record.schema_id);
    try bindText(stmt, 3, record.content);
    try bindText(stmt, 4, record.facets_json);
    try bindText(stmt, 5, record.workspace_id);
    try bindInt64(stmt, 6, record.doc_id);
    if (record.source_ref) |source_ref| try bindText(stmt, 7, source_ref) else try bindNull(stmt, 7);
    try stepDone(stmt);
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
    try facet_sqlite.upsertFacetTable(db, table_id, schema_name, table_name, chunk_bits);

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

            const parent_value_id = try ensureFacetValueNodeId(db, allocator, table_id, parent.facet_id, parent.facet_value);
            const child_value_id = try ensureFacetValueNodeId(db, allocator, table_id, child.facet_id, child.facet_value);
            try appendFacetChildLink(db, allocator, table_id, parent_value_id, child_value_id);
        }
    }
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
    const stmt = try prepare(db, "SELECT id, schema_id, content, facets_json, workspace_id, doc_id, source_ref FROM facets WHERE workspace_id = ?1 AND schema_id = 'ghostcrab:taxonomy' ORDER BY doc_id");
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

pub fn materializePackProjections(
    db: Database,
    allocator: std.mem.Allocator,
    agent_id: []const u8,
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

    return inserted;
}

pub fn coverageReport(
    db: Database,
    allocator: std.mem.Allocator,
    workspace_id: []const u8,
    entity_types: ?[]const []const u8,
) !CoverageReport {
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
        if (containsGraphEntity(graph_entities, node_id)) {
            covered += 1;
            continue;
        }

        const label = try extractFacetJsonValue(allocator, facet.facets_json, "label");
        defer if (label) |value| allocator.free(value);
        const criticality = try extractFacetJsonValue(allocator, facet.facets_json, "criticality");
        defer if (criticality) |value| allocator.free(value);
        const gap_label = label orelse facet.content;
        const gap_entity_type = maybe_entity_type orelse "unknown";
        const decayed_confidence = if (findGraphEntityId(graph_entities, node_id, gap_label, gap_entity_type)) |entity_id|
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
            .facet_rows = facets.len,
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
    return try graph_sqlite.marketplaceSearch(
        db,
        allocator,
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
    const stmt = try prepare(db, "SELECT workspace_id, domain_profile_json FROM workspaces");
    defer finalize(stmt);

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const workspace_id = try dupeColumnText(allocator, stmt, 0);
        errdefer allocator.free(workspace_id);
        const profile = try dupeColumnText(allocator, stmt, 1);
        defer allocator.free(profile);

        if (std.mem.eql(u8, workspace_id, domain_or_workspace) or std.mem.indexOf(u8, profile, domain_or_workspace) != null) {
            return workspace_id;
        }
        allocator.free(workspace_id);
    }
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

fn loadWorkspaceFacets(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8) ![]FacetRecord {
    const stmt = try prepare(db, "SELECT id, schema_id, content, facets_json, workspace_id, doc_id, source_ref FROM facets WHERE workspace_id = ?1");
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
    const stmt = try prepare(db, "SELECT entity_id, name, entity_type FROM graph_entity WHERE json_extract(metadata_json, '$.workspace_id') = ?1 OR ?1 = 'default'");
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
        const entity_type = try dupeColumnText(allocator, stmt, 1);
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
    const stmt = try prepare(db, "SELECT COUNT(*) FROM projections WHERE scope = ?1 OR scope IS NULL");
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
    return std.mem.eql(u8, workspace_id.?, scope.?);
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
    const hay = lowerOwned(std.heap.page_allocator, haystack) catch return false;
    defer std.heap.page_allocator.free(hay);
    const ned = lowerOwned(std.heap.page_allocator, needle) catch return false;
    defer std.heap.page_allocator.free(ned);
    return std.mem.indexOf(u8, hay, ned) != null;
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

fn extractFacetJsonValue(allocator: std.mem.Allocator, facets_json: []const u8, key_name: []const u8) !?[]const u8 {
    const pattern = try std.fmt.allocPrint(allocator, "\"{s}\":\"", .{key_name});
    defer allocator.free(pattern);

    if (std.mem.indexOf(u8, facets_json, pattern)) |start| {
        const value_start = start + pattern.len;
        const rest = facets_json[value_start..];
        const end = std.mem.indexOfScalar(u8, rest, '"') orelse return null;
        return try allocator.dupe(u8, rest[0..end]);
    }
    return null;
}

fn extractFacetIdentity(allocator: std.mem.Allocator, facet: FacetRecord) ![]const u8 {
    const keys = [_][]const u8{ "\"node_id\":\"", "\"entity_id\":\"", "\"name\":\"", "\"label\":\"" };
    for (keys) |key| {
        if (std.mem.indexOf(u8, facet.facets_json, key)) |start| {
            const value_start = start + key.len;
            const rest = facet.facets_json[value_start..];
            const end = std.mem.indexOfScalar(u8, rest, '"') orelse continue;
            return try allocator.dupe(u8, rest[0..end]);
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
    const shift_bits: u6 = @intCast(chunk_bits);
    const chunk_id: u32 = @intCast(doc_id >> shift_bits);
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

fn ensureFacetValueNodeId(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    facet_id: u32,
    facet_value: []const u8,
) !u32 {
    const existing_stmt = try prepare(db, "SELECT value_id FROM facet_value_nodes WHERE table_id = ?1 AND facet_id = ?2 AND facet_value = ?3");
    defer finalize(existing_stmt);
    try bindInt64(existing_stmt, 1, table_id);
    try bindInt64(existing_stmt, 2, facet_id);
    try bindText(existing_stmt, 3, facet_value);
    if (c.sqlite3_step(existing_stmt) == c.SQLITE_ROW) {
        return try columnU32(existing_stmt, 0);
    }

    const next_stmt = try prepare(db, "SELECT COALESCE(MAX(value_id), 0) + 1 FROM facet_value_nodes WHERE table_id = ?1");
    defer finalize(next_stmt);
    try bindInt64(next_stmt, 1, table_id);
    if (c.sqlite3_step(next_stmt) != c.SQLITE_ROW) return error.MissingRow;
    const value_id = try columnU32(next_stmt, 0);
    try facet_sqlite.upsertFacetValueNode(db, allocator, table_id, value_id, facet_id, facet_value, null);
    return value_id;
}

fn appendFacetChildLink(
    db: Database,
    allocator: std.mem.Allocator,
    table_id: u64,
    parent_value_id: u32,
    child_value_id: u32,
) !void {
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

    child_bitmap.add(child_value_id);
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
