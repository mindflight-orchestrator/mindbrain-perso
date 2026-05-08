const std = @import("std");
const interfaces = @import("interfaces.zig");
const roaring = @import("roaring.zig");
const graph_store = @import("graph_store.zig");
const facet_sqlite = @import("facet_sqlite.zig");
const toon_exports = @import("toon_exports.zig");
const tokenization_sqlite = @import("tokenization_sqlite.zig");
const compat = @import("zig16_compat.zig");
const ztoon = @import("ztoon");

const Field = ztoon.Field;
const Value = ztoon.Value;

pub const Database = facet_sqlite.Database;
pub const c = facet_sqlite.c;
const Error = facet_sqlite.Error;

pub const EntityRecord = struct {
    entity_id: u32,
    entity_type: []const u8,
    name: []const u8,
};

pub const PathNode = struct {
    entity_id: u32,
    entity_type: []const u8,
    name: []const u8,
};

pub const PathSegment = struct {
    relation_id: u32,
    relation_type: []const u8,
    source: PathNode,
    target: PathNode,
    confidence: f32,
};

pub const GraphPathResult = struct {
    hops: usize,
    segments: []PathSegment,

    pub fn deinit(self: *GraphPathResult, allocator: std.mem.Allocator) void {
        for (self.segments) |*segment| {
            allocator.free(segment.relation_type);
            allocator.free(segment.source.entity_type);
            allocator.free(segment.source.name);
            allocator.free(segment.target.entity_type);
            allocator.free(segment.target.name);
        }
        allocator.free(self.segments);
        self.* = undefined;
    }
};

pub const TraverseDirection = enum {
    outbound,
    inbound,
};

pub const TraverseRow = struct {
    node_id: []const u8,
    node_label: []const u8,
    node_type: []const u8,
    metadata_json: []const u8,
    edge_label: ?[]const u8,
    depth: usize,
    path: [][]const u8,
};

pub const TraverseResult = struct {
    rows: []TraverseRow,
    target_found: bool,

    pub fn deinit(self: *TraverseResult, allocator: std.mem.Allocator) void {
        for (self.rows) |*row| {
            allocator.free(row.node_id);
            allocator.free(row.node_label);
            allocator.free(row.node_type);
            allocator.free(row.metadata_json);
            if (row.edge_label) |edge_label| allocator.free(edge_label);
            for (row.path) |segment| allocator.free(segment);
            allocator.free(row.path);
        }
        allocator.free(self.rows);
        self.* = undefined;
    }
};

pub const EntityAliasMatch = struct {
    entity_id: u32,
    name: []const u8,
    entity_type: []const u8,
    confidence: f32,
};

pub const EntityDocumentLink = struct {
    entity_id: u32,
    doc_id: u64,
    table_id: u64,
    role: ?[]const u8,
    confidence: f32,
};

pub const EntityChunkLink = struct {
    entity_id: u32,
    workspace_id: []const u8,
    collection_id: []const u8,
    doc_id: u64,
    chunk_index: u32,
    role: ?[]const u8,
    confidence: f32,
    metadata_json: []const u8,

    pub fn deinit(self: *EntityChunkLink, allocator: std.mem.Allocator) void {
        allocator.free(self.workspace_id);
        allocator.free(self.collection_id);
        if (self.role) |role| allocator.free(role);
        allocator.free(self.metadata_json);
        self.* = undefined;
    }
};

pub const RelationRecordFull = struct {
    relation_id: u32,
    workspace_id: []const u8,
    relation_type: []const u8,
    source_id: u32,
    target_id: u32,
    valid_from_unix: ?i64,
    valid_to_unix: ?i64,
    confidence: f32,
    deprecated_at_unix: ?i64,
    run_id: ?u32,
    patch_id: ?u32,
    metadata_json: []const u8,
    created_at_unix: i64,

    pub fn deinit(self: *RelationRecordFull, allocator: std.mem.Allocator) void {
        allocator.free(self.workspace_id);
        allocator.free(self.relation_type);
        allocator.free(self.metadata_json);
        self.* = undefined;
    }
};

pub const EntityRecordFull = struct {
    entity_id: u32,
    entity_type: []const u8,
    name: []const u8,
    confidence: f32,
    metadata_json: []const u8,
    deprecated_at_unix: ?i64,
    created_at_unix: i64,

    pub fn deinit(self: *EntityRecordFull, allocator: std.mem.Allocator) void {
        allocator.free(self.entity_type);
        allocator.free(self.name);
        allocator.free(self.metadata_json);
        self.* = undefined;
    }
};

pub const GraphStreamEvent = struct {
    seq: u32,
    kind: []const u8,
    payload: []const u8,

    pub fn deinit(self: *GraphStreamEvent, allocator: std.mem.Allocator) void {
        allocator.free(self.payload);
        self.* = undefined;
    }
};

pub const StreamEntityPayload = struct {
    entity_id: u32,
    name: []const u8,
    entity_type: []const u8,
    confidence: f32,
    metadata_json: []const u8,
    deprecated_at: ?i64,
    created_at: i64,
};

pub const StreamRelationPayload = struct {
    relation_id: u32,
    workspace_id: []const u8,
    relation_type: []const u8,
    source_id: u32,
    target_id: u32,
    valid_from_unix: ?i64,
    valid_to_unix: ?i64,
    confidence: f32,
    deprecated_at: ?i64,
    run_id: ?u32,
    patch_id: ?u32,
    metadata_json: []const u8,
    created_at: i64,
};

pub const StreamNeighborhoodPayload = struct {
    entity: StreamEntityPayload,
};

pub const StreamEdgePayload = struct {
    direction: []const u8,
    relation: StreamRelationPayload,
    source: StreamEntityPayload,
    target: StreamEntityPayload,
};

pub const StreamDonePayload = struct {
    kind: []const u8,
    seed_count: usize,
    hops: usize,
    node_count: usize,
    edge_count: usize,
};

pub const EntitySearchResult = struct {
    entity_id: u32,
    name: []const u8,
    entity_type: []const u8,
    confidence: f32,
    fts_rank: f32,
    metadata_json: []const u8,
};

pub const MarketplaceResult = struct {
    entity_id: u32,
    name: []const u8,
    entity_type: []const u8,
    confidence: f32,
    fts_rank: f32,
    is_direct_match: bool,
    hub_score: f32,
    composite_score: f32,
    metadata_json: []const u8,
};

pub const SkillDependency = struct {
    dep_entity_id: u32,
    dep_name: []const u8,
    dep_type: []const u8,
    dep_confidence: f32,
    relation_type: []const u8,
    depth: u32,
};

pub const NeighborhoodEntity = struct {
    id: u32,
    type: []const u8,
    name: []const u8,
    confidence: f32,
    metadata: []const u8,
    deprecated_at: ?i64 = null,
    created_at: i64,
};

pub const NeighborhoodEdge = struct {
    type: []const u8,
    confidence: f32,
    target_id: ?u32 = null,
    target_name: ?[]const u8 = null,
    source_id: ?u32 = null,
    source_name: ?[]const u8 = null,
};

pub const Neighborhood = struct {
    entity: NeighborhoodEntity,
    outgoing: []NeighborhoodEdge,
    incoming: []NeighborhoodEdge,

    pub fn deinit(self: *Neighborhood, allocator: std.mem.Allocator) void {
        allocator.free(self.entity.name);
        allocator.free(self.entity.type);
        allocator.free(self.entity.metadata);
        for (self.outgoing) |edge| {
            allocator.free(edge.type);
            if (edge.target_name) |t| allocator.free(t);
        }
        allocator.free(self.outgoing);
        for (self.incoming) |edge| {
            allocator.free(edge.type);
            if (edge.source_name) |s| allocator.free(s);
        }
        allocator.free(self.incoming);
        self.* = undefined;
    }
};

pub const ConceptInput = struct {
    entity_type: []const u8,
    name: []const u8,
    confidence: f32 = 1.0,
    metadata_json: ?[]const u8 = null,
};

pub const RelationInput = struct {
    relation_type: []const u8,
    source: []const u8,
    target: []const u8,
    confidence: f32 = 1.0,
    metadata_json: ?[]const u8 = null,
    valid_from_unix: ?i64 = null,
    valid_to_unix: ?i64 = null,
};

pub const KnowledgePatchArtifact = struct {
    action: []const u8,
    entity_type: ?[]const u8 = null,
    name: ?[]const u8 = null,
    relation_type: ?[]const u8 = null,
    source: ?[]const u8 = null,
    target: ?[]const u8 = null,
    confidence: ?f32 = null,
    metadata_json: ?[]const u8 = null,
};

pub const DurableRepository = struct {
    db: *const Database,

    pub fn asGraphRepository(self: *const DurableRepository) interfaces.GraphRepository {
        return .{
            .ctx = @ptrCast(@constCast(self)),
            .getOutgoingEdgesFn = getOutgoingEdges,
            .getIncomingEdgesFn = getIncomingEdges,
            .filterEdgesFn = filterEdges,
            .projectNeighborNodesFn = projectNeighborNodes,
            .expandNeighborStepsFn = expandNeighborSteps,
            .expandFrontierStepsFn = expandFrontierSteps,
        };
    }

    fn getOutgoingEdges(ctx: *anyopaque, allocator: std.mem.Allocator, nodes: roaring.Bitmap) anyerror!roaring.Bitmap {
        const self: *const DurableRepository = @ptrCast(@alignCast(ctx));
        return try loadAdjacencyUnion(self.db.*, allocator, "graph_lj_out", nodes);
    }

    fn getIncomingEdges(ctx: *anyopaque, allocator: std.mem.Allocator, nodes: roaring.Bitmap) anyerror!roaring.Bitmap {
        const self: *const DurableRepository = @ptrCast(@alignCast(ctx));
        return try loadAdjacencyUnion(self.db.*, allocator, "graph_lj_in", nodes);
    }

    fn filterEdges(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        edges: roaring.Bitmap,
        filter: interfaces.GraphEdgeFilter,
    ) anyerror!roaring.Bitmap {
        const self: *const DurableRepository = @ptrCast(@alignCast(ctx));
        const edge_ids = try edges.toArray(allocator);
        defer allocator.free(edge_ids);

        var result = try roaring.Bitmap.empty();
        errdefer result.deinit();

        for (edge_ids) |edge_id| {
            const relation = try loadRelation(self.db.*, allocator, edge_id) orelse continue;
            defer allocator.free(relation.relation_type);
            if (!passesFilter(relation, filter)) continue;
            result.add(edge_id);
        }
        return result;
    }

    fn projectNeighborNodes(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        frontier: roaring.Bitmap,
        edges: roaring.Bitmap,
    ) anyerror!roaring.Bitmap {
        const self: *const DurableRepository = @ptrCast(@alignCast(ctx));
        const edge_ids = try edges.toArray(allocator);
        defer allocator.free(edge_ids);

        var result = try roaring.Bitmap.empty();
        errdefer result.deinit();

        for (edge_ids) |edge_id| {
            const relation = try loadRelation(self.db.*, allocator, edge_id) orelse continue;
            defer allocator.free(relation.relation_type);
            if (frontier.contains(relation.source_id)) result.add(relation.target_id);
            if (frontier.contains(relation.target_id)) result.add(relation.source_id);
        }
        return result;
    }

    fn expandNeighborSteps(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        frontier: roaring.Bitmap,
        edges: roaring.Bitmap,
    ) anyerror![]interfaces.GraphNeighborStep {
        const self: *const DurableRepository = @ptrCast(@alignCast(ctx));
        const edge_ids = try edges.toArray(allocator);
        defer allocator.free(edge_ids);

        var steps = std.ArrayList(interfaces.GraphNeighborStep).empty;
        defer steps.deinit(allocator);

        for (edge_ids) |edge_id| {
            const relation = try loadRelation(self.db.*, allocator, edge_id) orelse continue;
            defer allocator.free(relation.relation_type);

            if (frontier.contains(relation.source_id)) {
                try steps.append(allocator, .{
                    .from_node = relation.source_id,
                    .to_node = relation.target_id,
                    .relation_id = relation.relation_id,
                });
            }
            if (frontier.contains(relation.target_id)) {
                try steps.append(allocator, .{
                    .from_node = relation.target_id,
                    .to_node = relation.source_id,
                    .relation_id = relation.relation_id,
                });
            }
        }

        return steps.toOwnedSlice(allocator);
    }

    fn expandFrontierSteps(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        frontier: roaring.Bitmap,
        filter: interfaces.GraphEdgeFilter,
    ) anyerror![]interfaces.GraphNeighborStep {
        const self: *const DurableRepository = @ptrCast(@alignCast(ctx));
        const frontier_ids = try frontier.toArray(allocator);
        defer allocator.free(frontier_ids);

        var steps = std.ArrayList(interfaces.GraphNeighborStep).empty;
        defer steps.deinit(allocator);

        for (frontier_ids) |node_id| {
            var outgoing = loadAdjacencyBitmap(self.db.*, allocator, "graph_lj_out", @intCast(node_id)) catch |err| switch (err) {
                error.MissingRow => null,
                else => return err,
            };
            defer if (outgoing) |*bitmap| bitmap.deinit();

            var incoming = loadAdjacencyBitmap(self.db.*, allocator, "graph_lj_in", @intCast(node_id)) catch |err| switch (err) {
                error.MissingRow => null,
                else => return err,
            };
            defer if (incoming) |*bitmap| bitmap.deinit();

            if (outgoing) |bitmap| {
                const edge_ids = try bitmap.toArray(allocator);
                defer allocator.free(edge_ids);
                try appendStepsForEdges(self.db.*, allocator, &steps, frontier, edge_ids, filter);
            }
            if (incoming) |bitmap| {
                const edge_ids = try bitmap.toArray(allocator);
                defer allocator.free(edge_ids);
                try appendStepsForEdges(self.db.*, allocator, &steps, frontier, edge_ids, filter);
            }
        }

        return steps.toOwnedSlice(allocator);
    }
};

pub const InMemoryRuntime = struct {
    store: graph_store.Store,

    pub fn deinit(self: *InMemoryRuntime) void {
        self.store.deinit();
        self.* = undefined;
    }

    pub fn kHops(
        self: *const InMemoryRuntime,
        allocator: std.mem.Allocator,
        seed_nodes: []const u32,
        max_hops: usize,
        filter: interfaces.GraphEdgeFilter,
    ) !roaring.Bitmap {
        return try self.store.kHopsFast(allocator, seed_nodes, max_hops, filter);
    }

    pub fn shortestPathHops(
        self: *const InMemoryRuntime,
        allocator: std.mem.Allocator,
        source_id: u32,
        target_id: u32,
        filter: interfaces.GraphEdgeFilter,
        max_depth: usize,
    ) !?usize {
        return try self.store.shortestPathHopsFast(allocator, source_id, target_id, filter, max_depth);
    }

    pub fn shortestPath(
        self: *const InMemoryRuntime,
        allocator: std.mem.Allocator,
        source_id: u32,
        target_id: u32,
        filter: interfaces.GraphEdgeFilter,
        max_depth: usize,
    ) !?[]graph_store.PathEdge {
        return try self.store.shortestPathFast(allocator, source_id, target_id, filter, max_depth);
    }
};

pub fn loadStore(db: Database, allocator: std.mem.Allocator) !graph_store.Store {
    var store = graph_store.Store.init(allocator);
    errdefer store.deinit();

    try loadRelationsIntoStore(db, &store, allocator);
    try loadAdjacencyIntoStore(db, &store, allocator, "graph_lj_out", true);
    try loadAdjacencyIntoStore(db, &store, allocator, "graph_lj_in", false);

    return store;
}

pub fn loadRuntime(db: Database, allocator: std.mem.Allocator) !InMemoryRuntime {
    return .{
        .store = try loadStore(db, allocator),
    };
}

pub fn insertEntity(db: Database, entity_id: u32, entity_type: []const u8, name: []const u8) !void {
    return try insertEntityWithMetadata(db, entity_id, entity_type, name, "{}");
}

pub fn insertEntityWithMetadata(
    db: Database,
    entity_id: u32,
    entity_type: []const u8,
    name: []const u8,
    metadata_json: []const u8,
) !void {
    const stmt = try prepare(db, "INSERT INTO graph_entity(entity_id, entity_type, name, metadata_json) VALUES (?1, ?2, ?3, ?4)");
    defer finalize(stmt);
    try bindInt64(stmt, 1, entity_id);
    try bindText(stmt, 2, entity_type);
    try bindText(stmt, 3, name);
    try bindText(stmt, 4, metadata_json);
    try stepDone(stmt);
}

pub fn upsertEntity(db: Database, entity_id: u32, entity_type: []const u8, name: []const u8) !void {
    return try upsertEntityWithMetadata(db, entity_id, entity_type, name, "{}");
}

pub fn upsertEntityWithMetadata(
    db: Database,
    entity_id: u32,
    entity_type: []const u8,
    name: []const u8,
    metadata_json: []const u8,
) !void {
    const stmt = try prepare(db, "INSERT OR REPLACE INTO graph_entity(entity_id, entity_type, name, metadata_json) VALUES (?1, ?2, ?3, ?4)");
    defer finalize(stmt);
    try bindInt64(stmt, 1, entity_id);
    try bindText(stmt, 2, entity_type);
    try bindText(stmt, 3, name);
    try bindText(stmt, 4, metadata_json);
    try stepDone(stmt);
}

pub fn upsertEntityFull(
    db: Database,
    entity_id: u32,
    workspace_id: []const u8,
    entity_type: []const u8,
    name: []const u8,
    confidence: f32,
    metadata_json: []const u8,
) !void {
    const stmt = try prepare(
        db,
        "INSERT OR REPLACE INTO graph_entity(entity_id, workspace_id, entity_type, name, confidence, metadata_json, deprecated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL)",
    );
    defer finalize(stmt);
    try bindInt64(stmt, 1, entity_id);
    try bindText(stmt, 2, workspace_id);
    try bindText(stmt, 3, entity_type);
    try bindText(stmt, 4, name);
    if (c.sqlite3_bind_double(stmt, 5, confidence) != c.SQLITE_OK) return error.BindFailed;
    try bindText(stmt, 6, metadata_json);
    try stepDone(stmt);
}

pub fn insertRelation(db: Database, relation: graph_store.RelationRecord) !void {
    const stmt = try prepare(db, "INSERT INTO graph_relation(relation_id, relation_type, source_id, target_id, valid_from_unix, valid_to_unix, confidence) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)");
    defer finalize(stmt);
    try bindInt64(stmt, 1, relation.relation_id);
    try bindText(stmt, 2, relation.relation_type);
    try bindInt64(stmt, 3, relation.source_id);
    try bindInt64(stmt, 4, relation.target_id);
    if (relation.valid_from_unix) |value| try bindInt64(stmt, 5, value) else try bindNull(stmt, 5);
    if (relation.valid_to_unix) |value| try bindInt64(stmt, 6, value) else try bindNull(stmt, 6);
    if (c.sqlite3_bind_double(stmt, 7, relation.confidence) != c.SQLITE_OK) return error.BindFailed;
    try stepDone(stmt);
}

pub fn upsertRelation(db: Database, relation: graph_store.RelationRecord) !void {
    const stmt = try prepare(db, "INSERT OR REPLACE INTO graph_relation(relation_id, relation_type, source_id, target_id, valid_from_unix, valid_to_unix, confidence) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)");
    defer finalize(stmt);
    try bindInt64(stmt, 1, relation.relation_id);
    try bindText(stmt, 2, relation.relation_type);
    try bindInt64(stmt, 3, relation.source_id);
    try bindInt64(stmt, 4, relation.target_id);
    if (relation.valid_from_unix) |value| try bindInt64(stmt, 5, value) else try bindNull(stmt, 5);
    if (relation.valid_to_unix) |value| try bindInt64(stmt, 6, value) else try bindNull(stmt, 6);
    if (c.sqlite3_bind_double(stmt, 7, relation.confidence) != c.SQLITE_OK) return error.BindFailed;
    try stepDone(stmt);
}

pub fn upsertRelationFull(
    db: Database,
    relation_id: u32,
    workspace_id: []const u8,
    relation_type: []const u8,
    source_id: u32,
    target_id: u32,
    valid_from_unix: ?i64,
    valid_to_unix: ?i64,
    confidence: f32,
    metadata_json: []const u8,
) !void {
    const stmt = try prepare(
        db,
        "INSERT OR REPLACE INTO graph_relation(relation_id, workspace_id, relation_type, source_id, target_id, valid_from_unix, valid_to_unix, confidence, metadata_json, deprecated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL)",
    );
    defer finalize(stmt);
    try bindInt64(stmt, 1, relation_id);
    try bindText(stmt, 2, workspace_id);
    try bindText(stmt, 3, relation_type);
    try bindInt64(stmt, 4, source_id);
    try bindInt64(stmt, 5, target_id);
    if (valid_from_unix) |value| try bindInt64(stmt, 6, value) else try bindNull(stmt, 6);
    if (valid_to_unix) |value| try bindInt64(stmt, 7, value) else try bindNull(stmt, 7);
    if (c.sqlite3_bind_double(stmt, 8, confidence) != c.SQLITE_OK) return error.BindFailed;
    try bindText(stmt, 9, metadata_json);
    try stepDone(stmt);
}

pub fn upsertRelationNatural(
    db: Database,
    relation_type: []const u8,
    source_id: u32,
    target_id: u32,
    confidence: f32,
    run_id: ?u32,
    patch_id: ?u32,
    metadata_json: []const u8,
    valid_from_unix: ?i64,
    valid_to_unix: ?i64,
) !u32 {
    const existing_stmt = try prepare(
        db,
        "SELECT relation_id FROM graph_relation WHERE relation_type = ?1 AND source_id = ?2 AND target_id = ?3 AND (run_id IS ?4 OR run_id = ?4) AND deprecated_at IS NULL ORDER BY relation_id LIMIT 1",
    );
    defer finalize(existing_stmt);
    try bindText(existing_stmt, 1, relation_type);
    try bindInt64(existing_stmt, 2, source_id);
    try bindInt64(existing_stmt, 3, target_id);
    if (run_id) |value| try bindInt64(existing_stmt, 4, value) else try bindNull(existing_stmt, 4);

    if (c.sqlite3_step(existing_stmt) == c.SQLITE_ROW) {
        const relation_id = try columnU32(existing_stmt, 0);
        const update_stmt = try prepare(
            db,
            "UPDATE graph_relation SET confidence = MAX(confidence, ?2), patch_id = COALESCE(?3, patch_id), metadata_json = CASE WHEN metadata_json = '{}' THEN ?4 WHEN ?4 = '{}' THEN metadata_json ELSE ?4 END, valid_from_unix = COALESCE(valid_from_unix, ?5), valid_to_unix = COALESCE(valid_to_unix, ?6) WHERE relation_id = ?1",
        );
        defer finalize(update_stmt);
        try bindInt64(update_stmt, 1, relation_id);
        if (c.sqlite3_bind_double(update_stmt, 2, confidence) != c.SQLITE_OK) return error.BindFailed;
        if (patch_id) |value| try bindInt64(update_stmt, 3, value) else try bindNull(update_stmt, 3);
        try bindText(update_stmt, 4, metadata_json);
        if (valid_from_unix) |value| try bindInt64(update_stmt, 5, value) else try bindNull(update_stmt, 5);
        if (valid_to_unix) |value| try bindInt64(update_stmt, 6, value) else try bindNull(update_stmt, 6);
        try stepDone(update_stmt);
        return relation_id;
    }

    const insert_stmt = try prepare(
        db,
        "INSERT INTO graph_relation(relation_type, source_id, target_id, confidence, run_id, patch_id, metadata_json, valid_from_unix, valid_to_unix, deprecated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL) RETURNING relation_id",
    );
    defer finalize(insert_stmt);
    try bindText(insert_stmt, 1, relation_type);
    try bindInt64(insert_stmt, 2, source_id);
    try bindInt64(insert_stmt, 3, target_id);
    if (c.sqlite3_bind_double(insert_stmt, 4, confidence) != c.SQLITE_OK) return error.BindFailed;
    if (run_id) |value| try bindInt64(insert_stmt, 5, value) else try bindNull(insert_stmt, 5);
    if (patch_id) |value| try bindInt64(insert_stmt, 6, value) else try bindNull(insert_stmt, 6);
    try bindText(insert_stmt, 7, metadata_json);
    if (valid_from_unix) |value| try bindInt64(insert_stmt, 8, value) else try bindNull(insert_stmt, 8);
    if (valid_to_unix) |value| try bindInt64(insert_stmt, 9, value) else try bindNull(insert_stmt, 9);
    if (c.sqlite3_step(insert_stmt) != c.SQLITE_ROW) return error.StepFailed;
    return try columnU32(insert_stmt, 0);
}

pub fn insertEntityAlias(db: Database, term: []const u8, entity_id: u32, confidence: f32) !void {
    const stmt = try prepare(
        db,
        "INSERT INTO graph_entity_alias(term, entity_id, confidence) VALUES (?1, ?2, ?3) " ++
            "ON CONFLICT(term, entity_id) DO UPDATE SET confidence = MAX(graph_entity_alias.confidence, excluded.confidence)",
    );
    defer finalize(stmt);
    try bindText(stmt, 1, term);
    try bindInt64(stmt, 2, entity_id);
    if (c.sqlite3_bind_double(stmt, 3, confidence) != c.SQLITE_OK) return error.BindFailed;
    try stepDone(stmt);
}

pub fn insertEntityDocument(
    db: Database,
    entity_id: u32,
    doc_id: u64,
    table_id: u64,
    role: ?[]const u8,
    confidence: f32,
) !void {
    const stmt = try prepare(db, "INSERT INTO graph_entity_document(entity_id, doc_id, table_id, role, confidence) VALUES (?1, ?2, ?3, ?4, ?5)");
    defer finalize(stmt);
    try bindInt64(stmt, 1, entity_id);
    try bindInt64(stmt, 2, doc_id);
    try bindInt64(stmt, 3, table_id);
    if (role) |value| try bindText(stmt, 4, value) else try bindNull(stmt, 4);
    if (c.sqlite3_bind_double(stmt, 5, confidence) != c.SQLITE_OK) return error.BindFailed;
    try stepDone(stmt);
}

pub fn upsertEntityDocument(
    db: Database,
    entity_id: u32,
    doc_id: u64,
    table_id: u64,
    role: ?[]const u8,
    confidence: f32,
) !void {
    const stmt = try prepare(
        db,
        "INSERT INTO graph_entity_document(entity_id, doc_id, table_id, role, confidence) VALUES (?1, ?2, ?3, ?4, ?5) " ++
            "ON CONFLICT(entity_id, doc_id, table_id) DO UPDATE SET role = excluded.role, confidence = excluded.confidence",
    );
    defer finalize(stmt);
    try bindInt64(stmt, 1, entity_id);
    try bindInt64(stmt, 2, doc_id);
    try bindInt64(stmt, 3, table_id);
    if (role) |value| try bindText(stmt, 4, value) else try bindNull(stmt, 4);
    if (c.sqlite3_bind_double(stmt, 5, confidence) != c.SQLITE_OK) return error.BindFailed;
    try stepDone(stmt);
}

pub fn upsertEntityChunk(
    db: Database,
    entity_id: u32,
    workspace_id: []const u8,
    collection_id: []const u8,
    doc_id: u64,
    chunk_index: u32,
    role: ?[]const u8,
    confidence: f32,
    metadata_json: []const u8,
) !void {
    const stmt = try prepare(
        db,
        "INSERT INTO graph_entity_chunk(entity_id, workspace_id, collection_id, doc_id, chunk_index, role, confidence, metadata_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) " ++
            "ON CONFLICT(entity_id, workspace_id, collection_id, doc_id, chunk_index) DO UPDATE SET role = excluded.role, confidence = excluded.confidence, metadata_json = excluded.metadata_json",
    );
    defer finalize(stmt);
    try bindInt64(stmt, 1, entity_id);
    try bindText(stmt, 2, workspace_id);
    try bindText(stmt, 3, collection_id);
    try bindInt64(stmt, 4, doc_id);
    try bindInt64(stmt, 5, chunk_index);
    if (role) |value| try bindText(stmt, 6, value) else try bindNull(stmt, 6);
    if (c.sqlite3_bind_double(stmt, 7, confidence) != c.SQLITE_OK) return error.BindFailed;
    try bindText(stmt, 8, metadata_json);
    try stepDone(stmt);
}

pub fn upsertEntityNatural(
    db: Database,
    allocator: std.mem.Allocator,
    entity_type: []const u8,
    name: []const u8,
    confidence: f32,
    metadata_json: []const u8,
) !u32 {
    _ = allocator;
    const stmt = try prepare(
        db,
        "INSERT INTO graph_entity(entity_type, name, confidence, metadata_json, deprecated_at) VALUES (?1, ?2, ?3, ?4, NULL) " ++ "ON CONFLICT(entity_type, name) DO UPDATE SET confidence = MAX(graph_entity.confidence, excluded.confidence), " ++ "metadata_json = CASE " ++ "WHEN graph_entity.metadata_json = '{}' THEN excluded.metadata_json " ++ "WHEN excluded.metadata_json = '{}' THEN graph_entity.metadata_json " ++ "ELSE excluded.metadata_json END, " ++ "deprecated_at = NULL " ++ "RETURNING entity_id",
    );
    defer finalize(stmt);
    try bindText(stmt, 1, entity_type);
    try bindText(stmt, 2, name);
    if (c.sqlite3_bind_double(stmt, 3, confidence) != c.SQLITE_OK) return error.BindFailed;
    try bindText(stmt, 4, metadata_json);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StepFailed;
    return try columnU32(stmt, 0);
}

pub fn deprecateEntity(db: Database, entity_id: u32) !void {
    const stmt = try prepare(db, "UPDATE graph_entity SET deprecated_at = unixepoch() WHERE entity_id = ?1");
    defer finalize(stmt);
    try bindInt64(stmt, 1, entity_id);
    try stepDone(stmt);
}

pub fn findEntitiesByType(
    db: Database,
    allocator: std.mem.Allocator,
    entity_type: []const u8,
) ![]EntityRecordFull {
    const stmt = try prepare(
        db,
        "SELECT entity_id, entity_type, name, confidence, metadata_json, deprecated_at, created_at_unix FROM graph_entity WHERE entity_type = ?1 AND deprecated_at IS NULL ORDER BY confidence DESC, name ASC",
    );
    defer finalize(stmt);
    try bindText(stmt, 1, entity_type);

    var rows = std.ArrayList(EntityRecordFull).empty;
    defer {
        for (rows.items) |row| {
            allocator.free(row.entity_type);
            allocator.free(row.name);
            allocator.free(row.metadata_json);
        }
        rows.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(allocator, .{
            .entity_id = try columnU32(stmt, 0),
            .entity_type = try dupeColumnText(allocator, stmt, 1),
            .name = try dupeColumnText(allocator, stmt, 2),
            .confidence = @floatCast(c.sqlite3_column_double(stmt, 3)),
            .metadata_json = try dupeColumnText(allocator, stmt, 4),
            .deprecated_at_unix = columnOptionalI64(stmt, 5),
            .created_at_unix = @intCast(c.sqlite3_column_int64(stmt, 6)),
        });
    }

    return rows.toOwnedSlice(allocator);
}

pub fn registerAliases(db: Database, entity_id: u32, terms: []const []const u8, confidence: f32) !void {
    for (terms) |term| {
        const stmt = try prepare(
            db,
            "INSERT INTO graph_entity_alias(term, entity_id, confidence) VALUES (?1, ?2, ?3) " ++ "ON CONFLICT(term, entity_id) DO UPDATE SET confidence = MAX(graph_entity_alias.confidence, excluded.confidence)",
        );
        defer finalize(stmt);
        try bindText(stmt, 1, term);
        try bindInt64(stmt, 2, entity_id);
        if (c.sqlite3_bind_double(stmt, 3, confidence) != c.SQLITE_OK) return error.BindFailed;
        try stepDone(stmt);
    }
}

pub fn resolveTerms(
    db: Database,
    allocator: std.mem.Allocator,
    terms: []const []const u8,
    min_confidence: f32,
) !roaring.Bitmap {
    var result = try roaring.Bitmap.empty();
    errdefer result.deinit();

    const stmt = try prepare(
        db,
        "SELECT entity_id FROM graph_entity_alias WHERE term = ?1 AND confidence >= ?2 ORDER BY confidence DESC, entity_id ASC",
    );
    defer finalize(stmt);

    for (terms) |term| {
        try resetStatement(stmt);
        try bindText(stmt, 1, term);
        if (c.sqlite3_bind_double(stmt, 2, min_confidence) != c.SQLITE_OK) return error.BindFailed;

        while (true) {
            const rc = c.sqlite3_step(stmt);
            if (rc == c.SQLITE_DONE) break;
            if (rc != c.SQLITE_ROW) return error.StepFailed;
            result.add(try columnU32(stmt, 0));
        }
    }

    _ = allocator;
    return result;
}

pub fn entityDocs(
    db: Database,
    allocator: std.mem.Allocator,
    entity_ids: roaring.Bitmap,
    table_id: ?u64,
) ![]EntityDocumentLink {
    const ids = try entity_ids.toArray(allocator);
    defer allocator.free(ids);

    var docs = std.ArrayList(EntityDocumentLink).empty;
    defer {
        for (docs.items) |doc| if (doc.role) |role| allocator.free(role);
        docs.deinit(allocator);
    }

    const stmt = try prepare(
        db,
        "SELECT entity_id, doc_id, table_id, role, confidence FROM graph_entity_document WHERE entity_id = ?1 AND (?2 IS NULL OR table_id = ?2) ORDER BY confidence DESC, doc_id ASC",
    );
    defer finalize(stmt);

    for (ids) |entity_id| {
        try resetStatement(stmt);
        try bindInt64(stmt, 1, entity_id);
        if (table_id) |value| try bindInt64(stmt, 2, value) else try bindNull(stmt, 2);
        while (true) {
            const rc = c.sqlite3_step(stmt);
            if (rc == c.SQLITE_DONE) break;
            if (rc != c.SQLITE_ROW) return error.StepFailed;
            try docs.append(allocator, .{
                .entity_id = try columnU32(stmt, 0),
                .doc_id = try columnU64(stmt, 1),
                .table_id = try columnU64(stmt, 2),
                .role = if (c.sqlite3_column_type(stmt, 3) == c.SQLITE_NULL) null else try dupeColumnText(allocator, stmt, 3),
                .confidence = @floatCast(c.sqlite3_column_double(stmt, 4)),
            });
        }
    }

    return docs.toOwnedSlice(allocator);
}

pub fn loadEntityFull(
    db: Database,
    allocator: std.mem.Allocator,
    entity_id: u32,
) !EntityRecordFull {
    const stmt = try prepare(
        db,
        "SELECT entity_id, entity_type, name, confidence, metadata_json, deprecated_at, created_at_unix FROM graph_entity WHERE entity_id = ?1",
    );
    defer finalize(stmt);
    try bindInt64(stmt, 1, entity_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;
    return .{
        .entity_id = try columnU32(stmt, 0),
        .entity_type = try dupeColumnText(allocator, stmt, 1),
        .name = try dupeColumnText(allocator, stmt, 2),
        .confidence = @floatCast(c.sqlite3_column_double(stmt, 3)),
        .metadata_json = try dupeColumnText(allocator, stmt, 4),
        .deprecated_at_unix = columnOptionalI64(stmt, 5),
        .created_at_unix = @intCast(c.sqlite3_column_int64(stmt, 6)),
    };
}

pub fn getEntity(
    db: Database,
    allocator: std.mem.Allocator,
    entity_id: u32,
) !EntityRecordFull {
    return try loadEntityFull(db, allocator, entity_id);
}

pub fn findEntityByName(
    db: Database,
    allocator: std.mem.Allocator,
    name: []const u8,
) !?EntityRecordFull {
    const stmt = try prepare(
        db,
        "SELECT entity_id, entity_type, name, confidence, metadata_json, deprecated_at, created_at_unix FROM graph_entity WHERE name = ?1 AND deprecated_at IS NULL LIMIT 1",
    );
    defer finalize(stmt);
    try bindText(stmt, 1, name);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return null;
    return .{
        .entity_id = try columnU32(stmt, 0),
        .entity_type = try dupeColumnText(allocator, stmt, 1),
        .name = try dupeColumnText(allocator, stmt, 2),
        .confidence = @floatCast(c.sqlite3_column_double(stmt, 3)),
        .metadata_json = try dupeColumnText(allocator, stmt, 4),
        .deprecated_at_unix = columnOptionalI64(stmt, 5),
        .created_at_unix = @intCast(c.sqlite3_column_int64(stmt, 6)),
    };
}

pub fn getEntityByName(
    db: Database,
    allocator: std.mem.Allocator,
    name: []const u8,
) !EntityRecordFull {
    return (try findEntityByName(db, allocator, name)) orelse error.MissingRow;
}

/// Bulk lookup mirroring `graph.find_entities_by_names(p_names text[])`.
/// Preserves the order of `names`; missing or deprecated entities are
/// silently dropped, matching the PostgreSQL semantics.
pub fn findEntitiesByNames(
    db: Database,
    allocator: std.mem.Allocator,
    names: []const []const u8,
) ![]EntityRecordFull {
    var rows = std.ArrayList(EntityRecordFull).empty;
    errdefer {
        for (rows.items) |*row| row.deinit(allocator);
        rows.deinit(allocator);
    }
    for (names) |name| {
        if (try findEntityByName(db, allocator, name)) |row| {
            try rows.append(allocator, row);
        }
    }
    return rows.toOwnedSlice(allocator);
}

/// Alias of `findEntitiesByNames` to match the PG `get_entities_by_names`
/// surface (which is itself an alias of `find_entities_by_names`).
pub fn getEntitiesByNames(
    db: Database,
    allocator: std.mem.Allocator,
    names: []const []const u8,
) ![]EntityRecordFull {
    return findEntitiesByNames(db, allocator, names);
}

/// JSON containment lookup mirroring `graph.find_entities_by_metadata`.
/// `metadata_pattern_json` must be a JSON object literal; every (key,
/// value) pair from the pattern must be present in the entity's
/// `metadata_json` for the row to match. Values are compared via SQLite's
/// `json_extract` to keep the SQLite implementation entirely SQL-driven.
pub fn findEntitiesByMetadata(
    db: Database,
    allocator: std.mem.Allocator,
    metadata_pattern_json: []const u8,
) ![]EntityRecordFull {
    const stmt = try prepare(
        db,
        \\WITH pattern AS (SELECT ?1 AS data),
        \\     filtered AS (
        \\         SELECT e.entity_id, e.entity_type, e.name, e.confidence,
        \\                e.metadata_json, e.deprecated_at, e.created_at_unix
        \\         FROM graph_entity e
        \\         WHERE e.deprecated_at IS NULL
        \\           AND NOT EXISTS (
        \\               SELECT 1
        \\               FROM json_each((SELECT data FROM pattern)) p
        \\               WHERE json_extract(e.metadata_json, '$.' || p.key) IS NOT p.value
        \\                  OR json_extract(e.metadata_json, '$.' || p.key) IS NULL
        \\           )
        \\     )
        \\SELECT entity_id, entity_type, name, confidence, metadata_json,
        \\       deprecated_at, created_at_unix
        \\FROM filtered
        \\ORDER BY confidence DESC, name ASC
        ,
    );
    defer finalize(stmt);
    try bindText(stmt, 1, metadata_pattern_json);

    var rows = std.ArrayList(EntityRecordFull).empty;
    errdefer {
        for (rows.items) |*row| row.deinit(allocator);
        rows.deinit(allocator);
    }
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(allocator, .{
            .entity_id = try columnU32(stmt, 0),
            .entity_type = try dupeColumnText(allocator, stmt, 1),
            .name = try dupeColumnText(allocator, stmt, 2),
            .confidence = @floatCast(c.sqlite3_column_double(stmt, 3)),
            .metadata_json = try dupeColumnText(allocator, stmt, 4),
            .deprecated_at_unix = columnOptionalI64(stmt, 5),
            .created_at_unix = @intCast(c.sqlite3_column_int64(stmt, 6)),
        });
    }
    return rows.toOwnedSlice(allocator);
}

/// Alias of `findEntitiesByMetadata` for parity with `get_entities_by_metadata`.
pub fn getEntitiesByMetadata(
    db: Database,
    allocator: std.mem.Allocator,
    metadata_pattern_json: []const u8,
) ![]EntityRecordFull {
    return findEntitiesByMetadata(db, allocator, metadata_pattern_json);
}

/// Soft-delete a relation row, mirroring `graph.deprecate_relation`.
pub fn deprecateRelation(db: Database, relation_id: u32) !void {
    const stmt = try prepare(
        db,
        "UPDATE graph_relation SET deprecated_at = unixepoch() WHERE relation_id = ?1 AND deprecated_at IS NULL",
    );
    defer finalize(stmt);
    try bindInt64(stmt, 1, relation_id);
    try stepDone(stmt);
}

pub fn setEntityWorkspaceId(
    db: Database,
    entity_id: u32,
    workspace_id: []const u8,
) !void {
    const stmt = try prepare(db, "UPDATE graph_entity SET workspace_id = ?2 WHERE entity_id = ?1");
    defer finalize(stmt);
    try bindInt64(stmt, 1, entity_id);
    try bindText(stmt, 2, workspace_id);
    try stepDone(stmt);
}

pub fn countEntitiesByWorkspace(
    db: Database,
    workspace_id: []const u8,
) !u64 {
    const stmt = try prepare(db, "SELECT COUNT(*) FROM graph_entity WHERE workspace_id = ?1 AND deprecated_at IS NULL");
    defer finalize(stmt);
    try bindText(stmt, 1, workspace_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StepFailed;
    return try columnU64(stmt, 0);
}

pub fn loadEntityIdByName(db: Database, name: []const u8) !u32 {
    const stmt = try prepare(db, "SELECT entity_id FROM graph_entity WHERE name = ?1 AND deprecated_at IS NULL LIMIT 1");
    defer finalize(stmt);
    try bindText(stmt, 1, name);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;
    return try columnU32(stmt, 0);
}

pub fn entityFtsSearch(
    db: Database,
    allocator: std.mem.Allocator,
    query: []const u8,
    type_filter: ?[]const []const u8,
    domain_filter: ?[]const u8,
    min_confidence: f32,
    limit: usize,
) ![]EntitySearchResult {
    if (limit == 0) return allocator.alloc(EntitySearchResult, 0);

    const stmt = try prepare(
        db,
        "SELECT entity_id, entity_type, name, confidence, metadata_json, COALESCE(json_extract(metadata_json, '$.domain'), '') FROM graph_entity WHERE deprecated_at IS NULL AND confidence >= ?1 ORDER BY confidence DESC, name ASC",
    );
    defer finalize(stmt);
    if (c.sqlite3_bind_double(stmt, 1, min_confidence) != c.SQLITE_OK) return error.BindFailed;

    const query_score_terms = try splitSearchTerms(allocator, query);
    defer deinitStringSlice(allocator, query_score_terms);

    var results = std.ArrayList(EntitySearchResult).empty;
    defer {
        for (results.items) |row| {
            allocator.free(row.name);
            allocator.free(row.entity_type);
            allocator.free(row.metadata_json);
        }
        results.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const entity_type = try dupeColumnText(allocator, stmt, 1);
        const domain = try dupeColumnText(allocator, stmt, 5);
        defer allocator.free(domain);

        if (type_filter) |types| {
            var matched_type = false;
            for (types) |type_name| {
                if (std.mem.eql(u8, type_name, entity_type)) {
                    matched_type = true;
                    break;
                }
            }
            if (!matched_type) {
                allocator.free(entity_type);
                continue;
            }
        }

        if (domain_filter) |wanted_domain| {
            if (!std.mem.eql(u8, wanted_domain, domain)) {
                allocator.free(entity_type);
                continue;
            }
        }

        const name = try dupeColumnText(allocator, stmt, 2);
        const metadata_json = try dupeColumnText(allocator, stmt, 4);
        const score = try ftsScore(allocator, query_score_terms, name, metadata_json, entity_type, domain);
        if (score <= 0) {
            allocator.free(entity_type);
            allocator.free(name);
            allocator.free(metadata_json);
            continue;
        }

        try results.append(allocator, .{
            .entity_id = try columnU32(stmt, 0),
            .name = name,
            .entity_type = entity_type,
            .confidence = @floatCast(c.sqlite3_column_double(stmt, 3)),
            .fts_rank = score,
            .metadata_json = metadata_json,
        });
    }

    std.mem.sort(EntitySearchResult, results.items, {}, struct {
        fn lessThan(_: void, lhs: EntitySearchResult, rhs: EntitySearchResult) bool {
            if (lhs.fts_rank != rhs.fts_rank) return lhs.fts_rank > rhs.fts_rank;
            if (lhs.confidence != rhs.confidence) return lhs.confidence > rhs.confidence;
            return std.mem.order(u8, lhs.name, rhs.name) == .lt;
        }
    }.lessThan);

    const n = @min(results.items.len, limit);
    if (results.items.len > n) {
        for (results.items[n..]) |row| {
            allocator.free(row.name);
            allocator.free(row.entity_type);
            allocator.free(row.metadata_json);
        }
        results.shrinkRetainingCapacity(n);
    }

    return results.toOwnedSlice(allocator);
}

pub fn marketplaceSearch(
    db: Database,
    allocator: std.mem.Allocator,
    query: []const u8,
    domain_filter: ?[]const u8,
    min_confidence: f32,
    max_hops: usize,
    limit: usize,
) ![]MarketplaceResult {
    const marketplace_edge_types = [_][]const u8{ "related_to", "requires", "improves", "supersedes", "contains" };
    const seeds = try entityFtsSearch(db, allocator, query, null, domain_filter, min_confidence, 200);
    defer {
        for (seeds) |seed| {
            allocator.free(seed.name);
            allocator.free(seed.entity_type);
            allocator.free(seed.metadata_json);
        }
        allocator.free(seeds);
    }

    if (seeds.len == 0) return allocator.alloc(MarketplaceResult, 0);

    const candidate_hits = try entityFtsSearch(db, allocator, query, null, domain_filter, 0.0, 1000);
    defer {
        for (candidate_hits) |hit| {
            allocator.free(hit.name);
            allocator.free(hit.entity_type);
            allocator.free(hit.metadata_json);
        }
        allocator.free(candidate_hits);
    }

    var runtime = try loadRuntime(db, allocator);
    defer runtime.deinit();

    var seed_ids = try allocator.alloc(u32, seeds.len);
    defer allocator.free(seed_ids);
    for (seeds, 0..) |seed, index| seed_ids[index] = seed.entity_id;

    var reachable = try runtime.kHops(allocator, seed_ids, max_hops, .{ .edge_types = marketplace_edge_types[0..] });
    defer reachable.deinit();

    const reachable_ids = try reachable.toArray(allocator);
    defer allocator.free(reachable_ids);

    var reachable_set = std.AutoHashMap(u32, void).init(allocator);
    defer reachable_set.deinit();
    for (reachable_ids) |id| _ = try reachable_set.put(id, {});

    var seed_set = std.AutoHashMap(u32, void).init(allocator);
    defer seed_set.deinit();
    for (seeds) |seed| _ = try seed_set.put(seed.entity_id, {});

    var candidate_rank = std.AutoHashMap(u32, f32).init(allocator);
    defer candidate_rank.deinit();
    for (candidate_hits) |hit| {
        _ = try candidate_rank.put(hit.entity_id, hit.fts_rank);
    }

    const stmt = try prepare(
        db,
        "SELECT entity_id, name, entity_type, confidence, metadata_json, COALESCE((SELECT total_degree FROM graph_entity_degree d WHERE d.entity_id = graph_entity.entity_id), 0) FROM graph_entity WHERE deprecated_at IS NULL AND confidence >= ?1 ORDER BY confidence DESC, name ASC",
    );
    defer finalize(stmt);
    if (c.sqlite3_bind_double(stmt, 1, min_confidence) != c.SQLITE_OK) return error.BindFailed;

    var results = std.ArrayList(MarketplaceResult).empty;
    defer {
        for (results.items) |row| {
            allocator.free(row.name);
            allocator.free(row.entity_type);
            allocator.free(row.metadata_json);
        }
        results.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const entity_id = try columnU32(stmt, 0);
        if (!reachable_set.contains(entity_id)) continue;

        const name = try dupeColumnText(allocator, stmt, 1);
        const entity_type = try dupeColumnText(allocator, stmt, 2);
        const metadata_json = try dupeColumnText(allocator, stmt, 4);
        const fts_rank = candidate_rank.get(entity_id) orelse 0.0;
        const confidence: f32 = @floatCast(c.sqlite3_column_double(stmt, 3));
        const total_degree: f32 = @floatCast(c.sqlite3_column_double(stmt, 5));
        const degree_log = std.math.log(f64, std.math.e, @as(f64, total_degree) + 1.0);
        const hub_score: f32 = @floatCast(@min(1.0, degree_log / 4.0));
        const direct_boost: f32 = if (seed_set.contains(entity_id)) 1.5 else 1.0;
        const hub_boost: f32 = @floatCast(@min(0.5, degree_log / 8.0));
        const composite_score = (if (fts_rank > 0) fts_rank else 0.1) * confidence * direct_boost * (1.0 + hub_boost);
        if (composite_score <= 0) {
            allocator.free(name);
            allocator.free(entity_type);
            allocator.free(metadata_json);
            continue;
        }
        try results.append(allocator, .{
            .entity_id = entity_id,
            .name = name,
            .entity_type = entity_type,
            .confidence = confidence,
            .fts_rank = fts_rank,
            .is_direct_match = seed_set.contains(entity_id),
            .hub_score = hub_score,
            .composite_score = composite_score,
            .metadata_json = metadata_json,
        });
    }

    std.mem.sort(MarketplaceResult, results.items, {}, struct {
        fn lessThan(_: void, lhs: MarketplaceResult, rhs: MarketplaceResult) bool {
            if (lhs.composite_score != rhs.composite_score) return lhs.composite_score > rhs.composite_score;
            return std.mem.order(u8, lhs.name, rhs.name) == .lt;
        }
    }.lessThan);

    if (results.items.len > limit) {
        for (results.items[limit..]) |row| {
            allocator.free(row.name);
            allocator.free(row.entity_type);
            allocator.free(row.metadata_json);
        }
        results.shrinkRetainingCapacity(limit);
    }

    return results.toOwnedSlice(allocator);
}

pub fn skillDependencies(
    db: Database,
    allocator: std.mem.Allocator,
    entity_id: u32,
    max_depth: usize,
    min_confidence: f32,
) ![]SkillDependency {
    const stmt = try prepare(
        db,
        "WITH RECURSIVE deps(dep_entity_id, relation_type, depth) AS (" ++ "SELECT r.target_id, r.relation_type, 1 FROM graph_relation r " ++ "WHERE r.source_id = ?1 AND r.relation_type IN ('requires', 'contains', 'uses', 'depends_on') " ++ "AND r.confidence >= ?2 AND r.deprecated_at IS NULL " ++ "UNION " ++ "SELECT r.target_id, r.relation_type, d.depth + 1 FROM deps d JOIN graph_relation r ON r.source_id = d.dep_entity_id " ++ "WHERE r.relation_type IN ('requires', 'contains', 'uses', 'depends_on') " ++ "AND r.confidence >= ?2 AND r.deprecated_at IS NULL AND d.depth < ?3" ++ ") " ++ "SELECT e.entity_id, e.name, e.entity_type, e.confidence, d.relation_type, d.depth " ++ "FROM deps d JOIN graph_entity e ON e.entity_id = d.dep_entity_id " ++ "WHERE e.confidence >= ?2 AND e.deprecated_at IS NULL ORDER BY d.depth, e.entity_type, e.name",
    );
    defer finalize(stmt);
    try bindInt64(stmt, 1, entity_id);
    if (c.sqlite3_bind_double(stmt, 2, min_confidence) != c.SQLITE_OK) return error.BindFailed;
    try bindInt64(stmt, 3, max_depth);

    var deps = std.ArrayList(SkillDependency).empty;
    defer {
        for (deps.items) |dep| {
            allocator.free(dep.dep_name);
            allocator.free(dep.dep_type);
            allocator.free(dep.relation_type);
        }
        deps.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try deps.append(allocator, .{
            .dep_entity_id = try columnU32(stmt, 0),
            .dep_name = try dupeColumnText(allocator, stmt, 1),
            .dep_type = try dupeColumnText(allocator, stmt, 2),
            .dep_confidence = @floatCast(c.sqlite3_column_double(stmt, 3)),
            .relation_type = try dupeColumnText(allocator, stmt, 4),
            .depth = @intCast(c.sqlite3_column_int(stmt, 5)),
        });
    }

    return deps.toOwnedSlice(allocator);
}

pub fn confidenceDecay(
    db: Database,
    entity_id: u32,
    half_life_days: usize,
) !f32 {
    const stmt = try prepare(
        db,
        "SELECT MAX(er.created_at_unix) FROM graph_relation r JOIN graph_execution_run er ON er.id = r.run_id WHERE (r.source_id = ?1 OR r.target_id = ?1) AND er.outcome = 'success'",
    );
    defer finalize(stmt);
    try bindInt64(stmt, 1, entity_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StepFailed;
    const last_seen = columnOptionalI64(stmt, 0);
    if (last_seen == null) return 0.1;

    var entity = try loadEntityFull(db, std.heap.page_allocator, entity_id);
    defer entity.deinit(std.heap.page_allocator);
    const now = unixTimestamp();
    const age_days = @as(f64, @floatFromInt(if (now > last_seen.?) now - last_seen.? else 0)) / 86400.0;
    const half_life = if (half_life_days == 0) 90.0 else @as(f64, @floatFromInt(half_life_days));
    const decay = @as(f32, @floatCast(entity.confidence * @as(f32, @floatCast(std.math.pow(f64, 0.5, age_days / half_life)))));
    return decay;
}

fn unixTimestamp() i64 {
    return std.Io.Timestamp.now(compat.io(), .real).toSeconds();
}

/// Name-based overload of `graph.confidence_decay(text, ...)` that
/// returns `null` (signaled with a sentinel `NaN`) when the entity is not
/// known. The PG SQL function returns SQL `NULL` directly; on the Zig
/// side we surface "missing" as an optional rather than as `NaN`.
pub fn confidenceDecayByName(
    db: Database,
    name: []const u8,
    half_life_days: usize,
) !?f32 {
    const id = loadEntityIdByName(db, name) catch |err| switch (err) {
        error.MissingRow => return null,
        else => return err,
    };
    return try confidenceDecay(db, id, half_life_days);
}

pub fn entityNeighborhood(
    db: Database,
    allocator: std.mem.Allocator,
    entity_id: u32,
    max_out: usize,
    max_in: usize,
    min_confidence: f32,
) ![]u8 {
    var entity = try loadEntityFull(db, allocator, entity_id);
    defer entity.deinit(allocator);

    const out_stmt = try prepare(
        db,
        "SELECT r.relation_type, r.confidence, e.entity_id, e.name FROM graph_relation r JOIN graph_entity e ON e.entity_id = r.target_id WHERE r.source_id = ?1 AND r.deprecated_at IS NULL AND r.confidence >= ?2 ORDER BY r.confidence DESC, r.relation_id ASC LIMIT ?3",
    );
    defer finalize(out_stmt);
    try bindInt64(out_stmt, 1, entity_id);
    if (c.sqlite3_bind_double(out_stmt, 2, min_confidence) != c.SQLITE_OK) return error.BindFailed;
    try bindInt64(out_stmt, 3, max_out);

    const in_stmt = try prepare(
        db,
        "SELECT r.relation_type, r.confidence, e.entity_id, e.name FROM graph_relation r JOIN graph_entity e ON e.entity_id = r.source_id WHERE r.target_id = ?1 AND r.deprecated_at IS NULL AND r.confidence >= ?2 ORDER BY r.confidence DESC, r.relation_id ASC LIMIT ?3",
    );
    defer finalize(in_stmt);
    try bindInt64(in_stmt, 1, entity_id);
    if (c.sqlite3_bind_double(in_stmt, 2, min_confidence) != c.SQLITE_OK) return error.BindFailed;
    try bindInt64(in_stmt, 3, max_in);

    var outgoing = std.ArrayList(NeighborhoodEdge).empty;
    defer {
        for (outgoing.items) |edge| {
            allocator.free(edge.type);
            if (edge.target_name) |value| allocator.free(value);
        }
        outgoing.deinit(allocator);
    }
    while (true) {
        const rc = c.sqlite3_step(out_stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try outgoing.append(allocator, .{
            .type = try dupeColumnText(allocator, out_stmt, 0),
            .confidence = @floatCast(c.sqlite3_column_double(out_stmt, 1)),
            .target_id = try columnU32(out_stmt, 2),
            .target_name = try dupeColumnText(allocator, out_stmt, 3),
        });
    }

    var incoming = std.ArrayList(NeighborhoodEdge).empty;
    defer {
        for (incoming.items) |edge| {
            allocator.free(edge.type);
            if (edge.target_name) |value| allocator.free(value);
            if (edge.source_name) |value| allocator.free(value);
        }
        incoming.deinit(allocator);
    }
    while (true) {
        const rc = c.sqlite3_step(in_stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try incoming.append(allocator, .{
            .type = try dupeColumnText(allocator, in_stmt, 0),
            .confidence = @floatCast(c.sqlite3_column_double(in_stmt, 1)),
            .source_id = try columnU32(in_stmt, 2),
            .source_name = try dupeColumnText(allocator, in_stmt, 3),
        });
    }

    var payload = std.Io.Writer.Allocating.init(allocator);
    defer payload.deinit();
    const created_at = entity.created_at_unix;
    try payload.writer.print("{f}", .{std.json.fmt(.{
        .entity = .{
            .id = entity.entity_id,
            .type = entity.entity_type,
            .name = entity.name,
            .confidence = entity.confidence,
            .metadata = entity.metadata_json,
            .deprecated_at = entity.deprecated_at_unix,
            .created_at = created_at,
        },
        .outgoing = outgoing.items,
        .incoming = incoming.items,
    }, .{})});
    return try payload.toOwnedSlice();
}

pub fn learnFromRun(
    db: Database,
    allocator: std.mem.Allocator,
    run_key: []const u8,
    domain: []const u8,
    outcome: []const u8,
    concepts: []const ConceptInput,
    relations: []const RelationInput,
    transcript: ?[]const u8,
    metadata_json: ?[]const u8,
) !u32 {
    const run_confidence: f32 = if (std.mem.eql(u8, outcome, "success")) 1.0 else if (std.mem.eql(u8, outcome, "partial")) 0.6 else 0.2;
    const transcript_value = transcript orelse "";
    const metadata_value = metadata_json orelse "{}";

    const insert_run = try prepare(
        db,
        "INSERT INTO graph_execution_run(run_key, domain, outcome, confidence, transcript, metadata_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6) " ++ "ON CONFLICT(run_key) DO UPDATE SET outcome = excluded.outcome, confidence = excluded.confidence, transcript = COALESCE(excluded.transcript, graph_execution_run.transcript), metadata_json = COALESCE(excluded.metadata_json, graph_execution_run.metadata_json) " ++ "RETURNING id",
    );
    defer finalize(insert_run);
    try bindText(insert_run, 1, run_key);
    try bindText(insert_run, 2, domain);
    try bindText(insert_run, 3, outcome);
    if (c.sqlite3_bind_double(insert_run, 4, run_confidence) != c.SQLITE_OK) return error.BindFailed;
    try bindText(insert_run, 5, transcript_value);
    try bindText(insert_run, 6, metadata_value);
    if (c.sqlite3_step(insert_run) != c.SQLITE_ROW) return error.StepFailed;
    const run_id = try columnU32(insert_run, 0);

    for (concepts) |concept| {
        const metadata = concept.metadata_json orelse "{}";
        _ = try upsertEntityNatural(db, allocator, concept.entity_type, concept.name, concept.confidence, metadata);
    }

    for (relations) |relation| {
        const src_id = try loadEntityIdByName(db, relation.source);
        const tgt_id = try loadEntityIdByName(db, relation.target);
        _ = try upsertRelationNatural(
            db,
            relation.relation_type,
            src_id,
            tgt_id,
            relation.confidence,
            run_id,
            null,
            relation.metadata_json orelse "{}",
            relation.valid_from_unix,
            relation.valid_to_unix,
        );
    }

    try refreshEntityDegree(db);
    try rebuildAdjacency(db, allocator);
    return run_id;
}

pub fn applyKnowledgePatch(
    db: Database,
    allocator: std.mem.Allocator,
    patch_key: []const u8,
    domain: []const u8,
    confidence: f32,
    artifacts: []const KnowledgePatchArtifact,
    applied_by: []const u8,
) !u32 {
    var artifacts_buf = std.Io.Writer.Allocating.init(allocator);
    defer artifacts_buf.deinit();
    try artifacts_buf.writer.print("{f}", .{std.json.fmt(artifacts, .{})});
    const artifacts_json = try artifacts_buf.toOwnedSlice();
    defer allocator.free(artifacts_json);

    const insert_patch = try prepare(
        db,
        "INSERT INTO graph_knowledge_patch(patch_key, domain, status, confidence, artifacts_json, applied_by) VALUES (?1, ?2, 'approved', ?3, ?4, ?5) " ++ "ON CONFLICT(patch_key) DO UPDATE SET domain = excluded.domain, confidence = excluded.confidence, artifacts_json = excluded.artifacts_json, applied_by = excluded.applied_by, status = 'approved' " ++ "RETURNING id",
    );
    defer finalize(insert_patch);
    try bindText(insert_patch, 1, patch_key);
    try bindText(insert_patch, 2, domain);
    if (c.sqlite3_bind_double(insert_patch, 3, confidence) != c.SQLITE_OK) return error.BindFailed;
    try bindText(insert_patch, 4, artifacts_json);
    try bindText(insert_patch, 5, applied_by);
    if (c.sqlite3_step(insert_patch) != c.SQLITE_ROW) return error.StepFailed;
    const patch_id = try columnU32(insert_patch, 0);

    var applied_count: u32 = 0;
    for (artifacts) |artifact| {
        if (std.mem.eql(u8, artifact.action, "upsert_entity")) {
            if (artifact.entity_type == null or artifact.name == null) return error.MissingRow;
            _ = try upsertEntityNatural(
                db,
                allocator,
                artifact.entity_type.?,
                artifact.name.?,
                artifact.confidence orelse confidence,
                artifact.metadata_json orelse "{}",
            );
            applied_count += 1;
            continue;
        }

        if (std.mem.eql(u8, artifact.action, "upsert_relation")) {
            if (artifact.relation_type == null or artifact.source == null or artifact.target == null) return error.MissingRow;
            const src_id = try loadEntityIdByName(db, artifact.source.?);
            const tgt_id = try loadEntityIdByName(db, artifact.target.?);
            _ = try upsertRelationNatural(
                db,
                artifact.relation_type.?,
                src_id,
                tgt_id,
                artifact.confidence orelse confidence,
                null,
                patch_id,
                artifact.metadata_json orelse "{}",
                null,
                null,
            );
            applied_count += 1;
            continue;
        }

        if (std.mem.eql(u8, artifact.action, "deprecate")) {
            if (artifact.relation_type == null or artifact.source == null or artifact.target == null) return error.MissingRow;
            const src_id = try loadEntityIdByName(db, artifact.source.?);
            const tgt_id = try loadEntityIdByName(db, artifact.target.?);
            const stmt = try prepare(
                db,
                "UPDATE graph_relation SET deprecated_at = unixepoch(), patch_id = ?4 WHERE relation_type = ?1 AND source_id = ?2 AND target_id = ?3 AND deprecated_at IS NULL",
            );
            defer finalize(stmt);
            try bindText(stmt, 1, artifact.relation_type.?);
            try bindInt64(stmt, 2, src_id);
            try bindInt64(stmt, 3, tgt_id);
            try bindInt64(stmt, 4, patch_id);
            try stepDone(stmt);
            applied_count += 1;
            continue;
        }

        return error.InvalidValue;
    }

    const update_patch = try prepare(db, "UPDATE graph_knowledge_patch SET status = 'applied', applied_at_unix = unixepoch() WHERE id = ?1");
    defer finalize(update_patch);
    try bindInt64(update_patch, 1, patch_id);
    try stepDone(update_patch);

    try refreshEntityDegree(db);
    try rebuildAdjacency(db, allocator);
    return applied_count;
}

pub fn insertAdjacency(db: Database, table_name: []const u8, entity_id: u32, relation_ids: []const u32, allocator: std.mem.Allocator) !void {
    const sql = try std.fmt.allocPrint(allocator, "INSERT INTO {s}(entity_id, relation_ids_blob) VALUES (?1, ?2)", .{table_name});
    defer allocator.free(sql);
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    var bitmap = try roaring.Bitmap.fromSlice(relation_ids);
    defer bitmap.deinit();
    const bytes = try bitmap.serializePortableStable(allocator);
    defer allocator.free(bytes);

    try bindInt64(stmt, 1, entity_id);
    try bindBlob(stmt, 2, bytes);
    try stepDone(stmt);
}

pub fn upsertAdjacency(db: Database, table_name: []const u8, entity_id: u32, relation_ids: []const u32, allocator: std.mem.Allocator) !void {
    const sql = try std.fmt.allocPrint(allocator, "INSERT OR REPLACE INTO {s}(entity_id, relation_ids_blob) VALUES (?1, ?2)", .{table_name});
    defer allocator.free(sql);
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    var bitmap = try roaring.Bitmap.fromSlice(relation_ids);
    defer bitmap.deinit();
    const bytes = try bitmap.serializePortableStable(allocator);
    defer allocator.free(bytes);

    try bindInt64(stmt, 1, entity_id);
    try bindBlob(stmt, 2, bytes);
    try stepDone(stmt);
}

pub fn deleteAdjacency(db: Database, table_name: []const u8, entity_id: u32, allocator: std.mem.Allocator) !void {
    const sql = try std.fmt.allocPrint(allocator, "DELETE FROM {s} WHERE entity_id = ?1", .{table_name});
    defer allocator.free(sql);
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, entity_id);
    try stepDone(stmt);
}

pub fn deleteRelation(db: Database, relation_id: u32) !void {
    const stmt = try prepare(db, "DELETE FROM graph_relation WHERE relation_id = ?1");
    defer finalize(stmt);
    try bindInt64(stmt, 1, relation_id);
    try stepDone(stmt);
}

pub fn rebuildAdjacency(db: Database, allocator: std.mem.Allocator) !void {
    try db.exec("DELETE FROM graph_lj_out");
    try db.exec("DELETE FROM graph_lj_in");

    const stmt = try prepare(db, "SELECT relation_id, source_id, target_id FROM graph_relation ORDER BY relation_id");
    defer finalize(stmt);

    var outgoing = std.AutoHashMap(u32, std.ArrayList(u32)).init(allocator);
    defer {
        var it = outgoing.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(allocator);
        outgoing.deinit();
    }

    var incoming = std.AutoHashMap(u32, std.ArrayList(u32)).init(allocator);
    defer {
        var it = incoming.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(allocator);
        incoming.deinit();
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const relation_id = try columnU32(stmt, 0);
        const source_id = try columnU32(stmt, 1);
        const target_id = try columnU32(stmt, 2);

        const outgoing_entry = try outgoing.getOrPut(source_id);
        if (!outgoing_entry.found_existing) outgoing_entry.value_ptr.* = .empty;
        try outgoing_entry.value_ptr.append(allocator, relation_id);

        const incoming_entry = try incoming.getOrPut(target_id);
        if (!incoming_entry.found_existing) incoming_entry.value_ptr.* = .empty;
        try incoming_entry.value_ptr.append(allocator, relation_id);
    }

    var outgoing_it = outgoing.iterator();
    while (outgoing_it.next()) |entry| {
        try upsertAdjacency(db, "graph_lj_out", entry.key_ptr.*, entry.value_ptr.items, allocator);
    }

    var incoming_it = incoming.iterator();
    while (incoming_it.next()) |entry| {
        try upsertAdjacency(db, "graph_lj_in", entry.key_ptr.*, entry.value_ptr.items, allocator);
    }

    try refreshEntityDegree(db);
}

pub fn rebuildLjRelations(db: Database, allocator: std.mem.Allocator) !void {
    try rebuildAdjacency(db, allocator);
}

pub fn rebuildLjForEntities(db: Database, entity_ids: []const u32, allocator: std.mem.Allocator) !void {
    if (entity_ids.len == 0) return;

    try db.exec("BEGIN IMMEDIATE");
    errdefer _ = db.exec("ROLLBACK") catch {};

    const out_stmt = try prepare(db, "DELETE FROM graph_lj_out WHERE entity_id = ?1");
    defer finalize(out_stmt);
    const in_stmt = try prepare(db, "DELETE FROM graph_lj_in WHERE entity_id = ?1");
    defer finalize(in_stmt);

    const rel_stmt = try prepare(db, "SELECT relation_id, source_id, target_id FROM graph_relation WHERE deprecated_at IS NULL ORDER BY relation_id");
    defer finalize(rel_stmt);

    var outgoing = std.AutoHashMap(u32, std.ArrayList(u32)).init(allocator);
    defer {
        var it = outgoing.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(allocator);
        outgoing.deinit();
    }

    var incoming = std.AutoHashMap(u32, std.ArrayList(u32)).init(allocator);
    defer {
        var it = incoming.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(allocator);
        incoming.deinit();
    }

    var target_set = std.AutoHashMap(u32, void).init(allocator);
    defer target_set.deinit();
    for (entity_ids) |entity_id| {
        _ = try target_set.put(entity_id, {});
    }

    while (true) {
        const rc = c.sqlite3_step(rel_stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const relation_id = try columnU32(rel_stmt, 0);
        const source_id = try columnU32(rel_stmt, 1);
        const target_id = try columnU32(rel_stmt, 2);
        if (!target_set.contains(source_id) and !target_set.contains(target_id)) continue;

        const outgoing_entry = try outgoing.getOrPut(source_id);
        if (!outgoing_entry.found_existing) outgoing_entry.value_ptr.* = .empty;
        try outgoing_entry.value_ptr.append(allocator, relation_id);

        const incoming_entry = try incoming.getOrPut(target_id);
        if (!incoming_entry.found_existing) incoming_entry.value_ptr.* = .empty;
        try incoming_entry.value_ptr.append(allocator, relation_id);
    }

    for (entity_ids) |entity_id| {
        try resetStatement(out_stmt);
        try bindInt64(out_stmt, 1, entity_id);
        try stepDone(out_stmt);

        try resetStatement(in_stmt);
        try bindInt64(in_stmt, 1, entity_id);
        try stepDone(in_stmt);
    }

    var outgoing_it = outgoing.iterator();
    while (outgoing_it.next()) |entry| {
        try upsertAdjacency(db, "graph_lj_out", entry.key_ptr.*, entry.value_ptr.items, allocator);
    }
    var incoming_it = incoming.iterator();
    while (incoming_it.next()) |entry| {
        try upsertAdjacency(db, "graph_lj_in", entry.key_ptr.*, entry.value_ptr.items, allocator);
    }

    try db.exec("COMMIT");
    try refreshEntityDegree(db);
}

pub fn refreshEntityDegree(db: Database) !void {
    try db.exec("DELETE FROM graph_entity_degree");
    const stmt = try prepare(
        db,
        "INSERT INTO graph_entity_degree(entity_id, name, entity_type, confidence, out_degree, in_degree, total_degree) " ++ "SELECT e.entity_id, e.name, e.entity_type, e.confidence, " ++ "COALESCE((SELECT COUNT(*) FROM graph_relation r WHERE r.source_id = e.entity_id AND r.deprecated_at IS NULL), 0), " ++ "COALESCE((SELECT COUNT(*) FROM graph_relation r WHERE r.target_id = e.entity_id AND r.deprecated_at IS NULL), 0), " ++ "COALESCE((SELECT COUNT(*) FROM graph_relation r WHERE r.source_id = e.entity_id AND r.deprecated_at IS NULL), 0) + " ++ "COALESCE((SELECT COUNT(*) FROM graph_relation r WHERE r.target_id = e.entity_id AND r.deprecated_at IS NULL), 0) " ++ "FROM graph_entity e WHERE e.deprecated_at IS NULL",
    );
    defer finalize(stmt);
    try stepDone(stmt);
}

pub fn buildPathResult(
    db: Database,
    allocator: std.mem.Allocator,
    path: []const graph_store.PathEdge,
) !GraphPathResult {
    const segments = try allocator.alloc(PathSegment, path.len);
    var initialized: usize = 0;
    errdefer {
        for (segments[0..initialized]) |*segment| {
            allocator.free(segment.relation_type);
            allocator.free(segment.source.entity_type);
            allocator.free(segment.source.name);
            allocator.free(segment.target.entity_type);
            allocator.free(segment.target.name);
        }
        allocator.free(segments);
    }

    for (path, 0..) |edge, index| {
        const relation = (try loadRelation(db, allocator, edge.relation_id)).?;
        errdefer allocator.free(relation.relation_type);
        const source = try loadEntity(db, allocator, edge.from_node);
        errdefer {
            allocator.free(source.entity_type);
            allocator.free(source.name);
        }
        const target = try loadEntity(db, allocator, edge.to_node);
        errdefer {
            allocator.free(target.entity_type);
            allocator.free(target.name);
        }

        segments[index] = .{
            .relation_id = relation.relation_id,
            .relation_type = relation.relation_type,
            .source = .{
                .entity_id = source.entity_id,
                .entity_type = source.entity_type,
                .name = source.name,
            },
            .target = .{
                .entity_id = target.entity_id,
                .entity_type = target.entity_type,
                .name = target.name,
            },
            .confidence = relation.confidence,
        };
        initialized += 1;
    }

    return .{
        .hops = path.len,
        .segments = segments,
    };
}

pub fn traverse(
    db: Database,
    allocator: std.mem.Allocator,
    start_name: []const u8,
    direction: TraverseDirection,
    edge_types: ?[]const []const u8,
    depth: usize,
    target_name: ?[]const u8,
) !TraverseResult {
    const filter: interfaces.GraphEdgeFilter = .{
        .edge_types = edge_types,
    };
    const start = loadTraverseEntityByName(db, allocator, start_name) catch |err| switch (err) {
        error.MissingRow => {
            return .{
                .rows = try allocator.alloc(TraverseRow, 0),
                .target_found = false,
            };
        },
        else => return err,
    };
    defer deinitTraverseEntitySummary(allocator, start);

    const target_id: ?u32 = if (target_name) |name|
        (loadTraverseEntityIdByName(db, name) catch |err| switch (err) {
            error.MissingRow => null,
            else => return err,
        })
    else
        null;

    var discovered_order = std.ArrayList(u32).empty;
    defer discovered_order.deinit(allocator);
    try discovered_order.append(allocator, start.entity_id);

    var frontier = std.ArrayList(u32).empty;
    defer frontier.deinit(allocator);
    try frontier.append(allocator, start.entity_id);

    var next_frontier = std.ArrayList(u32).empty;
    defer next_frontier.deinit(allocator);

    var visits = std.AutoHashMap(u32, TraverseVisit).init(allocator);
    defer visits.deinit();
    try visits.put(start.entity_id, .{
        .depth = 0,
        .predecessor = null,
    });

    if (target_id != null and target_id.? == start.entity_id) {
        const rows = try buildTraverseRows(db, allocator, visits, &.{start.entity_id});
        return .{
            .rows = rows,
            .target_found = true,
        };
    }

    var found_target = false;
    var found_target_id: ?u32 = null;

    var current_depth: usize = 0;
    outer: while (current_depth < depth and frontier.items.len != 0) : (current_depth += 1) {
        next_frontier.clearRetainingCapacity();

        for (frontier.items) |node_id| {
            const neighbors = try loadTraverseNeighbors(db, allocator, node_id, direction, filter);
            defer {
                for (neighbors) |neighbor| deinitTraverseNeighbor(allocator, neighbor);
                allocator.free(neighbors);
            }

            for (neighbors) |neighbor| {
                if (visits.contains(neighbor.entity.entity_id)) continue;

                try visits.put(neighbor.entity.entity_id, .{
                    .depth = current_depth + 1,
                    .predecessor = .{
                        .from_node = node_id,
                        .relation_id = neighbor.relation_id,
                    },
                });
                try discovered_order.append(allocator, neighbor.entity.entity_id);

                if (target_id != null and neighbor.entity.entity_id == target_id.?) {
                    found_target = true;
                    found_target_id = neighbor.entity.entity_id;
                    break :outer;
                }

                try next_frontier.append(allocator, neighbor.entity.entity_id);
            }
        }

        std.mem.swap(std.ArrayList(u32), &frontier, &next_frontier);
    }

    const output_ids = if (found_target and found_target_id != null)
        try buildTraversePathIds(allocator, visits, found_target_id.?)
    else
        try allocator.dupe(u32, discovered_order.items);
    defer allocator.free(output_ids);

    const rows = try buildTraverseRows(db, allocator, visits, output_ids);
    return .{
        .rows = rows,
        .target_found = found_target,
    };
}

pub fn traverseToon(
    db: Database,
    allocator: std.mem.Allocator,
    start_name: []const u8,
    direction: TraverseDirection,
    edge_types: ?[]const []const u8,
    depth: usize,
    target_name: ?[]const u8,
) ![]u8 {
    var result = try traverse(db, allocator, start_name, direction, edge_types, depth, target_name);
    defer result.deinit(allocator);

    const root = try buildTraverseResultValue(allocator, start_name, direction, result);
    defer toon_exports.deinitOwnedValue(allocator, root);

    return try toon_exports.encodeValueAlloc(
        allocator,
        root,
        toon_exports.default_options,
    );
}

pub fn shortestPathToon(
    db: Database,
    allocator: std.mem.Allocator,
    source_name: []const u8,
    target_name: []const u8,
    edge_types: ?[]const []const u8,
    max_depth: usize,
) ![]u8 {
    var runtime = try loadRuntime(db, allocator);
    defer runtime.deinit();

    const source = try loadTraverseEntityByName(db, allocator, source_name);
    defer deinitTraverseEntitySummary(allocator, source);
    const target = try loadTraverseEntityByName(db, allocator, target_name);
    defer deinitTraverseEntitySummary(allocator, target);

    const path = (try runtime.shortestPath(
        allocator,
        source.entity_id,
        target.entity_id,
        .{ .edge_types = edge_types },
        max_depth,
    )) orelse {
        const root = try buildGraphPathValue(allocator, source_name, target_name, null);
        defer toon_exports.deinitOwnedValue(allocator, root);
        return try toon_exports.encodeValueAlloc(allocator, root, toon_exports.default_options);
    };
    defer allocator.free(path);

    var result = try buildPathResult(db, allocator, path);
    defer result.deinit(allocator);

    const root = try buildGraphPathValue(allocator, source_name, target_name, result);
    defer toon_exports.deinitOwnedValue(allocator, root);

    return try toon_exports.encodeValueAlloc(allocator, root, toon_exports.default_options);
}

fn buildGraphPathValue(
    allocator: std.mem.Allocator,
    source_name: []const u8,
    target_name: []const u8,
    maybe_result: ?GraphPathResult,
) !Value {
    const fields = try allocator.alloc(Field, 6);
    fields[0] = .{ .key = "kind", .value = .{ .string = "graph_path" } };
    fields[1] = .{ .key = "source_name", .value = .{ .string = source_name } };
    fields[2] = .{ .key = "target_name", .value = .{ .string = target_name } };
    fields[3] = .{ .key = "target_found", .value = .{ .bool = maybe_result != null } };
    fields[4] = .{ .key = "hops", .value = if (maybe_result) |result| toon_exports.intValue(result.hops) else .null };
    fields[5] = .{ .key = "segments", .value = .{ .array = try buildGraphPathSegments(allocator, maybe_result) } };
    return .{ .object = fields };
}

fn buildGraphPathSegments(allocator: std.mem.Allocator, maybe_result: ?GraphPathResult) ![]Value {
    const result = maybe_result orelse return allocator.alloc(Value, 0);
    const values = try allocator.alloc(Value, result.segments.len);
    errdefer allocator.free(values);

    for (result.segments, 0..) |segment, index| {
        const fields = try allocator.alloc(Field, 7);
        fields[0] = .{ .key = "relation_id", .value = toon_exports.intValue(segment.relation_id) };
        fields[1] = .{ .key = "relation_type", .value = .{ .string = segment.relation_type } };
        fields[2] = .{ .key = "source_id", .value = toon_exports.intValue(segment.source.entity_id) };
        fields[3] = .{ .key = "source_name", .value = .{ .string = segment.source.name } };
        fields[4] = .{ .key = "target_id", .value = toon_exports.intValue(segment.target.entity_id) };
        fields[5] = .{ .key = "target_name", .value = .{ .string = segment.target.name } };
        fields[6] = .{ .key = "confidence", .value = .{ .float = segment.confidence } };
        values[index] = .{ .object = fields };
    }

    return values;
}

fn buildTraverseResultValue(
    allocator: std.mem.Allocator,
    start_name: []const u8,
    direction: TraverseDirection,
    result: TraverseResult,
) !Value {
    const fields = try allocator.alloc(Field, 5);
    fields[0] = .{ .key = "kind", .value = .{ .string = "graph_traverse" } };
    fields[1] = .{ .key = "start_name", .value = .{ .string = start_name } };
    fields[2] = .{ .key = "direction", .value = .{ .string = @tagName(direction) } };
    fields[3] = .{ .key = "target_found", .value = .{ .bool = result.target_found } };
    fields[4] = .{ .key = "rows", .value = .{ .array = try buildTraverseRowsValue(allocator, result.rows) } };
    return .{ .object = fields };
}

fn buildTraverseRowsValue(allocator: std.mem.Allocator, rows: []const TraverseRow) ![]Value {
    const values = try allocator.alloc(Value, rows.len);
    errdefer allocator.free(values);

    for (rows, 0..) |row, index| {
        const fields = try allocator.alloc(Field, 7);
        fields[0] = .{ .key = "node_id", .value = .{ .string = row.node_id } };
        fields[1] = .{ .key = "node_label", .value = .{ .string = row.node_label } };
        fields[2] = .{ .key = "node_type", .value = .{ .string = row.node_type } };
        fields[3] = .{ .key = "metadata_json", .value = .{ .string = row.metadata_json } };
        fields[4] = .{ .key = "edge_label", .value = toon_exports.optionalStringValue(row.edge_label) };
        fields[5] = .{ .key = "depth", .value = toon_exports.intValue(row.depth) };
        fields[6] = .{ .key = "path", .value = .{ .array = try toon_exports.buildStringArray(allocator, row.path) } };
        values[index] = .{ .object = fields };
    }

    return values;
}

pub fn marketplaceSearchToon(
    db: Database,
    allocator: std.mem.Allocator,
    query: []const u8,
    domain_filter: ?[]const u8,
    min_confidence: f32,
    max_hops: usize,
    limit: usize,
) ![]u8 {
    const results = try marketplaceSearch(db, allocator, query, domain_filter, min_confidence, max_hops, limit);
    defer {
        for (results) |row| {
            allocator.free(row.name);
            allocator.free(row.entity_type);
            allocator.free(row.metadata_json);
        }
        allocator.free(results);
    }

    const root = try buildMarketplaceSearchValue(allocator, query, results);
    defer toon_exports.deinitOwnedValue(allocator, root);
    return try toon_exports.encodeValueAlloc(allocator, root, toon_exports.default_options);
}

pub fn entityFtsSearchToon(
    db: Database,
    allocator: std.mem.Allocator,
    query: []const u8,
    type_filter: ?[]const []const u8,
    domain_filter: ?[]const u8,
    min_confidence: f32,
    limit: usize,
) ![]u8 {
    const results = try entityFtsSearch(db, allocator, query, type_filter, domain_filter, min_confidence, limit);
    defer {
        for (results) |row| {
            allocator.free(row.name);
            allocator.free(row.entity_type);
            allocator.free(row.metadata_json);
        }
        allocator.free(results);
    }

    const root = try buildEntitySearchValue(allocator, "graph_entity_fts_search", query, results);
    defer toon_exports.deinitOwnedValue(allocator, root);
    return try toon_exports.encodeValueAlloc(allocator, root, toon_exports.default_options);
}

pub fn skillDependenciesToon(
    db: Database,
    allocator: std.mem.Allocator,
    entity_id: u32,
    max_depth: usize,
    min_confidence: f32,
) ![]u8 {
    const results = try skillDependencies(db, allocator, entity_id, max_depth, min_confidence);
    defer {
        for (results) |row| {
            allocator.free(row.dep_name);
            allocator.free(row.dep_type);
            allocator.free(row.relation_type);
        }
        allocator.free(results);
    }

    const root = try buildSkillDependenciesValue(allocator, entity_id, results);
    defer toon_exports.deinitOwnedValue(allocator, root);
    return try toon_exports.encodeValueAlloc(allocator, root, toon_exports.default_options);
}

fn buildMarketplaceSearchValue(
    allocator: std.mem.Allocator,
    query: []const u8,
    results: []const MarketplaceResult,
) !Value {
    const fields = try allocator.alloc(Field, 3);
    fields[0] = .{ .key = "kind", .value = .{ .string = "graph_marketplace_search" } };
    fields[1] = .{ .key = "query", .value = .{ .string = query } };
    fields[2] = .{ .key = "results", .value = .{ .array = try buildMarketplaceResults(allocator, results) } };
    return .{ .object = fields };
}

fn buildMarketplaceResults(allocator: std.mem.Allocator, results: []const MarketplaceResult) ![]Value {
    const values = try allocator.alloc(Value, results.len);
    errdefer allocator.free(values);

    for (results, 0..) |result, index| {
        const fields = try allocator.alloc(Field, 9);
        fields[0] = .{ .key = "entity_id", .value = toon_exports.intValue(result.entity_id) };
        fields[1] = .{ .key = "name", .value = .{ .string = result.name } };
        fields[2] = .{ .key = "entity_type", .value = .{ .string = result.entity_type } };
        fields[3] = .{ .key = "confidence", .value = .{ .float = result.confidence } };
        fields[4] = .{ .key = "fts_rank", .value = .{ .float = result.fts_rank } };
        fields[5] = .{ .key = "is_direct_match", .value = .{ .bool = result.is_direct_match } };
        fields[6] = .{ .key = "hub_score", .value = .{ .float = result.hub_score } };
        fields[7] = .{ .key = "composite_score", .value = .{ .float = result.composite_score } };
        fields[8] = .{ .key = "metadata_json", .value = .{ .string = result.metadata_json } };
        values[index] = .{ .object = fields };
    }

    return values;
}

fn buildEntitySearchValue(
    allocator: std.mem.Allocator,
    kind: []const u8,
    query: []const u8,
    results: []const EntitySearchResult,
) !Value {
    const fields = try allocator.alloc(Field, 3);
    fields[0] = .{ .key = "kind", .value = .{ .string = kind } };
    fields[1] = .{ .key = "query", .value = .{ .string = query } };
    fields[2] = .{ .key = "results", .value = .{ .array = try buildEntitySearchResults(allocator, results) } };
    return .{ .object = fields };
}

fn buildEntitySearchResults(allocator: std.mem.Allocator, results: []const EntitySearchResult) ![]Value {
    const values = try allocator.alloc(Value, results.len);
    errdefer allocator.free(values);

    for (results, 0..) |result, index| {
        const fields = try allocator.alloc(Field, 6);
        fields[0] = .{ .key = "entity_id", .value = toon_exports.intValue(result.entity_id) };
        fields[1] = .{ .key = "name", .value = .{ .string = result.name } };
        fields[2] = .{ .key = "entity_type", .value = .{ .string = result.entity_type } };
        fields[3] = .{ .key = "confidence", .value = .{ .float = result.confidence } };
        fields[4] = .{ .key = "fts_rank", .value = .{ .float = result.fts_rank } };
        fields[5] = .{ .key = "metadata_json", .value = .{ .string = result.metadata_json } };
        values[index] = .{ .object = fields };
    }

    return values;
}

fn buildSkillDependenciesValue(
    allocator: std.mem.Allocator,
    entity_id: u32,
    results: []const SkillDependency,
) !Value {
    const fields = try allocator.alloc(Field, 3);
    fields[0] = .{ .key = "kind", .value = .{ .string = "graph_skill_dependencies" } };
    fields[1] = .{ .key = "entity_id", .value = toon_exports.intValue(entity_id) };
    fields[2] = .{ .key = "dependencies", .value = .{ .array = try buildSkillDependencyRows(allocator, results) } };
    return .{ .object = fields };
}

fn buildSkillDependencyRows(allocator: std.mem.Allocator, results: []const SkillDependency) ![]Value {
    const values = try allocator.alloc(Value, results.len);
    errdefer allocator.free(values);

    for (results, 0..) |result, index| {
        const fields = try allocator.alloc(Field, 6);
        fields[0] = .{ .key = "dep_entity_id", .value = toon_exports.intValue(result.dep_entity_id) };
        fields[1] = .{ .key = "dep_name", .value = .{ .string = result.dep_name } };
        fields[2] = .{ .key = "dep_type", .value = .{ .string = result.dep_type } };
        fields[3] = .{ .key = "dep_confidence", .value = .{ .float = result.dep_confidence } };
        fields[4] = .{ .key = "relation_type", .value = .{ .string = result.relation_type } };
        fields[5] = .{ .key = "depth", .value = toon_exports.intValue(result.depth) };
        values[index] = .{ .object = fields };
    }

    return values;
}

pub fn resolveAlias(
    db: Database,
    allocator: std.mem.Allocator,
    term: []const u8,
) ![]EntityAliasMatch {
    const stmt = try prepare(db, "SELECT a.entity_id, e.name, e.entity_type, a.confidence FROM graph_entity_alias a JOIN graph_entity e ON e.entity_id = a.entity_id WHERE a.term = ?1 ORDER BY a.confidence DESC, e.name ASC");
    defer finalize(stmt);
    try bindText(stmt, 1, term);

    var matches = std.ArrayList(EntityAliasMatch).empty;
    defer {
        for (matches.items) |match| {
            allocator.free(match.name);
            allocator.free(match.entity_type);
        }
        matches.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        try matches.append(allocator, .{
            .entity_id = try columnU32(stmt, 0),
            .name = try dupeColumnText(allocator, stmt, 1),
            .entity_type = try dupeColumnText(allocator, stmt, 2),
            .confidence = @floatCast(c.sqlite3_column_double(stmt, 3)),
        });
    }

    return matches.toOwnedSlice(allocator);
}

pub fn loadEntityDocuments(
    db: Database,
    allocator: std.mem.Allocator,
    entity_id: u32,
) ![]EntityDocumentLink {
    const stmt = try prepare(db, "SELECT entity_id, doc_id, table_id, role, confidence FROM graph_entity_document WHERE entity_id = ?1 ORDER BY confidence DESC, doc_id ASC");
    defer finalize(stmt);
    try bindInt64(stmt, 1, entity_id);

    var docs = std.ArrayList(EntityDocumentLink).empty;
    defer {
        for (docs.items) |doc| if (doc.role) |role| allocator.free(role);
        docs.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        try docs.append(allocator, .{
            .entity_id = try columnU32(stmt, 0),
            .doc_id = try columnU64(stmt, 1),
            .table_id = try columnU64(stmt, 2),
            .role = if (c.sqlite3_column_type(stmt, 3) == c.SQLITE_NULL) null else try dupeColumnText(allocator, stmt, 3),
            .confidence = @floatCast(c.sqlite3_column_double(stmt, 4)),
        });
    }

    return docs.toOwnedSlice(allocator);
}

pub fn loadEntityChunks(
    db: Database,
    allocator: std.mem.Allocator,
    entity_id: u32,
) ![]EntityChunkLink {
    const stmt = try prepare(
        db,
        "SELECT entity_id, workspace_id, collection_id, doc_id, chunk_index, role, confidence, metadata_json FROM graph_entity_chunk WHERE entity_id = ?1 ORDER BY confidence DESC, doc_id ASC, chunk_index ASC",
    );
    defer finalize(stmt);
    try bindInt64(stmt, 1, entity_id);

    var chunks = std.ArrayList(EntityChunkLink).empty;
    defer {
        for (chunks.items) |*chunk| chunk.deinit(allocator);
        chunks.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        try chunks.append(allocator, .{
            .entity_id = try columnU32(stmt, 0),
            .workspace_id = try dupeColumnText(allocator, stmt, 1),
            .collection_id = try dupeColumnText(allocator, stmt, 2),
            .doc_id = try columnU64(stmt, 3),
            .chunk_index = try columnU32(stmt, 4),
            .role = if (c.sqlite3_column_type(stmt, 5) == c.SQLITE_NULL) null else try dupeColumnText(allocator, stmt, 5),
            .confidence = @floatCast(c.sqlite3_column_double(stmt, 6)),
            .metadata_json = try dupeColumnText(allocator, stmt, 7),
        });
    }

    return chunks.toOwnedSlice(allocator);
}

pub fn findRelationByIds(
    db: Database,
    allocator: std.mem.Allocator,
    source_id: u32,
    target_id: u32,
    relation_type: []const u8,
) !?RelationRecordFull {
    const stmt = try prepare(
        db,
        "SELECT relation_id, workspace_id, relation_type, source_id, target_id, valid_from_unix, valid_to_unix, confidence, deprecated_at, run_id, patch_id, metadata_json, created_at_unix FROM graph_relation WHERE source_id = ?1 AND target_id = ?2 AND relation_type = ?3 AND deprecated_at IS NULL ORDER BY relation_id LIMIT 1",
    );
    defer finalize(stmt);
    try bindInt64(stmt, 1, source_id);
    try bindInt64(stmt, 2, target_id);
    try bindText(stmt, 3, relation_type);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return null;
    return try loadRelationFullFromStmt(allocator, stmt);
}

pub fn findRelationByEndpoints(
    db: Database,
    allocator: std.mem.Allocator,
    source_name: []const u8,
    target_name: []const u8,
    relation_type: []const u8,
) !?RelationRecordFull {
    var source = try findEntityByName(db, allocator, source_name) orelse return null;
    defer source.deinit(allocator);
    var target = try findEntityByName(db, allocator, target_name) orelse return null;
    defer target.deinit(allocator);
    return try findRelationByIds(db, allocator, source.entity_id, target.entity_id, relation_type);
}

pub fn getRelationsFrom(
    db: Database,
    allocator: std.mem.Allocator,
    source_id: u32,
    relation_type: ?[]const u8,
) ![]RelationRecordFull {
    var sql_buf: [512]u8 = undefined;
    const query = if (relation_type) |_|
        std.fmt.bufPrintZ(
            &sql_buf,
            "SELECT relation_id, workspace_id, relation_type, source_id, target_id, valid_from_unix, valid_to_unix, confidence, deprecated_at, run_id, patch_id, metadata_json, created_at_unix FROM graph_relation WHERE source_id = {d} AND relation_type = $1 AND deprecated_at IS NULL ORDER BY confidence DESC, relation_id ASC",
            .{source_id},
        ) catch return error.BufferTooSmall
    else
        std.fmt.bufPrintZ(
            &sql_buf,
            "SELECT relation_id, workspace_id, relation_type, source_id, target_id, valid_from_unix, valid_to_unix, confidence, deprecated_at, run_id, patch_id, metadata_json, created_at_unix FROM graph_relation WHERE source_id = {d} AND deprecated_at IS NULL ORDER BY confidence DESC, relation_id ASC",
            .{source_id},
        ) catch return error.BufferTooSmall;

    return try loadRelationsWithQuery(db, allocator, query, relation_type);
}

pub fn getRelationsTo(
    db: Database,
    allocator: std.mem.Allocator,
    target_id: u32,
    relation_type: ?[]const u8,
) ![]RelationRecordFull {
    var sql_buf: [512]u8 = undefined;
    const query = if (relation_type) |_|
        std.fmt.bufPrintZ(
            &sql_buf,
            "SELECT relation_id, workspace_id, relation_type, source_id, target_id, valid_from_unix, valid_to_unix, confidence, deprecated_at, run_id, patch_id, metadata_json, created_at_unix FROM graph_relation WHERE target_id = {d} AND relation_type = $1 AND deprecated_at IS NULL ORDER BY confidence DESC, relation_id ASC",
            .{target_id},
        ) catch return error.BufferTooSmall
    else
        std.fmt.bufPrintZ(
            &sql_buf,
            "SELECT relation_id, workspace_id, relation_type, source_id, target_id, valid_from_unix, valid_to_unix, confidence, deprecated_at, run_id, patch_id, metadata_json, created_at_unix FROM graph_relation WHERE target_id = {d} AND deprecated_at IS NULL ORDER BY confidence DESC, relation_id ASC",
            .{target_id},
        ) catch return error.BufferTooSmall;

    return try loadRelationsWithQuery(db, allocator, query, relation_type);
}

pub fn findRelationsFromSourceByType(
    db: Database,
    allocator: std.mem.Allocator,
    source_name: []const u8,
    relation_type: []const u8,
) ![]RelationRecordFull {
    var source = try findEntityByName(db, allocator, source_name) orelse return allocator.alloc(RelationRecordFull, 0);
    defer source.deinit(allocator);
    return try getRelationsFrom(db, allocator, source.entity_id, relation_type);
}

pub fn cleanupTestData(
    db: Database,
    prefix: []const u8,
) !void {
    var sql_buf: [1024]u8 = undefined;
    const query = std.fmt.bufPrintZ(
        &sql_buf,
        \\BEGIN IMMEDIATE;
        \\DELETE FROM graph_knowledge_patch WHERE patch_key LIKE '{s}%';
        \\DELETE FROM graph_execution_run WHERE run_key LIKE '{s}%';
        \\DELETE FROM graph_relation WHERE metadata_json LIKE '{{"test_prefix":"{s}"%';
        \\DELETE FROM graph_entity_alias WHERE term LIKE '{s}%';
        \\DELETE FROM graph_entity_document WHERE role LIKE '{s}%';
        \\DELETE FROM graph_entity WHERE name LIKE '{s}%';
        \\COMMIT;
    ,
        .{ prefix, prefix, prefix, prefix, prefix, prefix },
    ) catch return error.BufferTooSmall;
    try db.exec(query);
}

pub fn streamEntityNeighborhood(
    db: Database,
    allocator: std.mem.Allocator,
    entity_id: u32,
    max_out: usize,
    max_in: usize,
    min_confidence: f32,
) ![]GraphStreamEvent {
    var events = std.ArrayList(GraphStreamEvent).empty;
    errdefer {
        for (events.items) |*event| event.deinit(allocator);
        events.deinit(allocator);
    }

    var seed = try loadEntityFull(db, allocator, entity_id);
    defer seed.deinit(allocator);

    var seen_nodes = std.AutoHashMap(u32, void).init(allocator);
    defer seen_nodes.deinit();
    _ = try seen_nodes.put(entity_id, {});

    try appendJsonEvent(allocator, &events, 0, "seed_node", .{
        .entity = buildEntityPayload(seed),
    });

    var seq: u32 = 1;
    var emitted_edges = std.AutoHashMap(u32, void).init(allocator);
    defer emitted_edges.deinit();

    const outbound = try loadTraverseNeighbors(
        db,
        allocator,
        entity_id,
        .outbound,
        .{ .confidence_min = min_confidence },
    );
    defer {
        for (outbound) |neighbor| deinitTraverseNeighbor(allocator, neighbor);
        allocator.free(outbound);
    }
    for (outbound[0..@min(outbound.len, max_out)]) |neighbor| {
        if ((try emitted_edges.getOrPut(neighbor.relation_id)).found_existing) continue;
        var relation = (try loadRelationFull(db, allocator, neighbor.relation_id)) orelse continue;
        defer relation.deinit(allocator);
        var target = try loadEntityFull(db, allocator, neighbor.entity.entity_id);
        defer target.deinit(allocator);
        try appendJsonEvent(allocator, &events, seq, "edge", .{
            .direction = "outbound",
            .relation = buildRelationPayload(relation),
            .source = buildEntityPayload(seed),
            .target = buildEntityPayload(target),
        });
        seq += 1;
        if (!(try seen_nodes.getOrPut(target.entity_id)).found_existing) {
            try appendJsonEvent(allocator, &events, seq, "node", .{
                .entity = buildEntityPayload(target),
            });
            seq += 1;
        }
    }

    const inbound = try loadTraverseNeighbors(
        db,
        allocator,
        entity_id,
        .inbound,
        .{ .confidence_min = min_confidence },
    );
    defer {
        for (inbound) |neighbor| deinitTraverseNeighbor(allocator, neighbor);
        allocator.free(inbound);
    }
    for (inbound[0..@min(inbound.len, max_in)]) |neighbor| {
        if ((try emitted_edges.getOrPut(neighbor.relation_id)).found_existing) continue;
        var relation = (try loadRelationFull(db, allocator, neighbor.relation_id)) orelse continue;
        defer relation.deinit(allocator);
        var source = try loadEntityFull(db, allocator, neighbor.entity.entity_id);
        defer source.deinit(allocator);
        try appendJsonEvent(allocator, &events, seq, "edge", .{
            .direction = "inbound",
            .relation = buildRelationPayload(relation),
            .source = buildEntityPayload(source),
            .target = buildEntityPayload(seed),
        });
        seq += 1;
        if (!(try seen_nodes.getOrPut(source.entity_id)).found_existing) {
            try appendJsonEvent(allocator, &events, seq, "node", .{
                .entity = buildEntityPayload(source),
            });
            seq += 1;
        }
    }

    try appendJsonEvent(allocator, &events, seq, "done", .{
        .kind = "neighborhood",
        .seed_count = 1,
        .hops = 1,
        .node_count = seen_nodes.count(),
        .edge_count = emitted_edges.count(),
    });

    return events.toOwnedSlice(allocator);
}

pub fn streamSubgraph(
    db: Database,
    allocator: std.mem.Allocator,
    seed_ids: []const u32,
    max_hops: usize,
    edge_types: ?[]const []const u8,
    confidence_min: ?f32,
    confidence_max: ?f32,
    after_unix_seconds: ?i64,
    before_unix_seconds: ?i64,
) ![]GraphStreamEvent {
    if (seed_ids.len == 0) return allocator.alloc(GraphStreamEvent, 0);

    var events = std.ArrayList(GraphStreamEvent).empty;
    errdefer {
        for (events.items) |*event| event.deinit(allocator);
        events.deinit(allocator);
    }

    var seen_nodes = std.AutoHashMap(u32, void).init(allocator);
    defer seen_nodes.deinit();
    var seen_edges = std.AutoHashMap(u32, void).init(allocator);
    defer seen_edges.deinit();

    var frontier = std.ArrayList(u32).empty;
    defer frontier.deinit(allocator);
    for (seed_ids) |seed_id| {
        if ((try seen_nodes.getOrPut(seed_id)).found_existing) continue;
        try frontier.append(allocator, seed_id);
        var seed = try loadEntityFull(db, allocator, seed_id);
        defer seed.deinit(allocator);
        try appendJsonEvent(allocator, &events, @intCast(events.items.len), "seed_node", .{
            .entity = buildEntityPayload(seed),
        });
    }

    var depth: usize = 0;
    while (depth < max_hops and frontier.items.len != 0) : (depth += 1) {
        var next_frontier = std.ArrayList(u32).empty;
        defer next_frontier.deinit(allocator);

        for (frontier.items) |node_id| {
            const outbound = try loadTraverseNeighbors(
                db,
                allocator,
                node_id,
                .outbound,
                .{
                    .edge_types = edge_types,
                    .confidence_min = confidence_min,
                    .confidence_max = confidence_max,
                    .after_unix_seconds = after_unix_seconds,
                    .before_unix_seconds = before_unix_seconds,
                },
            );
            defer {
                for (outbound) |neighbor| deinitTraverseNeighbor(allocator, neighbor);
                allocator.free(outbound);
            }
            for (outbound) |neighbor| {
                if ((try seen_edges.getOrPut(neighbor.relation_id)).found_existing) continue;
                var relation = (try loadRelationFull(db, allocator, neighbor.relation_id)) orelse continue;
                defer relation.deinit(allocator);
                var source = try loadEntityFull(db, allocator, relation.source_id);
                defer source.deinit(allocator);
                var target = try loadEntityFull(db, allocator, relation.target_id);
                defer target.deinit(allocator);
                try appendJsonEvent(allocator, &events, @intCast(events.items.len), "edge", .{
                    .direction = "outbound",
                    .depth = depth + 1,
                    .relation = buildRelationPayload(relation),
                    .source = buildEntityPayload(source),
                    .target = buildEntityPayload(target),
                });
                if ((try seen_nodes.getOrPut(neighbor.entity.entity_id)).found_existing) continue;
                try next_frontier.append(allocator, neighbor.entity.entity_id);
                try appendJsonEvent(allocator, &events, @intCast(events.items.len), "node", .{
                    .depth = depth + 1,
                    .entity = buildEntityPayload(target),
                });
            }

            const inbound = try loadTraverseNeighbors(
                db,
                allocator,
                node_id,
                .inbound,
                .{
                    .edge_types = edge_types,
                    .confidence_min = confidence_min,
                    .confidence_max = confidence_max,
                    .after_unix_seconds = after_unix_seconds,
                    .before_unix_seconds = before_unix_seconds,
                },
            );
            defer {
                for (inbound) |neighbor| deinitTraverseNeighbor(allocator, neighbor);
                allocator.free(inbound);
            }
            for (inbound) |neighbor| {
                if ((try seen_edges.getOrPut(neighbor.relation_id)).found_existing) continue;
                var relation = (try loadRelationFull(db, allocator, neighbor.relation_id)) orelse continue;
                defer relation.deinit(allocator);
                var source = try loadEntityFull(db, allocator, relation.source_id);
                defer source.deinit(allocator);
                var target = try loadEntityFull(db, allocator, relation.target_id);
                defer target.deinit(allocator);
                try appendJsonEvent(allocator, &events, @intCast(events.items.len), "edge", .{
                    .direction = "inbound",
                    .depth = depth + 1,
                    .relation = buildRelationPayload(relation),
                    .source = buildEntityPayload(source),
                    .target = buildEntityPayload(target),
                });
                if ((try seen_nodes.getOrPut(neighbor.entity.entity_id)).found_existing) continue;
                try next_frontier.append(allocator, neighbor.entity.entity_id);
                try appendJsonEvent(allocator, &events, @intCast(events.items.len), "node", .{
                    .depth = depth + 1,
                    .entity = buildEntityPayload(source),
                });
            }
        }

        frontier.clearRetainingCapacity();
        try frontier.appendSlice(allocator, next_frontier.items);
    }

    try appendJsonEvent(allocator, &events, @intCast(events.items.len), "done", .{
        .kind = "subgraph",
        .seed_count = seed_ids.len,
        .hops = max_hops,
        .node_count = seen_nodes.count(),
        .edge_count = seen_edges.count(),
    });

    return events.toOwnedSlice(allocator);
}

fn loadAdjacencyUnion(db: Database, allocator: std.mem.Allocator, table_name: []const u8, nodes: roaring.Bitmap) !roaring.Bitmap {
    const node_ids = try nodes.toArray(allocator);
    defer allocator.free(node_ids);

    var result = try roaring.Bitmap.empty();
    errdefer result.deinit();

    for (node_ids) |node_id| {
        var adjacency = loadAdjacencyBitmap(db, allocator, table_name, node_id) catch |err| switch (err) {
            error.MissingRow => continue,
            else => return err,
        };
        defer adjacency.deinit();
        result.orInPlace(adjacency);
    }

    return result;
}

fn deinitStringSlice(allocator: std.mem.Allocator, items: [][]const u8) void {
    for (items) |item| allocator.free(item);
    allocator.free(items);
}

fn deinitTokenList(allocator: std.mem.Allocator, tokens: *std.ArrayList([]const u8)) void {
    for (tokens.items) |token| allocator.free(token);
    tokens.deinit(allocator);
}

fn splitSearchTerms(allocator: std.mem.Allocator, query: []const u8) ![][]const u8 {
    var tokens = try tokenization_sqlite.tokenizePure(query, allocator);
    return tokens.toOwnedSlice(allocator);
}

fn lowerDup(allocator: std.mem.Allocator, text: []const u8) ![]const u8 {
    const copy = try allocator.dupe(u8, text);
    for (copy) |*ch| ch.* = std.ascii.toLower(ch.*);
    return copy;
}

fn fieldContainsAllTokens(
    allocator: std.mem.Allocator,
    text: []const u8,
    query_terms: []const []const u8,
) !bool {
    var tokens = try tokenization_sqlite.tokenizePure(text, allocator);
    defer deinitTokenList(allocator, &tokens);

    for (query_terms) |term| {
        var found = false;
        for (tokens.items) |token| {
            if (std.mem.eql(u8, token, term)) {
                found = true;
                break;
            }
        }
        if (!found) return false;
    }
    return true;
}

fn tokenFrequencyInField(
    allocator: std.mem.Allocator,
    text: []const u8,
    term: []const u8,
) !u32 {
    var tokens = try tokenization_sqlite.tokenizePure(text, allocator);
    defer deinitTokenList(allocator, &tokens);

    var count: u32 = 0;
    for (tokens.items) |token| {
        if (std.mem.eql(u8, token, term)) count += 1;
    }
    return count;
}

fn ftsScore(
    allocator: std.mem.Allocator,
    terms: []const []const u8,
    name: []const u8,
    metadata_json: []const u8,
    entity_type: []const u8,
    domain: []const u8,
) !f32 {
    if (terms.len == 0) return 0;
    var name_tokens = try tokenization_sqlite.tokenizePure(name, allocator);
    defer deinitTokenList(allocator, &name_tokens);
    var meta_tokens = try tokenization_sqlite.tokenizePure(metadata_json, allocator);
    defer deinitTokenList(allocator, &meta_tokens);
    var type_tokens = try tokenization_sqlite.tokenizePure(entity_type, allocator);
    defer deinitTokenList(allocator, &type_tokens);
    var domain_tokens = try tokenization_sqlite.tokenizePure(domain, allocator);
    defer deinitTokenList(allocator, &domain_tokens);

    var score: f32 = 0;
    var matched_terms: usize = 0;
    var name_hits: usize = 0;
    var meta_hits: usize = 0;

    for (terms) |term| {
        if (term.len == 0) continue;

        var hit = false;
        for (name_tokens.items) |token| {
            if (std.mem.eql(u8, token, term)) {
                score += 2.0;
                name_hits += 1;
                hit = true;
                break;
            }
        }
        if (!hit) {
            for (meta_tokens.items) |token| {
                if (std.mem.eql(u8, token, term)) {
                    score += 1.0;
                    meta_hits += 1;
                    hit = true;
                    break;
                }
            }
        }
        if (!hit) {
            for (type_tokens.items) |token| {
                if (std.mem.eql(u8, token, term)) {
                    score += 0.5;
                    hit = true;
                    break;
                }
            }
        }
        if (!hit) {
            for (domain_tokens.items) |token| {
                if (std.mem.eql(u8, token, term)) {
                    score += 0.5;
                    hit = true;
                    break;
                }
            }
        }
        if (!hit) return 0;
        matched_terms += 1;
    }

    const coverage = @as(f32, @floatFromInt(matched_terms)) / @as(f32, @floatFromInt(terms.len));
    const phrase_boost: f32 = if (fieldContainsAllTokens(allocator, name, terms) catch false) 1.0 else 0.0;
    const exact_name_boost: f32 = if (name_hits == terms.len and name_tokens.items.len == terms.len) 0.5 else 0.0;
    const metadata_boost: f32 = if (meta_hits > 0) 0.25 else 0.0;

    return (score * coverage) + phrase_boost + exact_name_boost + metadata_boost;
}

fn marketplaceFtsRank(query: []const u8, name: []const u8, metadata_json: []const u8) f32 {
    const terms = splitSearchTerms(std.heap.page_allocator, query) catch return 0;
    defer deinitStringSlice(std.heap.page_allocator, terms);
    return ftsScore(std.heap.page_allocator, terms, name, metadata_json, "", "") catch 0;
}

fn loadRelationsIntoStore(db: Database, store: *graph_store.Store, allocator: std.mem.Allocator) !void {
    const stmt = try prepare(db, "SELECT relation_id, source_id, target_id, relation_type, valid_from_unix, valid_to_unix, confidence FROM graph_relation ORDER BY relation_id");
    defer finalize(stmt);

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const relation_type = try dupeColumnText(allocator, stmt, 3);
        defer allocator.free(relation_type);

        try store.addRelation(.{
            .relation_id = try columnU32(stmt, 0),
            .source_id = try columnU32(stmt, 1),
            .target_id = try columnU32(stmt, 2),
            .relation_type = relation_type,
            .valid_from_unix = columnOptionalI64(stmt, 4),
            .valid_to_unix = columnOptionalI64(stmt, 5),
            .confidence = @floatCast(c.sqlite3_column_double(stmt, 6)),
        });
    }
}

fn loadAdjacencyIntoStore(
    db: Database,
    store: *graph_store.Store,
    allocator: std.mem.Allocator,
    table_name: []const u8,
    outgoing: bool,
) !void {
    const sql = try std.fmt.allocPrint(allocator, "SELECT entity_id, relation_ids_blob FROM {s} ORDER BY entity_id", .{table_name});
    defer allocator.free(sql);
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const entity_id = try columnU32(stmt, 0);
        var bitmap = try bitmapFromBlobColumn(stmt, 1);
        defer bitmap.deinit();

        const relation_ids = try bitmap.toArray(allocator);
        defer allocator.free(relation_ids);

        if (outgoing) {
            try store.setOutgoing(entity_id, relation_ids);
        } else {
            try store.setIncoming(entity_id, relation_ids);
        }
    }
}

fn loadAdjacencyBitmap(db: Database, allocator: std.mem.Allocator, table_name: []const u8, entity_id: u32) !roaring.Bitmap {
    const sql = try std.fmt.allocPrint(allocator, "SELECT relation_ids_blob FROM {s} WHERE entity_id = ?1", .{table_name});
    defer allocator.free(sql);
    const stmt = try prepare(db, sql);
    defer finalize(stmt);

    try bindInt64(stmt, 1, entity_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;
    return try bitmapFromBlobColumn(stmt, 0);
}

const TraverseVisit = struct {
    depth: usize,
    predecessor: ?TraversePredecessor,
};

const TraversePredecessor = struct {
    from_node: u32,
    relation_id: u32,
};

const TraverseEntitySummary = struct {
    entity_id: u32,
    name: []const u8,
    node_label: []const u8,
    node_type: []const u8,
    metadata_json: []const u8,
};

const TraverseNeighbor = struct {
    relation_id: u32,
    entity: TraverseEntitySummary,
};

fn loadRelation(db: Database, allocator: std.mem.Allocator, relation_id: u32) !?graph_store.RelationRecord {
    const stmt = try prepare(db, "SELECT relation_id, source_id, target_id, relation_type, valid_from_unix, valid_to_unix, confidence FROM graph_relation WHERE relation_id = ?1");
    defer finalize(stmt);
    try bindInt64(stmt, 1, relation_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return null;

    return .{
        .relation_id = try columnU32(stmt, 0),
        .source_id = try columnU32(stmt, 1),
        .target_id = try columnU32(stmt, 2),
        .relation_type = try dupeColumnText(allocator, stmt, 3),
        .valid_from_unix = columnOptionalI64(stmt, 4),
        .valid_to_unix = columnOptionalI64(stmt, 5),
        .confidence = @floatCast(c.sqlite3_column_double(stmt, 6)),
    };
}

fn loadRelationFull(db: Database, allocator: std.mem.Allocator, relation_id: u32) !?RelationRecordFull {
    const stmt = try prepare(
        db,
        "SELECT relation_id, workspace_id, relation_type, source_id, target_id, valid_from_unix, valid_to_unix, confidence, deprecated_at, run_id, patch_id, metadata_json, created_at_unix FROM graph_relation WHERE relation_id = ?1",
    );
    defer finalize(stmt);
    try bindInt64(stmt, 1, relation_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return null;
    return try loadRelationFullFromStmt(allocator, stmt);
}

fn loadRelationFullFromStmt(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt) !RelationRecordFull {
    return .{
        .relation_id = try columnU32(stmt, 0),
        .workspace_id = try dupeColumnText(allocator, stmt, 1),
        .relation_type = try dupeColumnText(allocator, stmt, 2),
        .source_id = try columnU32(stmt, 3),
        .target_id = try columnU32(stmt, 4),
        .valid_from_unix = columnOptionalI64(stmt, 5),
        .valid_to_unix = columnOptionalI64(stmt, 6),
        .confidence = @floatCast(c.sqlite3_column_double(stmt, 7)),
        .deprecated_at_unix = columnOptionalI64(stmt, 8),
        .run_id = if (c.sqlite3_column_type(stmt, 9) == c.SQLITE_NULL) null else @intCast(c.sqlite3_column_int64(stmt, 9)),
        .patch_id = if (c.sqlite3_column_type(stmt, 10) == c.SQLITE_NULL) null else @intCast(c.sqlite3_column_int64(stmt, 10)),
        .metadata_json = try dupeColumnText(allocator, stmt, 11),
        .created_at_unix = c.sqlite3_column_int64(stmt, 12),
    };
}

fn loadRelationsWithQuery(
    db: Database,
    allocator: std.mem.Allocator,
    query: []const u8,
    relation_type: ?[]const u8,
) ![]RelationRecordFull {
    const stmt = try prepare(db, query);
    defer finalize(stmt);
    if (relation_type) |kind| try bindText(stmt, 1, kind);

    var rows = std.ArrayList(RelationRecordFull).empty;
    defer {
        for (rows.items) |row| {
            allocator.free(row.workspace_id);
            allocator.free(row.relation_type);
            allocator.free(row.metadata_json);
        }
        rows.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(allocator, try loadRelationFullFromStmt(allocator, stmt));
    }

    return rows.toOwnedSlice(allocator);
}

fn buildEntityPayload(entity: EntityRecordFull) StreamEntityPayload {
    return .{
        .entity_id = entity.entity_id,
        .name = entity.name,
        .entity_type = entity.entity_type,
        .confidence = entity.confidence,
        .metadata_json = entity.metadata_json,
        .deprecated_at = entity.deprecated_at_unix,
        .created_at = entity.created_at_unix,
    };
}

fn buildRelationPayload(relation: RelationRecordFull) StreamRelationPayload {
    return .{
        .relation_id = relation.relation_id,
        .workspace_id = relation.workspace_id,
        .relation_type = relation.relation_type,
        .source_id = relation.source_id,
        .target_id = relation.target_id,
        .valid_from_unix = relation.valid_from_unix,
        .valid_to_unix = relation.valid_to_unix,
        .confidence = relation.confidence,
        .deprecated_at = relation.deprecated_at_unix,
        .run_id = relation.run_id,
        .patch_id = relation.patch_id,
        .metadata_json = relation.metadata_json,
        .created_at = relation.created_at_unix,
    };
}

fn appendJsonEvent(
    allocator: std.mem.Allocator,
    events: *std.ArrayList(GraphStreamEvent),
    seq: u32,
    kind: []const u8,
    payload: anytype,
) !void {
    const json = try std.json.Stringify.valueAlloc(allocator, payload, .{});
    errdefer allocator.free(json);
    try events.append(allocator, .{
        .seq = seq,
        .kind = kind,
        .payload = json,
    });
}

fn appendStepsForEdges(
    db: Database,
    allocator: std.mem.Allocator,
    steps: *std.ArrayList(interfaces.GraphNeighborStep),
    frontier: roaring.Bitmap,
    edge_ids: []const u32,
    filter: interfaces.GraphEdgeFilter,
) !void {
    for (edge_ids) |edge_id| {
        const relation = try loadRelation(db, allocator, edge_id) orelse continue;
        defer allocator.free(relation.relation_type);
        if (!passesFilter(relation, filter)) continue;

        if (frontier.contains(relation.source_id)) {
            try steps.append(allocator, .{
                .from_node = relation.source_id,
                .to_node = relation.target_id,
                .relation_id = relation.relation_id,
            });
        }
        if (frontier.contains(relation.target_id)) {
            try steps.append(allocator, .{
                .from_node = relation.target_id,
                .to_node = relation.source_id,
                .relation_id = relation.relation_id,
            });
        }
    }
}

fn loadEntity(db: Database, allocator: std.mem.Allocator, entity_id: u32) !EntityRecord {
    const stmt = try prepare(db, "SELECT entity_id, entity_type, name FROM graph_entity WHERE entity_id = ?1");
    defer finalize(stmt);
    try bindInt64(stmt, 1, entity_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;

    return .{
        .entity_id = try columnU32(stmt, 0),
        .entity_type = try dupeColumnText(allocator, stmt, 1),
        .name = try dupeColumnText(allocator, stmt, 2),
    };
}

fn loadTraverseEntityByName(db: Database, allocator: std.mem.Allocator, name: []const u8) !TraverseEntitySummary {
    const stmt = try prepare(
        db,
        "SELECT entity_id, name, COALESCE(json_extract(metadata_json, '$.label'), name), COALESCE(json_extract(metadata_json, '$.node_type'), 'entity'), metadata_json FROM graph_entity WHERE name = ?1 LIMIT 1",
    );
    defer finalize(stmt);
    try bindText(stmt, 1, name);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;
    return .{
        .entity_id = try columnU32(stmt, 0),
        .name = try dupeColumnText(allocator, stmt, 1),
        .node_label = try dupeColumnText(allocator, stmt, 2),
        .node_type = try dupeColumnText(allocator, stmt, 3),
        .metadata_json = try dupeColumnText(allocator, stmt, 4),
    };
}

fn loadTraverseEntityById(db: Database, allocator: std.mem.Allocator, entity_id: u32) !TraverseEntitySummary {
    const stmt = try prepare(
        db,
        "SELECT entity_id, name, COALESCE(json_extract(metadata_json, '$.label'), name), COALESCE(json_extract(metadata_json, '$.node_type'), 'entity'), metadata_json FROM graph_entity WHERE entity_id = ?1",
    );
    defer finalize(stmt);
    try bindInt64(stmt, 1, entity_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;
    return .{
        .entity_id = try columnU32(stmt, 0),
        .name = try dupeColumnText(allocator, stmt, 1),
        .node_label = try dupeColumnText(allocator, stmt, 2),
        .node_type = try dupeColumnText(allocator, stmt, 3),
        .metadata_json = try dupeColumnText(allocator, stmt, 4),
    };
}

fn loadTraverseEntityIdByName(db: Database, name: []const u8) !u32 {
    const stmt = try prepare(db, "SELECT entity_id FROM graph_entity WHERE name = ?1 LIMIT 1");
    defer finalize(stmt);
    try bindText(stmt, 1, name);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;
    return try columnU32(stmt, 0);
}

fn loadTraverseNeighbors(
    db: Database,
    allocator: std.mem.Allocator,
    entity_id: u32,
    direction: TraverseDirection,
    filter: interfaces.GraphEdgeFilter,
) ![]TraverseNeighbor {
    const sql = switch (direction) {
        .outbound => "SELECT r.relation_id, r.relation_type, r.source_id, r.target_id, r.valid_from_unix, r.valid_to_unix, r.confidence, n.entity_id, n.name, COALESCE(json_extract(n.metadata_json, '$.label'), n.name), COALESCE(json_extract(n.metadata_json, '$.node_type'), 'entity'), n.metadata_json FROM graph_relation r JOIN graph_entity n ON n.entity_id = r.target_id WHERE r.source_id = ?1 ORDER BY r.relation_id ASC",
        .inbound => "SELECT r.relation_id, r.relation_type, r.source_id, r.target_id, r.valid_from_unix, r.valid_to_unix, r.confidence, n.entity_id, n.name, COALESCE(json_extract(n.metadata_json, '$.label'), n.name), COALESCE(json_extract(n.metadata_json, '$.node_type'), 'entity'), n.metadata_json FROM graph_relation r JOIN graph_entity n ON n.entity_id = r.source_id WHERE r.target_id = ?1 ORDER BY r.relation_id ASC",
    };
    const stmt = try prepare(db, sql);
    defer finalize(stmt);
    try bindInt64(stmt, 1, entity_id);

    var neighbors = std.ArrayList(TraverseNeighbor).empty;
    errdefer {
        for (neighbors.items) |neighbor| deinitTraverseNeighbor(allocator, neighbor);
        neighbors.deinit(allocator);
    }

    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const relation = graph_store.RelationRecord{
            .relation_id = try columnU32(stmt, 0),
            .source_id = try columnU32(stmt, 2),
            .target_id = try columnU32(stmt, 3),
            .relation_type = try dupeColumnText(allocator, stmt, 1),
            .valid_from_unix = columnOptionalI64(stmt, 4),
            .valid_to_unix = columnOptionalI64(stmt, 5),
            .confidence = @floatCast(c.sqlite3_column_double(stmt, 6)),
        };
        defer allocator.free(relation.relation_type);
        if (!passesFilter(relation, filter)) continue;

        try neighbors.append(allocator, .{
            .relation_id = relation.relation_id,
            .entity = .{
                .entity_id = try columnU32(stmt, 7),
                .name = try dupeColumnText(allocator, stmt, 8),
                .node_label = try dupeColumnText(allocator, stmt, 9),
                .node_type = try dupeColumnText(allocator, stmt, 10),
                .metadata_json = try dupeColumnText(allocator, stmt, 11),
            },
        });
    }

    return neighbors.toOwnedSlice(allocator);
}

fn deinitTraverseNeighbor(allocator: std.mem.Allocator, neighbor: TraverseNeighbor) void {
    deinitTraverseEntitySummary(allocator, neighbor.entity);
}

fn deinitTraverseEntitySummary(allocator: std.mem.Allocator, entity: TraverseEntitySummary) void {
    allocator.free(entity.name);
    allocator.free(entity.node_label);
    allocator.free(entity.node_type);
    allocator.free(entity.metadata_json);
}

fn buildTraversePathIds(
    allocator: std.mem.Allocator,
    visits: std.AutoHashMap(u32, TraverseVisit),
    node_id: u32,
) ![]u32 {
    var reversed = std.ArrayList(u32).empty;
    defer reversed.deinit(allocator);

    var current = node_id;
    while (true) {
        try reversed.append(allocator, current);
        const visit = visits.get(current) orelse return error.MissingRow;
        if (visit.predecessor) |predecessor| {
            current = predecessor.from_node;
        } else {
            break;
        }
    }

    const path = try allocator.alloc(u32, reversed.items.len);
    for (reversed.items, 0..) |id, index| {
        path[reversed.items.len - 1 - index] = id;
    }
    return path;
}

fn buildTraversePathNames(
    db: Database,
    allocator: std.mem.Allocator,
    visits: std.AutoHashMap(u32, TraverseVisit),
    node_id: u32,
) ![][]const u8 {
    const path_ids = try buildTraversePathIds(allocator, visits, node_id);
    defer allocator.free(path_ids);

    const path = try allocator.alloc([]const u8, path_ids.len);
    var initialized: usize = 0;
    errdefer {
        for (path[0..initialized]) |segment| allocator.free(segment);
        allocator.free(path);
    }

    for (path_ids, 0..) |path_id, index| {
        const entity = try loadTraverseEntityById(db, allocator, path_id);
        path[index] = entity.name;
        initialized += 1;
        allocator.free(entity.node_label);
        allocator.free(entity.node_type);
        allocator.free(entity.metadata_json);
    }

    return path;
}

fn buildTraverseRows(
    db: Database,
    allocator: std.mem.Allocator,
    visits: std.AutoHashMap(u32, TraverseVisit),
    node_ids: []const u32,
) ![]TraverseRow {
    const rows = try allocator.alloc(TraverseRow, node_ids.len);
    var initialized: usize = 0;
    errdefer {
        for (rows[0..initialized]) |*row| {
            allocator.free(row.node_id);
            allocator.free(row.node_label);
            allocator.free(row.node_type);
            allocator.free(row.metadata_json);
            if (row.edge_label) |edge_label| allocator.free(edge_label);
            for (row.path) |segment| allocator.free(segment);
            allocator.free(row.path);
        }
        allocator.free(rows);
    }

    for (node_ids, 0..) |node_id, index| {
        const entity = try loadTraverseEntityById(db, allocator, node_id);
        errdefer deinitTraverseEntitySummary(allocator, entity);
        const visit = visits.get(node_id) orelse return error.MissingRow;
        const path = try buildTraversePathNames(db, allocator, visits, node_id);
        const path_initialized: usize = path.len;
        errdefer {
            for (path[0..path_initialized]) |segment| allocator.free(segment);
            allocator.free(path);
        }

        var edge_label: ?[]const u8 = null;
        if (visit.predecessor) |predecessor| {
            const relation = (try loadRelation(db, allocator, predecessor.relation_id)) orelse return error.MissingRow;
            edge_label = relation.relation_type;
        }

        rows[index] = .{
            .node_id = entity.name,
            .node_label = entity.node_label,
            .node_type = entity.node_type,
            .metadata_json = entity.metadata_json,
            .edge_label = edge_label,
            .depth = visit.depth,
            .path = path,
        };
        initialized += 1;
    }

    return rows;
}

fn passesFilter(relation: graph_store.RelationRecord, filter: interfaces.GraphEdgeFilter) bool {
    if (filter.edge_types) |edge_types| {
        var matched = false;
        for (edge_types) |edge_type| {
            if (std.mem.eql(u8, edge_type, relation.relation_type)) {
                matched = true;
                break;
            }
        }
        if (!matched) return false;
    }
    if (filter.confidence_min) |min| if (relation.confidence < min) return false;
    if (filter.confidence_max) |max| if (relation.confidence > max) return false;
    if (filter.after_unix_seconds) |after| if (relation.valid_to_unix) |valid_to| if (valid_to < after) return false;
    if (filter.before_unix_seconds) |before| if (relation.valid_from_unix) |valid_from| if (valid_from > before) return false;
    return true;
}

fn bitmapFromBlobColumn(stmt: *c.sqlite3_stmt, index: c_int) !roaring.Bitmap {
    const len = c.sqlite3_column_bytes(stmt, index);
    const ptr = c.sqlite3_column_blob(stmt, index) orelse return error.MissingRow;
    const bytes: []const u8 = @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
    return try roaring.Bitmap.deserializePortable(bytes);
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

fn resetStatement(stmt: *c.sqlite3_stmt) Error!void {
    if (c.sqlite3_reset(stmt) != c.SQLITE_OK) return error.StepFailed;
    if (c.sqlite3_clear_bindings(stmt) != c.SQLITE_OK) return error.StepFailed;
}

fn bindInt64(stmt: *c.sqlite3_stmt, index: c_int, value: anytype) Error!void {
    if (c.sqlite3_bind_int64(stmt, index, @intCast(value)) != c.SQLITE_OK) return error.BindFailed;
}

fn bindText(stmt: *c.sqlite3_stmt, index: c_int, value: []const u8) Error!void {
    if (c.sqlite3_bind_text(stmt, index, value.ptr, @intCast(value.len), facet_sqlite.sqliteTransient()) != c.SQLITE_OK) {
        return error.BindFailed;
    }
}

fn bindBlob(stmt: *c.sqlite3_stmt, index: c_int, value: []const u8) Error!void {
    if (c.sqlite3_bind_blob(stmt, index, value.ptr, @intCast(value.len), facet_sqlite.sqliteTransient()) != c.SQLITE_OK) {
        return error.BindFailed;
    }
}

fn bindNull(stmt: *c.sqlite3_stmt, index: c_int) Error!void {
    if (c.sqlite3_bind_null(stmt, index) != c.SQLITE_OK) return error.BindFailed;
}

fn columnU32(stmt: *c.sqlite3_stmt, index: c_int) Error!u32 {
    const value = c.sqlite3_column_int64(stmt, index);
    return std.math.cast(u32, value) orelse error.ValueOutOfRange;
}

fn columnU64(stmt: *c.sqlite3_stmt, index: c_int) Error!u64 {
    const value = c.sqlite3_column_int64(stmt, index);
    return std.math.cast(u64, value) orelse error.ValueOutOfRange;
}

fn columnOptionalI64(stmt: *c.sqlite3_stmt, index: c_int) ?i64 {
    if (c.sqlite3_column_type(stmt, index) == c.SQLITE_NULL) return null;
    return c.sqlite3_column_int64(stmt, index);
}

fn dupeColumnText(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, index: c_int) ![]const u8 {
    const len = c.sqlite3_column_bytes(stmt, index);
    const ptr = c.sqlite3_column_text(stmt, index) orelse return error.MissingRow;
    const bytes: []const u8 = @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
    return try allocator.dupe(u8, bytes);
}

test "sqlite-backed graph repository performs k-hop traversal" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertEntity(db, 1, "person", "Ada");
    try insertEntity(db, 2, "company", "Acme");
    try insertEntity(db, 3, "article", "Paper");
    try insertEntity(db, 4, "company", "Labs");

    try insertRelation(db, .{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "works_for", .confidence = 0.9 });
    try insertRelation(db, .{ .relation_id = 11, .source_id = 2, .target_id = 3, .relation_type = "published", .confidence = 0.8 });
    try insertRelation(db, .{ .relation_id = 12, .source_id = 2, .target_id = 4, .relation_type = "works_for", .confidence = 0.95 });

    try insertAdjacency(db, "graph_lj_out", 1, &.{10}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_out", 2, &.{ 11, 12 }, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 2, &.{10}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 3, &.{11}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 4, &.{12}, std.testing.allocator);

    const repository = DurableRepository{ .db = &db };
    var seed = try roaring.Bitmap.fromSlice(&.{1});
    defer seed.deinit();

    var result = try graph_store.kHops(std.testing.allocator, repository.asGraphRepository(), seed, 2, .{
        .edge_types = &.{"works_for"},
    });
    defer result.deinit();

    const ids = try result.toArray(std.testing.allocator);
    defer std.testing.allocator.free(ids);

    try std.testing.expectEqualSlices(u32, &.{ 1, 2, 4 }, ids);
}

test "sqlite graph traverse returns outbound rows with metadata and paths" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertEntityWithMetadata(db, 1, "entity", "knowledge:Pricing", "{\"label\":\"Pricing\",\"node_type\":\"concept\",\"mastery\":1}");
    try insertEntityWithMetadata(db, 2, "entity", "knowledge:Yield", "{\"label\":\"Yield\",\"node_type\":\"concept\",\"mastery\":0}");
    try insertEntityWithMetadata(db, 3, "entity", "knowledge:Hotel", "{\"label\":\"Hotel\",\"node_type\":\"concept\",\"mastery\":0}");
    try insertEntityWithMetadata(db, 4, "entity", "knowledge:Ignore", "{\"label\":\"Ignore\",\"node_type\":\"concept\",\"mastery\":0}");

    try insertRelation(db, .{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "depends_on", .confidence = 0.9 });
    try insertRelation(db, .{ .relation_id = 11, .source_id = 2, .target_id = 3, .relation_type = "depends_on", .confidence = 0.8 });
    try insertRelation(db, .{ .relation_id = 12, .source_id = 1, .target_id = 4, .relation_type = "related_to", .confidence = 0.7 });

    var result = try traverse(
        db,
        std.testing.allocator,
        "knowledge:Pricing",
        .outbound,
        &.{"depends_on"},
        2,
        null,
    );
    defer result.deinit(std.testing.allocator);

    try std.testing.expect(!result.target_found);
    try std.testing.expectEqual(@as(usize, 3), result.rows.len);
    try std.testing.expectEqualStrings("knowledge:Pricing", result.rows[0].node_id);
    try std.testing.expectEqualStrings("Pricing", result.rows[0].node_label);
    try std.testing.expectEqualStrings("concept", result.rows[0].node_type);
    try std.testing.expectEqual(@as(usize, 1), result.rows[0].path.len);
    try std.testing.expectEqualStrings("knowledge:Pricing", result.rows[0].path[0]);

    try std.testing.expectEqualStrings("knowledge:Yield", result.rows[1].node_id);
    try std.testing.expectEqualStrings("depends_on", result.rows[1].edge_label.?);
    try std.testing.expectEqual(@as(usize, 2), result.rows[1].path.len);
    try std.testing.expectEqualStrings("knowledge:Pricing", result.rows[1].path[0]);
    try std.testing.expectEqualStrings("knowledge:Yield", result.rows[1].path[1]);

    try std.testing.expectEqualStrings("knowledge:Hotel", result.rows[2].node_id);
    try std.testing.expectEqual(@as(usize, 3), result.rows[2].path.len);
    try std.testing.expect(std.mem.indexOf(u8, result.rows[2].metadata_json, "\"mastery\":0") != null);
}

test "sqlite graph traverse returns only the directed target path when found" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertEntityWithMetadata(db, 1, "entity", "knowledge:Pricing", "{\"label\":\"Pricing\",\"node_type\":\"concept\"}");
    try insertEntityWithMetadata(db, 2, "entity", "knowledge:Yield", "{\"label\":\"Yield\",\"node_type\":\"concept\"}");
    try insertEntityWithMetadata(db, 3, "entity", "knowledge:Hotel", "{\"label\":\"Hotel\",\"node_type\":\"concept\"}");
    try insertEntityWithMetadata(db, 4, "entity", "knowledge:Branch", "{\"label\":\"Branch\",\"node_type\":\"concept\"}");

    try insertRelation(db, .{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "depends_on", .confidence = 0.9 });
    try insertRelation(db, .{ .relation_id = 11, .source_id = 2, .target_id = 3, .relation_type = "depends_on", .confidence = 0.8 });
    try insertRelation(db, .{ .relation_id = 12, .source_id = 1, .target_id = 4, .relation_type = "depends_on", .confidence = 0.7 });

    var result = try traverse(
        db,
        std.testing.allocator,
        "knowledge:Pricing",
        .outbound,
        &.{"depends_on"},
        3,
        "knowledge:Hotel",
    );
    defer result.deinit(std.testing.allocator);

    try std.testing.expect(result.target_found);
    try std.testing.expectEqual(@as(usize, 3), result.rows.len);
    try std.testing.expectEqualStrings("knowledge:Pricing", result.rows[0].node_id);
    try std.testing.expectEqualStrings("knowledge:Yield", result.rows[1].node_id);
    try std.testing.expectEqualStrings("knowledge:Hotel", result.rows[2].node_id);
    try std.testing.expectEqual(@as(usize, 3), result.rows[2].path.len);
    try std.testing.expectEqualStrings("knowledge:Pricing", result.rows[2].path[0]);
    try std.testing.expectEqualStrings("knowledge:Yield", result.rows[2].path[1]);
    try std.testing.expectEqualStrings("knowledge:Hotel", result.rows[2].path[2]);
}

test "sqlite graph traverse supports inbound direction" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertEntityWithMetadata(db, 1, "entity", "knowledge:Pricing", "{\"label\":\"Pricing\",\"node_type\":\"concept\"}");
    try insertEntityWithMetadata(db, 2, "entity", "knowledge:Yield", "{\"label\":\"Yield\",\"node_type\":\"concept\"}");
    try insertEntityWithMetadata(db, 3, "entity", "knowledge:Hotel", "{\"label\":\"Hotel\",\"node_type\":\"concept\"}");

    try insertRelation(db, .{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "depends_on", .confidence = 0.9 });
    try insertRelation(db, .{ .relation_id = 11, .source_id = 2, .target_id = 3, .relation_type = "depends_on", .confidence = 0.8 });

    var result = try traverse(
        db,
        std.testing.allocator,
        "knowledge:Hotel",
        .inbound,
        &.{"depends_on"},
        2,
        null,
    );
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 3), result.rows.len);
    try std.testing.expectEqualStrings("knowledge:Hotel", result.rows[0].node_id);
    try std.testing.expectEqualStrings("knowledge:Yield", result.rows[1].node_id);
    try std.testing.expectEqualStrings("knowledge:Pricing", result.rows[2].node_id);
    try std.testing.expectEqual(@as(usize, 3), result.rows[2].path.len);
    try std.testing.expectEqualStrings("knowledge:Hotel", result.rows[2].path[0]);
    try std.testing.expectEqualStrings("knowledge:Yield", result.rows[2].path[1]);
    try std.testing.expectEqualStrings("knowledge:Pricing", result.rows[2].path[2]);
}

test "sqlite-backed graph repository computes shortest path hop count" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertEntity(db, 1, "person", "Ada");
    try insertEntity(db, 2, "company", "Acme");
    try insertEntity(db, 3, "article", "Paper");
    try insertEntity(db, 4, "company", "Labs");

    try insertRelation(db, .{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "works_for", .confidence = 0.9 });
    try insertRelation(db, .{ .relation_id = 11, .source_id = 2, .target_id = 3, .relation_type = "published", .confidence = 0.8 });
    try insertRelation(db, .{ .relation_id = 12, .source_id = 2, .target_id = 4, .relation_type = "works_for", .confidence = 0.95 });

    try insertAdjacency(db, "graph_lj_out", 1, &.{10}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_out", 2, &.{ 11, 12 }, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 2, &.{10}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 3, &.{11}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 4, &.{12}, std.testing.allocator);

    const repository = DurableRepository{ .db = &db };
    const hops = try graph_store.shortestPathHops(
        std.testing.allocator,
        repository.asGraphRepository(),
        1,
        4,
        .{ .edge_types = &.{"works_for"} },
        4,
    );
    try std.testing.expectEqual(@as(?usize, 2), hops);
}

test "sqlite-backed graph repository reconstructs shortest path edges" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertEntity(db, 1, "person", "Ada");
    try insertEntity(db, 2, "company", "Acme");
    try insertEntity(db, 3, "article", "Paper");
    try insertEntity(db, 4, "place", "City");

    try insertRelation(db, .{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "works_for", .confidence = 0.9 });
    try insertRelation(db, .{ .relation_id = 11, .source_id = 2, .target_id = 3, .relation_type = "published", .confidence = 0.8 });
    try insertRelation(db, .{ .relation_id = 12, .source_id = 3, .target_id = 4, .relation_type = "located_in", .confidence = 0.9 });

    try insertAdjacency(db, "graph_lj_out", 1, &.{10}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_out", 2, &.{11}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_out", 3, &.{12}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 2, &.{10}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 3, &.{11}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 4, &.{12}, std.testing.allocator);

    const repository = DurableRepository{ .db = &db };
    const path = (try graph_store.shortestPath(
        std.testing.allocator,
        repository.asGraphRepository(),
        1,
        4,
        .{},
        4,
    )).?;
    defer std.testing.allocator.free(path);

    try std.testing.expectEqual(@as(usize, 3), path.len);
    try std.testing.expectEqual(@as(u32, 10), path[0].relation_id);
    try std.testing.expectEqual(@as(u32, 11), path[1].relation_id);
    try std.testing.expectEqual(@as(u32, 12), path[2].relation_id);
}

test "sqlite graph store loader rehydrates adjacency for in-memory traversal" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertEntity(db, 1, "person", "Ada");
    try insertEntity(db, 2, "company", "Acme");
    try insertEntity(db, 3, "article", "Paper");

    try insertRelation(db, .{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "works_for", .confidence = 0.9 });
    try insertRelation(db, .{ .relation_id = 11, .source_id = 2, .target_id = 3, .relation_type = "published", .confidence = 0.8 });

    try insertAdjacency(db, "graph_lj_out", 1, &.{10}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_out", 2, &.{11}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 2, &.{10}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 3, &.{11}, std.testing.allocator);

    var runtime = try loadRuntime(db, std.testing.allocator);
    defer runtime.deinit();

    const hops = try runtime.shortestPathHops(std.testing.allocator, 1, 3, .{}, 4);
    try std.testing.expectEqual(@as(?usize, 2), hops);

    const path = (try runtime.shortestPath(
        std.testing.allocator,
        1,
        3,
        .{},
        4,
    )).?;
    defer std.testing.allocator.free(path);

    try std.testing.expectEqual(@as(usize, 2), path.len);
    try std.testing.expectEqual(@as(u32, 10), path[0].relation_id);
    try std.testing.expectEqual(@as(u32, 11), path[1].relation_id);

    var reachable = try runtime.kHops(
        std.testing.allocator,
        &.{1},
        2,
        .{},
    );
    defer reachable.deinit();

    const reachable_ids = try reachable.toArray(std.testing.allocator);
    defer std.testing.allocator.free(reachable_ids);
    try std.testing.expectEqualSlices(u32, &.{ 1, 2, 3 }, reachable_ids);
}

test "sqlite-backed graph path result includes node and edge metadata" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertEntity(db, 1, "person", "Ada");
    try insertEntity(db, 2, "company", "Acme");
    try insertEntity(db, 3, "article", "Paper");

    try insertRelation(db, .{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "works_for", .confidence = 0.9 });
    try insertRelation(db, .{ .relation_id = 11, .source_id = 2, .target_id = 3, .relation_type = "published", .confidence = 0.8 });

    try insertAdjacency(db, "graph_lj_out", 1, &.{10}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_out", 2, &.{11}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 2, &.{10}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 3, &.{11}, std.testing.allocator);

    var runtime = try loadRuntime(db, std.testing.allocator);
    defer runtime.deinit();

    const path = (try runtime.shortestPath(
        std.testing.allocator,
        1,
        3,
        .{},
        4,
    )).?;
    defer std.testing.allocator.free(path);

    var result = try buildPathResult(db, std.testing.allocator, path);
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), result.hops);
    try std.testing.expectEqualStrings("works_for", result.segments[0].relation_type);
    try std.testing.expectEqualStrings("Ada", result.segments[0].source.name);
    try std.testing.expectEqualStrings("Acme", result.segments[0].target.name);
    try std.testing.expectEqualStrings("published", result.segments[1].relation_type);
    try std.testing.expectEqualStrings("Paper", result.segments[1].target.name);
}

test "sqlite-backed graph path result fails cleanly when path metadata is missing" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertEntity(db, 1, "person", "Ada");
    try insertRelation(db, .{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "works_for", .confidence = 0.9 });

    const path = [_]graph_store.PathEdge{
        .{ .from_node = 1, .to_node = 2, .relation_id = 10 },
    };

    try std.testing.expectError(error.MissingRow, buildPathResult(db, std.testing.allocator, &path));
}

test "sqlite-backed graph traversal has a TOON export variant" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertEntityWithMetadata(db, 1, "person", "Ada", "{\"label\":\"Ada\",\"node_type\":\"person\"}");
    try insertEntityWithMetadata(db, 2, "company", "Acme", "{\"label\":\"Acme\",\"node_type\":\"company\"}");
    try insertRelation(db, .{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "works_for", .confidence = 0.9 });

    const toon = try traverseToon(
        db,
        std.testing.allocator,
        "Ada",
        .outbound,
        null,
        1,
        null,
    );
    defer std.testing.allocator.free(toon);

    try std.testing.expect(std.mem.indexOf(u8, toon, "kind: graph_traverse") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "start_name: Ada") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "direction: outbound") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "node_id: Ada") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "node_id: Acme") != null);
}

test "sqlite-backed graph shortest path has a TOON export variant" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertEntityWithMetadata(db, 1, "person", "Ada", "{\"label\":\"Ada\",\"node_type\":\"person\"}");
    try insertEntityWithMetadata(db, 2, "company", "Acme", "{\"label\":\"Acme\",\"node_type\":\"company\"}");
    try insertEntityWithMetadata(db, 3, "article", "Paper", "{\"label\":\"Paper\",\"node_type\":\"document\"}");
    try insertRelation(db, .{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "works_for", .confidence = 0.9 });
    try insertRelation(db, .{ .relation_id = 11, .source_id = 2, .target_id = 3, .relation_type = "published", .confidence = 0.8 });

    try insertAdjacency(db, "graph_lj_out", 1, &.{10}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_out", 2, &.{11}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 2, &.{10}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 3, &.{11}, std.testing.allocator);

    const toon = try shortestPathToon(
        db,
        std.testing.allocator,
        "Ada",
        "Paper",
        null,
        4,
    );
    defer std.testing.allocator.free(toon);

    try std.testing.expect(std.mem.indexOf(u8, toon, "kind: graph_path") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "source_name: Ada") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "target_name: Paper") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "target_found: true") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "segments[2\t]{relation_id\trelation_type\tsource_id\tsource_name\ttarget_id\ttarget_name\tconfidence}:") != null);
}

test "sqlite graph search helpers have TOON export variants" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertEntityWithMetadata(db, 1, "person", "Ada", "{\"label\":\"Ada\",\"node_type\":\"person\",\"domain\":\"systems\"}");
    try insertEntityWithMetadata(db, 2, "company", "Acme", "{\"label\":\"Acme\",\"node_type\":\"company\",\"domain\":\"systems\"}");
    try insertEntityWithMetadata(db, 3, "topic", "Graph", "{\"label\":\"Graph\",\"node_type\":\"topic\",\"domain\":\"systems\"}");
    try insertRelation(db, .{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "works_for", .confidence = 0.9 });
    try insertRelation(db, .{ .relation_id = 11, .source_id = 1, .target_id = 3, .relation_type = "requires", .confidence = 0.8 });
    try insertRelation(db, .{ .relation_id = 12, .source_id = 2, .target_id = 3, .relation_type = "related_to", .confidence = 0.7 });
    try insertAdjacency(db, "graph_lj_out", 1, &.{ 10, 11 }, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_out", 2, &.{12}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 2, &.{10}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 3, &.{ 11, 12 }, std.testing.allocator);

    const marketplace_toon = try marketplaceSearchToon(db, std.testing.allocator, "Ada", "systems", 0.0, 2, 10);
    defer std.testing.allocator.free(marketplace_toon);
    try std.testing.expect(std.mem.indexOf(u8, marketplace_toon, "kind: graph_marketplace_search") != null);
    try std.testing.expect(std.mem.indexOf(u8, marketplace_toon, "query: Ada") != null);
    try std.testing.expect(std.mem.indexOf(u8, marketplace_toon, "results[") != null);

    const entity_toon = try entityFtsSearchToon(db, std.testing.allocator, "Ada", &.{"person"}, "systems", 0.0, 10);
    defer std.testing.allocator.free(entity_toon);
    try std.testing.expect(std.mem.indexOf(u8, entity_toon, "kind: graph_entity_fts_search") != null);
    try std.testing.expect(std.mem.indexOf(u8, entity_toon, "query: Ada") != null);
    try std.testing.expect(std.mem.indexOf(u8, entity_toon, "results[") != null);

    const deps_toon = try skillDependenciesToon(db, std.testing.allocator, 1, 5, 0.0);
    defer std.testing.allocator.free(deps_toon);
    try std.testing.expect(std.mem.indexOf(u8, deps_toon, "kind: graph_skill_dependencies") != null);
    try std.testing.expect(std.mem.indexOf(u8, deps_toon, "requires") != null);
}

test "sqlite graph canonical tables support alias resolution and entity-document links" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertEntity(db, 1, "person", "Ada Lovelace");
    try insertEntity(db, 2, "person", "Ada Byron");
    try insertEntityAlias(db, "ada", 1, 0.95);
    try insertEntityAlias(db, "ada", 2, 0.80);
    try insertEntityDocument(db, 1, 42, 9001, "author", 0.9);
    try insertEntityDocument(db, 1, 43, 9001, "subject", 0.8);

    const matches = try resolveAlias(db, std.testing.allocator, "ada");
    defer {
        for (matches) |match| {
            std.testing.allocator.free(match.name);
            std.testing.allocator.free(match.entity_type);
        }
        std.testing.allocator.free(matches);
    }

    try std.testing.expectEqual(@as(usize, 2), matches.len);
    try std.testing.expectEqual(@as(u32, 1), matches[0].entity_id);
    try std.testing.expectEqualStrings("Ada Lovelace", matches[0].name);
    try std.testing.expect(matches[0].confidence > matches[1].confidence);

    const docs = try loadEntityDocuments(db, std.testing.allocator, 1);
    defer {
        for (docs) |doc| if (doc.role) |role| std.testing.allocator.free(role);
        std.testing.allocator.free(docs);
    }

    try std.testing.expectEqual(@as(usize, 2), docs.len);
    try std.testing.expectEqual(@as(u64, 42), docs[0].doc_id);
    try std.testing.expectEqual(@as(u64, 9001), docs[0].table_id);
    try std.testing.expectEqualStrings("author", docs[0].role.?);
}

test "sqlite graph lookup helpers expose entity and relation records" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertEntityWithMetadata(db, 1, "person", "Ada", "{\"label\":\"Ada\",\"domain\":\"math\"}");
    try insertEntityWithMetadata(db, 2, "company", "Acme", "{\"label\":\"Acme\"}");
    try insertRelation(db, .{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "works_for", .confidence = 0.9 });

    var ada = try getEntity(db, std.testing.allocator, 1);
    defer ada.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("Ada", ada.name);
    try std.testing.expectEqualStrings("person", ada.entity_type);

    var by_name = try getEntityByName(db, std.testing.allocator, "Ada");
    defer by_name.deinit(std.testing.allocator);
    try std.testing.expectEqual(ada.entity_id, by_name.entity_id);

    try setEntityWorkspaceId(db, 1, "workspace-alpha");
    try setEntityWorkspaceId(db, 2, "workspace-alpha");
    try std.testing.expectEqual(@as(u64, 2), try countEntitiesByWorkspace(db, "workspace-alpha"));
    try std.testing.expectEqual(@as(u64, 0), try countEntitiesByWorkspace(db, "workspace-beta"));

    const from = try getRelationsFrom(db, std.testing.allocator, 1, "works_for");
    defer {
        for (from) |*relation| relation.deinit(std.testing.allocator);
        std.testing.allocator.free(from);
    }
    try std.testing.expectEqual(@as(usize, 1), from.len);
    try std.testing.expectEqualStrings("works_for", from[0].relation_type);
    try std.testing.expectEqualStrings("default", from[0].workspace_id);

    const to = try getRelationsTo(db, std.testing.allocator, 2, null);
    defer {
        for (to) |*relation| relation.deinit(std.testing.allocator);
        std.testing.allocator.free(to);
    }
    try std.testing.expectEqual(@as(usize, 1), to.len);

    const match = try findRelationByEndpoints(db, std.testing.allocator, "Ada", "Acme", "works_for");
    try std.testing.expect(match != null);
    if (match) |relation| {
        var mutable_relation = relation;
        defer mutable_relation.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(u32, 10), mutable_relation.relation_id);
        try std.testing.expectEqualStrings("works_for", mutable_relation.relation_type);
    }

    const missing = try findEntityByName(db, std.testing.allocator, "Unknown");
    try std.testing.expect(missing == null);
}

test "sqlite graph cleanup helper removes prefixed test data" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertEntityWithMetadata(db, 1, "person", "cleanup-ada", "{\"label\":\"cleanup-ada\"}");
    try insertEntityWithMetadata(db, 2, "company", "cleanup-acme", "{\"label\":\"cleanup-acme\"}");
    try insertEntityAlias(db, "cleanup-term", 1, 0.9);
    try insertEntityDocument(db, 1, 42, 7, "cleanup-role", 0.8);
    try db.exec("INSERT INTO graph_execution_run(run_key, domain, outcome, confidence, metadata_json) VALUES ('cleanup-run', 'test', 'success', 1.0, '{}')");
    try db.exec("INSERT INTO graph_knowledge_patch(patch_key, domain, status, confidence, artifacts_json) VALUES ('cleanup-patch', 'test', 'pending', 1.0, '[]')");
    try db.exec("INSERT INTO graph_relation(relation_id, relation_type, source_id, target_id, confidence, metadata_json) VALUES (77, 'related_to', 1, 2, 0.8, '{\"test_prefix\":\"cleanup\"}')");

    try cleanupTestData(db, "cleanup");

    try std.testing.expectEqual(@as(u64, 0), try countEntitiesByWorkspace(db, "default"));
    try std.testing.expectEqual(@as(u64, 0), try countEntitiesByWorkspace(db, "cleanup"));

    const alias_stmt = try prepare(db, "SELECT COUNT(*) FROM graph_entity_alias WHERE term LIKE 'cleanup%'");
    defer finalize(alias_stmt);
    try std.testing.expect(c.sqlite3_step(alias_stmt) == c.SQLITE_ROW);
    try std.testing.expectEqual(@as(i64, 0), c.sqlite3_column_int64(alias_stmt, 0));
}

test "sqlite graph stream helpers emit json events" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try insertEntityWithMetadata(db, 1, "person", "Ada", "{\"label\":\"Ada\",\"node_type\":\"person\"}");
    try insertEntityWithMetadata(db, 2, "company", "Acme", "{\"label\":\"Acme\",\"node_type\":\"company\"}");
    try insertEntityWithMetadata(db, 3, "company", "Labs", "{\"label\":\"Labs\",\"node_type\":\"company\"}");
    try insertRelation(db, .{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "works_for", .confidence = 0.9 });
    try insertRelation(db, .{ .relation_id = 11, .source_id = 3, .target_id = 1, .relation_type = "reports_to", .confidence = 0.85 });
    try insertAdjacency(db, "graph_lj_out", 1, &.{10}, std.testing.allocator);
    try insertAdjacency(db, "graph_lj_in", 1, &.{11}, std.testing.allocator);

    const neighborhood = try streamEntityNeighborhood(db, std.testing.allocator, 1, 10, 10, 0.0);
    defer {
        for (neighborhood) |*event| event.deinit(std.testing.allocator);
        std.testing.allocator.free(neighborhood);
    }
    try std.testing.expectEqualStrings("seed_node", neighborhood[0].kind);
    try std.testing.expect(std.mem.indexOf(u8, neighborhood[0].payload, "\"entity_id\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, neighborhood[1].payload, "\"direction\":\"outbound\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, neighborhood[2].payload, "\"entity_id\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, neighborhood[3].payload, "\"direction\":\"inbound\"") != null);
    try std.testing.expectEqualStrings("done", neighborhood[neighborhood.len - 1].kind);
    try std.testing.expect(std.mem.indexOf(u8, neighborhood[neighborhood.len - 1].payload, "\"node_count\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, neighborhood[neighborhood.len - 1].payload, "\"edge_count\":2") != null);

    const subgraph = try streamSubgraph(db, std.testing.allocator, &.{1}, 2, null, null, null, null, null);
    defer {
        for (subgraph) |*event| event.deinit(std.testing.allocator);
        std.testing.allocator.free(subgraph);
    }
    try std.testing.expectEqualStrings("seed_node", subgraph[0].kind);
    try std.testing.expectEqualStrings("done", subgraph[subgraph.len - 1].kind);
    try std.testing.expect(std.mem.indexOf(u8, subgraph[0].payload, "\"entity_id\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, subgraph[subgraph.len - 1].payload, "\"kind\":\"subgraph\"") != null);
}

test "sqlite graph migration supports natural-key upserts and deprecation" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    const id1 = try upsertEntityNatural(db, std.testing.allocator, "skill", "zig", 0.7, "{\"domain\":\"systems\"}");
    const id2 = try upsertEntityNatural(db, std.testing.allocator, "skill", "zig", 0.95, "{\"domain\":\"systems\"}");
    try std.testing.expectEqual(id1, id2);

    const active = try findEntitiesByType(db, std.testing.allocator, "skill");
    defer {
        for (active) |row| {
            std.testing.allocator.free(row.entity_type);
            std.testing.allocator.free(row.name);
            std.testing.allocator.free(row.metadata_json);
        }
        std.testing.allocator.free(active);
    }
    try std.testing.expectEqual(@as(usize, 1), active.len);
    try std.testing.expect(active[0].confidence >= 0.94);

    try deprecateEntity(db, id1);
    const deprecated = try findEntitiesByType(db, std.testing.allocator, "skill");
    defer {
        for (deprecated) |row| {
            std.testing.allocator.free(row.entity_type);
            std.testing.allocator.free(row.name);
            std.testing.allocator.free(row.metadata_json);
        }
        std.testing.allocator.free(deprecated);
    }
    try std.testing.expectEqual(@as(usize, 0), deprecated.len);
}

test "sqlite graph migration supports learning, search, and analytics helpers" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    const concepts = [_]ConceptInput{
        .{ .entity_type = "skill", .name = "zig", .confidence = 0.9, .metadata_json = "{\"domain\":\"systems\",\"description\":\"zig language\"}" },
        .{ .entity_type = "concept", .name = "compile-time", .confidence = 0.8, .metadata_json = "{\"domain\":\"systems\"}" },
        .{ .entity_type = "concept", .name = "memory-safety", .confidence = 0.85, .metadata_json = "{\"domain\":\"systems\"}" },
        .{ .entity_type = "concept", .name = "orphan", .confidence = 0.7, .metadata_json = "{\"domain\":\"systems\"}" },
    };
    const relations = [_]RelationInput{
        .{ .relation_type = "requires", .source = "zig", .target = "compile-time", .confidence = 0.9 },
        .{ .relation_type = "requires", .source = "compile-time", .target = "memory-safety", .confidence = 0.85 },
        .{ .relation_type = "associated_with", .source = "zig", .target = "orphan", .confidence = 0.95 },
    };

    const run_id = try learnFromRun(
        db,
        std.testing.allocator,
        "run-zig-1",
        "systems",
        "success",
        concepts[0..],
        relations[0..],
        "standalone learn-from-run",
        "{\"source\":\"test\"}",
    );
    try std.testing.expect(run_id > 0);

    const hits = try entityFtsSearch(db, std.testing.allocator, "zig", null, "systems", 0.0, 10);
    defer {
        for (hits) |row| {
            std.testing.allocator.free(row.name);
            std.testing.allocator.free(row.entity_type);
            std.testing.allocator.free(row.metadata_json);
        }
        std.testing.allocator.free(hits);
    }
    try std.testing.expect(hits.len > 0);
    try std.testing.expectEqualStrings("zig", hits[0].name);

    const marketplace = try marketplaceSearch(db, std.testing.allocator, "zig", "systems", 0.0, 2, 10);
    defer {
        for (marketplace) |row| {
            std.testing.allocator.free(row.name);
            std.testing.allocator.free(row.entity_type);
            std.testing.allocator.free(row.metadata_json);
        }
        std.testing.allocator.free(marketplace);
    }
    try std.testing.expect(marketplace.len > 0);
    try std.testing.expectEqualStrings("zig", marketplace[0].name);
    for (marketplace) |row| {
        try std.testing.expect(!std.mem.eql(u8, row.name, "orphan"));
    }

    const deps = try skillDependencies(db, std.testing.allocator, try loadEntityIdByName(db, "zig"), 5, 0.0);
    defer {
        for (deps) |dep| {
            std.testing.allocator.free(dep.dep_name);
            std.testing.allocator.free(dep.dep_type);
            std.testing.allocator.free(dep.relation_type);
        }
        std.testing.allocator.free(deps);
    }
    try std.testing.expectEqual(@as(usize, 2), deps.len);
    try std.testing.expectEqualStrings("compile-time", deps[0].dep_name);

    const decay = try confidenceDecay(db, try loadEntityIdByName(db, "zig"), 90);
    try std.testing.expect(decay >= 0.0);

    const neighborhood = try entityNeighborhood(db, std.testing.allocator, try loadEntityIdByName(db, "zig"), 10, 10, 0.0);
    defer std.testing.allocator.free(neighborhood);
    try std.testing.expect(std.mem.indexOf(u8, neighborhood, "\"zig\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, neighborhood, "\"outgoing\"") != null);

    const inbound_neighborhood = try entityNeighborhood(db, std.testing.allocator, try loadEntityIdByName(db, "compile-time"), 10, 10, 0.0);
    defer std.testing.allocator.free(inbound_neighborhood);
    try std.testing.expect(std.mem.indexOf(u8, inbound_neighborhood, "\"incoming\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, inbound_neighborhood, "\"zig\"") != null);
}

test "sqlite graph migration supports knowledge patch application" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    _ = try learnFromRun(
        db,
        std.testing.allocator,
        "run-patch-1",
        "systems",
        "success",
        &.{
            .{ .entity_type = "skill", .name = "zig", .confidence = 0.9, .metadata_json = "{\"domain\":\"systems\"}" },
            .{ .entity_type = "concept", .name = "compile-time", .confidence = 0.8, .metadata_json = "{\"domain\":\"systems\"}" },
        },
        &.{
            .{ .relation_type = "requires", .source = "zig", .target = "compile-time", .confidence = 0.9 },
        },
        null,
        null,
    );

    const patch_count = try applyKnowledgePatch(
        db,
        std.testing.allocator,
        "patch-1",
        "systems",
        0.8,
        &.{
            .{ .action = "deprecate", .relation_type = "requires", .source = "zig", .target = "compile-time" },
        },
        "tester",
    );
    try std.testing.expectEqual(@as(u32, 1), patch_count);

    const deps = try skillDependencies(db, std.testing.allocator, try loadEntityIdByName(db, "zig"), 5, 0.0);
    defer {
        for (deps) |dep| {
            std.testing.allocator.free(dep.dep_name);
            std.testing.allocator.free(dep.dep_type);
            std.testing.allocator.free(dep.relation_type);
        }
        std.testing.allocator.free(deps);
    }
    try std.testing.expectEqual(@as(usize, 0), deps.len);
}

test "graph parity helpers cover bulk-name and metadata lookups, deprecation, decay overload" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    _ = try learnFromRun(
        db,
        std.testing.allocator,
        "run-parity-1",
        "systems",
        "success",
        &.{
            .{ .entity_type = "skill", .name = "zig", .confidence = 0.9, .metadata_json = "{\"domain\":\"systems\",\"team\":\"core\"}" },
            .{ .entity_type = "skill", .name = "rust", .confidence = 0.85, .metadata_json = "{\"domain\":\"systems\",\"team\":\"infra\"}" },
            .{ .entity_type = "concept", .name = "memory-safety", .confidence = 0.8, .metadata_json = "{\"domain\":\"systems\",\"team\":\"core\"}" },
        },
        &.{
            .{ .relation_type = "requires", .source = "zig", .target = "memory-safety", .confidence = 0.9 },
            .{ .relation_type = "requires", .source = "rust", .target = "memory-safety", .confidence = 0.95 },
        },
        null,
        null,
    );

    const names = [_][]const u8{ "zig", "missing", "rust" };
    const by_names = try findEntitiesByNames(db, std.testing.allocator, &names);
    defer {
        for (by_names) |*row| @constCast(row).deinit(std.testing.allocator);
        std.testing.allocator.free(by_names);
    }
    try std.testing.expectEqual(@as(usize, 2), by_names.len);
    try std.testing.expectEqualStrings("zig", by_names[0].name);
    try std.testing.expectEqualStrings("rust", by_names[1].name);

    const by_metadata = try findEntitiesByMetadata(
        db,
        std.testing.allocator,
        "{\"team\":\"core\"}",
    );
    defer {
        for (by_metadata) |*row| @constCast(row).deinit(std.testing.allocator);
        std.testing.allocator.free(by_metadata);
    }
    try std.testing.expectEqual(@as(usize, 2), by_metadata.len);

    const by_decay_known = try confidenceDecayByName(db, "zig", 90);
    try std.testing.expect(by_decay_known != null);
    try std.testing.expect(by_decay_known.? >= 0.0);

    const by_decay_missing = try confidenceDecayByName(db, "no-such-entity", 90);
    try std.testing.expect(by_decay_missing == null);

    const zig_id = try loadEntityIdByName(db, "zig");
    const safety_id = try loadEntityIdByName(db, "memory-safety");
    const before = try findRelationByIds(db, std.testing.allocator, zig_id, safety_id, "requires");
    defer if (before) |row| {
        std.testing.allocator.free(row.workspace_id);
        std.testing.allocator.free(row.relation_type);
        std.testing.allocator.free(row.metadata_json);
    };
    try std.testing.expect(before != null);
    try deprecateRelation(db, before.?.relation_id);

    const after = try findRelationByIds(db, std.testing.allocator, zig_id, safety_id, "requires");
    try std.testing.expect(after == null);
}
