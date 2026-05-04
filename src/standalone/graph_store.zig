const std = @import("std");
const interfaces = @import("interfaces.zig");
const roaring = @import("roaring.zig");

pub const RelationRecord = struct {
    relation_id: u32,
    source_id: u32,
    target_id: u32,
    relation_type: []const u8,
    valid_from_unix: ?i64 = null,
    valid_to_unix: ?i64 = null,
    confidence: f32 = 1.0,
};

pub const Store = struct {
    allocator: std.mem.Allocator,
    outgoing: std.ArrayList(AdjacencyEntry),
    outgoing_index: std.AutoHashMap(u32, usize),
    incoming: std.ArrayList(AdjacencyEntry),
    incoming_index: std.AutoHashMap(u32, usize),
    relations: std.ArrayList(RelationRecord),
    relation_index: std.AutoHashMap(u32, usize),

    pub fn init(allocator: std.mem.Allocator) Store {
        return .{
            .allocator = allocator,
            .outgoing = .empty,
            .outgoing_index = std.AutoHashMap(u32, usize).init(allocator),
            .incoming = .empty,
            .incoming_index = std.AutoHashMap(u32, usize).init(allocator),
            .relations = .empty,
            .relation_index = std.AutoHashMap(u32, usize).init(allocator),
        };
    }

    pub fn deinit(self: *Store) void {
        for (self.outgoing.items) |*entry| {
            entry.bitmap.deinit();
            self.allocator.free(entry.relation_ids);
        }
        self.outgoing.deinit(self.allocator);
        self.outgoing_index.deinit();

        for (self.incoming.items) |*entry| {
            entry.bitmap.deinit();
            self.allocator.free(entry.relation_ids);
        }
        self.incoming.deinit(self.allocator);
        self.incoming_index.deinit();

        for (self.relations.items) |relation| self.allocator.free(relation.relation_type);
        self.relations.deinit(self.allocator);
        self.relation_index.deinit();
        self.* = undefined;
    }

    pub fn asRepository(self: *Store) interfaces.GraphRepository {
        return .{
            .ctx = @ptrCast(self),
            .getOutgoingEdgesFn = getOutgoingEdges,
            .getIncomingEdgesFn = getIncomingEdges,
            .filterEdgesFn = filterEdges,
            .projectNeighborNodesFn = projectNeighborNodes,
            .expandNeighborStepsFn = expandNeighborSteps,
            .expandFrontierStepsFn = expandFrontierSteps,
        };
    }

    pub fn addRelation(self: *Store, relation: RelationRecord) !void {
        try self.relations.append(self.allocator, .{
            .relation_id = relation.relation_id,
            .source_id = relation.source_id,
            .target_id = relation.target_id,
            .relation_type = try self.allocator.dupe(u8, relation.relation_type),
            .valid_from_unix = relation.valid_from_unix,
            .valid_to_unix = relation.valid_to_unix,
            .confidence = relation.confidence,
        });
        try self.relation_index.put(relation.relation_id, self.relations.items.len - 1);
    }

    pub fn setOutgoing(self: *Store, entity_id: u32, relation_ids: []const u32) !void {
        try upsertAdjacency(self.allocator, &self.outgoing, &self.outgoing_index, entity_id, relation_ids);
    }

    pub fn setIncoming(self: *Store, entity_id: u32, relation_ids: []const u32) !void {
        try upsertAdjacency(self.allocator, &self.incoming, &self.incoming_index, entity_id, relation_ids);
    }

    pub fn shortestPathHopsFast(
        self: *const Store,
        allocator: std.mem.Allocator,
        source_id: u32,
        target_id: u32,
        filter: interfaces.GraphEdgeFilter,
        max_depth: usize,
    ) !?usize {
        if (source_id == target_id) return 0;

        var visited = std.AutoHashMap(u32, void).init(allocator);
        defer visited.deinit();
        try visited.put(source_id, {});

        var frontier = std.ArrayList(u32).empty;
        defer frontier.deinit(allocator);
        try frontier.append(allocator, source_id);

        var next_frontier = std.ArrayList(u32).empty;
        defer next_frontier.deinit(allocator);

        var depth: usize = 0;
        while (depth < max_depth and frontier.items.len != 0) : (depth += 1) {
            next_frontier.clearRetainingCapacity();

            for (frontier.items) |node_id| {
                if (self.outgoing_index.get(node_id)) |entry_index| {
                    const found = try appendNeighborsForAdjacency(
                        self,
                        allocator,
                        self.outgoing.items[entry_index].relation_ids,
                        true,
                        target_id,
                        filter,
                        &visited,
                        &next_frontier,
                    );
                    if (found) return depth + 1;
                }

                if (self.incoming_index.get(node_id)) |entry_index| {
                    const found = try appendNeighborsForAdjacency(
                        self,
                        allocator,
                        self.incoming.items[entry_index].relation_ids,
                        false,
                        target_id,
                        filter,
                        &visited,
                        &next_frontier,
                    );
                    if (found) return depth + 1;
                }
            }

            std.mem.swap(std.ArrayList(u32), &frontier, &next_frontier);
        }

        return null;
    }

    pub fn shortestPathFast(
        self: *const Store,
        allocator: std.mem.Allocator,
        source_id: u32,
        target_id: u32,
        filter: interfaces.GraphEdgeFilter,
        max_depth: usize,
    ) !?[]PathEdge {
        if (source_id == target_id) return try allocator.dupe(PathEdge, &.{});

        var visited = std.AutoHashMap(u32, void).init(allocator);
        defer visited.deinit();
        try visited.put(source_id, {});

        var frontier = std.ArrayList(u32).empty;
        defer frontier.deinit(allocator);
        try frontier.append(allocator, source_id);

        var next_frontier = std.ArrayList(u32).empty;
        defer next_frontier.deinit(allocator);

        var predecessors = std.AutoHashMap(u32, PathEdge).init(allocator);
        defer predecessors.deinit();

        var depth: usize = 0;
        while (depth < max_depth and frontier.items.len != 0) : (depth += 1) {
            next_frontier.clearRetainingCapacity();

            for (frontier.items) |node_id| {
                if (self.outgoing_index.get(node_id)) |entry_index| {
                    const found = try appendPathNeighborsForAdjacency(
                        self,
                        allocator,
                        self.outgoing.items[entry_index].relation_ids,
                        true,
                        node_id,
                        target_id,
                        filter,
                        &visited,
                        &predecessors,
                        &next_frontier,
                    );
                    if (found) return try buildPath(allocator, predecessors, source_id, target_id);
                }

                if (self.incoming_index.get(node_id)) |entry_index| {
                    const found = try appendPathNeighborsForAdjacency(
                        self,
                        allocator,
                        self.incoming.items[entry_index].relation_ids,
                        false,
                        node_id,
                        target_id,
                        filter,
                        &visited,
                        &predecessors,
                        &next_frontier,
                    );
                    if (found) return try buildPath(allocator, predecessors, source_id, target_id);
                }
            }

            std.mem.swap(std.ArrayList(u32), &frontier, &next_frontier);
        }

        return null;
    }

    pub fn kHopsFast(
        self: *const Store,
        allocator: std.mem.Allocator,
        seed_nodes: []const u32,
        max_hops: usize,
        filter: interfaces.GraphEdgeFilter,
    ) !roaring.Bitmap {
        var visited = try roaring.Bitmap.empty();
        errdefer visited.deinit();

        var visited_set = std.AutoHashMap(u32, void).init(allocator);
        defer visited_set.deinit();

        var frontier = std.ArrayList(u32).empty;
        defer frontier.deinit(allocator);
        for (seed_nodes) |node_id| {
            if ((try visited_set.getOrPut(node_id)).found_existing) continue;
            visited.add(node_id);
            try frontier.append(allocator, node_id);
        }

        var next_frontier = std.ArrayList(u32).empty;
        defer next_frontier.deinit(allocator);

        var depth: usize = 0;
        while (depth < max_hops and frontier.items.len != 0) : (depth += 1) {
            next_frontier.clearRetainingCapacity();

            for (frontier.items) |node_id| {
                if (self.outgoing_index.get(node_id)) |entry_index| {
                    try appendReachableNeighborsForAdjacency(
                        self,
                        allocator,
                        self.outgoing.items[entry_index].relation_ids,
                        true,
                        filter,
                        &visited_set,
                        &next_frontier,
                        &visited,
                    );
                }
                if (self.incoming_index.get(node_id)) |entry_index| {
                    try appendReachableNeighborsForAdjacency(
                        self,
                        allocator,
                        self.incoming.items[entry_index].relation_ids,
                        false,
                        filter,
                        &visited_set,
                        &next_frontier,
                        &visited,
                    );
                }
            }

            std.mem.swap(std.ArrayList(u32), &frontier, &next_frontier);
        }

        return visited;
    }
};

const AdjacencyEntry = struct {
    entity_id: u32,
    relation_ids: []u32,
    bitmap: roaring.Bitmap,
};

fn upsertAdjacency(
    allocator: std.mem.Allocator,
    entries: *std.ArrayList(AdjacencyEntry),
    index: *std.AutoHashMap(u32, usize),
    entity_id: u32,
    relation_ids: []const u32,
) !void {
    if (index.get(entity_id)) |entry_index| {
        allocator.free(entries.items[entry_index].relation_ids);
        entries.items[entry_index].bitmap.deinit();
        entries.items[entry_index].relation_ids = try allocator.dupe(u32, relation_ids);
        entries.items[entry_index].bitmap = try roaring.Bitmap.fromSlice(relation_ids);
        return;
    }

    try entries.append(allocator, .{
        .entity_id = entity_id,
        .relation_ids = try allocator.dupe(u32, relation_ids),
        .bitmap = try roaring.Bitmap.fromSlice(relation_ids),
    });
    try index.put(entity_id, entries.items.len - 1);
}

fn getOutgoingEdges(ctx: *anyopaque, allocator: std.mem.Allocator, nodes: roaring.Bitmap) anyerror!roaring.Bitmap {
    const self: *Store = @ptrCast(@alignCast(ctx));
    return try unionAdjacency(allocator, self.outgoing.items, self.outgoing_index, nodes);
}

fn getIncomingEdges(ctx: *anyopaque, allocator: std.mem.Allocator, nodes: roaring.Bitmap) anyerror!roaring.Bitmap {
    const self: *Store = @ptrCast(@alignCast(ctx));
    return try unionAdjacency(allocator, self.incoming.items, self.incoming_index, nodes);
}

fn unionAdjacency(
    allocator: std.mem.Allocator,
    entries: []const AdjacencyEntry,
    index: std.AutoHashMap(u32, usize),
    nodes: roaring.Bitmap,
) !roaring.Bitmap {
    const node_ids = try nodes.toArray(allocator);
    defer allocator.free(node_ids);

    var result = try roaring.Bitmap.empty();
    errdefer result.deinit();

    for (node_ids) |node_id| {
        const entry_index = index.get(node_id) orelse continue;
        result.orInPlace(entries[entry_index].bitmap);
    }
    return result;
}

fn filterEdges(
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    edges: roaring.Bitmap,
    filter: interfaces.GraphEdgeFilter,
) anyerror!roaring.Bitmap {
    const self: *Store = @ptrCast(@alignCast(ctx));
    const edge_ids = try edges.toArray(allocator);
    defer allocator.free(edge_ids);

    var result = try roaring.Bitmap.empty();
    errdefer result.deinit();

    for (edge_ids) |edge_id| {
        const relation = findRelation(self, edge_id) orelse continue;
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
    const self: *Store = @ptrCast(@alignCast(ctx));
    const edge_ids = try edges.toArray(allocator);
    defer allocator.free(edge_ids);

    var result = try roaring.Bitmap.empty();
    errdefer result.deinit();

    for (edge_ids) |edge_id| {
        const relation = findRelation(self, edge_id) orelse continue;
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
    const self: *Store = @ptrCast(@alignCast(ctx));
    const edge_ids = try edges.toArray(allocator);
    defer allocator.free(edge_ids);

    var steps = std.ArrayList(interfaces.GraphNeighborStep).empty;
    defer steps.deinit(allocator);

    for (edge_ids) |edge_id| {
        const relation = findRelation(self, edge_id) orelse continue;
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
    const self: *Store = @ptrCast(@alignCast(ctx));
    const frontier_ids = try frontier.toArray(allocator);
    defer allocator.free(frontier_ids);

    var steps = std.ArrayList(interfaces.GraphNeighborStep).empty;
    defer steps.deinit(allocator);

    for (frontier_ids) |node_id| {
        if (self.outgoing_index.get(node_id)) |entry_index| {
            const edge_ids = try self.outgoing.items[entry_index].bitmap.toArray(allocator);
            defer allocator.free(edge_ids);
            try appendStepsForEdges(self, allocator, &steps, edge_ids, filter, true);
        }
        if (self.incoming_index.get(node_id)) |entry_index| {
            const edge_ids = try self.incoming.items[entry_index].bitmap.toArray(allocator);
            defer allocator.free(edge_ids);
            try appendStepsForEdges(self, allocator, &steps, edge_ids, filter, false);
        }
    }

    return steps.toOwnedSlice(allocator);
}

fn appendStepsForEdges(
    self: *Store,
    allocator: std.mem.Allocator,
    steps: *std.ArrayList(interfaces.GraphNeighborStep),
    edge_ids: []const u32,
    filter: interfaces.GraphEdgeFilter,
    outgoing: bool,
) !void {
    for (edge_ids) |edge_id| {
        const relation = findRelation(self, edge_id) orelse continue;
        if (!passesFilter(relation, filter)) continue;

        try steps.append(allocator, .{
            .from_node = if (outgoing) relation.source_id else relation.target_id,
            .to_node = if (outgoing) relation.target_id else relation.source_id,
            .relation_id = relation.relation_id,
        });
    }
}

fn appendNeighborsForAdjacency(
    self: *const Store,
    allocator: std.mem.Allocator,
    relation_ids: []const u32,
    outgoing: bool,
    target_id: u32,
    filter: interfaces.GraphEdgeFilter,
    visited: *std.AutoHashMap(u32, void),
    next_frontier: *std.ArrayList(u32),
) !bool {
    for (relation_ids) |relation_id| {
        const relation = findRelationConst(self, relation_id) orelse continue;
        if (!passesFilter(relation, filter)) continue;

        const neighbor = if (outgoing) relation.target_id else relation.source_id;
        if (neighbor == target_id) return true;

        const gop = try visited.getOrPut(neighbor);
        if (!gop.found_existing) {
            try next_frontier.append(allocator, neighbor);
        }
    }

    return false;
}

fn appendPathNeighborsForAdjacency(
    self: *const Store,
    allocator: std.mem.Allocator,
    relation_ids: []const u32,
    outgoing: bool,
    from_node: u32,
    target_id: u32,
    filter: interfaces.GraphEdgeFilter,
    visited: *std.AutoHashMap(u32, void),
    predecessors: *std.AutoHashMap(u32, PathEdge),
    next_frontier: *std.ArrayList(u32),
) !bool {
    for (relation_ids) |relation_id| {
        const relation = findRelationConst(self, relation_id) orelse continue;
        if (!passesFilter(relation, filter)) continue;

        const neighbor = if (outgoing) relation.target_id else relation.source_id;
        const gop = try visited.getOrPut(neighbor);
        if (gop.found_existing) continue;

        try predecessors.put(neighbor, .{
            .from_node = from_node,
            .to_node = neighbor,
            .relation_id = relation.relation_id,
        });
        try next_frontier.append(allocator, neighbor);

        if (neighbor == target_id) return true;
    }

    return false;
}

fn appendReachableNeighborsForAdjacency(
    self: *const Store,
    allocator: std.mem.Allocator,
    relation_ids: []const u32,
    outgoing: bool,
    filter: interfaces.GraphEdgeFilter,
    visited: *std.AutoHashMap(u32, void),
    next_frontier: *std.ArrayList(u32),
    reachable: *roaring.Bitmap,
) !void {
    for (relation_ids) |relation_id| {
        const relation = findRelationConst(self, relation_id) orelse continue;
        if (!passesFilter(relation, filter)) continue;

        const neighbor = if (outgoing) relation.target_id else relation.source_id;
        const gop = try visited.getOrPut(neighbor);
        if (gop.found_existing) continue;

        reachable.add(neighbor);
        try next_frontier.append(allocator, neighbor);
    }
}

fn findRelation(self: *Store, relation_id: u32) ?*const RelationRecord {
    const index = self.relation_index.get(relation_id) orelse return null;
    return &self.relations.items[index];
}

fn findRelationConst(self: *const Store, relation_id: u32) ?*const RelationRecord {
    const index = self.relation_index.get(relation_id) orelse return null;
    return &self.relations.items[index];
}

fn passesFilter(relation: *const RelationRecord, filter: interfaces.GraphEdgeFilter) bool {
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
    if (filter.after_unix_seconds) |after| {
        if (relation.valid_to_unix) |valid_to| if (valid_to < after) return false;
    }
    if (filter.before_unix_seconds) |before| {
        if (relation.valid_from_unix) |valid_from| if (valid_from > before) return false;
    }
    return true;
}

pub fn kHops(
    allocator: std.mem.Allocator,
    repository: interfaces.GraphRepository,
    seed_nodes: roaring.Bitmap,
    max_hops: usize,
    filter: interfaces.GraphEdgeFilter,
) !roaring.Bitmap {
    var frontier = try seed_nodes.clone();
    defer frontier.deinit();

    var visited = try seed_nodes.clone();
    errdefer visited.deinit();

    var depth: usize = 0;
    while (depth < max_hops and !frontier.isEmpty()) : (depth += 1) {
        const steps = try repository.expandFrontierStepsFn(repository.ctx, allocator, frontier, filter);
        defer allocator.free(steps);

        var neighbors = try roaring.Bitmap.empty();
        defer neighbors.deinit();
        for (steps) |step| neighbors.add(@intCast(step.to_node));

        var next_frontier = try neighbors.andNotNew(visited);
        defer next_frontier.deinit();

        if (next_frontier.isEmpty()) break;
        visited.orInPlace(next_frontier);

        frontier.deinit();
        frontier = try next_frontier.clone();
    }

    return visited;
}

pub fn shortestPathHops(
    allocator: std.mem.Allocator,
    repository: interfaces.GraphRepository,
    source_id: u32,
    target_id: u32,
    filter: interfaces.GraphEdgeFilter,
    max_depth: usize,
) !?usize {
    if (source_id == target_id) return 0;

    var frontier = try roaring.Bitmap.fromSlice(&.{source_id});
    defer frontier.deinit();

    var visited = try roaring.Bitmap.fromSlice(&.{source_id});
    defer visited.deinit();

    var depth: usize = 0;
    while (depth < max_depth and !frontier.isEmpty()) : (depth += 1) {
        const steps = try repository.expandFrontierStepsFn(repository.ctx, allocator, frontier, filter);
        defer allocator.free(steps);

        var neighbors = try roaring.Bitmap.empty();
        defer neighbors.deinit();
        for (steps) |step| neighbors.add(@intCast(step.to_node));

        if (neighbors.contains(target_id)) return depth + 1;

        var next_frontier = try neighbors.andNotNew(visited);
        defer next_frontier.deinit();
        if (next_frontier.isEmpty()) return null;

        visited.orInPlace(next_frontier);
        frontier.deinit();
        frontier = try next_frontier.clone();
    }

    return null;
}

pub const PathEdge = struct {
    from_node: u32,
    to_node: u32,
    relation_id: u32,
};

pub fn shortestPath(
    allocator: std.mem.Allocator,
    repository: interfaces.GraphRepository,
    source_id: u32,
    target_id: u32,
    filter: interfaces.GraphEdgeFilter,
    max_depth: usize,
) !?[]PathEdge {
    if (source_id == target_id) return try allocator.dupe(PathEdge, &.{});

    var frontier = try roaring.Bitmap.fromSlice(&.{source_id});
    defer frontier.deinit();

    var visited = try roaring.Bitmap.fromSlice(&.{source_id});
    defer visited.deinit();

    var predecessors = std.AutoHashMap(u32, PathEdge).init(allocator);
    defer predecessors.deinit();

    var depth: usize = 0;
    while (depth < max_depth and !frontier.isEmpty()) : (depth += 1) {
        const steps = try repository.expandFrontierStepsFn(repository.ctx, allocator, frontier, filter);
        defer allocator.free(steps);

        var next_frontier = try roaring.Bitmap.empty();
        defer next_frontier.deinit();

        for (steps) |step| {
            if (visited.contains(@intCast(step.to_node))) continue;
            if (!predecessors.contains(@intCast(step.to_node))) {
                try predecessors.put(@intCast(step.to_node), .{
                    .from_node = @intCast(step.from_node),
                    .to_node = @intCast(step.to_node),
                    .relation_id = step.relation_id,
                });
            }
            next_frontier.add(@intCast(step.to_node));
        }

        if (predecessors.contains(target_id)) {
            return try buildPath(allocator, predecessors, source_id, target_id);
        }
        if (next_frontier.isEmpty()) return null;

        visited.orInPlace(next_frontier);
        frontier.deinit();
        frontier = try next_frontier.clone();
    }

    return null;
}

fn buildPath(
    allocator: std.mem.Allocator,
    predecessors: std.AutoHashMap(u32, PathEdge),
    source_id: u32,
    target_id: u32,
) ![]PathEdge {
    var reversed = std.ArrayList(PathEdge).empty;
    defer reversed.deinit(allocator);

    var current = target_id;
    while (current != source_id) {
        const edge = predecessors.get(current) orelse return error.PathNotFound;
        try reversed.append(allocator, edge);
        current = edge.from_node;
    }

    const path = try allocator.alloc(PathEdge, reversed.items.len);
    for (reversed.items, 0..) |edge, index| {
        path[reversed.items.len - 1 - index] = edge;
    }
    return path;
}

test "graph store performs k-hop traversal with edge type filtering" {
    var store = Store.init(std.testing.allocator);
    defer store.deinit();

    try store.addRelation(.{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "works_for", .confidence = 0.9 });
    try store.addRelation(.{ .relation_id = 11, .source_id = 2, .target_id = 3, .relation_type = "published", .confidence = 0.8 });
    try store.addRelation(.{ .relation_id = 12, .source_id = 2, .target_id = 4, .relation_type = "works_for", .confidence = 0.95 });

    try store.setOutgoing(1, &.{10});
    try store.setOutgoing(2, &.{ 11, 12 });
    try store.setIncoming(2, &.{10});
    try store.setIncoming(3, &.{11});
    try store.setIncoming(4, &.{12});

    var seed = try roaring.Bitmap.fromSlice(&.{1});
    defer seed.deinit();

    var result = try kHops(std.testing.allocator, store.asRepository(), seed, 2, .{
        .edge_types = &.{"works_for"},
    });
    defer result.deinit();

    const ids = try result.toArray(std.testing.allocator);
    defer std.testing.allocator.free(ids);

    try std.testing.expectEqualSlices(u32, &.{ 1, 2, 4 }, ids);

    var fast_result = try store.kHopsFast(
        std.testing.allocator,
        &.{1},
        2,
        .{ .edge_types = &.{"works_for"} },
    );
    defer fast_result.deinit();

    const fast_ids = try fast_result.toArray(std.testing.allocator);
    defer std.testing.allocator.free(fast_ids);
    try std.testing.expectEqualSlices(u32, &.{ 1, 2, 4 }, fast_ids);
}

test "graph store computes shortest path hop count with filters" {
    var store = Store.init(std.testing.allocator);
    defer store.deinit();

    try store.addRelation(.{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "works_for", .confidence = 0.9 });
    try store.addRelation(.{ .relation_id = 11, .source_id = 2, .target_id = 3, .relation_type = "published", .confidence = 0.8 });
    try store.addRelation(.{ .relation_id = 12, .source_id = 2, .target_id = 4, .relation_type = "works_for", .confidence = 0.95 });

    try store.setOutgoing(1, &.{10});
    try store.setOutgoing(2, &.{ 11, 12 });
    try store.setIncoming(2, &.{10});
    try store.setIncoming(3, &.{11});
    try store.setIncoming(4, &.{12});

    const works_for_hops = try shortestPathHops(
        std.testing.allocator,
        store.asRepository(),
        1,
        4,
        .{ .edge_types = &.{"works_for"} },
        4,
    );
    try std.testing.expectEqual(@as(?usize, 2), works_for_hops);

    const published_hops = try shortestPathHops(
        std.testing.allocator,
        store.asRepository(),
        1,
        3,
        .{ .edge_types = &.{"works_for"} },
        4,
    );
    try std.testing.expectEqual(@as(?usize, null), published_hops);

    const fast_works_for_hops = try store.shortestPathHopsFast(
        std.testing.allocator,
        1,
        4,
        .{ .edge_types = &.{"works_for"} },
        4,
    );
    try std.testing.expectEqual(@as(?usize, 2), fast_works_for_hops);
}

test "graph store reconstructs shortest path edges" {
    var store = Store.init(std.testing.allocator);
    defer store.deinit();

    try store.addRelation(.{ .relation_id = 10, .source_id = 1, .target_id = 2, .relation_type = "works_for", .confidence = 0.9 });
    try store.addRelation(.{ .relation_id = 11, .source_id = 2, .target_id = 3, .relation_type = "published", .confidence = 0.8 });
    try store.addRelation(.{ .relation_id = 12, .source_id = 3, .target_id = 4, .relation_type = "published", .confidence = 0.9 });

    try store.setOutgoing(1, &.{10});
    try store.setOutgoing(2, &.{11});
    try store.setOutgoing(3, &.{12});
    try store.setIncoming(2, &.{10});
    try store.setIncoming(3, &.{11});
    try store.setIncoming(4, &.{12});

    const path = (try shortestPath(
        std.testing.allocator,
        store.asRepository(),
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

    const fast_path = (try store.shortestPathFast(
        std.testing.allocator,
        1,
        4,
        .{},
        4,
    )).?;
    defer std.testing.allocator.free(fast_path);

    try std.testing.expectEqual(@as(usize, 3), fast_path.len);
    try std.testing.expectEqual(@as(u32, 10), fast_path[0].relation_id);
    try std.testing.expectEqual(@as(u32, 11), fast_path[1].relation_id);
    try std.testing.expectEqual(@as(u32, 12), fast_path[2].relation_id);
}
