const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const helper_api = @import("helper_api.zig");

const c = facet_sqlite.c;

pub const Error = error{
    MissingWorkspace,
    MissingMatch,
    MissingProject,
    InvalidLimit,
    InvalidMatch,
    InvalidPredicate,
    UnsupportedQuery,
    UnsupportedProject,
    StepFailed,
};

pub const Query = struct {
    workspace_id: []const u8,
    match: Match,
    predicates: []Predicate,
    project: Project,
    limit: usize,
    hops: ?HopRange = null,

    pub fn deinit(self: Query, allocator: std.mem.Allocator) void {
        allocator.free(self.workspace_id);
        self.match.deinit(allocator);
        for (self.predicates) |predicate| predicate.deinit(allocator);
        allocator.free(self.predicates);
        self.project.deinit(allocator);
    }
};

pub const Match = union(enum) {
    node: NodePattern,
    edge: EdgePattern,

    fn deinit(self: Match, allocator: std.mem.Allocator) void {
        switch (self) {
            .node => |node| node.deinit(allocator),
            .edge => |edge| edge.deinit(allocator),
        }
    }
};

pub const NodePattern = struct {
    variable: []const u8,
    entity_type: []const u8,
    inline_name: ?[]const u8 = null,

    fn deinit(self: NodePattern, allocator: std.mem.Allocator) void {
        allocator.free(self.variable);
        allocator.free(self.entity_type);
        if (self.inline_name) |value| allocator.free(value);
    }
};

pub const EdgePattern = struct {
    source: NodePattern,
    relation_variable: []const u8,
    relation_type: []const u8,
    target: NodePattern,

    fn deinit(self: EdgePattern, allocator: std.mem.Allocator) void {
        self.source.deinit(allocator);
        allocator.free(self.relation_variable);
        allocator.free(self.relation_type);
        self.target.deinit(allocator);
    }
};

pub const HopRange = struct {
    min: usize,
    max: usize,
};

pub const Predicate = struct {
    path: []const u8,
    op: Op,
    value: Value,

    pub const Op = enum {
        eq,
        gt,
        gte,
        lt,
        lte,
        in,
    };

    pub const Value = union(enum) {
        text: []const u8,
        number: f64,
        list_text: [][]const u8,

        fn deinit(self: Value, allocator: std.mem.Allocator) void {
            switch (self) {
                .text => |value| allocator.free(value),
                .number => {},
                .list_text => |values| {
                    for (values) |value| allocator.free(value);
                    allocator.free(values);
                },
            }
        }
    };

    fn deinit(self: Predicate, allocator: std.mem.Allocator) void {
        allocator.free(self.path);
        self.value.deinit(allocator);
    }
};

pub const Project = union(enum) {
    fields: [][]const u8,
    bundle_projection_get,

    fn deinit(self: Project, allocator: std.mem.Allocator) void {
        switch (self) {
            .fields => |fields| {
                for (fields) |field| allocator.free(field);
                allocator.free(fields);
            },
            .bundle_projection_get => {},
        }
    }
};

pub fn parse(allocator: std.mem.Allocator, text: []const u8) !Query {
    var workspace_id: ?[]const u8 = null;
    errdefer if (workspace_id) |value| allocator.free(value);

    var match_pattern: ?Match = null;
    errdefer if (match_pattern) |value| value.deinit(allocator);

    var predicates = std.ArrayList(Predicate).empty;
    errdefer {
        for (predicates.items) |predicate| predicate.deinit(allocator);
        predicates.deinit(allocator);
    }

    var project: ?Project = null;
    errdefer if (project) |value| value.deinit(allocator);

    var limit: ?usize = null;
    var hops: ?HopRange = null;

    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |raw_line| {
        var line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0) continue;

        if (std.mem.startsWith(u8, line, "WORKSPACE ")) {
            if (workspace_id) |old| allocator.free(old);
            workspace_id = try allocator.dupe(u8, std.mem.trim(u8, line["WORKSPACE ".len..], " \t"));
        } else if (std.mem.startsWith(u8, line, "MATCH ")) {
            if (match_pattern) |old| old.deinit(allocator);
            match_pattern = try parseMatch(allocator, std.mem.trim(u8, line["MATCH ".len..], " \t"));
        } else if (std.mem.startsWith(u8, line, "WHERE ")) {
            line = std.mem.trim(u8, line["WHERE ".len..], " \t");
            try appendPredicateLine(allocator, &predicates, line);
        } else if (std.mem.startsWith(u8, line, "AND ")) {
            line = std.mem.trim(u8, line["AND ".len..], " \t");
            try appendPredicateLine(allocator, &predicates, line);
        } else if (std.mem.startsWith(u8, line, "PROJECT BUNDLE ")) {
            const bundle_name = std.mem.trim(u8, line["PROJECT BUNDLE ".len..], " \t");
            if (!std.mem.eql(u8, bundle_name, "projection_get")) return Error.UnsupportedProject;
            if (project) |old| old.deinit(allocator);
            project = .bundle_projection_get;
        } else if (std.mem.startsWith(u8, line, "PROJECT ")) {
            if (project) |old| old.deinit(allocator);
            project = .{ .fields = try parseProjectFields(allocator, line["PROJECT ".len..]) };
        } else if (std.mem.startsWith(u8, line, "LIMIT ")) {
            limit = std.fmt.parseInt(usize, std.mem.trim(u8, line["LIMIT ".len..], " \t"), 10) catch return Error.InvalidLimit;
            if (limit.? == 0 or limit.? > 1000) return Error.InvalidLimit;
        } else if (std.mem.startsWith(u8, line, "HOPS ")) {
            hops = try parseHops(std.mem.trim(u8, line["HOPS ".len..], " \t"));
        }
    }

    return .{
        .workspace_id = workspace_id orelse return Error.MissingWorkspace,
        .match = match_pattern orelse return Error.MissingMatch,
        .predicates = try predicates.toOwnedSlice(allocator),
        .project = project orelse return Error.MissingProject,
        .limit = limit orelse 100,
        .hops = hops,
    };
}

/// Serialize a parsed GPQ `Query` to the canonical jsonb AST consumed by `graph.pattern_query_ast`.
pub fn queryToJsonAst(allocator: std.mem.Allocator, query: Query) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    try out.writer.writeAll("{\"workspace_id\":");
    try writeJsonString(&out.writer, query.workspace_id);
    try out.writer.writeAll(",\"match\":");
    try writeMatchJson(&out.writer, query.match);
    try out.writer.writeAll(",\"where\":");
    try writePredicatesJson(&out.writer, query.predicates);
    try out.writer.writeAll(",\"project\":");
    try writeProjectJson(&out.writer, query.project);
    try out.writer.print(",\"limit\":{d}", .{query.limit});
    if (query.hops) |hops| {
        try out.writer.print(",\"hops\":{{\"min\":{d},\"max\":{d}}}", .{ hops.min, hops.max });
    }
    try out.writer.writeAll("}");

    return out.toOwnedSlice();
}

/// Parse GPQ text and return the canonical Postgres jsonb AST string.
pub fn parseToJsonAst(allocator: std.mem.Allocator, text: []const u8) ![]u8 {
    var query = try parse(allocator, text);
    defer query.deinit(allocator);
    try validateQuery(query);
    return queryToJsonAst(allocator, query);
}

fn writeJsonString(writer: anytype, text: []const u8) !void {
    try writer.writeByte('"');
    for (text) |ch| {
        switch (ch) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => {
                if (ch < 0x20) {
                    try writer.print("\\u{x:0>4}", .{ch});
                } else {
                    try writer.writeByte(ch);
                }
            },
        }
    }
    try writer.writeByte('"');
}

fn writeNodePatternJson(writer: anytype, node: NodePattern) !void {
    try writer.writeAll("{\"kind\":\"node\",\"var\":");
    try writeJsonString(writer, node.variable);
    try writer.writeAll(",\"type\":");
    try writeJsonString(writer, node.entity_type);
    if (node.inline_name) |name| {
        try writer.writeAll(",\"name\":");
        try writeJsonString(writer, name);
    }
    try writer.writeByte('}');
}

fn writeMatchJson(writer: anytype, match: Match) !void {
    switch (match) {
        .node => |node| {
            try writer.writeByte('[');
            try writeNodePatternJson(writer, node);
            try writer.writeByte(']');
        },
        .edge => |edge| {
            try writer.writeByte('[');
            try writeNodePatternJson(writer, edge.source);
            try writer.writeAll(",{\"kind\":\"edge\",\"var\":");
            try writeJsonString(writer, edge.relation_variable);
            try writer.writeAll(",\"type\":");
            try writeJsonString(writer, edge.relation_type);
            try writer.writeAll(",\"direction\":\"out\"},");
            try writeNodePatternJson(writer, edge.target);
            try writer.writeByte(']');
        },
    }
}

fn predicateOpToken(op: Predicate.Op) []const u8 {
    return switch (op) {
        .eq => "=",
        .gt => ">",
        .gte => ">=",
        .lt => "<",
        .lte => "<=",
        .in => "IN",
    };
}

fn numberIsInteger(value: f64) bool {
    const rounded = @round(value);
    if (@abs(value - rounded) > 1e-9) return false;
    const min_f = @as(f64, @floatFromInt(std.math.minInt(i64)));
    const max_f = @as(f64, @floatFromInt(std.math.maxInt(i64)));
    return rounded >= min_f and rounded <= max_f;
}

fn writePredicateValueJson(writer: anytype, value: Predicate.Value) !void {
    switch (value) {
        .text => |text| {
            try writer.writeAll(",\"value\":");
            try writeJsonString(writer, text);
        },
        .number => |number| {
            if (numberIsInteger(number)) {
                try writer.print(",\"value_integer\":{d}", .{@as(i64, @intFromFloat(@round(number)))});
            } else {
                try writer.print(",\"value_number\":{d}", .{number});
            }
        },
        .list_text => |values| {
            try writer.writeAll(",\"value_list\":");
            try writer.writeByte('[');
            for (values, 0..) |list_value, index| {
                if (index > 0) try writer.writeByte(',');
                try writeJsonString(writer, list_value);
            }
            try writer.writeByte(']');
        },
    }
}

fn writePredicatesJson(writer: anytype, predicates: []const Predicate) !void {
    try writer.writeByte('[');
    for (predicates, 0..) |predicate, index| {
        if (index > 0) try writer.writeByte(',');
        try writer.writeAll("{\"path\":");
        try writeJsonString(writer, predicate.path);
        try writer.writeAll(",\"op\":");
        try writeJsonString(writer, predicateOpToken(predicate.op));
        try writePredicateValueJson(writer, predicate.value);
        try writer.writeByte('}');
    }
    try writer.writeByte(']');
}

fn writeProjectJson(writer: anytype, project: Project) !void {
    switch (project) {
        .bundle_projection_get => try writer.writeAll("{\"bundle\":\"projection_get\"}"),
        .fields => |fields| {
            try writer.writeByte('[');
            for (fields, 0..) |field, index| {
                if (index > 0) try writer.writeByte(',');
                try writeJsonString(writer, field);
            }
            try writer.writeByte(']');
        },
    }
}

fn appendPredicateLine(allocator: std.mem.Allocator, predicates: *std.ArrayList(Predicate), line: []const u8) !void {
    var rest = line;
    while (true) {
        if (std.mem.indexOf(u8, rest, " AND ")) |idx| {
            try predicates.append(allocator, try parsePredicate(allocator, std.mem.trim(u8, rest[0..idx], " \t")));
            rest = rest[idx + " AND ".len ..];
            continue;
        }
        try predicates.append(allocator, try parsePredicate(allocator, std.mem.trim(u8, rest, " \t")));
        break;
    }
}

fn parseHops(text: []const u8) !HopRange {
    if (std.mem.indexOf(u8, text, "..")) |idx| {
        const min = std.fmt.parseInt(usize, std.mem.trim(u8, text[0..idx], " \t"), 10) catch return Error.InvalidPredicate;
        const max = std.fmt.parseInt(usize, std.mem.trim(u8, text[idx + 2 ..], " \t"), 10) catch return Error.InvalidPredicate;
        if (min == 0 or max < min) return Error.InvalidPredicate;
        return .{ .min = min, .max = max };
    }
    const exact = std.fmt.parseInt(usize, text, 10) catch return Error.InvalidPredicate;
    if (exact == 0) return Error.InvalidPredicate;
    return .{ .min = exact, .max = exact };
}

fn parseProjectFields(allocator: std.mem.Allocator, text: []const u8) ![][]const u8 {
    var fields = std.ArrayList([]const u8).empty;
    errdefer {
        for (fields.items) |field| allocator.free(field);
        fields.deinit(allocator);
    }
    var parts = std.mem.splitScalar(u8, text, ',');
    while (parts.next()) |part| {
        const field = std.mem.trim(u8, part, " \t");
        if (field.len == 0) return Error.UnsupportedProject;
        try fields.append(allocator, try allocator.dupe(u8, field));
    }
    return fields.toOwnedSlice(allocator);
}

fn parseMatch(allocator: std.mem.Allocator, text: []const u8) !Match {
    if (std.mem.indexOf(u8, text, "]->")) |_| {
        const close_source = std.mem.indexOf(u8, text, ")") orelse return Error.InvalidMatch;
        const source = try parseNodePattern(allocator, text[0 .. close_source + 1]);
        errdefer source.deinit(allocator);

        const edge_start = close_source + 1;
        if (!std.mem.startsWith(u8, text[edge_start..], "-[")) return Error.InvalidMatch;
        const edge_rel_end = std.mem.indexOf(u8, text[edge_start..], "]->") orelse return Error.InvalidMatch;
        const edge_text = text[edge_start + 2 .. edge_start + edge_rel_end];
        const target_text = std.mem.trim(u8, text[edge_start + edge_rel_end + 3 ..], " \t");

        const edge_colon = std.mem.indexOfScalar(u8, edge_text, ':') orelse return Error.InvalidMatch;
        const relation_variable = try allocator.dupe(u8, std.mem.trim(u8, edge_text[0..edge_colon], " \t"));
        errdefer allocator.free(relation_variable);
        const relation_type = try allocator.dupe(u8, std.mem.trim(u8, edge_text[edge_colon + 1 ..], " \t"));
        errdefer allocator.free(relation_type);

        const target = try parseNodePattern(allocator, target_text);
        errdefer target.deinit(allocator);

        return .{ .edge = .{
            .source = source,
            .relation_variable = relation_variable,
            .relation_type = relation_type,
            .target = target,
        } };
    }
    return .{ .node = try parseNodePattern(allocator, text) };
}

fn parseNodePattern(allocator: std.mem.Allocator, text: []const u8) !NodePattern {
    const trimmed = std.mem.trim(u8, text, " \t");
    if (trimmed.len < 3 or trimmed[0] != '(' or trimmed[trimmed.len - 1] != ')') return Error.InvalidMatch;
    const inner = std.mem.trim(u8, trimmed[1 .. trimmed.len - 1], " \t");
    const colon = std.mem.indexOfScalar(u8, inner, ':') orelse return Error.InvalidMatch;
    const variable = try allocator.dupe(u8, std.mem.trim(u8, inner[0..colon], " \t"));
    errdefer allocator.free(variable);

    var type_part = std.mem.trim(u8, inner[colon + 1 ..], " \t");
    var inline_name: ?[]const u8 = null;
    errdefer if (inline_name) |value| allocator.free(value);

    if (std.mem.indexOfScalar(u8, type_part, '{')) |brace| {
        const close = std.mem.lastIndexOfScalar(u8, type_part, '}') orelse return Error.InvalidMatch;
        const props = type_part[brace + 1 .. close];
        type_part = std.mem.trim(u8, type_part[0..brace], " \t");
        inline_name = try parseInlineName(allocator, props);
    }

    return .{
        .variable = variable,
        .entity_type = try allocator.dupe(u8, type_part),
        .inline_name = inline_name,
    };
}

fn parseInlineName(allocator: std.mem.Allocator, props: []const u8) !?[]const u8 {
    var parts = std.mem.splitScalar(u8, props, ',');
    while (parts.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t");
        if (std.mem.startsWith(u8, trimmed, "name")) {
            const eq = std.mem.indexOfScalar(u8, trimmed, ':') orelse return Error.InvalidMatch;
            return try parseScalarText(allocator, std.mem.trim(u8, trimmed[eq + 1 ..], " \t"));
        }
    }
    return null;
}

fn parsePredicate(allocator: std.mem.Allocator, text: []const u8) !Predicate {
    if (std.mem.indexOf(u8, text, " IN ")) |idx| {
        return .{
            .path = try allocator.dupe(u8, std.mem.trim(u8, text[0..idx], " \t")),
            .op = .in,
            .value = .{ .list_text = try parseTextList(allocator, std.mem.trim(u8, text[idx + " IN ".len ..], " \t")) },
        };
    }

    const ops = [_]struct { token: []const u8, op: Predicate.Op }{
        .{ .token = ">=", .op = .gte },
        .{ .token = "<=", .op = .lte },
        .{ .token = "=", .op = .eq },
        .{ .token = ">", .op = .gt },
        .{ .token = "<", .op = .lt },
    };
    for (ops) |candidate| {
        if (std.mem.indexOf(u8, text, candidate.token)) |idx| {
            return .{
                .path = try allocator.dupe(u8, std.mem.trim(u8, text[0..idx], " \t")),
                .op = candidate.op,
                .value = try parseScalarValue(allocator, std.mem.trim(u8, text[idx + candidate.token.len ..], " \t")),
            };
        }
    }
    return Error.InvalidPredicate;
}

fn parseTextList(allocator: std.mem.Allocator, text: []const u8) ![][]const u8 {
    const trimmed = std.mem.trim(u8, text, " \t");
    if (trimmed.len < 2 or trimmed[0] != '(' or trimmed[trimmed.len - 1] != ')') return Error.InvalidPredicate;

    var values = std.ArrayList([]const u8).empty;
    errdefer {
        for (values.items) |value| allocator.free(value);
        values.deinit(allocator);
    }

    var parts = std.mem.splitScalar(u8, trimmed[1 .. trimmed.len - 1], ',');
    while (parts.next()) |part| {
        try values.append(allocator, try parseScalarText(allocator, std.mem.trim(u8, part, " \t")));
    }
    return values.toOwnedSlice(allocator);
}

fn parseScalarValue(allocator: std.mem.Allocator, text: []const u8) !Predicate.Value {
    if (text.len > 0 and text[0] == '\'') return .{ .text = try parseScalarText(allocator, text) };
    if (std.fmt.parseFloat(f64, text)) |number| return .{ .number = number } else |_| {}
    return .{ .text = try allocator.dupe(u8, text) };
}

fn parseScalarText(allocator: std.mem.Allocator, text: []const u8) ![]const u8 {
    const trimmed = std.mem.trim(u8, text, " \t");
    if (trimmed.len >= 2 and trimmed[0] == '\'' and trimmed[trimmed.len - 1] == '\'') {
        return try allocator.dupe(u8, trimmed[1 .. trimmed.len - 1]);
    }
    return try allocator.dupe(u8, trimmed);
}

fn validateQuery(query: Query) !void {
    switch (query.match) {
        .node => |node| try validateNodePattern(node),
        .edge => |edge| {
            try validateNodePattern(edge.source);
            try validateIdentifier(edge.relation_variable);
            try validateNodePattern(edge.target);
        },
    }

    for (query.predicates) |predicate| try validatePredicatePath(query, predicate.path);

    switch (query.project) {
        .fields => |fields| for (fields) |field| try validateProjectField(query, field),
        .bundle_projection_get => {},
    }
}

fn validateNodePattern(node: NodePattern) !void {
    try validateIdentifier(node.variable);
}

fn validatePredicatePath(query: Query, path: []const u8) !void {
    switch (query.match) {
        .node => |node| if (startsWithVariable(path, node.variable)) {
            return validateNodePathSuffix(path[node.variable.len + 1 ..]);
        },
        .edge => |edge| {
            if (startsWithVariable(path, edge.source.variable)) return validateNodePathSuffix(path[edge.source.variable.len + 1 ..]);
            if (startsWithVariable(path, edge.target.variable)) return validateNodePathSuffix(path[edge.target.variable.len + 1 ..]);
            if (startsWithVariable(path, edge.relation_variable)) return validateRelationPathSuffix(path[edge.relation_variable.len + 1 ..]);
        },
    }
    return Error.InvalidPredicate;
}

fn validateProjectField(query: Query, field: []const u8) !void {
    switch (query.match) {
        .node => |node| if (startsWithVariable(field, node.variable)) {
            return validateNodePathSuffix(field[node.variable.len + 1 ..]);
        },
        .edge => |edge| {
            if (query.hops != null) {
                if (startsWithVariable(field, edge.target.variable)) return validateNodePathSuffix(field[edge.target.variable.len + 1 ..]);
            } else {
                if (startsWithVariable(field, edge.source.variable)) return validateNodePathSuffix(field[edge.source.variable.len + 1 ..]);
                if (startsWithVariable(field, edge.target.variable)) return validateNodePathSuffix(field[edge.target.variable.len + 1 ..]);
                if (startsWithVariable(field, edge.relation_variable)) return validateRelationPathSuffix(field[edge.relation_variable.len + 1 ..]);
            }
        },
    }
    return Error.UnsupportedProject;
}

fn validateNodePathSuffix(suffix: []const u8) !void {
    if (std.mem.eql(u8, suffix, "entity_id")) return;
    if (std.mem.eql(u8, suffix, "entity_type")) return;
    if (std.mem.eql(u8, suffix, "name")) return;
    if (std.mem.eql(u8, suffix, "confidence")) return;
    if (std.mem.eql(u8, suffix, "metadata")) return;
    if (std.mem.startsWith(u8, suffix, "metadata.")) return validateJsonPathKey(suffix["metadata.".len..]);
    return Error.UnsupportedQuery;
}

fn validateRelationPathSuffix(suffix: []const u8) !void {
    if (std.mem.eql(u8, suffix, "relation_id")) return;
    if (std.mem.eql(u8, suffix, "relation_type")) return;
    if (std.mem.eql(u8, suffix, "confidence")) return;
    if (std.mem.eql(u8, suffix, "metadata")) return;
    if (std.mem.startsWith(u8, suffix, "metadata.")) return validateJsonPathKey(suffix["metadata.".len..]);
    if (std.mem.startsWith(u8, suffix, "prop.")) return validatePropertyKey(suffix["prop.".len..]);
    return Error.UnsupportedQuery;
}

fn validateIdentifier(value: []const u8) !void {
    if (value.len == 0) return Error.InvalidMatch;
    for (value) |ch| {
        if (!isIdentifierChar(ch)) return Error.InvalidMatch;
    }
}

fn validateJsonPathKey(value: []const u8) !void {
    if (value.len == 0) return Error.InvalidPredicate;
    for (value) |ch| {
        if (!isIdentifierChar(ch) and ch != '-') return Error.InvalidPredicate;
    }
}

fn validatePropertyKey(value: []const u8) !void {
    if (value.len == 0) return Error.InvalidPredicate;
    for (value) |ch| {
        if (!isIdentifierChar(ch) and ch != '-' and ch != ':') return Error.InvalidPredicate;
    }
}

fn isIdentifierChar(ch: u8) bool {
    return std.ascii.isAlphanumeric(ch) or ch == '_';
}

pub fn executeSqlite(allocator: std.mem.Allocator, db: facet_sqlite.Database, text: []const u8) ![]u8 {
    var query = try parse(allocator, text);
    defer query.deinit(allocator);
    try validateQuery(query);

    if (query.project == .bundle_projection_get) {
        return try executeBundleProjectionGet(allocator, db, query);
    }
    if (query.hops != null) {
        return try executeHops(allocator, db, query);
    }

    return switch (query.match) {
        .node => try executeNode(allocator, db, query),
        .edge => try executeEdge(allocator, db, query),
    };
}

fn executeNode(allocator: std.mem.Allocator, db: facet_sqlite.Database, query: Query) ![]u8 {
    const node = query.match.node;
    var sql: std.Io.Writer.Allocating = .init(allocator);
    defer sql.deinit();

    try sql.writer.writeAll(
        \\SELECT entity_id, entity_type, name, confidence, metadata_json
        \\FROM graph_entity
        \\WHERE workspace_id = ?1 AND entity_type = ?2 AND deprecated_at IS NULL
    );
    if (node.inline_name != null) try sql.writer.writeAll(" AND name = ?3");
    try appendNodePredicatesSql(&sql.writer, query, node.variable, if (node.inline_name != null) 4 else 3);
    try sql.writer.writeAll(" ORDER BY confidence DESC, entity_id ASC LIMIT ");
    try sql.writer.print("{}", .{query.limit});

    const sql_text = try sql.toOwnedSlice();
    defer allocator.free(sql_text);

    const stmt = try facet_sqlite.prepare(db, sql_text);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, query.workspace_id);
    try facet_sqlite.bindText(stmt, 2, node.entity_type);
    var bind_index: c_int = 3;
    if (node.inline_name) |name| {
        try facet_sqlite.bindText(stmt, bind_index, name);
        bind_index += 1;
    }
    try bindNodePredicates(stmt, query, node.variable, &bind_index);

    return try rowsToJson(allocator, stmt, query, null);
}

fn executeEdge(allocator: std.mem.Allocator, db: facet_sqlite.Database, query: Query) ![]u8 {
    const edge = query.match.edge;
    var sql: std.Io.Writer.Allocating = .init(allocator);
    defer sql.deinit();

    const inline_count: c_int = @intCast((if (edge.source.inline_name != null) @as(usize, 1) else 0) + (if (edge.target.inline_name != null) @as(usize, 1) else 0));
    const source_predicate_start: c_int = 5 + inline_count;
    const target_predicate_start = source_predicate_start + countNodePredicateBinds(query, edge.source.variable);
    const projected_property_start = target_predicate_start + countNodePredicateBinds(query, edge.target.variable);
    const property_predicate_start = projected_property_start + countProjectedRelationProperties(query, edge.relation_variable);
    const relation_predicate_start = property_predicate_start + countRelationPropertyPredicateBinds(query, edge.relation_variable);

    try sql.writer.writeAll(
        \\SELECT src.entity_id, src.entity_type, src.name, src.confidence, src.metadata_json,
        \\       r.relation_id, r.relation_type, r.confidence, r.metadata_json,
        \\       dst.entity_id, dst.entity_type, dst.name, dst.confidence, dst.metadata_json
    );
    try appendProjectedRelationPropertySelects(&sql.writer, query, edge.relation_variable);
    try sql.writer.writeAll(
        \\ FROM graph_entity src
        \\JOIN graph_relation r ON r.workspace_id = src.workspace_id AND r.source_id = src.entity_id
        \\JOIN graph_entity dst ON dst.workspace_id = src.workspace_id AND dst.entity_id = r.target_id
    );
    try appendProjectedRelationPropertyJoins(&sql.writer, query, edge.relation_variable, projected_property_start);
    try appendPropertyPredicateJoins(&sql.writer, query, edge.relation_variable, property_predicate_start);
    try sql.writer.writeAll(
        \\ WHERE src.workspace_id = ?1
        \\   AND src.entity_type = ?2
        \\   AND r.relation_type = ?3
        \\   AND dst.entity_type = ?4
        \\   AND src.deprecated_at IS NULL
        \\   AND r.deprecated_at IS NULL
        \\   AND dst.deprecated_at IS NULL
    );
    if (edge.source.inline_name != null) try sql.writer.writeAll(" AND src.name = ?5");
    if (edge.target.inline_name != null) try sql.writer.writeAll(if (edge.source.inline_name != null) " AND dst.name = ?6" else " AND dst.name = ?5");
    try appendNodePredicatesSqlAliased(&sql.writer, query, edge.source.variable, "src", source_predicate_start);
    try appendNodePredicatesSqlAliased(&sql.writer, query, edge.target.variable, "dst", target_predicate_start);
    try appendRelationPredicatesSql(&sql.writer, query, edge.relation_variable, relation_predicate_start);
    try sql.writer.writeAll(" ORDER BY r.confidence DESC, r.relation_id ASC LIMIT ");
    try sql.writer.print("{}", .{query.limit});

    const sql_text = try sql.toOwnedSlice();
    defer allocator.free(sql_text);

    const stmt = try facet_sqlite.prepare(db, sql_text);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, query.workspace_id);
    try facet_sqlite.bindText(stmt, 2, edge.source.entity_type);
    try facet_sqlite.bindText(stmt, 3, edge.relation_type);
    try facet_sqlite.bindText(stmt, 4, edge.target.entity_type);
    var bind_index: c_int = 5;
    if (edge.source.inline_name) |name| {
        try facet_sqlite.bindText(stmt, bind_index, name);
        bind_index += 1;
    }
    if (edge.target.inline_name) |name| {
        try facet_sqlite.bindText(stmt, bind_index, name);
        bind_index += 1;
    }
    try bindNodePredicates(stmt, query, edge.source.variable, &bind_index);
    try bindNodePredicates(stmt, query, edge.target.variable, &bind_index);
    try bindProjectedRelationPropertyKeys(stmt, query, edge.relation_variable, &bind_index);
    try bindRelationPropertyPredicates(stmt, query, edge.relation_variable, &bind_index);
    try bindRelationPredicates(stmt, query, edge.relation_variable, &bind_index);

    return try rowsToJson(allocator, stmt, query, edge);
}

fn appendProjectedRelationPropertyJoins(writer: *std.Io.Writer, query: Query, relation_var: []const u8, start_index: c_int) !void {
    if (query.project != .fields) return;
    var bind_offset: c_int = 0;
    var alias_count: usize = 0;
    for (query.project.fields) |field| {
        if (projectedRelationPropertyKey(field, relation_var) == null) continue;
        try writer.print(
            " LEFT JOIN graph_relation_property rpp{} ON rpp{}.relation_id = r.relation_id AND rpp{}.property_key = ?{}",
            .{ alias_count, alias_count, alias_count, start_index + bind_offset },
        );
        bind_offset += 1;
        alias_count += 1;
    }
}

fn appendPropertyPredicateJoins(writer: *std.Io.Writer, query: Query, relation_var: []const u8, start_index: c_int) !void {
    var bind_offset: c_int = 0;
    var alias_count: usize = 0;
    for (query.predicates) |predicate| {
        if (!isRelationPropertyPredicate(predicate, relation_var)) continue;
        if (predicate.op == .in) return Error.UnsupportedQuery;
        try writer.print(
            " JOIN graph_relation_property rp{} ON rp{}.relation_id = r.relation_id AND rp{}.property_key = ?{} AND ",
            .{ alias_count, alias_count, alias_count, start_index + bind_offset },
        );
        bind_offset += 1;
        try appendRelationPropertyValueSql(writer, alias_count, predicate.value);
        try writer.print(" {s} ?{}", .{ sqlOp(predicate.op), start_index + bind_offset });
        bind_offset += 1;
        alias_count += 1;
    }
}

fn appendNodePredicatesSql(writer: *std.Io.Writer, query: Query, variable: []const u8, start_index: c_int) !void {
    try appendNodePredicatesSqlAliased(writer, query, variable, "", start_index);
}

fn appendNodePredicatesSqlAliased(writer: *std.Io.Writer, query: Query, variable: []const u8, alias: []const u8, start_index: c_int) !void {
    var bind_offset: c_int = 0;
    for (query.predicates) |predicate| {
        if (!isNodePredicate(predicate, variable)) continue;
        try writer.writeAll(" AND ");
        try appendNodePathSql(writer, predicate.path, variable, alias);
        try writer.print(" {s} ?{}", .{ sqlOp(predicate.op), start_index + bind_offset });
        bind_offset += 1;
    }
}

fn appendRelationPredicatesSql(writer: *std.Io.Writer, query: Query, variable: []const u8, start_index: c_int) !void {
    var bind_offset: c_int = 0;
    for (query.predicates) |predicate| {
        if (isRelationPropertyPredicate(predicate, variable)) continue;
        if (!startsWithVariable(predicate.path, variable)) continue;
        try writer.writeAll(" AND ");
        try appendRelationPathSql(writer, predicate.path, variable);
        if (predicate.op == .in) return Error.UnsupportedQuery;
        try writer.print(" {s} ?{}", .{ sqlOp(predicate.op), start_index + bind_offset });
        bind_offset += 1;
    }
}

fn appendNodePathSql(writer: *std.Io.Writer, path: []const u8, variable: []const u8, alias: []const u8) !void {
    const field = path[variable.len + 1 ..];
    const prefix = if (alias.len == 0) "" else alias;
    if (std.mem.eql(u8, field, "entity_id")) return writer.print("{s}{s}entity_id", .{ prefix, if (prefix.len == 0) "" else "." });
    if (std.mem.eql(u8, field, "entity_type")) return writer.print("{s}{s}entity_type", .{ prefix, if (prefix.len == 0) "" else "." });
    if (std.mem.eql(u8, field, "name")) return writer.print("{s}{s}name", .{ prefix, if (prefix.len == 0) "" else "." });
    if (std.mem.eql(u8, field, "confidence")) return writer.print("{s}{s}confidence", .{ prefix, if (prefix.len == 0) "" else "." });
    if (std.mem.startsWith(u8, field, "metadata.")) {
        return writer.print("json_extract({s}{s}metadata_json, '$.{s}')", .{ prefix, if (prefix.len == 0) "" else ".", field["metadata.".len..] });
    }
    return Error.UnsupportedQuery;
}

fn appendRelationPathSql(writer: *std.Io.Writer, path: []const u8, variable: []const u8) !void {
    const field = path[variable.len + 1 ..];
    if (std.mem.eql(u8, field, "relation_id")) return writer.writeAll("r.relation_id");
    if (std.mem.eql(u8, field, "relation_type")) return writer.writeAll("r.relation_type");
    if (std.mem.eql(u8, field, "confidence")) return writer.writeAll("r.confidence");
    if (std.mem.startsWith(u8, field, "metadata.")) {
        return writer.print("json_extract(r.metadata_json, '$.{s}')", .{field["metadata.".len..]});
    }
    return Error.UnsupportedQuery;
}

fn bindNodePredicates(stmt: *c.sqlite3_stmt, query: Query, variable: []const u8, bind_index: *c_int) !void {
    for (query.predicates) |predicate| {
        if (!isNodePredicate(predicate, variable)) continue;
        try bindPredicateValue(stmt, bind_index.*, predicate.value);
        bind_index.* += 1;
    }
}

fn bindRelationPredicates(stmt: *c.sqlite3_stmt, query: Query, variable: []const u8, bind_index: *c_int) !void {
    for (query.predicates) |predicate| {
        if (isRelationPropertyPredicate(predicate, variable)) continue;
        if (!startsWithVariable(predicate.path, variable)) continue;
        try bindPredicateValue(stmt, bind_index.*, predicate.value);
        bind_index.* += 1;
    }
}

fn bindPredicateValue(stmt: *c.sqlite3_stmt, index: c_int, value: Predicate.Value) !void {
    switch (value) {
        .text => |text| try facet_sqlite.bindText(stmt, index, text),
        .number => |number| if (c.sqlite3_bind_double(stmt, index, number) != c.SQLITE_OK) return error.BindFailed,
        .list_text => return Error.UnsupportedQuery,
    }
}

fn executeHops(allocator: std.mem.Allocator, db: facet_sqlite.Database, query: Query) ![]u8 {
    const edge = query.match.edge;
    const hops = query.hops orelse return Error.UnsupportedQuery;
    if (hops.min != 1) return Error.UnsupportedQuery;

    const relation_types = relationTypeFilter(query, edge.relation_variable) orelse return Error.UnsupportedQuery;
    const type_sql = try placeholders(allocator, 4, relation_types.len);
    defer allocator.free(type_sql);
    const max_index: c_int = 4 + @as(c_int, @intCast(relation_types.len));
    const min_index = max_index + 1;
    const limit_index = min_index + 1;

    const seed_name = edge.source.inline_name orelse return Error.UnsupportedQuery;
    const sql = try std.fmt.allocPrint(allocator,
        \\WITH RECURSIVE walk(entity_id, depth) AS (
        \\  SELECT entity_id, 0 FROM graph_entity
        \\  WHERE workspace_id = ?1 AND entity_type = ?2 AND name = ?3 AND deprecated_at IS NULL
        \\  UNION
        \\  SELECT r.target_id, walk.depth + 1
        \\  FROM walk
        \\  JOIN graph_relation r ON r.source_id = walk.entity_id
        \\  WHERE r.workspace_id = ?1 AND r.deprecated_at IS NULL
        \\    AND r.relation_type IN ({s})
        \\    AND walk.depth < ?{}
        \\)
        \\SELECT e.entity_id, e.entity_type, e.name, e.confidence, e.metadata_json
        \\FROM walk
        \\JOIN graph_entity e ON e.entity_id = walk.entity_id
        \\WHERE walk.depth >= ?{} AND e.deprecated_at IS NULL
        \\ORDER BY walk.depth, e.entity_id
        \\LIMIT ?{}
    , .{ type_sql, max_index, min_index, limit_index });
    defer allocator.free(sql);

    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, query.workspace_id);
    try facet_sqlite.bindText(stmt, 2, edge.source.entity_type);
    try facet_sqlite.bindText(stmt, 3, seed_name);
    var bind_index: c_int = 4;
    for (relation_types) |relation_type| {
        try facet_sqlite.bindText(stmt, bind_index, relation_type);
        bind_index += 1;
    }
    try facet_sqlite.bindInt64(stmt, max_index, hops.max);
    try facet_sqlite.bindInt64(stmt, min_index, hops.min);
    try facet_sqlite.bindInt64(stmt, limit_index, query.limit);

    return try rowsToJson(allocator, stmt, query, null);
}

fn placeholders(allocator: std.mem.Allocator, start_index: c_int, count: usize) ![]u8 {
    if (count == 0) return Error.UnsupportedQuery;
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    for (0..count) |i| {
        if (i != 0) try out.writer.writeAll(",");
        try out.writer.print("?{}", .{start_index + @as(c_int, @intCast(i))});
    }
    return out.toOwnedSlice();
}

fn executeBundleProjectionGet(allocator: std.mem.Allocator, db: facet_sqlite.Database, query: Query) ![]u8 {
    const node = query.match.node;
    if (!std.mem.eql(u8, node.entity_type, "ProjectionResult")) return Error.UnsupportedQuery;
    const projection_id = findTextPredicate(query, node.variable, "metadata.projection_id") orelse return Error.UnsupportedQuery;
    const sql =
        \\SELECT entity_id, entity_type, name, confidence, metadata_json
        \\FROM graph_entity
        \\WHERE workspace_id = ?1
        \\  AND entity_type = 'ProjectionResult'
        \\  AND json_extract(metadata_json, '$.projection_id') = ?2
        \\  AND deprecated_at IS NULL
        \\ORDER BY confidence DESC, entity_id ASC
        \\LIMIT ?3
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, query.workspace_id);
    try facet_sqlite.bindText(stmt, 2, projection_id);
    try facet_sqlite.bindInt64(stmt, 3, query.limit);

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.print(
        "{{\"workspace_id\":{f},\"backend\":\"sqlite\",\"bundle\":\"projection_get\",\"projection_id\":{f},\"projection_results\":[",
        .{ std.json.fmt(query.workspace_id, .{}), std.json.fmt(projection_id, .{}) },
    );
    var first = true;
    while (try helper_api.stepRow(stmt)) {
        if (!first) try out.writer.writeAll(",");
        first = false;
        try writeNodeObject(&out.writer, stmt, 0);
    }
    try out.writer.writeAll("]}");
    return out.toOwnedSlice();
}

fn rowsToJson(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, query: Query, edge: ?EdgePattern) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    const fields = query.project.fields;
    try out.writer.print("{{\"workspace_id\":{f},\"backend\":\"sqlite\",\"limit\":{},\"columns\":[", .{
        std.json.fmt(query.workspace_id, .{}),
        query.limit,
    });
    for (fields, 0..) |field, i| {
        if (i != 0) try out.writer.writeAll(",");
        try out.writer.print("{f}", .{std.json.fmt(field, .{})});
    }
    try out.writer.writeAll("],\"rows\":[");

    var returned: usize = 0;
    while (try helper_api.stepRow(stmt)) {
        if (returned != 0) try out.writer.writeAll(",");
        try out.writer.writeAll("{");
        for (fields, 0..) |field, i| {
            if (i != 0) try out.writer.writeAll(",");
            try out.writer.print("{f}:", .{std.json.fmt(field, .{})});
            try writeProjectedValue(&out.writer, stmt, query, edge, field);
        }
        try out.writer.writeAll("}");
        returned += 1;
    }
    try out.writer.print("],\"returned\":{}}}", .{returned});
    return out.toOwnedSlice();
}

fn writeProjectedValue(writer: *std.Io.Writer, stmt: *c.sqlite3_stmt, query: Query, edge: ?EdgePattern, field: []const u8) !void {
    if (edge) |edge_pattern| {
        if (startsWithVariable(field, edge_pattern.source.variable)) return writeNodeField(writer, stmt, field, edge_pattern.source.variable, 0);
        if (startsWithVariable(field, edge_pattern.relation_variable)) return writeRelationField(writer, stmt, query, field, edge_pattern.relation_variable);
        if (startsWithVariable(field, edge_pattern.target.variable)) return writeNodeField(writer, stmt, field, edge_pattern.target.variable, 9);
    } else {
        if (query.hops != null) {
            const edge_pattern = query.match.edge;
            if (startsWithVariable(field, edge_pattern.target.variable)) return writeNodeField(writer, stmt, field, edge_pattern.target.variable, 0);
            return writer.writeAll("null");
        }
        const node = query.match.node;
        if (startsWithVariable(field, node.variable)) return writeNodeField(writer, stmt, field, node.variable, 0);
    }
    return writer.writeAll("null");
}

fn writeNodeField(writer: *std.Io.Writer, stmt: *c.sqlite3_stmt, field: []const u8, variable: []const u8, offset: c_int) !void {
    const path = field[variable.len + 1 ..];
    if (std.mem.eql(u8, path, "entity_id")) return writer.print("{}", .{c.sqlite3_column_int64(stmt, offset + 0)});
    if (std.mem.eql(u8, path, "entity_type")) return writeSqliteTextJson(writer, stmt, offset + 1);
    if (std.mem.eql(u8, path, "name")) return writeSqliteTextJson(writer, stmt, offset + 2);
    if (std.mem.eql(u8, path, "confidence")) return writer.print("{d}", .{c.sqlite3_column_double(stmt, offset + 3)});
    if (std.mem.eql(u8, path, "metadata")) return writeRawJsonObject(writer, stmt, offset + 4);
    if (std.mem.startsWith(u8, path, "metadata.")) return writeJsonExtract(writer, stmt, offset + 4, path["metadata.".len..]);
    return writer.writeAll("null");
}

fn writeRelationField(writer: *std.Io.Writer, stmt: *c.sqlite3_stmt, query: Query, field: []const u8, variable: []const u8) !void {
    const path = field[variable.len + 1 ..];
    if (std.mem.eql(u8, path, "relation_id")) return writer.print("{}", .{c.sqlite3_column_int64(stmt, 5)});
    if (std.mem.eql(u8, path, "relation_type")) return writeSqliteTextJson(writer, stmt, 6);
    if (std.mem.eql(u8, path, "confidence")) return writer.print("{d}", .{c.sqlite3_column_double(stmt, 7)});
    if (std.mem.eql(u8, path, "metadata")) return writeRawJsonObject(writer, stmt, 8);
    if (std.mem.startsWith(u8, path, "prop.")) {
        const prop_index = projectedRelationPropertyIndex(query, variable, field) orelse return writer.writeAll("null");
        const base: c_int = 14 + @as(c_int, @intCast(prop_index * 3));
        if (c.sqlite3_column_type(stmt, base) != c.SQLITE_NULL) return writeSqliteTextJson(writer, stmt, base);
        if (c.sqlite3_column_type(stmt, base + 1) != c.SQLITE_NULL) return writer.print("{d}", .{c.sqlite3_column_double(stmt, base + 1)});
        if (c.sqlite3_column_type(stmt, base + 2) != c.SQLITE_NULL) return writer.print("{}", .{c.sqlite3_column_int64(stmt, base + 2)});
        return writer.writeAll("null");
    }
    return writer.writeAll("null");
}

fn writeNodeObject(writer: *std.Io.Writer, stmt: *c.sqlite3_stmt, offset: c_int) !void {
    try writer.print("{{\"entity_id\":{},\"entity_type\":", .{c.sqlite3_column_int64(stmt, offset)});
    try writeSqliteTextJson(writer, stmt, offset + 1);
    try writer.writeAll(",\"name\":");
    try writeSqliteTextJson(writer, stmt, offset + 2);
    try writer.print(",\"confidence\":{d},\"metadata\":", .{c.sqlite3_column_double(stmt, offset + 3)});
    try writeRawJsonObject(writer, stmt, offset + 4);
    try writer.writeAll("}");
}

fn writeSqliteTextJson(writer: *std.Io.Writer, stmt: *c.sqlite3_stmt, col: c_int) !void {
    const ptr = c.sqlite3_column_text(stmt, col) orelse return writer.writeAll("null");
    const text = ptr[0..@intCast(c.sqlite3_column_bytes(stmt, col))];
    return writer.print("{f}", .{std.json.fmt(text, .{})});
}

fn writeRawJsonObject(writer: *std.Io.Writer, stmt: *c.sqlite3_stmt, col: c_int) !void {
    const ptr = c.sqlite3_column_text(stmt, col) orelse return writer.writeAll("{}");
    const text = ptr[0..@intCast(c.sqlite3_column_bytes(stmt, col))];
    if (text.len == 0) return writer.writeAll("{}");
    return writer.writeAll(text);
}

fn writeJsonExtract(writer: *std.Io.Writer, stmt: *c.sqlite3_stmt, metadata_col: c_int, key: []const u8) !void {
    const ptr = c.sqlite3_column_text(stmt, metadata_col) orelse return writer.writeAll("null");
    const text = ptr[0..@intCast(c.sqlite3_column_bytes(stmt, metadata_col))];
    var parsed = std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, text, .{}) catch return writer.writeAll("null");
    defer parsed.deinit();
    const object = parsed.value.object;
    const value = object.get(key) orelse return writer.writeAll("null");
    return writer.print("{f}", .{std.json.fmt(value, .{})});
}

fn startsWithVariable(path: []const u8, variable: []const u8) bool {
    return path.len > variable.len + 1 and std.mem.eql(u8, path[0..variable.len], variable) and path[variable.len] == '.';
}

fn isNodePredicate(predicate: Predicate, variable: []const u8) bool {
    if (!startsWithVariable(predicate.path, variable)) return false;
    const field = predicate.path[variable.len + 1 ..];
    return !std.mem.startsWith(u8, field, "prop.") and !std.mem.eql(u8, field, "relation_type") and !std.mem.eql(u8, field, "relation_id");
}

fn isRelationPropertyPredicate(predicate: Predicate, relation_var: []const u8) bool {
    return startsWithVariable(predicate.path, relation_var) and std.mem.startsWith(u8, predicate.path[relation_var.len + 1 ..], "prop.");
}

fn propertyKey(path: []const u8) []const u8 {
    const prop = ".prop.";
    const idx = std.mem.indexOf(u8, path, prop).?;
    return path[idx + prop.len ..];
}

fn sqlOp(op: Predicate.Op) []const u8 {
    return switch (op) {
        .eq => "=",
        .gt => ">",
        .gte => ">=",
        .lt => "<",
        .lte => "<=",
        .in => "IN",
    };
}

fn countProjectedRelationProperties(query: Query, relation_var: []const u8) c_int {
    if (query.project != .fields) return 0;
    var count: c_int = 0;
    for (query.project.fields) |field| {
        if (projectedRelationPropertyKey(field, relation_var) != null) count += 1;
    }
    return count;
}

fn countRelationPropertyPredicateBinds(query: Query, relation_var: []const u8) c_int {
    var count: c_int = 0;
    for (query.predicates) |predicate| {
        if (isRelationPropertyPredicate(predicate, relation_var)) count += 2;
    }
    return count;
}

fn countNodePredicateBinds(query: Query, variable: []const u8) c_int {
    var count: c_int = 0;
    for (query.predicates) |predicate| {
        if (isNodePredicate(predicate, variable)) count += 1;
    }
    return count;
}

fn relationTypeFilter(query: Query, variable: []const u8) ?[][]const u8 {
    for (query.predicates) |predicate| {
        if (!startsWithVariable(predicate.path, variable)) continue;
        if (!std.mem.eql(u8, predicate.path[variable.len + 1 ..], "relation_type")) continue;
        if (predicate.op != .in) return null;
        return switch (predicate.value) {
            .list_text => |values| values,
            else => null,
        };
    }
    return null;
}

fn appendProjectedRelationPropertySelects(writer: *std.Io.Writer, query: Query, relation_var: []const u8) !void {
    if (query.project != .fields) return;
    var alias_count: usize = 0;
    for (query.project.fields) |field| {
        if (projectedRelationPropertyKey(field, relation_var) == null) continue;
        try writer.print(
            ", rpp{}.value_text, rpp{}.value_number, rpp{}.value_integer",
            .{ alias_count, alias_count, alias_count },
        );
        alias_count += 1;
    }
}

fn projectedRelationPropertyKey(field: []const u8, relation_var: []const u8) ?[]const u8 {
    if (!startsWithVariable(field, relation_var)) return null;
    const suffix = field[relation_var.len + 1 ..];
    if (!std.mem.startsWith(u8, suffix, "prop.")) return null;
    return suffix["prop.".len..];
}

fn appendRelationPropertyValueSql(writer: *std.Io.Writer, alias_count: usize, value: Predicate.Value) !void {
    return switch (value) {
        .text, .list_text => writer.print("rp{}.value_text", .{alias_count}),
        .number => writer.print("COALESCE(rp{}.value_number, CAST(rp{}.value_integer AS REAL))", .{ alias_count, alias_count }),
    };
}

fn bindProjectedRelationPropertyKeys(stmt: *c.sqlite3_stmt, query: Query, relation_var: []const u8, bind_index: *c_int) !void {
    if (query.project != .fields) return;
    for (query.project.fields) |field| {
        const key = projectedRelationPropertyKey(field, relation_var) orelse continue;
        try facet_sqlite.bindText(stmt, bind_index.*, key);
        bind_index.* += 1;
    }
}

fn bindRelationPropertyPredicates(stmt: *c.sqlite3_stmt, query: Query, variable: []const u8, bind_index: *c_int) !void {
    for (query.predicates) |predicate| {
        if (!isRelationPropertyPredicate(predicate, variable)) continue;
        try facet_sqlite.bindText(stmt, bind_index.*, propertyKey(predicate.path));
        bind_index.* += 1;
        try bindPredicateValue(stmt, bind_index.*, predicate.value);
        bind_index.* += 1;
    }
}

fn projectedRelationPropertyIndex(query: Query, relation_var: []const u8, wanted: []const u8) ?usize {
    if (query.project != .fields) return null;
    var index: usize = 0;
    for (query.project.fields) |field| {
        if (startsWithVariable(field, relation_var) and std.mem.startsWith(u8, field[relation_var.len + 1 ..], "prop.")) {
            if (std.mem.eql(u8, field, wanted)) return index;
            index += 1;
        }
    }
    return null;
}

fn findTextPredicate(query: Query, variable: []const u8, suffix: []const u8) ?[]const u8 {
    for (query.predicates) |predicate| {
        if (!startsWithVariable(predicate.path, variable)) continue;
        if (!std.mem.eql(u8, predicate.path[variable.len + 1 ..], suffix)) continue;
        if (predicate.op != .eq) continue;
        return switch (predicate.value) {
            .text => |value| value,
            else => null,
        };
    }
    return null;
}

test "graph pattern parser covers documented node edge hops and bundle syntax" {
    const allocator = std.testing.allocator;

    var node = try parse(allocator,
        \\WORKSPACE immeuble-demo
        \\MATCH (u:unit {name: 'Tilleuls Appartement A3'})
        \\WHERE u.metadata.building_id = 1
        \\PROJECT u.entity_id, u.name, u.metadata
        \\LIMIT 10
    );
    defer node.deinit(allocator);
    try std.testing.expectEqualStrings("immeuble-demo", node.workspace_id);
    try std.testing.expect(node.match == .node);
    try std.testing.expectEqual(@as(usize, 1), node.predicates.len);
    try std.testing.expectEqual(@as(usize, 10), node.limit);

    var edge = try parse(allocator,
        \\WORKSPACE immeuble-demo
        \\MATCH (p:person)-[o:owns]->(u:unit)
        \\WHERE o.prop.quote_part >= 0.5
        \\PROJECT p.name, u.name, o.relation_id, o.prop.quote_part
        \\LIMIT 20
    );
    defer edge.deinit(allocator);
    try std.testing.expect(edge.match == .edge);
    try std.testing.expectEqual(@as(usize, 4), edge.project.fields.len);

    var hops = try parse(allocator,
        \\WORKSPACE immeuble-demo
        \\MATCH (b:building {name: 'Résidence Les Tilleuls'})-[r:contains]->(x:unit)
        \\HOPS 1..3
        \\WHERE r.relation_type IN ('contains', 'owns')
        \\PROJECT x.entity_id, x.entity_type, x.name
        \\LIMIT 50
    );
    defer hops.deinit(allocator);
    try std.testing.expect(hops.hops != null);
    try std.testing.expectEqual(@as(usize, 3), hops.hops.?.max);

    var bundle = try parse(allocator,
        \\WORKSPACE seo-audit
        \\MATCH (pr:ProjectionResult)
        \\WHERE pr.metadata.projection_id = 'proj_keyword_opportunities'
        \\PROJECT BUNDLE projection_get
        \\LIMIT 1
    );
    defer bundle.deinit(allocator);
    try std.testing.expect(bundle.project == .bundle_projection_get);
}

test "graph pattern queryToJsonAst matches postgres canonical shape" {
    const allocator = std.testing.allocator;

    const edge_ast = try parseToJsonAst(allocator,
        \\WORKSPACE immeuble-demo
        \\MATCH (p:person)-[o:owns]->(u:unit)
        \\WHERE o.prop.quote_part >= 0.5
        \\PROJECT p.name, u.name, o.relation_id, o.prop.quote_part
        \\LIMIT 20
    );
    defer allocator.free(edge_ast);

    try std.testing.expect(std.mem.indexOf(u8, edge_ast, "\"workspace_id\":\"immeuble-demo\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edge_ast, "\"kind\":\"edge\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edge_ast, "\"direction\":\"out\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edge_ast, "\"path\":\"o.prop.quote_part\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edge_ast, "\"op\":\">=\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edge_ast, "\"value_number\":0.5") != null);
    try std.testing.expect(std.mem.indexOf(u8, edge_ast, "\"limit\":20") != null);

    const inline_ast = try parseToJsonAst(allocator,
        \\WORKSPACE immeuble-demo
        \\MATCH (p:person {name: 'Nicolas Dupont'})-[o:owns]->(u:unit {name: 'Tilleuls Appartement A3'})
        \\WHERE p.metadata.age_band = '45-54' AND u.metadata.lot = 'A3' AND o.prop.quote_part >= 0.5
        \\PROJECT p.name, u.name, o.relation_id, o.prop.quote_part
        \\LIMIT 20
    );
    defer allocator.free(inline_ast);

    try std.testing.expect(std.mem.indexOf(u8, inline_ast, "\"name\":\"Nicolas Dupont\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, inline_ast, "\"name\":\"Tilleuls Appartement A3\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, inline_ast, "\"path\":\"p.metadata.age_band\"") != null);
}

test "graph pattern parseToJsonAst rejects unsafe dynamic path fragments before export" {
    try std.testing.expectError(Error.InvalidPredicate, parseToJsonAst(std.testing.allocator,
        \\WORKSPACE immeuble-demo
        \\MATCH (p:person)-[o:owns]->(u:unit)
        \\WHERE o.prop.quote_part' = 0.5
        \\PROJECT p.name, u.name
        \\LIMIT 20
    ));
}

fn setupGraphPatternFixture(db: facet_sqlite.Database) !void {
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name, confidence, metadata_json) VALUES
        \\  (1, 'immeuble-demo', 'building', 'Résidence Les Tilleuls', 1.0, '{"building_id":1,"quota_basis":1000}'),
        \\  (2, 'immeuble-demo', 'block', 'Tilleuls Bloc A', 1.0, '{"building_id":1,"block":"A"}'),
        \\  (3, 'immeuble-demo', 'unit', 'Tilleuls Appartement A3', 0.97, '{"building_id":1,"floor":1,"lot":"A3","usage_status":"owner_occupied"}'),
        \\  (4, 'immeuble-demo', 'person', 'Nicolas Dupont', 0.95, '{"age_band":"45-54","household_role":"parent"}'),
        \\  (5, 'immeuble-demo', 'organization', 'Immo Invest SRL', 0.92, '{"kind":"landlord"}'),
        \\  (6, 'immeuble-demo', 'household', 'Ménage Karim Benali', 0.91, '{"household_status":"tenant"}'),
        \\  (7, 'seo-audit', 'ProjectionResult', 'keyword opportunity set', 0.99, '{"projection_id":"proj_keyword_opportunities","score":0.82}');
        \\INSERT INTO graph_relation(relation_id, workspace_id, relation_type, source_id, target_id, confidence, metadata_json) VALUES
        \\  (10, 'immeuble-demo', 'contains', 1, 2, 1.0, '{}'),
        \\  (11, 'immeuble-demo', 'contains', 2, 3, 1.0, '{}'),
        \\  (12, 'immeuble-demo', 'owns', 4, 3, 0.98, '{"source":"notary"}'),
        \\  (13, 'immeuble-demo', 'rented_to', 5, 6, 0.9, '{"source":"lease"}');
        \\INSERT INTO graph_relation_property(relation_id, property_key, value_type, value_number, value_text, value_integer, currency) VALUES
        \\  (12, 'quote_part', 'number', 0.5, NULL, NULL, NULL),
        \\  (12, 'right_type', 'text', NULL, 'pleine_propriete', NULL, NULL),
        \\  (13, 'monthly_rent', 'money_minor', NULL, NULL, 83500, 'EUR');
    );
}

test "graph pattern executor runs node metadata syntax on sqlite fixture" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try setupGraphPatternFixture(db);

    const json = try executeSqlite(std.testing.allocator, db,
        \\WORKSPACE immeuble-demo
        \\MATCH (u:unit {name: 'Tilleuls Appartement A3'})
        \\WHERE u.metadata.building_id = 1
        \\PROJECT u.entity_id, u.name, u.metadata.lot, u.metadata.usage_status
        \\LIMIT 5
    );
    defer std.testing.allocator.free(json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"returned\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Tilleuls Appartement A3") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"A3\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "owner_occupied") != null);
}

test "graph pattern executor runs one-edge typed relation property syntax" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try setupGraphPatternFixture(db);

    const json = try executeSqlite(std.testing.allocator, db,
        \\WORKSPACE immeuble-demo
        \\MATCH (p:person)-[o:owns]->(u:unit)
        \\WHERE o.prop.quote_part >= 0.5
        \\PROJECT p.name, u.name, o.relation_id, o.prop.quote_part
        \\LIMIT 20
    );
    defer std.testing.allocator.free(json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"returned\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Nicolas Dupont") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Tilleuls Appartement A3") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "0.5") != null);
}

test "graph pattern executor binds inline node predicates and relation properties in order" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try setupGraphPatternFixture(db);

    const json = try executeSqlite(std.testing.allocator, db,
        \\WORKSPACE immeuble-demo
        \\MATCH (p:person {name: 'Nicolas Dupont'})-[o:owns]->(u:unit {name: 'Tilleuls Appartement A3'})
        \\WHERE p.metadata.age_band = '45-54' AND u.metadata.lot = 'A3' AND o.prop.quote_part >= 0.5
        \\PROJECT p.name, u.name, o.relation_id, o.prop.quote_part
        \\LIMIT 20
    );
    defer std.testing.allocator.free(json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"returned\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Nicolas Dupont") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Tilleuls Appartement A3") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "0.5") != null);
}

test "graph pattern executor runs money and text relation property syntax" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try setupGraphPatternFixture(db);

    const money_json = try executeSqlite(std.testing.allocator, db,
        \\WORKSPACE immeuble-demo
        \\MATCH (org:organization)-[r:rented_to]->(h:household)
        \\WHERE r.prop.monthly_rent >= 80000
        \\PROJECT org.name, h.name, r.prop.monthly_rent
        \\LIMIT 20
    );
    defer std.testing.allocator.free(money_json);
    try std.testing.expect(std.mem.indexOf(u8, money_json, "Immo Invest SRL") != null);
    try std.testing.expect(std.mem.indexOf(u8, money_json, "83500") != null);

    const text_json = try executeSqlite(std.testing.allocator, db,
        \\WORKSPACE immeuble-demo
        \\MATCH (p:person)-[o:owns]->(u:unit)
        \\WHERE o.prop.right_type = 'pleine_propriete'
        \\PROJECT p.name, u.name, o.prop.right_type
        \\LIMIT 20
    );
    defer std.testing.allocator.free(text_json);
    try std.testing.expect(std.mem.indexOf(u8, text_json, "pleine_propriete") != null);
}

test "graph pattern executor runs hops and projection bundle syntax" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try setupGraphPatternFixture(db);

    const hops_json = try executeSqlite(std.testing.allocator, db,
        \\WORKSPACE immeuble-demo
        \\MATCH (b:building {name: 'Résidence Les Tilleuls'})-[r:contains]->(x:unit)
        \\HOPS 1..2
        \\WHERE r.relation_type IN ('contains')
        \\PROJECT x.entity_id, x.entity_type, x.name
        \\LIMIT 20
    );
    defer std.testing.allocator.free(hops_json);
    try std.testing.expect(std.mem.indexOf(u8, hops_json, "Tilleuls Appartement A3") != null);

    const bundle_json = try executeSqlite(std.testing.allocator, db,
        \\WORKSPACE seo-audit
        \\MATCH (pr:ProjectionResult)
        \\WHERE pr.metadata.projection_id = 'proj_keyword_opportunities'
        \\PROJECT BUNDLE projection_get
        \\LIMIT 1
    );
    defer std.testing.allocator.free(bundle_json);
    try std.testing.expect(std.mem.indexOf(u8, bundle_json, "\"bundle\":\"projection_get\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle_json, "keyword opportunity set") != null);
}

test "graph pattern parser rejects key invalid syntaxes" {
    try std.testing.expectError(Error.MissingWorkspace, parse(std.testing.allocator,
        \\MATCH (u:unit)
        \\PROJECT u.name
        \\LIMIT 1
    ));
    try std.testing.expectError(Error.InvalidPredicate, parse(std.testing.allocator,
        \\WORKSPACE immeuble-demo
        \\MATCH (u:unit)
        \\HOPS 3..1
        \\PROJECT u.name
        \\LIMIT 1
    ));
    try std.testing.expectError(Error.InvalidLimit, parse(std.testing.allocator,
        \\WORKSPACE immeuble-demo
        \\MATCH (u:unit)
        \\PROJECT u.name
        \\LIMIT 0
    ));
}

test "graph pattern executor rejects unsafe dynamic path fragments" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try setupGraphPatternFixture(db);

    try std.testing.expectError(Error.InvalidPredicate, executeSqlite(std.testing.allocator, db,
        \\WORKSPACE immeuble-demo
        \\MATCH (p:person)-[o:owns]->(u:unit)
        \\WHERE o.prop.quote_part' = 0.5
        \\PROJECT p.name, u.name
        \\LIMIT 20
    ));

    try std.testing.expectError(Error.InvalidPredicate, executeSqlite(std.testing.allocator, db,
        \\WORKSPACE immeuble-demo
        \\MATCH (u:unit)
        \\PROJECT u.metadata.lot')
        \\LIMIT 20
    ));
}
