const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const ontology_sqlite = @import("ontology_sqlite.zig");

const c = facet_sqlite.c;
const Database = facet_sqlite.Database;

pub const Direction = enum {
    out,
    in,
    either,

    fn parse(value: []const u8) ?Direction {
        if (std.mem.eql(u8, value, "out")) return .out;
        if (std.mem.eql(u8, value, "in")) return .in;
        if (std.mem.eql(u8, value, "either")) return .either;
        return null;
    }

    fn label(self: Direction) []const u8 {
        return switch (self) {
            .out => "out",
            .in => "in",
            .either => "either",
        };
    }
};

pub const DiagnosticsOptions = struct {
    workspace_id: []const u8,
    ontology_id: ?[]const u8 = null,
    limit: usize = 200,
    component_small_max: usize = 2,
};

const GapRule = struct {
    rule_id: []u8,
    ontology_id: []u8,
    workspace_id: ?[]u8,
    entity_type: []u8,
    relation_type: []u8,
    direction: Direction,
    target_entity_type: ?[]u8,
    min_count: i64,
    max_count: ?i64,
    severity: []u8,
    label: []u8,
    enabled: bool,
    metadata_json: []u8,

    fn deinit(self: GapRule, allocator: std.mem.Allocator) void {
        allocator.free(self.rule_id);
        allocator.free(self.ontology_id);
        if (self.workspace_id) |value| allocator.free(value);
        allocator.free(self.entity_type);
        allocator.free(self.relation_type);
        if (self.target_entity_type) |value| allocator.free(value);
        allocator.free(self.severity);
        allocator.free(self.label);
        allocator.free(self.metadata_json);
    }
};

pub const Issue = struct {
    kind: []u8,
    severity: []u8,
    label: []u8,
    suggested_action: []u8,
    entity_id: ?u64 = null,
    relation_id: ?u64 = null,
    rule_id: ?[]u8 = null,
    observed_count: ?i64 = null,
    expected_min: ?i64 = null,
    expected_max: ?i64 = null,

    fn deinit(self: Issue, allocator: std.mem.Allocator) void {
        allocator.free(self.kind);
        allocator.free(self.severity);
        allocator.free(self.label);
        allocator.free(self.suggested_action);
        if (self.rule_id) |value| allocator.free(value);
    }
};

pub const Summary = struct {
    workspace_id: []u8,
    ontology_id: ?[]u8,
    rules_evaluated: usize,
    issues_total: usize,
    missing_required_relations: usize = 0,
    cardinality_violations: usize = 0,
    isolated_entities: usize = 0,
    small_components: usize = 0,
    relation_type_mismatches: usize = 0,
    evidence_gaps: usize = 0,
    ontology_coverage_gaps: usize = 0,

    fn deinit(self: Summary, allocator: std.mem.Allocator) void {
        allocator.free(self.workspace_id);
        if (self.ontology_id) |value| allocator.free(value);
    }
};

pub const Report = struct {
    summary: Summary,
    issues: []Issue,

    pub fn deinit(self: Report, allocator: std.mem.Allocator) void {
        self.summary.deinit(allocator);
        for (self.issues) |issue| issue.deinit(allocator);
        allocator.free(self.issues);
    }
};

const RuleImportEnvelope = struct {
    ontology_id: ?[]const u8 = null,
    workspace_id: ?[]const u8 = null,
    replace: bool = false,
    rules: []RuleImportRow = &.{},
};

const RuleImportRow = struct {
    rule_id: []const u8,
    ontology_id: ?[]const u8 = null,
    workspace_id: ?[]const u8 = null,
    entity_type: []const u8,
    relation_type: []const u8,
    direction: []const u8 = "out",
    target_entity_type: ?[]const u8 = null,
    min_count: i64 = 1,
    max_count: ?i64 = null,
    severity: []const u8 = "warning",
    label: []const u8,
    enabled: bool = true,
    metadata_json: []const u8 = "{}",
};

pub fn resolveOntologyId(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8, explicit: ?[]const u8) ![]u8 {
    if (explicit) |ontology_id| return try allocator.dupe(u8, ontology_id);
    const stmt = try facet_sqlite.prepare(db, "SELECT default_ontology_id FROM workspace_settings WHERE workspace_id = ?1 AND default_ontology_id IS NOT NULL");
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.NotFound;
    return try dupeColumnText(allocator, stmt, 0);
}

pub fn importRulesJson(db: Database, allocator: std.mem.Allocator, json: []const u8) !usize {
    var parsed = try std.json.parseFromSlice(RuleImportEnvelope, allocator, json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    });
    defer parsed.deinit();
    const envelope = parsed.value;

    if (envelope.rules.len == 0) return 0;
    const scope_ontology = envelope.ontology_id orelse envelope.rules[0].ontology_id orelse return error.InvalidArguments;
    if (scope_ontology.len == 0) return error.InvalidArguments;

    if (envelope.replace) {
        const delete_sql = if (envelope.workspace_id) |_| "DELETE FROM graph_gap_rules WHERE ontology_id = ?1 AND workspace_id = ?2" else "DELETE FROM graph_gap_rules WHERE ontology_id = ?1 AND workspace_id IS NULL";
        const delete_stmt = try facet_sqlite.prepare(db, delete_sql);
        defer facet_sqlite.finalize(delete_stmt);
        try facet_sqlite.bindText(delete_stmt, 1, scope_ontology);
        if (envelope.workspace_id) |workspace_id| try facet_sqlite.bindText(delete_stmt, 2, workspace_id);
        try facet_sqlite.stepDone(delete_stmt);
    }

    const stmt = try facet_sqlite.prepare(db,
        \\INSERT INTO graph_gap_rules(
        \\  rule_id, ontology_id, workspace_id, entity_type, relation_type,
        \\  direction, target_entity_type, min_count, max_count, severity,
        \\  label, enabled, metadata_json, updated_at
        \\)
        \\VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, CURRENT_TIMESTAMP)
        \\ON CONFLICT(rule_id) DO UPDATE SET
        \\  ontology_id = excluded.ontology_id,
        \\  workspace_id = excluded.workspace_id,
        \\  entity_type = excluded.entity_type,
        \\  relation_type = excluded.relation_type,
        \\  direction = excluded.direction,
        \\  target_entity_type = excluded.target_entity_type,
        \\  min_count = excluded.min_count,
        \\  max_count = excluded.max_count,
        \\  severity = excluded.severity,
        \\  label = excluded.label,
        \\  enabled = excluded.enabled,
        \\  metadata_json = excluded.metadata_json,
        \\  updated_at = CURRENT_TIMESTAMP
    );
    defer facet_sqlite.finalize(stmt);

    var imported: usize = 0;
    for (envelope.rules) |rule| {
        if (rule.rule_id.len == 0 or rule.entity_type.len == 0 or rule.relation_type.len == 0 or rule.label.len == 0) return error.InvalidArguments;
        _ = Direction.parse(rule.direction) orelse return error.InvalidArguments;
        const ontology_id = rule.ontology_id orelse envelope.ontology_id orelse return error.InvalidArguments;
        const workspace_id = rule.workspace_id orelse envelope.workspace_id;

        try facet_sqlite.resetStatement(stmt);
        try facet_sqlite.bindText(stmt, 1, rule.rule_id);
        try facet_sqlite.bindText(stmt, 2, ontology_id);
        if (workspace_id) |value| try facet_sqlite.bindText(stmt, 3, value) else try facet_sqlite.bindNull(stmt, 3);
        try facet_sqlite.bindText(stmt, 4, rule.entity_type);
        try facet_sqlite.bindText(stmt, 5, rule.relation_type);
        try facet_sqlite.bindText(stmt, 6, rule.direction);
        if (rule.target_entity_type) |value| try facet_sqlite.bindText(stmt, 7, value) else try facet_sqlite.bindNull(stmt, 7);
        try facet_sqlite.bindInt64(stmt, 8, rule.min_count);
        if (rule.max_count) |value| try facet_sqlite.bindInt64(stmt, 9, value) else try facet_sqlite.bindNull(stmt, 9);
        try facet_sqlite.bindText(stmt, 10, rule.severity);
        try facet_sqlite.bindText(stmt, 11, rule.label);
        const enabled_int: i64 = if (rule.enabled) 1 else 0;
        try facet_sqlite.bindInt64(stmt, 12, enabled_int);
        try facet_sqlite.bindText(stmt, 13, rule.metadata_json);
        try facet_sqlite.stepDone(stmt);
        imported += 1;
    }
    return imported;
}

pub fn rulesJson(db: Database, allocator: std.mem.Allocator, ontology_id: []const u8, workspace_id: ?[]const u8) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.writeAll("{\"kind\":\"graph_gap_rules\",\"ontology_id\":");
    try out.writer.print("{f},\"workspace_id\":", .{std.json.fmt(ontology_id, .{})});
    try writeOptionalJsonString(&out.writer, workspace_id);
    try out.writer.writeAll(",\"rules\":[");

    const sql =
        \\SELECT rule_id, ontology_id, workspace_id, entity_type, relation_type, direction,
        \\       target_entity_type, min_count, max_count, severity, label, enabled, metadata_json
        \\FROM graph_gap_rules
        \\WHERE ontology_id = ?1
        \\  AND (workspace_id IS NULL OR workspace_id = ?2)
        \\ORDER BY rule_id
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, ontology_id);
    try facet_sqlite.bindText(stmt, 2, workspace_id orelse "");
    var first = true;
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
        if (!first) try out.writer.writeAll(",");
        first = false;
        try out.writer.writeAll("{");
        try writeJsonField(&out.writer, "rule_id", columnText(stmt, 0));
        try out.writer.writeAll(",");
        try writeJsonField(&out.writer, "ontology_id", columnText(stmt, 1));
        try out.writer.writeAll(",\"workspace_id\":");
        try writeOptionalColumnJsonString(&out.writer, stmt, 2);
        try out.writer.writeAll(",");
        try writeJsonField(&out.writer, "entity_type", columnText(stmt, 3));
        try out.writer.writeAll(",");
        try writeJsonField(&out.writer, "relation_type", columnText(stmt, 4));
        try out.writer.writeAll(",");
        try writeJsonField(&out.writer, "direction", columnText(stmt, 5));
        try out.writer.writeAll(",\"target_entity_type\":");
        try writeOptionalColumnJsonString(&out.writer, stmt, 6);
        try out.writer.print(",\"min_count\":{},\"max_count\":", .{c.sqlite3_column_int64(stmt, 7)});
        try writeOptionalColumnInt(&out.writer, stmt, 8);
        try out.writer.writeAll(",");
        try writeJsonField(&out.writer, "severity", columnText(stmt, 9));
        try out.writer.writeAll(",");
        try writeJsonField(&out.writer, "label", columnText(stmt, 10));
        try out.writer.print(",\"enabled\":{}", .{c.sqlite3_column_int64(stmt, 11) != 0});
        try out.writer.writeAll(",\"metadata\":");
        try writeRawJsonObject(&out.writer, columnText(stmt, 12));
        try out.writer.writeAll("}");
    }
    try out.writer.writeAll("]}");
    return try out.toOwnedSlice();
}

pub fn buildReport(db: Database, allocator: std.mem.Allocator, options: DiagnosticsOptions) !Report {
    const ontology_id = try resolveOntologyId(db, allocator, options.workspace_id, options.ontology_id);
    errdefer allocator.free(ontology_id);
    defer allocator.free(ontology_id);

    const rules = try loadRules(db, allocator, ontology_id, options.workspace_id);
    defer {
        for (rules) |rule| rule.deinit(allocator);
        allocator.free(rules);
    }

    var issues = std.ArrayList(Issue).empty;
    defer {
        for (issues.items) |issue| issue.deinit(allocator);
        issues.deinit(allocator);
    }
    var summary = Summary{
        .workspace_id = try allocator.dupe(u8, options.workspace_id),
        .ontology_id = try allocator.dupe(u8, ontology_id),
        .rules_evaluated = rules.len,
        .issues_total = 0,
    };
    errdefer summary.deinit(allocator);

    try evaluateRules(db, allocator, options, rules, &issues, &summary);
    try evaluateRelationTypeMismatches(db, allocator, options, ontology_id, &issues, &summary);
    try evaluateIsolatedEntities(db, allocator, options, &issues, &summary);
    try evaluateSmallComponents(db, allocator, options, &issues, &summary);
    try evaluateEvidenceGaps(db, allocator, options, &issues, &summary);
    try appendCoverageGaps(db, allocator, options, ontology_id, &issues, &summary);

    summary.issues_total = issues.items.len;
    return .{
        .summary = summary,
        .issues = try issues.toOwnedSlice(allocator),
    };
}

pub fn reportJson(allocator: std.mem.Allocator, report: Report) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.writeAll("{\"kind\":\"graph_diagnostics_report\",\"summary\":");
    try writeSummaryJson(&out.writer, report.summary);
    try out.writer.writeAll(",\"issues\":[");
    for (report.issues, 0..) |issue, index| {
        if (index > 0) try out.writer.writeAll(",");
        try writeIssueJson(&out.writer, issue);
    }
    try out.writer.writeAll("]}");
    return try out.toOwnedSlice();
}

pub fn reportToon(allocator: std.mem.Allocator, report: Report) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.writeAll("kind: graph_diagnostics_report\nsummary:\n");
    try out.writer.print("  workspace_id: {s}\n", .{report.summary.workspace_id});
    if (report.summary.ontology_id) |ontology_id| try out.writer.print("  ontology_id: {s}\n", .{ontology_id});
    try out.writer.print("  rules_evaluated: {}\n  issues_total: {}\n", .{ report.summary.rules_evaluated, report.summary.issues_total });
    try out.writer.writeAll("issues:\n");
    for (report.issues) |issue| {
        try out.writer.print("  - kind: {s}\n    severity: {s}\n    label: {s}\n", .{ issue.kind, issue.severity, issue.label });
        if (issue.entity_id) |value| try out.writer.print("    entity_id: {}\n", .{value});
        if (issue.relation_id) |value| try out.writer.print("    relation_id: {}\n", .{value});
        if (issue.rule_id) |value| try out.writer.print("    rule_id: {s}\n", .{value});
        try out.writer.print("    suggested_action: {s}\n", .{issue.suggested_action});
    }
    return try out.toOwnedSlice();
}

fn loadRules(db: Database, allocator: std.mem.Allocator, ontology_id: []const u8, workspace_id: []const u8) ![]GapRule {
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT rule_id, ontology_id, workspace_id, entity_type, relation_type, direction,
        \\       target_entity_type, min_count, max_count, severity, label, enabled, metadata_json
        \\FROM graph_gap_rules
        \\WHERE ontology_id = ?1
        \\  AND enabled != 0
        \\  AND (workspace_id IS NULL OR workspace_id = ?2)
        \\ORDER BY rule_id
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, ontology_id);
    try facet_sqlite.bindText(stmt, 2, workspace_id);

    var rows = std.ArrayList(GapRule).empty;
    errdefer {
        for (rows.items) |rule| rule.deinit(allocator);
        rows.deinit(allocator);
    }
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
        const direction = Direction.parse(columnText(stmt, 5)) orelse return error.InvalidArguments;
        try rows.append(allocator, .{
            .rule_id = try dupeColumnText(allocator, stmt, 0),
            .ontology_id = try dupeColumnText(allocator, stmt, 1),
            .workspace_id = try optionalColumnText(allocator, stmt, 2),
            .entity_type = try dupeColumnText(allocator, stmt, 3),
            .relation_type = try dupeColumnText(allocator, stmt, 4),
            .direction = direction,
            .target_entity_type = try optionalColumnText(allocator, stmt, 6),
            .min_count = c.sqlite3_column_int64(stmt, 7),
            .max_count = optionalColumnI64(stmt, 8),
            .severity = try dupeColumnText(allocator, stmt, 9),
            .label = try dupeColumnText(allocator, stmt, 10),
            .enabled = c.sqlite3_column_int64(stmt, 11) != 0,
            .metadata_json = try dupeColumnText(allocator, stmt, 12),
        });
    }
    return try rows.toOwnedSlice(allocator);
}

fn evaluateRules(db: Database, allocator: std.mem.Allocator, options: DiagnosticsOptions, rules: []const GapRule, issues: *std.ArrayList(Issue), summary: *Summary) !void {
    for (rules) |rule| {
        const entity_stmt = try facet_sqlite.prepare(db,
            \\SELECT entity_id
            \\FROM graph_entity
            \\WHERE workspace_id = ?1 AND entity_type = ?2 AND deprecated_at IS NULL
            \\ORDER BY entity_id
        );
        defer facet_sqlite.finalize(entity_stmt);
        try facet_sqlite.bindText(entity_stmt, 1, options.workspace_id);
        try facet_sqlite.bindText(entity_stmt, 2, rule.entity_type);
        while (c.sqlite3_step(entity_stmt) == c.SQLITE_ROW and issues.items.len < options.limit) {
            const entity_id: u64 = @intCast(c.sqlite3_column_int64(entity_stmt, 0));
            const observed = try countRuleRelations(db, options.workspace_id, entity_id, rule);
            if (observed < rule.min_count) {
                try appendIssue(allocator, issues, .{
                    .kind = "missing_required_relation",
                    .severity = rule.severity,
                    .label = rule.label,
                    .suggested_action = "add_or_confirm_required_relation",
                    .entity_id = entity_id,
                    .rule_id = rule.rule_id,
                    .observed_count = observed,
                    .expected_min = rule.min_count,
                });
                summary.missing_required_relations += 1;
            } else if (rule.max_count) |max_count| {
                if (observed > max_count) {
                    try appendIssue(allocator, issues, .{
                        .kind = "too_many_relations",
                        .severity = rule.severity,
                        .label = rule.label,
                        .suggested_action = "review_duplicate_or_conflicting_relations",
                        .entity_id = entity_id,
                        .rule_id = rule.rule_id,
                        .observed_count = observed,
                        .expected_min = rule.min_count,
                        .expected_max = max_count,
                    });
                    summary.cardinality_violations += 1;
                }
            }
        }
    }
}

const IssueSeed = struct {
    kind: []const u8,
    severity: []const u8,
    label: []const u8,
    suggested_action: []const u8,
    entity_id: ?u64 = null,
    relation_id: ?u64 = null,
    rule_id: ?[]const u8 = null,
    observed_count: ?i64 = null,
    expected_min: ?i64 = null,
    expected_max: ?i64 = null,
};

fn appendIssue(allocator: std.mem.Allocator, issues: *std.ArrayList(Issue), seed: IssueSeed) !void {
    try issues.append(allocator, .{
        .kind = try allocator.dupe(u8, seed.kind),
        .severity = try allocator.dupe(u8, seed.severity),
        .label = try allocator.dupe(u8, seed.label),
        .suggested_action = try allocator.dupe(u8, seed.suggested_action),
        .entity_id = seed.entity_id,
        .relation_id = seed.relation_id,
        .rule_id = if (seed.rule_id) |value| try allocator.dupe(u8, value) else null,
        .observed_count = seed.observed_count,
        .expected_min = seed.expected_min,
        .expected_max = seed.expected_max,
    });
}

fn countRuleRelations(db: Database, workspace_id: []const u8, entity_id: u64, rule: GapRule) !i64 {
    const sql = switch (rule.direction) {
        .out =>
        \\SELECT COUNT(*)
        \\FROM graph_relation r
        \\JOIN graph_entity target ON target.entity_id = r.target_id
        \\WHERE r.workspace_id = ?1 AND r.source_id = ?2 AND r.relation_type = ?3
        \\  AND r.deprecated_at IS NULL
        \\  AND (?4 IS NULL OR target.entity_type = ?4)
        ,
        .in =>
        \\SELECT COUNT(*)
        \\FROM graph_relation r
        \\JOIN graph_entity source ON source.entity_id = r.source_id
        \\WHERE r.workspace_id = ?1 AND r.target_id = ?2 AND r.relation_type = ?3
        \\  AND r.deprecated_at IS NULL
        \\  AND (?4 IS NULL OR source.entity_type = ?4)
        ,
        .either =>
        \\SELECT COUNT(*)
        \\FROM graph_relation r
        \\JOIN graph_entity source ON source.entity_id = r.source_id
        \\JOIN graph_entity target ON target.entity_id = r.target_id
        \\WHERE r.workspace_id = ?1 AND (r.source_id = ?2 OR r.target_id = ?2) AND r.relation_type = ?3
        \\  AND r.deprecated_at IS NULL
        \\  AND (?4 IS NULL OR source.entity_type = ?4 OR target.entity_type = ?4)
        ,
    };
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindInt64(stmt, 2, entity_id);
    try facet_sqlite.bindText(stmt, 3, rule.relation_type);
    if (rule.target_entity_type) |value| try facet_sqlite.bindText(stmt, 4, value) else try facet_sqlite.bindNull(stmt, 4);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StepFailed;
    return c.sqlite3_column_int64(stmt, 0);
}

fn evaluateRelationTypeMismatches(db: Database, allocator: std.mem.Allocator, options: DiagnosticsOptions, ontology_id: []const u8, issues: *std.ArrayList(Issue), summary: *Summary) !void {
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT r.relation_id, r.relation_type, s.entity_type, t.entity_type, oe.source_entity_type, oe.target_entity_type
        \\FROM graph_relation r
        \\JOIN graph_entity s ON s.entity_id = r.source_id
        \\JOIN graph_entity t ON t.entity_id = r.target_id
        \\JOIN ontology_edge_types oe ON oe.ontology_id = ?1 AND oe.edge_type = r.relation_type
        \\WHERE r.workspace_id = ?2 AND r.deprecated_at IS NULL
        \\  AND (
        \\    (oe.source_entity_type IS NOT NULL AND oe.source_entity_type != s.entity_type)
        \\    OR (oe.target_entity_type IS NOT NULL AND oe.target_entity_type != t.entity_type)
        \\  )
        \\ORDER BY r.relation_id
        \\LIMIT ?3
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, ontology_id);
    try facet_sqlite.bindText(stmt, 2, options.workspace_id);
    try facet_sqlite.bindInt64(stmt, 3, @as(i64, @intCast(options.limit)));
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW and issues.items.len < options.limit) {
        const relation_id: u64 = @intCast(c.sqlite3_column_int64(stmt, 0));
        const label = try std.fmt.allocPrint(allocator, "Relation {s} does not match ontology source/target types", .{columnText(stmt, 1)});
        defer allocator.free(label);
        try appendIssue(allocator, issues, .{
            .kind = "relation_type_mismatch",
            .severity = "error",
            .label = label,
            .suggested_action = "review_relation_type_or_endpoint_types",
            .relation_id = relation_id,
        });
        summary.relation_type_mismatches += 1;
    }
}

fn evaluateIsolatedEntities(db: Database, allocator: std.mem.Allocator, options: DiagnosticsOptions, issues: *std.ArrayList(Issue), summary: *Summary) !void {
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT e.entity_id, e.name
        \\FROM graph_entity e
        \\WHERE e.workspace_id = ?1 AND e.deprecated_at IS NULL
        \\  AND NOT EXISTS (SELECT 1 FROM graph_relation r WHERE r.workspace_id = e.workspace_id AND r.deprecated_at IS NULL AND (r.source_id = e.entity_id OR r.target_id = e.entity_id))
        \\ORDER BY e.entity_id
        \\LIMIT ?2
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, options.workspace_id);
    try facet_sqlite.bindInt64(stmt, 2, @as(i64, @intCast(options.limit)));
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW and issues.items.len < options.limit) {
        const label = try std.fmt.allocPrint(allocator, "Entity {s} has no graph relations", .{columnText(stmt, 1)});
        defer allocator.free(label);
        try appendIssue(allocator, issues, .{
            .kind = "isolated_entity",
            .severity = "warning",
            .label = label,
            .suggested_action = "connect_entity_or_confirm_it_is_intentionally_standalone",
            .entity_id = @intCast(c.sqlite3_column_int64(stmt, 0)),
        });
        summary.isolated_entities += 1;
    }
}

const EntityNode = struct { id: u64, name: []u8 };

fn evaluateSmallComponents(db: Database, allocator: std.mem.Allocator, options: DiagnosticsOptions, issues: *std.ArrayList(Issue), summary: *Summary) !void {
    if (options.component_small_max < 2) return;
    const nodes = try loadEntityNodes(db, allocator, options.workspace_id);
    defer {
        for (nodes) |node| allocator.free(node.name);
        allocator.free(nodes);
    }
    if (nodes.len == 0) return;
    var index_by_id = std.AutoHashMap(u64, usize).init(allocator);
    defer index_by_id.deinit();
    var parent = try allocator.alloc(usize, nodes.len);
    defer allocator.free(parent);
    for (nodes, 0..) |node, index| {
        parent[index] = index;
        try index_by_id.put(node.id, index);
    }

    const rel_stmt = try facet_sqlite.prepare(db, "SELECT source_id, target_id FROM graph_relation WHERE workspace_id = ?1 AND deprecated_at IS NULL");
    defer facet_sqlite.finalize(rel_stmt);
    try facet_sqlite.bindText(rel_stmt, 1, options.workspace_id);
    while (c.sqlite3_step(rel_stmt) == c.SQLITE_ROW) {
        const source_id: u64 = @intCast(c.sqlite3_column_int64(rel_stmt, 0));
        const target_id: u64 = @intCast(c.sqlite3_column_int64(rel_stmt, 1));
        if (index_by_id.get(source_id)) |source_index| {
            if (index_by_id.get(target_id)) |target_index| unionParents(parent, source_index, target_index);
        }
    }

    var sizes = std.AutoHashMap(usize, usize).init(allocator);
    defer sizes.deinit();
    for (nodes, 0..) |_, index| {
        const root = findParent(parent, index);
        const entry = try sizes.getOrPut(root);
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* += 1;
    }

    var emitted: usize = 0;
    for (nodes, 0..) |node, index| {
        if (issues.items.len >= options.limit) break;
        const root = findParent(parent, index);
        const size = sizes.get(root) orelse 0;
        if (size <= 1 or size > options.component_small_max) continue;
        if (root != index) continue;
        const label = try std.fmt.allocPrint(allocator, "Small disconnected component of {d} entities around {s}", .{ size, node.name });
        defer allocator.free(label);
        try appendIssue(allocator, issues, .{
            .kind = "small_component",
            .severity = "info",
            .label = label,
            .suggested_action = "review_component_for_missing_bridge_relations",
            .entity_id = node.id,
            .observed_count = @intCast(size),
        });
        emitted += 1;
    }
    summary.small_components += emitted;
}

fn loadEntityNodes(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8) ![]EntityNode {
    const stmt = try facet_sqlite.prepare(db, "SELECT entity_id, name FROM graph_entity WHERE workspace_id = ?1 AND deprecated_at IS NULL ORDER BY entity_id");
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(EntityNode).empty;
    errdefer {
        for (rows.items) |row| allocator.free(row.name);
        rows.deinit(allocator);
    }
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
        try rows.append(allocator, .{
            .id = @intCast(c.sqlite3_column_int64(stmt, 0)),
            .name = try dupeColumnText(allocator, stmt, 1),
        });
    }
    return try rows.toOwnedSlice(allocator);
}

fn findParent(parent: []usize, index: usize) usize {
    var current = index;
    while (parent[current] != current) current = parent[current];
    return current;
}

fn unionParents(parent: []usize, lhs: usize, rhs: usize) void {
    const a = findParent(parent, lhs);
    const b = findParent(parent, rhs);
    if (a != b) parent[b] = a;
}

fn evaluateEvidenceGaps(db: Database, allocator: std.mem.Allocator, options: DiagnosticsOptions, issues: *std.ArrayList(Issue), summary: *Summary) !void {
    const entity_stmt = try facet_sqlite.prepare(db,
        \\SELECT e.entity_id, e.name
        \\FROM graph_entity e
        \\WHERE e.workspace_id = ?1 AND e.deprecated_at IS NULL
        \\  AND NOT EXISTS (SELECT 1 FROM graph_entity_chunk c WHERE c.workspace_id = e.workspace_id AND c.entity_id = e.entity_id)
        \\  AND NOT EXISTS (SELECT 1 FROM graph_entity_document d WHERE d.entity_id = e.entity_id)
        \\ORDER BY e.entity_id
        \\LIMIT ?2
    );
    defer facet_sqlite.finalize(entity_stmt);
    try facet_sqlite.bindText(entity_stmt, 1, options.workspace_id);
    try facet_sqlite.bindInt64(entity_stmt, 2, @as(i64, @intCast(options.limit)));
    while (c.sqlite3_step(entity_stmt) == c.SQLITE_ROW and issues.items.len < options.limit) {
        const label = try std.fmt.allocPrint(allocator, "Entity {s} has no document or chunk evidence", .{columnText(entity_stmt, 1)});
        defer allocator.free(label);
        try appendIssue(allocator, issues, .{
            .kind = "entity_without_evidence",
            .severity = "warning",
            .label = label,
            .suggested_action = "attach_source_document_or_chunk_evidence",
            .entity_id = @intCast(c.sqlite3_column_int64(entity_stmt, 0)),
        });
        summary.evidence_gaps += 1;
    }

    const relation_stmt = try facet_sqlite.prepare(db,
        \\SELECT r.relation_id, r.relation_type
        \\FROM graph_relation r
        \\WHERE r.workspace_id = ?1 AND r.deprecated_at IS NULL
        \\  AND NOT EXISTS (
        \\    SELECT 1 FROM graph_relation_property p
        \\    WHERE p.relation_id = r.relation_id AND p.ref_doc_id IS NOT NULL
        \\  )
        \\ORDER BY r.relation_id
        \\LIMIT ?2
    );
    defer facet_sqlite.finalize(relation_stmt);
    try facet_sqlite.bindText(relation_stmt, 1, options.workspace_id);
    try facet_sqlite.bindInt64(relation_stmt, 2, @as(i64, @intCast(options.limit)));
    while (c.sqlite3_step(relation_stmt) == c.SQLITE_ROW and issues.items.len < options.limit) {
        const label = try std.fmt.allocPrint(allocator, "Relation {s} has no document reference evidence", .{columnText(relation_stmt, 1)});
        defer allocator.free(label);
        try appendIssue(allocator, issues, .{
            .kind = "relation_without_evidence",
            .severity = "warning",
            .label = label,
            .suggested_action = "attach_ref_doc_id_or_relation_evidence",
            .relation_id = @intCast(c.sqlite3_column_int64(relation_stmt, 0)),
        });
        summary.evidence_gaps += 1;
    }
}

fn appendCoverageGaps(db: Database, allocator: std.mem.Allocator, options: DiagnosticsOptions, ontology_id: []const u8, issues: *std.ArrayList(Issue), summary: *Summary) !void {
    _ = ontology_id;
    const coverage = try ontology_sqlite.coverageReport(db, allocator, options.workspace_id, null);
    defer {
        allocator.free(coverage.summary.workspace_id);
        for (coverage.gaps) |gap| {
            allocator.free(gap.id);
            allocator.free(gap.label);
            allocator.free(gap.entity_type);
            allocator.free(gap.criticality);
        }
        allocator.free(coverage.gaps);
    }
    for (coverage.gaps) |gap| {
        if (issues.items.len >= options.limit) break;
        const label = try std.fmt.allocPrint(allocator, "Ontology or taxonomy node {s} is not covered by the graph", .{gap.label});
        defer allocator.free(label);
        try appendIssue(allocator, issues, .{
            .kind = "ontology_coverage_gap",
            .severity = gap.criticality,
            .label = label,
            .suggested_action = "add_matching_graph_entity_or_confirm_out_of_scope",
            .rule_id = gap.id,
        });
        summary.ontology_coverage_gaps += 1;
    }
}

fn writeSummaryJson(writer: *std.Io.Writer, summary: Summary) !void {
    try writer.writeAll("{\"workspace_id\":");
    try writer.print("{f},\"ontology_id\":", .{std.json.fmt(summary.workspace_id, .{})});
    try writeOptionalJsonString(writer, summary.ontology_id);
    try writer.print(
        ",\"rules_evaluated\":{},\"issues_total\":{},\"missing_required_relations\":{},\"cardinality_violations\":{},\"isolated_entities\":{},\"small_components\":{},\"relation_type_mismatches\":{},\"evidence_gaps\":{},\"ontology_coverage_gaps\":{}",
        .{
            summary.rules_evaluated,
            summary.issues_total,
            summary.missing_required_relations,
            summary.cardinality_violations,
            summary.isolated_entities,
            summary.small_components,
            summary.relation_type_mismatches,
            summary.evidence_gaps,
            summary.ontology_coverage_gaps,
        },
    );
    try writer.writeAll("}");
}

fn writeIssueJson(writer: *std.Io.Writer, issue: Issue) !void {
    try writer.writeAll("{");
    try writeJsonField(writer, "kind", issue.kind);
    try writer.writeAll(",");
    try writeJsonField(writer, "severity", issue.severity);
    try writer.writeAll(",");
    try writeJsonField(writer, "label", issue.label);
    try writer.writeAll(",");
    try writeJsonField(writer, "suggested_action", issue.suggested_action);
    try writer.writeAll(",\"entity_id\":");
    try writeOptionalInt(writer, issue.entity_id);
    try writer.writeAll(",\"relation_id\":");
    try writeOptionalInt(writer, issue.relation_id);
    try writer.writeAll(",\"rule_id\":");
    try writeOptionalJsonString(writer, issue.rule_id);
    try writer.writeAll(",\"observed_count\":");
    try writeOptionalSignedInt(writer, issue.observed_count);
    try writer.writeAll(",\"expected_min\":");
    try writeOptionalSignedInt(writer, issue.expected_min);
    try writer.writeAll(",\"expected_max\":");
    try writeOptionalSignedInt(writer, issue.expected_max);
    try writer.writeAll("}");
}

fn writeJsonField(writer: *std.Io.Writer, key: []const u8, value: []const u8) !void {
    try writer.print("\"{s}\":{f}", .{ key, std.json.fmt(value, .{}) });
}

fn writeOptionalJsonString(writer: *std.Io.Writer, value: ?[]const u8) !void {
    if (value) |text| try writer.print("{f}", .{std.json.fmt(text, .{})}) else try writer.writeAll("null");
}

fn writeOptionalInt(writer: *std.Io.Writer, value: ?u64) !void {
    if (value) |int| try writer.print("{}", .{int}) else try writer.writeAll("null");
}

fn writeOptionalSignedInt(writer: *std.Io.Writer, value: ?i64) !void {
    if (value) |int| try writer.print("{}", .{int}) else try writer.writeAll("null");
}

fn writeOptionalColumnInt(writer: *std.Io.Writer, stmt: *c.sqlite3_stmt, column: c_int) !void {
    if (c.sqlite3_column_type(stmt, column) == c.SQLITE_NULL) try writer.writeAll("null") else try writer.print("{}", .{c.sqlite3_column_int64(stmt, column)});
}

fn writeOptionalColumnJsonString(writer: *std.Io.Writer, stmt: *c.sqlite3_stmt, column: c_int) !void {
    if (c.sqlite3_column_type(stmt, column) == c.SQLITE_NULL) try writer.writeAll("null") else try writer.print("{f}", .{std.json.fmt(columnText(stmt, column), .{})});
}

fn writeRawJsonObject(writer: *std.Io.Writer, value: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, value, .{}) catch {
        try writer.writeAll("{}");
        return;
    };
    defer parsed.deinit();
    if (parsed.value != .object) {
        try writer.writeAll("{}");
        return;
    }
    try writer.writeAll(value);
}

fn columnText(stmt: *c.sqlite3_stmt, column: c_int) []const u8 {
    return std.mem.span(c.sqlite3_column_text(stmt, column) orelse return "");
}

fn dupeColumnText(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, column: c_int) ![]u8 {
    return try allocator.dupe(u8, columnText(stmt, column));
}

fn optionalColumnText(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, column: c_int) !?[]u8 {
    if (c.sqlite3_column_type(stmt, column) == c.SQLITE_NULL) return null;
    return try dupeColumnText(allocator, stmt, column);
}

fn optionalColumnI64(stmt: *c.sqlite3_stmt, column: c_int) ?i64 {
    if (c.sqlite3_column_type(stmt, column) == c.SQLITE_NULL) return null;
    return c.sqlite3_column_int64(stmt, column);
}

test "graph diagnostics report finds rule cardinality type evidence and component gaps" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_diag', 'ws_diag', 'Diagnostics');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('ws_diag::core', 'ws_diag', 'core');
        \\INSERT INTO workspace_settings(workspace_id, default_ontology_id) VALUES ('ws_diag', 'ws_diag::core');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_diag::core', 'building', 'Building');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_diag::core', 'unit', 'Unit');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_diag::core', 'person', 'Person');
        \\INSERT INTO ontology_edge_types(ontology_id, edge_type, source_entity_type, target_entity_type) VALUES ('ws_diag::core', 'part_of', 'unit', 'building');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (1, 'ws_diag', 'unit', 'A1');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (2, 'ws_diag', 'building', 'Building A');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (3, 'ws_diag', 'building', 'Building B');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (4, 'ws_diag', 'person', 'Loose Person');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (5, 'ws_diag', 'person', 'Island A');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (6, 'ws_diag', 'person', 'Island B');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (7, 'ws_diag', 'person', 'Wrong Source');
        \\INSERT INTO graph_relation(relation_id, workspace_id, relation_type, source_id, target_id) VALUES (10, 'ws_diag', 'part_of', 1, 2);
        \\INSERT INTO graph_relation(relation_id, workspace_id, relation_type, source_id, target_id) VALUES (11, 'ws_diag', 'part_of', 1, 3);
        \\INSERT INTO graph_relation(relation_id, workspace_id, relation_type, source_id, target_id) VALUES (12, 'ws_diag', 'related_to', 5, 6);
        \\INSERT INTO graph_relation(relation_id, workspace_id, relation_type, source_id, target_id) VALUES (13, 'ws_diag', 'part_of', 7, 2);
    );
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_diag::core","workspace_id":"ws_diag","rules":[{"rule_id":"unit-one-building","entity_type":"unit","relation_type":"part_of","direction":"out","target_entity_type":"building","min_count":1,"max_count":1,"severity":"error","label":"Unit must belong to one building"}]}
    );

    var report = try buildReport(db, std.testing.allocator, .{
        .workspace_id = "ws_diag",
        .component_small_max = 2,
        .limit = 50,
    });
    defer report.deinit(std.testing.allocator);

    try std.testing.expect(report.summary.cardinality_violations >= 1);
    try std.testing.expect(report.summary.relation_type_mismatches >= 1);
    try std.testing.expect(report.summary.isolated_entities >= 1);
    try std.testing.expect(report.summary.small_components >= 1);
    try std.testing.expect(report.summary.evidence_gaps >= 1);
}
