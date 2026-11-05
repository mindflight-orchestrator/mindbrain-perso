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
    validation_mode: []const u8 = "warn",
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

pub const ReconciliationOptions = struct {
    workspace_id: []const u8,
    ontology_id: ?[]const u8 = null,
    limit: usize = 200,
};

pub const RuleEvaluationOptions = struct {
    workspace_id: []const u8,
    ontology_id: ?[]const u8 = null,
    limit: usize = 200,
    create_remediation_actions: bool = true,
};

pub const RuleEvent = struct {
    event_id: []u8,
    rule_id: []u8,
    subject_entity_id: u64,
    from_state: []u8,
    to_state: []u8,
    observed_count: i64,
    expected_min: i64,
    expected_max: ?i64,
    idempotency_key: []u8,
    created_at_unix: i64,

    fn deinit(self: RuleEvent, allocator: std.mem.Allocator) void {
        allocator.free(self.event_id);
        allocator.free(self.rule_id);
        allocator.free(self.from_state);
        allocator.free(self.to_state);
        allocator.free(self.idempotency_key);
    }
};

pub const RuleEvaluationRun = struct {
    workspace_id: []u8,
    ontology_id: []u8,
    evaluated: usize,
    changed: usize,
    events_created: usize,
    invalid_count: usize,
    remediation_actions_created: usize,
    events: []RuleEvent,

    pub fn deinit(self: RuleEvaluationRun, allocator: std.mem.Allocator) void {
        allocator.free(self.workspace_id);
        allocator.free(self.ontology_id);
        for (self.events) |event| event.deinit(allocator);
        allocator.free(self.events);
    }
};

pub fn resolveOntologyId(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8, explicit: ?[]const u8) ![]u8 {
    if (explicit) |ontology_id| return try allocator.dupe(u8, ontology_id);
    const stmt = try facet_sqlite.prepare(db, "SELECT default_ontology_id FROM workspace_settings WHERE workspace_id = ?1 AND default_ontology_id IS NOT NULL");
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (c.sqlite3_step(stmt) == c.SQLITE_ROW) return try dupeColumnText(allocator, stmt, 0);
    if (try fallbackOntologyId(db, allocator, workspace_id)) |ontology_id| return ontology_id;
    return try std.fmt.allocPrint(allocator, "{s}::default", .{workspace_id});
}

fn fallbackOntologyId(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8) !?[]u8 {
    const core_id = try std.fmt.allocPrint(allocator, "{s}::core", .{workspace_id});
    defer allocator.free(core_id);
    const default_id = try std.fmt.allocPrint(allocator, "{s}::default", .{workspace_id});
    defer allocator.free(default_id);
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT ontology_id
        \\FROM ontologies
        \\WHERE workspace_id = ?1
        \\ORDER BY CASE
        \\  WHEN ontology_id = ?1 THEN 0
        \\  WHEN ontology_id = ?2 THEN 1
        \\  WHEN ontology_id = ?3 THEN 2
        \\  ELSE 3
        \\END, ontology_id
        \\LIMIT 1
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, core_id);
    try facet_sqlite.bindText(stmt, 3, default_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return null;
    return try dupeColumnText(allocator, stmt, 0);
}

fn rowExists1(db: Database, sql: []const u8, value: []const u8) !bool {
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, value);
    return c.sqlite3_step(stmt) == c.SQLITE_ROW;
}

fn rowExists2(db: Database, sql: []const u8, a: []const u8, b: []const u8) !bool {
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, a);
    try facet_sqlite.bindText(stmt, 2, b);
    return c.sqlite3_step(stmt) == c.SQLITE_ROW;
}

fn ontologyExists(db: Database, ontology_id: []const u8) !bool {
    return rowExists1(db, "SELECT 1 FROM ontologies WHERE ontology_id = ?1 LIMIT 1", ontology_id);
}

fn ontologyEntityTypeExists(db: Database, ontology_id: []const u8, entity_type: []const u8) !bool {
    return rowExists2(db, "SELECT 1 FROM ontology_entity_types WHERE ontology_id = ?1 AND entity_type = ?2 LIMIT 1", ontology_id, entity_type);
}

fn ontologyEdgeTypeExists(db: Database, ontology_id: []const u8, edge_type: []const u8) !bool {
    return rowExists2(db, "SELECT 1 FROM ontology_edge_types WHERE ontology_id = ?1 AND edge_type = ?2 LIMIT 1", ontology_id, edge_type);
}

fn gapRuleReferencesOntology(db: Database, ontology_id: []const u8, rule: RuleImportRow) !bool {
    if (!try ontologyEntityTypeExists(db, ontology_id, rule.entity_type)) return false;
    if (!try ontologyEdgeTypeExists(db, ontology_id, rule.relation_type)) return false;
    if (rule.target_entity_type) |target| {
        if (!try ontologyEntityTypeExists(db, ontology_id, target)) return false;
    }
    return true;
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
    const strict_validation = if (std.mem.eql(u8, envelope.validation_mode, "strict"))
        true
    else if (std.mem.eql(u8, envelope.validation_mode, "warn"))
        false
    else
        return error.InvalidArguments;

    // One transaction for the whole import: a mid-loop validation or bind
    // failure in replace mode used to lose the deleted rules while keeping
    // only the rows inserted so far.
    var tx = try facet_sqlite.Transaction.begin(db);
    defer tx.deinit();

    if (envelope.replace) {
        const workspace_id = envelope.workspace_id orelse return error.MissingWorkspace;
        const delete_sql = "DELETE FROM graph_gap_rules WHERE ontology_id = ?1 AND workspace_id = ?2";
        const delete_stmt = try facet_sqlite.prepare(db, delete_sql);
        defer facet_sqlite.finalize(delete_stmt);
        try facet_sqlite.bindText(delete_stmt, 1, scope_ontology);
        try facet_sqlite.bindText(delete_stmt, 2, workspace_id);
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
        const workspace_id = rule.workspace_id orelse envelope.workspace_id orelse return error.MissingWorkspace;
        if (strict_validation and !try gapRuleReferencesOntology(db, ontology_id, rule)) return error.InvalidArguments;
        var rule_filter = try RuleFilter.init(allocator, rule.metadata_json);
        defer rule_filter.deinit();
        if (rule_filter.invalid) {
            std.log.warn("gap rule '{s}' rejected: invalid entity_filter in metadata_json", .{rule.rule_id});
            return error.InvalidArguments;
        }

        try facet_sqlite.resetStatement(stmt);
        try facet_sqlite.bindText(stmt, 1, rule.rule_id);
        try facet_sqlite.bindText(stmt, 2, ontology_id);
        try facet_sqlite.bindText(stmt, 3, workspace_id);
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
    try tx.commit();
    return imported;
}

const RuleDeleteEnvelope = struct {
    rule_ids: []const []const u8,
    ontology_id: ?[]const u8 = null,
    workspace_id: ?[]const u8 = null,
};

pub fn deleteRulesJson(db: Database, allocator: std.mem.Allocator, json: []const u8) !usize {
    var parsed = try std.json.parseFromSlice(RuleDeleteEnvelope, allocator, json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    });
    defer parsed.deinit();
    const envelope = parsed.value;
    if (envelope.rule_ids.len == 0) return 0;
    const workspace_id = envelope.workspace_id orelse return error.MissingWorkspace;

    var deleted: usize = 0;
    for (envelope.rule_ids) |rule_id| {
        if (rule_id.len == 0) return error.InvalidArguments;
        const sql = if (envelope.ontology_id) |_| "DELETE FROM graph_gap_rules WHERE rule_id = ?1 AND ontology_id = ?2 AND workspace_id = ?3" else "DELETE FROM graph_gap_rules WHERE rule_id = ?1 AND workspace_id = ?2";
        const stmt = try facet_sqlite.prepare(db, sql);
        defer facet_sqlite.finalize(stmt);
        try facet_sqlite.bindText(stmt, 1, rule_id);
        if (envelope.ontology_id) |ontology_id| {
            try facet_sqlite.bindText(stmt, 2, ontology_id);
            try facet_sqlite.bindText(stmt, 3, workspace_id);
        } else {
            try facet_sqlite.bindText(stmt, 2, workspace_id);
        }
        try facet_sqlite.stepDone(stmt);
        if (c.sqlite3_changes(db.handle) > 0) deleted += 1;
    }
    return deleted;
}

fn metadataFieldString(allocator: std.mem.Allocator, metadata_json: []const u8, field: []const u8) !?[]const u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, metadata_json, .{
        .allocate = .alloc_if_needed,
    });
    defer parsed.deinit();
    const root = parsed.value;
    if (root != .object) return null;
    const value = root.object.get(field) orelse return null;
    return switch (value) {
        .string => |text| try allocator.dupe(u8, text),
        .integer => |number| try std.fmt.allocPrint(allocator, "{d}", .{number}),
        .float => |number| try std.fmt.allocPrint(allocator, "{d}", .{number}),
        .bool => |flag| try allocator.dupe(u8, if (flag) "true" else "false"),
        else => null,
    };
}

const filter_operators = [_][]const u8{ "eq", "one_of", "not_one_of", "gt", "gte", "lt", "lte" };

fn isKnownFilterOperator(key: []const u8) bool {
    for (filter_operators) |op| {
        if (std.mem.eql(u8, key, op)) return true;
    }
    return false;
}

fn isScalarFilterValue(value: std.json.Value) bool {
    return switch (value) {
        .string, .integer, .float, .bool => true,
        else => false,
    };
}

fn orderedThresholdIsValid(value: std.json.Value) bool {
    return switch (value) {
        .integer => true,
        .float => |number| std.math.isFinite(number),
        .string => |text| parseIsoInstant(text) != null or parseExactNumber(text) != null,
        else => false,
    };
}

/// A field filter must be a non-empty object of known operators whose operands
/// can actually be evaluated. Anything else used to select every entity or none.
fn fieldFilterIsValid(filter_value: std.json.Value) bool {
    if (filter_value != .object) return false;
    if (filter_value.object.count() == 0) return false;
    var iterator = filter_value.object.iterator();
    while (iterator.next()) |entry| {
        const key = entry.key_ptr.*;
        const operand = entry.value_ptr.*;
        if (!isKnownFilterOperator(key)) return false;
        if (std.mem.eql(u8, key, "eq")) {
            if (!isScalarFilterValue(operand)) return false;
        } else if (std.mem.eql(u8, key, "one_of") or std.mem.eql(u8, key, "not_one_of")) {
            if (operand != .array) return false;
            if (std.mem.eql(u8, key, "one_of") and operand.array.items.len == 0) return false;
            for (operand.array.items) |item| {
                if (!isScalarFilterValue(item)) return false;
            }
        } else if (!orderedThresholdIsValid(operand)) {
            return false;
        }
    }
    return true;
}

/// `entity_filters`, `entityFilter`, `entity-filter`: misspellings that would
/// otherwise evaluate the rule against every entity of its type.
fn isMisspelledEntityFilterKey(key: []const u8) bool {
    if (std.mem.eql(u8, key, "entity_filter")) return false;
    const expected = "entityfilter";
    var matched: usize = 0;
    for (key) |ch| {
        if (ch == '_' or ch == '-') continue;
        if (matched == expected.len) return true;
        if (std.ascii.toLower(ch) != expected[matched]) return false;
        matched += 1;
    }
    return matched == expected.len;
}

fn ruleMetadataFilterIsValid(root: std.json.Value) bool {
    if (root != .object) return true;
    var root_iterator = root.object.iterator();
    while (root_iterator.next()) |entry| {
        if (isMisspelledEntityFilterKey(entry.key_ptr.*)) return false;
    }
    const entity_filter = root.object.get("entity_filter") orelse return true;
    if (entity_filter != .object) return false;
    var filter_iterator = entity_filter.object.iterator();
    while (filter_iterator.next()) |entry| {
        if (!std.mem.eql(u8, entry.key_ptr.*, "metadata")) return false;
    }
    const metadata = entity_filter.object.get("metadata") orelse return true;
    if (metadata != .object) return false;
    var field_iterator = metadata.object.iterator();
    while (field_iterator.next()) |entry| {
        if (!fieldFilterIsValid(entry.value_ptr.*)) return false;
    }
    return true;
}

fn jsonValueAsCompareText(allocator: std.mem.Allocator, value: std.json.Value) !?[]u8 {
    return switch (value) {
        .string => |text| try allocator.dupe(u8, text),
        .integer => |number| try std.fmt.allocPrint(allocator, "{d}", .{number}),
        .float => |number| try std.fmt.allocPrint(allocator, "{d}", .{number}),
        .bool => |flag| try allocator.dupe(u8, if (flag) "true" else "false"),
        else => null,
    };
}

fn parseExactNumber(text: []const u8) ?f64 {
    const trimmed = std.mem.trim(u8, text, " \t");
    if (trimmed.len == 0) return null;
    for (trimmed) |ch| {
        const numeric = (ch >= '0' and ch <= '9') or ch == '.' or ch == '-' or ch == '+' or ch == 'e' or ch == 'E';
        if (!numeric) return null;
    }
    const number = std.fmt.parseFloat(f64, trimmed) catch return null;
    return if (std.math.isFinite(number)) number else null;
}

fn daysFromCivil(year: i32, month: u32, day: u32) i64 {
    var y: i32 = year;
    const m: i32 = @intCast(month);
    const d: i32 = @intCast(day);
    if (m <= 2) y -= 1;
    const era: i32 = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe: u32 = @intCast(y - era * 400);
    const mp: u32 = @intCast(if (m > 2) m - 3 else m + 9);
    const doy: u32 = (153 * mp + 2) / 5 + @as(u32, @intCast(d)) - 1;
    const doe: u32 = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return @as(i64, era) * 146097 + @as(i64, doe) - 719468;
}

fn parseDigits(text: []const u8) ?u32 {
    if (text.len == 0) return null;
    for (text) |ch| {
        if (ch < '0' or ch > '9') return null;
    }
    return std.fmt.parseInt(u32, text, 10) catch null;
}

fn daysInMonth(year: u32, month: u32) u32 {
    return switch (month) {
        2 => if ((year % 4 == 0 and year % 100 != 0) or year % 400 == 0) 29 else 28,
        4, 6, 9, 11 => 30,
        else => 31,
    };
}

/// ISO-8601 date or date-time, in milliseconds since the epoch. A date is
/// midnight UTC, and a date-time without a zone is read as UTC.
fn parseIsoInstant(text: []const u8) ?i64 {
    const trimmed = std.mem.trim(u8, text, " \t");
    if (trimmed.len < 10 or trimmed[4] != '-' or trimmed[7] != '-') return null;
    const year = parseDigits(trimmed[0..4]) orelse return null;
    const month = parseDigits(trimmed[5..7]) orelse return null;
    const day = parseDigits(trimmed[8..10]) orelse return null;
    if (month < 1 or month > 12 or day < 1 or day > daysInMonth(year, month)) return null;
    const midnight_ms = daysFromCivil(@intCast(year), month, day) * 86_400_000;
    if (trimmed.len == 10) return midnight_ms;

    if (trimmed.len < 19 or (trimmed[10] != 'T' and trimmed[10] != 't')) return null;
    if (trimmed[13] != ':' or trimmed[16] != ':') return null;
    const hour = parseDigits(trimmed[11..13]) orelse return null;
    const minute = parseDigits(trimmed[14..16]) orelse return null;
    const second = parseDigits(trimmed[17..19]) orelse return null;
    if (hour > 23 or minute > 59 or second > 60) return null;

    var index: usize = 19;
    var millis: i64 = 0;
    if (index < trimmed.len and trimmed[index] == '.') {
        index += 1;
        const fraction_start = index;
        var scale: i64 = 100;
        while (index < trimmed.len and trimmed[index] >= '0' and trimmed[index] <= '9') : (index += 1) {
            millis += @as(i64, trimmed[index] - '0') * scale;
            scale = @divTrunc(scale, 10);
        }
        if (index == fraction_start) return null;
    }

    var offset_ms: i64 = 0;
    const zone = trimmed[index..];
    if (zone.len == 0 or std.mem.eql(u8, zone, "Z") or std.mem.eql(u8, zone, "z")) {
        offset_ms = 0;
    } else {
        if (zone.len != 6 or (zone[0] != '+' and zone[0] != '-') or zone[3] != ':') return null;
        const zone_hour = parseDigits(zone[1..3]) orelse return null;
        const zone_minute = parseDigits(zone[4..6]) orelse return null;
        if (zone_hour > 23 or zone_minute > 59) return null;
        offset_ms = (@as(i64, zone_hour) * 60 + @as(i64, zone_minute)) * 60_000;
        if (zone[0] == '-') offset_ms = -offset_ms;
    }
    return midnight_ms + @as(i64, hour) * 3_600_000 + @as(i64, minute) * 60_000 + @as(i64, second) * 1000 + millis - offset_ms;
}

const OrderedCmp = enum { gt, gte, lt, lte };

const FilterOutcome = enum {
    selected,
    excluded,
    /// The entity carries a value the filter cannot read. Not a subject, but
    /// reported: treating it as out of scope would turn a rule silently green.
    unreadable,
};

/// The threshold decides the kind: a date threshold needs a date value.
fn orderedCompare(entity_value: []const u8, expected: std.json.Value, op: OrderedCmp) FilterOutcome {
    switch (expected) {
        .integer => |number| return compareNumber(entity_value, @floatFromInt(number), op),
        .float => |number| return compareNumber(entity_value, number, op),
        .string => |text| {
            if (parseIsoInstant(text)) |right| {
                const left = parseIsoInstant(entity_value) orelse return .unreadable;
                return if (compareOrd(left, right, op)) .selected else .excluded;
            }
            const right = parseExactNumber(text) orelse return .unreadable;
            return compareNumber(entity_value, right, op);
        },
        else => return .unreadable,
    }
}

fn compareNumber(entity_value: []const u8, right: f64, op: OrderedCmp) FilterOutcome {
    const left = parseExactNumber(entity_value) orelse return .unreadable;
    return if (compareOrd(left, right, op)) .selected else .excluded;
}

fn compareOrd(left: anytype, right: anytype, op: OrderedCmp) bool {
    return switch (op) {
        .gt => left > right,
        .gte => left >= right,
        .lt => left < right,
        .lte => left <= right,
    };
}

/// Expects a filter accepted by `fieldFilterIsValid`. An explicit exclusion
/// outranks an unreadable value on another operator of the same field.
fn fieldFilterOutcome(allocator: std.mem.Allocator, entity_value: ?[]const u8, filter: std.json.ObjectMap) !FilterOutcome {
    if (filter.get("one_of")) |one_of| {
        if (entity_value == null or !try scalarInList(allocator, entity_value.?, one_of.array.items)) return .excluded;
    }

    if (filter.get("not_one_of")) |not_one_of| {
        if (entity_value) |value| {
            if (try scalarInList(allocator, value, not_one_of.array.items)) return .excluded;
        }
    }

    if (filter.get("eq")) |eq| {
        const expected = (try jsonValueAsCompareText(allocator, eq)) orelse return .excluded;
        defer allocator.free(expected);
        if (entity_value == null or !std.mem.eql(u8, entity_value.?, expected)) return .excluded;
    }

    const ordered = [_]struct { key: []const u8, op: OrderedCmp }{
        .{ .key = "gt", .op = .gt },
        .{ .key = "gte", .op = .gte },
        .{ .key = "lt", .op = .lt },
        .{ .key = "lte", .op = .lte },
    };
    var unreadable = false;
    for (ordered) |item| {
        const threshold = filter.get(item.key) orelse continue;
        // No value at all means the field does not apply to this entity.
        const value = entity_value orelse return .excluded;
        if (std.mem.trim(u8, value, " \t").len == 0) return .excluded;
        switch (orderedCompare(value, threshold, item.op)) {
            .selected => {},
            .excluded => return .excluded,
            .unreadable => unreadable = true,
        }
    }
    return if (unreadable) .unreadable else .selected;
}

fn scalarInList(allocator: std.mem.Allocator, value: []const u8, list: []const std.json.Value) !bool {
    for (list) |item| {
        const text = (try jsonValueAsCompareText(allocator, item)) orelse continue;
        defer allocator.free(text);
        if (std.mem.eql(u8, value, text)) return true;
    }
    return false;
}

/// Pre-parsed `entity_filter.metadata` object from a rule's metadata JSON.
/// Parsed once per rule; the previous code re-parsed the rule JSON for
/// every evaluated entity.
const RuleFilter = struct {
    parsed: ?std.json.Parsed(std.json.Value) = null,
    metadata: ?std.json.ObjectMap = null,
    /// Stored rules can predate import validation, so evaluation re-checks.
    invalid: bool = false,

    fn init(allocator: std.mem.Allocator, rule_metadata_json: []const u8) !RuleFilter {
        var parsed = std.json.parseFromSlice(std.json.Value, allocator, rule_metadata_json, .{
            .allocate = .alloc_if_needed,
            .ignore_unknown_fields = true,
        }) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return .{ .invalid = true },
        };
        if (!ruleMetadataFilterIsValid(parsed.value)) {
            parsed.deinit();
            return .{ .invalid = true };
        }
        const metadata: ?std.json.ObjectMap = blk: {
            const root = parsed.value;
            if (root != .object) break :blk null;
            const entity_filter = root.object.get("entity_filter") orelse break :blk null;
            const metadata_value = entity_filter.object.get("metadata") orelse break :blk null;
            break :blk metadata_value.object;
        };
        if (metadata == null) {
            parsed.deinit();
            return .{};
        }
        return .{ .parsed = parsed, .metadata = metadata };
    }

    fn deinit(self: *RuleFilter) void {
        if (self.parsed) |*parsed| parsed.deinit();
        self.* = .{};
    }

    fn outcome(self: RuleFilter, allocator: std.mem.Allocator, entity_metadata_json: []const u8) !FilterOutcome {
        const metadata = self.metadata orelse return .selected;
        var unreadable = false;
        var iterator = metadata.iterator();
        while (iterator.next()) |entry| {
            const field_value = try metadataFieldString(allocator, entity_metadata_json, entry.key_ptr.*);
            defer if (field_value) |value| allocator.free(value);
            switch (try fieldFilterOutcome(allocator, field_value, entry.value_ptr.object)) {
                .selected => {},
                .excluded => return .excluded,
                .unreadable => unreadable = true,
            }
        }
        return if (unreadable) .unreadable else .selected;
    }
};

pub fn rulesJson(db: Database, allocator: std.mem.Allocator, ontology_id: []const u8, workspace_id: ?[]const u8) ![]u8 {
    const resolved_workspace_id = workspace_id orelse return error.MissingWorkspace;
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
        \\  AND workspace_id = ?2
        \\ORDER BY rule_id
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, ontology_id);
    try facet_sqlite.bindText(stmt, 2, resolved_workspace_id);
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

    if (!try ontologyExists(db, ontology_id)) {
        const label = try std.fmt.allocPrint(allocator, "Workspace {s} has no default ontology configured or imported for {s}", .{ options.workspace_id, ontology_id });
        defer allocator.free(label);
        try appendIssue(allocator, &issues, .{
            .kind = "workspace_default_ontology_missing",
            .severity = "error",
            .label = label,
            .suggested_action = "set_workspace_default_ontology_or_import_ontology",
        });
    }
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

pub fn runRuleEvaluations(db: Database, allocator: std.mem.Allocator, options: RuleEvaluationOptions) !RuleEvaluationRun {
    const ontology_id = try resolveOntologyId(db, allocator, options.workspace_id, options.ontology_id);
    defer allocator.free(ontology_id);

    if (!try ontologyExists(db, ontology_id)) {
        return .{
            .workspace_id = try allocator.dupe(u8, options.workspace_id),
            .ontology_id = try allocator.dupe(u8, ontology_id),
            .evaluated = 0,
            .changed = 0,
            .events_created = 0,
            .invalid_count = 0,
            .remediation_actions_created = 0,
            .events = try allocator.alloc(RuleEvent, 0),
        };
    }

    const rules = try loadRules(db, allocator, ontology_id, options.workspace_id);
    defer {
        for (rules) |rule| rule.deinit(allocator);
        allocator.free(rules);
    }

    const run_id = try std.fmt.allocPrint(allocator, "graph_rule_eval__{s}__{d}", .{ options.workspace_id, try currentUnix(db) });
    defer allocator.free(run_id);

    // ~5 statements per evaluated entity; one transaction for the whole run
    // instead of one journal sync per statement.
    var tx = try facet_sqlite.Transaction.begin(db);
    defer tx.deinit();

    try ensureQualityRunForRuleEvaluation(db, options.workspace_id, ontology_id, run_id);

    var events = std.ArrayList(RuleEvent).empty;
    defer {
        for (events.items) |event| event.deinit(allocator);
        events.deinit(allocator);
    }

    var evaluated: usize = 0;
    var changed: usize = 0;
    var events_created: usize = 0;
    var invalid_count: usize = 0;
    var remediation_actions_created: usize = 0;

    // Statements are prepared once for the whole run and reset per row;
    // the previous version re-prepared 3-5 statements per (rule, entity).
    const entity_stmt = try facet_sqlite.prepare(db,
        \\SELECT entity_id, metadata_json
        \\FROM graph_entity
        \\WHERE workspace_id = ?1 AND entity_type = ?2 AND deprecated_at IS NULL
        \\ORDER BY entity_id
    );
    defer facet_sqlite.finalize(entity_stmt);

    const state_stmt = try facet_sqlite.prepare(db,
        \\SELECT state
        \\FROM graph_rule_evaluations
        \\WHERE workspace_id = ?1 AND ontology_id = ?2 AND rule_id = ?3 AND subject_entity_id = ?4
        \\LIMIT 1
    );
    defer facet_sqlite.finalize(state_stmt);

    const upsert_stmt = try facet_sqlite.prepare(db,
        \\INSERT INTO graph_rule_evaluations(
        \\  workspace_id, ontology_id, rule_id, subject_entity_id, state,
        \\  observed_count, expected_min, expected_max, last_evaluated_at_unix, updated_at_unix
        \\)
        \\VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, unixepoch(), unixepoch())
        \\ON CONFLICT(workspace_id, ontology_id, rule_id, subject_entity_id) DO UPDATE SET
        \\  state = excluded.state,
        \\  observed_count = excluded.observed_count,
        \\  expected_min = excluded.expected_min,
        \\  expected_max = excluded.expected_max,
        \\  last_evaluated_at_unix = unixepoch(),
        \\  updated_at_unix = unixepoch()
    );
    defer facet_sqlite.finalize(upsert_stmt);

    var counter = try RuleRelationCounter.init(db);
    defer counter.deinit();

    for (rules) |rule| {
        var rule_filter = try RuleFilter.init(allocator, rule.metadata_json);
        defer rule_filter.deinit();

        try facet_sqlite.resetStatement(entity_stmt);
        try facet_sqlite.bindText(entity_stmt, 1, options.workspace_id);
        try facet_sqlite.bindText(entity_stmt, 2, rule.entity_type);
        if (rule_filter.invalid) {
            if (c.sqlite3_step(entity_stmt) == c.SQLITE_ROW) {
                const entity_id: u64 = @intCast(c.sqlite3_column_int64(entity_stmt, 0));
                invalid_count += 1;
                evaluated += 1;
                try recordInvalidEvaluation(db, allocator, state_stmt, upsert_stmt, options, ontology_id, run_id, rule, entity_id, &changed, &events_created, &events);
            }
            continue;
        }
        while (c.sqlite3_step(entity_stmt) == c.SQLITE_ROW) {
            const entity_id: u64 = @intCast(c.sqlite3_column_int64(entity_stmt, 0));
            const entity_metadata = columnText(entity_stmt, 1);
            switch (try rule_filter.outcome(allocator, entity_metadata)) {
                .selected => {},
                .excluded => continue,
                .unreadable => {
                    invalid_count += 1;
                    evaluated += 1;
                    try recordInvalidEvaluation(db, allocator, state_stmt, upsert_stmt, options, ontology_id, run_id, rule, entity_id, &changed, &events_created, &events);
                    continue;
                },
            }

            const observed = try counter.count(options.workspace_id, entity_id, rule);
            const state = ruleState(observed, rule);
            if (std.mem.eql(u8, state, "invalid")) invalid_count += 1;
            evaluated += 1;

            const previous_state = try loadEvaluationState(state_stmt, allocator, options.workspace_id, ontology_id, rule.rule_id, entity_id);
            defer if (previous_state) |value| allocator.free(value);
            const from_state = previous_state orelse "unknown";

            try upsertEvaluationState(upsert_stmt, options.workspace_id, ontology_id, rule, entity_id, state, observed);
            if (previous_state == null or !std.mem.eql(u8, previous_state.?, state)) {
                changed += 1;
                const event = try insertRuleEvent(db, allocator, options.workspace_id, ontology_id, run_id, rule, entity_id, from_state, state, observed);
                events_created += 1;
                if (options.create_remediation_actions and std.mem.eql(u8, from_state, "invalid") and std.mem.eql(u8, state, "valid")) {
                    if (try maybeCreateRemediationAction(db, allocator, options.workspace_id, ontology_id, run_id, rule, entity_id, event.idempotency_key)) {
                        remediation_actions_created += 1;
                    }
                }
                if (events.items.len < options.limit) {
                    try events.append(allocator, event);
                } else {
                    event.deinit(allocator);
                }
            }
        }
    }

    try tx.commit();

    return .{
        .workspace_id = try allocator.dupe(u8, options.workspace_id),
        .ontology_id = try allocator.dupe(u8, ontology_id),
        .evaluated = evaluated,
        .changed = changed,
        .events_created = events_created,
        .invalid_count = invalid_count,
        .remediation_actions_created = remediation_actions_created,
        .events = try events.toOwnedSlice(allocator),
    };
}

pub fn ruleEvaluationRunJson(allocator: std.mem.Allocator, run: RuleEvaluationRun) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.writeAll("{\"kind\":\"graph_rule_evaluation_run\"");
    try out.writer.writeAll(",\"workspace_id\":");
    try out.writer.print("{f}", .{std.json.fmt(run.workspace_id, .{})});
    try out.writer.writeAll(",\"ontology_id\":");
    try out.writer.print("{f}", .{std.json.fmt(run.ontology_id, .{})});
    try out.writer.print(
        ",\"evaluated\":{},\"changed\":{},\"events_created\":{},\"invalid_count\":{},\"remediation_actions_created\":{}",
        .{ run.evaluated, run.changed, run.events_created, run.invalid_count, run.remediation_actions_created },
    );
    try out.writer.writeAll(",\"events\":[");
    for (run.events, 0..) |event, index| {
        if (index > 0) try out.writer.writeAll(",");
        try writeRuleEventJson(&out.writer, event);
    }
    try out.writer.writeAll("]}");
    return try out.toOwnedSlice();
}

pub fn ruleEvaluationsJson(db: Database, allocator: std.mem.Allocator, options: RuleEvaluationOptions) ![]u8 {
    const ontology_id = try resolveOntologyId(db, allocator, options.workspace_id, options.ontology_id);
    defer allocator.free(ontology_id);

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.writeAll("{\"kind\":\"graph_rule_evaluations\",\"workspace_id\":");
    try out.writer.print("{f},\"ontology_id\":{f},\"evaluations\":[", .{
        std.json.fmt(options.workspace_id, .{}),
        std.json.fmt(ontology_id, .{}),
    });

    const stmt = try facet_sqlite.prepare(db,
        \\SELECT rule_id, subject_entity_id, state, observed_count, expected_min, expected_max,
        \\       last_evaluated_at_unix, updated_at_unix
        \\FROM graph_rule_evaluations
        \\WHERE workspace_id = ?1 AND ontology_id = ?2
        \\ORDER BY updated_at_unix DESC, rule_id, subject_entity_id
        \\LIMIT ?3
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, options.workspace_id);
    try facet_sqlite.bindText(stmt, 2, ontology_id);
    try facet_sqlite.bindInt64(stmt, 3, @as(i64, @intCast(options.limit)));

    var first = true;
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
        if (!first) try out.writer.writeAll(",");
        first = false;
        try out.writer.writeAll("{");
        try writeJsonField(&out.writer, "rule_id", columnText(stmt, 0));
        try out.writer.print(",\"subject_entity_id\":{},", .{c.sqlite3_column_int64(stmt, 1)});
        try writeJsonField(&out.writer, "state", columnText(stmt, 2));
        try out.writer.print(
            ",\"observed_count\":{},\"expected_min\":{},\"expected_max\":",
            .{ c.sqlite3_column_int64(stmt, 3), c.sqlite3_column_int64(stmt, 4) },
        );
        try writeOptionalColumnInt(&out.writer, stmt, 5);
        try out.writer.print(",\"last_evaluated_at_unix\":{},\"updated_at_unix\":{}", .{
            c.sqlite3_column_int64(stmt, 6),
            c.sqlite3_column_int64(stmt, 7),
        });
        try out.writer.writeAll("}");
    }
    try out.writer.writeAll("]}");
    return try out.toOwnedSlice();
}

pub fn ruleEventsJson(db: Database, allocator: std.mem.Allocator, options: RuleEvaluationOptions) ![]u8 {
    const ontology_id = try resolveOntologyId(db, allocator, options.workspace_id, options.ontology_id);
    defer allocator.free(ontology_id);

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.writeAll("{\"kind\":\"graph_rule_events\",\"workspace_id\":");
    try out.writer.print("{f},\"ontology_id\":{f},\"events\":[", .{
        std.json.fmt(options.workspace_id, .{}),
        std.json.fmt(ontology_id, .{}),
    });

    const stmt = try facet_sqlite.prepare(db,
        \\SELECT event_id, rule_id, subject_entity_id, from_state, to_state,
        \\       observed_count, expected_min, expected_max, idempotency_key, created_at_unix
        \\FROM graph_rule_events
        \\WHERE workspace_id = ?1 AND ontology_id = ?2
        \\ORDER BY created_at_unix DESC, event_id DESC
        \\LIMIT ?3
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, options.workspace_id);
    try facet_sqlite.bindText(stmt, 2, ontology_id);
    try facet_sqlite.bindInt64(stmt, 3, @as(i64, @intCast(options.limit)));

    var first = true;
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
        if (!first) try out.writer.writeAll(",");
        first = false;
        try out.writer.writeAll("{");
        try writeJsonField(&out.writer, "event_id", columnText(stmt, 0));
        try out.writer.writeAll(",");
        try writeJsonField(&out.writer, "rule_id", columnText(stmt, 1));
        try out.writer.print(",\"subject_entity_id\":{},", .{c.sqlite3_column_int64(stmt, 2)});
        try writeJsonField(&out.writer, "from_state", columnText(stmt, 3));
        try out.writer.writeAll(",");
        try writeJsonField(&out.writer, "to_state", columnText(stmt, 4));
        try out.writer.print(
            ",\"observed_count\":{},\"expected_min\":{},\"expected_max\":",
            .{ c.sqlite3_column_int64(stmt, 5), c.sqlite3_column_int64(stmt, 6) },
        );
        try writeOptionalColumnInt(&out.writer, stmt, 7);
        try out.writer.writeAll(",");
        try writeJsonField(&out.writer, "idempotency_key", columnText(stmt, 8));
        try out.writer.print(",\"created_at_unix\":{}", .{c.sqlite3_column_int64(stmt, 9)});
        try out.writer.writeAll("}");
    }
    try out.writer.writeAll("]}");
    return try out.toOwnedSlice();
}

fn currentUnix(db: Database) !i64 {
    const stmt = try facet_sqlite.prepare(db, "SELECT unixepoch()");
    defer facet_sqlite.finalize(stmt);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StepFailed;
    return c.sqlite3_column_int64(stmt, 0);
}

fn randomToken(db: Database, allocator: std.mem.Allocator) ![]u8 {
    const stmt = try facet_sqlite.prepare(db, "SELECT lower(hex(randomblob(8)))");
    defer facet_sqlite.finalize(stmt);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StepFailed;
    return try dupeColumnText(allocator, stmt, 0);
}

fn ensureQualityRunForRuleEvaluation(db: Database, workspace_id: []const u8, ontology_id: []const u8, run_id: []const u8) !void {
    const stmt = try facet_sqlite.prepare(db,
        \\INSERT INTO quality_convergence_run(run_id, workspace_id, ontology_id, run_kind, status, canonical_layer, input_fingerprint, summary_json, report_json)
        \\VALUES (?1, ?2, ?3, 'graph_rule_evaluation', 'completed', 'runtime_graph', ?4, '{}', '{}')
        \\ON CONFLICT(run_id) DO UPDATE SET status = excluded.status, updated_at_unix = unixepoch()
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, run_id);
    try facet_sqlite.bindText(stmt, 2, workspace_id);
    try facet_sqlite.bindText(stmt, 3, ontology_id);
    try facet_sqlite.bindText(stmt, 4, run_id);
    try facet_sqlite.stepDone(stmt);
}

fn ruleState(observed: i64, rule: GapRule) []const u8 {
    if (observed < rule.min_count) return "invalid";
    if (rule.max_count) |max_count| {
        if (observed > max_count) return "invalid";
    }
    return "valid";
}

fn recordInvalidEvaluation(
    db: Database,
    allocator: std.mem.Allocator,
    state_stmt: *c.sqlite3_stmt,
    upsert_stmt: *c.sqlite3_stmt,
    options: RuleEvaluationOptions,
    ontology_id: []const u8,
    run_id: []const u8,
    rule: GapRule,
    entity_id: u64,
    changed: *usize,
    events_created: *usize,
    events: *std.ArrayList(RuleEvent),
) !void {
    const state = "invalid";
    const previous_state = try loadEvaluationState(state_stmt, allocator, options.workspace_id, ontology_id, rule.rule_id, entity_id);
    defer if (previous_state) |value| allocator.free(value);
    try upsertEvaluationState(upsert_stmt, options.workspace_id, ontology_id, rule, entity_id, state, 0);
    if (previous_state != null and std.mem.eql(u8, previous_state.?, state)) return;
    changed.* += 1;
    const event = try insertRuleEvent(db, allocator, options.workspace_id, ontology_id, run_id, rule, entity_id, previous_state orelse "unknown", state, 0);
    events_created.* += 1;
    if (events.items.len < options.limit) {
        try events.append(allocator, event);
    } else {
        event.deinit(allocator);
    }
}

fn loadEvaluationState(stmt: *c.sqlite3_stmt, allocator: std.mem.Allocator, workspace_id: []const u8, ontology_id: []const u8, rule_id: []const u8, subject_entity_id: u64) !?[]u8 {
    try facet_sqlite.resetStatement(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, ontology_id);
    try facet_sqlite.bindText(stmt, 3, rule_id);
    try facet_sqlite.bindInt64(stmt, 4, @as(i64, @intCast(subject_entity_id)));
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return null;
    return try dupeColumnText(allocator, stmt, 0);
}

fn upsertEvaluationState(stmt: *c.sqlite3_stmt, workspace_id: []const u8, ontology_id: []const u8, rule: GapRule, subject_entity_id: u64, state: []const u8, observed: i64) !void {
    try facet_sqlite.resetStatement(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, ontology_id);
    try facet_sqlite.bindText(stmt, 3, rule.rule_id);
    try facet_sqlite.bindInt64(stmt, 4, @as(i64, @intCast(subject_entity_id)));
    try facet_sqlite.bindText(stmt, 5, state);
    try facet_sqlite.bindInt64(stmt, 6, observed);
    try facet_sqlite.bindInt64(stmt, 7, rule.min_count);
    if (rule.max_count) |value| try facet_sqlite.bindInt64(stmt, 8, value) else try facet_sqlite.bindNull(stmt, 8);
    try facet_sqlite.stepDone(stmt);
}

fn insertRuleEvent(
    db: Database,
    allocator: std.mem.Allocator,
    workspace_id: []const u8,
    ontology_id: []const u8,
    run_id: []const u8,
    rule: GapRule,
    subject_entity_id: u64,
    from_state: []const u8,
    to_state: []const u8,
    observed: i64,
) !RuleEvent {
    const now = try currentUnix(db);
    const token = try randomToken(db, allocator);
    defer allocator.free(token);
    const event_id = try std.fmt.allocPrint(allocator, "graph_rule_event__{s}__{s}__{d}__{s}", .{ workspace_id, rule.rule_id, subject_entity_id, token });
    errdefer allocator.free(event_id);
    // Derived from the run + transition (not a random token): the unique
    // index on (workspace_id, idempotency_key) can only deduplicate retried
    // inserts when the key is reproducible, while a legitimately recurring
    // transition in a later run still gets its own event.
    const idempotency_key = try std.fmt.allocPrint(allocator, "graph_rule_transition__{s}__{s}__{s}__{d}__{s}__{s}", .{ run_id, workspace_id, rule.rule_id, subject_entity_id, from_state, to_state });
    errdefer allocator.free(idempotency_key);

    const stmt = try facet_sqlite.prepare(db,
        \\INSERT INTO graph_rule_events(
        \\  event_id, workspace_id, ontology_id, rule_id, subject_entity_id,
        \\  from_state, to_state, observed_count, expected_min, expected_max,
        \\  idempotency_key, created_at_unix
        \\)
        \\VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, event_id);
    try facet_sqlite.bindText(stmt, 2, workspace_id);
    try facet_sqlite.bindText(stmt, 3, ontology_id);
    try facet_sqlite.bindText(stmt, 4, rule.rule_id);
    try facet_sqlite.bindInt64(stmt, 5, @as(i64, @intCast(subject_entity_id)));
    try facet_sqlite.bindText(stmt, 6, from_state);
    try facet_sqlite.bindText(stmt, 7, to_state);
    try facet_sqlite.bindInt64(stmt, 8, observed);
    try facet_sqlite.bindInt64(stmt, 9, rule.min_count);
    if (rule.max_count) |value| try facet_sqlite.bindInt64(stmt, 10, value) else try facet_sqlite.bindNull(stmt, 10);
    try facet_sqlite.bindText(stmt, 11, idempotency_key);
    try facet_sqlite.bindInt64(stmt, 12, now);
    try facet_sqlite.stepDone(stmt);

    return .{
        .event_id = event_id,
        .rule_id = try allocator.dupe(u8, rule.rule_id),
        .subject_entity_id = subject_entity_id,
        .from_state = try allocator.dupe(u8, from_state),
        .to_state = try allocator.dupe(u8, to_state),
        .observed_count = observed,
        .expected_min = rule.min_count,
        .expected_max = rule.max_count,
        .idempotency_key = idempotency_key,
        .created_at_unix = now,
    };
}

fn metadataStringField(allocator: std.mem.Allocator, object: std.json.ObjectMap, field: []const u8) !?[]u8 {
    const value = object.get(field) orelse return null;
    return switch (value) {
        .string => |text| try allocator.dupe(u8, text),
        else => null,
    };
}

fn metadataFloatField(object: std.json.ObjectMap, field: []const u8, fallback: f64) f64 {
    const value = object.get(field) orelse return fallback;
    return switch (value) {
        .float => |number| number,
        .integer => |number| @floatFromInt(number),
        else => fallback,
    };
}

fn metadataJsonField(allocator: std.mem.Allocator, object: std.json.ObjectMap, field: []const u8) ![]u8 {
    const value = object.get(field) orelse return try allocator.dupe(u8, "{}");
    if (value == .string) return try allocator.dupe(u8, value.string);
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.print("{f}", .{std.json.fmt(value, .{})});
    return try out.toOwnedSlice();
}

fn maybeCreateRemediationAction(
    db: Database,
    allocator: std.mem.Allocator,
    workspace_id: []const u8,
    ontology_id: []const u8,
    run_id: []const u8,
    rule: GapRule,
    subject_entity_id: u64,
    event_idempotency_key: []const u8,
) !bool {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, rule.metadata_json, .{
        .allocate = .alloc_if_needed,
    }) catch return false;
    defer parsed.deinit();
    if (parsed.value != .object) return false;
    const action_value = parsed.value.object.get("remediation_action") orelse return false;
    if (action_value != .object) return false;
    const action_object = action_value.object;

    const action_base = try metadataStringField(allocator, action_object, "action_id") orelse try allocator.dupe(u8, rule.rule_id);
    defer allocator.free(action_base);
    const action_id = try std.fmt.allocPrint(allocator, "graph_rule_action__{s}__{d}", .{ action_base, subject_entity_id });
    defer allocator.free(action_id);
    const idempotency_key = try std.fmt.allocPrint(allocator, "graph_rule_action__{s}__{s}__{s}__{d}", .{ workspace_id, ontology_id, rule.rule_id, subject_entity_id });
    defer allocator.free(idempotency_key);
    const reason = try metadataStringField(allocator, action_object, "reason") orelse try allocator.dupe(u8, rule.label);
    defer allocator.free(reason);
    const execution_mode = try metadataStringField(allocator, action_object, "execution_mode") orelse try allocator.dupe(u8, "manual");
    defer allocator.free(execution_mode);
    const mcp_tool = try metadataStringField(allocator, action_object, "mcp_tool");
    defer if (mcp_tool) |value| allocator.free(value);
    const tool_args_json = try metadataJsonField(allocator, action_object, "tool_args_json");
    defer allocator.free(tool_args_json);
    const evidence_json = try std.fmt.allocPrint(
        allocator,
        "{{\"rule_id\":{f},\"subject_entity_id\":{},\"event_idempotency_key\":{f}}}",
        .{ std.json.fmt(rule.rule_id, .{}), subject_entity_id, std.json.fmt(event_idempotency_key, .{}) },
    );
    defer allocator.free(evidence_json);

    try ensureQualityRunForRuleEvaluation(db, workspace_id, ontology_id, run_id);
    const stmt = try facet_sqlite.prepare(db,
        \\INSERT INTO quality_remediation_action(
        \\  action_id, run_id, workspace_id, ontology_id, issue_type, severity,
        \\  confidence, reason, schema_id, entity_type, projection_id,
        \\  evidence_json, mcp_tool, tool_args_json, execution_mode, idempotency_key, status
        \\)
        \\VALUES (?1, ?2, ?3, ?4, 'graph_rule_valid_transition', ?5, ?6, ?7, NULL, ?8, NULL, ?9, ?10, ?11, ?12, ?13, 'proposed')
        \\ON CONFLICT(action_id) DO UPDATE SET
        \\  run_id = excluded.run_id,
        \\  severity = excluded.severity,
        \\  confidence = excluded.confidence,
        \\  reason = excluded.reason,
        \\  evidence_json = excluded.evidence_json,
        \\  mcp_tool = excluded.mcp_tool,
        \\  tool_args_json = excluded.tool_args_json,
        \\  execution_mode = excluded.execution_mode,
        \\  idempotency_key = excluded.idempotency_key,
        \\  updated_at_unix = unixepoch()
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, action_id);
    try facet_sqlite.bindText(stmt, 2, run_id);
    try facet_sqlite.bindText(stmt, 3, workspace_id);
    try facet_sqlite.bindText(stmt, 4, ontology_id);
    try facet_sqlite.bindText(stmt, 5, rule.severity);
    if (c.sqlite3_bind_double(stmt, 6, metadataFloatField(action_object, "confidence", 0.75)) != c.SQLITE_OK) return error.BindFailed;
    try facet_sqlite.bindText(stmt, 7, reason);
    try facet_sqlite.bindText(stmt, 8, rule.entity_type);
    try facet_sqlite.bindText(stmt, 9, evidence_json);
    if (mcp_tool) |value| try facet_sqlite.bindText(stmt, 10, value) else try facet_sqlite.bindNull(stmt, 10);
    try facet_sqlite.bindText(stmt, 11, tool_args_json);
    try facet_sqlite.bindText(stmt, 12, execution_mode);
    try facet_sqlite.bindText(stmt, 13, idempotency_key);
    try facet_sqlite.stepDone(stmt);
    return c.sqlite3_changes(db.handle) > 0;
}

const ReconciliationWriter = struct {
    allocator: std.mem.Allocator,
    writer: *std.Io.Writer,
    first_issue: bool = true,
    issue_count: usize = 0,
    limit: usize,

    fn append(
        self: *ReconciliationWriter,
        kind: []const u8,
        severity: []const u8,
        section: []const u8,
        label: []const u8,
        suggested_action: []const u8,
    ) !void {
        _ = self.allocator;
        if (self.issue_count >= self.limit) return;
        if (!self.first_issue) try self.writer.writeAll(",");
        self.first_issue = false;
        self.issue_count += 1;
        try self.writer.writeAll("{");
        try writeJsonField(self.writer, "kind", kind);
        try self.writer.writeAll(",");
        try writeJsonField(self.writer, "severity", severity);
        try self.writer.writeAll(",");
        try writeJsonField(self.writer, "section", section);
        try self.writer.writeAll(",");
        try writeJsonField(self.writer, "label", label);
        try self.writer.writeAll(",");
        try writeJsonField(self.writer, "suggested_action", suggested_action);
        try self.writer.writeAll("}");
    }
};

pub fn reconciliationJson(db: Database, allocator: std.mem.Allocator, options: ReconciliationOptions) ![]u8 {
    const ontology_id = try resolveOntologyId(db, allocator, options.workspace_id, options.ontology_id);
    defer allocator.free(ontology_id);

    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    try out.writer.writeAll("{\"kind\":\"ontology_reconciliation_report\",\"summary\":{");
    try writeJsonField(&out.writer, "workspace_id", options.workspace_id);
    try out.writer.writeAll(",");
    try writeJsonField(&out.writer, "ontology_id", ontology_id);
    try out.writer.writeAll(",\"issues_total\":__ISSUES_TOTAL__},\"issues\":[");

    var rw = ReconciliationWriter{
        .allocator = allocator,
        .writer = &out.writer,
        .limit = options.limit,
    };

    if (!try ontologyExists(db, ontology_id)) {
        const label = try std.fmt.allocPrint(allocator, "Workspace {s} resolves to missing ontology {s}", .{ options.workspace_id, ontology_id });
        defer allocator.free(label);
        try rw.append("workspace_default_ontology_missing", "error", "workspace", label, "set_workspace_default_ontology_or_import_ontology");
    }

    try reconcileRegistryProjections(db, allocator, options.workspace_id, ontology_id, &rw);
    try reconcileRawGraph(db, allocator, options.workspace_id, ontology_id, &rw);
    try reconcileRuntimeGraph(db, allocator, options.workspace_id, ontology_id, &rw);
    try reconcileGapRules(db, allocator, options.workspace_id, ontology_id, &rw);
    try reconcileFacetAssignments(db, allocator, options.workspace_id, ontology_id, &rw);
    try reconcileCollectionOntologyBindings(db, allocator, options.workspace_id, ontology_id, &rw);

    try out.writer.writeAll("]}");
    const raw = try out.toOwnedSlice();
    const marker = "__ISSUES_TOTAL__";
    if (std.mem.indexOf(u8, raw, marker)) |index| {
        const replacement = try std.fmt.allocPrint(allocator, "{d}", .{rw.issue_count});
        defer allocator.free(replacement);
        var patched: std.Io.Writer.Allocating = .init(allocator);
        defer patched.deinit();
        try patched.writer.writeAll(raw[0..index]);
        try patched.writer.writeAll(replacement);
        try patched.writer.writeAll(raw[index + marker.len ..]);
        allocator.free(raw);
        return try patched.toOwnedSlice();
    }
    return raw;
}

fn reconcileRegistryProjections(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8, ontology_id: []const u8, rw: *ReconciliationWriter) !void {
    const aggregate_exists = try registrySchemaExists(db, workspace_id, ontology_id);
    if (!aggregate_exists) {
        const label = try std.fmt.allocPrint(allocator, "MCP schema registry projection is missing for ontology {s}", .{ontology_id});
        defer allocator.free(label);
        try rw.append("registry_projection_missing", "warning", "mcp_schema_registry", label, "generate_ontology_registry_projection");
    }

    const stmt = try facet_sqlite.prepare(db,
        \\SELECT json_extract(facets_json, '$.schema_id')
        \\FROM agent_facts
        \\WHERE workspace_id = ?1
        \\  AND schema_id = 'mindbrain:schema'
        \\  AND json_extract(facets_json, '$.generated_from') = 'ontology'
        \\  AND json_extract(facets_json, '$.ontology_id') IS NOT NULL
        \\  AND json_extract(facets_json, '$.ontology_id') != ?2
        \\ORDER BY id
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, ontology_id);
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW and rw.issue_count < rw.limit) {
        const label = try std.fmt.allocPrint(allocator, "Generated registry schema {s} points at a different ontology", .{columnText(stmt, 0)});
        defer allocator.free(label);
        try rw.append("registry_projection_orphan", "warning", "mcp_schema_registry", label, "review_or_remove_orphan_generated_schema_projection");
    }
}

fn registrySchemaExists(db: Database, workspace_id: []const u8, schema_id: []const u8) !bool {
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT 1
        \\FROM agent_facts
        \\WHERE workspace_id = ?1
        \\  AND schema_id = 'mindbrain:schema'
        \\  AND json_extract(facets_json, '$.schema_id') = ?2
        \\LIMIT 1
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, schema_id);
    return c.sqlite3_step(stmt) == c.SQLITE_ROW;
}

fn reconcileRawGraph(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8, ontology_id: []const u8, rw: *ReconciliationWriter) !void {
    try emitUnknownRows(db, allocator, rw,
        \\SELECT DISTINCT e.entity_type
        \\FROM entities_raw e
        \\LEFT JOIN ontology_entity_types oe ON oe.ontology_id = ?2 AND oe.entity_type = e.entity_type
        \\WHERE e.workspace_id = ?1 AND e.ontology_id = ?2 AND oe.entity_type IS NULL
        \\ORDER BY e.entity_type
    , workspace_id, ontology_id, "raw_entity_unknown_type", "raw_graph", "Raw entity type ", " is not declared in ontology", "declare_entity_type_or_fix_raw_entity_type");

    try emitUnknownRows(db, allocator, rw,
        \\SELECT DISTINCT r.edge_type
        \\FROM relations_raw r
        \\LEFT JOIN ontology_edge_types oe ON oe.ontology_id = ?2 AND oe.edge_type = r.edge_type
        \\WHERE r.workspace_id = ?1 AND r.ontology_id = ?2 AND oe.edge_type IS NULL
        \\ORDER BY r.edge_type
    , workspace_id, ontology_id, "raw_relation_unknown_edge_type", "raw_graph", "Raw relation edge type ", " is not declared in ontology", "declare_edge_type_or_fix_raw_relation_type");

    const stmt = try facet_sqlite.prepare(db,
        \\SELECT r.relation_id, r.edge_type
        \\FROM relations_raw r
        \\JOIN entities_raw s ON s.workspace_id = r.workspace_id AND s.entity_id = r.source_entity_id
        \\JOIN entities_raw t ON t.workspace_id = r.workspace_id AND t.entity_id = r.target_entity_id
        \\JOIN ontology_edge_types oe ON oe.ontology_id = ?2 AND oe.edge_type = r.edge_type
        \\WHERE r.workspace_id = ?1 AND r.ontology_id = ?2
        \\  AND ((oe.source_entity_type IS NOT NULL AND oe.source_entity_type != s.entity_type)
        \\    OR (oe.target_entity_type IS NOT NULL AND oe.target_entity_type != t.entity_type))
        \\ORDER BY r.relation_id
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, ontology_id);
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW and rw.issue_count < rw.limit) {
        const label = try std.fmt.allocPrint(allocator, "Raw relation {d} ({s}) endpoint types do not match ontology", .{ c.sqlite3_column_int64(stmt, 0), columnText(stmt, 1) });
        defer allocator.free(label);
        try rw.append("raw_relation_endpoint_mismatch", "error", "raw_graph", label, "review_raw_relation_type_or_endpoint_entity_types");
    }
}

fn reconcileRuntimeGraph(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8, ontology_id: []const u8, rw: *ReconciliationWriter) !void {
    try emitUnknownRows(db, allocator, rw,
        \\SELECT DISTINCT e.entity_type
        \\FROM graph_entity e
        \\LEFT JOIN ontology_entity_types oe ON oe.ontology_id = ?2 AND oe.entity_type = e.entity_type
        \\WHERE e.workspace_id = ?1 AND e.deprecated_at IS NULL AND oe.entity_type IS NULL
        \\ORDER BY e.entity_type
    , workspace_id, ontology_id, "runtime_entity_unknown_type", "runtime_graph", "Runtime entity type ", " is not declared in ontology", "reindex_after_fixing_raw_graph_or_declare_entity_type");

    try emitUnknownRows(db, allocator, rw,
        \\SELECT DISTINCT r.relation_type
        \\FROM graph_relation r
        \\LEFT JOIN ontology_edge_types oe ON oe.ontology_id = ?2 AND oe.edge_type = r.relation_type
        \\WHERE r.workspace_id = ?1 AND r.deprecated_at IS NULL AND oe.edge_type IS NULL
        \\ORDER BY r.relation_type
    , workspace_id, ontology_id, "runtime_relation_unknown_edge_type", "runtime_graph", "Runtime relation type ", " is not declared in ontology", "reindex_after_fixing_raw_graph_or_declare_edge_type");

    const stmt = try facet_sqlite.prepare(db,
        \\SELECT r.relation_id, r.relation_type
        \\FROM graph_relation r
        \\JOIN graph_entity s ON s.entity_id = r.source_id
        \\JOIN graph_entity t ON t.entity_id = r.target_id
        \\JOIN ontology_edge_types oe ON oe.ontology_id = ?2 AND oe.edge_type = r.relation_type
        \\WHERE r.workspace_id = ?1 AND r.deprecated_at IS NULL
        \\  AND ((oe.source_entity_type IS NOT NULL AND oe.source_entity_type != s.entity_type)
        \\    OR (oe.target_entity_type IS NOT NULL AND oe.target_entity_type != t.entity_type))
        \\ORDER BY r.relation_id
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, ontology_id);
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW and rw.issue_count < rw.limit) {
        const label = try std.fmt.allocPrint(allocator, "Runtime relation {d} ({s}) endpoint types do not match ontology", .{ c.sqlite3_column_int64(stmt, 0), columnText(stmt, 1) });
        defer allocator.free(label);
        try rw.append("runtime_relation_endpoint_mismatch", "error", "runtime_graph", label, "review_relation_type_or_endpoint_entity_types_then_reindex");
    }
}

fn reconcileGapRules(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8, ontology_id: []const u8, rw: *ReconciliationWriter) !void {
    try emitUnknownRows(db, allocator, rw,
        \\SELECT DISTINCT g.entity_type
        \\FROM graph_gap_rules g
        \\LEFT JOIN ontology_entity_types oe ON oe.ontology_id = ?2 AND oe.entity_type = g.entity_type
        \\WHERE g.ontology_id = ?2 AND g.workspace_id = ?1 AND oe.entity_type IS NULL
        \\ORDER BY g.entity_type
    , workspace_id, ontology_id, "gap_rule_unknown_entity_type", "gap_rules", "Gap rule entity type ", " is not declared in ontology", "fix_gap_rule_or_declare_entity_type");

    try emitUnknownRows(db, allocator, rw,
        \\SELECT DISTINCT g.relation_type
        \\FROM graph_gap_rules g
        \\LEFT JOIN ontology_edge_types oe ON oe.ontology_id = ?2 AND oe.edge_type = g.relation_type
        \\WHERE g.ontology_id = ?2 AND g.workspace_id = ?1 AND oe.edge_type IS NULL
        \\ORDER BY g.relation_type
    , workspace_id, ontology_id, "gap_rule_unknown_relation_type", "gap_rules", "Gap rule relation type ", " is not declared in ontology", "fix_gap_rule_or_declare_edge_type");

    try emitUnknownRows(db, allocator, rw,
        \\SELECT DISTINCT g.target_entity_type
        \\FROM graph_gap_rules g
        \\LEFT JOIN ontology_entity_types oe ON oe.ontology_id = ?2 AND oe.entity_type = g.target_entity_type
        \\WHERE g.ontology_id = ?2 AND g.workspace_id = ?1
        \\  AND g.target_entity_type IS NOT NULL AND oe.entity_type IS NULL
        \\ORDER BY g.target_entity_type
    , workspace_id, ontology_id, "gap_rule_target_type_mismatch", "gap_rules", "Gap rule target type ", " is not declared in ontology", "fix_gap_rule_target_type_or_declare_entity_type");
}

fn reconcileFacetAssignments(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8, ontology_id: []const u8, rw: *ReconciliationWriter) !void {
    try emitUnknownRows(db, allocator, rw,
        \\SELECT DISTINCT f.namespace || ':' || f.dimension
        \\FROM facet_assignments_raw f
        \\LEFT JOIN ontology_dimensions d ON d.ontology_id = ?2 AND d.namespace = f.namespace AND d.dimension = f.dimension
        \\WHERE f.workspace_id = ?1 AND f.ontology_id = ?2 AND d.dimension IS NULL
        \\ORDER BY 1
    , workspace_id, ontology_id, "facet_assignment_unknown_dimension", "collection_facets", "Facet assignment dimension ", " is not declared in ontology", "declare_dimension_or_fix_facet_assignment");

    try emitUnknownRows(db, allocator, rw,
        \\SELECT DISTINCT f.namespace || ':' || f.dimension || '=' || f.value
        \\FROM facet_assignments_raw f
        \\LEFT JOIN ontology_values v ON v.ontology_id = ?2 AND v.namespace = f.namespace AND v.dimension = f.dimension AND v.value = f.value
        \\WHERE f.workspace_id = ?1 AND f.ontology_id = ?2 AND v.value IS NULL
        \\ORDER BY 1
    , workspace_id, ontology_id, "facet_assignment_unknown_value", "collection_facets", "Facet assignment value ", " is not declared in ontology", "declare_value_or_fix_facet_assignment");
}

fn reconcileCollectionOntologyBindings(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8, ontology_id: []const u8, rw: *ReconciliationWriter) !void {
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT DISTINCT f.collection_id
        \\FROM facet_assignments_raw f
        \\LEFT JOIN collection_ontologies co
        \\  ON co.workspace_id = f.workspace_id AND co.collection_id = f.collection_id AND co.ontology_id = f.ontology_id
        \\WHERE f.workspace_id = ?1 AND f.ontology_id = ?2 AND co.ontology_id IS NULL
        \\ORDER BY f.collection_id
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, ontology_id);
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW and rw.issue_count < rw.limit) {
        const label = try std.fmt.allocPrint(allocator, "Collection {s} has ontology-backed facets without a collection_ontologies binding", .{columnText(stmt, 0)});
        defer allocator.free(label);
        try rw.append("collection_ontology_missing", "warning", "collection_bindings", label, "attach_collection_to_ontology");
    }
}

fn emitUnknownRows(
    db: Database,
    allocator: std.mem.Allocator,
    rw: *ReconciliationWriter,
    sql: []const u8,
    workspace_id: []const u8,
    ontology_id: []const u8,
    kind: []const u8,
    section: []const u8,
    label_prefix: []const u8,
    label_suffix: []const u8,
    suggested_action: []const u8,
) !void {
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, ontology_id);
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW and rw.issue_count < rw.limit) {
        const label = try std.fmt.allocPrint(allocator, "{s}{s}{s}", .{ label_prefix, columnText(stmt, 0), label_suffix });
        defer allocator.free(label);
        try rw.append(kind, "error", section, label, suggested_action);
    }
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
        \\  AND workspace_id = ?2
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
    // Prepared once per run and reset per row; see runRuleEvaluations.
    const entity_stmt = try facet_sqlite.prepare(db,
        \\SELECT entity_id, metadata_json
        \\FROM graph_entity
        \\WHERE workspace_id = ?1 AND entity_type = ?2 AND deprecated_at IS NULL
        \\ORDER BY entity_id
    );
    defer facet_sqlite.finalize(entity_stmt);

    var counter = try RuleRelationCounter.init(db);
    defer counter.deinit();

    for (rules) |rule| {
        var rule_filter = try RuleFilter.init(allocator, rule.metadata_json);
        defer rule_filter.deinit();
        if (rule_filter.invalid) {
            try appendIssue(allocator, issues, .{
                .kind = "invalid_filter_operator",
                .severity = "error",
                .label = rule.label,
                .suggested_action = "use_eq_one_of_not_one_of_gt_gte_lt_or_lte",
                .rule_id = rule.rule_id,
            });
            continue;
        }

        try facet_sqlite.resetStatement(entity_stmt);
        try facet_sqlite.bindText(entity_stmt, 1, options.workspace_id);
        try facet_sqlite.bindText(entity_stmt, 2, rule.entity_type);
        while (c.sqlite3_step(entity_stmt) == c.SQLITE_ROW and issues.items.len < options.limit) {
            const entity_id: u64 = @intCast(c.sqlite3_column_int64(entity_stmt, 0));
            const entity_metadata = columnText(entity_stmt, 1);
            switch (try rule_filter.outcome(allocator, entity_metadata)) {
                .selected => {},
                .excluded => continue,
                .unreadable => {
                    try appendIssue(allocator, issues, .{
                        .kind = "unreadable_filter_value",
                        .severity = "error",
                        .label = rule.label,
                        .suggested_action = "fix_entity_value_or_rule_filter",
                        .entity_id = entity_id,
                        .rule_id = rule.rule_id,
                    });
                    continue;
                },
            }
            const observed = try counter.count(options.workspace_id, entity_id, rule);
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

/// One prepared COUNT statement per direction, reset and rebound per
/// (rule, entity) pair instead of re-prepared each time.
const RuleRelationCounter = struct {
    out_stmt: *c.sqlite3_stmt,
    in_stmt: *c.sqlite3_stmt,
    either_stmt: *c.sqlite3_stmt,

    fn init(db: Database) !RuleRelationCounter {
        const out_stmt = try facet_sqlite.prepare(db,
            \\SELECT COUNT(*)
            \\FROM graph_relation r
            \\JOIN graph_entity target ON target.entity_id = r.target_id
            \\WHERE r.workspace_id = ?1 AND r.source_id = ?2 AND r.relation_type = ?3
            \\  AND r.deprecated_at IS NULL
            \\  AND (?4 IS NULL OR target.entity_type = ?4)
        );
        errdefer facet_sqlite.finalize(out_stmt);
        const in_stmt = try facet_sqlite.prepare(db,
            \\SELECT COUNT(*)
            \\FROM graph_relation r
            \\JOIN graph_entity source ON source.entity_id = r.source_id
            \\WHERE r.workspace_id = ?1 AND r.target_id = ?2 AND r.relation_type = ?3
            \\  AND r.deprecated_at IS NULL
            \\  AND (?4 IS NULL OR source.entity_type = ?4)
        );
        errdefer facet_sqlite.finalize(in_stmt);
        const either_stmt = try facet_sqlite.prepare(db,
            \\SELECT COUNT(*)
            \\FROM graph_relation r
            \\JOIN graph_entity source ON source.entity_id = r.source_id
            \\JOIN graph_entity target ON target.entity_id = r.target_id
            \\WHERE r.workspace_id = ?1 AND (r.source_id = ?2 OR r.target_id = ?2) AND r.relation_type = ?3
            \\  AND r.deprecated_at IS NULL
            \\  AND (?4 IS NULL OR source.entity_type = ?4 OR target.entity_type = ?4)
        );
        return .{
            .out_stmt = out_stmt,
            .in_stmt = in_stmt,
            .either_stmt = either_stmt,
        };
    }

    fn deinit(self: *RuleRelationCounter) void {
        facet_sqlite.finalize(self.out_stmt);
        facet_sqlite.finalize(self.in_stmt);
        facet_sqlite.finalize(self.either_stmt);
        self.* = undefined;
    }

    fn count(self: *RuleRelationCounter, workspace_id: []const u8, entity_id: u64, rule: GapRule) !i64 {
        const stmt = switch (rule.direction) {
            .out => self.out_stmt,
            .in => self.in_stmt,
            .either => self.either_stmt,
        };
        try facet_sqlite.resetStatement(stmt);
        try facet_sqlite.bindText(stmt, 1, workspace_id);
        try facet_sqlite.bindInt64(stmt, 2, entity_id);
        try facet_sqlite.bindText(stmt, 3, rule.relation_type);
        if (rule.target_entity_type) |value| try facet_sqlite.bindText(stmt, 4, value) else try facet_sqlite.bindNull(stmt, 4);
        if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StepFailed;
        return c.sqlite3_column_int64(stmt, 0);
    }
};

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
    var root = index;
    while (parent[root] != root) root = parent[root];
    // Path compression keeps degenerate chains from turning component
    // evaluation quadratic.
    var current = index;
    while (parent[current] != root) {
        const next = parent[current];
        parent[current] = root;
        current = next;
    }
    return root;
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

fn writeRuleEventJson(writer: *std.Io.Writer, event: RuleEvent) !void {
    try writer.writeAll("{");
    try writeJsonField(writer, "event_id", event.event_id);
    try writer.writeAll(",");
    try writeJsonField(writer, "rule_id", event.rule_id);
    try writer.print(",\"subject_entity_id\":{},", .{event.subject_entity_id});
    try writeJsonField(writer, "from_state", event.from_state);
    try writer.writeAll(",");
    try writeJsonField(writer, "to_state", event.to_state);
    try writer.print(",\"observed_count\":{},\"expected_min\":{},\"expected_max\":", .{ event.observed_count, event.expected_min });
    try writeOptionalSignedInt(writer, event.expected_max);
    try writer.writeAll(",");
    try writeJsonField(writer, "idempotency_key", event.idempotency_key);
    try writer.print(",\"created_at_unix\":{}", .{event.created_at_unix});
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

test "resolveOntologyId falls back to workspace aggregate ontology when default is missing" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('serenity-production-v6', 'serenity-production-v6', 'Serenity');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('serenity-production-v6::administrative', 'serenity-production-v6', 'administrative');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('serenity-production-v6', 'serenity-production-v6', 'core');
    );

    const ontology_id = try resolveOntologyId(db, std.testing.allocator, "serenity-production-v6", null);
    defer std.testing.allocator.free(ontology_id);
    try std.testing.expectEqualStrings("serenity-production-v6", ontology_id);
}

test "diagnostics without default ontology returns issue instead of NotFound" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_no_default', 'ws_no_default', 'No Default');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (1, 'ws_no_default', 'unit', 'A1');
    );

    var report = try buildReport(db, std.testing.allocator, .{
        .workspace_id = "ws_no_default",
        .limit = 10,
    });
    defer report.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("ws_no_default::default", report.summary.ontology_id.?);
    try std.testing.expect(report.summary.issues_total >= 1);
    try std.testing.expectEqualStrings("workspace_default_ontology_missing", report.issues[0].kind);
}

test "rule evaluations without default ontology return empty run instead of NotFound" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_eval_no_default', 'ws_eval_no_default', 'No Default');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (1, 'ws_eval_no_default', 'unit', 'A1');
    );

    var run = try runRuleEvaluations(db, std.testing.allocator, .{
        .workspace_id = "ws_eval_no_default",
        .limit = 10,
    });
    defer run.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("ws_eval_no_default::default", run.ontology_id);
    try std.testing.expectEqual(@as(usize, 0), run.evaluated);
    try std.testing.expectEqual(@as(usize, 0), run.events.len);
}

test "entity_filter excludes vacant units from syndic rules" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_filter', 'ws_filter', 'Filter');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('ws_filter::core', 'ws_filter', 'core');
        \\INSERT INTO workspace_settings(workspace_id, default_ontology_id) VALUES ('ws_filter', 'ws_filter::core');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_filter::core', 'unit', 'Unit');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_filter::core', 'person', 'Person');
        \\INSERT INTO ontology_edge_types(ontology_id, edge_type, source_entity_type, target_entity_type) VALUES ('ws_filter::core', 'owns', 'person', 'unit');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name, metadata_json) VALUES (1, 'ws_filter', 'unit', 'Vacant Works', '{"usage_status":"vacant_works"}');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name, metadata_json) VALUES (2, 'ws_filter', 'unit', 'Tenant Unit', '{"usage_status":"tenant_occupied"}');
    );
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_filter::core","workspace_id":"ws_filter","rules":[{"rule_id":"unit-has-owner","entity_type":"unit","relation_type":"owns","direction":"in","min_count":1,"severity":"error","label":"Occupied unit must have owner","metadata_json":"{\"entity_filter\":{\"metadata\":{\"usage_status\":{\"not_one_of\":[\"vacant_works\",\"vacant\"]}}}}"}]}
    );

    var report = try buildReport(db, std.testing.allocator, .{
        .workspace_id = "ws_filter",
        .limit = 50,
    });
    defer report.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), report.summary.missing_required_relations);
    try std.testing.expectEqual(@as(?u64, 2), report.issues[0].entity_id);
}

/// Subjects the filter selected when `min_count` is 0: unreadable values are `invalid`.
fn expectValidIds(db: Database, workspace_id: []const u8, expected: []const i64) !void {
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT subject_entity_id FROM graph_rule_evaluations
        \\WHERE workspace_id = ?1 AND state = 'valid' ORDER BY subject_entity_id
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var index: usize = 0;
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
        try std.testing.expect(index < expected.len);
        try std.testing.expectEqual(expected[index], c.sqlite3_column_int64(stmt, 0));
        index += 1;
    }
    try std.testing.expectEqual(expected.len, index);
}

fn expectEvaluatedIds(db: Database, workspace_id: []const u8, expected: []const i64) !void {
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT subject_entity_id FROM graph_rule_evaluations
        \\WHERE workspace_id = ?1 ORDER BY subject_entity_id
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var index: usize = 0;
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
        try std.testing.expect(index < expected.len);
        try std.testing.expectEqual(expected[index], c.sqlite3_column_int64(stmt, 0));
        index += 1;
    }
    try std.testing.expectEqual(expected.len, index);
}

test "entity_filter compares numbers and ISO dates and rejects unknown operators" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_cmp', 'ws_cmp', 'Compare');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('ws_cmp::core', 'ws_cmp', 'core');
        \\INSERT INTO workspace_settings(workspace_id, default_ontology_id) VALUES ('ws_cmp', 'ws_cmp::core');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_cmp::core', 'charge', 'Charge');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name, metadata_json) VALUES
        \\ (1, 'ws_cmp', 'charge', 'n2401', '{"amount":2401}'),
        \\ (2, 'ws_cmp', 'charge', 'n2400_5', '{"amount":2400.5}'),
        \\ (3, 'ws_cmp', 'charge', 'n2400', '{"amount":2400}'),
        \\ (4, 'ws_cmp', 'charge', 'n2399', '{"amount":2399}'),
        \\ (5, 'ws_cmp', 'charge', 's2400', '{"amount":"2400"}'),
        \\ (6, 'ws_cmp', 'charge', 's2400eur', '{"amount":"2400 EUR"}'),
        \\ (7, 'ws_cmp', 'charge', 'missing', '{}'),
        \\ (8, 'ws_cmp', 'charge', 'empty', '{"amount":""}');
    );
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_cmp::core","workspace_id":"ws_cmp","replace":true,"rules":[{"rule_id":"amount-gt","entity_type":"charge","relation_type":"pays","direction":"out","min_count":0,"severity":"error","label":"amount gt","metadata_json":"{\"entity_filter\":{\"metadata\":{\"amount\":{\"gt\":2400}}}}"}]}
    );
    var gt_run = try runRuleEvaluations(db, std.testing.allocator, .{ .workspace_id = "ws_cmp", .limit = 50 });
    defer gt_run.deinit(std.testing.allocator);
    try expectValidIds(db, "ws_cmp", &.{ 1, 2 });

    try db.exec("DELETE FROM graph_rule_events; DELETE FROM graph_rule_evaluations;");
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_cmp::core","workspace_id":"ws_cmp","replace":true,"rules":[{"rule_id":"amount-gte","entity_type":"charge","relation_type":"pays","direction":"out","min_count":0,"severity":"error","label":"amount gte","metadata_json":"{\"entity_filter\":{\"metadata\":{\"amount\":{\"gte\":2400}}}}"}]}
    );
    var gte_run = try runRuleEvaluations(db, std.testing.allocator, .{ .workspace_id = "ws_cmp", .limit = 50 });
    defer gte_run.deinit(std.testing.allocator);
    try expectValidIds(db, "ws_cmp", &.{ 1, 2, 3, 5 });

    try db.exec("DELETE FROM graph_rule_events; DELETE FROM graph_rule_evaluations;");
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_cmp::core","workspace_id":"ws_cmp","replace":true,"rules":[{"rule_id":"amount-lt","entity_type":"charge","relation_type":"pays","direction":"out","min_count":0,"severity":"error","label":"amount lt","metadata_json":"{\"entity_filter\":{\"metadata\":{\"amount\":{\"lt\":2400}}}}"}]}
    );
    var lt_amount_run = try runRuleEvaluations(db, std.testing.allocator, .{ .workspace_id = "ws_cmp", .limit = 50 });
    defer lt_amount_run.deinit(std.testing.allocator);
    try expectValidIds(db, "ws_cmp", &.{4});

    try db.exec("DELETE FROM graph_rule_events; DELETE FROM graph_rule_evaluations;");
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_cmp::core","workspace_id":"ws_cmp","replace":true,"rules":[{"rule_id":"amount-lte","entity_type":"charge","relation_type":"pays","direction":"out","min_count":0,"severity":"error","label":"amount lte","metadata_json":"{\"entity_filter\":{\"metadata\":{\"amount\":{\"lte\":2400}}}}"}]}
    );
    var lte_amount_run = try runRuleEvaluations(db, std.testing.allocator, .{ .workspace_id = "ws_cmp", .limit = 50 });
    defer lte_amount_run.deinit(std.testing.allocator);
    try expectValidIds(db, "ws_cmp", &.{ 3, 4, 5 });

    try db.exec("DELETE FROM graph_rule_events; DELETE FROM graph_rule_evaluations;");
    try db.exec(
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name, metadata_json) VALUES
        \\ (11, 'ws_cmp', 'charge', 'd0228', '{"due_on":"2026-02-28"}'),
        \\ (12, 'ws_cmp', 'charge', 'd0301', '{"due_on":"2026-03-01"}'),
        \\ (13, 'ws_cmp', 'charge', 'd0302', '{"due_on":"2026-03-02"}'),
        \\ (14, 'ws_cmp', 'charge', 'd0301z', '{"due_on":"2026-03-01T00:00:00Z"}'),
        \\ (15, 'ws_cmp', 'charge', 'dbad', '{"due_on":"not-a-date"}');
    );
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_cmp::core","workspace_id":"ws_cmp","replace":true,"rules":[{"rule_id":"due-lt","entity_type":"charge","relation_type":"pays","direction":"out","min_count":0,"severity":"error","label":"due lt","metadata_json":"{\"entity_filter\":{\"metadata\":{\"due_on\":{\"lt\":\"2026-03-01\"}}}}"}]}
    );
    var lt_run = try runRuleEvaluations(db, std.testing.allocator, .{ .workspace_id = "ws_cmp", .limit = 50 });
    defer lt_run.deinit(std.testing.allocator);
    try expectValidIds(db, "ws_cmp", &.{11});

    try db.exec("DELETE FROM graph_rule_events; DELETE FROM graph_rule_evaluations;");
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_cmp::core","workspace_id":"ws_cmp","replace":true,"rules":[{"rule_id":"due-lte","entity_type":"charge","relation_type":"pays","direction":"out","min_count":0,"severity":"error","label":"due lte","metadata_json":"{\"entity_filter\":{\"metadata\":{\"due_on\":{\"lte\":\"2026-03-01\"}}}}"}]}
    );
    var lte_run = try runRuleEvaluations(db, std.testing.allocator, .{ .workspace_id = "ws_cmp", .limit = 50 });
    defer lte_run.deinit(std.testing.allocator);
    try expectValidIds(db, "ws_cmp", &.{ 11, 12, 14 });

    try db.exec("DELETE FROM graph_rule_events; DELETE FROM graph_rule_evaluations;");
    try db.exec(
        \\DELETE FROM graph_gap_rules;
        \\INSERT INTO graph_gap_rules(rule_id, ontology_id, workspace_id, entity_type, relation_type, direction, min_count, severity, label, enabled, metadata_json)
        \\VALUES ('bad-op', 'ws_cmp::core', 'ws_cmp', 'charge', 'pays', 'out', 0, 'error', 'bad op', 1, '{"entity_filter":{"metadata":{"amount":{"neq":1}}}}');
    );
    var report = try buildReport(db, std.testing.allocator, .{ .workspace_id = "ws_cmp", .limit = 50 });
    defer report.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("invalid_filter_operator", report.issues[0].kind);

    var bad_run = try runRuleEvaluations(db, std.testing.allocator, .{ .workspace_id = "ws_cmp", .limit = 50 });
    defer bad_run.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), bad_run.evaluated);
    try std.testing.expectEqual(@as(usize, 1), bad_run.invalid_count);
    try std.testing.expectEqualStrings("invalid", bad_run.events[0].to_state);

    var second_bad = try runRuleEvaluations(db, std.testing.allocator, .{ .workspace_id = "ws_cmp", .limit = 50 });
    defer second_bad.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), second_bad.events_created);
}

test "gap rule filters keep eq lists disabled rules and ignore relation properties" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_misc', 'ws_misc', 'Misc');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('ws_misc::core', 'ws_misc', 'core');
        \\INSERT INTO workspace_settings(workspace_id, default_ontology_id) VALUES ('ws_misc', 'ws_misc::core');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_misc::core', 'unit', 'Unit');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_misc::core', 'person', 'Person');
        \\INSERT INTO ontology_edge_types(ontology_id, edge_type, source_entity_type, target_entity_type) VALUES ('ws_misc::core', 'owns', 'person', 'unit');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name, metadata_json) VALUES
        \\ (1, 'ws_misc', 'unit', 'flagged', '{"active":true,"usage_status":"tenant_occupied","amount":2500}'),
        \\ (2, 'ws_misc', 'unit', 'other', '{"active":false,"usage_status":"vacant","amount":100}');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (3, 'ws_misc', 'person', 'Owner');
        \\INSERT INTO graph_relation(relation_id, workspace_id, relation_type, source_id, target_id, metadata_json) VALUES (10, 'ws_misc', 'owns', 3, 2, '{"amount":9999}');
    );
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_misc::core","workspace_id":"ws_misc","replace":true,"rules":[{"rule_id":"eq-bool","entity_type":"unit","relation_type":"owns","direction":"in","min_count":0,"label":"active true","metadata_json":"{\"entity_filter\":{\"metadata\":{\"active\":{\"eq\":true}}}}"},{"rule_id":"disabled","entity_type":"unit","relation_type":"owns","direction":"in","min_count":1,"enabled":false,"label":"disabled"},{"rule_id":"numeric-in","entity_type":"unit","relation_type":"owns","direction":"in","min_count":1,"label":"amount inbound","metadata_json":"{\"entity_filter\":{\"metadata\":{\"amount\":{\"gte\":2400}}}}"}]}
    );
    var run = try runRuleEvaluations(db, std.testing.allocator, .{ .workspace_id = "ws_misc", .limit = 50 });
    defer run.deinit(std.testing.allocator);

    const stmt = try facet_sqlite.prepare(db, "SELECT rule_id, subject_entity_id, state FROM graph_rule_evaluations WHERE workspace_id = 'ws_misc' ORDER BY rule_id, subject_entity_id");
    defer facet_sqlite.finalize(stmt);
    try std.testing.expect(c.sqlite3_step(stmt) == c.SQLITE_ROW);
    try std.testing.expectEqualStrings("eq-bool", std.mem.span(c.sqlite3_column_text(stmt, 0)));
    try std.testing.expectEqual(@as(i64, 1), c.sqlite3_column_int64(stmt, 1));
    try std.testing.expect(c.sqlite3_step(stmt) == c.SQLITE_ROW);
    try std.testing.expectEqualStrings("numeric-in", std.mem.span(c.sqlite3_column_text(stmt, 0)));
    try std.testing.expectEqual(@as(i64, 1), c.sqlite3_column_int64(stmt, 1));
    try std.testing.expectEqualStrings("invalid", std.mem.span(c.sqlite3_column_text(stmt, 2)));
    try std.testing.expect(c.sqlite3_step(stmt) != c.SQLITE_ROW);

    try db.exec("DELETE FROM graph_rule_events; DELETE FROM graph_rule_evaluations;");
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_misc::core","workspace_id":"ws_misc","replace":true,"rules":[{"rule_id":"kept","entity_type":"unit","relation_type":"owns","direction":"out","min_count":0,"label":"kept"}]}
    );
    const count_stmt = try facet_sqlite.prepare(db, "SELECT COUNT(*) FROM graph_gap_rules WHERE workspace_id = 'ws_misc'");
    defer facet_sqlite.finalize(count_stmt);
    try std.testing.expect(c.sqlite3_step(count_stmt) == c.SQLITE_ROW);
    try std.testing.expectEqual(@as(i64, 1), c.sqlite3_column_int64(count_stmt, 0));
}

test "entity_filter amount gte with out and either directions" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_dir', 'ws_dir', 'Dir');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('ws_dir::core', 'ws_dir', 'core');
        \\INSERT INTO workspace_settings(workspace_id, default_ontology_id) VALUES ('ws_dir', 'ws_dir::core');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_dir::core', 'charge', 'Charge');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_dir::core', 'person', 'Person');
        \\INSERT INTO ontology_edge_types(ontology_id, edge_type, source_entity_type, target_entity_type) VALUES ('ws_dir::core', 'pays', 'charge', 'person');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name, metadata_json) VALUES
        \\ (1, 'ws_dir', 'charge', 'out-high', '{"amount":2500}'),
        \\ (2, 'ws_dir', 'charge', 'in-high', '{"amount":2500}'),
        \\ (3, 'ws_dir', 'charge', 'out-low', '{"amount":100}'),
        \\ (4, 'ws_dir', 'charge', 'none-high', '{"amount":2500}');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (10, 'ws_dir', 'person', 'Payee');
        \\INSERT INTO graph_relation(relation_id, workspace_id, relation_type, source_id, target_id) VALUES
        \\ (20, 'ws_dir', 'pays', 1, 10),
        \\ (21, 'ws_dir', 'pays', 10, 2),
        \\ (22, 'ws_dir', 'pays', 3, 10);
    );
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_dir::core","workspace_id":"ws_dir","replace":true,"rules":[{"rule_id":"amount-out","entity_type":"charge","relation_type":"pays","direction":"out","min_count":1,"severity":"error","label":"out gte","metadata_json":"{\"entity_filter\":{\"metadata\":{\"amount\":{\"gte\":2400}}}}"}]}
    );
    var out_run = try runRuleEvaluations(db, std.testing.allocator, .{ .workspace_id = "ws_dir", .limit = 50 });
    defer out_run.deinit(std.testing.allocator);
    try expectEvaluatedIds(db, "ws_dir", &.{ 1, 2, 4 });
    const out_stmt = try facet_sqlite.prepare(db, "SELECT subject_entity_id, state FROM graph_rule_evaluations WHERE workspace_id = 'ws_dir' ORDER BY subject_entity_id");
    defer facet_sqlite.finalize(out_stmt);
    try std.testing.expect(c.sqlite3_step(out_stmt) == c.SQLITE_ROW);
    try std.testing.expectEqual(@as(i64, 1), c.sqlite3_column_int64(out_stmt, 0));
    try std.testing.expectEqualStrings("valid", std.mem.span(c.sqlite3_column_text(out_stmt, 1)));
    try std.testing.expect(c.sqlite3_step(out_stmt) == c.SQLITE_ROW);
    try std.testing.expectEqual(@as(i64, 2), c.sqlite3_column_int64(out_stmt, 0));
    try std.testing.expectEqualStrings("invalid", std.mem.span(c.sqlite3_column_text(out_stmt, 1)));
    try std.testing.expect(c.sqlite3_step(out_stmt) == c.SQLITE_ROW);
    try std.testing.expectEqual(@as(i64, 4), c.sqlite3_column_int64(out_stmt, 0));
    try std.testing.expectEqualStrings("invalid", std.mem.span(c.sqlite3_column_text(out_stmt, 1)));
    try std.testing.expect(c.sqlite3_step(out_stmt) != c.SQLITE_ROW);

    try db.exec("DELETE FROM graph_rule_events; DELETE FROM graph_rule_evaluations;");
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_dir::core","workspace_id":"ws_dir","replace":true,"rules":[{"rule_id":"amount-either","entity_type":"charge","relation_type":"pays","direction":"either","min_count":1,"severity":"error","label":"either gte","metadata_json":"{\"entity_filter\":{\"metadata\":{\"amount\":{\"gte\":2400}}}}"}]}
    );
    var either_run = try runRuleEvaluations(db, std.testing.allocator, .{ .workspace_id = "ws_dir", .limit = 50 });
    defer either_run.deinit(std.testing.allocator);
    try expectEvaluatedIds(db, "ws_dir", &.{ 1, 2, 4 });
    const either_stmt = try facet_sqlite.prepare(db, "SELECT subject_entity_id, state FROM graph_rule_evaluations WHERE workspace_id = 'ws_dir' ORDER BY subject_entity_id");
    defer facet_sqlite.finalize(either_stmt);
    try std.testing.expect(c.sqlite3_step(either_stmt) == c.SQLITE_ROW);
    try std.testing.expectEqual(@as(i64, 1), c.sqlite3_column_int64(either_stmt, 0));
    try std.testing.expectEqualStrings("valid", std.mem.span(c.sqlite3_column_text(either_stmt, 1)));
    try std.testing.expect(c.sqlite3_step(either_stmt) == c.SQLITE_ROW);
    try std.testing.expectEqual(@as(i64, 2), c.sqlite3_column_int64(either_stmt, 0));
    try std.testing.expectEqualStrings("valid", std.mem.span(c.sqlite3_column_text(either_stmt, 1)));
    try std.testing.expect(c.sqlite3_step(either_stmt) == c.SQLITE_ROW);
    try std.testing.expectEqual(@as(i64, 4), c.sqlite3_column_int64(either_stmt, 0));
    try std.testing.expectEqualStrings("invalid", std.mem.span(c.sqlite3_column_text(either_stmt, 1)));
    try std.testing.expect(c.sqlite3_step(either_stmt) != c.SQLITE_ROW);
}

test "entity_filter string eq selects tenant_occupied units" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_eq', 'ws_eq', 'Eq');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('ws_eq::core', 'ws_eq', 'core');
        \\INSERT INTO workspace_settings(workspace_id, default_ontology_id) VALUES ('ws_eq', 'ws_eq::core');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_eq::core', 'unit', 'Unit');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name, metadata_json) VALUES
        \\ (1, 'ws_eq', 'unit', 'tenant', '{"usage_status":"tenant_occupied"}'),
        \\ (2, 'ws_eq', 'unit', 'vacant', '{"usage_status":"vacant"}');
    );
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_eq::core","workspace_id":"ws_eq","replace":true,"rules":[{"rule_id":"eq-status","entity_type":"unit","relation_type":"owns","direction":"in","min_count":0,"label":"tenant occupied","metadata_json":"{\"entity_filter\":{\"metadata\":{\"usage_status\":{\"eq\":\"tenant_occupied\"}}}}"}]}
    );
    var run = try runRuleEvaluations(db, std.testing.allocator, .{ .workspace_id = "ws_eq", .limit = 50 });
    defer run.deinit(std.testing.allocator);
    try expectEvaluatedIds(db, "ws_eq", &.{1});
}

test "entity_filter ignores relation properties when selecting subjects" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_relprop', 'ws_relprop', 'Relprop');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('ws_relprop::core', 'ws_relprop', 'core');
        \\INSERT INTO workspace_settings(workspace_id, default_ontology_id) VALUES ('ws_relprop', 'ws_relprop::core');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_relprop::core', 'unit', 'Unit');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_relprop::core', 'person', 'Person');
        \\INSERT INTO ontology_edge_types(ontology_id, edge_type, source_entity_type, target_entity_type) VALUES ('ws_relprop::core', 'owns', 'person', 'unit');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name, metadata_json) VALUES
        \\ (1, 'ws_relprop', 'unit', 'high', '{"amount":2500}'),
        \\ (2, 'ws_relprop', 'unit', 'low', '{"amount":100}');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (3, 'ws_relprop', 'person', 'Owner');
        \\INSERT INTO graph_relation(relation_id, workspace_id, relation_type, source_id, target_id, metadata_json) VALUES (10, 'ws_relprop', 'owns', 3, 2, '{"amount":9999}');
    );
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_relprop::core","workspace_id":"ws_relprop","replace":true,"rules":[{"rule_id":"amount-in","entity_type":"unit","relation_type":"owns","direction":"in","min_count":1,"label":"entity amount only","metadata_json":"{\"entity_filter\":{\"metadata\":{\"amount\":{\"gte\":2400}}}}"}]}
    );
    var run = try runRuleEvaluations(db, std.testing.allocator, .{ .workspace_id = "ws_relprop", .limit = 50 });
    defer run.deinit(std.testing.allocator);
    try expectEvaluatedIds(db, "ws_relprop", &.{1});
    const stmt = try facet_sqlite.prepare(db, "SELECT state FROM graph_rule_evaluations WHERE workspace_id = 'ws_relprop' AND subject_entity_id = 1");
    defer facet_sqlite.finalize(stmt);
    try std.testing.expect(c.sqlite3_step(stmt) == c.SQLITE_ROW);
    try std.testing.expectEqualStrings("invalid", std.mem.span(c.sqlite3_column_text(stmt, 0)));
}

test "gap rule for a removed entity type evaluates zero subjects" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_gone', 'ws_gone', 'Gone');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('ws_gone::core', 'ws_gone', 'core');
        \\INSERT INTO workspace_settings(workspace_id, default_ontology_id) VALUES ('ws_gone', 'ws_gone::core');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_gone::core', 'building', 'Building');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (1, 'ws_gone', 'building', 'Keep');
    );
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_gone::core","workspace_id":"ws_gone","rules":[{"rule_id":"unit-owner","entity_type":"unit","relation_type":"owns","direction":"in","min_count":1,"label":"removed type"}]}
    );
    try db.exec("DELETE FROM ontology_entity_types WHERE entity_type = 'unit'");
    var run = try runRuleEvaluations(db, std.testing.allocator, .{ .workspace_id = "ws_gone", .limit = 50 });
    defer run.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), run.evaluated);
    try expectEvaluatedIds(db, "ws_gone", &.{});
}

test "deleteRulesJson removes selected rule ids" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_delete', 'ws_delete', 'Delete');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('ws_delete::core', 'ws_delete', 'core');
    );
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_delete::core","workspace_id":"ws_delete","rules":[{"rule_id":"keep-me","entity_type":"unit","relation_type":"contains","direction":"in","label":"Keep"},{"rule_id":"drop-me","entity_type":"unit","relation_type":"owns","direction":"in","label":"Drop"}]}
    );
    const deleted = try deleteRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_delete::core","workspace_id":"ws_delete","rule_ids":["drop-me"]}
    );
    try std.testing.expectEqual(@as(usize, 1), deleted);
    const rules_json = try rulesJson(db, std.testing.allocator, "ws_delete::core", "ws_delete");
    defer std.testing.allocator.free(rules_json);
    try std.testing.expect(std.mem.indexOf(u8, rules_json, "keep-me") != null);
    try std.testing.expect(std.mem.indexOf(u8, rules_json, "drop-me") == null);
}

test "importRulesJson rejects workspace-less gap rules" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_required', 'ws_required', 'Required');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('ws_required::core', 'ws_required', 'core');
    );

    try std.testing.expectError(
        error.MissingWorkspace,
        importRulesJson(db, std.testing.allocator,
            \\{"ontology_id":"ws_required::core","rules":[{"rule_id":"missing-workspace","entity_type":"unit","relation_type":"owns","direction":"in","label":"Missing workspace"}]}
        ),
    );
}

test "strict gap rule import validates ontology references" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_strict', 'ws_strict', 'Strict');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('ws_strict::core', 'ws_strict', 'core');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_strict::core', 'unit', 'Unit');
    );

    try std.testing.expectError(error.InvalidArguments, importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_strict::core","workspace_id":"ws_strict","validation_mode":"strict","rules":[{"rule_id":"bad-edge","entity_type":"unit","relation_type":"missing_edge","direction":"out","label":"Bad edge"}]}
    ));

    const imported = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_strict::core","workspace_id":"ws_strict","validation_mode":"warn","rules":[{"rule_id":"bad-edge","entity_type":"unit","relation_type":"missing_edge","direction":"out","label":"Bad edge"}]}
    );
    try std.testing.expectEqual(@as(usize, 1), imported);
}

test "rule evaluations persist state and emit events only on transitions" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_rule_eval', 'ws_rule_eval', 'Rule Eval');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('ws_rule_eval::core', 'ws_rule_eval', 'core');
        \\INSERT INTO workspace_settings(workspace_id, default_ontology_id) VALUES ('ws_rule_eval', 'ws_rule_eval::core');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_rule_eval::core', 'unit', 'Unit');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_rule_eval::core', 'person', 'Person');
        \\INSERT INTO ontology_edge_types(ontology_id, edge_type, source_entity_type, target_entity_type) VALUES ('ws_rule_eval::core', 'owns', 'person', 'unit');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (1, 'ws_rule_eval', 'unit', 'A1');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (2, 'ws_rule_eval', 'person', 'Owner');
    );
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_rule_eval::core","workspace_id":"ws_rule_eval","rules":[{"rule_id":"unit-has-owner","entity_type":"unit","relation_type":"owns","direction":"in","target_entity_type":"person","min_count":1,"severity":"error","label":"Unit must have owner","metadata_json":"{\"remediation_action\":{\"action_id\":\"confirm-owner\",\"mcp_tool\":\"ghostcrab_graph_edge_confirm\",\"tool_args_json\":{\"relation_type\":\"owns\"},\"execution_mode\":\"manual\",\"confidence\":0.8}}"}]}
    );

    var first = try runRuleEvaluations(db, std.testing.allocator, .{
        .workspace_id = "ws_rule_eval",
        .limit = 10,
    });
    defer first.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), first.evaluated);
    try std.testing.expectEqual(@as(usize, 1), first.changed);
    try std.testing.expectEqual(@as(usize, 1), first.events_created);
    try std.testing.expectEqual(@as(usize, 1), first.invalid_count);
    try std.testing.expectEqual(@as(usize, 0), first.remediation_actions_created);
    try std.testing.expectEqualStrings("invalid", first.events[0].to_state);

    var second = try runRuleEvaluations(db, std.testing.allocator, .{
        .workspace_id = "ws_rule_eval",
        .limit = 10,
    });
    defer second.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), second.evaluated);
    try std.testing.expectEqual(@as(usize, 0), second.changed);
    try std.testing.expectEqual(@as(usize, 0), second.events_created);
    try std.testing.expectEqual(@as(usize, 1), second.invalid_count);

    try db.exec("INSERT INTO graph_relation(relation_id, workspace_id, relation_type, source_id, target_id) VALUES (10, 'ws_rule_eval', 'owns', 2, 1);");
    var third = try runRuleEvaluations(db, std.testing.allocator, .{
        .workspace_id = "ws_rule_eval",
        .limit = 10,
    });
    defer third.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), third.evaluated);
    try std.testing.expectEqual(@as(usize, 1), third.changed);
    try std.testing.expectEqual(@as(usize, 1), third.events_created);
    try std.testing.expectEqual(@as(usize, 0), third.invalid_count);
    try std.testing.expectEqual(@as(usize, 1), third.remediation_actions_created);
    try std.testing.expectEqualStrings("invalid", third.events[0].from_state);
    try std.testing.expectEqualStrings("valid", third.events[0].to_state);

    const evaluations_json = try ruleEvaluationsJson(db, std.testing.allocator, .{
        .workspace_id = "ws_rule_eval",
        .limit = 10,
    });
    defer std.testing.allocator.free(evaluations_json);
    try std.testing.expect(std.mem.indexOf(u8, evaluations_json, "\"state\":\"valid\"") != null);

    const events_json = try ruleEventsJson(db, std.testing.allocator, .{
        .workspace_id = "ws_rule_eval",
        .limit = 10,
    });
    defer std.testing.allocator.free(events_json);
    try std.testing.expect(std.mem.indexOf(u8, events_json, "\"from_state\":\"invalid\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, events_json, "\"to_state\":\"valid\"") != null);
}

test "ontology reconciliation reports registry and graph drift" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_reconcile', 'ws_reconcile', 'Reconcile');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('ws_reconcile::core', 'ws_reconcile', 'core');
        \\INSERT INTO workspace_settings(workspace_id, default_ontology_id) VALUES ('ws_reconcile', 'ws_reconcile::core');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_reconcile::core', 'unit', 'Unit');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_reconcile::core', 'building', 'Building');
        \\INSERT INTO ontology_edge_types(ontology_id, edge_type, source_entity_type, target_entity_type) VALUES ('ws_reconcile::core', 'part_of', 'unit', 'building');
        \\INSERT INTO entities_raw(workspace_id, ontology_id, entity_type, name) VALUES ('ws_reconcile', 'ws_reconcile::core', 'ghost_type', 'Raw unknown');
        \\INSERT INTO entities_raw(workspace_id, ontology_id, entity_type, name) VALUES ('ws_reconcile', 'ws_reconcile::core', 'unit', 'Raw unit');
        \\INSERT INTO entities_raw(workspace_id, ontology_id, entity_type, name) VALUES ('ws_reconcile', 'ws_reconcile::core', 'building', 'Raw building');
        \\INSERT INTO relations_raw(workspace_id, ontology_id, edge_type, source_entity_id, target_entity_id) VALUES ('ws_reconcile', 'ws_reconcile::core', 'unknown_edge', 2, 3);
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (10, 'ws_reconcile', 'unknown_runtime', 'Runtime unknown');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (11, 'ws_reconcile', 'building', 'Runtime building');
        \\INSERT INTO graph_relation(relation_id, workspace_id, relation_type, source_id, target_id) VALUES (20, 'ws_reconcile', 'part_of', 11, 10);
    );
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_reconcile::core","workspace_id":"ws_reconcile","rules":[{"rule_id":"bad-rule","entity_type":"missing_type","relation_type":"missing_edge","direction":"out","label":"Bad rule"}]}
    );

    const json = try reconciliationJson(db, std.testing.allocator, .{
        .workspace_id = "ws_reconcile",
        .limit = 50,
    });
    defer std.testing.allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "registry_projection_missing") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "raw_entity_unknown_type") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "raw_relation_unknown_edge_type") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "runtime_entity_unknown_type") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "runtime_relation_endpoint_mismatch") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "gap_rule_unknown_entity_type") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"issues_total\":") != null);
}

test "gap rule import rejects filters that cannot be evaluated" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_v', 'ws_v', 'V');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('ws_v::core', 'ws_v', 'core');
    );
    const invalid = [_][]const u8{
        "{\"entity_filter\":{\"metadata\":{\"a\":{\"neq\":1}}}}",
        "{\"entity_filter\":{\"metadata\":{\"a\":{\"gt\":\"soon\"}}}}",
        "{\"entity_filter\":{\"metadata\":{\"a\":{\"gt\":true}}}}",
        "{\"entity_filter\":{\"metadata\":{\"a\":{\"gte\":null}}}}",
        "{\"entity_filter\":{\"metadata\":{\"a\":{\"lt\":[1]}}}}",
        "{\"entity_filter\":{\"metadata\":{\"a\":{\"lt\":\"2026-02-31\"}}}}",
        "{\"entity_filter\":{\"metadata\":{\"a\":5000}}}",
        "{\"entity_filter\":{\"metadata\":{\"a\":{}}}}",
        "{\"entity_filter\":{\"metadata\":{\"a\":{\"one_of\":\"x\"}}}}",
        "{\"entity_filter\":{\"metadata\":{\"a\":{\"one_of\":[]}}}}",
        "{\"entity_filter\":{\"metadata\":{\"a\":{\"eq\":[1]}}}}",
        "{\"entity_filter\":{\"metadata\":{\"a\":{\"eq\":null}}}}",
        "{\"entity_filter\":{\"metadata\":\"a\"}}",
        "{\"entity_filter\":{\"metdata\":{\"a\":{\"eq\":1}}}}",
        "{\"entity_filter\":true}",
        "{\"entity_filters\":{\"metadata\":{\"a\":{\"gt\":1}}}}",
        "{\"entityFilter\":{\"metadata\":{\"a\":{\"gt\":1}}}}",
        "not json",
    };
    for (invalid) |metadata_json| {
        const json = try std.json.Stringify.valueAlloc(std.testing.allocator, .{
            .ontology_id = "ws_v::core",
            .workspace_id = "ws_v",
            .rules = &[_]struct {
                rule_id: []const u8 = "r",
                entity_type: []const u8 = "charge",
                relation_type: []const u8 = "pays",
                direction: []const u8 = "out",
                label: []const u8 = "r",
                metadata_json: []const u8,
            }{.{ .metadata_json = metadata_json }},
        }, .{});
        defer std.testing.allocator.free(json);
        try std.testing.expectError(error.InvalidArguments, importRulesJson(db, std.testing.allocator, json));
    }
    try std.testing.expectEqual(@as(usize, 1), try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_v::core","workspace_id":"ws_v","rules":[{"rule_id":"ok","entity_type":"charge","relation_type":"pays","direction":"out","label":"ok","metadata_json":"{\"note\":\"free text\",\"entity_filter\":{\"metadata\":{\"a\":{\"gte\":\"2026-02-28\",\"lt\":\"2026-03-01T00:00:00+01:00\"}}}}"}]}
    ));
}

test "parseIsoInstant handles offsets fractions and impossible dates" {
    const midnight = parseIsoInstant("2026-03-01").?;
    try std.testing.expectEqual(midnight, parseIsoInstant("2026-03-01T00:00:00Z").?);
    try std.testing.expectEqual(midnight, parseIsoInstant("2026-03-01T00:00:00").?);
    try std.testing.expectEqual(midnight, parseIsoInstant("2026-03-01T01:00:00+01:00").?);
    try std.testing.expectEqual(midnight, parseIsoInstant("2026-02-28T19:00:00-05:00").?);
    try std.testing.expectEqual(midnight + 900, parseIsoInstant("2026-03-01T00:00:00.900Z").?);
    try std.testing.expect(parseIsoInstant("2024-02-29") != null);
    for ([_][]const u8{ "2026-02-29", "2026-02-31", "2026-04-31", "15/02/2026", "2026-13-01", "+026-03-01", "2026-03-01T25:00:00Z", "2026-03-01T00:00:00+0100", "2026-03-01T00:00:00." }) |text| {
        try std.testing.expect(parseIsoInstant(text) == null);
    }
    try std.testing.expect(parseExactNumber("1e999") == null);
    try std.testing.expectEqual(@as(?f64, 2400), parseExactNumber("2400"));
}

test "entity_filter reports unreadable values instead of hiding them" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_u', 'ws_u', 'U');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('ws_u::core', 'ws_u', 'core');
        \\INSERT INTO workspace_settings(workspace_id, default_ontology_id) VALUES ('ws_u', 'ws_u::core');
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_u::core', 'charge', 'Charge');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name, metadata_json) VALUES
        \\ (1, 'ws_u', 'charge', 'iso', '{"due_on":"2026-02-15","usage":"a"}'),
        \\ (2, 'ws_u', 'charge', 'paris', '{"due_on":"2026-02-15T10:00:00+01:00","usage":"a"}'),
        \\ (3, 'ws_u', 'charge', 'no_zone', '{"due_on":"2026-02-15T10:00:00","usage":"a"}'),
        \\ (4, 'ws_u', 'charge', 'french', '{"due_on":"15/02/2026","usage":"a"}'),
        \\ (5, 'ws_u', 'charge', 'impossible', '{"due_on":"2026-02-31","usage":"a"}'),
        \\ (6, 'ws_u', 'charge', 'missing', '{"usage":"a"}'),
        \\ (7, 'ws_u', 'charge', 'empty', '{"due_on":"","usage":"a"}'),
        \\ (8, 'ws_u', 'charge', 'later', '{"due_on":"2026-06-01","usage":"a"}'),
        \\ (9, 'ws_u', 'charge', 'french_other_usage', '{"due_on":"15/02/2026","usage":"b"}');
    );
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_u::core","workspace_id":"ws_u","rules":[{"rule_id":"due","entity_type":"charge","relation_type":"pays","direction":"out","min_count":1,"severity":"error","label":"due","metadata_json":"{\"entity_filter\":{\"metadata\":{\"usage\":{\"eq\":\"a\"},\"due_on\":{\"lt\":\"2026-03-01\"}}}}"}]}
    );

    var report = try buildReport(db, std.testing.allocator, .{ .workspace_id = "ws_u", .limit = 50 });
    defer report.deinit(std.testing.allocator);
    var missing_relation: [16]u64 = undefined;
    var missing_count: usize = 0;
    var unreadable: [16]u64 = undefined;
    var unreadable_count: usize = 0;
    for (report.issues) |issue| {
        if (std.mem.eql(u8, issue.kind, "missing_required_relation")) {
            missing_relation[missing_count] = issue.entity_id.?;
            missing_count += 1;
        } else if (std.mem.eql(u8, issue.kind, "unreadable_filter_value")) {
            unreadable[unreadable_count] = issue.entity_id.?;
            unreadable_count += 1;
        }
    }
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 3 }, missing_relation[0..missing_count]);
    try std.testing.expectEqualSlices(u64, &.{ 4, 5 }, unreadable[0..unreadable_count]);

    var run = try runRuleEvaluations(db, std.testing.allocator, .{ .workspace_id = "ws_u", .limit = 50 });
    defer run.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 5), run.evaluated);
    try std.testing.expectEqual(@as(usize, 5), run.invalid_count);

    // With nothing required, only the unreadable values stay invalid.
    try db.exec("DELETE FROM graph_rule_events; DELETE FROM graph_rule_evaluations;");
    _ = try importRulesJson(db, std.testing.allocator,
        \\{"ontology_id":"ws_u::core","workspace_id":"ws_u","replace":true,"rules":[{"rule_id":"due","entity_type":"charge","relation_type":"pays","direction":"out","min_count":0,"severity":"error","label":"due","metadata_json":"{\"entity_filter\":{\"metadata\":{\"usage\":{\"eq\":\"a\"},\"due_on\":{\"lt\":\"2026-03-01\"}}}}"}]}
    );
    var lenient = try runRuleEvaluations(db, std.testing.allocator, .{ .workspace_id = "ws_u", .limit = 50 });
    defer lenient.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), lenient.invalid_count);
    try expectValidIds(db, "ws_u", &.{ 1, 2, 3 });
}
