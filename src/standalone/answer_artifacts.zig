const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");

const Database = facet_sqlite.Database;
const c = facet_sqlite.c;

pub const ArtifactRow = struct {
    artifact_id: []const u8,
    slug: []const u8,
    workspace_id: ?[]const u8,
    agent_id: ?[]const u8,
    scope: ?[]const u8,
    artifact_kind: []const u8,
    public_label: []const u8,
    lifecycle: []const u8,
    state: []const u8,
    current_version: i64,
    payload_json: []const u8,
    legacy_ref: ?[]const u8,

    pub fn deinit(self: ArtifactRow, allocator: std.mem.Allocator) void {
        allocator.free(self.artifact_id);
        allocator.free(self.slug);
        if (self.workspace_id) |v| allocator.free(v);
        if (self.agent_id) |v| allocator.free(v);
        if (self.scope) |v| allocator.free(v);
        allocator.free(self.artifact_kind);
        allocator.free(self.public_label);
        allocator.free(self.lifecycle);
        allocator.free(self.state);
        allocator.free(self.payload_json);
        if (self.legacy_ref) |v| allocator.free(v);
    }
};

pub const EventRow = struct {
    event_id: []const u8,
    artifact_id: []const u8,
    event_kind: []const u8,
    from_version: ?i64,
    to_version: ?i64,
    signal_json: []const u8,
    created_at_unix: i64,

    pub fn deinit(self: EventRow, allocator: std.mem.Allocator) void {
        allocator.free(self.event_id);
        allocator.free(self.artifact_id);
        allocator.free(self.event_kind);
        allocator.free(self.signal_json);
    }
};

pub const RepairStats = struct {
    projection_rows: usize = 0,
    projection_result_rows: usize = 0,
    inserted_or_updated: usize = 0,
};

const Candidate = struct {
    kind: []const u8,
    legacy_ref: []const u8,
    workspace_id: ?[]const u8 = null,
    agent_id: ?[]const u8 = null,
    scope: ?[]const u8 = null,
    slug_hint: ?[]const u8 = null,
    label: []const u8,
    state: []const u8,
    lifecycle: []const u8,
    payload_json: []const u8,
};

pub fn repairFromLegacy(db: Database, allocator: std.mem.Allocator) !RepairStats {
    var stats: RepairStats = .{};
    try db.exec("BEGIN IMMEDIATE");
    errdefer db.exec("ROLLBACK") catch {};

    stats.projection_rows = try backfillProjections(db, allocator, &stats);
    stats.projection_result_rows = try backfillProjectionResults(db, allocator, &stats);

    try db.exec("COMMIT");
    return stats;
}

pub fn dryRunLegacyRepair(db: Database) !RepairStats {
    return .{
        .projection_rows = try countRows(db, "SELECT COUNT(*) FROM projections"),
        .projection_result_rows = try countRows(db, "SELECT COUNT(*) FROM graph_entity WHERE entity_type = 'ProjectionResult' AND deprecated_at IS NULL"),
        .inserted_or_updated = 0,
    };
}

pub fn getArtifact(db: Database, allocator: std.mem.Allocator, artifact_id: []const u8) !?ArtifactRow {
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT artifact_id, slug, workspace_id, agent_id, scope, artifact_kind,
        \\       public_label, lifecycle, state, current_version, payload_json, legacy_ref
        \\FROM mindbrain_answer_artifacts
        \\WHERE artifact_id = ?1
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, artifact_id);
    const rc = c.sqlite3_step(stmt);
    if (rc == c.SQLITE_DONE) return null;
    if (rc != c.SQLITE_ROW) return error.StepFailed;
    return try artifactFromStmt(allocator, stmt);
}

pub fn refreshLiveAnswerView(db: Database, allocator: std.mem.Allocator, artifact_id: []const u8) !ArtifactRow {
    try db.exec("BEGIN IMMEDIATE");
    errdefer db.exec("ROLLBACK") catch {};

    var row = (try getArtifact(db, allocator, artifact_id)) orelse return error.MissingRow;
    defer row.deinit(allocator);
    if (!std.mem.eql(u8, row.artifact_kind, "live_answer_view")) return error.ValueOutOfRange;

    const from_version = row.current_version;
    const to_version = from_version + 1;
    const payload_json = if (row.workspace_id) |workspace_id|
        try materializeWorkspacePayload(db, allocator, workspace_id)
    else
        try allocator.dupe(u8, row.payload_json);
    defer allocator.free(payload_json);
    const update_stmt = try facet_sqlite.prepare(db,
        \\UPDATE mindbrain_answer_artifacts
        \\SET current_version = ?2,
        \\    lifecycle = 'active',
        \\    state = 'refreshed',
        \\    payload_json = ?4,
        \\    updated_at_unix = unixepoch()
        \\WHERE artifact_id = ?1 AND current_version = ?3
    );
    defer facet_sqlite.finalize(update_stmt);
    try facet_sqlite.bindText(update_stmt, 1, artifact_id);
    try facet_sqlite.bindInt64(update_stmt, 2, to_version);
    try facet_sqlite.bindInt64(update_stmt, 3, from_version);
    try facet_sqlite.bindText(update_stmt, 4, payload_json);
    try facet_sqlite.stepDone(update_stmt);
    if (c.sqlite3_changes(db.handle) != 1) return error.StepFailed;

    const event_id = try std.fmt.allocPrint(allocator, "answer_update_event__{s}__{d}", .{ artifact_id, to_version });
    defer allocator.free(event_id);
    const signal_json = try std.fmt.allocPrint(allocator, "{{\"refresh\":\"explicit\",\"to_version\":{d}}}", .{to_version});
    defer allocator.free(signal_json);
    const event_stmt = try facet_sqlite.prepare(db,
        \\INSERT OR REPLACE INTO mindbrain_answer_events(event_id, artifact_id, event_kind, from_version, to_version, signal_json)
        \\VALUES (?1, ?2, 'answer_update_event', ?3, ?4, ?5)
    );
    defer facet_sqlite.finalize(event_stmt);
    try facet_sqlite.bindText(event_stmt, 1, event_id);
    try facet_sqlite.bindText(event_stmt, 2, artifact_id);
    try facet_sqlite.bindInt64(event_stmt, 3, from_version);
    try facet_sqlite.bindInt64(event_stmt, 4, to_version);
    try facet_sqlite.bindText(event_stmt, 5, signal_json);
    try facet_sqlite.stepDone(event_stmt);

    try db.exec("COMMIT");
    return (try getArtifact(db, allocator, artifact_id)).?;
}

pub fn markWorkspaceLiveViewsStale(db: Database, workspace_id: []const u8) !usize {
    const stmt = try facet_sqlite.prepare(db,
        \\UPDATE mindbrain_answer_artifacts
        \\SET lifecycle = 'stale',
        \\    state = 'dirty',
        \\    updated_at_unix = unixepoch()
        \\WHERE artifact_kind = 'live_answer_view'
        \\  AND workspace_id = ?1
        \\  AND lifecycle NOT IN ('frozen', 'archived', 'deleted')
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.stepDone(stmt);
    return @intCast(c.sqlite3_changes(db.handle));
}

pub fn listEvents(db: Database, allocator: std.mem.Allocator, artifact_id: []const u8, limit: usize) ![]EventRow {
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT event_id, artifact_id, event_kind, from_version, to_version, signal_json, created_at_unix
        \\FROM mindbrain_answer_events
        \\WHERE artifact_id = ?1
        \\ORDER BY created_at_unix DESC, event_id DESC
        \\LIMIT ?2
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, artifact_id);
    try facet_sqlite.bindInt64(stmt, 2, limit);

    var rows = std.ArrayList(EventRow).empty;
    errdefer {
        for (rows.items) |row| row.deinit(allocator);
        rows.deinit(allocator);
    }
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(allocator, .{
            .event_id = try facet_sqlite.dupeColumnText(allocator, stmt, 0),
            .artifact_id = try facet_sqlite.dupeColumnText(allocator, stmt, 1),
            .event_kind = try facet_sqlite.dupeColumnText(allocator, stmt, 2),
            .from_version = maybeI64(stmt, 3),
            .to_version = maybeI64(stmt, 4),
            .signal_json = try facet_sqlite.dupeColumnText(allocator, stmt, 5),
            .created_at_unix = c.sqlite3_column_int64(stmt, 6),
        });
    }
    return rows.toOwnedSlice(allocator);
}

fn backfillProjections(db: Database, allocator: std.mem.Allocator, stats: *RepairStats) !usize {
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT id, agent_id, scope, proj_type, content, status
        \\FROM projections
        \\ORDER BY agent_id, scope, id
    );
    defer facet_sqlite.finalize(stmt);
    var count: usize = 0;
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        count += 1;
        const projection_id = try facet_sqlite.dupeColumnText(allocator, stmt, 0);
        defer allocator.free(projection_id);
        const agent_id = try facet_sqlite.dupeColumnText(allocator, stmt, 1);
        defer allocator.free(agent_id);
        const scope = try maybeText(allocator, stmt, 2);
        defer if (scope) |v| allocator.free(v);
        const content = try facet_sqlite.dupeColumnText(allocator, stmt, 4);
        defer allocator.free(content);
        const status = try facet_sqlite.dupeColumnText(allocator, stmt, 5);
        defer allocator.free(status);
        const resolved_scope = scope orelse "default";
        const workspace_id = try workspaceForScope(db, allocator, resolved_scope);
        defer allocator.free(workspace_id);
        var parsed_content = parseProjectionContentMetadata(allocator, content);
        defer if (parsed_content) |*parsed| parsed.deinit();
        const slug_hint = if (parsed_content) |parsed| parsed.slug_hint else null;
        const label = if (parsed_content) |parsed| parsed.label orelse content else content;
        const legacy_ref = try std.fmt.allocPrint(allocator, "projection:{s}", .{projection_id});
        defer allocator.free(legacy_ref);
        const payload_json = try std.fmt.allocPrint(allocator, "{{\"projection_id\":{f}}}", .{std.json.fmt(projection_id, .{})});
        defer allocator.free(payload_json);
        try upsertCandidate(db, allocator, .{
            .kind = "analysis_plan",
            .legacy_ref = legacy_ref,
            .workspace_id = workspace_id,
            .agent_id = agent_id,
            .scope = resolved_scope,
            .slug_hint = slug_hint,
            .label = label,
            .state = status,
            .lifecycle = if (std.mem.eql(u8, status, "active")) "active" else "archived",
            .payload_json = payload_json,
        });
        stats.inserted_or_updated += 1;
    }
    return count;
}

fn workspaceForScope(db: Database, allocator: std.mem.Allocator, scope: []const u8) ![]const u8 {
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT workspace_id
        \\FROM workspaces
        \\WHERE ?1 = workspace_id
        \\   OR ?1 LIKE workspace_id || ':%'
        \\ORDER BY length(workspace_id) DESC, workspace_id ASC
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, scope);

    const first_rc = c.sqlite3_step(stmt);
    if (first_rc == c.SQLITE_DONE) return error.MissingWorkspace;
    if (first_rc != c.SQLITE_ROW) return error.StepFailed;
    const workspace_id = try facet_sqlite.dupeColumnText(allocator, stmt, 0);
    errdefer allocator.free(workspace_id);

    const second_rc = c.sqlite3_step(stmt);
    if (second_rc == c.SQLITE_ROW) return error.AmbiguousWorkspace;
    if (second_rc != c.SQLITE_DONE) return error.StepFailed;
    return workspace_id;
}

fn countRows(db: Database, sql: []const u8) !usize {
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StepFailed;
    return @intCast(c.sqlite3_column_int64(stmt, 0));
}

fn countRowsByWorkspace(db: Database, sql: []const u8, workspace_id: []const u8) !usize {
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StepFailed;
    return @intCast(c.sqlite3_column_int64(stmt, 0));
}

fn materializeWorkspacePayload(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8) ![]const u8 {
    const graph_entities = try countRowsByWorkspace(db, "SELECT COUNT(*) FROM graph_entity WHERE workspace_id = ?1 AND deprecated_at IS NULL", workspace_id);
    const graph_relations = try countRowsByWorkspace(db, "SELECT COUNT(*) FROM graph_relation WHERE workspace_id = ?1 AND deprecated_at IS NULL", workspace_id);
    const facts = try countRowsByWorkspace(db, "SELECT COUNT(*) FROM agent_facts WHERE workspace_id = ?1", workspace_id);
    return try std.fmt.allocPrint(
        allocator,
        "{{\"workspace_id\":{f},\"graph_entities\":{},\"graph_relations\":{},\"facts\":{}}}",
        .{ std.json.fmt(workspace_id, .{}), graph_entities, graph_relations, facts },
    );
}

const ProjectionContentMetadata = struct {
    parsed: std.json.Parsed(std.json.Value),
    slug_hint: ?[]const u8,
    label: ?[]const u8,

    fn deinit(self: *ProjectionContentMetadata) void {
        self.parsed.deinit();
    }
};

fn parseProjectionContentMetadata(allocator: std.mem.Allocator, content: []const u8) ?ProjectionContentMetadata {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, content, .{}) catch return null;
    if (parsed.value != .object) {
        parsed.deinit();
        return null;
    }

    const object = parsed.value.object;
    const slug_hint = firstStringField(object, &.{ "name", "slug", "label" });
    const label = firstStringField(object, &.{ "label", "public_label", "name" });
    if (slug_hint == null and label == null) {
        parsed.deinit();
        return null;
    }

    return .{
        .parsed = parsed,
        .slug_hint = slug_hint,
        .label = label,
    };
}

fn firstStringField(
    object: std.json.ObjectMap,
    comptime fields: []const []const u8,
) ?[]const u8 {
    inline for (fields) |field| {
        if (object.get(field)) |value| {
            if (value == .string and std.mem.trim(u8, value.string, " \t\r\n").len > 0) {
                return value.string;
            }
        }
    }
    return null;
}

fn backfillProjectionResults(db: Database, allocator: std.mem.Allocator, stats: *RepairStats) !usize {
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT entity_id, workspace_id, name, metadata_json
        \\FROM graph_entity
        \\WHERE entity_type = 'ProjectionResult' AND deprecated_at IS NULL
        \\ORDER BY workspace_id, entity_id
    );
    defer facet_sqlite.finalize(stmt);
    var count: usize = 0;
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        count += 1;
        const entity_id = c.sqlite3_column_int64(stmt, 0);
        const workspace_id = try facet_sqlite.dupeColumnText(allocator, stmt, 1);
        defer allocator.free(workspace_id);
        const name = try facet_sqlite.dupeColumnText(allocator, stmt, 2);
        defer allocator.free(name);
        const metadata_json = try facet_sqlite.dupeColumnText(allocator, stmt, 3);
        defer allocator.free(metadata_json);
        const legacy_ref = try std.fmt.allocPrint(allocator, "graph_entity:{d}", .{entity_id});
        defer allocator.free(legacy_ref);
        try upsertCandidate(db, allocator, .{
            .kind = "answer_snapshot",
            .legacy_ref = legacy_ref,
            .workspace_id = workspace_id,
            .label = name,
            .state = "frozen",
            .lifecycle = "frozen",
            .payload_json = metadata_json,
        });
        stats.inserted_or_updated += 1;
    }
    return count;
}

fn upsertCandidate(db: Database, allocator: std.mem.Allocator, candidate: Candidate) !void {
    const slug = try slugForCandidate(db, allocator, candidate);
    defer allocator.free(slug);
    const artifact_id = try std.fmt.allocPrint(allocator, "{s}__{s}", .{ candidate.kind, slug });
    defer allocator.free(artifact_id);
    if (try updateCandidateByLegacy(db, allocator, candidate, slug, artifact_id)) return;
    const stmt = try facet_sqlite.prepare(db,
        \\INSERT INTO mindbrain_answer_artifacts(
        \\  artifact_id, slug, workspace_id, agent_id, scope, artifact_kind,
        \\  public_label, lifecycle, state, current_version, payload_json, legacy_ref,
        \\  updated_at_unix
        \\) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1, ?10, ?11, unixepoch())
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, artifact_id);
    try facet_sqlite.bindText(stmt, 2, slug);
    if (candidate.workspace_id) |workspace_id| {
        try facet_sqlite.bindText(stmt, 3, workspace_id);
    } else {
        return error.MissingWorkspace;
    }
    try bindOptionalText(stmt, 4, candidate.agent_id);
    try bindOptionalText(stmt, 5, candidate.scope);
    try facet_sqlite.bindText(stmt, 6, candidate.kind);
    try facet_sqlite.bindText(stmt, 7, candidate.label);
    try facet_sqlite.bindText(stmt, 8, candidate.lifecycle);
    try facet_sqlite.bindText(stmt, 9, candidate.state);
    try facet_sqlite.bindText(stmt, 10, candidate.payload_json);
    try facet_sqlite.bindText(stmt, 11, candidate.legacy_ref);
    try facet_sqlite.stepDone(stmt);
}

fn updateCandidateByLegacy(db: Database, allocator: std.mem.Allocator, candidate: Candidate, slug: []const u8, artifact_id: []const u8) !bool {
    const existing_artifact_id = try artifactIdByLegacy(db, allocator, candidate.legacy_ref);
    defer if (existing_artifact_id) |v| allocator.free(v);

    const stmt = try facet_sqlite.prepare(db,
        \\UPDATE mindbrain_answer_artifacts
        \\SET public_label = ?2,
        \\    lifecycle = ?3,
        \\    state = ?4,
        \\    payload_json = ?5,
        \\    artifact_id = ?6,
        \\    slug = ?7,
        \\    workspace_id = ?8,
        \\    updated_at_unix = unixepoch()
        \\WHERE legacy_ref = ?1
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, candidate.legacy_ref);
    try facet_sqlite.bindText(stmt, 2, candidate.label);
    try facet_sqlite.bindText(stmt, 3, candidate.lifecycle);
    try facet_sqlite.bindText(stmt, 4, candidate.state);
    try facet_sqlite.bindText(stmt, 5, candidate.payload_json);
    try facet_sqlite.bindText(stmt, 6, artifact_id);
    try facet_sqlite.bindText(stmt, 7, slug);
    if (candidate.workspace_id) |workspace_id| {
        try facet_sqlite.bindText(stmt, 8, workspace_id);
    } else {
        return error.MissingWorkspace;
    }
    try facet_sqlite.stepDone(stmt);
    const changed = c.sqlite3_changes(db.handle) > 0;
    if (changed) {
        if (existing_artifact_id) |old_id| {
            if (!std.mem.eql(u8, old_id, artifact_id)) {
                try updateEventsArtifactId(db, old_id, artifact_id);
            }
        }
    }
    return changed;
}

fn artifactIdByLegacy(db: Database, allocator: std.mem.Allocator, legacy_ref: []const u8) !?[]const u8 {
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT artifact_id
        \\FROM mindbrain_answer_artifacts
        \\WHERE legacy_ref = ?1
        \\LIMIT 1
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, legacy_ref);
    const rc = c.sqlite3_step(stmt);
    if (rc == c.SQLITE_DONE) return null;
    if (rc != c.SQLITE_ROW) return error.StepFailed;
    return try facet_sqlite.dupeColumnText(allocator, stmt, 0);
}

fn updateEventsArtifactId(db: Database, old_artifact_id: []const u8, new_artifact_id: []const u8) !void {
    const stmt = try facet_sqlite.prepare(db,
        \\UPDATE mindbrain_answer_events
        \\SET artifact_id = ?2
        \\WHERE artifact_id = ?1
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, old_artifact_id);
    try facet_sqlite.bindText(stmt, 2, new_artifact_id);
    try facet_sqlite.stepDone(stmt);
}

fn slugForCandidate(db: Database, allocator: std.mem.Allocator, candidate: Candidate) ![]const u8 {
    const base = try slugify(allocator, candidate.slug_hint orelse candidate.label);
    defer allocator.free(base);
    var suffix: usize = 1;
    while (true) : (suffix += 1) {
        const slug = if (suffix == 1)
            try allocator.dupe(u8, base)
        else
            try std.fmt.allocPrint(allocator, "{s}__{d}", .{ base, suffix });
        if (!try slugTakenByOtherLegacy(db, candidate, slug)) return slug;
        allocator.free(slug);
    }
}

fn slugTakenByOtherLegacy(db: Database, candidate: Candidate, slug: []const u8) !bool {
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT legacy_ref
        \\FROM mindbrain_answer_artifacts
        \\WHERE artifact_kind = ?1
        \\  AND slug = ?2
        \\  AND COALESCE(workspace_id, '') = COALESCE(?3, '')
        \\  AND COALESCE(agent_id, '') = COALESCE(?4, '')
        \\  AND COALESCE(scope, '') = COALESCE(?5, '')
        \\LIMIT 1
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, candidate.kind);
    try facet_sqlite.bindText(stmt, 2, slug);
    try bindOptionalText(stmt, 3, candidate.workspace_id);
    try bindOptionalText(stmt, 4, candidate.agent_id);
    try bindOptionalText(stmt, 5, candidate.scope);
    const rc = c.sqlite3_step(stmt);
    if (rc == c.SQLITE_DONE) return false;
    if (rc != c.SQLITE_ROW) return error.StepFailed;
    const existing = std.mem.span(c.sqlite3_column_text(stmt, 0));
    return !std.mem.eql(u8, existing, candidate.legacy_ref);
}

fn slugify(allocator: std.mem.Allocator, input: []const u8) ![]const u8 {
    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    var last_sep = false;
    for (input) |ch| {
        const mapped: ?u8 = if (std.ascii.isAlphanumeric(ch))
            std.ascii.toLower(ch)
        else if (ch == '-' or ch == '_' or std.ascii.isWhitespace(ch))
            '_'
        else
            null;
        if (mapped) |value| {
            if (value == '_') {
                if (out.items.len == 0 or last_sep) continue;
                last_sep = true;
            } else {
                last_sep = false;
            }
            try out.append(allocator, value);
        }
        if (out.items.len >= 80) break;
    }
    while (out.items.len > 0 and out.items[out.items.len - 1] == '_') {
        _ = out.pop();
    }
    if (out.items.len == 0) try out.appendSlice(allocator, "artifact");
    return out.toOwnedSlice(allocator);
}

fn artifactFromStmt(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt) !ArtifactRow {
    return .{
        .artifact_id = try facet_sqlite.dupeColumnText(allocator, stmt, 0),
        .slug = try facet_sqlite.dupeColumnText(allocator, stmt, 1),
        .workspace_id = try maybeText(allocator, stmt, 2),
        .agent_id = try maybeText(allocator, stmt, 3),
        .scope = try maybeText(allocator, stmt, 4),
        .artifact_kind = try facet_sqlite.dupeColumnText(allocator, stmt, 5),
        .public_label = try facet_sqlite.dupeColumnText(allocator, stmt, 6),
        .lifecycle = try facet_sqlite.dupeColumnText(allocator, stmt, 7),
        .state = try facet_sqlite.dupeColumnText(allocator, stmt, 8),
        .current_version = c.sqlite3_column_int64(stmt, 9),
        .payload_json = try facet_sqlite.dupeColumnText(allocator, stmt, 10),
        .legacy_ref = try maybeText(allocator, stmt, 11),
    };
}

fn bindOptionalText(stmt: *c.sqlite3_stmt, index: c_int, value: ?[]const u8) !void {
    if (value) |v| try facet_sqlite.bindText(stmt, index, v) else try facet_sqlite.bindNull(stmt, index);
}

fn maybeText(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, index: c_int) !?[]const u8 {
    if (c.sqlite3_column_type(stmt, index) == c.SQLITE_NULL) return null;
    return try facet_sqlite.dupeColumnText(allocator, stmt, index);
}

fn maybeI64(stmt: *c.sqlite3_stmt, index: c_int) ?i64 {
    if (c.sqlite3_column_type(stmt, index) == c.SQLITE_NULL) return null;
    return c.sqlite3_column_int64(stmt, index);
}

test "answer artifact repair backfills projections and projection results idempotently" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES
        \\  ('scope-a', 'scope-a', 'Scope A'),
        \\  ('ws-a', 'ws-a', 'Workspace A');
        \\INSERT INTO projections(id, agent_id, scope, proj_type, content, status) VALUES
        \\  ('p1', 'agent-a', 'scope-a', 'FACT', 'Weekly Pilotage', 'active'),
        \\  ('p2', 'agent-a', 'scope-a', 'GOAL', 'Weekly Pilotage', 'active'),
        \\  ('p3', 'agent-a', 'scope-a', 'STEP', '{"artifact_kind":"analysis_plan","name":"copropriete_360","label":"Copropriete 360"}', 'active');
        \\INSERT INTO mindbrain_answer_artifacts(
        \\  artifact_id, slug, workspace_id, agent_id, scope, artifact_kind, public_label, lifecycle, state, payload_json, legacy_ref
        \\) VALUES (
        \\  'analysis_plan__artifact_kindanalysis_plannamecopropriete_360',
        \\  'artifact_kindanalysis_plannamecopropriete_360',
        \\  'scope-a',
        \\  'agent-a',
        \\  'scope-a',
        \\  'analysis_plan',
        \\  '{"artifact_kind":"analysis_plan","name":"copropriete_360","label":"Copropriete 360"}',
        \\  'active',
        \\  'active',
        \\  '{}',
        \\  'projection:p3'
        \\);
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name, metadata_json) VALUES
        \\  (1, 'ws-a', 'ProjectionResult', 'Frozen Result', '{"projection_id":"proj-a"}');
    );

    const first = try repairFromLegacy(db, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), first.projection_rows);
    try std.testing.expectEqual(@as(usize, 1), first.projection_result_rows);
    const second = try repairFromLegacy(db, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), second.projection_rows);
    try std.testing.expectEqual(@as(usize, 1), second.projection_result_rows);

    const count_stmt = try facet_sqlite.prepare(db, "SELECT COUNT(*) FROM mindbrain_answer_artifacts");
    defer facet_sqlite.finalize(count_stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(count_stmt));
    try std.testing.expectEqual(@as(i64, 4), c.sqlite3_column_int64(count_stmt, 0));

    const slug_stmt = try facet_sqlite.prepare(db, "SELECT slug FROM mindbrain_answer_artifacts WHERE legacy_ref = 'projection:p2'");
    defer facet_sqlite.finalize(slug_stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(slug_stmt));
    try std.testing.expectEqualStrings("weekly_pilotage__2", std.mem.span(c.sqlite3_column_text(slug_stmt, 0)));

    const json_slug_stmt = try facet_sqlite.prepare(db, "SELECT artifact_id, slug, public_label FROM mindbrain_answer_artifacts WHERE legacy_ref = 'projection:p3'");
    defer facet_sqlite.finalize(json_slug_stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(json_slug_stmt));
    try std.testing.expectEqualStrings("analysis_plan__copropriete_360", std.mem.span(c.sqlite3_column_text(json_slug_stmt, 0)));
    try std.testing.expectEqualStrings("copropriete_360", std.mem.span(c.sqlite3_column_text(json_slug_stmt, 1)));
    try std.testing.expectEqualStrings("Copropriete 360", std.mem.span(c.sqlite3_column_text(json_slug_stmt, 2)));
}

test "answer artifact repair normalizes json plans, scoped rows, events, and collisions" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES
        \\  ('serenity-v4', 'serenity-v4', 'Serenity V4');
        \\INSERT INTO projections(id, agent_id, scope, proj_type, content, status) VALUES
        \\  ('p_json_bad', 'agent:self', 'serenity-v4:production:copropriete_360', 'STEP', '{"artifact_kind":"analysis_plan","name":"copropriete_360","label":"Copropriete 360"}', 'active'),
        \\  ('p_json_collision', 'agent:self', 'serenity-v4:production:copropriete_360', 'GOAL', '{"artifact_kind":"analysis_plan","label":"Copropriete 360"}', 'active'),
        \\  ('p_plain', 'agent:self', 'serenity-v4:production:plain', 'FACT', 'Plain fallback plan', 'resolved');
        \\INSERT INTO mindbrain_answer_artifacts(
        \\  artifact_id, slug, workspace_id, agent_id, scope, artifact_kind, public_label, lifecycle, state, payload_json, legacy_ref
        \\) VALUES (
        \\  'analysis_plan__artifact_kindanalysis_plannamecopropriete_360labelcopropriete_360',
        \\  'artifact_kindanalysis_plannamecopropriete_360labelcopropriete_360',
        \\  'serenity-v4',
        \\  'agent:self',
        \\  'serenity-v4:production:copropriete_360',
        \\  'analysis_plan',
        \\  '{"artifact_kind":"analysis_plan","name":"copropriete_360","label":"Copropriete 360"}',
        \\  'active',
        \\  'active',
        \\  '{}',
        \\  'projection:p_json_bad'
        \\);
        \\INSERT INTO mindbrain_answer_events(
        \\  event_id, artifact_id, event_kind, from_version, to_version, signal_json
        \\) VALUES (
        \\  'evt_bad_plan', 'analysis_plan__artifact_kindanalysis_plannamecopropriete_360labelcopropriete_360',
        \\  'answer_update_event', 1, 2, '{"repair":"legacy-id"}'
        \\);
    );

    const first = try repairFromLegacy(db, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), first.projection_rows);
    try std.testing.expectEqual(@as(usize, 0), first.projection_result_rows);
    const second = try repairFromLegacy(db, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), second.projection_rows);
    try std.testing.expectEqual(@as(usize, 0), second.projection_result_rows);

    const canonical_stmt = try facet_sqlite.prepare(db,
        \\SELECT artifact_id, slug, workspace_id, public_label
        \\FROM mindbrain_answer_artifacts
        \\WHERE legacy_ref = 'projection:p_json_bad'
    );
    defer facet_sqlite.finalize(canonical_stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(canonical_stmt));
    try std.testing.expectEqualStrings("analysis_plan__copropriete_360", std.mem.span(c.sqlite3_column_text(canonical_stmt, 0)));
    try std.testing.expectEqualStrings("copropriete_360", std.mem.span(c.sqlite3_column_text(canonical_stmt, 1)));
    try std.testing.expectEqualStrings("serenity-v4", std.mem.span(c.sqlite3_column_text(canonical_stmt, 2)));
    try std.testing.expectEqualStrings("Copropriete 360", std.mem.span(c.sqlite3_column_text(canonical_stmt, 3)));

    const collision_stmt = try facet_sqlite.prepare(db,
        \\SELECT artifact_id, slug
        \\FROM mindbrain_answer_artifacts
        \\WHERE legacy_ref = 'projection:p_json_collision'
    );
    defer facet_sqlite.finalize(collision_stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(collision_stmt));
    try std.testing.expectEqualStrings("analysis_plan__copropriete_360__2", std.mem.span(c.sqlite3_column_text(collision_stmt, 0)));
    try std.testing.expectEqualStrings("copropriete_360__2", std.mem.span(c.sqlite3_column_text(collision_stmt, 1)));

    const fallback_stmt = try facet_sqlite.prepare(db,
        \\SELECT slug, lifecycle
        \\FROM mindbrain_answer_artifacts
        \\WHERE legacy_ref = 'projection:p_plain'
    );
    defer facet_sqlite.finalize(fallback_stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(fallback_stmt));
    try std.testing.expectEqualStrings("plain_fallback_plan", std.mem.span(c.sqlite3_column_text(fallback_stmt, 0)));
    try std.testing.expectEqualStrings("archived", std.mem.span(c.sqlite3_column_text(fallback_stmt, 1)));

    const event_stmt = try facet_sqlite.prepare(db,
        \\SELECT artifact_id
        \\FROM mindbrain_answer_events
        \\WHERE event_id = 'evt_bad_plan'
    );
    defer facet_sqlite.finalize(event_stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(event_stmt));
    try std.testing.expectEqualStrings("analysis_plan__copropriete_360", std.mem.span(c.sqlite3_column_text(event_stmt, 0)));

    const count_stmt = try facet_sqlite.prepare(db, "SELECT COUNT(*) FROM mindbrain_answer_artifacts");
    defer facet_sqlite.finalize(count_stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(count_stmt));
    try std.testing.expectEqual(@as(i64, 3), c.sqlite3_column_int64(count_stmt, 0));
}

test "live answer view refresh increments version and writes event" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO mindbrain_answer_artifacts(
        \\  artifact_id, slug, workspace_id, artifact_kind, public_label, lifecycle, state, payload_json
        \\) VALUES (
        \\  'live_answer_view__weekly', 'weekly', 'ws-a', 'live_answer_view', 'Weekly', 'stale', 'dirty', '{}'
        \\);
    );

    var row = try refreshLiveAnswerView(db, std.testing.allocator, "live_answer_view__weekly");
    defer row.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(i64, 2), row.current_version);
    try std.testing.expectEqualStrings("refreshed", row.state);
    try std.testing.expect(std.mem.indexOf(u8, row.payload_json, "\"workspace_id\":\"ws-a\"") != null);

    const events = try listEvents(db, std.testing.allocator, "live_answer_view__weekly", 10);
    defer {
        for (events) |event| event.deinit(std.testing.allocator);
        std.testing.allocator.free(events);
    }
    try std.testing.expectEqual(@as(usize, 1), events.len);
    try std.testing.expectEqual(@as(i64, 1), events[0].from_version.?);
    try std.testing.expectEqual(@as(i64, 2), events[0].to_version.?);
}

test "workspace writes can mark live answer views stale" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO mindbrain_answer_artifacts(
        \\  artifact_id, slug, workspace_id, artifact_kind, public_label, lifecycle, state, payload_json
        \\) VALUES (
        \\  'live_answer_view__stale', 'stale', 'ws-a', 'live_answer_view', 'Stale', 'active', 'refreshed', '{}'
        \\);
    );
    try std.testing.expectEqual(@as(usize, 1), try markWorkspaceLiveViewsStale(db, "ws-a"));
    var row = (try getArtifact(db, std.testing.allocator, "live_answer_view__stale")).?;
    defer row.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("stale", row.lifecycle);
    try std.testing.expectEqualStrings("dirty", row.state);
}
