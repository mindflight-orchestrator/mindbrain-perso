const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const graph_diagnostics = @import("graph_diagnostics.zig");
const ontology_sqlite = @import("ontology_sqlite.zig");
const zig16_compat = @import("zig16_compat.zig");

pub const Database = facet_sqlite.Database;
pub const c = facet_sqlite.c;

pub const RunOptions = struct {
    workspace_id: []const u8,
    ontology_id: ?[]const u8 = null,
    persist: bool = true,
    limit: usize = 200,
    component_small_max: usize = 2,
};

const Counts = struct {
    registry_schemas: u64 = 0,
    registry_graph_node_schemas: u64 = 0,
    registry_graph_edge_schemas: u64 = 0,
    registry_facet_definitions: u64 = 0,
    registry_property_facets: u64 = 0,
    native_ontologies: u64 = 0,
    native_entity_types: u64 = 0,
    native_edge_types: u64 = 0,
    native_unique_edge_labels: u64 = 0,
    native_values: u64 = 0,
    native_triples: u64 = 0,
    graph_entities: u64 = 0,
    graph_relations: u64 = 0,
    graph_relation_properties: u64 = 0,
    projections: u64 = 0,
    projection_results: u64 = 0,
    coverage_total_nodes: u64 = 0,
    coverage_covered_nodes: u64 = 0,
    coverage_gaps: u64 = 0,
    diagnostics_issues: u64 = 0,
};

const Action = struct {
    action_id: []const u8,
    issue_type: []const u8,
    severity: []const u8,
    confidence: f64,
    reason: []const u8,
    schema_id: ?[]const u8 = null,
    entity_type: ?[]const u8 = null,
    projection_id: ?[]const u8 = null,
    evidence_json: []const u8 = "{}",
    mcp_tool: ?[]const u8 = null,
    tool_args_json: []const u8 = "{}",
    execution_mode: []const u8,
    idempotency_key: []const u8,
};

pub const RunResult = struct {
    run_id: []const u8,
    workspace_id: []const u8,
    ontology_id: ?[]const u8,
    summary_json: []const u8,
    report_json: []const u8,
    actions_json: []const u8,
    actions_total: usize,
    persisted: bool,

    pub fn deinit(self: RunResult, allocator: std.mem.Allocator) void {
        allocator.free(self.run_id);
        allocator.free(self.workspace_id);
        if (self.ontology_id) |value| allocator.free(value);
        allocator.free(self.summary_json);
        allocator.free(self.report_json);
        allocator.free(self.actions_json);
    }
};

pub fn runConvergence(db: Database, allocator: std.mem.Allocator, options: RunOptions) !RunResult {
    const ontology_id = if (options.ontology_id) |value|
        try allocator.dupe(u8, value)
    else
        try graph_diagnostics.resolveOntologyId(db, allocator, options.workspace_id, null);
    defer allocator.free(ontology_id);

    var counts = try loadCounts(db, options.workspace_id, ontology_id);

    const coverage = ontology_sqlite.coverageReport(db, allocator, options.workspace_id, null) catch null;
    defer if (coverage) |report| deinitCoverageReport(allocator, report);
    if (coverage) |report| {
        counts.coverage_total_nodes = report.summary.total_nodes;
        counts.coverage_covered_nodes = report.summary.covered_nodes;
        counts.coverage_gaps = report.gaps.len;
    }

    var diagnostics = graph_diagnostics.buildReport(db, allocator, .{
        .workspace_id = options.workspace_id,
        .ontology_id = ontology_id,
        .limit = options.limit,
        .component_small_max = options.component_small_max,
    }) catch null;
    defer if (diagnostics) |*report| report.deinit(allocator);
    if (diagnostics) |report| counts.diagnostics_issues = report.issues.len;

    const run_id = try makeRunId(allocator, options.workspace_id);
    errdefer allocator.free(run_id);
    const fingerprint = try makeFingerprint(allocator, options.workspace_id, ontology_id, counts);
    defer allocator.free(fingerprint);
    const summary_json = try renderSummaryJson(allocator, counts);
    errdefer allocator.free(summary_json);

    var actions = std.ArrayList(Action).empty;
    defer {
        for (actions.items) |action| deinitAction(allocator, action);
        actions.deinit(allocator);
    }
    try buildActions(allocator, &actions, options.workspace_id, ontology_id, counts);

    const actions_json = try renderActionsJson(allocator, actions.items);
    errdefer allocator.free(actions_json);
    const report_json = try renderReportJson(allocator, .{
        .run_id = run_id,
        .workspace_id = options.workspace_id,
        .ontology_id = ontology_id,
        .fingerprint = fingerprint,
        .counts = counts,
        .actions = actions.items,
    });
    errdefer allocator.free(report_json);

    if (options.persist) {
        try persistRun(db, run_id, options.workspace_id, ontology_id, fingerprint, summary_json, report_json);
        for (actions.items) |action| {
            try persistAction(db, run_id, options.workspace_id, ontology_id, action);
        }
    }

    return .{
        .run_id = run_id,
        .workspace_id = try allocator.dupe(u8, options.workspace_id),
        .ontology_id = try allocator.dupe(u8, ontology_id),
        .summary_json = summary_json,
        .report_json = report_json,
        .actions_json = actions_json,
        .actions_total = actions.items.len,
        .persisted = options.persist,
    };
}

pub fn listRunsJson(db: Database, allocator: std.mem.Allocator, workspace_id: []const u8, limit: usize) ![]const u8 {
    const stmt = try prepare(db,
        \\SELECT run_id, workspace_id, ontology_id, run_kind, status, canonical_layer,
        \\       summary_json, created_at_unix, updated_at_unix
        \\FROM quality_convergence_run
        \\WHERE workspace_id = ?1
        \\ORDER BY created_at_unix DESC
        \\LIMIT ?2
    );
    defer finalize(stmt);
    try bindText(stmt, 1, workspace_id);
    try bindInt64(stmt, 2, @intCast(limit));

    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    try out.writer.writeAll("{\"kind\":\"quality_convergence_runs\",\"workspace_id\":");
    try writeJsonString(&out.writer, workspace_id);
    try out.writer.writeAll(",\"runs\":[");
    var first = true;
    while (try stepRow(stmt)) {
        if (!first) try out.writer.writeAll(",");
        first = false;
        try out.writer.writeAll("{\"run_id\":");
        try writeJsonString(&out.writer, columnText(stmt, 0));
        try out.writer.writeAll(",\"workspace_id\":");
        try writeJsonString(&out.writer, columnText(stmt, 1));
        try out.writer.writeAll(",\"ontology_id\":");
        try writeOptionalColumnString(&out.writer, stmt, 2);
        try out.writer.writeAll(",\"run_kind\":");
        try writeJsonString(&out.writer, columnText(stmt, 3));
        try out.writer.writeAll(",\"status\":");
        try writeJsonString(&out.writer, columnText(stmt, 4));
        try out.writer.writeAll(",\"canonical_layer\":");
        try writeJsonString(&out.writer, columnText(stmt, 5));
        try out.writer.writeAll(",\"summary\":");
        try writeRawJsonObject(&out.writer, columnText(stmt, 6));
        try out.writer.print(",\"created_at_unix\":{},\"updated_at_unix\":{}", .{ c.sqlite3_column_int64(stmt, 7), c.sqlite3_column_int64(stmt, 8) });
        try out.writer.writeAll("}");
    }
    try out.writer.writeAll("]}");
    return try out.toOwnedSlice();
}

pub fn getRunJson(db: Database, allocator: std.mem.Allocator, run_id: []const u8) ![]const u8 {
    const stmt = try prepare(db, "SELECT report_json FROM quality_convergence_run WHERE run_id = ?1");
    defer finalize(stmt);
    try bindText(stmt, 1, run_id);
    if (!try stepRow(stmt)) return error.NotFound;
    return try allocator.dupe(u8, columnText(stmt, 0));
}

pub fn actionsJson(db: Database, allocator: std.mem.Allocator, run_id: []const u8, status: ?[]const u8) ![]const u8 {
    const sql =
        if (status) |_|
            "SELECT action_id, issue_type, severity, confidence, reason, schema_id, entity_type, projection_id, evidence_json, mcp_tool, tool_args_json, execution_mode, idempotency_key, status, decision_actor, decision_note, result_json, created_at_unix, updated_at_unix FROM quality_remediation_action WHERE run_id = ?1 AND status = ?2 ORDER BY severity DESC, action_id"
        else
            "SELECT action_id, issue_type, severity, confidence, reason, schema_id, entity_type, projection_id, evidence_json, mcp_tool, tool_args_json, execution_mode, idempotency_key, status, decision_actor, decision_note, result_json, created_at_unix, updated_at_unix FROM quality_remediation_action WHERE run_id = ?1 ORDER BY severity DESC, action_id";
    const stmt = try prepare(db, sql);
    defer finalize(stmt);
    try bindText(stmt, 1, run_id);
    if (status) |value| try bindText(stmt, 2, value);

    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    try out.writer.writeAll("{\"kind\":\"quality_remediation_actions\",\"run_id\":");
    try writeJsonString(&out.writer, run_id);
    try out.writer.writeAll(",\"actions\":[");
    var first = true;
    while (try stepRow(stmt)) {
        if (!first) try out.writer.writeAll(",");
        first = false;
        try writeActionRowJson(&out.writer, stmt);
    }
    try out.writer.writeAll("]}");
    return try out.toOwnedSlice();
}

pub fn decideAction(db: Database, action_id: []const u8, decision: []const u8, actor: ?[]const u8, note: ?[]const u8) !void {
    if (!std.mem.eql(u8, decision, "approved") and !std.mem.eql(u8, decision, "rejected")) return error.InvalidDecision;
    const stmt = try prepare(db,
        \\UPDATE quality_remediation_action
        \\SET status = ?2,
        \\    decision_actor = ?3,
        \\    decision_note = ?4,
        \\    decided_at_unix = unixepoch(),
        \\    updated_at_unix = unixepoch()
        \\WHERE action_id = ?1 AND status IN ('proposed', 'approved', 'rejected')
    );
    defer finalize(stmt);
    try bindText(stmt, 1, action_id);
    try bindText(stmt, 2, decision);
    if (actor) |value| try bindText(stmt, 3, value) else try bindNull(stmt, 3);
    if (note) |value| try bindText(stmt, 4, value) else try bindNull(stmt, 4);
    try stepDone(stmt);
}

pub fn updateActionStatus(db: Database, action_id: []const u8, status: []const u8, result_json: ?[]const u8) !void {
    if (!isActionStatus(status)) return error.InvalidStatus;
    const stmt = try prepare(db,
        \\UPDATE quality_remediation_action
        \\SET status = ?2,
        \\    result_json = ?3,
        \\    updated_at_unix = unixepoch(),
        \\    applied_at_unix = CASE WHEN ?2 = 'applied' THEN unixepoch() ELSE applied_at_unix END
        \\WHERE action_id = ?1
    );
    defer finalize(stmt);
    try bindText(stmt, 1, action_id);
    try bindText(stmt, 2, status);
    try bindText(stmt, 3, result_json orelse "{}");
    try stepDone(stmt);
}

fn loadCounts(db: Database, workspace_id: []const u8, ontology_id: []const u8) !Counts {
    return .{
        .registry_schemas = try count(db, "SELECT COUNT(*) FROM agent_facts WHERE workspace_id = ?1 AND schema_id = 'mindbrain:schema'", workspace_id),
        .registry_graph_node_schemas = try count(db, "SELECT COUNT(*) FROM agent_facts WHERE workspace_id = ?1 AND schema_id = 'mindbrain:schema' AND json_extract(facets_json, '$.target') = 'graph_node'", workspace_id),
        .registry_graph_edge_schemas = try count(db, "SELECT COUNT(*) FROM agent_facts WHERE workspace_id = ?1 AND schema_id = 'mindbrain:schema' AND json_extract(facets_json, '$.target') = 'graph_edge'", workspace_id),
        .registry_facet_definitions = try count(db, "SELECT COUNT(*) FROM agent_facts WHERE workspace_id = ?1 AND schema_id = 'mindbrain:facet-definition'", workspace_id),
        .registry_property_facets = try count(db, "SELECT COUNT(*) FROM agent_facts WHERE workspace_id = ?1 AND schema_id = 'mindbrain:facet-definition' AND json_extract(facets_json, '$.facet_name') LIKE '%.property.%'", workspace_id),
        .native_ontologies = try count(db, "SELECT COUNT(*) FROM ontologies WHERE workspace_id = ?1", workspace_id),
        .native_entity_types = try countForOntology(db, "SELECT COUNT(*) FROM ontology_entity_types WHERE ontology_id = ?1", ontology_id),
        .native_edge_types = try countForOntology(db, "SELECT COUNT(*) FROM ontology_edge_types WHERE ontology_id = ?1", ontology_id),
        .native_unique_edge_labels = try countForOntology(db, "SELECT COUNT(DISTINCT edge_type) FROM ontology_edge_types WHERE ontology_id = ?1", ontology_id),
        .native_values = try countForOntology(db, "SELECT COUNT(*) FROM ontology_values WHERE ontology_id = ?1", ontology_id),
        .native_triples = try countForOntology(db, "SELECT COUNT(*) FROM ontology_triples_raw WHERE ontology_id = ?1", ontology_id),
        .graph_entities = try count(db, "SELECT COUNT(*) FROM graph_entity WHERE workspace_id = ?1 AND deprecated_at IS NULL", workspace_id),
        .graph_relations = try count(db, "SELECT COUNT(*) FROM graph_relation WHERE workspace_id = ?1 AND deprecated_at IS NULL", workspace_id),
        .graph_relation_properties = try count(db, "SELECT COUNT(*) FROM graph_relation_property grp JOIN graph_relation gr ON gr.relation_id = grp.relation_id WHERE gr.workspace_id = ?1 AND gr.deprecated_at IS NULL", workspace_id),
        .projections = try count(db, "SELECT COUNT(*) FROM projections WHERE scope = ?1 OR scope IS NULL", workspace_id),
        .projection_results = try count(db, "SELECT COUNT(*) FROM graph_entity WHERE workspace_id = ?1 AND entity_type = 'ProjectionResult' AND deprecated_at IS NULL", workspace_id),
    };
}

fn buildActions(allocator: std.mem.Allocator, actions: *std.ArrayList(Action), workspace_id: []const u8, ontology_id: []const u8, counts: Counts) !void {
    if (counts.registry_graph_node_schemas != counts.native_entity_types) {
        try appendAction(allocator, actions, workspace_id, .{
            .issue_type = "native_entity_registry_mismatch",
            .severity = "high",
            .confidence = 0.9,
            .reason = try std.fmt.allocPrint(allocator, "Registry has {} graph_node schemas while native ontology has {} entity types.", .{ counts.registry_graph_node_schemas, counts.native_entity_types }),
            .evidence_json = try std.fmt.allocPrint(allocator, "{{\"registry_graph_node_schemas\":{},\"native_entity_types\":{}}}", .{ counts.registry_graph_node_schemas, counts.native_entity_types }),
            .mcp_tool = "ghostcrab_schema_sync_preview",
            .tool_args_json = try std.fmt.allocPrint(allocator, "{{\"workspace_id\":{f},\"ontology_id\":{f}}}", .{ std.json.fmt(workspace_id, .{}), std.json.fmt(ontology_id, .{}) }),
            .execution_mode = "manual",
        });
    }
    if (counts.registry_graph_edge_schemas > 0 and counts.native_unique_edge_labels > 0 and counts.registry_graph_edge_schemas != counts.native_unique_edge_labels) {
        try appendAction(allocator, actions, workspace_id, .{
            .issue_type = "native_edge_registry_granularity_mismatch",
            .severity = "medium",
            .confidence = 0.85,
            .reason = try std.fmt.allocPrint(allocator, "Registry has {} graph_edge schemas while native ontology has {} unique edge labels.", .{ counts.registry_graph_edge_schemas, counts.native_unique_edge_labels }),
            .evidence_json = try std.fmt.allocPrint(allocator, "{{\"registry_graph_edge_schemas\":{},\"native_unique_edge_labels\":{},\"native_edge_types\":{}}}", .{ counts.registry_graph_edge_schemas, counts.native_unique_edge_labels, counts.native_edge_types }),
            .mcp_tool = "ghostcrab_schema_sync_preview",
            .tool_args_json = try std.fmt.allocPrint(allocator, "{{\"workspace_id\":{f},\"ontology_id\":{f}}}", .{ std.json.fmt(workspace_id, .{}), std.json.fmt(ontology_id, .{}) }),
            .execution_mode = "manual",
        });
    }
    if (counts.registry_graph_edge_schemas > 0 and counts.graph_entities > 0 and counts.graph_relations == 0) {
        try appendAction(allocator, actions, workspace_id, .{
            .issue_type = "runtime_graph_relation_materialization_gap",
            .severity = "critical",
            .confidence = 0.95,
            .reason = try std.fmt.allocPrint(allocator, "Runtime graph has {} entities but no materialized relations while {} graph_edge schemas exist.", .{ counts.graph_entities, counts.registry_graph_edge_schemas }),
            .evidence_json = try std.fmt.allocPrint(allocator, "{{\"graph_entities\":{},\"graph_relations\":{},\"registry_graph_edge_schemas\":{}}}", .{ counts.graph_entities, counts.graph_relations, counts.registry_graph_edge_schemas }),
            .mcp_tool = "ghostcrab_graph_diagnostics",
            .tool_args_json = try std.fmt.allocPrint(allocator, "{{\"workspace_id\":{f},\"ontology_id\":{f}}}", .{ std.json.fmt(workspace_id, .{}), std.json.fmt(ontology_id, .{}) }),
            .execution_mode = "diagnostic_only",
        });
    }
    if (counts.coverage_gaps > 0) {
        try appendAction(allocator, actions, workspace_id, .{
            .issue_type = "ontology_coverage_gap",
            .severity = "high",
            .confidence = 0.9,
            .reason = try std.fmt.allocPrint(allocator, "Coverage report has {} ontology gaps.", .{counts.coverage_gaps}),
            .evidence_json = try std.fmt.allocPrint(allocator, "{{\"coverage_gaps\":{},\"covered_nodes\":{},\"total_nodes\":{}}}", .{ counts.coverage_gaps, counts.coverage_covered_nodes, counts.coverage_total_nodes }),
            .mcp_tool = "ghostcrab_coverage",
            .tool_args_json = try std.fmt.allocPrint(allocator, "{{\"domain\":{f}}}", .{std.json.fmt(workspace_id, .{})}),
            .execution_mode = "manual",
        });
    }
    if (counts.diagnostics_issues > 0) {
        try appendAction(allocator, actions, workspace_id, .{
            .issue_type = "graph_diagnostics_issues",
            .severity = "high",
            .confidence = 0.9,
            .reason = try std.fmt.allocPrint(allocator, "Graph diagnostics reported {} issues.", .{counts.diagnostics_issues}),
            .evidence_json = try std.fmt.allocPrint(allocator, "{{\"diagnostics_issues\":{}}}", .{counts.diagnostics_issues}),
            .mcp_tool = "ghostcrab_graph_diagnostics",
            .tool_args_json = try std.fmt.allocPrint(allocator, "{{\"workspace_id\":{f},\"ontology_id\":{f}}}", .{ std.json.fmt(workspace_id, .{}), std.json.fmt(ontology_id, .{}) }),
            .execution_mode = "manual",
        });
    }
}

fn appendAction(allocator: std.mem.Allocator, actions: *std.ArrayList(Action), workspace_id: []const u8, partial: struct {
    issue_type: []const u8,
    severity: []const u8,
    confidence: f64,
    reason: []const u8,
    evidence_json: []const u8,
    mcp_tool: ?[]const u8,
    tool_args_json: []const u8,
    execution_mode: []const u8,
}) !void {
    const idem = try hashText(allocator, partial.issue_type, partial.evidence_json);
    errdefer allocator.free(idem);
    const action_id = try std.fmt.allocPrint(allocator, "quality::{s}::{s}", .{ workspace_id, idem[0..12] });
    errdefer allocator.free(action_id);
    try actions.append(allocator, .{
        .action_id = action_id,
        .issue_type = partial.issue_type,
        .severity = partial.severity,
        .confidence = partial.confidence,
        .reason = partial.reason,
        .evidence_json = partial.evidence_json,
        .mcp_tool = partial.mcp_tool,
        .tool_args_json = partial.tool_args_json,
        .execution_mode = partial.execution_mode,
        .idempotency_key = idem,
    });
}

fn renderSummaryJson(allocator: std.mem.Allocator, counts: Counts) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    try out.writer.print(
        "{{\"registry_schemas\":{},\"registry_graph_node_schemas\":{},\"registry_graph_edge_schemas\":{},\"registry_facet_definitions\":{},\"native_entity_types\":{},\"native_edge_types\":{},\"native_unique_edge_labels\":{},\"graph_entities\":{},\"graph_relations\":{},\"coverage_gaps\":{},\"diagnostics_issues\":{},\"projection_results\":{}}}",
        .{ counts.registry_schemas, counts.registry_graph_node_schemas, counts.registry_graph_edge_schemas, counts.registry_facet_definitions, counts.native_entity_types, counts.native_edge_types, counts.native_unique_edge_labels, counts.graph_entities, counts.graph_relations, counts.coverage_gaps, counts.diagnostics_issues, counts.projection_results },
    );
    return try out.toOwnedSlice();
}

fn renderReportJson(allocator: std.mem.Allocator, input: struct {
    run_id: []const u8,
    workspace_id: []const u8,
    ontology_id: []const u8,
    fingerprint: []const u8,
    counts: Counts,
    actions: []const Action,
}) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    try out.writer.writeAll("{\"kind\":\"quality_convergence_report\",\"run_id\":");
    try writeJsonString(&out.writer, input.run_id);
    try out.writer.writeAll(",\"workspace_id\":");
    try writeJsonString(&out.writer, input.workspace_id);
    try out.writer.writeAll(",\"ontology_id\":");
    try writeJsonString(&out.writer, input.ontology_id);
    try out.writer.writeAll(",\"canonical_layer\":\"ghostcrab_runtime_registry\",\"input_fingerprint\":");
    try writeJsonString(&out.writer, input.fingerprint);
    try out.writer.writeAll(",\"layers\":");
    try renderLayerCounts(&out.writer, input.counts);
    try out.writer.writeAll(",\"remediation\":{\"actions_total\":");
    try out.writer.print("{}", .{input.actions.len});
    try out.writer.writeAll(",\"actions\":");
    try writeActionsArray(&out.writer, input.actions);
    try out.writer.writeAll("}}");
    return try out.toOwnedSlice();
}

fn renderActionsJson(allocator: std.mem.Allocator, actions: []const Action) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    try out.writer.writeAll("{\"kind\":\"quality_remediation_actions\",\"actions\":");
    try writeActionsArray(&out.writer, actions);
    try out.writer.writeAll("}");
    return try out.toOwnedSlice();
}

fn renderLayerCounts(writer: *std.Io.Writer, counts: Counts) !void {
    try writer.print(
        "{{\"registry\":{{\"schemas\":{},\"graph_node_schemas\":{},\"graph_edge_schemas\":{},\"facet_definitions\":{},\"property_facets\":{}}},\"native_ontology\":{{\"ontologies\":{},\"entity_types\":{},\"edge_types\":{},\"unique_edge_labels\":{},\"values\":{},\"triples\":{}}},\"runtime_graph\":{{\"entities\":{},\"relations\":{},\"relation_properties\":{}}},\"coverage\":{{\"total_nodes\":{},\"covered_nodes\":{},\"gaps\":{}}},\"graph_diagnostics\":{{\"issues\":{}}},\"projections\":{{\"rows\":{},\"projection_results\":{}}}}}",
        .{ counts.registry_schemas, counts.registry_graph_node_schemas, counts.registry_graph_edge_schemas, counts.registry_facet_definitions, counts.registry_property_facets, counts.native_ontologies, counts.native_entity_types, counts.native_edge_types, counts.native_unique_edge_labels, counts.native_values, counts.native_triples, counts.graph_entities, counts.graph_relations, counts.graph_relation_properties, counts.coverage_total_nodes, counts.coverage_covered_nodes, counts.coverage_gaps, counts.diagnostics_issues, counts.projections, counts.projection_results },
    );
}

fn writeActionsArray(writer: *std.Io.Writer, actions: []const Action) !void {
    try writer.writeAll("[");
    for (actions, 0..) |action, index| {
        if (index > 0) try writer.writeAll(",");
        try writer.writeAll("{\"action_id\":");
        try writeJsonString(writer, action.action_id);
        try writer.writeAll(",\"issue_type\":");
        try writeJsonString(writer, action.issue_type);
        try writer.writeAll(",\"severity\":");
        try writeJsonString(writer, action.severity);
        try writer.print(",\"confidence\":{d}", .{action.confidence});
        try writer.writeAll(",\"reason\":");
        try writeJsonString(writer, action.reason);
        try writer.writeAll(",\"schema_id\":");
        try writeOptionalJsonString(writer, action.schema_id);
        try writer.writeAll(",\"entity_type\":");
        try writeOptionalJsonString(writer, action.entity_type);
        try writer.writeAll(",\"projection_id\":");
        try writeOptionalJsonString(writer, action.projection_id);
        try writer.writeAll(",\"evidence\":");
        try writeRawJsonObject(writer, action.evidence_json);
        try writer.writeAll(",\"mcp_tool\":");
        try writeOptionalJsonString(writer, action.mcp_tool);
        try writer.writeAll(",\"tool_args\":");
        try writeRawJsonObject(writer, action.tool_args_json);
        try writer.writeAll(",\"execution_mode\":");
        try writeJsonString(writer, action.execution_mode);
        try writer.writeAll(",\"idempotency_key\":");
        try writeJsonString(writer, action.idempotency_key);
        try writer.writeAll("}");
    }
    try writer.writeAll("]");
}

fn persistRun(db: Database, run_id: []const u8, workspace_id: []const u8, ontology_id: []const u8, fingerprint: []const u8, summary_json: []const u8, report_json: []const u8) !void {
    const stmt = try prepare(db,
        \\INSERT INTO quality_convergence_run(run_id, workspace_id, ontology_id, run_kind, status, canonical_layer, input_fingerprint, summary_json, report_json)
        \\VALUES (?1, ?2, ?3, 'convergence', 'completed', 'ghostcrab_runtime_registry', ?4, ?5, ?6)
        \\ON CONFLICT(run_id) DO UPDATE SET status = excluded.status, summary_json = excluded.summary_json, report_json = excluded.report_json, updated_at_unix = unixepoch()
    );
    defer finalize(stmt);
    try bindText(stmt, 1, run_id);
    try bindText(stmt, 2, workspace_id);
    try bindText(stmt, 3, ontology_id);
    try bindText(stmt, 4, fingerprint);
    try bindText(stmt, 5, summary_json);
    try bindText(stmt, 6, report_json);
    try stepDone(stmt);
}

fn persistAction(db: Database, run_id: []const u8, workspace_id: []const u8, ontology_id: []const u8, action: Action) !void {
    const stmt = try prepare(db,
        \\INSERT INTO quality_remediation_action(action_id, run_id, workspace_id, ontology_id, issue_type, severity, confidence, reason, schema_id, entity_type, projection_id, evidence_json, mcp_tool, tool_args_json, execution_mode, idempotency_key, status)
        \\VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, 'proposed')
        \\ON CONFLICT(action_id) DO UPDATE SET run_id = excluded.run_id, severity = excluded.severity, confidence = excluded.confidence, reason = excluded.reason, evidence_json = excluded.evidence_json, mcp_tool = excluded.mcp_tool, tool_args_json = excluded.tool_args_json, execution_mode = excluded.execution_mode, updated_at_unix = unixepoch()
    );
    defer finalize(stmt);
    try bindText(stmt, 1, action.action_id);
    try bindText(stmt, 2, run_id);
    try bindText(stmt, 3, workspace_id);
    try bindText(stmt, 4, ontology_id);
    try bindText(stmt, 5, action.issue_type);
    try bindText(stmt, 6, action.severity);
    if (c.sqlite3_bind_double(stmt, 7, action.confidence) != c.SQLITE_OK) return error.BindFailed;
    try bindText(stmt, 8, action.reason);
    if (action.schema_id) |value| try bindText(stmt, 9, value) else try bindNull(stmt, 9);
    if (action.entity_type) |value| try bindText(stmt, 10, value) else try bindNull(stmt, 10);
    if (action.projection_id) |value| try bindText(stmt, 11, value) else try bindNull(stmt, 11);
    try bindText(stmt, 12, action.evidence_json);
    if (action.mcp_tool) |value| try bindText(stmt, 13, value) else try bindNull(stmt, 13);
    try bindText(stmt, 14, action.tool_args_json);
    try bindText(stmt, 15, action.execution_mode);
    try bindText(stmt, 16, action.idempotency_key);
    try stepDone(stmt);
}

fn writeActionRowJson(writer: *std.Io.Writer, stmt: *c.sqlite3_stmt) !void {
    try writer.writeAll("{\"action_id\":");
    try writeJsonString(writer, columnText(stmt, 0));
    try writer.writeAll(",\"issue_type\":");
    try writeJsonString(writer, columnText(stmt, 1));
    try writer.writeAll(",\"severity\":");
    try writeJsonString(writer, columnText(stmt, 2));
    try writer.print(",\"confidence\":{d}", .{c.sqlite3_column_double(stmt, 3)});
    try writer.writeAll(",\"reason\":");
    try writeJsonString(writer, columnText(stmt, 4));
    try writer.writeAll(",\"schema_id\":");
    try writeOptionalColumnString(writer, stmt, 5);
    try writer.writeAll(",\"entity_type\":");
    try writeOptionalColumnString(writer, stmt, 6);
    try writer.writeAll(",\"projection_id\":");
    try writeOptionalColumnString(writer, stmt, 7);
    try writer.writeAll(",\"evidence\":");
    try writeRawJsonObject(writer, columnText(stmt, 8));
    try writer.writeAll(",\"mcp_tool\":");
    try writeOptionalColumnString(writer, stmt, 9);
    try writer.writeAll(",\"tool_args\":");
    try writeRawJsonObject(writer, columnText(stmt, 10));
    try writer.writeAll(",\"execution_mode\":");
    try writeJsonString(writer, columnText(stmt, 11));
    try writer.writeAll(",\"idempotency_key\":");
    try writeJsonString(writer, columnText(stmt, 12));
    try writer.writeAll(",\"status\":");
    try writeJsonString(writer, columnText(stmt, 13));
    try writer.writeAll(",\"decision_actor\":");
    try writeOptionalColumnString(writer, stmt, 14);
    try writer.writeAll(",\"decision_note\":");
    try writeOptionalColumnString(writer, stmt, 15);
    try writer.writeAll(",\"result\":");
    try writeRawJsonObject(writer, columnText(stmt, 16));
    try writer.print(",\"created_at_unix\":{},\"updated_at_unix\":{}", .{ c.sqlite3_column_int64(stmt, 17), c.sqlite3_column_int64(stmt, 18) });
    try writer.writeAll("}");
}

fn count(db: Database, sql: []const u8, workspace_id: []const u8) !u64 {
    const stmt = try prepare(db, sql);
    defer finalize(stmt);
    try bindText(stmt, 1, workspace_id);
    if (!try stepRow(stmt)) return 0;
    return @intCast(c.sqlite3_column_int64(stmt, 0));
}

fn countForOntology(db: Database, sql: []const u8, ontology_id: []const u8) !u64 {
    const stmt = try prepare(db, sql);
    defer finalize(stmt);
    try bindText(stmt, 1, ontology_id);
    if (!try stepRow(stmt)) return 0;
    return @intCast(c.sqlite3_column_int64(stmt, 0));
}

fn makeRunId(allocator: std.mem.Allocator, workspace_id: []const u8) ![]const u8 {
    return try std.fmt.allocPrint(allocator, "quality::{s}::{}", .{ workspace_id, std.Io.Timestamp.now(zig16_compat.io(), .real).toMilliseconds() });
}

fn makeFingerprint(allocator: std.mem.Allocator, workspace_id: []const u8, ontology_id: []const u8, counts: Counts) ![]const u8 {
    const raw = try std.fmt.allocPrint(allocator, "{s}|{s}|{}|{}|{}|{}|{}|{}", .{ workspace_id, ontology_id, counts.registry_schemas, counts.registry_facet_definitions, counts.native_entity_types, counts.native_edge_types, counts.graph_entities, counts.graph_relations });
    defer allocator.free(raw);
    return try hashText(allocator, "quality", raw);
}

fn hashText(allocator: std.mem.Allocator, prefix: []const u8, text: []const u8) ![]const u8 {
    var digest: [32]u8 = undefined;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(prefix);
    hasher.update("|");
    hasher.update(text);
    hasher.final(&digest);
    var hex_buf: [64]u8 = undefined;
    for (digest, 0..) |byte, i| {
        _ = std.fmt.bufPrint(hex_buf[i * 2 ..][0..2], "{x:0>2}", .{byte}) catch unreachable;
    }
    return try std.fmt.allocPrint(allocator, "{s}", .{hex_buf});
}

fn deinitAction(allocator: std.mem.Allocator, action: Action) void {
    allocator.free(action.action_id);
    allocator.free(action.reason);
    allocator.free(action.evidence_json);
    allocator.free(action.tool_args_json);
    allocator.free(action.idempotency_key);
}

fn deinitCoverageReport(allocator: std.mem.Allocator, report: ontology_sqlite.CoverageReport) void {
    allocator.free(report.summary.workspace_id);
    for (report.gaps) |gap| {
        allocator.free(gap.id);
        allocator.free(gap.label);
        allocator.free(gap.entity_type);
        allocator.free(gap.criticality);
    }
    allocator.free(report.gaps);
}

fn isActionStatus(status: []const u8) bool {
    return std.mem.eql(u8, status, "proposed") or
        std.mem.eql(u8, status, "approved") or
        std.mem.eql(u8, status, "rejected") or
        std.mem.eql(u8, status, "applied") or
        std.mem.eql(u8, status, "failed") or
        std.mem.eql(u8, status, "skipped");
}

fn writeJsonString(writer: *std.Io.Writer, text: []const u8) !void {
    try writer.print("{f}", .{std.json.fmt(text, .{})});
}

fn writeOptionalJsonString(writer: *std.Io.Writer, value: ?[]const u8) !void {
    if (value) |text| try writeJsonString(writer, text) else try writer.writeAll("null");
}

fn writeOptionalColumnString(writer: *std.Io.Writer, stmt: *c.sqlite3_stmt, column: c_int) !void {
    if (c.sqlite3_column_type(stmt, column) == c.SQLITE_NULL) try writer.writeAll("null") else try writeJsonString(writer, columnText(stmt, column));
}

fn writeRawJsonObject(writer: *std.Io.Writer, value: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, value, .{}) catch {
        try writer.writeAll("{}");
        return;
    };
    defer parsed.deinit();
    switch (parsed.value) {
        .object, .array => try writer.writeAll(value),
        else => try writer.writeAll("{}"),
    }
}

fn prepare(db: Database, sql: []const u8) !*c.sqlite3_stmt {
    return facet_sqlite.prepare(db, sql);
}

fn finalize(stmt: *c.sqlite3_stmt) void {
    facet_sqlite.finalize(stmt);
}

fn bindText(stmt: *c.sqlite3_stmt, index: c_int, value: []const u8) !void {
    try facet_sqlite.bindText(stmt, index, value);
}

fn bindNull(stmt: *c.sqlite3_stmt, index: c_int) !void {
    if (c.sqlite3_bind_null(stmt, index) != c.SQLITE_OK) return error.BindFailed;
}

fn bindInt64(stmt: *c.sqlite3_stmt, index: c_int, value: i64) !void {
    if (c.sqlite3_bind_int64(stmt, index, value) != c.SQLITE_OK) return error.BindFailed;
}

fn stepRow(stmt: *c.sqlite3_stmt) !bool {
    const rc = c.sqlite3_step(stmt);
    if (rc == c.SQLITE_ROW) return true;
    if (rc == c.SQLITE_DONE) return false;
    return error.StepFailed;
}

fn stepDone(stmt: *c.sqlite3_stmt) !void {
    const rc = c.sqlite3_step(stmt);
    if (rc != c.SQLITE_DONE) return error.StepFailed;
}

fn columnText(stmt: *c.sqlite3_stmt, column: c_int) []const u8 {
    return std.mem.span(c.sqlite3_column_text(stmt, column) orelse return "");
}

test "quality convergence persists run and actions" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(id, workspace_id, label) VALUES ('ws_quality', 'ws_quality', 'Quality');
        \\INSERT INTO ontologies(ontology_id, workspace_id, name) VALUES ('ws_quality::core', 'ws_quality', 'core');
        \\INSERT INTO workspace_settings(workspace_id, default_ontology_id) VALUES ('ws_quality', 'ws_quality::core');
        \\INSERT INTO agent_facts(id, schema_id, content, facets_json, workspace_id, doc_id) VALUES ('schema-node', 'mindbrain:schema', '{}', '{"target":"graph_node","schema_id":"ws_quality:unit"}', 'ws_quality', 1);
        \\INSERT INTO agent_facts(id, schema_id, content, facets_json, workspace_id, doc_id) VALUES ('schema-edge', 'mindbrain:schema', '{}', '{"target":"graph_edge","schema_id":"ws_quality:edge:owns"}', 'ws_quality', 2);
        \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label) VALUES ('ws_quality::core', 'unit', 'Unit');
        \\INSERT INTO ontology_edge_types(ontology_id, edge_type, source_entity_type, target_entity_type) VALUES ('ws_quality::core', 'owns', 'person', 'unit');
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name) VALUES (1, 'ws_quality', 'unit', 'Unit 1');
    );

    var result = try runConvergence(db, std.testing.allocator, .{ .workspace_id = "ws_quality", .persist = true });
    defer result.deinit(std.testing.allocator);

    try std.testing.expect(result.persisted);
    try std.testing.expect(result.actions_total >= 1);
    const stored = try getRunJson(db, std.testing.allocator, result.run_id);
    defer std.testing.allocator.free(stored);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"quality_convergence_report\"") != null);
}
