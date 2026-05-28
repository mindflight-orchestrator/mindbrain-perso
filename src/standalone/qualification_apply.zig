const std = @import("std");
const collections_sqlite = @import("collections_sqlite.zig");
const facet_sqlite = @import("facet_sqlite.zig");
const qualification_normalize = @import("qualification_normalize.zig");
const zig16_compat = @import("zig16_compat.zig");

const Allocator = std.mem.Allocator;

pub const CliError = error{ InvalidArguments };

pub const QualificationAssignmentRow = struct {
    target_kind: []const u8 = "doc",
    doc_id: u64,
    chunk_index: ?u32 = null,
    ontology_id: ?[]const u8 = null,
    namespace: []const u8,
    dimension: []const u8,
    value: []const u8,
    value_id: ?u32 = null,
    weight: f64 = 1.0,
    source: ?[]const u8 = null,
};

const QualificationEnvelope = struct {
    assignments: []QualificationAssignmentRow = &.{},
};

pub const ApplyQualificationOptions = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    ontology_id: []const u8,
    facet_filter: []const u8,
    source: []const u8,
    dry_run: bool,
    json: []const u8,
};

const QualificationApplyFailure = struct {
    reason: []const u8,
    assignment_index: usize,
    allowed_facets: []const u8,
    row: QualificationAssignmentRow,
    envelope_json: []const u8,
};

pub fn applyQualificationEnvelope(allocator: Allocator, db: facet_sqlite.Database, opts: ApplyQualificationOptions) !usize {
    var parsed = try std.json.parseFromSlice(QualificationEnvelope, allocator, opts.json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    var accepted: usize = 0;
    for (parsed.value.assignments, 0..) |row, row_index| {
        const normalized = qualification_normalize.normalizeQualificationFacet(
            row.namespace,
            row.dimension,
            opts.facet_filter,
        );
        if (!qualification_normalize.facetCsvContains(opts.facet_filter, normalized.namespace, normalized.dimension)) {
            try writeQualificationApplyFailure(allocator, .{
                .reason = "facet_not_allowed",
                .assignment_index = row_index,
                .allowed_facets = opts.facet_filter,
                .row = row,
                .envelope_json = opts.json,
            });
            return CliError.InvalidArguments;
        }
        const target_kind: collections_sqlite.TargetKind = if (std.mem.eql(u8, row.target_kind, "doc"))
            .doc
        else if (std.mem.eql(u8, row.target_kind, "chunk"))
            .chunk
        else
            return CliError.InvalidArguments;
        if (!try documentOrChunkExists(db, opts.workspace_id, opts.collection_id, row.doc_id, target_kind, row.chunk_index)) return CliError.InvalidArguments;
        if (!opts.dry_run) {
            try collections_sqlite.upsertFacetAssignmentRaw(db, .{
                .workspace_id = opts.workspace_id,
                .collection_id = opts.collection_id,
                .target_kind = target_kind,
                .doc_id = row.doc_id,
                .chunk_index = if (target_kind == .chunk) (row.chunk_index orelse return CliError.InvalidArguments) else null,
                .ontology_id = row.ontology_id orelse opts.ontology_id,
                .namespace = normalized.namespace,
                .dimension = normalized.dimension,
                .value = row.value,
                .value_id = row.value_id,
                .weight = row.weight,
                .source = row.source orelse opts.source,
            });
        }
        accepted += 1;
    }
    return accepted;
}

fn writeQualificationApplyFailure(allocator: Allocator, failure: QualificationApplyFailure) !void {
    const json = try std.json.Stringify.valueAlloc(allocator, failure, .{});
    defer allocator.free(json);
    var stderr_file_writer = std.Io.File.stderr().writer(zig16_compat.io(), &.{});
    const stderr = &stderr_file_writer.interface;
    try stderr.print("QUALIFICATION_APPLY_FAILURE_JSON={s}\n", .{json});
    try stderr.flush();
}

fn documentOrChunkExists(
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    collection_id: []const u8,
    doc_id: u64,
    target_kind: collections_sqlite.TargetKind,
    chunk_index: ?u32,
) !bool {
    const sql_doc = "SELECT COUNT(*) FROM documents_raw WHERE workspace_id = ?1 AND collection_id = ?2 AND doc_id = ?3";
    const sql_chunk = "SELECT COUNT(*) FROM chunks_raw WHERE workspace_id = ?1 AND collection_id = ?2 AND doc_id = ?3 AND chunk_index = ?4";
    const stmt = try facet_sqlite.prepare(db, if (target_kind == .doc) sql_doc else sql_chunk);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, collection_id);
    try facet_sqlite.bindInt64(stmt, 3, @as(i64, @intCast(doc_id)));
    if (target_kind == .chunk) {
        const ci = chunk_index orelse return CliError.InvalidArguments;
        try facet_sqlite.bindInt64(stmt, 4, @as(i64, @intCast(ci)));
    }
    const c = facet_sqlite.c;
    const rc = c.sqlite3_step(stmt);
    if (rc != c.SQLITE_ROW) return error.StepFailed;
    return c.sqlite3_column_int64(stmt, 0) > 0;
}
