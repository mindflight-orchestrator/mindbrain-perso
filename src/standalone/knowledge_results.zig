const std = @import("std");
const sql = @import("facet_sqlite.zig");
const interfaces = @import("interfaces.zig");
pub const EntityRef = struct { kind: []const u8 = "external_id", value: []const u8, ontology_id: []const u8, entity_type: []const u8 };
pub const Fact = struct { id: []const u8, doc_id: u64, schema_id: []const u8, content: []const u8, facets: std.json.Value, created_at_unix: i64, version: u64, source_ref: ?[]const u8, entity_ref: ?EntityRef, score: f64 };

/// Materialize native search hits in their ranked order within the same read snapshot.
pub fn load(db: sql.Database, a: std.mem.Allocator, workspace: []const u8, matches: []const interfaces.HybridSearchMatch) ![]Fact {
    const stmt = try sql.prepare(db, "SELECT a.id,a.schema_id,a.content,a.facets_json,a.created_at_unix,a.version,a.source_ref,r.external_id,r.ontology_id,r.entity_type FROM agent_facts a LEFT JOIN entities_raw r ON r.workspace_id=a.workspace_id AND r.external_id=a.source_ref AND EXISTS(SELECT 1 FROM structured_import_provenance p WHERE p.workspace_id=a.workspace_id AND p.fact_id=a.id AND p.entity_external_id=r.external_id) WHERE a.workspace_id=?1 AND a.doc_id=?2");
    defer sql.finalize(stmt);
    const result = try a.alloc(Fact, matches.len);
    for (matches, result) |hit, *row| {
        try sql.resetStatement(stmt);
        try sql.bindText(stmt, 1, workspace);
        try sql.bindInt64(stmt, 2, hit.doc_id);
        if (sql.c.sqlite3_step(stmt) != sql.c.SQLITE_ROW) return error.NotFound;
        row.* = .{ .id = try sql.dupeColumnText(a, stmt, 0), .doc_id = hit.doc_id, .schema_id = try sql.dupeColumnText(a, stmt, 1), .content = try sql.dupeColumnText(a, stmt, 2), .facets = try std.json.parseFromSliceLeaky(std.json.Value, a, try sql.dupeColumnText(a, stmt, 3), .{}), .created_at_unix = try sql.columnI64(stmt, 4), .version = try sql.columnU64(stmt, 5), .source_ref = if (sql.c.sqlite3_column_type(stmt, 6) == sql.c.SQLITE_NULL) null else try sql.dupeColumnText(a, stmt, 6), .entity_ref = if (sql.c.sqlite3_column_type(stmt, 7) == sql.c.SQLITE_NULL) null else .{ .value = try sql.dupeColumnText(a, stmt, 7), .ontology_id = try sql.dupeColumnText(a, stmt, 8), .entity_type = try sql.dupeColumnText(a, stmt, 9) }, .score = hit.combined_score };
    }
    return result;
}
