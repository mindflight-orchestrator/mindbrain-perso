//! Native fact-index qualification and workspace-scoped repair.
const std = @import("std");
const sql = @import("facet_sqlite.zig");
const search = @import("search_sqlite.zig");
const c = sql.c;
const active = @import("facts_sqlite.zig").activeWindowSql("a.");

pub const Status = struct {
    scanned: u64 = 0,
    eligible: u64 = 0,
    indexed: u64 = 0,
    pending: u64 = 0,
    ready: bool = false,
};

const matched =
    \\EXISTS (SELECT 1 FROM search_documents d
    \\ JOIN search_fts_docs m ON m.table_id=d.table_id AND m.doc_id=d.doc_id
    \\ JOIN search_fts f ON f.rowid=m.fts_rowid
    \\ WHERE d.table_id=1 AND d.doc_id=a.doc_id AND d.content=a.content AND f.content=a.content)
;

pub fn status(db: sql.Database, workspace: []const u8) !Status {
    const stmt = try sql.prepare(db, "SELECT count(*),coalesce(sum(" ++ active ++ "),0),coalesce(sum((" ++ active ++ ") AND " ++ matched ++ "),0) FROM agent_facts a WHERE workspace_id=?1");
    defer sql.finalize(stmt);
    try sql.bindText(stmt, 1, workspace);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StepFailed;
    const scanned = try sql.columnU64(stmt, 0);
    const eligible = try sql.columnU64(stmt, 1);
    const indexed = try sql.columnU64(stmt, 2);
    return .{ .scanned = scanned, .eligible = eligible, .indexed = indexed, .pending = eligible - indexed, .ready = eligible == indexed };
}

pub const Repair = struct { before: Status, after: Status, indexed: u64, unchanged: u64, skipped: u64 = 0, failed: u64 = 0 };

pub fn reconcile(db: sql.Database, allocator: std.mem.Allocator, workspace: []const u8) !Repair {
    var tx = try sql.Transaction.begin(db);
    defer tx.deinit();
    const before = try status(db, workspace);
    const stmt = try sql.prepare(db, "SELECT doc_id,content FROM agent_facts a WHERE workspace_id=?1 AND doc_id IS NOT NULL AND " ++ active ++ " AND NOT " ++ matched ++ " ORDER BY doc_id");
    defer sql.finalize(stmt);
    try sql.bindText(stmt, 1, workspace);
    var indexed: u64 = 0;
    while (true) {
        const step = c.sqlite3_step(stmt);
        if (step == c.SQLITE_DONE) break;
        if (step != c.SQLITE_ROW) return error.StepFailed;
        const doc_id = try sql.columnU64(stmt, 0);
        const content = try sql.dupeColumnText(allocator, stmt, 1);
        defer allocator.free(content);
        try search.syncSearchDocument(db, allocator, 1, doc_id, content, "english");
        indexed += 1;
    }
    const after = try status(db, workspace);
    try tx.commit();
    return .{ .before = before, .after = after, .indexed = indexed, .unchanged = before.indexed, .skipped = before.scanned - before.eligible };
}

test "native facts are searchable without registration and repair is scoped and idempotent" {
    const facts = @import("facts_sqlite.zig");
    var db = try sql.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    const a = std.testing.allocator;
    const row = try facts.writeFact(db, a, .{ .workspace_id = "b1", .schema_id = "Assertion", .source_ref = "assertion:a", .content = "Remise de l’ordonnance au greffe par mise à disposition." });
    defer facts.deinitFactWriteResult(a, row);
    try std.testing.expect((try status(db, "b1")).ready);
    const hits = try search.searchFts5Bm25Workspace(db, a, 1, "b1", "Quand l'ordonnance de référé a-t-elle été remise au greffe par mise à disposition ?", 10);
    defer a.free(hits);
    try std.testing.expectEqual(@as(usize, 1), hits.len);
    const updated = try facts.writeFact(db, a, .{ .workspace_id = "b1", .schema_id = "Assertion", .source_ref = "assertion:a", .content = "nouveautémarque" });
    defer facts.deinitFactWriteResult(a, updated);
    const old = try search.searchFts5Bm25Workspace(db, a, 1, "b1", "greffe", 10);
    defer a.free(old);
    try std.testing.expectEqual(@as(usize, 0), old.len);
    try db.exec("INSERT INTO agent_facts(id,schema_id,content,workspace_id) VALUES('legacy','Assertion','ancienne échéance','b1'),('foreign','Assertion','secret','other')");
    try std.testing.expect(!(try status(db, "b1")).ready);
    const repaired = try reconcile(db, a, "b1");
    try std.testing.expect(repaired.after.ready);
    try std.testing.expectEqual(@as(u64, 1), repaired.indexed);
    try std.testing.expect(!(try status(db, "other")).ready);
    try std.testing.expectEqual(@as(u64, 0), (try reconcile(db, a, "b1")).indexed);
}

test "index failure rolls back native fact writes and inactive history does not block readiness" {
    const facts = @import("facts_sqlite.zig");
    var db = try sql.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try db.exec("CREATE TRIGGER reject_index BEFORE INSERT ON search_documents BEGIN SELECT RAISE(ABORT,'index unavailable'); END");
    const result = facts.writeFact(db, std.testing.allocator, .{ .id = "rollback", .workspace_id = "b1", .schema_id = "Assertion", .content = "must roll back" });
    try std.testing.expectError(error.StepFailed, result);
    try std.testing.expectEqual(@as(u64, 0), (try status(db, "b1")).scanned);
    try db.exec("DROP TRIGGER reject_index; INSERT INTO agent_facts(id,schema_id,content,workspace_id,valid_until_unix) VALUES('closed','Assertion','archive','b1',1)");
    const state = try status(db, "b1");
    try std.testing.expectEqual(@as(u64, 1), state.scanned);
    try std.testing.expectEqual(@as(u64, 0), state.eligible);
    try std.testing.expect(state.ready);
}
