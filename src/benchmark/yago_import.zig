const std = @import("std");
const mindbrain = @import("mindbrain");
const compat = @import("zig16_compat.zig");
const facet_sqlite = mindbrain.facet_sqlite;
const graph_sqlite = mindbrain.graph_sqlite;
const collections_sqlite = mindbrain.collections_sqlite;

const Allocator = std.mem.Allocator;
const c = facet_sqlite.c;
const transaction_chunk_rows: usize = 50_000;
const progress_rows: usize = 1_000_000;
const stage_entity_table = "temp_yago_entity_stage";
const stage_alias_table = "temp_yago_alias_stage";
const stage_relation_table = "temp_yago_relation_stage";

pub const default_workspace_id = "yago";
pub const default_collection_id = "yago::core_facts";
pub const default_ontology_id = "yago::core";

pub const ImportOptions = struct {
    yago_dir: ?[]const u8 = null,
    yago_path: ?[]const u8 = null,
    row_limit: ?usize = null,
    /// Workspace under which YAGO entities and relations are recorded.
    workspace_id: []const u8 = default_workspace_id,
    /// Collection that owns mirrored YAGO documents/links (currently unused by
    /// the importer itself, kept for symmetry with the Pipeline API).
    collection_id: []const u8 = default_collection_id,
    /// Ontology id stamped onto entities_raw/relations_raw so the raw layer can
    /// be restored or re-indexed without losing provenance.
    ontology_id: []const u8 = default_ontology_id,
};

pub const ImportSummary = struct {
    yago_path: []const u8 = "",
    row_limit: ?usize = null,
    triple_rows: usize = 0,
    entity_rows: usize = 0,
    relation_rows: usize = 0,
    alias_rows: usize = 0,
    label_rows: usize = 0,
    skipped_rows: usize = 0,
    total_ns: u64 = 0,
    load_ns: u64 = 0,
    rebuild_adjacency_ns: u64 = 0,
    rebuild_degree_ns: u64 = 0,
};

const Importer = struct {
    allocator: Allocator,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    stage_entity_stmt: *c.sqlite3_stmt,
    stage_alias_stmt: *c.sqlite3_stmt,
    stage_relation_stmt: *c.sqlite3_stmt,
    entities: std.StringHashMap(u32),
    next_entity_id: u32 = 1,
    next_relation_id: u32 = 1,
    entity_rows: usize = 0,
    relation_rows: usize = 0,
    alias_rows: usize = 0,
    label_rows: usize = 0,
    skipped_rows: usize = 0,

    fn init(db: facet_sqlite.Database, allocator: Allocator, workspace_id: []const u8) !Importer {
        try db.exec(
            \\PRAGMA foreign_keys = ON;
            \\PRAGMA journal_mode = WAL;
            \\PRAGMA wal_autocheckpoint = 0;
            \\PRAGMA synchronous = NORMAL;
            \\PRAGMA temp_store = MEMORY;
            \\PRAGMA cache_size = -200000;
            \\DROP INDEX IF EXISTS graph_entity_name_idx;
            \\DROP INDEX IF EXISTS graph_entity_workspace_id_idx;
            \\DROP INDEX IF EXISTS graph_relation_source_id_idx;
            \\DROP INDEX IF EXISTS graph_relation_target_id_idx;
            \\DROP INDEX IF EXISTS graph_relation_workspace_id_idx;
            \\PRAGMA foreign_keys = OFF;
            \\DELETE FROM graph_lj_in;
            \\DELETE FROM graph_lj_out;
            \\DELETE FROM graph_entity_degree;
            \\DELETE FROM graph_entity_document;
            \\DELETE FROM graph_relation;
            \\DELETE FROM graph_entity_alias;
            \\DELETE FROM graph_entity;
            \\PRAGMA foreign_keys = ON;
            \\DROP TABLE IF EXISTS temp_yago_entity_stage;
            \\DROP TABLE IF EXISTS temp_yago_alias_stage;
            \\DROP TABLE IF EXISTS temp_yago_relation_stage;
            \\CREATE TEMP TABLE temp_yago_entity_stage (
            \\    entity_id INTEGER NOT NULL,
            \\    workspace_id TEXT NOT NULL,
            \\    entity_type TEXT NOT NULL,
            \\    name TEXT NOT NULL,
            \\    confidence REAL NOT NULL,
            \\    metadata_json TEXT NOT NULL
            \\);
            \\CREATE TEMP TABLE temp_yago_alias_stage (
            \\    term TEXT NOT NULL,
            \\    entity_id INTEGER NOT NULL,
            \\    confidence REAL NOT NULL
            \\);
            \\CREATE TEMP TABLE temp_yago_relation_stage (
            \\    relation_id INTEGER NOT NULL,
            \\    workspace_id TEXT NOT NULL,
            \\    relation_type TEXT NOT NULL,
            \\    source_id INTEGER NOT NULL,
            \\    target_id INTEGER NOT NULL,
            \\    confidence REAL NOT NULL,
            \\    metadata_json TEXT NOT NULL
            \\);
        );

        return .{
            .allocator = allocator,
            .db = db,
            .workspace_id = workspace_id,
            .stage_entity_stmt = try prepare(
                db,
                "INSERT INTO temp_yago_entity_stage(entity_id, workspace_id, entity_type, name, confidence, metadata_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            ),
            .stage_alias_stmt = try prepare(
                db,
                "INSERT INTO temp_yago_alias_stage(term, entity_id, confidence) VALUES (?1, ?2, ?3)",
            ),
            .stage_relation_stmt = try prepare(
                db,
                "INSERT INTO temp_yago_relation_stage(relation_id, workspace_id, relation_type, source_id, target_id, confidence, metadata_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            ),
            .entities = std.StringHashMap(u32).init(allocator),
        };
    }

    fn deinit(self: *Importer) void {
        finalize(self.stage_relation_stmt);
        finalize(self.stage_alias_stmt);
        finalize(self.stage_entity_stmt);
        var it = self.entities.keyIterator();
        while (it.next()) |key| self.allocator.free(key.*);
        self.entities.deinit();
        self.* = undefined;
    }

    fn importFile(self: *Importer, yago_path: []const u8, limit: ?usize) !usize {
        const file = try std.Io.Dir.cwd().openFile(compat.io(), yago_path, .{ .mode = .read_only });
        defer file.close(compat.io());
        return try importStream(self, file, limit, yago_path);
    }

    fn importDirectory(self: *Importer, yago_dir: []const u8, limit: ?usize) !usize {
        var dir = try std.Io.Dir.cwd().openDir(compat.io(), yago_dir, .{ .iterate = true });
        defer dir.close(compat.io());

        var total: usize = 0;
        const files = [_][]const u8{ "taxonomy.txt", "facts.txt", "beyond-wikipedia.txt", "meta.txt", "schema.txt" };
        var imported_any = false;
        for (files) |file_name| {
            if (dir.openFile(compat.io(), file_name, .{ .mode = .read_only })) |file| {
                defer file.close(compat.io());
                total += try self.importStream(file, limit, file_name);
                imported_any = true;
            } else |_| {}
        }

        if (!imported_any) {
            var iter = dir.iterateAssumeFirstIteration();
            while (try iter.next(compat.io())) |entry| {
                if (entry.kind != .file) continue;
                if (!isYagoSourceFile(entry.name)) continue;
                if (dir.openFile(compat.io(), entry.name, .{ .mode = .read_only })) |file| {
                    defer file.close(compat.io());
                    total += try self.importStream(file, limit, entry.name);
                    imported_any = true;
                } else |_| {}
            }
        }

        return total;
    }

    fn importStream(self: *Importer, file: std.Io.File, limit: ?usize, label: []const u8) !usize {
        var read_buf: [1024 * 1024]u8 = undefined;
        var reader = file.reader(compat.io(), &read_buf);
        reader.pos = reader.pos;

        try dbExec(self.db, "BEGIN IMMEDIATE");
        var txn_open = true;
        errdefer if (txn_open) dbExec(self.db, "ROLLBACK") catch {};

        var timer = try compat.Timer.start();
        var count: usize = 0;
        var chunk_rows: usize = 0;
        var next_report: usize = progress_rows;
        std.debug.print("[yago] starting {s}\n", .{label});
        while (true) {
            const raw_row = try readNextLine(&reader.interface) orelse break;
            const loaded = try self.handleRow(raw_row);
            count += loaded;
            chunk_rows += loaded;
            if (limit) |max_rows| if (count >= max_rows) break;
            if (count >= next_report) {
                std.debug.print(
                    "[yago] {s}: {d} triples staged, entities={d}, relations={d}, aliases={d}, labels={d}, skipped={d}\n",
                    .{ label, count, self.entity_rows, self.relation_rows, self.alias_rows, self.label_rows, self.skipped_rows },
                );
                next_report += progress_rows;
            }
            if (chunk_rows >= transaction_chunk_rows) {
                try dbExec(self.db, "COMMIT");
                txn_open = false;
                try dbExec(self.db, "PRAGMA wal_checkpoint(TRUNCATE);");
                try dbExec(self.db, "BEGIN IMMEDIATE");
                txn_open = true;
                chunk_rows = 0;
            }
        }

        if (txn_open) {
            try dbExec(self.db, "COMMIT");
            txn_open = false;
            try dbExec(self.db, "PRAGMA wal_checkpoint(TRUNCATE);");
        }
        std.debug.print(
            "[yago] finished {s}: {d} triples in {d} ms, entities={d}, relations={d}, aliases={d}, labels={d}, skipped={d}\n",
            .{
                label,
                count,
                timer.read() / std.time.ns_per_ms,
                self.entity_rows,
                self.relation_rows,
                self.alias_rows,
                self.label_rows,
                self.skipped_rows,
            },
        );
        return count;
    }

    fn handleRow(self: *Importer, raw_row: []const u8) !usize {
        const row = std.mem.trim(u8, raw_row, "\r");
        if (row.len == 0) return 0;
        if (row[0] == '#') return 0;
        if (std.mem.startsWith(u8, row, "@prefix")) return 0;

        var index: usize = 0;
        var loaded: usize = 0;
        while (index < row.len) {
            skipWhitespace(row, &index);
            if (index >= row.len) break;
            if (row[index] == '#') break;

            const triple = try parseTriple(row, &index) orelse break;
            try self.handleTriple(triple);
            loaded += 1;

            skipWhitespace(row, &index);
            if (index < row.len and row[index] == '#') break;
            if (index < row.len and row[index] == '.') index += 1;
        }
        if (loaded == 0) self.skipped_rows += 1;
        return loaded;
    }

    fn handleTriple(self: *Importer, triple: Triple) !void {
        const subject_id = try self.resolveEntity(triple.subject, .resource);

        switch (triple.object) {
            .literal => |literal| {
                const object_id = try self.resolveEntity(literal, .literal);
                const relation_type = relationTypeFromPredicate(triple.predicate);
                try self.insertRelation(self.nextRelationId(), relation_type, subject_id, object_id, 1.0, "{}");
                self.relation_rows += 1;
                if (isLabelPredicate(triple.predicate)) {
                    try self.upsertLabel(subject_id, literal);
                    self.label_rows += 1;
                }
            },
            .uri => |uri| {
                const object_id = try self.resolveEntity(uri, .resource);
                const relation_type = relationTypeFromPredicate(triple.predicate);
                try self.insertRelation(self.nextRelationId(), relation_type, subject_id, object_id, 1.0, "{}");
                self.relation_rows += 1;
            },
        }
    }

    fn resolveEntity(self: *Importer, key: []const u8, entity_type: EntityType) !u32 {
        const entity_key = try self.makeEntityKey(key, entity_type);
        defer self.allocator.free(entity_key);

        if (self.entities.get(entity_key)) |entity_id| {
            return entity_id;
        }

        const entity_id = self.next_entity_id;
        self.next_entity_id += 1;

        const owned_key = try self.allocator.dupe(u8, entity_key);
        errdefer self.allocator.free(owned_key);
        try self.entities.put(owned_key, entity_id);

        try self.upsertEntity(entity_id, entityTypeName(entity_type), key, 0.5);
        return entity_id;
    }

    fn makeEntityKey(self: *Importer, key: []const u8, entity_type: EntityType) ![]u8 {
        return try std.fmt.allocPrint(self.allocator, "{s}:{s}", .{ entityTypeName(entity_type), key });
    }

    fn upsertEntity(self: *Importer, entity_id: u32, entity_type: []const u8, name: []const u8, confidence: f32) !void {
        resetStatement(self.stage_entity_stmt) catch unreachable;
        try bindInt64(self.stage_entity_stmt, 1, entity_id);
        try bindText(self.stage_entity_stmt, 2, self.workspace_id);
        try bindText(self.stage_entity_stmt, 3, entity_type);
        try bindText(self.stage_entity_stmt, 4, name);
        if (c.sqlite3_bind_double(self.stage_entity_stmt, 5, confidence) != c.SQLITE_OK) return error.BindFailed;
        try bindText(self.stage_entity_stmt, 6, "{}");
        try stepDone(self.stage_entity_stmt);
        self.entity_rows += 1;
    }

    fn upsertLabel(self: *Importer, entity_id: u32, label: []const u8) !void {
        try self.insertAlias(entity_id, label, 1.0);
    }

    fn insertAlias(self: *Importer, entity_id: u32, term: []const u8, confidence: f32) !void {
        resetStatement(self.stage_alias_stmt) catch unreachable;
        try bindText(self.stage_alias_stmt, 1, term);
        try bindInt64(self.stage_alias_stmt, 2, entity_id);
        if (c.sqlite3_bind_double(self.stage_alias_stmt, 3, confidence) != c.SQLITE_OK) return error.BindFailed;
        try stepDone(self.stage_alias_stmt);
        self.alias_rows += 1;
    }

    fn insertRelation(
        self: *Importer,
        relation_id: u32,
        relation_type: []const u8,
        source_id: u32,
        target_id: u32,
        confidence: f32,
        metadata_json: []const u8,
    ) !void {
        resetStatement(self.stage_relation_stmt) catch unreachable;
        try bindInt64(self.stage_relation_stmt, 1, relation_id);
        try bindText(self.stage_relation_stmt, 2, self.workspace_id);
        try bindText(self.stage_relation_stmt, 3, relation_type);
        try bindInt64(self.stage_relation_stmt, 4, source_id);
        try bindInt64(self.stage_relation_stmt, 5, target_id);
        if (c.sqlite3_bind_double(self.stage_relation_stmt, 6, confidence) != c.SQLITE_OK) return error.BindFailed;
        try bindText(self.stage_relation_stmt, 7, metadata_json);
        try stepDone(self.stage_relation_stmt);
    }

    fn nextRelationId(self: *Importer) u32 {
        const relation_id = self.next_relation_id;
        self.next_relation_id += 1;
        return relation_id;
    }
};

const EntityType = enum {
    resource,
    literal,
};

const Triple = struct {
    subject: []const u8,
    predicate: []const u8,
    object: Term,
};

const Term = union(enum) {
    uri: []const u8,
    literal: []const u8,
};

fn parseTriple(line: []const u8, index: *usize) !?Triple {
    skipWhitespace(line, index);
    if (index.* >= line.len) return null;

    const subject = try parseToken(line, index) orelse return null;
    skipWhitespace(line, index);
    const predicate = try parseToken(line, index) orelse return null;
    skipWhitespace(line, index);
    const object = try parseObject(line, index) orelse return null;
    return .{ .subject = subject, .predicate = predicate, .object = object };
}

fn parseObject(line: []const u8, index: *usize) !?Term {
    if (index.* >= line.len) return null;
    switch (line[index.*]) {
        '<' => return .{ .uri = try parseUriLike(line, index) orelse return null },
        '"' => return .{ .literal = try parseLiteral(line, index) orelse return null },
        else => return .{ .uri = try parseToken(line, index) orelse return null },
    }
}

fn parseToken(line: []const u8, index: *usize) !?[]const u8 {
    if (index.* >= line.len) return null;
    if (line[index.*] == '<') return try parseUriLike(line, index);

    const start = index.*;
    while (index.* < line.len and !std.ascii.isWhitespace(line[index.*]) and line[index.*] != '.') : (index.* += 1) {}
    if (index.* == start) return null;
    return line[start..index.*];
}

fn parseUriLike(line: []const u8, index: *usize) !?[]const u8 {
    if (index.* >= line.len) return null;
    const start = index.*;

    if (line[index.*] == '<') {
        index.* += 1;
        const uri_start = index.*;
        while (index.* < line.len and line[index.*] != '>') : (index.* += 1) {}
        if (index.* >= line.len) return null;
        const value = line[uri_start..index.*];
        index.* += 1;
        return value;
    }

    if (line[index.*] == '_' and index.* + 1 < line.len and line[index.* + 1] == ':') {
        index.* += 2;
        while (index.* < line.len and !std.ascii.isWhitespace(line[index.*]) and line[index.*] != '.') : (index.* += 1) {}
        return line[start..index.*];
    }

    while (index.* < line.len and !std.ascii.isWhitespace(line[index.*]) and line[index.*] != '.') : (index.* += 1) {}
    if (index.* == start) return null;
    return line[start..index.*];
}

fn parseLiteral(line: []const u8, index: *usize) !?[]const u8 {
    if (index.* >= line.len or line[index.*] != '"') return null;
    index.* += 1;
    const start = index.*;
    while (index.* < line.len) : (index.* += 1) {
        if (line[index.*] == '"' and !isEscaped(line, index.*)) {
            const value = line[start..index.*];
            index.* += 1;
            while (index.* < line.len and !std.ascii.isWhitespace(line[index.*]) and line[index.*] != '.') : (index.* += 1) {}
            return value;
        }
    }
    return null;
}

fn isEscaped(line: []const u8, pos: usize) bool {
    if (pos == 0) return false;
    var backslashes: usize = 0;
    var index: usize = pos;
    while (index > 0) {
        index -= 1;
        if (line[index] != '\\') break;
        backslashes += 1;
    }
    return (backslashes % 2) == 1;
}

fn skipWhitespace(line: []const u8, index: *usize) void {
    while (index.* < line.len and std.ascii.isWhitespace(line[index.*])) : (index.* += 1) {}
}

fn relationTypeFromPredicate(predicate: []const u8) []const u8 {
    if (isRdfType(predicate)) return "type";
    if (isLabelPredicate(predicate)) return "label";
    return localName(predicate);
}

fn isRdfType(predicate: []const u8) bool {
    return std.mem.endsWith(u8, predicate, "rdf-syntax-ns#type") or std.mem.endsWith(u8, predicate, "/type");
}

fn isLabelPredicate(predicate: []const u8) bool {
    return std.mem.endsWith(u8, predicate, "rdf-schema#label") or
        std.mem.endsWith(u8, predicate, "rdfs#label") or
        std.mem.endsWith(u8, predicate, "schema.org/name") or
        std.mem.endsWith(u8, predicate, "/name") or
        std.mem.endsWith(u8, predicate, "/label");
}

fn localName(value: []const u8) []const u8 {
    var index: usize = value.len;
    while (index > 0) : (index -= 1) {
        const ch = value[index - 1];
        if (ch == '/' or ch == '#' or ch == ':') return value[index..];
    }
    return value;
}

fn isYagoSourceFile(name: []const u8) bool {
    return std.mem.endsWith(u8, name, ".ttl") or
        std.mem.endsWith(u8, name, ".txt") or
        std.mem.endsWith(u8, name, ".nt") or
        std.mem.endsWith(u8, name, ".ntx");
}

fn entityTypeName(_: EntityType) []const u8 {
    return "resource";
}

fn prepare(db: facet_sqlite.Database, sql: []const u8) !*c.sqlite3_stmt {
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
        return error.PrepareFailed;
    }
    return stmt.?;
}

fn finalize(stmt: *c.sqlite3_stmt) void {
    _ = c.sqlite3_finalize(stmt);
}

fn resetStatement(stmt: *c.sqlite3_stmt) !void {
    if (c.sqlite3_reset(stmt) != c.SQLITE_OK) return error.StepFailed;
    if (c.sqlite3_clear_bindings(stmt) != c.SQLITE_OK) return error.BindFailed;
}

fn stepDone(stmt: *c.sqlite3_stmt) !void {
    const rc = c.sqlite3_step(stmt);
    if (rc != c.SQLITE_DONE) {
        const db = c.sqlite3_db_handle(stmt);
        const msg: []const u8 = if (db != null) std.mem.span(c.sqlite3_errmsg(db)) else "unknown";
        std.debug.print("sqlite step failed rc={d} msg={s}\n", .{ rc, msg });
        return error.StepFailed;
    }
}

fn bindInt64(stmt: *c.sqlite3_stmt, index: c_int, value: anytype) !void {
    if (c.sqlite3_bind_int64(stmt, index, @intCast(value)) != c.SQLITE_OK) return error.BindFailed;
}

fn bindText(stmt: *c.sqlite3_stmt, index: c_int, value: []const u8) !void {
    if (c.sqlite3_bind_text(stmt, index, value.ptr, @intCast(value.len), facet_sqlite.sqliteTransient()) != c.SQLITE_OK) {
        return error.BindFailed;
    }
}

fn readNextLine(reader: *std.Io.Reader) !?[]const u8 {
    return reader.takeSentinel('\n') catch |err| switch (err) {
        error.EndOfStream => null,
        else => return err,
    };
}

fn dbExec(db: facet_sqlite.Database, sql: []const u8) !void {
    try db.exec(sql);
}

/// Bulk-mirror staged graph data into the workspace-scoped raw tables so the
/// raw layer can drive backup/restore and reindex flows without re-parsing
/// YAGO source files.
fn mirrorRawTables(db: facet_sqlite.Database, options: ImportOptions) !void {
    const ws_lit = try sqlEscapeLiteral(db, options.workspace_id);
    defer std.heap.page_allocator.free(ws_lit);
    const onto_lit = try sqlEscapeLiteral(db, options.ontology_id);
    defer std.heap.page_allocator.free(onto_lit);

    const entities_sql = try std.fmt.allocPrint(std.heap.page_allocator,
        \\INSERT OR REPLACE INTO entities_raw(workspace_id, ontology_id, entity_id, entity_type, name, confidence, metadata_json)
        \\SELECT '{s}', '{s}', entity_id, MIN(entity_type), MIN(name), MAX(confidence), MIN(metadata_json)
        \\FROM temp_yago_entity_stage
        \\GROUP BY entity_id;
    , .{ ws_lit, onto_lit });
    defer std.heap.page_allocator.free(entities_sql);
    try db.exec(entities_sql);

    const aliases_sql = try std.fmt.allocPrint(std.heap.page_allocator,
        \\INSERT OR REPLACE INTO entity_aliases_raw(workspace_id, entity_id, term, confidence)
        \\SELECT '{s}', entity_id, term, MAX(confidence)
        \\FROM temp_yago_alias_stage
        \\GROUP BY term, entity_id;
    , .{ws_lit});
    defer std.heap.page_allocator.free(aliases_sql);
    try db.exec(aliases_sql);

    const relations_sql = try std.fmt.allocPrint(std.heap.page_allocator,
        \\INSERT OR REPLACE INTO relations_raw(workspace_id, ontology_id, relation_id, edge_type, source_entity_id, target_entity_id, confidence, metadata_json)
        \\SELECT '{s}', '{s}', relation_id, relation_type, source_id, target_id, confidence, metadata_json
        \\FROM temp_yago_relation_stage
        \\ORDER BY relation_id;
    , .{ ws_lit, onto_lit });
    defer std.heap.page_allocator.free(relations_sql);
    try db.exec(relations_sql);
}

/// Doubles single quotes so the value can be safely interpolated into a
/// SQL string literal (we use literal substitution because db.exec doesn't
/// expose parameter binding).
fn sqlEscapeLiteral(db: facet_sqlite.Database, value: []const u8) ![]u8 {
    _ = db;
    var out = try std.heap.page_allocator.alloc(u8, value.len * 2);
    var len: usize = 0;
    for (value) |ch| {
        out[len] = ch;
        len += 1;
        if (ch == '\'') {
            out[len] = '\'';
            len += 1;
        }
    }
    return std.heap.page_allocator.realloc(out, len);
}

fn parseTripleTest(line: []const u8) !Triple {
    var index: usize = 0;
    return (try parseTriple(line, &index)).?;
}

test "yago triple parser handles uri and label rows" {
    const rel = try parseTripleTest("<http://yago-knowledge.org/resource/Ada_Lovelace> <http://www.w3.org/2000/01/rdf-schema#label> \"Ada Lovelace\"@en .");
    try std.testing.expectEqualStrings("http://yago-knowledge.org/resource/Ada_Lovelace", rel.subject);
    try std.testing.expect(isLabelPredicate(rel.predicate));
    try std.testing.expectEqualStrings("Ada Lovelace", rel.object.literal);

    const edge = try parseTripleTest("<http://yago-knowledge.org/resource/Ada_Lovelace> <http://yago-knowledge.org/prop/direct/knows> <http://yago-knowledge.org/resource/Charles_Babbage> .");
    try std.testing.expectEqualStrings("http://yago-knowledge.org/resource/Ada_Lovelace", edge.subject);
    try std.testing.expectEqualStrings("http://yago-knowledge.org/prop/direct/knows", edge.predicate);
    try std.testing.expectEqualStrings("http://yago-knowledge.org/resource/Charles_Babbage", edge.object.uri);
}

test "yago importer loads a small rdf fixture" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const yago_file = try tmp.dir.createFile("sample.nt", .{});
    defer yago_file.close();
    try yago_file.writeAll(
        "<http://yago-knowledge.org/resource/Ada_Lovelace> <http://www.w3.org/2000/01/rdf-schema#label> \"Ada Lovelace\"@en .\n" ++
            "<http://yago-knowledge.org/resource/Ada_Lovelace> <http://yago-knowledge.org/prop/direct/knows> <http://yago-knowledge.org/resource/Charles_Babbage> .\n",
    );

    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneImportSchema();

    const root_path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer std.testing.allocator.free(root_path);

    const sample_path = try std.fs.path.join(std.testing.allocator, &.{ root_path, "sample.nt" });
    defer std.testing.allocator.free(sample_path);

    const summary = try runYagoImportBenchmark(db, std.testing.allocator, .{
        .yago_path = sample_path,
        .row_limit = null,
    });
    try std.testing.expectEqual(@as(usize, 2), summary.triple_rows);
    try std.testing.expectEqual(@as(usize, 3), summary.entity_rows);
    try std.testing.expectEqual(@as(usize, 2), summary.relation_rows);
    try std.testing.expectEqual(@as(usize, 1), summary.alias_rows);

    try expectRowCount(db, "SELECT COUNT(*) FROM workspaces WHERE workspace_id = 'yago'", 1);
    try expectRowCount(db, "SELECT COUNT(*) FROM collections WHERE collection_id = 'yago::core_facts'", 1);
    try expectRowCount(db, "SELECT COUNT(*) FROM ontologies WHERE ontology_id = 'yago::core'", 1);
    try expectRowCount(db, "SELECT COUNT(*) FROM entities_raw WHERE workspace_id = 'yago'", 3);
    try expectRowCount(db, "SELECT COUNT(*) FROM entity_aliases_raw WHERE workspace_id = 'yago'", 1);
    try expectRowCount(db, "SELECT COUNT(*) FROM relations_raw WHERE workspace_id = 'yago'", 2);
}

fn expectRowCount(db: facet_sqlite.Database, sql: []const u8, expected: i64) !void {
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
        return error.PrepareFailed;
    }
    defer _ = c.sqlite3_finalize(stmt.?);
    if (c.sqlite3_step(stmt.?) != c.SQLITE_ROW) return error.StepFailed;
    try std.testing.expectEqual(expected, c.sqlite3_column_int64(stmt.?, 0));
}

pub fn runYagoImportBenchmark(db: facet_sqlite.Database, allocator: Allocator, options: ImportOptions) !ImportSummary {
    var summary: ImportSummary = .{
        .yago_path = options.yago_dir orelse options.yago_path orelse "",
        .row_limit = options.row_limit,
    };

    var total_timer = try compat.Timer.start();
    try ensureYagoScaffold(db, options);

    var importer = try Importer.init(db, allocator, options.workspace_id);
    defer importer.deinit();

    summary.load_ns = try timeImport(&importer, options, &summary.triple_rows);

    try flushStagedWrites(db, options);

    var timer = try compat.Timer.start();
    try graph_sqlite.rebuildAdjacency(db, allocator);
    summary.rebuild_adjacency_ns = timer.read();

    timer = try compat.Timer.start();
    try graph_sqlite.refreshEntityDegree(db);
    summary.rebuild_degree_ns = timer.read();

    summary.total_ns = total_timer.read();
    summary.entity_rows = importer.entity_rows;
    summary.relation_rows = importer.relation_rows;
    summary.alias_rows = importer.alias_rows;
    summary.label_rows = importer.label_rows;
    summary.skipped_rows = importer.skipped_rows;
    try db.exec("PRAGMA foreign_keys = ON;");
    return summary;
}

/// Creates the workspace, collection, and yago-core ontology bundle so the
/// raw layer is ready to receive the staged data when flushStagedWrites runs.
fn ensureYagoScaffold(db: facet_sqlite.Database, options: ImportOptions) !void {
    try collections_sqlite.ensureWorkspace(db, .{
        .workspace_id = options.workspace_id,
        .label = "YAGO knowledge graph",
        .domain_profile = "knowledge_graph",
    });
    try collections_sqlite.ensureCollection(db, .{
        .workspace_id = options.workspace_id,
        .collection_id = options.collection_id,
        .name = "core_facts",
    });
    try collections_sqlite.loadOntologyBundle(db, .{
        .header = .{
            .ontology_id = options.ontology_id,
            .workspace_id = options.workspace_id,
            .name = "yago-core",
            .source_kind = "imported",
        },
        .entity_types = &.{
            .{ .ontology_id = options.ontology_id, .entity_type = "resource" },
            .{ .ontology_id = options.ontology_id, .entity_type = "literal" },
        },
        .edge_types = &.{
            .{ .ontology_id = options.ontology_id, .edge_type = "type" },
            .{ .ontology_id = options.ontology_id, .edge_type = "label" },
            .{ .ontology_id = options.ontology_id, .edge_type = "knows" },
        },
    });
    try collections_sqlite.attachOntologyToCollection(
        db,
        options.workspace_id,
        options.collection_id,
        options.ontology_id,
        "primary",
    );
    try collections_sqlite.setDefaultOntology(db, options.workspace_id, options.ontology_id);
}

fn timeImport(
    importer: *Importer,
    options: ImportOptions,
    out_rows: *usize,
) !u64 {
    var timer = try compat.Timer.start();
    out_rows.* = if (options.yago_dir) |yago_dir|
        try importer.importDirectory(yago_dir, options.row_limit)
    else if (options.yago_path) |yago_path|
        try importer.importFile(yago_path, options.row_limit)
    else
        return error.InvalidArguments;
    return timer.read();
}

fn flushStagedWrites(db: facet_sqlite.Database, options: ImportOptions) !void {
    try db.exec("BEGIN IMMEDIATE");
    errdefer db.exec("ROLLBACK") catch {};

    try db.exec(
        \\WITH entity_confidence AS (
        \\    SELECT
        \\        e.entity_id AS entity_id,
        \\        MIN(e.workspace_id) AS workspace_id,
        \\        MIN(e.entity_type) AS entity_type,
        \\        MIN(e.name) AS name,
        \\        MAX(e.confidence) AS confidence,
        \\        MIN(e.metadata_json) AS metadata_json
        \\    FROM temp_yago_entity_stage e
        \\    GROUP BY e.entity_id
        \\)
        \\INSERT OR REPLACE INTO graph_entity(entity_id, workspace_id, entity_type, name, confidence, metadata_json, deprecated_at)
        \\SELECT entity_id, workspace_id, entity_type, name, confidence, metadata_json, NULL
        \\FROM entity_confidence
    );
    try db.exec(
        \\INSERT OR REPLACE INTO graph_entity_alias(term, entity_id, confidence)
        \\SELECT term, entity_id, MAX(confidence)
        \\FROM temp_yago_alias_stage
        \\GROUP BY term, entity_id;
    );
    try db.exec(
        \\INSERT OR REPLACE INTO graph_relation(relation_id, workspace_id, relation_type, source_id, target_id, confidence, metadata_json, deprecated_at)
        \\SELECT relation_id, workspace_id, relation_type, source_id, target_id, confidence, metadata_json, NULL
        \\FROM temp_yago_relation_stage
        \\ORDER BY relation_id;
    );

    try mirrorRawTables(db, options);

    try db.exec("COMMIT");
    try db.exec(
        \\CREATE UNIQUE INDEX IF NOT EXISTS graph_entity_entity_id_uniq
        \\    ON graph_entity(entity_id);
        \\CREATE INDEX IF NOT EXISTS graph_entity_type_name_idx
        \\    ON graph_entity(entity_type, name);
        \\CREATE UNIQUE INDEX IF NOT EXISTS graph_entity_alias_term_entity_uniq
        \\    ON graph_entity_alias(term, entity_id);
        \\CREATE UNIQUE INDEX IF NOT EXISTS graph_relation_relation_id_uniq
        \\    ON graph_relation(relation_id);
        \\CREATE INDEX IF NOT EXISTS graph_entity_workspace_id_idx
        \\    ON graph_entity(workspace_id);
        \\CREATE INDEX IF NOT EXISTS graph_relation_workspace_id_idx
        \\    ON graph_relation(workspace_id);
    );
    try db.exec("PRAGMA wal_checkpoint(TRUNCATE);");
}
