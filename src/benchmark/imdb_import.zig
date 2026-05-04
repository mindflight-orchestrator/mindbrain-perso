const std = @import("std");
const mindbrain = @import("mindbrain");
const compat = @import("zig16_compat.zig");
const facet_sqlite = mindbrain.facet_sqlite;
const collections_sqlite = mindbrain.collections_sqlite;

const Allocator = std.mem.Allocator;
const c = facet_sqlite.c;

pub const default_workspace_id = "imdb";
pub const default_collection_id = "imdb::titles";
pub const default_ontology_id = "imdb::core";

pub const ImportOptions = struct {
    imdb_dir: []const u8,
    row_limit: ?usize = null,
    /// Workspace under which IMDb entities/relations are recorded.
    workspace_id: []const u8 = default_workspace_id,
    /// Collection that owns mirrored IMDb documents (kept for symmetry with
    /// the Pipeline API; the importer itself does not yet emit documents).
    collection_id: []const u8 = default_collection_id,
    /// Ontology id stamped on entities_raw / relations_raw rows.
    ontology_id: []const u8 = default_ontology_id,
};

pub const ImportSummary = struct {
    imdb_dir: []const u8 = "",
    row_limit: ?usize = null,
    title_basics_rows: usize = 0,
    name_basics_rows: usize = 0,
    title_akas_rows: usize = 0,
    title_crew_rows: usize = 0,
    title_episode_rows: usize = 0,
    title_principals_rows: usize = 0,
    title_ratings_rows: usize = 0,
    entity_rows: usize = 0,
    relation_rows: usize = 0,
    alias_rows: usize = 0,
    total_ns: u64 = 0,
    parse_stage_ns: u64 = 0,
    flush_insert_ns: u64 = 0,
    flush_index_ns: u64 = 0,
    flush_checkpoint_ns: u64 = 0,
    title_basics_ns: u64 = 0,
    name_basics_ns: u64 = 0,
    title_ratings_ns: u64 = 0,
    title_akas_ns: u64 = 0,
    title_episode_ns: u64 = 0,
    title_crew_ns: u64 = 0,
    title_principals_ns: u64 = 0,
};

const FlushSummary = struct {
    insert_ns: u64 = 0,
    index_ns: u64 = 0,
    checkpoint_ns: u64 = 0,
};

const EntityKind = enum(u64) {
    title = 1,
    person = 2,
};

const transaction_chunk_rows: usize = 50_000;
const progress_rows: usize = 1_000_000;
const stage_entity_table = "temp_imdb_entity_stage";
const stage_alias_table = "temp_imdb_alias_stage";
const stage_relation_table = "temp_imdb_relation_stage";

const Importer = struct {
    allocator: Allocator,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    stage_entity_stmt: *c.sqlite3_stmt,
    stage_alias_stmt: *c.sqlite3_stmt,
    stage_relation_stmt: *c.sqlite3_stmt,
    next_relation_id: u32 = 1,
    entity_rows: usize = 0,
    relation_rows: usize = 0,
    alias_rows: usize = 0,

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
            \\DROP TABLE IF EXISTS temp_imdb_entity_stage;
            \\DROP TABLE IF EXISTS temp_imdb_alias_stage;
            \\DROP TABLE IF EXISTS temp_imdb_relation_stage;
            \\CREATE TEMP TABLE temp_imdb_entity_stage (
            \\    entity_id INTEGER NOT NULL,
            \\    workspace_id TEXT NOT NULL,
            \\    entity_type TEXT NOT NULL,
            \\    name TEXT NOT NULL,
            \\    confidence REAL NOT NULL,
            \\    metadata_json TEXT NOT NULL,
            \\    PRIMARY KEY(entity_id)
            \\);
            \\CREATE TEMP TABLE temp_imdb_alias_stage (
            \\    term TEXT NOT NULL,
            \\    entity_id INTEGER NOT NULL,
            \\    confidence REAL NOT NULL,
            \\    PRIMARY KEY(term, entity_id)
            \\);
            \\CREATE TEMP TABLE temp_imdb_relation_stage (
            \\    relation_id INTEGER NOT NULL,
            \\    workspace_id TEXT NOT NULL,
            \\    relation_type TEXT NOT NULL,
            \\    source_id INTEGER NOT NULL,
            \\    target_id INTEGER NOT NULL,
            \\    confidence REAL NOT NULL,
            \\    metadata_json TEXT NOT NULL,
            \\    PRIMARY KEY(relation_id)
            \\);
        );

        return .{
            .allocator = allocator,
            .db = db,
            .workspace_id = workspace_id,
            .stage_entity_stmt = try prepare(
                db,
                "INSERT INTO temp_imdb_entity_stage(entity_id, workspace_id, entity_type, name, confidence, metadata_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT(entity_id) DO UPDATE SET workspace_id = excluded.workspace_id, entity_type = excluded.entity_type, name = MIN(temp_imdb_entity_stage.name, excluded.name), confidence = MAX(temp_imdb_entity_stage.confidence, excluded.confidence), metadata_json = MIN(temp_imdb_entity_stage.metadata_json, excluded.metadata_json)",
            ),
            .stage_alias_stmt = try prepare(
                db,
                "INSERT INTO temp_imdb_alias_stage(term, entity_id, confidence) VALUES (?1, ?2, ?3) ON CONFLICT(term, entity_id) DO UPDATE SET confidence = MAX(temp_imdb_alias_stage.confidence, excluded.confidence)",
            ),
            .stage_relation_stmt = try prepare(
                db,
                "INSERT INTO temp_imdb_relation_stage(relation_id, workspace_id, relation_type, source_id, target_id, confidence, metadata_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            ),
        };
    }

    fn deinit(self: *Importer) void {
        finalize(self.stage_relation_stmt);
        finalize(self.stage_alias_stmt);
        finalize(self.stage_entity_stmt);
        self.* = undefined;
    }

    fn importTitleBasics(self: *Importer, imdb_dir: []const u8, limit: ?usize) !usize {
        const file = try openDatasetFile(imdb_dir, "title.basics.tsv");
        defer file.close(compat.io());
        return try importRows(self, file, limit, "title.basics.tsv", handleTitleBasicsRow);
    }

    fn importNameBasics(self: *Importer, imdb_dir: []const u8, limit: ?usize) !usize {
        const file = try openDatasetFile(imdb_dir, "name.basics.tsv");
        defer file.close(compat.io());
        return try importRows(self, file, limit, "name.basics.tsv", handleNameBasicsRow);
    }

    fn importTitleRatings(self: *Importer, imdb_dir: []const u8, limit: ?usize) !usize {
        const file = try openDatasetFile(imdb_dir, "title.ratings.tsv");
        defer file.close(compat.io());
        return try importRows(self, file, limit, "title.ratings.tsv", handleTitleRatingsRow);
    }

    fn importTitleAkas(self: *Importer, imdb_dir: []const u8, limit: ?usize) !usize {
        const file = try openDatasetFile(imdb_dir, "title.akas.tsv");
        defer file.close(compat.io());
        return try importRows(self, file, limit, "title.akas.tsv", handleTitleAkasRow);
    }

    fn importTitleEpisode(self: *Importer, imdb_dir: []const u8, limit: ?usize) !usize {
        const file = try openDatasetFile(imdb_dir, "title.episode.tsv");
        defer file.close(compat.io());
        return try importRows(self, file, limit, "title.episode.tsv", handleTitleEpisodeRow);
    }

    fn importTitleCrew(self: *Importer, imdb_dir: []const u8, limit: ?usize) !usize {
        const file = try openDatasetFile(imdb_dir, "title.crew.tsv");
        defer file.close(compat.io());
        return try importRows(self, file, limit, "title.crew.tsv", handleTitleCrewRow);
    }

    fn importTitlePrincipals(self: *Importer, imdb_dir: []const u8, limit: ?usize) !usize {
        const file = try openDatasetFile(imdb_dir, "title.principals.tsv");
        defer file.close(compat.io());
        return try importRows(self, file, limit, "title.principals.tsv", handleTitlePrincipalsRow);
    }

    fn importRows(
        self: *Importer,
        file: std.Io.File,
        limit: ?usize,
        label: []const u8,
        comptime handler: fn (*Importer, []const u8) anyerror!void,
    ) !usize {
        var read_buf: [1024 * 1024]u8 = undefined;
        var reader = file.reader(compat.io(), &read_buf);
        reader.pos = reader.pos;

        const header = try readNextLine(&reader.interface);
        if (header == null) return 0;

        try dbExec(self.db, "BEGIN IMMEDIATE");
        var txn_open = true;
        errdefer if (txn_open) dbExec(self.db, "ROLLBACK") catch {};

        var timer = try compat.Timer.start();
        var count: usize = 0;
        var chunk_rows: usize = 0;
        var next_report: usize = progress_rows;
        std.debug.print("[imdb] starting {s}\n", .{label});
        while (true) {
            const raw_row = try readNextLine(&reader.interface) orelse break;
            const row = std.mem.trim(u8, raw_row, "\r");
            if (row.len == 0) continue;

            try handler(self, row);
            count += 1;
            chunk_rows += 1;
            if (limit) |max_rows| {
                if (count >= max_rows) break;
            }
            if (count >= next_report) {
                std.debug.print(
                    "[imdb] {s}: {d} rows staged, entities={d}, relations={d}, aliases={d}\n",
                    .{ label, count, self.entity_rows, self.relation_rows, self.alias_rows },
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
            "[imdb] finished {s}: {d} rows in {d} ms, entities={d}, relations={d}, aliases={d}\n",
            .{
                label,
                count,
                timer.read() / std.time.ns_per_ms,
                self.entity_rows,
                self.relation_rows,
                self.alias_rows,
            },
        );

        return count;
    }

    fn handleTitleBasicsRow(self: *Importer, line: []const u8) !void {
        var it = std.mem.splitScalar(u8, line, '\t');
        const tconst = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;
        const primary_title = it.next() orelse return error.InvalidRow;
        const original_title = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;

        const entity_id = try imdbEntityId(.title, try parseImdbNumericId(tconst, "tt"));
        try self.upsertEntity(entity_id, "title", tconst, 1.0);

        if (parseNullableField(primary_title) != null) {
            try self.insertAlias(entity_id, primary_title, 1.0);
        }
        if (parseNullableField(original_title)) |value| {
            if (!std.mem.eql(u8, value, primary_title)) {
                try self.insertAlias(entity_id, value, 0.9);
            }
        }
    }

    fn handleNameBasicsRow(self: *Importer, line: []const u8) !void {
        var it = std.mem.splitScalar(u8, line, '\t');
        const nconst = it.next() orelse return error.InvalidRow;
        const primary_name = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;
        const known_for_titles = it.next() orelse return error.InvalidRow;

        const entity_id = try imdbEntityId(.person, try parseImdbNumericId(nconst, "nm"));
        try self.upsertEntity(entity_id, "person", nconst, 1.0);

        if (parseNullableField(primary_name) != null) {
            try self.insertAlias(entity_id, primary_name, 1.0);
        }

        if (parseNullableField(known_for_titles)) |value| {
            var titles = std.mem.splitScalar(u8, value, ',');
            while (titles.next()) |title_id| {
                const trimmed = std.mem.trim(u8, title_id, " ");
                if (trimmed.len == 0) continue;
                const title_entity_id = try imdbEntityId(.title, try parseImdbNumericId(trimmed, "tt"));
                try self.upsertEntity(title_entity_id, "title", trimmed, 0.5);
                try self.insertRelation(self.nextRelationId(), "known_for", entity_id, title_entity_id, 0.75, "{}");
            }
        }
    }

    fn handleTitleRatingsRow(self: *Importer, line: []const u8) !void {
        var it = std.mem.splitScalar(u8, line, '\t');
        const tconst = it.next() orelse return error.InvalidRow;
        const average_rating = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;

        const title_entity_id = try imdbEntityId(.title, try parseImdbNumericId(tconst, "tt"));
        const rating = try std.fmt.parseFloat(f32, average_rating);
        try self.upsertEntity(title_entity_id, "title", tconst, rating / 10.0);
    }

    fn handleTitleAkasRow(self: *Importer, line: []const u8) !void {
        var it = std.mem.splitScalar(u8, line, '\t');
        const title_id = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;
        const title = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;
        const is_original_title = it.next() orelse return error.InvalidRow;

        const entity_id = try imdbEntityId(.title, try parseImdbNumericId(title_id, "tt"));
        const confidence: f32 = if (std.mem.eql(u8, is_original_title, "1")) 0.95 else 0.6;
        try self.upsertEntity(entity_id, "title", title_id, confidence);
        if (parseNullableField(title)) |value| {
            try self.insertAlias(entity_id, value, confidence);
        }
    }

    fn handleTitleEpisodeRow(self: *Importer, line: []const u8) !void {
        var it = std.mem.splitScalar(u8, line, '\t');
        const tconst = it.next() orelse return error.InvalidRow;
        const parent_tconst = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;

        const child_id = try imdbEntityId(.title, try parseImdbNumericId(tconst, "tt"));
        const parent_id = try imdbEntityId(.title, try parseImdbNumericId(parent_tconst, "tt"));
        try self.upsertEntity(child_id, "title", tconst, 0.5);
        try self.upsertEntity(parent_id, "title", parent_tconst, 0.5);
        try self.insertRelation(self.nextRelationId(), "episode_of", child_id, parent_id, 1.0, "{}");
    }

    fn handleTitleCrewRow(self: *Importer, line: []const u8) !void {
        var it = std.mem.splitScalar(u8, line, '\t');
        const tconst = it.next() orelse return error.InvalidRow;
        const directors = it.next() orelse return error.InvalidRow;
        const writers = it.next() orelse return error.InvalidRow;

        const title_id = try imdbEntityId(.title, try parseImdbNumericId(tconst, "tt"));
        try self.upsertEntity(title_id, "title", tconst, 0.5);
        try self.insertCrewList(title_id, "director", directors, 1.0);
        try self.insertCrewList(title_id, "writer", writers, 1.0);
    }

    fn handleTitlePrincipalsRow(self: *Importer, line: []const u8) !void {
        var it = std.mem.splitScalar(u8, line, '\t');
        const tconst = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;
        const nconst = it.next() orelse return error.InvalidRow;
        const category = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;
        _ = it.next() orelse return error.InvalidRow;

        const title_id = try imdbEntityId(.title, try parseImdbNumericId(tconst, "tt"));
        const person_id = try imdbEntityId(.person, try parseImdbNumericId(nconst, "nm"));
        const role = std.mem.trim(u8, category, " ");
        if (role.len == 0) return;
        try self.upsertEntity(title_id, "title", tconst, 0.5);
        try self.upsertEntity(person_id, "person", nconst, 0.5);
        try self.insertRelation(self.nextRelationId(), role, title_id, person_id, 0.9, "{}");
    }

    fn insertCrewList(self: *Importer, title_id: u32, relation_type: []const u8, list: []const u8, confidence: f32) !void {
        const field = parseNullableField(list) orelse return;
        var it = std.mem.splitScalar(u8, field, ',');
        while (it.next()) |item| {
            const trimmed = std.mem.trim(u8, item, " ");
            if (trimmed.len == 0) continue;
            const person_id = try imdbEntityId(.person, try parseImdbNumericId(trimmed, "nm"));
            try self.upsertEntity(person_id, "person", trimmed, 0.5);
            try self.insertRelation(self.nextRelationId(), relation_type, title_id, person_id, confidence, "{}");
        }
    }

    fn upsertEntity(self: *Importer, entity_id: u32, entity_type: []const u8, name: []const u8, confidence: f32) !void {
        resetStatement(self.stage_entity_stmt) catch unreachable;
        try bindInt64(self.stage_entity_stmt, 1, entity_id);
        try bindText(self.stage_entity_stmt, 2, self.workspace_id);
        try bindText(self.stage_entity_stmt, 3, entity_type);
        try bindText(self.stage_entity_stmt, 4, name);
        if (c.sqlite3_bind_double(self.stage_entity_stmt, 5, confidence) != c.SQLITE_OK) return error.BindFailed;
        try bindText(self.stage_entity_stmt, 6, "{}");
        // Collapse repeated sightings of the same logical entity into one staged row.
        try stepDone(self.stage_entity_stmt);
        self.entity_rows += 1;
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
        self.relation_rows += 1;
    }

    fn nextRelationId(self: *Importer) u32 {
        const relation_id = self.next_relation_id;
        self.next_relation_id += 1;
        return relation_id;
    }
};

pub fn runImdbImportBenchmark(db: facet_sqlite.Database, allocator: Allocator, options: ImportOptions) !ImportSummary {
    var summary: ImportSummary = .{
        .imdb_dir = options.imdb_dir,
        .row_limit = options.row_limit,
    };

    var total_timer = try compat.Timer.start();
    try ensureImdbScaffold(db, options);
    var importer = try Importer.init(db, allocator, options.workspace_id);
    defer importer.deinit();

    summary.title_basics_ns = try timeImport(&importer, options.imdb_dir, options.row_limit, Importer.importTitleBasics, &summary.title_basics_rows);
    summary.name_basics_ns = try timeImport(&importer, options.imdb_dir, options.row_limit, Importer.importNameBasics, &summary.name_basics_rows);
    summary.title_ratings_ns = try timeImport(&importer, options.imdb_dir, options.row_limit, Importer.importTitleRatings, &summary.title_ratings_rows);
    summary.title_akas_ns = try timeImport(&importer, options.imdb_dir, options.row_limit, Importer.importTitleAkas, &summary.title_akas_rows);
    summary.title_episode_ns = try timeImport(&importer, options.imdb_dir, options.row_limit, Importer.importTitleEpisode, &summary.title_episode_rows);
    summary.title_crew_ns = try timeImport(&importer, options.imdb_dir, options.row_limit, Importer.importTitleCrew, &summary.title_crew_rows);
    summary.title_principals_ns = try timeImport(&importer, options.imdb_dir, options.row_limit, Importer.importTitlePrincipals, &summary.title_principals_rows);

    summary.entity_rows = try countRows(db, stage_entity_table);
    summary.relation_rows = try countRows(db, stage_relation_table);
    summary.alias_rows = try countRows(db, stage_alias_table);
    summary.parse_stage_ns =
        summary.title_basics_ns +
        summary.name_basics_ns +
        summary.title_ratings_ns +
        summary.title_akas_ns +
        summary.title_episode_ns +
        summary.title_crew_ns +
        summary.title_principals_ns;

    const flush_summary = try flushStagedWrites(db, options);
    summary.flush_insert_ns = flush_summary.insert_ns;
    summary.flush_index_ns = flush_summary.index_ns;
    summary.flush_checkpoint_ns = flush_summary.checkpoint_ns;

    summary.total_ns = total_timer.read();
    return summary;
}

fn timeImport(
    importer: *Importer,
    imdb_dir: []const u8,
    limit: ?usize,
    comptime method: fn (*Importer, []const u8, ?usize) anyerror!usize,
    out_rows: *usize,
) !u64 {
    var timer = try compat.Timer.start();
    out_rows.* = try method(importer, imdb_dir, limit);
    return timer.read();
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
    if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
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

fn openDatasetFile(imdb_dir: []const u8, file_name: []const u8) !std.Io.File {
    const allocator = std.heap.page_allocator;

    const direct_path = try std.fs.path.join(allocator, &.{ imdb_dir, file_name });
    defer allocator.free(direct_path);
    if (std.Io.Dir.cwd().openFile(compat.io(), direct_path, .{ .mode = .read_only })) |file| {
        return file;
    } else |_| {}

    const nested_path = try std.fs.path.join(allocator, &.{ imdb_dir, "imdb-datasets", file_name });
    defer allocator.free(nested_path);
    return try std.Io.Dir.cwd().openFile(compat.io(), nested_path, .{ .mode = .read_only });
}

fn dbExec(db: facet_sqlite.Database, sql: []const u8) !void {
    try db.exec(sql);
}

fn parseNullableField(value: []const u8) ?[]const u8 {
    if (std.mem.eql(u8, value, "\\N")) return null;
    return value;
}

fn parseImdbNumericId(value: []const u8, prefix: []const u8) !u64 {
    if (!std.mem.startsWith(u8, value, prefix)) return error.InvalidRow;
    if (value.len <= prefix.len) return error.InvalidRow;
    return std.fmt.parseInt(u64, value[prefix.len..], 10);
}

fn flushStagedWrites(db: facet_sqlite.Database, options: ImportOptions) !FlushSummary {
    var summary: FlushSummary = .{};

    var insert_timer = try compat.Timer.start();
    try db.exec("BEGIN IMMEDIATE");
    errdefer db.exec("ROLLBACK") catch {};

    try db.exec(
        \\INSERT INTO graph_entity(entity_id, workspace_id, entity_type, name, confidence, metadata_json, deprecated_at)
        \\SELECT entity_id, workspace_id, entity_type, name, confidence, metadata_json, NULL
        \\FROM temp_imdb_entity_stage;
    );
    try db.exec(
        \\INSERT INTO graph_entity_alias(term, entity_id, confidence)
        \\SELECT term, entity_id, confidence
        \\FROM temp_imdb_alias_stage;
    );
    try db.exec(
        \\INSERT INTO graph_relation(relation_id, workspace_id, relation_type, source_id, target_id, confidence, metadata_json, deprecated_at)
        \\SELECT relation_id, workspace_id, relation_type, source_id, target_id, confidence, metadata_json, NULL
        \\FROM temp_imdb_relation_stage;
    );

    try mirrorImdbRawTables(db, options);

    try db.exec("COMMIT");
    summary.insert_ns = insert_timer.read();

    var index_timer = try compat.Timer.start();
    try db.exec(
        \\CREATE INDEX IF NOT EXISTS graph_entity_type_name_idx
        \\    ON graph_entity(entity_type, name);
        \\CREATE UNIQUE INDEX IF NOT EXISTS graph_entity_alias_term_entity_uniq
        \\    ON graph_entity_alias(term, entity_id);
        \\CREATE INDEX IF NOT EXISTS graph_entity_workspace_id_idx
        \\    ON graph_entity(workspace_id);
        \\CREATE INDEX IF NOT EXISTS graph_relation_workspace_id_idx
        \\    ON graph_relation(workspace_id);
    );
    summary.index_ns = index_timer.read();

    var checkpoint_timer = try compat.Timer.start();
    try db.exec("PRAGMA wal_checkpoint(TRUNCATE);");
    summary.checkpoint_ns = checkpoint_timer.read();
    return summary;
}

fn countRows(db: facet_sqlite.Database, table_name: []const u8) !usize {
    const sql = try std.fmt.allocPrint(std.heap.page_allocator, "SELECT COUNT(*) FROM {s}", .{table_name});
    defer std.heap.page_allocator.free(sql);

    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
        return error.PrepareFailed;
    }
    defer _ = c.sqlite3_finalize(stmt.?);
    if (c.sqlite3_step(stmt.?) != c.SQLITE_ROW) return error.StepFailed;
    return @intCast(c.sqlite3_column_int64(stmt.?, 0));
}

fn imdbEntityId(kind: EntityKind, source_key: u64) !u32 {
    const encoded = (source_key << 1) | @intFromEnum(kind);
    return std.math.cast(u32, encoded) orelse error.ValueOutOfRange;
}

/// Creates the workspace, collection and imdb-core ontology bundle so the
/// raw layer is ready to receive the staged data when flushStagedWrites runs.
fn ensureImdbScaffold(db: facet_sqlite.Database, options: ImportOptions) !void {
    try collections_sqlite.ensureWorkspace(db, .{
        .workspace_id = options.workspace_id,
        .label = "IMDb dataset",
        .domain_profile = "media",
    });
    try collections_sqlite.ensureCollection(db, .{
        .workspace_id = options.workspace_id,
        .collection_id = options.collection_id,
        .name = "titles",
    });
    try collections_sqlite.loadOntologyBundle(db, .{
        .header = .{
            .ontology_id = options.ontology_id,
            .workspace_id = options.workspace_id,
            .name = "imdb-core",
            .source_kind = "imported",
        },
        .entity_types = &.{
            .{ .ontology_id = options.ontology_id, .entity_type = "title" },
            .{ .ontology_id = options.ontology_id, .entity_type = "person" },
        },
        .edge_types = &.{
            .{ .ontology_id = options.ontology_id, .edge_type = "director" },
            .{ .ontology_id = options.ontology_id, .edge_type = "writer" },
            .{ .ontology_id = options.ontology_id, .edge_type = "actor" },
            .{ .ontology_id = options.ontology_id, .edge_type = "actress" },
            .{ .ontology_id = options.ontology_id, .edge_type = "known_for" },
            .{ .ontology_id = options.ontology_id, .edge_type = "episode_of" },
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

/// Bulk-mirror staged graph data into the workspace-scoped raw tables so the
/// raw layer can drive backup/restore and reindex flows without re-parsing
/// IMDb TSV files.
fn mirrorImdbRawTables(db: facet_sqlite.Database, options: ImportOptions) !void {
    const ws_lit = try escapeSqlLiteral(options.workspace_id);
    defer std.heap.page_allocator.free(ws_lit);
    const onto_lit = try escapeSqlLiteral(options.ontology_id);
    defer std.heap.page_allocator.free(onto_lit);

    const entities_sql = try std.fmt.allocPrint(std.heap.page_allocator,
        \\INSERT OR REPLACE INTO entities_raw(workspace_id, ontology_id, entity_id, entity_type, name, confidence, metadata_json)
        \\SELECT '{s}', '{s}', entity_id, entity_type, name, confidence, metadata_json
        \\FROM temp_imdb_entity_stage;
    , .{ ws_lit, onto_lit });
    defer std.heap.page_allocator.free(entities_sql);
    try db.exec(entities_sql);

    const aliases_sql = try std.fmt.allocPrint(std.heap.page_allocator,
        \\INSERT OR REPLACE INTO entity_aliases_raw(workspace_id, entity_id, term, confidence)
        \\SELECT '{s}', entity_id, term, confidence
        \\FROM temp_imdb_alias_stage;
    , .{ws_lit});
    defer std.heap.page_allocator.free(aliases_sql);
    try db.exec(aliases_sql);

    const relations_sql = try std.fmt.allocPrint(std.heap.page_allocator,
        \\INSERT OR REPLACE INTO relations_raw(workspace_id, ontology_id, relation_id, edge_type, source_entity_id, target_entity_id, confidence, metadata_json)
        \\SELECT '{s}', '{s}', relation_id, relation_type, source_id, target_id, confidence, metadata_json
        \\FROM temp_imdb_relation_stage
        \\ORDER BY relation_id;
    , .{ ws_lit, onto_lit });
    defer std.heap.page_allocator.free(relations_sql);
    try db.exec(relations_sql);
}

/// Doubles single quotes so the value can be safely interpolated into a
/// SQL string literal (we use literal substitution because db.exec doesn't
/// expose parameter binding).
fn escapeSqlLiteral(value: []const u8) ![]u8 {
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

test "imdb entity encoding keeps titles and people in separate spaces" {
    try std.testing.expectEqual(@as(u32, 3), try imdbEntityId(.title, 1));
    try std.testing.expectEqual(@as(u32, 4), try imdbEntityId(.person, 2));
}

test "imdb importer handles a small TSV fixture" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var imdb_dir = try tmp.dir.makeOpenPath("imdb-datasets", .{});
    defer imdb_dir.close();

    try writeFixture(imdb_dir, "title.basics.tsv", "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n" ++
        "tt0000001\tshort\tShort Film\tShort Film\t0\t2001\t\\N\t5\tDrama\n" ++
        "tt0000002\tmovie\tFeature Film\tFeature Film\t0\t2002\t\\N\t95\tAction\n");
    try writeFixture(imdb_dir, "name.basics.tsv", "nconst\tprimaryName\tbirthYear\tdeathYear\tprimaryProfession\tknownForTitles\n" ++
        "nm0000001\tAda Lovelace\t1815\t1852\twriter\ttt0000001,tt0000002\n");
    try writeFixture(imdb_dir, "title.ratings.tsv", "tconst\taverageRating\tnumVotes\n" ++
        "tt0000001\t8.5\t1234\n");
    try writeFixture(imdb_dir, "title.akas.tsv", "titleId\tordering\ttitle\tregion\tlanguage\ttypes\tattributes\tisOriginalTitle\n" ++
        "tt0000001\t1\tShort Film EU\tFR\tfr\t\\N\t\\N\t0\n");
    try writeFixture(imdb_dir, "title.episode.tsv", "tconst\tparentTconst\tseasonNumber\tepisodeNumber\n" ++
        "tt0000002\ttt0000001\t1\t1\n");
    try writeFixture(imdb_dir, "title.crew.tsv", "tconst\tdirectors\twriters\n" ++
        "tt0000002\tnm0000001\tnm0000001\n");
    try writeFixture(imdb_dir, "title.principals.tsv", "tconst\tordering\tnconst\tcategory\tjob\tcharacters\n" ++
        "tt0000002\t1\tnm0000001\tactor\thero\t[\"Ada\"]\n");

    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneImportSchema();

    const root_path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer std.testing.allocator.free(root_path);

    const summary = try runImdbImportBenchmark(db, std.testing.allocator, .{ .imdb_dir = root_path, .row_limit = null });
    try std.testing.expectEqual(@as(usize, 2), summary.title_basics_rows);
    try std.testing.expectEqual(@as(usize, 1), summary.name_basics_rows);
    try std.testing.expectEqual(@as(usize, 1), summary.title_ratings_rows);
    try std.testing.expectEqual(@as(usize, 1), summary.title_akas_rows);
    try std.testing.expectEqual(@as(usize, 1), summary.title_episode_rows);
    try std.testing.expectEqual(@as(usize, 1), summary.title_crew_rows);
    try std.testing.expectEqual(@as(usize, 1), summary.title_principals_rows);
    try std.testing.expect(summary.entity_rows >= 3);
    try std.testing.expect(summary.relation_rows >= 5);
    try std.testing.expect(summary.alias_rows >= 2);

    try expectImdbRowCount(db, "SELECT COUNT(*) FROM workspaces WHERE workspace_id = 'imdb'", 1);
    try expectImdbRowCount(db, "SELECT COUNT(*) FROM collections WHERE collection_id = 'imdb::titles'", 1);
    try expectImdbRowCount(db, "SELECT COUNT(*) FROM ontologies WHERE ontology_id = 'imdb::core'", 1);
    try expectAtLeastImdbRowCount(db, "SELECT COUNT(*) FROM entities_raw WHERE workspace_id = 'imdb'", 3);
    try expectAtLeastImdbRowCount(db, "SELECT COUNT(*) FROM relations_raw WHERE workspace_id = 'imdb'", 5);
}

fn expectImdbRowCount(db: facet_sqlite.Database, sql: []const u8, expected: i64) !void {
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
        return error.PrepareFailed;
    }
    defer _ = c.sqlite3_finalize(stmt.?);
    if (c.sqlite3_step(stmt.?) != c.SQLITE_ROW) return error.StepFailed;
    try std.testing.expectEqual(expected, c.sqlite3_column_int64(stmt.?, 0));
}

fn expectAtLeastImdbRowCount(db: facet_sqlite.Database, sql: []const u8, minimum: i64) !void {
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
        return error.PrepareFailed;
    }
    defer _ = c.sqlite3_finalize(stmt.?);
    if (c.sqlite3_step(stmt.?) != c.SQLITE_ROW) return error.StepFailed;
    const got = c.sqlite3_column_int64(stmt.?, 0);
    try std.testing.expect(got >= minimum);
}

fn writeFixture(dir: std.fs.Dir, file_name: []const u8, content: []const u8) !void {
    var file = try dir.createFile(file_name, .{ .truncate = true });
    defer file.close(compat.io());
    try file.writeAll(content);
    if (content.len == 0 or content[content.len - 1] != '\n') {
        try file.writeAll("\n");
    }
}
