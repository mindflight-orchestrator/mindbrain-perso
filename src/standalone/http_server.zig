const std = @import("std");
const mindbrain = @import("mindbrain");
const facet_sqlite = mindbrain.facet_sqlite;
const facts_sqlite = mindbrain.facts_sqlite;
const graph_sqlite = mindbrain.graph_sqlite;
const helper_api = mindbrain.helper_api;
const ontology_sqlite = mindbrain.ontology_sqlite;
const pragma_sqlite = mindbrain.pragma_sqlite;
const queue_sqlite = mindbrain.queue_sqlite;
const search_sqlite = mindbrain.search_sqlite;
const toon_exports = mindbrain.toon_exports;
const workspace_sqlite = mindbrain.workspace_sqlite;
const http_config = @import("http_server_config.zig");

const http = std.http;
const log = std.log.scoped(.mindbrain_http);

fn unixTimestamp() i64 {
    return std.Io.Timestamp.now(mindbrain.zig16_compat.io(), .real).toSeconds();
}

fn milliTimestamp() i64 {
    return std.Io.Timestamp.now(mindbrain.zig16_compat.io(), .real).toMilliseconds();
}

const SqlSession = struct {
    db: facet_sqlite.Database,
    mutex: std.Io.Mutex = .init,
};

const SqlRequest = struct {
    sql: []const u8 = "",
    params: []const std.json.Value = &.{},
    session_id: ?u64 = null,
    commit: ?bool = null,
};

const FactWriteRequest = struct {
    id: ?[]const u8 = null,
    workspace_id: ?[]const u8 = null,
    schema_id: ?[]const u8 = null,
    content: ?[]const u8 = null,
    facets_json: ?[]const u8 = null,
    embedding_blob: ?[]const u8 = null,
    created_by: ?[]const u8 = null,
    valid_from_unix: ?i64 = null,
    valid_until_unix: ?i64 = null,
    source_ref: ?[]const u8 = null,
};

pub fn main(init: std.process.Init) !void {
    mindbrain.zig16_compat.setIo(init.io);
    var app = try MindbrainHttpApp.init(
        init.gpa,
        init.io,
        try init.minimal.args.toSlice(init.arena.allocator()),
        init.environ_map,
    );
    defer app.deinit();

    if (app.init_only) return;

    try app.serve();
}

const MindbrainHttpApp = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    db_path: []const u8,
    static_dir: []const u8,
    listen_addr: std.Io.net.IpAddress,
    listen_addr_text: []const u8,
    init_only: bool,
    max_body_bytes: usize,
    max_connections: u32,
    db_path_owned: []u8,
    static_dir_owned: []u8,
    listen_addr_text_owned: []u8,
    sql_sessions_mutex: std.Io.Mutex,
    sql_sessions: std.AutoHashMap(u64, *SqlSession),
    next_sql_session_id: u64,
    connection_slots_mutex: std.Io.Mutex,
    active_connections: u32,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        args: []const []const u8,
        environ_map: *const std.process.Environ.Map,
    ) !MindbrainHttpApp {
        const env_addr = environ_map.get("MINDBRAIN_HTTP_ADDR");
        const env_db = environ_map.get("MINDBRAIN_DB_PATH");
        const env_static_dir = environ_map.get("MINDBRAIN_STATIC_DIR");
        const env_max_body = environ_map.get("MINDBRAIN_HTTP_MAX_BODY_BYTES");
        const env_max_conns = environ_map.get("MINDBRAIN_HTTP_MAX_CONNS");

        const options = try http_config.resolveStartupOptions(
            args,
            env_addr,
            env_db,
            env_static_dir,
            env_max_body,
            env_max_conns,
            printUsage,
        );

        const listen_addr = try http_config.parseListenAddress(options.addr_text);
        const listen_addr_text_owned = try allocator.dupe(u8, options.addr_text);
        const db_path_owned = allocator.dupe(u8, options.db_path) catch |err| {
            allocator.free(listen_addr_text_owned);
            return err;
        };
        const static_dir_owned = allocator.dupe(u8, options.static_dir) catch |err| {
            allocator.free(db_path_owned);
            allocator.free(listen_addr_text_owned);
            return err;
        };
        var app = MindbrainHttpApp{
            .allocator = allocator,
            .io = io,
            .db_path = db_path_owned,
            .static_dir = static_dir_owned,
            .listen_addr = listen_addr,
            .listen_addr_text = listen_addr_text_owned,
            .init_only = options.init_only,
            .max_body_bytes = options.max_body_bytes,
            .max_connections = options.max_connections,
            .db_path_owned = db_path_owned,
            .static_dir_owned = static_dir_owned,
            .listen_addr_text_owned = listen_addr_text_owned,
            .sql_sessions_mutex = .init,
            .sql_sessions = std.AutoHashMap(u64, *SqlSession).init(allocator),
            .next_sql_session_id = 1,
            .connection_slots_mutex = .init,
            .active_connections = 0,
        };
        errdefer app.deinit();
        try app.initDatabase();
        return app;
    }

    pub fn deinit(self: *MindbrainHttpApp) void {
        self.closeAllSqlSessions();
        self.sql_sessions.deinit();
        self.allocator.free(self.db_path_owned);
        self.allocator.free(self.static_dir_owned);
        self.allocator.free(self.listen_addr_text_owned);
    }

    pub fn serve(self: *MindbrainHttpApp) !void {
        var listener = try self.listen_addr.listen(self.io, .{
            .reuse_address = true,
        });
        defer listener.deinit(self.io);

        log.info(
            "mindbrain http listening on {s} ({s}, max_body_bytes={}, max_connections={})",
            .{
                self.listen_addr_text,
                if (http_config.isLoopbackListenText(self.listen_addr_text)) "loopback-only" else "non-loopback",
                self.max_body_bytes,
                self.max_connections,
            },
        );
        while (true) {
            const connection = listener.accept(self.io) catch |err| {
                log.err("accept failed: {s}", .{@errorName(err)});
                continue;
            };

            if (!self.tryAcquireConnectionSlot()) {
                log.warn("rejecting connection on {s}: max_connections={} reached", .{
                    self.listen_addr_text,
                    self.max_connections,
                });
                rejectBusyConnection(self.io, connection);
                continue;
            }

            const thread = std.Thread.spawn(.{}, connectionWorker, .{
                self,
                connection,
            }) catch |err| {
                self.releaseConnectionSlot();
                log.err("spawn worker failed: {s}", .{@errorName(err)});
                connection.close(self.io);
                continue;
            };
            thread.detach();
        }
    }

    fn connectionWorker(
        self: *MindbrainHttpApp,
        connection: std.Io.net.Stream,
    ) void {
        defer self.releaseConnectionSlot();
        self.serveConnection(connection) catch |err| {
            if (err != error.ConnectionResetByPeer and err != error.BrokenPipe) {
                log.err("connection failed: {s}", .{@errorName(err)});
            }
        };
    }

    fn tryAcquireConnectionSlot(self: *MindbrainHttpApp) bool {
        self.connection_slots_mutex.lockUncancelable(self.io);
        defer self.connection_slots_mutex.unlock(self.io);

        if (self.active_connections >= self.max_connections) return false;
        self.active_connections += 1;
        return true;
    }

    fn releaseConnectionSlot(self: *MindbrainHttpApp) void {
        self.connection_slots_mutex.lockUncancelable(self.io);
        defer self.connection_slots_mutex.unlock(self.io);

        if (self.active_connections > 0) {
            self.active_connections -= 1;
        }
    }

    fn initDatabase(self: *MindbrainHttpApp) !void {
        if (self.db_path.len == 0) return error.InvalidArguments;

        if (std.fs.path.dirname(self.db_path)) |dir| {
            if (dir.len != 0 and !std.mem.eql(u8, dir, ".")) {
                try std.Io.Dir.cwd().createDirPath(self.io, dir);
            }
        }

        var db = try facet_sqlite.Database.open(self.db_path);
        defer db.close();
        try db.applyStandaloneSchema();
        try workspace_sqlite.upsertWorkspace(db, "default", "{\"domain\":\"ghostcrab\"}");

        if (try self.graphEntityCount(db) == 0) {
            log.warn("standalone sqlite is empty at {s}; bootstrap it with the baseline SQL before starting the dashboard", .{self.db_path});
        } else {
            log.info("standalone sqlite ready at {s}", .{self.db_path});
        }
    }

    fn graphEntityCount(self: *MindbrainHttpApp, db: facet_sqlite.Database) !i64 {
        return self.countRows(db, "graph_entity");
    }

    fn countRows(self: *MindbrainHttpApp, db: facet_sqlite.Database, table_name: []const u8) !i64 {
        const c = facet_sqlite.c;
        const sql = try std.fmt.allocPrint(self.allocator, "SELECT COUNT(*) FROM {s}", .{table_name});
        defer self.allocator.free(sql);
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
            return error.PrepareFailed;
        }
        defer _ = c.sqlite3_finalize(stmt.?);
        if (c.sqlite3_step(stmt.?) != c.SQLITE_ROW) return error.StepFailed;
        return c.sqlite3_column_int64(stmt.?, 0);
    }

    fn closeAllSqlSessions(self: *MindbrainHttpApp) void {
        self.sql_sessions_mutex.lockUncancelable(self.io);
        defer self.sql_sessions_mutex.unlock(self.io);

        var it = self.sql_sessions.iterator();
        while (it.next()) |entry| {
            const session = entry.value_ptr.*;
            session.db.close();
            self.allocator.destroy(session);
        }
        self.sql_sessions.clearRetainingCapacity();
    }

    fn handleSqlSessionOpen(
        self: *MindbrainHttpApp,
        allocator: std.mem.Allocator,
        request: *http.Server.Request,
        body_buffer: []u8,
    ) !Response {
        _ = request;
        _ = body_buffer;

        var session = try self.allocator.create(SqlSession);
        errdefer self.allocator.destroy(session);
        session.* = .{
            .db = try self.openDb(),
            .mutex = .init,
        };
        errdefer session.db.close();

        session.db.exec("BEGIN IMMEDIATE") catch |err| {
            session.db.close();
            return err;
        };

        self.sql_sessions_mutex.lockUncancelable(self.io);
        defer self.sql_sessions_mutex.unlock(self.io);

        const session_id = self.next_sql_session_id;
        self.next_sql_session_id += 1;
        try self.sql_sessions.put(session_id, session);

        return toResponse(
            try helper_api.jsonResponse(allocator, .{
                .ok = true,
                .session_id = session_id,
            }),
        );
    }

    fn handleSqlSessionClose(
        self: *MindbrainHttpApp,
        allocator: std.mem.Allocator,
        request: *http.Server.Request,
        body_buffer: []u8,
    ) !Response {
        const sql_request = try self.parseSqlRequest(allocator, request, body_buffer);
        const session_id = sql_request.session_id orelse return error.BadRequest;
        const commit = sql_request.commit orelse true;

        const session = try self.takeSqlSession(session_id);
        defer self.allocator.destroy(session);

        session.mutex.lockUncancelable(self.io);
        defer session.mutex.unlock(self.io);
        defer session.db.close();

        if (commit) {
            session.db.exec("COMMIT") catch |commit_err| {
                var rollback_err: ?anyerror = null;
                session.db.exec("ROLLBACK") catch |err| {
                    rollback_err = err;
                };
                return try self.sqliteErrorResponse(allocator, session.db, .internal_server_error, "COMMIT", commit_err, rollback_err);
            };
        } else {
            session.db.exec("ROLLBACK") catch |rollback_err| {
                return try self.sqliteErrorResponse(allocator, session.db, .internal_server_error, "ROLLBACK", rollback_err, null);
            };
        }

        return toResponse(
            try helper_api.jsonResponse(allocator, .{
                .ok = true,
                .session_id = session_id,
                .committed = commit,
            }),
        );
    }

    fn handleSqlRequest(
        self: *MindbrainHttpApp,
        allocator: std.mem.Allocator,
        request: *http.Server.Request,
        body_buffer: []u8,
        session_required: bool,
    ) !Response {
        const sql_request = try self.parseSqlRequest(allocator, request, body_buffer);
        if (sql_request.sql.len == 0) return error.BadRequest;

        if (session_required and sql_request.session_id == null) {
            return error.BadRequest;
        }

        if (sql_request.session_id) |session_id| {
            const session = try self.getSqlSession(session_id);
            session.mutex.lockUncancelable(self.io);
            defer session.mutex.unlock(self.io);

            return try self.executeSql(allocator, session.db, sql_request.sql, sql_request.params);
        }

        var db = try self.openDb();
        defer db.close();
        return try self.executeSql(allocator, db, sql_request.sql, sql_request.params);
    }

    fn parseSqlRequest(
        self: *MindbrainHttpApp,
        allocator: std.mem.Allocator,
        request: *http.Server.Request,
        body_buffer: []u8,
    ) !SqlRequest {
        const content_length = request.head.content_length orelse 0;
        if (content_length == 0) return .{};
        if (content_length > self.max_body_bytes) return error.RequestTooLarge;

        const reader = request.readerExpectNone(body_buffer);
        const body = try reader.readAlloc(allocator, @intCast(content_length));
        const parsed = try std.json.parseFromSliceLeaky(
            SqlRequest,
            allocator,
            body,
            .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = false,
            },
        );
        return parsed;
    }

    fn getSqlSession(self: *MindbrainHttpApp, session_id: u64) !*SqlSession {
        self.sql_sessions_mutex.lockUncancelable(self.io);
        defer self.sql_sessions_mutex.unlock(self.io);

        return self.sql_sessions.get(session_id) orelse error.NotFound;
    }

    fn takeSqlSession(self: *MindbrainHttpApp, session_id: u64) !*SqlSession {
        self.sql_sessions_mutex.lockUncancelable(self.io);
        defer self.sql_sessions_mutex.unlock(self.io);

        const removed = self.sql_sessions.fetchRemove(session_id) orelse return error.NotFound;
        return removed.value;
    }

    fn executeSql(
        self: *MindbrainHttpApp,
        allocator: std.mem.Allocator,
        db: facet_sqlite.Database,
        sql: []const u8,
        params: []const std.json.Value,
    ) !Response {
        const c = facet_sqlite.c;
        const has_multiple_statements = params.len == 0 and countSqlStatements(sql) > 1;

        if (has_multiple_statements) {
            db.exec(sql) catch |err| {
                return try self.sqliteErrorResponse(allocator, db, .internal_server_error, "exec", err, null);
            };
            return toResponse(
                try helper_api.jsonResponse(allocator, .{
                    .ok = true,
                    .columns = [_][]const u8{},
                    .rows = [_][]const u8{},
                    .changes = c.sqlite3_changes(db.handle),
                    .last_insert_rowid = c.sqlite3_last_insert_rowid(db.handle),
                }),
            );
        }

        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
            return try self.sqliteErrorResponse(allocator, db, .bad_request, "prepare", error.PrepareFailed, null);
        }
        defer _ = c.sqlite3_finalize(stmt.?);

        bindSqlParams(stmt.?, params) catch |err| switch (err) {
            error.BindFailed => return try self.sqliteErrorResponse(allocator, db, .bad_request, "bind", err, null),
            else => return err,
        };

        const column_count: usize = @intCast(c.sqlite3_column_count(stmt.?));
        var columns = std.ArrayList([]const u8).empty;
        defer columns.deinit(allocator);
        try columns.ensureUnusedCapacity(allocator, column_count);
        for (0..column_count) |index| {
            const name_ptr = c.sqlite3_column_name(stmt.?, @intCast(index)) orelse return error.StepFailed;
            try columns.append(allocator, try allocator.dupe(u8, std.mem.span(name_ptr)));
        }

        var rows = std.ArrayList([]std.json.Value).empty;
        defer {
            for (rows.items) |row| {
                allocator.free(row);
            }
            rows.deinit(allocator);
        }

        while (true) {
            const rc = c.sqlite3_step(stmt.?);
            if (rc == c.SQLITE_DONE) break;
            if (rc != c.SQLITE_ROW) {
                return try self.sqliteErrorResponse(allocator, db, .internal_server_error, "step", error.StepFailed, null);
            }

            const row = try allocator.alloc(std.json.Value, column_count);
            errdefer allocator.free(row);
            for (0..column_count) |index| {
                row[index] = try sqlValueToJson(allocator, stmt.?, @intCast(index));
            }
            try rows.append(allocator, row);
        }

        var out: std.Io.Writer.Allocating = .init(allocator);
        defer out.deinit();
        try out.writer.print(
            "{{\"ok\":true,\"columns\":{f},\"rows\":[",
            .{std.json.fmt(columns.items, .{})},
        );
        for (rows.items, 0..) |row, row_index| {
            if (row_index > 0) try out.writer.writeAll(",");
            try out.writer.print("{f}", .{std.json.fmt(row, .{})});
        }
        try out.writer.print(
            "],\"changes\":{},\"last_insert_rowid\":{}}}",
            .{ c.sqlite3_changes(db.handle), c.sqlite3_last_insert_rowid(db.handle) },
        );
        const body = try out.toOwnedSlice();
        return .{
            .status = .ok,
            .content_type = "application/json; charset=utf-8",
            .body = body,
        };
    }

    fn serveConnection(self: *MindbrainHttpApp, connection: std.Io.net.Stream) !void {
        defer connection.close(self.io);

        var send_buffer: [4096]u8 = undefined;
        var recv_buffer: [4096]u8 = undefined;
        var body_buffer: [65536]u8 = undefined;
        var connection_reader = connection.reader(self.io, &recv_buffer);
        var connection_writer = connection.writer(self.io, &send_buffer);
        var server: http.Server = .init(&connection_reader.interface, &connection_writer.interface);

        while (true) {
            var request = server.receiveHead() catch |err| switch (err) {
                error.HttpConnectionClosing => return,
                else => {
                    log.err("failed to receive request: {s}", .{@errorName(err)});
                    return;
                },
            };

            if (request.upgradeRequested() != .none) {
                try request.respond(
                    "websocket upgrades are not supported",
                    .{
                        .version = request.head.version,
                        .status = .not_implemented,
                        .keep_alive = false,
                        .extra_headers = &.{
                            .{ .name = "content-type", .value = "text/plain; charset=utf-8" },
                        },
                    },
                );
                continue;
            }

            var request_arena = std.heap.ArenaAllocator.init(self.allocator);
            defer request_arena.deinit();
            const request_allocator = request_arena.allocator();

            const parsed = parseTarget(request.head.target) catch {
                try self.respondError(request_allocator, &request, .bad_request, "invalid request target");
                continue;
            };

            if (std.mem.eql(u8, parsed.path, "/api/mindbrain/events") or
                std.mem.eql(u8, parsed.path, "/api/events"))
            {
                self.handleEventStream(&request) catch |err| {
                    if (err != error.ConnectionResetByPeer and err != error.BrokenPipe) {
                        log.err("event stream failed: {s}", .{@errorName(err)});
                    }
                };
                return;
            }
            const path_owned = try request_allocator.dupe(u8, parsed.path);
            const query_owned = try request_allocator.dupe(u8, parsed.query);

            const response = self.dispatch(request_allocator, &request, path_owned, query_owned, body_buffer[0..]) catch |err| {
                const status: http.Status = switch (err) {
                    error.BadRequest => .bad_request,
                    error.RequestTooLarge => .bad_request,
                    error.NotFound => .not_found,
                    error.MethodNotAllowed => .method_not_allowed,
                    else => .internal_server_error,
                };
                if (status == .internal_server_error) {
                    log.err("request failed on {s}: {s}", .{ parsed.path, @errorName(err) });
                }
                try self.respondError(request_allocator, &request, status, @errorName(err));
                continue;
            };

            try request.respond(
                response.body,
                .{
                    .version = request.head.version,
                    .status = response.status,
                    .keep_alive = true,
                    .extra_headers = &.{
                        .{ .name = "content-type", .value = response.content_type },
                        .{ .name = "cache-control", .value = "no-store" },
                    },
                },
            );
        }
    }

    fn dispatch(
        self: *MindbrainHttpApp,
        allocator: std.mem.Allocator,
        request: *http.Server.Request,
        path: []const u8,
        query: []const u8,
        body_buffer: []u8,
    ) !Response {
        if (std.mem.eql(u8, path, "/api/mindbrain/facts/write")) {
            if (request.head.method != .POST) return error.MethodNotAllowed;
            return try self.handleFactWrite(allocator, request, body_buffer);
        }

        if (std.mem.eql(u8, path, "/api/mindbrain/sql")) {
            if (request.head.method != .POST) return error.MethodNotAllowed;
            return try self.handleSqlRequest(allocator, request, body_buffer, false);
        }

        if (std.mem.eql(u8, path, "/api/mindbrain/sql/session/open")) {
            if (request.head.method != .POST) return error.MethodNotAllowed;
            return try self.handleSqlSessionOpen(allocator, request, body_buffer);
        }

        if (std.mem.eql(u8, path, "/api/mindbrain/sql/session/close")) {
            if (request.head.method != .POST) return error.MethodNotAllowed;
            return try self.handleSqlSessionClose(allocator, request, body_buffer);
        }

        if (std.mem.eql(u8, path, "/api/mindbrain/sql/session/query")) {
            if (request.head.method != .POST) return error.MethodNotAllowed;
            return try self.handleSqlRequest(allocator, request, body_buffer, true);
        }

        if (request.head.method != .GET and request.head.method != .HEAD) return error.MethodNotAllowed;

        if (std.mem.eql(u8, path, "/health")) {
            return .{
                .status = .ok,
                .content_type = "text/plain; charset=utf-8",
                .body = try allocator.dupe(u8, "ok\n"),
            };
        }

        if (std.mem.eql(u8, path, "/api/mindbrain/simulate")) {
            return self.handleSimulate(allocator);
        }
        if (std.mem.eql(u8, path, "/api/mindbrain/search-compact-info")) {
            return self.handleSearchCompactInfo(allocator);
        }
        if (std.mem.eql(u8, path, "/api/mindbrain/coverage")) {
            return self.handleCoverage(allocator, query, false);
        }
        if (std.mem.eql(u8, path, "/api/mindbrain/coverage-by-domain")) {
            return self.handleCoverage(allocator, query, true);
        }
        if (std.mem.eql(u8, path, "/api/mindbrain/workspace-export")) {
            return self.handleWorkspaceExport(allocator, query, false);
        }
        if (std.mem.eql(u8, path, "/api/mindbrain/workspace-export-by-domain")) {
            return self.handleWorkspaceExport(allocator, query, true);
        }
        if (std.mem.eql(u8, path, "/api/mindbrain/graph-path")) {
            return self.handleGraphPath(allocator, query);
        }
        if (std.mem.eql(u8, path, "/api/mindbrain/graph/subgraph")) {
            return try self.handleGraphSubgraph(allocator, query);
        }
        if (std.mem.eql(u8, path, "/api/mindbrain/traverse")) {
            return self.handleTraverse(allocator, query);
        }
        if (std.mem.eql(u8, path, "/api/mindbrain/pack")) {
            return self.handlePack(allocator, query);
        }
        if (std.mem.eql(u8, path, "/api/mindbrain/ghostcrab/pack-projections")) {
            return self.handleGhostcrabPackProjections(allocator, query);
        }
        if (std.mem.eql(u8, path, "/api/mindbrain/ghostcrab/projection-get")) {
            return self.handleGhostcrabProjectionGet(allocator, query);
        }
        if (std.mem.eql(u8, path, "/api/mindbrain/ghostcrab/graph-search")) {
            return self.handleGhostcrabGraphSearch(allocator, query);
        }

        return self.handleStatic(allocator, path);
    }

    fn handleFactWrite(
        self: *MindbrainHttpApp,
        allocator: std.mem.Allocator,
        request: *http.Server.Request,
        body_buffer: []u8,
    ) !Response {
        const write_request = try self.parseFactWriteRequest(allocator, request, body_buffer);
        const schema_id = write_request.schema_id orelse return error.BadRequest;
        const content = write_request.content orelse return error.BadRequest;
        if (schema_id.len == 0 or content.len == 0) return error.BadRequest;

        const workspace_id = write_request.workspace_id orelse "default";
        if (workspace_id.len == 0) return error.BadRequest;

        const facets_json = write_request.facets_json orelse "{}";
        facts_sqlite.validateFacetsJsonObject(allocator, facets_json) catch return error.BadRequest;

        const source_ref = normalizeOptionalText(write_request.source_ref);
        var db = try self.openDb();
        defer db.close();

        const result = facts_sqlite.writeFact(db, allocator, .{
            .id = write_request.id,
            .workspace_id = workspace_id,
            .schema_id = schema_id,
            .content = content,
            .facets_json = facets_json,
            .embedding_blob = write_request.embedding_blob,
            .created_by = write_request.created_by,
            .valid_from_unix = write_request.valid_from_unix,
            .valid_until_unix = write_request.valid_until_unix,
            .source_ref = source_ref,
        }) catch |err| switch (err) {
            error.InvalidFactWrite, error.InvalidFacetsJson => return error.BadRequest,
            error.PrepareFailed, error.StepFailed, error.BindFailed, error.ExecFailed => {
                return try self.sqliteErrorResponse(allocator, db, .internal_server_error, "facts.write", err, null);
            },
            else => return err,
        };
        defer facts_sqlite.deinitFactWriteResult(allocator, result);

        return toResponse(
            try helper_api.jsonResponse(allocator, .{
                .ok = true,
                .id = result.id,
                .doc_id = result.doc_id,
                .created = result.created,
                .updated = result.updated,
            }),
        );
    }

    fn parseFactWriteRequest(
        self: *MindbrainHttpApp,
        allocator: std.mem.Allocator,
        request: *http.Server.Request,
        body_buffer: []u8,
    ) !FactWriteRequest {
        const content_length = request.head.content_length orelse return error.BadRequest;
        if (content_length == 0) return error.BadRequest;
        if (content_length > self.max_body_bytes) return error.RequestTooLarge;

        const reader = request.readerExpectNone(body_buffer);
        const body = try reader.readAlloc(allocator, @intCast(content_length));
        return try std.json.parseFromSliceLeaky(
            FactWriteRequest,
            allocator,
            body,
            .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = false,
            },
        );
    }

    fn handleSimulate(self: *MindbrainHttpApp, allocator: std.mem.Allocator) !Response {
        var db = try self.openDb();
        defer db.close();

        var queue = queue_sqlite.QueueStore.init(db, allocator);
        const event = .{
            .type = "simulation",
            .event = "manual-trigger",
            .stream = "demo_firehose",
            .source = "mindbrain-http",
            .ts_unix = unixTimestamp(),
            .graph_entities = try self.countRows(db, "graph_entity"),
            .graph_relations = try self.countRows(db, "graph_relation"),
            .search_documents = try self.countRows(db, "search_documents"),
            .registered_queues = try self.countRows(db, "queue_registry"),
        };
        const msg_id = try queue.sendJson("demo_firehose", event);

        const summary = .{
            .ok = true,
            .event = event.event,
            .stream = event.stream,
            .msg_id = msg_id,
            .ts_unix = event.ts_unix,
            .graph_entities = event.graph_entities,
            .graph_relations = event.graph_relations,
            .search_documents = event.search_documents,
            .registered_queues = event.registered_queues,
        };
        var out: std.Io.Writer.Allocating = .init(allocator);
        defer out.deinit();
        try out.writer.print("{f}", .{std.json.fmt(summary, .{})});
        const body = try out.toOwnedSlice();
        return .{
            .status = .ok,
            .content_type = "application/json; charset=utf-8",
            .body = body,
        };
    }

    fn handleGhostcrabProjectionGet(self: *MindbrainHttpApp, allocator: std.mem.Allocator, query: []const u8) !Response {
        var db = try self.openDb();
        defer db.close();

        const workspace_id = (try queryValue(allocator, query, "workspace_id")) orelse return error.BadRequest;
        const projection_id = (try queryValue(allocator, query, "projection_id")) orelse return error.BadRequest;
        const collection_id = normalizeOptionalQueryValue(try queryValue(allocator, query, "collection_id"));
        const include_evidence = parseBoolQuery((try queryValue(allocator, query, "include_evidence")) orelse "false");
        const include_deltas = parseBoolQuery((try queryValue(allocator, query, "include_deltas")) orelse "false");

        const projection_results = try loadGhostcrabProjectionEntities(allocator, db, workspace_id, collection_id, projection_id, "ProjectionResult", "projection_id");
        defer deinitGhostcrabProjectionEntities(allocator, projection_results);

        const linked_evidence = if (include_evidence)
            try loadGhostcrabProjectionEvidence(allocator, db, workspace_id, collection_id, projection_id)
        else
            try allocator.alloc(GhostcrabProjectionEvidenceRow, 0);
        defer deinitGhostcrabProjectionEvidence(allocator, linked_evidence);

        const deltas = if (include_deltas)
            try loadGhostcrabProjectionEntities(allocator, db, workspace_id, collection_id, projection_id, "DeltaFinding", "metric")
        else
            try allocator.alloc(GhostcrabProjectionEntityRow, 0);
        defer deinitGhostcrabProjectionEntities(allocator, deltas);

        const report = .{
            .workspace_id = workspace_id,
            .collection_id = collection_id,
            .projection_id = projection_id,
            .projection_result_count = projection_results.len,
            .linked_evidence_count = linked_evidence.len,
            .delta_count = deltas.len,
            .has_projection = projection_results.len > 0,
        };
        const payload = .{
            .workspace_id = workspace_id,
            .collection_id = collection_id,
            .projection_id = projection_id,
            .projection_results = projection_results,
            .linked_evidence = linked_evidence,
            .deltas = deltas,
            .report = report,
        };

        var out: std.Io.Writer.Allocating = .init(allocator);
        defer out.deinit();
        try out.writer.print("{f}", .{std.json.fmt(payload, .{})});
        return .{
            .status = .ok,
            .content_type = "application/json; charset=utf-8",
            .body = try out.toOwnedSlice(),
        };
    }

    fn handleGhostcrabGraphSearch(self: *MindbrainHttpApp, allocator: std.mem.Allocator, query: []const u8) !Response {
        var db = try self.openDb();
        defer db.close();

        const workspace_id = (try queryValue(allocator, query, "workspace_id")) orelse return error.BadRequest;
        const query_text = (try queryValue(allocator, query, "query")) orelse "";
        const collection_id = normalizeOptionalQueryValue(try queryValue(allocator, query, "collection_id"));
        const metadata_filters = normalizeOptionalQueryValue(try queryValue(allocator, query, "metadata_filters"));
        const entity_types = try queryValues(allocator, query, "entity_type");
        const limit = if (try queryValue(allocator, query, "limit")) |value|
            try std.fmt.parseInt(usize, value, 10)
        else
            20;

        const rows = try loadGhostcrabGraphSearchEntities(
            allocator,
            db,
            workspace_id,
            collection_id,
            query_text,
            entity_types,
            metadata_filters,
            @min(limit, 100),
        );
        defer deinitGhostcrabGraphSearchRows(allocator, rows);

        const payload = .{
            .workspace_id = workspace_id,
            .collection_id = collection_id,
            .query = query_text,
            .entity_types = entity_types,
            .returned = rows.len,
            .rows = rows,
            .searched_layers = [_][]const u8{"graph_entity"},
        };

        var out: std.Io.Writer.Allocating = .init(allocator);
        defer out.deinit();
        try out.writer.print("{f}", .{std.json.fmt(payload, .{})});
        return .{
            .status = .ok,
            .content_type = "application/json; charset=utf-8",
            .body = try out.toOwnedSlice(),
        };
    }

    fn handleEventStream(self: *MindbrainHttpApp, request: *http.Server.Request) !void {
        var db = try self.openDb();
        defer db.close();

        var queue = queue_sqlite.QueueStore.init(db, self.allocator);
        var send_buffer: [8192]u8 = undefined;
        var response = try request.respondStreaming(&send_buffer, .{
            .respond_options = .{
                .version = request.head.version,
                .status = .ok,
                .keep_alive = false,
                .extra_headers = &.{
                    .{ .name = "content-type", .value = "text/event-stream; charset=utf-8" },
                    .{ .name = "cache-control", .value = "no-cache" },
                    .{ .name = "x-accel-buffering", .value = "no" },
                },
            },
        });

        try response.writer.writeAll("retry: 1000\n");
        try response.flush();

        var last_heartbeat_ms: i64 = milliTimestamp();
        while (true) {
            const messages = try queue.read("demo_firehose", 5, 15);
            defer {
                for (messages) |message| self.allocator.free(message.message);
                self.allocator.free(messages);
            }

            if (messages.len == 0) {
                const now = milliTimestamp();
                if (now - last_heartbeat_ms >= 15_000) {
                    response.writer.writeAll(": heartbeat\n\n") catch return;
                    response.flush() catch return;
                    last_heartbeat_ms = now;
                }
                std.Io.Clock.Duration.sleep(.{ .raw = .fromMilliseconds(500), .clock = .awake }, self.io) catch return;
                continue;
            }

            for (messages) |message| {
                response.writer.print("data: {s}\n\n", .{message.message}) catch return;
                _ = queue.archive("demo_firehose", message.msg_id) catch {};
            }
            response.flush() catch return;
        }
    }

    fn handleSearchCompactInfo(self: *MindbrainHttpApp, allocator: std.mem.Allocator) !Response {
        var db = try self.openDb();
        defer db.close();

        const body = try search_sqlite.compactSearchSnapshotToon(db, allocator);
        return .{
            .status = .ok,
            .content_type = "text/plain; charset=utf-8",
            .body = body,
        };
    }

    fn handleCoverage(self: *MindbrainHttpApp, allocator: std.mem.Allocator, query: []const u8, by_domain: bool) !Response {
        var db = try self.openDb();
        defer db.close();

        const id = if (by_domain) blk: {
            break :blk (try queryValue(allocator, query, "domain_or_workspace")) orelse return error.BadRequest;
        } else blk: {
            break :blk (try queryValue(allocator, query, "workspace_id")) orelse return error.BadRequest;
        };

        var resolved_workspace_id: ?[]const u8 = null;
        defer if (resolved_workspace_id) |value| allocator.free(value);

        const workspace_id = if (by_domain) blk: {
            resolved_workspace_id = try ontology_sqlite.resolveWorkspace(db, allocator, id);
            if (resolved_workspace_id == null) return error.NotFound;
            break :blk resolved_workspace_id.?;
        } else id;

        const entity_types = try queryValues(allocator, query, "entity_type");
        defer allocator.free(entity_types);

        const report = try ontology_sqlite.coverageReport(
            db,
            allocator,
            workspace_id,
            if (entity_types.len > 0) entity_types else null,
        );
        defer {
            allocator.free(report.summary.workspace_id);
            for (report.gaps) |gap| {
                allocator.free(gap.id);
                allocator.free(gap.label);
                allocator.free(gap.entity_type);
                allocator.free(gap.criticality);
            }
            allocator.free(report.gaps);
        }

        const body = try toon_exports.encodeCoverageReportAlloc(allocator, report, toon_exports.default_options);
        return .{
            .status = .ok,
            .content_type = "text/plain; charset=utf-8",
            .body = body,
        };
    }

    fn handleWorkspaceExport(self: *MindbrainHttpApp, allocator: std.mem.Allocator, query: []const u8, by_domain: bool) !Response {
        var db = try self.openDb();
        defer db.close();

        const workspace_or_domain = if (by_domain) blk: {
            break :blk (try queryValue(allocator, query, "domain_or_workspace")) orelse return error.BadRequest;
        } else blk: {
            break :blk (try queryValue(allocator, query, "workspace_id")) orelse return error.BadRequest;
        };

        const body = if (by_domain) blk: {
            const maybe = try workspace_sqlite.exportWorkspaceModelToonByDomain(db, allocator, workspace_or_domain);
            if (maybe == null) return error.NotFound;
            break :blk maybe.?;
        } else try workspace_sqlite.exportWorkspaceModelToon(db, allocator, workspace_or_domain);

        return .{
            .status = .ok,
            .content_type = "text/plain; charset=utf-8",
            .body = body,
        };
    }

    fn handleGraphPath(self: *MindbrainHttpApp, allocator: std.mem.Allocator, query: []const u8) !Response {
        var db = try self.openDb();
        defer db.close();

        const source = (try queryValue(allocator, query, "source")) orelse return error.BadRequest;
        const target = (try queryValue(allocator, query, "target")) orelse return error.BadRequest;
        const max_depth = if (try queryValue(allocator, query, "max_depth")) |value|
            try std.fmt.parseInt(usize, value, 10)
        else
            4;
        const edge_labels = try queryValues(allocator, query, "edge_label");
        defer allocator.free(edge_labels);

        const body = try graph_sqlite.shortestPathToon(
            db,
            allocator,
            source,
            target,
            if (edge_labels.len > 0) edge_labels else null,
            max_depth,
        );
        return .{
            .status = .ok,
            .content_type = "text/plain; charset=utf-8",
            .body = body,
        };
    }

    fn handleGraphSubgraph(self: *MindbrainHttpApp, allocator: std.mem.Allocator, query: []const u8) !Response {
        var db = try self.openDb();
        defer db.close();

        const seed_ids_text = (try queryValue(allocator, query, "seed_ids")) orelse return error.BadRequest;
        defer allocator.free(seed_ids_text);
        const seed_ids = try parseCsvU32List(allocator, seed_ids_text);
        defer allocator.free(seed_ids);
        if (seed_ids.len == 0) return error.BadRequest;

        const hops = if (try queryValue(allocator, query, "hops")) |value|
            try std.fmt.parseInt(usize, value, 10)
        else
            2;

        const edge_types_text = try queryValue(allocator, query, "edge_types");
        defer if (edge_types_text) |value| allocator.free(value);
        var edge_types_owned: ?[]const []const u8 = null;
        defer if (edge_types_owned) |value| allocator.free(value);
        if (edge_types_text) |value| {
            edge_types_owned = try parseCsvStringList(allocator, value);
        }

        const events = try graph_sqlite.streamSubgraph(
            db,
            allocator,
            seed_ids,
            hops,
            edge_types_owned,
            null,
            null,
            null,
            null,
        );
        defer {
            for (events) |*event| event.deinit(allocator);
            allocator.free(events);
        }

        const body = try encodeGraphSubgraphSseBody(allocator, events);
        return .{
            .status = .ok,
            .content_type = "text/event-stream; charset=utf-8",
            .body = body,
        };
    }

    fn handleTraverse(self: *MindbrainHttpApp, allocator: std.mem.Allocator, query: []const u8) !Response {
        var db = try self.openDb();
        defer db.close();

        const start = (try queryValue(allocator, query, "start")) orelse return error.BadRequest;
        const direction = if (try queryValue(allocator, query, "direction")) |value|
            parseDirection(value) orelse return error.BadRequest
        else
            graph_sqlite.TraverseDirection.outbound;
        const depth = if (try queryValue(allocator, query, "depth")) |value|
            try std.fmt.parseInt(usize, value, 10)
        else
            3;
        const target = try queryValue(allocator, query, "target");
        const edge_labels = try queryValues(allocator, query, "edge_label");
        defer allocator.free(edge_labels);

        var result = try graph_sqlite.traverse(
            db,
            allocator,
            start,
            direction,
            if (edge_labels.len > 0) edge_labels else null,
            depth,
            target,
        );
        defer result.deinit(allocator);

        const payload = .{
            .target_found = result.target_found,
            .rows = result.rows,
        };
        var out: std.Io.Writer.Allocating = .init(allocator);
        defer out.deinit();
        try out.writer.print("{f}", .{std.json.fmt(payload, .{})});
        const body = try out.toOwnedSlice();
        return .{
            .status = .ok,
            .content_type = "application/json; charset=utf-8",
            .body = body,
        };
    }

    fn handlePack(self: *MindbrainHttpApp, allocator: std.mem.Allocator, query: []const u8) !Response {
        var db = try self.openDb();
        defer db.close();

        const user_id = (try queryValue(allocator, query, "user_id")) orelse return error.BadRequest;
        const query_text = (try queryValue(allocator, query, "query")) orelse return error.BadRequest;
        const scope = try queryValue(allocator, query, "scope");
        const limit = if (try queryValue(allocator, query, "limit")) |value|
            try std.fmt.parseInt(usize, value, 10)
        else
            15;

        const rows = try pragma_sqlite.packContextScoped(db, allocator, user_id, query_text, scope, limit);
        defer {
            for (rows) |row| pragma_sqlite.deinitPackedRow(allocator, row);
            allocator.free(rows);
        }

        const body = try toon_exports.encodePackContextAlloc(
            allocator,
            user_id,
            query_text,
            scope,
            rows,
            toon_exports.default_options,
        );
        return .{
            .status = .ok,
            .content_type = "text/plain; charset=utf-8",
            .body = body,
        };
    }

    fn handleGhostcrabPackProjections(self: *MindbrainHttpApp, allocator: std.mem.Allocator, query: []const u8) !Response {
        var db = try self.openDb();
        defer db.close();

        const agent_id = (try queryValue(allocator, query, "agent_id")) orelse return error.BadRequest;
        const query_text = (try queryValue(allocator, query, "query")) orelse return error.BadRequest;
        const scope = try queryValue(allocator, query, "scope");
        const limit = if (try queryValue(allocator, query, "limit")) |value|
            try std.fmt.parseInt(usize, value, 10)
        else
            15;

        const rows = try ontology_sqlite.materializePackProjections(
            db,
            allocator,
            agent_id,
            scope,
            query_text,
            limit,
        );
        defer ontology_sqlite.deinitProjectionRows(allocator, rows);

        const ResponseRow = struct {
            id: []const u8,
            proj_type: []const u8,
            content: []const u8,
            weight: f32,
            source_ref: ?[]const u8,
            status: []const u8,
        };
        const response_rows = try allocator.alloc(ResponseRow, rows.len);
        defer allocator.free(response_rows);
        for (rows, 0..) |row, index| {
            response_rows[index] = .{
                .id = row.id,
                .proj_type = row.proj_type,
                .content = row.content,
                .weight = row.weight,
                .source_ref = row.source_ref,
                .status = row.status,
            };
        }

        const payload = .{
            .agent_id = agent_id,
            .query = query_text,
            .scope = scope,
            .rows = response_rows,
        };
        var out: std.Io.Writer.Allocating = .init(allocator);
        defer out.deinit();
        try out.writer.print("{f}", .{std.json.fmt(payload, .{})});
        const body = try out.toOwnedSlice();
        return .{
            .status = .ok,
            .content_type = "application/json; charset=utf-8",
            .body = body,
        };
    }

    fn sqlValueToJson(
        allocator: std.mem.Allocator,
        stmt: *facet_sqlite.c.sqlite3_stmt,
        index: c_int,
    ) !std.json.Value {
        const c = facet_sqlite.c;
        switch (c.sqlite3_column_type(stmt, index)) {
            c.SQLITE_INTEGER => return .{ .integer = c.sqlite3_column_int64(stmt, index) },
            c.SQLITE_FLOAT => return .{ .float = c.sqlite3_column_double(stmt, index) },
            c.SQLITE_TEXT => return .{ .string = try helper_api.dupeColText(allocator, stmt, index) },
            c.SQLITE_BLOB => {
                const len: usize = @intCast(c.sqlite3_column_bytes(stmt, index));
                const ptr = c.sqlite3_column_blob(stmt, index) orelse return .null;
                const bytes: []const u8 = @as([*]const u8, @ptrCast(ptr))[0..len];
                const hex_chars = "0123456789abcdef";
                const out = try allocator.alloc(u8, 2 + bytes.len * 2);
                out[0] = '0';
                out[1] = 'x';
                for (bytes, 0..) |byte, i| {
                    out[2 + i * 2] = hex_chars[byte >> 4];
                    out[3 + i * 2] = hex_chars[byte & 0x0f];
                }
                return .{ .string = out };
            },
            else => return .null,
        }
    }

    fn bindSqlParams(stmt: *facet_sqlite.c.sqlite3_stmt, params: []const std.json.Value) !void {
        const c = facet_sqlite.c;
        for (params, 0..) |param, idx| {
            const bind_index: c_int = @intCast(idx + 1);
            switch (param) {
                .null => if (c.sqlite3_bind_null(stmt, bind_index) != c.SQLITE_OK) return error.BindFailed,
                .bool => |value| if (c.sqlite3_bind_int64(stmt, bind_index, if (value) 1 else 0) != c.SQLITE_OK) return error.BindFailed,
                .integer => |value| if (c.sqlite3_bind_int64(stmt, bind_index, value) != c.SQLITE_OK) return error.BindFailed,
                .float => |value| if (c.sqlite3_bind_double(stmt, bind_index, value) != c.SQLITE_OK) return error.BindFailed,
                .number_string => |value| {
                    const parsed = std.fmt.parseFloat(f64, value) catch return error.BindFailed;
                    if (c.sqlite3_bind_double(stmt, bind_index, parsed) != c.SQLITE_OK) return error.BindFailed;
                },
                .string => |value| {
                    if (c.sqlite3_bind_text(stmt, bind_index, value.ptr, @intCast(value.len), facet_sqlite.sqliteTransient()) != c.SQLITE_OK) {
                        return error.BindFailed;
                    }
                },
                .array, .object => {
                    var json_buf: std.Io.Writer.Allocating = .init(std.heap.page_allocator);
                    defer json_buf.deinit();
                    try json_buf.writer.print("{f}", .{std.json.fmt(param, .{})});
                    const json_text = try json_buf.toOwnedSlice();
                    defer std.heap.page_allocator.free(json_text);
                    if (c.sqlite3_bind_text(stmt, bind_index, json_text.ptr, @intCast(json_text.len), facet_sqlite.sqliteTransient()) != c.SQLITE_OK) {
                        return error.BindFailed;
                    }
                },
            }
        }
    }

    fn countSqlStatements(sql: []const u8) usize {
        var count: usize = 0;
        var i: usize = 0;
        while (i < sql.len) {
            while (i < sql.len and std.ascii.isWhitespace(sql[i])) : (i += 1) {}
            if (i >= sql.len) break;
            count += 1;
            var in_single = false;
            var in_double = false;
            while (i < sql.len) : (i += 1) {
                const ch = sql[i];
                if (ch == '\'' and !in_double) {
                    in_single = !in_single;
                } else if (ch == '"' and !in_single) {
                    in_double = !in_double;
                } else if (ch == ';' and !in_single and !in_double) {
                    i += 1;
                    break;
                }
            }
        }
        return count;
    }

    fn toResponse(api_response: helper_api.ApiResponse) Response {
        return .{
            .status = api_response.status,
            .content_type = "application/json; charset=utf-8",
            .body = api_response.body,
        };
    }

    fn openDb(self: *MindbrainHttpApp) !facet_sqlite.Database {
        return try facet_sqlite.Database.open(self.db_path);
    }

    fn handleStatic(self: *MindbrainHttpApp, allocator: std.mem.Allocator, path: []const u8) !Response {
        if (self.static_dir.len == 0) return error.NotFound;

        const rel_path = self.resolveStaticPath(path) orelse return error.NotFound;
        const full_path = try std.fs.path.join(allocator, &.{ self.static_dir, rel_path });
        defer allocator.free(full_path);

        const body = std.Io.Dir.cwd().readFileAlloc(self.io, full_path, allocator, .limited(16 * 1024 * 1024)) catch |err| switch (err) {
            error.FileNotFound => return error.NotFound,
            else => return err,
        };
        return .{
            .status = .ok,
            .content_type = mimeTypeForPath(rel_path),
            .body = body,
        };
    }

    fn resolveStaticPath(self: *MindbrainHttpApp, path: []const u8) ?[]const u8 {
        _ = self;
        if (std.mem.containsAtLeast(u8, path, 1, "..")) return null;
        if (std.mem.eql(u8, path, "/")) return "index.html";
        if (path.len > 0 and path[0] == '/') {
            const rel = path[1..];
            if (rel.len == 0) return "index.html";
            if (std.mem.lastIndexOfScalar(u8, rel, '/')) |idx| {
                const leaf = rel[idx + 1 ..];
                if (std.mem.indexOfScalar(u8, leaf, '.') == null) return "index.html";
            } else if (std.mem.indexOfScalar(u8, rel, '.') == null) {
                return "index.html";
            }
            return rel;
        }
        return null;
    }

    fn respondError(_: *MindbrainHttpApp, allocator: std.mem.Allocator, request: *http.Server.Request, status: http.Status, message: []const u8) !void {
        var out: std.Io.Writer.Allocating = .init(allocator);
        defer out.deinit();
        try out.writer.print("{f}", .{std.json.fmt(.{ .@"error" = message }, .{})});
        const payload = try out.toOwnedSlice();
        try request.respond(
            payload,
            .{
                .version = request.head.version,
                .status = status,
                .keep_alive = false,
                .extra_headers = &.{
                    .{ .name = "content-type", .value = "application/json; charset=utf-8" },
                    .{ .name = "cache-control", .value = "no-store" },
                },
            },
        );
    }

    fn sqliteErrorResponse(
        self: *MindbrainHttpApp,
        allocator: std.mem.Allocator,
        db: facet_sqlite.Database,
        status: http.Status,
        operation: []const u8,
        err: anyerror,
        rollback_err: ?anyerror,
    ) !Response {
        _ = self;
        const rc = facet_sqlite.c.sqlite3_errcode(db.handle);
        const extended_rc = facet_sqlite.c.sqlite3_extended_errcode(db.handle);
        const message = std.mem.span(facet_sqlite.c.sqlite3_errmsg(db.handle));
        var out: std.Io.Writer.Allocating = .init(allocator);
        defer out.deinit();
        try out.writer.print(
            "{{\"ok\":false,\"error\":{{\"kind\":{f},\"operation\":{f},\"sqlite_code\":{},\"sqlite_extended_code\":{},\"sqlite_message\":{f}",
            .{
                std.json.fmt(@errorName(err), .{}),
                std.json.fmt(operation, .{}),
                rc,
                extended_rc,
                std.json.fmt(message, .{}),
            },
        );
        if (rollback_err) |rollback| {
            try out.writer.print(",\"rollback_error\":{f}", .{std.json.fmt(@errorName(rollback), .{})});
        }
        try out.writer.writeAll("}}");
        return .{
            .status = status,
            .content_type = "application/json; charset=utf-8",
            .body = try out.toOwnedSlice(),
        };
    }
};

const Response = struct {
    status: http.Status,
    content_type: []const u8,
    body: []const u8,

    fn deinit(self: Response, allocator: std.mem.Allocator) void {
        _ = self;
        _ = allocator;
    }
};

const GhostcrabProjectionEntityRow = struct {
    entity_id: u32,
    entity_type: []const u8,
    name: []const u8,
    confidence: f32,
    metadata_json: []const u8,
};

const GhostcrabProjectionEvidenceRow = struct {
    relation_id: u32,
    relation_type: []const u8,
    source_id: u32,
    target_id: u32,
    relation_metadata_json: []const u8,
    evidence_entity_id: u32,
    evidence_entity_type: []const u8,
    evidence_name: []const u8,
    evidence_confidence: f32,
    evidence_metadata_json: []const u8,
};

const GhostcrabGraphSearchRow = struct {
    entity_id: u32,
    entity_type: []const u8,
    name: []const u8,
    confidence: f32,
    metadata_json: []const u8,
    score: f32,
};

fn parseBoolQuery(value: []const u8) bool {
    return std.mem.eql(u8, value, "1") or
        std.ascii.eqlIgnoreCase(value, "true") or
        std.ascii.eqlIgnoreCase(value, "yes");
}

fn normalizeOptionalQueryValue(value: ?[]const u8) ?[]const u8 {
    const text = value orelse return null;
    if (text.len == 0) return null;
    if (std.ascii.eqlIgnoreCase(text, "null")) return null;
    if (std.ascii.eqlIgnoreCase(text, "nil")) return null;
    return text;
}

fn normalizeOptionalText(value: ?[]const u8) ?[]const u8 {
    const text = value orelse return null;
    if (text.len == 0) return null;
    return text;
}

fn bindOptionalText(stmt: *facet_sqlite.c.sqlite3_stmt, index: c_int, value: ?[]const u8) !void {
    if (value) |text| {
        try facet_sqlite.bindText(stmt, index, text);
    } else {
        try facet_sqlite.bindNull(stmt, index);
    }
}

fn loadGhostcrabGraphSearchEntities(
    allocator: std.mem.Allocator,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    collection_id: ?[]const u8,
    query_text: []const u8,
    entity_types: []const []const u8,
    metadata_filters: ?[]const u8,
    limit: usize,
) ![]GhostcrabGraphSearchRow {
    const sql =
        \\SELECT entity_id, entity_type, name, confidence, metadata_json
        \\FROM graph_entity e
        \\WHERE workspace_id = ?1
        \\  AND deprecated_at IS NULL
        \\  AND (?2 IS NULL OR json_extract(metadata_json, '$.collection_id') = ?2)
        \\  AND (?3 IS NULL OR NOT EXISTS (
        \\    SELECT 1
        \\    FROM json_each(?3) f
        \\    WHERE json_extract(e.metadata_json, '$.' || f.key) IS NOT f.value
        \\       OR json_extract(e.metadata_json, '$.' || f.key) IS NULL
        \\  ))
        \\ORDER BY confidence DESC, entity_id ASC
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);

    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try bindOptionalText(stmt, 2, collection_id);
    try bindOptionalText(stmt, 3, metadata_filters);

    const query_terms = try splitSimpleTerms(allocator, query_text);
    defer freeStringSlice(allocator, query_terms);

    var rows = std.ArrayList(GhostcrabGraphSearchRow).empty;
    errdefer {
        for (rows.items) |row| {
            allocator.free(row.entity_type);
            allocator.free(row.name);
            allocator.free(row.metadata_json);
        }
        rows.deinit(allocator);
    }

    const c = facet_sqlite.c;
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const entity_type = try helper_api.dupeColText(allocator, stmt, 1);
        if (!matchesAnyString(entity_type, entity_types)) {
            allocator.free(entity_type);
            continue;
        }

        const name = try helper_api.dupeColText(allocator, stmt, 2);
        const metadata_json = try helper_api.dupeColText(allocator, stmt, 4);
        const score = graphSearchScore(query_terms, entity_type, name, metadata_json);
        if (query_terms.len > 0 and score <= 0) {
            allocator.free(entity_type);
            allocator.free(name);
            allocator.free(metadata_json);
            continue;
        }

        try rows.append(allocator, .{
            .entity_id = @intCast(c.sqlite3_column_int64(stmt, 0)),
            .entity_type = entity_type,
            .name = name,
            .confidence = @floatCast(c.sqlite3_column_double(stmt, 3)),
            .metadata_json = metadata_json,
            .score = if (query_terms.len == 0) @as(f32, @floatCast(c.sqlite3_column_double(stmt, 3))) else score,
        });
    }

    std.mem.sort(GhostcrabGraphSearchRow, rows.items, {}, struct {
        fn lessThan(_: void, lhs: GhostcrabGraphSearchRow, rhs: GhostcrabGraphSearchRow) bool {
            if (lhs.score != rhs.score) return lhs.score > rhs.score;
            if (lhs.confidence != rhs.confidence) return lhs.confidence > rhs.confidence;
            return lhs.entity_id < rhs.entity_id;
        }
    }.lessThan);

    if (rows.items.len > limit) {
        for (rows.items[limit..]) |row| {
            allocator.free(row.entity_type);
            allocator.free(row.name);
            allocator.free(row.metadata_json);
        }
        rows.shrinkRetainingCapacity(limit);
    }

    return rows.toOwnedSlice(allocator);
}

fn deinitGhostcrabGraphSearchRows(allocator: std.mem.Allocator, rows: []GhostcrabGraphSearchRow) void {
    for (rows) |row| {
        allocator.free(row.entity_type);
        allocator.free(row.name);
        allocator.free(row.metadata_json);
    }
    allocator.free(rows);
}

fn splitSimpleTerms(allocator: std.mem.Allocator, query_text: []const u8) ![]const []const u8 {
    var terms = std.ArrayList([]const u8).empty;
    errdefer freeStringSlice(allocator, terms.items);

    var it = std.mem.tokenizeAny(u8, query_text, " \t\r\n");
    while (it.next()) |term| {
        try terms.append(allocator, try allocator.dupe(u8, term));
    }

    return terms.toOwnedSlice(allocator);
}

fn freeStringSlice(allocator: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| allocator.free(value);
    allocator.free(values);
}

fn matchesAnyString(value: []const u8, candidates: []const []const u8) bool {
    if (candidates.len == 0) return true;
    for (candidates) |candidate| {
        if (std.mem.eql(u8, value, candidate)) return true;
    }
    return false;
}

fn graphSearchScore(terms: []const []const u8, entity_type: []const u8, name: []const u8, metadata_json: []const u8) f32 {
    var score: f32 = 0;
    for (terms) |term| {
        if (containsIgnoreCase(name, term)) score += 4;
        if (containsIgnoreCase(entity_type, term)) score += 3;
        if (containsIgnoreCase(metadata_json, term)) score += 1;
    }
    return score;
}

fn containsIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0) return true;
    if (needle.len > haystack.len) return false;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        if (std.ascii.eqlIgnoreCase(haystack[i .. i + needle.len], needle)) return true;
    }
    return false;
}

fn loadGhostcrabProjectionEntities(
    allocator: std.mem.Allocator,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    collection_id: ?[]const u8,
    projection_id: []const u8,
    entity_type: []const u8,
    metadata_key: []const u8,
) ![]GhostcrabProjectionEntityRow {
    const sql =
        \\SELECT entity_id, entity_type, name, confidence, metadata_json
        \\FROM graph_entity
        \\WHERE workspace_id = ?1
        \\  AND entity_type = ?2
        \\  AND json_extract(metadata_json, '$.' || ?3) = ?4
        \\  AND (?5 IS NULL OR json_extract(metadata_json, '$.collection_id') = ?5)
        \\  AND deprecated_at IS NULL
        \\ORDER BY confidence DESC, entity_id ASC
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);

    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, entity_type);
    try facet_sqlite.bindText(stmt, 3, metadata_key);
    try facet_sqlite.bindText(stmt, 4, projection_id);
    try bindOptionalText(stmt, 5, collection_id);

    var rows = std.ArrayList(GhostcrabProjectionEntityRow).empty;
    errdefer {
        for (rows.items) |row| {
            allocator.free(row.entity_type);
            allocator.free(row.name);
            allocator.free(row.metadata_json);
        }
        rows.deinit(allocator);
    }

    const c = facet_sqlite.c;
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(allocator, .{
            .entity_id = @intCast(c.sqlite3_column_int64(stmt, 0)),
            .entity_type = try helper_api.dupeColText(allocator, stmt, 1),
            .name = try helper_api.dupeColText(allocator, stmt, 2),
            .confidence = @floatCast(c.sqlite3_column_double(stmt, 3)),
            .metadata_json = try helper_api.dupeColText(allocator, stmt, 4),
        });
    }

    return rows.toOwnedSlice(allocator);
}

fn deinitGhostcrabProjectionEntities(allocator: std.mem.Allocator, rows: []GhostcrabProjectionEntityRow) void {
    for (rows) |row| {
        allocator.free(row.entity_type);
        allocator.free(row.name);
        allocator.free(row.metadata_json);
    }
    allocator.free(rows);
}

fn loadGhostcrabProjectionEvidence(
    allocator: std.mem.Allocator,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    collection_id: ?[]const u8,
    projection_id: []const u8,
) ![]GhostcrabProjectionEvidenceRow {
    const sql =
        \\SELECT
        \\  r.relation_id,
        \\  r.relation_type,
        \\  r.source_id,
        \\  r.target_id,
        \\  r.metadata_json,
        \\  e.entity_id,
        \\  e.entity_type,
        \\  e.name,
        \\  e.confidence,
        \\  e.metadata_json
        \\FROM graph_entity p
        \\JOIN graph_relation r ON r.source_id = p.entity_id
        \\JOIN graph_entity e ON e.entity_id = r.target_id
        \\WHERE p.workspace_id = ?1
        \\  AND p.entity_type = 'ProjectionResult'
        \\  AND json_extract(p.metadata_json, '$.projection_id') = ?2
        \\  AND (?3 IS NULL OR json_extract(p.metadata_json, '$.collection_id') = ?3)
        \\  AND p.deprecated_at IS NULL
        \\  AND r.deprecated_at IS NULL
        \\  AND e.deprecated_at IS NULL
        \\ORDER BY r.relation_id ASC, e.entity_id ASC
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);

    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, projection_id);
    try bindOptionalText(stmt, 3, collection_id);

    var rows = std.ArrayList(GhostcrabProjectionEvidenceRow).empty;
    errdefer {
        for (rows.items) |row| {
            allocator.free(row.relation_type);
            allocator.free(row.relation_metadata_json);
            allocator.free(row.evidence_entity_type);
            allocator.free(row.evidence_name);
            allocator.free(row.evidence_metadata_json);
        }
        rows.deinit(allocator);
    }

    const c = facet_sqlite.c;
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(allocator, .{
            .relation_id = @intCast(c.sqlite3_column_int64(stmt, 0)),
            .relation_type = try helper_api.dupeColText(allocator, stmt, 1),
            .source_id = @intCast(c.sqlite3_column_int64(stmt, 2)),
            .target_id = @intCast(c.sqlite3_column_int64(stmt, 3)),
            .relation_metadata_json = try helper_api.dupeColText(allocator, stmt, 4),
            .evidence_entity_id = @intCast(c.sqlite3_column_int64(stmt, 5)),
            .evidence_entity_type = try helper_api.dupeColText(allocator, stmt, 6),
            .evidence_name = try helper_api.dupeColText(allocator, stmt, 7),
            .evidence_confidence = @floatCast(c.sqlite3_column_double(stmt, 8)),
            .evidence_metadata_json = try helper_api.dupeColText(allocator, stmt, 9),
        });
    }

    return rows.toOwnedSlice(allocator);
}

fn deinitGhostcrabProjectionEvidence(allocator: std.mem.Allocator, rows: []GhostcrabProjectionEvidenceRow) void {
    for (rows) |row| {
        allocator.free(row.relation_type);
        allocator.free(row.relation_metadata_json);
        allocator.free(row.evidence_entity_type);
        allocator.free(row.evidence_name);
        allocator.free(row.evidence_metadata_json);
    }
    allocator.free(rows);
}

fn mimeTypeForPath(path: []const u8) []const u8 {
    if (std.mem.endsWith(u8, path, ".html")) return "text/html; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".js")) return "application/javascript; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".css")) return "text/css; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".json")) return "application/json; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".svg")) return "image/svg+xml";
    if (std.mem.endsWith(u8, path, ".png")) return "image/png";
    if (std.mem.endsWith(u8, path, ".ico")) return "image/x-icon";
    if (std.mem.endsWith(u8, path, ".woff2")) return "font/woff2";
    return "application/octet-stream";
}

fn printUsage() !void {
    std.debug.print(
        \\usage:
        \\  mindbrain-http [--addr <ip:port>] [--db <sqlite_path>] [--static-dir <dir>] [--init-only]
        \\
        \\env:
        \\  MINDBRAIN_HTTP_ADDR=127.0.0.1:8091   bind IP + port (default loopback only)
        \\  MINDBRAIN_HTTP_MAX_BODY_BYTES=1048576  cap SQL JSON request bodies
        \\  MINDBRAIN_HTTP_MAX_CONNS=128           cap concurrent connections
        \\  MINDBRAIN_DB_PATH=data/mindbrain.sqlite
        \\  MINDBRAIN_STATIC_DIR=dashboard/dist
        \\
        \\security:
        \\  POST /api/mindbrain/sql* exposes unauthenticated SQLite admin access.
        \\  Keep the server on loopback or place it behind your own auth/proxy layer.
        \\
        \\routes:
        \\  POST /api/mindbrain/sql
        \\  POST /api/mindbrain/sql/session/open
        \\  POST /api/mindbrain/sql/session/query
        \\  POST /api/mindbrain/sql/session/close
        \\  POST /api/mindbrain/facts/write
        \\  GET /health
        \\  GET /api/events  (SSE demo_firehose, long-lived)
        \\  GET /api/mindbrain/simulate
        \\  GET /api/mindbrain/events  (alias SSE)
        \\  GET /api/mindbrain/search-compact-info
        \\  GET /api/mindbrain/coverage?workspace_id=...
        \\  GET /api/mindbrain/coverage-by-domain?domain_or_workspace=...
        \\  GET /api/mindbrain/workspace-export?workspace_id=...
        \\  GET /api/mindbrain/workspace-export-by-domain?domain_or_workspace=...
        \\  GET /api/mindbrain/graph-path?source=...&target=...&max_depth=...
        \\  GET /api/mindbrain/graph/subgraph?seed_ids=1,2&hops=2&edge_types=requires
        \\  GET /api/mindbrain/traverse?start=...&direction=...&depth=...
        \\  GET /api/mindbrain/pack?user_id=...&query=...&scope=...&limit=...
        \\
        \\notes:
        \\  Use bracketed IPv6 in MINDBRAIN_HTTP_ADDR / --addr, for example [::1]:8091.
        \\  Passing :8091 still binds 0.0.0.0:8091 explicitly; the safe default is 127.0.0.1:8091.
        \\
    , .{});
}

fn rejectBusyConnection(io: std.Io, connection: std.Io.net.Stream) void {
    defer connection.close(io);

    const body = "{\"error\":\"server overloaded\"}";
    var send_buffer: [512]u8 = undefined;
    var stream_writer = connection.writer(io, &send_buffer);
    const writer = &stream_writer.interface;
    writer.print(
        "HTTP/1.1 503 Service Unavailable\r\ncontent-type: application/json; charset=utf-8\r\ncache-control: no-store\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{s}",
        .{ body.len, body },
    ) catch {};
}

fn parseTarget(target: []const u8) !struct { path: []const u8, query: []const u8 } {
    if (target.len == 0) return error.InvalidArguments;
    if (std.mem.indexOfScalar(u8, target, '?')) |idx| {
        return .{ .path = target[0..idx], .query = target[idx + 1 ..] };
    }
    return .{ .path = target, .query = "" };
}

fn queryValue(allocator: std.mem.Allocator, query: []const u8, key: []const u8) !?[]const u8 {
    if (query.len == 0) return null;

    var it = std.mem.splitScalar(u8, query, '&');
    while (it.next()) |pair| {
        if (pair.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, pair, '=') orelse pair.len;
        const raw_key = pair[0..eq];
        if (!std.mem.eql(u8, raw_key, key)) continue;
        const raw_value = if (eq < pair.len) pair[eq + 1 ..] else "";
        return try decodeQueryComponent(allocator, raw_value);
    }

    return null;
}

fn queryValues(allocator: std.mem.Allocator, query: []const u8, key: []const u8) ![]const []const u8 {
    var result = std.ArrayList([]const u8).empty;
    errdefer result.deinit(allocator);

    if (query.len == 0) return result.toOwnedSlice(allocator);

    var it = std.mem.splitScalar(u8, query, '&');
    while (it.next()) |pair| {
        if (pair.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, pair, '=') orelse pair.len;
        const raw_key = pair[0..eq];
        if (!std.mem.eql(u8, raw_key, key)) continue;
        const raw_value = if (eq < pair.len) pair[eq + 1 ..] else "";
        const decoded = try decodeQueryComponent(allocator, raw_value);
        try result.append(allocator, decoded);
    }

    return result.toOwnedSlice(allocator);
}

fn encodeGraphSubgraphSseBody(allocator: std.mem.Allocator, events: []const graph_sqlite.GraphStreamEvent) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    try out.writer.writeAll("retry: 1000\n");
    for (events) |event| {
        try out.writer.print("id: {d}\n", .{event.seq});
        try out.writer.print("event: {s}\n", .{event.kind});
        try out.writer.print("data: {s}\n\n", .{event.payload});
    }
    return try out.toOwnedSlice();
}

fn parseCsvU32List(allocator: std.mem.Allocator, text: []const u8) ![]u32 {
    var result = std.ArrayList(u32).empty;
    errdefer result.deinit(allocator);

    var it = std.mem.splitScalar(u8, text, ',');
    while (it.next()) |raw| {
        const trimmed = std.mem.trim(u8, raw, " \t\r\n");
        if (trimmed.len == 0) continue;
        try result.append(allocator, try std.fmt.parseInt(u32, trimmed, 10));
    }

    return result.toOwnedSlice(allocator);
}

fn parseCsvStringList(allocator: std.mem.Allocator, text: []const u8) ![]const []const u8 {
    var result = std.ArrayList([]const u8).empty;
    errdefer result.deinit(allocator);

    var it = std.mem.splitScalar(u8, text, ',');
    while (it.next()) |raw| {
        const trimmed = std.mem.trim(u8, raw, " \t\r\n");
        if (trimmed.len == 0) continue;
        try result.append(allocator, trimmed);
    }

    return result.toOwnedSlice(allocator);
}

fn decodeQueryComponent(allocator: std.mem.Allocator, raw: []const u8) ![]const u8 {
    const copy = try allocator.alloc(u8, raw.len);
    @memcpy(copy, raw);
    for (copy) |*byte| {
        if (byte.* == '+') byte.* = ' ';
    }
    return std.Uri.percentDecodeInPlace(copy);
}

fn parseDirection(text: []const u8) ?graph_sqlite.TraverseDirection {
    if (std.mem.eql(u8, text, "outbound")) return .outbound;
    if (std.mem.eql(u8, text, "inbound")) return .inbound;
    return null;
}

test "graph subgraph sse encoder emits expected frames" {
    var events = try std.testing.allocator.alloc(graph_sqlite.GraphStreamEvent, 2);
    defer {
        for (events) |*event| event.deinit(std.testing.allocator);
        std.testing.allocator.free(events);
    }

    events[0] = .{
        .seq = 0,
        .kind = "seed_node",
        .payload = try std.testing.allocator.dupe(u8, "{\"entity\":{\"entity_id\":1}}"),
    };
    events[1] = .{
        .seq = 1,
        .kind = "done",
        .payload = try std.testing.allocator.dupe(u8, "{\"kind\":\"subgraph\",\"node_count\":2}"),
    };

    const body = try encodeGraphSubgraphSseBody(std.testing.allocator, events);
    defer std.testing.allocator.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "retry: 1000\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "id: 0\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "event: seed_node\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "data: {\"entity\":{\"entity_id\":1}}") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "event: done\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"kind\":\"subgraph\"") != null);
}
