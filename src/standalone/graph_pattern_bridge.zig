const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const graph_pattern = @import("graph_pattern.zig");
const zig16_compat = @import("zig16_compat.zig");

pub const Backend = enum {
    sqlite,
    postgres,
};

pub const ExecuteError = error{
    InvalidRequest,
    PostgresUnavailable,
    PostgresExecutionFailed,
    StepFailed,
} || graph_pattern.Error;

pub const Request = struct {
    query: []const u8,
    backend: Backend = .sqlite,
    options_json: []const u8 = "{}",
    debug: bool = false,
};

/// Execute GPQ text on SQLite (parse → validate → native SQLite SQL).
pub fn executeSqlite(
    allocator: std.mem.Allocator,
    db: facet_sqlite.Database,
    text: []const u8,
) ![]u8 {
    return graph_pattern.executeSqlite(allocator, db, text);
}

/// Execute GPQ text on PostgreSQL via `graph.pattern_query_ast(jsonb)` (requires `psql` + PG* env).
pub fn executePostgres(
    allocator: std.mem.Allocator,
    text: []const u8,
    options_json: []const u8,
) ![]u8 {
    const ast_json = try graph_pattern.parseToJsonAst(allocator, text);
    defer allocator.free(ast_json);
    return executePostgresAst(allocator, ast_json, options_json);
}

/// Call `graph.pattern_query_ast` with a pre-serialized AST json string.
pub fn executePostgresAst(
    allocator: std.mem.Allocator,
    ast_json: []const u8,
    options_json: []const u8,
) ![]u8 {
    const options = if (options_json.len == 0) "{}" else options_json;

    // Randomize the dollar-quote tag per call and require it absent from
    // both payloads: with a fixed tag, a crafted AST/options string could
    // close the quote and inject arbitrary SQL into the psql invocation.
    var tag_buf: [2 + 3 + 16]u8 = undefined;
    const tag: []const u8 = blk: {
        var attempt: usize = 0;
        while (attempt < 8) : (attempt += 1) {
            var raw: [8]u8 = undefined;
            std.crypto.random.bytes(&raw);
            const hex = std.fmt.bytesToHex(raw, .lower);
            const candidate = std.fmt.bufPrint(&tag_buf, "$gpq{s}$", .{hex}) catch unreachable;
            if (std.mem.indexOf(u8, ast_json, candidate) == null and
                std.mem.indexOf(u8, options, candidate) == null)
            {
                break :blk candidate;
            }
        }
        return error.InvalidRequest;
    };

    var sql: std.Io.Writer.Allocating = .init(allocator);
    defer sql.deinit();
    try sql.writer.writeAll("SELECT graph.pattern_query_ast(");
    try sql.writer.writeAll(tag);
    try sql.writer.writeAll(ast_json);
    try sql.writer.writeAll(tag);
    try sql.writer.writeAll("::jsonb, ");
    try sql.writer.writeAll(tag);
    try sql.writer.writeAll(options);
    try sql.writer.writeAll(tag);
    try sql.writer.writeAll("::jsonb);");

    const sql_cmd = try sql.toOwnedSlice();
    defer allocator.free(sql_cmd);

    return try runPsqlScalarJson(allocator, sql_cmd);
}

pub fn execute(
    allocator: std.mem.Allocator,
    db: facet_sqlite.Database,
    request: Request,
) ![]u8 {
    const options_json = try normalizeOptionsJson(allocator, request.options_json, request.debug);
    defer allocator.free(options_json);

    return switch (request.backend) {
        .sqlite => executeSqlite(allocator, db, request.query),
        .postgres => executePostgres(allocator, request.query, options_json),
    };
}

fn normalizeOptionsJson(allocator: std.mem.Allocator, options_json: []const u8, debug: bool) ![]u8 {
    if (!debug and options_json.len > 0) return allocator.dupe(u8, options_json);
    if (!debug) return allocator.dupe(u8, "{}");

    var parsed = try std.json.parseFromSlice(
        std.json.Value,
        allocator,
        if (options_json.len == 0) "{}" else options_json,
        .{},
    );
    defer parsed.deinit();

    switch (parsed.value) {
        .object => {
            try parsed.value.object.put(allocator, "debug", .{ .bool = true });
        },
        else => return error.InvalidRequest,
    }
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.print("{f}", .{std.json.fmt(parsed.value, .{})});
    return out.toOwnedSlice();
}

fn runPsqlScalarJson(allocator: std.mem.Allocator, sql: []const u8) ![]u8 {
    const psql = try psqlPath(allocator);
    defer allocator.free(psql);

    const io = zig16_compat.io();
    // stderr is discarded (never read before) rather than piped: draining
    // stdout to EOF while a stderr pipe fills past the kernel buffer used to
    // deadlock both processes.
    var child = try std.process.spawn(io, .{
        .argv = &.{ psql, "-v", "ON_ERROR_STOP=1", "-t", "-A", "-c", sql },
        .stdin = .ignore,
        .stdout = .pipe,
        .stderr = .ignore,
    });
    errdefer child.kill(io);

    const stdout_file = child.stdout orelse return error.PostgresExecutionFailed;
    var stdout_buffer: [4096]u8 = undefined;
    var stdout_reader = stdout_file.reader(io, &stdout_buffer);
    const stdout = try stdout_reader.interface.allocRemaining(allocator, .limited(16 * 1024 * 1024));
    errdefer allocator.free(stdout);

    const term = try child.wait(io);
    switch (term) {
        .exited => |code| {
            if (code != 0) return error.PostgresExecutionFailed;
        },
        else => return error.PostgresExecutionFailed,
    }

    const trimmed = std.mem.trim(u8, stdout, " \t\r\n");
    if (trimmed.len == 0) {
        allocator.free(stdout);
        return error.PostgresExecutionFailed;
    }
    const owned = try allocator.dupe(u8, trimmed);
    allocator.free(stdout);
    return owned;
}

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]u8;

fn psqlPath(allocator: std.mem.Allocator) ![]const u8 {
    if (getenv("PSQL")) |ptr| {
        const path = std.mem.span(ptr);
        if (path.len > 0) return allocator.dupe(u8, path);
    }
    return allocator.dupe(u8, "psql");
}

test "graph pattern bridge exports postgres-shaped AST for node query" {
    const ast = try graph_pattern.parseToJsonAst(std.testing.allocator,
        \\WORKSPACE immeuble-demo
        \\MATCH (u:unit {name: 'Tilleuls Appartement A3'})
        \\WHERE u.metadata.building_id = 1
        \\PROJECT u.entity_id, u.name, u.metadata.lot
        \\LIMIT 5
    );
    defer std.testing.allocator.free(ast);
    try std.testing.expect(std.mem.indexOf(u8, ast, "\"value_integer\":1") != null);
}
