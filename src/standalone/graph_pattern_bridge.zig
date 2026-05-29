const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const graph_pattern = @import("graph_pattern.zig");

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
    var options_buf: std.Io.Writer.Allocating = .init(allocator);
    defer options_buf.deinit();
    if (options_json.len == 0) {
        try options_buf.writer.writeAll("{}");
    } else {
        try options_buf.writer.writeAll(options_json);
    }
    const options = try options_buf.toOwnedSlice();

    var sql: std.Io.Writer.Allocating = .init(allocator);
    defer sql.deinit();
    try sql.writer.writeAll("SELECT graph.pattern_query_ast($gpq_ast$");
    try sql.writer.writeAll(ast_json);
    try sql.writer.writeAll("$gpq_ast$::jsonb, $gpq_opt$");
    try sql.writer.writeAll(options);
    try sql.writer.writeAll("$gpq_opt$::jsonb);");

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

    const root = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidRequest,
    };
    try root.put("debug", .bool(true));
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try std.json.stringify(parsed.value, .{}, out.writer());
    return out.toOwnedSlice();
}

fn runPsqlScalarJson(allocator: std.mem.Allocator, sql: []const u8) ![]u8 {
    const psql = try psqlPath(allocator);
    defer allocator.free(psql);

    var child = std.process.Child.init(&.{
        psql,
        "-v",
        "ON_ERROR_STOP=1",
        "-t",
        "-A",
        "-c",
        sql,
    }, allocator);
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;

    try child.spawn();
    const stdout = try child.stdout.?.readToEndAlloc(allocator, 16 * 1024 * 1024);
    errdefer allocator.free(stdout);
    const stderr = try child.stderr.?.readToEndAlloc(allocator, 1024 * 1024);
    defer allocator.free(stderr);

    const term = try child.wait();
    switch (term) {
        .Exited => |code| {
            if (code != 0) return error.PostgresExecutionFailed;
        },
        else => return error.PostgresExecutionFailed,
    }

    const trimmed = std.mem.trim(u8, stdout, " \t\r\n");
    if (trimmed.len == 0) return error.PostgresExecutionFailed;
    return allocator.dupe(u8, trimmed);
}

fn psqlPath(allocator: std.mem.Allocator) ![]const u8 {
    return std.process.getEnvVarOwned(allocator, "PSQL") catch return allocator.dupe(u8, "psql");
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
