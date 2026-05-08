const std = @import("std");
const mindbrain = @import("mindbrain");
const benchmark = @import("benchmark");

const facet_sqlite = mindbrain.facet_sqlite;

const Allocator = std.mem.Allocator;

const CliError = error{
    InvalidArguments,
};

pub fn main(init: std.process.Init) !void {
    benchmark.zig16_compat.setIo(init.io);
    const allocator = init.gpa;
    const args = try init.minimal.args.toSlice(init.arena.allocator());

    if (args.len < 2) {
        try printUsage();
        return CliError.InvalidArguments;
    }

    if (std.mem.eql(u8, args[1], "imdb-benchmark")) {
        try runImdbBenchmarkCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "fts5-compare")) {
        try runFts5CompareCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "yago-import")) {
        try runYagoBenchmarkCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "yago-benchmark")) {
        try runYagoBenchmarkCommand(allocator, args[2..]);
        return;
    }

    try printUsage();
    return CliError.InvalidArguments;
}

fn printUsage() !void {
    var stderr_file_writer = std.Io.File.stderr().writer(benchmark.zig16_compat.io(), &.{});
    const stderr = &stderr_file_writer.interface;
    try stderr.writeAll(
        \\usage:
        \\  mindbrain-benchmark-tool imdb-benchmark --db <sqlite_path> --imdb-dir <path> [--limit <n>]
        \\      [--workspace-id <id>] [--collection-id <id>] [--ontology-id <id>]
        \\  mindbrain-benchmark-tool fts5-compare [--db <sqlite_path>] [--limit <n>] [--query <text>]...
        \\  mindbrain-benchmark-tool yago-import --db <sqlite_path> --yago-dir <path> [--limit <n>]
        \\      [--workspace-id <id>] [--collection-id <id>] [--ontology-id <id>]
        \\  mindbrain-benchmark-tool yago-benchmark --db <sqlite_path> --yago-dir <path> [--limit <n>]
        \\      [--workspace-id <id>] [--collection-id <id>] [--ontology-id <id>]
        \\
    );
    try stderr.flush();
}

fn runFts5CompareCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var limit: usize = 10;
    var queries = std.ArrayList([]const u8).empty;
    defer queries.deinit(allocator);

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else if (std.mem.eql(u8, arg, "--limit")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            limit = try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--query")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            try queries.append(allocator, args[index]);
        } else {
            return CliError.InvalidArguments;
        }
    }

    const summary = try benchmark.fts5_compare.run(allocator, .{
        .db_path = db_path,
        .limit = limit,
        .queries = queries.items,
    });
    defer benchmark.fts5_compare.deinitSummary(allocator, summary);

    var stdout_file_writer = std.Io.File.stdout().writer(benchmark.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;
    try stdout.print("{f}\n", .{std.json.fmt(summary, .{})});
    try stdout.flush();
}

fn runImdbBenchmarkCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var imdb_dir: ?[]const u8 = null;
    var row_limit: ?usize = null;
    var workspace_id: []const u8 = benchmark.imdb_import.default_workspace_id;
    var collection_id: []const u8 = benchmark.imdb_import.default_collection_id;
    var ontology_id: []const u8 = benchmark.imdb_import.default_ontology_id;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else if (std.mem.eql(u8, arg, "--imdb-dir")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            imdb_dir = args[index];
        } else if (std.mem.eql(u8, arg, "--limit")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            row_limit = try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--workspace-id")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            workspace_id = args[index];
        } else if (std.mem.eql(u8, arg, "--collection-id")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            collection_id = args[index];
        } else if (std.mem.eql(u8, arg, "--ontology-id")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            ontology_id = args[index];
        } else {
            return CliError.InvalidArguments;
        }
    }

    if (db_path == null or imdb_dir == null) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();

    const summary = try benchmark.imdb_import.runImdbImportBenchmark(db, allocator, .{
        .imdb_dir = imdb_dir.?,
        .row_limit = row_limit,
        .workspace_id = workspace_id,
        .collection_id = collection_id,
        .ontology_id = ontology_id,
    });

    var stdout_file_writer = std.Io.File.stdout().writer(benchmark.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;
    try stdout.print("{f}\n", .{std.json.fmt(summary, .{})});
    try stdout.flush();
}

fn runYagoBenchmarkCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var yago_dir: ?[]const u8 = null;
    var yago_path: ?[]const u8 = null;
    var row_limit: ?usize = null;
    var workspace_id: []const u8 = benchmark.yago_import.default_workspace_id;
    var collection_id: []const u8 = benchmark.yago_import.default_collection_id;
    var ontology_id: []const u8 = benchmark.yago_import.default_ontology_id;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else if (std.mem.eql(u8, arg, "--yago-dir")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            yago_dir = args[index];
        } else if (std.mem.eql(u8, arg, "--yago-path")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            yago_path = args[index];
        } else if (std.mem.eql(u8, arg, "--limit")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            row_limit = try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--workspace-id")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            workspace_id = args[index];
        } else if (std.mem.eql(u8, arg, "--collection-id")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            collection_id = args[index];
        } else if (std.mem.eql(u8, arg, "--ontology-id")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            ontology_id = args[index];
        } else {
            return CliError.InvalidArguments;
        }
    }

    if (db_path == null or (yago_dir == null and yago_path == null)) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();

    const summary = try benchmark.yago_import.runYagoImportBenchmark(db, allocator, .{
        .yago_dir = yago_dir,
        .yago_path = yago_path,
        .row_limit = row_limit,
        .workspace_id = workspace_id,
        .collection_id = collection_id,
        .ontology_id = ontology_id,
    });

    var stdout_file_writer = std.Io.File.stdout().writer(benchmark.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;
    try stdout.print("{f}\n", .{std.json.fmt(summary, .{})});
    try stdout.flush();
}
