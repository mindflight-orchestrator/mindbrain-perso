//! Schema drift validation for structured import (Phase 3+).
const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const structured_import = @import("structured_import.zig");
const zig16_compat = @import("zig16_compat.zig");

pub const DriftOptions = struct {
    model_path: []const u8,
    mapping_path: ?[]const u8 = null,
    input_path: ?[]const u8 = null,
    input_dir: ?[]const u8 = null,
    db_path: ?[]const u8 = null,
    workspace_id: ?[]const u8 = null,
    strict: bool = false,
};

pub const DriftReport = struct {
    errors: []const []const u8,
    warnings: []const []const u8,

    pub fn deinit(self: DriftReport, allocator: std.mem.Allocator) void {
        for (self.errors) |e| allocator.free(e);
        allocator.free(self.errors);
        for (self.warnings) |w| allocator.free(w);
        allocator.free(self.warnings);
    }

    pub fn ok(self: DriftReport) bool {
        return self.errors.len == 0;
    }
};

pub fn validateDrift(allocator: std.mem.Allocator, opts: DriftOptions) !DriftReport {
    const model_text = try structured_import.readJsonFile(allocator, opts.model_path);
    defer allocator.free(model_text);
    var model = try std.json.parseFromSlice(std.json.Value, allocator, model_text, .{});
    defer model.deinit();

    var errors = std.ArrayList([]const u8).empty;
    var warnings = std.ArrayList([]const u8).empty;
    errdefer {
        for (errors.items) |e| allocator.free(e);
        errors.deinit(allocator);
        for (warnings.items) |w| allocator.free(w);
        warnings.deinit(allocator);
    }

    var observed = std.StringHashMap(void).init(allocator);
    defer observed.deinit();

    if (opts.input_path) |input_path| {
        var bundle = try structured_import.readTabularBundle(allocator, input_path);
        defer bundle.deinit(allocator);
        for (bundle.tables) |named| {
            for (named.table.headers) |header| {
                try observed.put(try std.fmt.allocPrint(allocator, "{s}.{s}", .{ named.name, header }), {});
            }
        }
    } else if (opts.input_dir) |input_dir| {
        try collectObservedFromDir(allocator, input_dir, opts.mapping_path, &observed, &errors);
    } else if (opts.mapping_path) |mp| {
        const mapping_text = try structured_import.readJsonFile(allocator, mp);
        defer allocator.free(mapping_text);
        var mapping = try std.json.parseFromSlice(std.json.Value, allocator, mapping_text, .{});
        defer mapping.deinit();
        var bundle = try structured_import.readTabularBundleFromMappingDir(allocator, mp, mapping.value);
        defer bundle.deinit(allocator);
        for (bundle.tables) |named| {
            for (named.table.headers) |header| {
                try observed.put(try std.fmt.allocPrint(allocator, "{s}.{s}", .{ named.name, header }), {});
            }
        }
    }

    const entity_types = model.value.object.get("entity_types") orelse return error.InvalidModel;
    if (entity_types != .array) return error.InvalidModel;

    for (entity_types.array.items) |item| {
        if (item != .object) continue;
        const entity_name = item.object.get("name") orelse continue;
        if (entity_name != .string) continue;
        const facets = item.object.get("facets") orelse continue;
        if (facets != .array) continue;
        for (facets.array.items) |facet| {
            if (facet != .object) continue;
            const column_name = facet.object.get("name") orelse continue;
            if (column_name != .string) continue;
            const key = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ entity_name.string, column_name.string });
            defer allocator.free(key);
            if (observed.count() > 0 and !observed.contains(key)) {
                const msg = try std.fmt.allocPrint(allocator, "semantic column without observed data: {s}", .{key});
                try warnings.append(allocator, msg);
            }
        }
    }

    if (observed.count() > 0) {
        var known = std.StringHashMap(void).init(allocator);
        defer known.deinit();
        for (entity_types.array.items) |item| {
            if (item != .object) continue;
            const entity_name = item.object.get("name") orelse continue;
            if (entity_name != .string) continue;
            const facets = item.object.get("facets") orelse continue;
            if (facets != .array) continue;
            for (facets.array.items) |facet| {
                if (facet != .object) continue;
                const column_name = facet.object.get("name") orelse continue;
                if (column_name != .string) continue;
                const key = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ entity_name.string, column_name.string });
                try known.put(key, {});
            }
        }
        var it = observed.keyIterator();
        while (it.next()) |key_ptr| {
            if (!known.contains(key_ptr.*)) {
                const msg = try std.fmt.allocPrint(allocator, "observed column not in model: {s}", .{key_ptr.*});
                if (opts.strict) try errors.append(allocator, msg) else try warnings.append(allocator, msg);
            }
        }
    }

    if (opts.db_path) |db_path| {
        if (opts.workspace_id) |workspace_id| {
            var db = try facet_sqlite.Database.open(db_path);
            defer db.close();
            try compareRegisteredColumns(allocator, db, workspace_id, &observed, &errors, &warnings, opts.strict);
        }
    }

    return .{
        .errors = try errors.toOwnedSlice(allocator),
        .warnings = try warnings.toOwnedSlice(allocator),
    };
}

fn collectObservedFromDir(
    allocator: std.mem.Allocator,
    input_dir: []const u8,
    mapping_path: ?[]const u8,
    observed: *std.StringHashMap(void),
    errors: *std.ArrayList([]const u8),
) !void {
    _ = errors;
    var mapping_value: ?std.json.Value = null;
    if (mapping_path) |mp| {
        const mapping_text = try structured_import.readJsonFile(allocator, mp);
        defer allocator.free(mapping_text);
        var mapping = try std.json.parseFromSlice(std.json.Value, allocator, mapping_text, .{});
        defer mapping.deinit();
        mapping_value = mapping.value;
    }
    var dir = try std.Io.Dir.cwd().openDir(zig16_compat.io(), input_dir, .{ .iterate = true });
    defer dir.close(zig16_compat.io());
    var iter = dir.iterate();
    while (try iter.next(zig16_compat.io())) |entry| {
        if (entry.kind != .file) continue;
        const full_path = try std.fs.path.join(allocator, &.{ input_dir, entry.name });
        defer allocator.free(full_path);
        var table = structured_import.readTableFile(allocator, full_path) catch continue;
        defer table.deinit(allocator);
        const table_name = blk: {
            if (mapping_value) |map_value| {
                if (map_value == .object) {
                    if (map_value.object.get("entities")) |entities| {
                        if (entities == .object) {
                            for (entities.object.keys()) |entity_name| {
                                const entity_val = entities.object.get(entity_name) orelse continue;
                                if (entity_val != .object) continue;
                                const csv_val = entity_val.object.get("csv") orelse continue;
                                if (csv_val != .string) continue;
                                if (std.mem.endsWith(u8, csv_val.string, entry.name)) break :blk try allocator.dupe(u8, entity_name);
                            }
                        }
                    }
                }
            }
            break :blk try stripExtension(allocator, entry.name);
        };
        defer allocator.free(table_name);
        for (table.headers) |header| {
            try observed.put(try std.fmt.allocPrint(allocator, "{s}.{s}", .{ table_name, header }), {});
        }
    }
}

fn stripExtension(allocator: std.mem.Allocator, filename: []const u8) ![]const u8 {
    if (std.mem.lastIndexOfScalar(u8, filename, '.')) |dot| {
        return try allocator.dupe(u8, filename[0..dot]);
    }
    return try allocator.dupe(u8, filename);
}

fn compareRegisteredColumns(
    allocator: std.mem.Allocator,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    observed: *std.StringHashMap(void),
    errors: *std.ArrayList([]const u8),
    warnings: *std.ArrayList([]const u8),
    strict: bool,
) !void {
    _ = strict;
    _ = errors;
    const sql =
        \\SELECT table_name, column_name
        \\FROM column_semantics
        \\WHERE workspace_id = ?1 AND table_schema = 'structured'
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    while (true) {
        const rc = facet_sqlite.c.sqlite3_step(stmt);
        if (rc == facet_sqlite.c.SQLITE_DONE) break;
        if (rc != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;
        const table_name = try facet_sqlite.dupeColumnText(allocator, stmt, 0);
        defer allocator.free(table_name);
        const column_name = try facet_sqlite.dupeColumnText(allocator, stmt, 1);
        defer allocator.free(column_name);
        const key = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ table_name, column_name });
        defer allocator.free(key);
        if (observed.count() > 0 and !observed.contains(key)) {
            const msg = try std.fmt.allocPrint(allocator, "registered column not in observed input: {s}", .{key});
            try warnings.append(allocator, msg);
        }
    }
}

pub fn validateDriftOrFail(allocator: std.mem.Allocator, opts: DriftOptions) !void {
    var report = try validateDrift(allocator, opts);
    defer report.deinit(allocator);
    for (report.warnings) |warning| std.log.warn("{s}", .{warning});
    if (!report.ok()) {
        for (report.errors) |err_msg| std.log.err("{s}", .{err_msg});
        return error.DriftValidationFailed;
    }
}
