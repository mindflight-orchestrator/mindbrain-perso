//! Infer and register table_semantics for structured import (Phase 3 control plane).
const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const structured_import = @import("structured_import.zig");
const workspace_sqlite = @import("workspace_sqlite.zig");
const zig16_compat = @import("zig16_compat.zig");

pub const InferOptions = struct {
    model_path: []const u8,
    mapping_path: ?[]const u8 = null,
    input_path: ?[]const u8 = null,
    input_dir: ?[]const u8 = null,
};

pub const RegisterReport = struct {
    tables: u64 = 0,
    columns: u64 = 0,
    relations: u64 = 0,
    source_mappings: u64 = 0,
};

pub fn inferSemanticsJson(allocator: std.mem.Allocator, opts: InferOptions) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const model_text = try structured_import.readJsonFile(a, opts.model_path);
    var model = try std.json.parseFromSlice(std.json.Value, a, model_text, .{});
    defer model.deinit();

    const workspace_id = model.value.object.get("workspace_id") orelse return error.InvalidModel;
    if (workspace_id != .string) return error.InvalidModel;

    var mapping_parsed: ?std.json.Parsed(std.json.Value) = null;
    defer if (mapping_parsed) |*mapping| mapping.deinit();
    var mapping_value: ?std.json.Value = null;
    if (opts.mapping_path) |mp| {
        const mapping_text = try structured_import.readJsonFile(a, mp);
        mapping_parsed = try std.json.parseFromSlice(std.json.Value, a, mapping_text, .{});
        mapping_value = mapping_parsed.?.value;
    }

    const tables = try buildTableSemantics(a, model.value, mapping_value);
    const columns = try buildColumnSemantics(a, model.value);
    const relations = try buildRelationSemantics(a, model.value, mapping_value);
    const mappings = try buildSourceMappingsFromPaths(a, workspace_id.string, opts.mapping_path);

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    const w = &out.writer;
    try w.writeAll("{\"table_semantics\":");
    try w.print("{f}", .{std.json.fmt(tables, .{})});
    try w.writeAll(",\"column_semantics\":");
    try w.print("{f}", .{std.json.fmt(columns, .{})});
    try w.writeAll(",\"relation_semantics\":");
    try w.print("{f}", .{std.json.fmt(relations, .{})});
    try w.writeAll(",\"source_mappings\":");
    try w.print("{f}", .{std.json.fmt(mappings, .{})});

    var profiles = std.json.Array.init(a);
    if (opts.input_path) |input_path| {
        var bundle = try structured_import.readTabularBundle(a, input_path);
        defer bundle.deinit(a);
        for (bundle.tables) |named| {
            const profile = try buildInputProfile(a, input_path, named.name, &named.table);
            try profiles.append(profile);
        }
    } else if (opts.input_dir) |input_dir| {
        try appendInputProfilesFromDir(a, &profiles, input_dir, mapping_value);
    } else if (mapping_value) |map_value| {
        if (opts.mapping_path) |mp| {
            try appendInputProfilesFromMapping(a, &profiles, mp, map_value);
        }
    }

    if (profiles.items.len == 1) {
        try w.writeAll(",\"input_profile\":");
        try w.print("{f}", .{std.json.fmt(profiles.items[0], .{})});
    }
    if (profiles.items.len > 0) {
        try w.writeAll(",\"input_profiles\":[");
        for (profiles.items, 0..) |profile, idx| {
            if (idx > 0) try w.writeAll(",");
            try w.print("{f}", .{std.json.fmt(profile, .{})});
        }
        try w.writeAll("]");
    }
    try w.writeAll("}");
    return out.toOwnedSlice();
}

pub fn registerSemanticsJson(
    allocator: std.mem.Allocator,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    proposal_json: []const u8,
) !RegisterReport {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, proposal_json, .{});
    defer parsed.deinit();
    try workspace_sqlite.upsertWorkspace(db, workspace_id, "{\"domain\":\"structured_import\"}");

    var report = RegisterReport{};
    const root = parsed.value.object;

    if (root.get("table_semantics")) |tables| {
        if (tables == .array) {
            for (tables.array.items) |item| {
                if (item != .object) continue;
                const schema = jsonStringOr(item.object, "table_schema", "structured") orelse "structured";
                const table_name = item.object.get("table_name") orelse continue;
                if (table_name != .string) continue;
                const table_id = try resolveTableId(db, workspace_id, schema, table_name.string);
                const notes = try buildTableNotesJson(allocator, item.object);
                defer if (notes) |n| allocator.free(n);
                const key_column = jsonStringOr(item.object, "key_column", "record_id") orelse "record_id";
                const content_column = jsonStringOr(item.object, "content_column", "content") orelse "content";
                try workspace_sqlite.upsertTableSemanticFull(db, .{
                    .table_id = table_id,
                    .workspace_id = workspace_id,
                    .schema_name = schema,
                    .table_name = table_name.string,
                    .key_column = key_column,
                    .content_column = content_column,
                    .business_role = jsonStringOr(item.object, "business_role", null),
                    .generation_strategy = jsonStringOr(item.object, "generation_strategy", "structured_import") orelse "structured_import",
                    .emit_facets = jsonBoolOr(item.object, "emit_facets", true),
                    .emit_graph_entity = jsonBoolOr(item.object, "emit_graph_entity", true),
                    .emit_graph_relation = jsonBoolOr(item.object, "emit_graph_relation", true),
                    .notes = notes,
                });
                report.tables += 1;
            }
        }
    }

    if (root.get("column_semantics")) |columns| {
        if (columns == .array) {
            for (columns.array.items) |item| {
                if (item != .object) continue;
                const schema = jsonStringOr(item.object, "table_schema", "structured") orelse "structured";
                const table_name = item.object.get("table_name") orelse continue;
                const column_name = item.object.get("column_name") orelse continue;
                if (table_name != .string or column_name != .string) continue;
                const table_id = try resolveTableId(db, workspace_id, schema, table_name.string);
                const column_id = try workspace_sqlite.nextColumnSemanticId(db);
                const role = jsonStringOr(item.object, "column_role", "unknown") orelse "unknown";
                const data_type = jsonStringOr(item.object, "semantic_type", null);
                const nullable = jsonBoolOr(item.object, "is_nullable", true);
                const rich_meta = try buildColumnRichMetaJson(allocator, item.object);
                defer if (rich_meta) |m| allocator.free(m);
                try workspace_sqlite.upsertColumnSemanticFull(db, .{
                    .column_semantic_id = column_id,
                    .table_id = table_id,
                    .column_name = column_name.string,
                    .column_role = role,
                    .data_type = data_type,
                    .is_nullable = nullable,
                    .rich_meta = rich_meta,
                });
                report.columns += 1;
            }
        }
    }

    if (root.get("relation_semantics")) |relations| {
        if (relations == .array) {
            for (relations.array.items) |item| {
                if (item != .object) continue;
                const from_schema = jsonStringOr(item.object, "from_schema", "structured") orelse "structured";
                const from_table = item.object.get("from_table") orelse continue;
                const to_schema = jsonStringOr(item.object, "to_schema", "structured") orelse "structured";
                const to_table = item.object.get("to_table") orelse continue;
                if (from_table != .string or to_table != .string) continue;
                const source_table_id = try resolveTableId(db, workspace_id, from_schema, from_table.string);
                const target_table_id = try resolveTableId(db, workspace_id, to_schema, to_table.string);
                const relation_id = try workspace_sqlite.nextRelationSemanticId(db);
                const fk_column = jsonStringOr(item.object, "fk_column", "") orelse "";
                const relation_kind = jsonStringOr(item.object, "relation_kind", "unknown") orelse "unknown";
                const relation_name = jsonStringOr(item.object, "graph_label", "relation") orelse "relation";
                const edge_type = jsonStringOr(item.object, "graph_label", null);
                const metadata_json = try buildRelationRichMetaJson(allocator, item.object);
                defer allocator.free(metadata_json);
                try workspace_sqlite.upsertRelationSemanticWithFk(
                    db,
                    relation_id,
                    workspace_id,
                    relation_name,
                    source_table_id,
                    target_table_id,
                    fk_column,
                    relation_kind,
                    edge_type,
                    metadata_json,
                );
                report.relations += 1;
            }
        }
    }

    if (root.get("source_mappings")) |mappings| {
        if (mappings == .array) {
            for (mappings.array.items) |item| {
                if (item != .object) continue;
                const source_key = item.object.get("source_key") orelse continue;
                const source_kind = item.object.get("source_kind") orelse continue;
                if (source_key != .string or source_kind != .string) continue;
                const target_table = jsonStringOr(item.object, "target_table", null);
                var target_table_id: ?u64 = null;
                if (target_table) |table_name| {
                    target_table_id = try resolveTableId(db, workspace_id, "structured", table_name);
                }
                const mapping_id = try workspace_sqlite.nextSourceMappingId(db);
                const metadata = jsonStringOr(item.object, "metadata_json", "{}") orelse "{}";
                try workspace_sqlite.upsertSourceMapping(db, mapping_id, workspace_id, source_key.string, source_kind.string, target_table_id, metadata);
                report.source_mappings += 1;
            }
        }
    }

    return report;
}

fn resolveTableId(db: facet_sqlite.Database, workspace_id: []const u8, schema: []const u8, table_name: []const u8) !u64 {
    if (try workspace_sqlite.lookupTableId(db, workspace_id, schema, table_name)) |existing| return existing;
    return workspace_sqlite.nextTableId(db);
}

const EntityMappingHints = struct {
    key_column: []const u8 = "record_id",
    content_column: []const u8 = "content",
};

fn entityMappingHints(allocator: std.mem.Allocator, mapping: ?std.json.Value, entity_name: []const u8) !EntityMappingHints {
    _ = allocator;
    var hints = EntityMappingHints{};
    const map = mapping orelse return hints;
    if (map != .object) return hints;
    const entities = map.object.get("entities") orelse return hints;
    if (entities != .object) return hints;
    const entity_val = entities.object.get(entity_name) orelse return hints;
    if (entity_val != .object) return hints;
    if (jsonStringOr(entity_val.object, "record_id_column", null)) |col| hints.key_column = col;
    if (entity_val.object.get("content_columns")) |cols| {
        if (cols == .array and cols.array.items.len > 0) {
            const first = cols.array.items[0];
            if (first == .string) hints.content_column = first.string;
        }
    }
    return hints;
}

fn buildTableSemantics(allocator: std.mem.Allocator, model: std.json.Value, mapping: ?std.json.Value) !std.json.Value {
    const entity_types = model.object.get("entity_types") orelse return .{ .array = std.json.Array.init(allocator) };
    if (entity_types != .array) return error.InvalidModel;

    var out = std.json.Array.init(allocator);
    for (entity_types.array.items) |item| {
        if (item != .object) continue;
        const name = item.object.get("name") orelse continue;
        if (name != .string) continue;

        const hints = try entityMappingHints(allocator, mapping, name.string);
        const notes = try buildEntityNotesJson(allocator, item.object);

        var row = std.json.ObjectMap.empty;
        try row.put(allocator, "table_schema", .{ .string = try allocator.dupe(u8, "structured") });
        try row.put(allocator, "table_name", .{ .string = try allocator.dupe(u8, name.string) });
        try row.put(allocator, "business_role", .{ .string = try allocator.dupe(u8, "entity") });
        try row.put(allocator, "generation_strategy", .{ .string = try allocator.dupe(u8, "structured_import") });
        try row.put(allocator, "emit_facets", .{ .bool = true });
        try row.put(allocator, "emit_graph_entity", .{ .bool = true });
        try row.put(allocator, "emit_graph_relation", .{ .bool = true });
        try row.put(allocator, "notes", .{ .string = notes });
        try row.put(allocator, "key_column", .{ .string = try allocator.dupe(u8, hints.key_column) });
        try row.put(allocator, "content_column", .{ .string = try allocator.dupe(u8, hints.content_column) });
        try out.append(.{ .object = row });
    }
    return .{ .array = out };
}

fn buildEntityNotesJson(allocator: std.mem.Allocator, entity: std.json.ObjectMap) ![]const u8 {
    var parts = std.ArrayList(u8).empty;
    defer parts.deinit(allocator);
    try parts.appendSlice(allocator, "{\"source\":\"structured_import\"");
    if (entity.get("schema_id")) |schema_id| {
        if (schema_id == .string) {
            try parts.appendSlice(allocator, ",\"schema_id\":\"");
            try jsonAppendEscaped(allocator, &parts, schema_id.string);
            try parts.append(allocator, '"');
        }
    }
    if (entity.get("ontology")) |ontology| {
        if (ontology == .string) {
            try parts.appendSlice(allocator, ",\"entity_family\":\"");
            try jsonAppendEscaped(allocator, &parts, ontology.string);
            try parts.append(allocator, '"');
        }
    }
    try parts.append(allocator, '}');
    return parts.toOwnedSlice(allocator);
}

fn buildColumnSemantics(allocator: std.mem.Allocator, model: std.json.Value) !std.json.Value {
    const entity_types = model.object.get("entity_types") orelse return .{ .array = std.json.Array.init(allocator) };
    if (entity_types != .array) return error.InvalidModel;

    var out = std.json.Array.init(allocator);
    for (entity_types.array.items) |item| {
        if (item != .object) continue;
        const table_name = item.object.get("name") orelse continue;
        if (table_name != .string) continue;
        const facets = item.object.get("facets") orelse continue;
        if (facets != .array) continue;
        for (facets.array.items) |facet| {
            if (facet != .object) continue;
            const column_name = facet.object.get("name") orelse continue;
            const facet_type = facet.object.get("type") orelse continue;
            if (column_name != .string or facet_type != .string) continue;
            const role = inferColumnRole(column_name.string, facet_type.string);
            const nullable = blk: {
                if (facet.object.get("required")) |required| {
                    if (required == .bool) break :blk !required.bool;
                }
                break :blk true;
            };

            var row = std.json.ObjectMap.empty;
            try row.put(allocator, "table_schema", .{ .string = try allocator.dupe(u8, "structured") });
            try row.put(allocator, "table_name", .{ .string = try allocator.dupe(u8, table_name.string) });
            try row.put(allocator, "column_name", .{ .string = try allocator.dupe(u8, column_name.string) });
            try row.put(allocator, "column_role", .{ .string = try allocator.dupe(u8, role) });
            try row.put(allocator, "public_column_role", .{ .string = try allocator.dupe(u8, role) });
            try row.put(allocator, "semantic_type", .{ .string = try allocator.dupe(u8, facet_type.string) });
            try row.put(allocator, "facet_key", .{ .string = try allocator.dupe(u8, column_name.string) });
            try row.put(allocator, "is_nullable", .{ .bool = nullable });
            if (std.mem.eql(u8, facet_type.string, "ref") or std.mem.eql(u8, facet_type.string, "ref_list")) {
                if (facet.object.get("target")) |target| {
                    if (target == .string) {
                        try row.put(allocator, "graph_usage", .{ .string = try std.fmt.allocPrint(allocator, "ref:{s}", .{target.string}) });
                    }
                }
            }
            try out.append(.{ .object = row });
        }
    }
    return .{ .array = out };
}

fn buildRelationSemantics(allocator: std.mem.Allocator, model: std.json.Value, mapping: ?std.json.Value) !std.json.Value {
    var out = std.json.Array.init(allocator);

    if (mapping) |map_value| {
        const relations = map_value.object.get("contract_relations") orelse null;
        if (relations) |rel_array| {
            if (rel_array == .array) {
                for (rel_array.array.items) |item| {
                    if (item != .object) continue;
                    const source_type = item.object.get("source_type") orelse continue;
                    const target_type = item.object.get("target_type") orelse continue;
                    const edge_label = item.object.get("edge_label") orelse continue;
                    const fk_column = item.object.get("source_ref_column") orelse continue;
                    if (source_type != .string or target_type != .string or edge_label != .string or fk_column != .string) continue;
                    try appendRelationRow(allocator, &out, source_type.string, target_type.string, fk_column.string, edge_label.string);
                }
            }
        }
    }

    const graph_relations = model.object.get("graph_relations") orelse null;
    if (graph_relations) |rel_array| {
        if (rel_array == .array) {
            for (rel_array.array.items) |item| {
                if (item != .object) continue;
                const source_type = item.object.get("source_type") orelse continue;
                const target_type = item.object.get("target_type") orelse continue;
                const edge_label = item.object.get("edge_label") orelse item.object.get("label") orelse continue;
                const fk_text = blk: {
                    if (item.object.get("source_ref_column")) |col| {
                        if (col == .string) break :blk col.string;
                    }
                    if (item.object.get("via_column")) |col| {
                        if (col == .string) break :blk col.string;
                    }
                    break :blk "";
                };
                if (source_type != .string or target_type != .string or edge_label != .string) continue;
                try appendRelationRow(allocator, &out, source_type.string, target_type.string, fk_text, edge_label.string);
            }
        }
    }

    return .{ .array = out };
}

fn appendRelationRow(
    allocator: std.mem.Allocator,
    out: *std.json.Array,
    from_table: []const u8,
    to_table: []const u8,
    fk_column: []const u8,
    graph_label: []const u8,
) !void {
    var row = std.json.ObjectMap.empty;
    try row.put(allocator, "from_schema", .{ .string = try allocator.dupe(u8, "structured") });
    try row.put(allocator, "from_table", .{ .string = try allocator.dupe(u8, from_table) });
    try row.put(allocator, "to_schema", .{ .string = try allocator.dupe(u8, "structured") });
    try row.put(allocator, "to_table", .{ .string = try allocator.dupe(u8, to_table) });
    try row.put(allocator, "fk_column", .{ .string = try allocator.dupe(u8, fk_column) });
    try row.put(allocator, "relation_kind", .{ .string = try allocator.dupe(u8, "many_to_one") });
    try row.put(allocator, "graph_label", .{ .string = try allocator.dupe(u8, graph_label) });
    try out.append(.{ .object = row });
}

fn buildSourceMappingsFromPaths(allocator: std.mem.Allocator, workspace_id: []const u8, mapping_path: ?[]const u8) !std.json.Value {
    const mp = mapping_path orelse return .{ .array = std.json.Array.init(allocator) };

    const mapping_text = try structured_import.readJsonFile(allocator, mp);
    defer allocator.free(mapping_text);
    var mapping = try std.json.parseFromSlice(std.json.Value, allocator, mapping_text, .{});
    defer mapping.deinit();

    return buildSourceMappings(allocator, workspace_id, mapping.value);
}

fn buildSourceMappings(allocator: std.mem.Allocator, workspace_id: []const u8, mapping: std.json.Value) !std.json.Value {
    var out = std.json.Array.init(allocator);

    if (mapping != .object) return .{ .array = out };

    if (mapping.object.get("entities")) |entities| {
        if (entities == .object) {
            for (entities.object.keys()) |entity_name| {
                const entity_val = entities.object.get(entity_name) orelse continue;
                if (entity_val != .object) continue;
                const csv = jsonStringOr(entity_val.object, "csv", null) orelse continue;
                const source_key = try std.fmt.allocPrint(allocator, "{s}:{s}", .{ workspace_id, csv });
                var row = std.json.ObjectMap.empty;
                try row.put(allocator, "source_key", .{ .string = try allocator.dupe(u8, source_key) });
                try row.put(allocator, "source_kind", .{ .string = try allocator.dupe(u8, "structured_import_facet_graph") });
                try row.put(allocator, "target_table", .{ .string = try allocator.dupe(u8, entity_name) });
                try row.put(allocator, "metadata_json", .{ .string = try allocator.dupe(u8, "{\"pipeline\":\"structured-import\"}") });
                try out.append(.{ .object = row });
            }
        }
    }
    if (mapping.object.get("import_ready")) |import_ready| {
        if (import_ready == .object) {
            if (import_ready.object.get("facets_csv")) |facets_csv| {
                if (facets_csv == .string) {
                    const source_key = try std.fmt.allocPrint(allocator, "{s}:import_ready:facets", .{workspace_id});
                    var row = std.json.ObjectMap.empty;
                    try row.put(allocator, "source_key", .{ .string = try allocator.dupe(u8, source_key) });
                    try row.put(allocator, "source_kind", .{ .string = try allocator.dupe(u8, "structured_import_ready") });
                    try row.put(allocator, "metadata_json", .{ .string = try std.fmt.allocPrint(allocator, "{{\"path\":\"{s}\"}}", .{facets_csv.string}) });
                    try out.append(.{ .object = row });
                }
            }
        }
    }

    return .{ .array = out };
}

fn buildInputProfile(allocator: std.mem.Allocator, path: []const u8, table_name: []const u8, table: *const structured_import.CsvTable) !std.json.Value {
    var columns = std.json.Array.init(allocator);
    for (table.headers, 0..) |header, idx| {
        var non_empty: u64 = 0;
        for (table.rows) |row| {
            if (idx < row.len and row[idx].len > 0) non_empty += 1;
        }
        var row = std.json.ObjectMap.empty;
        try row.put(allocator, "name", .{ .string = try allocator.dupe(u8, header) });
        try row.put(allocator, "non_empty", .{ .integer = @intCast(non_empty) });
        try columns.append(.{ .object = row });
    }
    var out = std.json.ObjectMap.empty;
    try out.put(allocator, "path", .{ .string = try allocator.dupe(u8, path) });
    try out.put(allocator, "table_name", .{ .string = try allocator.dupe(u8, table_name) });
    try out.put(allocator, "row_count", .{ .integer = @intCast(table.rows.len) });
    try out.put(allocator, "columns", .{ .array = columns });
    return .{ .object = out };
}

fn appendInputProfilesFromMapping(allocator: std.mem.Allocator, profiles: *std.json.Array, mapping_path: []const u8, mapping: std.json.Value) !void {
    if (mapping != .object) return;
    const entities = mapping.object.get("entities") orelse return;
    if (entities != .object) return;
    const mapping_dir = std.fs.path.dirname(mapping_path) orelse ".";
    for (entities.object.keys()) |entity_name| {
        const entity_val = entities.object.get(entity_name) orelse continue;
        if (entity_val != .object) continue;
        const csv_rel = jsonStringOr(entity_val.object, "csv", null) orelse continue;
        const csv_path = try std.fs.path.join(allocator, &.{ mapping_dir, csv_rel });
        defer allocator.free(csv_path);
        var table = structured_import.readTableFile(allocator, csv_path) catch continue;
        defer table.deinit(allocator);
        const profile = try buildInputProfile(allocator, csv_path, entity_name, &table);
        try profiles.append(profile);
    }
}

fn appendInputProfilesFromDir(allocator: std.mem.Allocator, profiles: *std.json.Array, input_dir: []const u8, mapping: ?std.json.Value) !void {
    var dir = try std.Io.Dir.cwd().openDir(zig16_compat.io(), input_dir, .{ .iterate = true });
    defer dir.close(zig16_compat.io());
    var iter = dir.iterate();
    while (try iter.next(zig16_compat.io())) |entry| {
        if (entry.kind != .file) continue;
        const name = entry.name;
        if (!std.mem.endsWith(u8, name, ".csv") and !std.mem.endsWith(u8, name, ".json") and !std.mem.endsWith(u8, name, ".toon")) continue;
        const full_path = try std.fs.path.join(allocator, &.{ input_dir, name });
        defer allocator.free(full_path);
        const table_name = blk: {
            if (mapping) |map_value| {
                if (map_value == .object) {
                    if (map_value.object.get("entities")) |entities| {
                        if (entities == .object) {
                            for (entities.object.keys()) |entity_name| {
                                const entity_val = entities.object.get(entity_name) orelse continue;
                                if (entity_val != .object) continue;
                                const csv_rel = jsonStringOr(entity_val.object, "csv", null) orelse continue;
                                if (std.mem.endsWith(u8, csv_rel, name)) break :blk try allocator.dupe(u8, entity_name);
                            }
                        }
                    }
                }
            }
            break :blk try stripExtension(allocator, name);
        };
        defer allocator.free(table_name);
        var table = try structured_import.readTableFile(allocator, full_path);
        defer table.deinit(allocator);
        const profile = try buildInputProfile(allocator, full_path, table_name, &table);
        try profiles.append(profile);
    }
}

fn stripExtension(allocator: std.mem.Allocator, filename: []const u8) ![]const u8 {
    if (std.mem.lastIndexOfScalar(u8, filename, '.')) |dot| {
        return try allocator.dupe(u8, filename[0..dot]);
    }
    return try allocator.dupe(u8, filename);
}

fn buildColumnRichMetaJson(allocator: std.mem.Allocator, object: std.json.ObjectMap) !?[]const u8 {
    var parts = std.ArrayList(u8).empty;
    defer parts.deinit(allocator);
    var wrote_any = false;
    try parts.append(allocator, '{');
    if (jsonStringOr(object, "public_column_role", null)) |value| {
        try appendJsonField(allocator, &parts, "public_column_role", value, &wrote_any);
    }
    if (jsonStringOr(object, "semantic_type", null)) |value| {
        try appendJsonField(allocator, &parts, "semantic_type", value, &wrote_any);
    }
    if (jsonStringOr(object, "facet_key", null)) |value| {
        try appendJsonField(allocator, &parts, "facet_key", value, &wrote_any);
    }
    if (jsonStringOr(object, "graph_usage", null)) |value| {
        try appendJsonField(allocator, &parts, "graph_usage", value, &wrote_any);
    }
    if (jsonStringOr(object, "projection_signal", null)) |value| {
        try appendJsonField(allocator, &parts, "projection_signal", value, &wrote_any);
    }
    if (object.get("is_nullable")) |nullable| {
        if (nullable == .bool) {
            if (wrote_any) try parts.append(allocator, ',');
            try parts.appendSlice(allocator, "\"is_nullable\":");
            try parts.appendSlice(allocator, if (nullable.bool) "true" else "false");
            wrote_any = true;
        }
    }
    try parts.append(allocator, '}');
    if (!wrote_any) return null;
    return try parts.toOwnedSlice(allocator);
}

fn appendJsonField(allocator: std.mem.Allocator, parts: *std.ArrayList(u8), key: []const u8, value: []const u8, wrote_any: *bool) !void {
    if (wrote_any.*) try parts.append(allocator, ',');
    try parts.append(allocator, '"');
    try parts.appendSlice(allocator, key);
    try parts.appendSlice(allocator, "\":\"");
    try jsonAppendEscaped(allocator, parts, value);
    try parts.append(allocator, '"');
    wrote_any.* = true;
}

fn buildRelationRichMetaJson(allocator: std.mem.Allocator, object: std.json.ObjectMap) ![]const u8 {
    var parts = std.ArrayList(u8).empty;
    defer parts.deinit(allocator);
    try parts.appendSlice(allocator, "{\"source\":\"structured_import\"");
    if (jsonStringOr(object, "graph_label", null)) |label| {
        try parts.appendSlice(allocator, ",\"graph_label\":\"");
        try jsonAppendEscaped(allocator, &parts, label);
        try parts.append(allocator, '"');
    }
    if (jsonStringOr(object, "relation_kind", null)) |kind| {
        try parts.appendSlice(allocator, ",\"relation_kind\":\"");
        try jsonAppendEscaped(allocator, &parts, kind);
        try parts.append(allocator, '"');
    }
    try parts.append(allocator, '}');
    return parts.toOwnedSlice(allocator);
}

fn buildTableNotesJson(allocator: std.mem.Allocator, object: std.json.ObjectMap) !?[]const u8 {
    if (object.get("notes")) |notes| {
        if (notes == .string) return try allocator.dupe(u8, notes.string);
    }
    return null;
}

fn jsonAppendEscaped(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), text: []const u8) !void {
    for (text) |c| switch (c) {
        '"' => try buf.appendSlice(allocator, "\\\""),
        '\\' => try buf.appendSlice(allocator, "\\\\"),
        '\n' => try buf.appendSlice(allocator, "\\n"),
        '\r' => try buf.appendSlice(allocator, "\\r"),
        '\t' => try buf.appendSlice(allocator, "\\t"),
        else => try buf.append(allocator, c),
    };
}

fn inferColumnRole(column_name: []const u8, facet_type: []const u8) []const u8 {
    if (std.mem.eql(u8, facet_type, "ref") or std.mem.eql(u8, facet_type, "ref_list")) return "fk";
    if (std.mem.eql(u8, column_name, "record_id") or std.mem.eql(u8, column_name, "id")) return "id";
    if (std.mem.indexOf(u8, column_name, "statut") != null or std.mem.indexOf(u8, column_name, "status") != null) return "status";
    if (std.mem.indexOf(u8, column_name, "nom") != null or std.mem.indexOf(u8, column_name, "label") != null or std.mem.indexOf(u8, column_name, "libelle") != null) return "label";
    return "attribute";
}

fn jsonStringOr(object: std.json.ObjectMap, key: []const u8, default: ?[]const u8) ?[]const u8 {
    const value = object.get(key) orelse return default;
    return switch (value) {
        .string => |text| text,
        else => default,
    };
}

fn jsonBoolOr(object: std.json.ObjectMap, key: []const u8, default: bool) bool {
    const value = object.get(key) orelse return default;
    return switch (value) {
        .bool => |v| v,
        else => default,
    };
}

test "inferColumnRole maps ref facets to fk" {
    try std.testing.expectEqualStrings("fk", inferColumnRole("copropriete_id", "ref"));
    try std.testing.expectEqualStrings("id", inferColumnRole("record_id", "string"));
}

test "buildEntityNotesJson includes schema_id and entity_family" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var obj = std.json.ObjectMap.empty;
    try obj.put(a, "schema_id", .{ .string = "immeuble-structured-import:core:copropriete" });
    try obj.put(a, "ontology", .{ .string = "production" });
    const notes = try buildEntityNotesJson(a, obj);
    try std.testing.expect(std.mem.indexOf(u8, notes, "\"schema_id\":\"immeuble-structured-import:core:copropriete\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, notes, "\"entity_family\":\"production\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, notes, "\"source\":\"structured_import\"") != null);
}
