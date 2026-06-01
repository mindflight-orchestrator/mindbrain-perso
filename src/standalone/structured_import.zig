//! Structured tabular import: validate, apply import-ready CSVs, project via mapping, reindex.
const std = @import("std");
const collections_sqlite = @import("collections_sqlite.zig");
const facet_sqlite = @import("facet_sqlite.zig");
const facts_sqlite = @import("facts_sqlite.zig");
const reindex_http = @import("reindex_http.zig");
const search_sqlite = @import("search_sqlite.zig");
const workspace_sqlite = @import("workspace_sqlite.zig");
const zig16_compat = @import("zig16_compat.zig");
const ztoon = @import("ztoon");

pub const AgentFactsSearchTableId: u64 = 1;

pub const ImportMode = enum {
    reset,
    append,
    ignore_duplicates,

    pub fn parse(text: []const u8) ?ImportMode {
        if (std.mem.eql(u8, text, "reset")) return .reset;
        if (std.mem.eql(u8, text, "append")) return .append;
        if (std.mem.eql(u8, text, "ignore-duplicates") or std.mem.eql(u8, text, "ignore_duplicates")) return .ignore_duplicates;
        return null;
    }
};

pub const ReindexScope = enum {
    graph,
    facets,
    all,
    provenance,

    pub fn parse(text: []const u8) ?ReindexScope {
        if (std.mem.eql(u8, text, "graph")) return .graph;
        if (std.mem.eql(u8, text, "facets")) return .facets;
        if (std.mem.eql(u8, text, "all")) return .all;
        if (std.mem.eql(u8, text, "provenance")) return .provenance;
        return null;
    }
};

pub const SourceTag = "structured_import";
pub const LegacySourceTag = "fake_data";
pub const LegacySourceTagSerenity = "serenity_structured_import";

const taggedJsonSourceSql =
    \\(
    \\    json_extract(facets_json, '$.source') = ?2
    \\    OR json_extract(facets_json, '$.source') = ?3
    \\    OR json_extract(facets_json, '$.source') = ?4
    \\  )
;

const taggedMetadataSourceSql =
    \\(
    \\    json_extract(metadata_json, '$.source') = ?2
    \\    OR json_extract(metadata_json, '$.source') = ?3
    \\    OR json_extract(metadata_json, '$.source') = ?4
    \\  )
;

const taggedProvenanceSourceSql = "(source_tag = ?2 OR source_tag = ?3 OR source_tag = ?4)";

fn bindTaggedSourceTags(stmt: *facet_sqlite.c.sqlite3_stmt, source_tag: []const u8, start_idx: c_int) !void {
    try facet_sqlite.bindText(stmt, start_idx, source_tag);
    try facet_sqlite.bindText(stmt, start_idx + 1, LegacySourceTag);
    try facet_sqlite.bindText(stmt, start_idx + 2, LegacySourceTagSerenity);
}

pub const EdgesMode = enum {
    provided,
    derived,
    merge,

    pub fn parse(text: []const u8) ?EdgesMode {
        if (std.mem.eql(u8, text, "provided")) return .provided;
        if (std.mem.eql(u8, text, "derived")) return .derived;
        if (std.mem.eql(u8, text, "merge")) return .merge;
        return null;
    }
};

pub const ContractRelation = struct {
    source_type: []const u8,
    target_type: []const u8,
    edge_label: []const u8,
    source_ref_column: []const u8,
};

pub const MappingContract = struct {
    workspace_id: []const u8,
    ontology_id: []const u8,
    source_tag: []const u8,
    contract_relations: []const ContractRelation,
    edges_mode: EdgesMode,

    pub fn deinit(self: MappingContract, allocator: std.mem.Allocator) void {
        allocator.free(self.workspace_id);
        allocator.free(self.ontology_id);
        allocator.free(self.source_tag);
        for (self.contract_relations) |rel| {
            allocator.free(rel.source_type);
            allocator.free(rel.target_type);
            allocator.free(rel.edge_label);
            allocator.free(rel.source_ref_column);
        }
        allocator.free(self.contract_relations);
    }
};

pub const CsvTable = struct {
    headers: []const []const u8,
    rows: []const []const []const u8,

    pub fn deinit(self: CsvTable, allocator: std.mem.Allocator) void {
        for (self.headers) |h| allocator.free(h);
        allocator.free(self.headers);
        for (self.rows) |row| {
            for (row) |cell_value| allocator.free(cell_value);
            allocator.free(row);
        }
        allocator.free(self.rows);
    }

    pub fn columnIndex(self: CsvTable, name: []const u8) ?usize {
        for (self.headers, 0..) |header, idx| {
            if (std.mem.eql(u8, header, name)) return idx;
        }
        return null;
    }

    pub fn cell(self: CsvTable, row_index: usize, column: []const u8) ?[]const u8 {
        const idx = self.columnIndex(column) orelse return null;
        if (row_index >= self.rows.len) return null;
        const row = self.rows[row_index];
        if (idx >= row.len) return null;
        return row[idx];
    }
};

pub fn readCsvFile(allocator: std.mem.Allocator, path: []const u8) !CsvTable {
    const content = try std.Io.Dir.cwd().readFileAlloc(
        zig16_compat.io(),
        path,
        allocator,
        .limited(64 * 1024 * 1024),
    );
    defer allocator.free(content);
    return try parseCsv(allocator, content);
}

pub const NamedTable = struct {
    name: []const u8,
    table: CsvTable,

    pub fn deinit(self: NamedTable, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        self.table.deinit(allocator);
    }
};

pub const TabularBundle = struct {
    tables: []NamedTable,

    pub fn deinit(self: TabularBundle, allocator: std.mem.Allocator) void {
        for (self.tables) |named| named.deinit(allocator);
        allocator.free(self.tables);
    }

    pub fn single(self: TabularBundle) !CsvTable {
        if (self.tables.len != 1) return error.InvalidToonTable;
        return self.tables[0].table;
    }
};

pub fn readTabularBundle(allocator: std.mem.Allocator, path: []const u8) !TabularBundle {
    if (std.mem.endsWith(u8, path, ".toon")) return readToonBundle(allocator, path);
    if (std.mem.endsWith(u8, path, ".json")) return readJsonTabularBundle(allocator, path);
    const table = try readCsvFile(allocator, path);
    const base = std.fs.path.basename(path);
    const name = try stripFileExtension(allocator, base);
    const named = try allocator.alloc(NamedTable, 1);
    named[0] = .{ .name = name, .table = table };
    return .{ .tables = named };
}

pub fn readTabularBundleFromMappingDir(
    allocator: std.mem.Allocator,
    mapping_path: []const u8,
    mapping: std.json.Value,
) !TabularBundle {
    if (mapping != .object) return error.InvalidMapping;
    const entities = mapping.object.get("entities") orelse return error.InvalidMapping;
    if (entities != .object) return error.InvalidMapping;
    const mapping_dir = std.fs.path.dirname(mapping_path) orelse ".";
    var tables = std.ArrayList(NamedTable).empty;
    errdefer {
        for (tables.items) |named| named.deinit(allocator);
        tables.deinit(allocator);
    }
    for (entities.object.keys()) |entity_name| {
        const entity_val = entities.object.get(entity_name) orelse continue;
        if (entity_val != .object) continue;
        const csv_rel = jsonStringValue(entity_val.object, "csv") orelse continue;
        const csv_path = try std.fs.path.join(allocator, &.{ mapping_dir, csv_rel });
        defer allocator.free(csv_path);
        const table = try readTableFile(allocator, csv_path);
        try tables.append(allocator, .{
            .name = try allocator.dupe(u8, entity_name),
            .table = table,
        });
    }
    if (tables.items.len == 0) return error.InvalidMapping;
    return .{ .tables = try tables.toOwnedSlice(allocator) };
}

pub fn readTableFile(allocator: std.mem.Allocator, path: []const u8) !CsvTable {
    const bundle = try readTabularBundle(allocator, path);
    defer bundle.deinit(allocator);
    return try cloneCsvTable(allocator, bundle.tables[0].table);
}

fn cloneCsvTable(allocator: std.mem.Allocator, source: CsvTable) !CsvTable {
    const headers = try allocator.alloc([]const u8, source.headers.len);
    errdefer {
        for (headers) |h| allocator.free(h);
        allocator.free(headers);
    }
    for (source.headers, 0..) |header, idx| headers[idx] = try allocator.dupe(u8, header);

    const rows = try allocator.alloc([]const []const u8, source.rows.len);
    errdefer {
        for (rows) |row| {
            for (row) |cell| allocator.free(cell);
            allocator.free(row);
        }
        allocator.free(rows);
    }
    for (source.rows, 0..) |source_row, row_idx| {
        const row = try allocator.alloc([]const u8, source_row.len);
        for (source_row, 0..) |cell, col_idx| row[col_idx] = try allocator.dupe(u8, cell);
        rows[row_idx] = row;
    }
    return .{ .headers = headers, .rows = rows };
}

fn stripFileExtension(allocator: std.mem.Allocator, filename: []const u8) ![]const u8 {
    if (std.mem.lastIndexOfScalar(u8, filename, '.')) |dot| {
        return try allocator.dupe(u8, filename[0..dot]);
    }
    return try allocator.dupe(u8, filename);
}

fn jsonStringValue(object: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const value = object.get(key) orelse return null;
    return switch (value) {
        .string => |text| text,
        else => null,
    };
}

fn readToonBundle(allocator: std.mem.Allocator, path: []const u8) !TabularBundle {
    const content = try std.Io.Dir.cwd().readFileAlloc(
        zig16_compat.io(),
        path,
        allocator,
        .limited(64 * 1024 * 1024),
    );
    defer allocator.free(content);
    const root = try ztoon.decodeAlloc(allocator, content, .{});
    defer ztoon.deinitValue(allocator, root);
    return try tabularBundleFromToonRoot(allocator, root);
}

fn readJsonTabularBundle(allocator: std.mem.Allocator, path: []const u8) !TabularBundle {
    const content = try readJsonFile(allocator, path);
    defer allocator.free(content);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, content, .{});
    defer parsed.deinit();
    return try tabularBundleFromJsonRoot(allocator, parsed.value);
}

fn dataPlaneFromMappingValue(mapping: std.json.Value) []const u8 {
    if (mapping == .object) {
        if (mapping.object.get("data_plane")) |plane| {
            if (plane == .string) return plane.string;
        }
    }
    return "import_ready";
}

fn tabularBundleFromJsonRoot(allocator: std.mem.Allocator, root: std.json.Value) !TabularBundle {
    switch (root) {
        .array => |items| {
            if (items.items.len == 0) return error.EmptyCsv;
            const table = try csvTableFromJsonRows(allocator, items.items);
            const named = try allocator.alloc(NamedTable, 1);
            named[0] = .{ .name = try allocator.dupe(u8, "default"), .table = table };
            return .{ .tables = named };
        },
        .object => |obj| {
            if (obj.get("tables")) |tables_val| {
                if (tables_val == .object) {
                    var out = std.ArrayList(NamedTable).empty;
                    errdefer {
                        for (out.items) |named| named.deinit(allocator);
                        out.deinit(allocator);
                    }
                    for (tables_val.object.keys()) |table_name| {
                        const table_val = tables_val.object.get(table_name) orelse continue;
                        const table = try csvTableFromHeadersRowsObject(allocator, table_val);
                        try out.append(allocator, .{
                            .name = try allocator.dupe(u8, table_name),
                            .table = table,
                        });
                    }
                    if (out.items.len == 0) return error.InvalidJsonTable;
                    return .{ .tables = try out.toOwnedSlice(allocator) };
                }
            }
            var out = std.ArrayList(NamedTable).empty;
            errdefer {
                for (out.items) |named| named.deinit(allocator);
                out.deinit(allocator);
            }
            for (obj.keys()) |key| {
                const value = obj.get(key) orelse continue;
                if (value != .array) continue;
                if (value.array.items.len == 0) continue;
                const first = value.array.items[0];
                if (first != .object) continue;
                const table = try csvTableFromJsonRows(allocator, value.array.items);
                try out.append(allocator, .{
                    .name = try allocator.dupe(u8, key),
                    .table = table,
                });
            }
            if (out.items.len == 0) return error.InvalidJsonTable;
            return .{ .tables = try out.toOwnedSlice(allocator) };
        },
        else => return error.InvalidJsonTable,
    }
}

fn csvTableFromHeadersRowsObject(allocator: std.mem.Allocator, value: std.json.Value) !CsvTable {
    if (value != .object) return error.InvalidJsonTable;
    const headers_val = value.object.get("headers") orelse return error.InvalidJsonTable;
    const rows_val = value.object.get("rows") orelse return error.InvalidJsonTable;
    if (headers_val != .array or rows_val != .array) return error.InvalidJsonTable;

    const headers = try allocator.alloc([]const u8, headers_val.array.items.len);
    errdefer {
        for (headers) |h| allocator.free(h);
        allocator.free(headers);
    }
    for (headers_val.array.items, 0..) |header, idx| {
        if (header != .string) return error.InvalidJsonTable;
        headers[idx] = try allocator.dupe(u8, header.string);
    }

    const rows = try allocator.alloc([]const []const u8, rows_val.array.items.len);
    errdefer {
        for (rows) |row| {
            for (row) |cell| allocator.free(cell);
            allocator.free(row);
        }
        allocator.free(rows);
    }
    for (rows_val.array.items, 0..) |row_val, row_idx| {
        if (row_val != .array) return error.InvalidJsonTable;
        const row = try allocator.alloc([]const u8, headers.len);
        errdefer allocator.free(row);
        for (0..headers.len) |col_idx| {
            const cell_val = if (col_idx < row_val.array.items.len) row_val.array.items[col_idx] else std.json.Value{ .string = "" };
            row[col_idx] = try jsonScalarToString(allocator, cell_val);
        }
        rows[row_idx] = row;
    }
    return .{ .headers = headers, .rows = rows };
}

fn csvTableFromJsonRows(allocator: std.mem.Allocator, rows_value: []const std.json.Value) !CsvTable {
    const first_obj = switch (rows_value[0]) {
        .object => |obj| obj,
        else => return error.InvalidJsonTable,
    };
    const header_keys = first_obj.keys();
    const headers = try allocator.alloc([]const u8, header_keys.len);
    errdefer {
        for (headers) |h| allocator.free(h);
        allocator.free(headers);
    }
    for (header_keys, 0..) |key, idx| headers[idx] = try allocator.dupe(u8, key);

    const rows = try allocator.alloc([]const []const u8, rows_value.len);
    errdefer {
        for (rows) |row| {
            for (row) |cell| allocator.free(cell);
            allocator.free(row);
        }
        allocator.free(rows);
    }
    for (rows_value, 0..) |row_value, row_idx| {
        const row_obj = switch (row_value) {
            .object => |obj| obj,
            else => return error.InvalidJsonTable,
        };
        const row = try allocator.alloc([]const u8, headers.len);
        errdefer allocator.free(row);
        for (headers, 0..) |header, col_idx| {
            const cell_val = row_obj.get(header) orelse std.json.Value{ .string = "" };
            row[col_idx] = try jsonScalarToString(allocator, cell_val);
        }
        rows[row_idx] = row;
    }
    return .{ .headers = headers, .rows = rows };
}

fn jsonScalarToString(allocator: std.mem.Allocator, value: std.json.Value) ![]const u8 {
    return switch (value) {
        .null => try allocator.dupe(u8, ""),
        .bool => |v| try std.fmt.allocPrint(allocator, "{}", .{v}),
        .integer => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
        .float => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
        .string => |v| try allocator.dupe(u8, v),
        else => error.UnsupportedJsonCell,
    };
}

fn tabularBundleFromToonRoot(allocator: std.mem.Allocator, root: ztoon.Value) !TabularBundle {
    const fields = switch (root) {
        .object => |items| items,
        else => return error.InvalidToonTable,
    };
    var out = std.ArrayList(NamedTable).empty;
    errdefer {
        for (out.items) |named| named.deinit(allocator);
        out.deinit(allocator);
    }
    for (fields) |field| {
        const rows_value = switch (field.value) {
            .array => |items| items,
            else => continue,
        };
        if (rows_value.len == 0) continue;
        const table = try csvTableFromToonRows(allocator, rows_value);
        try out.append(allocator, .{
            .name = try allocator.dupe(u8, field.key),
            .table = table,
        });
    }
    if (out.items.len == 0) return error.InvalidToonTable;
    return .{ .tables = try out.toOwnedSlice(allocator) };
}

fn csvTableFromToonRows(allocator: std.mem.Allocator, rows_value: []const ztoon.Value) !CsvTable {
    const first_row = switch (rows_value[0]) {
        .object => |items| items,
        else => return error.InvalidToonTable,
    };
    const headers = try allocator.alloc([]const u8, first_row.len);
    errdefer {
        for (headers) |header| allocator.free(header);
        allocator.free(headers);
    }
    for (first_row, 0..) |field, idx| headers[idx] = try allocator.dupe(u8, field.key);

    var rows = try allocator.alloc([]const []const u8, rows_value.len);
    errdefer {
        for (rows) |row| {
            for (row) |cell| allocator.free(cell);
            allocator.free(row);
        }
        allocator.free(rows);
    }
    for (rows_value, 0..) |row_value, row_idx| {
        const row_fields = switch (row_value) {
            .object => |items| items,
            else => return error.InvalidToonTable,
        };
        const row = try allocator.alloc([]const u8, headers.len);
        errdefer allocator.free(row);
        for (headers, 0..) |header, col_idx| {
            var cell_text: []const u8 = try allocator.dupe(u8, "");
            for (row_fields) |field| {
                if (std.mem.eql(u8, field.key, header)) {
                    allocator.free(cell_text);
                    cell_text = try scalarToString(allocator, field.value);
                    break;
                }
            }
            row[col_idx] = cell_text;
        }
        rows[row_idx] = row;
    }
    return .{ .headers = headers, .rows = rows };
}

fn readToonTableFile(allocator: std.mem.Allocator, path: []const u8) !CsvTable {
    const bundle = try readToonBundle(allocator, path);
    defer bundle.deinit(allocator);
    return try cloneCsvTable(allocator, bundle.tables[0].table);
}

fn csvTableFromToonRoot(allocator: std.mem.Allocator, root: ztoon.Value) !CsvTable {
    const bundle = try tabularBundleFromToonRoot(allocator, root);
    defer bundle.deinit(allocator);
    return try cloneCsvTable(allocator, bundle.tables[0].table);
}

fn scalarToString(allocator: std.mem.Allocator, value: ztoon.Value) ![]const u8 {
    return switch (value) {
        .null => try allocator.dupe(u8, ""),
        .bool => |v| try std.fmt.allocPrint(allocator, "{}", .{v}),
        .integer => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
        .float => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
        .string => |v| try allocator.dupe(u8, v),
        else => error.UnsupportedToonCell,
    };
}

fn parseCsv(allocator: std.mem.Allocator, content: []const u8) !CsvTable {
    var headers_list = std.ArrayList([]const u8).empty;
    errdefer {
        for (headers_list.items) |h| allocator.free(h);
        headers_list.deinit(allocator);
    }
    var rows_list = std.ArrayList([]const []const u8).empty;
    errdefer {
        for (rows_list.items) |row| {
            for (row) |cell_value| allocator.free(cell_value);
            allocator.free(row);
        }
        rows_list.deinit(allocator);
    }

    var line_iter = std.mem.splitScalar(u8, content, '\n');
    var first = true;
    while (line_iter.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, "\r");
        if (line.len == 0) continue;
        var fields = try parseCsvLine(allocator, line);
        defer {
            for (fields.items) |f| allocator.free(f);
            fields.deinit(allocator);
        }
        if (first) {
            first = false;
            for (fields.items) |f| try headers_list.append(allocator, try allocator.dupe(u8, f));
            continue;
        }
        const row = try allocator.alloc([]const u8, fields.items.len);
        for (fields.items, 0..) |f, i| row[i] = try allocator.dupe(u8, f);
        try rows_list.append(allocator, row);
    }
    if (headers_list.items.len == 0) return error.EmptyCsv;
    return .{
        .headers = try headers_list.toOwnedSlice(allocator),
        .rows = try rows_list.toOwnedSlice(allocator),
    };
}

fn parseCsvLine(allocator: std.mem.Allocator, line: []const u8) !std.ArrayList([]const u8) {
    var out = std.ArrayList([]const u8).empty;
    var i: usize = 0;
    while (i < line.len) {
        if (line[i] == '"') {
            i += 1;
            var start = i;
            var buf = std.ArrayList(u8).empty;
            defer buf.deinit(allocator);
            while (i < line.len) {
                if (line[i] == '"') {
                    if (i + 1 < line.len and line[i + 1] == '"') {
                        try buf.appendSlice(allocator, line[start..i]);
                        try buf.append(allocator, '"');
                        i += 2;
                        start = i;
                        continue;
                    }
                    try buf.appendSlice(allocator, line[start..i]);
                    i += 1;
                    break;
                }
                i += 1;
            }
            try out.append(allocator, try buf.toOwnedSlice(allocator));
            if (i < line.len and line[i] == ',') i += 1;
            continue;
        }
        const comma = std.mem.indexOfScalar(u8, line[i..], ',') orelse line.len - i;
        const end = i + comma;
        try out.append(allocator, try allocator.dupe(u8, std.mem.trim(u8, line[i..end], " \t")));
        i = if (comma == line.len - i) line.len else end + 1;
    }
    if (line.len > 0 and line[line.len - 1] == ',') {
        try out.append(allocator, try allocator.dupe(u8, ""));
    }
    return out;
}

pub fn readJsonFile(allocator: std.mem.Allocator, path: []const u8) ![]const u8 {
    return std.Io.Dir.cwd().readFileAlloc(
        zig16_compat.io(),
        path,
        allocator,
        .limited(32 * 1024 * 1024),
    );
}

pub fn readMappingContract(allocator: std.mem.Allocator, mapping_path: []const u8) !MappingContract {
    const mapping_text = try readJsonFile(allocator, mapping_path);
    defer allocator.free(mapping_text);
    var mapping = try std.json.parseFromSlice(std.json.Value, allocator, mapping_text, .{});
    defer mapping.deinit();

    const workspace_id = mapping.value.object.get("workspace_id") orelse return error.InvalidMapping;
    const ontology_id = mapping.value.object.get("ontology_id") orelse workspace_id;
    const source_tag_str: []const u8 = blk: {
        if (mapping.value.object.get("source_tag")) |tag| {
            if (tag == .string) break :blk tag.string;
        }
        break :blk SourceTag;
    };
    if (workspace_id != .string or ontology_id != .string) return error.InvalidMapping;

    var relations = std.ArrayList(ContractRelation).empty;
    errdefer {
        for (relations.items) |rel| {
            allocator.free(rel.source_type);
            allocator.free(rel.target_type);
            allocator.free(rel.edge_label);
            allocator.free(rel.source_ref_column);
        }
        relations.deinit(allocator);
    }

    if (mapping.value.object.get("contract_relations")) |raw| {
        if (raw == .array) {
            for (raw.array.items) |item| {
                if (item != .object) continue;
                const source_type = item.object.get("source_type") orelse continue;
                const target_type = item.object.get("target_type") orelse continue;
                const edge_label = item.object.get("edge_label") orelse continue;
                const source_ref_column = item.object.get("source_ref_column") orelse continue;
                if (source_type != .string or target_type != .string or edge_label != .string or source_ref_column != .string) continue;
                try relations.append(allocator, .{
                    .source_type = try allocator.dupe(u8, source_type.string),
                    .target_type = try allocator.dupe(u8, target_type.string),
                    .edge_label = try allocator.dupe(u8, edge_label.string),
                    .source_ref_column = try allocator.dupe(u8, source_ref_column.string),
                });
            }
        }
    }

    const edges_mode = blk: {
        if (mapping.value.object.get("edges_mode")) |mode| {
            if (mode == .string) {
                if (EdgesMode.parse(mode.string)) |parsed| break :blk parsed;
            }
        }
        break :blk EdgesMode.merge;
    };

    return .{
        .workspace_id = try allocator.dupe(u8, workspace_id.string),
        .ontology_id = try allocator.dupe(u8, ontology_id.string),
        .source_tag = try allocator.dupe(u8, source_tag_str),
        .contract_relations = try relations.toOwnedSlice(allocator),
        .edges_mode = edges_mode,
    };
}

pub fn validateBundle(allocator: std.mem.Allocator, model_path: []const u8, mapping_path: ?[]const u8, input_dir: ?[]const u8) !void {
    try validateBundleWithOptions(allocator, .{
        .model_path = model_path,
        .mapping_path = mapping_path,
        .input_dir = input_dir,
        .strict_drift = false,
        .strict_provenance = false,
    });
}

pub const ValidateBundleOptions = struct {
    model_path: []const u8,
    mapping_path: ?[]const u8 = null,
    input_dir: ?[]const u8 = null,
    strict_drift: bool = false,
    strict_provenance: bool = false,
    db_path: ?[]const u8 = null,
    workspace_id: ?[]const u8 = null,
};

pub fn validateBundleWithOptions(allocator: std.mem.Allocator, opts: ValidateBundleOptions) !void {
    const model_text = try readJsonFile(allocator, opts.model_path);
    defer allocator.free(model_text);
    var model = try std.json.parseFromSlice(std.json.Value, allocator, model_text, .{});
    defer model.deinit();

    const entity_types = model.value.object.get("entity_types") orelse return error.InvalidModel;
    if (entity_types != .array) return error.InvalidModel;

    var known = std.StringHashMap(void).init(allocator);
    defer known.deinit();
    for (entity_types.array.items) |item| {
        const name = item.object.get("name") orelse continue;
        if (name != .string) continue;
        try known.put(name.string, {});
    }

    var errors = std.ArrayList([]const u8).empty;
    defer {
        for (errors.items) |e| allocator.free(e);
        errors.deinit(allocator);
    }

    for (entity_types.array.items) |item| {
        const facets = item.object.get("facets") orelse continue;
        if (facets != .array) continue;
        const entity_name = item.object.get("name").?.string;
        for (facets.array.items) |facet| {
            const ftype = facet.object.get("type") orelse continue;
            if (ftype != .string) continue;
            if (!std.mem.eql(u8, ftype.string, "ref") and !std.mem.eql(u8, ftype.string, "ref_list")) continue;
            const target = facet.object.get("target") orelse continue;
            if (target != .string) continue;
            if (!known.contains(target.string)) {
                const msg = try std.fmt.allocPrint(allocator, "{s}.{s}: ref target '{s}' not in entity_types", .{
                    entity_name,
                    facet.object.get("name").?.string,
                    target.string,
                });
                try errors.append(allocator, msg);
            }
        }
    }

    if (opts.mapping_path) |mp| {
        const mapping_text = try readJsonFile(allocator, mp);
        defer allocator.free(mapping_text);
        var mapping = try std.json.parseFromSlice(std.json.Value, allocator, mapping_text, .{});
        defer mapping.deinit();
        if (mapping.value.object.get("workspace_id") == null) {
            try errors.append(allocator, try allocator.dupe(u8, "mapping: missing workspace_id"));
        }
    }

    if (opts.input_dir) |dir| {
        const facets_path = try std.fmt.allocPrint(allocator, "{s}/import_ready/mfo_facets_import.csv", .{dir});
        defer allocator.free(facets_path);
        const facets = readCsvFile(allocator, facets_path) catch |err| switch (err) {
            error.FileNotFound => blk: {
                const alt = try std.fmt.allocPrint(allocator, "{s}/mfo_facets_import.csv", .{dir});
                defer allocator.free(alt);
                break :blk try readCsvFile(allocator, alt);
            },
            else => return err,
        };
        defer facets.deinit(allocator);

        var seen = std.StringHashMap(void).init(allocator);
        defer seen.deinit();
        for (facets.rows, 0..) |_, row_idx| {
            const source_ref = facets.cell(row_idx, "source_ref") orelse {
                try errors.append(allocator, try std.fmt.allocPrint(allocator, "facets row {d}: missing source_ref", .{row_idx + 1}));
                continue;
            };
            if (seen.contains(source_ref)) {
                try errors.append(allocator, try std.fmt.allocPrint(allocator, "duplicate source_ref: {s}", .{source_ref}));
            } else {
                try seen.put(try allocator.dupe(u8, source_ref), {});
            }
        }
    }

    if (errors.items.len > 0) {
        for (errors.items) |e| std.log.err("{s}", .{e});
        return error.ValidationFailed;
    }

    if (opts.strict_drift) {
        const drift = @import("structured_import_drift.zig");
        try drift.validateDriftOrFail(allocator, .{
            .model_path = opts.model_path,
            .mapping_path = opts.mapping_path,
            .input_dir = opts.input_dir,
            .db_path = opts.db_path,
            .workspace_id = opts.workspace_id,
            .strict = true,
        });
    }

    if (opts.strict_provenance) {
        if (opts.db_path == null or opts.workspace_id == null) return error.InvalidArguments;
        var db = try facet_sqlite.Database.open(opts.db_path.?);
        defer db.close();
        const report = try validateImportProvenance(db, opts.workspace_id.?, SourceTag, true);
        if (report.provenance_without_table_id > 0) {
            std.log.err("provenance rows without table_id: {d}", .{report.provenance_without_table_id});
            return error.ValidationFailed;
        }
    }
}

pub const ApplyOptions = struct {
    workspace_id: []const u8,
    ontology_id: []const u8,
    mode: ImportMode = .append,
    source_tag: []const u8 = SourceTag,
    facets_path: ?[]const u8 = null,
    edges_path: ?[]const u8 = null,
    mapping: ?MappingContract = null,
    mapping_path: ?[]const u8 = null,
    edges_mode: EdgesMode = .merge,
    created_by: []const u8 = "structured-import",
    require_semantics: bool = false,
    data_plane: []const u8 = "import_ready",
};

const ApplyBatch = struct {
    fact_exists_stmt: *facet_sqlite.c.sqlite3_stmt,
    provenance_stmt: *facet_sqlite.c.sqlite3_stmt,
    relation_exists_stmt: *facet_sqlite.c.sqlite3_stmt,

    fn init(db: facet_sqlite.Database) !ApplyBatch {
        const fact_exists_stmt = try facet_sqlite.prepare(db, "SELECT 1 FROM agent_facts WHERE workspace_id = ?1 AND source_ref = ?2 LIMIT 1");
        errdefer facet_sqlite.finalize(fact_exists_stmt);
        const provenance_stmt = try facet_sqlite.prepare(db,
            \\INSERT OR REPLACE INTO structured_import_provenance(
            \\  workspace_id, source_ref, source_tag, table_id, row_fingerprint,
            \\  fact_id, entity_external_id, relation_external_ids_json, updated_at
            \\) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, CURRENT_TIMESTAMP)
        );
        errdefer facet_sqlite.finalize(provenance_stmt);
        const relation_exists_stmt = try facet_sqlite.prepare(db, "SELECT 1 FROM relations_raw WHERE workspace_id = ?1 AND external_id = ?2 LIMIT 1");
        return .{
            .fact_exists_stmt = fact_exists_stmt,
            .provenance_stmt = provenance_stmt,
            .relation_exists_stmt = relation_exists_stmt,
        };
    }

    fn deinit(self: ApplyBatch) void {
        facet_sqlite.finalize(self.fact_exists_stmt);
        facet_sqlite.finalize(self.provenance_stmt);
        facet_sqlite.finalize(self.relation_exists_stmt);
    }

    fn factExistsBySourceRef(self: *ApplyBatch, workspace_id: []const u8, source_ref: []const u8) bool {
        facet_sqlite.resetStatement(self.fact_exists_stmt) catch return false;
        facet_sqlite.bindText(self.fact_exists_stmt, 1, workspace_id) catch return false;
        facet_sqlite.bindText(self.fact_exists_stmt, 2, source_ref) catch return false;
        return facet_sqlite.c.sqlite3_step(self.fact_exists_stmt) == facet_sqlite.c.SQLITE_ROW;
    }

    fn relationExistsByExternalId(self: *ApplyBatch, workspace_id: []const u8, external_id: []const u8) bool {
        facet_sqlite.resetStatement(self.relation_exists_stmt) catch return false;
        facet_sqlite.bindText(self.relation_exists_stmt, 1, workspace_id) catch return false;
        facet_sqlite.bindText(self.relation_exists_stmt, 2, external_id) catch return false;
        return facet_sqlite.c.sqlite3_step(self.relation_exists_stmt) == facet_sqlite.c.SQLITE_ROW;
    }

    fn upsertProvenance(self: *ApplyBatch, row: ProvenanceUpsert) !void {
        try facet_sqlite.resetStatement(self.provenance_stmt);
        try bindProvenanceRow(self.provenance_stmt, row);
        try facet_sqlite.stepDone(self.provenance_stmt);
    }
};

pub const FacetRecord = struct {
    source_ref: []const u8,
    schema_id: []const u8,
    content: []const u8,
    facets_json_raw: []const u8,
    table_id: ?u64 = null,
};

pub fn readDataPlane(mapping_path: []const u8, allocator: std.mem.Allocator) ![]const u8 {
    const mapping_text = try readJsonFile(allocator, mapping_path);
    defer allocator.free(mapping_text);
    var mapping = try std.json.parseFromSlice(std.json.Value, allocator, mapping_text, .{});
    defer mapping.deinit();
    return try allocator.dupe(u8, dataPlaneFromMappingValue(mapping.value));
}

fn applyFacetRecord(
    allocator: std.mem.Allocator,
    db: facet_sqlite.Database,
    effective: ApplyOptions,
    record: FacetRecord,
    contract_relations: []const ContractRelation,
    report: *ApplyReport,
    batch: ?*ApplyBatch,
) !void {
    const workspace_id = effective.workspace_id;

    const fact_exists = if (batch) |b|
        b.factExistsBySourceRef(workspace_id, record.source_ref)
    else
        factExistsBySourceRef(db, workspace_id, record.source_ref);
    if (effective.mode == .ignore_duplicates and fact_exists) {
        report.facets_skipped += 1;
        return;
    }

    const facets_json = try normalizeFacetsJsonSource(allocator, record.facets_json_raw, effective.source_tag);
    defer allocator.free(facets_json);

    const stable_id = try stableFactId(allocator, workspace_id, record.source_ref);
    defer allocator.free(stable_id);

    const result = try facts_sqlite.writeFactWithOptions(db, allocator, .{
        .id = stable_id,
        .workspace_id = workspace_id,
        .schema_id = record.schema_id,
        .content = record.content,
        .facets_json = facets_json,
        .created_by = effective.created_by,
        .source_ref = record.source_ref,
    }, .{
        .manage_transaction = batch == null,
    });
    defer facts_sqlite.deinitFactWriteResult(allocator, result);
    if (result.created) report.facets_inserted += 1 else report.facets_updated += 1;

    const entity_type = try entityTypeFromSourceRef(allocator, workspace_id, record.source_ref);
    defer allocator.free(entity_type);
    const entity_metadata = try mergeImportMetadata(allocator, facets_json, effective.source_tag, record.source_ref, null);
    defer allocator.free(entity_metadata);
    _ = try collections_sqlite.upsertEntityRawAuto(db, .{
        .workspace_id = workspace_id,
        .ontology_id = effective.ontology_id,
        .external_id = record.source_ref,
        .entity_type = entity_type,
        .name = record.content,
        .confidence = 0.9,
        .metadata_json = entity_metadata,
    });
    report.entities_upserted += 1;

    if (effective.edges_mode != .provided) {
        const derived = try deriveEdgesFromFacetRow(allocator, db, .{
            .workspace_id = workspace_id,
            .ontology_id = effective.ontology_id,
            .source_tag = effective.source_tag,
            .source_ref = record.source_ref,
            .facets_json = facets_json,
            .contract_relations = contract_relations,
            .mode = effective.mode,
            .batch = batch,
        });
        report.edges_inserted += derived.inserted;
        report.edges_skipped += derived.skipped;
    }

    const fingerprint = try rowFingerprint(allocator, record.source_ref, facets_json);
    defer allocator.free(fingerprint);
    const table_id = record.table_id orelse lookupTableIdForSourceRef(db, workspace_id, record.source_ref);
    if (effective.require_semantics and table_id == null) {
        const local_type = entityLocalTypeFromSourceRef(record.source_ref) orelse "unknown";
        std.log.err("missing table_semantics for entity '{s}' (source_ref={s}); run register-semantics before apply", .{ local_type, record.source_ref });
        return error.MissingTableSemantics;
    }
    try upsertProvenance(db, .{
        .workspace_id = workspace_id,
        .source_ref = record.source_ref,
        .source_tag = effective.source_tag,
        .table_id = table_id,
        .row_fingerprint = fingerprint,
        .fact_id = stable_id,
        .entity_external_id = record.source_ref,
    }, batch);
}

const TableSemanticsRow = struct {
    table_id: u64,
    table_name: []const u8,
    key_column: []const u8,
    content_column: []const u8,
    schema_id: []const u8,
    ontology: []const u8,
};

fn wsTableName(allocator: std.mem.Allocator, entity_name: []const u8) ![]const u8 {
    return try std.fmt.allocPrint(allocator, "ws_{s}", .{entity_name});
}

fn parseNotesJsonField(allocator: std.mem.Allocator, notes: []const u8, field: []const u8) !?[]const u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, notes, .{ .ignore_unknown_fields = true }) catch return null;
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const value = parsed.value.object.get(field) orelse return null;
    if (value != .string) return null;
    return try allocator.dupe(u8, value.string);
}

fn loadTableSemanticsForEntity(
    allocator: std.mem.Allocator,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    entity_name: []const u8,
    mapping_entity: ?std.json.Value,
) !TableSemanticsRow {
    const sql =
        \\SELECT ts.table_id, cs.table_name, ts.key_column, ts.content_column, ts.notes
        \\FROM column_semantics cs
        \\JOIN table_semantics ts ON ts.table_id = cs.table_id AND ts.workspace_id = cs.workspace_id
        \\WHERE cs.workspace_id = ?1 AND cs.table_schema = 'structured' AND cs.table_name = ?2
        \\LIMIT 1
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, entity_name);
    const rc = facet_sqlite.c.sqlite3_step(stmt);
    if (rc != facet_sqlite.c.SQLITE_ROW) return error.MissingTableSemantics;

    const table_id = try facet_sqlite.columnU64(stmt, 0);
    const table_name = try facet_sqlite.dupeColumnText(allocator, stmt, 1);
    const key_column = try facet_sqlite.dupeColumnText(allocator, stmt, 2);
    const content_column = try facet_sqlite.dupeColumnText(allocator, stmt, 3);
    const notes_owned = blk: {
        const ptr = facet_sqlite.c.sqlite3_column_text(stmt, 4);
        if (ptr == null) break :blk try allocator.dupe(u8, "");
        const len = facet_sqlite.c.sqlite3_column_bytes(stmt, 4);
        const bytes: []const u8 = @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
        break :blk try allocator.dupe(u8, bytes);
    };
    defer allocator.free(notes_owned);
    const notes = notes_owned;

    var schema_id = parseNotesJsonField(allocator, notes, "schema_id") catch null;
    if (schema_id == null and mapping_entity != null and mapping_entity.? == .object) {
        if (mapping_entity.?.object.get("schema_id")) |sid| {
            if (sid == .string) schema_id = try allocator.dupe(u8, sid.string);
        }
    }
    if (schema_id == null) {
        allocator.free(table_name);
        allocator.free(key_column);
        allocator.free(content_column);
        return error.MissingTableSemantics;
    }

    const ontology = parseNotesJsonField(allocator, notes, "entity_family") catch null orelse
        (try allocator.dupe(u8, "core"));

    return .{
        .table_id = table_id,
        .table_name = table_name,
        .key_column = key_column,
        .content_column = content_column,
        .schema_id = schema_id.?,
        .ontology = ontology,
    };
}

fn findColumnValue(columns: []const []const u8, values: []const []const u8, name: []const u8) ?[]const u8 {
    for (columns, 0..) |col, idx| {
        if (std.mem.eql(u8, col, name)) return values[idx];
    }
    return null;
}

fn buildFacetsJsonFromWsRow(
    allocator: std.mem.Allocator,
    entity_name: []const u8,
    ontology: []const u8,
    record_id: []const u8,
    source_tag: []const u8,
    columns: []const []const u8,
    column_index: *const std.StringHashMap(usize),
    values: []const []const u8,
) ![]const u8 {
    var buf: std.Io.Writer.Allocating = .init(allocator);
    try buf.writer.writeAll("{");
    var wrote_any = false;

    for (columns, values) |col, val| {
        if (wrote_any) try buf.writer.writeAll(",");
        wrote_any = true;
        try buf.writer.print("\"{s}\":", .{col});
        try buf.writer.print("{f}", .{std.json.fmt(val, .{})});
    }

    const injects = [_]struct { key: []const u8, value: []const u8 }{
        .{ .key = "entity_type", .value = entity_name },
        .{ .key = "ontology", .value = ontology },
        .{ .key = "record_id", .value = record_id },
        .{ .key = "source", .value = source_tag },
    };
    for (injects) |item| {
        if (column_index.contains(item.key)) continue;
        if (wrote_any) try buf.writer.writeAll(",");
        wrote_any = true;
        try buf.writer.print("\"{s}\":", .{item.key});
        try buf.writer.print("{f}", .{std.json.fmt(item.value, .{})});
    }

    try buf.writer.writeAll("}");
    return buf.toOwnedSlice();
}

fn findColumnValueIndexed(column_index: *const std.StringHashMap(usize), values: []const []const u8, name: []const u8) ?[]const u8 {
    const idx = column_index.get(name) orelse return null;
    if (idx >= values.len) return null;
    return values[idx];
}

const WsApplyContext = struct {
    db: facet_sqlite.Database,
    effective: ApplyOptions,
    entity_name: []const u8,
    semantics: *const TableSemanticsRow,
    contract_relations: []const ContractRelation,
    batch: ?*ApplyBatch,
    report: *ApplyReport,
};

fn forEachWsTableRow(
    allocator: std.mem.Allocator,
    db: facet_sqlite.Database,
    ws_name: []const u8,
    ctx: *WsApplyContext,
) !void {
    const sql = try std.fmt.allocPrint(allocator, "SELECT * FROM {s}", .{ws_name});
    defer allocator.free(sql);

    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);

    const col_count: usize = @intCast(facet_sqlite.c.sqlite3_column_count(stmt));
    if (col_count == 0) return error.WsTableEmpty;

    const columns = try allocator.alloc([]const u8, col_count);
    errdefer allocator.free(columns);
    for (0..col_count) |idx| {
        const name = facet_sqlite.c.sqlite3_column_name(stmt, @intCast(idx)) orelse return error.StepFailed;
        columns[idx] = try allocator.dupe(u8, std.mem.span(name));
    }
    defer {
        for (columns) |col| allocator.free(col);
        allocator.free(columns);
    }

    var column_index = std.StringHashMap(usize).init(allocator);
    defer column_index.deinit();
    for (columns, 0..) |col, idx| {
        try column_index.put(col, idx);
    }

    while (true) {
        const rc = facet_sqlite.c.sqlite3_step(stmt);
        if (rc == facet_sqlite.c.SQLITE_DONE) break;
        if (rc != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;
        const row = try allocator.alloc([]const u8, col_count);
        errdefer allocator.free(row);
        for (0..col_count) |idx| {
            const text = blk: {
                const ptr = facet_sqlite.c.sqlite3_column_text(stmt, @intCast(idx));
                if (ptr == null) break :blk try allocator.dupe(u8, "");
                const len = facet_sqlite.c.sqlite3_column_bytes(stmt, @intCast(idx));
                const bytes: []const u8 = @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
                break :blk try allocator.dupe(u8, bytes);
            };
            row[idx] = text;
        }
        defer {
            for (row) |cell| allocator.free(cell);
            allocator.free(row);
        }
        try applyWsRow(allocator, ctx, columns, &column_index, row);
    }
}

fn applyWsRow(
    allocator: std.mem.Allocator,
    ctx: *WsApplyContext,
    columns: []const []const u8,
    column_index: *const std.StringHashMap(usize),
    row_values: []const []const u8,
) !void {
    const semantics = ctx.semantics.*;
    const record_id = findColumnValueIndexed(column_index, row_values, semantics.key_column) orelse return error.InvalidWsRow;
    const content = findColumnValueIndexed(column_index, row_values, semantics.content_column) orelse record_id;
    const source_ref = try expandEntityExternalId(allocator, ctx.entity_name, record_id);
    defer allocator.free(source_ref);

    const facets_json_raw = try buildFacetsJsonFromWsRow(
        allocator,
        ctx.entity_name,
        semantics.ontology,
        record_id,
        ctx.effective.source_tag,
        columns,
        column_index,
        row_values,
    );
    defer allocator.free(facets_json_raw);

    try applyFacetRecord(allocator, ctx.db, ctx.effective, .{
        .source_ref = source_ref,
        .schema_id = semantics.schema_id,
        .content = content,
        .facets_json_raw = facets_json_raw,
        .table_id = semantics.table_id,
    }, ctx.contract_relations, ctx.report, ctx.batch);
}

fn applyFromWsTables(
    allocator: std.mem.Allocator,
    db: facet_sqlite.Database,
    effective: ApplyOptions,
    contract_relations: []const ContractRelation,
    mapping: std.json.Value,
    batch: ?*ApplyBatch,
) !ApplyReport {
    if (mapping != .object) return error.InvalidMapping;
    const entities = mapping.object.get("entities") orelse return error.InvalidMapping;
    if (entities != .object) return error.InvalidMapping;

    var report = ApplyReport{};
    var entity_idx: usize = 0;
    const entity_keys = entities.object.keys();
    while (entity_idx < entity_keys.len) : (entity_idx += 1) {
        const entity_name = entity_keys[entity_idx];
        const entity_val = entities.object.get(entity_name) orelse continue;
        if (entity_val != .object) continue;

        const semantics = try loadTableSemanticsForEntity(allocator, db, effective.workspace_id, entity_name, entity_val);
        defer {
            allocator.free(semantics.table_name);
            allocator.free(semantics.key_column);
            allocator.free(semantics.content_column);
            allocator.free(semantics.schema_id);
            allocator.free(semantics.ontology);
        }

        const ws_name = try wsTableName(allocator, entity_name);
        defer allocator.free(ws_name);

        var ctx = WsApplyContext{
            .db = db,
            .effective = effective,
            .entity_name = entity_name,
            .semantics = &semantics,
            .contract_relations = contract_relations,
            .batch = batch,
            .report = &report,
        };
        forEachWsTableRow(allocator, db, ws_name, &ctx) catch |err| switch (err) {
            error.StepFailed, error.WsTableEmpty => return error.WsTableMissing,
            else => |e| return e,
        };
    }

    if (report.entities_upserted == 0 and report.facets_inserted + report.facets_updated == 0) {
        return error.WsTableEmpty;
    }

    return report;
}

pub fn applyImportReady(allocator: std.mem.Allocator, db: facet_sqlite.Database, opts: ApplyOptions) !ApplyReport {
    var effective = opts;
    if (opts.mapping) |mapping| {
        effective.source_tag = mapping.source_tag;
        effective.edges_mode = mapping.edges_mode;
    }

    var mapping_text_owned: ?[]const u8 = null;
    defer if (mapping_text_owned) |text| allocator.free(text);
    var mapping_parsed: ?std.json.Parsed(std.json.Value) = null;
    defer if (mapping_parsed) |*parsed| parsed.deinit();
    var mapping_value: ?std.json.Value = null;
    var data_plane = effective.data_plane;
    if (opts.mapping_path) |mp| {
        mapping_text_owned = try readJsonFile(allocator, mp);
        mapping_parsed = try std.json.parseFromSlice(std.json.Value, allocator, mapping_text_owned.?, .{});
        mapping_value = mapping_parsed.?.value;
        data_plane = dataPlaneFromMappingValue(mapping_value.?);
    }

    try collections_sqlite.ensureWorkspace(db, .{
        .workspace_id = effective.workspace_id,
        .label = effective.workspace_id,
    });
    try collections_sqlite.ensureOntology(db, .{
        .ontology_id = effective.ontology_id,
        .workspace_id = effective.workspace_id,
        .name = effective.ontology_id,
    });

    const contract_relations = if (opts.mapping) |mapping| mapping.contract_relations else &[_]ContractRelation{};

    if (std.mem.eql(u8, data_plane, "ws")) {
        const map = mapping_value orelse return error.InvalidMapping;
        try db.exec("BEGIN IMMEDIATE");
        var transaction_active = true;
        errdefer if (transaction_active) {
            db.exec("ROLLBACK") catch |rollback_err| {
                std.log.warn("structured import rollback failed: {s}", .{@errorName(rollback_err)});
            };
        };
        var batch = try ApplyBatch.init(db);
        defer batch.deinit();
        if (effective.mode == .reset) try purgeTaggedImport(db, effective.workspace_id, effective.source_tag);
        const report = try applyFromWsTables(allocator, db, effective, contract_relations, map, &batch);
        try db.exec("COMMIT");
        transaction_active = false;
        return report;
    }

    const facets_path = effective.facets_path orelse return error.InvalidFacetsCsv;

    var facets = try readCsvFile(allocator, facets_path);
    defer facets.deinit(allocator);

    var edges_table: ?CsvTable = null;
    defer if (edges_table) |*t| t.deinit(allocator);
    const use_provided_edges = effective.edges_mode != .derived;
    if (use_provided_edges) {
        if (effective.edges_path) |ep| {
            edges_table = try readCsvFile(allocator, ep);
        }
    }

    try db.exec("BEGIN IMMEDIATE");
    var transaction_active = true;
    errdefer if (transaction_active) {
        db.exec("ROLLBACK") catch |rollback_err| {
            std.log.warn("structured import rollback failed: {s}", .{@errorName(rollback_err)});
        };
    };
    var batch = try ApplyBatch.init(db);
    defer batch.deinit();
    if (effective.mode == .reset) try purgeTaggedImport(db, effective.workspace_id, effective.source_tag);

    var report = ApplyReport{};
    for (facets.rows, 0..) |_, row_idx| {
        const workspace_id = facets.cell(row_idx, "workspace_id") orelse return error.InvalidFacetsCsv;
        if (!std.mem.eql(u8, workspace_id, effective.workspace_id)) return error.WorkspaceMismatch;
        const source_ref = facets.cell(row_idx, "source_ref") orelse return error.InvalidFacetsCsv;
        const schema_id = facets.cell(row_idx, "schema_id") orelse return error.InvalidFacetsCsv;
        const content = facets.cell(row_idx, "content") orelse return error.InvalidFacetsCsv;
        const facets_json_raw = facets.cell(row_idx, "facets") orelse return error.InvalidFacetsCsv;

        try applyFacetRecord(allocator, db, effective, .{
            .source_ref = source_ref,
            .schema_id = schema_id,
            .content = content,
            .facets_json_raw = facets_json_raw,
        }, contract_relations, &report, &batch);
    }

    if (edges_table) |edges| {
        for (edges.rows, 0..) |_, row_idx| {
            const inserted = try applyEdgeRow(allocator, db, edges, row_idx, effective, &batch);
            if (inserted) report.edges_inserted += 1 else report.edges_skipped += 1;
        }
    }

    try db.exec("COMMIT");
    transaction_active = false;
    return report;
}

const ApplyEdgeOutcome = struct {
    inserted: u64 = 0,
    skipped: u64 = 0,
};

const DeriveEdgeContext = struct {
    workspace_id: []const u8,
    ontology_id: []const u8,
    source_tag: []const u8,
    source_ref: []const u8,
    facets_json: []const u8,
    contract_relations: []const ContractRelation,
    mode: ImportMode,
    batch: ?*ApplyBatch = null,
};

fn applyEdgeRow(allocator: std.mem.Allocator, db: facet_sqlite.Database, edges: CsvTable, row_idx: usize, opts: ApplyOptions, batch: ?*ApplyBatch) !bool {
    const workspace_id = edges.cell(row_idx, "workspace_id") orelse return error.InvalidEdgesCsv;
    const source = edges.cell(row_idx, "source") orelse return error.InvalidEdgesCsv;
    const target = edges.cell(row_idx, "target") orelse return error.InvalidEdgesCsv;
    const label = edges.cell(row_idx, "label") orelse return error.InvalidEdgesCsv;
    const confidence_raw = edges.cell(row_idx, "confidence") orelse "0.85";
    const confidence = std.fmt.parseFloat(f64, confidence_raw) catch 0.85;
    const metadata_raw = edges.cell(row_idx, "metadata_json") orelse "{}";

    const metadata = try mergeImportMetadata(allocator, metadata_raw, opts.source_tag, source, target);
    defer allocator.free(metadata);

    const source_type = try entityTypeFromSourceRef(allocator, opts.workspace_id, source);
    defer allocator.free(source_type);
    const target_type = try entityTypeFromSourceRef(allocator, opts.workspace_id, target);
    defer allocator.free(target_type);

    if (opts.mode == .ignore_duplicates) {
        const ext = try edgeExternalId(allocator, source, label, target);
        defer allocator.free(ext);
        const exists = if (batch) |b| b.relationExistsByExternalId(workspace_id, ext) else relationExistsByExternalId(db, workspace_id, ext);
        if (exists) return false;
    }

    const source_id = try collections_sqlite.upsertEntityRawAuto(db, .{
        .workspace_id = workspace_id,
        .ontology_id = opts.ontology_id,
        .external_id = source,
        .entity_type = source_type,
        .name = source,
        .confidence = confidence,
        .metadata_json = metadata,
    });
    const target_id = try collections_sqlite.upsertEntityRawAuto(db, .{
        .workspace_id = workspace_id,
        .ontology_id = opts.ontology_id,
        .external_id = target,
        .entity_type = target_type,
        .name = target,
        .confidence = confidence,
        .metadata_json = metadata,
    });

    const edge_ext = try edgeExternalId(allocator, source, label, target);
    defer allocator.free(edge_ext);
    _ = try collections_sqlite.upsertRelationRawAuto(db, .{
        .workspace_id = workspace_id,
        .ontology_id = opts.ontology_id,
        .external_id = edge_ext,
        .edge_type = label,
        .source_entity_id = source_id,
        .target_entity_id = target_id,
        .confidence = confidence,
        .metadata_json = metadata,
    });
    return true;
}

fn deriveEdgesFromFacetRow(allocator: std.mem.Allocator, db: facet_sqlite.Database, ctx: DeriveEdgeContext) !ApplyEdgeOutcome {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, ctx.facets_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return .{};

    const entity_type_value = parsed.value.object.get("entity_type") orelse return .{};
    if (entity_type_value != .string) return .{};
    const current_entity_type = entity_type_value.string;

    var out = ApplyEdgeOutcome{};
    for (ctx.contract_relations) |rel| {
        if (!std.mem.eql(u8, rel.target_type, current_entity_type)) continue;
        const ref_value = parsed.value.object.get(rel.source_ref_column) orelse continue;
        if (ref_value != .string or ref_value.string.len == 0) continue;

        const source_external = try expandEntityExternalId(allocator, rel.source_type, ref_value.string);
        defer allocator.free(source_external);
        const target_external = try allocator.dupe(u8, ctx.source_ref);
        defer allocator.free(target_external);

        const metadata = try mergeImportMetadata(allocator, ctx.facets_json, ctx.source_tag, source_external, target_external);
        defer allocator.free(metadata);

        const edge_ext = try edgeExternalId(allocator, source_external, rel.edge_label, target_external);
        defer allocator.free(edge_ext);
        const relation_exists = if (ctx.batch) |batch|
            batch.relationExistsByExternalId(ctx.workspace_id, edge_ext)
        else
            relationExistsByExternalId(db, ctx.workspace_id, edge_ext);
        if (ctx.mode == .ignore_duplicates and relation_exists) {
            out.skipped += 1;
            continue;
        }

        const source_type = try entityTypeFromSourceRef(allocator, ctx.workspace_id, source_external);
        defer allocator.free(source_type);
        const target_type = try entityTypeFromSourceRef(allocator, ctx.workspace_id, target_external);
        defer allocator.free(target_type);

        const source_id = try collections_sqlite.upsertEntityRawAuto(db, .{
            .workspace_id = ctx.workspace_id,
            .ontology_id = ctx.ontology_id,
            .external_id = source_external,
            .entity_type = source_type,
            .name = source_external,
            .confidence = 0.85,
            .metadata_json = metadata,
        });
        const target_id = try collections_sqlite.upsertEntityRawAuto(db, .{
            .workspace_id = ctx.workspace_id,
            .ontology_id = ctx.ontology_id,
            .external_id = target_external,
            .entity_type = target_type,
            .name = target_external,
            .confidence = 0.85,
            .metadata_json = metadata,
        });
        _ = try collections_sqlite.upsertRelationRawAuto(db, .{
            .workspace_id = ctx.workspace_id,
            .ontology_id = ctx.ontology_id,
            .external_id = edge_ext,
            .edge_type = rel.edge_label,
            .source_entity_id = source_id,
            .target_entity_id = target_id,
            .confidence = 0.85,
            .metadata_json = metadata,
        });
        out.inserted += 1;
    }
    return out;
}

pub const ProvenanceUpsert = struct {
    workspace_id: []const u8,
    source_ref: []const u8,
    source_tag: []const u8,
    table_id: ?u64 = null,
    row_fingerprint: ?[]const u8 = null,
    fact_id: ?[]const u8 = null,
    entity_external_id: ?[]const u8 = null,
    relation_external_ids_json: []const u8 = "[]",
};

fn bindProvenanceRow(stmt: *facet_sqlite.c.sqlite3_stmt, row: ProvenanceUpsert) !void {
    try facet_sqlite.bindText(stmt, 1, row.workspace_id);
    try facet_sqlite.bindText(stmt, 2, row.source_ref);
    try facet_sqlite.bindText(stmt, 3, row.source_tag);
    if (row.table_id) |table_id| {
        try facet_sqlite.bindInt64(stmt, 4, table_id);
    } else {
        try facet_sqlite.bindNull(stmt, 4);
    }
    if (row.row_fingerprint) |fp| try facet_sqlite.bindText(stmt, 5, fp) else try facet_sqlite.bindNull(stmt, 5);
    if (row.fact_id) |fact_id| try facet_sqlite.bindText(stmt, 6, fact_id) else try facet_sqlite.bindNull(stmt, 6);
    if (row.entity_external_id) |entity_id| try facet_sqlite.bindText(stmt, 7, entity_id) else try facet_sqlite.bindNull(stmt, 7);
    try facet_sqlite.bindText(stmt, 8, row.relation_external_ids_json);
}

fn upsertProvenance(db: facet_sqlite.Database, row: ProvenanceUpsert, batch: ?*ApplyBatch) !void {
    if (batch) |b| {
        try b.upsertProvenance(row);
        return;
    }
    const sql =
        \\INSERT OR REPLACE INTO structured_import_provenance(
        \\  workspace_id, source_ref, source_tag, table_id, row_fingerprint,
        \\  fact_id, entity_external_id, relation_external_ids_json, updated_at
        \\) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, CURRENT_TIMESTAMP)
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try bindProvenanceRow(stmt, row);
    try facet_sqlite.stepDone(stmt);
}

fn rowFingerprint(allocator: std.mem.Allocator, source_ref: []const u8, facets_json: []const u8) ![]const u8 {
    var hash: [32]u8 = undefined;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(source_ref);
    hasher.update(":");
    hasher.update(facets_json);
    hasher.final(&hash);
    var hex_buf: [64]u8 = undefined;
    for (hash, 0..) |byte, i| {
        _ = std.fmt.bufPrint(hex_buf[i * 2 ..][0..2], "{x:0>2}", .{byte}) catch unreachable;
    }
    return try std.fmt.allocPrint(allocator, "{s}", .{hex_buf});
}

fn mergeImportMetadata(
    allocator: std.mem.Allocator,
    raw: []const u8,
    tag: []const u8,
    source_endpoint: []const u8,
    target_endpoint: ?[]const u8,
) ![]const u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, raw, .{ .ignore_unknown_fields = true }) catch {
        if (target_endpoint) |target| {
            return try std.fmt.allocPrint(allocator, "{{\"source\":\"{s}\",\"source_endpoint\":\"{s}\",\"target_endpoint\":\"{s}\"}}", .{ tag, source_endpoint, target });
        }
        return try std.fmt.allocPrint(allocator, "{{\"source\":\"{s}\",\"source_endpoint\":\"{s}\"}}", .{ tag, source_endpoint });
    };
    defer parsed.deinit();

    if (parsed.value != .object) {
        if (target_endpoint) |target| {
            return try std.fmt.allocPrint(allocator, "{{\"source\":\"{s}\",\"source_endpoint\":\"{s}\",\"target_endpoint\":\"{s}\"}}", .{ tag, source_endpoint, target });
        }
        return try std.fmt.allocPrint(allocator, "{{\"source\":\"{s}\",\"source_endpoint\":\"{s}\"}}", .{ tag, source_endpoint });
    }

    var buf: std.Io.Writer.Allocating = .init(allocator);
    try buf.writer.writeAll("{\"source\":");
    try buf.writer.print("{f}", .{std.json.fmt(tag, .{})});
    if (source_endpoint.len > 0) {
        try buf.writer.writeAll(",\"source_endpoint\":");
        try buf.writer.print("{f}", .{std.json.fmt(source_endpoint, .{})});
    }
    if (target_endpoint) |target| {
        try buf.writer.writeAll(",\"target_endpoint\":");
        try buf.writer.print("{f}", .{std.json.fmt(target, .{})});
    }
    for (parsed.value.object.keys()) |key| {
        if (std.mem.eql(u8, key, "source")) continue;
        if (std.mem.eql(u8, key, "source_endpoint")) continue;
        if (std.mem.eql(u8, key, "target_endpoint")) continue;
        const value = parsed.value.object.get(key) orelse continue;
        try buf.writer.writeAll(",");
        try buf.writer.print("\"{s}\":", .{key});
        try buf.writer.print("{f}", .{std.json.fmt(value, .{})});
    }
    try buf.writer.writeAll("}");
    return buf.toOwnedSlice();
}

fn normalizeFacetsJsonSource(allocator: std.mem.Allocator, raw: []const u8, source_tag: []const u8) ![]const u8 {
    return mergeImportMetadata(allocator, raw, source_tag, "", null);
}

fn expandEntityExternalId(allocator: std.mem.Allocator, entity_type: []const u8, ref_value: []const u8) ![]const u8 {
    var colon_count: usize = 0;
    for (ref_value) |ch| {
        if (ch == ':') colon_count += 1;
    }
    if (colon_count >= 2) return try allocator.dupe(u8, ref_value);
    if (colon_count == 1) {
        const sep = std.mem.indexOfScalar(u8, ref_value, ':') orelse return try std.fmt.allocPrint(allocator, "{s}:{s}", .{ entity_type, ref_value });
        const local_type = ref_value[0..sep];
        const local_id = ref_value[sep + 1 ..];
        return try std.fmt.allocPrint(allocator, "{s}:{s}:{s}", .{ local_type, local_type, local_id });
    }
    return try std.fmt.allocPrint(allocator, "{s}:{s}:{s}", .{ entity_type, entity_type, ref_value });
}

fn entityTypeFromSourceRef(allocator: std.mem.Allocator, workspace_id: []const u8, source_ref: []const u8) ![]const u8 {
    return endpointEntityType(allocator, workspace_id, source_ref);
}

fn purgeTaggedImport(db: facet_sqlite.Database, workspace_id: []const u8, source_tag: []const u8) !void {
    const del_facts =
        \\DELETE FROM agent_facts
        \\WHERE workspace_id = ?1
        \\  AND 
    ++ taggedJsonSourceSql;
    const s1 = try facet_sqlite.prepare(db, del_facts);
    defer facet_sqlite.finalize(s1);
    try facet_sqlite.bindText(s1, 1, workspace_id);
    try bindTaggedSourceTags(s1, source_tag, 2);
    try facet_sqlite.stepDone(s1);

    const del_rel =
        \\DELETE FROM relations_raw
        \\WHERE workspace_id = ?1
        \\  AND 
    ++ taggedMetadataSourceSql;
    const s2 = try facet_sqlite.prepare(db, del_rel);
    defer facet_sqlite.finalize(s2);
    try facet_sqlite.bindText(s2, 1, workspace_id);
    try bindTaggedSourceTags(s2, source_tag, 2);
    try facet_sqlite.stepDone(s2);

    const del_ent =
        \\DELETE FROM entities_raw
        \\WHERE workspace_id = ?1
        \\  AND 
    ++ taggedMetadataSourceSql;
    const s3 = try facet_sqlite.prepare(db, del_ent);
    defer facet_sqlite.finalize(s3);
    try facet_sqlite.bindText(s3, 1, workspace_id);
    try bindTaggedSourceTags(s3, source_tag, 2);
    try facet_sqlite.stepDone(s3);

    const del_prov = "DELETE FROM structured_import_provenance WHERE workspace_id = ?1 AND " ++ taggedProvenanceSourceSql;
    const s4 = try facet_sqlite.prepare(db, del_prov);
    defer facet_sqlite.finalize(s4);
    try facet_sqlite.bindText(s4, 1, workspace_id);
    try bindTaggedSourceTags(s4, source_tag, 2);
    try facet_sqlite.stepDone(s4);
}

pub const ApplyReport = struct {
    facets_inserted: u64 = 0,
    facets_updated: u64 = 0,
    facets_skipped: u64 = 0,
    entities_upserted: u64 = 0,
    edges_inserted: u64 = 0,
    edges_skipped: u64 = 0,
};

pub const ReindexOptions = struct {
    scope: ReindexScope = .graph,
    source_ref: ?[]const u8 = null,
    since_fingerprint: ?[]const u8 = null,
};

pub fn reindexStructured(allocator: std.mem.Allocator, db: *facet_sqlite.Database, workspace_id: []const u8, opts: ReindexOptions) !ReindexReport {
    var out = ReindexReport{};
    switch (opts.scope) {
        .graph, .all, .provenance => {
            if (opts.scope == .provenance) {
                const result = try reindexGraphForProvenance(allocator, db, workspace_id, opts);
                out.graph_projected = result;
            } else {
                const result = try reindex_http.reindexGraph(allocator, db, workspace_id, null);
                out.graph_projected = result.projected_count;
            }
        },
        .facets => {},
    }
    switch (opts.scope) {
        .facets, .all => {
            out.facet_assignments = try reindexAgentFactsSearch(allocator, db, workspace_id);
        },
        .provenance => {
            out.facet_assignments = try reindexProvenanceFactsSearch(allocator, db, workspace_id, opts);
        },
        else => {},
    }
    return out;
}

fn reindexGraphForProvenance(allocator: std.mem.Allocator, db: *facet_sqlite.Database, workspace_id: []const u8, opts: ReindexOptions) !u64 {
    _ = opts;
    const result = try reindex_http.reindexGraph(allocator, db, workspace_id, null);
    return result.projected_count;
}

fn reindexProvenanceFactsSearch(allocator: std.mem.Allocator, db: *facet_sqlite.Database, workspace_id: []const u8, opts: ReindexOptions) !u64 {
    var sql = std.ArrayList(u8).empty;
    defer sql.deinit(allocator);
    try sql.appendSlice(allocator,
        \\SELECT af.doc_id, af.content
        \\FROM agent_facts af
        \\INNER JOIN structured_import_provenance p
        \\  ON p.workspace_id = af.workspace_id AND p.source_ref = af.source_ref
        \\WHERE af.workspace_id = ?1 AND af.doc_id IS NOT NULL
    );
    if (opts.source_ref) |source_ref| {
        _ = source_ref;
        try sql.appendSlice(allocator, " AND p.source_ref = ?2");
    }
    if (opts.since_fingerprint) |fingerprint| {
        _ = fingerprint;
        try sql.appendSlice(allocator, " AND p.row_fingerprint >= ?3");
    }

    const stmt = try facet_sqlite.prepare(db.*, sql.items);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var bind_idx: i32 = 2;
    if (opts.source_ref) |source_ref| {
        try facet_sqlite.bindText(stmt, bind_idx, source_ref);
        bind_idx += 1;
    }
    if (opts.since_fingerprint) |fingerprint| {
        try facet_sqlite.bindText(stmt, bind_idx, fingerprint);
    }

    var synced: u64 = 0;
    while (true) {
        const rc = facet_sqlite.c.sqlite3_step(stmt);
        if (rc == facet_sqlite.c.SQLITE_DONE) break;
        if (rc != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;
        const doc_id = try facet_sqlite.columnU64(stmt, 0);
        const content = try facet_sqlite.dupeColumnText(allocator, stmt, 1);
        defer allocator.free(content);
        search_sqlite.syncSearchDocumentIfTriggered(db.*, allocator, AgentFactsSearchTableId, doc_id, content, "english") catch continue;
        synced += 1;
    }
    return synced;
}

fn reindexAgentFactsSearch(allocator: std.mem.Allocator, db: *facet_sqlite.Database, workspace_id: []const u8) !u64 {
    const sql =
        \\SELECT doc_id, content
        \\FROM agent_facts
        \\WHERE workspace_id = ?1 AND doc_id IS NOT NULL
        \\ORDER BY doc_id
    ;
    const stmt = try facet_sqlite.prepare(db.*, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);

    var synced: u64 = 0;
    while (true) {
        const rc = facet_sqlite.c.sqlite3_step(stmt);
        if (rc == facet_sqlite.c.SQLITE_DONE) break;
        if (rc != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;
        const doc_id = try facet_sqlite.columnU64(stmt, 0);
        const content = try facet_sqlite.dupeColumnText(allocator, stmt, 1);
        defer allocator.free(content);
        search_sqlite.syncSearchDocumentIfTriggered(db.*, allocator, AgentFactsSearchTableId, doc_id, content, "english") catch continue;
        synced += 1;
    }
    return synced;
}

pub const ProvenanceValidationReport = struct {
    provenance_rows: u64 = 0,
    tagged_facts: u64 = 0,
    facts_without_provenance: u64 = 0,
    provenance_without_fact: u64 = 0,
    provenance_without_table_id: u64 = 0,

    pub fn ok(self: ProvenanceValidationReport) bool {
        return self.facts_without_provenance == 0 and self.provenance_without_fact == 0 and self.provenance_without_table_id == 0;
    }
};

pub fn validateImportProvenance(
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    source_tag: []const u8,
    strict_table_id: bool,
) !ProvenanceValidationReport {
    var report = ProvenanceValidationReport{};

    const count_prov = "SELECT COUNT(*) FROM structured_import_provenance WHERE workspace_id = ?1 AND " ++ taggedProvenanceSourceSql;
    const s1 = try facet_sqlite.prepare(db, count_prov);
    defer facet_sqlite.finalize(s1);
    try facet_sqlite.bindText(s1, 1, workspace_id);
    try bindTaggedSourceTags(s1, source_tag, 2);
    if (facet_sqlite.c.sqlite3_step(s1) != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;
    report.provenance_rows = try facet_sqlite.columnU64(s1, 0);

    const count_facts =
        \\SELECT COUNT(*)
        \\FROM agent_facts
        \\WHERE workspace_id = ?1
        \\  AND 
    ++ taggedJsonSourceSql;
    const s2 = try facet_sqlite.prepare(db, count_facts);
    defer facet_sqlite.finalize(s2);
    try facet_sqlite.bindText(s2, 1, workspace_id);
    try bindTaggedSourceTags(s2, source_tag, 2);
    if (facet_sqlite.c.sqlite3_step(s2) != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;
    report.tagged_facts = try facet_sqlite.columnU64(s2, 0);

    const orphan_facts =
        \\SELECT COUNT(*)
        \\FROM agent_facts af
        \\WHERE af.workspace_id = ?1
        \\  AND 
    ++ taggedJsonSourceSql ++
        \\  AND NOT EXISTS (
        \\    SELECT 1 FROM structured_import_provenance p
        \\    WHERE p.workspace_id = af.workspace_id AND p.source_ref = af.source_ref
        \\  )
    ;
    const s3 = try facet_sqlite.prepare(db, orphan_facts);
    defer facet_sqlite.finalize(s3);
    try facet_sqlite.bindText(s3, 1, workspace_id);
    try bindTaggedSourceTags(s3, source_tag, 2);
    if (facet_sqlite.c.sqlite3_step(s3) != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;
    report.facts_without_provenance = try facet_sqlite.columnU64(s3, 0);

    const orphan_prov =
        \\SELECT COUNT(*)
        \\FROM structured_import_provenance p
        \\WHERE p.workspace_id = ?1
        \\  AND p.fact_id IS NOT NULL
        \\  AND NOT EXISTS (SELECT 1 FROM agent_facts af WHERE af.id = p.fact_id)
    ;
    const s4 = try facet_sqlite.prepare(db, orphan_prov);
    defer facet_sqlite.finalize(s4);
    try facet_sqlite.bindText(s4, 1, workspace_id);
    if (facet_sqlite.c.sqlite3_step(s4) != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;
    report.provenance_without_fact = try facet_sqlite.columnU64(s4, 0);

    const missing_table_id =
        \\SELECT COUNT(*)
        \\FROM structured_import_provenance p
        \\WHERE p.workspace_id = ?1
        \\  AND 
    ++ taggedProvenanceSourceSql ++
        \\  AND p.table_id IS NULL
    ;
    const s5 = try facet_sqlite.prepare(db, missing_table_id);
    defer facet_sqlite.finalize(s5);
    try facet_sqlite.bindText(s5, 1, workspace_id);
    try bindTaggedSourceTags(s5, source_tag, 2);
    if (facet_sqlite.c.sqlite3_step(s5) != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;
    report.provenance_without_table_id = try facet_sqlite.columnU64(s5, 0);

    if (strict_table_id and report.provenance_without_table_id > 0) {
        return error.MissingTableSemantics;
    }

    return report;
}

fn lookupTableIdForSourceRef(db: facet_sqlite.Database, workspace_id: []const u8, source_ref: []const u8) ?u64 {
    const local_type = entityLocalTypeFromSourceRef(source_ref) orelse return null;
    return workspace_sqlite.lookupTableId(db, workspace_id, "structured", local_type) catch null;
}

fn entityLocalTypeFromSourceRef(source_ref: []const u8) ?[]const u8 {
    const sep = std.mem.indexOfScalar(u8, source_ref, ':') orelse return null;
    if (sep == 0) return null;
    return source_ref[0..sep];
}

pub const OrphanAuditReport = struct {
    orphan_entities: u64 = 0,
    tagged_entities: u64 = 0,
    orphan_ratio: f64 = 0,

    pub fn withinThreshold(self: OrphanAuditReport, max_ratio: f64) bool {
        return self.orphan_ratio <= max_ratio;
    }
};

pub fn auditOrphans(db: facet_sqlite.Database, workspace_id: []const u8, source_tag: []const u8) !OrphanAuditReport {
    const count_tagged =
        \\SELECT COUNT(*)
        \\FROM entities_raw
        \\WHERE workspace_id = ?1
        \\  AND 
    ++ taggedMetadataSourceSql;
    const s1 = try facet_sqlite.prepare(db, count_tagged);
    defer facet_sqlite.finalize(s1);
    try facet_sqlite.bindText(s1, 1, workspace_id);
    try bindTaggedSourceTags(s1, source_tag, 2);
    if (facet_sqlite.c.sqlite3_step(s1) != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;
    const tagged = try facet_sqlite.columnU64(s1, 0);

    const count_orphans =
        \\SELECT COUNT(*)
        \\FROM entities_raw e
        \\WHERE e.workspace_id = ?1
        \\  AND 
    ++ taggedMetadataSourceSql ++
        \\  AND NOT EXISTS (
        \\    SELECT 1 FROM relations_raw r
        \\    WHERE r.workspace_id = e.workspace_id
        \\      AND (r.source_entity_id = e.entity_id OR r.target_entity_id = e.entity_id)
        \\  )
    ;
    const s2 = try facet_sqlite.prepare(db, count_orphans);
    defer facet_sqlite.finalize(s2);
    try facet_sqlite.bindText(s2, 1, workspace_id);
    try bindTaggedSourceTags(s2, source_tag, 2);
    if (facet_sqlite.c.sqlite3_step(s2) != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;
    const orphans = try facet_sqlite.columnU64(s2, 0);

    const ratio = if (tagged == 0) 0 else @as(f64, @floatFromInt(orphans)) / @as(f64, @floatFromInt(tagged));
    return .{
        .orphan_entities = orphans,
        .tagged_entities = tagged,
        .orphan_ratio = ratio,
    };
}

pub const ApplyReportOld = ApplyReport;

pub const ReindexReport = struct {
    graph_projected: u64 = 0,
    facet_assignments: u64 = 0,
};

pub fn dryRunCounts(allocator: std.mem.Allocator, facets_path: []const u8, edges_path: ?[]const u8) !DryRunReport {
    var facets = try readCsvFile(allocator, facets_path);
    defer facets.deinit(allocator);
    var report = DryRunReport{ .facet_rows = @intCast(facets.rows.len) };
    if (edges_path) |ep| {
        var edges = try readCsvFile(allocator, ep);
        defer edges.deinit(allocator);
        report.edge_rows = @intCast(edges.rows.len);
    }
    return report;
}

pub const DryRunReport = struct {
    facet_rows: u64 = 0,
    edge_rows: u64 = 0,
};

pub fn profileCsv(allocator: std.mem.Allocator, input_path: []const u8, output_path: []const u8) !void {
    var table = try readTableFile(allocator, input_path);
    defer table.deinit(allocator);

    var payload = std.ArrayList(u8).empty;
    defer payload.deinit(allocator);
    try payload.appendSlice(allocator, "{\"path\":\"");
    try jsonAppendEscaped(allocator, &payload, input_path);
    const head = try std.fmt.allocPrint(allocator, "\",\"row_count\":{d},\"columns\":[", .{table.rows.len});
    defer allocator.free(head);
    try payload.appendSlice(allocator, head);

    for (table.headers, 0..) |header, idx| {
        var non_empty: u64 = 0;
        for (table.rows) |row| {
            if (idx < row.len and row[idx].len > 0) non_empty += 1;
        }
        if (idx > 0) try payload.append(allocator, ',');
        try payload.appendSlice(allocator, "{\"name\":\"");
        try jsonAppendEscaped(allocator, &payload, header);
        const col = try std.fmt.allocPrint(allocator, "\",\"non_empty\":{d}}}", .{non_empty});
        defer allocator.free(col);
        try payload.appendSlice(allocator, col);
    }
    try payload.appendSlice(allocator, "]}");

    try std.Io.Dir.cwd().writeFile(zig16_compat.io(), .{
        .sub_path = output_path,
        .data = payload.items,
        .flags = .{ .truncate = true },
    });
}

fn jsonAppendEscaped(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), text: []const u8) !void {
    for (text) |c| switch (c) {
        '"' => try buf.appendSlice(allocator, "\\\""),
        '\\' => try buf.appendSlice(allocator, "\\\\"),
        else => try buf.append(allocator, c),
    };
}

fn factExistsBySourceRef(db: facet_sqlite.Database, workspace_id: []const u8, source_ref: []const u8) bool {
    const stmt = facet_sqlite.prepare(db, "SELECT 1 FROM agent_facts WHERE workspace_id = ?1 AND source_ref = ?2 LIMIT 1") catch return false;
    defer facet_sqlite.finalize(stmt);
    facet_sqlite.bindText(stmt, 1, workspace_id) catch return false;
    facet_sqlite.bindText(stmt, 2, source_ref) catch return false;
    const rc = facet_sqlite.c.sqlite3_step(stmt);
    return rc == facet_sqlite.c.SQLITE_ROW;
}

fn entityExistsByName(db: facet_sqlite.Database, workspace_id: []const u8, entity_type: []const u8, name: []const u8) bool {
    const stmt = facet_sqlite.prepare(db, "SELECT 1 FROM entities_raw WHERE workspace_id = ?1 AND entity_type = ?2 AND name = ?3 LIMIT 1") catch return false;
    defer facet_sqlite.finalize(stmt);
    facet_sqlite.bindText(stmt, 1, workspace_id) catch return false;
    facet_sqlite.bindText(stmt, 2, entity_type) catch return false;
    facet_sqlite.bindText(stmt, 3, name) catch return false;
    return facet_sqlite.c.sqlite3_step(stmt) == facet_sqlite.c.SQLITE_ROW;
}

fn relationExistsByExternalId(db: facet_sqlite.Database, workspace_id: []const u8, external_id: []const u8) bool {
    const stmt = facet_sqlite.prepare(db, "SELECT 1 FROM relations_raw WHERE workspace_id = ?1 AND external_id = ?2 LIMIT 1") catch return false;
    defer facet_sqlite.finalize(stmt);
    facet_sqlite.bindText(stmt, 1, workspace_id) catch return false;
    facet_sqlite.bindText(stmt, 2, external_id) catch return false;
    return facet_sqlite.c.sqlite3_step(stmt) == facet_sqlite.c.SQLITE_ROW;
}

fn stableFactId(allocator: std.mem.Allocator, workspace_id: []const u8, source_ref: []const u8) ![]const u8 {
    var hash: [32]u8 = undefined;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(workspace_id);
    hasher.update(":");
    hasher.update(source_ref);
    hasher.final(&hash);
    var hex_buf: [32]u8 = undefined;
    for (hash[0..16], 0..) |byte, i| {
        _ = std.fmt.bufPrint(hex_buf[i * 2 ..][0..2], "{x:0>2}", .{byte}) catch unreachable;
    }
    return try std.fmt.allocPrint(allocator, "facet_{s}", .{hex_buf[0..32]});
}

fn endpointEntityType(allocator: std.mem.Allocator, workspace_id: []const u8, endpoint: []const u8) ![]const u8 {
    const sep = std.mem.indexOfScalar(u8, endpoint, ':') orelse return try std.fmt.allocPrint(allocator, "{s}:{s}", .{ workspace_id, endpoint });
    const local = endpoint[0..sep];
    return try std.fmt.allocPrint(allocator, "{s}:{s}", .{ workspace_id, local });
}

fn edgeExternalId(allocator: std.mem.Allocator, source: []const u8, label: []const u8, target: []const u8) ![]const u8 {
    return try std.fmt.allocPrint(allocator, "{s}|{s}|{s}", .{ source, label, target });
}

test "parseCsv reads header and rows" {
    const csv =
        \\workspace_id,source_ref
        \\ws-a,entity:1
        \\ws-a,entity:2
    ;
    var table = try parseCsv(std.testing.allocator, csv);
    defer table.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), table.headers.len);
    try std.testing.expectEqual(@as(usize, 2), table.rows.len);
    try std.testing.expectEqualStrings("entity:1", table.cell(0, "source_ref").?);
}

test "stableFactId is deterministic" {
    const a = try stableFactId(std.testing.allocator, "ws", "lot:1");
    defer std.testing.allocator.free(a);
    const b = try stableFactId(std.testing.allocator, "ws", "lot:1");
    defer std.testing.allocator.free(b);
    try std.testing.expectEqualStrings(a, b);
}

test "entityLocalTypeFromSourceRef extracts prefix" {
    try std.testing.expectEqualStrings("lot", entityLocalTypeFromSourceRef("lot:lot:0001").?);
}

test "csvTableFromToonRoot reads tabular toon" {
    const toon =
        \\people[2]{id,name,active}:
        \\  1,Ari,true
        \\  2,Bea,false
    ;
    const root = try ztoon.decodeAlloc(std.testing.allocator, toon, .{});
    defer ztoon.deinitValue(std.testing.allocator, root);
    var table = try csvTableFromToonRoot(std.testing.allocator, root);
    defer table.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), table.headers.len);
    try std.testing.expectEqual(@as(usize, 2), table.rows.len);
    try std.testing.expectEqualStrings("1", table.cell(0, "id").?);
    try std.testing.expectEqualStrings("Ari", table.cell(0, "name").?);
    try std.testing.expectEqualStrings("true", table.cell(0, "active").?);
    try std.testing.expectEqualStrings("false", table.cell(1, "active").?);
}

fn countRows(db: facet_sqlite.Database, sql: []const u8) !u64 {
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    if (facet_sqlite.c.sqlite3_step(stmt) != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;
    return try facet_sqlite.columnU64(stmt, 0);
}

test "applyFromWsTables streams ws rows into facts and provenance" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws-test", .label = "ws-test" });
    try collections_sqlite.ensureOntology(db, .{ .workspace_id = "ws-test", .ontology_id = "onto-test", .name = "onto-test" });

    try workspace_sqlite.upsertWorkspace(db, "ws-test", "{\"domain\":\"test\"}");
    try workspace_sqlite.upsertTableSemanticFull(db, .{
        .table_id = 1,
        .workspace_id = "ws-test",
        .schema_name = "structured",
        .table_name = "lot",
        .key_column = "record_id",
        .content_column = "content",
        .notes = "{\"schema_id\":\"test:lot\",\"entity_family\":\"test\"}",
    });
    try workspace_sqlite.upsertColumnSemanticFull(db, .{
        .column_semantic_id = 1,
        .table_id = 1,
        .column_name = "record_id",
        .column_role = "primary_key",
        .data_type = "text",
    });

    try db.exec(
        \\CREATE TABLE ws_lot(record_id TEXT PRIMARY KEY, content TEXT, status TEXT);
        \\INSERT INTO ws_lot(record_id, content, status) VALUES ('001', 'Lot 001', 'active');
        \\INSERT INTO ws_lot(record_id, content, status) VALUES ('002', 'Lot 002', 'inactive');
    );

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"entities":{"lot":{"schema_id":"test:lot"}}}
    , .{});
    defer parsed.deinit();

    const report = try applyFromWsTables(std.testing.allocator, db, .{
        .workspace_id = "ws-test",
        .ontology_id = "onto-test",
        .data_plane = "ws",
    }, &[_]ContractRelation{}, parsed.value, null);

    try std.testing.expectEqual(@as(u64, 2), report.facets_inserted);
    try std.testing.expectEqual(@as(u64, 2), report.entities_upserted);
    try std.testing.expectEqual(@as(u64, 2), try countRows(db, "SELECT COUNT(*) FROM agent_facts WHERE workspace_id = 'ws-test'"));
    try std.testing.expectEqual(@as(u64, 2), try countRows(db, "SELECT COUNT(*) FROM structured_import_provenance WHERE workspace_id = 'ws-test'"));
}
