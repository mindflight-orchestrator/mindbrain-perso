const std = @import("std");
const collections_io = @import("collections_io.zig");
const collections_sqlite = @import("collections_sqlite.zig");
const facet_sqlite = @import("facet_sqlite.zig");
const zig16_compat = @import("zig16_compat.zig");

const Allocator = std.mem.Allocator;
const Database = facet_sqlite.Database;
const c = facet_sqlite.c;

const Annotation = struct {
    key: []const u8,
    value: []const u8,

    fn deinit(self: *Annotation, allocator: Allocator) void {
        allocator.free(self.key);
        allocator.free(self.value);
        self.* = undefined;
    }
};

const Prefix = struct {
    key: []const u8,
    value: []const u8,

    fn deinit(self: *Prefix, allocator: Allocator) void {
        allocator.free(self.key);
        allocator.free(self.value);
        self.* = undefined;
    }
};

const ClassDef = struct {
    name: []const u8,
    is_root: bool = false,
    is_a: ?[]const u8 = null,
    class_uri: ?[]const u8 = null,
    description: ?[]const u8 = null,
    abstract: bool = false,
    mixin: bool = false,
    annotations: std.ArrayList(Annotation) = .empty,

    fn deinit(self: *ClassDef, allocator: Allocator) void {
        allocator.free(self.name);
        if (self.is_a) |value| allocator.free(value);
        if (self.class_uri) |value| allocator.free(value);
        if (self.description) |value| allocator.free(value);
        for (self.annotations.items) |*annotation| annotation.deinit(allocator);
        self.annotations.deinit(allocator);
        self.* = undefined;
    }
};

const SlotDef = struct {
    name: []const u8,
    is_root: bool = false,
    domain: ?[]const u8 = null,
    range: ?[]const u8 = null,
    slot_uri: ?[]const u8 = null,
    description: ?[]const u8 = null,
    annotations: std.ArrayList(Annotation) = .empty,

    fn deinit(self: *SlotDef, allocator: Allocator) void {
        allocator.free(self.name);
        if (self.domain) |value| allocator.free(value);
        if (self.range) |value| allocator.free(value);
        if (self.slot_uri) |value| allocator.free(value);
        if (self.description) |value| allocator.free(value);
        for (self.annotations.items) |*annotation| annotation.deinit(allocator);
        self.annotations.deinit(allocator);
        self.* = undefined;
    }
};

const EnumValue = struct {
    value: []const u8,
    meaning: ?[]const u8 = null,
    description: ?[]const u8 = null,

    fn deinit(self: *EnumValue, allocator: Allocator) void {
        allocator.free(self.value);
        if (self.meaning) |value| allocator.free(value);
        if (self.description) |value| allocator.free(value);
        self.* = undefined;
    }
};

const EnumDef = struct {
    name: []const u8,
    description: ?[]const u8 = null,
    values: std.ArrayList(EnumValue) = .empty,

    fn deinit(self: *EnumDef, allocator: Allocator) void {
        allocator.free(self.name);
        if (self.description) |value| allocator.free(value);
        for (self.values.items) |*value| value.deinit(allocator);
        self.values.deinit(allocator);
        self.* = undefined;
    }
};

const Schema = struct {
    source_path: []const u8,
    id: ?[]const u8 = null,
    name: ?[]const u8 = null,
    title: ?[]const u8 = null,
    version: ?[]const u8 = null,
    default_prefix: ?[]const u8 = null,
    prefixes: std.ArrayList(Prefix) = .empty,
    imports: std.ArrayList([]const u8) = .empty,
    classes: std.ArrayList(ClassDef) = .empty,
    slots: std.ArrayList(SlotDef) = .empty,
    enums: std.ArrayList(EnumDef) = .empty,

    fn deinit(self: *Schema, allocator: Allocator) void {
        allocator.free(self.source_path);
        if (self.id) |value| allocator.free(value);
        if (self.name) |value| allocator.free(value);
        if (self.title) |value| allocator.free(value);
        if (self.version) |value| allocator.free(value);
        if (self.default_prefix) |value| allocator.free(value);
        for (self.prefixes.items) |*prefix| prefix.deinit(allocator);
        self.prefixes.deinit(allocator);
        for (self.imports.items) |value| allocator.free(value);
        self.imports.deinit(allocator);
        for (self.classes.items) |*class| class.deinit(allocator);
        self.classes.deinit(allocator);
        for (self.slots.items) |*slot| slot.deinit(allocator);
        self.slots.deinit(allocator);
        for (self.enums.items) |*enum_def| enum_def.deinit(allocator);
        self.enums.deinit(allocator);
        self.* = undefined;
    }
};

pub const CompileOptions = struct {
    input_path: []const u8,
    workspace_id: []const u8,
    ontology_id: []const u8,
    ontology_name: ?[]const u8 = null,
};

pub const CompileResult = struct {
    bundle_json: []const u8,
    ntriples: []const u8,
    entity_type_count: usize,
    edge_type_count: usize,
    enum_value_count: usize,
    triple_count: usize,

    pub fn deinit(self: *CompileResult, allocator: Allocator) void {
        allocator.free(self.bundle_json);
        allocator.free(self.ntriples);
        self.* = undefined;
    }
};

const Section = enum { none, prefixes, imports, classes, slots, enums };
const AnnotationTarget = enum { none, class, slot };
const EnumContext = enum { none, permissible_values, value };

const ImportLoadContext = struct {
    allocator: Allocator,
    visiting: std.StringHashMap(void),
    loaded: std.StringHashMap(void),

    fn init(allocator: Allocator) ImportLoadContext {
        return .{
            .allocator = allocator,
            .visiting = std.StringHashMap(void).init(allocator),
            .loaded = std.StringHashMap(void).init(allocator),
        };
    }

    fn deinit(self: *ImportLoadContext) void {
        freeStringMapKeys(self.allocator, &self.visiting);
        freeStringMapKeys(self.allocator, &self.loaded);
        self.visiting.deinit();
        self.loaded.deinit();
        self.* = undefined;
    }
};

const SchemaIndex = struct {
    native_type_by_class: std.StringHashMap([]const u8),
    class_uri_by_name: std.StringHashMap([]const u8),
    enum_names: std.StringHashMap(void),

    fn init(allocator: Allocator, schema: *const Schema) !SchemaIndex {
        var index = SchemaIndex{
            .native_type_by_class = std.StringHashMap([]const u8).init(allocator),
            .class_uri_by_name = std.StringHashMap([]const u8).init(allocator),
            .enum_names = std.StringHashMap(void).init(allocator),
        };
        errdefer index.deinit();

        for (schema.classes.items) |class| {
            if (!index.native_type_by_class.contains(class.name)) {
                try index.native_type_by_class.put(class.name, nativeEntityType(class));
            }
            if (class.class_uri) |class_uri| {
                if (!index.class_uri_by_name.contains(class.name)) try index.class_uri_by_name.put(class.name, class_uri);
            }
        }
        for (schema.enums.items) |enum_def| {
            if (!index.enum_names.contains(enum_def.name)) try index.enum_names.put(enum_def.name, {});
        }
        return index;
    }

    fn deinit(self: *SchemaIndex) void {
        self.native_type_by_class.deinit();
        self.class_uri_by_name.deinit();
        self.enum_names.deinit();
        self.* = undefined;
    }
};

pub fn compileLinkmlToBundle(allocator: Allocator, options: CompileOptions) !CompileResult {
    var schema = try loadSchema(allocator, options.input_path);
    defer schema.deinit(allocator);
    var index = try SchemaIndex.init(allocator, &schema);
    defer index.deinit();

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    var triples: std.Io.Writer.Allocating = .init(allocator);
    defer triples.deinit();

    const ontology_name = options.ontology_name orelse schema.name orelse lastOntologySegment(options.ontology_id);
    const version = schema.version orelse "1.0.0";
    const default_prefix = schema.default_prefix orelse "default";

    try out.writer.writeAll("{\n  \"kind\": \"ghostcrab_backup_bundle\",\n  \"schema_version\": \"2\",\n");
    try out.writer.print("  \"scope\": {{ \"kind\": \"taxonomies\", \"workspace_id\": {f}, \"collection_id\": null }},\n", .{std.json.fmt(options.workspace_id, .{})});
    try out.writer.writeAll("  \"workspaces\": [],\n  \"collections\": [],\n  \"ontologies\": [");
    try out.writer.print("{{ \"ontology_id\": {f}, \"workspace_id\": {f}, \"name\": {f}, \"version\": {f}, \"frozen\": false, \"source_kind\": \"linkml\", \"metadata_json\": ", .{
        std.json.fmt(options.ontology_id, .{}),
        std.json.fmt(options.workspace_id, .{}),
        std.json.fmt(ontology_name, .{}),
        std.json.fmt(version, .{}),
    });
    const ontology_metadata = try ontologyMetadataJson(allocator, schema);
    defer allocator.free(ontology_metadata);
    try out.writer.print("{f} }}],\n", .{std.json.fmt(ontology_metadata, .{})});

    try out.writer.writeAll("  \"ontology_namespaces\": [],\n  \"ontology_dimensions\": [");
    var enum_value_count: usize = 0;
    for (schema.enums.items, 0..) |enum_def, enum_index| {
        if (enum_index > 0) try out.writer.writeAll(",");
        const metadata = try enumMetadataJson(allocator, enum_def);
        defer allocator.free(metadata);
        try out.writer.print("\n    {{ \"ontology_id\": {f}, \"namespace\": {f}, \"dimension\": {f}, \"value_type\": \"string\", \"is_multi\": false, \"hierarchy_kind\": \"flat\", \"metadata_json\": {f} }}", .{
            std.json.fmt(options.ontology_id, .{}),
            std.json.fmt(default_prefix, .{}),
            std.json.fmt(enum_def.name, .{}),
            std.json.fmt(metadata, .{}),
        });
        enum_value_count += enum_def.values.items.len;
    }
    try out.writer.writeAll("\n  ],\n  \"ontology_values\": [");
    var first_value = true;
    for (schema.enums.items) |enum_def| {
        for (enum_def.values.items, 0..) |value, value_index| {
            if (!first_value) try out.writer.writeAll(",");
            first_value = false;
            const metadata = try enumValueMetadataJson(allocator, value);
            defer allocator.free(metadata);
            try out.writer.print("\n    {{ \"ontology_id\": {f}, \"namespace\": {f}, \"dimension\": {f}, \"value_id\": {}, \"value\": {f}, \"parent_value_id\": null, \"label\": {f}, \"metadata_json\": {f} }}", .{
                std.json.fmt(options.ontology_id, .{}),
                std.json.fmt(default_prefix, .{}),
                std.json.fmt(enum_def.name, .{}),
                value_index + 1,
                std.json.fmt(value.value, .{}),
                std.json.fmt(value.value, .{}),
                std.json.fmt(metadata, .{}),
            });
        }
    }
    try out.writer.writeAll("\n  ],\n  \"ontology_entity_types\": [");
    var entity_count: usize = 0;
    for (schema.classes.items) |class| {
        if (!shouldProjectClass(class)) continue;
        if (entity_count > 0) try out.writer.writeAll(",");
        const entity_type = nativeEntityType(class);
        const label = class.description orelse entity_type;
        const metadata = try classMetadataJson(allocator, schema, class);
        defer allocator.free(metadata);
        try out.writer.print("\n    {{ \"ontology_id\": {f}, \"entity_type\": {f}, \"label\": {f}, \"metadata_json\": {f} }}", .{
            std.json.fmt(options.ontology_id, .{}),
            std.json.fmt(entity_type, .{}),
            std.json.fmt(label, .{}),
            std.json.fmt(metadata, .{}),
        });
        entity_count += 1;
    }
    try out.writer.writeAll("\n  ],\n  \"ontology_edge_types\": [");
    var edge_count: usize = 0;
    for (schema.slots.items) |slot| {
        if (!shouldProjectSlot(&index, slot)) continue;
        if (edge_count > 0) try out.writer.writeAll(",");
        const edge_type = nativeEdgeType(slot);
        const source = if (slot.domain) |domain| nativeEntityTypeByName(&index, domain) else null;
        const target = if (slot.range) |range| nativeEntityTypeByName(&index, range) else null;
        const metadata = try slotMetadataJson(allocator, schema, slot);
        defer allocator.free(metadata);
        try out.writer.print("\n    {{ \"ontology_id\": {f}, \"edge_type\": {f}, \"directed\": true, \"source_entity_type\": ", .{
            std.json.fmt(options.ontology_id, .{}),
            std.json.fmt(edge_type, .{}),
        });
        try writeOptionalJsonString(&out.writer, source);
        try out.writer.writeAll(", \"target_entity_type\": ");
        try writeOptionalJsonString(&out.writer, target);
        try out.writer.print(", \"metadata_json\": {f} }}", .{std.json.fmt(metadata, .{})});
        edge_count += 1;
    }
    try out.writer.writeAll("\n  ],\n  \"ontology_entities\": [],\n  \"ontology_relations\": [],\n  \"ontology_triples\": [");

    var triple_count: usize = 0;
    try emitTriples(allocator, &schema, &index, options.ontology_id, &out.writer, &triples.writer, &triple_count);

    try out.writer.writeAll("\n  ],\n  \"collection_ontologies\": [],\n  \"workspace_settings\": [");
    try out.writer.print("{{ \"workspace_id\": {f}, \"default_ontology_id\": {f}, \"metadata_json\": \"{{}}\" }}],\n", .{
        std.json.fmt(options.workspace_id, .{}),
        std.json.fmt(options.ontology_id, .{}),
    });
    try out.writer.writeAll(
        \\  "documents_raw": [],
        \\  "chunks_raw": [],
        \\  "documents_raw_vector": [],
        \\  "chunks_raw_vector": [],
        \\  "facet_assignments_raw": [],
        \\  "entities_raw": [],
        \\  "entity_aliases_raw": [],
        \\  "relations_raw": [],
        \\  "relation_properties_raw": [],
        \\  "entity_documents_raw": [],
        \\  "entity_chunks_raw": [],
        \\  "document_links_raw": [],
        \\  "external_links_raw": []
        \\}
        \\
    );

    return .{
        .bundle_json = try out.toOwnedSlice(),
        .ntriples = try triples.toOwnedSlice(),
        .entity_type_count = entity_count,
        .edge_type_count = edge_count,
        .enum_value_count = enum_value_count,
        .triple_count = triple_count,
    };
}

pub fn importCompiledBundle(db: Database, allocator: Allocator, bundle_json: []const u8) !void {
    try collections_io.importBundleJson(db, allocator, bundle_json);
}

pub fn exportLinkmlFromDb(allocator: Allocator, db: Database, ontology_id: []const u8) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    const ontology = try loadOntologyForExport(allocator, db, ontology_id);
    defer {
        allocator.free(ontology.name);
        allocator.free(ontology.version);
    }

    try out.writer.print(
        \\id: https://mindbrain.local/ontology/{s}
        \\name: {s}
        \\title: {s}
        \\version: {s}
        \\
        \\prefixes:
        \\  ghostcrab: https://ghostcrab.be/ontology/
        \\  linkml: https://w3id.org/linkml/
        \\
        \\default_prefix: ghostcrab
        \\default_range: string
        \\
        \\imports:
        \\  - linkml:types
        \\
        \\classes:
        \\
    , .{ ontology_id, ontology.name, ontology.name, ontology.version });
    try writeClassesYaml(allocator, db, ontology_id, &out.writer);
    try out.writer.writeAll("\nslots:\n");
    try writeSlotsYaml(allocator, db, ontology_id, &out.writer);
    try out.writer.writeAll("\nenums:\n");
    try writeEnumsYaml(allocator, db, ontology_id, &out.writer);
    return try out.toOwnedSlice();
}

pub fn exportLinkmlFromBundleJson(allocator: Allocator, bundle_json: []const u8, ontology_id: []const u8) ![]const u8 {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try collections_io.importBundleJson(db, allocator, bundle_json);
    return try exportLinkmlFromDb(allocator, db, ontology_id);
}

fn loadSchema(allocator: Allocator, input_path: []const u8) !Schema {
    var context = ImportLoadContext.init(allocator);
    defer context.deinit();
    return try loadSchemaWithContext(allocator, input_path, &context);
}

fn loadSchemaWithContext(allocator: Allocator, input_path: []const u8, context: *ImportLoadContext) !Schema {
    const absolute_path = try std.fs.path.resolve(allocator, &.{input_path});
    defer allocator.free(absolute_path);
    if (context.visiting.contains(absolute_path)) return error.CyclicImport;
    if (context.loaded.contains(absolute_path)) return Schema{ .source_path = try allocator.dupe(u8, absolute_path) };

    const visiting_key = try allocator.dupe(u8, absolute_path);
    context.visiting.put(visiting_key, {}) catch |err| {
        allocator.free(visiting_key);
        return err;
    };
    defer {
        if (context.visiting.fetchRemove(absolute_path)) |entry| allocator.free(entry.key);
    }

    var raw = try parseSchemaFile(allocator, absolute_path);
    defer raw.deinit(allocator);

    var merged = Schema{ .source_path = try allocator.dupe(u8, absolute_path) };
    errdefer merged.deinit(allocator);
    for (raw.imports.items) |import_ref| {
        if (std.mem.startsWith(u8, import_ref, "linkml:")) continue;
        const import_path = try resolveImportPath(allocator, absolute_path, import_ref);
        defer allocator.free(import_path);
        var imported = try loadSchemaWithContext(allocator, import_path, context);
        defer imported.deinit(allocator);
        try mergeImportedSchema(allocator, &merged, imported);
    }
    try mergeRawSchema(allocator, &merged, raw);
    const loaded_key = try allocator.dupe(u8, absolute_path);
    context.loaded.put(loaded_key, {}) catch |err| {
        allocator.free(loaded_key);
        return err;
    };
    return merged;
}

fn parseSchemaFile(allocator: Allocator, absolute_path: []const u8) !Schema {
    const text = try std.Io.Dir.cwd().readFileAlloc(zig16_compat.io(), absolute_path, allocator, .limited(4 * 1024 * 1024));
    defer allocator.free(text);
    var schema = Schema{ .source_path = try allocator.dupe(u8, absolute_path) };
    errdefer schema.deinit(allocator);

    var section: Section = .none;
    var annotation_target: AnnotationTarget = .none;
    var enum_context: EnumContext = .none;
    var current_class: ?usize = null;
    var current_slot: ?usize = null;
    var current_enum: ?usize = null;
    var current_enum_value: ?usize = null;

    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |raw_line| {
        const line_no_cr = std.mem.trimEnd(u8, raw_line, "\r");
        const trimmed = std.mem.trim(u8, line_no_cr, " \t");
        if (trimmed.len == 0 or trimmed[0] == '#') continue;
        const indent = leadingSpaces(line_no_cr);

        if (indent == 0) {
            annotation_target = .none;
            enum_context = .none;
            current_class = null;
            current_slot = null;
            current_enum = null;
            current_enum_value = null;
            if (std.mem.eql(u8, trimmed, "prefixes:")) {
                section = .prefixes;
            } else if (std.mem.eql(u8, trimmed, "imports:")) {
                section = .imports;
            } else if (std.mem.eql(u8, trimmed, "classes:")) {
                section = .classes;
            } else if (std.mem.eql(u8, trimmed, "slots:")) {
                section = .slots;
            } else if (std.mem.eql(u8, trimmed, "enums:")) {
                section = .enums;
            } else if (splitYamlKeyValue(trimmed)) |kv| {
                try setSchemaScalar(allocator, &schema, kv.key, kv.value);
            }
            continue;
        }

        switch (section) {
            .prefixes => if (indent == 2) {
                if (splitYamlKeyValue(trimmed)) |kv| try schema.prefixes.append(allocator, .{
                    .key = try allocator.dupe(u8, kv.key),
                    .value = try dupeYamlScalar(allocator, kv.value),
                });
            },
            .imports => if (indent == 2 and std.mem.startsWith(u8, trimmed, "-")) {
                try schema.imports.append(allocator, try dupeYamlScalar(allocator, std.mem.trim(u8, trimmed[1..], " \t")));
            },
            .classes => {
                if (indent == 2 and std.mem.endsWith(u8, trimmed, ":")) {
                    const name = std.mem.trimEnd(u8, trimmed, ":");
                    try schema.classes.append(allocator, .{ .name = try allocator.dupe(u8, name), .is_root = true });
                    current_class = schema.classes.items.len - 1;
                    annotation_target = .none;
                } else if (indent == 4 and current_class != null) {
                    if (std.mem.eql(u8, trimmed, "annotations:")) {
                        annotation_target = .class;
                    } else if (splitYamlKeyValue(trimmed)) |kv| {
                        try setClassScalar(allocator, &schema.classes.items[current_class.?], kv.key, kv.value);
                    }
                } else if (indent == 6 and annotation_target == .class and current_class != null) {
                    if (splitYamlKeyValue(trimmed)) |kv| try appendAnnotation(allocator, &schema.classes.items[current_class.?].annotations, kv.key, kv.value);
                }
            },
            .slots => {
                if (indent == 2 and std.mem.endsWith(u8, trimmed, ":")) {
                    const name = std.mem.trimEnd(u8, trimmed, ":");
                    try schema.slots.append(allocator, .{ .name = try allocator.dupe(u8, name), .is_root = true });
                    current_slot = schema.slots.items.len - 1;
                    annotation_target = .none;
                } else if (indent == 4 and current_slot != null) {
                    if (std.mem.eql(u8, trimmed, "annotations:")) {
                        annotation_target = .slot;
                    } else if (splitYamlKeyValue(trimmed)) |kv| {
                        try setSlotScalar(allocator, &schema.slots.items[current_slot.?], kv.key, kv.value);
                    }
                } else if (indent == 6 and annotation_target == .slot and current_slot != null) {
                    if (splitYamlKeyValue(trimmed)) |kv| try appendAnnotation(allocator, &schema.slots.items[current_slot.?].annotations, kv.key, kv.value);
                }
            },
            .enums => {
                if (indent == 2 and std.mem.endsWith(u8, trimmed, ":")) {
                    const name = std.mem.trimEnd(u8, trimmed, ":");
                    try schema.enums.append(allocator, .{ .name = try allocator.dupe(u8, name) });
                    current_enum = schema.enums.items.len - 1;
                    enum_context = .none;
                } else if (indent == 4 and current_enum != null) {
                    if (std.mem.eql(u8, trimmed, "permissible_values:")) {
                        enum_context = .permissible_values;
                    } else if (splitYamlKeyValue(trimmed)) |kv| {
                        try setEnumScalar(allocator, &schema.enums.items[current_enum.?], kv.key, kv.value);
                    }
                } else if (indent == 6 and current_enum != null and std.mem.endsWith(u8, trimmed, ":")) {
                    const name = std.mem.trimEnd(u8, trimmed, ":");
                    try schema.enums.items[current_enum.?].values.append(allocator, .{ .value = try allocator.dupe(u8, name) });
                    current_enum_value = schema.enums.items[current_enum.?].values.items.len - 1;
                    enum_context = .value;
                } else if (indent == 8 and current_enum != null and current_enum_value != null and enum_context == .value) {
                    if (splitYamlKeyValue(trimmed)) |kv| try setEnumValueScalar(allocator, &schema.enums.items[current_enum.?].values.items[current_enum_value.?], kv.key, kv.value);
                }
            },
            else => {},
        }
    }
    return schema;
}

fn mergeImportedSchema(allocator: Allocator, dest: *Schema, imported: Schema) !void {
    for (imported.prefixes.items) |prefix| try appendPrefixIfMissing(allocator, dest, prefix);
    for (imported.classes.items) |class| {
        var copy = try cloneClass(allocator, class);
        copy.is_root = false;
        try dest.classes.append(allocator, copy);
    }
    for (imported.slots.items) |slot| {
        var copy = try cloneSlot(allocator, slot);
        copy.is_root = false;
        try dest.slots.append(allocator, copy);
    }
    for (imported.enums.items) |enum_def| try dest.enums.append(allocator, try cloneEnum(allocator, enum_def));
}

fn mergeRawSchema(allocator: Allocator, dest: *Schema, raw: Schema) !void {
    if (raw.id) |value| dest.id = try allocator.dupe(u8, value);
    if (raw.name) |value| dest.name = try allocator.dupe(u8, value);
    if (raw.title) |value| dest.title = try allocator.dupe(u8, value);
    if (raw.version) |value| dest.version = try allocator.dupe(u8, value);
    if (raw.default_prefix) |value| dest.default_prefix = try allocator.dupe(u8, value);
    for (raw.prefixes.items) |prefix| try appendPrefixIfMissing(allocator, dest, prefix);
    for (raw.imports.items) |import_ref| try dest.imports.append(allocator, try allocator.dupe(u8, import_ref));
    for (raw.classes.items) |class| try dest.classes.append(allocator, try cloneClass(allocator, class));
    for (raw.slots.items) |slot| try dest.slots.append(allocator, try cloneSlot(allocator, slot));
    for (raw.enums.items) |enum_def| try dest.enums.append(allocator, try cloneEnum(allocator, enum_def));
}

fn cloneClass(allocator: Allocator, src: ClassDef) !ClassDef {
    var out = ClassDef{
        .name = try allocator.dupe(u8, src.name),
        .is_root = src.is_root,
        .is_a = if (src.is_a) |value| try allocator.dupe(u8, value) else null,
        .class_uri = if (src.class_uri) |value| try allocator.dupe(u8, value) else null,
        .description = if (src.description) |value| try allocator.dupe(u8, value) else null,
        .abstract = src.abstract,
        .mixin = src.mixin,
    };
    for (src.annotations.items) |annotation| try out.annotations.append(allocator, .{
        .key = try allocator.dupe(u8, annotation.key),
        .value = try allocator.dupe(u8, annotation.value),
    });
    return out;
}

fn cloneSlot(allocator: Allocator, src: SlotDef) !SlotDef {
    var out = SlotDef{
        .name = try allocator.dupe(u8, src.name),
        .is_root = src.is_root,
        .domain = if (src.domain) |value| try allocator.dupe(u8, value) else null,
        .range = if (src.range) |value| try allocator.dupe(u8, value) else null,
        .slot_uri = if (src.slot_uri) |value| try allocator.dupe(u8, value) else null,
        .description = if (src.description) |value| try allocator.dupe(u8, value) else null,
    };
    for (src.annotations.items) |annotation| try out.annotations.append(allocator, .{
        .key = try allocator.dupe(u8, annotation.key),
        .value = try allocator.dupe(u8, annotation.value),
    });
    return out;
}

fn cloneEnum(allocator: Allocator, src: EnumDef) !EnumDef {
    var out = EnumDef{
        .name = try allocator.dupe(u8, src.name),
        .description = if (src.description) |value| try allocator.dupe(u8, value) else null,
    };
    for (src.values.items) |value| try out.values.append(allocator, .{
        .value = try allocator.dupe(u8, value.value),
        .meaning = if (value.meaning) |text| try allocator.dupe(u8, text) else null,
        .description = if (value.description) |text| try allocator.dupe(u8, text) else null,
    });
    return out;
}

fn appendPrefixIfMissing(allocator: Allocator, schema: *Schema, prefix: Prefix) !void {
    for (schema.prefixes.items) |existing| {
        if (std.mem.eql(u8, existing.key, prefix.key)) return;
    }
    try schema.prefixes.append(allocator, .{
        .key = try allocator.dupe(u8, prefix.key),
        .value = try allocator.dupe(u8, prefix.value),
    });
}

fn freeStringMapKeys(allocator: Allocator, map: *std.StringHashMap(void)) void {
    var iterator = map.iterator();
    while (iterator.next()) |entry| allocator.free(entry.key_ptr.*);
}

const KeyValue = struct { key: []const u8, value: []const u8 };

fn splitYamlKeyValue(line: []const u8) ?KeyValue {
    const idx = std.mem.indexOfScalar(u8, line, ':') orelse return null;
    return .{
        .key = std.mem.trim(u8, line[0..idx], " \t"),
        .value = std.mem.trim(u8, line[idx + 1 ..], " \t"),
    };
}

fn dupeYamlScalar(allocator: Allocator, raw: []const u8) ![]const u8 {
    const trimmed = std.mem.trim(u8, raw, " \t");
    if (trimmed.len >= 2 and ((trimmed[0] == '"' and trimmed[trimmed.len - 1] == '"') or (trimmed[0] == '\'' and trimmed[trimmed.len - 1] == '\''))) {
        return try allocator.dupe(u8, trimmed[1 .. trimmed.len - 1]);
    }
    if (std.mem.eql(u8, trimmed, ">") or std.mem.eql(u8, trimmed, "|")) return try allocator.dupe(u8, "");
    return try allocator.dupe(u8, trimmed);
}

fn setSchemaScalar(allocator: Allocator, schema: *Schema, key: []const u8, value: []const u8) !void {
    if (std.mem.eql(u8, key, "id")) schema.id = try dupeYamlScalar(allocator, value) else if (std.mem.eql(u8, key, "name")) schema.name = try dupeYamlScalar(allocator, value) else if (std.mem.eql(u8, key, "title")) schema.title = try dupeYamlScalar(allocator, value) else if (std.mem.eql(u8, key, "version")) schema.version = try dupeYamlScalar(allocator, value) else if (std.mem.eql(u8, key, "default_prefix")) schema.default_prefix = try dupeYamlScalar(allocator, value);
}

fn setClassScalar(allocator: Allocator, class: *ClassDef, key: []const u8, value: []const u8) !void {
    if (std.mem.eql(u8, key, "is_a")) class.is_a = try dupeYamlScalar(allocator, value) else if (std.mem.eql(u8, key, "class_uri")) class.class_uri = try dupeYamlScalar(allocator, value) else if (std.mem.eql(u8, key, "description")) class.description = try dupeYamlScalar(allocator, value) else if (std.mem.eql(u8, key, "abstract")) class.abstract = parseBool(value) else if (std.mem.eql(u8, key, "mixin")) class.mixin = parseBool(value);
}

fn setSlotScalar(allocator: Allocator, slot: *SlotDef, key: []const u8, value: []const u8) !void {
    if (std.mem.eql(u8, key, "domain")) slot.domain = try dupeYamlScalar(allocator, value) else if (std.mem.eql(u8, key, "range")) slot.range = try dupeYamlScalar(allocator, value) else if (std.mem.eql(u8, key, "slot_uri")) slot.slot_uri = try dupeYamlScalar(allocator, value) else if (std.mem.eql(u8, key, "description")) slot.description = try dupeYamlScalar(allocator, value);
}

fn setEnumScalar(allocator: Allocator, enum_def: *EnumDef, key: []const u8, value: []const u8) !void {
    if (std.mem.eql(u8, key, "description")) enum_def.description = try dupeYamlScalar(allocator, value);
}

fn setEnumValueScalar(allocator: Allocator, value: *EnumValue, key: []const u8, raw: []const u8) !void {
    if (std.mem.eql(u8, key, "meaning")) value.meaning = try dupeYamlScalar(allocator, raw) else if (std.mem.eql(u8, key, "description")) value.description = try dupeYamlScalar(allocator, raw);
}

fn appendAnnotation(allocator: Allocator, annotations: *std.ArrayList(Annotation), key: []const u8, value: []const u8) !void {
    try annotations.append(allocator, .{
        .key = try allocator.dupe(u8, key),
        .value = try dupeYamlScalar(allocator, value),
    });
}

fn parseBool(value: []const u8) bool {
    return std.mem.eql(u8, std.mem.trim(u8, value, " \t"), "true");
}

fn leadingSpaces(line: []const u8) usize {
    var count: usize = 0;
    while (count < line.len and line[count] == ' ') count += 1;
    return count;
}

fn resolveImportPath(allocator: Allocator, source_path: []const u8, import_ref: []const u8) ![]const u8 {
    const base_dir = std.fs.path.dirname(source_path) orelse ".";
    if (std.mem.endsWith(u8, import_ref, ".yaml") or std.mem.endsWith(u8, import_ref, ".yml") or std.mem.startsWith(u8, import_ref, "./") or std.mem.startsWith(u8, import_ref, "../")) {
        return try std.fs.path.resolve(allocator, &.{ base_dir, import_ref });
    }
    const with_ext = try std.fmt.allocPrint(allocator, "{s}.yaml", .{import_ref});
    defer allocator.free(with_ext);
    return try std.fs.path.resolve(allocator, &.{ base_dir, with_ext });
}

fn annotationValue(annotations: []const Annotation, key: []const u8) ?[]const u8 {
    for (annotations) |annotation| {
        if (std.mem.eql(u8, annotation.key, key)) return annotation.value;
    }
    return null;
}

fn shouldProjectClass(class: ClassDef) bool {
    if (annotationValue(class.annotations.items, "ghostcrab.native_entity_type") != null) return true;
    if (class.mixin) return false;
    if (!class.is_root) return false;
    const layer = annotationValue(class.annotations.items, "ghostcrab.layer") orelse "";
    return !std.mem.eql(u8, layer, "core") and !std.mem.eql(u8, layer, "generic_business");
}

fn shouldProjectSlot(index: *const SchemaIndex, slot: SlotDef) bool {
    if (annotationValue(slot.annotations.items, "ghostcrab.native_edge_type") != null) return true;
    if (!slot.is_root) return false;
    return isObjectRange(index, slot.range);
}

fn nativeEntityType(class: ClassDef) []const u8 {
    return annotationValue(class.annotations.items, "ghostcrab.native_entity_type") orelse class.name;
}

fn nativeEdgeType(slot: SlotDef) []const u8 {
    return annotationValue(slot.annotations.items, "ghostcrab.native_edge_type") orelse slot.name;
}

fn nativeEntityTypeByName(index: *const SchemaIndex, class_name: []const u8) ?[]const u8 {
    return index.native_type_by_class.get(class_name) orelse class_name;
}

fn isObjectRange(index: *const SchemaIndex, range: ?[]const u8) bool {
    const value = range orelse return false;
    if (isPrimitiveRange(value)) return false;
    if (index.enum_names.contains(value)) return false;
    return true;
}

fn isPrimitiveRange(value: []const u8) bool {
    const primitives = [_][]const u8{ "string", "integer", "float", "double", "boolean", "date", "datetime", "time", "decimal", "curie", "uri", "ncname" };
    for (primitives) |primitive| {
        if (std.mem.eql(u8, primitive, value)) return true;
    }
    return false;
}

fn expandCurie(allocator: Allocator, schema: Schema, value: ?[]const u8) !?[]const u8 {
    const raw = value orelse return null;
    if (std.mem.startsWith(u8, raw, "http://") or std.mem.startsWith(u8, raw, "https://")) return try allocator.dupe(u8, raw);
    if (std.mem.indexOfScalar(u8, raw, ':')) |idx| {
        const prefix = raw[0..idx];
        const local = raw[idx + 1 ..];
        for (schema.prefixes.items) |p| {
            if (std.mem.eql(u8, p.key, prefix)) return try std.fmt.allocPrint(allocator, "{s}{s}", .{ p.value, local });
        }
        return try allocator.dupe(u8, raw);
    }
    const default_prefix = schema.default_prefix orelse "ghostcrab";
    for (schema.prefixes.items) |p| {
        if (std.mem.eql(u8, p.key, default_prefix)) return try std.fmt.allocPrint(allocator, "{s}{s}", .{ p.value, raw });
    }
    return try allocator.dupe(u8, raw);
}

fn annotationsJson(allocator: Allocator, annotations: []const Annotation) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.writeAll("{");
    for (annotations, 0..) |annotation, i| {
        if (i > 0) try out.writer.writeAll(",");
        try out.writer.print("{f}:{f}", .{ std.json.fmt(annotation.key, .{}), std.json.fmt(annotation.value, .{}) });
    }
    try out.writer.writeAll("}");
    return try out.toOwnedSlice();
}

fn ontologyMetadataJson(allocator: Allocator, schema: Schema) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.print("{{\"linkml_id\":", .{});
    try writeOptionalJsonString(&out.writer, schema.id);
    try out.writer.writeAll(",\"linkml_name\":");
    try writeOptionalJsonString(&out.writer, schema.name);
    try out.writer.print(",\"source_path\":{f}}}", .{std.json.fmt(schema.source_path, .{})});
    return try out.toOwnedSlice();
}

fn classMetadataJson(allocator: Allocator, schema: Schema, class: ClassDef) ![]const u8 {
    const annotations = try annotationsJson(allocator, class.annotations.items);
    defer allocator.free(annotations);
    const class_uri = try expandCurie(allocator, schema, class.class_uri orelse class.name);
    defer if (class_uri) |value| allocator.free(value);
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.print("{{\"linkml_class\":{f},\"class_uri\":", .{std.json.fmt(class.name, .{})});
    try writeOptionalJsonString(&out.writer, class_uri);
    try out.writer.print(",\"abstract\":{},\"is_a\":", .{class.abstract});
    try writeOptionalJsonString(&out.writer, class.is_a);
    try out.writer.print(",\"ghostcrab\":{s}}}", .{annotations});
    return try out.toOwnedSlice();
}

fn slotMetadataJson(allocator: Allocator, schema: Schema, slot: SlotDef) ![]const u8 {
    const annotations = try annotationsJson(allocator, slot.annotations.items);
    defer allocator.free(annotations);
    const slot_uri = try expandCurie(allocator, schema, slot.slot_uri orelse slot.name);
    defer if (slot_uri) |value| allocator.free(value);
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.print("{{\"linkml_slot\":{f},\"slot_uri\":", .{std.json.fmt(slot.name, .{})});
    try writeOptionalJsonString(&out.writer, slot_uri);
    try out.writer.print(",\"ghostcrab\":{s}}}", .{annotations});
    return try out.toOwnedSlice();
}

fn enumMetadataJson(allocator: Allocator, enum_def: EnumDef) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.writeAll("{\"description\":");
    try writeOptionalJsonString(&out.writer, enum_def.description);
    try out.writer.writeAll("}");
    return try out.toOwnedSlice();
}

fn enumValueMetadataJson(allocator: Allocator, value: EnumValue) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.writeAll("{\"meaning\":");
    try writeOptionalJsonString(&out.writer, value.meaning);
    try out.writer.writeAll(",\"description\":");
    try writeOptionalJsonString(&out.writer, value.description);
    try out.writer.writeAll("}");
    return try out.toOwnedSlice();
}

fn writeOptionalJsonString(writer: *std.Io.Writer, value: ?[]const u8) !void {
    if (value) |text| try writer.print("{f}", .{std.json.fmt(text, .{})}) else try writer.writeAll("null");
}

fn emitTriples(allocator: Allocator, schema: *const Schema, index: *const SchemaIndex, ontology_id: []const u8, bundle_writer: *std.Io.Writer, nt_writer: *std.Io.Writer, count: *usize) !void {
    for (schema.classes.items) |class| {
        if (!shouldProjectClass(class)) continue;
        const class_uri = (try expandCurie(allocator, schema.*, class.class_uri orelse class.name)) orelse class.name;
        defer if (class_uri.ptr != class.name.ptr) allocator.free(class_uri);
        try emitTriple(allocator, ontology_id, count, bundle_writer, nt_writer, class_uri, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2002/07/owl#Class", false);
        try emitTriple(allocator, ontology_id, count, bundle_writer, nt_writer, class_uri, "http://www.w3.org/2000/01/rdf-schema#label", nativeEntityType(class), true);
        if (class.description) |description| try emitTriple(allocator, ontology_id, count, bundle_writer, nt_writer, class_uri, "http://www.w3.org/2004/02/skos/core#definition", description, true);
        if (class.is_a) |parent_name| {
            const parent_uri = try expandCurie(allocator, schema.*, index.class_uri_by_name.get(parent_name) orelse parent_name);
            defer if (parent_uri) |value| allocator.free(value);
            if (parent_uri) |value| try emitTriple(allocator, ontology_id, count, bundle_writer, nt_writer, class_uri, "http://www.w3.org/2000/01/rdf-schema#subClassOf", value, false);
        }
    }
    for (schema.slots.items) |slot| {
        if (!shouldProjectSlot(index, slot)) continue;
        const slot_uri = (try expandCurie(allocator, schema.*, slot.slot_uri orelse slot.name)) orelse slot.name;
        defer if (slot_uri.ptr != slot.name.ptr) allocator.free(slot_uri);
        try emitTriple(allocator, ontology_id, count, bundle_writer, nt_writer, slot_uri, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2002/07/owl#ObjectProperty", false);
        if (slot.domain) |domain| {
            const domain_uri = try expandCurie(allocator, schema.*, index.class_uri_by_name.get(domain) orelse domain);
            defer if (domain_uri) |value| allocator.free(value);
            if (domain_uri) |value| try emitTriple(allocator, ontology_id, count, bundle_writer, nt_writer, slot_uri, "http://www.w3.org/2000/01/rdf-schema#domain", value, false);
        }
        if (slot.range) |range| {
            const range_uri = try expandCurie(allocator, schema.*, index.class_uri_by_name.get(range) orelse range);
            defer if (range_uri) |value| allocator.free(value);
            if (range_uri) |value| try emitTriple(allocator, ontology_id, count, bundle_writer, nt_writer, slot_uri, "http://www.w3.org/2000/01/rdf-schema#range", value, false);
        }
    }
}

fn emitTriple(allocator: Allocator, ontology_id: []const u8, count: *usize, bundle_writer: *std.Io.Writer, nt_writer: *std.Io.Writer, subject: []const u8, predicate: []const u8, object_value: []const u8, literal: bool) !void {
    if (count.* > 0) try bundle_writer.writeAll(",");
    try bundle_writer.print("\n    {{ \"ontology_id\": {f}, \"triple_index\": {}, \"subject_kind\": \"iri\", \"subject\": {f}, \"predicate\": {f}, \"object_kind\": {f}, \"object_value\": {f}, \"object_datatype\": ", .{
        std.json.fmt(ontology_id, .{}),
        count.*,
        std.json.fmt(subject, .{}),
        std.json.fmt(predicate, .{}),
        std.json.fmt(if (literal) "literal" else "iri", .{}),
        std.json.fmt(object_value, .{}),
    });
    if (literal) try bundle_writer.writeAll("\"http://www.w3.org/2001/XMLSchema#string\"") else try bundle_writer.writeAll("null");
    try bundle_writer.print(", \"object_language\": null, \"source_line\": {f}, \"metadata_json\": \"{{}}\" }}", .{std.json.fmt("generated by mindbrain linkml interchange", .{})});
    if (literal) {
        const escaped = try escapeLiteral(allocator, object_value);
        defer allocator.free(escaped);
        try nt_writer.print("<{s}> <{s}> \"{s}\" .\n", .{ subject, predicate, escaped });
    } else {
        try nt_writer.print("<{s}> <{s}> <{s}> .\n", .{ subject, predicate, object_value });
    }
    count.* += 1;
}

fn escapeLiteral(allocator: Allocator, value: []const u8) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    for (value) |ch| {
        if (ch == '\\' or ch == '"') try out.writer.writeByte('\\');
        try out.writer.writeByte(ch);
    }
    return try out.toOwnedSlice();
}

fn lastOntologySegment(ontology_id: []const u8) []const u8 {
    if (std.mem.lastIndexOf(u8, ontology_id, "::")) |idx| return ontology_id[idx + 2 ..];
    return ontology_id;
}

const OntologyExport = struct { name: []const u8, version: []const u8 };

fn loadOntologyForExport(allocator: Allocator, db: Database, ontology_id: []const u8) !OntologyExport {
    const stmt = try prepare(db, "SELECT name, version FROM ontologies WHERE ontology_id = ?1");
    defer finalize(stmt);
    try bindText(stmt, 1, ontology_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.MissingRow;
    return .{
        .name = try dupeColumnText(allocator, stmt, 0),
        .version = try dupeColumnText(allocator, stmt, 1),
    };
}

fn writeClassesYaml(allocator: Allocator, db: Database, ontology_id: []const u8, writer: *std.Io.Writer) !void {
    const stmt = try prepare(db, "SELECT entity_type, label, metadata_json FROM ontology_entity_types WHERE ontology_id = ?1 ORDER BY entity_type");
    defer finalize(stmt);
    try bindText(stmt, 1, ontology_id);
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
        const entity_type = try dupeColumnText(allocator, stmt, 0);
        defer allocator.free(entity_type);
        const label = try dupeColumnText(allocator, stmt, 1);
        defer allocator.free(label);
        const metadata = try dupeColumnText(allocator, stmt, 2);
        defer allocator.free(metadata);
        const linkml_class = jsonStringField(metadata, "linkml_class") orelse entity_type;
        const class_uri = jsonStringField(metadata, "class_uri");
        try writer.print("  {s}:\n", .{linkml_class});
        if (class_uri) |value| try writer.print("    class_uri: {s}\n", .{value});
        try writer.print("    description: {s}\n    annotations:\n      ghostcrab.native_entity_type: {s}\n", .{ label, entity_type });
    }
}

fn writeSlotsYaml(allocator: Allocator, db: Database, ontology_id: []const u8, writer: *std.Io.Writer) !void {
    const stmt = try prepare(db, "SELECT edge_type, source_entity_type, target_entity_type, metadata_json FROM ontology_edge_types WHERE ontology_id = ?1 ORDER BY edge_type");
    defer finalize(stmt);
    try bindText(stmt, 1, ontology_id);
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
        const edge_type = try dupeColumnText(allocator, stmt, 0);
        defer allocator.free(edge_type);
        const source = try optionalColumnText(allocator, stmt, 1);
        defer if (source) |value| allocator.free(value);
        const target = try optionalColumnText(allocator, stmt, 2);
        defer if (target) |value| allocator.free(value);
        const metadata = try dupeColumnText(allocator, stmt, 3);
        defer allocator.free(metadata);
        const linkml_slot = jsonStringField(metadata, "linkml_slot") orelse edge_type;
        const slot_uri = jsonStringField(metadata, "slot_uri");
        try writer.print("  {s}:\n", .{linkml_slot});
        if (source) |value| try writer.print("    domain: {s}\n", .{value});
        if (target) |value| try writer.print("    range: {s}\n", .{value});
        if (slot_uri) |value| try writer.print("    slot_uri: {s}\n", .{value});
        try writer.print("    annotations:\n      ghostcrab.native_edge_type: {s}\n", .{edge_type});
    }
}

fn writeEnumsYaml(allocator: Allocator, db: Database, ontology_id: []const u8, writer: *std.Io.Writer) !void {
    const stmt = try prepare(db,
        \\SELECT d.namespace, d.dimension, v.value
        \\FROM ontology_dimensions d
        \\LEFT JOIN ontology_values v
        \\  ON v.ontology_id = d.ontology_id
        \\ AND v.namespace = d.namespace
        \\ AND v.dimension = d.dimension
        \\WHERE d.ontology_id = ?1
        \\ORDER BY d.namespace, d.dimension, v.value_id
    );
    defer finalize(stmt);
    try bindText(stmt, 1, ontology_id);
    var emitted = false;
    var current_namespace: ?[]const u8 = null;
    defer if (current_namespace) |value| allocator.free(value);
    var current_dimension: ?[]const u8 = null;
    defer if (current_dimension) |value| allocator.free(value);

    while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
        const namespace = try dupeColumnText(allocator, stmt, 0);
        defer allocator.free(namespace);
        const dimension = try dupeColumnText(allocator, stmt, 1);
        defer allocator.free(dimension);

        const changed = current_namespace == null or
            !std.mem.eql(u8, current_namespace.?, namespace) or
            !std.mem.eql(u8, current_dimension.?, dimension);
        if (changed) {
            emitted = true;
            const new_namespace = try allocator.dupe(u8, namespace);
            errdefer allocator.free(new_namespace);
            const new_dimension = try allocator.dupe(u8, dimension);
            if (current_namespace) |value| allocator.free(value);
            if (current_dimension) |value| allocator.free(value);
            current_namespace = new_namespace;
            current_dimension = new_dimension;
            try writer.print("  {s}:\n    permissible_values:\n", .{dimension});
        }

        const value = try optionalColumnText(allocator, stmt, 2);
        if (value) |text| {
            defer allocator.free(text);
            try writer.print("      {s}: {{}}\n", .{text});
        }
    }
    if (!emitted) try writer.writeAll("  {}\n");
}

fn jsonStringField(json_text: []const u8, field: []const u8) ?[]const u8 {
    const idx = std.mem.indexOf(u8, json_text, field) orelse return null;
    if (idx == 0 or json_text[idx - 1] != '"') return null;
    const after_field = idx + field.len;
    if (after_field + 2 > json_text.len or json_text[after_field] != '"' or json_text[after_field + 1] != ':') return null;
    var start = after_field + 2;
    while (start < json_text.len and (json_text[start] == ' ' or json_text[start] == '\t')) start += 1;
    if (start >= json_text.len or json_text[start] != '"') return null;
    start += 1;
    const end_rel = std.mem.indexOfScalar(u8, json_text[start..], '"') orelse return null;
    return json_text[start .. start + end_rel];
}

fn prepare(db: Database, sql: []const u8) !*c.sqlite3_stmt {
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) return error.PrepareFailed;
    return stmt.?;
}

fn finalize(stmt: *c.sqlite3_stmt) void {
    _ = c.sqlite3_finalize(stmt);
}

fn bindText(stmt: *c.sqlite3_stmt, index: c_int, value: []const u8) !void {
    if (c.sqlite3_bind_text(stmt, index, value.ptr, @intCast(value.len), facet_sqlite.sqliteTransient()) != c.SQLITE_OK) return error.BindFailed;
}

fn dupeColumnText(allocator: Allocator, stmt: *c.sqlite3_stmt, index: c_int) ![]const u8 {
    const len = c.sqlite3_column_bytes(stmt, index);
    const ptr = c.sqlite3_column_text(stmt, index) orelse return error.MissingRow;
    const bytes: []const u8 = @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
    return try allocator.dupe(u8, bytes);
}

fn optionalColumnText(allocator: Allocator, stmt: *c.sqlite3_stmt, index: c_int) !?[]const u8 {
    if (c.sqlite3_column_type(stmt, index) == c.SQLITE_NULL) return null;
    return try dupeColumnText(allocator, stmt, index);
}

test "LinkML interchange compiles reference ontology into native bundle" {
    var result = try compileLinkmlToBundle(std.testing.allocator, .{
        .input_path = "ontologies/immeuble-demo/core.yaml",
        .workspace_id = "immeuble-demo",
        .ontology_id = "immeuble-demo::core",
    });
    defer result.deinit(std.testing.allocator);
    try std.testing.expect(result.entity_type_count >= 7);
    try std.testing.expect(result.edge_type_count >= 7);
    try std.testing.expect(std.mem.indexOf(u8, result.bundle_json, "\"entity_type\": \"building\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.ntriples, "http://www.w3.org/2000/01/rdf-schema#domain") != null);
}

test "LinkML interchange imports bundle and exports YAML" {
    var result = try compileLinkmlToBundle(std.testing.allocator, .{
        .input_path = "ontologies/immeuble-demo/core.yaml",
        .workspace_id = "immeuble-demo",
        .ontology_id = "immeuble-demo::core",
    });
    defer result.deinit(std.testing.allocator);
    const yaml = try exportLinkmlFromBundleJson(std.testing.allocator, result.bundle_json, "immeuble-demo::core");
    defer std.testing.allocator.free(yaml);
    try std.testing.expect(std.mem.indexOf(u8, yaml, "classes:") != null);
    try std.testing.expect(std.mem.indexOf(u8, yaml, "ghostcrab.native_entity_type: building") != null);
    try std.testing.expect(std.mem.indexOf(u8, yaml, "ghostcrab.native_edge_type: owns") != null);
}

test "LinkML interchange rejects cyclic imports" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(zig16_compat.io(), .{
        .sub_path = "a.yaml",
        .data =
        \\id: test:a
        \\name: a
        \\imports:
        \\  - b.yaml
        \\classes:
        \\  A:
        \\    annotations:
        \\      ghostcrab.native_entity_type: a
        \\
        ,
        .flags = .{ .truncate = true },
    });
    try tmp.dir.writeFile(zig16_compat.io(), .{
        .sub_path = "b.yaml",
        .data =
        \\id: test:b
        \\name: b
        \\imports:
        \\  - a.yaml
        \\classes:
        \\  B:
        \\    annotations:
        \\      ghostcrab.native_entity_type: b
        \\
        ,
        .flags = .{ .truncate = true },
    });

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/a.yaml", .{tmp.sub_path[0..]});
    defer std.testing.allocator.free(path);
    try std.testing.expectError(error.CyclicImport, compileLinkmlToBundle(std.testing.allocator, .{
        .input_path = path,
        .workspace_id = "test",
        .ontology_id = "test::cycle",
    }));
}

test "LinkML interchange merges diamond imports once" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(zig16_compat.io(), .{
        .sub_path = "shared.yaml",
        .data =
        \\id: test:shared
        \\name: shared
        \\classes:
        \\  Shared:
        \\    annotations:
        \\      ghostcrab.native_entity_type: shared
        \\
        ,
        .flags = .{ .truncate = true },
    });
    try tmp.dir.writeFile(zig16_compat.io(), .{
        .sub_path = "a.yaml",
        .data =
        \\id: test:a
        \\name: a
        \\imports:
        \\  - shared.yaml
        \\classes:
        \\  A:
        \\    annotations:
        \\      ghostcrab.native_entity_type: a
        \\
        ,
        .flags = .{ .truncate = true },
    });
    try tmp.dir.writeFile(zig16_compat.io(), .{
        .sub_path = "b.yaml",
        .data =
        \\id: test:b
        \\name: b
        \\imports:
        \\  - shared.yaml
        \\classes:
        \\  B:
        \\    annotations:
        \\      ghostcrab.native_entity_type: b
        \\
        ,
        .flags = .{ .truncate = true },
    });
    try tmp.dir.writeFile(zig16_compat.io(), .{
        .sub_path = "root.yaml",
        .data =
        \\id: test:root
        \\name: root
        \\imports:
        \\  - a.yaml
        \\  - b.yaml
        \\classes:
        \\  Root:
        \\    annotations:
        \\      ghostcrab.native_entity_type: root
        \\
        ,
        .flags = .{ .truncate = true },
    });

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/root.yaml", .{tmp.sub_path[0..]});
    defer std.testing.allocator.free(path);
    var result = try compileLinkmlToBundle(std.testing.allocator, .{
        .input_path = path,
        .workspace_id = "test",
        .ontology_id = "test::diamond",
    });
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), countOccurrences(result.bundle_json, "\"entity_type\": \"shared\""));
}

test "LinkML interchange index preserves first duplicate class definition" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(zig16_compat.io(), .{
        .sub_path = "base.yaml",
        .data =
        \\id: test:base
        \\name: base
        \\classes:
        \\  Asset:
        \\    class_uri: test:ImportedAsset
        \\    annotations:
        \\      ghostcrab.native_entity_type: imported_asset
        \\
        ,
        .flags = .{ .truncate = true },
    });
    try tmp.dir.writeFile(zig16_compat.io(), .{
        .sub_path = "root.yaml",
        .data =
        \\id: test:root
        \\name: root
        \\prefixes:
        \\  test: https://example.test/
        \\imports:
        \\  - base.yaml
        \\classes:
        \\  Asset:
        \\    class_uri: test:LocalAsset
        \\    annotations:
        \\      ghostcrab.native_entity_type: local_asset
        \\  Holding:
        \\    class_uri: test:Holding
        \\    annotations:
        \\      ghostcrab.native_entity_type: holding
        \\slots:
        \\  owns:
        \\    domain: Holding
        \\    range: Asset
        \\    annotations:
        \\      ghostcrab.native_edge_type: owns
        \\
        ,
        .flags = .{ .truncate = true },
    });

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/root.yaml", .{tmp.sub_path[0..]});
    defer std.testing.allocator.free(path);
    var result = try compileLinkmlToBundle(std.testing.allocator, .{
        .input_path = path,
        .workspace_id = "test",
        .ontology_id = "test::duplicates",
    });
    defer result.deinit(std.testing.allocator);

    try std.testing.expect(std.mem.indexOf(u8, result.bundle_json, "\"target_entity_type\": \"imported_asset\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.bundle_json, "https://example.test/ImportedAsset") != null);
}

fn countOccurrences(haystack: []const u8, needle: []const u8) usize {
    var count: usize = 0;
    var rest = haystack;
    while (std.mem.indexOf(u8, rest, needle)) |index| {
        count += 1;
        rest = rest[index + needle.len ..];
    }
    return count;
}

test "compile import sets default_ontology_id" {
    var result = try compileLinkmlToBundle(std.testing.allocator, .{
        .input_path = "ontologies/immeuble-demo/core.yaml",
        .workspace_id = "ws-compile",
        .ontology_id = "ws-compile::core",
    });
    defer result.deinit(std.testing.allocator);

    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws-compile" });
    try importCompiledBundle(db, std.testing.allocator, result.bundle_json);
    try collections_sqlite.setDefaultOntology(db, "ws-compile", "ws-compile::core");

    const default_id = try collections_sqlite.defaultOntology(db, std.testing.allocator, "ws-compile");
    defer if (default_id) |id| std.testing.allocator.free(id);
    try std.testing.expect(default_id != null);
    try std.testing.expectEqualStrings("ws-compile::core", default_id.?);
}

test "compile import ensures workspace row exists for target workspace" {
    var result = try compileLinkmlToBundle(std.testing.allocator, .{
        .input_path = "ontologies/immeuble-demo/core.yaml",
        .workspace_id = "ws-only",
        .ontology_id = "ws-only::core",
    });
    defer result.deinit(std.testing.allocator);

    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws-only" });
    try importCompiledBundle(db, std.testing.allocator, result.bundle_json);

    const stmt = try facet_sqlite.prepare(db, "SELECT COUNT(*) FROM workspaces WHERE workspace_id = ?1");
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, "ws-only");
    try std.testing.expect(facet_sqlite.c.sqlite3_step(stmt) == facet_sqlite.c.SQLITE_ROW);
    try std.testing.expectEqual(@as(i64, 1), facet_sqlite.c.sqlite3_column_int64(stmt, 0));
}
