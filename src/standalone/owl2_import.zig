const std = @import("std");
const collections_io = @import("collections_io.zig");
const collections_sqlite = @import("collections_sqlite.zig");
const facet_sqlite = @import("facet_sqlite.zig");

const Allocator = std.mem.Allocator;
const Database = facet_sqlite.Database;
const c = facet_sqlite.c;

pub const rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
pub const rdfs_class = "http://www.w3.org/2000/01/rdf-schema#Class";
pub const rdfs_domain = "http://www.w3.org/2000/01/rdf-schema#domain";
pub const rdfs_range = "http://www.w3.org/2000/01/rdf-schema#range";
pub const rdfs_sub_class_of = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
pub const rdfs_sub_property_of = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
pub const owl_class = "http://www.w3.org/2002/07/owl#Class";
pub const owl_object_property = "http://www.w3.org/2002/07/owl#ObjectProperty";
pub const owl_datatype_property = "http://www.w3.org/2002/07/owl#DatatypeProperty";
pub const owl_ontology = "http://www.w3.org/2002/07/owl#Ontology";

pub const TermKind = enum {
    iri,
    blank,
    literal,

    pub fn label(self: TermKind) []const u8 {
        return switch (self) {
            .iri => "iri",
            .blank => "blank",
            .literal => "literal",
        };
    }
};

pub const Term = struct {
    kind: TermKind,
    value: []const u8,
    datatype: ?[]const u8 = null,
    language: ?[]const u8 = null,
};

pub const Triple = struct {
    subject: Term,
    predicate: []const u8,
    object: Term,
    source_line: []const u8,
};

pub const ImportOptions = struct {
    ontology_name: ?[]const u8 = null,
    materialize_graph: bool = false,
};

pub const ImportSummary = struct {
    ontology_id: []const u8,
    triples: usize = 0,
    classes: usize = 0,
    object_properties: usize = 0,
    datatype_properties: usize = 0,
    ontology_relations: usize = 0,
    graph_relations: usize = 0,
};

pub fn importNTriples(
    db: Database,
    allocator: Allocator,
    workspace_id: []const u8,
    ontology_id: []const u8,
    content: []const u8,
    options: ImportOptions,
) !ImportSummary {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const scratch = arena.allocator();

    try db.exec("BEGIN");
    errdefer db.exec("ROLLBACK") catch {};

    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = workspace_id, .label = workspace_id });
    try collections_sqlite.ensureOntology(db, .{
        .ontology_id = ontology_id,
        .workspace_id = workspace_id,
        .name = options.ontology_name orelse ontology_id,
        .source_kind = "owl2",
        .metadata_json = "{\"format\":\"ntriples\"}",
    });
    try seedOwlNamespaces(db, ontology_id);

    var summary = ImportSummary{ .ontology_id = ontology_id };
    var line_it = std.mem.splitScalar(u8, content, '\n');
    var line_no: usize = 0;
    while (line_it.next()) |raw_line| {
        line_no += 1;
        const line = std.mem.trim(u8, raw_line, " \t\r\n");
        if (line.len == 0 or line[0] == '#') continue;

        const triple = try parseNTripleLine(scratch, line);
        summary.triples += 1;
        try collections_sqlite.upsertOntologyTriple(db, .{
            .ontology_id = ontology_id,
            .triple_index = @intCast(summary.triples),
            .subject_kind = triple.subject.kind.label(),
            .subject = triple.subject.value,
            .predicate = triple.predicate,
            .object_kind = triple.object.kind.label(),
            .object_value = triple.object.value,
            .object_datatype = triple.object.datatype,
            .object_language = triple.object.language,
            .source_line = triple.source_line,
            .metadata_json = try lineMetadata(scratch, line_no),
        });

        try projectTriple(db, scratch, workspace_id, ontology_id, triple, options.materialize_graph, &summary);
    }

    try db.exec("COMMIT");
    return summary;
}

pub fn exportNTriples(db: Database, allocator: Allocator, ontology_id: []const u8) ![]const u8 {
    const sql =
        \\SELECT source_line
        \\FROM ontology_triples_raw
        \\WHERE ontology_id = ?1
        \\ORDER BY triple_index
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, ontology_id);

    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try out.writer.writeAll(columnText(stmt, 0));
        try out.writer.writeByte('\n');
    }
    return out.toOwnedSlice();
}

pub fn parseNTripleLine(allocator: Allocator, line: []const u8) !Triple {
    var index: usize = 0;
    const subject = try parseTerm(allocator, line, &index);
    if (subject.kind == .literal) return error.InvalidSubject;
    skipSpaces(line, &index);
    const predicate_term = try parseTerm(allocator, line, &index);
    if (predicate_term.kind != .iri) return error.InvalidPredicate;
    skipSpaces(line, &index);
    const object = try parseTerm(allocator, line, &index);
    skipSpaces(line, &index);
    if (index >= line.len or line[index] != '.') return error.ExpectedDot;
    index += 1;
    skipSpaces(line, &index);
    if (index != line.len) return error.TrailingContent;
    return .{
        .subject = subject,
        .predicate = predicate_term.value,
        .object = object,
        .source_line = try allocator.dupe(u8, line),
    };
}

fn projectTriple(
    db: Database,
    allocator: Allocator,
    workspace_id: []const u8,
    ontology_id: []const u8,
    triple: Triple,
    materialize_graph: bool,
    summary: *ImportSummary,
) !void {
    if (triple.subject.kind == .iri and std.mem.eql(u8, triple.predicate, rdf_type) and triple.object.kind == .iri) {
        if (std.mem.eql(u8, triple.object.value, owl_class) or std.mem.eql(u8, triple.object.value, rdfs_class)) {
            try upsertOntologyNode(db, allocator, ontology_id, triple.subject.value, "owl_class");
            summary.classes += 1;
            return;
        }
        if (std.mem.eql(u8, triple.object.value, owl_object_property)) {
            try ensureEdgeTypeForIri(db, allocator, ontology_id, triple.subject.value);
            try upsertOntologyNode(db, allocator, ontology_id, triple.subject.value, "owl_object_property");
            summary.object_properties += 1;
            return;
        }
        if (std.mem.eql(u8, triple.object.value, owl_datatype_property)) {
            try ensureDatatypeProperty(db, allocator, ontology_id, triple.subject.value);
            try upsertOntologyNode(db, allocator, ontology_id, triple.subject.value, "owl_datatype_property");
            summary.datatype_properties += 1;
            return;
        }
        if (!std.mem.eql(u8, triple.object.value, owl_ontology)) {
            const entity_type = try localNameAlloc(allocator, triple.object.value);
            try collections_sqlite.ensureEntityType(db, .{ .ontology_id = ontology_id, .entity_type = entity_type, .label = entity_type, .metadata_json = try iriMetadata(allocator, triple.object.value) });
            try upsertOntologyNode(db, allocator, ontology_id, triple.subject.value, entity_type);
            if (materialize_graph) try upsertGraphNode(db, allocator, workspace_id, ontology_id, triple.subject.value, entity_type);
        }
        return;
    }

    if (triple.object.kind == .literal) return;

    const edge_type = if (knownPredicateName(triple.predicate)) |name| name else try localNameAlloc(allocator, triple.predicate);
    try collections_sqlite.ensureEdgeType(db, .{ .ontology_id = ontology_id, .edge_type = edge_type, .directed = true, .metadata_json = try iriMetadata(allocator, triple.predicate) });
    try upsertOntologyNode(db, allocator, ontology_id, triple.subject.value, termNodeType(triple.subject));
    try upsertOntologyNode(db, allocator, ontology_id, triple.object.value, termNodeType(triple.object));
    try collections_sqlite.upsertOntologyRelation(db, .{
        .ontology_id = ontology_id,
        .relation_id = stableId(&.{ "ontology-relation", triple.subject.value, triple.predicate, triple.object.value }),
        .edge_type = edge_type,
        .source_entity_id = stableId(&.{ "ontology-entity", triple.subject.value }),
        .target_entity_id = stableId(&.{ "ontology-entity", triple.object.value }),
        .metadata_json = try iriMetadata(allocator, triple.predicate),
    });
    summary.ontology_relations += 1;

    if (materialize_graph and triple.subject.kind == .iri and triple.object.kind == .iri) {
        try upsertGraphNode(db, allocator, workspace_id, ontology_id, triple.subject.value, "owl_resource");
        try upsertGraphNode(db, allocator, workspace_id, ontology_id, triple.object.value, "owl_resource");
        try collections_sqlite.upsertRelationRaw(db, .{
            .workspace_id = workspace_id,
            .ontology_id = ontology_id,
            .relation_id = stableId(&.{ "graph-relation", triple.subject.value, triple.predicate, triple.object.value }),
            .edge_type = edge_type,
            .source_entity_id = stableId(&.{ "graph-entity", triple.subject.value }),
            .target_entity_id = stableId(&.{ "graph-entity", triple.object.value }),
            .metadata_json = try iriMetadata(allocator, triple.predicate),
        });
        summary.graph_relations += 1;
    }
}

fn seedOwlNamespaces(db: Database, ontology_id: []const u8) !void {
    try collections_sqlite.ensureNamespace(db, .{ .ontology_id = ontology_id, .namespace = "rdf", .label = "RDF" });
    try collections_sqlite.ensureNamespace(db, .{ .ontology_id = ontology_id, .namespace = "rdfs", .label = "RDFS" });
    try collections_sqlite.ensureNamespace(db, .{ .ontology_id = ontology_id, .namespace = "owl", .label = "OWL" });
    try collections_sqlite.ensureNamespace(db, .{ .ontology_id = ontology_id, .namespace = "xsd", .label = "XSD" });
    try collections_sqlite.ensureEntityType(db, .{ .ontology_id = ontology_id, .entity_type = "owl_class", .label = "OWL Class" });
    try collections_sqlite.ensureEntityType(db, .{ .ontology_id = ontology_id, .entity_type = "owl_resource", .label = "OWL Resource" });
    try collections_sqlite.ensureEntityType(db, .{ .ontology_id = ontology_id, .entity_type = "owl_blank_node", .label = "OWL Blank Node" });
}

fn termNodeType(term: Term) []const u8 {
    return switch (term.kind) {
        .blank => "owl_blank_node",
        .iri => "owl_resource",
        .literal => "owl_literal",
    };
}

fn upsertOntologyNode(db: Database, allocator: Allocator, ontology_id: []const u8, iri: []const u8, entity_type: []const u8) !void {
    try collections_sqlite.ensureEntityType(db, .{ .ontology_id = ontology_id, .entity_type = entity_type, .label = entity_type });
    try collections_sqlite.upsertOntologyEntity(db, .{
        .ontology_id = ontology_id,
        .entity_id = stableId(&.{ "ontology-entity", iri }),
        .entity_type = entity_type,
        .name = try localNameAlloc(allocator, iri),
        .metadata_json = try iriMetadata(allocator, iri),
    });
}

fn upsertGraphNode(db: Database, allocator: Allocator, workspace_id: []const u8, ontology_id: []const u8, iri: []const u8, entity_type: []const u8) !void {
    try collections_sqlite.upsertEntityRaw(db, .{
        .workspace_id = workspace_id,
        .ontology_id = ontology_id,
        .entity_id = stableId(&.{ "graph-entity", iri }),
        .entity_type = entity_type,
        .name = try localNameAlloc(allocator, iri),
        .metadata_json = try iriMetadata(allocator, iri),
    });
}

fn ensureEdgeTypeForIri(db: Database, allocator: Allocator, ontology_id: []const u8, iri: []const u8) !void {
    const edge_type = try localNameAlloc(allocator, iri);
    try collections_sqlite.ensureEdgeType(db, .{ .ontology_id = ontology_id, .edge_type = edge_type, .directed = true, .metadata_json = try iriMetadata(allocator, iri) });
}

fn ensureDatatypeProperty(db: Database, allocator: Allocator, ontology_id: []const u8, iri: []const u8) !void {
    try collections_sqlite.ensureNamespace(db, .{ .ontology_id = ontology_id, .namespace = "owl_datatype", .label = "OWL Datatype Properties" });
    try collections_sqlite.ensureDimension(db, .{
        .ontology_id = ontology_id,
        .namespace = "owl_datatype",
        .dimension = try localNameAlloc(allocator, iri),
        .value_type = "text",
        .is_multi = true,
        .metadata_json = try iriMetadata(allocator, iri),
    });
}

fn parseTerm(allocator: Allocator, line: []const u8, index: *usize) !Term {
    skipSpaces(line, index);
    if (index.* >= line.len) return error.UnexpectedEnd;
    if (line[index.*] == '<') {
        index.* += 1;
        const start = index.*;
        while (index.* < line.len and line[index.*] != '>') : (index.* += 1) {}
        if (index.* >= line.len) return error.UnclosedIri;
        const iri = try allocator.dupe(u8, line[start..index.*]);
        index.* += 1;
        return .{ .kind = .iri, .value = iri };
    }
    if (std.mem.startsWith(u8, line[index.*..], "_:")) {
        const start = index.*;
        index.* += 2;
        while (index.* < line.len and !std.ascii.isWhitespace(line[index.*]) and line[index.*] != '.') : (index.* += 1) {}
        return .{ .kind = .blank, .value = try allocator.dupe(u8, line[start..index.*]) };
    }
    if (line[index.*] == '"') return parseLiteral(allocator, line, index);
    return error.InvalidTerm;
}

fn parseLiteral(allocator: Allocator, line: []const u8, index: *usize) !Term {
    index.* += 1;
    var value = std.ArrayList(u8).empty;
    while (index.* < line.len) : (index.* += 1) {
        const ch = line[index.*];
        if (ch == '"') {
            index.* += 1;
            var language: ?[]const u8 = null;
            var datatype: ?[]const u8 = null;
            if (index.* < line.len and line[index.*] == '@') {
                index.* += 1;
                const start = index.*;
                while (index.* < line.len and (std.ascii.isAlphanumeric(line[index.*]) or line[index.*] == '-')) : (index.* += 1) {}
                language = try allocator.dupe(u8, line[start..index.*]);
            } else if (std.mem.startsWith(u8, line[index.*..], "^^")) {
                index.* += 2;
                if (index.* >= line.len or line[index.*] != '<') return error.InvalidDatatype;
                index.* += 1;
                const start = index.*;
                while (index.* < line.len and line[index.*] != '>') : (index.* += 1) {}
                if (index.* >= line.len) return error.UnclosedIri;
                datatype = try allocator.dupe(u8, line[start..index.*]);
                index.* += 1;
            }
            return .{ .kind = .literal, .value = try value.toOwnedSlice(allocator), .datatype = datatype, .language = language };
        }
        if (ch == '\\') {
            index.* += 1;
            if (index.* >= line.len) return error.InvalidEscape;
            const escaped = switch (line[index.*]) {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                '"' => '"',
                '\\' => '\\',
                else => line[index.*],
            };
            try value.append(allocator, escaped);
        } else {
            try value.append(allocator, ch);
        }
    }
    return error.UnclosedLiteral;
}

fn skipSpaces(line: []const u8, index: *usize) void {
    while (index.* < line.len and std.ascii.isWhitespace(line[index.*])) : (index.* += 1) {}
}

fn knownPredicateName(predicate: []const u8) ?[]const u8 {
    if (std.mem.eql(u8, predicate, rdfs_sub_class_of)) return "subClassOf";
    if (std.mem.eql(u8, predicate, rdfs_sub_property_of)) return "subPropertyOf";
    if (std.mem.eql(u8, predicate, rdfs_domain)) return "domain";
    if (std.mem.eql(u8, predicate, rdfs_range)) return "range";
    if (std.mem.endsWith(u8, predicate, "#equivalentClass")) return "equivalentClass";
    if (std.mem.endsWith(u8, predicate, "#equivalentProperty")) return "equivalentProperty";
    if (std.mem.endsWith(u8, predicate, "#disjointWith")) return "disjointWith";
    if (std.mem.endsWith(u8, predicate, "#inverseOf")) return "inverseOf";
    if (std.mem.endsWith(u8, predicate, "#sameAs")) return "sameAs";
    if (std.mem.endsWith(u8, predicate, "#differentFrom")) return "differentFrom";
    if (std.mem.endsWith(u8, predicate, "#onProperty")) return "onProperty";
    if (std.mem.endsWith(u8, predicate, "#someValuesFrom")) return "someValuesFrom";
    if (std.mem.endsWith(u8, predicate, "#allValuesFrom")) return "allValuesFrom";
    return null;
}

fn localNameAlloc(allocator: Allocator, iri: []const u8) ![]const u8 {
    const hash_index = std.mem.lastIndexOfScalar(u8, iri, '#');
    const slash_index = std.mem.lastIndexOfScalar(u8, iri, '/');
    const colon_index = std.mem.lastIndexOfScalar(u8, iri, ':');
    const pos = @max(hash_index orelse 0, @max(slash_index orelse 0, colon_index orelse 0));
    if (pos + 1 < iri.len) return allocator.dupe(u8, iri[pos + 1 ..]);
    return allocator.dupe(u8, iri);
}

fn iriMetadata(allocator: Allocator, iri: []const u8) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    try out.writer.writeAll("{\"iri\":");
    try out.writer.print("{f}", .{std.json.fmt(iri, .{})});
    try out.writer.writeByte('}');
    return out.toOwnedSlice();
}

fn lineMetadata(allocator: Allocator, line_no: usize) ![]const u8 {
    return std.fmt.allocPrint(allocator, "{{\"line\":{}}}", .{line_no});
}

fn stableId(parts: []const []const u8) u64 {
    var h = std.hash.Wyhash.init(0);
    for (parts) |part| {
        h.update(part);
        h.update(&.{0});
    }
    const value = h.final() & 0x7fff_ffff_ffff_ffff;
    return if (value == 0) 1 else value;
}

fn columnText(stmt: *c.sqlite3_stmt, index: c_int) []const u8 {
    const len: usize = @intCast(c.sqlite3_column_bytes(stmt, index));
    const ptr = c.sqlite3_column_text(stmt, index) orelse return "";
    return @as([*]const u8, @ptrCast(ptr))[0..len];
}

fn countRows(db: Database, sql: []const u8) !i64 {
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StepFailed;
    return c.sqlite3_column_int64(stmt, 0);
}

fn countRowsBound(db: Database, sql: []const u8, value: []const u8) !i64 {
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, value);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StepFailed;
    return c.sqlite3_column_int64(stmt, 0);
}

fn countFixtureTriples(content: []const u8) usize {
    var count: usize = 0;
    var it = std.mem.splitScalar(u8, content, '\n');
    while (it.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r\n");
        if (line.len == 0 or line[0] == '#') continue;
        count += 1;
    }
    return count;
}

fn normalizedFixtureNTriples(allocator: Allocator, content: []const u8) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    var it = std.mem.splitScalar(u8, content, '\n');
    while (it.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r\n");
        if (line.len == 0 or line[0] == '#') continue;
        try out.writer.writeAll(line);
        try out.writer.writeByte('\n');
    }
    return out.toOwnedSlice();
}

test "parse official docs source ntriples fixtures" {
    const allocator = std.testing.allocator;
    const fixture_paths = [_][]const u8{
        "docs/source/owl2-core.nt",
        "docs/source/owl2-rl-relations.nt",
        "docs/source/owl2-restrictions.nt",
    };
    for (fixture_paths) |path| {
        const fixture = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, allocator, .limited(1024 * 1024));
        defer allocator.free(fixture);
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        var parsed: usize = 0;
        var it = std.mem.splitScalar(u8, fixture, '\n');
        while (it.next()) |raw_line| {
            const line = std.mem.trim(u8, raw_line, " \t\r\n");
            if (line.len == 0 or line[0] == '#') continue;
            _ = try parseNTripleLine(arena.allocator(), line);
            parsed += 1;
        }
        try std.testing.expect(parsed > 0);
    }
}

test "import every docs source fixture into sqlite" {
    const allocator = std.testing.allocator;
    const fixtures = [_]struct {
        path: []const u8,
        ontology_id: []const u8,
        expected_fragment: []const u8,
    }{
        .{
            .path = "docs/source/owl2-core.nt",
            .ontology_id = "onto-core",
            .expected_fragment = "http://www.w3.org/2002/07/owl#Class",
        },
        .{
            .path = "docs/source/owl2-rl-relations.nt",
            .ontology_id = "onto-rl",
            .expected_fragment = "http://www.w3.org/2002/07/owl#sameAs",
        },
        .{
            .path = "docs/source/owl2-restrictions.nt",
            .ontology_id = "onto-restrictions",
            .expected_fragment = "_:managedEmployeeRestriction",
        },
    };

    for (fixtures) |fixture_spec| {
        var db = try Database.open(":memory:");
        defer db.close();
        try db.applyStandaloneSchema();

        const fixture = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, fixture_spec.path, allocator, .limited(1024 * 1024));
        defer allocator.free(fixture);
        const expected_triples = countFixtureTriples(fixture);
        const expected_export = try normalizedFixtureNTriples(allocator, fixture);
        defer allocator.free(expected_export);

        const summary = try importNTriples(db, allocator, "ws-owl", fixture_spec.ontology_id, fixture, .{
            .ontology_name = fixture_spec.ontology_id,
            .materialize_graph = true,
        });
        try std.testing.expectEqual(expected_triples, summary.triples);
        try std.testing.expectEqual(
            @as(i64, @intCast(expected_triples)),
            try countRowsBound(db, "SELECT COUNT(*) FROM ontology_triples_raw WHERE ontology_id = ?1", fixture_spec.ontology_id),
        );
        try std.testing.expect(try countRowsBound(db, "SELECT COUNT(*) FROM ontology_namespaces WHERE ontology_id = ?1", fixture_spec.ontology_id) >= 4);

        const exported = try exportNTriples(db, allocator, fixture_spec.ontology_id);
        defer allocator.free(exported);
        try std.testing.expectEqualStrings(expected_export, exported);
        try std.testing.expect(std.mem.indexOf(u8, exported, fixture_spec.expected_fragment) != null);
    }
}

test "core fixture projects ontology and graph rows" {
    const allocator = std.testing.allocator;
    var db = try Database.open(":memory:");
    defer db.close();
    try db.applyStandaloneSchema();

    const fixture = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "docs/source/owl2-core.nt", allocator, .limited(1024 * 1024));
    defer allocator.free(fixture);
    const summary = try importNTriples(db, allocator, "ws-core", "onto-core", fixture, .{
        .ontology_name = "OWL core fixture",
        .materialize_graph = true,
    });
    try std.testing.expectEqual(@as(usize, 17), summary.triples);
    try std.testing.expectEqual(@as(usize, 3), summary.classes);
    try std.testing.expectEqual(@as(usize, 1), summary.object_properties);
    try std.testing.expectEqual(@as(usize, 1), summary.datatype_properties);
    try std.testing.expect(try countRowsBound(db, "SELECT COUNT(*) FROM ontology_entity_types WHERE ontology_id = ?1", "onto-core") > 0);
    try std.testing.expect(try countRows(db, "SELECT COUNT(*) FROM ontology_edge_types WHERE ontology_id = 'onto-core' AND edge_type = 'worksFor'") == 1);
    try std.testing.expect(try countRows(db, "SELECT COUNT(*) FROM ontology_dimensions WHERE ontology_id = 'onto-core' AND namespace = 'owl_datatype' AND dimension = 'employeeNumber'") == 1);
    try std.testing.expect(try countRows(db, "SELECT COUNT(*) FROM relations_raw WHERE workspace_id = 'ws-core'") > 0);
}

test "owl rl fixture imports relation axioms into ontology graph" {
    const allocator = std.testing.allocator;
    var db = try Database.open(":memory:");
    defer db.close();
    try db.applyStandaloneSchema();

    const fixture = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "docs/source/owl2-rl-relations.nt", allocator, .limited(1024 * 1024));
    defer allocator.free(fixture);
    const summary = try importNTriples(db, allocator, "ws-rl", "onto-rl", fixture, .{
        .ontology_name = "OWL RL fixture",
        .materialize_graph = true,
    });
    try std.testing.expectEqual(@as(usize, 15), summary.triples);
    try std.testing.expect(summary.ontology_relations >= 9);
    try std.testing.expect(summary.graph_relations >= 9);
    try std.testing.expect(try countRows(db, "SELECT COUNT(*) FROM ontology_edge_types WHERE ontology_id = 'onto-rl' AND edge_type = 'subClassOf'") == 1);
    try std.testing.expect(try countRows(db, "SELECT COUNT(*) FROM ontology_edge_types WHERE ontology_id = 'onto-rl' AND edge_type = 'subPropertyOf'") == 1);
    try std.testing.expect(try countRows(db, "SELECT COUNT(*) FROM ontology_edge_types WHERE ontology_id = 'onto-rl' AND edge_type = 'inverseOf'") == 1);
    try std.testing.expect(try countRows(db, "SELECT COUNT(*) FROM ontology_edge_types WHERE ontology_id = 'onto-rl' AND edge_type = 'sameAs'") == 1);
    try std.testing.expect(try countRows(db, "SELECT COUNT(*) FROM ontology_edge_types WHERE ontology_id = 'onto-rl' AND edge_type = 'disjointWith'") == 1);
}

test "restriction fixture preserves blank nodes and typed literals" {
    const allocator = std.testing.allocator;
    var db = try Database.open(":memory:");
    defer db.close();
    try db.applyStandaloneSchema();

    const fixture = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "docs/source/owl2-restrictions.nt", allocator, .limited(1024 * 1024));
    defer allocator.free(fixture);
    const summary = try importNTriples(db, allocator, "ws-restrictions", "onto-restrictions", fixture, .{
        .ontology_name = "OWL restrictions fixture",
        .materialize_graph = true,
    });
    try std.testing.expectEqual(@as(usize, 14), summary.triples);
    try std.testing.expect(try countRows(db, "SELECT COUNT(*) FROM ontology_triples_raw WHERE ontology_id = 'onto-restrictions' AND subject_kind = 'blank'") >= 6);
    try std.testing.expect(try countRows(db, "SELECT COUNT(*) FROM ontology_triples_raw WHERE ontology_id = 'onto-restrictions' AND object_kind = 'blank'") >= 3);
    try std.testing.expect(try countRows(db, "SELECT COUNT(*) FROM ontology_triples_raw WHERE ontology_id = 'onto-restrictions' AND object_kind = 'literal' AND object_datatype = 'http://www.w3.org/2001/XMLSchema#nonNegativeInteger' AND object_value = '1'") == 1);
    try std.testing.expect(try countRows(db, "SELECT COUNT(*) FROM ontology_edge_types WHERE ontology_id = 'onto-restrictions' AND edge_type = 'onProperty'") == 1);
    try std.testing.expect(try countRows(db, "SELECT COUNT(*) FROM ontology_edge_types WHERE ontology_id = 'onto-restrictions' AND edge_type = 'someValuesFrom'") == 1);
}

test "ontology triples survive taxonomies bundle roundtrip" {
    const allocator = std.testing.allocator;
    var source_db = try Database.open(":memory:");
    defer source_db.close();
    try source_db.applyStandaloneSchema();

    const fixture_paths = [_][]const u8{
        "docs/source/owl2-core.nt",
        "docs/source/owl2-rl-relations.nt",
        "docs/source/owl2-restrictions.nt",
    };
    const ontology_ids = [_][]const u8{ "onto-core", "onto-rl", "onto-restrictions" };
    for (fixture_paths, ontology_ids) |path, ontology_id| {
        const fixture = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, allocator, .limited(1024 * 1024));
        defer allocator.free(fixture);
        _ = try importNTriples(source_db, allocator, "ws-bundle", ontology_id, fixture, .{
            .ontology_name = ontology_id,
            .materialize_graph = true,
        });
    }

    const bundle = try collections_io.exportToJson(allocator, source_db, .{ .taxonomies = "ws-bundle" });
    defer allocator.free(bundle);

    var dest_db = try Database.open(":memory:");
    defer dest_db.close();
    try dest_db.applyStandaloneSchema();
    try collections_io.importBundleJson(dest_db, allocator, bundle);

    for (fixture_paths, ontology_ids) |path, ontology_id| {
        const fixture = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, allocator, .limited(1024 * 1024));
        defer allocator.free(fixture);
        const expected_export = try normalizedFixtureNTriples(allocator, fixture);
        defer allocator.free(expected_export);
        const exported = try exportNTriples(dest_db, allocator, ontology_id);
        defer allocator.free(exported);
        try std.testing.expectEqualStrings(expected_export, exported);
    }
}
