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

// ImportSession holds pre-prepared statements and per-import dedup sets to
// reduce SQLite prepare/finalize overhead from O(T × stmts) to O(1) per import.
const ImportSession = struct {
    db: Database,
    workspace_id: []const u8,
    ontology_id: []const u8,
    materialize_graph: bool,
    scratch: Allocator,

    stmt_triple: *c.sqlite3_stmt,
    stmt_entity_type: *c.sqlite3_stmt,
    stmt_edge_type: *c.sqlite3_stmt,
    stmt_ont_entity: *c.sqlite3_stmt,
    stmt_ont_relation: *c.sqlite3_stmt,
    stmt_entity_raw: ?*c.sqlite3_stmt,
    stmt_relation_raw: ?*c.sqlite3_stmt,

    // Dedup sets: skip re-sending identical ensure calls for the same
    // entity_type/edge_type within one import.  Keys live in the scratch arena.
    seen_entity_types: std.StringHashMapUnmanaged(void),
    seen_edge_types: std.StringHashMapUnmanaged(void),

    fn init(
        db: Database,
        scratch: Allocator,
        workspace_id: []const u8,
        ontology_id: []const u8,
        materialize_graph: bool,
    ) !ImportSession {
        const stmt_triple = try facet_sqlite.prepare(db,
            \\INSERT INTO ontology_triples_raw(ontology_id, triple_index, subject_kind, subject, predicate, object_kind, object_value, object_datatype, object_language, source_line, metadata_json)
            \\VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            \\ON CONFLICT(ontology_id, triple_index) DO UPDATE SET
            \\    subject_kind = excluded.subject_kind,
            \\    subject = excluded.subject,
            \\    predicate = excluded.predicate,
            \\    object_kind = excluded.object_kind,
            \\    object_value = excluded.object_value,
            \\    object_datatype = excluded.object_datatype,
            \\    object_language = excluded.object_language,
            \\    source_line = excluded.source_line,
            \\    metadata_json = excluded.metadata_json
        );
        errdefer facet_sqlite.finalize(stmt_triple);

        const stmt_entity_type = try facet_sqlite.prepare(db,
            \\INSERT INTO ontology_entity_types(ontology_id, entity_type, label, metadata_json)
            \\VALUES(?1, ?2, ?3, ?4)
            \\ON CONFLICT(ontology_id, entity_type) DO UPDATE SET
            \\    label = COALESCE(excluded.label, ontology_entity_types.label),
            \\    metadata_json = excluded.metadata_json
        );
        errdefer facet_sqlite.finalize(stmt_entity_type);

        const stmt_edge_type = try facet_sqlite.prepare(db,
            \\INSERT INTO ontology_edge_types(ontology_id, edge_type, directed, source_entity_type, target_entity_type, metadata_json)
            \\VALUES(?1, ?2, ?3, ?4, ?5, ?6)
            \\ON CONFLICT(ontology_id, edge_type) DO UPDATE SET
            \\    directed = excluded.directed,
            \\    source_entity_type = excluded.source_entity_type,
            \\    target_entity_type = excluded.target_entity_type,
            \\    metadata_json = excluded.metadata_json
        );
        errdefer facet_sqlite.finalize(stmt_edge_type);

        const stmt_ont_entity = try facet_sqlite.prepare(db,
            \\INSERT INTO ontology_entities_raw(ontology_id, entity_id, entity_type, name, metadata_json)
            \\VALUES(?1, ?2, ?3, ?4, ?5)
            \\ON CONFLICT(ontology_id, entity_id) DO UPDATE SET
            \\    entity_type = excluded.entity_type,
            \\    name = excluded.name,
            \\    metadata_json = excluded.metadata_json
        );
        errdefer facet_sqlite.finalize(stmt_ont_entity);

        const stmt_ont_relation = try facet_sqlite.prepare(db,
            \\INSERT INTO ontology_relations_raw(ontology_id, relation_id, edge_type, source_entity_id, target_entity_id, metadata_json)
            \\VALUES(?1, ?2, ?3, ?4, ?5, ?6)
            \\ON CONFLICT(ontology_id, relation_id) DO UPDATE SET
            \\    edge_type = excluded.edge_type,
            \\    source_entity_id = excluded.source_entity_id,
            \\    target_entity_id = excluded.target_entity_id,
            \\    metadata_json = excluded.metadata_json
        );
        errdefer facet_sqlite.finalize(stmt_ont_relation);

        var stmt_entity_raw: ?*c.sqlite3_stmt = null;
        var stmt_relation_raw: ?*c.sqlite3_stmt = null;
        if (materialize_graph) {
            const er = try facet_sqlite.prepare(db,
                \\INSERT INTO entities_raw(workspace_id, ontology_id, entity_id, entity_type, name, confidence, metadata_json)
                \\VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
                \\ON CONFLICT(workspace_id, entity_id) DO UPDATE SET
                \\    ontology_id = excluded.ontology_id,
                \\    entity_type = excluded.entity_type,
                \\    name = excluded.name,
                \\    confidence = excluded.confidence,
                \\    metadata_json = excluded.metadata_json
            );
            errdefer facet_sqlite.finalize(er);
            stmt_entity_raw = er;

            stmt_relation_raw = try facet_sqlite.prepare(db,
                \\INSERT INTO relations_raw(workspace_id, ontology_id, relation_id, edge_type, source_entity_id, target_entity_id, valid_from, valid_to, confidence, metadata_json)
                \\VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                \\ON CONFLICT(workspace_id, relation_id) DO UPDATE SET
                \\    ontology_id = excluded.ontology_id,
                \\    edge_type = excluded.edge_type,
                \\    source_entity_id = excluded.source_entity_id,
                \\    target_entity_id = excluded.target_entity_id,
                \\    valid_from = excluded.valid_from,
                \\    valid_to = excluded.valid_to,
                \\    confidence = excluded.confidence,
                \\    metadata_json = excluded.metadata_json
            );
            errdefer if (stmt_relation_raw) |stmt| facet_sqlite.finalize(stmt);
        }

        // Pre-seed entity types written by seedOwlNamespaces so we skip them.
        var seen_entity_types = std.StringHashMapUnmanaged(void){};
        try seen_entity_types.put(scratch, "owl_class", {});
        try seen_entity_types.put(scratch, "owl_resource", {});
        try seen_entity_types.put(scratch, "owl_blank_node", {});

        return .{
            .db = db,
            .workspace_id = workspace_id,
            .ontology_id = ontology_id,
            .materialize_graph = materialize_graph,
            .scratch = scratch,
            .stmt_triple = stmt_triple,
            .stmt_entity_type = stmt_entity_type,
            .stmt_edge_type = stmt_edge_type,
            .stmt_ont_entity = stmt_ont_entity,
            .stmt_ont_relation = stmt_ont_relation,
            .stmt_entity_raw = stmt_entity_raw,
            .stmt_relation_raw = stmt_relation_raw,
            .seen_entity_types = seen_entity_types,
            .seen_edge_types = .{},
        };
    }

    fn deinit(s: *ImportSession) void {
        facet_sqlite.finalize(s.stmt_triple);
        facet_sqlite.finalize(s.stmt_entity_type);
        facet_sqlite.finalize(s.stmt_edge_type);
        facet_sqlite.finalize(s.stmt_ont_entity);
        facet_sqlite.finalize(s.stmt_ont_relation);
        if (s.stmt_entity_raw) |stmt| facet_sqlite.finalize(stmt);
        if (s.stmt_relation_raw) |stmt| facet_sqlite.finalize(stmt);
    }

    fn upsertTriple(
        s: *ImportSession,
        triple_index: usize,
        subject_kind: []const u8,
        subject: []const u8,
        predicate: []const u8,
        object_kind: []const u8,
        object_value: []const u8,
        object_datatype: ?[]const u8,
        object_language: ?[]const u8,
        source_line: []const u8,
        metadata_json: []const u8,
    ) !void {
        try facet_sqlite.resetStatement(s.stmt_triple);
        try facet_sqlite.bindText(s.stmt_triple, 1, s.ontology_id);
        try facet_sqlite.bindInt64(s.stmt_triple, 2, @as(i64, @intCast(triple_index)));
        try facet_sqlite.bindText(s.stmt_triple, 3, subject_kind);
        try facet_sqlite.bindText(s.stmt_triple, 4, subject);
        try facet_sqlite.bindText(s.stmt_triple, 5, predicate);
        try facet_sqlite.bindText(s.stmt_triple, 6, object_kind);
        try facet_sqlite.bindText(s.stmt_triple, 7, object_value);
        if (object_datatype) |dt| try facet_sqlite.bindText(s.stmt_triple, 8, dt) else try facet_sqlite.bindNull(s.stmt_triple, 8);
        if (object_language) |lang| try facet_sqlite.bindText(s.stmt_triple, 9, lang) else try facet_sqlite.bindNull(s.stmt_triple, 9);
        try facet_sqlite.bindText(s.stmt_triple, 10, source_line);
        try facet_sqlite.bindText(s.stmt_triple, 11, metadata_json);
        try facet_sqlite.stepDone(s.stmt_triple);
    }

    fn ensureEntityType(s: *ImportSession, entity_type: []const u8, label: ?[]const u8, metadata_json: []const u8) !void {
        const gop = try s.seen_entity_types.getOrPut(s.scratch, entity_type);
        if (gop.found_existing) return;
        try facet_sqlite.resetStatement(s.stmt_entity_type);
        try facet_sqlite.bindText(s.stmt_entity_type, 1, s.ontology_id);
        try facet_sqlite.bindText(s.stmt_entity_type, 2, entity_type);
        if (label) |l| try facet_sqlite.bindText(s.stmt_entity_type, 3, l) else try facet_sqlite.bindNull(s.stmt_entity_type, 3);
        try facet_sqlite.bindText(s.stmt_entity_type, 4, metadata_json);
        try facet_sqlite.stepDone(s.stmt_entity_type);
    }

    fn ensureEdgeType(s: *ImportSession, edge_type: []const u8, metadata_json: []const u8) !void {
        const gop = try s.seen_edge_types.getOrPut(s.scratch, edge_type);
        if (gop.found_existing) return;
        try facet_sqlite.resetStatement(s.stmt_edge_type);
        try facet_sqlite.bindText(s.stmt_edge_type, 1, s.ontology_id);
        try facet_sqlite.bindText(s.stmt_edge_type, 2, edge_type);
        try facet_sqlite.bindInt64(s.stmt_edge_type, 3, 1);
        try facet_sqlite.bindNull(s.stmt_edge_type, 4);
        try facet_sqlite.bindNull(s.stmt_edge_type, 5);
        try facet_sqlite.bindText(s.stmt_edge_type, 6, metadata_json);
        try facet_sqlite.stepDone(s.stmt_edge_type);
    }

    fn upsertOntologyEntity(s: *ImportSession, entity_id: u64, entity_type: []const u8, name: []const u8, metadata_json: []const u8) !void {
        try facet_sqlite.resetStatement(s.stmt_ont_entity);
        try facet_sqlite.bindText(s.stmt_ont_entity, 1, s.ontology_id);
        try facet_sqlite.bindInt64(s.stmt_ont_entity, 2, entity_id);
        try facet_sqlite.bindText(s.stmt_ont_entity, 3, entity_type);
        try facet_sqlite.bindText(s.stmt_ont_entity, 4, name);
        try facet_sqlite.bindText(s.stmt_ont_entity, 5, metadata_json);
        try facet_sqlite.stepDone(s.stmt_ont_entity);
    }

    fn upsertOntologyRelation(s: *ImportSession, relation_id: u64, edge_type: []const u8, source_entity_id: u64, target_entity_id: u64, metadata_json: []const u8) !void {
        try facet_sqlite.resetStatement(s.stmt_ont_relation);
        try facet_sqlite.bindText(s.stmt_ont_relation, 1, s.ontology_id);
        try facet_sqlite.bindInt64(s.stmt_ont_relation, 2, relation_id);
        try facet_sqlite.bindText(s.stmt_ont_relation, 3, edge_type);
        try facet_sqlite.bindInt64(s.stmt_ont_relation, 4, source_entity_id);
        try facet_sqlite.bindInt64(s.stmt_ont_relation, 5, target_entity_id);
        try facet_sqlite.bindText(s.stmt_ont_relation, 6, metadata_json);
        try facet_sqlite.stepDone(s.stmt_ont_relation);
    }

    fn upsertEntityRaw(s: *ImportSession, entity_id: u64, entity_type: []const u8, name: []const u8, metadata_json: []const u8) !void {
        const stmt = s.stmt_entity_raw.?;
        try facet_sqlite.resetStatement(stmt);
        try facet_sqlite.bindText(stmt, 1, s.workspace_id);
        try facet_sqlite.bindText(stmt, 2, s.ontology_id);
        try facet_sqlite.bindInt64(stmt, 3, entity_id);
        try facet_sqlite.bindText(stmt, 4, entity_type);
        try facet_sqlite.bindText(stmt, 5, name);
        if (c.sqlite3_bind_double(stmt, 6, 1.0) != c.SQLITE_OK) return error.BindFailed;
        try facet_sqlite.bindText(stmt, 7, metadata_json);
        try facet_sqlite.stepDone(stmt);
    }

    fn upsertRelationRaw(s: *ImportSession, relation_id: u64, edge_type: []const u8, source_entity_id: u64, target_entity_id: u64, metadata_json: []const u8) !void {
        const stmt = s.stmt_relation_raw.?;
        try facet_sqlite.resetStatement(stmt);
        try facet_sqlite.bindText(stmt, 1, s.workspace_id);
        try facet_sqlite.bindText(stmt, 2, s.ontology_id);
        try facet_sqlite.bindInt64(stmt, 3, relation_id);
        try facet_sqlite.bindText(stmt, 4, edge_type);
        try facet_sqlite.bindInt64(stmt, 5, source_entity_id);
        try facet_sqlite.bindInt64(stmt, 6, target_entity_id);
        try facet_sqlite.bindNull(stmt, 7);
        try facet_sqlite.bindNull(stmt, 8);
        if (c.sqlite3_bind_double(stmt, 9, 1.0) != c.SQLITE_OK) return error.BindFailed;
        try facet_sqlite.bindText(stmt, 10, metadata_json);
        try facet_sqlite.stepDone(stmt);
    }
};

// importNTriplesReader is the streaming implementation.  It reads N-Triples
// line-by-line from reader without buffering the entire file.  The reader's
// buffer must be large enough to hold the longest individual line.
pub fn importNTriplesReader(
    db: Database,
    allocator: Allocator,
    workspace_id: []const u8,
    ontology_id: []const u8,
    reader: *std.Io.Reader,
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

    var session = try ImportSession.init(db, scratch, workspace_id, ontology_id, options.materialize_graph);
    defer session.deinit();

    var summary = ImportSummary{ .ontology_id = ontology_id };
    var line_no: usize = 0;

    // takeDelimiter tosses the delimiter itself (unlike takeDelimiterExclusive)
    // so seek advances past each '\n', preventing an infinite loop on empty lines.
    while (try reader.takeDelimiter('\n')) |raw_line| {
        line_no += 1;
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0 or line[0] == '#') continue;

        const triple = try parseNTripleLine(scratch, line);
        summary.triples += 1;
        try session.upsertTriple(
            summary.triples,
            triple.subject.kind.label(),
            triple.subject.value,
            triple.predicate,
            triple.object.kind.label(),
            triple.object.value,
            triple.object.datatype,
            triple.object.language,
            triple.source_line,
            try lineMetadata(scratch, line_no),
        );

        try projectTriple(&session, scratch, triple, &summary);
    }

    try db.exec("COMMIT");
    return summary;
}

pub fn importNTriples(
    db: Database,
    allocator: Allocator,
    workspace_id: []const u8,
    ontology_id: []const u8,
    content: []const u8,
    options: ImportOptions,
) !ImportSummary {
    var reader = std.Io.Reader.fixed(content);
    return importNTriplesReader(db, allocator, workspace_id, ontology_id, &reader, options);
}

// exportNTriplesWriter writes N-Triples rows directly to writer, one per line,
// without buffering the full result set in memory.
pub fn exportNTriplesWriter(db: Database, ontology_id: []const u8, writer: *std.Io.Writer) !void {
    const sql =
        \\SELECT source_line
        \\FROM ontology_triples_raw
        \\WHERE ontology_id = ?1
        \\ORDER BY triple_index
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, ontology_id);

    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try writer.writeAll(columnText(stmt, 0));
        try writer.writeByte('\n');
    }
}

pub fn exportNTriples(db: Database, allocator: Allocator, ontology_id: []const u8) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    try exportNTriplesWriter(db, ontology_id, &out.writer);
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
    session: *ImportSession,
    allocator: Allocator,
    triple: Triple,
    summary: *ImportSummary,
) !void {
    if (triple.subject.kind == .iri and std.mem.eql(u8, triple.predicate, rdf_type) and triple.object.kind == .iri) {
        if (std.mem.eql(u8, triple.object.value, owl_class) or std.mem.eql(u8, triple.object.value, rdfs_class)) {
            try upsertOntologyNode(session, allocator, triple.subject.value, "owl_class");
            summary.classes += 1;
            return;
        }
        if (std.mem.eql(u8, triple.object.value, owl_object_property)) {
            try ensureEdgeTypeForIri(session, allocator, triple.subject.value);
            try upsertOntologyNode(session, allocator, triple.subject.value, "owl_object_property");
            summary.object_properties += 1;
            return;
        }
        if (std.mem.eql(u8, triple.object.value, owl_datatype_property)) {
            try ensureDatatypeProperty(session, allocator, triple.subject.value);
            try upsertOntologyNode(session, allocator, triple.subject.value, "owl_datatype_property");
            summary.datatype_properties += 1;
            return;
        }
        if (!std.mem.eql(u8, triple.object.value, owl_ontology)) {
            const entity_type = try localNameAlloc(allocator, triple.object.value);
            try session.ensureEntityType(entity_type, entity_type, try iriMetadata(allocator, triple.object.value));
            try upsertOntologyNode(session, allocator, triple.subject.value, entity_type);
            if (session.materialize_graph) try upsertGraphNode(session, allocator, triple.subject.value, entity_type);
        }
        return;
    }

    if (triple.object.kind == .literal) return;

    const edge_type = if (knownPredicateName(triple.predicate)) |name| name else try localNameAlloc(allocator, triple.predicate);
    try session.ensureEdgeType(edge_type, try iriMetadata(allocator, triple.predicate));
    try upsertOntologyNode(session, allocator, triple.subject.value, termNodeType(triple.subject));
    try upsertOntologyNode(session, allocator, triple.object.value, termNodeType(triple.object));
    try session.upsertOntologyRelation(
        stableId(&.{ "ontology-relation", triple.subject.value, triple.predicate, triple.object.value }),
        edge_type,
        stableId(&.{ "ontology-entity", triple.subject.value }),
        stableId(&.{ "ontology-entity", triple.object.value }),
        try iriMetadata(allocator, triple.predicate),
    );
    summary.ontology_relations += 1;

    if (session.materialize_graph and triple.subject.kind == .iri and triple.object.kind == .iri) {
        try upsertGraphNode(session, allocator, triple.subject.value, "owl_resource");
        try upsertGraphNode(session, allocator, triple.object.value, "owl_resource");
        try session.upsertRelationRaw(
            stableId(&.{ "graph-relation", triple.subject.value, triple.predicate, triple.object.value }),
            edge_type,
            stableId(&.{ "graph-entity", triple.subject.value }),
            stableId(&.{ "graph-entity", triple.object.value }),
            try iriMetadata(allocator, triple.predicate),
        );
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

fn upsertOntologyNode(session: *ImportSession, allocator: Allocator, iri: []const u8, entity_type: []const u8) !void {
    try session.ensureEntityType(entity_type, entity_type, "{}");
    try session.upsertOntologyEntity(
        stableId(&.{ "ontology-entity", iri }),
        entity_type,
        try localNameAlloc(allocator, iri),
        try iriMetadata(allocator, iri),
    );
}

fn upsertGraphNode(session: *ImportSession, allocator: Allocator, iri: []const u8, entity_type: []const u8) !void {
    try session.upsertEntityRaw(
        stableId(&.{ "graph-entity", iri }),
        entity_type,
        try localNameAlloc(allocator, iri),
        try iriMetadata(allocator, iri),
    );
}

fn ensureEdgeTypeForIri(session: *ImportSession, allocator: Allocator, iri: []const u8) !void {
    const edge_type = try localNameAlloc(allocator, iri);
    try session.ensureEdgeType(edge_type, try iriMetadata(allocator, iri));
}

fn ensureDatatypeProperty(session: *ImportSession, allocator: Allocator, iri: []const u8) !void {
    try collections_sqlite.ensureNamespace(session.db, .{ .ontology_id = session.ontology_id, .namespace = "owl_datatype", .label = "OWL Datatype Properties" });
    try collections_sqlite.ensureDimension(session.db, .{
        .ontology_id = session.ontology_id,
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

test "duplicate-heavy import produces stable projected table counts" {
    // Exercises the seen_entity_types / seen_edge_types dedup in ImportSession.
    // The same rdf:type and structural declarations appear multiple times.
    // The projected schema tables (entity_types, edge_types, entities) must have
    // the same row counts whether each triple is imported once or many times.
    const allocator = std.testing.allocator;

    const fixture = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "docs/source/owl2-core.nt", allocator, .limited(1024 * 1024));
    defer allocator.free(fixture);

    // Build a fixture that repeats every data line 4 times.
    var repeated = std.Io.Writer.Allocating.init(allocator);
    errdefer repeated.deinit();
    {
        var it = std.mem.splitScalar(u8, fixture, '\n');
        while (it.next()) |raw_line| {
            const line = std.mem.trim(u8, raw_line, " \t\r\n");
            if (line.len == 0 or line[0] == '#') continue;
            for (0..4) |_| {
                try repeated.writer.writeAll(line);
                try repeated.writer.writeByte('\n');
            }
        }
    }
    const repeated_content = try repeated.toOwnedSlice();
    defer allocator.free(repeated_content);

    // Import the original fixture.
    var db_single = try Database.open(":memory:");
    defer db_single.close();
    try db_single.applyStandaloneSchema();
    _ = try importNTriples(db_single, allocator, "ws", "onto", fixture, .{ .materialize_graph = true });

    // Import the 4x-repeated fixture into a fresh DB.
    var db_repeat = try Database.open(":memory:");
    defer db_repeat.close();
    try db_repeat.applyStandaloneSchema();
    _ = try importNTriples(db_repeat, allocator, "ws", "onto", repeated_content, .{ .materialize_graph = true });

    // Projected schema tables must have identical row counts in both DBs.
    const et_single = try countRowsBound(db_single, "SELECT COUNT(*) FROM ontology_entity_types WHERE ontology_id = ?1", "onto");
    const et_repeat = try countRowsBound(db_repeat, "SELECT COUNT(*) FROM ontology_entity_types WHERE ontology_id = ?1", "onto");
    try std.testing.expectEqual(et_single, et_repeat);

    const edge_single = try countRowsBound(db_single, "SELECT COUNT(*) FROM ontology_edge_types WHERE ontology_id = ?1", "onto");
    const edge_repeat = try countRowsBound(db_repeat, "SELECT COUNT(*) FROM ontology_edge_types WHERE ontology_id = ?1", "onto");
    try std.testing.expectEqual(edge_single, edge_repeat);

    const ent_single = try countRowsBound(db_single, "SELECT COUNT(*) FROM ontology_entities_raw WHERE ontology_id = ?1", "onto");
    const ent_repeat = try countRowsBound(db_repeat, "SELECT COUNT(*) FROM ontology_entities_raw WHERE ontology_id = ?1", "onto");
    try std.testing.expectEqual(ent_single, ent_repeat);

    try std.testing.expect(et_single > 0);
    try std.testing.expect(edge_single > 0);
    try std.testing.expect(ent_single > 0);
}

test "importNTriplesReader produces identical results to importNTriples" {
    // Smoke-tests the streaming reader path against the slice-based wrapper for
    // a fixture large enough to exercise multi-line buffering.
    const allocator = std.testing.allocator;

    const fixture_paths = [_]struct {
        path: []const u8,
        ontology_id: []const u8,
    }{
        .{ .path = "docs/source/owl2-core.nt", .ontology_id = "onto-reader-core" },
        .{ .path = "docs/source/owl2-rl-relations.nt", .ontology_id = "onto-reader-rl" },
        .{ .path = "docs/source/owl2-restrictions.nt", .ontology_id = "onto-reader-restr" },
    };

    for (fixture_paths) |spec| {
        const content = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, spec.path, allocator, .limited(1024 * 1024));
        defer allocator.free(content);

        // Slice-based import (reference).
        var db_ref = try Database.open(":memory:");
        defer db_ref.close();
        try db_ref.applyStandaloneSchema();
        const ref_summary = try importNTriples(db_ref, allocator, "ws-reader", spec.ontology_id, content, .{
            .materialize_graph = true,
        });
        const ref_export = try exportNTriples(db_ref, allocator, spec.ontology_id);
        defer allocator.free(ref_export);

        // Reader-based import.
        var db_rdr = try Database.open(":memory:");
        defer db_rdr.close();
        try db_rdr.applyStandaloneSchema();
        var reader = std.Io.Reader.fixed(content);
        const rdr_summary = try importNTriplesReader(db_rdr, allocator, "ws-reader", spec.ontology_id, &reader, .{
            .materialize_graph = true,
        });
        const rdr_export = try exportNTriples(db_rdr, allocator, spec.ontology_id);
        defer allocator.free(rdr_export);

        try std.testing.expectEqual(ref_summary.triples, rdr_summary.triples);
        try std.testing.expectEqual(ref_summary.classes, rdr_summary.classes);
        try std.testing.expectEqual(ref_summary.object_properties, rdr_summary.object_properties);
        try std.testing.expectEqual(ref_summary.datatype_properties, rdr_summary.datatype_properties);
        try std.testing.expectEqual(ref_summary.ontology_relations, rdr_summary.ontology_relations);
        try std.testing.expectEqualStrings(ref_export, rdr_export);
    }
}
