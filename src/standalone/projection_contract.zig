//! Bounded, ontology-scoped native graph projections. Reads never materialize.
const std = @import("std");
const sqlite = @import("facet_sqlite.zig");
const c = sqlite.c;
const Database = sqlite.Database;
const Value = std.json.Value;

pub const Operation = enum { missing_inbound, group_sum, dated_relations };
pub const Column = struct { key: []const u8, description: []const u8, concept: []const u8, unit: ?[]const u8 = null };
pub const Contract = struct {
    version: u32,
    ontology_id: []const u8,
    ontology_version: []const u8,
    business_question: []const u8,
    paraphrases: []const []const u8,
    operation: Operation,
    subject_type: []const u8,
    relation: []const u8,
    ontology_relation: []const u8,
    member_type: []const u8,
    member_types: []const []const u8 = &.{},
    required_fields: []const []const u8 = &.{},
    numeric_field: []const u8 = "",
    expected_field: []const u8 = "",
    member_basis_field: []const u8 = "",
    expected_value: ?f64 = null,
    start_field: []const u8 = "",
    end_field: []const u8 = "",
    as_of: []const u8,
    columns: []const Column,
    assumptions: []const []const u8,
};

fn stringify(a: std.mem.Allocator, value: anytype) ![]u8 {
    return std.json.Stringify.valueAlloc(a, value, .{});
}
fn hash(value: []const u8) [64]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(value, &digest, .{});
    return std.fmt.bytesToHex(digest, .lower);
}
fn fieldName(value: []const u8) bool {
    if (value.len == 0 or value.len > 80) return false;
    for (value) |ch| if (!std.ascii.isAlphanumeric(ch) and ch != '_') return false;
    return true;
}
fn parseContract(a: std.mem.Allocator, value: Value) !std.json.Parsed(Contract) {
    const json = try stringify(a, value);
    defer a.free(json);
    const parsed = std.json.parseFromSlice(Contract, a, json, .{ .allocate = .alloc_always }) catch return error.InvalidProjectionContract;
    errdefer parsed.deinit();
    const p = parsed.value;
    if (p.version != 1 or p.ontology_id.len == 0 or p.ontology_version.len == 0 or p.business_question.len == 0 or p.paraphrases.len > 40 or p.columns.len == 0 or p.columns.len > 20 or p.assumptions.len == 0 or p.assumptions.len > 20) return error.InvalidProjectionContract;
    if (!fieldName(p.subject_type) or !fieldName(p.member_type) or !fieldName(p.relation)) return error.InvalidProjectionContract;
    if (p.operation == .group_sum and (!fieldName(p.numeric_field) or !fieldName(p.expected_field))) return error.InvalidProjectionContract;
    if (p.member_basis_field.len > 0 and !fieldName(p.member_basis_field)) return error.InvalidProjectionContract;
    if (p.operation == .dated_relations and (!fieldName(p.start_field) or !fieldName(p.end_field))) return error.InvalidProjectionContract;
    if (p.member_types.len > 20 or p.required_fields.len > 20) return error.InvalidProjectionContract;
    if (p.operation == .missing_inbound and p.member_types.len == 0) return error.InvalidProjectionContract;
    for (p.member_types) |t| if (!fieldName(t)) return error.InvalidProjectionContract;
    for (p.required_fields) |f| if (!fieldName(f)) return error.InvalidProjectionContract;
    if (p.as_of.len != 10 or p.as_of[4] != '-' or p.as_of[7] != '-') return error.InvalidProjectionContract;
    for (p.columns) |column| if (!fieldName(column.key) or column.description.len == 0 or column.concept.len == 0) return error.InvalidProjectionContract;
    return parsed;
}

// JSON row sources are explicitly scoped before any join or aggregation.
const prefix =
    \\WITH e AS (SELECT * FROM entities_raw WHERE workspace_id=?1 AND ontology_id=?2),
    \\r AS (SELECT * FROM relations_raw WHERE workspace_id=?1 AND ontology_id=?2
    \\ AND (valid_from IS NULL OR julianday(valid_from)<=julianday(?8))
    \\ AND (valid_to IS NULL OR julianday(valid_to)>=julianday(?8)))
;
const missing_sql = prefix ++
    \\SELECT json_object('entity_id',s.entity_id,'name',s.name,'attributes',json(s.metadata_json),
    \\ 'finding','no_declared_inbound_relation','coverage','declared_records_only')
    \\FROM e s WHERE s.entity_type=?3 AND NOT EXISTS (
    \\ SELECT 1 FROM r JOIN e owner ON owner.entity_id=r.source_entity_id
    \\ WHERE r.target_entity_id=s.entity_id AND r.edge_type=?4 AND owner.entity_type IN (SELECT value FROM json_each(?9)))
    \\ORDER BY s.entity_id LIMIT 201
;
const sum_sql = prefix ++
    \\, members AS (SELECT DISTINCT r.source_entity_id, m.entity_id,m.metadata_json FROM r
    \\ JOIN e m ON m.entity_id=r.target_entity_id AND m.entity_type=?7 WHERE r.edge_type=?4)
    \\SELECT json_object('entity_id',s.entity_id,'name',s.name,
    \\ 'member_count',count(m.entity_id),
    \\ 'total',CASE WHEN count(m.entity_id)=0 OR sum(CASE WHEN json_type(m.metadata_json,?5) IN ('integer','real') THEN 0 ELSE 1 END)>0 THEN NULL ELSE sum(json_extract(m.metadata_json,?5)) END,
    \\ 'expected',json_extract(s.metadata_json,?6),
    \\ 'matches',CASE WHEN count(m.entity_id)=0 OR (?10 IS NOT NULL AND json_extract(s.metadata_json,?6)!=?10) OR (?9!='$' AND sum(CASE WHEN json_type(m.metadata_json,?9) IN ('integer','real') AND json_extract(m.metadata_json,?9)=json_extract(s.metadata_json,?6) THEN 0 ELSE 1 END)>0) OR json_type(s.metadata_json,?6) NOT IN ('integer','real') OR json_type(s.metadata_json,?6) IS NULL
    \\ OR sum(CASE WHEN json_type(m.metadata_json,?5) IN ('integer','real') THEN 0 ELSE 1 END)>0 THEN NULL
    \\ ELSE abs(sum(json_extract(m.metadata_json,?5))-json_extract(s.metadata_json,?6))<0.000001 END,
    \\ 'evidence_entity_ids',json(COALESCE((SELECT json_group_array(entity_id) FROM (SELECT entity_id FROM members WHERE source_entity_id=s.entity_id ORDER BY entity_id)),'[]')))
    \\FROM e s LEFT JOIN members m ON m.source_entity_id=s.entity_id
    \\WHERE s.entity_type=?3 GROUP BY s.entity_id ORDER BY s.entity_id LIMIT 201
;
const dates_sql = prefix ++
    \\SELECT json_object('entity_id',s.entity_id,'name',s.name,'attributes',json(s.metadata_json),
    \\ 'start_date',json_extract(s.metadata_json,?5),'end_date',json_extract(s.metadata_json,?6),
    \\ 'fields_complete',NOT EXISTS(SELECT 1 FROM json_each(?9) f WHERE json_extract(s.metadata_json,'$.'||f.value) IS NULL),
    \\ 'date_status',CASE WHEN COALESCE(json_extract(s.metadata_json,'$.status'),'active')!='active' OR julianday(json_extract(s.metadata_json,?5)) IS NULL OR (json_extract(s.metadata_json,?6) IS NOT NULL AND julianday(json_extract(s.metadata_json,?6)) IS NULL) THEN 'indeterminate' ELSE 'active' END,
    \\ 'related_entities',json((SELECT json_group_array(json_object('relation_id',r.relation_id,'predicate',r.edge_type,'entity_id',t.entity_id,'entity_type',t.entity_type,'name',t.name,'attributes',json(t.metadata_json)))
    \\ FROM r JOIN e t ON t.entity_id=r.target_entity_id WHERE r.source_entity_id=s.entity_id)),
    \\ 'required_relation_present',EXISTS(SELECT 1 FROM r JOIN e t ON t.entity_id=r.target_entity_id AND t.entity_type=?7 WHERE r.source_entity_id=s.entity_id AND r.edge_type=?4))
    \\FROM e s WHERE s.entity_type=?3
    \\ AND (julianday(json_extract(s.metadata_json,?5)) IS NULL OR julianday(json_extract(s.metadata_json,?5))<=julianday(?8))
    \\ AND (julianday(json_extract(s.metadata_json,?6)) IS NULL OR julianday(json_extract(s.metadata_json,?6))>=julianday(?8))
    \\ AND COALESCE(json_extract(s.metadata_json,'$.status'),'active') NOT IN ('cancelled','terminated','inactive')
    \\ORDER BY s.entity_id LIMIT 201
;

fn jsonRows(db: Database, a: std.mem.Allocator, sql: []const u8, ws: []const u8, p: Contract) !Value {
    const stmt = try sqlite.prepare(db, sql);
    defer sqlite.finalize(stmt);
    try sqlite.bindText(stmt, 1, ws);
    try sqlite.bindText(stmt, 2, p.ontology_id);
    try sqlite.bindText(stmt, 3, p.subject_type);
    try sqlite.bindText(stmt, 4, p.relation);
    const first = try std.fmt.allocPrint(a, "$.{s}", .{if (p.operation == .group_sum) p.numeric_field else p.start_field});
    defer a.free(first);
    const second = try std.fmt.allocPrint(a, "$.{s}", .{if (p.operation == .group_sum) p.expected_field else p.end_field});
    defer a.free(second);
    try sqlite.bindText(stmt, 5, first);
    try sqlite.bindText(stmt, 6, second);
    try sqlite.bindText(stmt, 7, p.member_type);
    try sqlite.bindText(stmt, 8, p.as_of);
    if (p.operation == .group_sum) {
        const basis = if (p.member_basis_field.len == 0) try a.dupe(u8, "$") else try std.fmt.allocPrint(a, "$.{s}", .{p.member_basis_field});
        defer a.free(basis);
        try sqlite.bindText(stmt, 9, basis);
        if (p.expected_value) |value| {
            if (c.sqlite3_bind_double(stmt, 10, value) != c.SQLITE_OK) return error.StepFailed;
        } else if (c.sqlite3_bind_null(stmt, 10) != c.SQLITE_OK) return error.StepFailed;
    } else {
        const fields = try stringify(a, if (p.operation == .missing_inbound) p.member_types else p.required_fields);
        defer a.free(fields);
        try sqlite.bindText(stmt, 9, fields);
    }
    var rows: std.array_list.Managed(Value) = .init(a);
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        if (rows.items.len == 200) return error.ProjectionResultTooLarge;
        const json = std.mem.span(c.sqlite3_column_text(stmt, 0));
        const parsed = try std.json.parseFromSlice(Value, a, json, .{ .allocate = .alloc_always });
        // Caller supplies an arena for the complete materialization.
        try rows.append(parsed.value);
    }
    return .{ .array = rows };
}

fn ontology(db: Database, a: std.mem.Allocator, ws: []const u8, p: Contract) !Value {
    const stmt = try sqlite.prepare(db,
        \\SELECT json_object('ontology_id',o.ontology_id,'version',o.version,'name',o.name,
        \\ 'entity_types',json((SELECT json_group_array(json_object('entity_type',entity_type,'description',label,'metadata',json(metadata_json))) FROM ontology_entity_types WHERE ontology_id=o.ontology_id)),
        \\ 'edge_types',json((SELECT json_group_array(json_object('edge_type',edge_type,'source',source_entity_type,'target',target_entity_type,'metadata',json(metadata_json))) FROM ontology_edge_types WHERE ontology_id=o.ontology_id)))
        \\FROM ontologies o WHERE o.workspace_id=?1 AND o.ontology_id=?2 AND o.version=?3
    );
    defer sqlite.finalize(stmt);
    try sqlite.bindText(stmt, 1, ws);
    try sqlite.bindText(stmt, 2, p.ontology_id);
    try sqlite.bindText(stmt, 3, p.ontology_version);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.ProjectionOntologyMismatch;
    const json = std.mem.span(c.sqlite3_column_text(stmt, 0));
    if (json.len > 131072) return error.ProjectionSourceTooLarge;
    const parsed = try std.json.parseFromSlice(Value, a, json, .{ .allocate = .alloc_always });
    const types = parsed.value.object.get("entity_types").?.array.items;
    for ([_][]const u8{ p.subject_type, p.member_type }) |required_type| {
        var found = false;
        for (types) |t| if (std.mem.eql(u8, t.object.get("entity_type").?.string, required_type)) {
            found = true;
            break;
        };
        if (!found) return error.ProjectionOntologyMismatch;
    }
    for (p.member_types) |required_type| {
        var found = false;
        for (types) |t| if (std.mem.eql(u8, t.object.get("entity_type").?.string, required_type)) {
            found = true;
            break;
        };
        if (!found) return error.ProjectionOntologyMismatch;
    }
    for (p.columns) |column| {
        var found = false;
        for (types) |t| if (std.mem.eql(u8, t.object.get("entity_type").?.string, column.concept)) {
            found = true;
            break;
        };
        if (!found) return error.ProjectionOntologyMismatch;
    }
    var relation_found = false;
    for (parsed.value.object.get("edge_types").?.array.items) |edge| {
        if (std.mem.eql(u8, edge.object.get("edge_type").?.string, p.ontology_relation)) relation_found = true;
    }
    if (!relation_found) return error.ProjectionOntologyMismatch;
    return parsed.value;
}

// Conservative invalidation: any change in this native ontology's source records
// or definitions invalidates the result. Bounded to the qualified pilot size.
fn sourceDigest(db: Database, a: std.mem.Allocator, ws: []const u8, p: Contract, onto: Value) ![64]u8 {
    var sha = std.crypto.hash.sha2.Sha256.init(.{});
    const defs = try stringify(a, onto);
    sha.update(defs);
    var bytes: usize = defs.len;
    for ([_][]const u8{
        "SELECT json_array(entity_id,external_id,entity_type,name,confidence,metadata_json) FROM entities_raw WHERE workspace_id=?1 AND ontology_id=?2 ORDER BY entity_id",
        "SELECT json_array(relation_id,external_id,edge_type,source_entity_id,target_entity_id,valid_from,valid_to,confidence,metadata_json) FROM relations_raw WHERE workspace_id=?1 AND ontology_id=?2 ORDER BY relation_id",
    }) |sql| {
        const stmt = try sqlite.prepare(db, sql);
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, ws);
        try sqlite.bindText(stmt, 2, p.ontology_id);
        var count: usize = 0;
        while (true) {
            const rc = c.sqlite3_step(stmt);
            if (rc == c.SQLITE_DONE) break;
            if (rc != c.SQLITE_ROW) return error.StepFailed;
            count += 1;
            if (count > 5000) return error.ProjectionSourceTooLarge;
            const row = std.mem.span(c.sqlite3_column_text(stmt, 0));
            bytes += row.len;
            if (row.len > 65536 or bytes > 4194304) return error.ProjectionSourceTooLarge;
            sha.update(row);
            sha.update("\n");
        }
    }
    return std.fmt.bytesToHex(sha.finalResult(), .lower);
}

pub fn validate(db: Database, allocator: std.mem.Allocator, ws: []const u8, value: Value) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const p = try parseContract(a, value);
    _ = try ontology(db, a, ws, p.value);
    const stmt = try sqlite.prepare(db, "SELECT date(?1) = ?1 AND julianday(?1) IS NOT NULL");
    defer sqlite.finalize(stmt);
    try sqlite.bindText(stmt, 1, p.value.as_of);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW or c.sqlite3_column_int(stmt, 0) != 1) return error.InvalidProjectionContract;
}

fn integrity(db: Database, ws: []const u8, p: Contract) !bool {
    const stmt = try sqlite.prepare(db,
        \\SELECT NOT EXISTS(SELECT 1 FROM relations_raw r WHERE r.workspace_id=?1 AND r.ontology_id=?2 AND r.edge_type=?3 AND (
        \\ (valid_from IS NOT NULL AND julianday(valid_from) IS NULL) OR (valid_to IS NOT NULL AND julianday(valid_to) IS NULL)
        \\ OR julianday(valid_to)<julianday(valid_from)
        \\ OR NOT EXISTS(SELECT 1 FROM entities_raw s WHERE s.workspace_id=?1 AND s.ontology_id=?2 AND s.entity_id=r.source_entity_id)
        \\ OR NOT EXISTS(SELECT 1 FROM entities_raw t WHERE t.workspace_id=?1 AND t.ontology_id=?2 AND t.entity_id=r.target_entity_id)))
    );
    defer sqlite.finalize(stmt);
    try sqlite.bindText(stmt, 1, ws);
    try sqlite.bindText(stmt, 2, p.ontology_id);
    try sqlite.bindText(stmt, 3, p.relation);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW or c.sqlite3_column_int(stmt, 0) != 1) return false;
    if (p.operation != .dated_relations) return true;
    const dates = try sqlite.prepare(db,
        \\SELECT NOT EXISTS(SELECT 1 FROM entities_raw WHERE workspace_id=?1 AND ontology_id=?2 AND entity_type=?3 AND (
        \\ julianday(json_extract(metadata_json,'$.'||?4)) IS NULL
        \\ OR (json_extract(metadata_json,'$.'||?5) IS NOT NULL AND julianday(json_extract(metadata_json,'$.'||?5)) IS NULL)
        \\ OR julianday(json_extract(metadata_json,'$.'||?5))<julianday(json_extract(metadata_json,'$.'||?4))
        \\ OR COALESCE(json_extract(metadata_json,'$.status'),'active') NOT IN ('active','cancelled','terminated','inactive')))
    );
    defer sqlite.finalize(dates);
    try sqlite.bindText(dates, 1, ws);
    try sqlite.bindText(dates, 2, p.ontology_id);
    try sqlite.bindText(dates, 3, p.subject_type);
    try sqlite.bindText(dates, 4, p.start_field);
    try sqlite.bindText(dates, 5, p.end_field);
    return c.sqlite3_step(dates) == c.SQLITE_ROW and c.sqlite3_column_int(dates, 0) == 1;
}

pub fn materialize(db: Database, allocator: std.mem.Allocator, ws: []const u8, payload_json: []const u8) !?[]u8 {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const payload = try std.json.parseFromSlice(Value, a, payload_json, .{ .allocate = .alloc_always });
    if (payload.value != .object) return error.InvalidArtifactPayload;
    const contract = payload.value.object.get("projection_contract") orelse return null;
    try validate(db, a, ws, contract);
    const p = (try parseContract(a, contract)).value;
    const onto = try ontology(db, a, ws, p);
    const source = try sourceDigest(db, a, ws, p, onto);
    const rows = try jsonRows(db, a, switch (p.operation) {
        .missing_inbound => missing_sql,
        .group_sum => sum_sql,
        .dated_relations => dates_sql,
    }, ws, p);
    var complete = try integrity(db, ws, p);
    for (rows.array.items) |row| {
        if (p.operation == .group_sum and row.object.get("matches").? == .null) complete = false;
        if (p.operation == .dated_relations and (!std.mem.eql(u8, row.object.get("date_status").?.string, "active") or row.object.get("required_relation_present").?.integer == 0 or row.object.get("fields_complete").?.integer == 0)) complete = false;
    }
    var explanation = onto;
    var concepts: std.array_list.Managed(Value) = .init(a);
    for (onto.object.get("entity_types").?.array.items) |t| {
        const name = t.object.get("entity_type").?.string;
        var used = std.mem.eql(u8, name, p.subject_type) or std.mem.eql(u8, name, p.member_type);
        for (p.member_types) |member| if (std.mem.eql(u8, name, member)) {
            used = true;
        };
        for (p.columns) |col| if (std.mem.eql(u8, name, col.concept)) {
            used = true;
        };
        for (rows.array.items) |row| {
            if (row.object.get("related_entities")) |related| {
                for (related.array.items) |entity| {
                    if (std.mem.eql(u8, name, entity.object.get("entity_type").?.string)) used = true;
                }
            }
        }
        if (used) try concepts.append(t);
    }
    var edges: std.array_list.Managed(Value) = .init(a);
    for (onto.object.get("edge_types").?.array.items) |e| {
        var used = std.mem.eql(u8, e.object.get("edge_type").?.string, p.ontology_relation);
        for (rows.array.items) |row| {
            if (row.object.get("related_entities")) |related| {
                for (related.array.items) |edge| if (std.mem.eql(u8, e.object.get("edge_type").?.string, edge.object.get("predicate").?.string)) {
                    used = true;
                };
            }
        }
        if (used) try edges.append(e);
    }
    try explanation.object.put(a, "entity_types", .{ .array = concepts });
    try explanation.object.put(a, "edge_types", .{ .array = edges });
    const contract_json = try stringify(a, contract);
    const contract_hash = hash(contract_json);
    const clock = try sqlite.prepare(db, "SELECT unixepoch()");
    defer sqlite.finalize(clock);
    if (c.sqlite3_step(clock) != c.SQLITE_ROW) return error.StepFailed;
    const result_json = try stringify(a, .{
        .status = if (complete) "ready" else "indeterminate",
        .complete = complete,
        .coverage = "declared_records_only",
        .workspace_id = ws,
        .as_of = p.as_of,
        .computed_at_unix = c.sqlite3_column_int64(clock, 0),
        .relation_binding = .{ .native_predicate = p.relation, .ontology_predicate = p.ontology_relation },
        .source_digest = &source,
        .contract_digest = &contract_hash,
        .row_count = rows.array.items.len,
        .rows = rows,
        .ontology = explanation,
        .columns = p.columns,
        .assumptions = p.assumptions,
    });
    if (result_json.len > 262144) return error.ProjectionResultTooLarge;
    const result = try std.json.parseFromSlice(Value, a, result_json, .{});
    var obj = payload.value;
    try obj.object.put(a, "qualified_result", result.value);
    return try stringify(allocator, obj);
}

pub fn readAnswer(db: Database, allocator: std.mem.Allocator, ws: []const u8, payload_json: []const u8) !?[]u8 {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const payload = try std.json.parseFromSlice(Value, a, payload_json, .{});
    if (payload.value != .object) return null;
    const contract = payload.value.object.get("projection_contract") orelse return null;
    const stored = payload.value.object.get("qualified_result") orelse return try allocator.dupe(u8, "{\"status\":\"unavailable\",\"reason\":\"refresh_required\"}");
    if (stored != .object) return error.InvalidProjectionContract;
    const p = (try parseContract(a, contract)).value;
    const onto = ontology(db, a, ws, p) catch return try allocator.dupe(u8, "{\"status\":\"stale\",\"reason\":\"ontology_changed\"}");
    const source = try sourceDigest(db, a, ws, p, onto);
    const contract_hash = hash(try stringify(a, contract));
    const saved_source = stored.object.get("source_digest") orelse return error.InvalidProjectionContract;
    const saved_contract = stored.object.get("contract_digest") orelse return error.InvalidProjectionContract;
    if (saved_source != .string or saved_contract != .string or !std.mem.eql(u8, saved_source.string, &source) or !std.mem.eql(u8, saved_contract.string, &contract_hash)) return try allocator.dupe(u8, "{\"status\":\"stale\",\"reason\":\"source_or_contract_changed\"}");
    return try stringify(allocator, stored);
}

const test_contract =
    \\{"version":1,"ontology_id":"w::core","ontology_version":"1","business_question":"Do totals match?","paraphrases":["Check totals"],"operation":"group_sum","subject_type":"building","relation":"contains","ontology_relation":"building_contains_unit","member_type":"unit","numeric_field":"quota","expected_field":"basis","as_of":"2026-09-13","columns":[{"key":"total","description":"Sum of declared lot quotas","concept":"unit","unit":"quota"}],"assumptions":["Declared records only; no completeness claim beyond this import."]}
;
fn testDb() !Database {
    var db = try Database.openInMemory();
    errdefer db.close();
    try db.applyStandaloneSchema();
    try db.exec(
        \\INSERT INTO workspaces(workspace_id) VALUES('w'),('foreign');
        \\INSERT INTO ontologies(ontology_id,workspace_id,name,version,source_kind) VALUES('w::core','w','Core','1','linkml');
        \\INSERT INTO ontology_entity_types(ontology_id,entity_type,label) VALUES('w::core','building','Building'),('w::core','unit','Lot'),('w::core','lease','Lease');
        \\INSERT INTO ontology_edge_types(ontology_id,edge_type,source_entity_type,target_entity_type) VALUES('w::core','building_contains_unit','building','unit'),('w::core','leases','lease','unit');
        \\INSERT INTO entities_raw(workspace_id,ontology_id,entity_id,external_id,entity_type,name,metadata_json) VALUES
        \\ ('w','w::core',1,'b','building','Building','{"basis":1000}'),
        \\ ('w','w::core',2,'u1','unit','Lot one','{"quota":400}'),
        \\ ('w','w::core',3,'u2','unit','Lot two','{"quota":600}');
        \\INSERT INTO relations_raw(workspace_id,ontology_id,relation_id,edge_type,source_entity_id,target_entity_id) VALUES
        \\ ('w','w::core',1,'contains',1,2),('w','w::core',2,'contains',1,3);
    );
    return db;
}

test "qualified sums return independent expected amounts, ontology and invalidate on source changes" {
    var db = try testDb();
    defer db.close();
    const a = std.testing.allocator;
    const input = try std.fmt.allocPrint(a, "{{\"projection_contract\":{s}}}", .{test_contract});
    defer a.free(input);
    const output = (try materialize(db, a, "w", input)).?;
    defer a.free(output);
    const answer = (try readAnswer(db, a, "w", output)).?;
    defer a.free(answer);
    const parsed = try std.json.parseFromSlice(Value, a, answer, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("ready", parsed.value.object.get("status").?.string);
    const row = parsed.value.object.get("rows").?.array.items[0];
    try std.testing.expectEqual(@as(i64, 1000), row.object.get("total").?.integer);
    try std.testing.expectEqual(@as(i64, 1), row.object.get("matches").?.integer);
    try std.testing.expectEqual(@as(usize, 2), row.object.get("evidence_entity_ids").?.array.items.len);
    try db.exec("UPDATE entities_raw SET metadata_json='{\"quota\":500}' WHERE entity_id=2");
    const stale = (try readAnswer(db, a, "w", output)).?;
    defer a.free(stale);
    try std.testing.expect(std.mem.indexOf(u8, stale, "\"status\":\"stale\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stale, "\"rows\"") == null);
    try std.testing.expectError(error.ProjectionOntologyMismatch, materialize(db, a, "foreign", input));
}

test "missing numeric evidence is indeterminate and ontology changes invalidate" {
    var db = try testDb();
    defer db.close();
    const a = std.testing.allocator;
    const input = try std.fmt.allocPrint(a, "{{\"projection_contract\":{s}}}", .{test_contract});
    defer a.free(input);
    try db.exec("UPDATE entities_raw SET metadata_json='{}' WHERE entity_id=2");
    const output = (try materialize(db, a, "w", input)).?;
    defer a.free(output);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"status\":\"indeterminate\"") != null);
    try db.exec("UPDATE ontologies SET version='2' WHERE ontology_id='w::core'");
    const stale = (try readAnswer(db, a, "w", output)).?;
    defer a.free(stale);
    try std.testing.expect(std.mem.indexOf(u8, stale, "ontology_changed") != null);
}

fn testInput(a: std.mem.Allocator, operation: Operation) ![]u8 {
    const parsed = try std.json.parseFromSlice(Contract, a, test_contract, .{});
    defer parsed.deinit();
    var p = parsed.value;
    p.operation = operation;
    if (operation == .missing_inbound) {
        p.subject_type = "unit";
        p.member_type = "building";
        p.member_types = &.{"building"};
    }
    if (operation == .dated_relations) {
        p.subject_type = "lease";
        p.relation = "leases";
        p.ontology_relation = "leases";
        p.start_field = "start";
        p.end_field = "end";
        p.required_fields = &.{"landlord"};
    }
    return try stringify(a, .{ .projection_contract = p });
}

test "missing inbound evidence stays scoped and responds to relation changes" {
    var db = try testDb();
    defer db.close();
    const a = std.testing.allocator;
    const input = try testInput(a, .missing_inbound);
    defer a.free(input);
    const empty = (try materialize(db, a, "w", input)).?;
    defer a.free(empty);
    try std.testing.expect(std.mem.indexOf(u8, empty, "\"row_count\":0") != null);
    try db.exec("DELETE FROM relations_raw WHERE relation_id=2");
    const output = (try materialize(db, a, "w", input)).?;
    defer a.free(output);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"row_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "Lot two") != null);
    const before = c.sqlite3_total_changes(db.handle);
    const read = (try readAnswer(db, a, "w", output)).?;
    defer a.free(read);
    try std.testing.expectEqual(before, c.sqlite3_total_changes(db.handle));
    try std.testing.expect(std.mem.indexOf(u8, read, "declared_records_only") != null);
}

test "dated relations use the explicit date and retain missing fields as indeterminate" {
    var db = try testDb();
    defer db.close();
    try db.exec(
        \\INSERT INTO entities_raw(workspace_id,ontology_id,entity_id,external_id,entity_type,name,metadata_json) VALUES
        \\ ('w','w::core',4,'l1','lease','Current','{"start":"2026-01-01","landlord":42}'),
        \\ ('w','w::core',5,'l2','lease','Expired','{"start":"2025-01-01","end":"2026-08-01","landlord":42}');
        \\INSERT INTO relations_raw(workspace_id,ontology_id,relation_id,edge_type,source_entity_id,target_entity_id) VALUES
        \\ ('w','w::core',3,'leases',4,2),('w','w::core',4,'leases',5,3);
    );
    const a = std.testing.allocator;
    const input = try testInput(a, .dated_relations);
    defer a.free(input);
    const output = (try materialize(db, a, "w", input)).?;
    defer a.free(output);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"row_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"status\":\"ready\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "Expired") == null);
    try db.exec("UPDATE entities_raw SET metadata_json='{\"start\":\"2026-01-01\"}' WHERE entity_id=4");
    const partial = (try materialize(db, a, "w", input)).?;
    defer a.free(partial);
    try std.testing.expect(std.mem.indexOf(u8, partial, "\"status\":\"indeterminate\"") != null);
}

test "oversized results fail instead of silently truncating" {
    var db = try testDb();
    defer db.close();
    try db.exec(
        \\WITH RECURSIVE n(i) AS (VALUES(10) UNION ALL SELECT i+1 FROM n WHERE i<211)
        \\INSERT INTO entities_raw(workspace_id,ontology_id,entity_id,external_id,entity_type,name)
        \\SELECT 'w','w::core',i,'extra-'||i,'unit','Extra '||i FROM n;
    );
    const a = std.testing.allocator;
    const input = try testInput(a, .missing_inbound);
    defer a.free(input);
    try std.testing.expectError(error.ProjectionResultTooLarge, materialize(db, a, "w", input));
}

test "another ontology in the same workspace cannot influence or invalidate the answer" {
    var db = try testDb();
    defer db.close();
    const a = std.testing.allocator;
    const input = try testInput(a, .group_sum);
    defer a.free(input);
    const output = (try materialize(db, a, "w", input)).?;
    defer a.free(output);
    try db.exec(
        \\INSERT INTO ontologies(ontology_id,workspace_id,name,version,source_kind) VALUES('w::other','w','Other','1','linkml');
        \\INSERT INTO entities_raw(workspace_id,ontology_id,entity_id,external_id,entity_type,name,metadata_json)
        \\VALUES('w','w::other',100,'other-b','building','Other building','{"basis":999999}');
    );
    const read = (try readAnswer(db, a, "w", output)).?;
    defer a.free(read);
    try std.testing.expect(std.mem.indexOf(u8, read, "\"status\":\"ready\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, read, "999999") == null);
    try db.exec("UPDATE ontology_entity_types SET label='Changed definition' WHERE ontology_id='w::core' AND entity_type='unit'");
    const stale = (try readAnswer(db, a, "w", output)).?;
    defer a.free(stale);
    try std.testing.expect(std.mem.indexOf(u8, stale, "\"status\":\"stale\"") != null);
}

test "inconsistent time intervals cannot disappear as a complete empty answer" {
    var db = try testDb();
    defer db.close();
    try db.exec(
        \\INSERT INTO entities_raw(workspace_id,ontology_id,entity_id,external_id,entity_type,name,metadata_json)
        \\VALUES('w','w::core',4,'l','lease','Invalid dates','{"start":"2026-01-01","end":"2025-01-01","landlord":42}');
    );
    const a = std.testing.allocator;
    const input = try testInput(a, .dated_relations);
    defer a.free(input);
    const output = (try materialize(db, a, "w", input)).?;
    defer a.free(output);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"status\":\"indeterminate\"") != null);
}

test "mismatched quota units and contract basis are indeterminate" {
    var db = try testDb();
    defer db.close();
    const a = std.testing.allocator;
    const parsed = try std.json.parseFromSlice(Contract, a, test_contract, .{});
    defer parsed.deinit();
    var p = parsed.value;
    p.member_basis_field = "basis";
    p.expected_value = 1000;
    const input = try stringify(a, .{ .projection_contract = p });
    defer a.free(input);
    try db.exec("UPDATE entities_raw SET metadata_json=json_set(metadata_json,'$.basis',10000) WHERE entity_type='unit'");
    const output = (try materialize(db, a, "w", input)).?;
    defer a.free(output);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"status\":\"indeterminate\"") != null);
}

fn denyAgentFacts(_: ?*anyopaque, action: c_int, first: [*c]const u8, _: [*c]const u8, _: [*c]const u8, _: [*c]const u8) callconv(.c) c_int {
    if (action == c.SQLITE_READ and first != null and std.mem.eql(u8, std.mem.span(first), "agent_facts")) return c.SQLITE_DENY;
    return c.SQLITE_OK;
}

test "native materialization and serving succeed with agent_facts reads forbidden" {
    var db = try testDb();
    defer db.close();
    try std.testing.expectEqual(c.SQLITE_OK, c.sqlite3_set_authorizer(db.handle, denyAgentFacts, null));
    defer _ = c.sqlite3_set_authorizer(db.handle, null, null);
    var denied: ?*c.sqlite3_stmt = null;
    try std.testing.expectEqual(c.SQLITE_AUTH, c.sqlite3_prepare_v2(db.handle, "SELECT * FROM agent_facts", -1, &denied, null));
    const a = std.testing.allocator;
    const input = try testInput(a, .group_sum);
    defer a.free(input);
    const output = (try materialize(db, a, "w", input)).?;
    defer a.free(output);
    const read = (try readAnswer(db, a, "w", output)).?;
    defer a.free(read);
    try std.testing.expect(std.mem.indexOf(u8, read, "\"status\":\"ready\"") != null);
}
