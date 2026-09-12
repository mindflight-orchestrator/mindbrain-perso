//! Bounded proof reads from declared ontology profiles. Caller owns an arena.
const std = @import("std");
const sql = @import("facet_sqlite.zig");
const refs = @import("entity_reference.zig");
const c = sql.c;
const A = std.mem.Allocator;

pub const Request = struct {
    workspace_id: []const u8,
    assertion_ref: refs.Reference,
    include_text: bool = true,
    limit: usize = 20,
    cursor: ?[]const u8 = null,
};
pub const Node = struct { entity_id: u32, name: []const u8, entity_type: []const u8, metadata: std.json.Value, external_id: ?[]const u8, ontology_id: ?[]const u8 };
pub const Edge = struct { relation_id: u64, predicate: []const u8, source_id: u32, target_id: u32, metadata: std.json.Value };
pub const Step = struct { predicate: []const u8, direction: enum { inbound, outbound } };
pub const Profile = struct { id: []const u8, representation: enum { direct_support, evidence_node }, assertion_type: []const u8, passage_type: []const u8, document_type: []const u8, evidence_type: ?[]const u8 = null, steps: []Step };
pub fn validateProfiles(profiles: []const Profile) !void {
    if (profiles.len == 0 or profiles.len > 8) return error.InvalidEvidenceProfile;
    for (profiles, 0..) |profile, index| {
        const count: usize = if (profile.representation == .direct_support) 2 else 3;
        if (profile.id.len == 0 or profile.id.len > 128 or profile.steps.len != count or
            profile.assertion_type.len == 0 or profile.passage_type.len == 0 or profile.document_type.len == 0 or
            (count == 3 and (profile.evidence_type == null or profile.evidence_type.?.len == 0))) return error.InvalidEvidenceProfile;
        for (profiles[0..index]) |previous| if (std.mem.eql(u8, previous.id, profile.id)) return error.InvalidEvidenceProfile;
        for (profile.steps) |step| if (step.predicate.len == 0 or step.predicate.len > 63) return error.InvalidEvidenceProfile;
    }
}
pub const Proof = struct {
    profile_id: []const u8,
    representation: []const u8,
    evidence_ref: ?Node,
    passage: ?Node,
    document: ?Node,
    edges: []?Edge,
    relationship_status: []const u8,
    text_status: []const u8 = "not_stored",
    text: ?[]const u8 = null,
    text_omitted: bool = false,
    declared_sha256: ?[]const u8 = null,
    computed_sha256: ?[]const u8 = null,
};
pub const Result = struct { workspace_id: []const u8, assertion: Node, paths: []Proof, complete: bool, next_cursor: ?[]const u8, snapshot: []const u8, backend: []const u8 = "native" };

fn text(value: std.json.Value, key: []const u8) ?[]const u8 {
    if (value != .object) return null;
    const item = value.object.get(key) orelse return null;
    return if (item == .string) item.string else null;
}
fn integer(value: std.json.Value, key: []const u8) ?usize {
    if (value != .object) return null;
    const item = value.object.get(key) orelse return null;
    return if (item == .integer) std.math.cast(usize, item.integer) else null;
}
fn parse(a: A, value: []const u8) !std.json.Value {
    return std.json.parseFromSliceLeaky(std.json.Value, a, value, .{ .allocate = .alloc_always });
}
fn string(a: A, stmt: *c.sqlite3_stmt, col: c_int) !?[]const u8 {
    return if (c.sqlite3_column_type(stmt, col) == c.SQLITE_NULL) null else try sql.dupeColumnText(a, stmt, col);
}
fn node(a: A, stmt: *c.sqlite3_stmt, col: c_int) !?Node {
    if (c.sqlite3_column_type(stmt, col) == c.SQLITE_NULL) return null;
    return .{ .entity_id = try sql.columnU32(stmt, col), .name = (try string(a, stmt, col + 1)).?, .entity_type = (try string(a, stmt, col + 2)).?, .metadata = try parse(a, (try string(a, stmt, col + 3)).?), .external_id = try string(a, stmt, col + 4), .ontology_id = try string(a, stmt, col + 5) };
}
fn edge(a: A, stmt: *c.sqlite3_stmt, col: c_int) !?Edge {
    if (c.sqlite3_column_type(stmt, col) == c.SQLITE_NULL) return null;
    return .{ .relation_id = try sql.columnU64(stmt, col), .predicate = (try string(a, stmt, col + 1)).?, .source_id = try sql.columnU32(stmt, col + 2), .target_id = try sql.columnU32(stmt, col + 3), .metadata = try parse(a, (try string(a, stmt, col + 4)).?) };
}
fn append(a: A, buf: *std.ArrayList(u8), comptime fmt: []const u8, args: anytype) !void {
    try buf.appendSlice(a, try std.fmt.allocPrint(a, fmt, args));
}

fn pathsForProfile(db: sql.Database, a: A, workspace: []const u8, assertion: Node, profile: Profile) ![]Proof {
    const count: usize = if (profile.representation == .direct_support) 2 else 3;
    if (profile.steps.len != count or profile.id.len == 0 or (count == 3 and profile.evidence_type == null)) return error.InvalidEvidenceProfile;
    var query: std.ArrayList(u8) = .empty;
    try query.appendSlice(a, "SELECT ");
    for (0..count) |i| {
        if (profile.steps[i].predicate.len == 0) return error.InvalidEvidenceProfile;
        if (i != 0) try query.appendSlice(a, ",");
        try append(a, &query, "g{d}.entity_id,g{d}.name,g{d}.entity_type,g{d}.metadata_json,e{d}.external_id,e{d}.ontology_id,r{d}.relation_id,r{d}.relation_type,r{d}.source_id,r{d}.target_id,r{d}.metadata_json", .{ i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i, i, i, i, i });
    }
    try query.appendSlice(a, " FROM graph_entity g0 ");
    for (profile.steps, 0..) |step, i| {
        const from = if (step.direction == .inbound) "target_id" else "source_id";
        const to = if (step.direction == .inbound) "source_id" else "target_id";
        try append(a, &query, " LEFT JOIN graph_relation r{d} ON r{d}.{s}=g{d}.entity_id AND r{d}.workspace_id=?1 AND r{d}.relation_type=?{d} AND r{d}.deprecated_at IS NULL AND (r{d}.valid_from_unix IS NULL OR r{d}.valid_from_unix<=unixepoch()) AND (r{d}.valid_to_unix IS NULL OR r{d}.valid_to_unix>unixepoch())", .{ i, i, from, i, i, i, 3 + i, i, i, i, i, i });
        try append(a, &query, " LEFT JOIN graph_entity g{d} ON g{d}.entity_id=r{d}.{s} AND g{d}.workspace_id=?1 AND g{d}.deprecated_at IS NULL LEFT JOIN entities_raw e{d} ON e{d}.entity_id=g{d}.entity_id AND e{d}.workspace_id=?1 AND e{d}.metadata_json=g{d}.metadata_json", .{ i + 1, i + 1, i, to, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1 });
    }
    try query.appendSlice(a, " WHERE g0.workspace_id=?1 AND g0.entity_id=?2 AND r0.relation_id IS NOT NULL ORDER BY ");
    for (0..count) |i| {
        if (i != 0) try query.appendSlice(a, ",");
        try append(a, &query, "r{d}.relation_id", .{i});
    }
    try query.appendSlice(a, " LIMIT 1001");
    const stmt = try sql.prepare(db, query.items);
    defer sql.finalize(stmt);
    try sql.bindText(stmt, 1, workspace);
    try sql.bindInt64(stmt, 2, assertion.entity_id);
    for (profile.steps, 0..) |step, i| try sql.bindText(stmt, @intCast(3 + i), step.predicate);
    var result: std.ArrayList(Proof) = .empty;
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        if (result.items.len == 1000) return error.EvidenceLimitExceeded;
        var nodes: [3]?Node = .{ null, null, null };
        const edges = try a.alloc(?Edge, count);
        var complete = true;
        for (0..count) |i| {
            nodes[i] = try node(a, stmt, @intCast(i * 11));
            edges[i] = try edge(a, stmt, @intCast(i * 11 + 6));
            const expected = if (i == count - 1) profile.document_type else if (i == count - 2) profile.passage_type else profile.evidence_type.?;
            if (nodes[i]) |n| {
                if (!std.mem.eql(u8, n.entity_type, expected) or n.ontology_id == null or !std.mem.eql(u8, n.ontology_id.?, assertion.ontology_id.?)) complete = false;
            } else complete = false;
            if (edges[i] == null) complete = false;
        }
        try result.append(a, .{ .profile_id = profile.id, .representation = @tagName(profile.representation), .evidence_ref = if (count == 3) nodes[0] else null, .passage = nodes[count - 2], .document = nodes[count - 1], .edges = edges, .relationship_status = if (complete) "complete" else "incomplete" });
    }
    return result.toOwnedSlice(a);
}

fn verifyText(a: A, proof: *Proof) !void {
    if (!std.mem.eql(u8, proof.relationship_status, "complete")) return;
    const passage = proof.passage.?.metadata;
    const document = proof.document.?.metadata;
    const source = text(document, "source_text") orelse return;
    proof.text_status = "invalid_span";
    const version = text(document, "text_version") orelse return;
    if (!std.mem.eql(u8, version, text(passage, "document_version") orelse return)) return;
    if (!std.mem.eql(u8, text(document, "encoding") orelse "utf-8", "utf-8")) return;
    const start = integer(passage, "start") orelse return;
    const end = integer(passage, "end") orelse return;
    if (start >= end) return;
    const unit = text(passage, "offset_unit") orelse return;
    var begin = start;
    var stop = end;
    const view = std.unicode.Utf8View.init(source) catch return;
    if (std.mem.eql(u8, unit, "unicode_codepoints")) {
        var iter = view.iterator();
        var index: usize = 0;
        begin = source.len;
        stop = source.len;
        while (iter.nextCodepointSlice()) |slice| : (index += 1) {
            const offset = iter.i - slice.len;
            if (index == start) begin = offset;
            if (index == end) stop = offset;
        }
        if (end > index) return;
    } else if (!std.mem.eql(u8, unit, "utf8_bytes")) return;
    if (begin >= stop or stop > source.len or !std.unicode.utf8ValidateSlice(source[begin..stop])) return;
    const excerpt = source[begin..stop];
    const declared = text(passage, "sha256") orelse return;
    proof.declared_sha256 = declared;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(excerpt, &digest, .{});
    proof.computed_sha256 = try a.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
    proof.text_status = if (std.mem.eql(u8, declared, proof.computed_sha256.?)) "verified" else "hash_mismatch";
    if (std.mem.eql(u8, proof.text_status, "verified")) proof.text = excerpt;
}

pub fn get(db: sql.Database, a: A, request: Request) !Result {
    if (request.limit == 0 or request.limit > 100) return error.BadRequest;
    try db.exec("BEGIN");
    defer db.exec("ROLLBACK") catch {};
    const id = try refs.resolve(db, request.workspace_id, request.assertion_ref);
    const stmt = try sql.prepare(db, "SELECT g.entity_id,g.name,g.entity_type,g.metadata_json,e.external_id,e.ontology_id,o.metadata_json FROM graph_entity g JOIN entities_raw e ON e.entity_id=g.entity_id AND e.workspace_id=g.workspace_id JOIN ontologies o ON o.ontology_id=e.ontology_id AND o.workspace_id=g.workspace_id WHERE g.workspace_id=?1 AND g.entity_id=?2");
    defer sql.finalize(stmt);
    try sql.bindText(stmt, 1, request.workspace_id);
    try sql.bindInt64(stmt, 2, id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.NotFound;
    var assertion = (try node(a, stmt, 0)).?;
    const metadata = try parse(a, (try string(a, stmt, 6)).?);
    if (metadata != .object) return error.InvalidEvidenceProfile;
    const profiles_value = metadata.object.get("evidence_profiles") orelse return error.EvidenceProfileUnavailable;
    const profiles = std.json.parseFromValueLeaky([]Profile, a, profiles_value, .{ .ignore_unknown_fields = false }) catch return error.InvalidEvidenceProfile;
    try validateProfiles(profiles);
    var paths: std.ArrayList(Proof) = .empty;
    var matched = false;
    for (profiles) |profile| {
        if (!std.mem.eql(u8, profile.assertion_type, assertion.entity_type)) continue;
        matched = true;
        try paths.appendSlice(a, try pathsForProfile(db, a, request.workspace_id, assertion, profile));
        if (paths.items.len > 1000) return error.EvidenceLimitExceeded;
    }
    if (!matched) return error.EvidenceProfileUnavailable;
    for (paths.items) |*proof| try verifyText(a, proof);
    const serialized = try std.json.Stringify.valueAlloc(a, .{ request.workspace_id, assertion, paths.items }, .{});
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(serialized, &digest, .{});
    const snapshot = try a.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
    var offset: usize = 0;
    if (request.cursor) |cursor| {
        if (cursor.len < 66 or cursor[64] != ':' or !std.mem.eql(u8, snapshot, cursor[0..64])) return error.StaleCursor;
        offset = std.fmt.parseInt(usize, cursor[65..], 10) catch return error.BadRequest;
        if (offset >= paths.items.len) return error.StaleCursor;
    }
    const end = @min(paths.items.len, offset + request.limit);
    const selected = paths.items[offset..end];
    var complete = paths.items.len > 0 and end == paths.items.len;
    var bytes: usize = 0;
    for (selected) |*proof| {
        if (!std.mem.eql(u8, proof.relationship_status, "complete") or !std.mem.eql(u8, proof.text_status, "verified")) complete = false;
        if (proof.text) |excerpt| {
            if (!request.include_text or bytes + excerpt.len > 65536) {
                proof.text = null;
                proof.text_omitted = true;
                if (request.include_text) complete = false;
            } else bytes += excerpt.len;
        }
        // Do not leak an unbounded source snapshot through graph metadata.
        for ([_]*?Node{ &proof.evidence_ref, &proof.passage, &proof.document }) |item| {
            if (item.*) |*n| if (n.metadata == .object) {
                _ = n.metadata.object.swapRemove("source_text");
            };
        }
    }
    if (assertion.metadata == .object) _ = assertion.metadata.object.swapRemove("source_text");
    return .{ .workspace_id = request.workspace_id, .assertion = assertion, .paths = selected, .complete = complete, .snapshot = snapshot, .next_cursor = if (end < paths.items.len) try std.fmt.allocPrint(a, "{s}:{d}", .{ snapshot, end }) else null };
}

test "source spans enforce Unicode boundaries version and hashes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const make = struct {
        fn proof(allocator: A, passage_json: []const u8) !Proof {
            return .{ .profile_id = "test", .representation = "direct_support", .evidence_ref = null, .edges = &.{}, .relationship_status = "complete", .passage = .{ .entity_id = 1, .name = "passage", .entity_type = "PassageStable", .ontology_id = "legal", .external_id = "p", .metadata = try parse(allocator, passage_json) }, .document = .{ .entity_id = 2, .name = "document", .entity_type = "Document", .ontology_id = "legal", .external_id = "d", .metadata = try parse(allocator, "{\"source_text\":\"àéz\",\"text_version\":\"v1\"}") } };
        }
    }.proof;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("é", &digest, .{});
    const hash = std.fmt.bytesToHex(digest, .lower);
    for ([_][]const u8{ "unicode_codepoints", "utf8_bytes" }) |unit| {
        const points = std.mem.eql(u8, unit, "unicode_codepoints");
        const json = try std.json.Stringify.valueAlloc(a, .{ .document_version = "v1", .start = @as(usize, if (points) 1 else 2), .end = @as(usize, if (points) 2 else 4), .offset_unit = unit, .sha256 = hash[0..] }, .{});
        var proof = try make(a, json);
        try verifyText(a, &proof);
        try std.testing.expectEqualStrings("verified", proof.text_status);
        try std.testing.expectEqualStrings("é", proof.text.?);
    }
    for ([_][]const u8{ "{\"document_version\":\"v1\",\"start\":1,\"end\":4,\"offset_unit\":\"utf8_bytes\"}", "{\"document_version\":\"v2\",\"start\":0,\"end\":2,\"offset_unit\":\"unicode_codepoints\"}", "{\"document_version\":\"v1\",\"start\":0,\"end\":20,\"offset_unit\":\"unicode_codepoints\"}" }) |json| {
        var proof = try make(a, json);
        try verifyText(a, &proof);
        try std.testing.expectEqualStrings("invalid_span", proof.text_status);
        try std.testing.expect(proof.text == null);
    }
}
