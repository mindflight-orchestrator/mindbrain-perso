const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");

pub const NormalizeError = error{
    InvalidEntityType,
};

pub fn loadOntologyEntityTypes(
    allocator: std.mem.Allocator,
    db: facet_sqlite.Database,
    ontology_id: []const u8,
) !std.StringHashMap(void) {
    var types = std.StringHashMap(void).init(allocator);
    errdefer types.deinit();

    const sql = "SELECT entity_type FROM ontology_entity_types WHERE ontology_id = ?1 ORDER BY entity_type";
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, ontology_id);

    const c = facet_sqlite.c;
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        const ptr = c.sqlite3_column_text(stmt, 0) orelse continue;
        const entity_type = ptr[0..@intCast(c.sqlite3_column_bytes(stmt, 0))];
        try types.put(try allocator.dupe(u8, entity_type), {});
    }
    return types;
}

pub fn inferEntityTypeFromExternalId(external_id: []const u8, name: []const u8) ?[]const u8 {
    if (std.mem.startsWith(u8, external_id, "shared_space_")) return "shared_space";
    if (std.mem.startsWith(u8, external_id, "shared_equipment_")) return "shared_equipment";
    if (std.mem.startsWith(u8, external_id, "household_") and std.mem.indexOf(u8, name, "Household") != null) {
        return "household";
    }
    return null;
}

pub fn normalizeBusinessEntityType(
    allocator: std.mem.Allocator,
    ontology_types: *const std.StringHashMap(void),
    external_id: []const u8,
    entity_type: []const u8,
    name: []const u8,
) ![]const u8 {
    const inferred = inferEntityTypeFromExternalId(external_id, name);
    const candidate = inferred orelse entity_type;

    if (std.mem.eql(u8, candidate, "entity") and ontology_types.get("entity") == null) {
        return NormalizeError.InvalidEntityType;
    }
    if (ontology_types.get(candidate) == null) {
        return NormalizeError.InvalidEntityType;
    }
    return try allocator.dupe(u8, candidate);
}

test "normalizeBusinessEntityType fixes shared_space prefix" {
    var types = std.StringHashMap(void).init(std.testing.allocator);
    defer types.deinit();
    try types.put("shared_space", {});
    try types.put("shared_equipment", {});
    try types.put("household", {});

    const normalized = try normalizeBusinessEntityType(
        std.testing.allocator,
        &types,
        "shared_space_hall",
        "entity",
        "Hall",
    );
    defer std.testing.allocator.free(normalized);
    try std.testing.expectEqualStrings("shared_space", normalized);
}

test "normalizeBusinessEntityType rejects generic entity type" {
    var types = std.StringHashMap(void).init(std.testing.allocator);
    defer types.deinit();
    try types.put("building", {});

    const result = normalizeBusinessEntityType(
        std.testing.allocator,
        &types,
        "misc_thing",
        "entity",
        "Misc",
    );
    try std.testing.expectError(NormalizeError.InvalidEntityType, result);
}
