const std = @import("std");
const collections_sqlite = @import("collections_sqlite.zig");
const facet_sqlite = @import("facet_sqlite.zig");

const SyndicDimension = struct {
    namespace: []const u8,
    dimension: []const u8,
    value_type: []const u8,
    is_multi: bool,
};

const syndic_dimensions = [_]SyndicDimension{
    .{ .namespace = "source", .dimension = "document_type", .value_type = "string", .is_multi = false },
    .{ .namespace = "domain", .dimension = "building", .value_type = "string", .is_multi = true },
    .{ .namespace = "domain", .dimension = "unit", .value_type = "string", .is_multi = true },
    .{ .namespace = "domain", .dimension = "role", .value_type = "string", .is_multi = true },
    .{ .namespace = "domain", .dimension = "scenario", .value_type = "string", .is_multi = true },
    .{ .namespace = "domain", .dimension = "status", .value_type = "string", .is_multi = true },
    .{ .namespace = "domain", .dimension = "decision", .value_type = "string", .is_multi = true },
    .{ .namespace = "finance", .dimension = "charge_status", .value_type = "string", .is_multi = true },
    .{ .namespace = "finance", .dimension = "payment_status", .value_type = "string", .is_multi = true },
};

const syndic_document_types = [_][]const u8{
    "statuts_copropriete",
    "registre_coproprietaires",
    "composition_menage",
    "bail",
    "pv_ag",
    "extrait_coda",
    "annexe_lot",
    "registre_facturation",
};

const syndic_namespaces = [_]struct { namespace: []const u8, label: []const u8 }{
    .{ .namespace = "source", .label = "Source documentaire" },
    .{ .namespace = "domain", .label = "Domaine syndic" },
    .{ .namespace = "finance", .label = "Finance syndic" },
};

/// Seeds syndic facet vocabulary on the target workspace/ontology only.
pub fn seedSyndicProfile(
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    ontology_id: []const u8,
) !void {
    try collections_sqlite.ensureWorkspace(db, .{
        .workspace_id = workspace_id,
        .domain_profile = "syndic",
    });
    try collections_sqlite.setDefaultOntology(db, workspace_id, ontology_id);

    for (syndic_namespaces) |ns| {
        try collections_sqlite.ensureNamespace(db, .{
            .ontology_id = ontology_id,
            .namespace = ns.namespace,
            .label = ns.label,
        });
    }

    for (syndic_dimensions) |dim| {
        try collections_sqlite.ensureDimension(db, .{
            .ontology_id = ontology_id,
            .namespace = dim.namespace,
            .dimension = dim.dimension,
            .value_type = dim.value_type,
            .is_multi = dim.is_multi,
            .hierarchy_kind = "flat",
        });
    }

    var value_id: u32 = 1;
    for (syndic_document_types) |value| {
        try collections_sqlite.ensureValue(db, .{
            .ontology_id = ontology_id,
            .namespace = "source",
            .dimension = "document_type",
            .value_id = value_id,
            .value = value,
        });
        value_id += 1;
    }
}

pub fn syndicDimensionCount() usize {
    return syndic_dimensions.len;
}

test "seedSyndicProfile seeds dimensions for target ontology only" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try seedSyndicProfile(db, "ws-syndic-a", "ws-syndic-a::core");
    try seedSyndicProfile(db, "ws-syndic-b", "ws-syndic-b::core");

    const countForOntology = struct {
        fn run(db_handle: facet_sqlite.Database, ontology_id: []const u8) !i64 {
            const stmt = try facet_sqlite.prepare(
                db_handle,
                "SELECT COUNT(*) FROM ontology_dimensions WHERE ontology_id = ?1",
            );
            defer facet_sqlite.finalize(stmt);
            try facet_sqlite.bindText(stmt, 1, ontology_id);
            try std.testing.expect(facet_sqlite.c.sqlite3_step(stmt) == facet_sqlite.c.SQLITE_ROW);
            return facet_sqlite.c.sqlite3_column_int64(stmt, 0);
        }
    };

    try std.testing.expect(try countForOntology.run(db, "ws-syndic-a::core") >= syndicDimensionCount());
    try std.testing.expect(try countForOntology.run(db, "ws-syndic-b::core") >= syndicDimensionCount());
}

test "seedSyndicProfile sets workspace domain_profile and default ontology" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try seedSyndicProfile(db, "ws-syndic", "ws-syndic::core");

    const default_id = try collections_sqlite.defaultOntology(db, std.testing.allocator, "ws-syndic");
    defer if (default_id) |id| std.testing.allocator.free(id);
    try std.testing.expect(default_id != null);
    try std.testing.expectEqualStrings("ws-syndic::core", default_id.?);

    const stmt = try facet_sqlite.prepare(db, "SELECT domain_profile FROM workspaces WHERE workspace_id = ?1");
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, "ws-syndic");
    try std.testing.expect(facet_sqlite.c.sqlite3_step(stmt) == facet_sqlite.c.SQLITE_ROW);
    const profile = facet_sqlite.c.sqlite3_column_text(stmt, 0).?[0..@intCast(facet_sqlite.c.sqlite3_column_bytes(stmt, 0))];
    try std.testing.expectEqualStrings("syndic", profile);
}
