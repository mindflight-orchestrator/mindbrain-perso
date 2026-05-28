const std = @import("std");

pub const QualificationFacetRow = struct {
    namespace: []const u8,
    dimension: []const u8,
};

/// Returns true when csv lists `namespace.dimension` (comma-separated facet ids).
pub fn facetCsvContains(csv: ?[]const u8, namespace: []const u8, dimension: []const u8) bool {
    const raw = csv orelse return true;
    var it = std.mem.splitScalar(u8, raw, ',');
    while (it.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t\r\n");
        if (trimmed.len == namespace.len + 1 + dimension.len and
            std.mem.eql(u8, trimmed[0..namespace.len], namespace) and
            trimmed[namespace.len] == '.' and
            std.mem.eql(u8, trimmed[namespace.len + 1 ..], dimension))
        {
            return true;
        }
    }
    return false;
}

fn splitFacetId(facet_id: []const u8) ?QualificationFacetRow {
    if (std.mem.indexOfScalar(u8, facet_id, '.')) |dot| {
        if (dot == 0 or dot + 1 >= facet_id.len) return null;
        return .{
            .namespace = facet_id[0..dot],
            .dimension = facet_id[dot + 1 ..],
        };
    }
    return null;
}

fn namespaceLooksLikeOntologyId(namespace: []const u8) bool {
    return std.mem.indexOf(u8, namespace, "::") != null;
}

/// Normalizes common LLM drift: ontology id used as namespace, or full facet id in dimension.
pub fn normalizeQualificationFacet(
    namespace: []const u8,
    dimension: []const u8,
    facet_filter: ?[]const u8,
) QualificationFacetRow {
    if (namespaceLooksLikeOntologyId(namespace) and std.mem.indexOfScalar(u8, dimension, '.') != null) {
        if (splitFacetId(dimension)) |split| return split;
    }
    if (std.mem.indexOfScalar(u8, namespace, '.') != null) {
        if (splitFacetId(namespace)) |split| return split;
    }
    if (namespaceLooksLikeOntologyId(namespace)) {
        if (facet_filter) |csv| {
            var it = std.mem.splitScalar(u8, csv, ',');
            while (it.next()) |part| {
                const trimmed = std.mem.trim(u8, part, " \t\r\n");
                if (splitFacetId(trimmed)) |split| {
                    if (std.mem.eql(u8, split.dimension, dimension)) return split;
                }
            }
        }
    }
    return .{ .namespace = namespace, .dimension = dimension };
}

test "normalizeQualificationFacet splits ontology namespace drift" {
    const facet_filter = "domain.building,domain.decision,source.document_type";
    const normalized = normalizeQualificationFacet("ws::core", "domain.building", facet_filter);
    try std.testing.expectEqualStrings("domain", normalized.namespace);
    try std.testing.expectEqualStrings("building", normalized.dimension);
}

test "facetCsvContains parses domain.building filter" {
    try std.testing.expect(facetCsvContains("domain.building,domain.unit", "domain", "building"));
    try std.testing.expect(!facetCsvContains("domain.building,domain.unit", "ws::core", "domain.building"));
}
