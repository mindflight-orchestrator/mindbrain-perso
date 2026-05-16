const std = @import("std");

pub const canonical_sqlite_schema = @embedFile("sqlite_mindbrain_schema");

pub const SchemaMode = enum {
    runtime,
    import,
};

pub fn renderMetadataSchema(allocator: std.mem.Allocator) ![]u8 {
    return try renderMetadataSchemaWithMode(allocator, .runtime);
}

pub fn renderMetadataSchemaWithMode(allocator: std.mem.Allocator, mode: SchemaMode) ![]u8 {
    return switch (mode) {
        .runtime => try allocator.dupe(u8, canonical_sqlite_schema),
        .import => try renderImportSchema(allocator),
    };
}

fn renderImportSchema(allocator: std.mem.Allocator) ![]u8 {
    var schema = try allocator.dupe(u8, canonical_sqlite_schema);
    errdefer allocator.free(schema);

    try replaceSchemaFragment(allocator, &schema,
        \\    created_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
        \\    UNIQUE(entity_type, name)
    ,
        \\    created_at_unix INTEGER NOT NULL DEFAULT (unixepoch())
    );

    try replaceSchemaFragment(allocator, &schema,
        \\    confidence REAL NOT NULL DEFAULT 1.0,
        \\    PRIMARY KEY(term, entity_id),
        \\    FOREIGN KEY(entity_id) REFERENCES graph_entity(entity_id)
    ,
        \\    confidence REAL NOT NULL DEFAULT 1.0
    );

    try replaceSchemaFragment(allocator, &schema,
        \\    created_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
        \\    FOREIGN KEY(source_id) REFERENCES graph_entity(entity_id),
        \\    FOREIGN KEY(target_id) REFERENCES graph_entity(entity_id)
    ,
        \\    created_at_unix INTEGER NOT NULL DEFAULT (unixepoch())
    );

    try replaceSchemaFragment(allocator, &schema,
        \\CREATE INDEX IF NOT EXISTS graph_entity_name_idx
        \\    ON graph_entity(name);
    ,
        \\
    );
    try replaceSchemaFragment(allocator, &schema,
        \\CREATE INDEX IF NOT EXISTS graph_entity_workspace_id_idx
        \\    ON graph_entity(workspace_id);
    ,
        \\
    );
    try replaceSchemaFragment(allocator, &schema,
        \\CREATE INDEX IF NOT EXISTS graph_relation_workspace_id_idx
        \\    ON graph_relation(workspace_id);
    ,
        \\
    );
    try replaceSchemaFragment(allocator, &schema,
        \\CREATE INDEX IF NOT EXISTS graph_relation_source_id_idx
        \\    ON graph_relation(source_id, relation_id);
    ,
        \\
    );
    try replaceSchemaFragment(allocator, &schema,
        \\CREATE INDEX IF NOT EXISTS graph_relation_target_id_idx
        \\    ON graph_relation(target_id, relation_id);
    ,
        \\
    );

    return schema;
}

fn replaceSchemaFragment(
    allocator: std.mem.Allocator,
    schema: *[]u8,
    needle: []const u8,
    replacement: []const u8,
) !void {
    const replaced = try replaceAll(allocator, schema.*, needle, replacement);
    allocator.free(schema.*);
    schema.* = replaced;
}

fn replaceAll(
    allocator: std.mem.Allocator,
    input: []const u8,
    needle: []const u8,
    replacement: []const u8,
) ![]u8 {
    std.debug.assert(needle.len > 0);

    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);

    var rest = input;
    while (std.mem.indexOf(u8, rest, needle)) |index| {
        try out.appendSlice(allocator, rest[0..index]);
        try out.appendSlice(allocator, replacement);
        rest = rest[index + needle.len ..];
    }
    try out.appendSlice(allocator, rest);

    return out.toOwnedSlice(allocator);
}

test "sqlite schema is loaded from the canonical install file" {
    const schema = try renderMetadataSchema(std.testing.allocator);
    defer std.testing.allocator.free(schema);

    try std.testing.expect(std.mem.indexOf(u8, schema, "MindBrain SQLite install schema") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS workspaces") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS table_semantics") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS column_semantics") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS relation_semantics") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS source_mappings") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS facet_tables") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS facet_definitions") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS facet_postings") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS facet_deltas") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS facet_value_nodes") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS graph_entity") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE INDEX IF NOT EXISTS graph_entity_name_idx") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS graph_relation") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE INDEX IF NOT EXISTS graph_relation_source_id_idx") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE INDEX IF NOT EXISTS graph_relation_target_id_idx") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS graph_entity_chunk") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE INDEX IF NOT EXISTS graph_entity_chunk_source_idx") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS graph_lj_out") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS graph_lj_in") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS graph_execution_run") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS graph_knowledge_patch") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS graph_entity_degree") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS facets") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS projections") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS agent_state") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS memory_items") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS memory_projections") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS memory_edges") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS search_documents") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS search_fts_docs") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE VIRTUAL TABLE IF NOT EXISTS search_fts USING fts5") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS search_embeddings") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS search_document_stats") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS search_collection_stats") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS search_term_stats") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS search_term_frequencies") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS search_postings") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS signal_raw") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS bm25_stopwords") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE INDEX IF NOT EXISTS bm25_stopwords_source_idx") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS alert_enriched") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS alert_escalated") != null);
}

test "sqlite schema includes the collection raw tables" {
    const schema = try renderMetadataSchema(std.testing.allocator);
    defer std.testing.allocator.free(schema);

    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS collections") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS ontologies") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS collection_ontologies") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS workspace_settings") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS ontology_namespaces") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS ontology_dimensions") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS ontology_values") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS ontology_entity_types") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS ontology_edge_types") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS ontology_entities_raw") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS ontology_relations_raw") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS documents_raw") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS chunks_raw") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS documents_raw_vector") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS chunks_raw_vector") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS facet_assignments_raw") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS entities_raw") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS entity_aliases_raw") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS relations_raw") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS entity_documents_raw") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS entity_chunks_raw") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS document_links_raw") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE TABLE IF NOT EXISTS external_links_raw") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "doc_nanoid TEXT NOT NULL DEFAULT ''") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "documents_raw_nanoid_uidx") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "summary TEXT") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "strategy TEXT") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "token_count INTEGER") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "parent_chunk_index INTEGER") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "external_links_raw_doc_idx") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "external_links_raw_uri_idx") != null);
}

test "sqlite import schema skips graph indexes and hot import constraints" {
    const schema = try renderMetadataSchemaWithMode(std.testing.allocator, .import);
    defer std.testing.allocator.free(schema);

    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE INDEX IF NOT EXISTS graph_entity_name_idx") == null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE INDEX IF NOT EXISTS graph_entity_workspace_id_idx") == null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE INDEX IF NOT EXISTS graph_relation_source_id_idx") == null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE INDEX IF NOT EXISTS graph_relation_target_id_idx") == null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "CREATE INDEX IF NOT EXISTS graph_relation_workspace_id_idx") == null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "UNIQUE(entity_type, name)") == null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "PRIMARY KEY(term, entity_id)") == null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "FOREIGN KEY(source_id) REFERENCES graph_entity(entity_id)") == null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "FOREIGN KEY(target_id) REFERENCES graph_entity(entity_id)") == null);
}
