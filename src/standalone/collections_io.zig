//! Backup/restore helpers for the workspace -> collection -> document/chunk
//! raw layer defined in collections_sqlite.zig. Exports produce a self-
//! contained JSON bundle; imports replay it through the canonical raw
//! upsert helpers so derived indexes can be rebuilt with reindexAll.

const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const collections_sqlite = @import("collections_sqlite.zig");
const search_sqlite = @import("search_sqlite.zig");
const schema_column_migrations = @import("schema_column_migrations.zig");

const Allocator = std.mem.Allocator;
const Database = facet_sqlite.Database;
const c = facet_sqlite.c;

pub const Scope = union(enum) {
    workspace: []const u8,
    taxonomies: []const u8,
    collection: struct { workspace_id: []const u8, collection_id: []const u8 },
};

pub const ExportOptions = struct {
    include_vectors: bool = true,
};

pub const BundleSummary = struct {
    kind: []const u8,
    schema_version: []const u8,
    scope_kind: []const u8,
    workspace_id: []const u8,
    collection_id: ?[]const u8,
    workspace_count: usize,
    collection_count: usize,
    ontology_count: usize,
    ontology_value_count: usize,
    document_count: usize,
    chunk_count: usize,
    facet_table_count: usize,
    facet_table_id: ?u64,
    facet_collection_id: ?[]const u8,
    relation_property_count: usize,
    answer_artifact_count: usize,
    answer_event_count: usize,
    exported_mindbrain_version: ?[]const u8 = null,

    pub fn deinit(self: BundleSummary, allocator: Allocator) void {
        allocator.free(self.kind);
        allocator.free(self.schema_version);
        allocator.free(self.scope_kind);
        allocator.free(self.workspace_id);
        if (self.collection_id) |value| allocator.free(value);
        if (self.facet_collection_id) |value| allocator.free(value);
        if (self.exported_mindbrain_version) |value| allocator.free(value);
    }
};

const json_writer_capacity: usize = 8 * 1024;

const Bundle = struct {
    kind: []const u8 = "ghostcrab_backup_bundle",
    schema_version: []const u8 = "2",
    exported_with: ?ExportedWithJson = null,
    scope: ScopeJson,
    workspaces: []WorkspaceRow,
    collections: []CollectionRow,
    ontologies: []OntologyRow,
    ontology_namespaces: []OntologyNamespaceRow = &.{},
    ontology_dimensions: []OntologyDimensionRow = &.{},
    ontology_values: []OntologyValueRow = &.{},
    ontology_entity_types: []OntologyEntityTypeRow = &.{},
    ontology_edge_types: []OntologyEdgeTypeRow = &.{},
    ontology_entities: []OntologyEntityRow = &.{},
    ontology_relations: []OntologyRelationRow = &.{},
    ontology_triples: []OntologyTripleRow = &.{},
    collection_ontologies: []CollectionOntologyRow,
    workspace_settings: []WorkspaceSettingsRow,
    agent_facts: []AgentFactRow = &.{},
    facet_tables: []FacetTableRow = &.{},
    facet_definitions: []FacetDefinitionRow = &.{},
    documents_raw: []DocumentRow,
    chunks_raw: []ChunkRow,
    documents_raw_vector: []VectorRow = &.{},
    chunks_raw_vector: []VectorRow = &.{},
    facet_assignments_raw: []FacetAssignmentRow,
    entities_raw: []EntityRow,
    entity_aliases_raw: []EntityAliasRow,
    relations_raw: []RelationRow,
    relation_properties_raw: []RelationPropertyRow = &.{},
    entity_documents_raw: []EntityDocumentRow,
    entity_chunks_raw: []EntityChunkRow,
    graph_entity: []GraphEntityRow = &.{},
    graph_entity_alias: []GraphEntityAliasRow = &.{},
    graph_relation: []GraphRelationRow = &.{},
    graph_relation_property: []GraphRelationPropertyRow = &.{},
    graph_entity_document: []GraphEntityDocumentRow = &.{},
    graph_entity_chunk: []GraphEntityChunkRow = &.{},
    document_links_raw: []DocumentLinkRow,
    external_links_raw: []ExternalLinkRow = &.{},
    mindbrain_answer_artifacts: []AnswerArtifactRow = &.{},
    mindbrain_answer_events: []AnswerEventRow = &.{},
    quality_convergence_run: []QualityConvergenceRunRow = &.{},
    quality_remediation_action: []QualityRemediationActionRow = &.{},
};

const ScopeJson = struct {
    kind: []const u8,
    workspace_id: []const u8,
    collection_id: ?[]const u8,
};

const ExportedWithJson = struct {
    mindbrain_version: []const u8,
    bundle_schema_version: []const u8,
};

const WorkspaceRow = struct {
    workspace_id: []const u8,
    label: ?[]const u8,
    description: ?[]const u8,
    domain_profile: ?[]const u8,
};

const CollectionRow = struct {
    collection_id: []const u8,
    workspace_id: []const u8,
    name: []const u8,
    key_kind: []const u8,
    chunk_bits: i64,
    default_language: []const u8,
    metadata_json: []const u8,
};

const OntologyRow = struct {
    ontology_id: []const u8,
    workspace_id: ?[]const u8,
    name: []const u8,
    version: []const u8,
    frozen: bool,
    source_kind: []const u8,
    metadata_json: []const u8,
};

const OntologyNamespaceRow = struct {
    ontology_id: []const u8,
    namespace: []const u8,
    label: ?[]const u8,
    parent_namespace: ?[]const u8,
    metadata_json: []const u8,
};

pub const OntologyDimensionRow = struct {
    ontology_id: []const u8,
    namespace: []const u8,
    dimension: []const u8,
    value_type: []const u8,
    is_multi: bool,
    hierarchy_kind: []const u8,
    metadata_json: []const u8,
};

pub const OntologyValueRow = struct {
    ontology_id: []const u8,
    namespace: []const u8,
    dimension: []const u8,
    value_id: i64,
    value: []const u8,
    parent_value_id: ?i64,
    label: ?[]const u8,
    metadata_json: []const u8,
};

const OntologyEntityTypeRow = struct {
    ontology_id: []const u8,
    entity_type: []const u8,
    label: ?[]const u8,
    metadata_json: []const u8,
};

const OntologyEdgeTypeRow = struct {
    ontology_id: []const u8,
    edge_type: []const u8,
    directed: bool,
    source_entity_type: ?[]const u8,
    target_entity_type: ?[]const u8,
    metadata_json: []const u8,
};

const OntologyEntityRow = struct {
    ontology_id: []const u8,
    entity_id: i64,
    entity_type: []const u8,
    label: []const u8,
    metadata_json: []const u8,
};

const OntologyRelationRow = struct {
    ontology_id: []const u8,
    relation_id: i64,
    edge_type: []const u8,
    source_entity_id: i64,
    target_entity_id: i64,
    metadata_json: []const u8,
};

const OntologyTripleRow = struct {
    ontology_id: []const u8,
    triple_index: i64,
    subject_kind: []const u8,
    subject: []const u8,
    predicate: []const u8,
    object_kind: []const u8,
    object_value: []const u8,
    object_datatype: ?[]const u8,
    object_language: ?[]const u8,
    source_line: []const u8,
    metadata_json: []const u8,
};

const CollectionOntologyRow = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    ontology_id: []const u8,
    role: []const u8,
};

const WorkspaceSettingsRow = struct {
    workspace_id: []const u8,
    default_ontology_id: ?[]const u8,
};

const AgentFactRow = struct {
    id: []const u8,
    schema_id: []const u8,
    content: []const u8,
    facets: []const u8,
    facets_json: []const u8,
    embedding: ?[]const u8,
    created_by: ?[]const u8,
    created_at: []const u8,
    created_at_unix: i64,
    updated_at: []const u8,
    updated_at_unix: i64,
    version: i64,
    supersedes: ?[]const u8,
    valid_from_unix: ?i64,
    valid_until_unix: ?i64,
    workspace_id: []const u8,
    source_ref: ?[]const u8,
    doc_id: ?i64,
};

const FacetTableRow = struct {
    table_id: i64,
    workspace_id: []const u8,
    collection_id: []const u8,
    schema_name: []const u8 = "public",
    table_name: []const u8,
    chunk_bits: i64,
    key_column: []const u8 = "doc_id",
    content_column: []const u8 = "content",
    metadata_column: []const u8 = "metadata_json",
    language: []const u8 = "english",
    bm25_enabled: bool = true,
};

const FacetDefinitionRow = struct {
    table_id: i64,
    facet_id: i64,
    facet_name: []const u8,
};

const AnswerArtifactRow = struct {
    artifact_id: []const u8,
    slug: []const u8,
    workspace_id: ?[]const u8,
    agent_id: ?[]const u8,
    scope: ?[]const u8,
    artifact_kind: []const u8,
    public_label_key: ?[]const u8,
    public_label: []const u8,
    lifecycle: []const u8,
    state: []const u8,
    current_version: i64,
    payload_json: []const u8,
    legacy_ref: ?[]const u8,
    created_at_unix: i64,
    updated_at_unix: i64,
};

const AnswerEventRow = struct {
    event_id: []const u8,
    artifact_id: []const u8,
    event_kind: []const u8,
    from_version: ?i64,
    to_version: ?i64,
    signal_json: []const u8,
    created_at_unix: i64,
};

const QualityConvergenceRunRow = struct {
    run_id: []const u8,
    workspace_id: []const u8,
    ontology_id: ?[]const u8,
    run_kind: []const u8,
    status: []const u8,
    canonical_layer: []const u8,
    input_fingerprint: []const u8,
    summary_json: []const u8,
    report_json: []const u8,
    created_at_unix: i64,
    updated_at_unix: i64,
};

const QualityRemediationActionRow = struct {
    action_id: []const u8,
    run_id: []const u8,
    workspace_id: []const u8,
    ontology_id: ?[]const u8,
    issue_type: []const u8,
    severity: []const u8,
    confidence: f64,
    reason: []const u8,
    schema_id: ?[]const u8,
    entity_type: ?[]const u8,
    projection_id: ?[]const u8,
    evidence_json: []const u8,
    mcp_tool: ?[]const u8,
    tool_args_json: []const u8,
    execution_mode: []const u8,
    idempotency_key: []const u8,
    status: []const u8,
    decision_actor: ?[]const u8,
    decision_note: ?[]const u8,
    result_json: []const u8,
    created_at_unix: i64,
    updated_at_unix: i64,
    decided_at_unix: ?i64,
    applied_at_unix: ?i64,
};

const DocumentRow = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    doc_id: i64,
    doc_nanoid: []const u8 = "",
    content: []const u8,
    language: ?[]const u8,
    source_ref: ?[]const u8,
    summary: ?[]const u8 = null,
    metadata_json: []const u8,
};

const ChunkRow = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    doc_id: i64,
    chunk_index: i64,
    content: []const u8,
    language: ?[]const u8,
    offset_start: ?i64,
    offset_end: ?i64,
    strategy: ?[]const u8 = null,
    token_count: ?i64 = null,
    parent_chunk_index: ?i64 = null,
    metadata_json: []const u8,
};

const VectorRow = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    doc_id: i64,
    chunk_index: ?i64 = null,
    dim: i64,
    embedding_blob: []const u8,
};

const FacetAssignmentRow = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    target_kind: []const u8,
    doc_id: i64,
    chunk_index: i64,
    ontology_id: []const u8,
    namespace: []const u8,
    dimension: []const u8,
    value: []const u8,
    value_id: ?i64,
    weight: f64,
    source: ?[]const u8,
};

const EntityRow = struct {
    workspace_id: []const u8,
    ontology_id: []const u8,
    entity_id: i64,
    external_id: ?[]const u8 = null,
    entity_type: []const u8,
    name: []const u8,
    confidence: f64,
    metadata_json: []const u8,
};

const EntityAliasRow = struct {
    workspace_id: []const u8,
    entity_id: i64,
    term: []const u8,
    confidence: f64,
};

const RelationRow = struct {
    workspace_id: []const u8,
    ontology_id: []const u8,
    relation_id: i64,
    external_id: ?[]const u8 = null,
    edge_type: []const u8,
    source_entity_id: i64,
    target_entity_id: i64,
    valid_from: ?[]const u8,
    valid_to: ?[]const u8,
    confidence: f64,
    metadata_json: []const u8,
};

const RelationPropertyRow = struct {
    workspace_id: []const u8,
    relation_id: i64,
    property_key: []const u8,
    value_type: []const u8,
    value_text: ?[]const u8,
    value_number: ?f64,
    value_integer: ?i64,
    ref_doc_id: ?i64,
    currency: ?[]const u8,
};

const EntityDocumentRow = struct {
    workspace_id: []const u8,
    entity_id: i64,
    collection_id: []const u8,
    doc_id: i64,
    role: ?[]const u8,
    confidence: f64,
};

const EntityChunkRow = struct {
    workspace_id: []const u8,
    entity_id: i64,
    collection_id: []const u8,
    doc_id: i64,
    chunk_index: i64,
    role: ?[]const u8,
    confidence: f64,
};

const GraphEntityRow = struct {
    entity_id: i64,
    workspace_id: []const u8,
    entity_type: []const u8,
    name: []const u8,
    confidence: f64,
    deprecated_at: ?i64,
    metadata_json: []const u8,
    created_at_unix: i64,
};

const GraphEntityAliasRow = struct {
    term: []const u8,
    entity_id: i64,
    confidence: f64,
};

const GraphRelationRow = struct {
    relation_id: i64,
    workspace_id: []const u8,
    relation_type: []const u8,
    source_id: i64,
    target_id: i64,
    valid_from_unix: ?i64,
    valid_to_unix: ?i64,
    confidence: f64,
    deprecated_at: ?i64,
    run_id: ?i64,
    patch_id: ?i64,
    metadata_json: []const u8,
    created_at_unix: i64,
};

const GraphRelationPropertyRow = struct {
    relation_id: i64,
    property_key: []const u8,
    value_type: []const u8,
    value_text: ?[]const u8,
    value_number: ?f64,
    value_integer: ?i64,
    ref_doc_id: ?i64,
    currency: ?[]const u8,
};

const GraphEntityDocumentRow = struct {
    entity_id: i64,
    doc_id: i64,
    table_id: i64,
    role: ?[]const u8,
    confidence: f64,
};

const GraphEntityChunkRow = struct {
    workspace_id: []const u8,
    entity_id: i64,
    collection_id: []const u8,
    doc_id: i64,
    chunk_index: i64,
    role: ?[]const u8,
    confidence: f64,
    metadata_json: []const u8,
};

const ExternalLinkRow = struct {
    workspace_id: []const u8,
    link_id: i64,
    source_collection_id: []const u8,
    source_doc_id: i64,
    source_chunk_index: ?i64,
    target_uri: []const u8,
    edge_type: []const u8,
    weight: f64,
    metadata_json: []const u8,
};

const DocumentLinkRow = struct {
    workspace_id: []const u8,
    link_id: i64,
    ontology_id: []const u8,
    edge_type: []const u8,
    source_collection_id: []const u8,
    source_doc_id: i64,
    source_chunk_index: ?i64,
    target_collection_id: []const u8,
    target_doc_id: i64,
    target_chunk_index: ?i64,
    weight: f64,
    metadata_json: []const u8,
    source: ?[]const u8,
};

// ---- Export ---------------------------------------------------------------

pub fn exportToJson(allocator: Allocator, db: Database, scope: Scope) ![]const u8 {
    return exportToJsonWithOptions(allocator, db, scope, .{});
}

pub fn exportToJsonWithOptions(allocator: Allocator, db: Database, scope: Scope, options: ExportOptions) ![]const u8 {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const workspace_id = switch (scope) {
        .workspace => |ws| ws,
        .taxonomies => |ws| ws,
        .collection => |coll| coll.workspace_id,
    };
    const collection_filter: ?[]const u8 = switch (scope) {
        .workspace => null,
        .taxonomies => null,
        .collection => |coll| coll.collection_id,
    };
    const taxonomies_only = switch (scope) {
        .taxonomies => true,
        else => false,
    };

    const bundle = Bundle{
        .exported_with = .{
            .mindbrain_version = schema_column_migrations.mindbrain_version,
            .bundle_schema_version = "2",
        },
        .scope = .{
            .kind = switch (scope) {
                .workspace => "workspace",
                .taxonomies => "taxonomies",
                .collection => "collection",
            },
            .workspace_id = workspace_id,
            .collection_id = collection_filter,
        },
        .workspaces = try selectWorkspaces(arena_allocator, db, workspace_id),
        .collections = if (taxonomies_only) &.{} else try selectCollections(arena_allocator, db, workspace_id, collection_filter),
        .ontologies = try selectOntologies(arena_allocator, db, workspace_id, collection_filter),
        .ontology_namespaces = try selectOntologyNamespaces(arena_allocator, db, workspace_id, collection_filter),
        .ontology_dimensions = try selectOntologyDimensions(arena_allocator, db, workspace_id, collection_filter),
        .ontology_values = try selectOntologyValues(arena_allocator, db, workspace_id, collection_filter),
        .ontology_entity_types = try selectOntologyEntityTypes(arena_allocator, db, workspace_id, collection_filter),
        .ontology_edge_types = try selectOntologyEdgeTypes(arena_allocator, db, workspace_id, collection_filter),
        .ontology_entities = try selectOntologyEntities(arena_allocator, db, workspace_id, collection_filter),
        .ontology_relations = try selectOntologyRelations(arena_allocator, db, workspace_id, collection_filter),
        .ontology_triples = try selectOntologyTriples(arena_allocator, db, workspace_id, collection_filter),
        .collection_ontologies = if (taxonomies_only) &.{} else try selectCollectionOntologies(arena_allocator, db, workspace_id, collection_filter),
        .workspace_settings = try selectWorkspaceSettings(arena_allocator, db, workspace_id),
        .agent_facts = if (taxonomies_only or collection_filter != null) &.{} else try selectAgentFacts(arena_allocator, db, workspace_id),
        .facet_tables = if (taxonomies_only) &.{} else try selectFacetTables(arena_allocator, db, workspace_id, collection_filter),
        .facet_definitions = if (taxonomies_only) &.{} else try selectFacetDefinitions(arena_allocator, db, workspace_id, collection_filter),
        .documents_raw = if (taxonomies_only) &.{} else try selectDocuments(arena_allocator, db, workspace_id, collection_filter),
        .chunks_raw = if (taxonomies_only) &.{} else try selectChunks(arena_allocator, db, workspace_id, collection_filter),
        .documents_raw_vector = if (taxonomies_only or !options.include_vectors) &.{} else try selectDocumentVectors(arena_allocator, db, workspace_id, collection_filter),
        .chunks_raw_vector = if (taxonomies_only or !options.include_vectors) &.{} else try selectChunkVectors(arena_allocator, db, workspace_id, collection_filter),
        .facet_assignments_raw = if (taxonomies_only) &.{} else try selectFacetAssignments(arena_allocator, db, workspace_id, collection_filter),
        .entities_raw = if (taxonomies_only) &.{} else try selectEntities(arena_allocator, db, workspace_id),
        .entity_aliases_raw = if (taxonomies_only) &.{} else try selectEntityAliases(arena_allocator, db, workspace_id),
        .relations_raw = if (taxonomies_only) &.{} else try selectRelations(arena_allocator, db, workspace_id),
        .relation_properties_raw = if (taxonomies_only) &.{} else try selectRelationProperties(arena_allocator, db, workspace_id),
        .entity_documents_raw = if (taxonomies_only) &.{} else try selectEntityDocuments(arena_allocator, db, workspace_id, collection_filter),
        .entity_chunks_raw = if (taxonomies_only) &.{} else try selectEntityChunks(arena_allocator, db, workspace_id, collection_filter),
        .graph_entity = if (taxonomies_only or collection_filter != null) &.{} else try selectGraphEntities(arena_allocator, db, workspace_id),
        .graph_entity_alias = if (taxonomies_only or collection_filter != null) &.{} else try selectGraphEntityAliases(arena_allocator, db, workspace_id),
        .graph_relation = if (taxonomies_only or collection_filter != null) &.{} else try selectGraphRelations(arena_allocator, db, workspace_id),
        .graph_relation_property = if (taxonomies_only or collection_filter != null) &.{} else try selectGraphRelationProperties(arena_allocator, db, workspace_id),
        .graph_entity_document = if (taxonomies_only or collection_filter != null) &.{} else try selectGraphEntityDocuments(arena_allocator, db, workspace_id),
        .graph_entity_chunk = if (taxonomies_only or collection_filter != null) &.{} else try selectGraphEntityChunks(arena_allocator, db, workspace_id),
        .document_links_raw = if (taxonomies_only) &.{} else try selectDocumentLinks(arena_allocator, db, workspace_id, collection_filter),
        .external_links_raw = if (taxonomies_only) &.{} else try selectExternalLinks(arena_allocator, db, workspace_id, collection_filter),
        .mindbrain_answer_artifacts = if (collection_filter == null) try selectAnswerArtifacts(arena_allocator, db, workspace_id) else &.{},
        .mindbrain_answer_events = if (collection_filter == null) try selectAnswerEvents(arena_allocator, db, workspace_id) else &.{},
        .quality_convergence_run = if (collection_filter == null and !taxonomies_only) try selectQualityConvergenceRuns(arena_allocator, db, workspace_id) else &.{},
        .quality_remediation_action = if (collection_filter == null and !taxonomies_only) try selectQualityRemediationActions(arena_allocator, db, workspace_id) else &.{},
    };

    return try std.json.Stringify.valueAlloc(allocator, bundle, .{ .whitespace = .indent_2 });
}

// ---- Import ---------------------------------------------------------------

pub fn importBundleJson(db: Database, allocator: Allocator, json_bytes: []const u8) !void {
    const parsed = try std.json.parseFromSlice(Bundle, allocator, json_bytes, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
    defer parsed.deinit();
    const bundle = parsed.value;

    try db.exec("BEGIN");
    errdefer db.exec("ROLLBACK") catch |rollback_err| {
        std.log.warn("collections import rollback failed: {s}", .{@errorName(rollback_err)});
    };

    for (bundle.workspaces) |row| {
        try collections_sqlite.ensureWorkspace(db, .{
            .workspace_id = row.workspace_id,
            .label = row.label,
            .description = row.description,
            .domain_profile = row.domain_profile,
        });
    }

    for (bundle.collections) |row| {
        try collections_sqlite.ensureCollection(db, .{
            .workspace_id = row.workspace_id,
            .collection_id = row.collection_id,
            .name = row.name,
            .key_kind = row.key_kind,
            .chunk_bits = std.math.cast(u8, row.chunk_bits) orelse return error.ValueOutOfRange,
            .default_language = row.default_language,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.ontologies) |row| {
        try collections_sqlite.ensureOntology(db, .{
            .ontology_id = row.ontology_id,
            .workspace_id = row.workspace_id,
            .name = row.name,
            .version = row.version,
            .frozen = row.frozen,
            .source_kind = row.source_kind,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.ontology_namespaces) |row| {
        try collections_sqlite.ensureNamespace(db, .{
            .ontology_id = row.ontology_id,
            .namespace = row.namespace,
            .label = row.label,
            .parent_namespace = row.parent_namespace,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.ontology_dimensions) |row| {
        try collections_sqlite.ensureDimension(db, .{
            .ontology_id = row.ontology_id,
            .namespace = row.namespace,
            .dimension = row.dimension,
            .value_type = row.value_type,
            .is_multi = row.is_multi,
            .hierarchy_kind = row.hierarchy_kind,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.ontology_values) |row| {
        try collections_sqlite.ensureValue(db, .{
            .ontology_id = row.ontology_id,
            .namespace = row.namespace,
            .dimension = row.dimension,
            .value_id = std.math.cast(u32, row.value_id) orelse return error.ValueOutOfRange,
            .value = row.value,
            .parent_value_id = if (row.parent_value_id) |v| std.math.cast(u32, v) orelse return error.ValueOutOfRange else null,
            .label = row.label,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.ontology_entity_types) |row| {
        try collections_sqlite.ensureEntityType(db, .{
            .ontology_id = row.ontology_id,
            .entity_type = row.entity_type,
            .label = row.label,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.ontology_edge_types) |row| {
        try collections_sqlite.ensureEdgeType(db, .{
            .ontology_id = row.ontology_id,
            .edge_type = row.edge_type,
            .directed = row.directed,
            .source_entity_type = row.source_entity_type,
            .target_entity_type = row.target_entity_type,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.ontology_entities) |row| {
        try collections_sqlite.upsertOntologyEntity(db, .{
            .ontology_id = row.ontology_id,
            .entity_id = @intCast(row.entity_id),
            .entity_type = row.entity_type,
            .name = row.label,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.ontology_relations) |row| {
        try collections_sqlite.upsertOntologyRelation(db, .{
            .ontology_id = row.ontology_id,
            .relation_id = @intCast(row.relation_id),
            .edge_type = row.edge_type,
            .source_entity_id = @intCast(row.source_entity_id),
            .target_entity_id = @intCast(row.target_entity_id),
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.ontology_triples) |row| {
        try collections_sqlite.upsertOntologyTriple(db, .{
            .ontology_id = row.ontology_id,
            .triple_index = @intCast(row.triple_index),
            .subject_kind = row.subject_kind,
            .subject = row.subject,
            .predicate = row.predicate,
            .object_kind = row.object_kind,
            .object_value = row.object_value,
            .object_datatype = row.object_datatype,
            .object_language = row.object_language,
            .source_line = row.source_line,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.workspace_settings) |row| {
        if (row.default_ontology_id) |oid| {
            try collections_sqlite.setDefaultOntology(db, row.workspace_id, oid);
        }
    }

    for (bundle.collection_ontologies) |row| {
        try collections_sqlite.attachOntologyToCollection(db, row.workspace_id, row.collection_id, row.ontology_id, row.role);
    }

    for (bundle.agent_facts) |row| {
        try upsertAgentFactBundleRow(db, row);
    }

    for (bundle.facet_tables) |row| {
        const table_id = std.math.cast(u64, row.table_id) orelse return error.ValueOutOfRange;
        const chunk_bits = std.math.cast(u8, row.chunk_bits) orelse return error.ValueOutOfRange;
        try search_sqlite.setupSearchTable(db, allocator, .{
            .table_id = table_id,
            .workspace_id = row.workspace_id,
            .schema_name = row.schema_name,
            .table_name = row.table_name,
            .key_column = row.key_column,
            .content_column = row.content_column,
            .metadata_column = row.metadata_column,
            .language = row.language,
            .populate = false,
        });
        try facet_sqlite.setupFacetTable(db, table_id, row.schema_name, row.table_name, chunk_bits, &.{});
        if (row.bm25_enabled) {
            try search_sqlite.bm25CreateSyncTrigger(db, .{
                .table_id = table_id,
                .id_column = row.key_column,
                .content_column = row.content_column,
                .language = row.language,
            });
        }
    }

    for (bundle.facet_definitions) |row| {
        const table_id = std.math.cast(u64, row.table_id) orelse return error.ValueOutOfRange;
        const facet_id = std.math.cast(u32, row.facet_id) orelse return error.ValueOutOfRange;
        try facet_sqlite.upsertFacetDefinition(db, table_id, facet_id, row.facet_name);
    }

    for (bundle.documents_raw) |row| {
        try collections_sqlite.upsertDocumentRaw(db, .{
            .workspace_id = row.workspace_id,
            .collection_id = row.collection_id,
            .doc_id = @intCast(row.doc_id),
            .doc_nanoid = row.doc_nanoid,
            .content = row.content,
            .language = row.language,
            .source_ref = row.source_ref,
            .summary = row.summary,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.chunks_raw) |row| {
        try collections_sqlite.upsertChunkRaw(db, .{
            .workspace_id = row.workspace_id,
            .collection_id = row.collection_id,
            .doc_id = @intCast(row.doc_id),
            .chunk_index = std.math.cast(u32, row.chunk_index) orelse return error.ValueOutOfRange,
            .content = row.content,
            .language = row.language,
            .offset_start = if (row.offset_start) |o| @intCast(o) else null,
            .offset_end = if (row.offset_end) |o| @intCast(o) else null,
            .strategy = row.strategy,
            .token_count = if (row.token_count) |tc| @intCast(tc) else null,
            .parent_chunk_index = if (row.parent_chunk_index) |pi| std.math.cast(u32, pi) orelse return error.ValueOutOfRange else null,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.documents_raw_vector) |row| {
        try collections_sqlite.upsertDocumentVector(db, .{
            .workspace_id = row.workspace_id,
            .collection_id = row.collection_id,
            .doc_id = @intCast(row.doc_id),
            .dim = std.math.cast(u32, row.dim) orelse return error.ValueOutOfRange,
            .embedding_blob = row.embedding_blob,
        });
    }

    for (bundle.chunks_raw_vector) |row| {
        try collections_sqlite.upsertChunkVector(db, .{
            .workspace_id = row.workspace_id,
            .collection_id = row.collection_id,
            .doc_id = @intCast(row.doc_id),
            .chunk_index = if (row.chunk_index) |v| std.math.cast(u32, v) orelse return error.ValueOutOfRange else return error.ValueOutOfRange,
            .dim = std.math.cast(u32, row.dim) orelse return error.ValueOutOfRange,
            .embedding_blob = row.embedding_blob,
        });
    }

    for (bundle.facet_assignments_raw) |row| {
        const target_kind: collections_sqlite.TargetKind = if (std.mem.eql(u8, row.target_kind, "doc")) .doc else .chunk;
        const chunk_index_opt: ?u32 = if (row.chunk_index < 0)
            null
        else
            (std.math.cast(u32, row.chunk_index) orelse return error.ValueOutOfRange);
        try collections_sqlite.upsertFacetAssignmentRaw(db, .{
            .workspace_id = row.workspace_id,
            .collection_id = row.collection_id,
            .target_kind = target_kind,
            .doc_id = @intCast(row.doc_id),
            .chunk_index = chunk_index_opt,
            .ontology_id = row.ontology_id,
            .namespace = row.namespace,
            .dimension = row.dimension,
            .value = row.value,
            .value_id = if (row.value_id) |v| std.math.cast(u32, v) orelse return error.ValueOutOfRange else null,
            .weight = row.weight,
            .source = row.source,
        });
    }

    var entity_id_map = std.StringHashMap(u64).init(allocator);
    defer freeStringHashMapKeys(allocator, &entity_id_map);
    var relation_id_map = std.StringHashMap(u64).init(allocator);
    defer freeStringHashMapKeys(allocator, &relation_id_map);
    var graph_entity_id_map = std.StringHashMap(u64).init(allocator);
    defer freeStringHashMapKeys(allocator, &graph_entity_id_map);
    var graph_relation_id_map = std.StringHashMap(u64).init(allocator);
    defer freeStringHashMapKeys(allocator, &graph_relation_id_map);

    for (bundle.entities_raw) |row| {
        const old_entity_id = std.math.cast(u64, row.entity_id) orelse return error.ValueOutOfRange;
        const external_id = row.external_id orelse try bundleEntityExternalId(allocator, row.workspace_id, old_entity_id);
        defer if (row.external_id == null) allocator.free(external_id);
        const new_entity_id = try collections_sqlite.upsertEntityRawAuto(db, .{
            .workspace_id = row.workspace_id,
            .ontology_id = row.ontology_id,
            .external_id = external_id,
            .entity_type = row.entity_type,
            .name = row.name,
            .confidence = row.confidence,
            .metadata_json = row.metadata_json,
        });
        const map_key = try rawIdMapKey(allocator, row.workspace_id, old_entity_id);
        try entity_id_map.put(map_key, new_entity_id);
    }

    for (bundle.entity_aliases_raw) |row| {
        const entity_id = try lookupRawId(&entity_id_map, row.workspace_id, row.entity_id);
        try collections_sqlite.upsertEntityAliasRaw(db, .{
            .workspace_id = row.workspace_id,
            .entity_id = entity_id,
            .term = row.term,
            .confidence = row.confidence,
        });
    }

    for (bundle.relations_raw) |row| {
        const old_relation_id = std.math.cast(u64, row.relation_id) orelse return error.ValueOutOfRange;
        const external_id = row.external_id orelse try bundleRelationExternalId(allocator, row.workspace_id, old_relation_id);
        defer if (row.external_id == null) allocator.free(external_id);
        const source_entity_id = try lookupRawId(&entity_id_map, row.workspace_id, row.source_entity_id);
        const target_entity_id = try lookupRawId(&entity_id_map, row.workspace_id, row.target_entity_id);
        const new_relation_id = try collections_sqlite.upsertRelationRawAuto(db, .{
            .workspace_id = row.workspace_id,
            .ontology_id = row.ontology_id,
            .external_id = external_id,
            .edge_type = row.edge_type,
            .source_entity_id = source_entity_id,
            .target_entity_id = target_entity_id,
            .valid_from = row.valid_from,
            .valid_to = row.valid_to,
            .confidence = row.confidence,
            .metadata_json = row.metadata_json,
        });
        const map_key = try rawIdMapKey(allocator, row.workspace_id, old_relation_id);
        try relation_id_map.put(map_key, new_relation_id);
    }

    for (bundle.relation_properties_raw) |row| {
        const relation_id = try lookupRawId(&relation_id_map, row.workspace_id, row.relation_id);
        try collections_sqlite.upsertRelationPropertyRaw(db, .{
            .workspace_id = row.workspace_id,
            .relation_id = relation_id,
            .property_key = row.property_key,
            .value_type = parseRelationPropertyValueType(row.value_type) orelse return error.ValueOutOfRange,
            .value_text = row.value_text,
            .value_number = row.value_number,
            .value_integer = row.value_integer,
            .ref_doc_id = if (row.ref_doc_id) |v| @intCast(v) else null,
            .currency = row.currency,
        });
    }

    for (bundle.entity_documents_raw) |row| {
        const entity_id = try lookupRawId(&entity_id_map, row.workspace_id, row.entity_id);
        try collections_sqlite.linkEntityDocumentRaw(db, .{
            .workspace_id = row.workspace_id,
            .entity_id = entity_id,
            .collection_id = row.collection_id,
            .doc_id = @intCast(row.doc_id),
            .role = row.role,
            .confidence = row.confidence,
        });
    }

    for (bundle.entity_chunks_raw) |row| {
        const entity_id = try lookupRawId(&entity_id_map, row.workspace_id, row.entity_id);
        try collections_sqlite.linkEntityChunkRaw(db, .{
            .workspace_id = row.workspace_id,
            .entity_id = entity_id,
            .collection_id = row.collection_id,
            .doc_id = @intCast(row.doc_id),
            .chunk_index = std.math.cast(u32, row.chunk_index) orelse return error.ValueOutOfRange,
            .role = row.role,
            .confidence = row.confidence,
        });
    }

    for (bundle.graph_entity) |row| {
        const old_entity_id = std.math.cast(u64, row.entity_id) orelse return error.ValueOutOfRange;
        const new_entity_id = try upsertGraphEntityBundleRow(db, row);
        const map_key = try rawIdMapKey(allocator, row.workspace_id, old_entity_id);
        try graph_entity_id_map.put(map_key, new_entity_id);
    }

    for (bundle.graph_entity_alias) |row| {
        const entity_id = try lookupRawId(&graph_entity_id_map, bundle.scope.workspace_id, row.entity_id);
        try upsertGraphEntityAliasBundleRow(db, row, entity_id);
    }

    for (bundle.graph_relation) |row| {
        const old_relation_id = std.math.cast(u64, row.relation_id) orelse return error.ValueOutOfRange;
        const source_id = try lookupRawId(&graph_entity_id_map, row.workspace_id, row.source_id);
        const target_id = try lookupRawId(&graph_entity_id_map, row.workspace_id, row.target_id);
        const new_relation_id = try upsertGraphRelationBundleRow(db, row, source_id, target_id);
        const map_key = try rawIdMapKey(allocator, row.workspace_id, old_relation_id);
        try graph_relation_id_map.put(map_key, new_relation_id);
    }

    for (bundle.graph_relation_property) |row| {
        const relation_id = try lookupRawId(&graph_relation_id_map, bundle.scope.workspace_id, row.relation_id);
        try upsertGraphRelationPropertyBundleRow(db, row, relation_id);
    }

    for (bundle.graph_entity_document) |row| {
        const entity_id = try lookupRawId(&graph_entity_id_map, bundle.scope.workspace_id, row.entity_id);
        try upsertGraphEntityDocumentBundleRow(db, row, entity_id);
    }

    for (bundle.graph_entity_chunk) |row| {
        const entity_id = try lookupRawId(&graph_entity_id_map, row.workspace_id, row.entity_id);
        try upsertGraphEntityChunkBundleRow(db, row, entity_id);
    }

    for (bundle.document_links_raw) |row| {
        try collections_sqlite.upsertDocumentLinkRaw(db, .{
            .workspace_id = row.workspace_id,
            .link_id = @intCast(row.link_id),
            .ontology_id = row.ontology_id,
            .edge_type = row.edge_type,
            .source_collection_id = row.source_collection_id,
            .source_doc_id = @intCast(row.source_doc_id),
            .source_chunk_index = if (row.source_chunk_index) |v| std.math.cast(u32, v) orelse return error.ValueOutOfRange else null,
            .target_collection_id = row.target_collection_id,
            .target_doc_id = @intCast(row.target_doc_id),
            .target_chunk_index = if (row.target_chunk_index) |v| std.math.cast(u32, v) orelse return error.ValueOutOfRange else null,
            .weight = row.weight,
            .source = row.source,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.external_links_raw) |row| {
        try collections_sqlite.upsertExternalLinkRaw(db, .{
            .workspace_id = row.workspace_id,
            .link_id = @intCast(row.link_id),
            .source_collection_id = row.source_collection_id,
            .source_doc_id = @intCast(row.source_doc_id),
            .source_chunk_index = if (row.source_chunk_index) |v| std.math.cast(u32, v) orelse return error.ValueOutOfRange else null,
            .target_uri = row.target_uri,
            .edge_type = row.edge_type,
            .weight = row.weight,
            .metadata_json = row.metadata_json,
        });
    }

    for (bundle.mindbrain_answer_artifacts) |row| {
        try upsertAnswerArtifact(db, row);
    }

    for (bundle.mindbrain_answer_events) |row| {
        try upsertAnswerEvent(db, row);
    }

    for (bundle.quality_convergence_run) |row| {
        try upsertQualityConvergenceRun(db, row);
    }

    for (bundle.quality_remediation_action) |row| {
        try upsertQualityRemediationAction(db, row);
    }

    try db.exec("COMMIT");
}

fn upsertQualityConvergenceRun(db: Database, row: QualityConvergenceRunRow) !void {
    const stmt = try facet_sqlite.prepare(db,
        \\INSERT INTO quality_convergence_run(
        \\  run_id, workspace_id, ontology_id, run_kind, status, canonical_layer,
        \\  input_fingerprint, summary_json, report_json, created_at_unix, updated_at_unix
        \\) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
        \\ON CONFLICT(run_id) DO UPDATE SET
        \\  workspace_id = excluded.workspace_id,
        \\  ontology_id = excluded.ontology_id,
        \\  run_kind = excluded.run_kind,
        \\  status = excluded.status,
        \\  canonical_layer = excluded.canonical_layer,
        \\  input_fingerprint = excluded.input_fingerprint,
        \\  summary_json = excluded.summary_json,
        \\  report_json = excluded.report_json,
        \\  created_at_unix = excluded.created_at_unix,
        \\  updated_at_unix = excluded.updated_at_unix
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, row.run_id);
    try facet_sqlite.bindText(stmt, 2, row.workspace_id);
    try bindMaybeText(stmt, 3, row.ontology_id);
    try facet_sqlite.bindText(stmt, 4, row.run_kind);
    try facet_sqlite.bindText(stmt, 5, row.status);
    try facet_sqlite.bindText(stmt, 6, row.canonical_layer);
    try facet_sqlite.bindText(stmt, 7, row.input_fingerprint);
    try facet_sqlite.bindText(stmt, 8, row.summary_json);
    try facet_sqlite.bindText(stmt, 9, row.report_json);
    try facet_sqlite.bindInt64(stmt, 10, row.created_at_unix);
    try facet_sqlite.bindInt64(stmt, 11, row.updated_at_unix);
    try execPreparedDone(stmt);
}

fn upsertQualityRemediationAction(db: Database, row: QualityRemediationActionRow) !void {
    const stmt = try facet_sqlite.prepare(db,
        \\INSERT INTO quality_remediation_action(
        \\  action_id, run_id, workspace_id, ontology_id, issue_type, severity,
        \\  confidence, reason, schema_id, entity_type, projection_id, evidence_json,
        \\  mcp_tool, tool_args_json, execution_mode, idempotency_key, status,
        \\  decision_actor, decision_note, result_json, created_at_unix,
        \\  updated_at_unix, decided_at_unix, applied_at_unix
        \\) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24)
        \\ON CONFLICT(action_id) DO UPDATE SET
        \\  run_id = excluded.run_id,
        \\  workspace_id = excluded.workspace_id,
        \\  ontology_id = excluded.ontology_id,
        \\  issue_type = excluded.issue_type,
        \\  severity = excluded.severity,
        \\  confidence = excluded.confidence,
        \\  reason = excluded.reason,
        \\  schema_id = excluded.schema_id,
        \\  entity_type = excluded.entity_type,
        \\  projection_id = excluded.projection_id,
        \\  evidence_json = excluded.evidence_json,
        \\  mcp_tool = excluded.mcp_tool,
        \\  tool_args_json = excluded.tool_args_json,
        \\  execution_mode = excluded.execution_mode,
        \\  idempotency_key = excluded.idempotency_key,
        \\  status = excluded.status,
        \\  decision_actor = excluded.decision_actor,
        \\  decision_note = excluded.decision_note,
        \\  result_json = excluded.result_json,
        \\  created_at_unix = excluded.created_at_unix,
        \\  updated_at_unix = excluded.updated_at_unix,
        \\  decided_at_unix = excluded.decided_at_unix,
        \\  applied_at_unix = excluded.applied_at_unix
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, row.action_id);
    try facet_sqlite.bindText(stmt, 2, row.run_id);
    try facet_sqlite.bindText(stmt, 3, row.workspace_id);
    try bindMaybeText(stmt, 4, row.ontology_id);
    try facet_sqlite.bindText(stmt, 5, row.issue_type);
    try facet_sqlite.bindText(stmt, 6, row.severity);
    try bindDouble(stmt, 7, row.confidence);
    try facet_sqlite.bindText(stmt, 8, row.reason);
    try bindMaybeText(stmt, 9, row.schema_id);
    try bindMaybeText(stmt, 10, row.entity_type);
    try bindMaybeText(stmt, 11, row.projection_id);
    try facet_sqlite.bindText(stmt, 12, row.evidence_json);
    try bindMaybeText(stmt, 13, row.mcp_tool);
    try facet_sqlite.bindText(stmt, 14, row.tool_args_json);
    try facet_sqlite.bindText(stmt, 15, row.execution_mode);
    try facet_sqlite.bindText(stmt, 16, row.idempotency_key);
    try facet_sqlite.bindText(stmt, 17, row.status);
    try bindMaybeText(stmt, 18, row.decision_actor);
    try bindMaybeText(stmt, 19, row.decision_note);
    try facet_sqlite.bindText(stmt, 20, row.result_json);
    try facet_sqlite.bindInt64(stmt, 21, row.created_at_unix);
    try facet_sqlite.bindInt64(stmt, 22, row.updated_at_unix);
    try bindMaybeInt(stmt, 23, row.decided_at_unix);
    try bindMaybeInt(stmt, 24, row.applied_at_unix);
    try execPreparedDone(stmt);
}

fn upsertAnswerArtifact(db: Database, row: AnswerArtifactRow) !void {
    const stmt = try facet_sqlite.prepare(db,
        \\INSERT INTO mindbrain_answer_artifacts(
        \\  artifact_id, slug, workspace_id, agent_id, scope, artifact_kind,
        \\  public_label_key, public_label, lifecycle, state, current_version,
        \\  payload_json, legacy_ref, created_at_unix, updated_at_unix
        \\) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
        \\ON CONFLICT(artifact_id) DO UPDATE SET
        \\  slug = excluded.slug,
        \\  workspace_id = excluded.workspace_id,
        \\  agent_id = excluded.agent_id,
        \\  scope = excluded.scope,
        \\  artifact_kind = excluded.artifact_kind,
        \\  public_label_key = excluded.public_label_key,
        \\  public_label = excluded.public_label,
        \\  lifecycle = excluded.lifecycle,
        \\  state = excluded.state,
        \\  current_version = excluded.current_version,
        \\  payload_json = excluded.payload_json,
        \\  legacy_ref = excluded.legacy_ref,
        \\  created_at_unix = excluded.created_at_unix,
        \\  updated_at_unix = excluded.updated_at_unix
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, row.artifact_id);
    try facet_sqlite.bindText(stmt, 2, row.slug);
    try bindMaybeText(stmt, 3, row.workspace_id);
    try bindMaybeText(stmt, 4, row.agent_id);
    try bindMaybeText(stmt, 5, row.scope);
    try facet_sqlite.bindText(stmt, 6, row.artifact_kind);
    try bindMaybeText(stmt, 7, row.public_label_key);
    try facet_sqlite.bindText(stmt, 8, row.public_label);
    try facet_sqlite.bindText(stmt, 9, row.lifecycle);
    try facet_sqlite.bindText(stmt, 10, row.state);
    try facet_sqlite.bindInt64(stmt, 11, row.current_version);
    try facet_sqlite.bindText(stmt, 12, row.payload_json);
    try bindMaybeText(stmt, 13, row.legacy_ref);
    try facet_sqlite.bindInt64(stmt, 14, row.created_at_unix);
    try facet_sqlite.bindInt64(stmt, 15, row.updated_at_unix);
    try facet_sqlite.stepDone(stmt);
}

fn upsertAnswerEvent(db: Database, row: AnswerEventRow) !void {
    const stmt = try facet_sqlite.prepare(db,
        \\INSERT INTO mindbrain_answer_events(
        \\  event_id, artifact_id, event_kind, from_version, to_version, signal_json, created_at_unix
        \\) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        \\ON CONFLICT(event_id) DO UPDATE SET
        \\  artifact_id = excluded.artifact_id,
        \\  event_kind = excluded.event_kind,
        \\  from_version = excluded.from_version,
        \\  to_version = excluded.to_version,
        \\  signal_json = excluded.signal_json,
        \\  created_at_unix = excluded.created_at_unix
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, row.event_id);
    try facet_sqlite.bindText(stmt, 2, row.artifact_id);
    try facet_sqlite.bindText(stmt, 3, row.event_kind);
    try bindMaybeInt(stmt, 4, row.from_version);
    try bindMaybeInt(stmt, 5, row.to_version);
    try facet_sqlite.bindText(stmt, 6, row.signal_json);
    try facet_sqlite.bindInt64(stmt, 7, row.created_at_unix);
    try facet_sqlite.stepDone(stmt);
}

fn rawIdMapKey(allocator: Allocator, workspace_id: []const u8, id: u64) ![]const u8 {
    return try std.fmt.allocPrint(allocator, "{s}:{d}", .{ workspace_id, id });
}

fn lookupRawId(map: *std.StringHashMap(u64), workspace_id: []const u8, old_id: i64) !u64 {
    const id = std.math.cast(u64, old_id) orelse return error.ValueOutOfRange;
    var buf: [256]u8 = undefined;
    const key = std.fmt.bufPrint(&buf, "{s}:{d}", .{ workspace_id, id }) catch return error.ValueOutOfRange;
    return map.get(key) orelse error.MissingRow;
}

fn bundleEntityExternalId(allocator: Allocator, workspace_id: []const u8, entity_id: u64) ![]const u8 {
    return try std.fmt.allocPrint(allocator, "legacy:entity:{s}:{d}", .{ workspace_id, entity_id });
}

fn bundleRelationExternalId(allocator: Allocator, workspace_id: []const u8, relation_id: u64) ![]const u8 {
    return try std.fmt.allocPrint(allocator, "legacy:relation:{s}:{d}", .{ workspace_id, relation_id });
}

fn freeStringHashMapKeys(allocator: Allocator, map: *std.StringHashMap(u64)) void {
    var it = map.keyIterator();
    while (it.next()) |key| allocator.free(key.*);
    map.deinit();
}

fn upsertAgentFactBundleRow(db: Database, row: AgentFactRow) !void {
    const stmt = try facet_sqlite.prepare(db,
        \\INSERT INTO agent_facts(
        \\  id, schema_id, content, facets, facets_json, embedding, created_by,
        \\  created_at, created_at_unix, updated_at, updated_at_unix, version,
        \\  supersedes, valid_from_unix, valid_until_unix, workspace_id, source_ref, doc_id
        \\) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
        \\ON CONFLICT(id) DO UPDATE SET
        \\  schema_id = excluded.schema_id,
        \\  content = excluded.content,
        \\  facets = excluded.facets,
        \\  facets_json = excluded.facets_json,
        \\  embedding = excluded.embedding,
        \\  created_by = excluded.created_by,
        \\  created_at = excluded.created_at,
        \\  created_at_unix = excluded.created_at_unix,
        \\  updated_at = excluded.updated_at,
        \\  updated_at_unix = excluded.updated_at_unix,
        \\  version = excluded.version,
        \\  supersedes = excluded.supersedes,
        \\  valid_from_unix = excluded.valid_from_unix,
        \\  valid_until_unix = excluded.valid_until_unix,
        \\  workspace_id = excluded.workspace_id,
        \\  source_ref = excluded.source_ref,
        \\  doc_id = excluded.doc_id
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, row.id);
    try facet_sqlite.bindText(stmt, 2, row.schema_id);
    try facet_sqlite.bindText(stmt, 3, row.content);
    try facet_sqlite.bindText(stmt, 4, row.facets);
    try facet_sqlite.bindText(stmt, 5, row.facets_json);
    try bindMaybeText(stmt, 6, row.embedding);
    try bindMaybeText(stmt, 7, row.created_by);
    try facet_sqlite.bindText(stmt, 8, row.created_at);
    try facet_sqlite.bindInt64(stmt, 9, row.created_at_unix);
    try facet_sqlite.bindText(stmt, 10, row.updated_at);
    try facet_sqlite.bindInt64(stmt, 11, row.updated_at_unix);
    try facet_sqlite.bindInt64(stmt, 12, row.version);
    try bindMaybeText(stmt, 13, row.supersedes);
    try bindMaybeInt(stmt, 14, row.valid_from_unix);
    try bindMaybeInt(stmt, 15, row.valid_until_unix);
    try facet_sqlite.bindText(stmt, 16, row.workspace_id);
    try bindMaybeText(stmt, 17, row.source_ref);
    try bindMaybeInt(stmt, 18, row.doc_id);
    try facet_sqlite.stepDone(stmt);
}

fn upsertGraphEntityBundleRow(db: Database, row: GraphEntityRow) !u64 {
    const stmt = try facet_sqlite.prepare(db,
        \\INSERT INTO graph_entity(
        \\  workspace_id, entity_type, name, confidence, deprecated_at, metadata_json, created_at_unix
        \\) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        \\ON CONFLICT(workspace_id, entity_type, name) DO UPDATE SET
        \\  confidence = excluded.confidence,
        \\  deprecated_at = excluded.deprecated_at,
        \\  metadata_json = excluded.metadata_json,
        \\  created_at_unix = excluded.created_at_unix
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, row.workspace_id);
    try facet_sqlite.bindText(stmt, 2, row.entity_type);
    try facet_sqlite.bindText(stmt, 3, row.name);
    try bindDouble(stmt, 4, row.confidence);
    try bindMaybeInt(stmt, 5, row.deprecated_at);
    try facet_sqlite.bindText(stmt, 6, row.metadata_json);
    try facet_sqlite.bindInt64(stmt, 7, row.created_at_unix);
    try facet_sqlite.stepDone(stmt);

    return try lookupGraphEntityId(db, row.workspace_id, row.entity_type, row.name);
}

fn lookupGraphEntityId(db: Database, workspace_id: []const u8, entity_type: []const u8, name: []const u8) !u64 {
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT entity_id FROM graph_entity
        \\WHERE workspace_id = ?1 AND entity_type = ?2 AND name = ?3
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, entity_type);
    try facet_sqlite.bindText(stmt, 3, name);
    const status = c.sqlite3_step(stmt);
    if (status == c.SQLITE_ROW) return std.math.cast(u64, c.sqlite3_column_int64(stmt, 0)) orelse error.ValueOutOfRange;
    if (status == c.SQLITE_DONE) return error.MissingRow;
    return error.StepFailed;
}

fn upsertGraphEntityAliasBundleRow(db: Database, row: GraphEntityAliasRow, entity_id: u64) !void {
    const stmt = try facet_sqlite.prepare(db,
        \\INSERT INTO graph_entity_alias(term, entity_id, confidence)
        \\VALUES (?1, ?2, ?3)
        \\ON CONFLICT(term, entity_id) DO UPDATE SET confidence = excluded.confidence
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, row.term);
    try facet_sqlite.bindInt64(stmt, 2, @as(i64, @intCast(entity_id)));
    try bindDouble(stmt, 3, row.confidence);
    try facet_sqlite.stepDone(stmt);
}

fn upsertGraphRelationBundleRow(db: Database, row: GraphRelationRow, source_id: u64, target_id: u64) !u64 {
    if (lookupGraphRelationId(db, row.workspace_id, row.relation_type, source_id, target_id)) |existing_id| {
        const update_stmt = try facet_sqlite.prepare(db,
            \\UPDATE graph_relation SET
            \\  valid_from_unix = ?1,
            \\  valid_to_unix = ?2,
            \\  confidence = ?3,
            \\  deprecated_at = ?4,
            \\  run_id = ?5,
            \\  patch_id = ?6,
            \\  metadata_json = ?7,
            \\  created_at_unix = ?8
            \\WHERE relation_id = ?9
        );
        defer facet_sqlite.finalize(update_stmt);
        try bindMaybeInt(update_stmt, 1, row.valid_from_unix);
        try bindMaybeInt(update_stmt, 2, row.valid_to_unix);
        try bindDouble(update_stmt, 3, row.confidence);
        try bindMaybeInt(update_stmt, 4, row.deprecated_at);
        try bindMaybeInt(update_stmt, 5, row.run_id);
        try bindMaybeInt(update_stmt, 6, row.patch_id);
        try facet_sqlite.bindText(update_stmt, 7, row.metadata_json);
        try facet_sqlite.bindInt64(update_stmt, 8, row.created_at_unix);
        try facet_sqlite.bindInt64(update_stmt, 9, @as(i64, @intCast(existing_id)));
        try facet_sqlite.stepDone(update_stmt);
        return existing_id;
    } else |err| switch (err) {
        error.MissingRow => {},
        else => return err,
    }

    const stmt = try facet_sqlite.prepare(db,
        \\INSERT INTO graph_relation(
        \\  workspace_id, relation_type, source_id, target_id, valid_from_unix, valid_to_unix,
        \\  confidence, deprecated_at, run_id, patch_id, metadata_json, created_at_unix
        \\) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, row.workspace_id);
    try facet_sqlite.bindText(stmt, 2, row.relation_type);
    try facet_sqlite.bindInt64(stmt, 3, @as(i64, @intCast(source_id)));
    try facet_sqlite.bindInt64(stmt, 4, @as(i64, @intCast(target_id)));
    try bindMaybeInt(stmt, 5, row.valid_from_unix);
    try bindMaybeInt(stmt, 6, row.valid_to_unix);
    try bindDouble(stmt, 7, row.confidence);
    try bindMaybeInt(stmt, 8, row.deprecated_at);
    try bindMaybeInt(stmt, 9, row.run_id);
    try bindMaybeInt(stmt, 10, row.patch_id);
    try facet_sqlite.bindText(stmt, 11, row.metadata_json);
    try facet_sqlite.bindInt64(stmt, 12, row.created_at_unix);
    try facet_sqlite.stepDone(stmt);

    return try lookupGraphRelationId(db, row.workspace_id, row.relation_type, source_id, target_id);
}

fn lookupGraphRelationId(db: Database, workspace_id: []const u8, relation_type: []const u8, source_id: u64, target_id: u64) !u64 {
    const stmt = try facet_sqlite.prepare(db,
        \\SELECT relation_id FROM graph_relation
        \\WHERE workspace_id = ?1 AND relation_type = ?2 AND source_id = ?3 AND target_id = ?4
        \\ORDER BY relation_id DESC LIMIT 1
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, relation_type);
    try facet_sqlite.bindInt64(stmt, 3, @as(i64, @intCast(source_id)));
    try facet_sqlite.bindInt64(stmt, 4, @as(i64, @intCast(target_id)));
    const status = c.sqlite3_step(stmt);
    if (status == c.SQLITE_ROW) return std.math.cast(u64, c.sqlite3_column_int64(stmt, 0)) orelse error.ValueOutOfRange;
    if (status == c.SQLITE_DONE) return error.MissingRow;
    return error.StepFailed;
}

fn upsertGraphRelationPropertyBundleRow(db: Database, row: GraphRelationPropertyRow, relation_id: u64) !void {
    const stmt = try facet_sqlite.prepare(db,
        \\INSERT INTO graph_relation_property(
        \\  relation_id, property_key, value_type, value_text, value_number, value_integer, ref_doc_id, currency
        \\) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        \\ON CONFLICT(relation_id, property_key) DO UPDATE SET
        \\  value_type = excluded.value_type,
        \\  value_text = excluded.value_text,
        \\  value_number = excluded.value_number,
        \\  value_integer = excluded.value_integer,
        \\  ref_doc_id = excluded.ref_doc_id,
        \\  currency = excluded.currency
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindInt64(stmt, 1, @as(i64, @intCast(relation_id)));
    try facet_sqlite.bindText(stmt, 2, row.property_key);
    try facet_sqlite.bindText(stmt, 3, row.value_type);
    try bindMaybeText(stmt, 4, row.value_text);
    try bindMaybeDouble(stmt, 5, row.value_number);
    try bindMaybeInt(stmt, 6, row.value_integer);
    try bindMaybeInt(stmt, 7, row.ref_doc_id);
    try bindMaybeText(stmt, 8, row.currency);
    try facet_sqlite.stepDone(stmt);
}

fn upsertGraphEntityDocumentBundleRow(db: Database, row: GraphEntityDocumentRow, entity_id: u64) !void {
    const stmt = try facet_sqlite.prepare(db,
        \\INSERT INTO graph_entity_document(entity_id, doc_id, table_id, role, confidence)
        \\VALUES (?1, ?2, ?3, ?4, ?5)
        \\ON CONFLICT(entity_id, doc_id, table_id) DO UPDATE SET
        \\  role = excluded.role,
        \\  confidence = excluded.confidence
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindInt64(stmt, 1, @as(i64, @intCast(entity_id)));
    try facet_sqlite.bindInt64(stmt, 2, row.doc_id);
    try facet_sqlite.bindInt64(stmt, 3, row.table_id);
    try bindMaybeText(stmt, 4, row.role);
    try bindDouble(stmt, 5, row.confidence);
    try facet_sqlite.stepDone(stmt);
}

fn upsertGraphEntityChunkBundleRow(db: Database, row: GraphEntityChunkRow, entity_id: u64) !void {
    const stmt = try facet_sqlite.prepare(db,
        \\INSERT INTO graph_entity_chunk(
        \\  entity_id, workspace_id, collection_id, doc_id, chunk_index, role, confidence, metadata_json
        \\) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        \\ON CONFLICT(entity_id, workspace_id, collection_id, doc_id, chunk_index) DO UPDATE SET
        \\  role = excluded.role,
        \\  confidence = excluded.confidence,
        \\  metadata_json = excluded.metadata_json
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindInt64(stmt, 1, @as(i64, @intCast(entity_id)));
    try facet_sqlite.bindText(stmt, 2, row.workspace_id);
    try facet_sqlite.bindText(stmt, 3, row.collection_id);
    try facet_sqlite.bindInt64(stmt, 4, row.doc_id);
    try facet_sqlite.bindInt64(stmt, 5, row.chunk_index);
    try bindMaybeText(stmt, 6, row.role);
    try bindDouble(stmt, 7, row.confidence);
    try facet_sqlite.bindText(stmt, 8, row.metadata_json);
    try facet_sqlite.stepDone(stmt);
}

pub fn summarizeBundleJson(allocator: Allocator, json_bytes: []const u8) !BundleSummary {
    const parsed = try std.json.parseFromSlice(Bundle, allocator, json_bytes, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
    defer parsed.deinit();
    const bundle = parsed.value;

    return .{
        .kind = try allocator.dupe(u8, bundle.kind),
        .schema_version = try allocator.dupe(u8, bundle.schema_version),
        .scope_kind = try allocator.dupe(u8, bundle.scope.kind),
        .workspace_id = try allocator.dupe(u8, bundle.scope.workspace_id),
        .collection_id = if (bundle.scope.collection_id) |value| try allocator.dupe(u8, value) else null,
        .workspace_count = bundle.workspaces.len,
        .collection_count = bundle.collections.len,
        .ontology_count = bundle.ontologies.len,
        .ontology_value_count = bundle.ontology_values.len,
        .document_count = bundle.documents_raw.len,
        .chunk_count = bundle.chunks_raw.len,
        .facet_table_count = bundle.facet_tables.len,
        .facet_table_id = if (bundle.facet_tables.len == 1) std.math.cast(u64, bundle.facet_tables[0].table_id) orelse return error.ValueOutOfRange else null,
        .facet_collection_id = if (bundle.facet_tables.len == 1) try allocator.dupe(u8, bundle.facet_tables[0].collection_id) else null,
        .relation_property_count = bundle.relation_properties_raw.len,
        .answer_artifact_count = bundle.mindbrain_answer_artifacts.len,
        .answer_event_count = bundle.mindbrain_answer_events.len,
        .exported_mindbrain_version = if (bundle.exported_with) |meta| try allocator.dupe(u8, meta.mindbrain_version) else null,
    };
}

pub const SchemaColumnsMissingError = error{
    SchemaColumnsMissing,
};

/// Returns missing manifest columns still absent after `applyStandaloneSchema`.
pub fn findMissingSchemaColumns(allocator: Allocator, db: Database) ![]schema_column_migrations.MissingColumn {
    return schema_column_migrations.findMissingColumns(allocator, db);
}

/// Fails with `SchemaColumnsMissing` when required columns are still absent.
pub fn ensureBundleSchemaReady(allocator: Allocator, db: Database) !void {
    const missing = try findMissingSchemaColumns(allocator, db);
    defer {
        for (missing) |item| {
            allocator.free(item.table);
            allocator.free(item.column);
        }
        allocator.free(missing);
    }
    if (missing.len == 0) return;

    std.debug.print("backup-load schema preflight failed: missing columns after upgrade:\n", .{});
    for (missing) |item| {
        std.debug.print("  - {s}.{s}\n", .{ item.table, item.column });
    }
    std.debug.print("Run mindbrain schema apply / gcp brain upgrade --db <path> on the target database first.\n", .{});
    return SchemaColumnsMissingError.SchemaColumnsMissing;
}

// ---- Internal selectors ---------------------------------------------------

fn selectWorkspaces(arena: Allocator, db: Database, workspace_id: []const u8) ![]WorkspaceRow {
    const sql =
        \\SELECT workspace_id, label, description, domain_profile
        \\FROM workspaces WHERE workspace_id = ?1
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(WorkspaceRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .label = try maybeColText(arena, stmt, 1),
            .description = try maybeColText(arena, stmt, 2),
            .domain_profile = try maybeColText(arena, stmt, 3),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectCollections(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]CollectionRow {
    const sql_all =
        \\SELECT collection_id, workspace_id, name, key_kind, chunk_bits, default_language, metadata_json
        \\FROM collections WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT collection_id, workspace_id, name, key_kind, chunk_bits, default_language, metadata_json
        \\FROM collections WHERE workspace_id = ?1 AND collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(CollectionRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .name = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .key_kind = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .chunk_bits = c.sqlite3_column_int64(stmt, 4),
            .default_language = try facet_sqlite.dupeColumnText(arena, stmt, 5),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 6),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectOntologies(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]OntologyRow {
    const sql_all =
        \\SELECT ontology_id, workspace_id, name, version, frozen, source_kind, metadata_json
        \\FROM ontologies WHERE workspace_id = ?1 OR workspace_id IS NULL
    ;
    const sql_attached =
        \\SELECT o.ontology_id, o.workspace_id, o.name, o.version, o.frozen, o.source_kind, o.metadata_json
        \\FROM ontologies o
        \\JOIN collection_ontologies co ON co.ontology_id = o.ontology_id
        \\WHERE co.workspace_id = ?1 AND co.collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_attached);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(OntologyRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .workspace_id = try maybeColText(arena, stmt, 1),
            .name = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .version = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .frozen = c.sqlite3_column_int64(stmt, 4) != 0,
            .source_kind = try facet_sqlite.dupeColumnText(arena, stmt, 5),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 6),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectOntologyNamespaces(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]OntologyNamespaceRow {
    const sql_all =
        \\SELECT n.ontology_id, n.namespace, n.label, n.parent_namespace, n.metadata_json
        \\FROM ontology_namespaces n
        \\JOIN ontologies o ON o.ontology_id = n.ontology_id
        \\WHERE o.workspace_id = ?1 OR o.workspace_id IS NULL
        \\ORDER BY n.ontology_id, n.namespace
    ;
    const sql_attached =
        \\SELECT n.ontology_id, n.namespace, n.label, n.parent_namespace, n.metadata_json
        \\FROM ontology_namespaces n
        \\JOIN collection_ontologies co ON co.ontology_id = n.ontology_id
        \\WHERE co.workspace_id = ?1 AND co.collection_id = ?2
        \\ORDER BY n.ontology_id, n.namespace
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_attached);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(OntologyNamespaceRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .namespace = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .label = try maybeColText(arena, stmt, 2),
            .parent_namespace = try maybeColText(arena, stmt, 3),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 4),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectOntologyDimensions(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]OntologyDimensionRow {
    const sql_all =
        \\SELECT d.ontology_id, d.namespace, d.dimension, d.value_type, d.is_multi, d.hierarchy_kind, d.metadata_json
        \\FROM ontology_dimensions d
        \\JOIN ontologies o ON o.ontology_id = d.ontology_id
        \\WHERE o.workspace_id = ?1 OR o.workspace_id IS NULL
        \\ORDER BY d.ontology_id, d.namespace, d.dimension
    ;
    const sql_attached =
        \\SELECT d.ontology_id, d.namespace, d.dimension, d.value_type, d.is_multi, d.hierarchy_kind, d.metadata_json
        \\FROM ontology_dimensions d
        \\JOIN collection_ontologies co ON co.ontology_id = d.ontology_id
        \\WHERE co.workspace_id = ?1 AND co.collection_id = ?2
        \\ORDER BY d.ontology_id, d.namespace, d.dimension
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_attached);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(OntologyDimensionRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .namespace = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .dimension = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .value_type = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .is_multi = c.sqlite3_column_int64(stmt, 4) != 0,
            .hierarchy_kind = try facet_sqlite.dupeColumnText(arena, stmt, 5),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 6),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectOntologyValues(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]OntologyValueRow {
    const sql_all =
        \\SELECT v.ontology_id, v.namespace, v.dimension, v.value_id, v.value, v.parent_value_id, v.label, v.metadata_json
        \\FROM ontology_values v
        \\JOIN ontologies o ON o.ontology_id = v.ontology_id
        \\WHERE o.workspace_id = ?1 OR o.workspace_id IS NULL
        \\ORDER BY v.ontology_id, v.namespace, v.dimension, v.value_id
    ;
    const sql_attached =
        \\SELECT v.ontology_id, v.namespace, v.dimension, v.value_id, v.value, v.parent_value_id, v.label, v.metadata_json
        \\FROM ontology_values v
        \\JOIN collection_ontologies co ON co.ontology_id = v.ontology_id
        \\WHERE co.workspace_id = ?1 AND co.collection_id = ?2
        \\ORDER BY v.ontology_id, v.namespace, v.dimension, v.value_id
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_attached);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(OntologyValueRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .namespace = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .dimension = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .value_id = c.sqlite3_column_int64(stmt, 3),
            .value = try facet_sqlite.dupeColumnText(arena, stmt, 4),
            .parent_value_id = if (c.sqlite3_column_type(stmt, 5) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 5),
            .label = try maybeColText(arena, stmt, 6),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 7),
        });
    }
    return rows.toOwnedSlice(arena);
}

pub fn selectDimensionsForOntology(arena: Allocator, db: Database, ontology_id: []const u8) ![]OntologyDimensionRow {
    const sql =
        \\SELECT ontology_id, namespace, dimension, value_type, is_multi, hierarchy_kind, metadata_json
        \\FROM ontology_dimensions
        \\WHERE ontology_id = ?1
        \\ORDER BY namespace, dimension
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, ontology_id);
    var rows = std.ArrayList(OntologyDimensionRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .namespace = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .dimension = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .value_type = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .is_multi = c.sqlite3_column_int64(stmt, 4) != 0,
            .hierarchy_kind = try facet_sqlite.dupeColumnText(arena, stmt, 5),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 6),
        });
    }
    return rows.toOwnedSlice(arena);
}

pub fn selectValuesForOntology(arena: Allocator, db: Database, ontology_id: []const u8) ![]OntologyValueRow {
    const sql =
        \\SELECT ontology_id, namespace, dimension, value_id, value, parent_value_id, label, metadata_json
        \\FROM ontology_values
        \\WHERE ontology_id = ?1
        \\ORDER BY namespace, dimension, value_id
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, ontology_id);
    var rows = std.ArrayList(OntologyValueRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .namespace = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .dimension = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .value_id = c.sqlite3_column_int64(stmt, 3),
            .value = try facet_sqlite.dupeColumnText(arena, stmt, 4),
            .parent_value_id = if (c.sqlite3_column_type(stmt, 5) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 5),
            .label = try maybeColText(arena, stmt, 6),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 7),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectOntologyEntityTypes(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]OntologyEntityTypeRow {
    const sql_all =
        \\SELECT et.ontology_id, et.entity_type, et.label, et.metadata_json
        \\FROM ontology_entity_types et
        \\JOIN ontologies o ON o.ontology_id = et.ontology_id
        \\WHERE o.workspace_id = ?1 OR o.workspace_id IS NULL
        \\ORDER BY et.ontology_id, et.entity_type
    ;
    const sql_attached =
        \\SELECT et.ontology_id, et.entity_type, et.label, et.metadata_json
        \\FROM ontology_entity_types et
        \\JOIN collection_ontologies co ON co.ontology_id = et.ontology_id
        \\WHERE co.workspace_id = ?1 AND co.collection_id = ?2
        \\ORDER BY et.ontology_id, et.entity_type
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_attached);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(OntologyEntityTypeRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .entity_type = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .label = try maybeColText(arena, stmt, 2),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 3),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectOntologyEdgeTypes(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]OntologyEdgeTypeRow {
    const sql_all =
        \\SELECT et.ontology_id, et.edge_type, et.directed, et.source_entity_type, et.target_entity_type, et.metadata_json
        \\FROM ontology_edge_types et
        \\JOIN ontologies o ON o.ontology_id = et.ontology_id
        \\WHERE o.workspace_id = ?1 OR o.workspace_id IS NULL
        \\ORDER BY et.ontology_id, et.edge_type
    ;
    const sql_attached =
        \\SELECT et.ontology_id, et.edge_type, et.directed, et.source_entity_type, et.target_entity_type, et.metadata_json
        \\FROM ontology_edge_types et
        \\JOIN collection_ontologies co ON co.ontology_id = et.ontology_id
        \\WHERE co.workspace_id = ?1 AND co.collection_id = ?2
        \\ORDER BY et.ontology_id, et.edge_type
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_attached);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(OntologyEdgeTypeRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .edge_type = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .directed = c.sqlite3_column_int64(stmt, 2) != 0,
            .source_entity_type = try maybeColText(arena, stmt, 3),
            .target_entity_type = try maybeColText(arena, stmt, 4),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 5),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectOntologyEntities(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]OntologyEntityRow {
    const sql_all =
        \\SELECT e.ontology_id, e.entity_id, e.entity_type, e.name, e.metadata_json
        \\FROM ontology_entities_raw e
        \\JOIN ontologies o ON o.ontology_id = e.ontology_id
        \\WHERE o.workspace_id = ?1 OR o.workspace_id IS NULL
        \\ORDER BY e.ontology_id, e.entity_id
    ;
    const sql_attached =
        \\SELECT e.ontology_id, e.entity_id, e.entity_type, e.name, e.metadata_json
        \\FROM ontology_entities_raw e
        \\JOIN collection_ontologies co ON co.ontology_id = e.ontology_id
        \\WHERE co.workspace_id = ?1 AND co.collection_id = ?2
        \\ORDER BY e.ontology_id, e.entity_id
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_attached);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(OntologyEntityRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .entity_id = c.sqlite3_column_int64(stmt, 1),
            .entity_type = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .label = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 4),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectOntologyRelations(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]OntologyRelationRow {
    const sql_all =
        \\SELECT r.ontology_id, r.relation_id, r.edge_type, r.source_entity_id, r.target_entity_id, r.metadata_json
        \\FROM ontology_relations_raw r
        \\JOIN ontologies o ON o.ontology_id = r.ontology_id
        \\WHERE o.workspace_id = ?1 OR o.workspace_id IS NULL
        \\ORDER BY r.ontology_id, r.relation_id
    ;
    const sql_attached =
        \\SELECT r.ontology_id, r.relation_id, r.edge_type, r.source_entity_id, r.target_entity_id, r.metadata_json
        \\FROM ontology_relations_raw r
        \\JOIN collection_ontologies co ON co.ontology_id = r.ontology_id
        \\WHERE co.workspace_id = ?1 AND co.collection_id = ?2
        \\ORDER BY r.ontology_id, r.relation_id
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_attached);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(OntologyRelationRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .relation_id = c.sqlite3_column_int64(stmt, 1),
            .edge_type = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .source_entity_id = c.sqlite3_column_int64(stmt, 3),
            .target_entity_id = c.sqlite3_column_int64(stmt, 4),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 5),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectOntologyTriples(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]OntologyTripleRow {
    const sql_all =
        \\SELECT t.ontology_id, t.triple_index, t.subject_kind, t.subject, t.predicate, t.object_kind, t.object_value, t.object_datatype, t.object_language, t.source_line, t.metadata_json
        \\FROM ontology_triples_raw t
        \\JOIN ontologies o ON o.ontology_id = t.ontology_id
        \\WHERE o.workspace_id = ?1 OR o.workspace_id IS NULL
        \\ORDER BY t.ontology_id, t.triple_index
    ;
    const sql_attached =
        \\SELECT t.ontology_id, t.triple_index, t.subject_kind, t.subject, t.predicate, t.object_kind, t.object_value, t.object_datatype, t.object_language, t.source_line, t.metadata_json
        \\FROM ontology_triples_raw t
        \\JOIN collection_ontologies co ON co.ontology_id = t.ontology_id
        \\WHERE co.workspace_id = ?1 AND co.collection_id = ?2
        \\ORDER BY t.ontology_id, t.triple_index
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_attached);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(OntologyTripleRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .triple_index = c.sqlite3_column_int64(stmt, 1),
            .subject_kind = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .subject = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .predicate = try facet_sqlite.dupeColumnText(arena, stmt, 4),
            .object_kind = try facet_sqlite.dupeColumnText(arena, stmt, 5),
            .object_value = try facet_sqlite.dupeColumnText(arena, stmt, 6),
            .object_datatype = try maybeColText(arena, stmt, 7),
            .object_language = try maybeColText(arena, stmt, 8),
            .source_line = try facet_sqlite.dupeColumnText(arena, stmt, 9),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 10),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectCollectionOntologies(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]CollectionOntologyRow {
    const sql_all =
        \\SELECT workspace_id, collection_id, ontology_id, role
        \\FROM collection_ontologies WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, collection_id, ontology_id, role
        \\FROM collection_ontologies WHERE workspace_id = ?1 AND collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(CollectionOntologyRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .role = try facet_sqlite.dupeColumnText(arena, stmt, 3),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectWorkspaceSettings(arena: Allocator, db: Database, workspace_id: []const u8) ![]WorkspaceSettingsRow {
    const sql = "SELECT workspace_id, default_ontology_id FROM workspace_settings WHERE workspace_id = ?1";
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(WorkspaceSettingsRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .default_ontology_id = try maybeColText(arena, stmt, 1),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectAgentFacts(arena: Allocator, db: Database, workspace_id: []const u8) ![]AgentFactRow {
    const sql =
        \\SELECT id, schema_id, content, facets, facets_json, embedding, created_by, created_at,
        \\       created_at_unix, updated_at, updated_at_unix, version, supersedes,
        \\       valid_from_unix, valid_until_unix, workspace_id, source_ref, doc_id
        \\FROM agent_facts
        \\WHERE workspace_id = ?1
        \\ORDER BY created_at_unix, id
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(AgentFactRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .schema_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .content = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .facets = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .facets_json = try facet_sqlite.dupeColumnText(arena, stmt, 4),
            .embedding = try maybeColText(arena, stmt, 5),
            .created_by = try maybeColText(arena, stmt, 6),
            .created_at = try facet_sqlite.dupeColumnText(arena, stmt, 7),
            .created_at_unix = c.sqlite3_column_int64(stmt, 8),
            .updated_at = try facet_sqlite.dupeColumnText(arena, stmt, 9),
            .updated_at_unix = c.sqlite3_column_int64(stmt, 10),
            .version = c.sqlite3_column_int64(stmt, 11),
            .supersedes = try maybeColText(arena, stmt, 12),
            .valid_from_unix = if (c.sqlite3_column_type(stmt, 13) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 13),
            .valid_until_unix = if (c.sqlite3_column_type(stmt, 14) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 14),
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 15),
            .source_ref = try maybeColText(arena, stmt, 16),
            .doc_id = if (c.sqlite3_column_type(stmt, 17) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 17),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectFacetTables(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]FacetTableRow {
    const sql_all =
        \\SELECT ft.table_id, ts.workspace_id, ft.table_name, ft.schema_name, ft.table_name, ft.chunk_bits,
        \\       COALESCE(ts.key_column, 'doc_id'), COALESCE(ts.content_column, 'content'),
        \\       COALESCE(ts.metadata_column, 'metadata_json'), COALESCE(ts.language, 'english'),
        \\       CASE WHEN bst.table_id IS NULL THEN 0 ELSE 1 END
        \\FROM facet_tables ft
        \\JOIN table_semantics ts ON ts.table_id = ft.table_id
        \\LEFT JOIN bm25_sync_triggers bst ON bst.table_id = ft.table_id
        \\WHERE ts.workspace_id = ?1
        \\ORDER BY ft.table_id
    ;
    const sql_one =
        \\SELECT ft.table_id, ts.workspace_id, ft.table_name, ft.schema_name, ft.table_name, ft.chunk_bits,
        \\       COALESCE(ts.key_column, 'doc_id'), COALESCE(ts.content_column, 'content'),
        \\       COALESCE(ts.metadata_column, 'metadata_json'), COALESCE(ts.language, 'english'),
        \\       CASE WHEN bst.table_id IS NULL THEN 0 ELSE 1 END
        \\FROM facet_tables ft
        \\JOIN table_semantics ts ON ts.table_id = ft.table_id
        \\LEFT JOIN bm25_sync_triggers bst ON bst.table_id = ft.table_id
        \\WHERE ts.workspace_id = ?1 AND ft.table_name = ?2
        \\ORDER BY ft.table_id
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);

    var rows = std.ArrayList(FacetTableRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .table_id = c.sqlite3_column_int64(stmt, 0),
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .schema_name = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .table_name = try facet_sqlite.dupeColumnText(arena, stmt, 4),
            .chunk_bits = c.sqlite3_column_int64(stmt, 5),
            .key_column = try facet_sqlite.dupeColumnText(arena, stmt, 6),
            .content_column = try facet_sqlite.dupeColumnText(arena, stmt, 7),
            .metadata_column = try facet_sqlite.dupeColumnText(arena, stmt, 8),
            .language = try facet_sqlite.dupeColumnText(arena, stmt, 9),
            .bm25_enabled = c.sqlite3_column_int64(stmt, 10) != 0,
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectFacetDefinitions(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]FacetDefinitionRow {
    const sql_all =
        \\SELECT fd.table_id, fd.facet_id, fd.facet_name
        \\FROM facet_definitions fd
        \\JOIN table_semantics ts ON ts.table_id = fd.table_id
        \\WHERE ts.workspace_id = ?1
        \\ORDER BY fd.table_id, fd.facet_id
    ;
    const sql_one =
        \\SELECT fd.table_id, fd.facet_id, fd.facet_name
        \\FROM facet_definitions fd
        \\JOIN table_semantics ts ON ts.table_id = fd.table_id
        \\JOIN facet_tables ft ON ft.table_id = fd.table_id
        \\WHERE ts.workspace_id = ?1 AND ft.table_name = ?2
        \\ORDER BY fd.table_id, fd.facet_id
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);

    var rows = std.ArrayList(FacetDefinitionRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .table_id = c.sqlite3_column_int64(stmt, 0),
            .facet_id = c.sqlite3_column_int64(stmt, 1),
            .facet_name = try facet_sqlite.dupeColumnText(arena, stmt, 2),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectDocuments(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]DocumentRow {
    const sql_all =
        \\SELECT workspace_id, collection_id, doc_id, doc_nanoid, content, language, source_ref, summary, metadata_json
        \\FROM documents_raw WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, collection_id, doc_id, doc_nanoid, content, language, source_ref, summary, metadata_json
        \\FROM documents_raw WHERE workspace_id = ?1 AND collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(DocumentRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .doc_id = c.sqlite3_column_int64(stmt, 2),
            .doc_nanoid = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .content = try facet_sqlite.dupeColumnText(arena, stmt, 4),
            .language = try maybeColText(arena, stmt, 5),
            .source_ref = try maybeColText(arena, stmt, 6),
            .summary = try maybeColText(arena, stmt, 7),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 8),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectChunks(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]ChunkRow {
    const sql_all =
        \\SELECT workspace_id, collection_id, doc_id, chunk_index, content, language, offset_start, offset_end, strategy, token_count, parent_chunk_index, metadata_json
        \\FROM chunks_raw WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, collection_id, doc_id, chunk_index, content, language, offset_start, offset_end, strategy, token_count, parent_chunk_index, metadata_json
        \\FROM chunks_raw WHERE workspace_id = ?1 AND collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(ChunkRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .doc_id = c.sqlite3_column_int64(stmt, 2),
            .chunk_index = c.sqlite3_column_int64(stmt, 3),
            .content = try facet_sqlite.dupeColumnText(arena, stmt, 4),
            .language = try maybeColText(arena, stmt, 5),
            .offset_start = if (c.sqlite3_column_type(stmt, 6) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 6),
            .offset_end = if (c.sqlite3_column_type(stmt, 7) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 7),
            .strategy = try maybeColText(arena, stmt, 8),
            .token_count = if (c.sqlite3_column_type(stmt, 9) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 9),
            .parent_chunk_index = if (c.sqlite3_column_type(stmt, 10) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 10),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 11),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectDocumentVectors(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]VectorRow {
    const sql_all =
        \\SELECT workspace_id, collection_id, doc_id, dim, embedding_blob
        \\FROM documents_raw_vector WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, collection_id, doc_id, dim, embedding_blob
        \\FROM documents_raw_vector WHERE workspace_id = ?1 AND collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(VectorRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .doc_id = c.sqlite3_column_int64(stmt, 2),
            .chunk_index = null,
            .dim = c.sqlite3_column_int64(stmt, 3),
            .embedding_blob = try facet_sqlite.dupeColumnText(arena, stmt, 4),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectChunkVectors(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]VectorRow {
    const sql_all =
        \\SELECT workspace_id, collection_id, doc_id, chunk_index, dim, embedding_blob
        \\FROM chunks_raw_vector WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, collection_id, doc_id, chunk_index, dim, embedding_blob
        \\FROM chunks_raw_vector WHERE workspace_id = ?1 AND collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(VectorRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .doc_id = c.sqlite3_column_int64(stmt, 2),
            .chunk_index = c.sqlite3_column_int64(stmt, 3),
            .dim = c.sqlite3_column_int64(stmt, 4),
            .embedding_blob = try facet_sqlite.dupeColumnText(arena, stmt, 5),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectExternalLinks(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]ExternalLinkRow {
    const sql_all =
        \\SELECT workspace_id, link_id, source_collection_id, source_doc_id, source_chunk_index, target_uri, edge_type, weight, metadata_json
        \\FROM external_links_raw WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, link_id, source_collection_id, source_doc_id, source_chunk_index, target_uri, edge_type, weight, metadata_json
        \\FROM external_links_raw WHERE workspace_id = ?1 AND source_collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(ExternalLinkRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .link_id = c.sqlite3_column_int64(stmt, 1),
            .source_collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .source_doc_id = c.sqlite3_column_int64(stmt, 3),
            .source_chunk_index = if (c.sqlite3_column_type(stmt, 4) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 4),
            .target_uri = try facet_sqlite.dupeColumnText(arena, stmt, 5),
            .edge_type = try facet_sqlite.dupeColumnText(arena, stmt, 6),
            .weight = c.sqlite3_column_double(stmt, 7),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 8),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectFacetAssignments(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]FacetAssignmentRow {
    const sql_all =
        \\SELECT workspace_id, collection_id, target_kind, doc_id, chunk_index, ontology_id, namespace, dimension, value, value_id, weight, source
        \\FROM facet_assignments_raw WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, collection_id, target_kind, doc_id, chunk_index, ontology_id, namespace, dimension, value, value_id, weight, source
        \\FROM facet_assignments_raw WHERE workspace_id = ?1 AND collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(FacetAssignmentRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .target_kind = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .doc_id = c.sqlite3_column_int64(stmt, 3),
            .chunk_index = c.sqlite3_column_int64(stmt, 4),
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 5),
            .namespace = try facet_sqlite.dupeColumnText(arena, stmt, 6),
            .dimension = try facet_sqlite.dupeColumnText(arena, stmt, 7),
            .value = try facet_sqlite.dupeColumnText(arena, stmt, 8),
            .value_id = if (c.sqlite3_column_type(stmt, 9) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 9),
            .weight = c.sqlite3_column_double(stmt, 10),
            .source = try maybeColText(arena, stmt, 11),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectEntities(arena: Allocator, db: Database, workspace_id: []const u8) ![]EntityRow {
    const sql =
        \\SELECT workspace_id, ontology_id, entity_id, external_id, entity_type, name, confidence, metadata_json
        \\FROM entities_raw WHERE workspace_id = ?1
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(EntityRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .entity_id = c.sqlite3_column_int64(stmt, 2),
            .external_id = try maybeColText(arena, stmt, 3),
            .entity_type = try facet_sqlite.dupeColumnText(arena, stmt, 4),
            .name = try facet_sqlite.dupeColumnText(arena, stmt, 5),
            .confidence = c.sqlite3_column_double(stmt, 6),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 7),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectEntityAliases(arena: Allocator, db: Database, workspace_id: []const u8) ![]EntityAliasRow {
    const sql =
        \\SELECT workspace_id, entity_id, term, confidence
        \\FROM entity_aliases_raw WHERE workspace_id = ?1
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(EntityAliasRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .entity_id = c.sqlite3_column_int64(stmt, 1),
            .term = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .confidence = c.sqlite3_column_double(stmt, 3),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectRelations(arena: Allocator, db: Database, workspace_id: []const u8) ![]RelationRow {
    const sql =
        \\SELECT r.workspace_id, r.ontology_id, r.relation_id, r.external_id, r.edge_type,
        \\       r.source_entity_id, r.target_entity_id, r.valid_from, r.valid_to, r.confidence, r.metadata_json
        \\FROM relations_raw r
        \\JOIN entities_raw s ON s.workspace_id = r.workspace_id AND s.entity_id = r.source_entity_id
        \\JOIN entities_raw t ON t.workspace_id = r.workspace_id AND t.entity_id = r.target_entity_id
        \\WHERE r.workspace_id = ?1
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(RelationRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .relation_id = c.sqlite3_column_int64(stmt, 2),
            .external_id = try maybeColText(arena, stmt, 3),
            .edge_type = try facet_sqlite.dupeColumnText(arena, stmt, 4),
            .source_entity_id = c.sqlite3_column_int64(stmt, 5),
            .target_entity_id = c.sqlite3_column_int64(stmt, 6),
            .valid_from = try maybeColText(arena, stmt, 7),
            .valid_to = try maybeColText(arena, stmt, 8),
            .confidence = c.sqlite3_column_double(stmt, 9),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 10),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectRelationProperties(arena: Allocator, db: Database, workspace_id: []const u8) ![]RelationPropertyRow {
    const sql =
        \\SELECT p.workspace_id, p.relation_id, p.property_key, p.value_type, p.value_text,
        \\       p.value_number, p.value_integer, p.ref_doc_id, p.currency
        \\FROM relation_properties_raw p
        \\JOIN relations_raw r ON r.workspace_id = p.workspace_id AND r.relation_id = p.relation_id
        \\JOIN entities_raw s ON s.workspace_id = r.workspace_id AND s.entity_id = r.source_entity_id
        \\JOIN entities_raw t ON t.workspace_id = r.workspace_id AND t.entity_id = r.target_entity_id
        \\WHERE p.workspace_id = ?1
        \\ORDER BY p.relation_id, p.property_key
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(RelationPropertyRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .relation_id = c.sqlite3_column_int64(stmt, 1),
            .property_key = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .value_type = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .value_text = try maybeColText(arena, stmt, 4),
            .value_number = if (c.sqlite3_column_type(stmt, 5) == c.SQLITE_NULL) null else c.sqlite3_column_double(stmt, 5),
            .value_integer = if (c.sqlite3_column_type(stmt, 6) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 6),
            .ref_doc_id = if (c.sqlite3_column_type(stmt, 7) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 7),
            .currency = try maybeColText(arena, stmt, 8),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectEntityDocuments(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]EntityDocumentRow {
    const sql_all =
        \\SELECT workspace_id, entity_id, collection_id, doc_id, role, confidence
        \\FROM entity_documents_raw WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, entity_id, collection_id, doc_id, role, confidence
        \\FROM entity_documents_raw WHERE workspace_id = ?1 AND collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(EntityDocumentRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .entity_id = c.sqlite3_column_int64(stmt, 1),
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .doc_id = c.sqlite3_column_int64(stmt, 3),
            .role = try maybeColText(arena, stmt, 4),
            .confidence = c.sqlite3_column_double(stmt, 5),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectEntityChunks(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]EntityChunkRow {
    const sql_all =
        \\SELECT workspace_id, entity_id, collection_id, doc_id, chunk_index, role, confidence
        \\FROM entity_chunks_raw WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, entity_id, collection_id, doc_id, chunk_index, role, confidence
        \\FROM entity_chunks_raw WHERE workspace_id = ?1 AND collection_id = ?2
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(EntityChunkRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .entity_id = c.sqlite3_column_int64(stmt, 1),
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .doc_id = c.sqlite3_column_int64(stmt, 3),
            .chunk_index = c.sqlite3_column_int64(stmt, 4),
            .role = try maybeColText(arena, stmt, 5),
            .confidence = c.sqlite3_column_double(stmt, 6),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectGraphEntities(arena: Allocator, db: Database, workspace_id: []const u8) ![]GraphEntityRow {
    const sql =
        \\SELECT entity_id, workspace_id, entity_type, name, confidence, deprecated_at, metadata_json, created_at_unix
        \\FROM graph_entity WHERE workspace_id = ?1
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(GraphEntityRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .entity_id = c.sqlite3_column_int64(stmt, 0),
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .entity_type = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .name = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .confidence = c.sqlite3_column_double(stmt, 4),
            .deprecated_at = if (c.sqlite3_column_type(stmt, 5) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 5),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 6),
            .created_at_unix = c.sqlite3_column_int64(stmt, 7),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectGraphEntityAliases(arena: Allocator, db: Database, workspace_id: []const u8) ![]GraphEntityAliasRow {
    const sql =
        \\SELECT a.term, a.entity_id, a.confidence
        \\FROM graph_entity_alias a
        \\JOIN graph_entity e ON e.entity_id = a.entity_id
        \\WHERE e.workspace_id = ?1
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(GraphEntityAliasRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .term = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .entity_id = c.sqlite3_column_int64(stmt, 1),
            .confidence = c.sqlite3_column_double(stmt, 2),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectGraphRelations(arena: Allocator, db: Database, workspace_id: []const u8) ![]GraphRelationRow {
    const sql =
        \\SELECT r.relation_id, r.workspace_id, r.relation_type, r.source_id, r.target_id, r.valid_from_unix, r.valid_to_unix,
        \\       r.confidence, r.deprecated_at, r.run_id, r.patch_id, r.metadata_json, r.created_at_unix
        \\FROM graph_relation r
        \\JOIN graph_entity s ON s.workspace_id = r.workspace_id AND s.entity_id = r.source_id
        \\JOIN graph_entity t ON t.workspace_id = r.workspace_id AND t.entity_id = r.target_id
        \\WHERE r.workspace_id = ?1
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(GraphRelationRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .relation_id = c.sqlite3_column_int64(stmt, 0),
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .relation_type = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .source_id = c.sqlite3_column_int64(stmt, 3),
            .target_id = c.sqlite3_column_int64(stmt, 4),
            .valid_from_unix = if (c.sqlite3_column_type(stmt, 5) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 5),
            .valid_to_unix = if (c.sqlite3_column_type(stmt, 6) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 6),
            .confidence = c.sqlite3_column_double(stmt, 7),
            .deprecated_at = if (c.sqlite3_column_type(stmt, 8) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 8),
            .run_id = if (c.sqlite3_column_type(stmt, 9) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 9),
            .patch_id = if (c.sqlite3_column_type(stmt, 10) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 10),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 11),
            .created_at_unix = c.sqlite3_column_int64(stmt, 12),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectGraphRelationProperties(arena: Allocator, db: Database, workspace_id: []const u8) ![]GraphRelationPropertyRow {
    const sql =
        \\SELECT p.relation_id, p.property_key, p.value_type, p.value_text, p.value_number,
        \\       p.value_integer, p.ref_doc_id, p.currency
        \\FROM graph_relation_property p
        \\JOIN graph_relation r ON r.relation_id = p.relation_id
        \\JOIN graph_entity s ON s.workspace_id = r.workspace_id AND s.entity_id = r.source_id
        \\JOIN graph_entity t ON t.workspace_id = r.workspace_id AND t.entity_id = r.target_id
        \\WHERE r.workspace_id = ?1
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(GraphRelationPropertyRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .relation_id = c.sqlite3_column_int64(stmt, 0),
            .property_key = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .value_type = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .value_text = try maybeColText(arena, stmt, 3),
            .value_number = if (c.sqlite3_column_type(stmt, 4) == c.SQLITE_NULL) null else c.sqlite3_column_double(stmt, 4),
            .value_integer = if (c.sqlite3_column_type(stmt, 5) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 5),
            .ref_doc_id = if (c.sqlite3_column_type(stmt, 6) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 6),
            .currency = try maybeColText(arena, stmt, 7),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectGraphEntityDocuments(arena: Allocator, db: Database, workspace_id: []const u8) ![]GraphEntityDocumentRow {
    const sql =
        \\SELECT d.entity_id, d.doc_id, d.table_id, d.role, d.confidence
        \\FROM graph_entity_document d
        \\JOIN graph_entity e ON e.entity_id = d.entity_id
        \\WHERE e.workspace_id = ?1
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(GraphEntityDocumentRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .entity_id = c.sqlite3_column_int64(stmt, 0),
            .doc_id = c.sqlite3_column_int64(stmt, 1),
            .table_id = c.sqlite3_column_int64(stmt, 2),
            .role = try maybeColText(arena, stmt, 3),
            .confidence = c.sqlite3_column_double(stmt, 4),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectGraphEntityChunks(arena: Allocator, db: Database, workspace_id: []const u8) ![]GraphEntityChunkRow {
    const sql =
        \\SELECT entity_id, workspace_id, collection_id, doc_id, chunk_index, role, confidence, metadata_json
        \\FROM graph_entity_chunk WHERE workspace_id = ?1
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(GraphEntityChunkRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .entity_id = c.sqlite3_column_int64(stmt, 0),
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .doc_id = c.sqlite3_column_int64(stmt, 3),
            .chunk_index = c.sqlite3_column_int64(stmt, 4),
            .role = try maybeColText(arena, stmt, 5),
            .confidence = c.sqlite3_column_double(stmt, 6),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 7),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectDocumentLinks(arena: Allocator, db: Database, workspace_id: []const u8, collection_filter: ?[]const u8) ![]DocumentLinkRow {
    const sql_all =
        \\SELECT workspace_id, link_id, ontology_id, edge_type, source_collection_id, source_doc_id, source_chunk_index, target_collection_id, target_doc_id, target_chunk_index, weight, metadata_json, source
        \\FROM document_links_raw WHERE workspace_id = ?1
    ;
    const sql_one =
        \\SELECT workspace_id, link_id, ontology_id, edge_type, source_collection_id, source_doc_id, source_chunk_index, target_collection_id, target_doc_id, target_chunk_index, weight, metadata_json, source
        \\FROM document_links_raw
        \\WHERE workspace_id = ?1 AND (source_collection_id = ?2 OR target_collection_id = ?2)
    ;
    const stmt = try facet_sqlite.prepare(db, if (collection_filter == null) sql_all else sql_one);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_filter) |coll| try facet_sqlite.bindText(stmt, 2, coll);
    var rows = std.ArrayList(DocumentLinkRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .link_id = c.sqlite3_column_int64(stmt, 1),
            .ontology_id = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .edge_type = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .source_collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 4),
            .source_doc_id = c.sqlite3_column_int64(stmt, 5),
            .source_chunk_index = if (c.sqlite3_column_type(stmt, 6) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 6),
            .target_collection_id = try facet_sqlite.dupeColumnText(arena, stmt, 7),
            .target_doc_id = c.sqlite3_column_int64(stmt, 8),
            .target_chunk_index = if (c.sqlite3_column_type(stmt, 9) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 9),
            .weight = c.sqlite3_column_double(stmt, 10),
            .metadata_json = try facet_sqlite.dupeColumnText(arena, stmt, 11),
            .source = try maybeColText(arena, stmt, 12),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectAnswerArtifacts(arena: Allocator, db: Database, workspace_id: []const u8) ![]AnswerArtifactRow {
    const sql =
        \\SELECT artifact_id, slug, workspace_id, agent_id, scope, artifact_kind,
        \\       public_label_key, public_label, lifecycle, state, current_version,
        \\       payload_json, legacy_ref, created_at_unix, updated_at_unix
        \\FROM mindbrain_answer_artifacts
        \\WHERE workspace_id = ?1 OR (artifact_kind = 'analysis_plan' AND scope = ?1)
        \\ORDER BY artifact_kind, slug, artifact_id
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(AnswerArtifactRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .artifact_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .slug = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .workspace_id = try maybeColText(arena, stmt, 2),
            .agent_id = try maybeColText(arena, stmt, 3),
            .scope = try maybeColText(arena, stmt, 4),
            .artifact_kind = try facet_sqlite.dupeColumnText(arena, stmt, 5),
            .public_label_key = try maybeColText(arena, stmt, 6),
            .public_label = try facet_sqlite.dupeColumnText(arena, stmt, 7),
            .lifecycle = try facet_sqlite.dupeColumnText(arena, stmt, 8),
            .state = try facet_sqlite.dupeColumnText(arena, stmt, 9),
            .current_version = c.sqlite3_column_int64(stmt, 10),
            .payload_json = try facet_sqlite.dupeColumnText(arena, stmt, 11),
            .legacy_ref = try maybeColText(arena, stmt, 12),
            .created_at_unix = c.sqlite3_column_int64(stmt, 13),
            .updated_at_unix = c.sqlite3_column_int64(stmt, 14),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectAnswerEvents(arena: Allocator, db: Database, workspace_id: []const u8) ![]AnswerEventRow {
    const sql =
        \\SELECT e.event_id, e.artifact_id, e.event_kind, e.from_version, e.to_version,
        \\       e.signal_json, e.created_at_unix
        \\FROM mindbrain_answer_events e
        \\JOIN mindbrain_answer_artifacts a ON a.artifact_id = e.artifact_id
        \\WHERE a.workspace_id = ?1 OR (a.artifact_kind = 'analysis_plan' AND a.scope = ?1)
        \\ORDER BY e.created_at_unix ASC, e.event_id ASC
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(AnswerEventRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .event_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .artifact_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .event_kind = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .from_version = if (c.sqlite3_column_type(stmt, 3) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 3),
            .to_version = if (c.sqlite3_column_type(stmt, 4) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 4),
            .signal_json = try facet_sqlite.dupeColumnText(arena, stmt, 5),
            .created_at_unix = c.sqlite3_column_int64(stmt, 6),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectQualityConvergenceRuns(arena: Allocator, db: Database, workspace_id: []const u8) ![]QualityConvergenceRunRow {
    const sql =
        \\SELECT run_id, workspace_id, ontology_id, run_kind, status, canonical_layer,
        \\       input_fingerprint, summary_json, report_json, created_at_unix, updated_at_unix
        \\FROM quality_convergence_run
        \\WHERE workspace_id = ?1
        \\ORDER BY created_at_unix ASC, run_id ASC
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(QualityConvergenceRunRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .run_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .ontology_id = try maybeColText(arena, stmt, 2),
            .run_kind = try facet_sqlite.dupeColumnText(arena, stmt, 3),
            .status = try facet_sqlite.dupeColumnText(arena, stmt, 4),
            .canonical_layer = try facet_sqlite.dupeColumnText(arena, stmt, 5),
            .input_fingerprint = try facet_sqlite.dupeColumnText(arena, stmt, 6),
            .summary_json = try facet_sqlite.dupeColumnText(arena, stmt, 7),
            .report_json = try facet_sqlite.dupeColumnText(arena, stmt, 8),
            .created_at_unix = c.sqlite3_column_int64(stmt, 9),
            .updated_at_unix = c.sqlite3_column_int64(stmt, 10),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn selectQualityRemediationActions(arena: Allocator, db: Database, workspace_id: []const u8) ![]QualityRemediationActionRow {
    const sql =
        \\SELECT action_id, run_id, workspace_id, ontology_id, issue_type, severity,
        \\       confidence, reason, schema_id, entity_type, projection_id, evidence_json,
        \\       mcp_tool, tool_args_json, execution_mode, idempotency_key, status,
        \\       decision_actor, decision_note, result_json, created_at_unix, updated_at_unix,
        \\       decided_at_unix, applied_at_unix
        \\FROM quality_remediation_action
        \\WHERE workspace_id = ?1
        \\ORDER BY created_at_unix ASC, action_id ASC
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    var rows = std.ArrayList(QualityRemediationActionRow).empty;
    while (true) {
        const status = c.sqlite3_step(stmt);
        if (status == c.SQLITE_DONE) break;
        if (status != c.SQLITE_ROW) return error.StepFailed;
        try rows.append(arena, .{
            .action_id = try facet_sqlite.dupeColumnText(arena, stmt, 0),
            .run_id = try facet_sqlite.dupeColumnText(arena, stmt, 1),
            .workspace_id = try facet_sqlite.dupeColumnText(arena, stmt, 2),
            .ontology_id = try maybeColText(arena, stmt, 3),
            .issue_type = try facet_sqlite.dupeColumnText(arena, stmt, 4),
            .severity = try facet_sqlite.dupeColumnText(arena, stmt, 5),
            .confidence = c.sqlite3_column_double(stmt, 6),
            .reason = try facet_sqlite.dupeColumnText(arena, stmt, 7),
            .schema_id = try maybeColText(arena, stmt, 8),
            .entity_type = try maybeColText(arena, stmt, 9),
            .projection_id = try maybeColText(arena, stmt, 10),
            .evidence_json = try facet_sqlite.dupeColumnText(arena, stmt, 11),
            .mcp_tool = try maybeColText(arena, stmt, 12),
            .tool_args_json = try facet_sqlite.dupeColumnText(arena, stmt, 13),
            .execution_mode = try facet_sqlite.dupeColumnText(arena, stmt, 14),
            .idempotency_key = try facet_sqlite.dupeColumnText(arena, stmt, 15),
            .status = try facet_sqlite.dupeColumnText(arena, stmt, 16),
            .decision_actor = try maybeColText(arena, stmt, 17),
            .decision_note = try maybeColText(arena, stmt, 18),
            .result_json = try facet_sqlite.dupeColumnText(arena, stmt, 19),
            .created_at_unix = c.sqlite3_column_int64(stmt, 20),
            .updated_at_unix = c.sqlite3_column_int64(stmt, 21),
            .decided_at_unix = if (c.sqlite3_column_type(stmt, 22) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 22),
            .applied_at_unix = if (c.sqlite3_column_type(stmt, 23) == c.SQLITE_NULL) null else c.sqlite3_column_int64(stmt, 23),
        });
    }
    return rows.toOwnedSlice(arena);
}

fn maybeColText(arena: Allocator, stmt: *c.sqlite3_stmt, index: c_int) !?[]const u8 {
    if (c.sqlite3_column_type(stmt, index) == c.SQLITE_NULL) return null;
    return try facet_sqlite.dupeColumnText(arena, stmt, index);
}

fn bindMaybeText(stmt: *c.sqlite3_stmt, index: c_int, value: ?[]const u8) !void {
    if (value) |text| try facet_sqlite.bindText(stmt, index, text) else try facet_sqlite.bindNull(stmt, index);
}

fn bindMaybeInt(stmt: *c.sqlite3_stmt, index: c_int, value: ?i64) !void {
    if (value) |int| try facet_sqlite.bindInt64(stmt, index, int) else try facet_sqlite.bindNull(stmt, index);
}

fn bindMaybeDouble(stmt: *c.sqlite3_stmt, index: c_int, value: ?f64) !void {
    if (value) |number| try bindDouble(stmt, index, number) else try facet_sqlite.bindNull(stmt, index);
}

fn bindDouble(stmt: *c.sqlite3_stmt, index: c_int, value: f64) !void {
    if (c.sqlite3_bind_double(stmt, index, value) != c.SQLITE_OK) return error.BindFailed;
}

fn execPreparedDone(stmt: *c.sqlite3_stmt) !void {
    const status = c.sqlite3_step(stmt);
    if (status != c.SQLITE_DONE) return error.StepFailed;
}

fn parseRelationPropertyValueType(value: []const u8) ?collections_sqlite.RelationPropertyValueType {
    if (std.mem.eql(u8, value, "text")) return .text;
    if (std.mem.eql(u8, value, "number")) return .number;
    if (std.mem.eql(u8, value, "percentage_bp")) return .percentage_bp;
    if (std.mem.eql(u8, value, "money_minor")) return .money_minor;
    if (std.mem.eql(u8, value, "date_unix")) return .date_unix;
    if (std.mem.eql(u8, value, "doc_ref")) return .doc_ref;
    if (std.mem.eql(u8, value, "uri")) return .uri;
    return null;
}

// ---- Tests ---------------------------------------------------------------

test "export+import bundle round-trips workspace, collection, raw rows" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws_round", .label = "Round" });
    try collections_sqlite.ensureCollection(db, .{
        .workspace_id = "ws_round",
        .collection_id = "ws_round::docs",
        .name = "docs",
    });
    try collections_sqlite.ensureOntology(db, .{
        .ontology_id = "ws_round::core",
        .workspace_id = "ws_round",
        .name = "core",
    });
    try collections_sqlite.attachOntologyToCollection(db, "ws_round", "ws_round::docs", "ws_round::core", "primary");
    try collections_sqlite.upsertDocumentRaw(db, .{
        .workspace_id = "ws_round",
        .collection_id = "ws_round::docs",
        .doc_id = 1,
        .content = "alpha",
    });
    try collections_sqlite.upsertChunkRaw(db, .{
        .workspace_id = "ws_round",
        .collection_id = "ws_round::docs",
        .doc_id = 1,
        .chunk_index = 0,
        .content = "alpha",
    });
    try collections_sqlite.ensureNamespace(db, .{ .ontology_id = "ws_round::core", .namespace = "topic" });
    try collections_sqlite.ensureDimension(db, .{ .ontology_id = "ws_round::core", .namespace = "topic", .dimension = "category" });
    try collections_sqlite.ensureValue(db, .{
        .ontology_id = "ws_round::core",
        .namespace = "topic",
        .dimension = "category",
        .value_id = 1,
        .value = "alpha",
    });
    try collections_sqlite.ensureEntityType(db, .{
        .ontology_id = "ws_round::core",
        .entity_type = "concept",
        .label = "Concept",
    });
    try collections_sqlite.ensureEdgeType(db, .{
        .ontology_id = "ws_round::core",
        .edge_type = "depends_on",
        .source_entity_type = "concept",
        .target_entity_type = "concept",
    });
    try collections_sqlite.upsertOntologyEntity(db, .{
        .ontology_id = "ws_round::core",
        .entity_id = 10,
        .entity_type = "concept",
        .name = "alpha",
    });
    try collections_sqlite.upsertOntologyEntity(db, .{
        .ontology_id = "ws_round::core",
        .entity_id = 11,
        .entity_type = "concept",
        .name = "beta",
    });
    try collections_sqlite.upsertOntologyRelation(db, .{
        .ontology_id = "ws_round::core",
        .relation_id = 12,
        .edge_type = "depends_on",
        .source_entity_id = 10,
        .target_entity_id = 11,
    });
    try collections_sqlite.upsertFacetAssignmentRaw(db, .{
        .workspace_id = "ws_round",
        .collection_id = "ws_round::docs",
        .target_kind = .doc,
        .doc_id = 1,
        .ontology_id = "ws_round::core",
        .namespace = "topic",
        .dimension = "category",
        .value = "alpha",
    });
    try collections_sqlite.upsertDocumentVector(db, .{
        .workspace_id = "ws_round",
        .collection_id = "ws_round::docs",
        .doc_id = 1,
        .dim = 3,
        .embedding_blob = "docvec",
    });
    try collections_sqlite.upsertChunkVector(db, .{
        .workspace_id = "ws_round",
        .collection_id = "ws_round::docs",
        .doc_id = 1,
        .chunk_index = 0,
        .dim = 3,
        .embedding_blob = "chunkvec",
    });
    try collections_sqlite.upsertEntityRaw(db, .{
        .workspace_id = "ws_round",
        .ontology_id = "ws_round::core",
        .entity_id = 20,
        .entity_type = "concept",
        .name = "runtime_alpha",
    });
    try collections_sqlite.upsertEntityRaw(db, .{
        .workspace_id = "ws_round",
        .ontology_id = "ws_round::core",
        .entity_id = 21,
        .entity_type = "concept",
        .name = "runtime_beta",
    });
    try collections_sqlite.upsertRelationRaw(db, .{
        .workspace_id = "ws_round",
        .ontology_id = "ws_round::core",
        .relation_id = 30,
        .edge_type = "depends_on",
        .source_entity_id = 20,
        .target_entity_id = 21,
    });
    try collections_sqlite.upsertRelationPropertyRaw(db, .{
        .workspace_id = "ws_round",
        .relation_id = 30,
        .property_key = "confidence_note",
        .value_type = .text,
        .value_text = "learned",
    });
    try db.exec(
        \\INSERT INTO mindbrain_answer_artifacts(
        \\  artifact_id, slug, workspace_id, artifact_kind, public_label,
        \\  lifecycle, state, current_version, payload_json, legacy_ref
        \\) VALUES (
        \\  'live_answer_view__round', 'round', 'ws_round', 'live_answer_view',
        \\  'Round live view', 'active', 'refreshed', 3, '{}', 'manual:round'
        \\);
        \\INSERT INTO mindbrain_answer_events(
        \\  event_id, artifact_id, event_kind, from_version, to_version, signal_json
        \\) VALUES (
        \\  'answer_update_event__round__3', 'live_answer_view__round',
        \\  'answer_update_event', 2, 3, '{"refresh":"explicit"}'
        \\);
    );

    const bundle = try exportToJson(std.testing.allocator, db, .{ .workspace = "ws_round" });
    defer std.testing.allocator.free(bundle);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "ws_round::docs") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "\"kind\": \"ghostcrab_backup_bundle\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "\"schema_version\": \"2\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "\"exported_with\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "\"mindbrain_version\": \"1.7.2\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "\"ontology_values\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "confidence_note") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "\"mindbrain_answer_artifacts\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "\"mindbrain_answer_events\"") != null);

    var dest = try Database.openInMemory();
    defer dest.close();
    try dest.applyStandaloneSchema();

    try importBundleJson(dest, std.testing.allocator, bundle);

    const sql = "SELECT COUNT(*) FROM documents_raw WHERE workspace_id = 'ws_round'";
    const stmt = try facet_sqlite.prepare(dest, sql);
    defer facet_sqlite.finalize(stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt));
    try std.testing.expectEqual(@as(i64, 1), c.sqlite3_column_int64(stmt, 0));

    const Counts = struct {
        fn one(d: Database, sql_count: []const u8) !i64 {
            const count_stmt = try facet_sqlite.prepare(d, sql_count);
            defer facet_sqlite.finalize(count_stmt);
            try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(count_stmt));
            return c.sqlite3_column_int64(count_stmt, 0);
        }
    };
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM ontology_values WHERE ontology_id = 'ws_round::core'"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM ontology_entities_raw WHERE ontology_id = 'ws_round::core' AND entity_id = 10"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM ontology_relations_raw WHERE ontology_id = 'ws_round::core'"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM relation_properties_raw WHERE workspace_id = 'ws_round' AND property_key = 'confidence_note'"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM documents_raw_vector WHERE workspace_id = 'ws_round'"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM chunks_raw_vector WHERE workspace_id = 'ws_round'"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM mindbrain_answer_artifacts WHERE workspace_id = 'ws_round' AND current_version = 3"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM mindbrain_answer_events WHERE artifact_id = 'live_answer_view__round'"));
}

test "legacy database missing additive columns imports bundle after schema apply" {
    var source = try Database.openInMemory();
    defer source.close();
    try source.applyStandaloneSchema();

    try collections_sqlite.ensureWorkspace(source, .{ .workspace_id = "ws_legacy", .label = "Legacy" });
    try collections_sqlite.ensureCollection(source, .{
        .workspace_id = "ws_legacy",
        .collection_id = "ws_legacy::docs",
        .name = "docs",
        .key_kind = "integer",
        .chunk_bits = 8,
        .default_language = "fr",
    });
    try collections_sqlite.upsertDocumentRaw(source, .{
        .workspace_id = "ws_legacy",
        .collection_id = "ws_legacy::docs",
        .doc_id = 1,
        .doc_nanoid = "doc1",
        .content = "hello",
        .summary = "short",
    });

    const bundle = try exportToJson(std.testing.allocator, source, .{ .workspace = "ws_legacy" });
    defer std.testing.allocator.free(bundle);

    var legacy = try Database.openInMemory();
    defer legacy.close();
    try legacy.applyStandaloneSchema();
    try legacy.exec("ALTER TABLE documents_raw DROP COLUMN summary");
    try std.testing.expect(!try schema_column_migrations.columnExists(legacy, "documents_raw", "summary"));

    try legacy.applyStandaloneSchema();
    try ensureBundleSchemaReady(std.testing.allocator, legacy);
    try importBundleJson(legacy, std.testing.allocator, bundle);

    const stmt = try facet_sqlite.prepare(
        legacy,
        "SELECT summary FROM documents_raw WHERE workspace_id = 'ws_legacy' AND doc_id = 1",
    );
    defer facet_sqlite.finalize(stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt));
    const summary_ptr = c.sqlite3_column_text(stmt, 0) orelse return error.MissingRow;
    try std.testing.expectEqualStrings("short", std.mem.span(summary_ptr));
}

test "taxonomies-only bundle round-trips ontology body without documents" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws_taxo", .label = "Taxonomies" });
    try collections_sqlite.ensureOntology(db, .{
        .ontology_id = "ws_taxo::core",
        .workspace_id = "ws_taxo",
        .name = "core",
    });
    try collections_sqlite.ensureNamespace(db, .{ .ontology_id = "ws_taxo::core", .namespace = "topic" });
    try collections_sqlite.ensureDimension(db, .{
        .ontology_id = "ws_taxo::core",
        .namespace = "topic",
        .dimension = "category",
        .is_multi = true,
    });
    try collections_sqlite.ensureValue(db, .{
        .ontology_id = "ws_taxo::core",
        .namespace = "topic",
        .dimension = "category",
        .value_id = 1,
        .value = "alpha",
        .label = "Alpha",
    });
    try collections_sqlite.ensureValue(db, .{
        .ontology_id = "ws_taxo::core",
        .namespace = "topic",
        .dimension = "category",
        .value_id = 2,
        .value = "beta",
        .label = "Beta",
    });
    try collections_sqlite.ensureEntityType(db, .{ .ontology_id = "ws_taxo::core", .entity_type = "concept" });
    try collections_sqlite.ensureEdgeType(db, .{
        .ontology_id = "ws_taxo::core",
        .edge_type = "related_to",
        .source_entity_type = "concept",
        .target_entity_type = "concept",
    });
    try collections_sqlite.upsertOntologyEntity(db, .{
        .ontology_id = "ws_taxo::core",
        .entity_id = 1,
        .entity_type = "concept",
        .name = "alpha",
    });
    try collections_sqlite.upsertOntologyRelation(db, .{
        .ontology_id = "ws_taxo::core",
        .relation_id = 1,
        .edge_type = "related_to",
        .source_entity_id = 1,
        .target_entity_id = 1,
    });

    const bundle = try exportToJsonWithOptions(std.testing.allocator, db, .{ .taxonomies = "ws_taxo" }, .{});
    defer std.testing.allocator.free(bundle);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "\"kind\": \"ghostcrab_backup_bundle\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "\"kind\": \"taxonomies\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "\"documents_raw\": []") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "\"ontology_values\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "\"value\": \"alpha\"") != null);

    const summary = try summarizeBundleJson(std.testing.allocator, bundle);
    defer summary.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("ghostcrab_backup_bundle", summary.kind);
    try std.testing.expectEqualStrings("taxonomies", summary.scope_kind);
    try std.testing.expectEqual(@as(usize, 2), summary.ontology_value_count);
    try std.testing.expectEqual(@as(usize, 0), summary.document_count);

    var dest = try Database.openInMemory();
    defer dest.close();
    try dest.applyStandaloneSchema();
    try importBundleJson(dest, std.testing.allocator, bundle);

    const Counts = struct {
        fn one(d: Database, sql_count: []const u8) !i64 {
            const stmt = try facet_sqlite.prepare(d, sql_count);
            defer facet_sqlite.finalize(stmt);
            try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt));
            return c.sqlite3_column_int64(stmt, 0);
        }
    };
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM ontology_dimensions WHERE ontology_id = 'ws_taxo::core'"));
    try std.testing.expectEqual(@as(i64, 2), try Counts.one(dest, "SELECT COUNT(*) FROM ontology_values WHERE ontology_id = 'ws_taxo::core'"));
    try std.testing.expectEqual(@as(i64, 0), try Counts.one(dest, "SELECT COUNT(*) FROM documents_raw WHERE workspace_id = 'ws_taxo'"));
}

test "export+import bundle round-trips chunks, cross-collection links and entity bindings" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws_xc", .label = "Cross" });
    try collections_sqlite.ensureCollection(db, .{
        .workspace_id = "ws_xc",
        .collection_id = "ws_xc::legal",
        .name = "legal",
    });
    try collections_sqlite.ensureCollection(db, .{
        .workspace_id = "ws_xc",
        .collection_id = "ws_xc::technical",
        .name = "technical",
    });
    try collections_sqlite.ensureOntology(db, .{
        .ontology_id = "ws_xc::core",
        .workspace_id = "ws_xc",
        .name = "core",
    });
    try collections_sqlite.attachOntologyToCollection(db, "ws_xc", "ws_xc::legal", "ws_xc::core", "primary");
    try collections_sqlite.attachOntologyToCollection(db, "ws_xc", "ws_xc::technical", "ws_xc::core", "primary");

    try collections_sqlite.upsertDocumentRaw(db, .{
        .workspace_id = "ws_xc",
        .collection_id = "ws_xc::legal",
        .doc_id = 100,
        .content = "Contract clause about retention.",
    });
    try collections_sqlite.upsertChunkRaw(db, .{
        .workspace_id = "ws_xc",
        .collection_id = "ws_xc::legal",
        .doc_id = 100,
        .chunk_index = 0,
        .content = "Retention clause",
    });
    try collections_sqlite.upsertDocumentRaw(db, .{
        .workspace_id = "ws_xc",
        .collection_id = "ws_xc::technical",
        .doc_id = 200,
        .content = "Storage retention spec.",
    });
    try collections_sqlite.upsertChunkRaw(db, .{
        .workspace_id = "ws_xc",
        .collection_id = "ws_xc::technical",
        .doc_id = 200,
        .chunk_index = 0,
        .content = "Storage spec",
    });

    try collections_sqlite.upsertEntityRaw(db, .{
        .workspace_id = "ws_xc",
        .ontology_id = "ws_xc::core",
        .entity_id = 5000,
        .entity_type = "concept",
        .name = "data_retention",
    });
    try collections_sqlite.linkEntityDocumentRaw(db, .{
        .workspace_id = "ws_xc",
        .entity_id = 5000,
        .collection_id = "ws_xc::legal",
        .doc_id = 100,
        .role = "subject",
    });
    try collections_sqlite.linkEntityDocumentRaw(db, .{
        .workspace_id = "ws_xc",
        .entity_id = 5000,
        .collection_id = "ws_xc::technical",
        .doc_id = 200,
        .role = "subject",
    });
    try collections_sqlite.linkEntityChunkRaw(db, .{
        .workspace_id = "ws_xc",
        .entity_id = 5000,
        .collection_id = "ws_xc::legal",
        .doc_id = 100,
        .chunk_index = 0,
        .role = "subject",
    });

    try collections_sqlite.upsertDocumentLinkRaw(db, .{
        .workspace_id = "ws_xc",
        .link_id = 1,
        .ontology_id = "ws_xc::core",
        .edge_type = "implements",
        .source_collection_id = "ws_xc::legal",
        .source_doc_id = 100,
        .target_collection_id = "ws_xc::technical",
        .target_doc_id = 200,
        .weight = 0.8,
    });

    // Upsert one document with the new doc_nanoid + summary fields and a chunk
    // carrying the chunker's strategy/token_count/parent_chunk_index columns
    // so the round-trip exercises every newly introduced raw column.
    try collections_sqlite.upsertDocumentRaw(db, .{
        .workspace_id = "ws_xc",
        .collection_id = "ws_xc::legal",
        .doc_id = 300,
        .doc_nanoid = "nano_round_trip_300",
        .content = "Annexed clause about retention.",
        .summary = "Short summary",
    });
    try collections_sqlite.upsertChunkRaw(db, .{
        .workspace_id = "ws_xc",
        .collection_id = "ws_xc::legal",
        .doc_id = 300,
        .chunk_index = 0,
        .content = "Annexed clause",
        .strategy = "fixed_token",
        .token_count = 3,
        .parent_chunk_index = null,
    });

    try collections_sqlite.upsertExternalLinkRaw(db, .{
        .workspace_id = "ws_xc",
        .link_id = 42,
        .source_collection_id = "ws_xc::legal",
        .source_doc_id = 100,
        .source_chunk_index = 0,
        .target_uri = "https://example.com/policy",
        .edge_type = "cites",
        .weight = 0.5,
        .metadata_json = "{\"note\":\"external\"}",
    });

    const bundle = try exportToJson(std.testing.allocator, db, .{ .workspace = "ws_xc" });
    defer std.testing.allocator.free(bundle);

    // Bundle must carry the freshly added columns + table verbatim so a fresh
    // import can rehydrate them.
    try std.testing.expect(std.mem.indexOf(u8, bundle, "nano_round_trip_300") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "fixed_token") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "external_links_raw") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle, "https://example.com/policy") != null);

    var dest = try Database.openInMemory();
    defer dest.close();
    try dest.applyStandaloneSchema();
    try importBundleJson(dest, std.testing.allocator, bundle);

    const Counts = struct {
        fn one(d: Database, sql: []const u8) !i64 {
            const stmt = try facet_sqlite.prepare(d, sql);
            defer facet_sqlite.finalize(stmt);
            try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt));
            return c.sqlite3_column_int64(stmt, 0);
        }
    };

    try std.testing.expectEqual(@as(i64, 2), try Counts.one(dest, "SELECT COUNT(*) FROM collections WHERE workspace_id = 'ws_xc'"));
    try std.testing.expectEqual(@as(i64, 3), try Counts.one(dest, "SELECT COUNT(*) FROM documents_raw WHERE workspace_id = 'ws_xc'"));
    try std.testing.expectEqual(@as(i64, 3), try Counts.one(dest, "SELECT COUNT(*) FROM chunks_raw WHERE workspace_id = 'ws_xc'"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM entities_raw WHERE workspace_id = 'ws_xc'"));
    try std.testing.expectEqual(@as(i64, 2), try Counts.one(dest, "SELECT COUNT(*) FROM entity_documents_raw WHERE workspace_id = 'ws_xc'"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM entity_chunks_raw WHERE workspace_id = 'ws_xc'"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM document_links_raw WHERE workspace_id = 'ws_xc'"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM external_links_raw WHERE workspace_id = 'ws_xc'"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM documents_raw WHERE doc_nanoid = 'nano_round_trip_300'"));
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(dest, "SELECT COUNT(*) FROM chunks_raw WHERE strategy = 'fixed_token' AND token_count = 3"));

    // Collection-scoped export should keep the cross-collection link because at
    // least one endpoint matches the requested collection, but should drop
    // documents that live exclusively in the other collection.
    const partial = try exportToJson(std.testing.allocator, db, .{
        .collection = .{ .workspace_id = "ws_xc", .collection_id = "ws_xc::legal" },
    });
    defer std.testing.allocator.free(partial);
    try std.testing.expect(std.mem.indexOf(u8, partial, "ws_xc::legal") != null);
    try std.testing.expect(std.mem.indexOf(u8, partial, "implements") != null);
    try std.testing.expect(std.mem.indexOf(u8, partial, "\"doc_id\": 100") != null);
    // Documents that belong only to the other collection must not leak.
    try std.testing.expect(std.mem.indexOf(u8, partial, "Storage retention spec") == null);
}
