const std = @import("std");
const roaring = @import("roaring.zig");

pub const DocId = u64;
pub const NodeId = u64;
pub const VectorId = u64;

pub const DocScore = struct {
    doc_id: DocId,
    score: f64,
};

pub const HybridSearchRequest = struct {
    bm25_table_id: u64,
    bm25_term_hashes: []const u64,
    vector: ?VectorSearchRequest = null,
    vector_weight: f64 = 0.5,
    limit: usize = 20,
};

pub const HybridSearchMatch = struct {
    doc_id: DocId,
    bm25_score: f64,
    vector_score: f64,
    combined_score: f64,
};

pub const VectorDistanceMetric = enum {
    cosine,
    l2,
    inner_product,
};

pub const VectorSearchMatch = struct {
    doc_id: DocId,
    distance: f64,
    similarity: f64,
};

pub const EmbeddingRecord = struct {
    doc_id: DocId,
    dimensions: usize,
    values: []const f32,
};

pub const GraphEdgeFilter = struct {
    edge_types: ?[]const []const u8 = null,
    doc_types: ?[]const []const u8 = null,
    jurisdictions: ?[]const []const u8 = null,
    after_unix_seconds: ?i64 = null,
    before_unix_seconds: ?i64 = null,
    confidence_min: ?f32 = null,
    confidence_max: ?f32 = null,
};

pub const GraphNeighborStep = struct {
    from_node: NodeId,
    to_node: NodeId,
    relation_id: u32,
};

pub const FacetValueFilter = struct {
    facet_name: []const u8,
    values: []const []const u8,
};

pub const FacetPosting = struct {
    chunk_id: u32,
    bitmap: roaring.Bitmap,
};

pub const FacetCount = struct {
    facet_name: []const u8,
    facet_value: []const u8,
    cardinality: u64,
    facet_id: u32,
};

pub const FacetValueNode = struct {
    value_id: u32,
    facet_id: u32,
    facet_value: []const u8,
    children_bitmap: ?roaring.Bitmap = null,
};

pub const FacetTableConfig = struct {
    table_id: u64,
    chunk_bits: u8,
    schema_name: []const u8,
    table_name: []const u8,
};

pub const CollectionStats = struct {
    total_documents: u64,
    avg_document_length: f64,
};

pub const CollectionStatsEntry = struct {
    table_id: u64,
    stats: CollectionStats,
};

pub const DocumentStats = struct {
    doc_id: DocId,
    document_length: u32,
    unique_terms: u32,
};

pub const TermStat = struct {
    term_hash: u64,
    document_frequency: u64,
};

pub const TermFrequency = struct {
    term_hash: u64,
    frequency: u32,
};

pub const UpsertDocumentRequest = struct {
    table_id: u64,
    doc_id: DocId,
    content: []const u8,
    language: []const u8,
};

pub const UpsertEmbeddingRequest = struct {
    table_id: u64,
    doc_id: DocId,
    values: []const f32,
};

pub const WorkspaceRecord = struct {
    workspace_id: []const u8,
    domain_profile_json: []const u8,
};

pub const FacetRepository = struct {
    ctx: *anyopaque,
    getTableConfigFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, table_name: []const u8) anyerror!FacetTableConfig,
    getFacetIdFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, facet_name: []const u8) anyerror!?u32,
    listFacetValuesFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, facet_id: u32) anyerror![][]const u8,
    getFacetValueNodeFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, facet_id: u32, facet_value: []const u8) anyerror!?FacetValueNode,
    getFacetChildrenFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, value_id: u32) anyerror![]FacetValueNode,
    getPostingsFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, facet_id: u32, values: []const []const u8) anyerror![]FacetPosting,
};

pub const Bm25Repository = struct {
    ctx: *anyopaque,
    getCollectionStatsFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64) anyerror!CollectionStats,
    getDocumentStatsFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: DocId) anyerror!?DocumentStats,
    getTermStatsFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, term_hashes: []const u64) anyerror![]TermStat,
    getTermFrequenciesFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: DocId, term_hashes: []const u64) anyerror![]TermFrequency,
    getPostingBitmapFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, term_hash: u64) anyerror!?roaring.Bitmap,
    upsertDocumentFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, request: UpsertDocumentRequest) anyerror!void,
    deleteDocumentFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: DocId) anyerror!void,
};

pub const GraphRepository = struct {
    ctx: *anyopaque,
    getOutgoingEdgesFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, nodes: roaring.Bitmap) anyerror!roaring.Bitmap,
    getIncomingEdgesFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, nodes: roaring.Bitmap) anyerror!roaring.Bitmap,
    filterEdgesFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, edges: roaring.Bitmap, filter: GraphEdgeFilter) anyerror!roaring.Bitmap,
    projectNeighborNodesFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, frontier: roaring.Bitmap, edges: roaring.Bitmap) anyerror!roaring.Bitmap,
    expandNeighborStepsFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, frontier: roaring.Bitmap, edges: roaring.Bitmap) anyerror![]GraphNeighborStep,
    expandFrontierStepsFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, frontier: roaring.Bitmap, filter: GraphEdgeFilter) anyerror![]GraphNeighborStep,
};

pub const VectorRepository = struct {
    ctx: *anyopaque,
    getEmbeddingDimensionsFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, table_name: []const u8, column_name: []const u8) anyerror!?usize,
    searchNearestFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, request: VectorSearchRequest) anyerror![]VectorSearchMatch,
    upsertEmbeddingFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, request: UpsertEmbeddingRequest) anyerror!void,
    deleteEmbeddingFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, table_id: u64, doc_id: DocId) anyerror!void,
};

pub const VectorSearchRequest = struct {
    table_name: []const u8,
    key_column: []const u8,
    vector_column: []const u8,
    query_vector: []const f32,
    limit: usize,
    metric: VectorDistanceMetric = .cosine,
};

pub const OntologyRepository = struct {
    ctx: *anyopaque,
    resolveWorkspaceFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, workspace_id: []const u8) anyerror!?WorkspaceRecord,
};
