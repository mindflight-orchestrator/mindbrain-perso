const std = @import("std");
const interfaces = @import("interfaces.zig");
const search_store = @import("search_store.zig");
const facet_store = @import("facet_store.zig");
const facet_sqlite = @import("facet_sqlite.zig");
const graph_store = @import("graph_store.zig");
const graph_sqlite = @import("graph_sqlite.zig");
const search_sqlite = @import("search_sqlite.zig");
const collections_sqlite = @import("collections_sqlite.zig");
const chunker = @import("chunker.zig");
const nanoid = @import("nanoid.zig");
const vector_blob = @import("vector_blob.zig");
const tokenization_sqlite = @import("tokenization_sqlite.zig");

pub const FacetRegistration = struct {
    table_id: u64,
    schema_name: []const u8,
    table_name: []const u8,
    chunk_bits: u8,
    facets: []const FacetDefinition,
};

pub const FacetDefinition = struct {
    facet_id: u32,
    facet_name: []const u8,
};

pub const FacetAssignment = struct {
    facet_id: u32,
    facet_name: []const u8,
    facet_value: []const u8,
};

pub const DocumentImport = struct {
    table_id: u64,
    doc_id: u64,
    content: []const u8,
    language: []const u8 = "english",
    embedding: ?[]const f32 = null,
    facets: []const FacetAssignment = &.{},
};

pub const EntityImport = struct {
    entity_id: u32,
    entity_type: []const u8,
    name: []const u8,
};

pub const RelationImport = struct {
    relation_id: u32,
    source_id: u32,
    target_id: u32,
    relation_type: []const u8,
    confidence: f32 = 1.0,
    valid_from_unix: ?i64 = null,
    valid_to_unix: ?i64 = null,
};

pub const EntityDocumentImport = struct {
    entity_id: u32,
    doc_id: u64,
    table_id: u64,
    role: ?[]const u8 = null,
    confidence: f32 = 1.0,
};

pub const Pipeline = struct {
    allocator: std.mem.Allocator,
    db: *const facet_sqlite.Database,
    search: *search_store.Store,
    facets: *facet_store.Store,
    graph: *graph_store.Store,
    /// When set, raw-first writes also record canonical rows in
    /// documents_raw, chunks_raw, and entity_*_raw under this workspace.
    workspace_id: ?[]const u8 = null,
    /// When set together with `workspace_id`, ingestDocument also persists
    /// the document into `documents_raw` for this collection.
    collection_id: ?[]const u8 = null,

    // ---- Legacy registration / ingest (unchanged signatures) -------------

    pub fn registerFacetTable(self: *Pipeline, registration: FacetRegistration) !void {
        try self.facets.registerTable(.{
            .table_id = registration.table_id,
            .chunk_bits = registration.chunk_bits,
            .schema_name = registration.schema_name,
            .table_name = registration.table_name,
        });
        try facet_sqlite.insertFacetTable(self.db.*, registration.table_id, registration.schema_name, registration.table_name, registration.chunk_bits);

        for (registration.facets) |facet| {
            try self.facets.registerFacet(registration.table_id, facet.facet_id, facet.facet_name);
            try facet_sqlite.insertFacetDefinition(self.db.*, registration.table_id, facet.facet_id, facet.facet_name);
        }
    }

    pub fn ingestDocument(self: *Pipeline, document: DocumentImport) !void {
        try self.search.upsertDocument(.{
            .table_id = document.table_id,
            .doc_id = document.doc_id,
            .content = document.content,
            .language = document.language,
        });
        try search_sqlite.syncSearchDocumentIfTriggered(self.db.*, self.allocator, document.table_id, document.doc_id, document.content, document.language);

        if (document.embedding) |embedding| {
            try self.search.upsertEmbedding(.{
                .table_id = document.table_id,
                .doc_id = document.doc_id,
                .values = embedding,
            });
            try search_sqlite.upsertSearchEmbedding(self.db.*, self.allocator, document.table_id, document.doc_id, embedding);
        }

        try syncFacetAssignments(self, document.table_id, document.doc_id, document.facets);

        // Mirror into the raw layer when the pipeline is bound to a workspace+collection.
        if (self.workspace_id) |ws| if (self.collection_id) |coll| {
            try collections_sqlite.upsertDocumentRaw(self.db.*, .{
                .workspace_id = ws,
                .collection_id = coll,
                .doc_id = document.doc_id,
                .content = document.content,
                .language = document.language,
            });
            if (document.embedding) |embedding| {
                const blob = try vector_blob.encodeF32Le(self.allocator, embedding);
                defer self.allocator.free(blob);
                try collections_sqlite.upsertDocumentVector(self.db.*, .{
                    .workspace_id = ws,
                    .collection_id = coll,
                    .doc_id = document.doc_id,
                    .dim = @intCast(embedding.len),
                    .embedding_blob = blob,
                });
            }
        };
    }

    pub fn upsertEntity(self: *Pipeline, entity: EntityImport) !void {
        try graph_sqlite.upsertEntity(self.db.*, entity.entity_id, entity.entity_type, entity.name);
    }

    pub fn addRelation(self: *Pipeline, relation: RelationImport) !void {
        try self.graph.addRelation(.{
            .relation_id = relation.relation_id,
            .source_id = relation.source_id,
            .target_id = relation.target_id,
            .relation_type = relation.relation_type,
            .confidence = relation.confidence,
            .valid_from_unix = relation.valid_from_unix,
            .valid_to_unix = relation.valid_to_unix,
        });
        try graph_sqlite.upsertRelation(self.db.*, .{
            .relation_id = relation.relation_id,
            .source_id = relation.source_id,
            .target_id = relation.target_id,
            .relation_type = relation.relation_type,
            .confidence = relation.confidence,
            .valid_from_unix = relation.valid_from_unix,
            .valid_to_unix = relation.valid_to_unix,
        });

        try syncAdjacencyForNode(self, relation.source_id, true);
        try syncAdjacencyForNode(self, relation.target_id, false);
    }

    pub fn linkEntityDocument(self: *Pipeline, link: EntityDocumentImport) !void {
        try graph_sqlite.insertEntityDocument(self.db.*, link.entity_id, link.doc_id, link.table_id, link.role, link.confidence);
    }

    // ---- Workspace / collection / ontology ------------------------------

    pub fn createWorkspace(self: *Pipeline, spec: collections_sqlite.WorkspaceSpec) !void {
        try collections_sqlite.ensureWorkspace(self.db.*, spec);
    }

    pub fn createCollection(self: *Pipeline, spec: collections_sqlite.CollectionSpec) !void {
        try collections_sqlite.ensureCollection(self.db.*, spec);
    }

    pub fn registerOntology(self: *Pipeline, bundle: collections_sqlite.OntologyBundle) !void {
        try collections_sqlite.loadOntologyBundle(self.db.*, bundle);
    }

    pub fn attachOntologyToCollection(
        self: *Pipeline,
        workspace_id: []const u8,
        collection_id: []const u8,
        ontology_id: []const u8,
        role: []const u8,
    ) !void {
        try collections_sqlite.attachOntologyToCollection(self.db.*, workspace_id, collection_id, ontology_id, role);
    }

    pub fn setActiveCollection(self: *Pipeline, workspace_id: []const u8, collection_id: []const u8) void {
        self.workspace_id = workspace_id;
        self.collection_id = collection_id;
    }

    // ---- Raw-first ingest ------------------------------------------------

    pub fn ingestDocumentRaw(self: *Pipeline, spec: collections_sqlite.DocumentRawSpec) !void {
        try collections_sqlite.upsertDocumentRaw(self.db.*, spec);
    }

    pub fn ingestChunkRaw(self: *Pipeline, spec: collections_sqlite.ChunkRawSpec) !void {
        try collections_sqlite.upsertChunkRaw(self.db.*, spec);
    }

    pub fn ingestDocumentVector(
        self: *Pipeline,
        workspace_id: []const u8,
        collection_id: []const u8,
        doc_id: u64,
        values: []const f32,
    ) !void {
        const blob = try vector_blob.encodeF32Le(self.allocator, values);
        defer self.allocator.free(blob);
        try collections_sqlite.upsertDocumentVector(self.db.*, .{
            .workspace_id = workspace_id,
            .collection_id = collection_id,
            .doc_id = doc_id,
            .dim = @intCast(values.len),
            .embedding_blob = blob,
        });
    }

    pub fn ingestChunkVector(
        self: *Pipeline,
        workspace_id: []const u8,
        collection_id: []const u8,
        doc_id: u64,
        chunk_index: u32,
        values: []const f32,
    ) !void {
        const blob = try vector_blob.encodeF32Le(self.allocator, values);
        defer self.allocator.free(blob);
        try collections_sqlite.upsertChunkVector(self.db.*, .{
            .workspace_id = workspace_id,
            .collection_id = collection_id,
            .doc_id = doc_id,
            .chunk_index = chunk_index,
            .dim = @intCast(values.len),
            .embedding_blob = blob,
        });
    }

    pub fn assignFacetRaw(self: *Pipeline, spec: collections_sqlite.FacetAssignmentRawSpec) !void {
        try collections_sqlite.upsertFacetAssignmentRaw(self.db.*, spec);
    }

    pub fn upsertEntityFull(self: *Pipeline, spec: collections_sqlite.EntityRawSpec) !void {
        try collections_sqlite.upsertEntityRaw(self.db.*, spec);
        const eid32: u32 = std.math.cast(u32, spec.entity_id) orelse return error.ValueOutOfRange;
        try graph_sqlite.upsertEntityFull(self.db.*, eid32, spec.workspace_id, spec.entity_type, spec.name, @floatCast(spec.confidence), spec.metadata_json);
    }

    pub fn upsertEntityAlias(self: *Pipeline, spec: collections_sqlite.EntityAliasRawSpec) !void {
        try collections_sqlite.upsertEntityAliasRaw(self.db.*, spec);
    }

    pub fn addRelationFull(self: *Pipeline, spec: collections_sqlite.RelationRawSpec) !void {
        try collections_sqlite.upsertRelationRaw(self.db.*, spec);
        const rid: u32 = std.math.cast(u32, spec.relation_id) orelse return error.ValueOutOfRange;
        const src: u32 = std.math.cast(u32, spec.source_entity_id) orelse return error.ValueOutOfRange;
        const tgt: u32 = std.math.cast(u32, spec.target_entity_id) orelse return error.ValueOutOfRange;
        const valid_from_unix = try unixepochText(self.db.*, spec.valid_from);
        const valid_to_unix = try unixepochText(self.db.*, spec.valid_to);
        try self.addRelation(.{
            .relation_id = rid,
            .source_id = src,
            .target_id = tgt,
            .relation_type = spec.edge_type,
            .confidence = @floatCast(spec.confidence),
            .valid_from_unix = valid_from_unix,
            .valid_to_unix = valid_to_unix,
        });
        try graph_sqlite.upsertRelationFull(self.db.*, rid, spec.workspace_id, spec.edge_type, src, tgt, valid_from_unix, valid_to_unix, @floatCast(spec.confidence), spec.metadata_json);
    }

    pub fn linkEntityToDocument(self: *Pipeline, spec: collections_sqlite.EntityDocumentRawSpec, table_id: ?u64) !void {
        try collections_sqlite.linkEntityDocumentRaw(self.db.*, spec);
        if (table_id) |tid| {
            const eid32: u32 = std.math.cast(u32, spec.entity_id) orelse return error.ValueOutOfRange;
            try graph_sqlite.insertEntityDocument(self.db.*, eid32, spec.doc_id, tid, spec.role, @floatCast(spec.confidence));
        }
    }

    pub fn linkEntityToChunk(self: *Pipeline, spec: collections_sqlite.EntityChunkRawSpec) !void {
        try collections_sqlite.linkEntityChunkRaw(self.db.*, spec);
        const eid32: u32 = std.math.cast(u32, spec.entity_id) orelse return error.ValueOutOfRange;
        try graph_sqlite.upsertEntityChunk(self.db.*, eid32, spec.workspace_id, spec.collection_id, spec.doc_id, spec.chunk_index, spec.role, @floatCast(spec.confidence), "{}");
    }

    pub fn linkDocuments(self: *Pipeline, spec: collections_sqlite.DocumentLinkRawSpec) !void {
        try collections_sqlite.upsertDocumentLinkRaw(self.db.*, spec);
    }

    /// Records a document → external URI relation in `external_links_raw`.
    /// Useful for capturing "outbound link" relationships after a document
    /// has been parsed (e.g. URLs found in markdown, references in a PDF).
    pub fn linkExternal(self: *Pipeline, spec: collections_sqlite.ExternalLinkRawSpec) !void {
        try collections_sqlite.upsertExternalLinkRaw(self.db.*, spec);
    }

    // ---- Chunked ingest -------------------------------------------------

    pub const IngestChunkedOptions = struct {
        workspace_id: []const u8,
        collection_id: []const u8,
        doc_id: u64,
        /// When the empty string, a fresh `nanoid` is generated and assigned.
        doc_nanoid: []const u8 = "",
        content: []const u8,
        language: []const u8 = "english",
        source_ref: ?[]const u8 = null,
        ingested_at: ?[]const u8 = null,
        /// Owner of the `source` namespace; defaults to the per-workspace
        /// `<workspace_id>::default` ontology, which `ensureWorkspace` keeps
        /// bootstrapped.
        ontology_id: ?[]const u8 = null,
        chunker_options: chunker.Options = .{},
        /// When set, parent-document content is also pushed through the
        /// search BM25 store under this `table_id`.
        bm25_table_id: ?u64 = null,
        /// When set, every chunk content is pushed through BM25 under this
        /// `table_id` using a synthetic doc id of
        /// `(doc_id << chunk_bits) | chunk_index`.
        chunk_bm25_table_id: ?u64 = null,
        /// Bit shift used when forming the synthetic chunk doc id.
        chunk_bits: u6 = 8,
    };

    pub const IngestChunkedResult = struct {
        /// Final canonical nanoid for the document. The caller owns this
        /// slice (allocated on `Pipeline.allocator`).
        doc_nanoid: []u8,
        chunk_count: u32,

        pub fn deinit(self: *IngestChunkedResult, allocator: std.mem.Allocator) void {
            allocator.free(self.doc_nanoid);
        }
    };

    /// End-to-end ingest of a single document: assigns / preserves a
    /// `doc_nanoid`, runs the chunker, persists chunks + auto-extracted
    /// `source.*` facets, and (optionally) syncs the parent and chunk
    /// content into BM25.
    pub fn ingestDocumentChunked(
        self: *Pipeline,
        opts: IngestChunkedOptions,
    ) !IngestChunkedResult {
        var generated_nanoid: ?[]u8 = null;
        errdefer if (generated_nanoid) |b| self.allocator.free(b);

        const nanoid_value: []const u8 = if (opts.doc_nanoid.len == 0) blk: {
            generated_nanoid = try nanoid.generateDefault(self.allocator);
            break :blk generated_nanoid.?;
        } else opts.doc_nanoid;

        try collections_sqlite.upsertDocumentRaw(self.db.*, .{
            .workspace_id = opts.workspace_id,
            .collection_id = opts.collection_id,
            .doc_id = opts.doc_id,
            .doc_nanoid = nanoid_value,
            .content = opts.content,
            .language = opts.language,
            .source_ref = opts.source_ref,
        });

        var owned_ontology: ?[]const u8 = null;
        defer if (owned_ontology) |o| self.allocator.free(o);
        const ontology_id: []const u8 = if (opts.ontology_id) |o| o else blk: {
            owned_ontology = try collections_sqlite.ensureDefaultOntology(
                self.db.*,
                self.allocator,
                opts.workspace_id,
            );
            break :blk owned_ontology.?;
        };

        const chunks = try chunker.chunk(self.allocator, opts.content, opts.chunker_options);
        defer chunker.freeChunks(self.allocator, chunks);
        const total_chunks: u32 = @intCast(chunks.len);

        for (chunks) |ch| {
            try collections_sqlite.upsertChunkRaw(self.db.*, .{
                .workspace_id = opts.workspace_id,
                .collection_id = opts.collection_id,
                .doc_id = opts.doc_id,
                .chunk_index = ch.index,
                .content = ch.content,
                .language = opts.language,
                .offset_start = ch.offset_start,
                .offset_end = ch.offset_end,
                .strategy = ch.strategy,
                .token_count = ch.token_count,
                .parent_chunk_index = ch.parent_chunk_index,
            });

            var source_facets = try chunker.deriveSourceFacets(self.allocator, .{
                .workspace_id = opts.workspace_id,
                .collection_id = opts.collection_id,
                .ontology_id = ontology_id,
                .doc_id = opts.doc_id,
                .ingested_at = opts.ingested_at,
                .source_ref = opts.source_ref,
            }, ch, total_chunks);
            defer source_facets.deinit();
            for (source_facets.rows) |row| {
                try collections_sqlite.upsertFacetAssignmentRaw(self.db.*, row);
            }

            if (opts.chunk_bm25_table_id) |chunk_tid| {
                const synthetic_id = chunkSyntheticId(opts.doc_id, ch.index, opts.chunk_bits);
                try self.search.upsertDocument(.{
                    .table_id = chunk_tid,
                    .doc_id = synthetic_id,
                    .content = ch.content,
                    .language = opts.language,
                });
                try search_sqlite.syncSearchDocumentIfTriggered(
                    self.db.*,
                    self.allocator,
                    chunk_tid,
                    synthetic_id,
                    ch.content,
                    opts.language,
                );
            }
        }

        if (opts.bm25_table_id) |tid| {
            try self.search.upsertDocument(.{
                .table_id = tid,
                .doc_id = opts.doc_id,
                .content = opts.content,
                .language = opts.language,
            });
            try search_sqlite.syncSearchDocumentIfTriggered(
                self.db.*,
                self.allocator,
                tid,
                opts.doc_id,
                opts.content,
                opts.language,
            );
        }

        const owned_nanoid: []u8 = if (generated_nanoid) |b| blk: {
            generated_nanoid = null;
            break :blk b;
        } else try self.allocator.dupe(u8, nanoid_value);

        return .{
            .doc_nanoid = owned_nanoid,
            .chunk_count = total_chunks,
        };
    }

    // ---- Reindex ---------------------------------------------------------

    pub const ReindexBm25Options = struct {
        /// Table id whose BM25 trigger receives parent-document rows.
        table_id: u64,
        /// Optional table id whose BM25 trigger receives per-chunk rows.
        /// Chunks are streamed under a synthetic doc id of
        /// `(doc_id << chunk_bits) | chunk_index` so that they don't
        /// collide with the parent rows.
        chunk_table_id: ?u64 = null,
        chunk_bits: u6 = 8,
    };

    pub const ReindexBm25Counts = struct {
        documents: u64,
        chunks: u64,
    };

    /// Replays documents_raw (and optionally chunks_raw) rows for
    /// `(workspace_id, collection_id)` through the BM25 trigger path so the
    /// search index can be rebuilt from raw data.
    pub fn reindexBm25(
        self: *Pipeline,
        workspace_id: []const u8,
        collection_id: []const u8,
        options: ReindexBm25Options,
    ) !ReindexBm25Counts {
        var counts: ReindexBm25Counts = .{ .documents = 0, .chunks = 0 };

        {
            const sql =
                \\SELECT doc_id, content, COALESCE(language, 'english')
                \\FROM documents_raw
                \\WHERE workspace_id = ?1 AND collection_id = ?2
                \\ORDER BY doc_id
            ;
            const stmt = try facet_sqlite.prepare(self.db.*, sql);
            defer facet_sqlite.finalize(stmt);
            try facet_sqlite.bindText(stmt, 1, workspace_id);
            try facet_sqlite.bindText(stmt, 2, collection_id);

            while (true) {
                const status = facet_sqlite.c.sqlite3_step(stmt);
                if (status == facet_sqlite.c.SQLITE_DONE) break;
                if (status != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;

                const doc_id = try facet_sqlite.columnU64(stmt, 0);
                const content = try facet_sqlite.dupeColumnText(self.allocator, stmt, 1);
                defer self.allocator.free(content);
                const language = try facet_sqlite.dupeColumnText(self.allocator, stmt, 2);
                defer self.allocator.free(language);

                try self.search.upsertDocument(.{
                    .table_id = options.table_id,
                    .doc_id = doc_id,
                    .content = content,
                    .language = language,
                });
                try search_sqlite.syncSearchDocumentIfTriggered(self.db.*, self.allocator, options.table_id, doc_id, content, language);
                counts.documents += 1;
            }
        }

        if (options.chunk_table_id) |chunk_tid| {
            const sql =
                \\SELECT doc_id, chunk_index, content, COALESCE(language, 'english')
                \\FROM chunks_raw
                \\WHERE workspace_id = ?1 AND collection_id = ?2
                \\ORDER BY doc_id, chunk_index
            ;
            const stmt = try facet_sqlite.prepare(self.db.*, sql);
            defer facet_sqlite.finalize(stmt);
            try facet_sqlite.bindText(stmt, 1, workspace_id);
            try facet_sqlite.bindText(stmt, 2, collection_id);

            while (true) {
                const status = facet_sqlite.c.sqlite3_step(stmt);
                if (status == facet_sqlite.c.SQLITE_DONE) break;
                if (status != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;

                const doc_id = try facet_sqlite.columnU64(stmt, 0);
                const chunk_index_i64: i64 = facet_sqlite.c.sqlite3_column_int64(stmt, 1);
                const chunk_index: u32 = @intCast(chunk_index_i64);
                const content = try facet_sqlite.dupeColumnText(self.allocator, stmt, 2);
                defer self.allocator.free(content);
                const language = try facet_sqlite.dupeColumnText(self.allocator, stmt, 3);
                defer self.allocator.free(language);

                const synthetic_id = chunkSyntheticId(doc_id, chunk_index, options.chunk_bits);
                try self.search.upsertDocument(.{
                    .table_id = chunk_tid,
                    .doc_id = synthetic_id,
                    .content = content,
                    .language = language,
                });
                try search_sqlite.syncSearchDocumentIfTriggered(self.db.*, self.allocator, chunk_tid, synthetic_id, content, language);
                counts.chunks += 1;
            }
        }

        return counts;
    }

    /// Replays facet_assignments_raw for the given collection through the
    /// existing facet_postings index. Each (workspace, collection) is mapped
    /// to a single previously-registered facet table; facets are looked up by
    /// "namespace.dimension" name.
    pub fn reindexFacets(
        self: *Pipeline,
        workspace_id: []const u8,
        collection_id: []const u8,
        table_id: u64,
    ) !u64 {
        const sql =
            \\SELECT doc_id, namespace, dimension, value
            \\FROM facet_assignments_raw
            \\WHERE workspace_id = ?1 AND collection_id = ?2 AND target_kind = 'doc'
            \\ORDER BY doc_id
        ;
        const stmt = try facet_sqlite.prepare(self.db.*, sql);
        defer facet_sqlite.finalize(stmt);
        try facet_sqlite.bindText(stmt, 1, workspace_id);
        try facet_sqlite.bindText(stmt, 2, collection_id);

        var current_doc: ?u64 = null;
        var pending = std.ArrayList(FacetAssignment).empty;
        defer {
            for (pending.items) |item| {
                self.allocator.free(item.facet_name);
                self.allocator.free(item.facet_value);
            }
            pending.deinit(self.allocator);
        }

        var total: u64 = 0;
        while (true) {
            const status = facet_sqlite.c.sqlite3_step(stmt);
            if (status == facet_sqlite.c.SQLITE_DONE) break;
            if (status != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;

            const doc_id = try facet_sqlite.columnU64(stmt, 0);
            const ns = try facet_sqlite.dupeColumnText(self.allocator, stmt, 1);
            defer self.allocator.free(ns);
            const dim = try facet_sqlite.dupeColumnText(self.allocator, stmt, 2);
            defer self.allocator.free(dim);
            const value = try facet_sqlite.dupeColumnText(self.allocator, stmt, 3);

            if (current_doc) |cd| {
                if (cd != doc_id) {
                    try flushFacetGroup(self, table_id, cd, &pending);
                }
            }
            current_doc = doc_id;

            const facet_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{ ns, dim });
            const facet_id: u32 = facet_sqlite.loadFacetId(self.db.*, table_id, facet_name) catch |err| switch (err) {
                error.MissingRow => 0,
                else => return err,
            };
            try pending.append(self.allocator, .{
                .facet_id = facet_id,
                .facet_name = facet_name,
                .facet_value = value,
            });
            total += 1;
        }
        if (current_doc) |cd| try flushFacetGroup(self, table_id, cd, &pending);
        return total;
    }

    /// Replays entities_raw + entity_aliases_raw + relations_raw into the
    /// graph store and adjacency tables. Currently this assumes entity ids
    /// fit in u32 (which is the legacy graph_entity contract).
    pub fn reindexGraph(self: *Pipeline, workspace_id: []const u8) !u64 {
        return try self.reindexGraphWithDocumentTable(workspace_id, null);
    }

    /// Replays the raw graph layer into the derived graph tables. When
    /// `document_table_id` is provided, raw entity-document links are projected
    /// into `graph_entity_document`; chunk links are always projected into
    /// `graph_entity_chunk`.
    pub fn reindexGraphWithDocumentTable(self: *Pipeline, workspace_id: []const u8, document_table_id: ?u64) !u64 {
        var projected_count: u64 = 0;

        // Entities first so relations can reference them.
        {
            const sql =
                \\SELECT entity_id, entity_type, name, confidence, metadata_json
                \\FROM entities_raw
                \\WHERE workspace_id = ?1
                \\ORDER BY entity_id
            ;
            const stmt = try facet_sqlite.prepare(self.db.*, sql);
            defer facet_sqlite.finalize(stmt);
            try facet_sqlite.bindText(stmt, 1, workspace_id);
            while (true) {
                const status = facet_sqlite.c.sqlite3_step(stmt);
                if (status == facet_sqlite.c.SQLITE_DONE) break;
                if (status != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;

                const entity_id = try facet_sqlite.columnU64(stmt, 0);
                const eid32: u32 = std.math.cast(u32, entity_id) orelse return error.ValueOutOfRange;
                const etype = try facet_sqlite.dupeColumnText(self.allocator, stmt, 1);
                defer self.allocator.free(etype);
                const name = try facet_sqlite.dupeColumnText(self.allocator, stmt, 2);
                defer self.allocator.free(name);
                const confidence: f32 = @floatCast(facet_sqlite.c.sqlite3_column_double(stmt, 3));
                const metadata_json = try facet_sqlite.dupeColumnText(self.allocator, stmt, 4);
                defer self.allocator.free(metadata_json);
                try graph_sqlite.upsertEntityFull(self.db.*, eid32, workspace_id, etype, name, confidence, metadata_json);
                projected_count += 1;
            }
        }

        {
            const sql =
                \\SELECT entity_id, term, confidence
                \\FROM entity_aliases_raw
                \\WHERE workspace_id = ?1
                \\ORDER BY entity_id, term
            ;
            const stmt = try facet_sqlite.prepare(self.db.*, sql);
            defer facet_sqlite.finalize(stmt);
            try facet_sqlite.bindText(stmt, 1, workspace_id);
            while (true) {
                const status = facet_sqlite.c.sqlite3_step(stmt);
                if (status == facet_sqlite.c.SQLITE_DONE) break;
                if (status != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;

                const eid64 = try facet_sqlite.columnU64(stmt, 0);
                const eid: u32 = std.math.cast(u32, eid64) orelse return error.ValueOutOfRange;
                const term = try facet_sqlite.dupeColumnText(self.allocator, stmt, 1);
                defer self.allocator.free(term);
                const confidence: f32 = @floatCast(facet_sqlite.c.sqlite3_column_double(stmt, 2));
                try graph_sqlite.insertEntityAlias(self.db.*, term, eid, confidence);
                projected_count += 1;
            }
        }

        const sql =
            \\SELECT relation_id, edge_type, source_entity_id, target_entity_id, unixepoch(valid_from), unixepoch(valid_to), confidence, metadata_json
            \\FROM relations_raw
            \\WHERE workspace_id = ?1
            \\ORDER BY relation_id
        ;
        const stmt = try facet_sqlite.prepare(self.db.*, sql);
        defer facet_sqlite.finalize(stmt);
        try facet_sqlite.bindText(stmt, 1, workspace_id);
        while (true) {
            const status = facet_sqlite.c.sqlite3_step(stmt);
            if (status == facet_sqlite.c.SQLITE_DONE) break;
            if (status != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;

            const rid64 = try facet_sqlite.columnU64(stmt, 0);
            const rid: u32 = std.math.cast(u32, rid64) orelse return error.ValueOutOfRange;
            const edge = try facet_sqlite.dupeColumnText(self.allocator, stmt, 1);
            defer self.allocator.free(edge);
            const src64 = try facet_sqlite.columnU64(stmt, 2);
            const tgt64 = try facet_sqlite.columnU64(stmt, 3);
            const src: u32 = std.math.cast(u32, src64) orelse return error.ValueOutOfRange;
            const tgt: u32 = std.math.cast(u32, tgt64) orelse return error.ValueOutOfRange;
            const valid_from_unix = columnOptionalI64(stmt, 4);
            const valid_to_unix = columnOptionalI64(stmt, 5);
            const confidence: f32 = @floatCast(facet_sqlite.c.sqlite3_column_double(stmt, 6));
            const metadata_json = try facet_sqlite.dupeColumnText(self.allocator, stmt, 7);
            defer self.allocator.free(metadata_json);

            try self.addRelation(.{
                .relation_id = rid,
                .source_id = src,
                .target_id = tgt,
                .relation_type = edge,
                .confidence = confidence,
                .valid_from_unix = valid_from_unix,
                .valid_to_unix = valid_to_unix,
            });
            try graph_sqlite.upsertRelationFull(self.db.*, rid, workspace_id, edge, src, tgt, valid_from_unix, valid_to_unix, confidence, metadata_json);
            projected_count += 1;
        }

        if (document_table_id) |tid| {
            {
                const delete_sql =
                    \\DELETE FROM graph_entity_document
                    \\WHERE table_id = ?1
                    \\  AND entity_id IN (SELECT entity_id FROM graph_entity WHERE workspace_id = ?2)
                ;
                const delete_stmt = try facet_sqlite.prepare(self.db.*, delete_sql);
                defer facet_sqlite.finalize(delete_stmt);
                try facet_sqlite.bindInt64(delete_stmt, 1, tid);
                try facet_sqlite.bindText(delete_stmt, 2, workspace_id);
                try facet_sqlite.stepDone(delete_stmt);
            }

            const doc_sql =
                \\SELECT entity_id, doc_id, role, MAX(confidence)
                \\FROM entity_documents_raw
                \\WHERE workspace_id = ?1
                \\GROUP BY entity_id, doc_id
                \\ORDER BY entity_id, doc_id
            ;
            const doc_stmt = try facet_sqlite.prepare(self.db.*, doc_sql);
            defer facet_sqlite.finalize(doc_stmt);
            try facet_sqlite.bindText(doc_stmt, 1, workspace_id);
            while (true) {
                const status = facet_sqlite.c.sqlite3_step(doc_stmt);
                if (status == facet_sqlite.c.SQLITE_DONE) break;
                if (status != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;

                const eid64 = try facet_sqlite.columnU64(doc_stmt, 0);
                const eid: u32 = std.math.cast(u32, eid64) orelse return error.ValueOutOfRange;
                const doc_id = try facet_sqlite.columnU64(doc_stmt, 1);
                const role = if (facet_sqlite.c.sqlite3_column_type(doc_stmt, 2) == facet_sqlite.c.SQLITE_NULL)
                    null
                else
                    try facet_sqlite.dupeColumnText(self.allocator, doc_stmt, 2);
                defer if (role) |value| self.allocator.free(value);
                const confidence: f32 = @floatCast(facet_sqlite.c.sqlite3_column_double(doc_stmt, 3));
                try graph_sqlite.upsertEntityDocument(self.db.*, eid, doc_id, tid, role, confidence);
                projected_count += 1;
            }
        }

        {
            const delete_sql = "DELETE FROM graph_entity_chunk WHERE workspace_id = ?1";
            const delete_stmt = try facet_sqlite.prepare(self.db.*, delete_sql);
            defer facet_sqlite.finalize(delete_stmt);
            try facet_sqlite.bindText(delete_stmt, 1, workspace_id);
            try facet_sqlite.stepDone(delete_stmt);
        }

        const chunk_sql =
            \\SELECT entity_id, collection_id, doc_id, chunk_index, role, MAX(confidence)
            \\FROM entity_chunks_raw
            \\WHERE workspace_id = ?1
            \\GROUP BY entity_id, collection_id, doc_id, chunk_index
            \\ORDER BY entity_id, collection_id, doc_id, chunk_index
        ;
        const chunk_stmt = try facet_sqlite.prepare(self.db.*, chunk_sql);
        defer facet_sqlite.finalize(chunk_stmt);
        try facet_sqlite.bindText(chunk_stmt, 1, workspace_id);
        while (true) {
            const status = facet_sqlite.c.sqlite3_step(chunk_stmt);
            if (status == facet_sqlite.c.SQLITE_DONE) break;
            if (status != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;

            const eid64 = try facet_sqlite.columnU64(chunk_stmt, 0);
            const eid: u32 = std.math.cast(u32, eid64) orelse return error.ValueOutOfRange;
            const collection_id = try facet_sqlite.dupeColumnText(self.allocator, chunk_stmt, 1);
            defer self.allocator.free(collection_id);
            const doc_id = try facet_sqlite.columnU64(chunk_stmt, 2);
            const chunk_index: u32 = @intCast(facet_sqlite.c.sqlite3_column_int64(chunk_stmt, 3));
            const role = if (facet_sqlite.c.sqlite3_column_type(chunk_stmt, 4) == facet_sqlite.c.SQLITE_NULL)
                null
            else
                try facet_sqlite.dupeColumnText(self.allocator, chunk_stmt, 4);
            defer if (role) |value| self.allocator.free(value);
            const confidence: f32 = @floatCast(facet_sqlite.c.sqlite3_column_double(chunk_stmt, 5));
            try graph_sqlite.upsertEntityChunk(self.db.*, eid, workspace_id, collection_id, doc_id, chunk_index, role, confidence, "{}");
            projected_count += 1;
        }

        return projected_count;
    }

    pub fn reindexAll(
        self: *Pipeline,
        workspace_id: []const u8,
        collection_id: []const u8,
        table_id: u64,
    ) !void {
        _ = try self.reindexBm25(workspace_id, collection_id, .{ .table_id = table_id });
        _ = try self.reindexFacets(workspace_id, collection_id, table_id);
        _ = try self.reindexGraphWithDocumentTable(workspace_id, table_id);
    }
};

fn chunkSyntheticId(doc_id: u64, chunk_index: u32, chunk_bits: u6) u64 {
    return (doc_id << chunk_bits) | @as(u64, chunk_index);
}

fn columnOptionalI64(stmt: *facet_sqlite.c.sqlite3_stmt, index: c_int) ?i64 {
    if (facet_sqlite.c.sqlite3_column_type(stmt, index) == facet_sqlite.c.SQLITE_NULL) return null;
    return facet_sqlite.c.sqlite3_column_int64(stmt, index);
}

fn unixepochText(db: facet_sqlite.Database, value: ?[]const u8) !?i64 {
    const text = value orelse return null;
    const stmt = try facet_sqlite.prepare(db, "SELECT unixepoch(?1)");
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, text);
    if (facet_sqlite.c.sqlite3_step(stmt) != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;
    return columnOptionalI64(stmt, 0);
}

fn flushFacetGroup(
    self: *Pipeline,
    table_id: u64,
    doc_id: u64,
    pending: *std.ArrayList(FacetAssignment),
) !void {
    if (pending.items.len == 0) return;
    try syncFacetAssignments(self, table_id, doc_id, pending.items);
    for (pending.items) |item| {
        self.allocator.free(item.facet_name);
        self.allocator.free(item.facet_value);
    }
    pending.clearRetainingCapacity();
}

fn syncFacetAssignments(self: *Pipeline, table_id: u64, doc_id: u64, facets: []const FacetAssignment) !void {
    if (facets.len == 0) return;

    const table = findFacetTable(self.facets, table_id) orelse return error.TableNotFound;
    const shift_bits: u6 = @intCast(table.chunk_bits);
    const chunk_id: u32 = @intCast(doc_id >> shift_bits);
    const chunk_mask: u64 = (@as(u64, 1) << shift_bits) - 1;
    const in_chunk_id: u32 = @intCast(doc_id & chunk_mask);

    var assignments = try self.allocator.alloc(facet_sqlite.FacetDocumentAssignment, facets.len);
    defer self.allocator.free(assignments);

    for (facets, 0..) |facet, index| {
        assignments[index] = .{
            .facet_id = facet.facet_id,
            .facet_value = facet.facet_value,
        };
    }

    _ = try facet_sqlite.syncFacetAssignments(self.db.*, table_id, doc_id, assignments);

    for (facets) |facet| {
        try self.facets.addPosting(table_id, facet.facet_id, facet.facet_value, chunk_id, &.{in_chunk_id});
    }
}

fn syncAdjacencyForNode(self: *Pipeline, entity_id: u32, outgoing: bool) !void {
    var relation_ids = std.ArrayList(u32).empty;
    defer relation_ids.deinit(self.allocator);

    for (self.graph.relations.items) |relation| {
        if (outgoing and relation.source_id == entity_id) {
            try relation_ids.append(self.allocator, relation.relation_id);
        } else if (!outgoing and relation.target_id == entity_id) {
            try relation_ids.append(self.allocator, relation.relation_id);
        }
    }

    if (outgoing) {
        try self.graph.setOutgoing(entity_id, relation_ids.items);
        try graph_sqlite.upsertAdjacency(self.db.*, "graph_lj_out", entity_id, relation_ids.items, self.allocator);
    } else {
        try self.graph.setIncoming(entity_id, relation_ids.items);
        try graph_sqlite.upsertAdjacency(self.db.*, "graph_lj_in", entity_id, relation_ids.items, self.allocator);
    }
}

fn findFacetTable(store: *facet_store.Store, table_id: u64) ?interfaces.FacetTableConfig {
    for (store.table_configs.items) |config| {
        if (config.table_id == table_id) return config;
    }
    return null;
}

test "import pipeline keeps search, facet, and graph state in sync" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    var search = search_store.Store.init(std.testing.allocator);
    defer search.deinit();
    var facets = facet_store.Store.init(std.testing.allocator);
    defer facets.deinit();
    var graph = graph_store.Store.init(std.testing.allocator);
    defer graph.deinit();

    var pipeline = Pipeline{
        .allocator = std.testing.allocator,
        .db = &db,
        .search = &search,
        .facets = &facets,
        .graph = &graph,
        .workspace_id = "ws_test",
        .collection_id = "ws_test::docs",
    };

    // Workspace + collection scaffold (raw layer).
    try pipeline.createWorkspace(.{ .workspace_id = "ws_test", .label = "Test workspace" });
    try pipeline.createCollection(.{
        .workspace_id = "ws_test",
        .collection_id = "ws_test::docs",
        .name = "docs",
        .chunk_bits = 4,
    });
    try pipeline.registerOntology(.{
        .header = .{
            .ontology_id = "ws_test::core",
            .workspace_id = "ws_test",
            .name = "core",
        },
        .namespaces = &.{.{ .ontology_id = "ws_test::core", .namespace = "topic" }},
        .dimensions = &.{.{ .ontology_id = "ws_test::core", .namespace = "topic", .dimension = "category" }},
        .entity_types = &.{.{ .ontology_id = "ws_test::core", .entity_type = "person" }},
        .edge_types = &.{.{ .ontology_id = "ws_test::core", .edge_type = "works_for" }},
    });
    try pipeline.attachOntologyToCollection("ws_test", "ws_test::docs", "ws_test::core", "primary");

    try pipeline.registerFacetTable(.{
        .table_id = 1,
        .schema_name = "public",
        .table_name = "docs_fixture",
        .chunk_bits = 4,
        .facets = &.{
            .{ .facet_id = 1, .facet_name = "category" },
            .{ .facet_id = 2, .facet_name = "topic.category" },
        },
    });

    try search_sqlite.setupSearchTable(db, std.testing.allocator, .{
        .table_id = 1,
        .workspace_id = "default",
        .schema_name = "public",
        .table_name = "docs_fixture",
        .key_column = "doc_id",
        .content_column = "content",
        .metadata_column = "metadata",
        .language = "english",
        .populate = false,
    });
    try search_sqlite.bm25CreateSyncTrigger(db, .{
        .table_id = 1,
        .id_column = "doc_id",
        .content_column = "content",
        .language = "english",
    });

    try pipeline.ingestDocument(.{
        .table_id = 1,
        .doc_id = 42,
        .content = "Graph systems use roaring bitmaps",
        .embedding = &.{ 0.8, 0.1 },
        .facets = &.{
            .{ .facet_id = 1, .facet_name = "category", .facet_value = "graph" },
        },
    });

    // Raw-first writes alongside the legacy ingest: a chunk plus a raw facet
    // assignment that the reindex pass will replay into facet_postings.
    try pipeline.ingestChunkRaw(.{
        .workspace_id = "ws_test",
        .collection_id = "ws_test::docs",
        .doc_id = 42,
        .chunk_index = 0,
        .content = "Graph systems",
    });
    try pipeline.assignFacetRaw(.{
        .workspace_id = "ws_test",
        .collection_id = "ws_test::docs",
        .target_kind = .doc,
        .doc_id = 42,
        .ontology_id = "ws_test::core",
        .namespace = "topic",
        .dimension = "category",
        .value = "graph",
    });

    // Use the raw-first variants so reindexGraph can later replay the same
    // entities and relations from entities_raw / relations_raw.
    try pipeline.upsertEntityFull(.{
        .workspace_id = "ws_test",
        .ontology_id = "ws_test::core",
        .entity_id = 1,
        .entity_type = "person",
        .name = "Ada",
        .confidence = 0.91,
        .metadata_json = "{\"collection_id\":\"ws_test::docs\",\"role\":\"engineer\"}",
    });
    try pipeline.upsertEntityFull(.{
        .workspace_id = "ws_test",
        .ontology_id = "ws_test::core",
        .entity_id = 2,
        .entity_type = "company",
        .name = "Acme",
    });
    try pipeline.addRelationFull(.{
        .workspace_id = "ws_test",
        .ontology_id = "ws_test::core",
        .relation_id = 10,
        .edge_type = "works_for",
        .source_entity_id = 1,
        .target_entity_id = 2,
        .valid_from = "2024-01-01",
        .valid_to = "2024-12-31",
        .confidence = 0.9,
        .metadata_json = "{\"source\":\"fixture\"}",
    });
    try pipeline.linkEntityToDocument(.{
        .workspace_id = "ws_test",
        .entity_id = 1,
        .collection_id = "ws_test::docs",
        .doc_id = 42,
        .role = "author",
        .confidence = 0.95,
    }, 1);
    try pipeline.linkEntityToChunk(.{
        .workspace_id = "ws_test",
        .entity_id = 1,
        .collection_id = "ws_test::docs",
        .doc_id = 42,
        .chunk_index = 0,
        .role = "mention",
        .confidence = 0.88,
    });

    // Cross-collection link in raw — exercises the new document_links_raw path.
    try pipeline.createCollection(.{
        .workspace_id = "ws_test",
        .collection_id = "ws_test::brief",
        .name = "brief",
        .chunk_bits = 4,
    });
    try pipeline.ingestDocumentRaw(.{
        .workspace_id = "ws_test",
        .collection_id = "ws_test::brief",
        .doc_id = 7,
        .content = "Brief mentioning graphs",
    });
    try pipeline.linkDocuments(.{
        .workspace_id = "ws_test",
        .link_id = 1,
        .ontology_id = "ws_test::core",
        .edge_type = "explains",
        .source_collection_id = "ws_test::brief",
        .source_doc_id = 7,
        .target_collection_id = "ws_test::docs",
        .target_doc_id = 42,
    });

    const bm25_bitmap = (try search.bm25Repository().getPostingBitmapFn(search.bm25Repository().ctx, std.testing.allocator, 1, tokenization_sqlite.hashTermLexeme("graph"))).?;
    defer {
        var bm = bm25_bitmap;
        bm.deinit();
    }
    try std.testing.expect(bm25_bitmap.contains(42));
    try std.testing.expectEqual(@as(u64, 0), try facet_sqlite.countFacetDeltas(db, 1, null));

    const vectors = try search.vectorRepository().searchNearestFn(search.vectorRepository().ctx, std.testing.allocator, .{
        .table_name = "docs_fixture",
        .key_column = "doc_id",
        .vector_column = "embedding",
        .query_vector = &.{ 0.8, 0.1 },
        .limit = 1,
    });
    defer std.testing.allocator.free(vectors);
    try std.testing.expectEqual(@as(u64, 42), vectors[0].doc_id);

    var facet_result = (try facet_store.filterDocuments(
        std.testing.allocator,
        facets.asRepository(),
        "docs_fixture",
        &.{.{ .facet_name = "category", .values = &.{"graph"} }},
    )).?;
    defer facet_result.deinit();
    try std.testing.expect(facet_result.contains(42));

    const hops = try graph.shortestPathHopsFast(std.testing.allocator, 1, 2, .{}, 2);
    try std.testing.expectEqual(@as(?usize, 1), hops);

    const docs = try graph_sqlite.loadEntityDocuments(db, std.testing.allocator, 1);
    defer {
        for (docs) |doc| if (doc.role) |role| std.testing.allocator.free(role);
        std.testing.allocator.free(docs);
    }
    try std.testing.expectEqual(@as(u64, 42), docs[0].doc_id);

    var graph_runtime = try graph_sqlite.loadRuntime(db, std.testing.allocator);
    defer graph_runtime.deinit();
    const reloaded_hops = try graph_runtime.shortestPathHops(std.testing.allocator, 1, 2, .{}, 2);
    try std.testing.expectEqual(@as(?usize, 1), reloaded_hops);

    var reloaded = try search_sqlite.loadSearchStore(db, std.testing.allocator);
    defer reloaded.deinit();

    const reloaded_repo = reloaded.bm25Repository();
    var reloaded_bitmap = (try reloaded_repo.getPostingBitmapFn(reloaded_repo.ctx, std.testing.allocator, 1, tokenization_sqlite.hashTermLexeme("graph"))).?;
    defer reloaded_bitmap.deinit();
    try std.testing.expect(reloaded_bitmap.contains(42));

    // Verify the raw layer captured the document and the cross-collection link.
    {
        const sql = "SELECT COUNT(*) FROM documents_raw WHERE workspace_id = 'ws_test'";
        const stmt = try facet_sqlite.prepare(db, sql);
        defer facet_sqlite.finalize(stmt);
        try std.testing.expectEqual(facet_sqlite.c.SQLITE_ROW, facet_sqlite.c.sqlite3_step(stmt));
        try std.testing.expectEqual(@as(i64, 2), facet_sqlite.c.sqlite3_column_int64(stmt, 0));
    }
    {
        const sql = "SELECT COUNT(*) FROM document_links_raw WHERE workspace_id = 'ws_test'";
        const stmt = try facet_sqlite.prepare(db, sql);
        defer facet_sqlite.finalize(stmt);
        try std.testing.expectEqual(facet_sqlite.c.SQLITE_ROW, facet_sqlite.c.sqlite3_step(stmt));
        try std.testing.expectEqual(@as(i64, 1), facet_sqlite.c.sqlite3_column_int64(stmt, 0));
    }

    // Reindex round-trip: raw -> derived. Wipe BM25 + facet posting state by
    // creating fresh in-memory stores, then call the pipeline reindex.
    var search2 = search_store.Store.init(std.testing.allocator);
    defer search2.deinit();
    var facets2 = facet_store.Store.init(std.testing.allocator);
    defer facets2.deinit();
    var graph2 = graph_store.Store.init(std.testing.allocator);
    defer graph2.deinit();

    var rebuild = Pipeline{
        .allocator = std.testing.allocator,
        .db = &db,
        .search = &search2,
        .facets = &facets2,
        .graph = &graph2,
        .workspace_id = "ws_test",
        .collection_id = "ws_test::docs",
    };
    try rebuild.facets.registerTable(.{
        .table_id = 1,
        .chunk_bits = 4,
        .schema_name = "public",
        .table_name = "docs_fixture",
    });
    try rebuild.facets.registerFacet(1, 2, "topic.category");

    const bm25_counts = try rebuild.reindexBm25("ws_test", "ws_test::docs", .{ .table_id = 1 });
    try std.testing.expectEqual(@as(u64, 1), bm25_counts.documents);
    try std.testing.expectEqual(@as(u64, 0), bm25_counts.chunks);
    const facet_count = try rebuild.reindexFacets("ws_test", "ws_test::docs", 1);
    try std.testing.expectEqual(@as(u64, 1), facet_count);
    const graph_count = try rebuild.reindexGraphWithDocumentTable("ws_test", 1);
    try std.testing.expectEqual(@as(u64, 5), graph_count);

    var rebuilt_entity = try graph_sqlite.loadEntityFull(db, std.testing.allocator, 1);
    defer rebuilt_entity.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(f32, 0.91), rebuilt_entity.confidence);
    try std.testing.expect(std.mem.indexOf(u8, rebuilt_entity.metadata_json, "\"role\":\"engineer\"") != null);

    var rebuilt_relation = (try graph_sqlite.findRelationByIds(db, std.testing.allocator, 1, 2, "works_for")).?;
    defer rebuilt_relation.deinit(std.testing.allocator);
    try std.testing.expect(rebuilt_relation.valid_from_unix != null);
    try std.testing.expect(rebuilt_relation.valid_to_unix != null);
    try std.testing.expect(std.mem.indexOf(u8, rebuilt_relation.metadata_json, "\"source\":\"fixture\"") != null);

    const rebuilt_docs = try graph_sqlite.loadEntityDocuments(db, std.testing.allocator, 1);
    defer {
        for (rebuilt_docs) |doc| if (doc.role) |role| std.testing.allocator.free(role);
        std.testing.allocator.free(rebuilt_docs);
    }
    try std.testing.expectEqual(@as(usize, 1), rebuilt_docs.len);
    try std.testing.expectEqual(@as(u64, 42), rebuilt_docs[0].doc_id);

    const rebuilt_chunks = try graph_sqlite.loadEntityChunks(db, std.testing.allocator, 1);
    defer {
        for (rebuilt_chunks) |*chunk| chunk.deinit(std.testing.allocator);
        std.testing.allocator.free(rebuilt_chunks);
    }
    try std.testing.expectEqual(@as(usize, 1), rebuilt_chunks.len);
    try std.testing.expectEqual(@as(u32, 0), rebuilt_chunks[0].chunk_index);
    try std.testing.expectEqualStrings("ws_test::docs", rebuilt_chunks[0].collection_id);

    var rebuilt_filter = (try facet_store.filterDocuments(
        std.testing.allocator,
        facets2.asRepository(),
        "docs_fixture",
        &.{.{ .facet_name = "topic.category", .values = &.{"graph"} }},
    )).?;
    defer rebuilt_filter.deinit();
    try std.testing.expect(rebuilt_filter.contains(42));
}

test "ingestDocumentChunked end-to-end persists nanoid, chunks, and source.* facets" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    var search = search_store.Store.init(std.testing.allocator);
    defer search.deinit();
    var facets = facet_store.Store.init(std.testing.allocator);
    defer facets.deinit();
    var graph = graph_store.Store.init(std.testing.allocator);
    defer graph.deinit();

    var pipeline = Pipeline{
        .allocator = std.testing.allocator,
        .db = &db,
        .search = &search,
        .facets = &facets,
        .graph = &graph,
    };

    try pipeline.createWorkspace(.{ .workspace_id = "ws_chunk", .label = "Chunk" });
    try pipeline.createCollection(.{
        .workspace_id = "ws_chunk",
        .collection_id = "ws_chunk::docs",
        .name = "docs",
    });

    const content =
        \\First paragraph about retention policies.
        \\
        \\Second paragraph about backup cadence.
    ;

    var result = try pipeline.ingestDocumentChunked(.{
        .workspace_id = "ws_chunk",
        .collection_id = "ws_chunk::docs",
        .doc_id = 1,
        .content = content,
        .source_ref = "/data/notes/policies/retention.md",
        .ingested_at = "2026-04-21T10:00:00Z",
        .chunker_options = .{ .strategy = .paragraph },
    });
    defer result.deinit(std.testing.allocator);

    // The pipeline must have generated a non-empty nanoid.
    try std.testing.expect(result.doc_nanoid.len > 0);
    try std.testing.expect(result.chunk_count >= 2);

    const Counts = struct {
        fn one(d: facet_sqlite.Database, sql: []const u8) !i64 {
            const stmt = try facet_sqlite.prepare(d, sql);
            defer facet_sqlite.finalize(stmt);
            try std.testing.expectEqual(facet_sqlite.c.SQLITE_ROW, facet_sqlite.c.sqlite3_step(stmt));
            return facet_sqlite.c.sqlite3_column_int64(stmt, 0);
        }
    };

    // Parent doc was upserted with the generated nanoid.
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(db, "SELECT COUNT(*) FROM documents_raw WHERE doc_id = 1 AND doc_nanoid <> ''"));
    // All chunks landed in chunks_raw.
    try std.testing.expectEqual(
        @as(i64, @intCast(result.chunk_count)),
        try Counts.one(db, "SELECT COUNT(*) FROM chunks_raw WHERE doc_id = 1"),
    );
    try pipeline.ingestChunkVector("ws_chunk", "ws_chunk::docs", 1, 0, &.{ 0.1, 0.9 });
    try std.testing.expectEqual(
        @as(i64, 1),
        try Counts.one(db, "SELECT COUNT(*) FROM chunks_raw_vector WHERE doc_id = 1 AND chunk_index = 0 AND dim = 2"),
    );
    // Every chunk records its strategy and a non-zero token count.
    try std.testing.expectEqual(
        @as(i64, @intCast(result.chunk_count)),
        try Counts.one(db, "SELECT COUNT(*) FROM chunks_raw WHERE strategy = 'paragraph' AND token_count > 0"),
    );

    // The auto-extracted source.* facets must be present per chunk.
    try std.testing.expectEqual(
        @as(i64, @intCast(result.chunk_count)),
        try Counts.one(db, "SELECT COUNT(*) FROM facet_assignments_raw WHERE namespace = 'source' AND dimension = 'filename' AND value = 'retention.md'"),
    );
    try std.testing.expectEqual(
        @as(i64, @intCast(result.chunk_count)),
        try Counts.one(db, "SELECT COUNT(*) FROM facet_assignments_raw WHERE namespace = 'source' AND dimension = 'extension' AND value = 'md'"),
    );
    // The path is split into cumulative directory prefixes (`data`,
    // `data/notes`, `data/notes/policies`); each prefix gets one row per chunk.
    try std.testing.expectEqual(
        @as(i64, @intCast(result.chunk_count)),
        try Counts.one(db, "SELECT COUNT(*) FROM facet_assignments_raw WHERE namespace = 'source' AND dimension = 'dir' AND value = 'data'"),
    );
    try std.testing.expectEqual(
        @as(i64, @intCast(result.chunk_count)),
        try Counts.one(db, "SELECT COUNT(*) FROM facet_assignments_raw WHERE namespace = 'source' AND dimension = 'dir' AND value = 'data/notes/policies'"),
    );

    // lookupDocByNanoid must round-trip the public id back to its internal triple.
    const lookup = try collections_sqlite.lookupDocByNanoid(db, std.testing.allocator, result.doc_nanoid);
    try std.testing.expect(lookup != null);
    if (lookup) |found| {
        defer found.deinit(std.testing.allocator);
        try std.testing.expectEqualStrings("ws_chunk", found.workspace_id);
        try std.testing.expectEqualStrings("ws_chunk::docs", found.collection_id);
        try std.testing.expectEqual(@as(u64, 1), found.doc_id);
    }

    // External-link helper records an outbound URI for the doc.
    try pipeline.linkExternal(.{
        .workspace_id = "ws_chunk",
        .link_id = 7,
        .source_collection_id = "ws_chunk::docs",
        .source_doc_id = 1,
        .target_uri = "https://example.com/policy",
        .edge_type = "cites",
    });
    try std.testing.expectEqual(@as(i64, 1), try Counts.one(db, "SELECT COUNT(*) FROM external_links_raw WHERE link_id = 7 AND target_uri = 'https://example.com/policy'"));
}
