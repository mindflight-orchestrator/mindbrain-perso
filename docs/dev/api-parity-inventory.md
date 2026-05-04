# pg_mindbrain → mindbrain API Parity Inventory

This is the parity matrix originally drafted alongside a **full API parity** planning thread (maintainer-local notes; not committed under `.cursor/`).

It catalogs every public API exposed by the PostgreSQL extension
[../pg_mindbrain](../../pg_mindbrain) and records the current SQLite-side
status in this repo.

The goal is **same public API, different database backend**:

- `../pg_mindbrain` = the contract (names, arguments, defaults, result shapes,
  semantics).
- `mindbrain` = the implementation, backed by SQLite, with Zig fallbacks for
  capabilities SQLite cannot natively provide.

Legend for the *Strategy* column:

| Tag | Meaning |
| --- | --- |
| `sql_view` | Direct SQLite view or table mapping. |
| `sql_wrapper` | Thin SQLite SQL or repository wrapper around an existing engine. |
| `sqlite_udf_zig` | Zig-implemented SQLite UDF / table function. |
| `zig_helper` | Zig API call from app code; not exposed as SQL. |
| `recursive_cte` | SQLite recursive CTE (no Zig required). |
| `roaring_zig_blob` | Roaring bitmap stored as `BLOB`, manipulated in Zig. |
| `fts5` | SQLite FTS5 virtual table. |
| `zig_traversal` | Zig in-process traversal/scoring. |
| `needs_design` | No clear equivalent yet; requires explicit design. |
| `n/a` | PostgreSQL-only feature with no SQLite equivalent (e.g. `dblink`). |

PostgreSQL features that recur as parity blockers across the surface:

- `roaringbitmap` SQL type and `rb_*` aggregate family (must round-trip as
  blob and through Zig).
- `tsvector` / `to_tsvector` / `plainto_tsquery` / `ts_rank` / `GIN` (replace
  with FTS5 + Zig ranking where the public API requires it).
- `jsonb` operators (`->`, `->>`, `@>`, `||`) (replace with JSON1).
- `regclass` / `oid` (replace with stable text `schema.table` keys).
- PL/pgSQL dynamic DDL and `EXECUTE format(...)`.
- `MATERIALIZED VIEW` + `REFRESH ... CONCURRENTLY` (use plain table + Zig
  rebuild).
- `dblink`, `COPY ... FROM PROGRAM`, `pg_terminate_backend`, `UNLOGGED`
  tables, `pg_stat_activity` (PostgreSQL operational features that have no
  SQLite analogue).
- SPI from C/Zig (replace with direct SQLite prepared statements).
- pgvector `vector` columns and `<->` (use `BLOB` embeddings + Zig math; or
  later, a vector ext).

The inventory below is grouped by API family. Within each family, items are
ordered roughly from foundational to advanced.

---

## 1. `mb_pragma.*` — Memory / projection contract

### 1.1 Persistent tables and trigger

| FQN | Mindbrain status | Strategy | Notes |
| --- | --- | --- | --- |
| `mb_pragma.facets` | Implemented in [src/standalone/sqlite_schema.zig](../src/standalone/sqlite_schema.zig) (`facets`) | `sql_view` + minor schema drift | Column name drift (TEXT ids, no `vector(1536)` / generated tsvector). |
| `mb_pragma.projections` | Implemented in [src/standalone/sqlite_schema.zig](../src/standalone/sqlite_schema.zig); runtime path in [src/standalone/pragma_sqlite.zig](../src/standalone/pragma_sqlite.zig) still uses legacy `memory_projections` | `needs_design` | Unify durable ontology tables and `memory_*` to one canonical model. |
| `mb_pragma.projection_types` | **MISSING** | `sql_view` + seed migration | Drives type matching, rank bias, pack priority, next-hop multipliers. |
| `mb_pragma.agent_state` | Implemented (`agent_state`) | `sql_view` | OK. |
| `mb_pragma.set_updated_at` | Implemented as SQLite trigger pair `trg_sync_facets_*` | `sql_wrapper` | Behaviour aligned. |

### 1.2 Projection-type config helpers

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `mb_pragma.pragma_projection_type_match` | Hardcoded subset in `pragma_sqlite.matchesProjectionTypes` | `sql_view` over new `projection_types` table | Today missing canonical/proposition/raw aliasing. |
| `mb_pragma.pragma_projection_rank_bias` | Inlined in `pragma_sqlite.scoreProjection` | `zig_helper` over `projection_types` | |
| `mb_pragma.pragma_projection_pack_priority` | Implicit ordering in `pragma_sqlite.packContext` | `zig_helper` | |
| `mb_pragma.pragma_projection_next_hop_multiplier` | Uses `pragma_dsl.typePrior` (different model) | `needs_design` | |

### 1.3 Parsing

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `mb_pragma.pragma_parse_proposition_line_native` | Zig implementation in [src/mb_pragma/main.zig](../src/mb_pragma/main.zig) (full, not stub) | `zig_helper` | Standalone path uses [src/standalone/pragma_dsl.zig](../src/standalone/pragma_dsl.zig). |
| `mb_pragma.pragma_parse_proposition_line` | SQL-only wrapper that returns `jsonb`; standalone returns `[]u8` JSON | `zig_helper` | SQLite UDF optional. |

### 1.4 Candidate filtering / graph edges

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `mb_pragma.pragma_native_agent_edges` | `pragma_sqlite.loadEdges` reads `memory_edges`, not `graph.relation` | `sql_view` over unified edges table | Needs a definition of "agent edges" in SQLite. |
| `mb_pragma.pragma_candidate_bitmap` | **MISSING** (no roaring SQL surface in standalone) | `needs_design` | Bitmap is internal; the public `pragma_rank_native` / `pragma_pack_context` callers do not need it directly. |

### 1.5 Ranking, next-hops, pack

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `mb_pragma.pragma_rank_native` | `pragma_sqlite.rankNative` (different filter / no rank-bias table) | `zig_helper` aligned to PG SQL | Move scoring to use `projection_types`. |
| `mb_pragma.pragma_rank_zig` | Empty stub in [src/mb_pragma/main.zig](../src/mb_pragma/main.zig) | `zig_helper` | Behaviour delegated to `pragma_sqlite.rankNative` once aligned. |
| `mb_pragma.pragma_next_hops_native` | `pragma_sqlite.nextHops` exists but omits `id` field expansion (gap vs PG) | `zig_helper` | Align node extraction with `getNodeIds`. |
| `mb_pragma.pragma_next_hops_zig` | Empty stub | `zig_helper` | Same as above. |
| `mb_pragma.pragma_pack_context` | `pragma_sqlite.packContext` (substring match, no tsquery) | `zig_helper` + optional FTS | |
| `mb_pragma.pragma_pack_context_scoped` | `pragma_sqlite.packContextScoped` (matches scope on JSON strings, not a `scope` column) | `needs_design` | Add `scope` column or normalize. |
| `mb_pragma.pragma_pack_context_toon` / `..._scoped_toon` | `toon_exports.encodePackContextAlloc` | `zig_helper` | Public TOON shape needs to match PG output. |

---

## 2. `graph.*` — Knowledge graph contract

### 2.1 Entities

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `graph.entity` | Table `graph_entity` in [src/standalone/sqlite_schema.zig](../src/standalone/sqlite_schema.zig) | `sql_view` | No generated `tsvector`; FTS handled in Zig. |
| `graph.get_entity(p_entity_id)` | `graph_sqlite.getEntity` | `sql_wrapper` | |
| `graph.find_entities_by_type(p_type)` | `graph_sqlite.findEntitiesByType` | `sql_wrapper` | |
| `graph.deprecate_entity(p_entity_id)` | `graph_sqlite.deprecateEntity` | `sql_wrapper` | |
| `graph.upsert_entity(...)` | `graph_sqlite.upsertEntityNatural` | `sql_wrapper` | Workspace merge rule must match PG conflict logic. |
| `graph.resolve_entity_id(p_name)` | `graph_sqlite.loadEntityIdByName` | `sql_wrapper` | |
| `graph.find_entity_by_name(p_name)` | `graph_sqlite.findEntityByName` | `sql_wrapper` | |
| `graph.get_entity_by_name(p_name)` | `graph_sqlite.getEntityByName` | `sql_wrapper` | |
| `graph.find_entities_by_names(p_names)` | **MISSING** | `sql_wrapper` | Bulk lookup preserving order. |
| `graph.get_entities_by_names(p_names)` | **MISSING** | `sql_wrapper` | Alias of above. |
| `graph.find_entities_by_metadata(p_metadata)` | **MISSING** | `sql_wrapper` (JSON1) | `metadata @> ...`. |
| `graph.get_entities_by_metadata(p_metadata)` | **MISSING** | `sql_wrapper` | Alias. |
| `graph.set_entity_workspace_id` | `graph_sqlite.setEntityWorkspaceId` | `sql_wrapper` | |
| `graph.count_entities_by_workspace` | `graph_sqlite.countEntitiesByWorkspace` | `sql_wrapper` | |
| `graph.cleanup_test_data(p_prefix)` | `graph_sqlite.cleanupTestData` | `sql_wrapper` | |

### 2.2 Relations

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `graph.relation` | Table `graph_relation` | `sql_view` | |
| `graph.get_relations_from(p_source_id)` | `graph_sqlite.getRelationsFrom` | `sql_wrapper` | |
| `graph.get_relations_to(p_target_id)` | `graph_sqlite.getRelationsTo` | `sql_wrapper` | |
| `graph.deprecate_relation(p_relation_id)` | **MISSING** as standalone | `sql_wrapper` | Currently only inside `applyKnowledgePatch`. |
| `graph.upsert_relation(...)` | `graph_sqlite.upsertRelationNatural` | `sql_wrapper` | |
| `graph.find_relation_by_ids` | `graph_sqlite.findRelationByIds` | `sql_wrapper` | |
| `graph.find_relation_by_endpoints` | `graph_sqlite.findRelationByEndpoints` | `sql_wrapper` | |
| `graph.find_relations_from_source_by_type` | `graph_sqlite.findRelationsFromSourceByType` | `sql_wrapper` | |

### 2.3 Aliases / entity-document bridge

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `graph.entity_alias` | Table `graph_entity_alias` | `sql_view` | |
| `graph.register_aliases(...)` | `graph_sqlite.registerAliases` | `sql_wrapper` | |
| `graph.resolve_terms(...)` | `graph_sqlite.resolveTerms` (returns id list, not bitmap) | `zig_helper` | Public API returns a roaring bitmap; need stable equivalent. |
| `graph.entity_document` | Table `graph_entity_document` (`table_id INTEGER` instead of `oid`) | `sql_view` | |
| `graph.entity_docs(entity_ids, target_table)` | `graph_sqlite.entityDocs` | `sql_wrapper` | |

### 2.4 Adjacency / k-hop

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `graph.lj_out` / `graph.lj_in` | `graph_lj_out` / `graph_lj_in` (blob) | `sql_view` | |
| `graph.graph_edge_int` | **MISSING** view | `sql_view` | |
| `graph.node_int_map` | **MISSING** view | `sql_view` | |
| `graph.lj_o` / `graph.lj_i` | **MISSING** views | `sql_view` | |
| `graph.rebuild_lj_relations()` | `graph_sqlite.rebuildLjRelations` | `sql_wrapper` | |
| `graph.rebuild_lj_for_entities(p_entity_ids)` | `graph_sqlite.rebuildLjForEntities` | `sql_wrapper` | |
| `graph.allowed_edges(...)` | **MISSING** as SQL function (logic in Zig filter) | `zig_helper` | |
| `graph.filter_edges_meta(...)` | **MISSING** as SQL function | `zig_helper` | |
| `graph.bfs_hop(...)` | **MISSING** | `zig_traversal` | |
| `graph.k_hops_filtered(...)` | `InMemoryRuntime.kHops` via `graph_sqlite.loadRuntime` | `zig_traversal` | |
| `graph.learn_from_run(...)` | `graph_sqlite.learnFromRun` | `sql_wrapper` | |
| `graph.apply_knowledge_patch(...)` | `graph_sqlite.applyKnowledgePatch` | `sql_wrapper` | |
| `graph.query_traversal(...)` | `graph_sqlite.traverse` (different shape) | `recursive_cte` or `zig_traversal` | |
| `graph.query_traversal_toon(...)` | `graph_sqlite.traverseToon` | `zig_traversal` | |
| `graph.stream_subgraph(...)` | `graph_sqlite.streamSubgraph` (unix time) | `zig_traversal` | |

### 2.5 Shortest path

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `graph.shortest_path_filtered(...)` | `graph_store.shortestPath` / `InMemoryRuntime` | `zig_traversal` | |
| `graph.shortest_path_filtered_toon(...)` | `graph_sqlite.shortestPathToon` | `zig_traversal` | |

### 2.6 Search / FTS

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `graph.entity_fts_search(...)` | `graph_sqlite.entityFtsSearch` (Zig token scoring, not FTS5) | `fts5` or `zig_traversal` | Public API returns `fts_rank`; align ranking semantics. |
| `graph.entity_fts_search_toon(...)` | `graph_sqlite.entityFtsSearchToon` | `sql_wrapper` | |

### 2.7 Marketplace / analytics

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `graph.marketplace_search(...)` | `graph_sqlite.marketplaceSearch` | `zig_traversal` + SQL | |
| `graph.marketplace_search_toon(...)` | `graph_sqlite.marketplaceSearchToon` | `sql_wrapper` | |
| `graph.skill_dependencies(...)` | `graph_sqlite.skillDependencies` | `recursive_cte` | |
| `graph.skill_dependencies_toon(...)` | `graph_sqlite.skillDependenciesToon` | `sql_wrapper` | |
| `graph.confidence_decay(p_entity_id, ...)` | `graph_sqlite.confidenceDecay` | `sql_wrapper` | |
| `graph.confidence_decay(p_name, ...)` (overload) | **MISSING** | `sql_wrapper` | |

### 2.8 Neighborhood / streaming

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `graph.entity_neighborhood(...)` | `graph_sqlite.entityNeighborhood` (JSON text) | `sql_wrapper` | |
| `graph.stream_node(...)` | **MISSING** | `sql_wrapper` | |
| `graph.stream_edge(...)` | **MISSING** | `sql_wrapper` | |
| `graph.stream_entity_neighborhood(...)` | `graph_sqlite.streamEntityNeighborhood` | `zig_traversal` | |

### 2.9 Views and materialized statistics

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `graph.entity_degree` | Table `graph_entity_degree` + `graph_sqlite.refreshEntityDegree` | `sql_wrapper` | No concurrent refresh. |
| `graph.active_relations` | **MISSING** view | `sql_view` | |
| `graph.knowledge_timeline` | **MISSING** view | `sql_view` | |

---

## 3. `mb_collections.*`, `mindbrain.*`, `mb_ontology.*`

### 3.1 Collections raw layer

All `mb_collections.*` raw tables have SQLite equivalents in
[src/standalone/sqlite_schema.zig](../src/standalone/sqlite_schema.zig)
plus repository functions in
[src/standalone/collections_sqlite.zig](../src/standalone/collections_sqlite.zig).

Active drift to fix for parity:

- `mb_collections.collection_ontologies.role` defaults to `'primary'` in
  PostgreSQL but to `'attached'` in SQLite.
- `mb_collections.external_links_raw.edge_type` defaults to
  `'external_link'` in PostgreSQL but to `'reference'` in SQLite.
- `documents_raw.metadata` is `jsonb` in PostgreSQL and stored as
  `metadata_json` TEXT in SQLite; column name and `updated_at` semantics
  must be reconciled.

### 3.2 Collections procedures

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `mb_collections.source_facet_dimensions()` | `chunker.deriveSourceFacets` + `collections_sqlite.ensureSourceNamespace` | `zig_helper` | |
| `mb_collections.ensure_source_namespace` | `collections_sqlite.ensureSourceNamespace` | `zig_helper` | |
| `mb_collections.ensure_workspace` | `collections_sqlite.ensureWorkspace` + `ensureDefaultOntology` | `zig_helper` | |
| `mb_collections.ensure_collection` | `collections_sqlite.ensureCollection` + `attachOntologyToCollection` | `zig_helper` | |
| `mb_collections.random_nanoid` | `nanoid.generateDefault` / `nanoid.generate` | `zig_helper` | |
| `mb_collections.lookup_doc_by_nanoid` | `collections_sqlite.lookupDocByNanoid` | `zig_helper` | |
| `mb_collections.upsert_document_raw` | `collections_sqlite.upsertDocumentRaw` | `zig_helper` | |
| `mb_collections.upsert_chunk_raw` | `collections_sqlite.upsertChunkRaw` | `zig_helper` | |
| `mb_collections.upsert_facet_assignment_raw` | `collections_sqlite.upsertFacetAssignmentRaw` | `zig_helper` | |
| `mb_collections.upsert_external_link_raw` | `collections_sqlite.upsertExternalLinkRaw` | `zig_helper` | |
| `mb_collections.derive_source_facets` | `chunker.deriveSourceFacets` + facet upserts in pipeline | `zig_helper` | |
| `mb_collections.ingest_document_chunked` | `import_pipeline.Pipeline.ingestDocumentChunked` | `zig_helper` | |
| `mb_collections.synthetic_chunk_doc_id` | `import_pipeline.chunkSyntheticId` | `zig_helper` | |
| `mb_collections.reindex_bm25` | `import_pipeline.Pipeline.reindexBm25` (FTS5 path) | `fts5` + `zig_helper` | |
| `mb_collections.reindex_graph` | `import_pipeline.Pipeline.reindexGraph` | `zig_helper` | |
| `mb_collections.reindex_all` | `import_pipeline.Pipeline.reindexAll` | `zig_helper` | |

### 3.3 `mindbrain.*` metadata

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `mindbrain.workspaces` | `workspaces` + `workspace_sqlite.upsertWorkspace` | `sql_view` | Extra SQLite columns. |
| `mindbrain.pending_migrations` | Table `pending_migrations` (no Zig CRUD found) | `needs_design` | Plumb through workspace API. |
| `mindbrain.query_templates` | **MISSING** | `needs_design` | |
| `mindbrain.source_mappings` | `source_mappings` (different shape) | `needs_design` | Reconcile to PG `field_map` + `source_kind`. |
| `mindbrain.table_semantics` | `table_semantics` + `workspace_sqlite.upsertTableSemantic` | `sql_view` | |
| `mindbrain.column_semantics` | `column_semantics` + `upsertColumnSemantic` | `sql_view` | |
| `mindbrain.relation_semantics` | `relation_semantics` + `upsertRelationSemantic` | `sql_view` | |

### 3.4 `mb_ontology.*` resolution / coverage / export

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `mb_ontology.resolve_workspace` | `ontology_sqlite.resolveWorkspace` | `zig_helper` | |
| `mb_ontology.coverage` | `ontology_sqlite.coverage` / `coverageReport` | `zig_helper` | |
| `mb_ontology.coverage_by_domain` | `ontology_sqlite.coverageByDomain` | `zig_helper` | |
| `mb_ontology.coverage_toon` | `toon_exports.encodeCoverageReportAlloc` | `zig_helper` | |
| `mb_ontology.coverage_by_domain_toon` | Compose `coverageByDomain` + `encodeCoverageReportAlloc` | `zig_helper` | |
| `mb_ontology.marketplace_search` | Partial; `graph_sqlite.marketplaceSearch` only | `needs_design` | Composite scoring missing. |
| `mb_ontology.marketplace_search_by_domain` | **MISSING** | `needs_design` | |
| `mb_ontology.generate_triggers` | **MISSING** | `needs_design` | |
| `mb_ontology.validate_ddl_proposal` | **MISSING** | `needs_design` | |
| `mb_ontology.export_workspace_model` | `workspace_sqlite.exportWorkspaceModel` | `zig_helper` | |
| `mb_ontology.export_workspace_model_toon` | `workspace_sqlite.exportWorkspaceModelToon` | `zig_helper` | |
| `mb_ontology.json_text_to_toon_native` | Same algorithm via `ztoon` in `toon_exports` / `workspace_sqlite` | `zig_helper` (optional `sqlite_udf_zig`) | |

### 3.5 `mb_ontology.*` Phase 2 registry

All Phase 2 surfaces are **MISSING** in the standalone stack:
`mb_ontology.ontologies` (UUID registry), `ontology_versions`,
`workspace_bridges`, `entity_types`, `relation_types`,
`register_entity_type`, `register_relation_type`, `list_entity_types`,
`list_relation_types`, `compare_workspaces`, `find_entity_bridges`,
`bridge_workspaces`, `detect_conflicts`, `federated_search`, plus the
`*_toon` wrappers.

Strategy: `needs_design`. These tables also overlap conceptually with
`mb_collections.ontologies`; clarify which is the canonical registry before
porting.

---

## 4. `facets.*` — Faceting and BM25

This is the largest and most PostgreSQL-coupled surface.

### 4.1 Core types and contract tables

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `facets.faceted_table` | `facet_tables` + `table_semantics` (partial) | `sql_view` + app mapping | |
| `facets.facet_definition` | `facet_definitions` | `sql_view` | |
| `facets.facet_counts` (composite) | Ad hoc structs in Zig | `needs_design` | |
| `facets.facet_filter` (composite) | Ad hoc structs in Zig | `needs_design` | |
| `facets.bm25_index` (table) | `search_postings` / BM25 tables (different shape) | `roaring_zig_blob` + `sqlite_udf_zig` | |
| `facets.bm25_term_frequencies` | **MISSING** as separate table | `needs_design` | |
| `facets.bm25_documents` | Partial via `bm25_sync_triggers` and search tables | `needs_design` | |
| `facets.bm25_statistics` | **MISSING** as single table | `sql_view` | |
| `facets.bm25_pending_terms` | **MISSING** | `needs_design` | |
| `facets.delta_merge_history` | **MISSING** | `needs_design` | |

### 4.2 Facet type constructors

All public `*_facet`, `*_facet_values`, `*_facet_subquery` (plain, datetrunc,
bucket, array, joined_plain, function, function_array, boolean, rating) are
**MISSING** as SQL functions on the SQLite side. Strategy: `needs_design`
unless the public contract can be reduced to "build descriptors in Zig".

### 4.3 Faceting setup, deltas, lifecycle

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `facets.optimal_chunk_bits` | `facet_sqlite.optimalChunkBits` | `zig_helper` | |
| `facets.add_faceting_to_table` | `facet_sqlite.setupFacetTable` + helpers | `sql_wrapper` + Zig | |
| `facets.add_facets` | `facet_sqlite.insertFacetDefinition` (partial) | `sql_wrapper` | |
| `facets.drop_facets` | `facet_sqlite.dropFacets` | `sql_wrapper` | |
| `facets.drop_faceting` | `facet_sqlite.dropFaceting` | `sql_wrapper` | |
| `facets.populate_facets_query` | **MISSING** | `needs_design` | |
| `facets.populate_facets` | Partial via `applyDeltas` | `sql_wrapper` | |
| `facets.refresh_facets` | `facet_sqlite.refreshFacets` | `sql_wrapper` | |
| `facets.setup_simple` | **MISSING** | `needs_design` | |
| `facets.create_delta_trigger` | Internal merge in `facet_sqlite` (no SQL trigger) | `needs_design` | |
| `facets.apply_deltas` | `facet_sqlite.applyDeltas` | `roaring_zig_blob` | |
| `facets.merge_deltas` | `facet_sqlite.mergeDeltas` / `mergeDeltasSafe` | `roaring_zig_blob` | |
| `facets.merge_deltas_native` | `src/mb_facets/deltas.zig` (PG only); SQLite uses `mergeDeltas` | `sqlite_udf_zig` | |
| `facets.merge_deltas_native_wrapper` | See above | `sql_wrapper` | |
| `facets.rebuild_hierarchy` | Partial | `roaring_zig_blob` | |
| `facets.set_table_unlogged` / `set_table_logged` / `check_table_logging_status` / `verify_before_logged_conversion` / `bulk_load_with_unlogged` | n/a | `n/a` | PG-only ops. |
| `facets.merge_deltas_all` | **MISSING** | `needs_design` | |
| `facets.merge_deltas_smart` | **MISSING** | `needs_design` | |
| `facets.merge_deltas_with_history` | **MISSING** | `needs_design` | |
| `facets.delta_status` / `check_delta_health` | `countFacetDeltas` (partial) | `sql_view` | |
| `facets.merge_deltas_safe` | `facet_sqlite.mergeDeltasSafe` | `sql_wrapper` | |

### 4.4 Counts, filters, hierarchy, search

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `facets.top_values` | `facet_sqlite.topValues` | `roaring_zig_blob` + Zig | |
| `facets.get_facet_counts` | `facet_sqlite.getFacetCounts` | `roaring_zig_blob` | |
| `facets.get_documents_with_facet` | `facet_sqlite.getPostings` / `loadPostingBitmap` | `roaring_zig_blob` | |
| `facets.get_documents_with_boolean_facet` | Same path | `roaring_zig_blob` | |
| `facets.get_boolean_facet_counts` | **MISSING** | `sql_view` | |
| `facets.get_filtered_boolean_facet_counts` | **MISSING** | `roaring_zig_blob` | |
| `facets.filter_with_boolean_facets` | **MISSING** | `roaring_zig_blob` | |
| `facets.calculate_facet_cardinality_stats` | **MISSING** | `sql_view` or materialized | |
| `facets.get_facet_cardinality_from_stats` | **MISSING** | `sql_view` | |
| `facets.filter_by_facets_with_cardinality_optimization` | **MISSING** (PG body has placeholder bug) | `needs_design` | |
| `facets.get_facet_counts_with_vector` | `hybrid_search.zig` (interface) | `vector` + `roaring_zig_blob` | |
| `facets.filter_with_facets_and_vector` | Partial | `needs_design` | |
| `facets.get_facet_counts_toon` | `toon_exports.zig` (test only) | `sqlite_udf_zig` + `needs_design` | |
| `facets.get_facet_counts_with_vector_toon` | **MISSING** | `needs_design` | |
| `facets.get_facet_counts_by_bitmap` | **MISSING** | `needs_design` | |
| `facets.hierarchical_facets` | Partial | `needs_design` | |
| `facets.hierarchical_facets_bitmap` | Partial | `needs_design` | |
| `facets.search_documents` | **MISSING** as a single function | `fts5` + Zig + `needs_design` | |
| `facets.search_documents_with_facets` | Partial via `import_pipeline` | `needs_design` | |
| `facets.count_results` | `facet_sqlite.countResults` | `roaring_zig_blob` | |
| `facets.build_filter_bitmap_native` | PG-only Zig | `sqlite_udf_zig` | |
| `facets.get_facet_counts_native` | PG-only Zig | `sqlite_udf_zig` | |
| `facets.search_documents_native` | PG-only Zig | `sqlite_udf_zig` | |
| `facets.filter_documents_by_facets_bitmap_jsonb_native` | PG-only Zig | `sqlite_udf_zig` | |
| `facets.filter_documents_by_facets` | `facet_sqlite.filterDocumentsByFacets` | `sql_wrapper` + Zig | |
| `facets.filter_documents_by_facets_bitmap` | `facet_sqlite.filterDocumentsByFacetsBitmap` | `roaring_zig_blob` | |
| `facets.current_hardware` | `mb_facets/main.zig` (PG only) | `sqlite_udf_zig` (optional) | |

### 4.5 BM25

The BM25 SQL surface is large. Of the public functions:

- Document index/delete/search/score/get_matches/term_stats/doc_stats/
  collection_stats/explain/hash/recalc/worker/tokenize: implementation
  exists in [src/mb_facets/bm25/](../src/mb_facets/bm25), but bound to
  PostgreSQL. SQLite parity must register them as SQLite UDFs or expose
  equivalent repository functions backed by FTS5 storage.
- `facets.bm25_index_documents_parallel`, `bm25_cleanup_dblinks`,
  `bm25_status` / `bm25_progress` / `bm25_active_processes`,
  `bm25_kill_stuck`: PostgreSQL operational features (`dblink`,
  `pg_stat_activity`, `pg_terminate_backend`). Strategy: `n/a` or app-level
  metrics.
- `facets.setup_table_with_bm25`, `bm25_create_sync_trigger`,
  `bm25_drop_sync_trigger`: rebind to SQLite triggers / repository helpers.
- `facets.bm25_get_worker_range`: ported to `search_sqlite.bm25GetWorkerRange`.

Status of all BM25 entries is summarized in the table above (4.4) and in
the source agent inventories.

### 4.6 Introspection / UI helpers

| FQN | Status | Strategy | Notes |
| --- | --- | --- | --- |
| `facets.list_table_facets` | `facet_sqlite.listFacetDefinitions` / `listTableFacets` | `sql_view` | |
| `facets.list_table_facet_names` | `facet_sqlite.listTableFacetNames` | `sql_view` | |
| `facets.list_table_facets_with_types` | **MISSING** | `sql_view` | |
| `facets.list_table_facets_simple` | Partial | `sql_view` | |
| `facets.describe_table` | `facet_sqlite.describeFacetTable` | `sql_view` | |
| `facets.list_tables` | Partial; no global list | `sql_view` | |
| `facets.get_facet_hierarchy` | **MISSING** | `needs_design` | |
| `facets.list_table_facets_for_ui` | **MISSING** | `needs_design` | |
| `facets.introspect` | **MISSING** | `needs_design` | |

---

## 5. PostgreSQL operational features explicitly out of scope

These features have no SQLite parity goal; the standalone stack must
provide the *behavior* through other means or document the gap:

- `dblink`-based BM25 parallel workers (`facets.bm25_index_documents_parallel`,
  `bm25_cleanup_dblinks`, `bm25_full_cleanup`).
- `pg_stat_activity` introspection (`bm25_status`, `bm25_progress`,
  `bm25_active_processes`).
- `pg_terminate_backend` (`bm25_kill_stuck`).
- `UNLOGGED` table conversion helpers
  (`facets.set_table_unlogged` / `set_table_logged` / `bulk_load_with_unlogged`).
- `COPY ... FROM PROGRAM` ingestion shortcuts.

These are tracked here as `n/a` so they do not silently appear as parity
gaps.

---

## 6. Migration order (mirrors the parity plan)

1. Pragma parity — smallest contained surface, biggest current drift
   (rank/next-hops stubs vs. SQL behavior). Establishes the wrapper / UDF
   pattern.
2. Schema convergence — unprefixed ontology tables, `projection_types`, source-mapping
   shape, `*_raw` defaults aligned with PostgreSQL.
3. Graph parity — bulk lookups, missing helpers, named views, FTS5 path.
4. Ontology, workspace, collections parity — close the *_raw drift, port
   `coverage*`, `marketplace_search*`, `*_toon` variants.
5. Facets and BM25 parity — last because of roaring + FTS + worker scope.
6. Parity tests and documentation pass — `test/run_all_tests.sh` becomes
   the PostgreSQL truth, and SQLite gets a mirror suite that asserts the
   same observable behavior per family.

This document is the single shared inventory across all of those
workstreams; future plans should update tables here when a row changes
status.
