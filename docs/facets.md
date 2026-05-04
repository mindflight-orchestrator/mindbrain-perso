# Facets and BM25

The standalone **`facets`** schema provides Roaring Bitmap–backed faceting on user tables, plus full-text and **BM25** search integrated with the same index metadata.

Definitive definitions live in the SQLite standalone schema and Zig entrypoints under `src/standalone/`.

> **Where the data comes from.** Facet definitions and per-document
> facet picks are sourced from the raw layer described in
> [collections.md](./collections.md) (`ontology_*` and
> `facet_assignments_raw`). The tables documented here are the
> *derived* index built by `Pipeline.reindexFacets(...)`.

> **Auto-extracted `source.*` facets.** Every workspace ships with a
> default ontology that exposes a built-in `source` namespace
> (`source.path`, `source.dir`, `source.filename`, `source.extension`,
> `source.ingested_at`, `source.chunk_index`, `source.chunk_count`,
> `source.strategy`). These are emitted automatically per chunk by
> `chunker.deriveSourceFacets` when documents are ingested through
> `Pipeline.ingestDocumentChunked`. See [`docs/chunking.md`](./chunking.md)
> for the full chunking pipeline that owns these rows.

## Concepts

- **`facets.faceted_table`** — Registry row per faceted user table (`table_id`, chunking, optional `bm25_language`).
- **`facets.facet_definition`** — Declares facet columns (plain, array, bucket, joined, function-backed, boolean, rating, date truncation, etc.).
- **Delta pipeline** — Changes flow through delta tables and SQLite-native orchestration; **`facets.merge_deltas`** applies them. The default implementation uses **SQL batching** (`facets.apply_deltas`) for large tables; **`merge_deltas_native(table_id)`** exists for special cases but is row-oriented and slower at scale.

## Typical workflow (SQL)

1. Register faceting: **`facets.add_faceting_to_table`** / **`facets.add_facets`** / **`facets.setup_simple`** (see SQL for signatures).
2. **Populate** bitmaps: **`facets.populate_facets`**, **`facets.refresh_facets`** as needed.
3. On data changes, the standalone sync path maintains deltas; call **`facets.merge_deltas(table_id)`** to consolidate.
4. Query:
   - **`facets.get_facet_counts`** / bitmap variants for navigation UI.
   - **`facets.search_documents`** / **`facets.search_documents_with_facets`** for search + facet filters.
   - **`facets.filter_documents_by_facets`** / bitmap variants to restrict document sets.

## Native (Zig) entrypoints

High-throughput paths call into the shared library. Public SQL wrappers include (non-exhaustive):

| SQL surface | Purpose |
|-------------|---------|
| `merge_deltas_native(table_id)` | Native delta merge |
| `build_filter_bitmap_native(table_id, facets.facet_filter[])` | Build filter bitmap |
| `get_facet_counts_native(...)` | Facet counts over a bitmap |
| `search_documents_native(...)` | Document IDs matching filters |
| `filter_documents_by_facets_bitmap_jsonb_native(schema, jsonb, table)` | JSONB facet filter → bitmap |
| `facets.current_hardware()` | SIMD / hardware support probe |

BM25-related native functions are exposed under **`facets.*`** with names such as **`facets.bm25_index_document_native`**, **`facets.bm25_search_native`**, **`facets.bm25_score_native`**, plus statistics helpers (`bm25_term_stats`, `bm25_doc_stats`, …). Higher-level **`facets.bm25_*`** SQL wrappers orchestrate indexing and search using those primitives.

See [native-reference.md](native-reference.md) for the full C symbol list.

## Optional vector hybrid

When vector search support is enabled, the SQLite standalone layer composes vector nearest-neighbor results with facet filtering. The combined helper lives in [src/standalone/query_executor.zig](../src/standalone/query_executor.zig) as `countFacetValuesWithVectorToon(...)`, which:

- loads nearest neighbors from a `VectorRepository`
- turns the matching document IDs into a bitmap
- reuses `facet_store.countFacetValuesToon(...)` for the final TOON export

This is the SQLite-first equivalent of the newer vector-backed facet-count tests in the sibling repo.

## Client examples

Language-specific examples under [examples/](../examples/) (Go, Python, Rust) show how applications call facet and BM25 APIs; use the standalone library entrypoints.
