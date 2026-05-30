# Native symbol reference

Reference for the native symbol layout. The current repo is SQLite-first; use this page when you need the native export names.

Below: **C symbol** → primary **SQL name** → **Zig module**.

## Facets (`src/mb_facets/main.zig`)

| C symbol | SQL function (typical) | Notes |
|----------|-------------------------|--------|
| `merge_deltas_native` | `merge_deltas_native(oid)` | Top-level; wrappers under `facets.merge_deltas_native_wrapper` |
| `build_filter_bitmap_native` | `build_filter_bitmap_native(oid, facets.facet_filter[])` | |
| `get_facet_counts_native` | `get_facet_counts_native(...)` | Returns `SETOF facets.facet_counts` |
| `search_documents_native` | `search_documents_native(...)` | Returns `SETOF bigint` |
| `filter_documents_by_facets_bitmap_jsonb_native` | `filter_documents_by_facets_bitmap_jsonb_native(text, jsonb, text)` | |
| `current_hardware` | `facets.current_hardware()` | OUT args: `support_code`, `description` |
| `bm25_index_document_native` | `facets.bm25_index_document_native(...)` | |
| `bm25_delete_document_native` | `facets.bm25_delete_document_native(...)` | |
| `bm25_search_native` | `facets.bm25_search_native(...)` | |
| `bm25_get_matches_bitmap_native` | `facets.bm25_get_matches_bitmap_native(...)` | |
| `bm25_score_native` | `facets.bm25_score_native(...)` | |
| `bm25_recalculate_statistics_native` | `facets.bm25_recalculate_statistics_native(...)` | |
| `bm25_index_worker_native` | `facets.bm25_index_worker_native(...)` | |
| `test_tokenize_only` | `test_tokenize_only(...)` | Diagnostic / test |
| `bm25_term_stats` | `bm25_term_stats(...)` | |
| `bm25_doc_stats` | `bm25_doc_stats(...)` | |
| `bm25_collection_stats` | `bm25_collection_stats(...)` | |
| `bm25_explain_doc` | `bm25_explain_doc(...)` | |

Exact argument lists and overloads: search the SQL install script for each symbol.

## Graph (`src/mb_graph/main.zig`)

| C symbol | SQL function |
|----------|----------------|
| `k_hops_filtered_native` | `k_hops_filtered(...)` |
| `shortest_path_filtered_native` | `shortest_path_filtered(...)` |

## Pragma (`src/mb_pragma/main.zig`)

| C symbol | SQL function | Status |
|----------|--------------|--------|
| `pragma_parse_proposition_line` | `pragma_parse_proposition_line_native` / `pragma_parse_proposition_line` | Implemented |
| `pragma_rank_native` | `pragma_rank_native` | **Stub** (empty result) |
| `pragma_next_hops_native` | `pragma_next_hops_native` | **Stub** (empty result) |

## Ontology (`src/mb_ontology/main.zig`)

| C symbol | SQL function | Status |
|----------|--------------|--------|
| `json_text_to_toon_native` | `mb_ontology.json_text_to_toon_native` | Implemented; invalid JSON is returned unchanged instead of SQL NULL |

The richer ontology runtime is in the standalone SQLite modules documented in
[ontology/README.md](ontology/README.md), not in exported C symbols.

## Regenerating this list

From the repo root:

```bash
rg '^export fn ' src/mb_facets/main.zig src/mb_graph/main.zig src/mb_pragma/main.zig src/mb_ontology/main.zig
```

## Standalone repository interfaces

The standalone Zig library also exposes repository-style interfaces from
[`src/standalone/interfaces.zig`](../src/standalone/interfaces.zig). These are
not exported C symbols, but they are the public in-repo contract used by the
SQLite search, vector, fixture, and PostgreSQL adapter layers.

`v1.4.0` adds batch BM25 read hooks and scoped vector requests:

| Interface item | Purpose |
|----------------|---------|
| `DocumentTermFrequency` | Batch row carrying `doc_id`, `term_hash`, and `frequency`. |
| `Bm25Repository.getDocumentStatsBatchFn` | Load stats for a set of candidate document IDs. |
| `Bm25Repository.getTermFrequenciesBatchFn` | Load term frequencies for a set of candidate document IDs and query terms. |
| `VectorSearchRequest.table_id` | Optional logical search table scope for in-memory vector stores. |

Hybrid search uses these hooks to avoid one BM25 lookup per candidate and to
keep vector results isolated to the same logical table as the BM25 request.
