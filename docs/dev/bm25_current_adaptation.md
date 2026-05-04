# BM25 Current Adaptation

This document records the BM25 indexing changes implemented in the current
adaptation pass.

## Scope

The adaptation targeted write amplification during indexing. The BM25 scoring
formula did not change.

The main changes are:

- Bulk/rebuild indexing routes through staged grouped merge.
- PostgreSQL term frequencies are normalized.
- Trigger-driven indexing writes pending deltas.
- SQLite rebuild writes reuse prepared statements.
- Standalone token hashing avoids per-token lowercase allocations.
- SQLite collection stats store total document length directly.

## PostgreSQL Storage

Before this pass, `facets.bm25_index` stored both postings and term
frequencies:

```sql
facets.bm25_index(
    table_id,
    term_hash,
    term_text,
    doc_ids,
    term_freqs jsonb,
    language
)
```

The new install schema removes `term_freqs` from `bm25_index` and adds:

```sql
facets.bm25_term_frequencies(
    table_id oid,
    term_hash bigint,
    doc_id bigint,
    frequency int,
    primary key (table_id, term_hash, doc_id)
)
```

`facets.bm25_index` remains the posting table:

```sql
facets.bm25_index(
    table_id,
    term_hash,
    term_text,
    doc_ids,
    language
)
```

This avoids repeatedly rewriting a growing JSONB object for hot terms.

## PostgreSQL Write Path

Single-document native indexing now:

1. Tokenizes the document.
2. Counts term frequencies.
3. Upserts posting bitmap rows in `facets.bm25_index`.
4. Upserts frequency rows in `facets.bm25_term_frequencies`.
5. Updates document metadata and collection stats.

Delete now:

1. Removes the document id from posting bitmaps.
2. Deletes all rows for the document from `bm25_term_frequencies`.
3. Removes empty posting rows.
4. Deletes document metadata.

## Bulk And Rebuild Path

`facets.bm25_index_documents_batch` now builds a source query over
`jsonb_to_recordset(...)` and sends it through:

```sql
facets.bm25_index_documents_parallel(...)
```

The parallel path uses staging tables and grouped merge:

- worker staging rows contain `(term_hash, term_text, doc_id, term_freq,
  doc_length)`
- `bm25_index` receives grouped posting bitmaps
- `bm25_term_frequencies` receives grouped normalized frequencies
- `bm25_documents` receives distinct document stats

The previous fallback to per-document indexing was removed from rebuild. If
`dblink` is unavailable, staged bulk/rebuild indexing now raises an error rather
than silently falling back to the slow path.

## Trigger Delta Path

The install schema now includes:

```sql
facets.bm25_pending_terms(
    table_id,
    doc_id,
    term_hash,
    term_text,
    frequency,
    doc_length,
    language,
    op,
    created_at
)
```

Sync triggers now enqueue work instead of immediately mutating the index:

- `facets.bm25_enqueue_document_delta(...)`
- `facets.bm25_enqueue_delete_delta(...)`

Pending deltas are merged with:

```sql
facets.bm25_flush_pending_terms(p_table_id oid DEFAULT NULL, p_limit int DEFAULT 50000)
```

The flush function:

1. Copies a limited pending batch to a temp table.
2. Removes old state for affected documents.
3. Merges update postings by term.
4. Upserts normalized frequencies.
5. Upserts document metadata.
6. Removes empty postings.
7. Deletes flushed pending rows.
8. Recalculates collection statistics for affected tables.

## PostgreSQL Read Path

Native BM25 search and single-document scoring now read term frequencies from
`facets.bm25_term_frequencies`.

Updated readers include:

- `src/mb_facets/bm25/search_native.zig`
- `src/mb_facets/bm25/search.zig`
- `src/mb_facets/bm25/stats_native.zig`
- `src/standalone/bm25_pg_adapter.zig`

The candidate bitmap path still uses `facets.bm25_index.doc_ids`.

## SQLite Adaptation

SQLite `search_collection_stats` now has:

```sql
total_document_length INTEGER NOT NULL DEFAULT 0
```

Incremental collection stat updates use exact total length instead of
reconstructing length from `avg_document_length * total_documents`.

`rebuildSearchArtifacts` now creates an `ArtifactWriter` that prepares these
statements once:

- document stats
- term frequencies
- collection stats
- term stats
- postings

The rebuild loop resets and rebinds those statements instead of preparing and
finalizing on every row.

Standalone token hashing now lowercases into a stack buffer while feeding
Wyhash, instead of allocating a lowercase copy per token.

## Tests Run

The following passed:

```sh
ZIG_LOCAL_CACHE_DIR=/tmp/zig-cache ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache zig build test-standalone
ZIG_LOCAL_CACHE_DIR=/tmp/zig-cache ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache zig build
```

The PostgreSQL SQL integration suite was not run in this pass.

## Remaining Risk

The install SQL has been updated, but existing deployed databases would need a
migration that:

1. Creates `facets.bm25_term_frequencies`.
2. Backfills it from old `bm25_index.term_freqs`.
3. Updates functions.
4. Drops or ignores old `term_freqs`.

The new trigger delta path also requires operational policy: either explicit
flush calls after writes or a scheduled/background flush mechanism.
