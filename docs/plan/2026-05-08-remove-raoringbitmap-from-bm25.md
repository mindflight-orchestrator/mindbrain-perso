# Remove RoaringBitmap From BM25

Date: 2026-05-08

## Context

The original MindBrain storage design used Roaring bitmaps for faceting and
directed graph traversal, where set algebra is the primary operation:

- `facet_postings`: facet value to document/chunk sets.
- graph adjacency/frontier indexes: node or edge sets.

The standalone SQLite BM25 path currently also uses Roaring through
`search_postings`:

- `search_postings(table_id, term_hash, posting_blob)` stores a serialized
  Roaring bitmap of matching document ids for each term.
- `search_term_frequencies` stores term frequency per document.
- `search_term_stats` stores document frequency per term.
- `search_document_stats` stores document length.
- `search_collection_stats` stores total document count and average document
  length.

This splits BM25 data across several generic tables and requires extra lookups
after candidate discovery. The benchmark harness added in
`src/benchmark/fts5_compare.zig` shows that removing document retokenization
helps query latency, but indexing remains much slower than SQLite FTS5 because
the custom path maintains generic SQL rows plus serialized bitmap blobs instead
of a purpose-built full-text index.

## Decision

Roaring bitmaps should not be the primary BM25 posting format in the SQLite
standalone backend.

Keep Roaring for:

- facets;
- graph traversal/frontiers;
- optional post-search filter sets when intersecting lexical candidates with
  facet or graph constraints.

Replace the BM25 Roaring posting path with an FTS5-backed lexical index for
SQLite search.

## Target Design

Use FTS5 as the SQLite lexical/BM25 engine:

```sql
CREATE VIRTUAL TABLE search_fts USING fts5(
  content,
  content='search_documents',
  content_rowid='doc_id'
);
```

The exact table shape may need adjustment because `search_documents` is keyed
by `(table_id, doc_id)`, while FTS5 rowids are a single integer. Acceptable
options:

- one FTS5 table per logical `table_id`;
- encode `(table_id, doc_id)` into one synthetic rowid;
- keep `rowid = doc_id` and add a side table mapping rowid to `table_id`, only
  if table ids cannot overlap.

The preferred option is a deterministic synthetic rowid so one FTS5 table can
serve multiple logical tables.

## Migration Scope

Remove BM25 dependency on:

- `search_postings`;
- Roaring bitmap BM25 candidate lookup;
- `search_compact_store.getPostingBitmap` as the lexical candidate source.

Keep or replace:

- `search_document_stats`: likely replace with FTS5 `xColumnSize`/docsize
  data where possible.
- `search_collection_stats`: likely replace with FTS5 row count and average
  size where possible.
- `search_term_stats`: replace with FTS5 term/document frequency access where
  available, or stop exposing exact custom stats if no caller needs them.
- `search_term_frequencies`: replace with FTS5 match info / auxiliary ranking
  path, or avoid exposing direct tf rows.

Do not remove the public hybrid search contract until callers have a compatible
replacement.

## Implementation Plan

1. Inventory callers.
   - Find every caller of `Bm25Repository.getPostingBitmapFn`,
     `loadCompactSearchStore`, `search_postings`, and BM25 artifact tables.
   - Classify each as query-time search, diagnostics/export, tests, or import
     pipeline.

2. Add an FTS5 search adapter.
   - Create a SQLite implementation that accepts `table_id`, query text, and
     limit.
   - Return ranked `{doc_id, bm25_score}` rows from FTS5.
   - Normalize score direction so public MindBrain results keep "higher is
     better" semantics.

3. Update indexing.
   - On document upsert, write `search_documents`.
   - Maintain the FTS5 index using either FTS5 external-content rebuild or
     explicit insert/update/delete helpers.
   - Add a bulk rebuild path equivalent to `rebuildSearchArtifacts`, but backed
     by FTS5.

4. Update hybrid search.
   - Add a direct BM25 result provider instead of requiring
     `getPostingBitmapFn` plus separate tf/df/doc-length lookups.
   - Keep vector fusion unchanged.
   - If a facet/graph filter is supplied later, intersect it after FTS5
     candidate retrieval or push it through a temporary rowid filter.

5. Preserve compatibility while migrating.
   - Keep existing artifact tables during one transition step.
   - Add feature selection so tests can compare old BM25 artifacts vs FTS5.
   - Mark Roaring-backed BM25 artifacts deprecated in docs once parity is
     validated.

6. Remove Roaring BM25 path.
   - Delete `search_postings` usage from BM25 query code.
   - Remove BM25-specific posting serialization/deserialization.
   - Keep `facet_postings` and graph Roaring code intact.

## Validation Plan

Run the benchmark harness across at least:

- 12-doc built-in sample;
- 1k generated docs;
- 10k generated docs;
- real imported corpus if available.

Track:

- index build time;
- query p50/p95/p99;
- database size;
- top-k overlap;
- first-rank agreement;
- score direction and range;
- hybrid BM25/vector result stability.

Acceptance criteria:

- FTS5-backed BM25 query latency is not worse than the current compact
  Roaring-backed path.
- Bulk index time is materially closer to FTS5 than the current artifact path.
- First-rank agreement and top-k overlap are documented for representative
  queries.
- Facet and graph Roaring tests remain unchanged and passing.

## Risks

- FTS5 uses a different tokenizer and BM25 variant, so scores and ordering will
  not be byte-for-byte compatible.
- Existing tests may assert exact custom BM25 scores.
- Multi-table `table_id` support must be designed carefully because FTS5 rowid
  is a single integer.
- Phrase/prefix behavior may improve, but it changes query semantics.

## Open Questions

- Should SQLite BM25 expose exact custom stats APIs after moving to FTS5, or
  should those become diagnostic-only / unsupported?
- Should the Postgres extension keep the current custom BM25 implementation, or
  should this plan only apply to standalone SQLite?
- Should FTS5 use the default tokenizer, porter tokenizer, or a custom tokenizer
  matching MindBrain stopword/language behavior?
