# BM25 Rework Notes

## Implementation Status

The first rework pass has been implemented:

- Bulk JSON/rebuild indexing routes through staged grouped merge instead of
  looping over per-document indexing.
- PostgreSQL term frequencies are normalized into
  `facets.bm25_term_frequencies`; `facets.bm25_index` now holds posting
  bitmaps and term metadata only.
- PostgreSQL sync triggers enqueue into `facets.bm25_pending_terms`; callers can
  merge batches with `facets.bm25_flush_pending_terms`.
- SQLite rebuilds reuse prepared artifact writer statements.
- Standalone token hashing no longer allocates a lowercase copy per token.
- SQLite collection stats now store `total_document_length` directly.

## Original calculation

Mindbrain has two BM25 storage paths.

Before the rework, the PostgreSQL extension stored one row per
`(table_id, term_hash)` in
`facets.bm25_index`. Each row contains:

- `doc_ids`: a roaring bitmap of documents containing the term.
- `term_freqs`: a JSONB map from `doc_id` to term frequency.
- `term_text`: the original lexeme for debugging, prefix, and fuzzy support.

Document lengths live in `facets.bm25_documents`, and collection-level
statistics live in `facets.bm25_statistics`.

The standalone SQLite path already stored the same logical artifacts in normalized
tables:

- `search_document_stats`
- `search_collection_stats`
- `search_term_stats`
- `search_term_frequencies`
- `search_postings`

The scoring formula itself is standard BM25-style scoring:

```text
idf = log((total_docs + 1.0) / (document_frequency + 0.5))

score += idf * (
    term_frequency * (k1 + 1.0)
    /
    (
        term_frequency
        + k1 * (1.0 - b + b * document_length / avg_document_length)
    )
)
```

Current constants are:

- `k1 = 1.2`
- `b = 0.75`

The expensive part is not the BM25 formula. The expensive part is index
maintenance.

## Why indexing is slow

### PostgreSQL single-document path

`src/mb_facets/bm25/index.zig` indexes one document by:

1. Opening an SPI connection.
2. Tokenizing the document.
3. Counting term frequencies in a per-document hash map.
4. Iterating every unique term.
5. Running one SQL upsert per unique term.
6. Updating document metadata.
7. Updating collection statistics.

The hot loop is effectively:

```zig
for (term_entries.items) |entry| {
    try updateInvertedIndex(...);
}
```

Each `updateInvertedIndex` call performs:

```sql
INSERT INTO facets.bm25_index (...)
VALUES (...)
ON CONFLICT (table_id, term_hash) DO UPDATE SET
    doc_ids = rb_or(facets.bm25_index.doc_ids, EXCLUDED.doc_ids),
    term_freqs = facets.bm25_index.term_freqs || EXCLUDED.term_freqs
```

That means one SPI query per distinct term in the document. For common terms,
the same large posting row is repeatedly rewritten. The roaring bitmap is
merged again, and the JSONB frequency object is rewritten again.

This gets worse as the index grows because hot terms become large rows.

### PostgreSQL batch and parallel path

The SQL install script already contains a better design:
`facets.bm25_index_documents_parallel`.

That path:

1. Creates a source staging table.
2. Splits rows across workers.
3. Lets workers write extracted term rows into private staging tables.
4. Merges all worker staging tables into `bm25_index` with grouped aggregate
   inserts.
5. Recalculates statistics once.

This avoids most per-document lock contention and avoids many repeated writes.
For bulk indexing and rebuilds, this staging-based path is structurally better
than looping over `bm25_index_document`.

The problem is that the single-document API still has the slow write pattern,
and some fallback paths still loop over document-level indexing.

### SQLite rebuild path

`src/standalone/search_sqlite.zig` has a full rebuild path that is conceptually
reasonable: it scans all `search_documents`, tokenizes each document, aggregates
document frequencies and posting lists in memory, then writes compact artifacts.

However, it still performs many small SQLite operations:

- one document-stat upsert per document
- one term-frequency upsert per `(doc_id, term_hash)`
- one term-stat upsert per term
- one posting upsert per term

Each helper prepares and finalizes its own statement. In large rebuilds, this
causes avoidable prepare/finalize overhead.

### SQLite incremental path

The incremental path is more expensive for hot terms.

`reconcileTermArtifact` loads the current posting bitmap for each changed term,
deserializes it, mutates it, serializes it, and writes it back.

That means frequent terms pay repeated large blob rewrite costs. The shape is:

```zig
var posting = loadPosting(...)
posting.add(doc_id) or posting.remove(doc_id)
try upsertPosting(...)
```

This is correct, but it is not a good write-amplification profile for high
ingest rates.

### Tokenization allocation overhead

The standalone tokenizer allocates a lowercase copy per token before hashing.
That creates one allocation per token:

```zig
const lower = try lowerAsciiOwned(allocator, word);
try tokens.append(allocator, std.hash.Wyhash.hash(0, lower));
```

For indexing-heavy workloads, tokenization allocation alone can become a
meaningful cost.

### Collection statistics shape

SQLite stores `total_documents` and `avg_document_length`, but not total token
length. Incremental updates reconstruct total length from:

```text
round(avg_document_length * total_documents)
```

That is lossy and unnecessary. Storing `total_document_length` directly makes
incremental updates exact and simpler.

## Optimization plan

### 1. Make staging-based indexing the default for bulk work

Bulk indexing and rebuilds should never loop over the single-document API unless
the input is tiny.

Preferred shape:

1. Tokenize documents into staging rows:
   `(table_id, doc_id, term_hash, term_text, frequency, document_length)`.
2. Group staging rows by `(table_id, term_hash)`.
3. Build posting bitmaps once per term.
4. Write term frequencies in bulk.
5. Write document stats in bulk.
6. Recalculate or aggregate collection stats once.

This is the most important optimization because it changes the write pattern
from many small random updates to fewer grouped writes.

### 2. Normalize PostgreSQL term frequencies

`term_freqs jsonb` is convenient but expensive. Every append rewrites a JSONB
object for that term. For hot terms, this becomes pathological.

Prefer a normalized layout:

```sql
CREATE TABLE facets.bm25_postings (
    table_id oid NOT NULL,
    term_hash bigint NOT NULL,
    term_text text NOT NULL,
    doc_ids roaringbitmap NOT NULL,
    language text DEFAULT 'english',
    PRIMARY KEY (table_id, term_hash)
);

CREATE TABLE facets.bm25_term_frequencies (
    table_id oid NOT NULL,
    term_hash bigint NOT NULL,
    doc_id bigint NOT NULL,
    frequency int NOT NULL,
    PRIMARY KEY (table_id, term_hash, doc_id)
);

CREATE TABLE facets.bm25_term_stats (
    table_id oid NOT NULL,
    term_hash bigint NOT NULL,
    document_frequency bigint NOT NULL,
    PRIMARY KEY (table_id, term_hash)
);
```

Scoring can fetch frequencies for candidate documents with indexed lookups
instead of JSONB extraction. This also allows bulk inserts and conflict updates
to target narrow rows.

### 3. Add delta tables for online indexing

Sync triggers and single-document writes should avoid immediate mutation of
large posting rows.

Use an append-only delta table:

```sql
CREATE TABLE facets.bm25_pending_terms (
    table_id oid NOT NULL,
    doc_id bigint NOT NULL,
    term_hash bigint NOT NULL,
    term_text text NOT NULL,
    frequency int NOT NULL,
    op char(1) NOT NULL,
    created_at timestamptz DEFAULT now()
);
```

Then periodically flush deltas:

1. Group pending rows by `(table_id, term_hash)`.
2. Build add/remove bitmaps.
3. Merge each affected term once.
4. Upsert normalized term frequencies.
5. Refresh document and collection stats.
6. Delete flushed deltas.

This keeps trigger-time work cheap and moves expensive compression/rewrite work
to a batchable flush.

### 4. Reuse SQLite prepared statements

`rebuildSearchArtifacts` should prepare write statements once and reuse them
inside loops with `sqlite3_reset` and `sqlite3_clear_bindings`.

Targets:

- `upsertDocumentStat`
- `upsertTermFrequency`
- `upsertCollectionStat`
- `upsertTermStat`
- `upsertPosting`

The rebuild should also run inside a single transaction unless the caller has
already started one.

### 5. Remove per-token lowercase allocations

Hash tokens while lowercasing instead of allocating a lowercase copy per token.

Options:

- Implement a lowercase-aware Wyhash wrapper.
- Reuse one scratch buffer sized to the longest token in the document.
- Store lowercase terms only when the caller actually needs term text.

For the standalone path, term hashes are the primary artifact, so hashing
lowercase bytes directly is enough for most indexing work.

### 6. Store total document length

Add `total_document_length` to collection stats:

```sql
ALTER TABLE search_collection_stats
ADD COLUMN total_document_length INTEGER NOT NULL DEFAULT 0;
```

Then collection updates become exact:

```text
new_total_length = old_total_length - old_doc_length + new_doc_length
avg_document_length = new_total_length / total_documents
```

The PostgreSQL path should also consider storing total length if future
incremental update correctness matters for document replacement.

## Expected impact

The highest-impact changes are storage and write-path changes:

1. Staging-based bulk indexing avoids per-document/per-term writes.
2. Normalized term frequencies avoid repeated JSONB rewrites.
3. Delta flushing avoids immediate rewrites of hot posting blobs.

Prepared statement reuse and tokenizer allocation cleanup are smaller but still
important. They reduce fixed overhead after the write-amplification problem is
under control.

The BM25 scoring formula does not need to change for performance. The current
bottleneck is index maintenance: repeated SQL calls, repeated JSONB/blob
rewrites, and avoidable allocation/statement overhead during tokenization and
artifact writes.
