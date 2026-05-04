# PostgreSQL BM25 Equivalent Plan

This plan covers the PostgreSQL hardening work needed to make the BM25 rework
production-grade and equivalent to the standalone/normalized indexing model.

## Goal

Make PostgreSQL BM25 indexing use the same durable model as the optimized
standalone path:

- postings are stored separately from term frequencies
- bulk/rebuild writes are staged and grouped
- trigger writes are append-oriented deltas
- search/scoring reads narrow normalized rows
- migrations preserve existing BM25 indexes

## Target PostgreSQL Model

### Canonical Tables

`facets.bm25_index` remains the posting table:

```sql
CREATE TABLE facets.bm25_index (
    table_id oid NOT NULL,
    term_hash bigint NOT NULL,
    term_text text NOT NULL,
    doc_ids roaringbitmap NOT NULL,
    language text DEFAULT 'english',
    PRIMARY KEY (table_id, term_hash)
);
```

Term frequencies are normalized:

```sql
CREATE TABLE facets.bm25_term_frequencies (
    table_id oid NOT NULL,
    term_hash bigint NOT NULL,
    doc_id bigint NOT NULL,
    frequency int NOT NULL,
    PRIMARY KEY (table_id, term_hash, doc_id)
);
```

Document stats remain:

```sql
CREATE TABLE facets.bm25_documents (
    table_id oid NOT NULL,
    doc_id bigint NOT NULL,
    doc_length int NOT NULL,
    language text DEFAULT 'english',
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now(),
    PRIMARY KEY (table_id, doc_id)
);
```

Collection stats should eventually include total length:

```sql
CREATE TABLE facets.bm25_statistics (
    table_id oid PRIMARY KEY,
    total_documents bigint NOT NULL,
    total_document_length bigint NOT NULL DEFAULT 0,
    avg_document_length float NOT NULL,
    last_updated timestamp DEFAULT now()
);
```

Pending deltas:

```sql
CREATE TABLE facets.bm25_pending_terms (
    table_id oid NOT NULL,
    doc_id bigint NOT NULL,
    term_hash bigint NOT NULL,
    term_text text NOT NULL,
    frequency int NOT NULL,
    doc_length int NOT NULL,
    language text DEFAULT 'english',
    op char(1) NOT NULL CHECK (op IN ('U', 'D')),
    created_at timestamptz DEFAULT now()
);
```

## Migration Plan

### 1. Add New Tables

Create:

- `facets.bm25_term_frequencies`
- `facets.bm25_pending_terms`

Add indexes:

```sql
CREATE INDEX bm25_term_frequencies_doc_idx
ON facets.bm25_term_frequencies(table_id, doc_id, term_hash);

CREATE INDEX bm25_pending_terms_flush_idx
ON facets.bm25_pending_terms(table_id, created_at, term_hash, doc_id);
```

### 2. Backfill From JSONB

For existing deployments that still have `bm25_index.term_freqs`, backfill:

```sql
INSERT INTO facets.bm25_term_frequencies(table_id, term_hash, doc_id, frequency)
SELECT
    i.table_id,
    i.term_hash,
    kv.key::bigint,
    kv.value::int
FROM facets.bm25_index i
CROSS JOIN LATERAL jsonb_each_text(i.term_freqs) kv
ON CONFLICT (table_id, term_hash, doc_id) DO UPDATE SET
    frequency = EXCLUDED.frequency;
```

Validate:

```sql
SELECT COUNT(*) FROM facets.bm25_term_frequencies;
SELECT SUM(jsonb_object_length(term_freqs)) FROM facets.bm25_index;
```

### 3. Cut Reads To Normalized Frequencies

Update all readers to use `bm25_term_frequencies`.

Required surfaces:

- native search
- single-document score
- debug stats
- document explanation
- repository adapter
- SQL tests

Search should use:

```sql
SELECT tf.term_hash, d.doc_id, d.doc_length, tf.frequency
FROM facets.bm25_term_frequencies tf
JOIN facets.bm25_documents d
  ON d.table_id = tf.table_id AND d.doc_id = tf.doc_id
WHERE tf.table_id = $1
  AND tf.term_hash = ANY($2);
```

### 4. Cut Writes To Normalized Frequencies

Update single-document indexing:

- update posting bitmap in `bm25_index`
- upsert `(table_id, term_hash, doc_id, frequency)` into
  `bm25_term_frequencies`

Update delete:

- remove doc id from posting bitmaps
- delete all frequency rows for `(table_id, doc_id)`
- delete empty posting rows

### 5. Migrate Bulk/Rebuild

Bulk/rebuild should always:

1. Create source staging table.
2. Tokenize into worker staging tables.
3. Merge `bm25_index` by `(table_id, term_hash)`.
4. Merge `bm25_term_frequencies` by `(table_id, term_hash, doc_id)`.
5. Merge `bm25_documents` by `(table_id, doc_id)`.
6. Recalculate stats once.

There should be no automatic fallback from rebuild to per-document indexing.
If `dblink` or the staging mechanism is unavailable, fail loudly.

### 6. Migrate Trigger Sync

Triggers should enqueue deltas:

- insert/update -> `bm25_enqueue_document_delta`
- delete -> `bm25_enqueue_delete_delta`

Flush should happen through:

```sql
SELECT * FROM facets.bm25_flush_pending_terms(NULL, 50000);
```

Operational options:

- application calls flush after a batch of writes
- scheduled database job
- explicit admin command
- future background worker

### 7. Drop Old JSONB Column

Only after read/write paths and backfill validation are complete:

```sql
ALTER TABLE facets.bm25_index DROP COLUMN term_freqs;
```

For compatibility, a transitional release can keep the column unused.

## Transaction And Concurrency Notes

### Bulk Merge

Bulk merge should happen in a transaction:

- staging table creation
- worker completion
- grouped merge
- stats recalculation
- staging cleanup

Worker staging tables should be `UNLOGGED` where possible.

### Delta Flush

Flush needs stable batch identity. Include `created_at` or a generated batch id
when copying pending rows into the temp batch table, then delete only those exact
rows after successful merge.

Avoid deleting by `(table_id, doc_id, term_hash, op)` alone because a concurrent
enqueue could otherwise be removed accidentally.

### Hot Terms

Hot terms still rewrite large roaring bitmaps during merge, but the rewrite
happens once per flush/bulk group instead of once per document. That is the main
write-amplification reduction.

## Statistics Plan

PostgreSQL should match SQLite and store total length directly:

```sql
ALTER TABLE facets.bm25_statistics
ADD COLUMN total_document_length bigint NOT NULL DEFAULT 0;
```

Then incremental update can be exact:

```text
new_total_length = old_total_length - old_doc_length + new_doc_length
avg_document_length = new_total_length / total_documents
```

Backfill:

```sql
UPDATE facets.bm25_statistics s
SET total_document_length = COALESCE(d.total_length, 0)
FROM (
    SELECT table_id, SUM(doc_length)::bigint AS total_length
    FROM facets.bm25_documents
    GROUP BY table_id
) d
WHERE s.table_id = d.table_id;
```

## Validation Checklist

Run these checks after migration:

- `bm25_search` returns the same top results before and after migration.
- `bm25_score` returns scores within expected tolerance.
- `bm25_get_matches_bitmap_native` still returns correct candidate sets.
- `bm25_term_stats` reports expected `ndoc` and `nentry`.
- deleting a document removes all normalized term-frequency rows.
- updating a document replaces old frequencies and postings.
- rebuilding an index leaves no pending deltas.
- pending flush is idempotent when called repeatedly.

## Performance Benchmarks

Measure:

- rebuild documents/sec
- terms/sec extracted by workers
- grouped merge duration
- trigger enqueue latency
- flush latency for 1k, 10k, and 50k pending rows
- query latency before/after normalized frequency reads
- storage size of old JSONB layout vs normalized rows

## Rollout Strategy

1. Ship additive schema and dual-read compatibility if needed.
2. Backfill `bm25_term_frequencies`.
3. Switch reads to normalized frequencies.
4. Switch writes to normalized frequencies.
5. Switch triggers to pending deltas.
6. Enable explicit flush in application/admin workflow.
7. Validate rebuild/search parity.
8. Drop old JSONB column in a later cleanup migration.

## Open Items

- Decide whether flush should be application-driven or scheduled.
- Decide whether `bm25_statistics.total_document_length` should be added in the
  same migration or separately.
- Add SQL integration coverage for pending delta flush.
- Add migration tests from old JSONB layout.
- Benchmark normalized frequency lookup against JSONB extraction for common
  query shapes.
