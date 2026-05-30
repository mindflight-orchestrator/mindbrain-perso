# Ingestion and reindex

Facet ingestion is raw-first. The main orchestration lives in
[`src/standalone/import_pipeline.zig`](../../src/standalone/import_pipeline.zig).

## Recommended flow

1. Ensure workspace, collection, and ontology rows.
2. Persist raw documents and chunks.
3. Persist raw facet assignments.
4. Register or resolve a facet table id for the collection.
5. Reindex facet assignments into bitmap postings.
6. Optionally reindex BM25/search artifacts for the same documents/chunks.

## Write helpers

Important `Pipeline` methods:

| Method | Role |
| --- | --- |
| `createWorkspace` | Ensures workspace metadata. |
| `createCollection` | Ensures the collection. |
| `registerOntology` | Loads ontology vocabulary. |
| `attachOntologyToCollection` | Connects ontology and collection. |
| `ingestDocumentRaw` / `ingestChunkRaw` | Writes raw document and chunk content. |
| `assignFacetRaw` | Writes one raw facet assignment. |
| `ingestDocumentChunked` | Creates document/chunk rows and source facets through the chunking pipeline. |
| `reindexFacets` | Replays `facet_assignments_raw` into `facet_postings`. |
| `reindexBm25` | Replays raw document/chunk text into search and BM25 artifacts. |
| `reindexAll` | Runs BM25, facet, and graph reindex paths together. |

## Reindex facets

`Pipeline.reindexFacets(workspace_id, collection_id, table_id)`:

- reads `facet_assignments_raw` for the workspace and collection;
- only projects `target_kind = 'doc'` rows;
- derives facet names as `namespace.dimension`;
- ensures each facet definition exists;
- groups pending assignments by `doc_id`;
- flushes each document group into the existing facet posting path.

The return value is the number of raw assignments replayed.

## Reindex BM25/search

`Pipeline.reindexBm25(workspace_id, collection_id, options)`:

- reads `documents_raw` and writes `search_documents` for `options.table_id`;
- optionally reads `chunks_raw` and writes chunk text to `options.chunk_table_id`;
- computes synthetic chunk doc ids with `chunkSyntheticId(doc_id, chunk_index, chunk_bits)`;
- calls `search_sqlite.syncSearchDocumentIfTriggered` so BM25/FTS artifacts stay aligned when a trigger row exists.

`search_sqlite.upsertSearchDocument` writes both `search_documents` and the FTS5
mapping/table rows. Compact BM25 artifacts are maintained by search sync/rebuild
paths.

## Embedding writes

Embeddings are indexed before search:

- `document-profile-worker --contextual-retrieval ... --embedding-model ...`
  can write contextualized text and embeddings.
- `search-embedding-batch` backfills `search_embeddings`.
- `POST /api/mindbrain/search-embedding-upsert` stores a JSON embedding array
  as packed little-endian `f32` bytes.

Live search may embed the query, but it does not create missing indexed
document embeddings.

## Consistency boundaries

- A raw facet assignment is not queryable through bitmap helpers until it has
  been reindexed.
- A raw document is not searchable through FTS/BM25 until it exists in
  `search_documents` and its search artifacts are synced.
- A vector query only runs when indexed embeddings already exist for the target
  `table_id`.
- Facet filters and graph traversal are separate. If a workflow needs both, it
  must deliberately join/filter candidate ids rather than pruning graph topology
  accidentally.

## Verification snippets

Count raw and derived facet rows:

```bash
sqlite3 -readonly data/immeuble-demo.sqlite "
SELECT 'facet_assignments_raw', COUNT(*) FROM facet_assignments_raw
UNION ALL SELECT 'facet_tables', COUNT(*) FROM facet_tables
UNION ALL SELECT 'facet_definitions', COUNT(*) FROM facet_definitions
UNION ALL SELECT 'facet_postings', COUNT(*) FROM facet_postings;
"
```

Check raw facet dimensions:

```sql
SELECT namespace || '.' || dimension AS facet_name,
       COUNT(*) AS assignments,
       COUNT(DISTINCT value) AS values_count
FROM facet_assignments_raw
WHERE workspace_id = 'immeuble-demo'
GROUP BY namespace, dimension
ORDER BY assignments DESC, facet_name;
```
