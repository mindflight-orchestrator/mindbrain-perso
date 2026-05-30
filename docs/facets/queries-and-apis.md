# Queries and APIs

Facets can be queried through direct SQLite helpers, HTTP routes, CLI commands,
and search APIs that compose with BM25/vector retrieval.

## HTTP routes

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` | `/api/mindbrain/collections/facet-search` | Search raw/derived collection facets by workspace, collection, optional table id, namespace, dimension, value, and limit. |
| `GET` | `/api/mindbrain/search-compact-info` | TOON snapshot of compact search artifacts. |
| `POST` | `/api/mindbrain/search-embedding-upsert` | Store JSON embedding arrays into `search_embeddings.embedding_blob`. |

`/api/mindbrain/collections/facet-search` accepts:

- `workspace_id`
- `collection_id`
- optional `table_id`
- optional `namespace`
- optional `dimension`
- optional `value`
- optional `limit`

## CLI surfaces

| Command | Purpose |
| --- | --- |
| `document-ingest` | Persist raw document/chunk rows and facet assignments. |
| `document-profile-worker` | Optional contextual retrieval indexing and embedding writes. |
| `contextual-search` | BM25 search, optional vector fusion, optional rerank. |
| `search-embedding-batch` | Backfill indexed embeddings for `search_documents`. |
| `search-compact-info` | Inspect compact BM25/search artifact counts. |
| `benchmark-db` | Runs deterministic facet and graph query/mutation benchmark paths. |

## Facet helper behavior

Important SQLite-backed helpers in `facet_sqlite.zig`:

| Helper | Purpose |
| --- | --- |
| `ensureFacetDefinitionId` | Resolve or create a facet id for `(table_id, facet_name)`. |
| `refreshFacets` / `mergeDeltas` | Merge pending deltas into postings. |
| `getFacetCounts` | Count documents per value for one facet. |
| `topValues` | Top-N facet values by document count. |
| `filterDocumentsByFacetsBitmap` | Build a bitmap for ANDed facet filters. |
| `filterDocumentsByFacets` | Return document ids satisfying all filters. |
| `listTableFacets` / `listTableFacetNames` | Discover configured facets. |
| `describeFacetTable` | Summarize table definitions and postings. |

## SQL snippets

List facet tables:

```sql
SELECT table_id, schema_name, table_name, chunk_bits
FROM facet_tables
ORDER BY table_id;
```

List facet definitions:

```sql
SELECT fd.table_id, ft.table_name, fd.facet_id, fd.facet_name
FROM facet_definitions fd
JOIN facet_tables ft ON ft.table_id = fd.table_id
ORDER BY fd.table_id, fd.facet_id;
```

Count raw assignments by dimension:

```sql
SELECT namespace || '.' || dimension AS facet_name,
       COUNT(*) AS assignments,
       COUNT(DISTINCT value) AS values_count
FROM facet_assignments_raw
WHERE workspace_id = 'immeuble-demo'
GROUP BY namespace, dimension
ORDER BY assignments DESC, facet_name;
```

Inspect derived postings:

```sql
SELECT fd.facet_name, fp.facet_value, fp.chunk_id, length(fp.posting_blob) AS bytes
FROM facet_postings fp
JOIN facet_definitions fd
  ON fd.table_id = fp.table_id
 AND fd.facet_id = fp.facet_id
WHERE fp.table_id = 77001
ORDER BY fd.facet_name, fp.facet_value, fp.chunk_id;
```

List search documents:

```sql
SELECT table_id, doc_id, language, substr(content, 1, 120) AS preview
FROM search_documents
ORDER BY table_id, doc_id
LIMIT 20;
```

Check embedding coverage:

```sql
SELECT sd.table_id,
       COUNT(*) AS search_docs,
       COUNT(se.doc_id) AS embedded_docs
FROM search_documents sd
LEFT JOIN search_embeddings se
  ON se.table_id = sd.table_id
 AND se.doc_id = sd.doc_id
GROUP BY sd.table_id
ORDER BY sd.table_id;
```

## Choosing the right query path

| Need | Preferred path |
| --- | --- |
| Facet navigation/counts | `getFacetCounts`, `topValues`, or collection facet search. |
| Exact facet filtering | `filterDocumentsByFacets*`. |
| Text retrieval | `contextual-search` BM25 mode. |
| Vector-aware retrieval | `contextual-search` with embedding flags and indexed embeddings. |
| Rich graph traversal | Graph APIs under `docs/graphs/`. |
| Projection bundle retrieval | Projection APIs, not facets. |
