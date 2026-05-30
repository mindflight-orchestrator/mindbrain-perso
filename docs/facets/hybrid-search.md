# Hybrid search

Hybrid search combines lexical BM25 candidates with optional vector candidates
and optional second-stage LLM reranking.

For the original process-level walkthrough, see
[`docs/faceted-hybrid-search.md`](../faceted-hybrid-search.md).

## Index families

| Family | Tables | Role |
| --- | --- | --- |
| Facets | `facet_tables`, `facet_definitions`, `facet_postings`, `facet_deltas` | Structured filtering and counts. |
| FTS5/BM25 | `search_documents`, `search_fts_docs`, `search_fts`, compact BM25 stats/postings | Lexical retrieval and scoring. |
| Embeddings | `search_embeddings` | Vector nearest-neighbor search. |

## Indexing flow

1. Persist raw source text into `documents_raw` and `chunks_raw`.
2. Persist raw facet decisions into `facet_assignments_raw`.
3. Reindex facets into `facet_postings`.
4. Write searchable text to `search_documents`.
5. Maintain `search_fts_docs` and `search_fts`.
6. Maintain compact BM25 stats/postings.
7. Optionally write embeddings to `search_embeddings`.

Search is read-only for indexed documents. Query-time provider calls can embed
the query, but they do not create missing document embeddings.

## Retrieval modes

| Condition | Mode | Provider call | Status |
| --- | --- | --- | --- |
| No embedding flags | BM25 only | No | `semantic_status: "not_requested"` |
| Embedding flags but no indexed embeddings | BM25 only | No | `semantic_status: "no_indexed_embeddings"` |
| Embedding flags and indexed embeddings exist | BM25 + vector | Query embedding only | `semantic_status: "ok"` |

## BM25 path

The lexical path uses SQLite FTS5:

- `search_documents` stores logical text rows.
- `search_fts_docs` maps logical ids to FTS row ids.
- `search_fts` stores FTS5 content.
- Search orders by SQLite `bm25(search_fts)`.

Compact BM25 tables also store collection/document/term stats and posting
bitmaps for repository-backed scoring.

## Vector path

`search_embeddings` stores packed little-endian `f32` vectors keyed by
`(table_id, doc_id)`. The query embedding must use the same model family as the
stored document or chunk embeddings.

The vector repository scopes by `table_id`, so `contextual-search --table-id`
does not mix embeddings from other logical search tables.

## Score fusion

When both BM25 and vector results are available:

```text
combined_score = bm25_score * (1.0 - vector_weight) + vector_score * vector_weight
```

Default `vector_weight` is `0.5`. This is deterministic weighted fusion, not a
neural reranker.

## LLM rerank

`--rerank` is optional and explicit. The reranker receives candidate ids, fused
scores, and clipped text from `search_documents`; it returns scores keyed by
`doc_id`.

Reranking does not replace retrieval. It only sorts the candidate set produced
by BM25/vector search.

## Facets with search

Facets can constrain or explain result sets:

- apply facet filters before retrieval when the candidate set must be scoped;
- apply facet filters after retrieval for UI refinement;
- count facet values over a search/vector candidate bitmap for navigation;
- avoid using facets to prune graph topology unless that is explicitly the
  desired analysis.
