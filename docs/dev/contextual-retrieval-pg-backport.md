# Contextual Retrieval PG Backport Notes

This note captures the SQLite-side update that should be ported to the sibling
Postgres repository.

The change has two independent parts:

- contextual retrieval now performs real indexing, not metadata-only storage;
- the standalone LLM HTTP client now deinitializes `std.http.Client` to release
  TLS certificate bundle allocations after HTTPS calls.

## Source Changes In This Repo

Primary implementation:

- `src/standalone/tool.zig`
- `src/standalone/llm/http_client.zig`

Docs updated here:

- `docs/document-profile.md`
- `docs/chunking.md`
- `docs/dev/sqlite-vector-search-testing.md`

## Intended Behavior

`document-profile-worker --contextual-retrieval` now follows Anthropic's
Contextual Retrieval preprocessing contract:

1. profile the document;
2. choose the chunking policy;
3. persist original chunk text in `chunks_raw.content`;
4. generate chunk-specific retrieval context;
5. store the context and `contextualized_content` in chunk metadata;
6. index `contextualized_content` into BM25;
7. embed `contextualized_content`;
8. store the embedding beside the same logical chunk id used by search;
9. allow runtime hybrid search to embed the query, run BM25 + vector retrieval,
   and rank by the existing hybrid scorer.

The original chunk text remains the raw evidence. Only derived search/vector
indexes receive the context-prepended text.

## New CLI Surface

`document-profile-worker` gained these contextual indexing flags:

```bash
--contextual-retrieval
--contextual-search-table-id <n>
--embedding-base-url <url>       # optional; defaults to --base-url
--embedding-api-key <key>        # optional; defaults to --api-key
--embedding-model <name>
--contextual-chunk-bits <n>      # optional; default 8
```

`--contextual-retrieval` now requires `--contextual-search-table-id` and
`--embedding-model`, because the worker performs indexing immediately.

A runtime smoke command was also added:

```bash
mindbrain-standalone-tool contextual-search \
  --db <sqlite_path> \
  --table-id <n> \
  --query <text> \
  --base-url <url> \
  --api-key <key> \
  --embedding-model <name> \
  --limit 5 \
  --vector-weight 0.65
```

For Postgres, the equivalent can be a SQL function, worker command, or API
endpoint. The important part is the same runtime flow: embed query, tokenize
query for BM25, retrieve from both indexes, merge/rank.

## SQLite Storage Path

The SQLite worker writes:

- `chunks_raw`: original chunk content and contextual metadata;
- `search_documents`: contextualized chunk text for BM25;
- `search_document_stats`, `search_term_stats`, `search_term_frequencies`,
  `search_postings`: BM25 artifacts generated from contextualized chunk text;
- `search_embeddings`: embedding rows keyed by the same synthetic chunk id used
  in `search_documents`;
- `chunks_raw_vector`: collection-scoped chunk embeddings keyed by
  `(workspace_id, collection_id, doc_id, chunk_index)`.

The synthetic search doc id is:

```zig
(doc_id << chunk_bits) | chunk_index
```

The Postgres backport should keep the BM25 id and vector id aligned. If the PG
repo has native chunk ids, use those directly. If it mirrors this repo's
synthetic id scheme, use the same `chunk_bits` value everywhere BM25 and vector
rows join.

## Postgres Mapping

Suggested mapping from this repo to the PG implementation:

| SQLite implementation | PG equivalent to port |
| --- | --- |
| `chunks_raw.content` | original chunk/evidence content column |
| `chunks_raw.metadata_json.contextual_retrieval.contextualized_content` | JSONB metadata or derived contextual text column |
| `search_documents(table_id, doc_id, content)` | BM25 document table/input using contextualized chunk text |
| `search_embeddings(table_id, doc_id, embedding_blob)` | pgvector row keyed to BM25 doc id |
| `chunks_raw_vector` | raw chunk vector table, likely `bytea` or `vector` |
| `search_sqlite.syncSearchDocument` | PG BM25 index/upsert function/trigger |
| `llm.Manager.embedTexts` | existing provider-backed embedding call |
| `hybrid_search.search` | PG hybrid search/rank fusion/rerank path |

Do not overwrite the raw chunk content with contextualized text. Contextual text
is a derived retrieval artifact.

## Backport Checklist

1. Add worker options for contextual search table, embedding provider URL/key,
   embedding model, and chunk id bits if the PG repo uses synthetic ids.
2. After generating `contextualized_content`, upsert the original chunk as
   before.
3. BM25-index `contextualized_content`, not the original chunk content.
4. Embed `contextualized_content` with the configured embedding model.
5. Store the vector in the PG vector/raw-vector table.
6. Ensure BM25 and vector rows use the same retrievable chunk identifier.
7. Add or update the runtime search path to embed the query and combine BM25
   and vector candidates.
8. Add a smoke test that proves contextualized chunk rows appear in both BM25
   and vector storage.
9. Add a runtime search smoke that returns a hit with both BM25 and vector
   scores populated.

## Validation Shape

Minimal ingestion smoke:

```bash
document-profile-enqueue ... \
  --workspace-id contextual_smoke \
  --collection-id contextual_smoke_docs \
  --doc-id-start 1

document-profile-worker ... \
  --contextual-retrieval \
  --contextual-search-table-id 77 \
  --embedding-model text-embedding-3-small \
  --limit 1
```

Expected database checks:

```sql
-- Original evidence chunks exist.
SELECT count(*) FROM chunks_raw;

-- Contextual metadata was persisted.
SELECT count(*)
FROM chunks_raw
WHERE metadata_json::jsonb #>> '{contextual_retrieval,enabled}' = 'true';

-- BM25/search input rows exist for contextualized chunks.
SELECT count(*) FROM <bm25_document_table>
WHERE table_id = 77;

-- Vector rows exist for those same chunk ids.
SELECT count(*) FROM <chunk_vector_table_or_search_embeddings>
WHERE table_id = 77;
```

Runtime search smoke:

```bash
contextual-search ... \
  --table-id 77 \
  --query "How can the agreement end after breach?" \
  --embedding-model text-embedding-3-small \
  --vector-weight 0.65
```

Expected result:

- non-empty results;
- top rows expose BM25 score, vector score, and combined score;
- result ids map back to the same chunk ids inserted during indexing.

## HTTPS Leak Fix

The leak fix is independent but should be ported if the PG repo shares the same
Zig LLM HTTP client wrapper.

Before:

```zig
var client: std.http.Client = .{
    .allocator = allocator,
    .io = io,
};
const fetch_result = try client.fetch(...);
```

After:

```zig
var client: std.http.Client = .{
    .allocator = allocator,
    .io = io,
};
defer client.deinit();
const fetch_result = try client.fetch(...);
```

Why: in Zig 0.16, `std.http.Client` owns TLS/certificate bundle allocations
used by HTTPS. Without `client.deinit()`, live profile/embedding calls can
succeed but still report debug allocator leaks from
`std.crypto.Certificate.Bundle`.

Validation:

1. run one live HTTPS chat or embedding request with the debug allocator;
2. confirm the process exits without certificate bundle leak reports.

