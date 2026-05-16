# SQLite Vector Search Testing

This guide explains how to test the SQLite embedding search path that works
without `pgvector`.

The important distinction is:

- `documents_raw` and `chunks_raw` store text.
- `documents_raw_vector` and `chunks_raw_vector` store embeddings.
- Vector search can only return semantic matches after embeddings have been
  written to the vector tables.

The unit tests seed small deterministic vectors directly, so they validate the
SQLite vector backend without calling an embedding provider. A real semantic
smoke test needs an embedding generation step first.

## Fillable `.env`

Create a local `.env.vector-search` file. Do not commit it.

```bash
# Zig 0.16 binary on your PATH, or an absolute path (do not rely on IDE-local toolchains).
MINDBRAIN_ZIG="$(command -v zig)"

# SQLite database used for manual smoke tests.
MINDBRAIN_DB=data/vector-search-smoke.sqlite

# Workspace/collection used by raw collection tables.
MINDBRAIN_WORKSPACE_ID=vector_smoke
MINDBRAIN_COLLECTION_ID=vector_smoke::docs
MINDBRAIN_COLLECTION_NAME="Vector smoke docs"

# Test source files for document-profile enqueue/worker.
MINDBRAIN_TEXT_DIR=fixtures/corpus_eval/legal_article
MINDBRAIN_DOC_ID_START=1

# Optional LLM profile provider. Used by document-profile-worker.
MINDBRAIN_PROFILE_BASE_URL=https://api.openai.com/v1
MINDBRAIN_PROFILE_API_KEY=
MINDBRAIN_PROFILE_MODEL=gpt-4.1-mini

# Optional embedding provider. Used by document-profile-worker when
# --contextual-retrieval is paired with --embedding-model.
MINDBRAIN_EMBED_PROVIDER=openai_compat
MINDBRAIN_EMBED_BASE_URL=https://api.openai.com/v1
MINDBRAIN_EMBED_API_KEY=
MINDBRAIN_EMBED_MODEL=text-embedding-3-small

# Search settings.
MINDBRAIN_VECTOR_SCOPE=chunks
MINDBRAIN_VECTOR_METRIC=cosine
MINDBRAIN_VECTOR_LIMIT=5
```

Load it before running commands:

```bash
set -a
. ./.env.vector-search
set +a
```

## What Works Today

The current implementation provides:

- little-endian `f32` BLOB encode/decode helpers in `src/standalone/vector_blob.zig`;
- shared cosine, L2, and inner-product scoring in `src/standalone/vector_distance.zig`;
- exact SQLite vector search in `src/standalone/vector_sqlite_exact.zig`;
- document search over `documents_raw_vector`;
- chunk search over `chunks_raw_vector`, preserving `doc_id` and `chunk_index`;
- a `VectorRepository` adapter for hybrid search and query-executor consumers.

The implementation does not vendor `sqlite-vec`, `vectorlite`, or USearch yet.
Those remain optional acceleration backends for later.

## Test 1: Backend Mechanics Without Real Embeddings

This is the fastest regression test. It inserts deterministic vectors like
`[1.0, 0.0]` and `[0.0, 1.0]` inside unit tests, then verifies ranking.

```bash
"$MINDBRAIN_ZIG" build test
```

This validates:

- BLOB encoding and decoding;
- SQLite readback from raw vector tables;
- cosine/L2/inner-product scoring;
- top-k sorting;
- chunk result mapping;
- hybrid search wiring.

It does not validate real semantic quality because no model embeddings are
generated.

## Test 2: Prove the Empty State

If you only run document ingestion or document profiling without contextual
embedding flags, vector tables may be empty. With `document-profile-worker
--contextual-retrieval --contextual-search-table-id <n> --embedding-model
<name>`, the worker writes contextual chunk embeddings during profiling.

Build the CLI:

```bash
"$MINDBRAIN_ZIG" build standalone-tool
```

Create a database and persist profiled documents/chunks:

```bash
mkdir -p "$(dirname "$MINDBRAIN_DB")"

./zig-out/bin/mindbrain-standalone-tool document-profile-enqueue \
  --db "$MINDBRAIN_DB" \
  --content-dir "$MINDBRAIN_TEXT_DIR" \
  --include-ext txt,md \
  --workspace-id "$MINDBRAIN_WORKSPACE_ID" \
  --collection-id "$MINDBRAIN_COLLECTION_ID" \
  --doc-id-start "$MINDBRAIN_DOC_ID_START"

./zig-out/bin/mindbrain-standalone-tool document-profile-worker \
  --db "$MINDBRAIN_DB" \
  --base-url "$MINDBRAIN_PROFILE_BASE_URL" \
  --api-key "$MINDBRAIN_PROFILE_API_KEY" \
  --model "$MINDBRAIN_PROFILE_MODEL" \
  --contextual-retrieval \
  --contextual-search-table-id 77 \
  --embedding-base-url "$MINDBRAIN_EMBED_BASE_URL" \
  --embedding-api-key "$MINDBRAIN_EMBED_API_KEY" \
  --embedding-model "$MINDBRAIN_EMBED_MODEL" \
  --limit 10
```

Inspect row counts:

```bash
sqlite3 "$MINDBRAIN_DB" <<'SQL'
SELECT 'documents_raw', COUNT(*) FROM documents_raw;
SELECT 'chunks_raw', COUNT(*) FROM chunks_raw;
SELECT 'documents_raw_vector', COUNT(*) FROM documents_raw_vector;
SELECT 'chunks_raw_vector', COUNT(*) FROM chunks_raw_vector;
SQL
```

Expected result with the contextual embedding flags:

- `documents_raw` and `chunks_raw` should have rows.
- `chunks_raw_vector` should have one row per persisted chunk.
- `search_embeddings` should have one row per contextualized BM25 chunk row.

## Test 3: Real Semantic Smoke Test

To test real semantic search, run the contextual profile worker as shown above.
The worker:

1. reads each persisted chunk;
2. generates contextual retrieval text;
3. calls the configured embedding provider;
4. encodes returned `[]f32` as little-endian bytes;
5. writes `chunks_raw_vector` and matching `search_embeddings` rows.

The library pieces already exist:

- call embeddings through `src/standalone/llm/manager.zig`;
- encode vectors with `vector_blob.encodeF32Le`;
- persist document vectors with `Pipeline.ingestDocumentVector`;
- persist chunk vectors with `Pipeline.ingestChunkVector`;
- search with `vector_sqlite_exact.Repository`.

The raw vector backend can then be queried through `vector_sqlite_exact`, and
the compact search store can use the `search_embeddings` rows for hybrid BM25
+ vector ranking.

The CLI also exposes that runtime path:

```bash
./zig-out/bin/mindbrain-standalone-tool contextual-search \
  --db "$MINDBRAIN_DB" \
  --table-id 77 \
  --query "How can the contract end?" \
  --base-url "$MINDBRAIN_EMBED_BASE_URL" \
  --api-key "$MINDBRAIN_EMBED_API_KEY" \
  --embedding-model "$MINDBRAIN_EMBED_MODEL" \
  --limit "$MINDBRAIN_VECTOR_LIMIT" \
  --vector-weight 0.65
```

`contextual-search` is BM25-only unless embedding provider flags are supplied.
Even with those flags, it only embeds the live query when `search_embeddings`
already contains indexed rows for the table. It does not create document or
chunk embeddings during search.

To backfill `search_embeddings` for existing `search_documents` rows outside
the live request path:

```bash
./zig-out/bin/mindbrain-standalone-tool search-embedding-batch \
  --db "$MINDBRAIN_DB" \
  --table-id 77 \
  --embedding-base-url "$MINDBRAIN_EMBED_BASE_URL" \
  --embedding-api-key "$MINDBRAIN_EMBED_API_KEY" \
  --embedding-model "$MINDBRAIN_EMBED_MODEL" \
  --missing-only
```

Optional real LLM reranking is separate from vector fusion and is never the
default:

```bash
./zig-out/bin/mindbrain-standalone-tool contextual-search \
  --db "$MINDBRAIN_DB" \
  --table-id 77 \
  --query "How can the contract end?" \
  --base-url "$MINDBRAIN_EMBED_BASE_URL" \
  --api-key "$MINDBRAIN_EMBED_API_KEY" \
  --embedding-model "$MINDBRAIN_EMBED_MODEL" \
  --rerank \
  --rerank-base-url "$MINDBRAIN_PROFILE_BASE_URL" \
  --rerank-api-key "$MINDBRAIN_PROFILE_API_KEY" \
  --rerank-model "$MINDBRAIN_PROFILE_MODEL"
```

## Minimal Zig Smoke Shape

A provider-backed smoke harness should follow this shape:

```zig
const mindbrain = @import("mindbrain");

// 1. Open DB and read chunks_raw rows for the workspace/collection.
// 2. Call mindbrain.llm.Manager.embedTexts(...).
// 3. For each returned vector, call Pipeline.ingestChunkVector(...).
// 4. Create vector_sqlite_exact.Repository with scope .chunks.
// 5. Embed the query text and call repo.searchNearestChunks(...).
```

The expected assertion is not a hard-coded vector value. Instead, use a tiny
fixture where the semantic intent is obvious, for example:

- chunk 0: "The contract may be terminated for breach."
- chunk 1: "The backup cadence is weekly."
- query: "How can the contract end?"

With real embeddings, chunk 0 should rank above chunk 1.

## SQL Checks

After embeddings are written, these queries should show non-zero vector rows:

```bash
sqlite3 "$MINDBRAIN_DB" <<SQL
SELECT doc_id, dim, length(embedding_blob)
FROM documents_raw_vector
WHERE workspace_id = '$MINDBRAIN_WORKSPACE_ID'
  AND collection_id = '$MINDBRAIN_COLLECTION_ID'
ORDER BY doc_id
LIMIT 5;

SELECT doc_id, chunk_index, dim, length(embedding_blob)
FROM chunks_raw_vector
WHERE workspace_id = '$MINDBRAIN_WORKSPACE_ID'
  AND collection_id = '$MINDBRAIN_COLLECTION_ID'
ORDER BY doc_id, chunk_index
LIMIT 10;
SQL
```

For `f32` vectors, `length(embedding_blob)` must equal `dim * 4`.

## Expected Failure Modes

| Symptom | Meaning | Fix |
|---------|---------|-----|
| Vector search returns empty results | No rows in `*_raw_vector`, wrong workspace/collection, or dimension mismatch | Check vector table counts and query vector dimensions |
| Rows exist but all searches look wrong | Provider/model mismatch or bad text used for embeddings | Confirm query and stored vectors use the same embedding model |
| `length(embedding_blob) != dim * 4` | Blob was not written as little-endian `f32` | Write through `vector_blob.encodeF32Le` |
| Profiling succeeds but vector tables stay empty | Contextual embedding flags were omitted | Add `--contextual-retrieval --contextual-search-table-id <n> --embedding-model <name>` |

## Recommended Next Step

For non-contextual or backfill workflows, add a small `document-embed-worker` or
`collection-embed` CLI command that uses the `.env` variables above, embeds
`chunks_raw.contextualized_content` when
available, writes `chunks_raw_vector`, and then runs a top-k smoke query. That
would make real semantic validation one command instead of a custom harness.
