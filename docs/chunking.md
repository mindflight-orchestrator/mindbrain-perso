# Document Chunking, Nanoids, and Source Facets

This document covers the **chunking pipeline** that turns a raw document
into the rows persisted in `documents_raw`, `chunks_raw`,
`facet_assignments_raw` (under the auto-managed `source.*` namespace),
and `external_links_raw`.

It complements [`collections.md`](./collections.md) (the raw layer
contract) and [`facets.md`](./facets.md) (how the derived facet index is
rebuilt from these rows).

## Public vs. internal IDs

Every document carries two identifiers:

| Field | Type | Scope | Used for |
|-------|------|-------|----------|
| `doc_id` | `bigint` | Unique within `(workspace, collection)` | Internal joins everywhere (raw layer FKs, BM25 doc id, graph entity-document links, …). |
| `doc_nanoid` | URL-safe text, 21 chars by default | Globally unique across the database | The **only** identifier that may appear in URLs or any externally facing surface. |

Chunks are addressed externally as `<doc_nanoid>#<chunk_index>`. Internal
joins still use `(workspace_id, collection_id, doc_id, chunk_index)`.

The nanoid alphabet is URL-safe
(`0-9 A-Z a-z _ -`) and the generator uses `std.crypto.random` with
rejection sampling so every alphabet position has equal probability.
See `src/standalone/nanoid.zig` for the implementation and tests.

The schema enforces uniqueness of every assigned `doc_nanoid` via a
**partial** unique index. Empty string remains an internal "not supplied"
sentinel for upsert helpers; public document URLs should always use a
generated nanoid.

## Chunking strategies

The chunker exposes seven strategies via `chunker.Strategy`. Each chunk
records the strategy that produced it so the facet/BM25 reindex paths
can re-derive consistent metadata later.

| Strategy | What it splits on | When to use |
|---------|--------------------|-------------|
| `fixed_token` | A target token count with optional overlap | Embedding pipelines that need a stable token budget per chunk. |
| `sentence` | Sentence boundaries with a soft `max_chars` ceiling | Short, sentence-grained context windows. |
| `paragraph` | Blank lines | Markdown / plain text where paragraphs are the natural unit. |
| `recursive_character` | Configurable separators, recursively, until each chunk fits `max_chars` | LangChain-style splitter; the safest deterministic default. |
| `structure_aware` | Markdown headings and code fences, then falls back to recursive splits | Code or Markdown documentation where structural boundaries matter. |
| `semantic` | Sentence-level cosine similarity (requires `EmbedSentencesFn`) | Quality > throughput; produces semantically coherent chunks. |
| `late` | Embeds the entire document then slices by token offsets (requires `EmbedFullDocFn`) | "Late chunking" for embedding models that benefit from full-document context. |

Embedding-aware strategies take a callback so the chunker stays
embedding-model-agnostic.

`countTokens` provides the simple whitespace + punctuation tokenizer
used for `fixed_token` budgeting and recorded in
`chunks_raw.token_count`.

## Auto-extracted `source.*` facets

`chunker.deriveSourceFacets(allocator, ctx, chunk, total_chunks)`
returns one `FacetAssignmentRawSpec` per row below, all under the
built-in `source` namespace and the workspace's default ontology
(bootstrapped by `ensureSourceNamespace` from `ensureWorkspace`).

| Dimension | Source | Multi-valued? |
|-----------|--------|---------------|
| `source.path` | Cleaned `source_ref` (URI scheme stripped) | No |
| `source.filename` | Basename of the path | No |
| `source.extension` | Lowercase extension (no leading dot) | No |
| `source.dir` | Cumulative directory prefixes along the path (e.g. `data`, `data/notes`, `data/notes/policies` for `/data/notes/policies/x.md`) | **Yes** (one row per prefix) |
| `source.ingested_at` | Caller-supplied ISO-8601 timestamp | No |
| `source.chunk_index` | The chunk's own index | No |
| `source.chunk_count` | Total chunks for the document | No |
| `source.strategy` | The chunker `Strategy.label()` that produced the chunk | No |

The arena returned with the rows owns every duplicated string; the
caller frees the lot with `SourceFacets.deinit()`.

## End-to-end ingest

The single recommended entry point is `Pipeline.ingestDocumentChunked`
(`src/standalone/import_pipeline.zig`). It:

1. Generates a fresh `doc_nanoid` if the caller did not supply one and
   upserts the parent `documents_raw` row.
2. Resolves (or creates) the workspace's default ontology so
   `source.*` rows have somewhere to live.
3. Runs the configured chunker.
4. Upserts each chunk into `chunks_raw` with its `strategy`,
   `token_count`, and (for `late`) `parent_chunk_index`.
5. Emits the `source.*` facet rows for every chunk via
   `deriveSourceFacets`.
6. Optionally syncs both the parent document and each chunk into BM25
   under the supplied `bm25_table_id` / `chunk_bm25_table_id`. Chunks
   are addressed in BM25 with the synthetic id
   `(doc_id << chunk_bits) | chunk_index`.

`Pipeline.linkExternal` is the matching helper for storing outbound
links (URLs found inside a document, references in a PDF, …) into
`external_links_raw`. The CLI surfaces this as `external-link-add`.

## CLI surfaces

The standalone tool exposes three verbs that mirror the Pipeline API:

```bash
mindbrain-standalone-tool document-ingest \
    --db data.sqlite \
    --workspace-id ws --collection-id ws::docs --doc-id 1 \
    --source-ref "/data/notes/intro.md" \
    --strategy structure_aware \
    --content-file ./intro.md
# -> ingested doc_id=1 nanoid=... chunks=N

mindbrain-standalone-tool document-by-nanoid --db data.sqlite --nanoid <id>
# -> workspace_id=... collection_id=... doc_id=...

mindbrain-standalone-tool external-link-add --db data.sqlite \
    --workspace-id ws --source-collection-id ws::docs --source-doc-id 1 \
    --target-uri https://example.com/spec --edge-type cites
```

## Postgres mirror

The Postgres extension install script
(`sql/sqlite_mindbrain--1.0.0.sql`) creates the same columns and
`external_links_raw` table on the `mb_collections` schema so that imports /
exports stay byte-compatible between the SQLite standalone build and the
Postgres extension build.

## Reindexing notes

`Pipeline.reindexBm25` accepts an optional `chunk_table_id` plus
`chunk_bits` so chunks can be re-indexed alongside their parent
documents using the same synthetic id formula as the live ingest path.
`reindexAll` wires this through automatically.

## LLM-assisted profile and legal splitters

The chunker strategies above are deterministic. For **corpus-specific** splitter choice (including legal article boundaries), the standalone tool can call an LLM to produce a validated document profile, map it through `chunking_policy`, and optionally persist chunks to `documents_raw` / `chunks_raw`. See [document-profile.md](document-profile.md).

The profile worker can also enable **contextual retrieval** with `--contextual-retrieval`. In that mode, each chunk receives a short LLM-generated context that situates it within the document, following Anthropic's Contextual Retrieval recommendation to prepend chunk-specific context before embedding and BM25 indexing. The original `chunks_raw.content` remains unchanged; the contextual text is stored in `chunks_raw.metadata_json.contextual_retrieval.contextualized_content`. When `--contextual-search-table-id` and `--embedding-model` are supplied, the worker immediately indexes that contextualized text into BM25/search artifacts and writes matching embeddings to `search_embeddings` plus `chunks_raw_vector`.

## Reserved fields

`documents_raw.summary` is provisioned for the LLM-generated document
summary that is intended to back context packing. There is no API
that writes to it yet; the column is reserved so that a future plan
can populate it without another migration.
