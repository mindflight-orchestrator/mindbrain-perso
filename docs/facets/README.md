# MindBrain facets

This directory is the canonical reference for MindBrain facets, BM25/FTS5
search, optional indexed embeddings, and hybrid retrieval.

The SQLite implementation lives mostly in
[`src/standalone/facet_sqlite.zig`](../../src/standalone/facet_sqlite.zig),
[`src/standalone/facet_store.zig`](../../src/standalone/facet_store.zig),
[`src/standalone/search_sqlite.zig`](../../src/standalone/search_sqlite.zig),
[`src/standalone/search_store.zig`](../../src/standalone/search_store.zig), and
[`src/standalone/hybrid_search.zig`](../../src/standalone/hybrid_search.zig).

## Core rule

Facets have two distinct layers:

| Layer | Tables | Role |
| --- | --- | --- |
| Raw source of truth | `facet_assignments_raw`, ontology vocabulary tables, documents/chunks raw tables | Durable facet decisions. Back this up. |
| Derived serving index | `facet_tables`, `facet_definitions`, `facet_postings`, `facet_deltas`, `facet_value_nodes` | Roaring bitmap navigation/filter/count index. Rebuildable from raw assignments. |

Search is a related but separate index family:

| Family | Tables | Role |
| --- | --- | --- |
| FTS/BM25 | `search_documents`, `search_fts_docs`, `search_fts`, compact BM25 stats/postings | Lexical retrieval and scoring. |
| Embeddings | `search_embeddings`, optional raw vector tables | Vector retrieval and hybrid scoring. |

Do not conflate facets, lexical search, vector search, and graph search. They
compose, but they answer different questions.

## Documents

| Document | Use |
| --- | --- |
| [model-and-storage.md](model-and-storage.md) | Derived facet/search tables, Roaring bitmap model, and boundaries. |
| [raw-layer.md](raw-layer.md) | Raw facet assignments, ontology vocabularies, source facets, and document/chunk inputs. |
| [ingestion-and-reindex.md](ingestion-and-reindex.md) | How raw rows become facet postings and BM25/search artifacts. |
| [queries-and-apis.md](queries-and-apis.md) | Facet search, contextual search, HTTP/CLI routes, and SQL snippets. |
| [hybrid-search.md](hybrid-search.md) | BM25, embeddings, score fusion, and optional LLM reranking. |
| [examples-immeuble-demo.md](examples-immeuble-demo.md) | Read-only snapshot and regeneration queries for `data/immeuble-demo.sqlite`. |

## What is possible today

- Register logical facet tables and facet definitions.
- Index exact facet values into Roaring bitmap postings.
- Count facet values and filter document ids by facet equality.
- Preserve source-of-truth facet assignments in `facet_assignments_raw`.
- Derive built-in `source.*` facets during chunked document ingestion.
- Rebuild derived facet postings from raw assignments.
- Store search documents, FTS5 rows, compact BM25 statistics, and BM25 postings.
- Run BM25-only contextual search without provider calls.
- Add optional indexed embeddings and fuse vector results with BM25.
- Run optional LLM reranking as a second stage over retrieved candidates.

## Important boundaries

- Current facets are equality/count/navigation structures backed by Roaring
  bitmaps. They are not a native numeric/date range or arbitrary sort engine.
- Ontology metadata can describe value types, but the serving facet postings
  are still keyed by string facet values.
- Graph metadata is not the facet store. Graph node/edge filters and facet
  filters must be projected or joined deliberately.
- `ghostcrab_search`-style surfaces should remain facets/search focused;
  graph and projection retrieval belong on their own surfaces.
- Live search may embed the query, but it does not backfill missing document or
  chunk embeddings.

## Primary source files

| Source | Role |
| --- | --- |
| [`sql/sqlite_mindbrain--1.0.0.sql`](../../sql/sqlite_mindbrain--1.0.0.sql) | Canonical SQLite schema snapshot. |
| [`src/standalone/sqlite_schema.zig`](../../src/standalone/sqlite_schema.zig) | Runtime schema bootstrap. |
| [`src/standalone/import_pipeline.zig`](../../src/standalone/import_pipeline.zig) | Raw ingestion and facet/BM25 reindex orchestration. |
| [`src/standalone/collections_sqlite.zig`](../../src/standalone/collections_sqlite.zig) | Raw collection and facet-assignment helpers. |
| [`src/standalone/facet_sqlite.zig`](../../src/standalone/facet_sqlite.zig) | SQLite facet repository, bitmap operations, and parity helpers. |
| [`src/standalone/facet_store.zig`](../../src/standalone/facet_store.zig) | Fixture-only in-memory facet repository. |
| [`src/standalone/search_sqlite.zig`](../../src/standalone/search_sqlite.zig) | Search documents, FTS5, BM25 artifacts, embeddings. |
| [`src/standalone/search_store.zig`](../../src/standalone/search_store.zig) | In-memory search/BM25/vector repository implementation. |
| [`src/standalone/hybrid_search.zig`](../../src/standalone/hybrid_search.zig) | BM25/vector fusion and rerank orchestration. |
