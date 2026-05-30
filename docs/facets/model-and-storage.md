# Facet model and storage

MindBrain facets are structured filters and counts over document ids. They are
stored as Roaring bitmap postings grouped by table, facet, value, and chunk.

Facets are separate from:

- graph entities and relations;
- full-text search rows;
- vector embeddings;
- projection packs.

They can be combined with those systems, but they are not the same storage
family.

## Derived facet tables

| Table | Purpose |
| --- | --- |
| `facet_tables` | Logical faceted table registry: `table_id`, schema/table name, and `chunk_bits`. |
| `facet_definitions` | Facet registry per table. Each facet has a stable `facet_id` and `facet_name`. |
| `facet_postings` | Merged bitmap postings for `(table_id, facet_id, facet_value, chunk_id)`. |
| `facet_deltas` | Pending changes before merge into postings. |
| `facet_value_nodes` | Optional hierarchy nodes and child bitmaps for tree-style navigation. |

`facet_postings.posting_blob` stores a Roaring bitmap of document ids inside a
chunk. The full document id is reconstructed from `chunk_id` and the local id
inside the bitmap.

`chunk_bits` controls the chunk size. `facet_sqlite.optimalChunkBits` currently
uses:

| Approximate document count | `chunk_bits` |
| --- | --- |
| `<= 1,024` | 10 |
| `<= 65,536` | 14 |
| `<= 1,048,576` | 16 |
| larger | 18 |

## Legacy facts table

The `facets` table is a separate fact-store compatibility table:

| Column family | Role |
| --- | --- |
| `id`, `schema_id`, `content` | Logical fact identity and text. |
| `facets`, `facets_json` | JSON facet payload compatibility columns. |
| `embedding_blob`, `embedding` | Legacy or compatibility embedding fields. |
| `workspace_id`, `source_ref`, `doc_id` | Workspace and search/facet linkage. |

This table is not the same as the derived bitmap index. It can feed search or
application workflows, but the bitmap serving model is the `facet_*` table
family.

## Search tables

Search has its own tables:

| Table | Purpose |
| --- | --- |
| `search_documents` | Searchable text keyed by `(table_id, doc_id)`. |
| `search_fts_docs` | Mapping from `(table_id, doc_id)` to FTS rowid. |
| `search_fts` | SQLite FTS5 virtual table for lexical search. |
| `search_embeddings` | Packed little-endian `f32` vectors keyed by `(table_id, doc_id)`. |
| `search_document_stats` | Document length and unique term counts. |
| `search_collection_stats` | Collection-level BM25 stats. |
| `search_term_stats` | Document frequency by term hash. |
| `search_term_frequencies` | Term frequency by document. |
| `search_postings` | Compact BM25 term posting bitmaps. |
| `bm25_sync_triggers` | Table-level BM25 artifact sync settings. |
| `bm25_stopwords` | Seeded stopword vocabulary by language/source. |

FTS5 BM25 and compact BM25 are derived search indexes. They are not the raw
document store.

## Type boundary

The derived facet index stores `facet_value` as text. This supports fast exact
filtering and counts, but it does not make facets a general typed range engine.

Use current facets for:

- equality filters;
- multi-value OR within a facet;
- AND across facets;
- navigation/counts;
- hierarchy children where value nodes exist.

Do not claim current facet postings support native numeric/date range filters
or arbitrary typed sorting. Those require a separate typed storage/query
contract.

## Runtime repositories

`facet_sqlite.Repository` is the production SQLite-backed repository. It uses
SQLite indexes and prepared statements.

`facet_store.Store` is intentionally fixture-only. Its repository methods scan
in-memory arrays and should not be evolved into a production runtime store
without replacing those scans with keyed maps.
