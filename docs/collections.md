# Collections, Ontologies and the Raw Layer

> **Public document IDs (`doc_nanoid`)**
> Every row in `documents_raw` carries a public, URL-safe `doc_nanoid`
> in addition to its internal integer `doc_id`. The integer is what every
> internal join uses; the nanoid is the **only** identifier that is ever
> exposed in URLs (chunks are addressed externally as
> `<doc_nanoid>#<chunk_index>`). See [`docs/chunking.md`](chunking.md) for
> the chunking pipeline that assigns these ids and emits the
> auto-extracted `source.*` facets.

This document describes the **document manager** that sits underneath
the facet, BM25, vector and graph indexes. It is the single ingestion
surface used by both ad-hoc imports (YAGO, IMDb, custom corpora) and the
live application code, and it is the source of truth from which every
derived index can be rebuilt.

## Why a raw layer?

The previous design conflated *source data* (documents, chunks, facet
choices, entities, relations) with the *indexes* built on top of them
(facet postings, BM25 tables, graph adjacency). That made backup,
restoration and re-indexation awkward: you had to re-run the importer to
recover state.

The collections layer separates the two:

- `*_raw` tables are the **source of truth**. They never change shape
  for index reasons; they are the part you back up.
- Facet, BM25, vector and graph tables are **derived**. They can be
  dropped and rebuilt at any time from the raw layer through
  `Pipeline.reindexAll(...)`.

## The hierarchy

```
Workspace (tenant)
├── Collection (group of related documents, e.g. "yago::core_facts")
│   ├── Document
│   │   └── Chunk
│   └── Facet assignments (doc and chunk level)
├── Collection (another, e.g. "imdb::titles")
│   └── ...
├── Ontology (workspace-scoped)
│   ├── Namespaces / Dimensions / Values
│   ├── Entity types / Edge types
│   └── Seed entities and relations
└── Cross-collection links (entity-mediated or direct doc-to-doc)
```

A workspace can hold multiple collections. Ontologies are
workspace-scoped and can be attached to any collection in the workspace
via `collection_ontologies`. If a collection has no ontology attached,
the workspace `default_ontology_id` is used.

## SQLite tables

All tables live in the standalone schema (`src/standalone/sqlite_schema.zig`):

| Table | Purpose |
|-------|---------|
| `workspaces` | Tenant container. |
| `collections` | A logical bucket of documents inside a workspace. |
| `ontologies` | Workspace-scoped graph of namespaces / dimensions / entity & edge types. |
| `collection_ontologies` | Many-to-many: which ontologies apply to which collection. |
| `workspace_settings` | Default ontology + tenant-wide knobs. |
| `ontology_namespaces` / `ontology_dimensions` / `ontology_values` | Hierarchical facet vocabulary. |
| `ontology_entity_types` / `ontology_edge_types` | Graph schema. |
| `ontology_entities_raw` / `ontology_relations_raw` | Seed entities & relations shipped with the ontology. |
| `documents_raw` / `chunks_raw` | Document and chunk source-of-truth content. |
| `documents_raw_vector` / `chunks_raw_vector` | Stored embeddings, ready to feed back into the vector index. |
| `facet_assignments_raw` | Per-(doc \| chunk) facet picks. |
| `entities_raw` / `entity_aliases_raw` / `relations_raw` | Live (non-seed) graph entities. |
| `entity_documents_raw` / `entity_chunks_raw` | Bind entities to docs/chunks for grounding. |
| `document_links_raw` | Cross-collection document/chunk relationships (e.g. legal ↔ technical). |

The PostgreSQL mirror lives in the extension install script,
[`sql/sqlite_mindbrain--1.0.0.sql`](../sql/sqlite_mindbrain--1.0.0.sql), under the
`mb_collections` schema with the same column shape so that any tool written
against either backend stays portable.

## Public API

### Raw helpers — `collections_sqlite.zig`

`collections_sqlite` exposes idempotent `ensure*`, `upsert*`, `link*` and
`loadOntologyBundle` helpers for every raw table. They are used both by
the high-level `Pipeline` and directly by importers that need bulk-load
performance.

### Pipeline — `import_pipeline.zig`

`Pipeline` is the single entry point that keeps raw and derived layers
in sync. Bind it to a workspace + collection once, then ingest:

```zig
var pipeline = import_pipeline.Pipeline{
    .allocator = allocator,
    .db = &db,
    .search = &search_store,
    .facets = &facet_store,
    .graph = &graph_store,
    .workspace_id = "ws",
    .collection_id = "ws::docs",
};

try pipeline.createWorkspace(.{ .workspace_id = "ws" });
try pipeline.createCollection(.{ .workspace_id = "ws", .collection_id = "ws::docs", .name = "docs" });
try pipeline.registerOntology(.{ .ontology_id = "ws::core", .workspace_id = "ws", .name = "core" });
try pipeline.attachOntologyToCollection("ws", "ws::docs", "ws::core", "primary");

// Raw-first ingest (raw tables + derived indexes in one call).
try pipeline.ingestDocumentRaw(.{ .workspace_id = "ws", .collection_id = "ws::docs", .doc_id = 1, .content = "..." });
try pipeline.ingestChunkRaw(.{ .workspace_id = "ws", .collection_id = "ws::docs", .doc_id = 1, .chunk_index = 0, .content = "..." });
try pipeline.assignFacetRaw(.{ .workspace_id = "ws", .collection_id = "ws::docs", .target_kind = .doc, .doc_id = 1, .ontology_id = "ws::core", .namespace = "topic", .dimension = "category", .value = "graph" });
try pipeline.upsertEntityFull(.{ .workspace_id = "ws", .ontology_id = "ws::core", .entity_id = 1, .entity_type = "person", .name = "Ada" });
try pipeline.addRelationFull(.{ .workspace_id = "ws", .ontology_id = "ws::core", .relation_id = 10, .edge_type = "works_for", .source_entity_id = 1, .target_entity_id = 2 });
try pipeline.linkDocuments(.{ .workspace_id = "ws", .link_id = 1, .ontology_id = "ws::core", .edge_type = "explains", .source_collection_id = "ws::brief", .source_doc_id = 7, .target_collection_id = "ws::docs", .target_doc_id = 1 });
```

### Reindex from raw

Drop or corrupt your derived stores at any time, then ask the pipeline
to rebuild them from the raw layer:

```zig
try pipeline.reindexAll("ws", "ws::docs", facet_table_id);
// or selectively:
_ = try pipeline.reindexBm25("ws", "ws::docs", facet_table_id);
_ = try pipeline.reindexFacets("ws", "ws::docs", facet_table_id);
_ = try pipeline.reindexGraph("ws");
```

### Backup & restore — `collections_io.zig`

```zig
const bundle = try collections_io.exportToJson(allocator, db, .{ .workspace = "ws" });
defer allocator.free(bundle);
// ...write `bundle` to disk or storage...

try collections_io.importBundleJson(target_db, allocator, bundle);
```

The exporter accepts either a workspace-wide scope or a single
collection scope:

```zig
.{ .collection = .{ .workspace_id = "ws", .collection_id = "ws::legal" } }
```

A collection-scoped export keeps cross-collection links whose **source
or target** lives inside the requested collection, so a legal export
still carries the `legal → technical` mappings it needs.

## CLI

`mindbrain-standalone-tool` implements workspace and collection management
against a SQLite file. Run the binary with **no arguments** to print the full
usage list to stderr, or see [standalone.md](standalone.md).

Typical collection verbs (all require `--db <sqlite_path>` where applicable):

```
workspace-create        --workspace-id <id> [--label ...] [--description ...] [--profile ...]
collection-create        --workspace-id <id> --collection-id <id> --name <n> [--chunk-bits N] [--language ...]
ontology-register        --workspace-id <id> --ontology-id <id> --name <n> ...
ontology-attach          --workspace-id <id> --collection-id <id> --ontology-id <id> [--role ...]
collection-export        --workspace-id <id> [--collection-id <id>] [--output <file>]
collection-import        --bundle <file>
```

For LLM document profiling and optional `documents_raw` / `chunks_raw` writes
from the queue worker, see [document-profile.md](document-profile.md).

## Cross-collection mapping

There are two recognized patterns:

1. **Entity-mediated.** A workspace-scoped entity (`entities_raw`) is
   grounded in documents from multiple collections through
   `entity_documents_raw` / `entity_chunks_raw`. This is the right
   choice when the same concept appears in different corpora and you
   want to query it across both.
2. **Direct doc-to-doc.** `document_links_raw` records a typed,
   weighted edge between two documents (or chunks) regardless of the
   collection they belong to. Use this when a specific *clause*
   implements a specific *spec*, regardless of any shared entity.

Both patterns are covered by the round-trip tests in
`collections_io.zig` and by the import pipeline test in
`import_pipeline.zig`.

## Importer integration

Both reference importers ([YAGO](./yago-import-status.md) and
[IMDb](./imdb_import_benchmark_status.md)) now sit on this layer:

- They call `ensureYagoScaffold` / `ensureImdbScaffold` to create their
  workspace, default collection and core ontology bundle.
- They mirror their staged graph data into `entities_raw`,
  `entity_aliases_raw` and `relations_raw` so a fresh database can
  rebuild the entire graph index from the raw rows alone.
- They accept `--workspace-id`, `--collection-id` and `--ontology-id`
  overrides on the CLI for multi-tenant runs.

Use this same template (`ensure*Scaffold` + `mirror*RawTables`) when
adding a new importer.
