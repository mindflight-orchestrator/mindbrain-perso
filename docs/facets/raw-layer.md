# Raw facet layer

Raw facet assignments live in `facet_assignments_raw`. They are the durable
source of truth for ontology-backed facet decisions.

The derived bitmap index can be rebuilt from these rows through
`Pipeline.reindexFacets`.

## Raw facet assignments

`facet_assignments_raw` columns:

| Column | Meaning |
| --- | --- |
| `workspace_id` | Tenant/workspace scope. |
| `collection_id` | Document collection. |
| `target_kind` | `doc` or `chunk`. |
| `doc_id` | Internal raw document id. |
| `chunk_index` | Chunk index, or `-1` for document-level assignments. |
| `ontology_id` | Ontology that owns the vocabulary. |
| `namespace` | Facet namespace, such as `source`, `domain`, or `finance`. |
| `dimension` | Facet dimension, such as `document_type` or `role`. |
| `value` | Assigned value. |
| `value_id` | Optional ontology value id. |
| `weight` | Assignment weight. |
| `source` | Assignment provenance label. |
| `created_at` | Insert timestamp. |

The primary key prevents duplicate assignment of the same value to the same
target in the same ontology/dimension.

## Facet names

The derived facet name is:

```text
<namespace>.<dimension>
```

Examples:

- `source.document_type`
- `source.path`
- `source.dir`
- `domain.role`
- `finance.payment_status`

`Pipeline.reindexFacets` uses this derived name to create or resolve
`facet_definitions.facet_name`.

## Source facets

Chunked document ingestion derives built-in `source.*` facets from file and
chunk metadata. The default source namespace includes:

- `source.path`
- `source.dir`
- `source.filename`
- `source.extension`
- `source.ingested_at`
- `source.chunk_index`
- `source.chunk_count`
- `source.strategy`

These are emitted by `chunker.deriveSourceFacets` when documents are ingested
through `Pipeline.ingestDocumentChunked`.

## Document vs chunk assignments

`target_kind = 'doc'` assignments are projected by `Pipeline.reindexFacets`
into the current bitmap index.

`target_kind = 'chunk'` assignments are preserved in raw form and used by
chunk-aware flows. Keep the raw rows even when the current serving facet table
is document-scoped.

## Ontology vocabulary

Facet assignments should align with ontology vocabulary tables:

- `ontologies`
- `ontology_namespaces`
- `ontology_dimensions`
- `ontology_values`

The ontology tables can describe labels, hierarchy, value type metadata, and
allowed values. The serving bitmap index still filters by the text value stored
in `facet_assignments_raw.value`.

## LLM qualification boundary

`document-profile` produces structured document profiles and can drive
contextual retrieval. It does not by itself assign ontology-backed facets.

`document-ingest --ontology-id` writes raw facet assignments through the import
path, currently using deterministic metadata/source/chunk derivation unless a
specific qualification flow supplies reviewed assignments.

Future LLM-assisted facet qualification should write reviewed assignments into
`facet_assignments_raw`; it should not bypass the raw layer.
