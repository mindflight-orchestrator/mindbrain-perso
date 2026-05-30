# Raw graph layer

The raw layer is the durable source of truth for graph-related imports. It is
owned by the collections model and is described at a higher level in
[`docs/collections.md`](../collections.md).

The rule is simple:

1. Import or update raw rows.
2. Reindex or project into derived serving tables.
3. Query the derived graph.

## Raw hierarchy

```text
workspace
  collection
    documents_raw
      chunks_raw
      facet_assignments_raw
    document_links_raw
    external_links_raw
  ontology
    ontology_entity_types
    ontology_edge_types
    ontology_triples_raw
    ontology_entities_raw
    ontology_relations_raw
  instance graph
    entities_raw
    entity_aliases_raw
    relations_raw
    relation_properties_raw
    entity_documents_raw
    entity_chunks_raw
```

## Raw tables

| Table | Purpose | Important keys |
| --- | --- | --- |
| `documents_raw` | Durable document text and metadata. | `(workspace_id, collection_id, doc_id)`, public `doc_nanoid`. |
| `chunks_raw` | Durable chunk text and chunking metadata. | `(workspace_id, collection_id, doc_id, chunk_index)`. |
| `documents_raw_vector` / `chunks_raw_vector` | Stored embeddings ready for vector index rebuilds. | Same parent key plus `dim`. |
| `facet_assignments_raw` | Raw facet choices for documents or chunks. | Workspace, collection, target kind, doc/chunk, ontology namespace/dimension/value. |
| `entities_raw` | Durable graph entity facts. | `entity_id`, `UNIQUE(workspace_id, external_id)`, `UNIQUE(workspace_id, entity_type, name)`. |
| `entity_aliases_raw` | Entity surface forms. | `(workspace_id, entity_id, term)`. |
| `relations_raw` | Durable directed edges between raw entities. | `relation_id`, `UNIQUE(workspace_id, external_id)`. |
| `relation_properties_raw` | Durable qualified edge attributes. | `(workspace_id, relation_id, property_key)`. |
| `entity_documents_raw` | Entity-to-document grounding. | `(workspace_id, entity_id, collection_id, doc_id)`. |
| `entity_chunks_raw` | Entity-to-chunk grounding. | `(workspace_id, entity_id, collection_id, doc_id, chunk_index)`. |
| `document_links_raw` | Internal doc/chunk links across collections. | `(workspace_id, link_id)`. |
| `external_links_raw` | Outbound links from raw docs/chunks to external URIs. | `(workspace_id, link_id)`. |

## Raw to derived mapping

| Raw table | Derived graph table |
| --- | --- |
| `entities_raw` | `graph_entity` |
| `entity_aliases_raw` | `graph_entity_alias` |
| `relations_raw` | `graph_relation` |
| `relation_properties_raw` | `graph_relation_property` |
| `entity_documents_raw` | `graph_entity_document` when a document table id is supplied |
| `entity_chunks_raw` | `graph_entity_chunk` |

`document_links_raw` and `external_links_raw` are raw collection links. They are
part of the durable bundle and may inform graph/domain workflows, but they are
not the same as entity-to-entity graph edges.

## Identity

Use `external_id` on `entities_raw` and `relations_raw` for idempotent replay
from external systems or backup bundles. Internal integer ids are still used for
joins and serving tables.

`documents_raw.doc_nanoid` is the public document id. Internal joins use
`doc_id`; URLs and external references should use the nanoid form.

## Evidence

Graph facts can be grounded in three complementary ways:

- `entity_documents_raw` links an entity to a source document.
- `entity_chunks_raw` links an entity to the precise chunk where it appears.
- `relation_properties_raw.ref_doc_id` can attach document evidence to an edge
  property, which is what diagnostics uses for relation evidence checks.

## OWL2 and LinkML

OWL2 imports should preserve source triples in `ontology_triples_raw` first.
Classes and properties project into ontology tables. Individuals and object
property assertions become `entities_raw` and `relations_raw` only when graph
materialization is requested.

LinkML interchange uses the native ontology tables plus `ontology_triples_raw`
as the preservation pivot. Instance data still belongs in `entities_raw`,
`relations_raw`, and related raw evidence tables.
