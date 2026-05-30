# Ingestion and reindex

Graph ingestion is raw-first. The main implementation is
[`src/standalone/import_pipeline.zig`](../../src/standalone/import_pipeline.zig).

## Recommended flow

1. Create or ensure the workspace.
2. Create or ensure collections and ontologies.
3. Write documents/chunks/facets/entities/relations into raw tables.
4. Reindex derived stores.
5. Query the serving graph.

The `Pipeline` type wires together the collection, facet, BM25, vector, and
graph stores so imports can keep raw and derived layers in sync.

## Write helpers

Important graph-related `Pipeline` methods:

| Method | Role |
| --- | --- |
| `createWorkspace` | Ensures the workspace container. |
| `createCollection` | Ensures a document collection. |
| `registerOntology` | Loads ontology metadata and vocabulary rows. |
| `attachOntologyToCollection` | Connects ontology and collection. |
| `ingestDocumentRaw` / `ingestChunkRaw` | Writes raw source text. |
| `assignFacetRaw` | Writes raw facet assignments. |
| `upsertEntityFull` | Writes an entity to `entities_raw` and derived graph state. |
| `upsertEntityAlias` | Writes alias rows. |
| `addRelationFull` | Writes a relation to `relations_raw` and derived graph state. |
| `addRelationProperty` / `addRelationPropertiesBatch` | Writes typed raw relation properties and projects them. |
| `linkEntityToDocument` / `linkEntityToChunk` | Writes raw evidence grounding and optional derived links. |
| `linkDocuments` / `linkExternal` | Writes internal and external raw document links. |

## Reindex graph

`Pipeline.reindexGraph(workspace_id)` calls
`reindexGraphWithDocumentTable(workspace_id, null)`.

`Pipeline.reindexGraphWithDocumentTable(workspace_id, document_table_id)`:

- replays `entities_raw` into `graph_entity`;
- replays `entity_aliases_raw` into `graph_entity_alias`;
- replays `relations_raw` into `graph_relation`;
- projects `relation_properties_raw` into `graph_relation_property`;
- optionally rebuilds `graph_entity_document` for the supplied document table;
- rebuilds `graph_entity_chunk` for the workspace.

The return value is the number of projected rows touched by the graph replay.

## Relation-property projection

`graph_sqlite.projectRelationProperties(db, workspace_id)` scans all
`relation_properties_raw` rows for the workspace and upserts the matching rows
into `graph_relation_property`.

`graph_sqlite.projectRelationPropertiesForIds(db, allocator, relation_ids)` is
the incremental path. It chunks ids at 500 and only projects properties for the
relations passed in.

Use the incremental path after a small set of relation property updates. Use the
workspace projection during full reindex.

## Full reindex

`Pipeline.reindexAll(workspace_id, collection_id, table_id)` runs:

1. BM25 reindex from `documents_raw` and `chunks_raw`.
2. Facet reindex from `facet_assignments_raw`.
3. Graph reindex from raw graph rows and evidence links.

This is the recovery path after dropping or corrupting derived stores.

## Adjacency and degree refresh

Derived relation rows are not the only traversal structure. The graph also uses
adjacency and degree tables:

- `graph_sqlite.rebuildLjRelations` rebuilds all adjacency bitmaps.
- `graph_sqlite.rebuildLjForEntities` refreshes adjacency for touched nodes.
- `graph_sqlite.refreshEntityDegree` rebuilds degree statistics.

Bulk imports can use full refreshes. Runtime patch paths should track touched
entity ids and use narrower refreshes where possible.

## Consistency boundaries

- A raw relation property write alone does not make the property visible in
  `graph_relation_property`; it must be projected.
- A raw entity or relation is the durable fact; a derived graph row is the
  queryable projection.
- If `entities_raw` or `relations_raw` change outside the `Pipeline`, run a
  graph reindex before relying on graph APIs.
- Keep workspace ids consistent across raw rows and derived graph rows.

## Verification snippets

Count raw and derived graph rows for one workspace:

```bash
sqlite3 -readonly data/immeuble-demo.sqlite "
SELECT 'entities_raw', COUNT(*) FROM entities_raw WHERE workspace_id='immeuble-demo'
UNION ALL SELECT 'relations_raw', COUNT(*) FROM relations_raw WHERE workspace_id='immeuble-demo'
UNION ALL SELECT 'graph_entity', COUNT(*) FROM graph_entity WHERE workspace_id='immeuble-demo'
UNION ALL SELECT 'graph_relation', COUNT(*) FROM graph_relation WHERE workspace_id='immeuble-demo';
"
```

Check raw relation properties that have not been projected:

```sql
SELECT rpr.workspace_id, rpr.relation_id, rpr.property_key
FROM relation_properties_raw rpr
LEFT JOIN graph_relation_property grp
  ON grp.relation_id = rpr.relation_id
 AND grp.property_key = rpr.property_key
WHERE rpr.workspace_id = 'immeuble-demo'
  AND grp.relation_id IS NULL;
```
