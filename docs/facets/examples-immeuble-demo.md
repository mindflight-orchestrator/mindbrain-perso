# Immeuble demo facets snapshot

This page records a read-only snapshot of `data/immeuble-demo.sqlite` as of the
current checkout. Regenerate it with the queries below before using it as a
release assertion.

Workspace: `immeuble-demo`

Collection facet table: `77001` / `public`.`immeuble-demo::docs`

## Raw, derived, and search counts

```text
facet_assignments_raw    22
facet_tables              1
facet_definitions         7
facet_postings           21
facet_deltas              0
facet_value_nodes         0
facets                    0
search_documents          7
search_fts_docs           7
search_embeddings         0
bm25_stopwords         6232
```

Regeneration query:

```bash
sqlite3 -readonly -header -column data/immeuble-demo.sqlite "
SELECT 'facet_assignments_raw' AS table_name, COUNT(*) AS rows FROM facet_assignments_raw
UNION ALL SELECT 'facet_tables', COUNT(*) FROM facet_tables
UNION ALL SELECT 'facet_definitions', COUNT(*) FROM facet_definitions
UNION ALL SELECT 'facet_postings', COUNT(*) FROM facet_postings
UNION ALL SELECT 'facet_deltas', COUNT(*) FROM facet_deltas
UNION ALL SELECT 'facet_value_nodes', COUNT(*) FROM facet_value_nodes
UNION ALL SELECT 'facets', COUNT(*) FROM facets WHERE workspace_id='immeuble-demo'
UNION ALL SELECT 'search_documents', COUNT(*) FROM search_documents
UNION ALL SELECT 'search_fts_docs', COUNT(*) FROM search_fts_docs
UNION ALL SELECT 'search_embeddings', COUNT(*) FROM search_embeddings
UNION ALL SELECT 'bm25_stopwords', COUNT(*) FROM bm25_stopwords;
"
```

## Raw facet dimensions

```text
source.document_type      7 assignments, 7 values
domain.role               4 assignments, 4 values
domain.building           3 assignments, 2 values
domain.scenario           3 assignments, 3 values
finance.payment_status    3 assignments, 3 values
domain.decision           1 assignment, 1 value
domain.unit               1 assignment, 1 value
```

Query:

```sql
SELECT namespace || '.' || dimension AS facet_name,
       COUNT(*) AS assignments,
       COUNT(DISTINCT value) AS values_count
FROM facet_assignments_raw
WHERE workspace_id = 'immeuble-demo'
GROUP BY namespace, dimension
ORDER BY assignments DESC, facet_name
LIMIT 20;
```

## Target kind

All current demo assignments are document-level:

```text
doc  22
```

Query:

```sql
SELECT target_kind, COUNT(*) AS assignments
FROM facet_assignments_raw
WHERE workspace_id = 'immeuble-demo'
GROUP BY target_kind
ORDER BY target_kind;
```

## Facet table

```text
table_id  schema_name  table_name           chunk_bits  definitions
77001     public       immeuble-demo::docs  8           7
```

Query:

```sql
SELECT ft.table_id,
       ft.schema_name,
       ft.table_name,
       ft.chunk_bits,
       COUNT(fd.facet_id) AS definitions
FROM facet_tables ft
LEFT JOIN facet_definitions fd ON fd.table_id = ft.table_id
GROUP BY ft.table_id, ft.schema_name, ft.table_name, ft.chunk_bits
ORDER BY ft.table_id;
```

## Derived posting values

Current derived postings include:

```text
domain.building         Résidence Les Tilleuls
domain.building         Résidence Les Érables
domain.role             locataire
domain.role             occupant
domain.role             titulaire
domain.role             usage_exclusif
domain.scenario         annexes
domain.scenario         occupants
domain.scenario         structure_copropriete
finance.payment_status  complete
finance.payment_status  manual_review
finance.payment_status  partial
source.document_type    annexe_lot
source.document_type    bail
source.document_type    composition_menage
source.document_type    extrait_coda
source.document_type    pv_ag
source.document_type    reglement_copropriete
source.document_type    titre_propriete
```

Regeneration query:

```sql
SELECT fd.facet_name, fp.facet_value, COUNT(*) AS chunks
FROM facet_postings fp
JOIN facet_definitions fd
  ON fd.table_id = fp.table_id
 AND fd.facet_id = fp.facet_id
GROUP BY fd.facet_name, fp.facet_value
ORDER BY fd.facet_name, fp.facet_value
LIMIT 30;
```

## Useful HTTP checks

```bash
curl -fsS 'http://127.0.0.1:8092/api/mindbrain/collections/facet-search?workspace_id=immeuble-demo&collection_id=immeuble-demo::docs&table_id=77001&limit=20'

curl -fsS 'http://127.0.0.1:8092/api/mindbrain/collections/facet-search?workspace_id=immeuble-demo&collection_id=immeuble-demo::docs&namespace=source&dimension=document_type&value=bail'

curl -fsS 'http://127.0.0.1:8092/api/mindbrain/search-compact-info'
```

These require a running `mindbrain-http` server pointed at the demo database.
