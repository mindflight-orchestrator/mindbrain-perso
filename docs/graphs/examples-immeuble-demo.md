# Immeuble demo graph snapshot

This page records a read-only snapshot of `data/immeuble-demo.sqlite` as of the
current checkout. Regenerate it with the queries below before using it as a
release assertion.

Workspace: `immeuble-demo`

Ontology: `immeuble-demo::core`

## Raw and derived counts

```text
entities_raw             131
relations_raw            265
relation_properties_raw   62
entity_documents_raw      72
entity_chunks_raw         72
documents_raw              7
chunks_raw                 7
graph_entity             131
graph_relation           265
graph_relation_property   62
```

Regeneration query:

```bash
sqlite3 -readonly -header -column data/immeuble-demo.sqlite "
SELECT 'entities_raw' AS table_name, COUNT(*) AS rows FROM entities_raw
UNION ALL SELECT 'relations_raw', COUNT(*) FROM relations_raw
UNION ALL SELECT 'relation_properties_raw', COUNT(*) FROM relation_properties_raw
UNION ALL SELECT 'entity_documents_raw', COUNT(*) FROM entity_documents_raw
UNION ALL SELECT 'entity_chunks_raw', COUNT(*) FROM entity_chunks_raw
UNION ALL SELECT 'documents_raw', COUNT(*) FROM documents_raw
UNION ALL SELECT 'chunks_raw', COUNT(*) FROM chunks_raw
UNION ALL SELECT 'graph_entity', COUNT(*) FROM graph_entity WHERE workspace_id='immeuble-demo'
UNION ALL SELECT 'graph_relation', COUNT(*) FROM graph_relation WHERE workspace_id='immeuble-demo'
UNION ALL SELECT 'graph_relation_property', COUNT(*) FROM graph_relation_property;
"
```

## Top entity types

```text
person            30
billing_group     13
cellar            13
household         13
unit              13
parking_space      7
private_garden     6
shared_space       6
lease_contract     5
organization       5
```

Query:

```sql
SELECT entity_type, COUNT(*) AS count
FROM graph_entity
WHERE workspace_id = 'immeuble-demo'
GROUP BY entity_type
ORDER BY count DESC, entity_type
LIMIT 20;
```

## Top relation types

```text
contains              69
household_member      27
occupies              27
uses_common           26
has_member            17
owns                  17
assigned_cellar       13
bills_to              13
primary_residence_of  13
rented_to             10
```

Query:

```sql
SELECT edge_type, COUNT(*) AS count
FROM relations_raw
WHERE workspace_id = 'immeuble-demo'
GROUP BY edge_type
ORDER BY count DESC, edge_type
LIMIT 20;
```

## Relation properties

```text
quote_part      number       17
right_type      text         17
status          text         17
monthly_rent    money_minor   5
amount          money_minor   3
payment_status  text          3
```

Query:

```sql
SELECT property_key, value_type, COUNT(*) AS count
FROM relation_properties_raw
WHERE workspace_id = 'immeuble-demo'
GROUP BY property_key, value_type
ORDER BY count DESC, property_key
LIMIT 20;
```

## Sample raw entities

```sql
SELECT entity_id, external_id, entity_type, name
FROM entities_raw
WHERE workspace_id = 'immeuble-demo'
ORDER BY entity_id
LIMIT 10;
```

Use this to confirm that raw ids, external ids, entity types, and display names
are populated before checking derived graph rows.

## Useful demo GPQ queries

Units in building 1:

```text
WORKSPACE immeuble-demo
MATCH (u:unit)
WHERE u.metadata.building_id = 1
PROJECT u.entity_id, u.name, u.metadata.lot, u.metadata.usage_status
LIMIT 20
```

Ownership with shares:

```text
WORKSPACE immeuble-demo
MATCH (p:person)-[o:owns]->(u:unit)
WHERE o.prop.quote_part >= 0.5
PROJECT p.name, u.name, o.relation_id, o.prop.quote_part
LIMIT 20
```

Ownership with right type:

```text
WORKSPACE immeuble-demo
MATCH (p:person)-[o:owns]->(u:unit)
WHERE o.prop.right_type = 'pleine_propriete'
PROJECT p.name, u.name, o.prop.right_type
LIMIT 20
```

Rent relations:

```text
WORKSPACE immeuble-demo
MATCH (org:organization)-[r:rented_to]->(h:household)
WHERE r.prop.monthly_rent >= 100000
PROJECT org.name, h.name, r.prop.monthly_rent
LIMIT 20
```

## Useful HTTP checks

```bash
curl -fsS 'http://127.0.0.1:8092/api/mindbrain/graph/type-counts?workspace_id=immeuble-demo'

curl -fsS 'http://127.0.0.1:8092/api/mindbrain/graph/diagnostics?workspace_id=immeuble-demo&limit=200'

curl -fsS 'http://127.0.0.1:8092/api/mindbrain/graph/subgraph?workspace_id=immeuble-demo&seed_ids=1&hops=2&format=json'
```

These require a running `mindbrain-http` server pointed at the demo database.
