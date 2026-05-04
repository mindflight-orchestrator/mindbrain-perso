# pg_dgraph — Python Client

Python client library for the `pg_dgraph` PostgreSQL extension (version 0.3.1 compatible).

## Overview

Provides a thin wrapper around the graph.* SQL functions so that callers don't need to write raw SQL for common operations.

## Installation

```bash
pip install psycopg[binary]
```

## Quick Start

```python
import psycopg
from pgdgraph import DgraphClient

conn = psycopg.connect("postgres://user:pass@localhost:5432/db")
client = DgraphClient(conn)
client.ensure_extension()

id = client.upsert_entity("skill", "my-skill", 0.9, None)
print(f"Created entity id={id}")

entity = client.get_entity(id)
print(f"Entity: {entity.name} ({entity.type})")

conn.close()
```

## API Reference

### Entity management
- `upsert_entity` — Create or merge entity
- `get_entity` — Get entity by ID
- `find_entities_by_type` — List active entities by type
- `deprecate_entity` — Mark entity as deprecated

### Relation management
- `upsert_relation` — Create/update directed relation
- `get_relations_from` — Get outgoing relations

### Alias management
- `register_aliases` — Map terms to canonical entity
- `resolve_terms` — Resolve text terms to entity IDs

### Search
- `entity_fts_search` — Full-text search over entities
- `marketplace_search` — Hybrid FTS + graph search

### Graph traversal
- `k_hops_filtered` — Find entities within k hops
- `shortest_path_filtered` — Shortest path between two entities

### Learning pipeline
- `learn_from_run` — Record agent run and upsert concepts/relations

## Testing

```bash
cd examples/python
make test   # Builds Docker, starts Postgres, runs tests, tears down
```

Or manually:
```bash
make start-db
TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5436/postgres?sslmode=disable" \
PG_DGRAPH_TEST_FAIL_ON_NO_DB=true \
pytest -v tests/
```
