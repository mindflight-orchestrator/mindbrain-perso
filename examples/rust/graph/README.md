# pg_dgraph — Rust Client

Rust client library for the `pg_dgraph` PostgreSQL extension (version 0.3.1 compatible).

## Overview

Provides a thin, idiomatic wrapper around the graph.* SQL functions so that callers don't need to write raw SQL for common operations.

## Dependencies

Add to your `Cargo.toml`:

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
tokio-postgres = { version = "0.7", features = ["with-serde_json-1", "with-chrono-0_4"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
chrono = { version = "0.4", features = ["serde"] }
```

Or use this crate as a path dependency.

## Quick Start

```rust
use pgdgraph::*;
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (client, connection) = tokio_postgres::connect(
        "postgres://user:pass@localhost:5432/db?sslmode=disable",
        NoTls,
    ).await?;
    tokio::spawn(async move { let _ = connection.await; });

    let dgraph = DgraphClient::new(client);
    dgraph.ensure_extension().await?;

    let id = dgraph.upsert_entity("skill", "my-skill", 0.9, None).await?;
    println!("Created entity id={}", id);

    let entity = dgraph.get_entity(id).await?.expect("entity exists");
    println!("Entity: {} ({})", entity.name, entity.entity_type);

    Ok(())
}
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

### Analytics
- `skill_dependencies` — Transitive dependency tree
- `entity_neighborhood` — One-hop neighborhood JSON
- `confidence_decay` — Time-decayed confidence

### Maintenance
- `rebuild_lj_relations` — Rebuild bitmap indexes
- `ensure_extension` — Create extensions if not present

## Build

If `cargo build` fails with "unknown proxy name: 'cursor'", use the toolchain cargo directly:

```bash
$HOME/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin/cargo build
```

Or set `CARGO` to that path. The `make test` and `run_tests.sh` scripts automatically use the toolchain cargo when available.

## Testing

### Recommended: Use the test runner

```bash
cd examples/rust
make test   # Builds Docker, starts Postgres, runs tests, tears down
```

### Alternative: Manual setup

```bash
# Start PostgreSQL with pg_dgraph
make start-db

# Run tests
TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5436/postgres?sslmode=disable" \
PG_DGRAPH_TEST_FAIL_ON_NO_DB=true \
cargo test

# Clean up
make clean
```

### Environment variables

| Variable | Description |
|----------|-------------|
| `TEST_DATABASE_URL` | Override database connection string |
| `PG_DGRAPH_TEST_FAIL_ON_NO_DB=true` | Fail instead of skip when no database (CI mode) |
