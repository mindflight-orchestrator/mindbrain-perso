# pg_dgraph Go Client

Go client library for the `pg_dgraph` PostgreSQL extension (version 0.3.1+).

## Overview

`DgraphClient` is a thin, idiomatic wrapper over the `graph.*` SQL functions provided by `pg_dgraph`. It lets Go code interact with the knowledge graph without writing raw SQL.

## Installation

```bash
go get github.com/jackc/pgx/v5
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    pgdgraph "pgdgraph"   // or your module path
    "github.com/jackc/pgx/v5/pgxpool"
)

func main() {
    ctx := context.Background()

    pool, err := pgxpool.New(ctx, "postgres://user:pass@localhost:5432/db")
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()

    client, err := pgdgraph.NewDgraphClient(pool, false)
    if err != nil {
        log.Fatal(err)
    }

    // Ensure extensions are installed
    if err := client.EnsureExtension(ctx); err != nil {
        log.Fatal(err)
    }

    // Create two entities
    idA, _ := client.UpsertEntity(ctx, "skill", "sprint-planning", 0.9, map[string]any{
        "domain": "webpm", "description": "Plan a sprint iteration",
    })
    idB, _ := client.UpsertEntity(ctx, "concept", "backlog", 0.85, map[string]any{
        "domain": "webpm",
    })

    // Link them
    _, _ = client.UpsertRelation(ctx, "requires", idA, idB, 0.9)

    // Rebuild bitmap indexes (call after bulk inserts)
    _ = client.RebuildLjRelations(ctx)

    // Search
    results, _ := client.MarketplaceSearch(ctx, "sprint planning", "webpm", 0.5, 2, 10)
    for _, r := range results {
        fmt.Printf("  %s (%s) score=%.4f\n", r.Name, r.Type, r.CompositeScore)
    }

    // Traverse: find all entities reachable within 2 hops from sprint-planning
    visited, _ := client.KHopsFiltered(ctx, []int64{idA}, 2, nil)
    fmt.Printf("Reachable from sprint-planning: %v\n", visited)
}
```

## API Reference

### Constructor

```go
func NewDgraphClient(pool *pgxpool.Pool, debug bool) (*DgraphClient, error)
```

### Entity Management

| Method | Description |
|--------|-------------|
| `UpsertEntity(ctx, type, name, confidence, metadata)` | Idempotent entity create/merge — raises confidence to max, deep-merges metadata |
| `GetEntity(ctx, id)` | Fetch a single entity by ID |
| `FindEntitiesByType(ctx, type)` | All active entities of a given type, ordered by confidence |
| `DeprecateEntity(ctx, id)` | Soft-delete an entity |

### Relation Management

| Method | Description |
|--------|-------------|
| `UpsertRelation(ctx, relType, sourceID, targetID, confidence)` | Idempotent directed relation create/merge |
| `GetRelationsFrom(ctx, sourceID)` | Active outgoing relations from an entity |

### Aliases & Resolution

| Method | Description |
|--------|-------------|
| `RegisterAliases(ctx, entityID, terms, confidence)` | Map surface forms to a canonical entity |
| `ResolveTerms(ctx, terms, minConfidence)` | Resolve text terms to entity IDs |

### Search

| Method | Description |
|--------|-------------|
| `EntityFtsSearch(ctx, query, typeFilter, domain, minConf, limit)` | Full-text search over entity name + metadata |
| `MarketplaceSearch(ctx, query, domain, minConf, maxHops, limit)` | Hybrid FTS + BFS + hub-degree scored search |

### Graph Traversal

| Method | Description |
|--------|-------------|
| `KHopsFiltered(ctx, seedIDs, maxHops, edgeTypes)` | BFS from seed nodes, optional edge-type filter |
| `KHopsFilteredFull(ctx, seedIDs, maxHops, edgeTypes, confMin, confMax)` | Full-parameter BFS with confidence bounds |
| `ShortestPathFiltered(ctx, srcID, destID, edgeTypes, maxDepth)` | Bidirectional shortest path; returns -1 if no path |

### Learning Pipeline

| Method | Description |
|--------|-------------|
| `LearnFromRun(ctx, runKey, domain, outcome, concepts, relations, transcript, meta)` | Batch-ingest knowledge from an agent run (idempotent) |

### Analytics & Maintenance

| Method | Description |
|--------|-------------|
| `SkillDependencies(ctx, entityID, maxDepth, minConf)` | Transitive dependency tree of an entity |
| `EntityNeighborhood(ctx, entityID, maxOut, maxIn, minConf)` | One-hop JSON summary (for LLM context injection) |
| `ConfidenceDecay(ctx, entityID, halfLifeDays)` | Time-decayed confidence score |
| `RebuildLjRelations(ctx)` | Full rebuild of bitmap adjacency indexes |
| `RebuildLjForEntities(ctx, entityIDs)` | Incremental bitmap rebuild for specific entities |
| `RefreshDegreeView(ctx)` | Refresh `graph.entity_degree` materialized view |
| `ExtensionVersion(ctx)` | Installed pg_dgraph version string |
| `EnsureExtension(ctx)` | CREATE EXTENSION IF NOT EXISTS for roaringbitmap + pg_dgraph |

## Data Types

### Entity

```go
type Entity struct {
    ID           int64
    Type         string
    Name         string
    Confidence   float32
    Metadata     map[string]any
    DeprecatedAt *time.Time
    CreatedAt    time.Time
}
```

### ConceptInput / RelationInput (for LearnFromRun)

```go
type ConceptInput struct {
    Type       string
    Name       string
    Confidence float32
    Metadata   map[string]any
}

type RelationInput struct {
    Source     string   // entity name
    Target     string   // entity name
    Type       string
    Confidence float32
}
```

### EntitySearchResult / MarketplaceResult

```go
type EntitySearchResult struct {
    EntityID   int64;  Name string;  Type string
    Confidence float32;  FtsRank float32
    Metadata   map[string]any
}

type MarketplaceResult struct {
    EntityID int64;  Name string;  Type string
    Confidence float32;  FtsRank float32
    IsDirectMatch bool;  HubScore float32;  CompositeScore float32
    Metadata map[string]any
}
```

## Examples

### Registering Aliases and Resolving Terms

```go
entityID, _ := client.UpsertEntity(ctx, "skill", "sprint-planning", 0.9, nil)

client.RegisterAliases(ctx, entityID,
    []string{"sprint planning", "iteration planning", "IP"},
    0.9,
)

// Text → entity IDs
ids, _ := client.ResolveTerms(ctx, []string{"sprint planning"}, 0.5)
fmt.Printf("Resolved: %v\n", ids)
```

### Learning from an Agent Run

```go
runID, err := client.LearnFromRun(ctx,
    "run-20260215-sprint-setup",   // unique run key (idempotent)
    "webpm",                       // domain
    "success",                     // outcome: success | partial | failure
    []pgdgraph.ConceptInput{
        {Type: "skill",   Name: "sprint-planning", Confidence: 0.9,
         Metadata: map[string]any{"domain": "webpm"}},
        {Type: "concept", Name: "backlog",          Confidence: 0.85},
    },
    []pgdgraph.RelationInput{
        {Source: "sprint-planning", Target: "backlog", Type: "requires", Confidence: 0.9},
    },
    "Agent planned sprint #42 successfully.",
    map[string]any{"agent_id": "planner-v2"},
)
```

### K-Hop Graph Traversal

```go
// Expand 3 hops from two seed entities, following only 'requires' edges.
visited, _ := client.KHopsFiltered(ctx,
    []int64{idA, idB},   // seed entity IDs
    3,                    // max hops
    []string{"requires"}, // edge-type filter (nil = all types)
)
fmt.Printf("Reachable: %v\n", visited)
```

### Shortest Path

```go
hops, err := client.ShortestPathFiltered(ctx, srcID, destID, nil, 20)
if hops == -1 {
    fmt.Println("No path found")
} else {
    fmt.Printf("Shortest path: %d hops\n", hops)
}
```

### Entity Neighborhood (LLM Context Injection)

```go
n, _ := client.EntityNeighborhood(ctx, entityID, 10, 10, 0.5)
// n.Entity, n.Outgoing, n.Incoming are ready for JSON serialisation into prompts
```

### Batch Knowledge Ingestion

```go
runs := []struct{ key, outcome string }{
    {"run-cold-start",  "success"},
    {"run-follow-up",   "partial"},
}
concepts := []pgdgraph.ConceptInput{
    {Type: "skill", Name: "sprint-planning", Confidence: 0.9},
}
for _, r := range runs {
    client.LearnFromRun(ctx, r.key, "webpm", r.outcome, concepts, nil, "", nil)
}
```

## Testing

**Tests require a real PostgreSQL instance with pg_dgraph installed.**

### Recommended: Docker-based runner

```bash
cd examples/golang

# Full cycle (build image, start DB, run tests, clean up)
make test

# Or use the script directly
./run_tests.sh
```

### Manual (against an already-running container)

```bash
# Start only the database
make start-db

# Run tests
make test-fast

# View logs
make logs

# Clean up
make clean
```

### Environment Variables

| Variable | Effect |
|----------|--------|
| `TEST_DATABASE_URL` | Override database connection string (default: `localhost:5436`) |
| `PG_DGRAPH_TEST_FAIL_ON_NO_DB=true` | FAIL instead of skip when no database reachable (CI mode) |

### CI/CD Integration

```yaml
# .github/workflows/test.yml
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          submodules: recursive

      - uses: actions/setup-go@v5
        with:
          go-version: '1.21'

      - name: Run Go tests
        working-directory: extensions/pg_dgraph/examples/golang
        run: make test
```

### What Tests Verify

- Extension present and version reported correctly
- `UpsertEntity` is idempotent (same ID, confidence raised to max)
- `FindEntitiesByType` returns active entities
- `DeprecateEntity` hides entity from active queries
- `UpsertRelation` creates relations, `GetRelationsFrom` returns them
- `RebuildLjRelations` succeeds without error
- `KHopsFiltered` traverses BFS correctly (A→B→C in 2 hops)
- `ShortestPathFiltered` returns correct hop count and -1 for no path
- Edge-type filtering in `KHopsFiltered` prunes unreachable nodes
- `RegisterAliases` + `ResolveTerms` round-trip
- `EntityFtsSearch` finds entities by description text
- `MarketplaceSearch` executes without error
- `LearnFromRun` is idempotent (same run_id on second call)
- `SkillDependencies` returns transitive deps
- `EntityNeighborhood` returns correct JSON with outgoing edges
- `ConfidenceDecay` returns a non-negative score

## Requirements

- PostgreSQL 17+
- `pg_dgraph` extension (version 0.3.1+)
- `pg_roaringbitmap` extension
- Go 1.21+

## License

MIT
