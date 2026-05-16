# MindBrain documentation

This repository ships a **SQLite-first** knowledge engine with faceted search, BM25, graph traversal, pragma helpers, workspace registry, and a small CLI.

## Start here

| Document | Description |
|----------|-------------|
| [overview.md](overview.md) | What the system contains and how the pieces fit together |
| [installation.md](installation.md) | Dependencies, build, and installing the runtime |
| [api-reference.md](api-reference.md) | Current HTTP routes, CLI commands, and native/SQL surface boundaries |
| [comparison/README.md](comparison/README.md) | API and SQL migration comparison with the sibling `pg_mindbrain` PostgreSQL extension |
| [facets.md](facets.md) | `facets` schema: faceting, search, BM25 |
| [faceted-hybrid-search.md](faceted-hybrid-search.md) | Process view for facets, FTS5 BM25, indexed embeddings, hybrid search, and optional reranking |
| [graph.md](graph.md) | `graph` schema: entities, relations, native traversal |
| [pragma.md](pragma.md) | Memory projection helpers and `pragma_*` SQL functions |
| [dsl-rules.md](dsl-rules.md) | Proposition DSL line format (for `projection_type = 'proposition'`) |
| [workspace.md](workspace.md) | `mindbrain.*` workspace and semantics tables |
| [standalone.md](standalone.md) | SQLite engine, tests, benchmarks, `mindbrain-standalone-tool` |
| [document-profile.md](document-profile.md) | PDF/HTML normalization, LLM document profiling, chunking policy, queue worker, `corpus-eval` |
| [demo-benchmark.md](demo-benchmark.md) | Demo seeding and IMDb/YAGO benchmark contract, including PostgreSQL-side implementation guidance |
| [native-reference.md](native-reference.md) | Native Zig symbol reference |
| [third-party.md](third-party.md) | Bundled native dependencies (attribution) |
| [dev/sqlite-backport-plan.md](dev/sqlite-backport-plan.md) | SQLite-first port plan for newer graph/facet behavior |
| [dev/sqlite-vector-search-testing.md](dev/sqlite-vector-search-testing.md) | How to test SQLite embedding search, including fillable `.env` variables |
| [sqlite-parity.md](sqlite-parity.md) | API-identical SQLite-backed contract (parity with `pg_mindbrain`) |
| [dev/api-parity-inventory.md](dev/api-parity-inventory.md) | Per-function `pg_mindbrain` → `mindbrain` parity matrix |

## Source of truth

- SQL objects: [../sql/sqlite_mindbrain--1.0.0.sql](../sql/sqlite_mindbrain--1.0.0.sql)
- Zig sources: [src/](../src/) (`mb_facets`, `mb_graph`, `mb_pragma`, `standalone`)
- Runtime entrypoints: [../src/standalone/tool.zig](../src/standalone/tool.zig), [../src/standalone/http_server.zig](../src/standalone/http_server.zig), [../build.zig](../build.zig)

This project is **not** the GhostCrab MCP server; it is the SQLite runtime and supporting library those applications may call into.
