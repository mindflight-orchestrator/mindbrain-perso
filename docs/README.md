# MindBrain documentation

This repository ships a **SQLite-first** knowledge engine with faceted search, BM25, graph traversal, pragma helpers, workspace registry, and a small CLI.

## Current release

`v1.4.0` is a performance and correctness release for the standalone runtime.
It keeps the public CLI/HTTP shape stable while improving graph ingestion,
graph stream expansion, BM25/hybrid search hot paths, compact-store lookup
speed, and vector result scoping by `table_id`.

## Current ontology work

OWL2 import/export has a first standalone implementation for normalized
RDF/N-Triples input. `mindbrain-standalone-tool ontology-import` preserves raw
triples in `ontology_triples_raw`, projects simple classes/properties/relations
into ontology tables, and can optionally materialize object triples into
`entities_raw` / `relations_raw`. `ontology-export` can emit preserved
N-Triples or a taxonomies bundle. Full OWL2 reasoning is still out of scope for
the SQLite MVP; follow-up work is tracked in
[plan/2026-05-20-owl.md](plan/2026-05-20-owl.md).

## Start here

| Document | Description |
|----------|-------------|
| [overview.md](overview.md) | What the system contains and how the pieces fit together |
| [installation.md](installation.md) | Dependencies, build, and installing the runtime |
| [api-reference.md](api-reference.md) | Current HTTP routes, CLI commands, and native/SQL surface boundaries |
| [comparison/README.md](comparison/README.md) | API and SQL migration comparison with the sibling `pg_mindbrain` PostgreSQL extension |
| [facets/README.md](facets/README.md) | Complete facets documentation: raw assignments, bitmap storage, BM25/FTS5, hybrid search, APIs |
| [facets.md](facets.md) | Legacy facets pointer kept for existing links |
| [faceted-hybrid-search.md](faceted-hybrid-search.md) | Legacy process view for facets, FTS5 BM25, indexed embeddings, hybrid search, and optional reranking |
| [graphs/README.md](graphs/README.md) | Complete graph documentation: raw data, derived storage, queries, APIs, diagnostics |
| [graph.md](graph.md) | Legacy graph overview and pointer to the canonical graph docs |
| [pragma/README.md](pragma/README.md) | Complete pragma documentation: memory projections, DSL, context packing, SQL/CLI/HTTP APIs |
| [pragma.md](pragma.md) | Legacy pragma pointer kept for existing links |
| [ontology/README.md](ontology/README.md) | Complete ontology documentation: raw triples, taxonomy, LinkML/OWL2, APIs, coverage |
| [dsl-rules.md](dsl-rules.md) | Proposition DSL line format (for `projection_type = 'proposition'`) |
| [workspace.md](workspace.md) | `mindbrain.*` workspace and semantics tables |
| [standalone.md](standalone.md) | SQLite engine, tests, benchmarks, `mindbrain-standalone-tool` |
| [methodology/graphing/immeuble-gap-diagnostics-demo.md](methodology/graphing/immeuble-gap-diagnostics-demo.md) | Immeuble demo: gap tools, findings, remediation (reparse vs add facts) |
| [document-profile.md](document-profile.md) | PDF/HTML normalization, LLM document profiling, chunking policy, queue worker, `corpus-eval` |
| [demo-benchmark.md](demo-benchmark.md) | Demo seeding and IMDb/YAGO benchmark contract, including PostgreSQL-side implementation guidance |
| [source/README.md](source/README.md) | Official OWL2 source references and local N-Triples fixtures used by ontology import tests |
| [native-reference.md](native-reference.md) | Native Zig symbol reference |
| [third-party.md](third-party.md) | Bundled native dependencies (attribution) |
| [dev/sqlite-backport-plan.md](dev/sqlite-backport-plan.md) | SQLite-first port plan for newer graph/facet behavior |
| [dev/sqlite-vector-search-testing.md](dev/sqlite-vector-search-testing.md) | How to test SQLite embedding search, including fillable `.env` variables |
| [dev/sqlite-parity.md](dev/sqlite-parity.md) | API-identical SQLite-backed contract (parity with `pg_mindbrain`) |
| [dev/api-parity-inventory.md](dev/api-parity-inventory.md) | Per-function `pg_mindbrain` → `mindbrain` parity matrix |

## Source of truth

- SQL objects: [../sql/sqlite_mindbrain--1.0.0.sql](../sql/sqlite_mindbrain--1.0.0.sql)
- Zig sources: [src/](../src/) (`mb_facets`, `mb_graph`, `mb_pragma`, `mb_ontology`, `standalone`)
- Runtime entrypoints: [../src/standalone/tool.zig](../src/standalone/tool.zig), [../src/standalone/http_server.zig](../src/standalone/http_server.zig), [../build.zig](../build.zig)

This project is **not** the GhostCrab MCP server; it is the SQLite runtime and supporting library those applications may call into.
