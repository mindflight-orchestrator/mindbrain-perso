# MindBrain graphs

This directory is the canonical reference for MindBrain graph storage,
ingestion, query, and diagnostics.

MindBrain is SQLite-first in this repository. The active implementation lives
mostly in [`src/standalone/graph_sqlite.zig`](../../src/standalone/graph_sqlite.zig),
with Graph Pattern Query support in
[`src/standalone/graph_pattern.zig`](../../src/standalone/graph_pattern.zig)
and HTTP routing in
[`src/standalone/http_app.zig`](../../src/standalone/http_app.zig).
The legacy `src/mb_graph` package contains native PostgreSQL extension entry
points for filtered k-hop traversal and shortest path wrappers.

## Core rule

The graph has two layers:

| Layer | Tables | Role |
| --- | --- | --- |
| Raw source of truth | `entities_raw`, `relations_raw`, `relation_properties_raw`, document/chunk/evidence raw tables | Durable import state. Back this up. |
| Derived graph index | `graph_entity`, `graph_relation`, `graph_relation_property`, adjacency and degree tables | Serving/index layer. Rebuildable from raw rows. |

Do not treat `graph_entity` and `graph_relation` as the only durable copy of
graph facts. In normal import flows, write raw rows first and rebuild or update
the derived graph through the pipeline.

## Documents

| Document | Use |
| --- | --- |
| [model-and-storage.md](model-and-storage.md) | Derived graph tables, indexes, workspace isolation, ontology-vs-instance graph. |
| [raw-layer.md](raw-layer.md) | Raw documents, chunks, entities, relations, properties, evidence, and links. |
| [ingestion-and-reindex.md](ingestion-and-reindex.md) | How raw rows become derived graph rows and when to refresh indexes. |
| [queries-and-apis.md](queries-and-apis.md) | GPQ, traversal, HTTP routes, CLI-facing surfaces, and SQL snippets. |
| [diagnostics-and-quality.md](diagnostics-and-quality.md) | Gap rules, diagnostics issue kinds, coverage, and remediation workflow. |
| [graph-conflict-taxonomy.md](graph-conflict-taxonomy.md) | `graph_conflict_*` kinds vs `graph_data_gap` and `graph_gap_rule`. |
| [graph-conflict-diagnostics-queries.md](graph-conflict-diagnostics-queries.md) | SQL detection for mutually exclusive, temporal, granularity, redundant facts. |
| [schema-pattern-frequency.md](schema-pattern-frequency.md) | Observed schema counts, genericity penalty, retrieval filtering. |
| [knowledge-patch-proposal-pipeline.md](knowledge-patch-proposal-pipeline.md) | Auto-generated pending patches with evidence scoring. |
| [memory-guided-recall.md](memory-guided-recall.md) | Unified recall: schema + entity + passage activation → traverse/PPR. |
| [examples-immeuble-demo.md](examples-immeuble-demo.md) | Read-only snapshot and queries for `data/immeuble-demo.sqlite`. |

## What is possible today

- Store workspace-scoped typed entities and directed relations.
- Store typed relation properties for rich edges such as ownership shares,
  right types, amounts, dates, document references, and URIs.
- Ground entities in documents and chunks through raw and derived link tables.
- Rebuild the derived graph from raw collection rows.
- Traverse the graph by outbound/inbound edges, shortest path, k-hop expansion,
  and subgraph streams.
- Query graph patterns with the shared GPQ syntax on SQLite and through a
  PostgreSQL AST execution bridge.
- Inspect graph type counts, entity detail, relation detail, diagnostics, and
  configured gap rules through HTTP.
- Run closed-world quality checks with `graph_gap_rules` and graph diagnostics.

## Primary source files

| Source | Role |
| --- | --- |
| [`sql/sqlite_mindbrain--1.0.0.sql`](../../sql/sqlite_mindbrain--1.0.0.sql) | Canonical SQLite schema snapshot. |
| [`sql/migrations/2026-05-23-graph-entity-workspace-unique.sql`](../../sql/migrations/2026-05-23-graph-entity-workspace-unique.sql) | Legacy migration that scopes entity natural keys by workspace. |
| [`src/standalone/sqlite_schema.zig`](../../src/standalone/sqlite_schema.zig) | Runtime schema bootstrap. |
| [`src/standalone/import_pipeline.zig`](../../src/standalone/import_pipeline.zig) | Raw ingestion and reindex orchestration. |
| [`src/standalone/collections_sqlite.zig`](../../src/standalone/collections_sqlite.zig) | Raw table helpers. |
| [`src/standalone/graph_sqlite.zig`](../../src/standalone/graph_sqlite.zig) | Derived graph helper, traversal, stream, and projection code. |
| [`src/standalone/graph_pattern.zig`](../../src/standalone/graph_pattern.zig) | GPQ parser, validator, SQLite executor, and JSON AST serializer. |
| [`src/standalone/graph_pattern_bridge.zig`](../../src/standalone/graph_pattern_bridge.zig) | SQLite/Postgres GPQ bridge. |
| [`src/standalone/graph_diagnostics.zig`](../../src/standalone/graph_diagnostics.zig) | Gap rules and diagnostics report generation. |
