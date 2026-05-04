# Workspace and ontology

## `mindbrain` schema

The runtime creates registry tables used for **workspace isolation**, **DDL proposals**, and **table/column/relation semantics**. Definitions start around **`CREATE SCHEMA IF NOT EXISTS mindbrain`** in the SQL install script.

| Object | Purpose |
|--------|---------|
| **`mindbrain.workspaces`** | Workspace id, label, target schema (`pg_schema`), status, optional `domain_profile` |
| **`mindbrain.pending_migrations`** | Proposed SQL + sync/semantic specs awaiting approval / execution |
| **`mindbrain.query_templates`** | Parameterized SQL templates per workspace |
| **`mindbrain.source_mappings`** | External source → table field maps |
| **`mindbrain.table_semantics`** | Per-table business role, generation strategy, flags for facet/graph emission |
| **`mindbrain.column_semantics`** | Column roles and rich metadata |
| **`mindbrain.relation_semantics`** | FK-style relation semantics between tables |

## Facet and graph alignment

- **`facets`** (and related tables in the SQL script) gain **`workspace_id`** with default `'default'`.
- **`graph.entity`** and **`graph.relation`** include **`workspace_id`** for the same reason.

Application queries should filter on **`workspace_id`** consistently across facets, graph, and registry tables.

## `mb_ontology` schema

**`mb_ontology`** is a **function-only** integration layer (no standalone ontology tables in the runtime script). It provides helpers that:

- Resolve a **domain** string to a **workspace id** (`resolve_workspace`, `coverage_by_domain`, …).
- Compute **coverage** reports over facets, graph entities, and optional projections when `public.facets`, `public.projections`, and graph tables exist.
- Run **marketplace-style search** combining FTS, graph distance, facet match counts, and projection relevance (`marketplace_search`, `marketplace_search_by_domain`).
- Support **DDL proposal validation** and **trigger generation** for workspace-scoped evolution (`validate_ddl_proposal`, `generate_triggers`).
- **Export** a workspace model as JSON (`export_workspace_model`) for consumers.

Many functions **guard** with `mb_ontology._relation_exists(...)` so partial installs degrade gracefully.

For signatures and return types, use the SQL file (search for `CREATE OR REPLACE FUNCTION mb_ontology.`).

## Standalone parity

The SQLite **standalone tool** can export workspace models and run coverage-style flows; see [standalone.md](standalone.md).
