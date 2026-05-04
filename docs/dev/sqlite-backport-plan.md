# SQLite Backport Plan

This repository is now SQLite-first.

The goal is not to restore extension packaging or reintroduce SQL-driven install scripts. The goal is to port the useful newer behavior into the SQLite-backed Zig code under `src/standalone/`, and to keep the public examples, tests, and docs aligned with that SQLite surface.

## Scope

- Keep the SQLite engine as the primary implementation.
- Port behavior, not extension-specific wiring.
- Prefer native Zig implementations and SQLite queries over SQL extension functions.
- Keep TOON output support where it already exists and extend it for the newer graph and facet surfaces.
- Update tests and docs after the code lands so the repository stays self-consistent.

## What Is Already Present

- TOON encoding infrastructure already exists in `src/standalone/toon_exports.zig`.
- Facet count TOON output already exists in `src/standalone/facet_store.zig`.
- Workspace export TOON output already exists in `src/standalone/workspace_sqlite.zig`.
- Graph traversal and shortest-path TOON output already exist in `src/standalone/graph_sqlite.zig`.
- Core graph capabilities already exist in `src/standalone/graph_sqlite.zig`:
  - entity upsert and deprecation
  - alias registration and resolution
  - entity-document links
  - marketplace search
  - skill dependency traversal
  - confidence decay
  - neighborhood export
- Workspace registry and ontology helpers already exist in `src/standalone/workspace_sqlite.zig` and `src/standalone/ontology_sqlite.zig`.

## Missing Feature Families To Port

### Facets

- Bring over the later facet-count behavior as SQLite-native logic.
- Add or verify TOON output for facet counts with vector-aware filtering when relevant.
- Ensure facet filtering and count ordering match the newer behavior.
- Add tests that cover the TOON representation, not just the raw row results.

### Graph

- Add explicit lookup helpers for graph entities and relations.
- Add the workspace mutation and count helpers used by newer clients.
- Add cleanup helpers for test data where needed.
- Add ordered stream APIs for graph consumption by UIs and SSE/JSONL clients.
- Add TOON wrappers for the newer graph search and stream outputs.
- Make sure the existing SQLite graph code continues to own the data model and traversal logic.

### Ontology / Workspace

- Keep the existing SQLite ontology and workspace export path.
- Add any missing TOON or helper variants only when a concrete SQLite consumer needs them.
- Avoid duplicating logic that is already covered by `workspace_sqlite.zig` and `ontology_sqlite.zig`.

## Implementation Order

### Phase 1: API Inventory

- Diff the current SQLite modules against the newer behavior.
- Record which missing capabilities already exist in another SQLite module.
- Separate true gaps from duplicate functionality that only needs a wrapper or test update.

### Phase 2: Graph Core Port

- Add the missing graph lookup and mutation helpers to `src/standalone/graph_sqlite.zig`.
- Add the stream-oriented graph APIs for incremental rendering.
- Add TOON variants for the new graph outputs.
- Keep the APIs SQLite-native and return SQLite-friendly row/value shapes.

### Phase 3: Facet Port

- Fold in the newer facet-count and filtering behavior.
- Add any missing TOON variant for facet counts and vector-aware counts.
- Keep facet count ordering and filtering stable across the SQLite store and the public helpers.

### Phase 4: Tests

- Extend the SQLite graph tests to cover the newly added helpers.
- Extend the facet tests to cover the TOON output and any vector-aware path.
- Update integration tests to use the new SQLite APIs instead of older assumptions.

### Phase 5: Documentation

- Update `docs/graph.md` and `docs/facets.md` to describe the SQLite behavior.
- Update `docs/native-reference.md` only if a new Zig export is added.
- Update example READMEs for the graph clients so they describe the SQLite-backed contract.
- Keep this plan document current as tasks land.

## Non-Goals

- Do not restore extension metadata or install scripts.
- Do not reintroduce the SQL install script as a source of truth.
- Do not port non-SQLite APIs unless there is a SQLite implementation behind them.
- Do not widen the scope into unrelated refactors while porting the feature gaps.

## Validation

- `zig build test`
- Targeted standalone graph tests
- Targeted standalone facet tests
- Example client tests for graph and facets
- Any new SQLite stream or TOON API should have a direct regression test

## Working Rule

When a feature exists in the newer code, the first question is whether an equivalent already exists in SQLite. If it does, reuse or adapt it. If it does not, implement the missing behavior in the standalone Zig modules and then update the docs and tests around that SQLite API.
