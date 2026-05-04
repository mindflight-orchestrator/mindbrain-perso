# Demo and Benchmark Contract

This document describes the **synthetic demo** and **dataset benchmark** flows that live beside the core library code.

The intent is twofold:

1. Keep `mindbrain` / `pg_mindbrain` library code free of dataset-specific fixtures.
2. Give the PostgreSQL equivalent implementation a clear behavioral contract for demo seeding and import benchmarking.

## Scope

The repository now separates three concerns:

1. **Library code**: reusable graph, facet, pragma, workspace, and import primitives.
2. **Demo code**: small deterministic data used to exercise the product end-to-end.
3. **Benchmark code**: dataset-specific importers for IMDb and YAGO plus full-DB facet/graph workload benchmarks.

The demo and benchmark entrypoints should remain thin wrappers around reusable library operations. They should not define core schema logic beyond what is required to seed or import their own data.

## Current Command Surface

The Zig implementation currently exposes these entrypoints:

| Command | Purpose |
|---------|---------|
| `mindbrain-standalone-tool seed-demo --db <sqlite_path>` | Persist the demo dataset into a database if it is still empty. |
| `mindbrain-standalone-tool benchmark-db --db data/imdb-full.sqlite` | Run the full IMDb SQLite benchmark suite for facet and graph queries plus mutation paths. |
| `mindbrain-standalone-tool simulate` | Run the same demo flow in-memory and emit a compact JSON summary. |
| `mindbrain-benchmark-tool imdb-benchmark --db <sqlite_path> --imdb-dir <path> [--limit <n>]` | Import IMDb TSV dumps and report timing plus row counts. |
| `mindbrain-benchmark-tool yago-import --db <sqlite_path> --yago-dir <path> [--limit <n>]` | Import YAGO RDF/Turtle files and report timing plus row counts. |
| `mindbrain-benchmark-tool yago-benchmark --db <sqlite_path> --yago-dir <path> [--limit <n>]` | Alias of `yago-import` for benchmark-oriented runs. |

The benchmark tool is intentionally separate from the main standalone CLI so dataset-specific import code stays out of the general-purpose binary.

For the full SQLite corpus already present in this repository, use `mindbrain-standalone-tool benchmark-db`. That command opens the SQLite file, seeds a temporary benchmark workspace/table, runs facet and graph queries, exercises single-row and batch insert/update/remove paths, and rolls everything back before exit.

## Demo Contract

The demo path is meant to be a small, deterministic, end-to-end scenario.

### Behavior

`seed-demo`:

1. Opens the target database.
2. Applies the standalone schema.
3. Checks whether `graph_entity` already contains rows.
4. If rows already exist, it returns JSON indicating the seed was skipped.
5. Otherwise it seeds demo data and returns JSON indicating the database was seeded.

`simulate`:

1. Opens an in-memory database.
2. Applies the standalone schema.
3. Seeds the same demo dataset used by `seed-demo`.
4. Runs a small queue, workspace export, context packing, graph traversal, and query execution scenario.
5. Emits a JSON summary with the resulting counts.

### Demo Data Shape

The demo is not intended to be a large fixture. It should instead exercise the main features together:

1. A default workspace with semantic metadata.
2. A small document table with taxonomy/facet assignments.
3. A small graph topology with at least one named traversal path.
4. A memory/projection sample for context packing.
5. A queue sample so queue persistence is covered.

The exact rows are implementation details, but the demo should preserve these invariants:

1. The dataset is deterministic.
2. The demo is small enough to seed quickly in CI.
3. The demo exercises graph, facet, workspace, queue, and projection code paths together.

### Demo Output

`seed-demo` should return JSON of the form:

```json
{ "seeded": true }
```

If the database already contains demo data, it should return:

```json
{ "seeded": false, "skipped": true }
```

`simulate` should return a JSON object with summary counters rather than a binary success flag. The exact field set may expand, but it should remain stable enough for smoke tests and dashboard checks.

## Benchmark Contract

The benchmark path is meant to measure dataset import throughput and post-import rebuild costs.

### Shared Rules

Both IMDb and YAGO benchmark flows should follow the same structural pattern:

1. Open a target database.
2. Apply the standalone schema.
3. Load source data into temporary staging tables.
4. Flush staged rows into the persistent graph tables.
5. Rebuild derived structures that the runtime depends on.
6. Return timing and row-count summaries as JSON.

### IMDb Import

The IMDb importer consumes the standard TSV dumps:

1. `title.basics.tsv`
2. `name.basics.tsv`
3. `title.ratings.tsv`
4. `title.akas.tsv`
5. `title.episode.tsv`
6. `title.crew.tsv`
7. `title.principals.tsv`

Required behaviors:

1. Support a `--limit` option that caps the number of rows imported per file.
2. Tolerate a directory that contains the files either directly or under an `imdb-datasets/` subdirectory.
3. Stage data in temporary tables before flushing into persistent graph tables.
4. Emit timing for each major file plus a total duration.
5. Report entity, relation, and alias row counts.

### YAGO Import

The YAGO importer consumes extracted RDF/Turtle-style files from either a directory or a single file.

Required behaviors:

1. Support `--yago-dir` and `--yago-path`.
2. Support `--limit`.
3. Accept common source file extensions such as `.ttl`, `.txt`, `.nt`, and `.ntx`.
4. Parse URI and literal triples.
5. Treat label predicates as aliases in addition to normal relations.
6. Stage data in temporary tables before flushing into persistent graph tables.
7. Rebuild adjacency and entity degree after import.

### Benchmark Output

The benchmark JSON should include at least:

1. Source path fields such as `imdb_dir`, `yago_dir`, or `yago_path`.
2. The requested row limit.
3. Imported row counts.
4. Derived row counts such as entity, relation, alias, and label rows.
5. Timing fields in nanoseconds or equivalent monotonic units.

## Suggested PostgreSQL Mapping

The PostgreSQL equivalent implementation should preserve the same logical split even if the SQL details differ.

### Library Side

Keep reusable code in `pg_mindbrain` library modules:

1. Graph/facet/workspace schema helpers.
2. Generic staging helpers.
3. Shared row parsing and import utilities that do not depend on a specific dataset.

### Demo Side

Implement the demo as a separate entrypoint or SQL routine that:

1. Seeds a small deterministic dataset.
2. Uses the same public schema objects as the library.
3. Avoids large external fixture files.
4. Can be run repeatedly without corrupting an already seeded database.

### Benchmark Side

Implement IMDb and YAGO as dedicated benchmark entrypoints or workers that:

1. Load dataset files from disk.
2. Use temp staging structures.
3. Flush to persistent tables in bulk.
4. Rebuild any derived search or graph structures after the import.
5. Return machine-readable timing and row-count summaries.

## Implementation Notes

1. Keep dataset-specific file paths, file discovery, and parser details out of the main library API.
2. Keep the benchmark and demo commands thin so they are easy to mirror in PostgreSQL.
3. Prefer deterministic output for the demo path.
4. Prefer stable JSON field names for benchmark summaries.
5. If a future PostgreSQL port cannot mirror a specific SQLite optimization, keep the user-visible contract and summary format the same.

## Acceptance Criteria

A PostgreSQL-side implementation is compatible if it can:

1. Seed a small demo database and expose a repeatable smoke test.
2. Run IMDb and YAGO imports through dedicated benchmark entrypoints.
3. Produce equivalent JSON summaries for demo and benchmark runs.
4. Keep the reusable library layer separate from dataset fixtures and benchmark wiring.
