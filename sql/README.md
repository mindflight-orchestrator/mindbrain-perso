# MindBrain SQLite Schema

This directory owns the SQLite schema inputs used by the standalone runtime.

## Files

| Path | Role |
|------|------|
| `sqlite_mindbrain--1.0.0.sql` | Canonical base schema embedded by `build.zig` through `sqlite_schema.zig`. |
| `migrations/*.sql` | Operator-facing or historical migrations kept as separate references. |
| `stopwords/` | BM25 stopword seed inputs and generated seed SQL. |
| `generated/sqlite_mindbrain--current.compiled.sql` | Intended final-schema snapshot for audits and LLM analysis. This file is not present yet and should be generated, not edited by hand. |

## Runtime Truth

The operational schema is not a plain concatenation of `sqlite_mindbrain--1.0.0.sql`
and `migrations/*.sql`.

The runtime applies the embedded base schema and then executes Zig-side schema
maintenance in `Database.applyStandaloneSchema()`, including additive column
migrations, guard migrations, and strict workspace migrations. For an exact
final schema snapshot, generate it from a temporary SQLite database after
running the same runtime path.

## Recommended Generated Snapshot

Add a generator command with this shape:

```bash
zig build schema-compile
```

The command should:

1. create a temporary SQLite database;
2. call `Database.applyStandaloneSchema()`;
3. dump deterministic DDL from `sqlite_master`;
4. write `sql/generated/sqlite_mindbrain--current.compiled.sql`.

A companion check command should regenerate into `/tmp` and compare against the
tracked snapshot:

```bash
zig build schema-compile-check
```

That check is the CI guard against schema drift between the runtime and the
LLM/audit-friendly compiled schema.
