# YAGO Import Status

## Current Action Done

- Added a dedicated YAGO import path for the standalone SQLite tool.
- Wired `mindbrain-benchmark-tool yago-import` and `yago-benchmark` to load YAGO data from an extracted directory or a single RDF/Turtle file.
- Downloaded the official YAGO 4.5 tiny archive into `/media/dlamotte/DATA3/yago` and extracted it to `yago-tiny.ttl`.
- Verified the import path with a smoke run and passed the standalone test suite.

## Next Steps

1. Add support for the full YAGO 4.5 archive layout, including the larger split files if you want to benchmark the full dataset.
2. Add optional decompression support for `.zip` and `.gz` inputs so the importer can work directly from downloaded archives.
3. Add a small benchmark wrapper that reports rows/sec and file throughput for repeatable comparisons.
4. Tighten RDF-star handling if you plan to load the `meta` annotations beyond the current plain RDF path.
5. Decide whether the importer should preserve literal nodes separately or collapse labels into aliases only.

## Status: workspace + collection + ontology now wired

The importer now sits on top of the generic collections / ontology raw layer
described in [`docs/collections.md`](./collections.md). On every import it:

- Calls `ensureYagoScaffold(...)` to idempotently create the workspace
  (default id `yago`), a collection (`yago::core_facts`) and a small
  `yago-core` ontology bundle declaring the `resource` / `literal` entity
  types and the `type`, `label` and `knows` edge types.
- Writes entities, aliases and relations into the canonical raw tables
  (`entities_raw`, `entity_aliases_raw`, `relations_raw`) via
  `mirrorRawTables` so reindex passes can rebuild every derived index.
- Accepts `--workspace-id`, `--collection-id`, `--ontology-id` overrides on
  both `yago-import` and `yago-benchmark`, e.g.

```bash
./zig-out/bin/mindbrain-benchmark-tool yago-import \
  --db /tmp/yago-tiny.sqlite \
  --yago-path /media/dlamotte/DATA3/yago/yago-tiny.ttl \
  --workspace-id yago \
  --collection-id yago::core_facts \
  --ontology-id yago-core
```

The previous hardcoded `workspace_id = "imdb"` issue noted below is gone:
all writes now use the configured workspace id.


Use a separate `--db` path for YAGO, exactly like you do for IMDb. For your files, the most direct command is the single-file form:

```bash
./zig-out/bin/mindbrain-benchmark-tool yago-import \
  --db /tmp/yago-tiny.sqlite \
  --yago-path /media/dlamotte/DATA3/yago/yago-tiny.ttl
```

If you want a quick smoke run first:

```bash
./zig-out/bin/mindbrain-benchmark-tool yago-import \
  --db /tmp/yago-smoke.sqlite \
  --yago-path /media/dlamotte/DATA3/yago/yago-tiny.ttl \
  --limit 1000
```

You can also point at the directory instead of the file:

```bash
./zig-out/bin/mindbrain-benchmark-tool yago-import \
  --db /tmp/yago-tiny.sqlite \
  --yago-dir /media/dlamotte/DATA3/yago
```

A few important details:

- `yago-import` and `yago-benchmark` currently go through the same code path in `mindbrain-benchmark-tool`, so either command works.
- Your `.zip` is not imported directly yet. The current importer needs an extracted RDF/Turtle file or directory, so `yago-tiny.ttl` is the right input.
- Using its own SQLite file, like `/tmp/yago-tiny.sqlite`, is what keeps it isolated from IMDb.

### Historical note (resolved)

Earlier revisions of the YAGO importer hardcoded the workspace id to
`"imdb"` in the `entity_stmt` / `relation_stmt` bindings. The new pipeline
threads `Importer.workspace_id` (defaulting to `yago`, configurable via
`--workspace-id`) through `upsertEntity` and `insertRelation`, so the legacy
behaviour is no longer reachable.
