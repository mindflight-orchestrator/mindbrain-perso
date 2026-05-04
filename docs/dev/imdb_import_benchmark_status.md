# IMDb Import Benchmark Status

## Done

- Added `mindbrain-benchmark-tool imdb-benchmark` in `src/benchmark/tool.zig`.
- Added a streaming IMDb TSV importer in `src/standalone/imdb_import.zig`.
- Wired the importer to handle:
  - `title.basics.tsv`
  - `name.basics.tsv`
  - `title.ratings.tsv`
  - `title.akas.tsv`
  - `title.episode.tsv`
  - `title.crew.tsv`
  - `title.principals.tsv`
- Made the importer safe for partial runs with `--limit`.
- Added a small TSV fixture test for the importer.
- Updated `docs/demo-benchmark.md` with the benchmark contract and implementation guidance.
- Verified:
  - `zig build test`
  - `zig build standalone-tool`
  - smoke import against `/media/dlamotte/DATA3/imdb` with `--limit 1000`

## Observed Results

- Smoke run completed successfully.
- The smoke summary showed the importer can write graph entities, aliases, and relations without failing on the mounted IMDb data.
- The unrestricted run started successfully and progressed into the larger IMDb files, but it was not allowed to finish inside this session.

## Next Steps

1. Add `.tsv.gz` support so the benchmark can read the compressed IMDb dumps directly.
2. Reduce full-run write amplification by batching relation/entity writes or adding explicit transaction chunking.
3. Add a progress log or periodic checkpoint output so long imports report live status.
4. Add a resume/checkpoint mode for long imports if the full IMDb corpus needs to be processed in multiple passes.
5. Consider a dedicated benchmark summary file or JSON output artifact for archiving import timings.

## Suggested Command

```bash
./zig-out/bin/mindbrain-benchmark-tool imdb-benchmark \
  --db /tmp/imdb-full.sqlite \
  --imdb-dir /media/dlamotte/DATA3/imdb
```
