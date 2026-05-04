# Zig 0.16 Migration Audit

Branch: `zig_0.16`

## Toolchain

This branch targets Zig 0.16.0 exactly. The build script enforces that version at compile time, and the README now calls out the same requirement.

For local validation during the migration, an official Zig 0.16.0 x86_64 Linux toolchain was downloaded into ignored workspace state under `.codex/toolchains/`.

## Upstream 0.16 Changes That Matter Here

The official Zig 0.16.0 release notes call out these migration-relevant changes:

- `@cImport` is deprecated and intended to move into the build system through `b.addTranslateC(...)`.
- `@Type` was replaced by more focused builtins such as `@Int`, `@Struct`, `@Union`, `@Enum`, and `@Pointer`.
- I/O is now modeled as `std.Io`; filesystem, networking, process, entropy, and time APIs moved behind that interface.
- `std.heap.GeneralPurposeAllocator` was renamed/replaced by `std.heap.DebugAllocator`.
- `linkSystemLibrary` moved from compile artifacts to modules in the build system.
- `std.process.Child.run` is gone for build scripts; use `std.Build.runAllowFail` or the new process APIs with an explicit `std.Io`.

## Completed Preparation

- Created branch `zig_0.16`.
- Added an exact Zig 0.16.0 guard in `build.zig`.
- Updated `README.md` with the Zig 0.16.0 requirement.
- Migrated `build.zig` from `std.process.Child.run` to `b.runAllowFail`.
- Migrated `build.zig` system library links from compile artifacts to modules.
- Replaced `std.heap.GeneralPurposeAllocator` with `std.heap.DebugAllocator` in the affected source/test files.
- Migrated standalone test, CLI, HTTP, and benchmark targets to the Zig 0.16 `std.Io` filesystem, process, network, time, random, and mutex APIs.
- Added small Zig 0.16 compatibility shims for targets that need a module-local `std.Io` bridge during migration.

## Current Validation State

Command used:

```bash
.codex/toolchains/zig-x86_64-linux-0.16.0/zig build test standalone-tool benchmark-tool standalone-http bench-standalone --cache-dir .codex/zig-cache --global-cache-dir .codex/zig-global-cache
```

Current result:

- `zig build test` passes under Zig 0.16.0. The aggregate standalone runner prints two expected SQLite rollback diagnostics (`no such savepoint: outer_facet_tx`) but exits successfully.
- `standalone-tool`, `benchmark-tool`, and `standalone-http` compile under Zig 0.16.0.
- `bench-standalone` compiles and runs under Zig 0.16.0.

## Comparative Benchmark Harness

Use the repo-local benchmark harness to compare the last Zig 0.15.2 baseline against the current Zig 0.16 migration tree:

```bash
scripts/compare-zig-toolchains.sh --iterations 3
```

Default behavior:

- Benchmarks Zig 0.15.2 against `main` in an ignored detached git worktree.
- Benchmarks Zig 0.16.0 against the current working tree.
- Runs `zig build test` by default.
- Writes logs, per-run cache directories, `results.csv`, and `summary.md` under `.codex/zig-benchmarks/<timestamp>/`.
- Keeps non-zero exit codes in the CSV so migration blockers remain measurable.

Useful options:

```bash
scripts/compare-zig-toolchains.sh --iterations 5 --build-tools
scripts/compare-zig-toolchains.sh --iterations 3 --build-tools --runtime
scripts/compare-zig-toolchains.sh --ref-015 main --zig-015 /usr/local/bin/zig --zig-016 .codex/toolchains/zig-x86_64-linux-0.16.0/zig
```

The `--build-tools` flag adds `standalone-tool` and `benchmark-tool` build steps. The `--runtime` flag adds `bench-standalone`, which runs the standalone runtime benchmark and should be used once both toolchains compile the benchmark target.

Previous one-iteration smoke run, captured before the final migration fixes:

```bash
scripts/compare-zig-toolchains.sh --iterations 1
```

Output:

- Summary: `.codex/zig-benchmarks/20260425T073605Z/summary.md`
- CSV: `.codex/zig-benchmarks/20260425T073605Z/results.csv`

Observed results:

| Toolchain | Ref / Tree | Step | Exit | Elapsed ms | Notes |
| --- | --- | --- | ---: | ---: | --- |
| Zig 0.15.2 | `main` | `zig build test` | 0 | 6833 | Baseline passes. |
| Zig 0.16.0 | current `zig_0.16` tree at that time | `zig build test` | 1 | 4566 | Historical pre-fix failure at standalone 0.16 API blockers; BM25 still reported `11/11 tests passed` before aggregate failure. |

Latest direct Zig 0.16 validation:

| Toolchain | Tree | Step | Exit | Notes |
| --- | --- | --- | ---: | --- |
| Zig 0.16.0 | current `zig_0.16` tree | `zig build test standalone-tool benchmark-tool standalone-http bench-standalone` | 0 | Unit tests pass; tool targets compile; standalone benchmark runs. |

## Remaining Migration Buckets

### I/O Interface

The blocking `std.Io` migration for current standalone/test/tool targets is complete. The code now uses:

- `std.Io.Dir` / `std.Io.File` for filesystem reads, writes, directory creation, and benchmark dataset reads.
- `std.Io.net.IpAddress`, `std.Io.net.Server`, and `std.Io.net.Stream` for HTTP listener and connection handling.
- `std.process.Init` for CLI/HTTP/benchmark entrypoints.
- `std.Io.Timestamp` and local timer shims for elapsed-time measurements.
- `std.Random.IoSource` backed by configured `std.Io` for nanoid generation.
- `std.Io.Mutex` for HTTP shared state.

Follow-up cleanup direction:

- Prefer passing `std.Io` explicitly through public APIs where it is not too invasive.
- Keep the temporary `zig16_compat.zig` shims only as long as they reduce churn during the migration branch.

### C Imports

These files still use deprecated `@cImport`:

- `src/mb_facets/utils.zig`
- `src/mb_graph/utils.zig`
- `src/mb_pragma/utils.zig`
- `src/standalone/facet_sqlite.zig`
- `src/standalone/roaring.zig`

Recommended direction:

- Add small C header shim files for PostgreSQL, SQLite, and CRoaring includes.
- Use `b.addTranslateC(...)` in `build.zig`.
- Inject translated modules with `.imports`/`addImport`.

### ArrayList API Drift

The known standalone ArrayList blockers are fixed:

- `src/standalone/pragma_dsl.zig` now uses `std.ArrayList(...).empty`.
- `src/standalone/ontology_sqlite.zig` no longer uses removed `buf.writer(allocator)` calls.

### Networking

The standalone HTTP networking migration is complete enough for the target to compile under Zig 0.16.0:

- `src/standalone/http_server_config.zig` parses listen addresses as `std.Io.net.IpAddress`.
- `src/standalone/http_server.zig` listens, accepts, reads, writes, sleeps, and synchronizes through `std.Io`.

## Suggested Next Pass

1. Move remaining deprecated `@cImport` usage to build-system `addTranslateC`.
2. Replace temporary `zig16_compat.zig` shims with explicit `std.Io` parameters where that improves API clarity.
3. Rerun `scripts/compare-zig-toolchains.sh --iterations 3 --build-tools --runtime` now that the Zig 0.16 runtime benchmark builds.

## Tools And Commands Used

Toolchain discovery and branch setup:

- `git status --short`, `git branch --list zig_0.16`, `git switch -c zig_0.16`
- `git branch --all --verbose --no-abbrev`, `git log --oneline --decorate -8`
- `zig version`, `command -v zig`, `command -v zig-0.16`, `command -v zig-0.15`, `command -v zig-0.15.2`

Official Zig references:

- Zig 0.16.0 language reference: `https://ziglang.org/documentation/0.16.0/`
- Zig 0.16.0 release notes: `https://ziglang.org/download/0.16.0/release-notes.html`
- Zig downloads page: `https://ziglang.org/download/`

Local Zig 0.16.0 toolchain preparation:

```bash
mkdir -p .codex/toolchains
curl -L --fail --show-error --silent -o .codex/toolchains/zig-0.16.0.tar.xz https://ziglang.org/download/0.16.0/zig-x86_64-linux-0.16.0.tar.xz
tar --no-same-owner -C .codex/toolchains -xf .codex/toolchains/zig-0.16.0.tar.xz
.codex/toolchains/zig-x86_64-linux-0.16.0/zig version
```

Build and validation commands:

```bash
.codex/toolchains/zig-x86_64-linux-0.16.0/zig build test --cache-dir .codex/zig-cache --global-cache-dir .codex/zig-global-cache
.codex/toolchains/zig-x86_64-linux-0.16.0/zig build standalone-tool benchmark-tool standalone-http --cache-dir .codex/zig-cache --global-cache-dir .codex/zig-global-cache
.codex/toolchains/zig-x86_64-linux-0.16.0/zig build bench-standalone --cache-dir .codex/zig-cache --global-cache-dir .codex/zig-global-cache
.codex/toolchains/zig-x86_64-linux-0.16.0/zig build test standalone-tool benchmark-tool standalone-http bench-standalone --cache-dir .codex/zig-cache --global-cache-dir .codex/zig-global-cache
.codex/toolchains/zig-x86_64-linux-0.16.0/zig fmt build.zig src/mb_facets/bm25/search_test.zig src/mb_facets/bm25/tokenizer_pure.zig src/standalone/tool.zig src/standalone/http_server.zig src/standalone/bench.zig src/benchmark/tool.zig
scripts/compare-zig-toolchains.sh --iterations 1
```

Benchmark harness internals:

- `git worktree add --detach` creates the Zig 0.15.2 baseline worktree from `main`.
- `git worktree remove --force` cleans up the generated baseline worktree.
- Separate `--cache-dir` and `--global-cache-dir` directories are created per toolchain, step, and iteration.
- `date +%s%N` measures elapsed time in milliseconds.
- `awk` generates the Markdown summary table from `results.csv`.

System tools and libraries touched by the build:

- `pg_config --includedir-server` and `pg_config --sharedir` are queried by `build.zig` when available.
- `sqlite3` is linked for standalone test and tool targets.
- `tar`, `curl`, `chmod`, `mkdir`, and POSIX shell utilities are used only for local toolchain/setup and benchmark automation.
