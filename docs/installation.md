# Installation

## Prerequisites

- **Zig** — [build.zig](../build.zig) requires **exactly 0.16.0** (`zig version` must report `0.16.0`).
- **Native headers** — PostgreSQL server headers are required when building the native shared library.
- **Bitmap dependency:** the vendored CRoaring sources under [deps/pg_roaringbitmap/](../deps/pg_roaringbitmap/) are linked by [build.zig](../build.zig). PostgreSQL deployments that use the native extension install path should also provide the `roaringbitmap` extension expected by that environment.
- **Optional:** **`vector`** — referenced in [docker/init/01-init-postgres.sql](../docker/init/01-init-postgres.sql) for embeddings on `facets` and similar objects.

## Build the shared library and standalone tools

From the repository root:

```bash
/opt/zig/zig-x86_64-linux-0.16.0/zig build
/opt/zig/zig-x86_64-linux-0.16.0/zig build standalone-tool
/opt/zig/zig-x86_64-linux-0.16.0/zig build standalone-http
```

Use any equivalent Zig 0.16.0 binary if your local path differs.

`zig build` produces the shared library under `zig-out/lib/` (name may vary by platform). The standalone build steps install `mindbrain-standalone-tool` and `mindbrain-http` under `zig-out/bin/`.

The build links the vendored CRoaring-backed roaring implementation used by Zig.

## Install the native surface

1. Copy the shared library to the package library directory used by the loader.
2. Install the control file and SQL install script expected by your target runtime.

`build.zig` contains install-file wiring for the native extension directory and installs [`sql/sqlite_mindbrain--1.0.0.sql`](../sql/sqlite_mindbrain--1.0.0.sql). The standalone SQLite API does not require PostgreSQL extension installation; use [standalone.md](standalone.md) and [api-reference.md](api-reference.md) for that path.

## Native dependencies

The native surface expects the bitmap implementation to be available. If you use vector columns, enable `vector` as well in the environment that hosts the native runtime.

## Smoke check

See [test/smoke_install.sql](../test/smoke_install.sql) for a minimal install verification pattern.

## Standalone SQLite components

For the SQLite tool and tests you need **libsqlite3** at link time. See [standalone.md](standalone.md).
