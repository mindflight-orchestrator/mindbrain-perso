# Installation

## Prerequisites

- **Zig** — [build.zig](../build.zig) requires **exactly 0.16.0** (`zig version` must report `0.16.0`).
- **Native headers** — required only when building the native surface.
- **Bitmap dependency:** **`roaringbitmap`** must be available for the native surface; the control file declares `requires = 'roaringbitmap'`.
- **Optional:** **`vector`** — referenced in [docker/init/01-init-postgres.sql](../docker/init/01-init-postgres.sql) for embeddings on `facets` and similar objects.

## Build the shared library

From the repository root:

```bash
zig build
```

This produces the shared library under `zig-out/lib/` (name may vary by platform).

The build links the vendored CRoaring-backed roaring implementation used by Zig.

## Install the native surface

1. Copy the shared library to the package library directory used by the loader.
2. Install the control file and SQL install script into the runtime directory used by the loader.

`build.zig` can install the control and SQL files when the native directory is available; adjust paths if you use a custom prefix (see comments in [build.zig](../build.zig) for common macOS/Linux include paths).

## Native dependencies

The native surface expects `roaringbitmap` to be available. If you use vector columns, enable `vector` as well in the environment that hosts the native runtime.

## Smoke check

See [test/smoke_install.sql](../test/smoke_install.sql) for a minimal install verification pattern.

## Standalone SQLite components

For the SQLite tool and tests you need **libsqlite3** at link time. See [standalone.md](standalone.md).
