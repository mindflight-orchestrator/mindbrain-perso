# MindBrain

SQLite-first knowledge runtime and native library for **faceted search** (Roaring Bitmaps + **BM25**), **graph traversal**, **memory projection** helpers, workspace metadata, document collections, and a small local HTTP/CLI API. The implementation is primarily **Zig**.

## Documentation

All user-facing documentation lives under **[docs/](docs/README.md)**:

- [docs/overview.md](docs/overview.md) — architecture and scope  
- [docs/installation.md](docs/installation.md) — build, native install notes, and standalone runtime prerequisites
- [docs/api-reference.md](docs/api-reference.md) — current HTTP routes and CLI commands
- [docs/facets.md](docs/facets.md) · [docs/graph.md](docs/graph.md) · [docs/pragma.md](docs/pragma.md)  
- [docs/workspace.md](docs/workspace.md) · [docs/standalone.md](docs/standalone.md) · [docs/demo-benchmark.md](docs/demo-benchmark.md)  
- [docs/native-reference.md](docs/native-reference.md) · [docs/third-party.md](docs/third-party.md)

## Quick build

This project requires **Zig 0.16.0** exactly (`zig version` → `0.16.0`). `build.zig` fails the build on any other version.

```bash
zig build
zig build standalone-tool   # optional: SQLite CLI
```

Requires PostgreSQL server headers for the native shared library and **SQLite** for standalone targets. Use Zig **0.16.0** explicitly if `zig` on `PATH` points to an older toolchain.

## Runtime metadata

- Standalone CLI: [src/standalone/tool.zig](src/standalone/tool.zig)
- Standalone HTTP server: [src/standalone/http_server.zig](src/standalone/http_server.zig)
- SQLite SQL install script: [sql/sqlite_mindbrain--1.0.0.sql](sql/sqlite_mindbrain--1.0.0.sql)
- Native shared library build target: `pg_mindbrain` in [build.zig](build.zig)

## Public GitHub mirror

The canonical OSS remote for consumers (including the `ghostcrab-personal-mcp` submodule) is **[github.com/mindflight-orchestrator/mindbrain](https://github.com/mindflight-orchestrator/mindbrain)**. After authenticating (`gh auth login` or SSH keys), publish the mirror from an existing checkout:

```bash
git remote add github git@github.com:mindflight-orchestrator/mindbrain.git   # once
git push -u github main
```

Adjust org/repo if your fork lives elsewhere; update `.gitmodules` in downstream repos accordingly.
