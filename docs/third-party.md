# Third-party components

MindBrain links or vendors the following projects. Full upstream trees and licenses live under **[deps/](../deps/)**; this file is a short pointer for readers of the documentation.

## CRoaring / roaringbitmap

- **Roaring Bitmap** C implementation and sources are vendored in the repository.
- Zig modules compile **`roaring.c`** from that tree (see `configureCroaring` in [build.zig](../build.zig)).
- The native surface declares `roaringbitmap` as a dependency in the control file.

Upstream references (verify versions in `deps/`):

- [CRoaring](https://github.com/RoaringBitmap/CRoaring)
- roaringbitmap upstream

## ztoon

- [deps/ztoon](../deps/ztoon) — used by standalone TOON serialization / exports (imported from [build.zig](../build.zig) into standalone modules).

## SQLite

- **libsqlite3** — system library for standalone tests, benchmarks, and `mindbrain-standalone-tool` (not bundled as source in this repo).
