# Graph Bitmap Width Strategy

The standalone SQLite runtime stores graph adjacency and search postings as
CRoaring 32-bit bitmaps. In code, `src/standalone/roaring.zig` defines
`DenseId = u32`, and graph adjacency tables persist portable CRoaring blobs in
`graph_lj_out` and `graph_lj_in`.

## Runtime Mode

The HTTP runtime accepts:

```text
MINDBRAIN_GRAPH_BITMAP_MODE=dense32
```

Allowed values are:

| Mode | Effective behavior |
| --- | --- |
| `dense32` | Current production path. Store dense `u32` graph/search identifiers in CRoaring bitmaps. |
| `auto` | Accepted as an operator intent, but currently resolves to `dense32`. |
| `direct64` | Accepted for diagnostics only and reported as unsupported. |

`GET /api/mindbrain/capabilities` reports:

- `bitmap_mode_configured`
- `bitmap_mode_effective`
- `direct64_supported`
- `bitmap_element_domain`

## Why Direct64 Is Separate

Switching to 64-bit bitmap elements is not a runtime boolean in SQLite. It would
require a separate wrapper around CRoaring 64-bit APIs, explicit blob format
versioning, and separate graph/search sidecars. The current tables and helper
APIs assume `u32` values.

Keep `dense32` as the default until benchmarks show that a 64-bit sidecar is
needed for a concrete dataset.
