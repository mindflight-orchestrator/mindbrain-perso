# Pragma Example: `immeuble-demo`

This page records the current pragma/projection state of
`data/immeuble-demo.sqlite` in this checkout.

The database has projection type policy installed, but no active memory or
durable projection rows.

## Table Counts

```text
table_name          rows
------------------  ----
projections         0
projection_types    5
agent_state         0
memory_items        0
memory_projections  0
memory_edges        0
```

## Projection Type Policy

```text
type_name   compatibility_aliases        rank_bias  pack_priority  next_hop_multiplier  structured
----------  ---------------------------  ---------  -------------  -------------------  ----------
FACT        ["canonical","proposition"]  1.3        1              1.2                  1
GOAL        ["canonical","proposition"]  1.2        1              1.1                  1
CONSTRAINT  ["proposition"]              1.0        2              1.0                  1
STEP        ["proposition"]              0.9        2              0.9                  1
NOTE        ["raw"]                      0.7        3              0.6                  0
```

## What This Means

- `mindbrain-standalone-tool pack` has no rows to return for this demo unless
  memory rows are inserted.
- `/api/mindbrain/ghostcrab/pack-projections` has no durable projections to
  return unless `projections` rows are inserted.
- The type policy is ready for both legacy memory aliases and semantic
  projection types.

## Reproduce

```bash
sqlite3 -header -column data/immeuble-demo.sqlite "
SELECT 'projections' AS table_name, COUNT(*) AS rows FROM projections
UNION ALL SELECT 'projection_types', COUNT(*) FROM projection_types
UNION ALL SELECT 'agent_state', COUNT(*) FROM agent_state
UNION ALL SELECT 'memory_items', COUNT(*) FROM memory_items
UNION ALL SELECT 'memory_projections', COUNT(*) FROM memory_projections
UNION ALL SELECT 'memory_edges', COUNT(*) FROM memory_edges;
"
```

```bash
sqlite3 -header -column data/immeuble-demo.sqlite "
SELECT type_name, compatibility_aliases, rank_bias, pack_priority,
       next_hop_multiplier, structured
FROM projection_types
ORDER BY pack_priority, type_name;
"
```
