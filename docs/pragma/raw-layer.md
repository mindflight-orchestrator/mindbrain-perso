# Pragma Raw Layer

Pragma is a retrieval layer. It should not be treated as the only durable copy
of important business facts unless an application explicitly chooses to use
projections as its write surface.

## Source Of Truth Boundary

| Data | Canonical layer | Pragma role |
|------|-----------------|-------------|
| Imported document text | `documents_raw`, `chunks_raw` | Project compact facts or notes for pack. |
| Facet decisions | `facet_assignments_raw` and derived facet indexes | Add scoped retrieval hints. |
| Graph facts | `entities_raw`, `relations_raw`, `graph_entity`, `graph_relation` | Summarize relevant relationships. |
| Ontology definitions | `ontology_*` tables | Project taxonomy/schema context when useful. |
| Agent working memory | `projections`, `memory_projections` | Direct retrieval material. |

`memory_projections` rows are already derived views. `projections` rows are
also derived unless an application deliberately writes them as the authoritative
state of an agent workflow.

## Three Meanings Of Projection

The repository uses the word projection in several contexts.

| Meaning | Where | Description |
|---------|-------|-------------|
| Agent projection rows | `projections` | Durable facts/goals/steps/constraints for an agent. |
| Memory projection rows | `memory_projections` | Legacy memory views typed as `canonical`, `proposition`, or `raw`. |
| Raw-to-derived materialization | reindex and graph/facet code | Rebuilding derived indexes from raw rows. |

Keep these separate when debugging. `ghostcrab_projection_get` style output is
also different from working-memory pack rows: it returns a materialized graph
projection bundle, not the same row family as `/ghostcrab/pack-projections`.

## Grounding

Prefer grounded projection rows:

| Field | Grounding example |
|-------|-------------------|
| `source_ref` | Document id, chunk id, graph relation id, taxonomy row id. |
| `source_type` | `document`, `chunk`, `graph_relation`, `taxonomy`, `user_request`. |
| `scope` | Workspace id, collection id, `player:<id>`, domain, project, or entity key. |

Ungrounded projections can be useful for planning or temporary context, but
they should usually have lower weight or explicit uncertainty in `content`.

## Raw Backup Implication

For backup and restore, preserve:

- the raw source rows that justify projections;
- `projections` if agent state and audit history matter;
- `memory_items`, `memory_projections`, and `memory_edges` if legacy memory pack
  behavior must round-trip.

Derived search, facet, and graph indexes can be rebuilt, but projections are not
automatically regenerated unless the application has an explicit materializer.
