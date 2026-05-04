# Optimize `flushStagedWrites`

This plan targets the IMDb benchmark path in `src/benchmark/imdb_import.zig`, where the end-to-end runtime is dominated by the final flush and post-flush index creation.

## Goal

Reduce the time spent after TSV parsing by:

1. eliminating avoidable final-table work,
2. shrinking the amount of duplicate staging data,
3. making the flush path more linear and less sort-heavy,
4. preserving benchmark correctness and final database usability.

## Mandatory Requirement

The staging design must be changed so the importer does **not** emit so many duplicate entity rows.

This is not optional. It is the primary structural bottleneck.

Today the importer records one staging write per encounter, then relies on `flushStagedWrites` to group and collapse the duplicates later. That is wasteful at IMDb scale:

- `entity_rows` becomes far larger than the final unique entity count
- the final flush has to group enormous temporary tables
- the flush work becomes the dominant runtime component

The redesign must reduce duplicate entity staging at the source.

## Current Bottlenecks

### 1. Duplicate entity staging

Relevant code paths:

- `handleTitleBasicsRow`
- `handleNameBasicsRow`
- `handleTitleRatingsRow`
- `handleTitleAkasRow`
- `handleTitleEpisodeRow`
- `handleTitleCrewRow`
- `handleTitlePrincipalsRow`
- `insertCrewList`

Each of these paths can call `upsertEntity` repeatedly for the same logical entity across multiple files. The importer is effectively generating a large duplicate working set.

### 2. Large final aggregation

`flushStagedWrites` currently:

- groups `temp_imdb_entity_stage` by `entity_id`
- groups `temp_imdb_alias_stage` by `(term, entity_id)`
- inserts `temp_imdb_relation_stage` with an explicit `ORDER BY relation_id`
- builds multiple indexes after the bulk insert

This is expensive because the temp tables are already huge before the flush begins.

### 3. Redundant index creation

The flush path currently creates unique indexes on columns that are already primary keys:

- `graph_entity(entity_id)`
- `graph_relation(relation_id)`

Those indexes are redundant and should not be rebuilt during import.

## Optimization Plan

### Phase 1: Remove avoidable flush work

1. Remove redundant unique indexes from `flushStagedWrites`.
2. Remove the `ORDER BY relation_id` from the relation flush.
3. Keep only the indexes that are required for the immediate post-import query contract.
4. Make benchmark-only index creation explicit so it is easy to measure separately.

Expected result:

- less B-tree work during finalization
- less sort overhead
- clearer separation between import and query-ready setup

### Phase 2: Redesign staging to avoid duplicate entity rows

This is the crucial change.

The importer should no longer treat entity staging as a log of every encounter. It should stage each logical IMDb entity once, then update its confidence and metadata as later rows are processed.

Possible approaches, in increasing order of invasiveness:

1. **Temp table with primary key / unique constraint**
   - Make the entity staging table keyed by `entity_id`.
   - Use `INSERT ... ON CONFLICT(entity_id) DO UPDATE` or equivalent merge logic.
   - Keep the strongest confidence and the best canonical name in place.
   - This immediately collapses duplicate entity rows before flush.

2. **Accumulator table with merge semantics**
   - Replace the current staging table with a true accumulator.
   - Track one row per entity and update it in place as files are processed.
   - This avoids the later `GROUP BY` entirely or reduces it to a trivial pass.

3. **File-aware entity ownership**
   - Let `title.basics.tsv` and `name.basics.tsv` own entity creation.
   - Let later files update metadata, confidence, aliases, and relations, but not create duplicate entity rows unless a new entity is genuinely discovered.
   - This reduces repeated writes from `title.akas`, `title.episode`, `title.crew`, `title.principals`, and `title.ratings`.

Recommended direction:

- Make the temp entity stage unique by `entity_id`.
- Merge updates into that row as later files contribute new evidence.
- Keep aliases and relations staged separately.

Expected result:

- much smaller temp entity table
- less work in `flushStagedWrites`
- lower peak memory and temp-storage pressure
- better scalability on full IMDb dumps

### Phase 3: Reduce relation flush overhead

1. Reconsider whether relations need to be staged as raw inserts with a full final sort.
2. If relation ids are already monotonic, preserve insertion order without a sorting step.
3. If deduplication is not needed for relations, avoid `GROUP BY`/`ORDER BY` patterns that force SQLite to materialize large intermediates.

### Phase 4: Add benchmark-specific measurement

1. Measure parse/stage time separately from flush/index time.
2. Emit a breakdown for:
   - entity staging
   - alias staging
   - relation staging
   - flush materialization
   - index creation
3. Record the improvement after each phase so regressions are visible.

## Acceptance Criteria

The optimization work is complete when all of the following are true:

1. The importer stages far fewer duplicate entity rows than it does today.
2. `flushStagedWrites` no longer spends most of its time collapsing avoidable duplicates.
3. Redundant primary-key indexes are not rebuilt during import.
4. Relation flushing no longer performs unnecessary large sorts.
5. The benchmark still produces the same final graph semantics.
6. The resulting SQLite database remains queryable and consistent after import.

## Suggested Implementation Order

1. Redesign entity staging so each logical IMDb entity is represented once.
2. Remove redundant index creation from the final flush.
3. Remove the relation `ORDER BY` if it is not required for correctness.
4. Add timing breakdowns for flush and index creation.
5. Measure the full benchmark again and compare against the current baseline.

## Notes

- This plan is intentionally scoped to the IMDb benchmark path.
- The same staging strategy may later be reused for YAGO or other bulk importers.
- Do not optimize only the SQL flush without addressing duplicate entity staging; that would leave the largest cost in place.
