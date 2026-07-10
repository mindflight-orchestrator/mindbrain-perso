-- Migration: index the workspace-export and coverage hot paths.
--
-- exportWorkspaceModel runs three lookups per table (column_semantics by
-- table_id, relation_semantics by source/target_table_id, source_mappings by
-- target_table_id); none of these columns were indexed, so the export was
-- O(tables x total_rows). Coverage additionally counts unscoped projections
-- (scope IS NULL), which the partial idx_proj_scope (scope IS NOT NULL)
-- cannot serve.
--
-- Idempotent: plain CREATE INDEX IF NOT EXISTS. The runtime applies the same
-- statements from the canonical schema (sql/sqlite_mindbrain--1.0.0.sql).

CREATE INDEX IF NOT EXISTS column_semantics_table_id_idx
    ON column_semantics(table_id);

CREATE INDEX IF NOT EXISTS relation_semantics_source_table_idx
    ON relation_semantics(source_table_id);

CREATE INDEX IF NOT EXISTS relation_semantics_target_table_idx
    ON relation_semantics(target_table_id);

CREATE INDEX IF NOT EXISTS source_mappings_target_table_idx
    ON source_mappings(target_table_id);

CREATE INDEX IF NOT EXISTS idx_proj_scope_null
    ON projections(scope) WHERE scope IS NULL;
