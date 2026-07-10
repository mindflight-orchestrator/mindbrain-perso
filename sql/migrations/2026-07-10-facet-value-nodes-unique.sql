-- Migration: enforce the (table_id, facet_id, facet_value) natural key on
-- facet_value_nodes.
--
-- Every reader and writer (ontology taxonomy import, facet hierarchy lookups)
-- already assumes this triple is unique and resolves value ids by looking it
-- up, but the schema only declared PRIMARY KEY(table_id, value_id), so those
-- lookups were unindexed full scans and duplicates were representable.
--
-- Guarded against pre-existing duplicates: keep the earliest row per natural
-- key before creating the unique index, so the index creation cannot fail on
-- legacy databases. Duplicate rows are dead data — lookups always resolved the
-- first match, so later duplicates were never returned deterministically.
--
-- Idempotent: the DELETE matches nothing once duplicates are gone and the
-- index is created with IF NOT EXISTS. The runtime applies the same
-- statements from the canonical schema (sql/sqlite_mindbrain--1.0.0.sql).

BEGIN IMMEDIATE;

DELETE FROM facet_value_nodes
WHERE rowid NOT IN (
    SELECT MIN(rowid)
    FROM facet_value_nodes
    GROUP BY table_id, facet_id, facet_value
);

CREATE UNIQUE INDEX IF NOT EXISTS facet_value_nodes_value_uidx
    ON facet_value_nodes(table_id, facet_id, facet_value);

COMMIT;
