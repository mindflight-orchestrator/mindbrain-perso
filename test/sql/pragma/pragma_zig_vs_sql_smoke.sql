-- Smoke / benchmark harness: compare row counts from SQL vs Zig rank/hops.
-- Requires test data (e.g. test_pg_pragma.sql) and a built libpg_mindbrain with pragma_rank_zig.

DO $$
DECLARE
  c_sql int;
  c_zig int;
BEGIN
  SELECT count(*)::int INTO c_sql
  FROM pragma_rank_native('user_test', 'offline', ARRAY['canonical','proposition']::text[], 10);
  SELECT count(*)::int INTO c_zig
  FROM pragma_rank_zig('user_test', 'offline', ARRAY['canonical','proposition']::text[], 10);
  RAISE NOTICE 'pragma_rank: sql rows=% zig rows=%', c_sql, c_zig;
END $$;

-- Document timing via: EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM pragma_rank_zig(...);
-- and scripts/explain-pragma-ontology-hot-paths.sql in the repo root (if present).
