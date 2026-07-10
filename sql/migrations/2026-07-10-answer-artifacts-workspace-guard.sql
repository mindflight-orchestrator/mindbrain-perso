-- Migration: pre-flight guard for the answer-artifacts workspace-strict
-- rebuild (2026-06-16-answer-artifacts-workspace-strict.sql).
--
-- The 2026-06-16 guard only inspects artifact_kind = 'analysis_plan', but its
-- backfill COALESCEs workspace_id from scope for rows of EVERY kind. A
-- live_answer_view / answer_snapshot / evidence_pack row with workspace_id
-- NULL and an unresolvable scope therefore fails the rebuild mid-transaction
-- with a bare "NOT NULL constraint failed" instead of a clear guard error.
--
-- Run this file BEFORE the 2026-06-16 strict migration on legacy databases
-- (or at any later point: it is a no-op once workspace_id is NOT NULL for all
-- rows). It intentionally checks >= 1 matching workspace rather than == 1:
-- the backfill resolves nested workspaces by longest prefix, so multiple
-- matches are fine — only zero matches would break the rebuild.
--
-- Idempotent and read-only apart from a TEMP guard table.

CREATE TEMP TABLE IF NOT EXISTS answer_artifact_workspace_guard_all_kinds (
    must_be_zero INTEGER NOT NULL CHECK (must_be_zero = 0)
);

INSERT INTO answer_artifact_workspace_guard_all_kinds(must_be_zero)
SELECT 1
WHERE EXISTS (
    SELECT 1
    FROM mindbrain_answer_artifacts a
    WHERE a.workspace_id IS NULL
      AND (
          a.scope IS NULL
          OR NOT EXISTS (
              SELECT 1
              FROM workspaces w
              WHERE a.scope = w.workspace_id
                 OR a.scope LIKE w.workspace_id || ':%'
          )
      )
);

DROP TABLE answer_artifact_workspace_guard_all_kinds;
