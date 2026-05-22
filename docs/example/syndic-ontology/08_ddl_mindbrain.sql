-- ============================================================
-- MINDBRAIN DDL — Syndic Ontology with Versioning
-- ============================================================
-- Modele :
--   * Chaque import/crawl produit un snapshot.
--   * Les entites et relations sont immuables par snapshot.
--   * Les changements creent de nouvelles lignes, jamais d'UPDATE destructif.
--   * Les user stories sont des projections requetables par agent.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS syndic_ontology;
SET search_path TO syndic_ontology, public;

-- ============================================================
-- 1. SNAPSHOTS
-- ============================================================

CREATE TABLE snapshot (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    captured_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_ref      TEXT        NOT NULL,
    capture_agent   TEXT,
    label           TEXT,
    notes           TEXT,
    is_baseline     BOOLEAN     NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- 2. DOMAIN ENTITIES
-- ============================================================

CREATE TABLE domain_entity (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id     UUID        NOT NULL REFERENCES snapshot(id) ON DELETE CASCADE,
    logical_id      TEXT        NOT NULL,
    entity_type     TEXT        NOT NULL,
    label           TEXT,
    attributes      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    content_hash    TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (snapshot_id, logical_id)
);

CREATE INDEX idx_domain_entity_snapshot ON domain_entity(snapshot_id);
CREATE INDEX idx_domain_entity_type     ON domain_entity(entity_type);
CREATE INDEX idx_domain_entity_logical  ON domain_entity(logical_id);

-- ============================================================
-- 3. DOMAIN RELATIONS
-- ============================================================

CREATE TABLE domain_relation (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id     UUID        NOT NULL REFERENCES snapshot(id) ON DELETE CASCADE,
    logical_id      TEXT        NOT NULL,
    relation_type   TEXT        NOT NULL,
    source_logical_id TEXT      NOT NULL,
    target_logical_id TEXT      NOT NULL,
    valid_from      DATE,
    valid_to        DATE,
    legal_effective_from DATE,
    legal_effective_to   DATE,
    billing_effective_from DATE,
    billing_effective_to   DATE,
    status          TEXT,
    properties      JSONB      NOT NULL DEFAULT '{}'::jsonb,
    content_hash    TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (snapshot_id, logical_id)
);

CREATE INDEX idx_domain_relation_snapshot ON domain_relation(snapshot_id);
CREATE INDEX idx_domain_relation_type     ON domain_relation(relation_type);
CREATE INDEX idx_domain_relation_source   ON domain_relation(source_logical_id);
CREATE INDEX idx_domain_relation_target   ON domain_relation(target_logical_id);
CREATE INDEX idx_domain_relation_validity ON domain_relation(valid_from, valid_to);

-- ============================================================
-- 4. SCENARIOS AND ACTION BRANCHES
-- ============================================================

CREATE TABLE scenario (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id     UUID        NOT NULL REFERENCES snapshot(id) ON DELETE CASCADE,
    logical_id      TEXT        NOT NULL,
    scenario_type   TEXT        NOT NULL,
    title           TEXT        NOT NULL,
    domain          TEXT        NOT NULL DEFAULT 'syndic',
    raw_yaml        JSONB,
    content_hash    TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (snapshot_id, logical_id)
);

CREATE TABLE scenario_action (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id     UUID        NOT NULL REFERENCES snapshot(id) ON DELETE CASCADE,
    scenario_id     UUID        NOT NULL REFERENCES scenario(id) ON DELETE CASCADE,
    logical_id      TEXT        NOT NULL,
    label           TEXT,
    method          TEXT,
    checks          TEXT[]      NOT NULL DEFAULT '{}',
    content_hash    TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (snapshot_id, scenario_id, logical_id)
);

CREATE TABLE branch (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id     UUID        NOT NULL REFERENCES snapshot(id) ON DELETE CASCADE,
    action_id       UUID        NOT NULL REFERENCES scenario_action(id) ON DELETE CASCADE,
    condition       TEXT        NOT NULL,
    next_scenario_logical_id TEXT,
    side_effect     TEXT,
    side_effects    TEXT[]      NOT NULL DEFAULT '{}',
    is_terminal     BOOLEAN     NOT NULL DEFAULT false,
    content_hash    TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_scenario_snapshot ON scenario(snapshot_id);
CREATE INDEX idx_action_scenario   ON scenario_action(scenario_id);
CREATE INDEX idx_branch_action     ON branch(action_id);
CREATE INDEX idx_branch_condition  ON branch(condition);

-- ============================================================
-- 5. USER STORIES
-- ============================================================

CREATE TABLE user_story (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id     UUID        NOT NULL REFERENCES snapshot(id) ON DELETE CASCADE,
    logical_id      TEXT        NOT NULL,
    name            TEXT        NOT NULL,
    actor           TEXT,
    goal            TEXT        NOT NULL,
    story_type      TEXT        NOT NULL,
    intent_keywords TEXT[]      NOT NULL DEFAULT '{}',
    summary         TEXT,
    entry_scenario_logical_id TEXT,
    terminal_scenario_logical_id TEXT,
    step_count      INTEGER,
    has_branches    BOOLEAN     NOT NULL DEFAULT false,
    content_hash    TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (snapshot_id, logical_id)
);

CREATE TABLE story_step (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id     UUID        NOT NULL REFERENCES snapshot(id) ON DELETE CASCADE,
    story_id        UUID        NOT NULL REFERENCES user_story(id) ON DELETE CASCADE,
    step_number     INTEGER     NOT NULL,
    scenario_logical_id TEXT,
    action_logical_id TEXT,
    branch_condition TEXT,
    node_logical_id TEXT,
    relation_type TEXT,
    note            TEXT,
    side_effects    TEXT[]      NOT NULL DEFAULT '{}',
    UNIQUE (story_id, step_number)
);

CREATE INDEX idx_story_keywords ON user_story USING GIN(intent_keywords);
CREATE INDEX idx_story_type     ON user_story(story_type);
CREATE INDEX idx_step_story     ON story_step(story_id);
CREATE INDEX idx_step_scenario  ON story_step(scenario_logical_id);

-- ============================================================
-- 6. DIFFS
-- ============================================================

CREATE TYPE diff_change_type AS ENUM ('added', 'removed', 'modified');

CREATE TABLE snapshot_diff (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_from   UUID        NOT NULL REFERENCES snapshot(id),
    snapshot_to     UUID        NOT NULL REFERENCES snapshot(id),
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    entities_added  INTEGER     NOT NULL DEFAULT 0,
    entities_removed INTEGER    NOT NULL DEFAULT 0,
    entities_changed INTEGER    NOT NULL DEFAULT 0,
    relations_added INTEGER     NOT NULL DEFAULT 0,
    relations_removed INTEGER   NOT NULL DEFAULT 0,
    relations_changed INTEGER   NOT NULL DEFAULT 0,
    stories_added   INTEGER     NOT NULL DEFAULT 0,
    stories_removed INTEGER     NOT NULL DEFAULT 0,
    stories_changed INTEGER     NOT NULL DEFAULT 0,
    diff_summary    TEXT,
    diff_json       JSONB,
    CHECK (snapshot_from <> snapshot_to)
);

CREATE TABLE diff_item (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    diff_id         UUID        NOT NULL REFERENCES snapshot_diff(id) ON DELETE CASCADE,
    entity_kind     TEXT        NOT NULL,
    logical_id      TEXT        NOT NULL,
    change_type     diff_change_type NOT NULL,
    changed_properties TEXT[],
    old_value       JSONB,
    new_value       JSONB,
    impact_note     TEXT,
    breaking_change BOOLEAN     NOT NULL DEFAULT false
);

-- ============================================================
-- 7. AGENT PROJECTIONS
-- ============================================================

CREATE OR REPLACE FUNCTION latest_snapshot()
RETURNS UUID LANGUAGE SQL STABLE AS $$
    SELECT id FROM snapshot ORDER BY captured_at DESC LIMIT 1;
$$;

CREATE OR REPLACE VIEW v_active_occupancy AS
SELECT
    r.snapshot_id,
    r.source_logical_id AS household_id,
    r.target_logical_id AS lot_id,
    r.valid_from,
    r.valid_to,
    r.status
FROM domain_relation r
WHERE r.relation_type = 'OCCUPE'
  AND r.valid_to IS NULL;

CREATE OR REPLACE VIEW v_current_billing_groups AS
SELECT
    r.snapshot_id,
    r.source_logical_id AS billing_group_version_id,
    r.target_logical_id AS lot_id,
    r.properties->>'billed_household' AS billed_household,
    r.valid_from,
    r.valid_to
FROM domain_relation r
WHERE r.relation_type = 'FACTURABLE_POUR'
  AND r.valid_to IS NULL;

CREATE OR REPLACE VIEW v_owner_group AS
SELECT
    r.snapshot_id,
    r.source_logical_id AS owner_group_version_id,
    r.target_logical_id AS owned_entity_id,
    r.properties->>'share_bp' AS share_bp,
    r.status
FROM domain_relation r
WHERE r.relation_type = 'POSSEDE'
  AND r.status = 'confirme';

CREATE OR REPLACE FUNCTION find_story(VARIADIC keywords TEXT[])
RETURNS TABLE (
    story_id    UUID,
    logical_id  TEXT,
    name        TEXT,
    story_type  TEXT,
    summary     TEXT,
    match_score INTEGER,
    snapshot_id UUID
) LANGUAGE SQL STABLE AS $$
    SELECT
        us.id,
        us.logical_id,
        us.name,
        us.story_type,
        us.summary,
        (SELECT COUNT(*)::INTEGER
         FROM unnest(keywords) kw
         WHERE us.intent_keywords @> ARRAY[kw]) AS match_score,
        us.snapshot_id
    FROM user_story us
    WHERE us.snapshot_id = latest_snapshot()
      AND us.intent_keywords && keywords
    ORDER BY match_score DESC, us.story_type;
$$;
