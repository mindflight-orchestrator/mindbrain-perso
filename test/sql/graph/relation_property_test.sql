\echo '=============================================='
\echo 'Relation Property Test Suite'
\echo '=============================================='
-- Scope: mb_collections.relation_properties_raw → graph.relation_property
-- Typed edge properties: text, number, percentage_bp, money_minor, date_ts,
--                        doc_ref, uri.
-- Temporal ownership: Contact --[POSSEDE]--> Bien with valid_from / valid_to.
-- Co-ownership: two contacts sharing one bien via share_bp summing to 10 000 bp.
-- BFS property filter: compute_property_allowed + bfs_hop 8th argument.
--
-- All assertions follow the RAISE EXCEPTION 'FAIL: ...' / RAISE NOTICE 'PASS: ...'
-- convention used throughout test/sql/graph/.
--
-- Pre-requisites:
--   CREATE EXTENSION IF NOT EXISTS roaringbitmap;
--   CREATE EXTENSION IF NOT EXISTS pg_mindbrain;
--
-- Tables expected to exist:
--   graph.relation_property   (derived / globally indexed)
--   mb_collections.relation_properties_raw  (workspace-scoped raw layer)
--
-- Functions expected to exist:
--   graph.upsert_relation_property(relation_id, property_key, value_type,
--       value_text, value_number, value_integer, ref_doc_id, currency)
--   graph.compute_property_allowed(filter_json jsonb, property_key text)
--       RETURNS roaringbitmap
--   graph.bfs_hop($1 int4[], $2 roaringbitmap, $3 text[],
--                 $4 real, $5 real, $6 date, $7 date,
--                 $8 roaringbitmap)   <-- $8 is p_property_allowed (new)
--
-- ============================================================================
-- Fixture setup
-- ============================================================================

DROP SCHEMA IF EXISTS relprop_test_docs CASCADE;
CREATE SCHEMA relprop_test_docs;

-- Minimal documents table for entity_document links (blocks 3 and 6)
CREATE TABLE relprop_test_docs.contracts (
    id      BIGINT PRIMARY KEY,
    title   TEXT NOT NULL
);
INSERT INTO relprop_test_docs.contracts (id, title) VALUES
    (1001, 'Acte de vente Maison 2020'),
    (1002, 'Acte de vente Maison 2024');

-- Wipe graph state (idempotent fixture)
DELETE FROM graph.entity_document;
DELETE FROM graph.relation_property;
DELETE FROM mb_collections.relation_properties_raw
    WHERE workspace_id = 'ws_relprop';
DELETE FROM graph.relation;
DELETE FROM graph.entity_alias;
DELETE FROM graph.lj_out;
DELETE FROM graph.lj_in;
DELETE FROM graph.entity;

-- ============================================================================
-- Block 1 — Schema: tables and indexes present
-- ============================================================================

DO $$
DECLARE
    v_table_count int;
    v_index_count int;
BEGIN
    SELECT COUNT(*) INTO v_table_count
    FROM information_schema.tables
    WHERE (table_schema = 'graph'          AND table_name = 'relation_property')
       OR (table_schema = 'mb_collections' AND table_name = 'relation_properties_raw');

    IF v_table_count != 2 THEN
        RAISE EXCEPTION 'FAIL: Expected 2 relation-property tables, found %', v_table_count;
    END IF;

    -- 5 partial indexes: relprop_raw_rel_idx (on raw table) +
    --   relprop_key_text_idx, relprop_key_int_idx, relprop_key_num_idx,
    --   relprop_doc_ref_idx (on derived table)
    SELECT COUNT(*) INTO v_index_count
    FROM pg_indexes
    WHERE indexname LIKE 'relprop_%';

    IF v_index_count < 5 THEN
        RAISE EXCEPTION 'FAIL: Expected at least 5 relprop_* indexes, found %', v_index_count;
    END IF;

    RAISE NOTICE 'PASS: relation-property schema and indexes present';
END;
$$;

-- ============================================================================
-- Block 2 — CHECK constraints
-- ============================================================================

DO $$
DECLARE
    v_alice  bigint;
    v_maison bigint;
    v_rel    bigint;
BEGIN
    v_alice  := graph.upsert_entity('contact', 'Alice_chk', 1.0, '{}');
    v_maison := graph.upsert_entity('bien',    'Maison_chk', 1.0, '{}');
    v_rel    := graph.upsert_relation('POSSEDE', v_alice, v_maison, 1.0,
                    NULL, NULL, NULL, NULL, NULL, NULL);

    -- money_minor without currency must be rejected
    BEGIN
        PERFORM graph.upsert_relation_property(
            v_rel, 'purchase_price', 'money_minor',
            NULL, NULL, 30000000, NULL, NULL);
        RAISE EXCEPTION 'FAIL: money_minor without currency should have been rejected';
    EXCEPTION WHEN check_violation OR raise_exception THEN
        NULL; -- expected
    END;

    -- text type with currency must be rejected
    BEGIN
        PERFORM graph.upsert_relation_property(
            v_rel, 'ownership_kind', 'text',
            'pleine_propriete', NULL, NULL, NULL, 'EUR');
        RAISE EXCEPTION 'FAIL: text with currency should have been rejected';
    EXCEPTION WHEN check_violation OR raise_exception THEN
        NULL; -- expected
    END;

    -- doc_ref without ref_doc_id must be rejected
    BEGIN
        PERFORM graph.upsert_relation_property(
            v_rel, 'contract_doc', 'doc_ref',
            NULL, NULL, NULL, NULL, NULL);
        RAISE EXCEPTION 'FAIL: doc_ref without ref_doc_id should have been rejected';
    EXCEPTION WHEN check_violation OR raise_exception THEN
        NULL; -- expected
    END;

    -- non-doc_ref with ref_doc_id must be rejected
    BEGIN
        PERFORM graph.upsert_relation_property(
            v_rel, 'ownership_kind', 'text',
            'pleine_propriete', NULL, NULL, 42, NULL);
        RAISE EXCEPTION 'FAIL: non-doc_ref with ref_doc_id should have been rejected';
    EXCEPTION WHEN check_violation OR raise_exception THEN
        NULL; -- expected
    END;

    RAISE NOTICE 'PASS: CHECK constraints correctly reject invalid property rows';
END;
$$;

-- ============================================================================
-- Block 3 — CRUD: insert 6 value types and verify typed columns
-- ============================================================================

DO $$
DECLARE
    v_alice   bigint;
    v_maison  bigint;
    v_rel     bigint;
    v_text    text;
    v_num     double precision;
    v_int     bigint;
    v_doc_id  bigint;
    v_currency text;
BEGIN
    v_alice  := graph.upsert_entity('contact', 'Alice_crud', 1.0, '{}');
    v_maison := graph.upsert_entity('bien',    'Maison_crud', 1.0, '{}');
    v_rel    := graph.upsert_relation('POSSEDE', v_alice, v_maison, 1.0,
                    NULL, NULL, NULL, NULL, NULL, NULL);

    -- 1. text
    PERFORM graph.upsert_relation_property(
        v_rel, 'ownership_kind', 'text',
        'pleine_propriete', NULL, NULL, NULL, NULL);

    -- 2. number
    PERFORM graph.upsert_relation_property(
        v_rel, 'confidence_score', 'number',
        NULL, 0.95, NULL, NULL, NULL);

    -- 3. percentage_bp (10 000 bp = 100 %)
    PERFORM graph.upsert_relation_property(
        v_rel, 'share_bp', 'percentage_bp',
        NULL, NULL, 10000, NULL, NULL);

    -- 4. money_minor (300 000 EUR = 30 000 000 cents)
    PERFORM graph.upsert_relation_property(
        v_rel, 'purchase_price', 'money_minor',
        NULL, NULL, 30000000, NULL, 'EUR');

    -- 5. date_ts (contract signed 2020-01-15)
    PERFORM graph.upsert_relation_property(
        v_rel, 'contract_signed_at', 'date_ts',
        '2020-01-15', NULL, NULL, NULL, NULL);

    -- 6. doc_ref (purchase contract document)
    PERFORM graph.upsert_relation_property(
        v_rel, 'contract_doc', 'doc_ref',
        NULL, NULL, NULL, 1001, NULL);

    -- 7. uri (land registry external link)
    PERFORM graph.upsert_relation_property(
        v_rel, 'registry_url', 'uri',
        'https://cadastre.fr/bien/12345', NULL, NULL, NULL, NULL);

    -- Verify text
    SELECT value_text INTO v_text
    FROM graph.relation_property
    WHERE relation_id = v_rel AND property_key = 'ownership_kind';
    IF v_text IS DISTINCT FROM 'pleine_propriete' THEN
        RAISE EXCEPTION 'FAIL: text value mismatch: %', v_text;
    END IF;

    -- Verify number (not NULL; unused integer columns are NULL)
    SELECT value_number, value_integer INTO v_num, v_int
    FROM graph.relation_property
    WHERE relation_id = v_rel AND property_key = 'confidence_score';
    IF v_num IS NULL OR v_num < 0.9 THEN
        RAISE EXCEPTION 'FAIL: number value mismatch: %', v_num;
    END IF;
    IF v_int IS NOT NULL THEN
        RAISE EXCEPTION 'FAIL: value_integer should be NULL for number type, got %', v_int;
    END IF;

    -- Verify percentage_bp
    SELECT value_integer INTO v_int
    FROM graph.relation_property
    WHERE relation_id = v_rel AND property_key = 'share_bp';
    IF v_int IS DISTINCT FROM 10000 THEN
        RAISE EXCEPTION 'FAIL: share_bp mismatch: %', v_int;
    END IF;

    -- Verify money_minor + currency
    SELECT value_integer, currency INTO v_int, v_currency
    FROM graph.relation_property
    WHERE relation_id = v_rel AND property_key = 'purchase_price';
    IF v_int IS DISTINCT FROM 30000000 THEN
        RAISE EXCEPTION 'FAIL: purchase_price minor-units mismatch: %', v_int;
    END IF;
    IF v_currency IS DISTINCT FROM 'EUR' THEN
        RAISE EXCEPTION 'FAIL: currency mismatch: %', v_currency;
    END IF;

    -- Verify doc_ref
    SELECT ref_doc_id INTO v_doc_id
    FROM graph.relation_property
    WHERE relation_id = v_rel AND property_key = 'contract_doc';
    IF v_doc_id IS DISTINCT FROM 1001 THEN
        RAISE EXCEPTION 'FAIL: doc_ref ref_doc_id mismatch: %', v_doc_id;
    END IF;

    -- Verify uri (stored as value_text)
    SELECT value_text INTO v_text
    FROM graph.relation_property
    WHERE relation_id = v_rel AND property_key = 'registry_url';
    IF v_text IS DISTINCT FROM 'https://cadastre.fr/bien/12345' THEN
        RAISE EXCEPTION 'FAIL: uri mismatch: %', v_text;
    END IF;

    RAISE NOTICE 'PASS: CRUD round-trip for all 6 value types (7 properties)';
END;
$$;

-- ============================================================================
-- Block 4 — Cascade delete: deleting the parent relation removes its properties
-- ============================================================================

DO $$
DECLARE
    v_src   bigint;
    v_tgt   bigint;
    v_rel   bigint;
    v_count bigint;
BEGIN
    v_src := graph.upsert_entity('contact', 'Alice_cascade', 1.0, '{}');
    v_tgt := graph.upsert_entity('bien',    'Maison_cascade', 1.0, '{}');
    v_rel := graph.upsert_relation('POSSEDE', v_src, v_tgt, 1.0,
                 NULL, NULL, NULL, NULL, NULL, NULL);

    PERFORM graph.upsert_relation_property(
        v_rel, 'ownership_kind', 'text', 'nue_propriete', NULL, NULL, NULL, NULL);
    PERFORM graph.upsert_relation_property(
        v_rel, 'share_bp', 'percentage_bp', NULL, NULL, 5000, NULL, NULL);

    SELECT COUNT(*) INTO v_count
    FROM graph.relation_property
    WHERE relation_id = v_rel;
    IF v_count != 2 THEN
        RAISE EXCEPTION 'FAIL: expected 2 properties before delete, got %', v_count;
    END IF;

    DELETE FROM graph.relation WHERE id = v_rel;

    SELECT COUNT(*) INTO v_count
    FROM graph.relation_property
    WHERE relation_id = v_rel;
    IF v_count != 0 THEN
        RAISE EXCEPTION 'FAIL: expected 0 properties after cascade delete, got %', v_count;
    END IF;

    RAISE NOTICE 'PASS: ON DELETE CASCADE removes relation properties';
END;
$$;

-- ============================================================================
-- Block 5 — Partial index: relprop_key_text_idx has a WHERE clause
-- ============================================================================

DO $$
DECLARE
    v_idx_def text;
BEGIN
    SELECT indexdef INTO v_idx_def
    FROM pg_indexes
    WHERE indexname = 'relprop_key_text_idx';

    IF v_idx_def IS NULL THEN
        RAISE EXCEPTION 'FAIL: relprop_key_text_idx index not found';
    END IF;

    IF v_idx_def NOT ILIKE '%WHERE%' THEN
        RAISE EXCEPTION 'FAIL: relprop_key_text_idx is not a partial index (no WHERE clause): %', v_idx_def;
    END IF;

    RAISE NOTICE 'PASS: relprop_key_text_idx is a partial index with WHERE clause';
END;
$$;

-- ============================================================================
-- Block 6 — compute_property_allowed
-- ============================================================================

DO $$
DECLARE
    v_alice   bigint;
    v_maison  bigint;
    v_rel_a   bigint;
    v_rel_b   bigint;
    v_bitmap  roaringbitmap;
BEGIN
    v_alice  := graph.upsert_entity('contact', 'Alice_allowed', 1.0, '{}');
    v_maison := graph.upsert_entity('bien',    'Maison_allowed', 1.0, '{}');

    v_rel_a := graph.upsert_relation('POSSEDE', v_alice, v_maison, 1.0,
                   NULL, NULL, NULL, NULL, NULL, NULL);
    PERFORM graph.upsert_relation_property(
        v_rel_a, 'ownership_kind', 'text', 'allowed', NULL, NULL, NULL, NULL);

    v_rel_b := graph.upsert_relation('POSSEDE', v_maison, v_alice, 1.0,
                   NULL, NULL, NULL, NULL, NULL, NULL);
    PERFORM graph.upsert_relation_property(
        v_rel_b, 'ownership_kind', 'text', 'blocked', NULL, NULL, NULL, NULL);

    -- NULL filter → NULL (skip bitmap AND in BFS)
    v_bitmap := graph.compute_property_allowed(NULL, 'ownership_kind');
    IF v_bitmap IS NOT NULL THEN
        RAISE EXCEPTION 'FAIL: NULL filter should return NULL, got non-null bitmap';
    END IF;

    -- Empty JSON object {} → NULL (no filter active)
    v_bitmap := graph.compute_property_allowed('{}'::jsonb, 'ownership_kind');
    IF v_bitmap IS NOT NULL THEN
        RAISE EXCEPTION 'FAIL: empty {} filter should return NULL';
    END IF;

    -- eq filter on 'allowed' → bitmap containing only v_rel_a
    v_bitmap := graph.compute_property_allowed(
        '{"op":"eq","value":"allowed"}'::jsonb,
        'ownership_kind');
    IF v_bitmap IS NULL THEN
        RAISE EXCEPTION 'FAIL: eq filter returned NULL instead of bitmap';
    END IF;
    IF NOT rb_contains(v_bitmap, v_rel_a::int) THEN
        RAISE EXCEPTION 'FAIL: eq filter bitmap does not contain v_rel_a (%)', v_rel_a;
    END IF;
    IF rb_contains(v_bitmap, v_rel_b::int) THEN
        RAISE EXCEPTION 'FAIL: eq filter bitmap incorrectly contains v_rel_b (%)', v_rel_b;
    END IF;

    -- eq filter with no matching value → empty bitmap (not NULL)
    v_bitmap := graph.compute_property_allowed(
        '{"op":"eq","value":"nonexistent"}'::jsonb,
        'ownership_kind');
    IF v_bitmap IS NULL THEN
        RAISE EXCEPTION 'FAIL: no-match eq filter should return empty bitmap, not NULL';
    END IF;
    IF NOT rb_is_empty(v_bitmap) THEN
        RAISE EXCEPTION 'FAIL: no-match eq filter should return empty bitmap';
    END IF;

    RAISE NOTICE 'PASS: compute_property_allowed returns correct bitmaps';
END;
$$;

-- ============================================================================
-- Block 7 — Temporal queries: current owner and historical owner
-- ============================================================================
--
-- Scenario:
--   2020-01-01: Alice acquires Maison (pleine_propriete, 100 %, 300 000 EUR)
--   2024-06-01: sale — Alice's edge closed, Bob's edge opened
--   At 2025-01-01: current owner = Bob, historical owner = Alice
--

DO $$
DECLARE
    v_alice   bigint;
    v_bob     bigint;
    v_maison  bigint;
    v_rel_alice bigint;
    v_rel_bob   bigint;
    v_at        date := '2025-01-01';

    v_current_owner bigint;
    v_past_owner    bigint;
    v_kind          text;
    v_price         bigint;
    v_currency      text;
BEGIN
    v_alice  := graph.upsert_entity('contact', 'Alice_temp', 1.0, '{}');
    v_bob    := graph.upsert_entity('contact', 'Bob_temp',   1.0, '{}');
    v_maison := graph.upsert_entity('bien',    'Maison_temp', 1.0, '{}');

    -- Alice acquires Maison 2020-01-01 (initially open)
    v_rel_alice := graph.upsert_relation('POSSEDE', v_alice, v_maison, 1.0,
                       NULL, NULL, NULL, '2020-01-01'::date, NULL, NULL);
    PERFORM graph.upsert_relation_property(
        v_rel_alice, 'ownership_kind', 'text', 'pleine_propriete', NULL, NULL, NULL, NULL);
    PERFORM graph.upsert_relation_property(
        v_rel_alice, 'share_bp', 'percentage_bp', NULL, NULL, 10000, NULL, NULL);
    PERFORM graph.upsert_relation_property(
        v_rel_alice, 'purchase_price', 'money_minor', NULL, NULL, 30000000, NULL, 'EUR');

    -- Alice sells: close her edge at 2024-06-01
    UPDATE graph.relation
    SET valid_to = '2024-06-01'::date
    WHERE id = v_rel_alice;

    -- Bob acquires from 2024-06-01 (open)
    v_rel_bob := graph.upsert_relation('POSSEDE', v_bob, v_maison, 1.0,
                     NULL, NULL, NULL, '2024-06-01'::date, NULL, NULL);
    PERFORM graph.upsert_relation_property(
        v_rel_bob, 'ownership_kind', 'text', 'pleine_propriete', NULL, NULL, NULL, NULL);
    PERFORM graph.upsert_relation_property(
        v_rel_bob, 'share_bp', 'percentage_bp', NULL, NULL, 10000, NULL, NULL);
    PERFORM graph.upsert_relation_property(
        v_rel_bob, 'purchase_price', 'money_minor', NULL, NULL, 35000000, NULL, 'EUR');

    PERFORM graph.rebuild_lj_relations();

    -- Current owner at v_at (2025-01-01): only Bob
    SELECT r.source_id INTO v_current_owner
    FROM graph.relation r
    WHERE r.relation_type = 'POSSEDE'
      AND r.target_id = v_maison
      AND r.valid_from <= v_at
      AND (r.valid_to IS NULL OR r.valid_to > v_at)
      AND r.deprecated_at IS NULL;

    IF v_current_owner IS DISTINCT FROM v_bob THEN
        RAISE EXCEPTION 'FAIL: current owner should be Bob (%), got %', v_bob, v_current_owner;
    END IF;

    -- Check Bob's ownership_kind and purchase_price via JOIN
    SELECT rp_kind.value_text, rp_price.value_integer, rp_price.currency
    INTO v_kind, v_price, v_currency
    FROM graph.relation r
    JOIN graph.relation_property rp_kind
      ON rp_kind.relation_id = r.id AND rp_kind.property_key = 'ownership_kind'
    JOIN graph.relation_property rp_price
      ON rp_price.relation_id = r.id AND rp_price.property_key = 'purchase_price'
    WHERE r.id = v_rel_bob;

    IF v_kind IS DISTINCT FROM 'pleine_propriete' THEN
        RAISE EXCEPTION 'FAIL: Bob ownership_kind mismatch: %', v_kind;
    END IF;
    IF v_price IS DISTINCT FROM 35000000 THEN
        RAISE EXCEPTION 'FAIL: Bob purchase_price mismatch: %', v_price;
    END IF;
    IF v_currency IS DISTINCT FROM 'EUR' THEN
        RAISE EXCEPTION 'FAIL: Bob currency mismatch: %', v_currency;
    END IF;

    -- Historical owner (closed valid_to): only Alice
    SELECT r.source_id INTO v_past_owner
    FROM graph.relation r
    WHERE r.relation_type = 'POSSEDE'
      AND r.target_id = v_maison
      AND r.valid_to IS NOT NULL
      AND r.deprecated_at IS NULL;

    IF v_past_owner IS DISTINCT FROM v_alice THEN
        RAISE EXCEPTION 'FAIL: historical owner should be Alice (%), got %', v_alice, v_past_owner;
    END IF;

    RAISE NOTICE 'PASS: temporal ownership — current and historical owners correct';
END;
$$;

-- ============================================================================
-- Block 8 — Co-ownership: two contacts share one bien, share_bp sums to 10 000
-- ============================================================================

DO $$
DECLARE
    v_alice   bigint;
    v_bob     bigint;
    v_chalet  bigint;
    v_rel_a   bigint;
    v_rel_b   bigint;
    v_at      date := '2025-01-01';

    v_owner_count  bigint;
    v_total_bp     bigint;
    v_kind_count   bigint;
BEGIN
    v_alice  := graph.upsert_entity('contact', 'Alice_co', 1.0, '{}');
    v_bob    := graph.upsert_entity('contact', 'Bob_co',   1.0, '{}');
    v_chalet := graph.upsert_entity('bien',    'Chalet_co', 1.0, '{}');

    -- Alice: usufruit 60 %
    v_rel_a := graph.upsert_relation('POSSEDE', v_alice, v_chalet, 1.0,
                   NULL, NULL, NULL, '2022-01-01'::date, NULL, NULL);
    PERFORM graph.upsert_relation_property(
        v_rel_a, 'ownership_kind', 'text', 'usufruit', NULL, NULL, NULL, NULL);
    PERFORM graph.upsert_relation_property(
        v_rel_a, 'share_bp', 'percentage_bp', NULL, NULL, 6000, NULL, NULL);

    -- Bob: nue_propriete 40 %
    v_rel_b := graph.upsert_relation('POSSEDE', v_bob, v_chalet, 1.0,
                   NULL, NULL, NULL, '2022-01-01'::date, NULL, NULL);
    PERFORM graph.upsert_relation_property(
        v_rel_b, 'ownership_kind', 'text', 'nue_propriete', NULL, NULL, NULL, NULL);
    PERFORM graph.upsert_relation_property(
        v_rel_b, 'share_bp', 'percentage_bp', NULL, NULL, 4000, NULL, NULL);

    -- 2 current co-owners and shares sum to 10 000
    SELECT COUNT(*), SUM(rp.value_integer)
    INTO v_owner_count, v_total_bp
    FROM graph.relation r
    JOIN graph.relation_property rp
      ON rp.relation_id = r.id AND rp.property_key = 'share_bp'
    WHERE r.relation_type = 'POSSEDE'
      AND r.target_id = v_chalet
      AND r.valid_from <= v_at
      AND (r.valid_to IS NULL OR r.valid_to > v_at)
      AND r.deprecated_at IS NULL;

    IF v_owner_count != 2 THEN
        RAISE EXCEPTION 'FAIL: expected 2 co-owners, got %', v_owner_count;
    END IF;
    IF v_total_bp IS DISTINCT FROM 10000 THEN
        RAISE EXCEPTION 'FAIL: total share_bp should be 10000, got %', v_total_bp;
    END IF;

    -- Distinct ownership kinds: usufruit and nue_propriete
    SELECT COUNT(DISTINCT rp.value_text)
    INTO v_kind_count
    FROM graph.relation r
    JOIN graph.relation_property rp
      ON rp.relation_id = r.id AND rp.property_key = 'ownership_kind'
    WHERE r.relation_type = 'POSSEDE'
      AND r.target_id = v_chalet
      AND r.deprecated_at IS NULL;

    IF v_kind_count != 2 THEN
        RAISE EXCEPTION 'FAIL: expected 2 distinct ownership kinds, got %', v_kind_count;
    END IF;

    RAISE NOTICE 'PASS: co-ownership — 2 owners, SUM(share_bp)=10000, 2 distinct kinds';
END;
$$;

-- ============================================================================
-- Block 9 — BFS with property filter: compute_property_allowed + bfs_hop $8
-- ============================================================================
--
-- Graph:  A --[KNOWS]--> B (ownership_kind=allowed)
--         A --[KNOWS]--> C (ownership_kind=blocked)
--
-- With filter eq:allowed on property 'ownership_kind', BFS from A should
-- reach B but not C. Without filter (NULL), BFS reaches both B and C.
--

DO $$
DECLARE
    v_a      bigint;
    v_b      bigint;
    v_c      bigint;
    v_rel_ab bigint;
    v_rel_ac bigint;

    v_seed        roaringbitmap;
    v_visited_empty roaringbitmap;
    v_allowed     roaringbitmap;
    v_result      roaringbitmap;
BEGIN
    v_a := graph.upsert_entity('node', 'BFS_A', 1.0, '{}');
    v_b := graph.upsert_entity('node', 'BFS_B', 1.0, '{}');
    v_c := graph.upsert_entity('node', 'BFS_C', 1.0, '{}');

    v_rel_ab := graph.upsert_relation('KNOWS', v_a, v_b, 1.0,
                    NULL, NULL, NULL, NULL, NULL, NULL);
    PERFORM graph.upsert_relation_property(
        v_rel_ab, 'ownership_kind', 'text', 'allowed', NULL, NULL, NULL, NULL);

    v_rel_ac := graph.upsert_relation('KNOWS', v_a, v_c, 1.0,
                    NULL, NULL, NULL, NULL, NULL, NULL);
    PERFORM graph.upsert_relation_property(
        v_rel_ac, 'ownership_kind', 'text', 'blocked', NULL, NULL, NULL, NULL);

    PERFORM graph.rebuild_lj_relations();

    v_seed          := rb_build(ARRAY[v_a::int]);
    v_visited_empty := rb_build(ARRAY[v_a::int]);

    -- With property filter: only relation v_rel_ab is allowed
    v_allowed := graph.compute_property_allowed(
        '{"op":"eq","value":"allowed"}'::jsonb,
        'ownership_kind');

    v_result := graph.bfs_hop(
        rb_to_array(v_seed),    -- $1 int4[] frontier
        v_visited_empty,        -- $2 roaringbitmap visited
        ARRAY['KNOWS']::text[], -- $3 edge_types
        NULL,                   -- $4 conf_min
        NULL,                   -- $5 conf_max
        NULL,                   -- $6 after_date
        NULL,                   -- $7 before_date
        v_allowed               -- $8 p_property_allowed (new parameter)
    );

    IF v_result IS NULL OR rb_is_empty(v_result) THEN
        RAISE EXCEPTION 'FAIL: BFS with property filter returned empty result (expected B)';
    END IF;
    IF NOT rb_contains(v_result, v_b::int) THEN
        RAISE EXCEPTION 'FAIL: BFS with property filter did not reach B (%)', v_b;
    END IF;
    IF rb_contains(v_result, v_c::int) THEN
        RAISE EXCEPTION 'FAIL: BFS with property filter incorrectly reached C (%)', v_c;
    END IF;

    -- Without filter (NULL): BFS reaches both B and C
    v_visited_empty := rb_build(ARRAY[v_a::int]);
    v_result := graph.bfs_hop(
        rb_to_array(v_seed),
        v_visited_empty,
        ARRAY['KNOWS']::text[],
        NULL, NULL, NULL, NULL,
        NULL    -- $8 NULL = no property filter
    );

    IF NOT rb_contains(v_result, v_b::int) THEN
        RAISE EXCEPTION 'FAIL: BFS without filter did not reach B';
    END IF;
    IF NOT rb_contains(v_result, v_c::int) THEN
        RAISE EXCEPTION 'FAIL: BFS without filter did not reach C';
    END IF;

    RAISE NOTICE 'PASS: BFS with property filter reaches allowed edge, skips blocked edge';
END;
$$;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA IF EXISTS relprop_test_docs CASCADE;

\echo 'Relation property tests passed'
