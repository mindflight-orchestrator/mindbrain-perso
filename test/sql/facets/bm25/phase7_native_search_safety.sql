-- Phase 7: Native BM25 search safety / boundary tests
-- Focused regression coverage for facets.bm25_search (which calls
-- bm25_search_native). The goal is to catch any regression that would
-- previously cause a backend SIGSEGV, an unhandled NaN, or a
-- forced-unwrap panic in the SRF setup path.
--
-- Each test is wrapped in DO $$ ... $$ so a backend crash terminates
-- the script via psql's ON_ERROR_STOP=1. If any of these triggers a
-- "the connection to the server was lost" the underlying SIGSEGV is
-- the regression we are guarding against.

\echo '=============================================='
\echo 'Phase 7: Native BM25 search safety tests'
\echo '=============================================='

CREATE EXTENSION IF NOT EXISTS roaringbitmap;
CREATE EXTENSION IF NOT EXISTS pg_mindbrain;

DROP SCHEMA IF EXISTS bm25_phase7_test CASCADE;
CREATE SCHEMA bm25_phase7_test;

CREATE TABLE bm25_phase7_test.documents (
    id BIGSERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    category TEXT NOT NULL
);

INSERT INTO bm25_phase7_test.documents (content, category) VALUES
    ('first principles validation project plan', 'tech'),
    ('postgresql database performance vacuum analyze', 'tech'),
    ('sourdough bread baking recipe',                 'food'),
    ('trail running fitness endurance',               'sports'),
    ('first principles design thinking workshop',     'tech'),
    ('database replication backup administration',    'tech');

SELECT facets.add_faceting_to_table(
    'bm25_phase7_test.documents',
    key => 'id',
    facets => ARRAY[facets.plain_facet('category')],
    populate => true
);

SELECT facets.bm25_set_language('bm25_phase7_test.documents'::regclass, 'english');
SELECT facets.bm25_rebuild_index(
    'bm25_phase7_test.documents'::regclass,
    'id',
    'content',
    'english',
    0
);

\echo ''
\echo '--- Test 7.1: Multi-term query similar to production crash repro ---'
DO $$
DECLARE
    v_count int;
BEGIN
    -- The exact phrase shape that crashed the backend in the wild.
    SELECT count(*) INTO v_count
    FROM facets.bm25_search(
        'bm25_phase7_test.documents'::regclass,
        'first principles validation project',
        'english',
        false, false, 0.3, 1.2, 0.75,
        10
    );

    IF v_count < 1 THEN
        RAISE EXCEPTION 'FAIL: expected matches for "first principles validation project", got %', v_count;
    END IF;

    RAISE NOTICE 'PASS: production-shape query returned % rows without backend crash', v_count;
END;
$$;

\echo ''
\echo '--- Test 7.2: Empty query string ---'
DO $$
DECLARE
    v_count int;
BEGIN
    SELECT count(*) INTO v_count
    FROM facets.bm25_search(
        'bm25_phase7_test.documents'::regclass,
        '',
        'english',
        false, false, 0.3, 1.2, 0.75,
        10
    );

    IF v_count <> 0 THEN
        RAISE EXCEPTION 'FAIL: empty query expected 0 rows, got %', v_count;
    END IF;

    RAISE NOTICE 'PASS: empty query returned 0 rows';
END;
$$;

\echo ''
\echo '--- Test 7.3: Stopword-only query (no surviving lexemes) ---'
DO $$
DECLARE
    v_count int;
BEGIN
    SELECT count(*) INTO v_count
    FROM facets.bm25_search(
        'bm25_phase7_test.documents'::regclass,
        'the and of for',
        'english',
        false, false, 0.3, 1.2, 0.75,
        10
    );

    IF v_count <> 0 THEN
        RAISE EXCEPTION 'FAIL: stopword-only query expected 0 rows, got %', v_count;
    END IF;

    RAISE NOTICE 'PASS: stopword-only query returned 0 rows';
END;
$$;

\echo ''
\echo '--- Test 7.4: Query with no hits in the corpus ---'
DO $$
DECLARE
    v_count int;
BEGIN
    SELECT count(*) INTO v_count
    FROM facets.bm25_search(
        'bm25_phase7_test.documents'::regclass,
        'zzzz_unknown_term_xyzzy',
        'english',
        false, false, 0.3, 1.2, 0.75,
        10
    );

    IF v_count <> 0 THEN
        RAISE EXCEPTION 'FAIL: unknown-term query expected 0 rows, got %', v_count;
    END IF;

    RAISE NOTICE 'PASS: unknown-term query returned 0 rows';
END;
$$;

\echo ''
\echo '--- Test 7.5: limit = 0 returns no rows but does not crash ---'
DO $$
DECLARE
    v_count int;
BEGIN
    SELECT count(*) INTO v_count
    FROM facets.bm25_search(
        'bm25_phase7_test.documents'::regclass,
        'database',
        'english',
        false, false, 0.3, 1.2, 0.75,
        0
    );

    IF v_count <> 0 THEN
        RAISE EXCEPTION 'FAIL: limit=0 expected 0 rows, got %', v_count;
    END IF;

    RAISE NOTICE 'PASS: limit=0 returned 0 rows without backend crash';
END;
$$;

\echo ''
\echo '--- Test 7.6: limit = 1 returns at most one row ---'
DO $$
DECLARE
    v_count int;
BEGIN
    SELECT count(*) INTO v_count
    FROM facets.bm25_search(
        'bm25_phase7_test.documents'::regclass,
        'database',
        'english',
        false, false, 0.3, 1.2, 0.75,
        1
    );

    IF v_count > 1 THEN
        RAISE EXCEPTION 'FAIL: limit=1 expected <= 1 rows, got %', v_count;
    END IF;

    RAISE NOTICE 'PASS: limit=1 returned % row(s)', v_count;
END;
$$;

\echo ''
\echo '--- Test 7.7: very large limit does not crash and returns sane scores ---'
DO $$
DECLARE
    v_count int;
    v_min_score float;
    v_max_score float;
BEGIN
    SELECT count(*), min(score), max(score)
      INTO v_count, v_min_score, v_max_score
    FROM facets.bm25_search(
        'bm25_phase7_test.documents'::regclass,
        'database',
        'english',
        false, false, 0.3, 1.2, 0.75,
        1000000
    );

    IF v_count < 1 THEN
        RAISE EXCEPTION 'FAIL: large-limit query expected >= 1 rows, got %', v_count;
    END IF;
    IF v_min_score IS NULL OR v_min_score <= 0.0 THEN
        RAISE EXCEPTION 'FAIL: large-limit query produced non-positive min score (%)', v_min_score;
    END IF;
    IF v_max_score = 'NaN'::float8 OR v_max_score = 'Infinity'::float8
       OR v_max_score = '-Infinity'::float8 THEN
        RAISE EXCEPTION 'FAIL: large-limit query produced non-finite score (%)', v_max_score;
    END IF;

    RAISE NOTICE 'PASS: large-limit returned % rows, scores in [%, %]',
        v_count, v_min_score, v_max_score;
END;
$$;

\echo ''
\echo '--- Test 7.8: default language path (NULL p_language) ---'
DO $$
DECLARE
    v_count int;
BEGIN
    -- Going through the SQL wrapper with the default value triggers the
    -- p_language NULL handling path inside bm25_search_native, where the
    -- language buffer is allocated from PostgreSQL memory.
    SELECT count(*) INTO v_count
    FROM facets.bm25_search(
        'bm25_phase7_test.documents'::regclass,
        'database'
    );

    IF v_count < 1 THEN
        RAISE EXCEPTION 'FAIL: default-language query expected >= 1 rows, got %', v_count;
    END IF;

    RAISE NOTICE 'PASS: default-language query returned % rows', v_count;
END;
$$;

\echo ''
\echo '--- Test 7.9: ordering is monotonically non-increasing by score ---'
DO $$
DECLARE
    v_prev float := 'Infinity'::float8;
    v_row record;
    v_violations int := 0;
BEGIN
    FOR v_row IN
        SELECT doc_id, score
        FROM facets.bm25_search(
            'bm25_phase7_test.documents'::regclass,
            'first principles',
            'english',
            false, false, 0.3, 1.2, 0.75,
            100
        )
        ORDER BY score DESC
    LOOP
        IF v_row.score > v_prev THEN
            v_violations := v_violations + 1;
        END IF;
        v_prev := v_row.score;
    END LOOP;

    IF v_violations <> 0 THEN
        RAISE EXCEPTION 'FAIL: % score-order violations detected', v_violations;
    END IF;

    RAISE NOTICE 'PASS: BM25 results are properly ordered by descending score';
END;
$$;

\echo ''
\echo '--- Test 7.10: repeated calls do not destabilize the backend ---'
DO $$
DECLARE
    v_count int;
    v_iter int;
BEGIN
    -- Hammers the native SRF in a loop. A use-after-free or memory
    -- context bug in the native path tends to surface within a few
    -- iterations rather than on the very first call.
    FOR v_iter IN 1 .. 25 LOOP
        SELECT count(*) INTO v_count
        FROM facets.bm25_search(
            'bm25_phase7_test.documents'::regclass,
            'first principles validation project',
            'english',
            false, false, 0.3, 1.2, 0.75,
            50
        );
        IF v_count < 1 THEN
            RAISE EXCEPTION 'FAIL: iteration % returned 0 rows', v_iter;
        END IF;
    END LOOP;

    RAISE NOTICE 'PASS: 25 repeated bm25_search calls completed without crash';
END;
$$;

\echo ''
\echo 'Phase 7 native BM25 search safety tests passed'
