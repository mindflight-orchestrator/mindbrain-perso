\echo '=============================================='
\echo 'Minimal BM25 Test Suite'
\echo '=============================================='

DROP SCHEMA IF EXISTS minimal_bm25 CASCADE;
CREATE SCHEMA minimal_bm25;

CREATE TABLE minimal_bm25.documents (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    category TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO minimal_bm25.documents
    (title, content, category, metadata)
VALUES
    ('PostgreSQL Performance Guide', 'postgresql database performance tuning vacuum analyze indexes', 'Technology', '{"author":"Alice"}'),
    ('PostgreSQL Administration', 'postgresql backup replication administration database security', 'Technology', '{"author":"Bob"}'),
    ('Sourdough Bread Basics', 'bread baking starter flour oven kitchen', 'Cooking', '{"author":"Carla"}'),
    ('Trail Running Program', 'trail running fitness endurance training sports', 'Sports', '{"author":"Dan"}'),
    ('SQL Joins Explained', 'sql database joins cte query planner execution', 'Technology', '{"author":"Eve"}');

SELECT facets.add_faceting_to_table(
    'minimal_bm25.documents',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category')
    ],
    populate => true
);

SELECT facets.bm25_set_language('minimal_bm25.documents'::regclass, 'english');
SELECT facets.bm25_create_sync_trigger(
    'minimal_bm25.documents'::regclass,
    'id',
    'content',
    'english'
);
SELECT facets.bm25_rebuild_index(
    'minimal_bm25.documents'::regclass,
    'id',
    'content',
    'english',
    0
);

DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents(
        'minimal_bm25',
        'documents',
        'PostgreSQL',
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        0.5,
        'english'
    );

    IF v_result.total_found != 2 THEN
        RAISE EXCEPTION 'FAIL: Expected 2 PostgreSQL BM25 matches, got %', v_result.total_found;
    END IF;

    RAISE NOTICE 'PASS: PostgreSQL BM25 search returned 2 documents';
END;
$$;

DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'minimal_bm25',
        'documents',
        'vacuum analyze',
        NULL,
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        NULL,
        10,
        'english'
    );

    IF v_result.total_found != 1 THEN
        RAISE EXCEPTION 'FAIL: Expected 1 result for "vacuum analyze", got %', v_result.total_found;
    END IF;

    RAISE NOTICE 'PASS: query "vacuum analyze" returned 1 document';
END;
$$;

DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'minimal_bm25',
        'documents',
        'PostgreSQL',
        '{"category":"Technology"}'::jsonb,
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        NULL,
        10,
        'english'
    );

    IF v_result.total_found != 2 THEN
        RAISE EXCEPTION 'FAIL: Expected 2 PostgreSQL+Technology results, got %', v_result.total_found;
    END IF;

    IF v_result.facets IS NULL OR jsonb_array_length(v_result.facets) = 0 THEN
        RAISE EXCEPTION 'FAIL: Expected non-empty facets for PostgreSQL+Technology search';
    END IF;

    RAISE NOTICE 'PASS: PostgreSQL+Technology search returned 2 documents with facets';
END;
$$;

INSERT INTO minimal_bm25.documents
    (title, content, category, metadata)
VALUES
    ('PostgreSQL Query Planner', 'postgresql query planner statistics database tuning', 'Technology', '{"author":"Frank"}');

DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents(
        'minimal_bm25',
        'documents',
        'PostgreSQL',
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        0.5,
        'english'
    );

    IF v_result.total_found != 3 THEN
        RAISE EXCEPTION 'FAIL: Expected trigger-maintained BM25 count of 3 after insert, got %', v_result.total_found;
    END IF;

    RAISE NOTICE 'PASS: BM25 trigger indexed inserted PostgreSQL document';
END;
$$;

DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'minimal_bm25',
        'documents',
        'running fitness',
        '{"category":"Sports"}'::jsonb,
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        NULL,
        10,
        'english'
    );

    IF v_result.total_found != 1 THEN
        RAISE EXCEPTION 'FAIL: Expected 1 Sports BM25 match for "running fitness", got %', v_result.total_found;
    END IF;

    RAISE NOTICE 'PASS: running fitness + Sports returned 1 document';
END;
$$;

-- Direct native SRF smoke test: exercise facets.bm25_search (the thin wrapper
-- around bm25_search_native). The default runner previously only hit the
-- search_documents path, so a regression in the native SRF (e.g. backend
-- segfault) was invisible to CI.
DO $$
DECLARE
    v_count int;
    v_top_doc bigint;
    v_top_score float;
BEGIN
    SELECT count(*) INTO v_count
    FROM facets.bm25_search(
        'minimal_bm25.documents'::regclass,
        'postgresql database',
        'english',
        false,
        false,
        0.3,
        1.2,
        0.75,
        10
    );

    IF v_count < 1 THEN
        RAISE EXCEPTION 'FAIL: facets.bm25_search returned 0 rows for "postgresql database"';
    END IF;

    SELECT doc_id, score
    INTO v_top_doc, v_top_score
    FROM facets.bm25_search(
        'minimal_bm25.documents'::regclass,
        'postgresql database',
        'english',
        false,
        false,
        0.3,
        1.2,
        0.75,
        10
    )
    ORDER BY score DESC
    LIMIT 1;

    IF v_top_doc IS NULL THEN
        RAISE EXCEPTION 'FAIL: facets.bm25_search top doc_id is NULL';
    END IF;
    IF v_top_score IS NULL OR v_top_score <= 0.0 THEN
        RAISE EXCEPTION 'FAIL: facets.bm25_search top score is non-positive (%)', v_top_score;
    END IF;
    IF v_top_score = 'NaN'::float8 OR v_top_score = 'Infinity'::float8 OR v_top_score = '-Infinity'::float8 THEN
        RAISE EXCEPTION 'FAIL: facets.bm25_search top score is non-finite (%)', v_top_score;
    END IF;

    RAISE NOTICE 'PASS: facets.bm25_search returned % rows, top doc_id=%, score=%',
        v_count, v_top_doc, v_top_score;
END;
$$;

\echo 'Minimal BM25 tests passed'
