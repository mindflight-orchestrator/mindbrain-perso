-- Phase 8: BM25 custom stopword tables
-- Tests table-backed stopword import and filtering in indexing/search.

\echo '=============================================='
\echo 'Phase 8: BM25 Stopword Tables'
\echo '=============================================='

CREATE EXTENSION IF NOT EXISTS roaringbitmap;
CREATE EXTENSION IF NOT EXISTS pg_mindbrain;

DROP SCHEMA IF EXISTS bm25_phase8_test CASCADE;
CREATE SCHEMA bm25_phase8_test;

SELECT facets.bm25_delete_stopwords('english', 'phase8');

\echo ''
\echo '--- Test 8.1: Stopword table exists ---'
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'facets'
          AND table_name = 'bm25_stopwords'
    ) THEN
        RAISE NOTICE 'PASS: facets.bm25_stopwords table exists';
    ELSE
        RAISE EXCEPTION 'FAIL: facets.bm25_stopwords table does not exist';
    END IF;
END;
$$;

\echo ''
\echo '--- Test 8.2: Import array is normalized and idempotent ---'
DO $$
DECLARE
    v_imported bigint;
    v_count bigint;
BEGIN
    v_imported := facets.bm25_import_stopwords(
        'english',
        ARRAY['PostgreSQL', 'PostgreSQL', '   '],
        'phase8'
    );

    IF v_imported <> 1 THEN
        RAISE EXCEPTION 'FAIL: expected one imported/upserted stopword, got %', v_imported;
    END IF;

    SELECT count(*) INTO v_count
    FROM facets.bm25_stopwords
    WHERE language = 'english'
      AND normalized_word = 'postgresql'
      AND source = 'phase8';

    IF v_count = 1 THEN
        RAISE NOTICE 'PASS: array import normalized and deduplicated stopwords';
    ELSE
        RAISE EXCEPTION 'FAIL: expected one normalized postgresql stopword, got %', v_count;
    END IF;
END;
$$;

\echo ''
\echo '--- Test 8.3: CSV import and bad header validation ---'
DO $$
DECLARE
    v_imported bigint;
    v_failed boolean := false;
BEGIN
    v_imported := facets.bm25_import_stopwords_csv(
        'english',
        'stop_word
database
',
        'phase8'
    );

    IF v_imported <> 1 THEN
        RAISE EXCEPTION 'FAIL: expected one CSV stopword, got %', v_imported;
    END IF;

    BEGIN
        PERFORM facets.bm25_import_stopwords_csv(
            'english',
            'bad_header
ignored
',
            'phase8'
        );
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;

    IF v_failed THEN
        RAISE NOTICE 'PASS: bad CSV header rejected';
    ELSE
        RAISE EXCEPTION 'FAIL: bad CSV header was accepted';
    END IF;
END;
$$;

CREATE TABLE bm25_phase8_test.documents (
    id bigint PRIMARY KEY,
    content text NOT NULL,
    category text
);

INSERT INTO bm25_phase8_test.documents (id, content, category) VALUES
    (1, 'PostgreSQL database indexing makes search fast', 'tech'),
    (2, 'Vector embeddings support semantic retrieval', 'tech');

SELECT facets.add_faceting_to_table(
    'bm25_phase8_test.documents',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category')
    ],
    populate => true
);

\echo ''
\echo '--- Test 8.4: Custom stopwords are filtered during indexing ---'
DO $$
DECLARE
    v_table_id oid;
    v_stopword_terms bigint;
    v_database_terms bigint;
BEGIN
    SELECT table_id INTO v_table_id
    FROM facets.faceted_table
    WHERE schemaname = 'bm25_phase8_test'
      AND tablename = 'documents';

    PERFORM facets.bm25_index_document(
        'bm25_phase8_test.documents'::regclass,
        1,
        'PostgreSQL database indexing makes search fast',
        'content',
        'english'
    );

    SELECT count(*) INTO v_stopword_terms
    FROM facets.bm25_index
    WHERE table_id = v_table_id
      AND term_text = 'postgresql';

    SELECT count(*) INTO v_database_terms
    FROM facets.bm25_index
    WHERE table_id = v_table_id
      AND term_text = 'databas';

    IF v_stopword_terms <> 0 THEN
        RAISE EXCEPTION 'FAIL: custom stopword postgresql was indexed';
    END IF;

    IF v_database_terms <> 0 THEN
        RAISE EXCEPTION 'FAIL: CSV stopword database was indexed';
    END IF;

    RAISE NOTICE 'PASS: custom stopwords filtered from BM25 index';
END;
$$;

\echo ''
\echo '--- Test 8.5: Stopword-only and mixed queries are filtered ---'
DO $$
DECLARE
    v_stopword_hits bigint;
    v_mixed_hits bigint;
BEGIN
    SELECT count(*) INTO v_stopword_hits
    FROM facets.bm25_search(
        'bm25_phase8_test.documents'::regclass,
        'PostgreSQL database',
        'english',
        false, false, 0.3, 1.2, 0.75,
        10
    );

    SELECT count(*) INTO v_mixed_hits
    FROM facets.bm25_search(
        'bm25_phase8_test.documents'::regclass,
        'PostgreSQL indexing',
        'english',
        false, false, 0.3, 1.2, 0.75,
        10
    );

    IF v_stopword_hits <> 0 THEN
        RAISE EXCEPTION 'FAIL: stopword-only query expected 0 rows, got %', v_stopword_hits;
    END IF;

    IF v_mixed_hits = 1 THEN
        RAISE NOTICE 'PASS: mixed query keeps non-stopword terms';
    ELSE
        RAISE EXCEPTION 'FAIL: mixed query expected one hit, got %', v_mixed_hits;
    END IF;
END;
$$;

SELECT facets.bm25_delete_stopwords('english', 'phase8');

\echo ''
\echo '=============================================='
\echo 'Phase 8 Complete: BM25 stopword table tests passed'
\echo '=============================================='
