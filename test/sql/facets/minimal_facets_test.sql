\echo '=============================================='
\echo 'Minimal Facets Test Suite'
\echo '=============================================='

DROP SCHEMA IF EXISTS minimal_facets CASCADE;
CREATE SCHEMA minimal_facets;

CREATE TABLE minimal_facets.documents (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    category TEXT NOT NULL,
    region TEXT NOT NULL,
    tags TEXT[] NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    in_stock BOOLEAN NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO minimal_facets.documents
    (title, content, category, region, tags, price, in_stock, metadata)
VALUES
    ('PostgreSQL Performance', 'postgresql tuning indexes vacuum analyze', 'Technology', 'EU', ARRAY['postgresql', 'performance'], 29.99, true,  '{"author":"Alice"}'),
    ('PostgreSQL Administration', 'postgresql backup replication security', 'Technology', 'US', ARRAY['postgresql', 'admin'], 24.99, true,  '{"author":"Bob"}'),
    ('Bread Baking', 'bread baking starter flour oven', 'Cooking', 'EU', ARRAY['baking', 'bread'], 12.50, true,  '{"author":"Carla"}'),
    ('Trail Running', 'trail running fitness endurance outdoors', 'Sports', 'EU', ARRAY['running', 'fitness'], 18.00, false, '{"author":"Dan"}'),
    ('SQL Joins', 'sql joins cte planner database', 'Technology', 'US', ARRAY['sql', 'database'], 34.00, true, '{"author":"Eve"}');

SELECT facets.add_faceting_to_table(
    'minimal_facets.documents',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category'),
        facets.plain_facet('region'),
        facets.array_facet('tags'),
        facets.boolean_facet('in_stock'),
        facets.bucket_facet('price', buckets => ARRAY[0, 20, 30, 40])
    ],
    populate => true
);

DO $$
DECLARE
    v_bitmap roaringbitmap;
    v_count bigint;
BEGIN
    SELECT facets.filter_documents_by_facets_bitmap(
        'minimal_facets',
        '{"category":"Technology"}'::jsonb,
        'documents'
    ) INTO v_bitmap;

    v_count := rb_cardinality(v_bitmap);

    IF v_count != 3 THEN
        RAISE EXCEPTION 'FAIL: Expected 3 Technology documents, got %', v_count;
    END IF;

    RAISE NOTICE 'PASS: category=Technology returns % documents', v_count;
END;
$$;

DO $$
DECLARE
    v_bitmap roaringbitmap;
    v_count bigint;
BEGIN
    SELECT facets.filter_documents_by_facets_bitmap(
        'minimal_facets',
        '{"category":"Technology","region":"US"}'::jsonb,
        'documents'
    ) INTO v_bitmap;

    v_count := rb_cardinality(v_bitmap);

    IF v_count != 2 THEN
        RAISE EXCEPTION 'FAIL: Expected 2 Technology+US documents, got %', v_count;
    END IF;

    RAISE NOTICE 'PASS: category=Technology and region=US returns % documents', v_count;
END;
$$;

DO $$
DECLARE
    v_table_id oid;
    v_filters facets.facet_filter[];
    v_bitmap roaringbitmap;
    v_count bigint;
BEGIN
    SELECT table_id INTO v_table_id
    FROM facets.faceted_table
    WHERE schemaname = 'minimal_facets' AND tablename = 'documents';

    SELECT array_agg(
        ROW(
            'category',
            CASE
                WHEN i = 1 THEN 'Technology'
                ELSE 'missing_' || i::text || repeat('_padding_', 16)
            END
        )::facets.facet_filter
    )
    INTO v_filters
    FROM generate_series(1, 256) AS gs(i);

    SELECT build_filter_bitmap_native(v_table_id, v_filters) INTO v_bitmap;
    v_count := rb_cardinality(v_bitmap);

    IF v_count != 3 THEN
        RAISE EXCEPTION 'FAIL: Expected 3 documents from toasted native filter array, got %', v_count;
    END IF;

    RAISE NOTICE 'PASS: build_filter_bitmap_native handles toasted filter arrays (% documents)', v_count;
END;
$$;

DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'minimal_facets',
        'documents',
        '',
        '{"category":"Technology"}'::jsonb,
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        NULL,
        NULL,
        10,
        'english'
    );

    IF v_result.total_found != 3 THEN
        RAISE EXCEPTION 'FAIL: Expected 3 browsing results for category Technology, got %', v_result.total_found;
    END IF;

    IF v_result.facets IS NULL OR jsonb_array_length(v_result.facets) = 0 THEN
        RAISE EXCEPTION 'FAIL: Expected non-empty facets for category Technology browsing';
    END IF;

    RAISE NOTICE 'PASS: browsing with facets returned 3 results and non-empty facets';
END;
$$;

DO $$
DECLARE
    v_table_id oid;
    v_filters facets.facet_filter[];
    v_count bigint;
BEGIN
    SELECT table_id INTO v_table_id
    FROM facets.faceted_table
    WHERE schemaname = 'minimal_facets' AND tablename = 'documents';

    SELECT array_agg(
        ROW(
            'category',
            CASE
                WHEN i = 1 THEN 'Technology'
                ELSE 'missing_' || i::text || repeat('_padding_', 16)
            END
        )::facets.facet_filter
    )
    INTO v_filters
    FROM generate_series(1, 256) AS gs(i);

    SELECT count(*) INTO v_count
    FROM search_documents_native(v_table_id, v_filters, 100, 0);

    IF v_count != 3 THEN
        RAISE EXCEPTION 'FAIL: Expected 3 documents from toasted native search filter array, got %', v_count;
    END IF;

    RAISE NOTICE 'PASS: search_documents_native handles toasted filter arrays (% documents)', v_count;
END;
$$;

DO $$
DECLARE
    v_table_id oid;
    v_target_facets text[];
    v_count bigint;
    v_top_value text;
    v_top_cardinality bigint;
BEGIN
    SELECT table_id INTO v_table_id
    FROM facets.faceted_table
    WHERE schemaname = 'minimal_facets' AND tablename = 'documents';

    SELECT array_agg(
        CASE
            WHEN i = 1 THEN 'category'
            ELSE 'missing_facet_' || i::text || repeat('_padding_', 16)
        END
    )
    INTO v_target_facets
    FROM generate_series(1, 256) AS gs(i);

    SELECT count(*), max(facet_value), max(cardinality)
    INTO v_count, v_top_value, v_top_cardinality
    FROM get_facet_counts_native(v_table_id, NULL, v_target_facets, 10);

    IF v_count != 3 THEN
        RAISE EXCEPTION 'FAIL: Expected 3 rows from toasted native facet array, got %', v_count;
    END IF;

    IF v_top_value IS NULL OR v_top_cardinality IS NULL THEN
        RAISE EXCEPTION 'FAIL: Expected non-empty rows from get_facet_counts_native with toasted facet array';
    END IF;

    RAISE NOTICE 'PASS: get_facet_counts_native handles toasted facet arrays (% rows)', v_count;
END;
$$;

DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'minimal_facets',
        'documents',
        '',
        '{"tags":"postgresql"}'::jsonb,
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        NULL,
        NULL,
        10,
        'english'
    );

    IF v_result.total_found != 2 THEN
        RAISE EXCEPTION 'FAIL: Expected 2 documents for tag postgresql, got %', v_result.total_found;
    END IF;

    RAISE NOTICE 'PASS: tag=postgresql returns 2 documents';
END;
$$;

DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'minimal_facets',
        'documents',
        '',
        '{"price":"2"}'::jsonb,
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        NULL,
        NULL,
        10,
        'english'
    );

    IF v_result.total_found != 2 THEN
        RAISE EXCEPTION 'FAIL: Expected 2 documents for bucket 2 (20-30), got %', v_result.total_found;
    END IF;

    RAISE NOTICE 'PASS: price bucket 2 returns 2 documents';
END;
$$;

\echo 'Minimal facets tests passed'
