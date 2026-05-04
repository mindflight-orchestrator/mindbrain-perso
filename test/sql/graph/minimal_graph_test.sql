\echo '=============================================='
\echo 'Minimal Graph Test Suite'
\echo '=============================================='

DROP SCHEMA IF EXISTS minimal_graph_docs CASCADE;
CREATE SCHEMA minimal_graph_docs;

CREATE TABLE minimal_graph_docs.documents (
    id BIGINT PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    category TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO minimal_graph_docs.documents
    (id, title, content, category, metadata)
VALUES
    (1, 'PostgreSQL by Alice', 'alice writes about postgresql indexes and tuning', 'Technology', '{"slug":"alice-postgres"}'),
    (2, 'Running with Bob', 'bob writes about trail running and endurance', 'Sports', '{"slug":"bob-running"}'),
    (3, 'SQL by Alice and Bob', 'alice and bob explain sql joins and planners', 'Technology', '{"slug":"alice-bob-sql"}');

DELETE FROM graph.entity_document;
DELETE FROM graph.relation;
DELETE FROM graph.entity_alias;
DELETE FROM graph.lj_out;
DELETE FROM graph.lj_in;
DELETE FROM graph.entity;

DO $$
DECLARE
    v_alice bigint;
    v_bob bigint;
    v_postgresql bigint;
    v_sql bigint;
    v_resolved roaringbitmap;
    v_hops roaringbitmap;
    v_docs_count bigint;
    v_path_len int;
BEGIN
    v_alice := graph.upsert_entity('person', 'Alice', 1.0, '{"role":"author"}');
    v_bob := graph.upsert_entity('person', 'Bob', 1.0, '{"role":"author"}');
    v_postgresql := graph.upsert_entity('topic', 'PostgreSQL', 1.0, '{"domain":"database"}');
    v_sql := graph.upsert_entity('topic', 'SQL', 1.0, '{"domain":"database"}');

    PERFORM graph.register_aliases(v_alice, ARRAY['Alice'], 1.0);
    PERFORM graph.register_aliases(v_bob, ARRAY['Bob'], 1.0);
    PERFORM graph.register_aliases(v_postgresql, ARRAY['PostgreSQL'], 1.0);
    PERFORM graph.register_aliases(v_sql, ARRAY['SQL'], 1.0);

    PERFORM graph.upsert_relation('knows', v_alice, v_bob, 1.0, NULL, NULL, NULL, NULL, NULL, NULL);
    PERFORM graph.upsert_relation('writes_about', v_alice, v_postgresql, 1.0, NULL, NULL, NULL, NULL, NULL, NULL);
    PERFORM graph.upsert_relation('writes_about', v_bob, v_sql, 1.0, NULL, NULL, NULL, NULL, NULL, NULL);

    INSERT INTO graph.entity_document (entity_id, doc_id, table_oid, role, confidence)
    VALUES
        (v_alice, 1, 'minimal_graph_docs.documents'::regclass::oid, 'author', 1.0),
        (v_alice, 3, 'minimal_graph_docs.documents'::regclass::oid, 'author', 1.0),
        (v_bob, 2, 'minimal_graph_docs.documents'::regclass::oid, 'author', 1.0),
        (v_bob, 3, 'minimal_graph_docs.documents'::regclass::oid, 'author', 1.0),
        (v_postgresql, 1, 'minimal_graph_docs.documents'::regclass::oid, 'topic', 1.0),
        (v_sql, 3, 'minimal_graph_docs.documents'::regclass::oid, 'topic', 1.0);

    PERFORM graph.rebuild_lj_relations();

    v_resolved := graph.resolve_terms(ARRAY['Alice']);
    IF v_resolved IS NULL OR rb_is_empty(v_resolved) OR NOT rb_contains(v_resolved, v_alice::int) THEN
        RAISE EXCEPTION 'FAIL: graph.resolve_terms did not resolve Alice';
    END IF;

    SELECT COUNT(DISTINCT doc_id) INTO v_docs_count
    FROM graph.entity_docs(v_resolved, 'minimal_graph_docs.documents'::regclass);

    IF v_docs_count != 2 THEN
        RAISE EXCEPTION 'FAIL: Expected 2 Alice documents, got %', v_docs_count;
    END IF;

    v_hops := k_hops_filtered(v_resolved, 2, ARRAY['knows', 'writes_about']::text[], NULL, NULL, NULL, NULL, NULL, NULL);
    IF v_hops IS NULL OR rb_is_empty(v_hops) OR NOT rb_contains(v_hops, v_sql::int) THEN
        RAISE EXCEPTION 'FAIL: k_hops_filtered did not reach SQL from Alice';
    END IF;

    v_path_len := shortest_path_filtered(v_alice::int, v_bob::int, ARRAY['knows']::text[], NULL, NULL, NULL, NULL, NULL, NULL, 5);
    IF v_path_len != 1 THEN
        RAISE EXCEPTION 'FAIL: Expected Alice -> Bob shortest path of 1, got %', v_path_len;
    END IF;

    v_path_len := shortest_path_filtered(v_bob::int, v_alice::int, ARRAY['knows']::text[], NULL, NULL, NULL, NULL, NULL, NULL, 5);
    IF v_path_len != 1 THEN
        RAISE EXCEPTION 'FAIL: Expected Bob -> Alice shortest path of 1, got %', v_path_len;
    END IF;

    v_path_len := shortest_path_filtered(v_alice::int, v_sql::int, ARRAY['knows', 'writes_about']::text[], NULL, NULL, NULL, NULL, NULL, NULL, 5);
    IF v_path_len != 2 THEN
        RAISE EXCEPTION 'FAIL: Expected Alice -> SQL shortest path of 2, got %', v_path_len;
    END IF;

    SELECT COUNT(*) INTO v_docs_count
    FROM graph.entity_fts_search('PostgreSQL', ARRAY['topic']::text[], 'database', 0.0, 10);

    IF v_docs_count != 1 THEN
        RAISE EXCEPTION 'FAIL: Expected 1 FTS graph entity hit for PostgreSQL topic, got %', v_docs_count;
    END IF;

    RAISE NOTICE 'PASS: minimal graph traversal and document linkage work';
END;
$$;

\echo 'Minimal graph tests passed'
