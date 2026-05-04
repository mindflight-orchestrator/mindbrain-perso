\echo '=============================================='
\echo 'Minimal Ontology Test Suite'
\echo '=============================================='

INSERT INTO mindbrain.workspaces (id, label, pg_schema, description, created_by, domain_profile)
VALUES ('ontology-test', 'Ontology Test', 'public', 'Ontology integration test workspace', 'test', 'project_delivery')
ON CONFLICT (id) DO UPDATE
SET label = EXCLUDED.label,
    pg_schema = EXCLUDED.pg_schema,
    description = EXCLUDED.description,
    created_by = EXCLUDED.created_by,
    domain_profile = EXCLUDED.domain_profile;

DELETE FROM mindbrain.relation_semantics WHERE workspace_id = 'ontology-test';
DELETE FROM mindbrain.column_semantics WHERE workspace_id = 'ontology-test';
DELETE FROM mindbrain.table_semantics WHERE workspace_id = 'ontology-test';
DELETE FROM mindbrain.pending_migrations WHERE workspace_id = 'ontology-test';
DELETE FROM projections WHERE scope = 'ontology-test' OR scope IS NULL;
DELETE FROM graph.entity_document;
DELETE FROM graph.relation WHERE workspace_id = 'ontology-test';
DELETE FROM graph.entity_alias
WHERE entity_id IN (
  SELECT id FROM graph.entity WHERE workspace_id = 'ontology-test'
);
DELETE FROM graph.entity WHERE workspace_id = 'ontology-test';
DELETE FROM facets WHERE workspace_id = 'ontology-test';

DROP TABLE IF EXISTS public.test_source;
CREATE TABLE public.test_source (
  id bigint PRIMARY KEY,
  status text
);

INSERT INTO mindbrain.table_semantics (
  workspace_id, table_schema, table_name, business_role,
  generation_strategy, emit_facets, emit_graph_entity, emit_graph_relation, notes
)
VALUES
  (
    'ontology-test', 'delivery', 'projects', 'stateful_item',
    'synthetic', true, true, true,
    '{"table_role":"stateful_item","entity_family":"project","volume_driver":"low","primary_time_column":"started_at","emit_projections":true}'::text
  ),
  (
    'ontology-test', 'delivery', 'tasks', 'stateful_item',
    'hybrid', true, true, true,
    '{"table_role":"stateful_item","entity_family":"task","volume_driver":"medium","primary_time_column":"created_at","emit_projections":true}'::text
  );

INSERT INTO mindbrain.column_semantics (
  workspace_id, table_schema, table_name, column_name, column_role, rich_meta
)
VALUES
  (
    'ontology-test', 'delivery', 'projects', 'status', 'status',
    '{"public_column_role":"status","semantic_type":"state","facet_key":"project_status","graph_usage":"entity_property","projection_signal":"alert_trigger","is_nullable":false}'::jsonb
  ),
  (
    'ontology-test', 'delivery', 'tasks', 'priority', 'attribute',
    '{"public_column_role":"score","semantic_type":"enum","facet_key":"task_priority","projection_signal":"priority","is_nullable":false}'::jsonb
  );

INSERT INTO mindbrain.relation_semantics (
  workspace_id, from_schema, from_table, to_schema, to_table, fk_column, relation_kind, rich_meta
)
VALUES
  (
    'ontology-test', 'delivery', 'tasks', 'delivery', 'projects', 'project_id', 'many_to_one',
    '{"relation_role":"depends_on","graph_label":"DEPENDS_ON","target_column":"id"}'::jsonb
  );

INSERT INTO facets (schema_id, content, facets, workspace_id)
VALUES
  (
    'mindbrain:ontology',
    'Project delivery domain concept',
    '{"node_id":"Project Alpha","label":"Project Alpha","entity_type":"project","criticality":"high"}'::jsonb,
    'ontology-test'
  ),
  (
    'mindbrain:ontology',
    'Task delivery domain concept',
    '{"node_id":"Task Blocker","label":"Task Blocker","entity_type":"task","criticality":"normal"}'::jsonb,
    'ontology-test'
  );

DO $$
DECLARE
  v_project bigint;
  v_task bigint;
BEGIN
  v_project := graph.upsert_entity('project', 'Project Alpha', 0.95, '{"domain":"project_delivery","entity_type":"project"}');
  v_task := graph.upsert_entity('task', 'Task Blocker', 0.80, '{"domain":"project_delivery","entity_type":"task"}');

  UPDATE graph.entity SET workspace_id = 'ontology-test' WHERE id IN (v_project, v_task);
  PERFORM graph.upsert_relation('depends_on', v_task, v_project, 0.90, NULL, NULL, NULL, NULL, NULL, NULL);
  UPDATE graph.relation
  SET workspace_id = 'ontology-test'
  WHERE source_id = v_task AND target_id = v_project AND type = 'depends_on';
END;
$$;

INSERT INTO projections (agent_id, scope, proj_type, content, weight, status)
VALUES
  ('agent:test', 'ontology-test', 'GOAL', 'Project Alpha must be delivered on time', 0.9, 'active'),
  ('agent:test', 'ontology-test', 'CONSTRAINT', 'Task Blocker is slowing Project Alpha', 0.8, 'blocking');

DO $$
DECLARE
  v_payload jsonb;
BEGIN
  SELECT mb_ontology.coverage('ontology-test') INTO v_payload;

  IF v_payload->>'workspace_id' <> 'ontology-test' THEN
    RAISE EXCEPTION 'FAIL: coverage workspace_id mismatch: %', v_payload;
  END IF;

  IF (v_payload->>'total_nodes')::int < 2 THEN
    RAISE EXCEPTION 'FAIL: coverage total_nodes too small: %', v_payload;
  END IF;

  IF (v_payload->>'covered_nodes')::int < 2 THEN
    RAISE EXCEPTION 'FAIL: coverage covered_nodes too small: %', v_payload;
  END IF;
END;
$$;

DO $$
DECLARE
  v_payload jsonb;
BEGIN
  SELECT mb_ontology.coverage_by_domain('project_delivery') INTO v_payload;

  IF v_payload->>'resolved_workspace_id' <> 'ontology-test' THEN
    RAISE EXCEPTION 'FAIL: coverage_by_domain did not resolve workspace: %', v_payload;
  END IF;

  IF (v_payload->>'total_nodes')::int < 2 THEN
    RAISE EXCEPTION 'FAIL: coverage_by_domain total_nodes too small: %', v_payload;
  END IF;
END;
$$;

DO $$
DECLARE
  v_count int;
BEGIN
  SELECT COUNT(*) INTO v_count
  FROM mb_ontology.marketplace_search('Project Alpha', 'ontology-test', NULL, 0.1, 2, NULL, 10);

  IF v_count < 1 THEN
    RAISE EXCEPTION 'FAIL: marketplace_search returned no rows';
  END IF;
END;
$$;

DO $$
DECLARE
  v_count int;
BEGIN
  SELECT COUNT(*) INTO v_count
  FROM mb_ontology.marketplace_search_by_domain('Project Alpha', 'project_delivery', NULL, 0.1, 2, NULL, 10);

  IF v_count < 1 THEN
    RAISE EXCEPTION 'FAIL: marketplace_search_by_domain returned no rows';
  END IF;
END;
$$;

DO $$
DECLARE
  v_sql text;
BEGIN
  SELECT mb_ontology.generate_triggers(
    'ontology-test',
    'public.test_source'::regclass,
    '[{"column_name":"status","facet_key":"project_status","index_in_bm25":true}]'::jsonb
  )
  INTO v_sql;

  IF position('CREATE OR REPLACE FUNCTION' IN v_sql) = 0 OR position('CREATE TRIGGER' IN v_sql) = 0 THEN
    RAISE EXCEPTION 'FAIL: generate_triggers did not return full trigger SQL';
  END IF;
END;
$$;

DO $$
DECLARE
  v_payload jsonb;
BEGIN
  SELECT mb_ontology.validate_ddl_proposal(
    'ontology-test',
    'CREATE TABLE public.ontology_items(id bigint primary key)',
    '{"table_semantics":[]}'::jsonb,
    NULL
  )
  INTO v_payload;

  IF (v_payload->>'valid')::boolean IS NOT TRUE THEN
    RAISE EXCEPTION 'FAIL: validate_ddl_proposal rejected safe SQL: %', v_payload;
  END IF;
END;
$$;

DO $$
DECLARE
  v_payload jsonb;
BEGIN
  SELECT mb_ontology.export_workspace_model('ontology-test') INTO v_payload;

  IF v_payload->>'schema_version' <> '1.0.0' THEN
    RAISE EXCEPTION 'FAIL: export schema_version mismatch: %', v_payload;
  END IF;

  IF jsonb_array_length(v_payload->'tables') < 2 THEN
    RAISE EXCEPTION 'FAIL: export tables missing: %', v_payload;
  END IF;

  IF jsonb_array_length(v_payload->'columns') < 2 THEN
    RAISE EXCEPTION 'FAIL: export columns missing: %', v_payload;
  END IF;

  IF jsonb_array_length(v_payload->'relations') < 1 THEN
    RAISE EXCEPTION 'FAIL: export relations missing: %', v_payload;
  END IF;
END;
$$;

DO $$
DECLARE
  v_toon text;
BEGIN
  SELECT mb_ontology.export_workspace_model_toon('ontology-test') INTO v_toon;

  IF position('schema_version: 1.0.0' IN v_toon) = 0 THEN
    RAISE EXCEPTION 'FAIL: TOON export missing schema_version: %', v_toon;
  END IF;

  IF position('workspace:' IN v_toon) = 0 THEN
    RAISE EXCEPTION 'FAIL: TOON export missing workspace block: %', v_toon;
  END IF;

  IF position('tables[' IN v_toon) = 0 THEN
    RAISE EXCEPTION 'FAIL: TOON export missing tables array: %', v_toon;
  END IF;
END;
$$;

-- json_text_to_toon_native: invalid JSON returns input (passthrough), not NULL
DO $$
DECLARE
  v_in text := 'this is not valid json';
  v_out text;
BEGIN
  SELECT mb_ontology.json_text_to_toon_native(v_in) INTO v_out;
  IF v_out IS NULL THEN
    RAISE EXCEPTION 'FAIL: json_text_to_toon_native should not return NULL for non-null input';
  END IF;
  IF v_out <> v_in THEN
    RAISE EXCEPTION 'FAIL: expected passthrough of invalid JSON, got: %', v_out;
  END IF;
END;
$$;

\echo 'Minimal ontology tests passed'
