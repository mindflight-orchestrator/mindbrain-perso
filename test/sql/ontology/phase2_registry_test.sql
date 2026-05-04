\echo '=============================================='
\echo 'Phase 2 Ontology Registry Test Suite'
\echo '=============================================='

INSERT INTO mindbrain.workspaces (id, label, pg_schema, description, created_by, domain_profile)
VALUES
  ('ontology-a', 'Ontology A', 'public', 'Cross-workspace ontology A', 'test', 'delivery'),
  ('ontology-b', 'Ontology B', 'public', 'Cross-workspace ontology B', 'test', 'delivery')
ON CONFLICT (id) DO UPDATE
SET label = EXCLUDED.label,
    pg_schema = EXCLUDED.pg_schema,
    description = EXCLUDED.description,
    created_by = EXCLUDED.created_by,
    domain_profile = EXCLUDED.domain_profile;

DELETE FROM mb_ontology.workspace_bridges
WHERE workspace_a IN ('ontology-a', 'ontology-b')
   OR workspace_b IN ('ontology-a', 'ontology-b');

DELETE FROM mb_ontology.relation_types WHERE workspace_id IN ('ontology-a', 'ontology-b');
DELETE FROM mb_ontology.entity_types WHERE workspace_id IN ('ontology-a', 'ontology-b');
DELETE FROM mb_ontology.ontology_versions
WHERE ontology_id IN (
  SELECT id FROM mb_ontology.ontologies WHERE workspace_id IN ('ontology-a', 'ontology-b')
);
DELETE FROM mb_ontology.ontologies WHERE workspace_id IN ('ontology-a', 'ontology-b');
DELETE FROM mindbrain.relation_semantics WHERE workspace_id IN ('ontology-a', 'ontology-b');
DELETE FROM mindbrain.column_semantics WHERE workspace_id IN ('ontology-a', 'ontology-b');
DELETE FROM mindbrain.table_semantics WHERE workspace_id IN ('ontology-a', 'ontology-b');

INSERT INTO mindbrain.table_semantics (
  workspace_id, table_schema, table_name, business_role,
  generation_strategy, emit_facets, emit_graph_entity, emit_graph_relation, notes
)
VALUES
  ('ontology-a', 'delivery', 'projects', 'stateful_item', 'synthetic', true, true, true, '{"entity_family":"project"}'),
  ('ontology-b', 'delivery', 'projects', 'stateful_item', 'synthetic', true, true, true, '{"entity_family":"project"}');

SELECT mb_ontology.register_entity_type(
  'ontology-a',
  '{"type_name":"project","label":"Project","semantic_domain":"delivery","description":"A delivery project"}'::jsonb
);

SELECT mb_ontology.register_entity_type(
  'ontology-a',
  '{"type_name":"task","label":"Task","semantic_domain":"delivery","description":"A unit of work"}'::jsonb
);

SELECT mb_ontology.register_entity_type(
  'ontology-b',
  '{"type_name":"project","label":"Project","semantic_domain":"delivery","description":"A delivery project"}'::jsonb
);

SELECT mb_ontology.register_entity_type(
  'ontology-b',
  '{"type_name":"task","label":"Task Card","semantic_domain":"operations","description":"A conflicting task definition"}'::jsonb
);

SELECT mb_ontology.register_relation_type(
  'ontology-a',
  '{"relation_name":"depends_on","source_type":"task","target_type":"project","semantic_domain":"delivery"}'::jsonb
);

SELECT mb_ontology.register_relation_type(
  'ontology-b',
  '{"relation_name":"depends_on","source_type":"task","target_type":"project","semantic_domain":"delivery"}'::jsonb
);

DO $$
DECLARE
  v_count int;
  v_payload jsonb;
BEGIN
  SELECT COUNT(*) INTO v_count
  FROM mb_ontology.list_entity_types('ontology-a');

  IF v_count <> 2 THEN
    RAISE EXCEPTION 'FAIL: expected 2 entity types for ontology-a, got %', v_count;
  END IF;

  SELECT mb_ontology.compare_workspaces('ontology-a', 'ontology-b') INTO v_payload;

  IF (v_payload->'entity_type_counts'->>'shared')::int < 2 THEN
    RAISE EXCEPTION 'FAIL: compare_workspaces did not detect shared entity types: %', v_payload;
  END IF;
END;
$$;

DO $$
DECLARE
  v_payload jsonb;
BEGIN
  SELECT mb_ontology.find_entity_bridges('ontology-a', 'ontology-b', 0.7) INTO v_payload;

  IF jsonb_array_length(v_payload) < 2 THEN
    RAISE EXCEPTION 'FAIL: find_entity_bridges returned too few matches: %', v_payload;
  END IF;

  SELECT mb_ontology.bridge_workspaces('ontology-a', 'ontology-b', 'semantic') INTO v_payload;

  IF (v_payload->>'bridge_count')::int < 2 THEN
    RAISE EXCEPTION 'FAIL: bridge_workspaces returned too few bridges: %', v_payload;
  END IF;
END;
$$;

DO $$
DECLARE
  v_payload jsonb;
BEGIN
  SELECT mb_ontology.detect_conflicts(ARRAY['ontology-a', 'ontology-b']) INTO v_payload;

  IF jsonb_array_length(v_payload) < 1 THEN
    RAISE EXCEPTION 'FAIL: detect_conflicts returned no conflicts';
  END IF;

  SELECT mb_ontology.federated_search('project', ARRAY['ontology-a', 'ontology-b'], 'hybrid') INTO v_payload;

  IF jsonb_array_length(v_payload->'results') < 2 THEN
    RAISE EXCEPTION 'FAIL: federated_search returned too few results: %', v_payload;
  END IF;
END;
$$;

\echo 'Phase 2 ontology registry tests passed'
