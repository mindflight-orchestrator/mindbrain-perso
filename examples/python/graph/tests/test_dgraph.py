"""Integration tests for the pg_dgraph Python client."""

import os
import time

import pytest
import psycopg

from pgdgraph import (
    ConceptInput,
    DgraphClient,
    RelationInput,
)

DEFAULT_DSN = "postgres://postgres:postgres@localhost:5436/postgres?sslmode=disable"


def get_conn():
    dsn = os.environ.get("TEST_DATABASE_URL", DEFAULT_DSN)
    return psycopg.connect(dsn)


def unique_name(base: str) -> str:
    return f"{base}-{int(time.time() * 1e9)}"


@pytest.fixture
def client():
    try:
        conn = get_conn()
    except Exception as e:
        if os.environ.get("PG_DGRAPH_TEST_FAIL_ON_NO_DB") == "true":
            pytest.fail(f"Failed to connect to database: {e}")
        pytest.skip(f"No database available: {e}")
    try:
        c = DgraphClient(conn)
        c.ensure_extension()
        yield c
    finally:
        conn.close()


def test_extension_present(client):
    version = client.extension_version()
    assert version


def test_entity_lifecycle(client):
    prefix = "pytest-entity"
    name = unique_name(prefix)
    try:
        id = client.upsert_entity("skill", name, 0.7, {"domain": "test", "description": "Python test"})
        assert id > 0

        id1 = client.upsert_entity("skill", name, 0.9, None)
        id2 = client.upsert_entity("skill", name, 0.9, None)
        assert id1 == id2

        entity = client.get_entity(id1)
        assert entity is not None
        assert entity.confidence >= 0.89

        entities = client.find_entities_by_type("skill")
        assert any(e.name == name for e in entities)

        client.deprecate_entity(id1)
        entities = client.find_entities_by_type("skill")
        assert not any(e.name == name for e in entities)
    finally:
        client.cleanup_test_data(prefix)


def test_relation_lifecycle(client):
    prefix = "pytest-rel"
    name_a = unique_name(f"{prefix}-A")
    name_b = unique_name(f"{prefix}-B")
    name_c = unique_name(f"{prefix}-C")
    try:
        id_a = client.upsert_entity("skill", name_a, 0.9, None)
        id_b = client.upsert_entity("concept", name_b, 0.85, None)
        id_c = client.upsert_entity("concept", name_c, 0.80, None)

        rel_id = client.upsert_relation("requires", id_a, id_b, 0.9)
        assert rel_id > 0

        client.upsert_relation("requires", id_b, id_c, 0.85)

        rels = client.get_relations_from(id_a)
        assert len(rels) > 0

        client.rebuild_lj_relations()

        visited = client.k_hops_filtered([id_a], 2, None)
        assert id_b in visited
        assert id_c in visited

        hops = client.shortest_path_filtered(id_a, id_c, None, 10)
        assert hops == 2
    finally:
        client.cleanup_test_data(prefix)


def test_alias_and_resolve(client):
    prefix = "pytest-alias"
    name = unique_name(prefix)
    try:
        id = client.upsert_entity("skill", name, 0.9, None)
        aliases = [f"{name} alias1", f"{name} alias2"]
        client.register_aliases(id, aliases, 0.9)

        ids = client.resolve_terms(aliases[:1], 0.0)
        assert id in ids
    finally:
        client.cleanup_test_data(prefix)


def test_learn_from_run(client):
    prefix = "pytest-lfr"
    run_key = f"{prefix}-run-{int(time.time() * 1e9)}"
    skill_name = unique_name(f"{prefix}-skill")
    concept_name = unique_name(f"{prefix}-concept")
    try:
        concepts = [
            ConceptInput(type="skill", name=skill_name, confidence=0.9, metadata={"domain": "test"}),
            ConceptInput(type="concept", name=concept_name, confidence=0.8, metadata={"domain": "test"}),
        ]
        relations = [
            RelationInput(source=skill_name, target=concept_name, type="requires", confidence=0.85),
        ]

        run_id = client.learn_from_run(
            run_key, "test", "success",
            concepts, relations,
            transcript="Integration test run.",
            run_meta=None,
        )
        assert run_id > 0

        run_id2 = client.learn_from_run(run_key, "test", "partial", concepts, relations, None, None)
        assert run_id == run_id2
    finally:
        client.cleanup_test_data(prefix)
