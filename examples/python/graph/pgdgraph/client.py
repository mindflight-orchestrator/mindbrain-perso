"""
DgraphClient - Python client for the pg_dgraph PostgreSQL extension.

Provides a thin wrapper around the graph.* SQL functions.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class Entity:
    id: int
    type: str
    name: str
    confidence: float
    metadata: Optional[dict[str, Any]] = None
    deprecated_at: Optional[Any] = None
    created_at: Any = None


@dataclass
class Relation:
    id: int
    type: str
    source_id: int
    target_id: int
    confidence: float
    created_at: Any = None


@dataclass
class EntitySearchResult:
    entity_id: int
    name: str
    type: str
    confidence: float
    fts_rank: float
    metadata: Optional[dict[str, Any]] = None


@dataclass
class MarketplaceResult:
    entity_id: int
    name: str
    type: str
    confidence: float
    fts_rank: float
    is_direct_match: bool
    hub_score: float
    composite_score: float
    metadata: Optional[dict[str, Any]] = None


@dataclass
class SkillDependency:
    dep_entity_id: int
    dep_name: str
    dep_type: str
    dep_confidence: float
    relation_type: str
    depth: int


@dataclass
class NeighborEntity:
    id: int
    name: str
    type: str
    confidence: float
    metadata: Optional[dict[str, Any]] = None


@dataclass
class NeighborEdge:
    target_id: Optional[int] = None
    target_name: Optional[str] = None
    source_id: Optional[int] = None
    source_name: Optional[str] = None
    type: str = ""
    confidence: float = 0.0


@dataclass
class Neighborhood:
    entity: NeighborEntity
    outgoing: list[NeighborEdge] = field(default_factory=list)
    incoming: list[NeighborEdge] = field(default_factory=list)


@dataclass
class ConceptInput:
    type: str
    name: str
    confidence: float
    metadata: Optional[dict[str, Any]] = None


@dataclass
class RelationInput:
    source: str
    target: str
    type: str
    confidence: float


class DgraphClient:
    """Thin Python client over the pg_dgraph SQL API."""

    def __init__(self, conn) -> None:
        """Create client from a psycopg connection or cursor factory."""
        self._conn = conn

    def _cursor(self):
        return self._conn.cursor()

    def ensure_extension(self) -> None:
        """Create roaringbitmap and pg_dgraph extensions if not present."""
        with self._cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS roaringbitmap")
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_dgraph")
        self._conn.commit()

    def extension_version(self) -> str:
        """Return the installed pg_dgraph version string."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT extversion FROM pg_extension WHERE extname = 'pg_dgraph'"
            )
            row = cur.fetchone()
            if not row:
                raise ValueError("pg_dgraph extension not found")
            return row[0]

    def upsert_entity(
        self,
        entity_type: str,
        name: str,
        confidence: float,
        metadata: Optional[dict[str, Any]] = None,
    ) -> int:
        """Create or merge entity. Returns entity ID."""
        meta_json = json.dumps(metadata) if metadata else None
        with self._cursor() as cur:
            cur.execute(
                "SELECT graph.upsert_entity(%s, %s, %s::real, %s::jsonb)",
                (entity_type, name, confidence, meta_json),
            )
            return cur.fetchone()[0]

    def get_entity(self, id: int) -> Optional[Entity]:
        """Get entity by ID. Returns None if not found."""
        with self._cursor() as cur:
            cur.execute(
                """SELECT id, type, name, confidence, metadata, deprecated_at, created_at
                   FROM graph.entity WHERE id = %s""",
                (id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            meta = json.loads(row[4]) if row[4] else None
            return Entity(
                id=row[0],
                type=row[1],
                name=row[2],
                confidence=row[3],
                metadata=meta,
                deprecated_at=row[5],
                created_at=row[6],
            )

    def find_entities_by_type(self, entity_type: str) -> list[Entity]:
        """Return all active entities of the given type."""
        with self._cursor() as cur:
            cur.execute(
                """SELECT id, type, name, confidence, metadata, deprecated_at, created_at
                   FROM graph.entity
                   WHERE type = %s AND deprecated_at IS NULL
                   ORDER BY confidence DESC""",
                (entity_type,),
            )
            rows = cur.fetchall()
        return [
            Entity(
                id=r[0],
                type=r[1],
                name=r[2],
                confidence=r[3],
                metadata=json.loads(r[4]) if r[4] else None,
                deprecated_at=r[5],
                created_at=r[6],
            )
            for r in rows
        ]

    def deprecate_entity(self, id: int) -> None:
        """Mark entity as deprecated."""
        with self._cursor() as cur:
            cur.execute(
                "UPDATE graph.entity SET deprecated_at = now() WHERE id = %s",
                (id,),
            )
        self._conn.commit()

    def upsert_relation(
        self,
        rel_type: str,
        source_id: int,
        target_id: int,
        confidence: float,
    ) -> int:
        """Create or update directed relation. Returns relation ID."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT graph.upsert_relation(%s, %s, %s, %s::real)",
                (rel_type, source_id, target_id, confidence),
            )
            return cur.fetchone()[0]

    def get_relations_from(self, source_id: int) -> list[Relation]:
        """Return all active outgoing relations from the entity."""
        with self._cursor() as cur:
            cur.execute(
                """SELECT id, type, source_id, target_id, confidence, created_at
                   FROM graph.relation
                   WHERE source_id = %s AND deprecated_at IS NULL
                   ORDER BY confidence DESC""",
                (source_id,),
            )
            rows = cur.fetchall()
        return [
            Relation(
                id=r[0],
                type=r[1],
                source_id=r[2],
                target_id=r[3],
                confidence=r[4],
                created_at=r[5],
            )
            for r in rows
        ]

    def register_aliases(
        self,
        entity_id: int,
        terms: list[str],
        confidence: float,
    ) -> None:
        """Map terms to canonical entity."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT graph.register_aliases(%s, %s, %s::real)",
                (entity_id, terms, confidence),
            )
        self._conn.commit()

    def resolve_terms(
        self,
        terms: list[str],
        min_confidence: float,
    ) -> list[int]:
        """Resolve text terms to entity IDs via alias matching."""
        with self._cursor() as cur:
            cur.execute(
                """SELECT COALESCE(
                    rb_to_array(graph.resolve_terms(%s, %s::real)),
                    ARRAY[]::int[]
                )""",
                (terms, min_confidence),
            )
            return list(cur.fetchone()[0] or [])

    def entity_fts_search(
        self,
        query: str,
        type_filter: Optional[list[str]] = None,
        domain: Optional[str] = None,
        min_confidence: float = 0.0,
        limit: int = 20,
    ) -> list[EntitySearchResult]:
        """Full-text search over entity name and metadata."""
        if limit <= 0:
            limit = 20
        with self._cursor() as cur:
            cur.execute(
                """SELECT entity_id, name, type, confidence, fts_rank, metadata
                   FROM graph.entity_fts_search(%s, %s, %s, %s::real, %s)""",
                (query, type_filter, domain, min_confidence, limit),
            )
            rows = cur.fetchall()
        return [
            EntitySearchResult(
                entity_id=r[0],
                name=r[1],
                type=r[2],
                confidence=r[3],
                fts_rank=r[4],
                metadata=json.loads(r[5]) if r[5] else None,
            )
            for r in rows
        ]

    def marketplace_search(
        self,
        query: str,
        domain: Optional[str] = None,
        min_confidence: float = 0.0,
        max_hops: int = 2,
        limit: int = 20,
    ) -> list[MarketplaceResult]:
        """Hybrid FTS + BFS + hub-degree scored search."""
        if max_hops <= 0:
            max_hops = 2
        if limit <= 0:
            limit = 20
        with self._cursor() as cur:
            cur.execute(
                """SELECT entity_id, name, type, confidence, fts_rank,
                          is_direct_match, hub_score, composite_score, metadata
                   FROM graph.marketplace_search(%s, %s, %s::real, %s, %s)""",
                (query, domain, min_confidence, max_hops, limit),
            )
            rows = cur.fetchall()
        return [
            MarketplaceResult(
                entity_id=r[0],
                name=r[1],
                type=r[2],
                confidence=r[3],
                fts_rank=r[4],
                is_direct_match=r[5],
                hub_score=r[6],
                composite_score=r[7],
                metadata=json.loads(r[8]) if r[8] else None,
            )
            for r in rows
        ]

    def k_hops_filtered(
        self,
        seed_ids: list[int],
        max_hops: int,
        edge_types: Optional[list[str]] = None,
    ) -> list[int]:
        """Find entities reachable within max_hops from seed set."""
        if not seed_ids:
            return []
        int_ids = [int(x) for x in seed_ids]
        with self._cursor() as cur:
            cur.execute(
                """SELECT COALESCE(
                    rb_to_array(k_hops_filtered(rb_build(%s::int[]), %s, %s)),
                    ARRAY[]::int[]
                )""",
                (int_ids, max_hops, edge_types),
            )
            row = cur.fetchone()
            ids = row[0] or []
            return [int(x) for x in ids]

    def shortest_path_filtered(
        self,
        src_id: int,
        dest_id: int,
        edge_types: Optional[list[str]] = None,
        max_depth: int = 20,
    ) -> int:
        """Shortest path length between two entities. Returns -1 if no path."""
        if max_depth <= 0:
            max_depth = 20
        with self._cursor() as cur:
            cur.execute(
                """SELECT shortest_path_filtered(
                    %s::int, %s::int, %s, NULL, NULL, NULL, NULL, 0.0::real, NULL, %s
                )""",
                (src_id, dest_id, edge_types, max_depth),
            )
            row = cur.fetchone()
            return row[0] if row[0] is not None else -1

    def learn_from_run(
        self,
        run_key: str,
        domain: str,
        outcome: str,
        concepts: list[ConceptInput],
        relations: list[RelationInput],
        transcript: Optional[str] = None,
        run_meta: Optional[dict[str, Any]] = None,
    ) -> int:
        """Record agent run and upsert concepts/relations. Returns execution_run.id."""
        concepts_json = json.dumps(
            [
                {
                    "type": c.type,
                    "name": c.name,
                    "confidence": c.confidence,
                    **({"metadata": c.metadata} if c.metadata else {}),
                }
                for c in concepts
            ]
        )
        relations_json = json.dumps(
            [
                {
                    "source": r.source,
                    "target": r.target,
                    "type": r.type,
                    "confidence": r.confidence,
                }
                for r in relations
            ]
        )
        meta_json = json.dumps(run_meta) if run_meta else None
        with self._cursor() as cur:
            cur.execute(
                """SELECT graph.learn_from_run(
                    %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s::jsonb
                )""",
                (run_key, domain, outcome, concepts_json, relations_json, transcript, meta_json),
            )
            return cur.fetchone()[0]

    def skill_dependencies(
        self,
        entity_id: int,
        max_depth: int = 5,
        min_confidence: float = 0.0,
    ) -> list[SkillDependency]:
        """Return transitive dependency tree of an entity."""
        if max_depth <= 0:
            max_depth = 5
        with self._cursor() as cur:
            cur.execute(
                """SELECT dep_entity_id, dep_name, dep_type, dep_confidence,
                          relation_type, depth
                   FROM graph.skill_dependencies(%s, %s, %s::real)""",
                (entity_id, max_depth, min_confidence),
            )
            rows = cur.fetchall()
        return [
            SkillDependency(
                dep_entity_id=r[0],
                dep_name=r[1],
                dep_type=r[2],
                dep_confidence=r[3],
                relation_type=r[4],
                depth=r[5],
            )
            for r in rows
        ]

    def entity_neighborhood(
        self,
        entity_id: int,
        max_out: int = 10,
        max_in: int = 10,
        min_confidence: float = 0.0,
    ) -> Neighborhood:
        """Return one-hop JSON neighborhood summary."""
        if max_out <= 0:
            max_out = 10
        if max_in <= 0:
            max_in = 10
        with self._cursor() as cur:
            cur.execute(
                "SELECT graph.entity_neighborhood(%s, %s, %s, %s::real)",
                (entity_id, max_out, max_in, min_confidence),
            )
            raw = cur.fetchone()[0]
        data = raw if isinstance(raw, dict) else json.loads(raw)
        entity_data = data.get("entity", {})
        entity = NeighborEntity(
            id=entity_data.get("id", 0),
            name=entity_data.get("name", ""),
            type=entity_data.get("type", ""),
            confidence=entity_data.get("confidence", 0.0),
            metadata=entity_data.get("metadata"),
        )
        outgoing = [
            NeighborEdge(
                target_id=e.get("target_id"),
                target_name=e.get("target_name"),
                source_id=e.get("source_id"),
                source_name=e.get("source_name"),
                type=e.get("type", ""),
                confidence=e.get("confidence", 0.0),
            )
            for e in data.get("outgoing", [])
        ]
        incoming = [
            NeighborEdge(
                target_id=e.get("target_id"),
                target_name=e.get("target_name"),
                source_id=e.get("source_id"),
                source_name=e.get("source_name"),
                type=e.get("type", ""),
                confidence=e.get("confidence", 0.0),
            )
            for e in data.get("incoming", [])
        ]
        return Neighborhood(entity=entity, outgoing=outgoing, incoming=incoming)

    def confidence_decay(
        self,
        entity_id: int,
        half_life_days: int = 90,
    ) -> float:
        """Return time-decayed confidence for an entity."""
        if half_life_days <= 0:
            half_life_days = 90
        with self._cursor() as cur:
            cur.execute(
                "SELECT graph.confidence_decay(%s, %s)",
                (entity_id, half_life_days),
            )
            return cur.fetchone()[0]

    def rebuild_lj_relations(self) -> None:
        """Rebuild bitmap adjacency indexes."""
        with self._cursor() as cur:
            cur.execute("SELECT graph.rebuild_lj_relations()")
        self._conn.commit()

    def cleanup_test_data(self, prefix: str) -> None:
        """Cleanup test data by name prefix. For integration tests."""
        with self._cursor() as cur:
            cur.execute("DELETE FROM graph.entity WHERE name LIKE %s", (f"{prefix}%",))
            cur.execute("DELETE FROM graph.execution_run WHERE run_key LIKE %s", (f"{prefix}%",))
        self._conn.commit()
