//! DgraphClient - Rust client for the pg_dgraph PostgreSQL extension.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio_postgres::Client;

// ============================================================================
// Data types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub id: i64,
    pub entity_type: String,
    pub name: String,
    pub confidence: f32,
    pub metadata: Option<HashMap<String, serde_json::Value>>,
    pub deprecated_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relation {
    pub id: i64,
    pub relation_type: String,
    pub source_id: i64,
    pub target_id: i64,
    pub confidence: f32,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntitySearchResult {
    pub entity_id: i64,
    pub name: String,
    pub entity_type: String,
    pub confidence: f32,
    pub fts_rank: f32,
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketplaceResult {
    pub entity_id: i64,
    pub name: String,
    pub entity_type: String,
    pub confidence: f32,
    pub fts_rank: f32,
    pub is_direct_match: bool,
    pub hub_score: f32,
    pub composite_score: f32,
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillDependency {
    pub dep_entity_id: i64,
    pub dep_name: String,
    pub dep_type: String,
    pub dep_confidence: f32,
    pub relation_type: String,
    pub depth: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeighborEntity {
    pub id: i64,
    pub name: String,
    #[serde(rename = "type")]
    pub entity_type: String,
    pub confidence: f32,
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NeighborEdge {
    pub target_id: Option<i64>,
    pub target_name: Option<String>,
    pub source_id: Option<i64>,
    pub source_name: Option<String>,
    pub edge_type: String,
    pub confidence: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Neighborhood {
    pub entity: NeighborEntity,
    pub outgoing: Vec<NeighborEdge>,
    pub incoming: Vec<NeighborEdge>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConceptInput {
    #[serde(rename = "type")]
    pub entity_type: String,
    pub name: String,
    pub confidence: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationInput {
    pub source: String,
    pub target: String,
    #[serde(rename = "type")]
    pub relation_type: String,
    pub confidence: f32,
}

// Fix serde rename for JSON from PostgreSQL (uses "type" not "entity_type")
impl<'de> Deserialize<'de> for NeighborEdge {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            target_id: Option<i64>,
            target_name: Option<String>,
            source_id: Option<i64>,
            source_name: Option<String>,
            #[serde(rename = "type")]
            edge_type: String,
            confidence: f32,
        }
        let h = Helper::deserialize(deserializer)?;
        Ok(NeighborEdge {
            target_id: h.target_id,
            target_name: h.target_name,
            source_id: h.source_id,
            source_name: h.source_name,
            edge_type: h.edge_type,
            confidence: h.confidence,
        })
    }
}


// ============================================================================
// Client
// ============================================================================

pub struct DgraphClient {
    client: Client,
}

impl DgraphClient {
    pub fn new(client: Client) -> Self {
        DgraphClient { client }
    }

    // ------------------------------------------------------------------------
    // Entity management
    // ------------------------------------------------------------------------

    pub async fn upsert_entity(
        &self,
        entity_type: &str,
        name: &str,
        confidence: f32,
        metadata: Option<&HashMap<String, serde_json::Value>>,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let meta_json = metadata.map(|m| serde_json::to_string(m).unwrap());
        let row = self
            .client
            .query_one(
                "SELECT graph.upsert_entity($1, $2, $3::real, $4::jsonb)",
                &[&entity_type, &name, &confidence, &meta_json],
            )
            .await?;
        let id: i64 = row.get(0);
        Ok(id)
    }

    pub async fn get_entity(&self, id: i64) -> Result<Option<Entity>, Box<dyn std::error::Error + Send + Sync>> {
        let rows = self
            .client
            .query(
                "SELECT id, type, name, confidence, metadata, deprecated_at, created_at FROM graph.entity WHERE id = $1",
                &[&id],
            )
            .await?;
        if rows.is_empty() {
            return Ok(None);
        }
        let row = &rows[0];
        let metadata: Option<serde_json::Value> = row.get(4);
        let metadata_map = metadata.and_then(|v| serde_json::from_value(v).ok());
        Ok(Some(Entity {
            id: row.get(0),
            entity_type: row.get(1),
            name: row.get(2),
            confidence: row.get(3),
            metadata: metadata_map,
            deprecated_at: row.get(5),
            created_at: row.get(6),
        }))
    }

    pub async fn find_entities_by_type(
        &self,
        entity_type: &str,
    ) -> Result<Vec<Entity>, Box<dyn std::error::Error + Send + Sync>> {
        let rows = self
            .client
            .query(
                "SELECT id, type, name, confidence, metadata, deprecated_at, created_at FROM graph.entity WHERE type = $1 AND deprecated_at IS NULL ORDER BY confidence DESC",
                &[&entity_type],
            )
            .await?;
        let mut entities = Vec::with_capacity(rows.len());
        for row in rows {
            let metadata: Option<serde_json::Value> = row.get(4);
            let metadata_map = metadata.and_then(|v| serde_json::from_value(v).ok());
            entities.push(Entity {
                id: row.get(0),
                entity_type: row.get(1),
                name: row.get(2),
                confidence: row.get(3),
                metadata: metadata_map,
                deprecated_at: row.get(5),
                created_at: row.get(6),
            });
        }
        Ok(entities)
    }

    pub async fn deprecate_entity(&self, id: i64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.client
            .execute("UPDATE graph.entity SET deprecated_at = now() WHERE id = $1", &[&id])
            .await?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Relation management
    // ------------------------------------------------------------------------

    pub async fn upsert_relation(
        &self,
        rel_type: &str,
        source_id: i64,
        target_id: i64,
        confidence: f32,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let row = self
            .client
            .query_one(
                "SELECT graph.upsert_relation($1, $2, $3, $4::real)",
                &[&rel_type, &source_id, &target_id, &confidence],
            )
            .await?;
        let id: i64 = row.get(0);
        Ok(id)
    }

    pub async fn get_relations_from(
        &self,
        source_id: i64,
    ) -> Result<Vec<Relation>, Box<dyn std::error::Error + Send + Sync>> {
        let rows = self
            .client
            .query(
                "SELECT id, type, source_id, target_id, confidence, created_at FROM graph.relation WHERE source_id = $1 AND deprecated_at IS NULL ORDER BY confidence DESC",
                &[&source_id],
            )
            .await?;
        let mut rels = Vec::with_capacity(rows.len());
        for row in rows {
            rels.push(Relation {
                id: row.get(0),
                relation_type: row.get(1),
                source_id: row.get(2),
                target_id: row.get(3),
                confidence: row.get(4),
                created_at: row.get(5),
            });
        }
        Ok(rels)
    }

    // ------------------------------------------------------------------------
    // Alias management
    // ------------------------------------------------------------------------

    pub async fn register_aliases(
        &self,
        entity_id: i64,
        terms: &[String],
        confidence: f32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.client
            .execute("SELECT graph.register_aliases($1, $2, $3::real)", &[&entity_id, &terms, &confidence])
            .await?;
        Ok(())
    }

    pub async fn resolve_terms(
        &self,
        terms: &[String],
        min_confidence: f32,
    ) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
        let row = self
            .client
            .query_one(
                "SELECT COALESCE(rb_to_array(graph.resolve_terms($1, $2::real)), ARRAY[]::int[])",
                &[&terms, &min_confidence],
            )
            .await?;
        let ids: Vec<i32> = row.get(0);
        Ok(ids.into_iter().map(i64::from).collect())
    }

    // ------------------------------------------------------------------------
    // Search
    // ------------------------------------------------------------------------

    pub async fn entity_fts_search(
        &self,
        query: &str,
        type_filter: Option<&[String]>,
        domain: Option<&str>,
        min_confidence: f32,
        limit: i32,
    ) -> Result<Vec<EntitySearchResult>, Box<dyn std::error::Error + Send + Sync>> {
        let limit = if limit <= 0 { 20 } else { limit };
        let rows = self
            .client
            .query(
                "SELECT entity_id, name, type, confidence, fts_rank, metadata FROM graph.entity_fts_search($1, $2, $3, $4::real, $5)",
                &[&query, &type_filter, &domain, &min_confidence, &limit],
            )
            .await?;
        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let metadata: Option<serde_json::Value> = row.get(5);
            let metadata_map = metadata.and_then(|v| serde_json::from_value(v).ok());
            results.push(EntitySearchResult {
                entity_id: row.get(0),
                name: row.get(1),
                entity_type: row.get(2),
                confidence: row.get(3),
                fts_rank: row.get(4),
                metadata: metadata_map,
            });
        }
        Ok(results)
    }

    pub async fn marketplace_search(
        &self,
        query: &str,
        domain: Option<&str>,
        min_confidence: f32,
        max_hops: i32,
        limit: i32,
    ) -> Result<Vec<MarketplaceResult>, Box<dyn std::error::Error + Send + Sync>> {
        let max_hops = if max_hops <= 0 { 2 } else { max_hops };
        let limit = if limit <= 0 { 20 } else { limit };
        let rows = self
            .client
            .query(
                "SELECT entity_id, name, type, confidence, fts_rank, is_direct_match, hub_score, composite_score, metadata FROM graph.marketplace_search($1, $2, $3::real, $4, $5)",
                &[&query, &domain, &min_confidence, &max_hops, &limit],
            )
            .await?;
        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let metadata: Option<serde_json::Value> = row.get(8);
            let metadata_map = metadata.and_then(|v| serde_json::from_value(v).ok());
            results.push(MarketplaceResult {
                entity_id: row.get(0),
                name: row.get(1),
                entity_type: row.get(2),
                confidence: row.get(3),
                fts_rank: row.get(4),
                is_direct_match: row.get(5),
                hub_score: row.get(6),
                composite_score: row.get(7),
                metadata: metadata_map,
            });
        }
        Ok(results)
    }

    // ------------------------------------------------------------------------
    // Graph traversal
    // ------------------------------------------------------------------------

    pub async fn k_hops_filtered(
        &self,
        seed_ids: &[i64],
        max_hops: i32,
        edge_types: Option<&[String]>,
    ) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
        if seed_ids.is_empty() {
            return Ok(vec![]);
        }
        let int_ids: Vec<i32> = seed_ids.iter().map(|&id| id as i32).collect();
        let row = self
            .client
            .query_one(
                "SELECT COALESCE(rb_to_array(k_hops_filtered(rb_build($1::int[]), $2, $3)), ARRAY[]::int[])",
                &[&int_ids, &max_hops, &edge_types],
            )
            .await?;
        let ids: Vec<i32> = row.get(0);
        Ok(ids.into_iter().map(i64::from).collect())
    }

    pub async fn shortest_path_filtered(
        &self,
        src_id: i64,
        dest_id: i64,
        edge_types: Option<&[String]>,
        max_depth: i32,
    ) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
        let max_depth = if max_depth <= 0 { 20 } else { max_depth };
        let row = self
            .client
            .query_opt(
                "SELECT shortest_path_filtered($1::int, $2::int, $3, NULL, NULL, NULL, NULL, 0.0::real, NULL, $4)",
                &[&src_id, &dest_id, &edge_types, &max_depth],
            )
            .await?;
        match row {
            Some(r) => {
                let path_len: Option<i32> = r.get(0);
                Ok(path_len.unwrap_or(-1))
            }
            None => Ok(-1),
        }
    }

    // ------------------------------------------------------------------------
    // Learning pipeline
    // ------------------------------------------------------------------------

    pub async fn learn_from_run(
        &self,
        run_key: &str,
        domain: &str,
        outcome: &str,
        concepts: &[ConceptInput],
        relations: &[RelationInput],
        transcript: Option<&str>,
        run_meta: Option<&HashMap<String, serde_json::Value>>,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let concepts_json = serde_json::to_string(concepts)?;
        let relations_json = serde_json::to_string(relations)?;
        let meta_json = run_meta.map(|m| serde_json::to_string(m).unwrap());
        let row = self
            .client
            .query_one(
                "SELECT graph.learn_from_run($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7::jsonb)",
                &[&run_key, &domain, &outcome, &concepts_json, &relations_json, &transcript, &meta_json],
            )
            .await?;
        let id: i64 = row.get(0);
        Ok(id)
    }

    // ------------------------------------------------------------------------
    // Analytics & maintenance
    // ------------------------------------------------------------------------

    pub async fn skill_dependencies(
        &self,
        entity_id: i64,
        max_depth: i32,
        min_confidence: f32,
    ) -> Result<Vec<SkillDependency>, Box<dyn std::error::Error + Send + Sync>> {
        let max_depth = if max_depth <= 0 { 5 } else { max_depth };
        let rows = self
            .client
            .query(
                "SELECT dep_entity_id, dep_name, dep_type, dep_confidence, relation_type, depth FROM graph.skill_dependencies($1, $2, $3::real)",
                &[&entity_id, &max_depth, &min_confidence],
            )
            .await?;
        let mut deps = Vec::with_capacity(rows.len());
        for row in rows {
            deps.push(SkillDependency {
                dep_entity_id: row.get(0),
                dep_name: row.get(1),
                dep_type: row.get(2),
                dep_confidence: row.get(3),
                relation_type: row.get(4),
                depth: row.get(5),
            });
        }
        Ok(deps)
    }

    pub async fn entity_neighborhood(
        &self,
        entity_id: i64,
        max_out: i32,
        max_in: i32,
        min_confidence: f32,
    ) -> Result<Neighborhood, Box<dyn std::error::Error + Send + Sync>> {
        let max_out = if max_out <= 0 { 10 } else { max_out };
        let max_in = if max_in <= 0 { 10 } else { max_in };
        let row = self
            .client
            .query_one(
                "SELECT graph.entity_neighborhood($1, $2, $3, $4::real)",
                &[&entity_id, &max_out, &max_in, &min_confidence],
            )
            .await?;
        let raw: serde_json::Value = row.get(0);
        let neighborhood: Neighborhood = serde_json::from_value(raw)?;
        Ok(neighborhood)
    }

    pub async fn confidence_decay(
        &self,
        entity_id: i64,
        half_life_days: i32,
    ) -> Result<f32, Box<dyn std::error::Error + Send + Sync>> {
        let half_life_days = if half_life_days <= 0 { 90 } else { half_life_days };
        let row = self
            .client
            .query_one("SELECT graph.confidence_decay($1, $2)", &[&entity_id, &half_life_days])
            .await?;
        let conf: f32 = row.get(0);
        Ok(conf)
    }

    pub async fn rebuild_lj_relations(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.client.execute("SELECT graph.rebuild_lj_relations()", &[]).await?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Extension helpers
    // ------------------------------------------------------------------------

    pub async fn extension_version(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let row = self
            .client
            .query_one("SELECT extversion FROM pg_extension WHERE extname = 'pg_dgraph'", &[])
            .await?;
        let version: String = row.get(0);
        Ok(version)
    }

    pub async fn ensure_extension(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.client
            .execute("CREATE EXTENSION IF NOT EXISTS roaringbitmap", &[])
            .await?;
        self.client
            .execute("CREATE EXTENSION IF NOT EXISTS pg_dgraph", &[])
            .await?;
        Ok(())
    }

    /// Cleanup test data by name prefix. For use in integration tests.
    pub async fn cleanup_test_data(&self, prefix: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pattern = format!("{}%", prefix);
        self.client
            .execute("DELETE FROM graph.entity WHERE name LIKE $1", &[&pattern])
            .await?;
        self.client
            .execute("DELETE FROM graph.execution_run WHERE run_key LIKE $1", &[&pattern])
            .await?;
        Ok(())
    }
}
