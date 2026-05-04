//! Integration tests for the pg_dgraph Rust client.
//!
//! Run with: ./run_tests.sh or make test
//! Or: TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5436/postgres?sslmode=disable" cargo test

use pgdgraph::*;
use std::collections::HashMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_postgres::NoTls;

const DEFAULT_DSN: &str = "postgres://postgres:postgres@localhost:5436/postgres?sslmode=disable";

fn unique_name(base: &str) -> String {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{}-{}", base, nanos)
}

#[tokio::test]
async fn test_extension_present() {
    let dsn = env::var("TEST_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DSN.to_string());
    let (client, connection) = match tokio_postgres::connect(&dsn, NoTls).await {
        Ok(c) => c,
        Err(e) => {
            if env::var("PG_DGRAPH_TEST_FAIL_ON_NO_DB").unwrap_or_default() == "true" {
                panic!("Failed to connect: {}", e);
            }
            eprintln!("Skipping: no database ({})", e);
            return;
        }
    };
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let dgraph = DgraphClient::new(client);
    dgraph.ensure_extension().await.expect("ensure_extension");
    let version = dgraph.extension_version().await.expect("extension_version");
    assert!(!version.is_empty(), "expected non-empty version");
}


#[tokio::test]
async fn test_entity_lifecycle() {
    let dsn = env::var("TEST_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DSN.to_string());
    let (client, connection) = match tokio_postgres::connect(&dsn, NoTls).await {
        Ok(c) => c,
        Err(e) => {
            if env::var("PG_DGRAPH_TEST_FAIL_ON_NO_DB").unwrap_or_default() == "true" {
                panic!("Failed to connect: {}", e);
            }
            eprintln!("Skipping: no database ({})", e);
            return;
        }
    };
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let dgraph = DgraphClient::new(client);
    dgraph.ensure_extension().await.expect("ensure_extension");

    let prefix = "rusttest-entity";
    let name = unique_name(prefix);

    // UpsertNew
    let mut meta = HashMap::new();
    meta.insert("domain".to_string(), serde_json::json!("test"));
    meta.insert("description".to_string(), serde_json::json!("Rust test entity"));
    let id = dgraph
        .upsert_entity("skill", &name, 0.7, Some(&meta))
        .await
        .expect("upsert_entity");
    assert!(id > 0, "expected positive ID");

    // UpsertIdempotent
    let id1 = dgraph.upsert_entity("skill", &name, 0.9, None).await.expect("upsert 2");
    let id2 = dgraph.upsert_entity("skill", &name, 0.9, None).await.expect("upsert 3");
    assert_eq!(id1, id2, "idempotent upsert should return same ID");

    let entity = dgraph.get_entity(id1).await.expect("get_entity").expect("entity exists");
    assert!(entity.confidence >= 0.89, "confidence should be >= 0.9");

    // FindByType
    let entities = dgraph.find_entities_by_type("skill").await.expect("find_entities_by_type");
    assert!(entities.iter().any(|e| e.name == name), "entity should be in results");

    // Deprecate
    dgraph.deprecate_entity(id1).await.expect("deprecate_entity");
    let entities = dgraph.find_entities_by_type("skill").await.expect("find");
    assert!(!entities.iter().any(|e| e.name == name), "deprecated entity should not appear");

    dgraph.cleanup_test_data(prefix).await.expect("cleanup");
}

#[tokio::test]
async fn test_relation_lifecycle() {
    let dsn = env::var("TEST_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DSN.to_string());
    let (client, connection) = match tokio_postgres::connect(&dsn, NoTls).await {
        Ok(c) => c,
        Err(e) => {
            if env::var("PG_DGRAPH_TEST_FAIL_ON_NO_DB").unwrap_or_default() == "true" {
                panic!("Failed to connect: {}", e);
            }
            eprintln!("Skipping: no database ({})", e);
            return;
        }
    };
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let dgraph = DgraphClient::new(client);
    dgraph.ensure_extension().await.expect("ensure_extension");

    let prefix = "rusttest-rel";
    let name_a = unique_name(&format!("{}-A", prefix));
    let name_b = unique_name(&format!("{}-B", prefix));
    let name_c = unique_name(&format!("{}-C", prefix));

    let id_a = dgraph.upsert_entity("skill", &name_a, 0.9, None).await.expect("upsert A");
    let id_b = dgraph.upsert_entity("concept", &name_b, 0.85, None).await.expect("upsert B");
    let id_c = dgraph.upsert_entity("concept", &name_c, 0.80, None).await.expect("upsert C");

    let rel_id = dgraph
        .upsert_relation("requires", id_a, id_b, 0.9)
        .await
        .expect("upsert_relation");
    assert!(rel_id > 0);

    dgraph.upsert_relation("requires", id_b, id_c, 0.85).await.expect("upsert B->C");

    let rels = dgraph.get_relations_from(id_a).await.expect("get_relations_from");
    assert!(!rels.is_empty(), "expected outgoing relations");

    dgraph.rebuild_lj_relations().await.expect("rebuild_lj");

    let visited = dgraph
        .k_hops_filtered(&[id_a], 2, None)
        .await
        .expect("k_hops_filtered");
    assert!(visited.contains(&id_b), "B should be in k-hop result");
    assert!(visited.contains(&id_c), "C should be in 2-hop result");

    let hops = dgraph
        .shortest_path_filtered(id_a, id_c, None, 10)
        .await
        .expect("shortest_path");
    assert_eq!(hops, 2, "expected 2 hops A->B->C");

    dgraph.cleanup_test_data(prefix).await.expect("cleanup");
}

#[tokio::test]
async fn test_alias_and_resolve() {
    let dsn = env::var("TEST_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DSN.to_string());
    let (client, connection) = match tokio_postgres::connect(&dsn, NoTls).await {
        Ok(c) => c,
        Err(e) => {
            if env::var("PG_DGRAPH_TEST_FAIL_ON_NO_DB").unwrap_or_default() == "true" {
                panic!("Failed to connect: {}", e);
            }
            eprintln!("Skipping: no database ({})", e);
            return;
        }
    };
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let dgraph = DgraphClient::new(client);
    dgraph.ensure_extension().await.expect("ensure_extension");

    let prefix = "rusttest-alias";
    let name = unique_name(prefix);
    let id = dgraph.upsert_entity("skill", &name, 0.9, None).await.expect("upsert");

    let aliases = vec![format!("{} alias1", name), format!("{} alias2", name)];
    dgraph
        .register_aliases(id, &aliases, 0.9)
        .await
        .expect("register_aliases");

    let ids = dgraph
        .resolve_terms(&aliases[..1], 0.0)
        .await
        .expect("resolve_terms");
    assert!(ids.contains(&id), "resolve_terms should return entity ID");

    dgraph.cleanup_test_data(prefix).await.expect("cleanup");
}

#[tokio::test]
async fn test_learn_from_run() {
    let dsn = env::var("TEST_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DSN.to_string());
    let (client, connection) = match tokio_postgres::connect(&dsn, NoTls).await {
        Ok(c) => c,
        Err(e) => {
            if env::var("PG_DGRAPH_TEST_FAIL_ON_NO_DB").unwrap_or_default() == "true" {
                panic!("Failed to connect: {}", e);
            }
            eprintln!("Skipping: no database ({})", e);
            return;
        }
    };
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let dgraph = DgraphClient::new(client);
    dgraph.ensure_extension().await.expect("ensure_extension");

    let prefix = "rusttest-lfr";
    let run_key = format!("{}-run-{}", prefix, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos());
    let skill_name = unique_name(&format!("{}-skill", prefix));
    let concept_name = unique_name(&format!("{}-concept", prefix));

    let mut meta = HashMap::new();
    meta.insert("domain".to_string(), serde_json::json!("test"));
    let concepts = vec![
        ConceptInput {
            entity_type: "skill".to_string(),
            name: skill_name.clone(),
            confidence: 0.9,
            metadata: Some(meta.clone()),
        },
        ConceptInput {
            entity_type: "concept".to_string(),
            name: concept_name.clone(),
            confidence: 0.8,
            metadata: Some(meta),
        },
    ];
    let relations = vec![RelationInput {
        source: skill_name.clone(),
        target: concept_name.clone(),
        relation_type: "requires".to_string(),
        confidence: 0.85,
    }];

    let run_id = dgraph
        .learn_from_run(
            &run_key,
            "test",
            "success",
            &concepts,
            &relations,
            Some("Integration test run."),
            None,
        )
        .await
        .expect("learn_from_run");
    assert!(run_id > 0);

    // Idempotent
    let run_id2 = dgraph
        .learn_from_run(&run_key, "test", "partial", &concepts, &relations, None, None)
        .await
        .expect("learn_from_run idempotent");
    assert_eq!(run_id, run_id2);

    dgraph.cleanup_test_data(prefix).await.expect("cleanup");
}
