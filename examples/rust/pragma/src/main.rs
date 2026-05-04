// Example: using pg_pragma with the Proposition DSL from Rust.
//
// Run: cargo run
// Requires: POSTGRES_DSN env (e.g. postgres://user:pass@localhost:5432/mindbrain)
use tokio_postgres::{Client, NoTls};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = std::env::var("POSTGRES_DSN")
        .unwrap_or_else(|_| "postgres://mindbrain:mindbrain@localhost:5432/mindbrain".to_string());

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // 1. Ensure extension
    let _ = client
        .execute("CREATE EXTENSION IF NOT EXISTS pg_pragma", &[])
        .await;

    // 2. Parse a DSL line
    let line = "fact|id=f42|subject=offline_sync|predicate=blocked_by|object=ios_background_limits|conf=0.91";
    let row = client
        .query_one("SELECT pragma_parse_proposition_line($1)::text", &[&line])
        .await?;
    let parsed: String = row.get(0);
    println!("Parsed: {}", parsed);

    // 3. Ensure schema and tables
    setup_schema(&client).await?;

    // 4. Insert proposition DSL content
    let user_id = "user_example";
    let item_id = "item_1";
    let proj_id = "proj_1";
    let content = "fact|id=f1|subject=user|predicate=wants|object=offline_sync|conf=0.95\nconstraint|id=c1|scope=memory|rule=keep_context_small\ngoal|id=g1|actor=user|wants=dynamic_memory";

    let _ = client
        .execute(
            r#"INSERT INTO "memory-server".memory_items (id, user_id, source_type, source_ref) VALUES ($1, $2, 'event', 'evt_1')
               ON CONFLICT (id) DO NOTHING"#,
            &[&item_id, &user_id],
        )
        .await;

    let _ = client
        .execute(
            r#"INSERT INTO "memory-server".memory_projections (id, item_id, user_id, projection_type, content, rank_hint)
               VALUES ($1, $2, $3, 'proposition', $4, 0.9)
               ON CONFLICT (id) DO UPDATE SET content = EXCLUDED.content"#,
            &[&proj_id, &item_id, &user_id, &content],
        )
        .await;

    // 5. Pack context
    let rows = client
        .query(
            "SELECT id, item_id, projection_type, content FROM pragma_pack_context($1, $2, 5)",
            &[&user_id, &"offline"],
        )
        .await?;
    println!("Pack context results:");
    for row in rows {
        let id: String = row.get(0);
        let item_id: String = row.get(1);
        let proj_type: String = row.get(2);
        let cnt: String = row.get(3);
        let preview = if cnt.len() > 60 { &cnt[..60] } else { &cnt[..] };
        println!("  {} | {} | {} | {:?}", id, item_id, proj_type, preview);
    }

    Ok(())
}

async fn setup_schema(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    client
        .execute(r#"CREATE SCHEMA IF NOT EXISTS "memory-server""#, &[])
        .await?;
    client
        .execute(
            r#"CREATE TABLE IF NOT EXISTS "memory-server".memory_items (
            id TEXT PRIMARY KEY, user_id TEXT NOT NULL, source_type TEXT, source_ref TEXT,
            importance FLOAT8 DEFAULT 0, confidence FLOAT8 DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW()
        )"#,
            &[],
        )
        .await?;
    client
        .execute(
            r#"CREATE TABLE IF NOT EXISTS "memory-server".memory_projections (
            id TEXT PRIMARY KEY, item_id TEXT NOT NULL, user_id TEXT NOT NULL,
            projection_type TEXT NOT NULL, content TEXT NOT NULL,
            content_tsvector TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', content)) STORED,
            facets JSONB DEFAULT '{}', metadata JSONB DEFAULT '{}',
            rank_hint FLOAT8 DEFAULT 0, confidence FLOAT8 DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW()
        )"#,
            &[],
        )
        .await?;
    Ok(())
}
