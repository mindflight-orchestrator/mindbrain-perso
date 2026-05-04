// Example: using pg_pragma with the Proposition DSL from Go.
//
// Run: go run example.go
// Requires: POSTGRES_DSN env (e.g. postgres://user:pass@localhost:5432/mindbrain)
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://mindbrain:mindbrain@localhost:5432/mindbrain?sslmode=disable"
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	// 1. Ensure extension
	if _, err := db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS pg_pragma"); err != nil {
		log.Printf("pg_pragma extension: %v (may already exist)", err)
	}

	// 2. Parse a DSL line
	line := `fact|id=f42|subject=offline_sync|predicate=blocked_by|object=ios_background_limits|conf=0.91`
	var parsed []byte
	err = db.QueryRowContext(ctx, "SELECT pragma_parse_proposition_line($1)", line).Scan(&parsed)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Parsed: %s\n", string(parsed))

	// 3. Ensure schema and tables exist (minimal for example)
	_, _ = db.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS "memory-server"`)
	_, _ = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS "memory-server".memory_items (
		id TEXT PRIMARY KEY, user_id TEXT NOT NULL, source_type TEXT, source_ref TEXT,
		importance FLOAT8 DEFAULT 0, confidence FLOAT8 DEFAULT 0,
		created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW()
	)`)
	_, _ = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS "memory-server".memory_projections (
		id TEXT PRIMARY KEY, item_id TEXT NOT NULL, user_id TEXT NOT NULL,
		projection_type TEXT NOT NULL, content TEXT NOT NULL,
		content_tsvector TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', content)) STORED,
		facets JSONB DEFAULT '{}', metadata JSONB DEFAULT '{}',
		rank_hint FLOAT8 DEFAULT 0, confidence FLOAT8 DEFAULT 0,
		created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW()
	)`)

	// 4. Insert proposition DSL content
	userID := "user_example"
	itemID := "item_1"
	projID := "proj_1"
	content := `fact|id=f1|subject=user|predicate=wants|object=offline_sync|conf=0.95
constraint|id=c1|scope=memory|rule=keep_context_small
goal|id=g1|actor=user|wants=dynamic_memory`

	_, err = db.ExecContext(ctx, `INSERT INTO "memory-server".memory_items (id, user_id, source_type, source_ref) VALUES ($1, $2, 'event', 'evt_1')
		ON CONFLICT (id) DO NOTHING`, itemID, userID)
	if err != nil {
		log.Printf("insert item: %v", err)
	}

	_, err = db.ExecContext(ctx, `INSERT INTO "memory-server".memory_projections (id, item_id, user_id, projection_type, content, rank_hint)
		VALUES ($1, $2, $3, 'proposition', $4, 0.9)
		ON CONFLICT (id) DO UPDATE SET content = EXCLUDED.content`,
		projID, itemID, userID, content)
	if err != nil {
		log.Printf("insert projection: %v", err)
	}

	// 5. Pack context (uses pg_pragma when available)
	rows, err := db.QueryContext(ctx, `SELECT id, item_id, projection_type, content FROM pragma_pack_context($1, $2, 5)`,
		userID, "offline")
	if err != nil {
		log.Printf("pack_context: %v", err)
		return
	}
	defer rows.Close()
	fmt.Println("Pack context results:")
	for rows.Next() {
		var id, itemID, projType, cnt string
		if err := rows.Scan(&id, &itemID, &projType, &cnt); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  %s | %s | %s | %q\n", id, itemID, projType, cnt[:min(60, len(cnt))])
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
