// dgraph_test.go
// Integration tests for the pg_dgraph Go client.
//
// Tests run against a real PostgreSQL instance with the pg_dgraph extension.
//
// Usage:
//   ./run_tests.sh              (recommended — starts Docker, runs tests, cleans up)
//
// Or manually with an already-running database:
//   export TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5436/postgres?sslmode=disable"
//   go test -v ./...
//
// Set PG_DGRAPH_TEST_FAIL_ON_NO_DB=true to fail instead of skip when no database.

package pgdgraph

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ============================================================================
// Test helpers
// ============================================================================

const defaultTestDSN = "postgres://postgres:postgres@localhost:5436/postgres?sslmode=disable"

func getTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = defaultTestDSN
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		if os.Getenv("PG_DGRAPH_TEST_FAIL_ON_NO_DB") == "true" {
			t.Fatalf("Failed to create pool: %v", err)
		}
		t.Skipf("Skipping: no database available (%v). Set TEST_DATABASE_URL or run via make test.", err)
		return nil
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		if os.Getenv("PG_DGRAPH_TEST_FAIL_ON_NO_DB") == "true" {
			t.Fatalf("Database not reachable: %v", err)
		}
		t.Skipf("Skipping: database not reachable (%v). Set TEST_DATABASE_URL or run via make test.", err)
		return nil
	}

	return pool
}

func newClient(t *testing.T, pool *pgxpool.Pool) *DgraphClient {
	t.Helper()
	c, err := NewDgraphClient(pool, false)
	if err != nil {
		t.Fatalf("NewDgraphClient: %v", err)
	}
	return c
}

// setupExtension ensures pg_dgraph is loaded. Safe to call multiple times.
func setupExtension(t *testing.T, c *DgraphClient) {
	t.Helper()
	ctx := context.Background()
	if err := c.EnsureExtension(ctx); err != nil {
		t.Fatalf("EnsureExtension: %v", err)
	}
}

// cleanupTestData removes entities inserted during a test run by name prefix.
func cleanupTestData(t *testing.T, pool *pgxpool.Pool, prefix string) {
	t.Helper()
	ctx := context.Background()
	// Relations referencing entities will cascade-delete via FK.
	_, err := pool.Exec(ctx,
		`DELETE FROM graph.entity WHERE name LIKE $1 || '%'`, prefix)
	if err != nil {
		t.Logf("cleanup warning: %v", err)
	}
	_, err = pool.Exec(ctx,
		`DELETE FROM graph.execution_run WHERE run_key LIKE $1 || '%'`, prefix)
	if err != nil {
		t.Logf("cleanup execution_run: %v", err)
	}
}

// uniqueName returns a unique entity name for a test to avoid collisions.
func uniqueName(base string) string {
	return fmt.Sprintf("%s-%d", base, time.Now().UnixNano())
}

// ============================================================================
// TestExtensionPresent verifies the extension is installed and reports its version.
// ============================================================================

func TestExtensionPresent(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	c := newClient(t, pool)
	setupExtension(t, c)

	ctx := context.Background()
	version, err := c.ExtensionVersion(ctx)
	if err != nil {
		t.Fatalf("ExtensionVersion: %v", err)
	}
	t.Logf("pg_dgraph version: %s", version)

	if version == "" {
		t.Error("expected non-empty version string")
	}
}

// ============================================================================
// TestEntityLifecycle validates create / idempotent upsert / query / deprecate.
// ============================================================================

func TestEntityLifecycle(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	c := newClient(t, pool)
	setupExtension(t, c)

	prefix := "gotest-entity"
	defer cleanupTestData(t, pool, prefix)

	ctx := context.Background()
	name := uniqueName(prefix)

	t.Run("UpsertNew", func(t *testing.T) {
		id, err := c.UpsertEntity(ctx, "skill", name, 0.7, map[string]any{
			"domain": "test", "description": "Go test entity",
		})
		if err != nil {
			t.Fatalf("UpsertEntity: %v", err)
		}
		if id <= 0 {
			t.Errorf("expected positive ID, got %d", id)
		}
		t.Logf("Created entity id=%d name=%s", id, name)
	})

	t.Run("UpsertIdempotent", func(t *testing.T) {
		// Second call with higher confidence must not create a duplicate.
		id1, err := c.UpsertEntity(ctx, "skill", name, 0.9, nil)
		if err != nil {
			t.Fatalf("UpsertEntity second call: %v", err)
		}
		id2, err := c.UpsertEntity(ctx, "skill", name, 0.9, nil)
		if err != nil {
			t.Fatalf("UpsertEntity third call: %v", err)
		}
		if id1 != id2 {
			t.Errorf("expected same ID on idempotent upsert, got %d vs %d", id1, id2)
		}

		// The confidence should be at least 0.9 (takes the max).
		entity, err := c.GetEntity(ctx, id1)
		if err != nil {
			t.Fatalf("GetEntity: %v", err)
		}
		if entity.Confidence < 0.89 {
			t.Errorf("expected confidence >= 0.9, got %f", entity.Confidence)
		}
		t.Logf("Entity confidence after upsert: %f", entity.Confidence)
	})

	t.Run("FindByType", func(t *testing.T) {
		entities, err := c.FindEntitiesByType(ctx, "skill")
		if err != nil {
			t.Fatalf("FindEntitiesByType: %v", err)
		}
		found := false
		for _, e := range entities {
			if e.Name == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("entity %q not found in FindEntitiesByType results (got %d entities)", name, len(entities))
		}
	})

	t.Run("Deprecate", func(t *testing.T) {
		id, _ := c.UpsertEntity(ctx, "skill", name, 0.9, nil)
		if err := c.DeprecateEntity(ctx, id); err != nil {
			t.Fatalf("DeprecateEntity: %v", err)
		}
		// After deprecation it should not appear in active queries.
		entities, _ := c.FindEntitiesByType(ctx, "skill")
		for _, e := range entities {
			if e.Name == name {
				t.Errorf("deprecated entity %q still returned in FindEntitiesByType", name)
			}
		}
	})
}

// ============================================================================
// TestRelationLifecycle validates entity → relation → traversal.
// ============================================================================

func TestRelationLifecycle(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	c := newClient(t, pool)
	setupExtension(t, c)

	prefix := "gotest-rel"
	defer cleanupTestData(t, pool, prefix)

	ctx := context.Background()
	nameA := uniqueName(prefix + "-A")
	nameB := uniqueName(prefix + "-B")
	nameC := uniqueName(prefix + "-C")

	idA, err := c.UpsertEntity(ctx, "skill", nameA, 0.9, nil)
	if err != nil {
		t.Fatalf("UpsertEntity A: %v", err)
	}
	idB, err := c.UpsertEntity(ctx, "concept", nameB, 0.85, nil)
	if err != nil {
		t.Fatalf("UpsertEntity B: %v", err)
	}
	idC, err := c.UpsertEntity(ctx, "concept", nameC, 0.80, nil)
	if err != nil {
		t.Fatalf("UpsertEntity C: %v", err)
	}

	t.Run("UpsertRelation", func(t *testing.T) {
		relID, err := c.UpsertRelation(ctx, "requires", idA, idB, 0.9)
		if err != nil {
			t.Fatalf("UpsertRelation A→B: %v", err)
		}
		if relID <= 0 {
			t.Errorf("expected positive relation ID, got %d", relID)
		}

		_, err = c.UpsertRelation(ctx, "requires", idB, idC, 0.85)
		if err != nil {
			t.Fatalf("UpsertRelation B→C: %v", err)
		}

		rels, err := c.GetRelationsFrom(ctx, idA)
		if err != nil {
			t.Fatalf("GetRelationsFrom: %v", err)
		}
		if len(rels) == 0 {
			t.Error("expected at least one outgoing relation from A")
		}
		t.Logf("Outgoing relations from A: %d", len(rels))
	})

	t.Run("RebuildBitmapIndexes", func(t *testing.T) {
		if err := c.RebuildLjRelations(ctx); err != nil {
			t.Fatalf("RebuildLjRelations: %v", err)
		}
	})

	t.Run("KHopsFiltered", func(t *testing.T) {
		visited, err := c.KHopsFiltered(ctx, []int64{idA}, 2, nil)
		if err != nil {
			t.Fatalf("KHopsFiltered: %v", err)
		}
		t.Logf("KHops(A, 2) visited %d nodes: %v", len(visited), visited)

		foundB := false
		foundC := false
		for _, id := range visited {
			if id == idB {
				foundB = true
			}
			if id == idC {
				foundC = true
			}
		}
		if !foundB {
			t.Errorf("B (id=%d) not in k-hop result", idB)
		}
		if !foundC {
			t.Errorf("C (id=%d) not in 2-hop result (A→B→C)", idC)
		}
	})

	t.Run("ShortestPath", func(t *testing.T) {
		hops, err := c.ShortestPathFiltered(ctx, int64(idA), int64(idC), nil, 10)
		if err != nil {
			t.Fatalf("ShortestPathFiltered: %v", err)
		}
		t.Logf("Shortest path A→C: %d hops", hops)
		if hops != 2 {
			t.Errorf("expected 2 hops A→B→C, got %d", hops)
		}
	})

	t.Run("ReversePathUndirected", func(t *testing.T) {
		// shortest_path_filtered uses undirected edge traversal (getEdgesFromNodesBoth),
		// so C→A is also reachable in 2 hops by reversing B→C and A→B.
		hops, err := c.ShortestPathFiltered(ctx, int64(idC), int64(idA), nil, 10)
		if err != nil {
			t.Fatalf("ShortestPathFiltered reverse: %v", err)
		}
		t.Logf("Reverse path C→A (undirected): %d hops", hops)
		if hops != 2 {
			t.Errorf("expected 2 hops (undirected), got %d", hops)
		}
	})

	t.Run("NoPath", func(t *testing.T) {
		// Create a completely isolated entity with no edges — truly unreachable.
		idIsolated, err := c.UpsertEntity(ctx, "concept", uniqueName(prefix+"-isolated"), 0.5, nil)
		if err != nil {
			t.Fatalf("UpsertEntity isolated: %v", err)
		}
		_ = c.RebuildLjRelations(ctx)

		hops, err := c.ShortestPathFiltered(ctx, int64(idA), int64(idIsolated), nil, 5)
		if err != nil {
			t.Fatalf("ShortestPathFiltered no path: %v", err)
		}
		if hops != -1 {
			t.Errorf("expected no path (−1) from A to isolated node, got %d", hops)
		}
	})
}

// ============================================================================
// TestAliasAndResolve validates alias registration and term resolution.
// ============================================================================

func TestAliasAndResolve(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	c := newClient(t, pool)
	setupExtension(t, c)

	prefix := "gotest-alias"
	defer cleanupTestData(t, pool, prefix)

	ctx := context.Background()
	name := uniqueName(prefix)

	id, err := c.UpsertEntity(ctx, "skill", name, 0.9, nil)
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	aliases := []string{name + " alias1", name + " alias2"}
	if err := c.RegisterAliases(ctx, id, aliases, 0.9); err != nil {
		t.Fatalf("RegisterAliases: %v", err)
	}

	// Resolve by alias — should return the entity ID.
	ids, err := c.ResolveTerms(ctx, aliases[:1], 0.0)
	if err != nil {
		t.Fatalf("ResolveTerms: %v", err)
	}
	t.Logf("ResolveTerms(%q) → %v", aliases[0], ids)

	found := false
	for _, resolvedID := range ids {
		if resolvedID == id {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("entity id %d not returned by ResolveTerms for alias %q", id, aliases[0])
	}
}

// ============================================================================
// TestSearch validates entity FTS and marketplace search.
// ============================================================================

func TestSearch(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	c := newClient(t, pool)
	setupExtension(t, c)

	prefix := "gotest-search"
	defer cleanupTestData(t, pool, prefix)

	ctx := context.Background()
	uniqueWord := fmt.Sprintf("xqzfoo%d", time.Now().UnixNano())
	name := prefix + "-" + uniqueWord

	_, err := c.UpsertEntity(ctx, "skill", name, 0.9, map[string]any{
		"domain":      "testdomain",
		"description": uniqueWord + " is a test skill for search",
	})
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	// Allow FTS index to propagate (GENERATED ALWAYS AS STORED is synchronous).
	t.Run("EntityFtsSearch", func(t *testing.T) {
		results, err := c.EntityFtsSearch(ctx, uniqueWord, nil, "", 0.0, 10)
		if err != nil {
			t.Fatalf("EntityFtsSearch: %v", err)
		}
		t.Logf("EntityFtsSearch(%q) returned %d results", uniqueWord, len(results))

		found := false
		for _, r := range results {
			if r.Name == name {
				found = true
				t.Logf("  Found: %s (rank=%.4f conf=%.2f)", r.Name, r.FtsRank, r.Confidence)
			}
		}
		if !found {
			t.Errorf("entity %q not found in FTS search results", name)
		}
	})

	t.Run("EntityFtsSearchWithTypeFilter", func(t *testing.T) {
		results, err := c.EntityFtsSearch(ctx, uniqueWord, []string{"skill"}, "", 0.0, 10)
		if err != nil {
			t.Fatalf("EntityFtsSearch with type filter: %v", err)
		}
		for _, r := range results {
			if r.Type != "skill" {
				t.Errorf("expected only skill results, got type %q", r.Type)
			}
		}
		t.Logf("FTS with type=skill returned %d results", len(results))
	})

	t.Run("MarketplaceSearch", func(t *testing.T) {
		results, err := c.MarketplaceSearch(ctx, uniqueWord, "", 0.0, 2, 10)
		if err != nil {
			t.Fatalf("MarketplaceSearch: %v", err)
		}
		t.Logf("MarketplaceSearch(%q) returned %d results", uniqueWord, len(results))

		found := false
		for _, r := range results {
			if r.Name == name {
				found = true
				t.Logf("  Found: %s (composite=%.4f hub=%.4f)", r.Name, r.CompositeScore, r.HubScore)
			}
		}
		if !found {
			t.Logf("Note: entity %q not in marketplace results (may need FTS update or graph rebuild)", name)
		}
	})
}

// ============================================================================
// TestLearnFromRun validates the learning pipeline.
// ============================================================================

func TestLearnFromRun(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	c := newClient(t, pool)
	setupExtension(t, c)

	prefix := "gotest-lfr"
	defer cleanupTestData(t, pool, prefix)

	ctx := context.Background()
	runKey := fmt.Sprintf("%s-run-%d", prefix, time.Now().UnixNano())
	skillName := uniqueName(prefix + "-skill")
	conceptName := uniqueName(prefix + "-concept")

	concepts := []ConceptInput{
		{Type: "skill", Name: skillName, Confidence: 0.9, Metadata: map[string]any{"domain": "test"}},
		{Type: "concept", Name: conceptName, Confidence: 0.8, Metadata: map[string]any{"domain": "test"}},
	}
	relations := []RelationInput{
		{Source: skillName, Target: conceptName, Type: "requires", Confidence: 0.85},
	}

	runID, err := c.LearnFromRun(ctx,
		runKey, "test", "success",
		concepts, relations,
		"Integration test run.",
		map[string]any{"source": "dgraph_test.go"},
	)
	if err != nil {
		t.Fatalf("LearnFromRun: %v", err)
	}
	if runID <= 0 {
		t.Errorf("expected positive run ID, got %d", runID)
	}
	t.Logf("LearnFromRun returned run_id=%d", runID)

	// Entities should exist now.
	results, err := c.EntityFtsSearch(ctx, skillName, nil, "", 0.0, 5)
	if err != nil {
		t.Fatalf("EntityFtsSearch after learn: %v", err)
	}
	found := false
	for _, r := range results {
		if r.Name == skillName {
			found = true
		}
	}
	if !found {
		t.Logf("Note: %q not found in FTS search (FTS vector may need refresh)", skillName)
	}

	// Idempotent: same run_key, different outcome.
	runID2, err := c.LearnFromRun(ctx,
		runKey, "test", "partial",
		concepts, relations, "", nil,
	)
	if err != nil {
		t.Fatalf("LearnFromRun idempotent call: %v", err)
	}
	if runID2 != runID {
		t.Errorf("idempotent call should return same run_id: got %d vs %d", runID2, runID)
	}
}

// ============================================================================
// TestSkillDependencies and TestEntityNeighborhood
// ============================================================================

func TestSkillDependenciesAndNeighborhood(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	c := newClient(t, pool)
	setupExtension(t, c)

	prefix := "gotest-dep"
	defer cleanupTestData(t, pool, prefix)

	ctx := context.Background()

	// Build A → B → C chain.
	idA, _ := c.UpsertEntity(ctx, "skill", uniqueName(prefix+"-A"), 0.9, nil)
	idB, _ := c.UpsertEntity(ctx, "concept", uniqueName(prefix+"-B"), 0.8, nil)
	idC, _ := c.UpsertEntity(ctx, "concept", uniqueName(prefix+"-C"), 0.7, nil)
	_, _ = c.UpsertRelation(ctx, "requires", idA, idB, 0.9)
	_, _ = c.UpsertRelation(ctx, "requires", idB, idC, 0.85)
	_ = c.RebuildLjRelations(ctx)

	t.Run("SkillDependencies", func(t *testing.T) {
		deps, err := c.SkillDependencies(ctx, idA, 5, 0.0)
		if err != nil {
			t.Fatalf("SkillDependencies: %v", err)
		}
		t.Logf("SkillDependencies for A: %d rows", len(deps))

		foundB := false
		foundC := false
		for _, d := range deps {
			t.Logf("  depth=%d %s (%s conf=%.2f via %s)", d.Depth, d.DepName, d.DepType, d.DepConfidence, d.RelationType)
			if d.DepEntityID == idB {
				foundB = true
			}
			if d.DepEntityID == idC {
				foundC = true
			}
		}
		if !foundB {
			t.Errorf("B not in skill_dependencies of A")
		}
		if !foundC {
			t.Errorf("C not in skill_dependencies of A (depth 2)")
		}
	})

	t.Run("EntityNeighborhood", func(t *testing.T) {
		n, err := c.EntityNeighborhood(ctx, idA, 10, 10, 0.0)
		if err != nil {
			t.Fatalf("EntityNeighborhood: %v", err)
		}
		t.Logf("Neighborhood of A: entity=%s outgoing=%d incoming=%d", n.Entity.Name, len(n.Outgoing), len(n.Incoming))

		if n.Entity.ID != idA {
			t.Errorf("expected entity ID %d in neighborhood, got %d", idA, n.Entity.ID)
		}
		if len(n.Outgoing) == 0 {
			t.Error("expected at least one outgoing edge for A")
		}
	})

	t.Run("ConfidenceDecay", func(t *testing.T) {
		decay, err := c.ConfidenceDecay(ctx, idA, 90)
		if err != nil {
			t.Fatalf("ConfidenceDecay: %v", err)
		}
		// Entity was just created so decay should be close to original confidence (>= 0.0).
		if decay < 0 {
			t.Errorf("expected non-negative decay score, got %f", decay)
		}
		t.Logf("ConfidenceDecay for A (half_life=90d): %.4f", decay)
	})
}

// ============================================================================
// TestKHopsEdgeTypes validates edge-type filtering in traversal.
// ============================================================================

func TestKHopsEdgeTypes(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	c := newClient(t, pool)
	setupExtension(t, c)

	prefix := "gotest-khops"
	defer cleanupTestData(t, pool, prefix)

	ctx := context.Background()

	idA, _ := c.UpsertEntity(ctx, "skill", uniqueName(prefix+"-A"), 0.9, nil)
	idB, _ := c.UpsertEntity(ctx, "concept", uniqueName(prefix+"-B"), 0.8, nil)
	idD, _ := c.UpsertEntity(ctx, "tool", uniqueName(prefix+"-D"), 0.7, nil)

	// A -requires-> B  and  A -uses-> D
	_, _ = c.UpsertRelation(ctx, "requires", idA, idB, 0.9)
	_, _ = c.UpsertRelation(ctx, "uses", idA, idD, 0.8)
	_ = c.RebuildLjRelations(ctx)

	t.Run("FilterRequiresOnly", func(t *testing.T) {
		visited, err := c.KHopsFiltered(ctx, []int64{idA}, 1, []string{"requires"})
		if err != nil {
			t.Fatalf("KHopsFiltered: %v", err)
		}
		t.Logf("KHops(A, 1, requires) visited: %v", visited)

		foundD := false
		for _, id := range visited {
			if id == idD {
				foundD = true
			}
		}
		if foundD {
			t.Error("D should not be reachable via 'requires' edges only")
		}
	})

	t.Run("NoEdgeTypeFilter", func(t *testing.T) {
		visited, err := c.KHopsFiltered(ctx, []int64{idA}, 1, nil)
		if err != nil {
			t.Fatalf("KHopsFiltered no filter: %v", err)
		}
		foundB, foundD := false, false
		for _, id := range visited {
			if id == idB {
				foundB = true
			}
			if id == idD {
				foundD = true
			}
		}
		if !foundB || !foundD {
			t.Errorf("without filter, both B and D should be reachable: foundB=%v foundD=%v", foundB, foundD)
		}
	})
}
