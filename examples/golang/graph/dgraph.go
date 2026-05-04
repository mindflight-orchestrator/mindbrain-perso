// dgraph.go
// Go client for the pg_dgraph PostgreSQL extension (version 0.3.1 compatible).
//
// Provides a thin, idiomatic wrapper around the graph.* SQL functions so that
// callers don't need to write raw SQL for common operations.

package pgdgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ============================================================================
// Data types
// ============================================================================

// Entity represents a row from graph.entity.
type Entity struct {
	ID          int64          `json:"id"`
	Type        string         `json:"type"`
	Name        string         `json:"name"`
	Confidence  float32        `json:"confidence"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	DeprecatedAt *time.Time    `json:"deprecated_at,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
}

// Relation represents a row from graph.relation.
type Relation struct {
	ID         int64      `json:"id"`
	Type       string     `json:"type"`
	SourceID   int64      `json:"source_id"`
	TargetID   int64      `json:"target_id"`
	Confidence float32    `json:"confidence"`
	CreatedAt  time.Time  `json:"created_at"`
}

// EntitySearchResult is one row returned by graph.entity_fts_search().
type EntitySearchResult struct {
	EntityID   int64          `json:"entity_id"`
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	Confidence float32        `json:"confidence"`
	FtsRank    float32        `json:"fts_rank"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// MarketplaceResult is one row returned by graph.marketplace_search().
type MarketplaceResult struct {
	EntityID       int64          `json:"entity_id"`
	Name           string         `json:"name"`
	Type           string         `json:"type"`
	Confidence     float32        `json:"confidence"`
	FtsRank        float32        `json:"fts_rank"`
	IsDirectMatch  bool           `json:"is_direct_match"`
	HubScore       float32        `json:"hub_score"`
	CompositeScore float32        `json:"composite_score"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

// SkillDependency is one row returned by graph.skill_dependencies().
type SkillDependency struct {
	DepEntityID   int64   `json:"dep_entity_id"`
	DepName       string  `json:"dep_name"`
	DepType       string  `json:"dep_type"`
	DepConfidence float32 `json:"dep_confidence"`
	RelationType  string  `json:"relation_type"`
	Depth         int     `json:"depth"`
}

// Neighborhood is the JSON structure returned by graph.entity_neighborhood().
type Neighborhood struct {
	Entity   NeighborEntity  `json:"entity"`
	Outgoing []NeighborEdge  `json:"outgoing"`
	Incoming []NeighborEdge  `json:"incoming"`
}

// NeighborEntity is the entity summary inside a Neighborhood.
type NeighborEntity struct {
	ID         int64          `json:"id"`
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	Confidence float32        `json:"confidence"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// NeighborEdge is one edge inside a Neighborhood.
type NeighborEdge struct {
	TargetID   int64   `json:"target_id,omitempty"`
	TargetName string  `json:"target_name,omitempty"`
	SourceID   int64   `json:"source_id,omitempty"`
	SourceName string  `json:"source_name,omitempty"`
	Type       string  `json:"type"`
	Confidence float32 `json:"confidence"`
}

// ConceptInput is the structure expected by learn_from_run concepts array.
type ConceptInput struct {
	Type       string         `json:"type"`
	Name       string         `json:"name"`
	Confidence float32        `json:"confidence"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// RelationInput is the structure expected by learn_from_run relations array.
type RelationInput struct {
	Source     string  `json:"source"`
	Target     string  `json:"target"`
	Type       string  `json:"type"`
	Confidence float32 `json:"confidence"`
}

// ============================================================================
// Client
// ============================================================================

// DgraphClient is a thin Go client over the pg_dgraph SQL API.
// All methods require the pg_dgraph extension to be installed in the connected
// database with schema "graph".
type DgraphClient struct {
	pool  *pgxpool.Pool
	debug bool
}

// NewDgraphClient creates a DgraphClient backed by the provided connection pool.
// Set debug=true to print queries to stderr.
func NewDgraphClient(pool *pgxpool.Pool, debug bool) (*DgraphClient, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool is required")
	}
	return &DgraphClient{pool: pool, debug: debug}, nil
}

// ============================================================================
// Entity management
// ============================================================================

// UpsertEntity creates or merges an entity identified by (entityType, name).
// On conflict the confidence is raised to max(old, new) and metadata is
// deep-merged. Returns the entity ID.
func (c *DgraphClient) UpsertEntity(ctx context.Context, entityType, name string, confidence float32, metadata map[string]any) (int64, error) {
	metaJSON, err := marshalNullable(metadata)
	if err != nil {
		return 0, fmt.Errorf("marshal metadata: %w", err)
	}

	var id int64
	err = c.pool.QueryRow(ctx,
		`SELECT graph.upsert_entity($1, $2, $3::real, $4::jsonb)`,
		entityType, name, confidence, metaJSON,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("upsert_entity: %w", err)
	}
	return id, nil
}

// GetEntity returns a single entity by ID. Returns pgx.ErrNoRows if not found.
func (c *DgraphClient) GetEntity(ctx context.Context, id int64) (*Entity, error) {
	row := c.pool.QueryRow(ctx,
		`SELECT id, type, name, confidence, metadata, deprecated_at, created_at
		   FROM graph.entity WHERE id = $1`, id)
	return scanEntity(row)
}

// FindEntitiesByType returns all active (non-deprecated) entities of a given type.
func (c *DgraphClient) FindEntitiesByType(ctx context.Context, entityType string) ([]Entity, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT id, type, name, confidence, metadata, deprecated_at, created_at
		   FROM graph.entity
		  WHERE type = $1 AND deprecated_at IS NULL
		  ORDER BY confidence DESC`, entityType)
	if err != nil {
		return nil, fmt.Errorf("find entities by type: %w", err)
	}
	return collectEntities(rows)
}

// DeprecateEntity marks an entity as deprecated.
func (c *DgraphClient) DeprecateEntity(ctx context.Context, id int64) error {
	_, err := c.pool.Exec(ctx,
		`UPDATE graph.entity SET deprecated_at = now() WHERE id = $1`, id)
	return err
}

// ============================================================================
// Relation management
// ============================================================================

// UpsertRelation creates or updates a directed relation between two entities.
// Returns the relation ID.
func (c *DgraphClient) UpsertRelation(ctx context.Context, relType string, sourceID, targetID int64, confidence float32) (int64, error) {
	var id int64
	err := c.pool.QueryRow(ctx,
		`SELECT graph.upsert_relation($1, $2, $3, $4::real)`,
		relType, sourceID, targetID, confidence,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("upsert_relation: %w", err)
	}
	return id, nil
}

// GetRelationsFrom returns all active outgoing relations from the given entity.
func (c *DgraphClient) GetRelationsFrom(ctx context.Context, sourceID int64) ([]Relation, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT id, type, source_id, target_id, confidence, created_at
		   FROM graph.relation
		  WHERE source_id = $1 AND deprecated_at IS NULL
		  ORDER BY confidence DESC`, sourceID)
	if err != nil {
		return nil, fmt.Errorf("get relations from: %w", err)
	}
	return collectRelations(rows)
}

// ============================================================================
// Alias management
// ============================================================================

// RegisterAliases maps one or more surface-form terms to a canonical entity.
// Idempotent: calling again with the same terms raises confidence if higher.
func (c *DgraphClient) RegisterAliases(ctx context.Context, entityID int64, terms []string, confidence float32) error {
	_, err := c.pool.Exec(ctx,
		`SELECT graph.register_aliases($1, $2, $3::real)`,
		entityID, terms, confidence,
	)
	if err != nil {
		return fmt.Errorf("register_aliases: %w", err)
	}
	return nil
}

// ResolveTerms resolves text terms to entity IDs via alias matching.
// Returns a slice of matching entity IDs.
func (c *DgraphClient) ResolveTerms(ctx context.Context, terms []string, minConfidence float32) ([]int64, error) {
	// graph.resolve_terms returns a roaringbitmap; convert to array immediately.
	var ids []int64
	err := c.pool.QueryRow(ctx,
		`SELECT COALESCE(rb_to_array(graph.resolve_terms($1, $2::real)), ARRAY[]::int[])`,
		terms, minConfidence,
	).Scan(&ids)
	if err != nil {
		return nil, fmt.Errorf("resolve_terms: %w", err)
	}
	return ids, nil
}

// ============================================================================
// Search
// ============================================================================

// EntityFtsSearch performs full-text search over entity name and metadata.
// typeFilter and domain may be empty to skip those filters.
func (c *DgraphClient) EntityFtsSearch(ctx context.Context, query string, typeFilter []string, domain string, minConfidence float32, limit int) ([]EntitySearchResult, error) {
	if limit <= 0 {
		limit = 20
	}
	var domainArg *string
	if domain != "" {
		domainArg = &domain
	}
	var typesArg *[]string
	if len(typeFilter) > 0 {
		typesArg = &typeFilter
	}

	rows, err := c.pool.Query(ctx,
		`SELECT entity_id, name, type, confidence, fts_rank, metadata
		   FROM graph.entity_fts_search($1, $2, $3, $4::real, $5)`,
		query, typesArg, domainArg, minConfidence, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("entity_fts_search: %w", err)
	}
	defer rows.Close()

	var results []EntitySearchResult
	for rows.Next() {
		var r EntitySearchResult
		var metaRaw []byte
		if err := rows.Scan(&r.EntityID, &r.Name, &r.Type, &r.Confidence, &r.FtsRank, &metaRaw); err != nil {
			return nil, fmt.Errorf("scan entity_fts_search row: %w", err)
		}
		if metaRaw != nil {
			_ = json.Unmarshal(metaRaw, &r.Metadata)
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

// MarketplaceSearch performs a hybrid FTS + BFS + hub-degree scored search.
// domain may be empty to search all domains.
func (c *DgraphClient) MarketplaceSearch(ctx context.Context, query, domain string, minConfidence float32, maxHops, limit int) ([]MarketplaceResult, error) {
	if maxHops <= 0 {
		maxHops = 2
	}
	if limit <= 0 {
		limit = 20
	}
	var domainArg *string
	if domain != "" {
		domainArg = &domain
	}

	rows, err := c.pool.Query(ctx,
		`SELECT entity_id, name, type, confidence, fts_rank,
		        is_direct_match, hub_score, composite_score, metadata
		   FROM graph.marketplace_search($1, $2, $3::real, $4, $5)`,
		query, domainArg, minConfidence, maxHops, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("marketplace_search: %w", err)
	}
	defer rows.Close()

	var results []MarketplaceResult
	for rows.Next() {
		var r MarketplaceResult
		var metaRaw []byte
		if err := rows.Scan(
			&r.EntityID, &r.Name, &r.Type, &r.Confidence, &r.FtsRank,
			&r.IsDirectMatch, &r.HubScore, &r.CompositeScore, &metaRaw,
		); err != nil {
			return nil, fmt.Errorf("scan marketplace_search row: %w", err)
		}
		if metaRaw != nil {
			_ = json.Unmarshal(metaRaw, &r.Metadata)
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

// ============================================================================
// Graph traversal
// ============================================================================

// KHopsFiltered finds all entities reachable within maxHops from the seed set.
// seedIDs are the starting entity integer IDs. edgeTypes may be nil to follow
// all edge types. Returns the visited entity IDs (including seeds).
func (c *DgraphClient) KHopsFiltered(ctx context.Context, seedIDs []int64, maxHops int, edgeTypes []string) ([]int64, error) {
	if len(seedIDs) == 0 {
		return nil, nil
	}

	// Convert []int64 → int[] parameter, build roaringbitmap inside SQL.
	intIDs := int64SliceToInt(seedIDs)

	var edgeTypesArg *[]string
	if len(edgeTypes) > 0 {
		edgeTypesArg = &edgeTypes
	}

	var ids []int64
	err := c.pool.QueryRow(ctx,
		`SELECT COALESCE(
		    rb_to_array(
		        k_hops_filtered(rb_build($1::int[]), $2, $3)
		    ),
		    ARRAY[]::int[]
		 )`,
		intIDs, maxHops, edgeTypesArg,
	).Scan(&ids)
	if err != nil {
		return nil, fmt.Errorf("k_hops_filtered: %w", err)
	}
	return ids, nil
}

// KHopsFilteredFull is the full-parameter variant of KHopsFiltered, exposing
// all filtering options that the Zig implementation supports.
func (c *DgraphClient) KHopsFilteredFull(ctx context.Context,
	seedIDs []int64,
	maxHops int,
	edgeTypes []string,
	confMin, confMax *float32,
) ([]int64, error) {
	if len(seedIDs) == 0 {
		return nil, nil
	}

	intIDs := int64SliceToInt(seedIDs)

	var edgeTypesArg *[]string
	if len(edgeTypes) > 0 {
		edgeTypesArg = &edgeTypes
	}

	var ids []int64
	err := c.pool.QueryRow(ctx,
		`SELECT COALESCE(
		    rb_to_array(
		        k_hops_filtered(
		            rb_build($1::int[]),
		            $2,
		            $3,
		            NULL, NULL, NULL, NULL,
		            $4::real, $5::real
		        )
		    ),
		    ARRAY[]::int[]
		 )`,
		intIDs, maxHops, edgeTypesArg, confMin, confMax,
	).Scan(&ids)
	if err != nil {
		return nil, fmt.Errorf("k_hops_filtered_full: %w", err)
	}
	return ids, nil
}

// ShortestPathFiltered finds the shortest path length between two entity IDs.
// Returns -1 if no path is found within maxDepth hops.
// edgeTypes may be nil to allow all edge types.
//
// Note: we always pass conf_min=0.0 (non-NULL) to work around a known behaviour
// in the Zig bidirectional BFS: when all meta-filter parameters are NULL the
// internal filterEdgesMeta helper returns nil (meaning "no filter needed"), but
// shortest_path_filtered incorrectly interprets nil as "no edges pass". Passing
// conf_min=0.0 forces filterEdgesMeta to run and return the full edge set.
func (c *DgraphClient) ShortestPathFiltered(ctx context.Context, srcID, destID int64, edgeTypes []string, maxDepth int) (int, error) {
	if maxDepth <= 0 {
		maxDepth = 20
	}

	var edgeTypesArg *[]string
	if len(edgeTypes) > 0 {
		edgeTypesArg = &edgeTypes
	}

	var pathLen *int
	err := c.pool.QueryRow(ctx,
		`SELECT shortest_path_filtered($1::int, $2::int, $3, NULL, NULL, NULL, NULL, 0.0::real, NULL, $4)`,
		srcID, destID, edgeTypesArg, maxDepth,
	).Scan(&pathLen)
	if err != nil {
		return -1, fmt.Errorf("shortest_path_filtered: %w", err)
	}
	if pathLen == nil {
		return -1, nil
	}
	return *pathLen, nil
}

// ============================================================================
// Learning pipeline
// ============================================================================

// LearnFromRun records an agent execution run and upserts all discovered
// entities and relations. Idempotent: calling again with the same runKey
// updates the outcome but does not duplicate data.
// Returns the execution_run.id.
func (c *DgraphClient) LearnFromRun(
	ctx context.Context,
	runKey, domain, outcome string,
	concepts []ConceptInput,
	relations []RelationInput,
	transcript string,
	runMeta map[string]any,
) (int64, error) {
	conceptsJSON, err := json.Marshal(concepts)
	if err != nil {
		return 0, fmt.Errorf("marshal concepts: %w", err)
	}
	relationsJSON, err := json.Marshal(relations)
	if err != nil {
		return 0, fmt.Errorf("marshal relations: %w", err)
	}
	metaJSON, err := marshalNullable(runMeta)
	if err != nil {
		return 0, fmt.Errorf("marshal run meta: %w", err)
	}

	var transcriptArg *string
	if transcript != "" {
		transcriptArg = &transcript
	}

	var id int64
	err = c.pool.QueryRow(ctx,
		`SELECT graph.learn_from_run($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7::jsonb)`,
		runKey, domain, outcome,
		string(conceptsJSON), string(relationsJSON),
		transcriptArg, metaJSON,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("learn_from_run: %w", err)
	}
	return id, nil
}

// ============================================================================
// Analytics & maintenance
// ============================================================================

// SkillDependencies returns the transitive dependency tree of an entity.
func (c *DgraphClient) SkillDependencies(ctx context.Context, entityID int64, maxDepth int, minConfidence float32) ([]SkillDependency, error) {
	if maxDepth <= 0 {
		maxDepth = 5
	}
	rows, err := c.pool.Query(ctx,
		`SELECT dep_entity_id, dep_name, dep_type, dep_confidence, relation_type, depth
		   FROM graph.skill_dependencies($1, $2, $3::real)`,
		entityID, maxDepth, minConfidence,
	)
	if err != nil {
		return nil, fmt.Errorf("skill_dependencies: %w", err)
	}
	defer rows.Close()

	var deps []SkillDependency
	for rows.Next() {
		var d SkillDependency
		if err := rows.Scan(&d.DepEntityID, &d.DepName, &d.DepType, &d.DepConfidence, &d.RelationType, &d.Depth); err != nil {
			return nil, fmt.Errorf("scan skill_dependencies row: %w", err)
		}
		deps = append(deps, d)
	}
	return deps, rows.Err()
}

// EntityNeighborhood returns a one-hop JSON neighborhood summary, suitable
// for LLM context injection.
func (c *DgraphClient) EntityNeighborhood(ctx context.Context, entityID int64, maxOut, maxIn int, minConfidence float32) (*Neighborhood, error) {
	if maxOut <= 0 {
		maxOut = 10
	}
	if maxIn <= 0 {
		maxIn = 10
	}
	var raw []byte
	err := c.pool.QueryRow(ctx,
		`SELECT graph.entity_neighborhood($1, $2, $3, $4::real)`,
		entityID, maxOut, maxIn, minConfidence,
	).Scan(&raw)
	if err != nil {
		return nil, fmt.Errorf("entity_neighborhood: %w", err)
	}
	var n Neighborhood
	if err := json.Unmarshal(raw, &n); err != nil {
		return nil, fmt.Errorf("unmarshal neighborhood: %w", err)
	}
	return &n, nil
}

// ConfidenceDecay returns the time-decayed confidence for an entity.
// halfLifeDays defaults to 90 if <= 0.
func (c *DgraphClient) ConfidenceDecay(ctx context.Context, entityID int64, halfLifeDays int) (float32, error) {
	if halfLifeDays <= 0 {
		halfLifeDays = 90
	}
	var conf float32
	err := c.pool.QueryRow(ctx,
		`SELECT graph.confidence_decay($1, $2)`,
		entityID, halfLifeDays,
	).Scan(&conf)
	if err != nil {
		return 0, fmt.Errorf("confidence_decay: %w", err)
	}
	return conf, nil
}

// RebuildLjRelations rebuilds the bitmap adjacency indexes (lj_out / lj_in)
// from scratch. Call this after bulk direct SQL inserts into graph.relation
// that bypass upsert_relation. Normal upsert_relation calls maintain the
// indexes via trigger automatically.
func (c *DgraphClient) RebuildLjRelations(ctx context.Context) error {
	_, err := c.pool.Exec(ctx, `SELECT graph.rebuild_lj_relations()`)
	if err != nil {
		return fmt.Errorf("rebuild_lj_relations: %w", err)
	}
	return nil
}

// RebuildLjForEntities does an incremental rebuild of lj_out / lj_in for the
// given entity IDs only. Faster than a full rebuild when few entities changed.
func (c *DgraphClient) RebuildLjForEntities(ctx context.Context, entityIDs []int64) error {
	_, err := c.pool.Exec(ctx,
		`SELECT graph.rebuild_lj_for_entities($1)`, entityIDs)
	if err != nil {
		return fmt.Errorf("rebuild_lj_for_entities: %w", err)
	}
	return nil
}

// RefreshDegreeView refreshes the entity_degree materialized view concurrently.
func (c *DgraphClient) RefreshDegreeView(ctx context.Context) error {
	_, err := c.pool.Exec(ctx,
		`REFRESH MATERIALIZED VIEW CONCURRENTLY graph.entity_degree`)
	return err
}

// ============================================================================
// Extension helpers
// ============================================================================

// ExtensionVersion returns the installed pg_dgraph version string.
func (c *DgraphClient) ExtensionVersion(ctx context.Context) (string, error) {
	var version string
	err := c.pool.QueryRow(ctx,
		`SELECT extversion FROM pg_extension WHERE extname = 'pg_dgraph'`,
	).Scan(&version)
	if err != nil {
		return "", fmt.Errorf("query pg_dgraph version: %w", err)
	}
	return version, nil
}

// EnsureExtension creates roaringbitmap and pg_dgraph extensions if not present.
func (c *DgraphClient) EnsureExtension(ctx context.Context) error {
	stmts := []string{
		`CREATE EXTENSION IF NOT EXISTS roaringbitmap`,
		`CREATE EXTENSION IF NOT EXISTS pg_dgraph`,
	}
	for _, s := range stmts {
		if _, err := c.pool.Exec(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", strings.Split(s, " ")[2], err)
		}
	}
	return nil
}

// ============================================================================
// Internal helpers
// ============================================================================

func marshalNullable(v any) (*string, error) {
	if v == nil {
		return nil, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}

// int64SliceToInt converts []int64 to []int for PostgreSQL int[] parameters.
// pg_roaringbitmap rb_build accepts int[] (32-bit range).
func int64SliceToInt(ids []int64) []int32 {
	out := make([]int32, len(ids))
	for i, id := range ids {
		out[i] = int32(id)
	}
	return out
}

func scanEntity(row pgx.Row) (*Entity, error) {
	var e Entity
	var metaRaw []byte
	if err := row.Scan(&e.ID, &e.Type, &e.Name, &e.Confidence, &metaRaw, &e.DeprecatedAt, &e.CreatedAt); err != nil {
		return nil, err
	}
	if metaRaw != nil {
		_ = json.Unmarshal(metaRaw, &e.Metadata)
	}
	return &e, nil
}

func collectEntities(rows pgx.Rows) ([]Entity, error) {
	defer rows.Close()
	var entities []Entity
	for rows.Next() {
		var e Entity
		var metaRaw []byte
		if err := rows.Scan(&e.ID, &e.Type, &e.Name, &e.Confidence, &metaRaw, &e.DeprecatedAt, &e.CreatedAt); err != nil {
			return nil, err
		}
		if metaRaw != nil {
			_ = json.Unmarshal(metaRaw, &e.Metadata)
		}
		entities = append(entities, e)
	}
	return entities, rows.Err()
}

func collectRelations(rows pgx.Rows) ([]Relation, error) {
	defer rows.Close()
	var rels []Relation
	for rows.Next() {
		var r Relation
		if err := rows.Scan(&r.ID, &r.Type, &r.SourceID, &r.TargetID, &r.Confidence, &r.CreatedAt); err != nil {
			return nil, err
		}
		rels = append(rels, r)
	}
	return rels, rows.Err()
}
