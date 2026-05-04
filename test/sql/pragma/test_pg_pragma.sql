\echo '=== pg_pragma integration tests ==='

-- App-owned schema/tables (minimal fixture matching extension assumptions)
CREATE SCHEMA IF NOT EXISTS "memory-server";

CREATE TABLE IF NOT EXISTS "memory-server".memory_items (
  id text PRIMARY KEY,
  user_id text NOT NULL,
  source_type text,
  source_ref text,
  thread_id text,
  epoch_id text,
  importance double precision DEFAULT 0,
  confidence double precision DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT NOW(),
  updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS "memory-server".memory_projections (
  id text PRIMARY KEY,
  item_id text NOT NULL,
  user_id text NOT NULL,
  projection_type text NOT NULL,
  content text NOT NULL,
  content_tsvector tsvector GENERATED ALWAYS AS (to_tsvector('english', content)) STORED,
  facets jsonb DEFAULT '{}'::jsonb,
  metadata jsonb DEFAULT '{}'::jsonb,
  source_span jsonb,
  rank_hint double precision DEFAULT 0,
  confidence double precision DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT NOW(),
  updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS "memory-server".memory_edges (
  id text PRIMARY KEY,
  user_id text NOT NULL,
  node_from text NOT NULL,
  edge_type text NOT NULL,
  node_to text NOT NULL,
  weight double precision DEFAULT 0,
  confidence double precision DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT NOW()
);

TRUNCATE "memory-server".memory_edges, "memory-server".memory_projections, "memory-server".memory_items;

INSERT INTO "memory-server".memory_items (id, user_id, source_type, source_ref)
VALUES ('item_1', 'user_test', 'event', 'evt_1');

INSERT INTO "memory-server".memory_items (id, user_id, source_type, source_ref)
VALUES
  ('item_2', 'user_test', 'event', 'evt_2'),
  ('item_3', 'user_test', 'event', 'evt_3');

INSERT INTO "memory-server".memory_projections (id, item_id, user_id, projection_type, content, rank_hint, confidence)
VALUES
  ('proj_can_1', 'item_1', 'user_test', 'canonical', 'User wants offline sync for exports.', 0.95, 0.90),
  ('proj_prop_1', 'item_1', 'user_test', 'proposition',
   'fact|id=f42|subject=offline_sync|predicate=blocked_by|object=ios_background_limits|conf=0.91
constraint|id=c9|scope=memory|rule=keep_context_small',
   0.92, 0.91),
  ('proj_raw_1', 'item_1', 'user_test', 'raw', 'Raw note: iOS background execution blocked sync attempts.', 0.50, 0.80);

INSERT INTO "memory-server".memory_projections (
  id, item_id, user_id, projection_type, content, facets, metadata, rank_hint, confidence
)
VALUES
  (
    'proj_scope_1',
    'item_2',
    'user_test',
    'proposition',
    'fact|id=f100|subject=player|predicate=preferred_offer|object=hotel|conf=0.93
step|id=s100|process=vip_reactivation|order=1|action=call_host',
    '{"player_key":"player_123","scope":"player:player_123"}'::jsonb,
    '{"player_id":"player_123","scope":"player:player_123"}'::jsonb,
    0.97,
    0.94
  ),
  (
    'proj_scope_2',
    'item_3',
    'user_test',
    'proposition',
    'fact|id=f101|subject=player|predicate=preferred_offer|object=hotel|conf=0.88
step|id=s101|process=vip_reactivation|order=1|action=send_offer',
    '{"player_key":"player_999","scope":"player:player_999"}'::jsonb,
    '{"player_id":"player_999","scope":"player:player_999"}'::jsonb,
    0.89,
    0.90
  );

DO $$
DECLARE parsed jsonb;
BEGIN
  SELECT pragma_parse_proposition_line('fact|id=f42|subject=offline_sync|predicate=blocked_by|object=ios_background_limits|conf=0.91')
    INTO parsed;

  IF parsed->>'type' <> 'fact' THEN
    RAISE EXCEPTION 'expected type=fact, got=%', parsed->>'type';
  END IF;
  IF parsed->>'subject' <> 'offline_sync' THEN
    RAISE EXCEPTION 'expected subject=offline_sync, got=%', parsed->>'subject';
  END IF;
  IF parsed->>'predicate' <> 'blocked_by' THEN
    RAISE EXCEPTION 'expected predicate=blocked_by, got=%', parsed->>'predicate';
  END IF;
END $$;

DO $$
DECLARE bm roaringbitmap;
BEGIN
  SELECT pragma_candidate_bitmap('user_test', 'offline', ARRAY['canonical','proposition'])
    INTO bm;
  IF rb_is_empty(bm) THEN
    RAISE EXCEPTION 'candidate bitmap should not be empty';
  END IF;
END $$;

DO $$
DECLARE c integer;
BEGIN
  SELECT count(*) INTO c
  FROM pragma_pack_context('user_test', 'offline', 10);
  IF c < 2 THEN
    RAISE EXCEPTION 'expected >=2 packed rows, got %', c;
  END IF;
END $$;

DO $$
DECLARE c integer;
BEGIN
  SELECT count(*) INTO c
  FROM pragma_pack_context('user_test', 'hotel', 10);
  IF c <> 2 THEN
    RAISE EXCEPTION 'expected 2 unscoped hotel rows, got %', c;
  END IF;
END $$;

DO $$
DECLARE c integer;
BEGIN
  SELECT count(*) INTO c
  FROM pragma_pack_context_scoped('user_test', 'hotel', 'player_123', 10);
  IF c <> 1 THEN
    RAISE EXCEPTION 'expected 1 scoped row for player_123, got %', c;
  END IF;
END $$;

DO $$
DECLARE c integer;
BEGIN
  SELECT count(*) INTO c
  FROM pragma_pack_context_scoped('user_test', 'hotel', 'player:player_123', 10);
  IF c <> 1 THEN
    RAISE EXCEPTION 'expected 1 scoped row for player:player_123, got %', c;
  END IF;
END $$;

DO $$
DECLARE c integer;
BEGIN
  SELECT count(*) INTO c
  FROM pragma_pack_context_scoped('user_test', 'hotel', 'player_missing', 10);
  IF c <> 0 THEN
    RAISE EXCEPTION 'expected 0 scoped rows for player_missing, got %', c;
  END IF;
END $$;

DO $$
DECLARE c integer;
BEGIN
  SELECT count(*) INTO c
  FROM pragma_rank_native('user_test', 'offline', ARRAY['canonical','proposition'], 10);
  IF c <> 0 THEN
    RAISE EXCEPTION 'expected pragma_rank_native stub to return 0 rows, got %', c;
  END IF;
END $$;

DO $$
DECLARE c integer;
BEGIN
  SELECT count(*) INTO c
  FROM pragma_next_hops_native('user_test', ARRAY['offline_sync'], 10);
  IF c <> 0 THEN
    RAISE EXCEPTION 'expected pragma_next_hops_native stub to return 0 rows, got %', c;
  END IF;
END $$;

DO $$
DECLARE c integer;
BEGIN
  SELECT count(*) INTO c
  FROM pragma_rank_zig('user_test', 'offline', ARRAY['canonical','proposition'], 10);
  IF c <> 0 THEN
    RAISE EXCEPTION 'expected pragma_rank_zig stub to return 0 rows, got %', c;
  END IF;
END $$;

DO $$
DECLARE c integer;
BEGIN
  SELECT count(*) INTO c
  FROM pragma_next_hops_zig('user_test', ARRAY['offline_sync'], 10);
  IF c <> 0 THEN
    RAISE EXCEPTION 'expected pragma_next_hops_zig stub to return 0 rows, got %', c;
  END IF;
END $$;

\echo '=== pg_pragma integration tests passed ==='
