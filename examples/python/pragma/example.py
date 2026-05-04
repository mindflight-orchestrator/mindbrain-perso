#!/usr/bin/env python3
"""
Example: using pg_pragma with the Proposition DSL from Python.

Run: python example.py
Requires: psycopg (pip install psycopg[binary])
Env: POSTGRES_DSN (default: postgres://mindbrain:mindbrain@localhost:5432/mindbrain)
"""
import os
import json

import psycopg
from psycopg.rows import dict_row


def main() -> None:
    dsn = os.environ.get("POSTGRES_DSN", "postgres://mindbrain:mindbrain@localhost:5432/mindbrain")

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            # 1. Ensure extension
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_pragma")
            conn.commit()

            # 2. Parse a DSL line
            line = "fact|id=f42|subject=offline_sync|predicate=blocked_by|object=ios_background_limits|conf=0.91"
            cur.execute("SELECT pragma_parse_proposition_line(%s) AS parsed", (line,))
            row = cur.fetchone()
            parsed = row["parsed"] if row else None
            print("Parsed:", json.dumps(parsed, indent=2) if isinstance(parsed, dict) else parsed)

            # 3. Ensure schema and tables
            cur.execute('CREATE SCHEMA IF NOT EXISTS "memory-server"')
            cur.execute("""
                CREATE TABLE IF NOT EXISTS "memory-server".memory_items (
                    id TEXT PRIMARY KEY, user_id TEXT NOT NULL, source_type TEXT, source_ref TEXT,
                    importance FLOAT8 DEFAULT 0, confidence FLOAT8 DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS "memory-server".memory_projections (
                    id TEXT PRIMARY KEY, item_id TEXT NOT NULL, user_id TEXT NOT NULL,
                    projection_type TEXT NOT NULL, content TEXT NOT NULL,
                    content_tsvector TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', content)) STORED,
                    facets JSONB DEFAULT '{}', metadata JSONB DEFAULT '{}',
                    rank_hint FLOAT8 DEFAULT 0, confidence FLOAT8 DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.commit()

            # 4. Insert proposition DSL content
            user_id = "user_example"
            item_id = "item_1"
            proj_id = "proj_1"
            content = """fact|id=f1|subject=user|predicate=wants|object=offline_sync|conf=0.95
constraint|id=c1|scope=memory|rule=keep_context_small
goal|id=g1|actor=user|wants=dynamic_memory"""

            cur.execute(
                '''INSERT INTO "memory-server".memory_items (id, user_id, source_type, source_ref)
                   VALUES (%s, %s, 'event', 'evt_1') ON CONFLICT (id) DO NOTHING''',
                (item_id, user_id),
            )
            cur.execute(
                """INSERT INTO "memory-server".memory_projections (id, item_id, user_id, projection_type, content, rank_hint)
                   VALUES (%s, %s, %s, 'proposition', %s, 0.9)
                   ON CONFLICT (id) DO UPDATE SET content = EXCLUDED.content""",
                (proj_id, item_id, user_id, content),
            )
            conn.commit()

            # 5. Pack context
            cur.execute(
                "SELECT id, item_id, projection_type, content FROM pragma_pack_context(%s, %s, 5)",
                (user_id, "offline"),
            )
            rows = cur.fetchall()
            print("Pack context results:")
            for r in rows:
                cnt = r["content"] or ""
                preview = cnt[:60] + "..." if len(cnt) > 60 else cnt
                print(f"  {r['id']} | {r['item_id']} | {r['projection_type']} | {preview!r}")


if __name__ == "__main__":
    main()
