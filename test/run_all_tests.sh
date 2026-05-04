#!/usr/bin/env bash
set -euo pipefail

export PGHOST="${PGHOST:-pg_mindbrain_test}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"
export PGDATABASE="${PGDATABASE:-postgres}"

psql_cmd() {
  psql -v ON_ERROR_STOP=1 "$@"
}

wait_for_pg() {
  until psql_cmd -c 'SELECT 1' >/dev/null 2>&1; do
    sleep 1
  done
}

run_sql_file() {
  local file="$1"
  echo
  echo "=============================================="
  echo "Running ${file}"
  echo "=============================================="
  psql_cmd -f "$file"
}

echo "Waiting for PostgreSQL..."
wait_for_pg
echo "PostgreSQL is ready"

echo "Installing extensions..."
psql_cmd -c 'CREATE EXTENSION IF NOT EXISTS roaringbitmap'
psql_cmd -c 'CREATE EXTENSION IF NOT EXISTS vector'
psql_cmd -c 'CREATE EXTENSION IF NOT EXISTS pg_mindbrain'

run_sql_file /tests/sql/facets/minimal_facets_test.sql
run_sql_file /tests/sql/facets/minimal_bm25_test.sql
run_sql_file /tests/sql/graph/minimal_graph_test.sql
run_sql_file /tests/sql/pragma/test_pg_pragma.sql
run_sql_file /tests/sql/ontology/minimal_ontology_test.sql
run_sql_file /tests/sql/ontology/phase2_registry_test.sql

echo
echo "All merged latest-only SQL tests completed"
