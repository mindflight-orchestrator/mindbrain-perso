#!/usr/bin/env bash
# SQLite parity smoke runner.
#
# Drives the standalone SQLite-backed API surface end-to-end without
# requiring a PostgreSQL host. This is the no-Postgres equivalent of
# test/run_all_tests.sh and exercises the same parity contract that
# the PostgreSQL SQL tests anchor.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}/.."

echo "=============================================="
echo "SQLite parity smoke (zig build test)"
echo "=============================================="
zig build test

echo
echo "All SQLite parity tests passed."
