#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STUDIO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GHOSTCRAB_ROOT="${GHOSTCRAB_ROOT:-$(cd "$STUDIO_ROOT/../ghostcrab-personal-mcp" && pwd)}"
SQLITE_PATH="${GHOSTCRAB_SQLITE_PATH:-$STUDIO_ROOT/data/immeuble-demo.sqlite}"
BACKEND_BIN="$GHOSTCRAB_ROOT/cmd/backend/zig-out/bin/ghostcrab-backend"
ADDR="${GHOSTCRAB_BACKEND_ADDR:-:8092}"

if [[ ! -f "$SQLITE_PATH" ]]; then
	echo "error: SQLite not found at $SQLITE_PATH" >&2
	echo "Run: pnpm load:immeuble" >&2
	exit 1
fi

if [[ ! -x "$BACKEND_BIN" ]]; then
	echo "error: ghostcrab-backend not found at $BACKEND_BIN" >&2
	echo "Run in ghostcrab-personal-mcp: pnpm run backend:build" >&2
	exit 1
fi

PORT="${ADDR#:}"
if command -v ss >/dev/null 2>&1; then
	if ss -tlnH "sport = :$PORT" 2>/dev/null | grep -q .; then
		echo "error: port $PORT already in use (another ghostcrab-backend or GhostCrab MCP may be listening)" >&2
		echo "Stop the other process, or pick a free port:" >&2
		echo "  GHOSTCRAB_BACKEND_ADDR=:8093 MINDBRAIN_HTTP_URL=http://127.0.0.1:8093 pnpm backend:immeuble" >&2
		exit 1
	fi
fi

echo "==> MindBrain backend on $ADDR"
echo "    DB: $SQLITE_PATH"
echo "    Smoke: curl -sf http://127.0.0.1:$PORT/api/mindbrain/workspace/list"
echo ""

export GHOSTCRAB_SQLITE_PATH="$SQLITE_PATH"
export GHOSTCRAB_BACKEND_ADDR="$ADDR"
exec "$BACKEND_BIN"
