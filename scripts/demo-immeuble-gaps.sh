#!/usr/bin/env bash
# Run graph gap diagnostics demo on immeuble-demo.sqlite:
# optional load, import gap-rules.demo.json, fetch diagnostics summary.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GHOSTCRAB_ROOT="${GHOSTCRAB_ROOT:-$(cd "$PROJECT_ROOT/../ghostcrab-personal-mcp" && pwd)}"
SQLITE_PATH="${GHOSTCRAB_SQLITE_PATH:-$PROJECT_ROOT/data/immeuble-demo.sqlite}"
RULES_FILE="${IMMEUBLE_GAP_RULES:-$GHOSTCRAB_ROOT/examples/immeuble-demo/gap-rules.demo.json}"
WORKSPACE_ID="${IMMEUBLE_WORKSPACE_ID:-immeuble-demo}"
BACKEND_URL="${MINDBRAIN_HTTP_URL:-http://127.0.0.1:8092}"
DO_LOAD=false
SIMULATE_ANOMALY=false
USE_HTTP=true

usage() {
	cat <<EOF
Usage: demo-immeuble-gaps.sh [OPTIONS]

Graph gap diagnostics demo for immeuble-demo (roadmap §9 / §9b).

Options:
  --load              Run load-immeuble-demo.sh before diagnostics
  --simulate-anomaly  Remove one assigned_cellar edge (act 3 demo), then run diagnostics
  --cli-only          Use mindbrain-standalone-tool on SQLite (no HTTP backend)
  -h, --help          Show this help

Environment:
  GHOSTCRAB_SQLITE_PATH   SQLite file (default: data/immeuble-demo.sqlite)
  GHOSTCRAB_ROOT          ghostcrab-personal-mcp checkout
  MINDBRAIN_HTTP_URL      Backend base URL (default: http://127.0.0.1:8092)
  IMMEUBLE_GAP_RULES      Rules JSON path
  ZIG                     Zig binary for standalone-tool fallback

Examples:
  pnpm load:immeuble && pnpm backend:immeuble   # terminal 1
  bash scripts/demo-immeuble-gaps.sh            # terminal 2
  bash scripts/demo-immeuble-gaps.sh --simulate-anomaly
EOF
}

for arg in "$@"; do
	case "$arg" in
		--load) DO_LOAD=true ;;
		--simulate-anomaly) SIMULATE_ANOMALY=true ;;
		--cli-only) USE_HTTP=false ;;
		-h | --help)
			usage
			exit 0
			;;
		*)
			echo "Unknown argument: $arg" >&2
			usage >&2
			exit 1
			;;
	esac
done

if [[ "$DO_LOAD" == true ]]; then
	echo "==> Loading immeuble demo bundle"
	bash "$SCRIPT_DIR/load-immeuble-demo.sh"
fi

if [[ ! -f "$SQLITE_PATH" ]]; then
	echo "error: SQLite not found at $SQLITE_PATH" >&2
	echo "Run: pnpm load:immeuble  or  demo-immeuble-gaps.sh --load" >&2
	exit 1
fi

if [[ ! -f "$RULES_FILE" ]]; then
	echo "error: gap rules file not found at $RULES_FILE" >&2
	exit 1
fi

backend_ready() {
	curl -sf "${BACKEND_URL}/health" >/dev/null 2>&1
}

capabilities_http() {
	curl -sf "${BACKEND_URL}/api/mindbrain/capabilities"
}

assert_graph_routes_http() {
	echo "==> Probe backend capabilities"
	local caps
	caps="$(capabilities_http)"
	if command -v jq >/dev/null 2>&1; then
		echo "$caps" | jq '.features'
		if [[ "$(echo "$caps" | jq -r '.features.graph_diagnostics // false')" != "true" ]]; then
			echo "error: backend missing graph_diagnostics capability (stale binary? rebuild ghostcrab-backend)" >&2
			exit 1
		fi
	else
		echo "$caps"
		case "$caps" in
			*"\"graph_diagnostics\":true"*) ;;
			*)
				echo "error: backend missing graph_diagnostics capability (stale binary? rebuild ghostcrab-backend)" >&2
				exit 1
				;;
		esac
	fi
}

resolve_standalone_tool() {
	if [[ -x "$PROJECT_ROOT/zig-out/bin/mindbrain-standalone-tool" ]]; then
		echo "$PROJECT_ROOT/zig-out/bin/mindbrain-standalone-tool"
		return 0
	fi
	local zig_bin="${ZIG:-/opt/zig/zig-x86_64-linux-0.16.0/zig}"
	if [[ -x "$zig_bin" ]] && [[ -f "$PROJECT_ROOT/build.zig" ]]; then
		(
			cd "$PROJECT_ROOT"
			ZIG_LOCAL_CACHE_DIR="${ZIG_LOCAL_CACHE_DIR:-/tmp/zig-cache}" \
				ZIG_GLOBAL_CACHE_DIR="${ZIG_GLOBAL_CACHE_DIR:-/tmp/zig-global-cache}" \
				"$zig_bin" build standalone-tool --summary none 2>/dev/null
		)
		if [[ -x "$PROJECT_ROOT/zig-out/bin/mindbrain-standalone-tool" ]]; then
			echo "$PROJECT_ROOT/zig-out/bin/mindbrain-standalone-tool"
			return 0
		fi
	fi
	return 1
}

import_rules_http() {
	curl -sf -X POST "${BACKEND_URL}/api/mindbrain/graph/gap-rules/import" \
		-H 'Content-Type: application/json' \
		-d @"$RULES_FILE"
}

import_rules_cli() {
	local tool="$1"
	"$tool" graph-gap-rules-import --db "$SQLITE_PATH" --input "$RULES_FILE"
}

diagnostics_http() {
	curl -sf "${BACKEND_URL}/api/mindbrain/graph/diagnostics?workspace_id=${WORKSPACE_ID}&limit=50"
}

diagnostics_cli() {
	local tool="$1"
	"$tool" graph-diagnostics --db "$SQLITE_PATH" --workspace-id "$WORKSPACE_ID" --limit 50 --format json
}

simulate_anomaly() {
	if ! command -v sqlite3 >/dev/null 2>&1; then
		echo "error: sqlite3 required for --simulate-anomaly" >&2
		exit 1
	fi
	echo "==> Simulating anomaly: remove one assigned_cellar for Tilleuls Appartement A3"
	local relation_id
	relation_id="$(
		sqlite3 "$SQLITE_PATH" <<SQL
SELECT r.relation_id
FROM graph_relation r
JOIN graph_entity src ON src.entity_id = r.source_id
JOIN graph_entity tgt ON tgt.entity_id = r.target_id
WHERE r.workspace_id = '${WORKSPACE_ID}'
  AND r.relation_type = 'assigned_cellar'
  AND r.deprecated_at IS NULL
  AND src.name = 'Tilleuls Appartement A3'
  AND tgt.entity_type = 'cellar'
LIMIT 1;
SQL
	)"
	if [[ -z "$relation_id" ]]; then
		echo "warning: no assigned_cellar edge found for Tilleuls Appartement A3; skipping" >&2
		return 0
	fi
	sqlite3 "$SQLITE_PATH" \
		"UPDATE graph_relation SET deprecated_at = datetime('now') WHERE relation_id = ${relation_id};"
	echo "    deprecated relation_id=${relation_id}"
}

print_summary() {
	local json="$1"
	if command -v jq >/dev/null 2>&1; then
		echo "==> Diagnostics summary"
		echo "$json" | jq '.summary // .'
		echo ""
		echo "==> Rule-driven issues (first 10)"
		echo "$json" | jq '[.issues[]? | select(.rule_id != null)] | .[0:10]'
		echo ""
		echo "==> Issue kinds (counts)"
		echo "$json" | jq '[.issues[]?.kind] | group_by(.) | map({kind: .[0], count: length})'
	else
		echo "==> Diagnostics report (install jq for formatted summary)"
		echo "$json"
	fi
}

echo "==> Immeuble graph gap demo"
echo "    DB: $SQLITE_PATH"
echo "    Rules: $RULES_FILE"
echo "    Workspace: $WORKSPACE_ID"

if [[ "$SIMULATE_ANOMALY" == true ]]; then
	simulate_anomaly
fi

report_json=""
if [[ "$USE_HTTP" == true ]] && backend_ready; then
	echo "==> Backend: $BACKEND_URL"
	assert_graph_routes_http
	echo ""
	echo "==> Import gap rules (HTTP)"
	import_rules_http
	echo ""
	echo "==> Run diagnostics (HTTP)"
	report_json="$(diagnostics_http)"
else
	tool="$(resolve_standalone_tool || true)"
	if [[ -z "$tool" ]]; then
		echo "error: backend not reachable at $BACKEND_URL and mindbrain-standalone-tool not built" >&2
		echo "Start: pnpm backend:immeuble   or build: zig build standalone-tool" >&2
		exit 1
	fi
	echo "==> Using CLI: $tool"
	echo "==> Import gap rules (CLI)"
	import_rules_cli "$tool"
	echo ""
	echo "==> Run diagnostics (CLI)"
	report_json="$(diagnostics_cli "$tool")"
fi

print_summary "$report_json"

if [[ "$SIMULATE_ANOMALY" == true ]] && command -v jq >/dev/null 2>&1; then
	missing="$(echo "$report_json" | jq '[.issues[]? | select(.kind == "missing_required_relation" and .rule_id == "unit-one-cellar")] | length')"
	if [[ "$missing" -ge 1 ]]; then
		echo "==> Act 3 OK: missing_required_relation for unit-one-cellar detected"
	else
		echo "warning: expected missing_required_relation for unit-one-cellar after anomaly simulation" >&2
		exit 1
	fi
fi

echo ""
echo "==> Done. MCP: ghostcrab_graph_diagnostics / ghostcrab_graph_gap_rules"
