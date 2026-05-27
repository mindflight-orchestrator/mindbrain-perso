#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STUDIO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GHOSTCRAB_ROOT="${GHOSTCRAB_ROOT:-$(cd "$STUDIO_ROOT/../ghostcrab-personal-mcp" && pwd)}"
SQLITE_PATH="$STUDIO_ROOT/data/immeuble-demo.sqlite"
BUNDLE="$GHOSTCRAB_ROOT/examples/immeuble-demo/bundle.json"
GCP="$GHOSTCRAB_ROOT/bin/gcp.mjs"
DRY_RUN=false

for arg in "$@"; do
	case "$arg" in
		--dry-run) DRY_RUN=true ;;
		-h | --help)
			echo "Usage: load-immeuble-demo.sh [--dry-run]"
			echo ""
			echo "Loads the immeuble syndic demo bundle into data/immeuble-demo.sqlite."
			echo "Use --dry-run to validate the bundle without writing to SQLite."
			echo "Override GHOSTCRAB_ROOT to point at a non-sibling GhostCrab checkout."
			exit 0
			;;
		*)
			echo "Unknown argument: $arg" >&2
			exit 1
			;;
	esac
done

if [[ ! -f "$GCP" ]]; then
	echo "error: gcp loader not found at $GCP" >&2
	echo "Set GHOSTCRAB_ROOT to your ghostcrab-personal-mcp checkout." >&2
	exit 1
fi

if [[ ! -f "$BUNDLE" ]]; then
	echo "error: bundle not found at $BUNDLE" >&2
	exit 1
fi

if [[ "$DRY_RUN" == true ]]; then
	echo "==> Dry-run bundle validation (no SQLite write)"
	node "$GCP" load "$BUNDLE" --dry-run
	exit 0
fi

mkdir -p "$STUDIO_ROOT/data"

echo "==> Loading bundle into $SQLITE_PATH"
GHOSTCRAB_SQLITE_PATH="$SQLITE_PATH" \
	node "$GCP" load "$BUNDLE" \
		--workspace immeuble-demo \
		--reindex all \
		--force

if ! command -v sqlite3 >/dev/null 2>&1; then
	echo "warning: sqlite3 not found; skipping post-load smoke check" >&2
	exit 0
fi

echo "==> Post-load smoke check"
ENTITY_COUNT="$(
	sqlite3 "$SQLITE_PATH" \
		"SELECT COUNT(*) FROM graph_entity WHERE workspace_id='immeuble-demo';"
)"
echo "graph_entity rows for immeuble-demo: $ENTITY_COUNT"

if [[ "$ENTITY_COUNT" -lt 100 ]]; then
	echo "error: expected at least 100 graph_entity rows after reindex" >&2
	exit 1
fi

echo "==> Done. Start backend and Studio with:"
echo "    pnpm backend:immeuble   # terminal 1"
echo "    pnpm dev:immeuble       # terminal 2"
echo "    bash scripts/demo-immeuble-gaps.sh   # graph gap diagnostics demo"
