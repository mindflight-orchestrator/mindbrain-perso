#!/usr/bin/env sh
# Cross-compile standalone MindBrain binaries for the supported distribution targets.
# Outputs binaries into prebuilds/{platform-arch}/bin/.
# Run from the repository root. Requires Zig 0.16.x and target sqlite3 dev/runtime libs.
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT"

if [ -n "${ZIG:-}" ]; then
    ZIG_BIN="$ZIG"
elif [ -x "$ROOT/.codex/toolchains/zig-x86_64-linux-0.16.0/zig" ]; then
    ZIG_BIN="$ROOT/.codex/toolchains/zig-x86_64-linux-0.16.0/zig"
elif command -v zig-0.16 >/dev/null 2>&1; then
    ZIG_BIN="$(command -v zig-0.16)"
else
    ZIG_BIN="zig"
fi

ZIG_OPTIMIZE="${ZIG_OPTIMIZE:-ReleaseFast}"
ZIG_CACHE="${ZIG_CACHE:-$ROOT/.zig-cache-cross}"
ZIG_LOCAL_CACHE_DIR="${ZIG_LOCAL_CACHE_DIR:-/tmp/mindbrain-zig-local-cache}"
ZIG_GLOBAL_CACHE_DIR="${ZIG_GLOBAL_CACHE_DIR:-$ZIG_CACHE/global}"
export ZIG_LOCAL_CACHE_DIR ZIG_GLOBAL_CACHE_DIR

if ! "$ZIG_BIN" version >/dev/null 2>&1; then
    echo "[prebuilds] Zig binary not executable: $ZIG_BIN" >&2
    exit 1
fi

ZIG_VERSION="$("$ZIG_BIN" version)"
case "$ZIG_VERSION" in
    0.16.*) ;;
    *)
        echo "[prebuilds] Zig 0.16.x required, found $ZIG_VERSION at $ZIG_BIN" >&2
        echo "[prebuilds] Set ZIG=/path/to/zig-0.16 or use the repo-local toolchain under .codex/toolchains." >&2
        exit 1
        ;;
esac

build_one() {
    zig_triple="$1"
    platform_key="$2"

    echo "[prebuilds] $zig_triple -> prebuilds/$platform_key/bin/"
    mkdir -p "prebuilds/$platform_key/bin"

    out_dir="$ROOT/.zig-out-cross/$platform_key"
    rm -rf "$out_dir"

    "$ZIG_BIN" build \
        standalone-tool \
        benchmark-tool \
        standalone-http \
        -Doptimize="$ZIG_OPTIMIZE" \
        -Dtarget="$zig_triple" \
        --prefix "$out_dir" \
        --global-cache-dir "$ZIG_CACHE"

    if [ "$platform_key" = "win32-x64" ]; then
        bin_suffix=".exe"
    else
        bin_suffix=""
    fi

    for binary_name in \
        "mindbrain-standalone-tool$bin_suffix" \
        "mindbrain-benchmark-tool$bin_suffix" \
        "mindbrain-http$bin_suffix"
    do
        target_path="prebuilds/$platform_key/bin/$binary_name"
        temp_target="${target_path}.tmp"
        cp "$out_dir/bin/$binary_name" "$temp_target"
        chmod +x "$temp_target" 2>/dev/null || true
        mv -f "$temp_target" "$target_path"
        echo "[prebuilds]   wrote prebuilds/$platform_key/bin/$binary_name"
    done

    rm -rf "$out_dir"
}

build_one "x86_64-linux-gnu" "linux-x64"
build_one "aarch64-linux-gnu" "linux-arm64"
build_one "x86_64-macos" "darwin-x64"
build_one "aarch64-macos" "darwin-arm64"
build_one "x86_64-windows-gnu" "win32-x64"

echo "[prebuilds] all platforms built."
