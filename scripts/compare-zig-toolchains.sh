#!/usr/bin/env bash
set -u

usage() {
  printf '%s\n' "Usage: $0 [options]"
  printf '%s\n' ""
  printf '%s\n' "Compare Zig 0.15.2 on a base ref against Zig 0.16.0 on the current tree."
  printf '%s\n' ""
  printf '%s\n' "Options:"
  printf '%s\n' "  --ref-015 REF        Git ref to benchmark with Zig 0.15.2 (default: HEAD)"
  printf '%s\n' "  --zig-015 PATH       Zig 0.15.2 executable (default: first zig on PATH)"
  printf '%s\n' "  --zig-016 PATH       Zig 0.16.0 executable (default: .codex/toolchains/zig-x86_64-linux-0.16.0/zig)"
  printf '%s\n' "  --iterations N       Number of iterations per step/toolchain (default: 3)"
  printf '%s\n' "  --out-dir DIR        Output directory (default: .codex/zig-benchmarks/<timestamp>)"
  printf '%s\n' "  --build-tools        Also benchmark standalone-tool and benchmark-tool build steps"
  printf '%s\n' "  --runtime            Also run zig build bench-standalone"
  printf '%s\n' "  -h, --help           Show this help"
}

repo_root="$(git rev-parse --show-toplevel)"
if git -C "$repo_root" rev-parse --verify --quiet main >/dev/null; then
  ref_015="main"
else
  ref_015="HEAD~1"
fi
zig_015="$(command -v zig || true)"
zig_016="$repo_root/.codex/toolchains/zig-x86_64-linux-0.16.0/zig"
iterations=3
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="$repo_root/.codex/zig-benchmarks/$timestamp"
include_build_tools=0
include_runtime=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --ref-015)
      ref_015="$2"
      shift 2
      ;;
    --zig-015)
      zig_015="$2"
      shift 2
      ;;
    --zig-016)
      zig_016="$2"
      shift 2
      ;;
    --iterations)
      iterations="$2"
      shift 2
      ;;
    --out-dir)
      out_dir="$2"
      shift 2
      ;;
    --build-tools)
      include_build_tools=1
      shift
      ;;
    --runtime)
      include_runtime=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [ -z "$zig_015" ] || [ ! -x "$zig_015" ]; then
  printf 'Zig 0.15.2 executable not found. Pass --zig-015 PATH.\n' >&2
  exit 1
fi

if [ ! -x "$zig_016" ]; then
  printf 'Zig 0.16.0 executable not found at %s. Pass --zig-016 PATH.\n' "$zig_016" >&2
  exit 1
fi

zig_015_version="$("$zig_015" version)"
zig_016_version="$("$zig_016" version)"

if [ "$zig_015_version" != "0.15.2" ]; then
  printf 'Expected Zig 0.15.2 for --zig-015, got %s from %s.\n' "$zig_015_version" "$zig_015" >&2
  exit 1
fi

if [ "$zig_016_version" != "0.16.0" ]; then
  printf 'Expected Zig 0.16.0 for --zig-016, got %s from %s.\n' "$zig_016_version" "$zig_016" >&2
  exit 1
fi

mkdir -p "$out_dir/logs" "$out_dir/cache" "$out_dir/worktrees"

worktree_015="$out_dir/worktrees/zig-0.15.2"
git -C "$repo_root" worktree add --detach "$worktree_015" "$ref_015" >/dev/null

results_csv="$out_dir/results.csv"
summary_md="$out_dir/summary.md"
printf 'toolchain,version,step,iteration,exit_code,elapsed_ms,log_file\n' > "$results_csv"

cleanup() {
  git -C "$repo_root" worktree remove --force "$worktree_015" >/dev/null 2>&1 || true
}
trap cleanup EXIT

steps=("test")
if [ "$include_build_tools" -eq 1 ]; then
  steps+=("standalone-tool" "benchmark-tool")
fi
if [ "$include_runtime" -eq 1 ]; then
  steps+=("bench-standalone")
fi

run_one() {
  toolchain="$1"
  version="$2"
  zig="$3"
  workdir="$4"
  step="$5"
  iteration="$6"

  cache_dir="$out_dir/cache/$toolchain/$step/$iteration/local"
  global_cache_dir="$out_dir/cache/$toolchain/$step/$iteration/global"
  mkdir -p "$cache_dir" "$global_cache_dir"

  log_file="$out_dir/logs/${toolchain}_${step}_${iteration}.log"
  printf 'toolchain=%s\nversion=%s\nstep=%s\niteration=%s\nworkdir=%s\n\n' \
    "$toolchain" "$version" "$step" "$iteration" "$workdir" > "$log_file"

  start_ns="$(date +%s%N)"
  (
    cd "$workdir" &&
      "$zig" build "$step" --cache-dir "$cache_dir" --global-cache-dir "$global_cache_dir"
  ) >> "$log_file" 2>&1
  exit_code="$?"
  end_ns="$(date +%s%N)"
  elapsed_ms="$(( (end_ns - start_ns) / 1000000 ))"

  printf '%s,%s,%s,%s,%s,%s,%s\n' \
    "$toolchain" "$version" "$step" "$iteration" "$exit_code" "$elapsed_ms" "$log_file" >> "$results_csv"
}

for step in "${steps[@]}"; do
  i=1
  while [ "$i" -le "$iterations" ]; do
    run_one "zig-0.15.2" "$zig_015_version" "$zig_015" "$worktree_015" "$step" "$i"
    run_one "zig-0.16.0" "$zig_016_version" "$zig_016" "$repo_root" "$step" "$i"
    i="$((i + 1))"
  done
done

{
  printf '# Zig Toolchain Benchmark\n\n'
  printf '%s\n' "- Generated: \`$timestamp\`"
  printf '%s\n' "- Zig 0.15.2 ref: \`$ref_015\`"
  printf '%s\n' "- Zig 0.15.2 executable: \`$zig_015\`"
  printf '%s\n' "- Zig 0.16.0 executable: \`$zig_016\`"
  printf '%s\n' "- Iterations: \`$iterations\`"
  printf '%s\n\n' "- Results CSV: \`$results_csv\`"
  printf '## Summary\n\n'
  printf '| Toolchain | Step | Runs | Successful Runs | Average ms | Best ms | Worst ms |\n'
  printf '| --- | --- | ---: | ---: | ---: | ---: | ---: |\n'
  awk -F, '
    NR == 1 { next }
    {
      key = $1 "|" $3
      runs[key] += 1
      if ($5 == 0) ok[key] += 1
      sum[key] += $6
      if (!(key in best) || $6 < best[key]) best[key] = $6
      if (!(key in worst) || $6 > worst[key]) worst[key] = $6
    }
    END {
      for (key in runs) {
        split(key, parts, "|")
        printf "| `%s` | `%s` | %d | %d | %.0f | %d | %d |\n", parts[1], parts[2], runs[key], ok[key], sum[key] / runs[key], best[key], worst[key]
      }
    }
  ' "$results_csv"
  printf '\n## Notes\n\n'
  printf '%s\n' "- A non-zero exit code is still recorded because the 0.16 migration is in progress."
  printf '%s\n' "- Use the per-run logs under \`$out_dir/logs/\` to inspect compiler errors and benchmark output."
} > "$summary_md"

printf 'Benchmark complete.\n'
printf 'Summary: %s\n' "$summary_md"
printf 'CSV: %s\n' "$results_csv"
