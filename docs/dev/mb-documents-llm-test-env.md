# SQLite document LLM test environment

This page covers the standalone SQLite document LLM flows. The default tests
and deterministic mock paths run locally; live provider checks are opt-in.

## What needs which variables

| Activity | SQLite file | LLM variables | Network to LLM API |
| --- | --- | --- | --- |
| `zig build test-standalone` | No | No | No |
| `mindbrain-standalone-tool document-profile --dry-run` | No | No | No |
| `document-profile` with `--mock-profile-json` | No | No | No |
| `document-qualify --dry-run` | Yes | No | No |
| `document-qualify --mock-qualification-json` | Yes | No | No |
| `document-business-extract --input-json` | Yes | No | No |
| Live `document-profile`, `document-qualify`, or `document-business-extract` | Optional/command-specific | Provider key/model | Yes |

## Provider variables

The standalone CLI loads repo-root `.env` when present, then command-line flags
override it. Provider-specific variables take precedence over generic
`MB_DOCUMENTS_LLM_*` values when `--llm-provider` or
`MB_DOCUMENTS_LLM_PROVIDER` selects that provider.

| Variable | Role |
| --- | --- |
| `MB_DOCUMENTS_LLM_PROVIDER` | `openai`, `openrouter`, or `anthropic`; CLI `--llm-provider` overrides it. |
| `MB_DOCUMENTS_LLM_BASE_URL` | Generic API root, defaulting to OpenAI-compatible `https://api.openai.com/v1`. |
| `MB_DOCUMENTS_LLM_MODEL` | Generic chat model id. |
| `MB_DOCUMENTS_LLM_API_KEY` | Generic bearer token for OpenAI-compatible HTTP APIs. |
| `OPENAI_API_KEY` | OpenAI API key fallback when provider is `openai` and the generic key is empty. |
| `OPENROUTER_BASE_URL` | OpenRouter OpenAI-compatible root, default `https://openrouter.ai/api/v1`. |
| `OPENROUTER_API_KEY` | OpenRouter API key. |
| `OPENROUTER_CHAT_MODEL` | Primary OpenRouter chat model. |
| `OPENROUTER_CHAT_FALLBACK_MODEL` | Fallback OpenRouter chat model. |
| `OPENROUTER_CHAT_SMOKE_TEST` | Low-cost/smoke OpenRouter model for live checks. |
| `ANTHROPIC_BASE_URL` | Anthropic native API root, default `https://api.anthropic.com/v1`. |
| `ANTHROPIC_API_KEY` | Anthropic API key. |
| `ANTHROPIC_MODEL` | Anthropic model id; current example/default is `claude-haiku-4-5-20251001`. |
| `ANTHROPIC_VERSION` | Anthropic API version header, default `2023-06-01`. |

## Quick commands

**Standalone unit tests, no LLM:**

```bash
ZIG_LOCAL_CACHE_DIR=/tmp/zig-cache ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache /usr/local/bin/zig-0.16 build test-standalone
```

**Build the standalone CLI:**

```bash
ZIG_LOCAL_CACHE_DIR=/tmp/zig-cache ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache /usr/local/bin/zig-0.16 build standalone-tool
```

**Inspect the document profile prompt split without a network call:**

```bash
./zig-out/bin/mindbrain-standalone-tool document-profile \
  --content "Article 1. Operators must document access control decisions." \
  --source-ref live-smoke.txt \
  --dry-run
```

**Run opt-in live provider smoke checks:**

```bash
MINDBRAIN_LIVE_LLM_TESTS=1 scripts/llm-provider-live-smoke.sh
```

The smoke script parses only known LLM keys from `.env`, does not print secrets,
tries OpenRouter smoke/primary/fallback models in order, and treats provider
rate-limit, quota, billing, or model-shape failures as skips when a later
configured model can still validate the provider wiring.

## Related files

- [.env.example](../../.env.example) - provider examples.
- [scripts/llm-provider-live-smoke.sh](../../scripts/llm-provider-live-smoke.sh) - opt-in live provider smoke.
- [docs/document-profile.md](../document-profile.md) - standalone document profiling, queue, and chunking behavior.
