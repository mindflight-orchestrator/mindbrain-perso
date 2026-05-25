# Dynamic LLM Provider Resolution

## Summary

MindBrain document LLM flows should support OpenAI, OpenRouter, and native
Anthropic without rewriting `MB_DOCUMENTS_LLM_BASE_URL` by hand. The CLI should
resolve provider settings from explicit flags first, then provider-specific
environment variables, then generic `MB_DOCUMENTS_LLM_*` defaults.

## Provider Endpoints

- OpenAI/OpenAI-compatible: `https://api.openai.com/v1`
- OpenRouter: `https://openrouter.ai/api/v1`
- Anthropic native: `https://api.anthropic.com/v1`, using `POST /v1/messages`
  and `ANTHROPIC_VERSION=2023-06-01`

## Implementation Plan

- [x] Add a shared provider resolver for document commands.
- [x] Add native Anthropic Messages API chat support while keeping OpenRouter on the
  existing OpenAI-compatible path.
- [x] Add `--llm-provider openai|openrouter|anthropic` to document profile,
  qualification, profile-worker, and business-extraction commands.
- [x] Move qualification, business-extraction, and document-profile instruction
  contracts into the system prompt, leaving source documents and variable
  requests in the user prompt.
- [x] Keep request debug output provider-specific so OpenAI/OpenRouter, Anthropic,
  and Gemini render their real wire shapes.
- [x] Add an opt-in live smoke script that parses `.env` safely without printing
  secrets and classifies provider quota/rate-limit conditions as skips.
- [ ] Run a full import-flow audit across providers after Anthropic billing is
  available: document profile -> qualification -> business extraction -> apply.

## Validation

- Unit tests cover provider resolution, Anthropic request rendering/parsing,
  prompt validation bounds, and the system/user prompt split for profile,
  qualification, and business-extraction prompts.
- Full standalone validation:

```sh
ZIG_LOCAL_CACHE_DIR=/tmp/zig-cache ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache /usr/local/bin/zig-0.16 build test-standalone
```

- Live provider checks remain opt-in and must never commit secrets.

## Current Status - 2026-05-25

- OpenRouter `/v1/chat/completions` is wired through the OpenAI-compatible
  client. The free smoke model was rate-limited upstream, then
  `qwen/qwen3-coder-next` returned a valid document profile JSON after the
  profile prompt was tightened with the chunk-budget bounds enforced by the
  validator.
- Anthropic `/v1/models` was checked directly. `claude-4-sonnet` is not a valid
  model id; the smallest listed model is `claude-haiku-4-5-20251001`, now used
  as the Anthropic default.
- Anthropic `/v1/messages` is wired natively with top-level `system`,
  `anthropic-version`, and `x-api-key`. Live generation is currently blocked by
  account credit balance, so it is classified as a provider availability skip.
- The live smoke script now keeps trying OpenRouter smoke, primary, and fallback
  models, and reports provider availability or model-shape failures without
  treating them as code regressions when a later configured model succeeds.
- `docs/plan/` is ignored by the repository `.gitignore`; this file exists in
  the working tree but needs force-add or an ignore-rule change if it should be
  committed.
