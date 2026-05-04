# `mb_documents` tests and LLM environment

This page is for people running **SQL integration tests** or the **Zig `mb-document-worker`** against a real LLM. The PostgreSQL extension and PL/pgSQL helpers do not read API keys or call remote HTTP services.

For the full operator guide (entrypoints, queue, datasets), see [pg-native-document-pipeline.md](pg-native-document-pipeline.md).

## What needs which variables

| Activity | `PG*` | `MB_DOCUMENTS_LLM_*` | Network to LLM API |
| --- | --- | --- | --- |
| `test/run_all_tests.sh` (including `test/sql/documents/*.sql`) | Yes | No | No |
| `zig build test` (host unit tests) | No | No | No |
| `mb-document-worker` `--mode mock` | Yes | Optional (defaults to mock) | No |
| `mb-document-worker` `--mode openai` | Yes | `MB_DOCUMENTS_LLM_API_KEY` (or `--api-key`) | Yes |
| `mb-document-worker` `--enqueue-only` | Yes | No (no profile step) | No |

`test/run_all_tests.sh` sources the repo-root `.env` when it exists, so the same `PGHOST` / `PGPORT` / `PGUSER` / `PGPASSWORD` / `PGDATABASE` values you use for `psql` apply. On a host, `PGHOST=pg_mindbrain_test` is rewritten to `127.0.0.1` by the script (same idea as the worker’s `.env` handling).

## SQL tests under `test/sql/documents/`

| File | What it checks |
| --- | --- |
| `profile_queue_mock_test.sql` | `profile_validate`, queue enqueue/claim/archive, and `profile_ingest_mock` updating `mb_collections` with a fixed profile JSON. |
| `profile_chunked_ingest_test.sql` | `chunk_plan` (Zig-backed chunking), `profile_ingest_chunked`, and persisted chunks / `source.*` facets in `mb_collections`. |

These scripts use **embedded profile JSON and content**; they never call the worker and never need an LLM.

## Variables consumed by `mb-document-worker`

The worker loads repo-root `.env` (if present) and then command-line flags override. Only these names are read for LLM and database access:

| Variable | Role |
| --- | --- |
| `MB_DOCUMENTS_LLM_MODE` | `mock` or `openai`. In `openai` mode, `MB_DOCUMENTS_LLM_API_KEY` (or `--api-key`) is required. |
| `MB_DOCUMENTS_LLM_BASE_URL` | OpenAI-compatible API root (e.g. `https://api.openai.com/v1` or an OpenRouter-style URL with `/v1`). |
| `MB_DOCUMENTS_LLM_MODEL` | Chat model id for the completions call. |
| `MB_DOCUMENTS_LLM_API_KEY` | Bearer token for the HTTP API. |
| `MB_DOCUMENTS_LLM_TIMEOUT_MS` | Milliseconds for the OpenAI HTTP call; also settable with `--timeout-ms`. The worker passes `max(1, timeout_ms/1000)` to `curl --max-time` (seconds). |
| `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` | Used to build a libpq DSN for `psql` when running ingest SQL. |
| `PSQL_BIN` | Optional path to the `psql` executable (default `psql`). |

The extension itself does not read any of the `MB_DOCUMENTS_*` variables.

## Quick commands

**Automated SQL tests (no LLM):**

```bash
bash test/run_all_tests.sh
```

**Import with deterministic profiles (no LLM, needs PostgreSQL and built worker):**

```bash
zig build
./zig-out/bin/mb-document-worker \
  --input-dir datasets/qualification/owasp-tech-md \
  --workspace-id qualification \
  --collection-id owasp-tech \
  --doc-id-start 1 \
  --mode mock
```

**Import with a real OpenAI-compatible endpoint:**

```bash
export MB_DOCUMENTS_LLM_MODE=openai
export MB_DOCUMENTS_LLM_API_KEY=sk-...
# Optional: OpenRouter or another /v1-compatible base
# export MB_DOCUMENTS_LLM_BASE_URL=https://openrouter.ai/api/v1
./zig-out/bin/mb-document-worker \
  --input-dir datasets/qualification/owasp-tech-md \
  --workspace-id qualification \
  --collection-id owasp-tech \
  --doc-id-start 1 \
  --mode openai
```

After a qualification run, use the verification queries in [pg-native-document-pipeline.md](pg-native-document-pipeline.md) to inspect `mb_collections` (documents, chunks, facet assignments). Directed graph building remains a separate step from document ingest.

## Related files

- [.env.example](../.env.example) — copy to `.env` and fill `PG*`; add `MB_DOCUMENTS_LLM_*` when using `openai` mode.
- [test/run_all_tests.sh](../test/run_all_tests.sh) — installs `pg_mindbrain` and runs document SQL tests among others.
- [test/README.md](../test/README.md) — general test layout.
