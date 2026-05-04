# Document normalization, profile, LLM, and durable queue

This document describes the **corpus document normalization and profiling** feature: external extractors convert PDF/HTML into text or Markdown, an LLM proposes a structured “semantic id card” ([`corpus_profile.zig`](../src/standalone/corpus_profile.zig)), the code maps it to deterministic chunking ([`chunking_policy.zig`](../src/standalone/chunking_policy.zig) and [`legal_chunker.zig`](../src/standalone/legal_chunker.zig) for legal text), and optional **SQLite-backed jobs** scale directory processing with visibility timeouts and retries.

The CLI for this lives in [`mindbrain-standalone-tool`](../src/standalone/tool.zig) (build with `zig build standalone-tool`).

## Prerequisites

- **Zig 0.16.0** — enforced by [`build.zig`](../build.zig) (use a `zig` binary that reports `0.16.0`).
- **`libsqlite3`** — required to link the standalone tool and tests.
- **Optional extraction tools** — `pdftotext` for PDF text layers, `ocrmypdf` + Tesseract language packs for OCR, and `pandoc` for HTML to Markdown. The CLI also has a minimal `builtin-strip` HTML fallback for CI/simple fixtures.
- **Network** — only when calling a remote LLM (not for `--dry-run` or `--mock-profile-json`).

## LLM providers

The standalone LLM library lives under [`src/standalone/llm/`](../src/standalone/llm) and is re-exported through [`llm.zig`](../src/standalone/llm.zig). Document profiling still uses OpenAI-compatible Chat Completions with JSON-mode requests for compatibility, while the library also exposes provider-neutral types for Responses/OpenResponses, streaming events, tool calls, multimodal message parts, embeddings, and audio transcription.

| Kind | Example base URL | Chat | Responses | Streaming | Tools | Images/audio parts | Embeddings | Audio transcription | State notes |
|------|------------------|------|-----------|-----------|-------|--------------------|------------|---------------------|-------------|
| OpenAI-compatible | `https://api.openai.com/v1` | Yes | Yes | Chat + Responses | Yes | Yes | Yes | Yes | Use `store: false` for stateless calls; `previous_response_id` is available on OpenAI. |
| Ollama | `http://127.0.0.1:11434/v1` | Yes | Yes, recent Ollama versions | Chat + Responses when supported | Depends on model/server | Depends on model/server | Yes, when `/v1/embeddings` is exposed | No | Responses are stateless: no `previous_response_id` or conversation object. |
| vLLM / llama.cpp server | your server `/v1` | Yes | Provider-dependent | When exposed | Depends on server | Depends on server | Yes, when exposed | No | Treat as stateless unless the server documents Responses state. |
| OpenRouter / other gateways | provider URL + `/v1` | Yes | Beta/OpenResponses | Yes | Yes, provider permitting | Yes, provider permitting | Provider-dependent | Provider-dependent | OpenRouter Responses are beta and stateless; include full history each request. |
| Gemini REST | `https://generativelanguage.googleapis.com/v1beta` | Yes | Via native `generateContent` adapter | Native streaming separately | Native tools later | Images/audio/file inline parts are rendered | Yes | Not yet | Does not expose OpenAI Responses; library normalizes native output into `ResponseResult`. |

Use `Manager.chat` for legacy Chat Completions workflows and `Manager.respond` for new OpenResponses-style generation. Responses structured output uses `text.format`; Chat Completions JSON mode still uses `response_format`.

**Security:** do not put API keys in committed scripts. Prefer environment variables and shell substitution (e.g. `export OPENAI_API_KEY=...` then `--api-key "$OPENAI_API_KEY"`).

## Commands

### `document-normalize`

Converts one PDF/HTML/text source into normalized `.md` or `.txt` files plus sidecar metadata before profiling.

Required:

- `--input <path>`
- `--output-dir <dir>`

Useful options:

- `--languages fr,nl` — language hints. For OCRmyPDF these map to Tesseract codes `fra+nld`.
- `--split-by-language` — for Belgian bilingual sources, emit separate normalized outputs when the text can be separated. Ambiguous documents fall back to one `language = und` output with `needs_language_split = true`.
- `--pdf-backend auto|pdftotext|ocrmypdf|deepseek|none` — default `auto`: try `pdftotext -layout` first, then OCR fallback if extracted text is too small.
- `--ocr-backend ocrmypdf|deepseek|none` — shorthand for OCR fallback selection.
- `--html-backend pandoc|builtin-strip` — default `pandoc`. `builtin-strip` is a lightweight fallback, not a full HTML parser.
- `--deepseek-command <template>` — shell command for a custom DeepSeek-OCR wrapper. The template may use `{input}`, `{output_dir}`, and `{output_text}` placeholders; the command must write normalized text/Markdown to `{output_text}`.

Outputs:

- normalized content files, for example `moniteur-belge-20241218-01-fr.txt` and `...-nl.txt`;
- one `*.metadata.json` sidecar per content file;
- `manifest.json` in the output directory.

The profiling enqueue command reads sidecars automatically when present, so normalized FR/NL files keep their per-file language and original source path in queue jobs.

### `document-profile`

One-shot profiling of a single file, inline text, or a whole directory (JSON array to stdout).

**Input (exactly one):**

- `--content <text>` — inline body, or
- `--content-file <path>` — read file, or
- `--content-dir <path>` — recurse regular files, sorted by path for stable order.

**LLM (exactly one mode):**

- `--base-url <url>` and `--model <name>` — live call; optional `--api-key <key>`.
- `--mock-profile-json <path>` — read JSON, validate with `corpus_profile.parseJson`, return as the profile (no network).
- `--dry-run` — print JSON with prompt messages and sample parameters (no LLM, no strict validation of profile shape).

**Optional:** `--source-ref <ref>`, `--sample-chars <n>` (default 12000), `--temperature`, `--max-tokens` (default 1200).

**Output:** for a single file or `--content`, one JSON object (the profile, or dry-run payload). For `--content-dir`, a JSON array of per-file results (`ok` / `error`, `source_ref`, `profile` or error name).

### `document-profile-enqueue`

Enqueues one or more profiling jobs in the **SQLite message queue** (table `queue_messages`, default queue name `document_profile`).

**Required:** `--db <sqlite_path>` and exactly one of `--content-file` or `--content-dir`.

**Optional:** `--queue <name>`, `--source-ref` (single-file only; else path is the ref), `--sample-chars`, `--language` (default `english`), `--include-ext md,txt` for normalized directories with manifest/metadata files.

**Optional persistence target** (embed in each job; required together when used):

- `--workspace-id <id>` and `--collection-id <id>`, and
- for `--content-file`: `--doc-id <n>`;
- for `--content-dir`: `--doc-id-start <n>` (increments by one per file in sorted path order; do not combine with `--doc-id`).

Prints: `{"queue":"…","enqueued":<count>}`.

### `document-profile-worker`

Pulls up to `--limit` messages with a **visibility timeout** (`--vt`, default 300 seconds), runs profiling, optionally **persists** raw documents and chunks, then **archives** successful jobs.

**Required:** `--db`, and either (`--base-url` and `--model`) or `--mock-profile-json`.

**Optional:** `--queue`, `--api-key`, `--vt`, `--limit`, `--temperature`, `--max-tokens`, `--archive-failures` (archive messages that fail parse/read/LLM so they do not retry; default is to leave them for retry when the lease expires).

**Optional contextual retrieval:** pass `--contextual-retrieval` to generate a concise LLM-written context for each persisted chunk, following Anthropic's Contextual Retrieval pattern of situating a chunk within the whole document before embedding/BM25 indexing. The worker keeps `chunks_raw.content` as the original chunk text and stores the generated context plus `contextualized_content` under `chunks_raw.metadata_json.contextual_retrieval`, so raw evidence remains unchanged. With `--contextual-search-table-id <n>` and `--embedding-model <name>`, the worker also indexes the contextualized chunk text into BM25/search artifacts and writes matching embeddings to `search_embeddings` and `chunks_raw_vector`. Use `--embedding-base-url` / `--embedding-api-key` when the embedding provider differs from the profile provider; otherwise the worker reuses `--base-url` / `--api-key`. Use `--contextual-doc-chars <n>` (default `60000`) to cap the document context supplied to the LLM and `--contextual-max-tokens <n>` (default `180`) to cap the generated chunk context.

**When the job includes `workspace_id`, `collection_id`, and `doc_id`:** the worker ensures workspace/collection rows, upserts `documents_raw` with full text and `metadata_json` containing the validated profile and **chunking decision**, then writes `chunks_raw` using the selected splitter (including legal splitters when `chunking_policy` marks the profile as specialized). Output JSON includes `persisted: true` and `chunk_count`.

**When those fields are absent:** output is only the profile JSON; no `documents_raw` / `chunks_raw` writes.

To inspect stored rows, use `collection-export` (see [standalone.md](standalone.md)).

### `corpus-eval`

Offline evaluation of **chunking and profiling** against [fixtures/](../fixtures/corpus_eval/) (no LLM). Optional `--fixtures <dir>`, `--case <name>`.

## End-to-end examples

**OpenAI (live):**

```bash
zig build standalone-tool   # from repo root, with Zig 0.16.0

export OPENAI_API_KEY="..."

./zig-out/bin/mindbrain-standalone-tool document-profile \
  --content-file fixtures/corpus_eval/legal_article/source.txt \
  --base-url https://api.openai.com/v1 \
  --api-key "$OPENAI_API_KEY" \
  --model gpt-4.1-mini
```

**Queue + persist + export:**

```bash
./zig-out/bin/mindbrain-standalone-tool document-profile-enqueue \
  --db data/corpus.sqlite \
  --content-dir ./mytexts \
  --include-ext md,txt \
  --workspace-id my_ws \
  --collection-id my_ws::docs \
  --doc-id-start 1

./zig-out/bin/mindbrain-standalone-tool document-profile-worker \
  --db data/corpus.sqlite \
  --base-url https://api.openai.com/v1 \
  --api-key "$OPENAI_API_KEY" \
  --model gpt-4.1-mini \
  --contextual-retrieval \
  --contextual-search-table-id 77 \
  --embedding-model text-embedding-3-small \
  --limit 4

./zig-out/bin/mindbrain-standalone-tool contextual-search \
  --db data/corpus.sqlite \
  --table-id 77 \
  --query "How can the agreement end after breach?" \
  --base-url https://api.openai.com/v1 \
  --api-key "$OPENAI_API_KEY" \
  --embedding-model text-embedding-3-small \
  --limit 5

./zig-out/bin/mindbrain-standalone-tool collection-export \
  --db data/corpus.sqlite \
  --workspace-id my_ws \
  --collection-id my_ws::docs
```

**PDF/HTML normalization before profiling:**

```bash
./zig-out/bin/mindbrain-standalone-tool document-normalize \
  --input /path/to/sample.pdf \
  --output-dir data/normalized/moniteur-belge-20241218-01 \
  --languages fr,nl \
  --split-by-language \
  --pdf-backend auto \
  --ocr-backend ocrmypdf

./zig-out/bin/mindbrain-standalone-tool document-normalize \
  --input data/justel/federal/fr/justel-2002021488-fr.html \
  --output-dir data/normalized/justel-2002021488-fr \
  --languages fr \
  --html-backend pandoc

./zig-out/bin/mindbrain-standalone-tool document-profile-enqueue \
  --db data/belgian-legal.sqlite \
  --content-dir data/normalized/moniteur-belge-20241218-01 \
  --include-ext txt,md \
  --workspace-id belgian_legal \
  --collection-id belgian_legal::federal \
  --doc-id-start 1
```

**No network (CI / regression):**

```bash
./zig-out/bin/mindbrain-standalone-tool document-profile \
  --content-file fixtures/corpus_eval/legal_article/source.txt \
  --mock-profile-json fixtures/corpus_eval/legal_article/expected_profile.json
```

## Related code and docs

- [Chunking](chunking.md) — chunk strategies and raw tables.
- [Collections](collections.md) — `documents_raw` / `chunks_raw` contract.
- [Projections](projections.md) — agent-facing views; chunk-first projection planning.
- [Standalone](standalone.md) — build targets and the rest of the CLI.
