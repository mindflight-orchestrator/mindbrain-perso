#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ZIG_BIN="${ZIG:-/opt/zig/zig-x86_64-linux-0.16.0/zig}"
PORT="${MINDBRAIN_HTTP_CONTRACT_PORT:-8097}"
BUSY_TIMEOUT_MS="${MINDBRAIN_SQLITE_BUSY_TIMEOUT_MS:-1000}"
ADDR="127.0.0.1:${PORT}"
BASE_URL="http://${ADDR}"
DB_PATH="$(mktemp "/tmp/mindbrain-http-contract.XXXXXX.sqlite")"
LOG_PATH="$(mktemp "/tmp/mindbrain-http-contract.XXXXXX.log")"

server_pid=""
cleanup() {
  if [[ -n "${server_pid}" ]]; then
    kill "${server_pid}" >/dev/null 2>&1 || true
    wait "${server_pid}" >/dev/null 2>&1 || true
  fi
  rm -f "${DB_PATH}" "${LOG_PATH}"
}
trap cleanup EXIT

cd "${ROOT_DIR}"
ZIG_LOCAL_CACHE_DIR="${ZIG_LOCAL_CACHE_DIR:-/tmp/zig-cache}" \
ZIG_GLOBAL_CACHE_DIR="${ZIG_GLOBAL_CACHE_DIR:-/tmp/zig-global-cache}" \
  "${ZIG_BIN}" build standalone-http --summary all --error-style minimal

MINDBRAIN_DB_PATH="${DB_PATH}" MINDBRAIN_SQLITE_BUSY_TIMEOUT_MS="${BUSY_TIMEOUT_MS}" ./zig-out/bin/mindbrain-http --addr "${ADDR}" >"${LOG_PATH}" 2>&1 &
server_pid="$!"

for _ in {1..80}; do
  if curl -fsS "${BASE_URL}/health" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "${server_pid}" >/dev/null 2>&1; then
    cat "${LOG_PATH}" >&2
    exit 1
  fi
  sleep 0.1
done

curl -fsS "${BASE_URL}/health" >/dev/null

json_field() {
  local json="$1"
  local expr="$2"
  JSON_PAYLOAD="${json}" NODE_EXPR="${expr}" node -e '
const value = JSON.parse(process.env.JSON_PAYLOAD);
const result = Function("value", `"use strict"; return (${process.env.NODE_EXPR});`)(value);
if (result === undefined || result === null) process.exit(2);
process.stdout.write(String(result));
'
}

assert_json() {
  local json="$1"
  local expr="$2"
  JSON_PAYLOAD="${json}" NODE_EXPR="${expr}" node -e '
const value = JSON.parse(process.env.JSON_PAYLOAD);
const ok = Function("value", `"use strict"; return Boolean(${process.env.NODE_EXPR});`)(value);
if (!ok) {
  console.error("assertion failed:", process.env.NODE_EXPR);
  console.error(JSON.stringify(value, null, 2));
  process.exit(1);
}
'
}

post_json() {
  local path="$1"
  local body="$2"
  curl -fsS -H "Content-Type: application/json" -X POST "${BASE_URL}${path}" --data "${body}"
}

get_json() {
  local path="$1"
  curl -fsS "${BASE_URL}${path}"
}

initial_write_status="$(get_json /api/mindbrain/sql/write-status)"
assert_json "${initial_write_status}" "value.ok === true && value.mode === 'serialized-writer' && value.active_session_id === null && typeof value.completed === 'number' && typeof value.failed === 'number' && value.busy_timeout_ms === ${BUSY_TIMEOUT_MS}"

append="$(post_json /api/mindbrain/facts/write '{"schema_id":"ghostcrab.fact","content":"append smoke","facets_json":"{\"kind\":\"note\"}"}')"
assert_json "${append}" "value.ok === true && value.created === true && value.updated === false && value.doc_id === 1"
after_append_status="$(get_json /api/mindbrain/sql/write-status)"
assert_json "${after_append_status}" "value.ok === true && value.completed >= 1 && value.active_session_id === null"

source_create="$(post_json /api/mindbrain/facts/write '{"workspace_id":"ws","schema_id":"ghostcrab.fact","content":"source smoke","facets_json":"{\"kind\":\"state\"}","source_ref":"sync:1"}')"
assert_json "${source_create}" "value.ok === true && value.created === true && value.updated === false && value.doc_id === 2"
source_id="$(json_field "${source_create}" "value.id")"
source_doc_id="$(json_field "${source_create}" "value.doc_id")"

source_update="$(post_json /api/mindbrain/facts/write '{"workspace_id":"ws","schema_id":"ghostcrab.fact","content":"source smoke updated","facets_json":"{\"kind\":\"state\",\"status\":\"updated\"}","source_ref":"sync:1"}')"
assert_json "${source_update}" "value.ok === true && value.created === false && value.updated === true"
assert_json "${source_update}" "value.id === '${source_id}' && value.doc_id === ${source_doc_id}"

other_workspace="$(post_json /api/mindbrain/facts/write '{"workspace_id":"ws-2","schema_id":"ghostcrab.fact","content":"other workspace","facets_json":"{}","source_ref":"sync:1"}')"
assert_json "${other_workspace}" "value.ok === true && value.created === true && value.doc_id !== ${source_doc_id}"

empty_source_a="$(post_json /api/mindbrain/facts/write '{"schema_id":"ghostcrab.fact","content":"empty source a","facets_json":"{}","source_ref":""}')"
empty_source_b="$(post_json /api/mindbrain/facts/write '{"schema_id":"ghostcrab.fact","content":"empty source b","facets_json":"{}","source_ref":""}')"
empty_a_doc_id="$(json_field "${empty_source_a}" "value.doc_id")"
assert_json "${empty_source_b}" "value.created === true && value.doc_id !== ${empty_a_doc_id}"

invalid_status="$(
  curl -sS -o /tmp/mindbrain-http-contract-invalid.json -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -X POST "${BASE_URL}/api/mindbrain/facts/write" \
    --data '{"schema_id":"ghostcrab.fact","content":"invalid","facets_json":"[]"}'
)"
test "${invalid_status}" = "400"
rm -f /tmp/mindbrain-http-contract-invalid.json

lock_session_open="$(post_json /api/mindbrain/sql/session/open '{}')"
lock_session_id="$(json_field "${lock_session_open}" "value.session_id")"
active_write_status="$(get_json /api/mindbrain/sql/write-status)"
assert_json "${active_write_status}" "value.ok === true && value.active_session_id === ${lock_session_id}"
second_open_body="$(mktemp "/tmp/mindbrain-http-contract-second-open.XXXXXX.json")"
second_open_start_ms="$(date +%s%3N)"
second_open_status="$(
  curl -sS --max-time 10 -o "${second_open_body}" -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -X POST "${BASE_URL}/api/mindbrain/sql/session/open" \
    --data '{}'
)"
second_open_elapsed_ms="$(( $(date +%s%3N) - second_open_start_ms ))"
test "${second_open_status}" = "503"
test "${second_open_elapsed_ms}" -lt 500
second_open_error="$(cat "${second_open_body}")"
rm -f "${second_open_body}"
assert_json "${second_open_error}" "value.ok === false && value.error.code === 'sql_session_busy'"
busy_write_body="$(mktemp "/tmp/mindbrain-http-contract-busy-write.XXXXXX.json")"
busy_write_start_ms="$(date +%s%3N)"
busy_write_status="$(
  curl -sS --max-time 10 -o "${busy_write_body}" -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -X POST "${BASE_URL}/api/mindbrain/sql" \
    --data '{"sql":"INSERT INTO facets (schema_id, content, facets, workspace_id) VALUES (\"busy:http\", \"busy raw\", \"{}\", \"default\")","params":[]}'
)"
busy_write_elapsed_ms="$(( $(date +%s%3N) - busy_write_start_ms ))"
test "${busy_write_status}" = "503"
test "${busy_write_elapsed_ms}" -lt 500
busy_write_error="$(cat "${busy_write_body}")"
rm -f "${busy_write_body}"
assert_json "${busy_write_error}" "value.ok === false && value.error.code === 'sql_session_busy'"
with_write_body="$(mktemp "/tmp/mindbrain-http-contract-with-write.XXXXXX.json")"
with_write_status="$(
  curl -sS --max-time 10 -o "${with_write_body}" -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -X POST "${BASE_URL}/api/mindbrain/sql" \
    --data '{"sql":"WITH row AS (SELECT 1) INSERT INTO facets (schema_id, content, facets, workspace_id) SELECT \"busy:with\", \"busy with\", \"{}\", \"default\" FROM row","params":[]}'
)"
test "${with_write_status}" = "503"
with_write_error="$(cat "${with_write_body}")"
rm -f "${with_write_body}"
assert_json "${with_write_error}" "value.ok === false && value.error.code === 'sql_session_busy'"
lock_session_close="$(post_json /api/mindbrain/sql/session/close "{\"session_id\":${lock_session_id},\"commit\":false}")"
assert_json "${lock_session_close}" "value.ok === true && value.session_id === ${lock_session_id} && value.committed === false"
inactive_write_status="$(get_json /api/mindbrain/sql/write-status)"
assert_json "${inactive_write_status}" "value.ok === true && value.active_session_id === null"
curl -fsS "${BASE_URL}/health" >/dev/null

constraint_error="$(
  curl -sS -H "Content-Type: application/json" -X POST "${BASE_URL}/api/mindbrain/sql" \
    --data '{"sql":"INSERT INTO facets (id, schema_id, content, facets, facets_json, workspace_id, doc_id) VALUES (\"dup-doc\", \"ghostcrab.fact\", \"dup\", \"{}\", \"{}\", \"default\", 1)","params":[]}'
)"
assert_json "${constraint_error}" "value.ok === false && value.error.kind === 'StepFailed' && value.error.sqlite_code === 19 && value.error.sqlite_extended_code === 2067 && value.error.sqlite_message.includes('UNIQUE constraint failed')"

session_open="$(post_json /api/mindbrain/sql/session/open '{}')"
session_id="$(json_field "${session_open}" "value.session_id")"
session_error="$(
  curl -sS -H "Content-Type: application/json" -X POST "${BASE_URL}/api/mindbrain/sql/session/query" \
    --data "{\"session_id\":${session_id},\"sql\":\"INSERT INTO facets (id, schema_id, content, facets, facets_json, workspace_id, doc_id) VALUES ('dup-in-session', 'ghostcrab.fact', 'dup', '{}', '{}', 'default', 1)\",\"params\":[]}"
)"
assert_json "${session_error}" "value.ok === false && value.error.kind === 'StepFailed'"
failed_status="$(get_json /api/mindbrain/sql/write-status)"
assert_json "${failed_status}" "value.ok === true && value.last_error && value.last_error.kind === 'StepFailed' && typeof value.last_error.message === 'string'"
session_close="$(post_json /api/mindbrain/sql/session/close "{\"session_id\":${session_id},\"commit\":false}")"
assert_json "${session_close}" "value.ok === true && value.session_id === ${session_id} && value.committed === false"

post_rollback_query="$(post_json /api/mindbrain/sql '{"sql":"SELECT COUNT(*) AS count FROM facets","params":[]}')"
assert_json "${post_rollback_query}" "value.ok === true && value.rows[0][0] >= 5"

legacy_insert="$(post_json /api/mindbrain/sql '{"sql":"INSERT INTO facets (schema_id, content, facets, workspace_id) VALUES (\"legacy:http\", \"legacy raw\", \"{}\", \"default\")","params":[]}')"
assert_json "${legacy_insert}" "value.ok === true"

schema_query="$(post_json /api/mindbrain/sql '{"sql":"SELECT COUNT(*) FROM facets WHERE schema_id = \"legacy:http\" AND doc_id IS NOT NULL","params":[]}')"
assert_json "${schema_query}" "value.ok === true && value.rows[0][0] === 1"

index_query="$(post_json /api/mindbrain/sql '{"sql":"SELECT name FROM pragma_index_list(\"facets\") WHERE name IN (\"facets_source_ref_workspace_uniq\", \"idx_facets_source_ref_workspace\") ORDER BY name","params":[]}')"
assert_json "${index_query}" "value.ok === true && value.rows.length === 2 && value.rows[0][0] === 'facets_source_ref_workspace_uniq' && value.rows[1][0] === 'idx_facets_source_ref_workspace'"

sync_query="$(post_json /api/mindbrain/sql '{"sql":"SELECT COUNT(*) FROM facets WHERE facets != facets_json OR doc_id IS NULL","params":[]}')"
assert_json "${sync_query}" "value.ok === true && value.rows[0][0] === 0"

echo "standalone HTTP contract passed on ${BASE_URL}"
