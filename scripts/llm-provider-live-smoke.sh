#!/usr/bin/env bash
set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOL="${ROOT_DIR}/zig-out/bin/mindbrain-standalone-tool"
SMOKE_RESULT="unknown"

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "${value}"
}

load_dotenv() {
  local path="$1"
  [[ -f "${path}" ]] || return 0

  declare -A from_dotenv=()
  local line key value
  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line%$'\r'}"
    line="$(trim "${line}")"
    [[ -z "${line}" || "${line}" == \#* || "${line}" != *=* ]] && continue

    key="$(trim "${line%%=*}")"
    value="$(trim "${line#*=}")"
    if [[ "${value}" == \"*\" && "${value}" == *\" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "${value}" == \'* && "${value}" == *\' ]]; then
      value="${value:1:${#value}-2}"
    fi

    case "${key}" in
      MB_DOCUMENTS_LLM_PROVIDER|MB_DOCUMENTS_LLM_BASE_URL|MB_DOCUMENTS_LLM_MODEL|MB_DOCUMENTS_LLM_API_KEY|OPENAI_API_KEY|OPENROUTER_BASE_URL|OPENROUTER_API_KEY|OPENROUTER_CHAT_MODEL|OPENROUTER_CHAT_FALLBACK_MODEL|OPENROUTER_CHAT_SMOKE_TEST|ANTHROPIC_BASE_URL|ANTHROPIC_API_KEY|ANTHROPIC_MODEL|ANTHROPIC_VERSION)
        if [[ -z "${!key+x}" || "${from_dotenv[$key]:-0}" == "1" ]]; then
          export "${key}=${value}"
          from_dotenv[$key]=1
        fi
        ;;
    esac
  done < "${path}"
}

print_failure_summary() {
  local stderr_path="$1"
  if grep -q 'LLM_HTTP_FAILURE_JSON=' "${stderr_path}"; then
    grep 'LLM_HTTP_FAILURE_JSON=' "${stderr_path}" | head -n 1 | sed -E 's/(sk-[A-Za-z0-9_-]+)/[redacted]/g' >&2
    return 0
  fi
  if grep -q 'LLM HTTP request failed:' "${stderr_path}"; then
    grep 'LLM HTTP request failed:' "${stderr_path}" | head -n 1 | sed -E 's/(sk-[A-Za-z0-9_-]+)/[redacted]/g' >&2
    return 0
  fi
  sed -E 's/(sk-[A-Za-z0-9_-]+)/[redacted]/g' "${stderr_path}" >&2
}

if [[ -f "${ROOT_DIR}/.env" ]]; then
  load_dotenv "${ROOT_DIR}/.env"
fi

if [[ "${MINDBRAIN_LIVE_LLM_TESTS:-0}" != "1" ]]; then
  echo "skip: set MINDBRAIN_LIVE_LLM_TESTS=1 to run live provider smokes"
  exit 0
fi

if [[ ! -x "${TOOL}" ]]; then
  echo "error: build ${TOOL} first with: /usr/local/bin/zig-0.16 build standalone-tool" >&2
  exit 2
fi

run_profile_smoke() {
  local provider="$1"
  local model="$2"
  local key_var="$3"
  local stderr_path="/tmp/mindbrain-llm-${provider}-smoke.stderr"
  SMOKE_RESULT="unknown"

  if [[ -z "${model}" ]]; then
    echo "skip ${provider}: model env var is empty"
    SMOKE_RESULT="skip"
    return 0
  fi
  if [[ -z "${!key_var:-}" ]]; then
    echo "skip ${provider}: ${key_var} is not set"
    SMOKE_RESULT="skip"
    return 0
  fi

  echo "smoke ${provider}: ${model}"
  if ! "${TOOL}" document-profile \
    --content "Article 1. Operators must document access control decisions." \
    --source-ref "live-smoke.txt" \
    --llm-provider "${provider}" \
    --model "${model}" \
    --sample-chars 800 \
    --max-tokens 700 >/tmp/mindbrain-llm-${provider}-smoke.json 2>"${stderr_path}"; then
    if grep -Eiq 'rate.?limit|quota|billing|credit balance|temporarily|429' "${stderr_path}"; then
      echo "skip ${provider}: provider availability/quota condition for ${model}"
      print_failure_summary "${stderr_path}"
      SMOKE_RESULT="skip"
      return 0
    fi
    if grep -Eiq 'InvalidChunkBudget|InvalidConfidence|MissingStructureMarkers|InvalidEnumTag|UnexpectedToken|SyntaxError|InvalidCharacter' "${stderr_path}"; then
      echo "skip ${provider}: model returned an invalid document profile for ${model}"
      print_failure_summary "${stderr_path}"
      SMOKE_RESULT="skip"
      return 0
    fi
    print_failure_summary "${stderr_path}"
    echo "fail ${provider}: live request failed for ${model}" >&2
    SMOKE_RESULT="fail"
    return 1
  fi
  rm -f "${stderr_path}"
  echo "pass ${provider}: ${model}"
  SMOKE_RESULT="success"
}

run_openrouter_smoke() {
  local seen=" "
  local models=()
  local saw_skip=0
  [[ -n "${OPENROUTER_CHAT_SMOKE_TEST:-}" ]] && models+=("${OPENROUTER_CHAT_SMOKE_TEST}")
  [[ -n "${OPENROUTER_CHAT_MODEL:-}" ]] && models+=("${OPENROUTER_CHAT_MODEL}")
  [[ -n "${OPENROUTER_CHAT_FALLBACK_MODEL:-}" ]] && models+=("${OPENROUTER_CHAT_FALLBACK_MODEL}")

  if [[ "${#models[@]}" -eq 0 ]]; then
    echo "skip openrouter: model env var is empty"
    return 0
  fi

  local model
  for model in "${models[@]}"; do
    if [[ "${seen}" == *" ${model} "* ]]; then
      continue
    fi
    seen+="${model} "
    if run_profile_smoke openrouter "${model}" OPENROUTER_API_KEY; then
      if [[ "${SMOKE_RESULT}" == "success" ]]; then
        return 0
      fi
      saw_skip=1
      continue
    fi
    return 1
  done
  [[ "${saw_skip}" == "1" ]]
}

status=0
run_openrouter_smoke || status=1
run_profile_smoke anthropic "${ANTHROPIC_MODEL:-}" ANTHROPIC_API_KEY || status=1

if [[ "${status}" == "0" ]]; then
  echo "live provider smoke complete"
else
  echo "live provider smoke failed" >&2
fi
exit "${status}"
