#!/usr/bin/env bash
# Core-logic recalc lint. Fails if any forbidden signature shows up at the
# API-serving boundary, where definitional logic must never live.
#
# See docs/design-docs/api-table-contract.md "Core-logic ownership rule"
# for what qualifies as forbidden vs. allowed at the API layer.
set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly SIGNATURES="${REPO_ROOT}/scripts/core-logic-signatures.txt"
readonly ALLOWLIST="${REPO_ROOT}/scripts/core-logic-allowlist.txt"

# Load the allowlist into an associative array keyed by "path:line" with
# phase-reference values. Lines starting with # or blank are ignored.
declare -A allowed=()
if [[ -f "${ALLOWLIST}" ]]; then
  while IFS=$'\t' read -r location phase; do
    [[ -z "${location}" || "${location}" =~ ^[[:space:]]*# ]] && continue
    allowed[${location}]="${phase}"
  done < "${ALLOWLIST}"
fi

# Paths scanned. Staying tight: serving handlers and SQL-owned api models
# only. Resolver (canonical writer) and dbt canonical/staging layers are
# allowed to hold definitional logic.
readonly PATHS=(
  "${REPO_ROOT}/api/internal/service"
  "${REPO_ROOT}/api/internal/repo"
  "${REPO_ROOT}/warehouse/models/api"
)

if [[ ! -f "${SIGNATURES}" ]]; then
  echo "lint: signatures file missing: ${SIGNATURES}" >&2
  exit 2
fi

unexpected=0
expected=0
while IFS= read -r line; do
  [[ -z "${line}" || "${line}" =~ ^[[:space:]]*# ]] && continue
  pattern="${line}"
  for path in "${PATHS[@]}"; do
    [[ -d "${path}" ]] || continue
    if matches=$(grep -rEn --color=never "${pattern}" "${path}" 2>/dev/null || true); then
      [[ -z "${matches}" ]] && continue
      while IFS= read -r m; do
        # m is "path:line:content"; split off the first two fields
        # to build the allowlist key.
        path_field="${m%%:*}"
        rest_after_path="${m#*:}"
        line_field="${rest_after_path%%:*}"
        rel_path="${path_field#${REPO_ROOT}/}"
        key="${rel_path}:${line_field}"
        if [[ -n "${allowed[${key}]+x}" ]]; then
          echo "ALLOWLISTED (${allowed[${key}]}) ${m#${REPO_ROOT}/}"
          expected=$((expected + 1))
        else
          echo "FORBIDDEN [${pattern}] ${m#${REPO_ROOT}/}"
          unexpected=$((unexpected + 1))
        fi
      done <<< "${matches}"
    fi
  done
done < "${SIGNATURES}"

echo ""
echo "lint: ${expected} allowlisted, ${unexpected} unexpected"
if (( unexpected > 0 )); then
  echo ""
  echo "Move the derivation into the resolver (canonical layer) and read the"
  echo "result from a canonical_* column. See docs/design-docs/api-table-contract.md."
  echo ""
  echo "If this violation is expected (tracked in the plan), add its"
  echo "path:line and phase reference to scripts/core-logic-allowlist.txt"
  echo "— new allowlist entries require plan-phase justification."
  exit 1
fi

echo "lint: core-logic recalc — clean (unexpected=0)"
