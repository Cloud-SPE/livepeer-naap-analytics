#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-.}"
cd "$ROOT"

printf '| Lines | Path | First Heading |\n' 2>/dev/null || exit 0
printf '|---:|---|---|\n' 2>/dev/null || exit 0

while IFS= read -r file; do
  lines="$(wc -l < "$file" | tr -d ' ')"
  heading="$(rg -m1 '^#{1,6} ' "$file" | sed -E 's/^#{1,6} //' || true)"
  if [[ -z "${heading}" ]]; then
    heading="(none)"
  fi
  heading="${heading//|/\\|}"
  printf '| %s | `%s` | %s |\n' "$lines" "$file" "$heading" 2>/dev/null || exit 0
done < <(rg --files --hidden -g '*.md' -g '!.git/**' | sort)
