#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-.}"
cd "$ROOT"

failed=0

while IFS= read -r file; do
  base_dir="$(dirname "$file")"
  while IFS= read -r link; do
    [[ -z "$link" ]] && continue

    # Ignore web/mail anchors and inline-only references.
    case "$link" in
      http:*|https:*|mailto:*|tel:*|data:*|'#'*)
        continue
        ;;
    esac

    clean="${link#<}"
    clean="${clean%>}"
    clean="${clean%%#*}"
    clean="${clean%%\?*}"
    [[ -z "$clean" ]] && continue

    if [[ "$clean" == /* ]]; then
      target=".${clean}"
    else
      target="${base_dir}/${clean}"
    fi

    if [[ ! -e "$target" ]]; then
      echo "Missing link target in ${file}: ${link} -> ${target}"
      failed=1
    fi
  done < <(perl -ne 'while (/\[[^\]]+\]\(([^)]+)\)/g) { print "$1\n"; }' "$file")
done < <(rg --files --hidden -g '*.md' -g '!.git/**')

if [[ "$failed" -ne 0 ]]; then
  echo "Link check failed."
  exit 1
fi

echo "Link check passed."
