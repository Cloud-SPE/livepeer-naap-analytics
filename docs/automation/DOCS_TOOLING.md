# Docs Tooling and Automation

Script reference for all repository scripts: `docs/automation/SCRIPTS_REFERENCE.md`

## Included Scripts

- Inventory all markdown files with line counts and headings:
  - `scripts/docs_inventory.sh`
- Validate local markdown links:
  - `scripts/docs_link_check.sh`

## Recommended CI Additions

1. Markdown linting:
   - `markdownlint-cli2 "**/*.md"`
2. Link checking:
   - `lychee --offline --no-progress "**/*.md"`
3. Docs freshness check:
   - fail CI if `docs/references/DOC_INVENTORY_AND_MIGRATION_MAP.md` is missing entries from `rg --files -g '*.md'`.

## Suggested Agent Tasks

- Weekly doc-gardening run:
  - identify stale references,
  - propose canonical/legacy status updates,
  - open small follow-up PRs.
- Contract drift run:
  - compare schema/mappers/tests/docs touched in PR,
  - fail if required companion updates are missing.
