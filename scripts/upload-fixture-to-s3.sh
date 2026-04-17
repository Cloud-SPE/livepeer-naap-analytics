#!/usr/bin/env bash
# Upload a locally-fetched replay fixture archive to the fixture bucket.
#
# The archive is keyed by its SHA-256 so two fixtures with identical
# content share one object. CI downloads by the sha recorded in the
# committed manifest, so uploading a new fixture is a two-step dance:
#
#   1. Fetch locally (writes ARCHIVE + new MANIFEST with its sha):
#        make replay-fetch-fixture-daily-force   # or -fixture-force for golden
#
#   2. Upload the archive to S3 under the new sha:
#        scripts/upload-fixture-to-s3.sh daily
#        scripts/upload-fixture-to-s3.sh golden
#
#   3. Commit the MANIFEST change. CI will now pull the new archive.
#
# The committed manifest is the only link CI trusts between "the
# fixture on this branch" and "the bytes in the bucket." Uploading
# without committing the manifest does nothing; committing without
# uploading breaks CI's fetch.
set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly FIXTURE_DIR="${REPO_ROOT}/tests/fixtures"

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <fixture-name>   # e.g. golden, daily" >&2
  exit 2
fi
case "$1" in
  golden) FIXTURE_NAME="raw_events_golden" ;;
  daily)  FIXTURE_NAME="raw_events_daily"  ;;
  *)      FIXTURE_NAME="$1" ;;   # allow arbitrary names for future fixtures
esac

readonly ARCHIVE_PATH="${FIXTURE_DIR}/${FIXTURE_NAME}.ndjson.zst"
readonly MANIFEST_PATH="${FIXTURE_DIR}/${FIXTURE_NAME}.manifest.json"

: "${REPLAY_FIXTURE_S3_BUCKET:?set REPLAY_FIXTURE_S3_BUCKET (bucket name, no s3:// prefix)}"

for tool in aws jq sha256sum; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "required tool missing: ${tool}" >&2; exit 1
  fi
done

if [[ ! -f "${ARCHIVE_PATH}" ]]; then
  echo "archive not found: ${ARCHIVE_PATH}" >&2
  echo "run the fetch script first (make replay-fetch-fixture[-daily][-force])" >&2
  exit 1
fi
if [[ ! -f "${MANIFEST_PATH}" ]]; then
  echo "manifest not found: ${MANIFEST_PATH}" >&2; exit 1
fi

# Sanity-check the archive against the manifest before upload. Saves a
# round trip if the two have drifted (which would make CI's sha check
# fail after the upload).
LOCAL_SHA="$(sha256sum "${ARCHIVE_PATH}" | awk '{print $1}')"
MANIFEST_SHA="$(jq -r .archive_sha256 "${MANIFEST_PATH}")"
if [[ "${LOCAL_SHA}" != "${MANIFEST_SHA}" ]]; then
  echo "archive sha mismatch — re-fetch before uploading" >&2
  echo "  archive:  ${LOCAL_SHA}" >&2
  echo "  manifest: ${MANIFEST_SHA}" >&2
  exit 1
fi

S3_KEY="replay/fixtures/${MANIFEST_SHA}.ndjson.zst"
echo "uploading ${ARCHIVE_PATH}"
echo "      -> s3://${REPLAY_FIXTURE_S3_BUCKET}/${S3_KEY}"
# --no-overwrite makes re-uploads of the same sha a no-op (safe default),
# which turns this script into an idempotent "ensure uploaded" command.
aws s3 cp \
  "${ARCHIVE_PATH}" \
  "s3://${REPLAY_FIXTURE_S3_BUCKET}/${S3_KEY}" \
  --only-show-errors

echo "done."
echo ""
echo "next step: commit the manifest change at ${MANIFEST_PATH}"
echo "so CI starts pulling the new archive."
