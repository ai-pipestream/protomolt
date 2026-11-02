#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 1 ]]; then
  echo "Usage: $0 <baseline-git-ref>" >&2
  exit 2
fi
cd "$(dirname "$0")/.."
compat_tmp=$(mktemp -d)
trap 'rm -rf "$compat_tmp"' EXIT
baseline_ref=$(git rev-parse --verify "$1^{commit}")

# Compare the complete descriptor inventory, independent of Buf module ownership.
# Moving a file between Gradle/Buf modules must not masquerade as wire deletion.
# Keep imports and all files: real deletions must still fail FILE compatibility.
buf build --as-file-descriptor-set --output "$compat_tmp/current.binpb"
buf build ".git#ref=$baseline_ref" --as-file-descriptor-set --output "$compat_tmp/baseline.binpb"
if buf breaking "$compat_tmp/current.binpb" --against "$compat_tmp/baseline.binpb" \
    --config buf.yaml --error-format=json >"$compat_tmp/diagnostics.jsonl"; then
  exit 0
else
  breaking_status=$?
fi
cat "$compat_tmp/diagnostics.jsonl"
# Only Buf's breaking-change exit code can enter the approved migration check.
# Build errors, unavailable tools and other failures retain their exit status.
if [[ $breaking_status != 100 ]]; then
  exit "$breaking_status"
fi
schema=repo/proto/src/main/proto/ai/protomolt/proto/repo/v2/document_service.proto
git show "$baseline_ref:$schema" >"$compat_tmp/baseline.proto"
if python3 scripts/check-approved-proto-migration.py \
    "$compat_tmp/baseline.proto" "$schema" "$compat_tmp/diagnostics.jsonl"; then
  exit 0
fi
exit "$breaking_status"
