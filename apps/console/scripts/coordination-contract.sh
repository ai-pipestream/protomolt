#!/usr/bin/env bash
# Regenerate the browser sample from its canonical protobuf, or check for drift.
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)
cd "$repo_dir"
proto=samples/src/main/proto/ai/protomolt/proto/samples/starter/v1/coordination_report.proto
assets=apps/console/public/contracts
work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT
buf build --as-file-descriptor-set --exclude-source-info --path "$proto" -o "$work_dir/coordination-report.binpb"
if [[ "${1:-}" == --check ]]; then
  cmp "$work_dir/coordination-report.binpb" "$assets/coordination-report.binpb"
  cmp "$proto" "$assets/coordination_report.proto"
else
  mkdir -p "$assets"
  cp "$work_dir/coordination-report.binpb" "$assets/coordination-report.binpb"
  cp "$proto" "$assets/coordination_report.proto"
fi
