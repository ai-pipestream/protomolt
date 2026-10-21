#!/usr/bin/env bash
# Regenerate the console's generated TypeScript stubs from their protos, or
# check the committed stubs for drift (--check). Needs `npm ci` first, so the
# pinned protoc-gen-es is in node_modules.
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)
cd "$repo_dir"
buf_version=1.70.0
if [[ "$(buf --version)" != "$buf_version" ]]; then
  echo "Console stub generation requires buf $buf_version (matching CI)." >&2
  exit 1
fi
plugin=apps/console/node_modules/.bin/protoc-gen-es
if [[ ! -x "$plugin" ]]; then
  echo "Missing $plugin: run npm ci in apps/console first." >&2
  exit 1
fi
# The protos the console calls or decodes, one line each. Their imports are
# generated with them (include_imports); the well-known types come from
# @bufbuild/protobuf. Output follows the import path under src/gen, with the
# file's name suffixed _pb.ts.
protos=(
  repo/proto/src/main/proto/ai/protomolt/proto/repo/v2/document_service.proto
  repo/proto/src/main/proto/ai/protomolt/proto/repo/v1/drive.proto
  repo/proto/src/main/proto/ai/protomolt/proto/repo/archive/v1/archive_service.proto
  protobuf/types/src/main/proto/ai/protomolt/proto/authz/v1/console_session.proto
)
stubs=apps/console/src/gen
work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT
template="$work_dir/buf.gen.yaml"
sed "s#out: $stubs#out: $work_dir/gen#" apps/console/buf.gen.yaml > "$template"
path_args=()
for proto in "${protos[@]}"; do
  path_args+=(--path "$proto")
done
buf generate --template "$template" "${path_args[@]}"
if [[ "${1:-}" == --check ]]; then
  if ! diff -r "$work_dir/gen" "$stubs"; then
    echo "Generated stubs in $stubs are stale: run apps/console/scripts/proto-stubs.sh and commit the result." >&2
    exit 1
  fi
else
  rm -rf "$stubs"
  cp -r "$work_dir/gen" "$stubs"
fi
