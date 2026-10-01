#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 1 ]]; then
  echo "Usage: $0 <baseline-git-ref>" >&2
  exit 2
fi
cd "$(dirname "$0")/.."
compat_tmp=$(mktemp -d)
trap 'rm -rf "$compat_tmp"' EXIT

# Compare the complete descriptor inventory, independent of Buf module ownership.
# Moving a file between Gradle/Buf modules must not masquerade as wire deletion.
# Keep imports and all files: real deletions must still fail FILE compatibility.
buf build --as-file-descriptor-set --output "$compat_tmp/current.binpb"
buf build ".git#ref=$1" --as-file-descriptor-set --output "$compat_tmp/baseline.binpb"
buf breaking "$compat_tmp/current.binpb" --against "$compat_tmp/baseline.binpb" --config buf.yaml
