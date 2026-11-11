#!/usr/bin/env bash
set -euo pipefail

source_commit=359ad92e3e6ba46b59d99ce51417ac35730a9abb
source_repository=https://github.com/ai-pipestream/protobuf4j.git

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# The checksum file records its path relative to core/, so every path here
# hangs off that directory rather than the repository root.
core_dir=$(cd -- "${script_dir}/../.." && pwd)
destination="${core_dir}/codegen/src/main/resources/ai/protomolt/proto/codegen/protoc-wrapper-v4.wasm"
work_dir=$(mktemp -d)
trap 'rm -rf "${work_dir}"' EXIT

git clone --filter=blob:none "${source_repository}" "${work_dir}/protobuf4j"
git -C "${work_dir}/protobuf4j" checkout --detach "${source_commit}"
test "$(git -C "${work_dir}/protobuf4j" rev-parse HEAD)" = "${source_commit}"

# Pin every remaining floating input of the pinned upstream recipe before
# anything executes: the linux/amd64 Docker base image by digest, apt to a
# fixed Ubuntu snapshot, the tool archives by SHA-256 (verified before
# extraction), and the git-sourced inputs (grpc-java, protobuf, Abseil) to
# exact commits. See README.md for the pin table.
# The patch is reviewed against ${source_commit}; if it stops applying, the
# source moved and the pins must be re-derived, not worked around.
git -C "${work_dir}/protobuf4j" apply "${script_dir}/pin-generator-inputs.patch"

make -C "${work_dir}/protobuf4j" build-v4
cp "${work_dir}/protobuf4j/wasm/protoc-wrapper-v4.wasm" "${destination}"

cd "${core_dir}"
sha256sum --check codegen/provenance/protoc-wrapper-v4.sha256
