#!/usr/bin/env bash
set -euo pipefail

source_commit=359ad92e3e6ba46b59d99ce51417ac35730a9abb
source_repository=https://github.com/ai-pipestream/protobuf4j.git

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "${script_dir}/../.." && pwd)
destination="${repo_root}/codegen/src/main/resources/ai/pipestream/proto/codegen/protoc-wrapper-v4.wasm"
work_dir=$(mktemp -d)
trap 'rm -rf "${work_dir}"' EXIT

git clone --filter=blob:none "${source_repository}" "${work_dir}/protobuf4j"
git -C "${work_dir}/protobuf4j" checkout --detach "${source_commit}"
make -C "${work_dir}/protobuf4j" build-v4
cp "${work_dir}/protobuf4j/wasm/protoc-wrapper-v4.wasm" "${destination}"

cd "${repo_root}"
sha256sum --check core/codegen/provenance/protoc-wrapper-v4.sha256
