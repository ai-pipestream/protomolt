#!/usr/bin/env bash
# Prepares the pinned TEI source and builds its CUDA-only gRPC image on Nano1.
set -euo pipefail

cd "$(dirname "$0")/.."

source_ref="0d124dc9773be6ac5a9a57d8439aba9bbbf33273"
source_dir="${PROTOMOLT_TEI_SOURCE_DIR:-$HOME/.cache/protomolt/tei-source}"
patch_file="$PWD/deploy/nano1/tei-no-flash.patch"

if [ ! -d "$source_dir/.git" ]; then
  install -d "$(dirname "$source_dir")"
  git clone --filter=blob:none \
    https://github.com/huggingface/text-embeddings-inference.git "$source_dir"
fi

if ! git -C "$source_dir" diff --quiet; then
  changed_files="$(git -C "$source_dir" diff --name-only)"
  if [ "$changed_files" != $'Dockerfile-cuda\nbackends/candle/src/compute_cap.rs' ]; then
    printf 'TEI source has unexpected local changes: %s\n' "$source_dir" >&2
    printf '%s\n' "$changed_files" >&2
    exit 1
  fi
  git -C "$source_dir" apply --unidiff-zero --reverse --check \
    "$patch_file" 2>/dev/null || {
    printf 'TEI source has an unrecognized local change\n' >&2
    exit 1
  }
fi

current_ref="$(git -C "$source_dir" rev-parse HEAD 2>/dev/null || true)"
if [ "$current_ref" != "$source_ref" ]; then
  git -C "$source_dir" diff --quiet || {
    printf 'TEI source must be clean before changing revisions\n' >&2
    exit 1
  }
  git -C "$source_dir" fetch --depth 1 origin "$source_ref"
  git -C "$source_dir" switch --detach "$source_ref"
fi

if git -C "$source_dir" apply --unidiff-zero --check "$patch_file" 2>/dev/null; then
  git -C "$source_dir" apply --unidiff-zero "$patch_file"
elif ! git -C "$source_dir" apply --unidiff-zero --reverse --check \
  "$patch_file" 2>/dev/null; then
  printf 'TEI no-Flash patch does not match the pinned source\n' >&2
  exit 1
fi

test "$(git -C "$source_dir" rev-parse HEAD)" = "$source_ref"

docker build \
  --progress=plain \
  --platform linux/arm64 \
  --target grpc \
  -f "$source_dir/Dockerfile-cuda" \
  --build-arg CUDA_COMPUTE_CAP=87 \
  --build-arg CARGO_BUILD_JOBS=1 \
  --build-arg CARGO_BUILD_INCREMENTAL=false \
  --build-arg DOCKER_LABEL=protomolt-nano1-sm87 \
  --build-arg GIT_SHA="$source_ref" \
  --build-arg RAYON_NUM_THREADS=1 \
  -t protomolt-tei-jetson-grpc:0d124dc \
  "$source_dir"
