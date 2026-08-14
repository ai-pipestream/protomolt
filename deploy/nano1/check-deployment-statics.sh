#!/usr/bin/env bash
# Static checks for the Nano1 DJL GPU deployment. Starts no container.
set -euo pipefail

cd "$(dirname "$0")"

say()  { printf '== %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

say "pinned ARM64 CUDA and DJL inputs"
dockerfile="$(cat Dockerfile.djl-gpu)"
printf '%s' "$dockerfile" | grep -q 'djl-serving:0.36.0-aarch64@sha256:' \
  || fail "DJL Serving source image must be digest-pinned"
printf '%s' "$dockerfile" | grep -q 'nvcr.io/nvidia/tensorrt:26.05-py3@sha256:' \
  || fail "NVIDIA TensorRT base must be digest-pinned to the CUDA 13.2 line"
printf '%s' "$dockerfile" | grep -q 'ai.djl.default_engine=Python' \
  || fail "DJL must use its Python engine with NVIDIA TensorRT"

say "strict GPU gate with no CPU fallback"
grep -q 'TensorRtProbe' start-djl.sh \
  || fail "startup must execute the TensorRT probe"
grep -q 'CPU inference is prohibited' models/cuda-probe/tensorrt_probe.py \
  || fail "startup must fail instead of using CPU"
grep -q 'cudaGetDeviceProperties' models/cuda-probe/tensorrt_probe.py \
  || fail "the model must require a CUDA device"
if grep -Eq "torch\\.device\\([\"']cpu|cpu_offload|cpu-offload" \
    start-djl.sh models/cuda-probe/*.py compose.yml; then
  fail "CPU fallback or offload is prohibited"
fi

say "bounded local service"
compose="$(cat compose.yml)"
printf '%s' "$compose" | grep -q 'runtime: nvidia' \
  || fail "Compose must select the NVIDIA runtime"
printf '%s' "$compose" | grep -q 'NANO1_DJL_BIND:-127.0.0.1' \
  || fail "DJL must bind to loopback by default"
printf '%s' "$compose" | grep -q 'mem_limit: 6g' \
  || fail "DJL memory must be bounded"
printf '%s' "$compose" | grep -q 'no-new-privileges:true' \
  || fail "DJL must prevent privilege escalation"

say "PASS: Nano1 DJL deployment statics"
