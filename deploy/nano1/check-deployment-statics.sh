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

say "pinned ARM64 TEI and embedding model"
compose="$(cat compose.yml)"
printf '%s' "$compose" | grep -q \
  "PROTOMOLT_TEI_SOURCE_DIR:-/var/lib/protomolt-runner/.cache/protomolt/tei-source" \
  || fail "TEI must build from the prepared pinned source checkout"
printf '%s' "$compose" | grep -q 'CUDA_COMPUTE_CAP: "87"' \
  || fail "TEI must compile for the Orin compute capability"
printf '%s' "$compose" | grep -q 'target: grpc' \
  || fail "TEI must expose its reflected gRPC contract"
grep -q 'candle-cuda-volta' tei-no-flash.patch \
  || fail "the TEI build must omit unused Flash Attention kernels"
grep -q 'compute_cap_matching(87, 87)' tei-no-flash.patch \
  || fail "the TEI build must accept the Orin's exact compute capability"
printf '%s' "$compose" | grep -q '5c38ec7c405ec4b44b94cc5a9bb96e735b38267a' \
  || fail "the embedding model revision must be pinned"

say "bounded local services"
compose="$(cat compose.yml)"
printf '%s' "$compose" | grep -q 'runtime: nvidia' \
  || fail "Compose must select the NVIDIA runtime"
printf '%s' "$compose" | grep -q 'NANO1_DJL_BIND:-127.0.0.1' \
  || fail "DJL must bind to loopback by default"
printf '%s' "$compose" | grep -q 'NANO1_TEI_BIND:-127.0.0.1' \
  || fail "TEI must bind to loopback by default"
test "$(grep -c 'runtime: nvidia' compose.yml)" = 2 \
  || fail "both inference services must select the NVIDIA runtime"
grep -q 'mem_limit: 2g' compose.yml \
  || fail "DJL memory must be bounded"
grep -q 'mem_limit: 3g' compose.yml \
  || fail "TEI memory must be bounded"
printf '%s' "$compose" | grep -q 'no-new-privileges:true' \
  || fail "inference services must prevent privilege escalation"

say "health-gated mesh publisher"
python3 -m py_compile ../../scripts/nano1-mesh-publisher.py \
  || fail "mesh publisher must compile"
grep -q 'probe_tei' ../../scripts/nano1-mesh-publisher.py \
  || fail "mesh publisher must gate the TEI lease"
grep -q 'probe_djl' ../../scripts/nano1-mesh-publisher.py \
  || fail "mesh publisher must gate the DJL lease"
grep -q 'probe_host' ../../scripts/nano1-mesh-publisher.py \
  || fail "mesh publisher must gate ARM64 build capacity"

say "PASS: Nano1 GPU deployment statics"
