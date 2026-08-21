#!/usr/bin/env bash
# Static checks for the Nano1 TEI GPU deployment. Starts no container.
set -euo pipefail

cd "$(dirname "$0")"

say()  { printf '== %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

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

say "strict GPU gate with no CPU fallback"
printf '%s' "$compose" | grep -q 'runtime: nvidia' \
  || fail "Compose must select the NVIDIA runtime"
if grep -Eq "torch\\.device\\([\"']cpu|cpu_offload|cpu-offload" compose.yml; then
  fail "CPU fallback or offload is prohibited"
fi

say "bounded local services"
printf '%s' "$compose" | grep -q 'NANO1_TEI_BIND:-127.0.0.1' \
  || fail "TEI must bind to loopback by default"
grep -q 'mem_limit: 3g' compose.yml \
  || fail "TEI memory must be bounded"
printf '%s' "$compose" | grep -q 'no-new-privileges:true' \
  || fail "inference services must prevent privilege escalation"

say "health-gated mesh publisher"
PYTHONPYCACHEPREFIX="${TMPDIR:-/tmp}/protomolt-pycache" \
  python3 -m py_compile ../../scripts/nano1-mesh-publisher.py \
  || fail "mesh publisher must compile"
PYTHONDONTWRITEBYTECODE=1 python3 ../../scripts/test_nano1_mesh_publisher.py \
  || fail "mesh publisher contract test must pass"
grep -q 'probe_tei' ../../scripts/nano1-mesh-publisher.py \
  || fail "mesh publisher must gate the TEI lease"
grep -q 'probe_host' ../../scripts/nano1-mesh-publisher.py \
  || fail "mesh publisher must gate ARM64 build capacity"
grep -q '^User=protomolt-runner$' protomolt-mesh-publisher.service \
  || fail "mesh publisher service must use the dedicated runner account"
grep -q '^EnvironmentFile=/etc/protomolt/nano1-mesh.env$' \
  protomolt-mesh-publisher.service \
  || fail "mesh publisher service must load its token outside the repository"
grep -q '^ProtectSystem=strict$' protomolt-mesh-publisher.service \
  || fail "mesh publisher service must protect the host filesystem"
grep -q '^StateDirectory=protomolt-runner/mesh$' \
  protomolt-mesh-publisher.service \
  || fail "mesh publisher service must preserve fenced sequence state"

say "PASS: Nano1 GPU deployment statics"
