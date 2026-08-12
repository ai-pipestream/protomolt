#!/usr/bin/env bash
# Static checks for the krick-1 deployment package: no containers, no GPU, no
# live calls. Fails when the compose file drifts from the verified single-B70
# baseline: the read-only by-path bind and pinned model mount must be present,
# CPU offload and swap must stay zero, and DFlash must remain opt-in only.
set -euo pipefail

cd "$(dirname "$0")"

say()  { printf '== %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

say "shell syntax and compose yaml"
bash -n check-deployment-statics.sh || fail "bash -n check-deployment-statics.sh"
bash -n ../../scripts/muse-glimmer-live.sh || fail "bash -n muse-glimmer-live.sh"

text="$(cat compose.yml)"

say "gpu mapping: /dev/dri device plus read-only by-path bind"
printf '%s' "$text" | grep -q '/dev/dri:/dev/dri' \
  || fail "missing /dev/dri device mapping"
printf '%s' "$text" | grep -q '/dev/dri/by-path:/dev/dri/by-path:ro' \
  || fail "missing read-only /dev/dri/by-path bind (oneCCL needs it)"

say "pinned local model artifacts, read-only"
printf '%s' "$text" | grep -q 'GLIMMER_MODEL_DIR:-/work/models/muse-glimmer' \
  || fail "model mount must default to the pinned krick-1 artifacts"
printf '%s' "$text" | grep -q ':/models:ro' \
  || fail "missing read-only /models mount"
printf '%s' "$text" | grep -q 'GLIMMER_MODEL_PATH:-/models/Muse-Glimmer-30B' \
  || fail "model path must default to the pinned local artifact"
printf '%s' "$text" | grep -q 'GLIMMER_SERVED_NAME:-muse-glimmer-30b' \
  || fail "served name must default to muse-glimmer-30b"

say "single-B70 baseline: no CPU offload, 4096 context, text-only"
printf '%s' "$text" | grep -q -- '--cpu-offload-gb=0' \
  || fail "cpu-offload-gb must stay zero"
printf '%s' "$text" | grep -q -- '--swap-space=0' \
  || fail "swap-space must stay zero"
printf '%s' "$text" | grep -q 'GLIMMER_MAX_MODEL_LEN:-4096' \
  || fail "context baseline must default to 4096"
printf '%s' "$text" | grep -q -- '--limit-mm-per-prompt=image=0,video=0' \
  || fail "text-only mode must disable both image and video prompts"

say "dflash is opt-in only, never a default"
printf '%s' "$text" | grep -q 'GLIMMER_SPECULATIVE_CONFIG:+--speculative-config' \
  || fail "speculative decoding must stay behind the opt-in variable"
if printf '%s' "$text" | grep -Eq 'GLIMMER_SPECULATIVE_CONFIG:-[^}]'; then
  fail "GLIMMER_SPECULATIVE_CONFIG must default to empty"
fi

say "loopback-only publish and required token"
printf '%s' "$text" | grep -q '127.0.0.1:${GLIMMER_HOST_PORT:-8011}:8011' \
  || fail "sidecar must publish on host loopback only"
if [ "$(printf '%s' "$text" | grep -c 'PROTOMOLT_MCP_TOKEN:?')" -ne 2 ]; then
  fail "both workers must require PROTOMOLT_MCP_TOKEN"
fi

say "PASS: krick-1 deployment statics"
