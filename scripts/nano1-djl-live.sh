#!/usr/bin/env bash
# Builds and exercises the Nano1 GPU-only DJL service. This changes the local
# Nano1 Compose deployment and therefore requires an explicit opt-in.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ "${PROTOMOLT_NANO1_DJL_LIVE:-}" != 1 ]; then
  printf 'SKIP: set PROTOMOLT_NANO1_DJL_LIVE=1 on Nano1 to run the live GPU test\n'
  exit 0
fi

deploy/nano1/check-host.sh
docker compose -f deploy/nano1/compose.yml up -d --build --wait djl-gpu

port="${NANO1_DJL_PORT:-8082}"
response="$(curl --fail --silent \
  -H 'content-type: application/json' \
  -d '{"values":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]}' \
  "http://127.0.0.1:${port}/predictions/cuda-probe")"

jq -e '
  .backend == "TensorRT" and
  (.device | length > 0) and
  .compute_capability == [8, 7] and
  .input == [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15] and
  .output == [2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17] and
  .total_device_bytes > 0
' <<<"$response" >/dev/null

printf '%s\n' "$response" | jq .

rejects_invalid_request() {
  local payload="$1"
  local response_file
  local status
  response_file="$(mktemp)"
  status="$(curl --silent --output "$response_file" --write-out '%{http_code}' \
    -H 'content-type: application/json' \
    -d "$payload" \
    "http://127.0.0.1:${port}/predictions/cuda-probe")"
  if [ "$status" != 400 ] || ! jq -e \
    '.code == 400 and (.error | length > 0)' "$response_file" >/dev/null; then
    printf 'expected HTTP 400 for invalid request, received %s:\n' "$status" >&2
    cat "$response_file" >&2
    rm -f "$response_file"
    return 1
  fi
  rm -f "$response_file"
}

rejects_invalid_request '{"values":[1,2]}'
rejects_invalid_request \
  '{"values":[true,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]}'
rejects_invalid_request \
  '{"values":["x",1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]}'

printf 'Nano1 DJL CUDA probe: OK\n'
