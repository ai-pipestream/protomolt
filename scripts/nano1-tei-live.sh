#!/usr/bin/env bash
# Builds and exercises Nano1's CUDA-only text embedding service.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ "${PROTOMOLT_NANO1_TEI_LIVE:-}" != 1 ]; then
  printf 'SKIP: set PROTOMOLT_NANO1_TEI_LIVE=1 on Nano1 to run the live TEI test\n'
  exit 0
fi

deploy/nano1/check-host.sh
scripts/nano1-build-tei.sh
docker compose -f deploy/nano1/compose.yml up -d --no-build --wait tei-gpu

port="${NANO1_TEI_PORT:-8083}"
host="${NANO1_TEI_BIND:-127.0.0.1}"
endpoint="${host}:${port}"
grpcurl_image="fullstorydev/grpcurl@sha256:3baecd2e73cd4c7e9c01e75af8f08d14c0c13a5767dc86db4eeffc24fae593d6"

grpcurl() {
  docker run --rm --network host "$grpcurl_image" "$@"
}

grpcurl -plaintext "$endpoint" list | grep -qx 'tei.v1.Embed'
health_dir="$(mktemp -d)"
trap 'rm -rf "$health_dir"' EXIT
printf '%s\n' \
  'syntax = "proto3";' \
  'package grpc.health.v1;' \
  'service Health {' \
  '  rpc Check(HealthCheckRequest) returns (HealthCheckResponse);' \
  '}' \
  'message HealthCheckRequest { string service = 1; }' \
  'message HealthCheckResponse {' \
  '  enum ServingStatus {' \
  '    UNKNOWN = 0;' \
  '    SERVING = 1;' \
  '    NOT_SERVING = 2;' \
  '    SERVICE_UNKNOWN = 3;' \
  '  }' \
  '  ServingStatus status = 1;' \
  '}' > "$health_dir/health.proto"
chmod 0755 "$health_dir"
chmod 0644 "$health_dir/health.proto"
docker run --rm --network host \
  --mount "type=bind,src=$health_dir,dst=/proto,readonly" \
  "$grpcurl_image" -plaintext -import-path /proto -proto health.proto \
  -d '{"service":"tei.v1.Embed"}' \
  "$endpoint" grpc.health.v1.Health/Check \
  | grep -q 'SERVING'
info="$(grpcurl -plaintext -d '{}' "$endpoint" tei.v1.Info/Info)"
first="$(grpcurl -plaintext -d \
  '{"inputs":"gRPC routes typed requests between services"}' \
  "$endpoint" tei.v1.Embed/Embed)"
second="$(grpcurl -plaintext -d \
  '{"inputs":"service meshes route typed network requests"}' \
  "$endpoint" tei.v1.Embed/Embed)"
third="$(grpcurl -plaintext -d \
  '{"inputs":"fresh bread tastes wonderful"}' \
  "$endpoint" tei.v1.Embed/Embed)"

python3 - "$info" "$first" "$second" "$third" <<'PYEOF'
import json
import math
import sys

info = json.loads(sys.argv[1])
responses = [json.loads(value) for value in sys.argv[2:]]
vectors = [response["embeddings"] for response in responses]

if info.get("modelId") != "BAAI/bge-small-en-v1.5":
    raise SystemExit(f"unexpected model: {info.get('modelId')}")
if info.get("modelSha") != "5c38ec7c405ec4b44b94cc5a9bb96e735b38267a":
    raise SystemExit(f"unexpected model revision: {info.get('modelSha')}")
if len(vectors) != 3 or any(len(vector) != 384 for vector in vectors):
    raise SystemExit("expected three 384-dimensional vectors")
if any(not math.isfinite(value) for vector in vectors for value in vector):
    raise SystemExit("embedding contains a non-finite value")

norms = [math.sqrt(sum(value * value for value in vector)) for vector in vectors]
if any(abs(norm - 1.0) > 0.01 for norm in norms):
    raise SystemExit(f"embeddings are not normalized: {norms}")

related = sum(a * b for a, b in zip(vectors[0], vectors[1]))
unrelated = sum(a * b for a, b in zip(vectors[0], vectors[2]))
if related <= unrelated:
    raise SystemExit(
        f"semantic ordering failed: related={related}, unrelated={unrelated}"
    )

print(json.dumps({
    "model": info["modelId"],
    "model_sha": info["modelSha"],
    "dimensions": len(vectors[0]),
    "related_similarity": round(related, 6),
    "unrelated_similarity": round(unrelated, 6),
}, indent=2))
PYEOF

docker logs protomolt-tei-jetson 2>&1 | grep -Eiq 'cuda|candle.*gpu' || {
  printf 'TEI logs do not contain CUDA device evidence\n' >&2
  exit 1
}

printf 'Nano1 TEI CUDA embeddings: OK\n'
