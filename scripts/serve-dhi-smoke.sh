#!/usr/bin/env bash
# Local smoke for the hardened public demo image (apps/serve/Dockerfile.dhi).
#
#   1. builds :protomolt-serve:installDist (unless --skip-dist)
#   2. docker-builds Dockerfile.dhi (unless --skip-build)
#   3. runs --demo, waits on GET /health from the host (no image HEALTHCHECK)
#   4. calls RenderJsonSchema on the seeded demo.shop.v1.Order
#   5. tears the container down
#
# --demo needs a writable /tmp (HOME and java.io.tmpdir in this image). This
# script does not pass --read-only.
#
#   ./scripts/serve-dhi-smoke.sh
#   ./scripts/serve-dhi-smoke.sh --skip-dist          # dist already built
#   ./scripts/serve-dhi-smoke.sh --image NAME --skip-build
set -euo pipefail

cd "$(dirname "$0")/.."

IMAGE="${SERVE_DHI_IMAGE:-protomolt-serve:dhi}"
SKIP_DIST=0
SKIP_BUILD=0
HTTP_PORT="${PROTOMOLT_HTTP_PORT:-38080}"
GRPC_PORT="${PROTOMOLT_GRPC_PORT:-39090}"
REGISTRY_PORT="${PROTOMOLT_REGISTRY_PORT:-38081}"
HTTP="http://127.0.0.1:${HTTP_PORT}"
NAME="protomolt-serve-dhi-smoke-$$"

while [ $# -gt 0 ]; do
  case "$1" in
    --image) IMAGE="$2"; shift 2 ;;
    --skip-dist) SKIP_DIST=1; shift ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    -h|--help)
      sed -n '2,18p' "$0"
      exit 0
      ;;
    *)
      printf 'unknown argument: %s\n' "$1" >&2
      exit 2
      ;;
  esac
done

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

if ! command -v docker >/dev/null; then
  fail "docker is required (build and run Dockerfile.dhi)"
fi

cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [ "$SKIP_DIST" -eq 0 ]; then
  say "Building the serve distribution"
  ./gradlew :protomolt-serve:installDist --console=plain -q
fi

if [ ! -d apps/serve/build/install/protomolt-serve/lib ]; then
  fail "missing apps/serve/build/install/protomolt-serve (run installDist or drop --skip-dist)"
fi

if [ "$SKIP_BUILD" -eq 0 ]; then
  say "Building the DHI image ($IMAGE)"
  # --load is required when the default builder is buildx (CI after
  # docker/setup-buildx-action); otherwise docker run cannot see the image.
  # Older docker-without-buildx rejects the flag, so fall back.
  build_args=(-f apps/serve/Dockerfile.dhi -t "$IMAGE" apps/serve)
  if docker build --help 2>&1 | grep -q -- '--load'; then
    build_args=(--load "${build_args[@]}")
  fi
  if ! docker build "${build_args[@]}"; then
    fail "docker build failed. If the error is unauthorized for dhi.io, run: docker login dhi.io"
  fi
fi

say "Starting --demo on host ports ${HTTP_PORT}/${GRPC_PORT}/${REGISTRY_PORT}"
docker run -d --name "$NAME" \
  -p "${HTTP_PORT}:8080" \
  -p "${GRPC_PORT}:9090" \
  -p "${REGISTRY_PORT}:8081" \
  "$IMAGE" --demo >/dev/null

say "Waiting for GET /health"
status=""
for _ in $(seq 1 60); do
  if status="$(curl -fsS "${HTTP}/health" 2>/dev/null)" && printf '%s' "$status" | grep -q UP; then
    break
  fi
  status=""
  sleep 2
done
[ -n "$status" ] || fail "serve did not answer GET /health on ${HTTP}"
echo "serve answered on ${HTTP}: $status"

say "REST — RenderJsonSchema on the seeded demo.shop.v1.Order"
curl -fsS -H 'content-type: application/json' \
  -d '{"schema": {"type": "demo.shop.v1.Order"}}' \
  "${HTTP}/grpc-json/ProtoMoltService/RenderJsonSchema" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print("  returned a JSON Schema document,", len(json.dumps(d)), "bytes")' \
  || fail "RenderJsonSchema did not return a document"

say "PASS — hardened --demo image answered /health and RenderJsonSchema"
