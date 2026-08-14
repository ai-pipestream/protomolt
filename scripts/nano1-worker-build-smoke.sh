#!/usr/bin/env bash
# Builds one coding worker natively on Nano1 and checks the resulting ARM64
# image. The agent-host Gradle distribution must already exist.
set -euo pipefail

cd "$(dirname "$0")/.."

worker="${1:-cpp}"
case "$worker" in
  cpp|java) ;;
  *) printf 'usage: %s {cpp|java}\n' "$0" >&2; exit 2 ;;
esac

tag="protomolt-worker-${worker}:nano1-smoke"
cleanup() {
  docker image rm -f "$tag" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker build \
  --target "$worker" \
  -f apps/agent-host/Dockerfile.workers \
  -t "$tag" \
  apps/agent-host

image_arch="$(docker image inspect --format '{{.Architecture}}' "$tag")"
test "$image_arch" = arm64 || {
  printf 'expected arm64 image, found %s\n' "$image_arch" >&2
  exit 1
}

docker run --rm --entrypoint bash "$tag" -lc '
  set -euo pipefail
  test "$(uname -m)" = aarch64
  protomolt-agent-host --help >/dev/null
  grpcurl --version
  protoc --version
'

case "$worker" in
  cpp)
    docker run --rm --entrypoint bash "$tag" -lc '
      set -euo pipefail
      clang++ --version | head -n 1
      cmake --version | head -n 1
      conan --version
      command -v grpc_cpp_plugin >/dev/null
    '
    ;;
  java)
    docker run --rm --entrypoint bash "$tag" -lc '
      set -euo pipefail
      source "$SDKMAN_DIR/bin/sdkman-init.sh"
      java -version
      gradle --version | grep Gradle
      mvn --version | head -n 1
      native-image --version
      command -v protoc-gen-grpc-java >/dev/null
    '
    ;;
esac

printf 'Nano1 %s worker build: OK\n' "$worker"

