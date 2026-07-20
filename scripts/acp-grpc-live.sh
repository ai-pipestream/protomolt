#!/usr/bin/env bash
# Live self-hosting proof: through the ACP agent, reflect ProtoMolt's OWN running gRPC service
# and invoke a method on it, over gRPC. The agent runs as a container joined to the server's
# Compose network, so it reaches the server as `serve:9090` — container to container.
set -euo pipefail

cd "$(dirname "$0")/.."

# Host ports default high so publishing does not fight whatever holds 8080 locally; the
# agent-to-server call goes over the Compose network, not these, so they are only for humans.
export PROTOMOLT_HTTP_PORT="${PROTOMOLT_HTTP_PORT:-38080}"
export PROTOMOLT_GRPC_PORT="${PROTOMOLT_GRPC_PORT:-39090}"
export PROTOMOLT_REGISTRY_PORT="${PROTOMOLT_REGISTRY_PORT:-38081}"

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

cleanup() { say "Tearing down"; docker compose down >/dev/null 2>&1 || true; }
trap cleanup EXIT

say "Building images"
./gradlew :protomolt-serve:installDist :protomolt-acp:installDist --console=plain -q
docker compose build serve acp

say "Starting the serve container (gRPC reflection on)"
docker compose up -d serve
for _ in $(seq 1 60); do
  status="$(docker inspect --format '{{.State.Health.Status}}' "$(docker compose ps -q serve)" 2>/dev/null || echo starting)"
  [ "$status" = "healthy" ] && break
  sleep 2
done
[ "${status:-}" = "healthy" ] || fail "serve did not become healthy (last: ${status:-none})"

NET="$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$(docker compose ps -q serve)")"
say "serve is on Compose network ${NET}; the agent will reach it as serve:9090"

say "ACP agent → reflect → grpc-invoke, against our own gRPC"
./gradlew :protomolt-acp:acpGrpcLive --console=plain -q \
  -Pagent="docker run -i --rm --network ${NET} protomolt-acp:local" \
  -Ptarget="serve:9090" \
  -Pmethod="ai.pipestream.protomolt.v1.ProtoMoltService/ListTypes" \
  || fail "the agent could not reflect or invoke our gRPC service"

say "PASS — the ACP agent reflected and invoked ProtoMolt's own gRPC service"
