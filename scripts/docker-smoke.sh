#!/usr/bin/env bash
# End-to-end proof that ProtoMolt's API answers over MCP and ACP from Docker.
#
#   1. builds the serve and acp distributions and their images
#   2. brings up the serve container and waits for it to report healthy
#   3. calls it over REST and over MCP (a real JSON-RPC initialize + tools/list)
#   4. drives the acp container over stdio and reads back the verb catalog
#   5. tears everything down
#
# Host ports default high so the stack does not fight whatever holds 8080 locally.
set -euo pipefail

cd "$(dirname "$0")/.."

export PROTOMOLT_HTTP_PORT="${PROTOMOLT_HTTP_PORT:-38080}"
export PROTOMOLT_GRPC_PORT="${PROTOMOLT_GRPC_PORT:-39090}"
export PROTOMOLT_REGISTRY_PORT="${PROTOMOLT_REGISTRY_PORT:-38081}"
HTTP="http://127.0.0.1:${PROTOMOLT_HTTP_PORT}"

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

# down (no --remove-orphans) only stops the services this file defines; it never reaches
# containers from another Compose project such as the integration stack.
cleanup() { say "Tearing down"; docker compose down >/dev/null 2>&1 || true; }
trap cleanup EXIT

say "Building distributions"
./gradlew :protomolt-serve:installDist :protomolt-acp:installDist --console=plain -q

say "Building images"
docker compose build serve acp

say "Starting the serve container"
docker compose up -d serve

say "Waiting for health"
for _ in $(seq 1 60); do
  status="$(docker inspect --format '{{.State.Health.Status}}' "$(docker compose ps -q serve)" 2>/dev/null || echo starting)"
  [ "$status" = "healthy" ] && break
  sleep 2
done
[ "${status:-}" = "healthy" ] || fail "serve did not become healthy (last status: ${status:-none})"
echo "serve is healthy on ${HTTP}"

say "REST — GET /health"
curl -fsS "${HTTP}/health" && echo

say "REST — RenderJsonSchema on the seeded demo.shop.v1.Order"
curl -fsS -H 'content-type: application/json' \
  -d '{"schema": {"type": "demo.shop.v1.Order"}}' \
  "${HTTP}/grpc-json/ProtoMoltService/RenderJsonSchema" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print("  returned a JSON Schema document,", len(json.dumps(d)), "bytes")' \
  || fail "RenderJsonSchema did not return a document"

say "MCP — initialize + tools/list over ${HTTP}/mcp"
curl -fsS -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"docker-smoke","version":"0"}}}' \
  "${HTTP}/mcp" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print("  server:", d["result"]["serverInfo"])' \
  || fail "MCP initialize failed"

curl -fsS -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
  "${HTTP}/mcp" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); t=d["result"]["tools"]; print("  MCP exposes", len(t), "tools, e.g.:", ", ".join(x["name"] for x in t[:6]))' \
  || fail "MCP tools/list failed"

say "ACP — driving the acp container over stdio"
./gradlew :protomolt-acp:acpSmoke --console=plain -q \
  || fail "the acp container did not answer the protocol"

say "PASS — MCP and ACP both answered from Docker"
