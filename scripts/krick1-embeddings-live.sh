#!/usr/bin/env bash
# Live smoke for the OpenVINO embeddings sidecar on krick-1. Requires the
# running compose.embeddings.yml stack: it checks the served model list, runs
# one bounded embeddings request against the MPNet pipeline, requires the
# returned vector to have the expected 768 dimensions, then requires log
# evidence that the embedding models compiled for the B70/GPU. It deploys
# nothing and changes nothing on the host.
set -euo pipefail

ENDPOINT="${EMBEDDER_ENDPOINT:-http://127.0.0.1:8091}"
PIPELINE="${EMBEDDER_PIPELINE:-mpnet_pipeline}"
EXPECTED_DIM="${EMBEDDER_EXPECTED_DIM:-768}"
COMPOSE_FILE="${EMBEDDER_COMPOSE_FILE:-deploy/krick-1/compose.embeddings.yml}"

say()  { printf '== %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

command -v curl >/dev/null || fail "curl is required"

say "model status at $ENDPOINT"
status="$(curl -fsS --max-time 15 "$ENDPOINT/v1/models/$PIPELINE")" \
  || fail "model status request failed"
printf '%s' "$status" | grep -q '"AVAILABLE"' \
  || fail "pipeline $PIPELINE is not AVAILABLE: $(printf '%s' "$status" | head -c 200)"
echo "model status OK: $PIPELINE is AVAILABLE"

say "one bounded embeddings request"
response="$(curl -fsS --max-time 60 \
  -H 'content-type: application/json' \
  -d '{"instances":[{"strings":"protomolt krick-1 embeddings smoke"}]}' \
  "$ENDPOINT/v1/models/$PIPELINE:predict")" \
  || fail "embeddings request failed"
# OVMS answers the instances-form predict either with the named output
# ({"predictions":[{"sentence_embedding":[...]}]}) or, for a single unnamed
# output, flattened ({"predictions":[[...]]}). Accept both.
vector="$(printf '%s' "$response" \
  | grep -o '"sentence_embedding":\[[^]]*\]' | head -1)" \
  || true
if [ -z "$vector" ]; then
  vector="$(printf '%s' "$response" \
    | grep -o '"predictions":\s*\[\[[^]]*\]' | head -1)" \
    || true
fi
[ -n "$vector" ] \
  || fail "response carried no embedding vector: $(printf '%s' "$response" | head -c 200)"
dim=$(( $(printf '%s' "$vector" | tr -cd ',' | wc -c) + 1 ))
[ "$dim" = "$EXPECTED_DIM" ] \
  || fail "embedding dimension $dim, expected $EXPECTED_DIM"
echo "embeddings OK: $dim-dimensional vector"

say "B70/GPU compile evidence"
evidence=""
if command -v docker >/dev/null && [ -f "$COMPOSE_FILE" ]; then
  if docker compose -f "$COMPOSE_FILE" logs embedder-ovms 2>/dev/null \
      | grep -Eiq 'target device: GPU|Plugin config for device: GPU'; then
    evidence="container logs report the GPU target device"
  fi
fi
[ -n "$evidence" ] || fail "no log evidence that the embedding models compiled for the GPU"
echo "load evidence OK: $evidence"

say "PASS: krick-1 embeddings live smoke"
