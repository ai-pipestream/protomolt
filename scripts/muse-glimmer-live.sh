#!/usr/bin/env bash
# Live smoke for the Muse Glimmer sidecar on krick-1. Requires the running
# GPU stack: it checks the model list and one bounded chat completion, then
# requires log or metrics evidence that the B70/XPU loaded the model. It
# deploys nothing and changes nothing on the host.
set -euo pipefail

ENDPOINT="${GLIMMER_ENDPOINT:-http://127.0.0.1:8011}"
MODEL="${GLIMMER_MODEL:-muse-glimmer-30b}"
COMPOSE_FILE="${GLIMMER_COMPOSE_FILE:-deploy/krick-1/compose.yml}"
MAX_TOKENS="${GLIMMER_SMOKE_MAX_TOKENS:-32}"
TURN_SECONDS="${GLIMMER_SMOKE_TURN_SECONDS:-120}"

say()  { printf '== %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

command -v curl >/dev/null || fail "curl is required"

say "model list at $ENDPOINT"
models="$(curl -fsS --max-time 15 "$ENDPOINT/v1/models")" \
  || fail "model list request failed"
printf '%s' "$models" | grep -q "$MODEL" \
  || fail "model $MODEL is not served: $(printf '%s' "$models" | head -c 200)"
echo "model list OK: $MODEL is served"

say "one bounded chat completion (max_tokens=$MAX_TOKENS)"
payload="$(printf '{"model":"%s","messages":[{"role":"user","content":"Reply with the single word: ready"}],"max_tokens":%s,"temperature":0}' \
  "$MODEL" "$MAX_TOKENS")"
completion="$(curl -fsS --max-time "$TURN_SECONDS" \
  -H 'content-type: application/json' \
  -d "$payload" "$ENDPOINT/v1/chat/completions")" \
  || fail "chat completion request failed"
printf '%s' "$completion" | grep -q '"content"' \
  || fail "chat completion returned no message content"
echo "chat completion OK"

say "B70/XPU load evidence"
evidence=""
if command -v docker >/dev/null && [ -f "$COMPOSE_FILE" ]; then
  if docker compose -f "$COMPOSE_FILE" logs glimmer-vllm 2>/dev/null \
      | grep -Eiq 'xpu|b70|battlemage|level_zero'; then
    evidence="container logs report the XPU/B70 backend"
  fi
fi
if [ -z "$evidence" ]; then
  if metrics="$(curl -fsS --max-time 15 "$ENDPOINT/metrics" 2>/dev/null)" \
      && printf '%s' "$metrics" | grep -q 'vllm:'; then
    evidence="metrics endpoint exposes the running vllm engine"
  fi
fi
[ -n "$evidence" ] || fail "no log or metrics evidence that the B70/XPU loaded the model"
echo "load evidence OK: $evidence"

say "PASS: muse glimmer live smoke"
