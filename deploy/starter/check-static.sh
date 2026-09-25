#!/bin/sh
# Validate the release starter without pulling application images.
set -eu
cd "$(dirname "$0")/../.."

PROTOMOLT_VERSION=1.2.3 \
  docker compose -f deploy/starter/compose.yml --profile acp config --format json \
  | jq -e '
      (.services | all(.[]; has("build") | not)) and
      ([.services.serve, .services.correction, .services.acp]
        | all(.[]; .image | endswith(":1.2.3"))) and
      (.services.serve.ports | length == 2 and all(.[]; .host_ip == "127.0.0.1")) and
      (.services.serve.environment.PROTOMOLT_GRPC_MAX_DEADLINE_MS | tonumber >= 330000) and
      (.services.bootstrap.command | length == 1) and
      (.services.serve.command | length == 1) and
      (.services.correction.command | length == 1) and
      (.services.acp.command | length == 1) and
      ([.services.correction.volumes[].source] | sort == ["correction-data", "correction-secret"]) and
      ([.services.acp.volumes[].source] | sort == ["operator-secret"]) and
      ([.services.serve.volumes[].source] | sort ==
        ["console-secret", "correction-secret", "operator-secret", "serve-data"])
    ' >/dev/null

PROTOMOLT_VERSION=1.2.3 PROTOMOLT_INFERENCE_TARGET=bridge:29930 \
  docker compose -f deploy/starter/compose.yml \
    -f deploy/starter/compose.live.yml config --format json \
  | jq -e '
      ([.services.correction.volumes[].source] | index("correction-live-data") != null) and
      ([.services.correction.volumes[].source] | index("correction-secret") != null) and
      ([.services.correction.volumes[].source] | index("operator-secret") == null) and
      ([.services.correction.volumes[].source] | index("console-secret") == null)
    ' >/dev/null

PROTOMOLT_VERSION=1.2.3 \
  PROTOMOLT_SERVE_IMAGE=ghcr.io/example/serve@sha256:serve \
  PROTOMOLT_CORRECTION_IMAGE=ghcr.io/example/correction@sha256:correction \
  PROTOMOLT_ACP_IMAGE=ghcr.io/example/acp@sha256:acp \
  docker compose -f deploy/starter/compose.yml --profile acp config --format json \
  | jq -e '
      .services.serve.image == "ghcr.io/example/serve@sha256:serve" and
      .services.correction.image == "ghcr.io/example/correction@sha256:correction" and
      .services.acp.image == "ghcr.io/example/acp@sha256:acp"
    ' >/dev/null

sh -n deploy/starter/verify-receipt.sh
echo 'starter Compose and helper static checks passed'
