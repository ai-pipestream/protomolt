#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."

PROTOMOLT_VERSION=1.2.3 docker compose -f deploy/agents/compose.yml \
  config --format json | jq -e '
    (.services | all(.[]; has("build") | not)) and
    (.services.serve.ports | length == 2) and
    (.services.serve.user == "10001:10001") and
    (.services["fixture-worker"].user == "10001:10001") and
    ([.services[].ports[]?] | all(.[]; .host_ip == "127.0.0.1")) and
    (.services["fixture-worker"].volumes | map(.source) | sort ==
      ["agent-state", "agent-workspace", "coordination-secret", "evidence-data"]) and
    ([.services["fixture-worker"].volumes[].source] | index("operator-secret") == null) and
    ([.services["fixture-worker"].volumes[].source] | index("signing-identity") == null) and
    ([.services["fixture-worker"].volumes[] | select(.source == "evidence-data") | .target] == ["/workspace/artifacts"]) and
    ([.services.serve.volumes[] | select(.source == "evidence-data") | .target] == ["/data/evidence"]) and
    (.services.rustfs.environment.RUSTFS_SECRET_KEY_FILE == "/run/s3/secret-key") and
    (.services["repo-postgres"].environment.POSTGRES_PASSWORD_FILE == "/run/database/password") and
    (.services.serve.environment.PROTOMOLT_ACCESS_POLICY == "/run/authz/policy.json")
  ' >/dev/null

PROTOMOLT_VERSION=1.2.3 KIMI_CODE_DIR=/tmp/protomolt-kimi-example \
  KIMI_AGENT_UID=1000 KIMI_AGENT_GID=1000 \
  docker compose -f deploy/agents/compose.yml -f deploy/agents/compose.kimi.yml \
    --profile kimi config --format json | jq -e '
      (.services | all(.[]; has("build") | not)) and
      ([.services["kimi-worker"].volumes[].source]
        | all(.[]; . != "operator-secret" and . != "console-secret"
          and . != "signing-identity" and . != "transcript-secret"
          and . != "database-secret" and . != "s3-secret")) and
      ([.services["kimi-worker"].volumes[] | select(.source == "evidence-data") | .target] == ["/workspace/artifacts"])
      and (.services["kimi-worker"].user == "1000:10001")
      and (.services["kimi-worker"].group_add == ["1000"])
    ' >/dev/null

PROTOMOLT_VERSION=1.2.3 \
  PROTOMOLT_ALPINE_IMAGE=alpine@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  PROTOMOLT_ACP_IMAGE=example/acp@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  docker compose -f deploy/agents/compose.yml --profile acp \
    config --format json | jq -e '
      .services.bootstrap.image == "alpine@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      and .services["acp-agent"].image == "example/acp@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      and (.services["acp-agent"].volumes | map(.source) == ["coordination-secret"])
      and ([.services["acp-agent"].volumes[].source] | index("operator-secret") == null)
      and ([.services["acp-agent"].volumes[].source] | index("signing-identity") == null)
    ' >/dev/null

echo 'agent starter Compose static checks passed'
