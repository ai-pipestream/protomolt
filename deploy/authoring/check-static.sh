#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."

PROTOMOLT_AUTHORING_IMAGE=example/authoring@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
PROTOMOLT_REPO_IMAGE=example/repo@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  docker compose -f deploy/authoring/compose.yml config --format json | jq -e '
    (.services | all(.[]; has("build") | not)) and
    ([.services[].ports[]?] | all(.[]; .host_ip == "127.0.0.1")) and
    (.services.serve.image | startswith("example/authoring@sha256:")) and
    (.services.fixture.image == .services.serve.image) and
    (.services.author.image == .services.serve.image) and
    (.services["policy-seed"].image == .services.serve.image) and
    (.services["signing-init"].image == .services.serve.image) and
    (.services["repo-service"].image | startswith("example/repo@sha256:")) and
    (.services["repo-service"].environment.DOCUMENT_PLATFORM_S3_CONDITIONAL_WRITES == "true") and
    (.services.serve.environment.PROTOMOLT_JOBS_JDBC == "jdbc:postgresql://jobs-postgres:5432/jobs") and
    (.services.serve.environment.PROTOMOLT_WORKFLOW_PREPARATION_TEMPLATE_PROVIDER == "normalize-record-v1") and
    (.services.serve.environment.PROTOMOLT_GRPC_ALLOWED_HOSTS == "fixture") and
    (.services.serve.environment.PROTOMOLT_GRPC_ALLOWED_PORTS == "9778") and
    (.services.serve.healthcheck.test | join(" ") | contains("GET /health")) and
    (.services["signing-init"].command | join(" ") | contains("/identity authoring")) and
    (. as $config | ["serve", "author", "fixture", "policy-seed", "signing-init"]
      | all(.[]; . as $service | $config.services[$service].user == "10001:10001"))
  ' >/dev/null

PROTOMOLT_AUTHORING_IMAGE=example/authoring@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
PROTOMOLT_REPO_IMAGE=example/repo@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  docker compose -f deploy/authoring/compose.yml config --format json | jq -e '
    (.services.author.volumes | map(.source) | sort == ["author-secret", "author-state"]) and
    ([.services.author.volumes[].source] | index("operator-secret") == null) and
    ([.services.author.volumes[].source] | index("coordination-secret") == null) and
    ([.services.author.volumes[].source] | index("signing-identity") == null) and
    ([.services.author.volumes[].source] | index("jobs-database-secret") == null) and
    ([.services.author.volumes[].source] | index("launch-browser-secret") == null) and
    (.services.fixture.volumes | map(.source) == ["fixture-records"]) and
    ([.services["policy-seed"].volumes[].source] == ["serve-data"]) and
    (.services["policy-seed"].depends_on.fixture.condition == "service_started") and
    (.services.serve.depends_on["policy-seed"].condition == "service_completed_successfully") and
    (.services.serve.depends_on["jobs-postgres"].condition == "service_healthy") and
    (.services.author.depends_on.serve.condition == "service_healthy")
  ' >/dev/null

echo 'authoring starter Compose static checks passed'
