#!/usr/bin/env bash
# Static, in-process checks for the deployment package: no containers, no GPU,
# no live calls. Verifies shell syntax, YAML parses, secret placeholders stay
# out of the compose files (values come from stack variables), the Dockerfiles
# copy real Gradle distribution layouts, and the authored files carry no em
# dashes. Runs shellcheck only when it is installed.
set -euo pipefail

cd "$(dirname "$0")/.."

say()  { printf '== %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

say "shell syntax: bash -n on every script"
for script in scripts/*.sh; do
  bash -n "$script" || fail "bash -n $script"
done
for script in deploy/*/*.sh; do
  bash -n "$script" || fail "bash -n $script"
done
echo "bash -n OK for repository deployment scripts"

if command -v shellcheck >/dev/null; then
  shellcheck -S warning scripts/*.sh || fail "shellcheck findings"
  echo "shellcheck OK"
else
  echo "SKIP: shellcheck not installed"
fi

say "yaml parses: compose files and workflows"
python3 - <<'PYEOF'
import sys
try:
    import yaml
except ImportError:
    print("SKIP: pyyaml not installed")
    sys.exit(0)
for path in [
    "deploy/document-platform/compose.yml",
    "deploy/document-platform/compose-roles.yml",
    "deploy/portainer/compose.yml",
    "deploy/krick/compose.yml",
    "deploy/nano1/compose.yml",
    "docker-compose.yml",
    ".github/workflows/ci.yml",
    ".github/workflows/docker-publish.yml",
    ".github/workflows/nano1-arm64-smoke.yml",
    ".forgejo/workflows/publish-registry.yml",
    ".forgejo/workflows/tei-integration.yml",
]:
    with open(path) as handle:
        yaml.safe_load(handle)
    print("yaml OK", path)
PYEOF

say "nano1 workflow: trusted manual dispatch only"
python3 - <<'PYEOF'
import re, sys
path = ".github/workflows/nano1-arm64-smoke.yml"
text = open(path).read()
if not re.search(r"(?m)^on:\s*\n\s+workflow_dispatch:\s*$", text):
    print(path, "must use workflow_dispatch as its only trigger")
    sys.exit(1)
for forbidden in ["pull_request:", "push:", "pull_request_target:"]:
    if forbidden in text:
        print(path, "contains forbidden trigger", forbidden)
        sys.exit(1)
if "runs-on: [self-hosted, Linux, ARM64, nano1]" not in text:
    print(path, "must select the dedicated nano1 runner")
    sys.exit(1)
if "permissions:\n  contents: read" not in text:
    print(path, "must retain read-only repository permissions")
    sys.exit(1)
print("nano1 workflow is manual-only with read-only repository permissions")
PYEOF

say "nano1 deployment statics"
deploy/nano1/check-deployment-statics.sh

say "portainer jobs-init: creates the jobs database only when it is missing"
deploy/portainer/test/jobs-init-test.sh

say "compose semantics: secrets use required stack variables, never literals"
python3 - <<'PYEOF'
import re, sys
text = open("deploy/portainer/compose.yml").read()
required = [
    "PROTOMOLT_API_TOKEN",
    "PROTOMOLT_RUSTFS_SECRET_KEY",
    "PROTOMOLT_KEYCLOAK_ADMIN_PASSWORD",
    "PROTOMOLT_KEYCLOAK_CLIENT_SECRET",
    "PROTOMOLT_TRANSCRIPT_KEY",
    "PROTOMOLT_REPO_DB_PASSWORD",
]
for name in required:
    if "${%s:?" % name not in text:
        print("missing required stack variable placeholder:", name)
        sys.exit(1)
print("portainer compose requires", len(required), "secret stack variables")
for flag in ["--mesh-cluster-id", "--mesh-created-at"]:
    if flag not in text:
        print("portainer compose is missing mesh option", flag)
        sys.exit(1)
print("portainer compose enables the durable mesh directory")

krick = open("deploy/krick/compose.yml").read()
if "${PROTOMOLT_MCP_TOKEN:?" not in krick:
    print("krick compose must require PROTOMOLT_MCP_TOKEN")
    sys.exit(1)
if krick.count("~/.gitconfig:/home/protomolt/.gitconfig:ro") != 2:
    print("krick agents must receive the host git config read-only")
    sys.exit(1)
if krick.count("/work/main/dev-tools/protomolt/.git") != 4:
    print("krick agents must receive linked-worktree git metadata")
    sys.exit(1)
print("krick compose requires PROTOMOLT_MCP_TOKEN")
print("krick compose mounts git config read-only for both agents")
print("krick compose mounts linked-worktree git metadata for both agents")
PYEOF

say "postgres volumes match the data path of the image major they pin"
python3 - <<'PYEOF'
import sys
try:
    import yaml
except ImportError:
    print("SKIP: pyyaml not installed")
    sys.exit(0)

# The official postgres images moved where the server keeps its cluster. Through
# 17, PGDATA and the declared VOLUME were both /var/lib/postgresql/data. From 18,
# PGDATA is /var/lib/postgresql/<major>/docker and the VOLUME is the parent.
# Pairing an 18 image with the 17 path fails silently rather than loudly: the
# server initialises a fresh cluster on a path the compose file does not mount,
# reports healthy on an empty database, and loses it on the next recreate. A
# major bump that "needs no source changes" is exactly how that arrives, so the
# pairing is checked here rather than discovered on a deploy.
LEGACY = "/var/lib/postgresql/data"
PARENT = "/var/lib/postgresql"


def image_major(tag):
    """Leading integer of a tag: 18.6-alpine and 18-alpine both give 18."""
    digits = ""
    for char in tag:
        if not char.isdigit():
            break
        digits += char
    return int(digits) if digits else None


checked = 0
for path in [
    "deploy/document-platform/compose.yml",
    "deploy/document-platform/compose-roles.yml",
    "deploy/portainer/compose.yml",
    "deploy/krick/compose.yml",
    "deploy/nano1/compose.yml",
    "docker-compose.yml",
]:
    with open(path) as handle:
        document = yaml.safe_load(handle) or {}
    for name, service in (document.get("services") or {}).items():
        image = (service or {}).get("image") or ""
        if not image.startswith("postgres:"):
            continue
        if service.get("entrypoint") is not None:
            # The image is also the psql client: jobs-init runs one command and
            # exits, with no data directory to place. Overriding the entrypoint
            # is what separates those from a server, so they are skipped rather
            # than made to carry a volume they never write.
            continue
        major = image_major(image.split(":", 1)[1])
        if major is None:
            print(f"{path}: {name} pins a postgres tag with no readable major: {image}")
            sys.exit(1)
        expected = PARENT if major >= 18 else LEGACY
        mounted = [
            entry.split(":")[1]
            for entry in (service.get("volumes") or [])
            if isinstance(entry, str) and ":" in entry
        ]
        found = [target for target in mounted if target in (LEGACY, PARENT)]
        if found != [expected]:
            print(f"{path}: {name} runs {image}, whose PGDATA lives under {expected},")
            print(f"  but the service mounts {found or 'nothing'} there.")
            print("  A postgres major bump moves PGDATA; move the volume with it.")
            sys.exit(1)
        checked += 1
print("postgres services mount the path their image major uses:", checked)
PYEOF

say "dockerfiles: COPY sources match the Gradle installDist layout"
python3 - <<'PYEOF'
import os, sys
pairs = [
    ("apps/serve/Dockerfile", "build/install/protomolt-serve", "apps/serve/build.gradle", "protomolt-serve"),
    ("repo/service/Dockerfile", "build/install/protomolt-repo-service", "repo/service/build.gradle", "protomolt-repo-service"),
    ("apps/document-platform/Dockerfile", "build/install/protomolt-document-platform", "apps/document-platform/build.gradle", "protomolt-document-platform"),
    ("apps/agent-host/Dockerfile", "build/install/protomolt-agent-host", "apps/agent-host/build.gradle", "protomolt-agent-host"),
    ("apps/agent-host/Dockerfile.workers", "build/install/protomolt-agent-host", "apps/agent-host/build.gradle", "protomolt-agent-host"),
]
for dockerfile, copy_source, gradle_file, distribution in pairs:
    text = open(dockerfile).read()
    if "COPY %s" % copy_source not in text:
        print(dockerfile, "does not COPY", copy_source)
        sys.exit(1)
    gradle = open(gradle_file).read()
    if "application" not in gradle:
        print(gradle_file, "applies no application plugin; the distribution cannot build")
        sys.exit(1)
    print("dockerfile OK", dockerfile, "->", copy_source)

agent_host = open("apps/agent-host/Dockerfile").read()
# The requirement is javac, not a particular release: this image runs delegated builds
# and tests. Asserting a version literal made the check fail the moment the JDK baseline
# moved, while a -jre image of the pinned version would have passed it.
temurin_bases = [line.split(None, 1)[1].split()[0]
                 for line in agent_host.splitlines()
                 if line.startswith("FROM eclipse-temurin:")]
if not temurin_bases:
    print("agent-host image must build on an eclipse-temurin base")
    sys.exit(1)
if not all(base.endswith("-jdk") for base in temurin_bases):
    print("agent-host image must include javac for delegated build and test work,",
          "found:", ", ".join(temurin_bases))
    sys.exit(1)
print("agent-host image includes a JDK for delegated build and test work")
if "chmod 755 /home/protomolt" not in agent_host:
    print("agent-host image home must be readable by provider file watchers")
    sys.exit(1)
print("agent-host image home is readable by provider file watchers")

workers = open("apps/agent-host/Dockerfile.workers").read()
for target in ["common", "java", "cpp"]:
    if " AS %s" % target not in workers:
        print("worker Dockerfile is missing target", target)
        sys.exit(1)
for tool in ["buf", "bun", "docker", "grpcurl", "protoc", "uv"]:
    if tool not in workers:
        print("worker Dockerfile is missing common tool", tool)
        sys.exit(1)
for version in ["21.0.12-tem", "25.0.4-tem", "26.0.2-tem", "25.2.4-graalce"]:
    if version not in workers:
        print("worker Java target is missing SDKMAN candidate", version)
        sys.exit(1)
if "protobuf-compiler-grpc" not in workers or "protoc-gen-grpc-java" not in workers:
    print("worker language targets must carry their gRPC generators")
    sys.exit(1)
print("worker images carry common gRPC tools and pinned Java SDKs")
PYEOF

say "no em dashes in authored deployment files"
python3 - <<'PYEOF'
import glob, sys
paths = []
for pattern in ["deploy/krick/*", "deploy/portainer/*", "deploy/nano1/*",
                "deploy/workers/*",
                "scripts/agent-host-live.sh", "scripts/check-deployment-statics.sh",
                "scripts/nano1-worker-build-smoke.sh",
                "repo/service/Dockerfile", "apps/agent-host/Dockerfile",
                "apps/agent-host/Dockerfile.workers"]:
    paths.extend(p for p in glob.glob(pattern) if not p.endswith("/"))
bad = []
for path in paths:
    try:
        text = open(path, encoding="utf-8").read()
    except (IsADirectoryError, UnicodeDecodeError):
        continue
    if "\u2014" in text or "\u2013" in text:
        bad.append(path)
if bad:
    print("em dash found in:", ", ".join(bad))
    sys.exit(1)
print("no em dashes in", len(paths), "files")
PYEOF

say "PASS: deployment statics"
