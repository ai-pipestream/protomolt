#!/usr/bin/env bash
# Offline repository backup and recovery rehearsal. One command from a clean checkout.
#
#   verification/repository-recovery/run.sh --out <new-or-empty-dir> [--runs 2] [--keep] [--skip-negatives] [--xid-burn 8192]
#
# The second positive run consumes --xid-burn transaction ids before seeding, so its restore
# must advance the new cluster with pg_resetwal; --xid-burn 0 disables that coverage.
#
# Prerequisites (checked, never worked around): Docker reachable by this user, Java 25
# toolchain for Gradle, network access to pull postgres:18-alpine, rustfs/rustfs
# 1.0.0-beta.11-preview.1 and alpine:3.20 if they are not present. pg_dump,
# pg_restore and pg_resetwal run inside the pinned PostgreSQL image, not on the host.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"
for tool in docker java; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing required tool: $tool" >&2; exit 2; }
done
docker version --format '{{.Server.Version}}' >/dev/null 2>&1 || { echo "Docker daemon is not reachable" >&2; exit 2; }
[[ " $* " == *" --out "* ]] || { echo "usage: $0 --out <new-or-empty-dir> [--runs N] [--keep] [--skip-negatives]" >&2; exit 2; }

# Gradle runs under an optional shared lock (PROTOMOLT_GRADLE_LOCK=<file>) so parallel agents
# on one machine do not build the same checkout at the same time.
gradle() {
  if [[ -n "${PROTOMOLT_GRADLE_LOCK:-}" ]]; then flock -w 7200 "$PROTOMOLT_GRADLE_LOCK" "$root/gradlew" "$@"; else "$root/gradlew" "$@"; fi
}
# The observed admission runtime bundle must come from the root build: its component names
# are what the production observer matches, and a composite build would prefix them.
gradle -p "$root" --console=plain -q :protomolt-repo-container:admissionTransportRuntimeInventory
bundle="$root/repo/container/build/admission-transport-runtime"
# Then the harness distribution on top of the same JARs, through the composite build.
gradle -p "$here" --console=plain -q installDist
[[ -f "$bundle/inventory.tsv" ]] || { echo "admission runtime bundle missing at $bundle" >&2; exit 2; }
exec "$here/build/install/repository-recovery/bin/repository-recovery" --bundle "$bundle" "$@"
