#!/bin/sh
# Export one terminal task, then verify its signed record and all referenced
# artifact bytes in a network-disabled container. No host JDK is required.
set -eu
if [ "$#" -ne 1 ]; then
  echo 'usage: ./verify-offline.sh <terminal-task-uuid>' >&2
  exit 2
fi
case "$1" in
  ''|*[!0-9a-fA-F-]*) echo 'task id must be a UUID' >&2; exit 2 ;;
esac
if [ "${#1}" -ne 36 ]; then
  echo 'task id must be a UUID' >&2
  exit 2
fi
cd "$(dirname "$0")"
test -f record-verifier.jar || { echo 'record-verifier.jar is missing from the release bundle' >&2; exit 2; }
command -v unzip >/dev/null || { echo 'unzip is required for offline verification' >&2; exit 2; }

mkdir -p evidence
out="$(pwd)/evidence/$1"
if [ -e "$out" ]; then
  echo 'this task already has an evidence export; move it before exporting again' >&2
  exit 2
fi
mkdir -m 0700 "$out"
./export-record.sh "$1" "$out/record.zip"
mkdir "$out/extracted"
unzip -q "$out/record.zip" -d "$out/extracted"
test -s "$out/extracted/record.binpb"
test -s "$out/extracted/trust.binpb"
test -d "$out/extracted/artifacts"

image_id=$(docker compose images -q serve | head -n 1)
test -n "$image_id" || { echo 'serve image is unavailable locally' >&2; exit 1; }
docker run --rm --pull=never --network none --read-only \
  --user "$(id -u):$(id -g)" --entrypoint java \
  -v "$out/extracted:/evidence:ro" \
  -v "$(pwd)/record-verifier.jar:/verifier.jar:ro" \
  "$image_id" -cp /verifier.jar ai.protomolt.receipt.verify.Main \
  /evidence/record.binpb /evidence/trust.binpb /evidence/artifacts
printf 'Offline verification passed; evidence exported to %s\n' "$out"
