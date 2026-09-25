#!/bin/sh
# Export a correction receipt and trust snapshot, restart, then verify offline.
set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: ./verify-receipt.sh <completed-run-id>" >&2
  exit 2
fi
run_id=$1
case "$run_id" in
  ''|.|..|*[!A-Za-z0-9._-]*) echo "invalid run id" >&2; exit 2 ;;
esac
if [ ! -f ./record-verifier.jar ]; then
  echo "record-verifier.jar is missing; use the versioned release bundle" >&2
  exit 2
fi

docker compose exec -T correction test -f "/data/correction/outcomes/$run_id/completed"
mkdir -p evidence
out="$(pwd)/evidence/$run_id"
if [ -e "$out" ]; then
  echo "evidence/$run_id already exists; choose an unused export directory" >&2
  exit 2
fi
mkdir -m 0700 "$out"
docker compose cp "correction:/data/correction/outcomes/$run_id/receipt.pb" "$out/receipt.pb" >/dev/null
docker compose cp "correction:/data/correction/outcomes/$run_id/execution.pb" "$out/execution.pb" >/dev/null
docker compose cp correction:/data/correction/trust.pb "$out/trust.pb" >/dev/null
docker compose cp "correction:/data/correction/outcomes/$run_id" "$out/outcome" >/dev/null
docker compose cp "correction:/data/correction/runs/$run_id.pb" "$out/run.pb" >/dev/null
verifier="$(pwd)/record-verifier.jar"
image_id=$(docker compose images -q correction | head -n 1)
if [ -z "$image_id" ]; then
  echo "correction image is unavailable locally" >&2
  exit 1
fi
for record in execution.pb receipt.pb; do
  docker run --rm --pull=never --network none --read-only \
    --entrypoint java \
    --user "$(id -u):$(id -g)" \
    -v "$out:/evidence:ro" -v "$verifier:/verifier.jar:ro" \
    "$image_id" \
    -cp /verifier.jar ai.protomolt.receipt.verify.Main \
    --list-artifacts "/evidence/$record" /evidence/trust.pb \
    > "$out/$record.artifact-digests"
done
sort -u "$out/execution.pb.artifact-digests" "$out/receipt.pb.artifact-digests" \
  > "$out/artifact-digests"
mkdir -m 0700 "$out/artifacts"
while IFS= read -r digest; do
  case "$digest" in
    *[!0-9a-f]*|'') echo "invalid verified artifact digest" >&2; exit 1 ;;
  esac
  if [ "${#digest}" -ne 64 ]; then
    echo "invalid verified artifact digest" >&2
    exit 1
  fi
  docker compose cp "correction:/data/correction/artifacts/$digest" \
    "$out/artifacts/$digest" >/dev/null
done < "$out/artifact-digests"
docker compose restart correction serve >/dev/null
attempt=0
until docker compose exec -T correction test -f "/data/correction/outcomes/$run_id/completed" \
  && docker compose exec -T serve bash -ec '
    exec 3<>/dev/tcp/127.0.0.1/8080
    printf "GET /health HTTP/1.0\r\n\r\n" >&3
    read -r line <&3
    [[ "$line" == *" 200 "* ]]
  '; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    echo "services did not return after restart" >&2
    exit 1
  fi
  sleep 2
done

docker compose cp "correction:/data/correction/outcomes/$run_id/receipt.pb" "$out/receipt-after-restart.pb" >/dev/null
docker compose cp "correction:/data/correction/outcomes/$run_id/execution.pb" "$out/execution-after-restart.pb" >/dev/null
docker compose cp correction:/data/correction/trust.pb "$out/trust-after-restart.pb" >/dev/null
cmp "$out/receipt.pb" "$out/receipt-after-restart.pb"
cmp "$out/execution.pb" "$out/execution-after-restart.pb"
cmp "$out/trust.pb" "$out/trust-after-restart.pb"

for record in execution.pb receipt.pb; do
  docker run --rm --pull=never --network none --read-only \
    --entrypoint java \
    --user "$(id -u):$(id -g)" \
    -v "$out:/evidence:ro" -v "$verifier:/verifier.jar:ro" \
    "$image_id" \
    -cp /verifier.jar ai.protomolt.receipt.verify.Main \
    "/evidence/$record" /evidence/trust.pb /evidence/artifacts
done

printf '%s\n' 'Receipt and trust bytes survived restart; records and artifact bytes verified offline.'
printf '%s\n' 'Evidence exported to:' "$out"
