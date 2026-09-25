#!/usr/bin/env bash
# Run from a disposable extracted agents-starter directory after compose up -d.
# Requires Docker Compose, Node 22, jq, grpcurl, Chromium, and the bundled verifier jar.
set -euo pipefail

HTTP_BASE=${PROTOMOLT_HTTP_BASE:-http://127.0.0.1:18080}
GRPC_TARGET=${PROTOMOLT_GRPC_TARGET:-127.0.0.1:19090}

ready() {
  docker compose ps --all --format json | jq -se '
    (map(select(.Service == "bootstrap" or .Service == "signing-init"
      or .Service == "rustfs-init" or .Service == "repo-init"))
      | length == 4 and all(.[]; .State == "exited" and (.ExitCode | tostring) == "0")) and
    (map(select(.Service == "rustfs" or .Service == "repo-postgres"
      or .Service == "repo-service" or .Service == "serve"))
      | length == 4 and all(.[]; .State == "running" and .Health == "healthy")) and
    (map(select(.Service == "fixture-worker"))
      | length == 1 and all(.[]; .State == "running"))
  ' >/dev/null
}

for attempt in $(seq 1 90); do
  if ready; then break; fi
  if [ "$attempt" -eq 90 ]; then
    docker compose ps --all
    echo 'agents starter services did not become ready' >&2
    exit 1
  fi
  sleep 2
done

SMOKE=$(HTTP_BASE="$HTTP_BASE" node smoke.mjs)
printf '%s\n' "$SMOKE" | jq -e '
  .taskId and .authenticated_console and .fixture_agent_host
  and .typed_candidate and .revision_bound_review and .question_reply
  and .unsupported_offer_rejected and .receipt_snapshot
  and .provider == "fixture" and .live_model == false' >/dev/null
TASK_ID=$(printf '%s\n' "$SMOKE" | jq -r '.taskId')

PROTOCOL=$(PROTOMOLT_HTTP_BASE="$HTTP_BASE" PROTOMOLT_GRPC_TARGET="$GRPC_TARGET" \
  PROTOMOLT_COMPOSE_DIR="$PWD" node protocol-smoke.mjs)
printf '%s\n' "$PROTOCOL" | jq -e \
  '.manualReviewPending and .acp_delegation == true
   and .provider == "protocol-fixture" and .liveModel == false
   and (.tests | index("authenticated MCP initialize") != null)
   and (.tests | index("authenticated direct ProtoMoltService gRPC contract validation") != null)
   and (.tests | index("authenticated native DelegationService gRPC transcript read") != null)
   and (.tests | index("caller-owned descriptor via MCP and actual content-addressed artifact") != null)
   and (.tests | index("native gRPC review bound to accepted MCP candidate identity") != null)
   and (.tests | index("cold ACP transcript renders caller-owned historical Any") != null)
   and (.tests | index("ACP typed custom candidate crossed native gRPC and MCP state") != null)
   and (.tests | index("wrong-count CEL refused before task transcript append") != null)
   and (.tests | index("wrong Any type refused before task transcript append") != null)
   and (.tests | index("missing required evidence refused before task transcript append") != null)
   and (.tests | index("bundled report valid candidate available for human review") != null)
   and (.tasks.bundledValid == .manualReviewPending)' >/dev/null

test -n "${CHROME_BIN:-}" && test -x "$CHROME_BIN" || {
  echo 'CHROME_BIN must identify an installed Chromium executable' >&2; exit 1;
}
BROWSER=$(HTTP_BASE="$HTTP_BASE" CHROME_BIN="$CHROME_BIN" node browser-smoke.mjs)
printf '%s\n' "$BROWSER" | jq -e '
  .browser == "Chromium" and .login == true
  and .coordination == "TYPED_CANDIDATE" and .session == true
  and .accepted == true and .custom_contract_upload == true
  and .invalid_contract_feedback == true and .unsupported_contract_rejected == true
  and .provider == "fixture" and .liveModel == false' >/dev/null

./verify-offline.sh "$TASK_ID"
EXTRACTED="$PWD/evidence/$TASK_ID/extracted"
TAMPER=$(mktemp -d)
trap 'rm -rf "$TAMPER"' EXIT
cp -a "$EXTRACTED/." "$TAMPER/"
ARTIFACT=$(find "$TAMPER/artifacts" -maxdepth 1 -type f -print -quit)
test -n "$ARTIFACT" || { echo 'no exported artifact for tamper check' >&2; exit 1; }
printf 'tamper\n' >> "$ARTIFACT"
IMAGE_ID=$(docker compose images -q serve | head -n 1)
TAMPER_STATUS=0
docker run --rm --pull=never --network none --read-only \
  --user "$(id -u):$(id -g)" --entrypoint java \
  -v "$TAMPER:/evidence:ro" -v "$PWD/record-verifier.jar:/verifier.jar:ro" \
  "$IMAGE_ID" -cp /verifier.jar ai.protomolt.receipt.verify.Main \
  /evidence/record.binpb /evidence/trust.binpb /evidence/artifacts \
  > "$TAMPER/verification.txt" 2>&1 || TAMPER_STATUS=$?
if [ "$TAMPER_STATUS" -ne 1 ] \
  || ! grep -q '^FAILED  artifact-rehash:' "$TAMPER/verification.txt" \
  || ! grep -q '^REFUSED$' "$TAMPER/verification.txt"; then
  echo 'offline tamper check did not refuse the changed artifact as expected' >&2
  exit 1
fi
rm -rf "$TAMPER"
TRUST_BEFORE=$(mktemp)
trap 'rm -f "$TRUST_BEFORE"' EXIT
docker compose exec -T serve cat /run/identity/trust.binpb > "$TRUST_BEFORE"
test -s "$TRUST_BEFORE"
docker compose restart serve fixture-worker >/dev/null
for attempt in $(seq 1 90); do
  if ready; then break; fi
  if [ "$attempt" -eq 90 ]; then
    docker compose ps --all
    echo 'agents starter did not recover after restart' >&2
    exit 1
  fi
  sleep 2
done
docker compose exec -T serve cat /run/identity/trust.binpb | cmp -s "$TRUST_BEFORE" - || {
  echo 'public signer trust changed after restart' >&2; exit 1;
}
RESUMED=$(VERIFY_SAVED_TASK="$TASK_ID" HTTP_BASE="$HTTP_BASE" node smoke.mjs)
printf '%s\n' "$RESUMED" | jq -e --arg id "$TASK_ID" '
    .retrieved_after_restart == true and .conversation_after_restart == true
    and .post_acceptance_record == true and .taskId == $id' >/dev/null
mv "evidence/$TASK_ID" "evidence/$TASK_ID-before-restart"
./verify-offline.sh "$TASK_ID"

TRUST_SHA=$(sha256sum "$TRUST_BEFORE" | cut -d ' ' -f1)
jq -n --argjson smoke "$SMOKE" --argjson protocol "$PROTOCOL" \
  --argjson browser "$BROWSER" --argjson resumed "$RESUMED" \
  --arg trustSha256 "$TRUST_SHA" '
  {taskId:$smoke.taskId, fixture:$smoke.fixture_agent_host,
   typedCandidate:$smoke.typed_candidate, revisionReview:$smoke.revision_bound_review,
   protocolTests:$protocol.tests, acpDelegation:$protocol.acp_delegation,
   browser:{login:$browser.login, accepted:$browser.accepted,
     typedCandidate:$browser.coordination, customContractUpload:$browser.custom_contract_upload,
     invalidContractFeedback:$browser.invalid_contract_feedback,
     unsupportedContractRejected:$browser.unsupported_contract_rejected,
     screenshot:$browser.screenshot},
   restart:{retrieved:$resumed.retrieved_after_restart,
     conversation:$resumed.conversation_after_restart,
     postAcceptanceRecord:$resumed.post_acceptance_record,
     trustSha256:$trustSha256},
   offline:{initialVerified:true, tamperRefused:true, postRestartVerified:true}}' \
  > qualification.json

printf 'Agents starter qualification passed: task %s survived restart and verified offline.\n' "$TASK_ID"
