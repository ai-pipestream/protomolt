#!/usr/bin/env bash
# Opt-in live acceptance for the merged agent host: one real Kimi worker and
# one real Codex coordinator drive one bounded task through the full delegation
# lifecycle against a running ProtoMolt coordinator, including a worker restart
# across its checkpoint that proves cursor resume and provider-session recovery.
#
# Ships disabled. It starts real provider processes and mutates the live
# server's transcript, so it requires an explicit opt-in:
#
#   PROTOMOLT_AGENT_HOST_LIVE=1            required; anything else skips
#   PROTOMOLT_MCP_TOKEN                    required; MCP bearer token
#   PROTOMOLT_MCP_ENDPOINT                 default https://protomolt.rokkon.com/mcp
#   PROTOMOLT_LIVE_WORKER_ID               default unique kimi-live-* identity
#   PROTOMOLT_LIVE_COORDINATOR_ID          default unique codex-live-* identity
#   PROTOMOLT_LIVE_BUDGET_SECONDS          default 1200 (overall deadline)
#   PROTOMOLT_LIVE_KEEP_WORKDIR=1          keep the temp workdir on success
#
# Every step prints a greppable marker (STEP, EVENT, PROOF, PASS, FAIL).
set -euo pipefail

if [ "${PROTOMOLT_AGENT_HOST_LIVE:-0}" != "1" ]; then
  echo "SKIP: agent-host live acceptance is opt-in; set PROTOMOLT_AGENT_HOST_LIVE=1 and PROTOMOLT_MCP_TOKEN to run it"
  exit 0
fi
: "${PROTOMOLT_MCP_TOKEN:?set PROTOMOLT_MCP_TOKEN to the coordinator MCP bearer token}"

cd "$(dirname "$0")/.."

ENDPOINT="${PROTOMOLT_MCP_ENDPOINT:-https://protomolt.rokkon.com/mcp}"
RUN_SUFFIX="$(date -u +%Y%m%d%H%M%S)-$$"
WORKER_ID="${PROTOMOLT_LIVE_WORKER_ID:-kimi-live-$RUN_SUFFIX}"
COORDINATOR_ID="${PROTOMOLT_LIVE_COORDINATOR_ID:-codex-live-$RUN_SUFFIX}"
BUDGET="${PROTOMOLT_LIVE_BUDGET_SECONDS:-1200}"
LAUNCHER="$PWD/apps/agent-host/build/install/protomolt-agent-host/bin/protomolt-agent-host"

WORK="$(mktemp -d /tmp/protomolt-agent-host-live.XXXXXX)"
mkdir -p "$WORK/workspace-kimi" "$WORK/workspace-codex" "$WORK/state" "$WORK/logs"
EVENTS_JSONL="$WORK/events.jsonl"
: > "$EVENTS_JSONL"

say()  { printf '\n== %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; echo "FAIL logs kept under $WORK/logs" >&2; exit 1; }

WORKER_PID=""
COORDINATOR_PID=""
cleanup() {
  [ -n "$WORKER_PID" ] && kill "$WORKER_PID" 2>/dev/null || true
  [ -n "$COORDINATOR_PID" ] && kill "$COORDINATOR_PID" 2>/dev/null || true
  if [ "${PROTOMOLT_LIVE_KEEP_WORKDIR:-0}" != "1" ] && [ -f "$WORK/.passed" ]; then
    rm -rf "$WORK"
  else
    echo "workdir kept: $WORK"
  fi
}
trap cleanup EXIT

# curl wrapper around one MCP tools/call. Prints the structuredContent object.
MCP_SESSION=""
mcp_initialize() {
  curl -fsS -m 15 -D "$WORK/logs/initialize.headers" \
    -H 'content-type: application/json' \
    -H 'accept: application/json, text/event-stream' \
    -H "Authorization: Bearer $PROTOMOLT_MCP_TOKEN" \
    -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"agent-host-live","version":"1"}}}' \
    "$ENDPOINT" > "$WORK/logs/initialize.json" \
    || fail "MCP initialize failed against $ENDPOINT"
  MCP_SESSION="$(sed -n 's/^[Mm]cp-[Ss]ession-[Ii]d: \(.*\)\r/\1/p' "$WORK/logs/initialize.headers" | head -1)"
  [ -n "$MCP_SESSION" ] || fail "MCP initialize returned no Mcp-Session-Id"
  local status
  status="$(curl -s -o /dev/null -w '%{http_code}' -m 15 \
    -H 'content-type: application/json' \
    -H 'accept: application/json, text/event-stream' \
    -H "Authorization: Bearer $PROTOMOLT_MCP_TOKEN" \
    -H "Mcp-Session-Id: $MCP_SESSION" \
    -H 'MCP-Protocol-Version: 2025-06-18' \
    -d '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
    "$ENDPOINT")"
  [ "$status" = "202" ] || fail "MCP initialized notification returned HTTP $status"
  echo "STEP mcp-initialize OK (session ${MCP_SESSION:0:8}...)"
}

# mcp_call <tool> <arguments-json>: prints structuredContent on stdout.
mcp_call() {
  local tool="$1" args="$2" body
  body="$(python3 -c 'import json,sys; print(json.dumps({"jsonrpc":"2.0","id":99,"method":"tools/call","params":{"name":sys.argv[1],"arguments":json.loads(sys.argv[2])}}))' "$tool" "$args")"
  curl -fsS -m 45 \
    -H 'content-type: application/json' \
    -H 'accept: application/json, text/event-stream' \
    -H "Authorization: Bearer $PROTOMOLT_MCP_TOKEN" \
    -H "Mcp-Session-Id: $MCP_SESSION" \
    -H 'MCP-Protocol-Version: 2025-06-18' \
    -d "$body" \
    "$ENDPOINT" | python3 -c '
import json, sys
raw = sys.stdin.read()
try:
    envelope = json.loads(raw)
except json.JSONDecodeError:
    data = [line[5:].strip() for line in raw.splitlines() if line.startswith("data:")]
    envelope = json.loads(data[-1]) if data else {}
if "error" in envelope:
    print("TOOL-ERROR " + str(envelope["error"].get("message")), file=sys.stderr)
    sys.exit(1)
result = envelope.get("result", {})
if result.get("isError"):
    print("TOOL-ERROR " + json.dumps(result)[:400], file=sys.stderr)
    sys.exit(1)
print(json.dumps(result.get("structuredContent", {})))'
}

# mcp_read <uri>: prints the first text content of a resources/read.
mcp_read() {
  local uri="$1" body
  body="$(python3 -c 'import json,sys; print(json.dumps({"jsonrpc":"2.0","id":98,"method":"resources/read","params":{"uri":sys.argv[1]}}))' "$uri")"
  curl -fsS -m 30 \
    -H 'content-type: application/json' \
    -H 'accept: application/json, text/event-stream' \
    -H "Authorization: Bearer $PROTOMOLT_MCP_TOKEN" \
    -H "Mcp-Session-Id: $MCP_SESSION" \
    -H 'MCP-Protocol-Version: 2025-06-18' \
    -d "$body" \
    "$ENDPOINT" | python3 -c '
import json, sys
raw = sys.stdin.read()
try:
    envelope = json.loads(raw)
except json.JSONDecodeError:
    data = [line[5:].strip() for line in raw.splitlines() if line.startswith("data:")]
    envelope = json.loads(data[-1]) if data else {}
if "error" in envelope:
    print("RESOURCE-ERROR " + str(envelope["error"].get("message")), file=sys.stderr)
    sys.exit(1)
contents = envelope.get("result", {}).get("contents", [])
print(contents[0].get("text", "") if contents else "")'
}

start_agent() { # start_agent <name> <logfile> <args...>: prints the child pid
  local log="$2"
  shift 2
  "$LAUNCHER" "$@" > "$log" 2>&1 < /dev/null &
  echo $!
}

# True when the watched task has emitted the named payload kind. Events from
# other live tasks share the same feed and must never satisfy this run.
task_has_kind() {
  local kind="$1"
  [ -n "$TASK_ID" ] || return 1
  python3 - "$EVENTS_JSONL" "$TASK_ID" "$kind" <<'PYEOF'
import json, sys
for line in open(sys.argv[1]):
    event = json.loads(line)
    if event.get("taskId") != sys.argv[2]:
        continue
    entry = event.get("entry", {})
    frame = entry.get("workerFrame") or entry.get("coordinatorFrame") or {}
    if sys.argv[3] in frame:
        sys.exit(0)
sys.exit(1)
PYEOF
}

task_has_failed_terminal() {
  [ -n "$TASK_ID" ] || return 1
  python3 - "$EVENTS_JSONL" "$TASK_ID" <<'PYEOF'
import json, sys
bad = {"failed", "cancelled", "rejected", "expired", "blocked"}
for line in open(sys.argv[1]):
    event = json.loads(line)
    if event.get("taskId") != sys.argv[2]:
        continue
    entry = event.get("entry", {})
    frame = entry.get("workerFrame") or entry.get("coordinatorFrame") or {}
    if bad.intersection(frame):
        sys.exit(0)
sys.exit(1)
PYEOF
}

say "STEP preflight: tools, launcher, endpoint $ENDPOINT"
command -v curl >/dev/null || fail "curl is required"
command -v python3 >/dev/null || fail "python3 is required"
command -v git >/dev/null || fail "git is required"
if [ ! -x "$LAUNCHER" ]; then
  echo "building the agent host distribution"
  ./gradlew :protomolt-agent-host:installDist -x test --console=plain -q || fail "gradle installDist failed"
fi

# The candidate contract requires a real commit reference. Give both provider
# processes isolated repositories with a valid HEAD instead of relying on a
# model to notice an empty directory and initialize Git itself.
for workspace in "$WORK/workspace-kimi" "$WORK/workspace-codex"; do
  git init -q -b main "$workspace"
  git -C "$workspace" -c user.name="ProtoMolt Live Acceptance" \
    -c user.email="protomolt-live@localhost" \
    commit --allow-empty -q -m "Initialize live acceptance workspace"
done
mcp_initialize

say "STEP worker-start: $WORKER_ID (kimi)"
WORKER_PID="$(start_agent worker "$WORK/logs/worker.log" \
  --endpoint "$ENDPOINT" \
  --role worker \
  --identity "$WORKER_ID" \
  --provider kimi \
  --workspace "$WORK/workspace-kimi" \
  --state "$WORK/state/kimi-worker.json" \
  --token-env PROTOMOLT_MCP_TOKEN)"
echo "worker pid $WORKER_PID, log $WORK/logs/worker.log"

deadline=$((SECONDS + 180))
until mcp_call delegation-worker-list '{}' 2>/dev/null | grep -q "\"$WORKER_ID\""; do
  [ $SECONDS -lt $deadline ] || { tail -20 "$WORK/logs/worker.log" >&2; fail "worker did not register within 180s"; }
  kill -0 "$WORKER_PID" 2>/dev/null || { tail -20 "$WORK/logs/worker.log" >&2; fail "worker process died before registering"; }
  sleep 3
done
echo "STEP worker-register OK: $WORKER_ID is registered"

python3 -c 'import json,sys; s=json.load(open(sys.argv[1])); print("worker state cursor=%s providerSession=%s" % (s.get("cursor", 0), s.get("providerSessionId", "")))' \
  "$WORK/state/kimi-worker.json" || fail "worker state file missing after registration"

# Snapshot the feed tail before starting the coordinator. A live server may
# already contain many completed tasks; beginning at zero would let historical
# offers and terminal frames impersonate this run.
CURSOR=0
while :; do
  baseline="$WORK/baseline.json"
  mcp_call delegation-watch "{\"afterCursor\": $CURSOR, \"timeoutMs\": 0, \"maxEvents\": 256}" > "$baseline" \
    || fail "could not establish the delegation-watch baseline"
  CURSOR="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("cursor", sys.argv[2]))' "$baseline" "$CURSOR")"
  truncated="$(python3 -c 'import json,sys; print(str(json.load(open(sys.argv[1])).get("truncated", False)).lower())' "$baseline")"
  [ "$truncated" = "true" ] || break
done
echo "STEP watch-baseline OK: cursor=$CURSOR"

say "STEP coordinator-start: $COORDINATOR_ID (codex) with bounded bootstrap objective"
cat > "$WORK/objective.txt" <<EOF
Offer exactly one bounded task to the worker named $WORKER_ID. Choose any task
uuid. The task must have two stages. In stage one the worker accepts the offer,
sends progress and one resumable checkpoint, then stops without creating the
deliverable or submitting a completion candidate. Stage two begins only after
the worker receives coordinator guidance whose exact text is
continue-after-live-restart. It then creates SUMMARY.md with three lines in its
workspace, runs the single required check named live-check that verifies the
file exists and is non-empty, commits it, and submits a completion candidate
with passing live-check evidence and the commit reference. Put these stage
rules in the task objective and require only live-check. When the checkpoint
arrives, use host-ack and do not send guidance; the acceptance harness sends
it after restarting the worker. When the candidate arrives, request a revision
if evidence or the commit reference is missing, otherwise accept the task.
EOF
COORDINATOR_PID="$(start_agent coordinator "$WORK/logs/coordinator.log" \
  --endpoint "$ENDPOINT" \
  --role coordinator \
  --identity "$COORDINATOR_ID" \
  --provider codex \
  --workspace "$WORK/workspace-codex" \
  --state "$WORK/state/codex-coordinator.json" \
  --bootstrap "$WORK/objective.txt" \
  --token-env PROTOMOLT_MCP_TOKEN)"
echo "coordinator pid $COORDINATOR_PID, log $WORK/logs/coordinator.log"

say "STEP watch: delegation-watch from cursor $CURSOR until the task is accepted (budget ${BUDGET}s)"
TASK_ID=""
RESTARTED=""
PRE_RESTART_SESSION=""
PRE_RESTART_CURSOR=""
end=$((SECONDS + BUDGET))
while [ $SECONDS -lt $end ]; do
  batch="$WORK/watch-batch.json"
  mcp_call delegation-watch "{\"afterCursor\": $CURSOR, \"timeoutMs\": 25000, \"maxEvents\": 64}" > "$batch" \
    || fail "delegation-watch call failed"
  python3 - "$batch" "$EVENTS_JSONL" <<'PYEOF'
import json, sys
batch = json.load(open(sys.argv[1]))
events = batch.get("events", [])
with open(sys.argv[2], "a") as out:
    for event in events:
        out.write(json.dumps(event) + "\n")
        entry = event.get("entry", {})
        frame = entry.get("workerFrame") or entry.get("coordinatorFrame") or {}
        kinds = [k for k in frame if k not in ("frameId", "taskId", "seq", "sentAt")]
        print("EVENT cursor=%s lane=%s kind=%s task=%s" % (
            event.get("cursor"), event.get("lane", "?"),
            kinds[0] if kinds else "?", frame.get("taskId", "")))
PYEOF
  CURSOR="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("cursor", 0))' "$batch")"
  # The first new offer addressed to this unique worker fixes the task id.
  if [ -z "$TASK_ID" ]; then
    TASK_ID="$(python3 -c '
import json, sys
for line in open(sys.argv[1]):
    event = json.loads(line)
    frame = (event.get("entry", {}).get("coordinatorFrame") or {})
    if event.get("workerId") == sys.argv[2] and "offer" in frame:
        print(event.get("taskId", ""))
        break' "$EVENTS_JSONL" "$WORKER_ID")"
    [ -n "$TASK_ID" ] && echo "STEP task-offered OK: taskId=$TASK_ID"
  fi
  # First checkpoint with no restart yet: bounce the worker mid-task.
  if [ -z "$RESTARTED" ] && task_has_kind checkpoint; then
    task_has_kind completion \
      && fail "worker submitted completion before the restart guidance gate"
    say "STEP worker-restart: checkpoint observed, restarting $WORKER_ID"
    PRE_RESTART_SESSION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("providerSessionId", ""))' "$WORK/state/kimi-worker.json")"
    PRE_RESTART_CURSOR="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("cursor", 0))' "$WORK/state/kimi-worker.json")"
    echo "pre-restart state cursor=$PRE_RESTART_CURSOR providerSession=$PRE_RESTART_SESSION"
    kill "$WORKER_PID" 2>/dev/null || true
    for _ in $(seq 1 15); do
      kill -0 "$WORKER_PID" 2>/dev/null || break
      sleep 1
    done
    # A replacement registration while the server still holds the old stream
    # fails fast, so give the bridge a moment to notice the dead stream.
    sleep 5
    WORKER_PID="$(start_agent worker "$WORK/logs/worker-restarted.log" \
      --endpoint "$ENDPOINT" \
      --role worker \
      --identity "$WORKER_ID" \
      --provider kimi \
      --workspace "$WORK/workspace-kimi" \
      --state "$WORK/state/kimi-worker.json" \
      --token-env PROTOMOLT_MCP_TOKEN)"
    echo "worker restarted, pid $WORKER_PID, log $WORK/logs/worker-restarted.log"
    deadline=$((SECONDS + 180))
    until mcp_call delegation-worker-list '{}' 2>/dev/null | python3 -c '
import json, sys
workers = json.load(sys.stdin).get("workers", [])
sys.exit(0 if any(w.get("workerId") == sys.argv[1] and w.get("connected")
                  for w in workers) else 1)' "$WORKER_ID"; do
      [ $SECONDS -lt $deadline ] || { tail -20 "$WORK/logs/worker-restarted.log" >&2; fail "replacement worker did not reconnect within 180s"; }
      kill -0 "$WORKER_PID" 2>/dev/null || { tail -20 "$WORK/logs/worker-restarted.log" >&2; fail "replacement worker died before reconnecting"; }
      sleep 3
    done
    echo "STEP worker-reregister OK: $WORKER_ID is connected"
    mcp_call delegation-message \
      "{\"taskId\": \"$TASK_ID\", \"sender\": \"coordinator\", \"recipient\": \"$WORKER_ID\", \"kind\": \"TASK_MESSAGE_KIND_GUIDANCE\", \"text\": \"continue-after-live-restart\"}" \
      > "$WORK/logs/restart-guidance.json" \
      || fail "could not send the post-restart guidance"
    echo "STEP restart-guidance OK: continue-after-live-restart"
    RESTARTED=1
  fi
  # Terminal: the coordinator accepted the candidate.
  if task_has_kind accepted; then
    echo "STEP task-accepted OK"
    break
  fi
  # Loud terminal failures.
  if task_has_failed_terminal; then
    tail -20 "$WORK/logs/worker.log" "$WORK/logs/coordinator.log" >&2 || true
    fail "task reached a failed terminal state; see $EVENTS_JSONL"
  fi
done
task_has_kind accepted || fail "task was not accepted within ${BUDGET}s"
[ -n "$TASK_ID" ] || fail "no task offer was ever observed"
[ -n "$RESTARTED" ] || fail "no checkpoint was observed, so the restart path went unproven"

# Completion after reconnect proves the replacement provider actually ran a
# turn. Read the state only now, after that resumed work, so the comparison is
# not merely observing the unchanged file immediately after process start.
POST_RESTART_SESSION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("providerSessionId", ""))' "$WORK/state/kimi-worker.json")"
POST_RESTART_CURSOR="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("cursor", 0))' "$WORK/state/kimi-worker.json")"
[ -n "$PRE_RESTART_SESSION" ] || fail "the worker recorded no provider session before restart"
[ "$POST_RESTART_SESSION" = "$PRE_RESTART_SESSION" ] \
  || fail "provider session changed across restart: $PRE_RESTART_SESSION -> $POST_RESTART_SESSION"
[ "$POST_RESTART_CURSOR" -gt "$PRE_RESTART_CURSOR" ] \
  || fail "cursor did not advance after restart: $PRE_RESTART_CURSOR -> $POST_RESTART_CURSOR"
echo "PROOF resume: provider session $POST_RESTART_SESSION kept, cursor advanced $PRE_RESTART_CURSOR -> $POST_RESTART_CURSOR"

say "STEP transcript: delegation-transcript for $TASK_ID"
transcript="$WORK/transcript.json"
mcp_call delegation-transcript "{\"taskId\": \"$TASK_ID\", \"maxEntries\": 256}" > "$transcript" \
  || fail "delegation-transcript call failed"
python3 - "$transcript" <<'PYEOF'
import json, sys
events = json.load(open(sys.argv[1])).get("events", [])
kinds, frame_ids, cursors = [], [], []
for event in events:
    entry = event.get("entry", {})
    frame = entry.get("workerFrame") or entry.get("coordinatorFrame") or {}
    kinds += [k for k in frame if k not in ("frameId", "taskId", "seq", "sentAt")]
    if frame.get("frameId"):
        frame_ids.append(frame["frameId"])
    cursors.append(event.get("cursor", 0))
required = {"offer", "accept", "progress", "checkpoint", "taskMessage",
            "completion", "accepted"}
missing = required.difference(kinds)
if missing:
    print("missing lifecycle frames: " + ", ".join(sorted(missing)), file=sys.stderr)
    sys.exit(1)
if len(frame_ids) != len(set(frame_ids)):
    print("duplicate frame ids in the transcript", file=sys.stderr)
    sys.exit(1)
if cursors != sorted(cursors) or len(cursors) != len(set(cursors)):
    print("transcript cursors are not strictly increasing", file=sys.stderr)
    sys.exit(1)
if not (kinds.index("checkpoint") < kinds.index("taskMessage")
        < kinds.index("completion")):
    print("restart guidance was not between checkpoint and completion",
          file=sys.stderr)
    sys.exit(1)
print("PROOF transcript: %d frames, kinds %s, no duplicate frame ids, cursors ordered" % (
    len(events), ",".join(dict.fromkeys(kinds))))
PYEOF

say "STEP transcript-resource: protomolt://delegation/tasks/$TASK_ID/transcript"
mcp_read "protomolt://delegation/tasks/$TASK_ID/transcript" | grep -q "$TASK_ID" \
  || fail "the transcript resource did not answer for $TASK_ID"
echo "PROOF resource: protomolt://delegation/tasks/$TASK_ID/transcript answers over resources/read"

say "STEP watch-continuity: the script's own cursor saw every event exactly once"
python3 - "$EVENTS_JSONL" <<'PYEOF'
import json, sys
cursors = [json.loads(line).get("cursor", 0) for line in open(sys.argv[1])]
if cursors != sorted(cursors) or len(cursors) != len(set(cursors)):
    print("watch cursors regressed or duplicated across the worker restart", file=sys.stderr)
    sys.exit(1)
print("PROOF watch: %d events, strictly increasing cursors across the restart, none lost or duplicated" % len(cursors))
PYEOF

: > "$WORK/.passed"
say "PASS: offer, accept, progress, checkpoint, candidate, review, accepted; worker restarted across its checkpoint; cursor resume and provider-session recovery proven"
