# Earning a signed receipt from a delegated task

You walk one task around the whole delegation loop: start the platform in
containers, register a worker, offer it a task with a contract of done, judge
the evidence it submits, and leave with a [signed work
record](../design/receipts.md) that verifies offline with nothing but a JDK.
That last step is the point — the rest is ordinary coordination.

You need Docker with Compose, a JDK 25 or newer, and `curl`, `jq`, `openssl`,
and [`buf`](https://buf.build) on your `PATH` (`buf` is already a contributor
prerequisite: see [CONTRIBUTING](../../CONTRIBUTING.md)).

## 1. Give the server a signing identity

A server that cannot sign cannot hand over a receipt. Signing is three
environment variables and one file holding a raw 32-byte Ed25519 seed:

```shell
mkdir -p receipts
openssl genpkey -algorithm ed25519 -out receipts/key.pem
openssl pkey -in receipts/key.pem -outform DER | tail -c 32 > receipts/seed.bin
openssl pkey -in receipts/key.pem -pubout -outform DER | tail -c 32 > receipts/public.raw
base64 -w0 receipts/public.raw; echo
```

```
94lJO44hDt1DmRNiI/9iS42XasUtu2c4sMVFd2Ba9WA=
```

Keep that base64 line: it is the public half, and step 9's trust snapshot is
where it goes.

> **Why raw bytes and not PEM?** The receipt layer stores Ed25519 material in
> the encodings the format names: a 32-byte seed, and the 32-byte RFC 8032
> point encoding whose declared rule is exactly 32 bytes. DER wraps each in a
> 48-byte PKCS#8 or 44-byte SPKI envelope, which `tail -c 32` unwraps.

## 2. Start the platform

Add the signing environment, the key mount, and a console login token to the
`serve` service in `docker-compose.yml`:

```yaml
    environment:
      PROTOMOLT_TASK_CONSOLE_TOKEN: "${PROTOMOLT_TASK_CONSOLE_TOKEN}"
      PROTOMOLT_RECEIPT_KEY_FILE: /etc/protomolt/seed.bin
      PROTOMOLT_RECEIPT_KEY_ID: key-delegation-demo
      PROTOMOLT_RECEIPT_ISSUER: records.protomolt.dev
    volumes:
      - ./receipts/seed.bin:/etc/protomolt/seed.bin:ro
```

The image runs as a non-root user (uid 10001) and the seed is mounted
read-only, so make it readable before you build:

```shell
export PROTOMOLT_TASK_CONSOLE_TOKEN="$(openssl rand -base64 32)"
chmod a+r receipts/seed.bin
./gradlew :protomolt-serve:installDist :protomolt-acp-agent:installDist
docker compose build
docker compose up
```

The server starts with `--demo`, and `up` reports it healthy once `/health`
answers — the one endpoint open in every authentication mode:

```shell
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/health
```

```
200
```

`http://localhost:8080` carries the console, the bounded task API, and MCP;
`localhost:9090` carries gRPC; the git-backed registry is on `localhost:8081`.

> **Which port?** Container ports are fixed, host ports are not. If something
> local holds 8080, run
> `PROTOMOLT_HTTP_PORT=38080 PROTOMOLT_GRPC_PORT=39090 PROTOMOLT_REGISTRY_PORT=38081 docker compose up`
> and substitute your port below.

## 3. Log in to the task console

The login token gates the browser boundary. It must be 32 to 1024 characters,
and it is exchanged — never reused — for a random session cookie:

```shell
COOKIE=$(curl -sS -D - -o /dev/null \
  -H 'content-type: application/json' \
  -d "{\"token\":\"$PROTOMOLT_TASK_CONSOLE_TOKEN\"}" \
  http://localhost:8080/api/task-session \
  | grep -i '^set-cookie:' | cut -d' ' -f2 | cut -d';' -f1)
```

The login answers `{"authenticated":true,"loginRequired":true}` and sets
`__Host-protomolt_task_session` as `HttpOnly; Secure; SameSite=Strict`. Every
call below carries it with `-b "$COOKIE"`; without it the task API answers 401.

> **The browser wants HTTPS.** That cookie is `__Host-` scoped and `Secure`, so
> serve the login over HTTPS for a browser to accept it. `curl` does not care,
> which is why this tutorial drives the API — but the same transcript renders
> at `/console/tasks`, where the contract of done and the review controls live.

## 4. Register a worker over MCP

The console is the coordinator's door. The worker's door is MCP, where the
[twelve delegation verbs](../transform/delegation.md#live-mcp-surface) live.
MCP here is stateless streamable HTTP, but it is still a session: initialize,
say you are initialized, then call tools.

```shell
SESSION=$(curl -sS -D - -o /dev/null \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}' \
  http://localhost:8080/mcp | grep -i '^mcp-session-id:' | cut -d' ' -f2 | tr -d '\r')

mcp() {
  curl -sS -H 'content-type: application/json' \
    -H 'accept: application/json, text/event-stream' \
    -H "mcp-session-id: $SESSION" \
    -H 'mcp-protocol-version: 2025-06-18' \
    -d "$1" http://localhost:8080/mcp
}

mcp '{"jsonrpc":"2.0","method":"notifications/initialized"}'
mcp '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"delegation-worker-register","arguments":{"workerId":"kimi-worker","provider":"kimi","capabilities":[{"name":"structured-delegation","description":"Consumes event batches and emits commands"}]}}}' \
  | jq '.result.structuredContent'
```

```json
{ "ok": true, "workerId": "kimi-worker", "admitted": true, "sessionId": "..." }
```

Read `admitted`: registration reaching the coordinator and the worker being
allowed to take work are two different facts. Every tool answer below is read
the same way, out of `.result.structuredContent`.

> **Both headers are mandatory.** A request whose `Accept` omits either
> `application/json` or `text/event-stream` is refused with 406, and every call
> after `initialize` must carry the session id and a matching
> `MCP-Protocol-Version`.

## 5. Offer the task

Offering is the one coordinator move the browser could not make on its own, so
the task API has a route for it: the objective, the acceptance checks that
define done, the scope the worker may touch, and a lease.

```shell
TASK=$(curl -sS -b "$COOKIE" -H 'content-type: application/json' \
  -d '{"workerId":"kimi-worker",
       "objective":"Bound the transcript export route",
       "allowedScopes":["apps/serve"],
       "requiredChecks":[{"name":"unit-tests","description":"focused tests pass"}],
       "leaseMinutes":15}' \
  http://localhost:8080/api/tasks/offer | jq -r .taskId)
```

The route answers 201 with a server-generated task id — identity is never the
caller's to choose. At least one required check is mandatory, and
`leaseMinutes` runs from 1 to 1440, defaulting to 30. That single check,
`unit-tests`, is now the task's contract of done: no candidate exists yet, so
no evidence does either, and the console draws the check **unproven** — the
third state beside passed and failed, never rendered as passing.

> **An unknown worker is a 409, not a 400.** Offering to a worker the
> coordinator has never admitted is a state conflict — there is nobody to offer
> to — rather than a malformed request. Register first.

## 6. The worker accepts and submits a candidate

A worker cannot mark its own task complete. It accepts the lease, then submits
a completion *candidate*: evidence for every required check plus at least one
commit or artifact reference.

```shell
mcp '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"delegation-accept","arguments":{"workerId":"kimi-worker","taskId":"'"$TASK"'","attempt":1}}}'

mcp '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"delegation-candidate","arguments":{"workerId":"kimi-worker","taskId":"'"$TASK"'","candidate":{"attempt":1,"revision":1,"summary":"bounded the route and covered it","evidence":[{"checkName":"unit-tests","verdict":"CHECK_VERDICT_PASSED","detail":"focused suite green","ranAt":"2026-08-12T00:00:00Z"}],"commits":[{"repository":"protomolt","commit":"0123456789abcdef0123456789abcdef01234567","subject":"bound the export route"}]}}}}'
```

The check is no longer unproven. Read the evidence off the transcript:

```shell
curl -sS -b "$COOKIE" "http://localhost:8080/api/tasks/$TASK" \
  | jq '[.events[].entry.workerFrame.completion.evidence[]? | {checkName, verdict}]'
```

```json
[{ "checkName": "unit-tests", "verdict": "CHECK_VERDICT_PASSED" }]
```

Said, not proven. Who decides is the next step.

## 7. Judge the candidate

The coordinator's default policy accepts nothing on its own: it leaves every
candidate pending for an external reviewer, and your console session is that
reviewer. Both decisions demand your words, because a judgement without a
reason is not one the transcript can defend later. Send it back first:

```shell
curl -sS -b "$COOKIE" -H 'content-type: application/json' \
  -d '{"decision":"revise",
       "feedback":"tests cover the happy path only",
       "failedChecks":["unit-tests"]}' \
  "http://localhost:8080/api/tasks/$TASK/review"
```

```json
{ "decision": "revise", "phase": "leased" }
```

The task is back with its worker and your feedback is a recorded protocol fact,
not a chat message. The worker answers with revision 2, and this time you
accept:

```shell
mcp '{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"delegation-candidate","arguments":{"workerId":"kimi-worker","taskId":"'"$TASK"'","candidate":{"attempt":1,"revision":2,"summary":"added the refusal cases","evidence":[{"checkName":"unit-tests","verdict":"CHECK_VERDICT_PASSED","detail":"happy path plus every refusal","ranAt":"2026-08-12T01:00:00Z"}],"commits":[{"repository":"protomolt","commit":"89abcdef0123456789abcdef0123456789abcdef","subject":"cover the refusal cases"}]}}}}'

curl -sS -b "$COOKIE" -H 'content-type: application/json' \
  -d '{"decision":"accept","verdict":"checks green and the diff is scoped"}' \
  "http://localhost:8080/api/tasks/$TASK/review"
```

```json
{ "decision": "accept", "phase": "accepted" }
```

> **Two refusals worth provoking.** `{"decision":"accept"}` with no `verdict`
> answers 400 naming the missing field, and a review sent when no candidate is
> open answers 409 — there is nothing to judge.

## 8. Export the receipt

A terminal task hands over a receipt: the route projects its transcript into a
signed work record under the `delegation-task` subject kind.

```shell
curl -sS -b "$COOKIE" -X POST -d '{}' \
  "http://localhost:8080/api/tasks/$TASK/record" > record.json
jq -r .recordBase64 record.json | base64 -d > record.binpb
```

The answer carries three fields. `recordId` is `record-<task id>`,
`manifestDigest` is the SHA-256 of the manifest bytes as signed — the record's
identity — and `recordBase64` is the whole signed container, which you have
just decoded into `record.binpb`.

> **503 means the server cannot sign.** Without the signing environment the
> route refuses by naming what it needs: `PROTOMOLT_RECEIPT_KEY_FILE`,
> `PROTOMOLT_RECEIPT_KEY_ID`, `PROTOMOLT_RECEIPT_ISSUER`. A task still in
> flight answers 409 saying so — a record claims what a delegation produced,
> and a live task is still producing. Cancelled, failed, and expired tasks do
> project, as partial records carrying the reason.

## 9. Verify it offline

Verification takes two inputs, the record and a trust snapshot, and makes zero
network calls. The snapshot is yours, not the record's: it names the issuers
you trust, their keys, and what each may sign.

```shell
cat > trust.json <<EOF
{"issuers":[{"issuer":"records.protomolt.dev",
             "keys":[{"keyId":"key-delegation-demo",
                      "algorithm":"SIGNATURE_ALGORITHM_ED25519",
                      "publicKey":"$(base64 -w0 receipts/public.raw)",
                      "state":"KEY_STATE_ACTIVE"}],
             "subjectKinds":["delegation-task"]}]}
EOF

buf convert --type ai.protomolt.proto.receipt.v1.TrustSnapshot \
  --from trust.json#format=json --to trust.binpb
```

Now build the external verifier and run it. It shares nothing with the platform
but the wire contract: no protobuf runtime, no platform modules, no
dependencies at all.

```shell
./gradlew :protomolt-record-verifier:jar
java -cp "$(ls apps/record-verifier/build/libs/protomolt-record-verifier-*.jar)" \
  ai.protomolt.receipt.verify.Main record.binpb trust.binpb
```

```
PASSED  container-bounds: 604 bytes, 1 signature(s)
PASSED  manifest-parse: manifest version 1, digest 3f2a...
PASSED  reserialization-equality: canonical
PASSED  key-trusted: 1 key(s) resolved under issuer 'records.protomolt.dev'
PASSED  signature-valid: 1 signature(s) verified
PASSED  issuer-authorized: authorized for 'delegation-task'
PASSED  completeness-consistent: status 1 against policy 'delegation-task-evidence'
SKIPPED  artifact-rehash: no artifact bytes supplied
non-claims: issuer-honesty, trusted-time, world-completeness, execution-correctness, artifact-custody
manifest digest: 3f2a...
VERIFIED
```

The byte count and digest are your record's own; the checks, their order, and
the non-claims are the format's. Exit 0 is verified, 1 is refused, 2 is a usage
or input error — an unparseable trust snapshot is your mistake, never the
record's. Note what is *not* claimed: `artifact-rehash` is skipped rather than
passed because you supplied no artifact bytes (pass a directory of files named
by their SHA-256 digests as a third argument and it runs), and the non-claims
line says plainly that a signature is integrity and attribution, never an
endorsement that the issuer told the truth or that the work was correct.

> **The server can hold the same snapshot.** Point `PROTOMOLT_TRUST_SNAPSHOT`
> at a `.json`, `.binpb`, or `.pb` file and the platform's own
> `verify-work-record` and `evaluate-work-record` verbs default to it. Same
> document, two custody models; the lane is never required to verify.

## What the receipt says

The steps are your own session read back: `offer` with the objective,
`accept-attempt-1`, `candidate-r1` with the worker's summary, `revision-r1`
recorded as a *failed* step carrying your feedback verbatim, `candidate-r2`,
and `accepted` carrying your verdict. The subject binds the task id, the worker
id, and a fingerprint of the offered spec; completeness is `COMPLETE` because
the task was accepted, evaluated against a policy named by id, version, and
digest, so a relying party can tell what "complete" was measured against. The
transcript's own deterministic bytes ride as a content-addressed artifact
reference, so whoever holds only the record still knows which transcript it
attests. The projector adds nothing the transcript did not record: your
judgement is in the receipt because you put it on the transcript.

## The same flow, driven by an agent

Everything here is available to a model against the same server. The worker
side already was; the coordinator side has verbs too — `delegation-offer`,
`delegation-review`, `delegation-watch` for a cursor-resumable long poll over
the event feed, and `delegation-transcript`.

```shell
claude mcp add --transport http protomolt http://localhost:8080/mcp
```

[`protomolt-agent-host`](../apps/agent-host.md) is the shipped shape of it: a
Codex, Kimi, or local OpenAI-compatible model attached to those tools while the
server stays the coordinator and the transcript stays the authority. Whichever
side is a model, the receipt is the same bytes, and it still verifies with
nothing but a JDK.
