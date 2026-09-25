# Goal 4 handoff — 2026-09-25

Read this before continuing. The user has about 3% Codex allowance remaining
until tomorrow and requested a durable stopping point and possible Kimi help.
Goal 4 is **active and incomplete**, not blocked by an external dependency.
Do not publish, merge, or declare completion from this checkpoint alone.

## Exact checkout and landed prerequisite

- Checkout: `/work/worktrees/protomolt/goal4-starter`
- Branch: `feat/goal4-starter`
- Base: `55caed3402335f7468ab4731bb79cf7b0349757a`
- PR #322, revision/attempt protection, is merged; its four hosted checks passed.
  <https://github.com/ai-pipestream/protomolt/pull/322>
- Goal 4 commits and checkpoint are local. No Goal 4 PR, push, release, or NAS
  promotion has happened. Recheck remotes and live main before publication.
- Push Forgejo `origin` first, GitHub `github` second. GitHub CI is the build of
  record; merge only green, then sync Forgejo main. Read root `AGENTS.md`.

The completion plan is `docs/plans/goal4-coordination.md`. Earlier goals and
scope boundaries are in `/work/website/protomolt/.research/adoption-goals.md`.
Goal 5 workflow authoring and Goal 7 Jev integration remain separate.

## Implemented in this branch

- Existing rejection frame exposed as a validated RPC/catalog action; AgentHost
  must accept or reject each offer for the correct attempt.
- Explicit fixture provider uses real AgentHost/MCP/cursor paths, validates its
  supported CoordinationReport, stores actual protobuf artifact bytes, answers
  questions and revisions, and rejects unsupported contracts.
- Caller-defined Any types resolve per task/attempt, including conflicting
  same-name descriptors and nested schema definitions. Historical events retain
  their original contract interpretation.
- Browser sample/custom descriptor offers, typed result display, revision-bound
  review, cancellation/reoffer recovery, and protection against stale async
  selection responses. Timeline/cancellation controls are collapsed in source.
- Persistent signing identity and authenticated all-artifact ZIP export.
- Correction duplicate-run race fixed, with active/durable duplicate tests.
- Native gRPC mount of existing contributed DelegationService contracts through
  the existing catalog/auth interceptor. Shared REST contract matching; native
  contributed successful responses are validated. No new lifecycle service.
- Remote ACP `delegation/<RpcName>` commands, same credentials/deadlines, typed
  candidate parsing and historical task/attempt descriptor recovery after
  adapter restart. Auth errors stay redacted; old commands remain available.
- Image-only `deploy/agents` Compose, optional Kimi and ACP, offline verifier
  helper, browser/protocol/restart qualification scripts, provider evidence.
- Separate manual agents-starter publication workflow for four native images
  and an immutable ZIP, with pinned application/external image digests and
  actual anonymous downloaded-package qualification on AMD64 and ARM64.

## Verified evidence and important limits

- Full `./gradlew build` passed before the final native/ACP binding additions:
  `/tmp/goal4-full-build-final.log` (1493 tasks, 25 seconds).
- Latest native binding tests passed:
  `./gradlew :protomolt-grpc-service:test :protomolt-serve:test --tests '*DelegationGrpcMountTest' --tests '*ProtoMoltRestMountTest' --tests '*ProtoMoltGrpcServerTest'`
  Log: `/tmp/goal4-native-test.log`.
- Latest `:protomolt-acp-agent:test --tests ai.protomolt.proto.acp.agent.RemoteDelegationLineRunnerTest`
  passed 3 tests: different contracts with the same type name across attempts,
  reconnect/history/watch decoding, invalid candidate with no mutation, and
  wrong-token typed submission. Runtime XML is under surface/acp/build.
- Console previously passed typecheck, all 184 tests, and production build.
- Buf build, lint, browser descriptor drift check, and breaking check against
  base passed. Workflow lint binary: `/tmp/protomolt-tools/actionlint`.
  Compose statics and shell/Node syntax passed.
- Local stack: `/tmp/protomolt-goal4-local`; project `protomolt-goal4-local`;
  HTTP 29832, gRPC 29833. It is healthy but uses **older development images**.
  Final native gRPC/ACP smoke has NOT run against rebuilt images.
- Fixture task `68cd117a-7581-4464-98c6-cbdf2160caec` accepted revision 2;
  stale review refused without mutation; restart preserved acceptance and trust;
  post-restart question got a correlated answer and a fresh record.
- Real Chrome created and accepted a fixture task, uploaded custom descriptor,
  displayed missing-type error, and observed unsupported-contract rejection.
  `/tmp/goal4-browser-upload.png` shows the older image UI; refresh after build.
- Offline signed export and both artifacts verified with networking disabled;
  changed artifact returned exit 1 and `FAILED artifact-rehash`/`REFUSED`.
- One live Kimi CLI 2.1.1 task was independently inspected and accepted:
  `eb99ba3c-335f-4a2f-95e4-453e29a834b5`, attempt 1/revision 1.
  `/tmp/goal4-kimi-accepted.zip` verified offline; details and image IDs are in
  `deploy/agents/PROVIDERS.md`. This is a bounded report, not coding proof.
- Kimi originally wrote group 1000, while serve's image default group was 999.
  Source Compose now explicitly shares primary group 10001; host UID and host
  supplementary group preserve CLI config access. Existing artifact group was
  repaired without changing bytes before review. Require group-readable report
  files; temporary 0600 files remain unreadable until the worker changes mode.
- Publication workflow has only static checks so far. No hosted native release
  job, anonymous image pull, or downloaded release qualification has run.

## Next actions, in order

1. Review the local checkpoint diff, especially native/ACP bindings and release
   gates. Kimi may perform a bounded read-only review; its findings need review.
2. Run final full build and ACP protocol lane (excluded from default build):
   `./gradlew build :protomolt-acp-agent:acpProtocolTest`.
   Keep Gradle sequential. No Gradle process remained at handoff.
3. Build fresh distributions with current console assets:
   `./gradlew :protomolt-serve:installDist :protomolt-repo-service:installDist :protomolt-agent-host:installDist :protomolt-acp-agent:installDist :protomolt-record-verifier:jar`.
   Build serve, repo/service, thin AgentHost (`Dockerfile.starter`) and surface/acp
   images. Refresh only the disposable local starter, preserving its volumes.
4. Copy current Compose/scripts/assets into the local starter and set its four
   application image overrides. Run `.github/scripts/agents-starter-qualify.sh`
   there with `PROTOMOLT_HTTP_BASE=http://127.0.0.1:29832`,
   `PROTOMOLT_GRPC_TARGET=127.0.0.1:29833`, `CHROME_BIN` pointing to Chrome.
   Include `record-verifier.jar`, `custom-report.binpb`, and browser smoke.
   The qualifier must prove native delegation and ACP, not only gRPC validation.
5. Fix/review failures; checkpoint only reviewed fixes. Push both remotes in
   order, create/attach PR, verify exact-head hosted CI, merge green, sync origin.
6. Create immutable `agents-starter-<12sha>` tag at the merged main SHA on both
   remotes, then dispatch `Agents Starter Publish` from main. Inspect actual
   native jobs and saved qualification JSON/screenshots. Do not overwrite assets.
7. Audit every item in the completion plan against current release evidence.
   Only then mark Goal 4 complete. NAS and website are not modified by this
   checkpoint; unfinished capabilities must not be advertised as released.

## Operational boundaries

Never print, commit, or copy provider/operator/console credentials into reports.
Kimi authentication stays in its existing runtime mount. No Docker socket in a
worker. Worker-coordinate includes review authority and is a trusted-operator
boundary, not hostile-client isolation. Invalid candidates are rejected before
transcript append/review; earlier claims otherwise were investigated and
retracted. A signed receipt proves integrity, not semantic correctness.

Root reviews/owns the design; Sol supplied bounded implementation, Luna tests.
All Codex subagents stopped at this checkpoint. Do not rely on old agent
activity or files as proof a background process is still running.

## Separate Kimi review

Checkpoint commit: `5afb985e4917745e853b5f38a3be46eb964a7f42`.
A separate Kimi CLI review is limited by its prompt to reading source
and reporting defects, with a 45-minute timeout. It cannot establish release
qualification and has not been authorized to edit, build, or deploy.

Runner and exact prompt: `/work/worktrees/protomolt/goal4-kimi-review/run.sh`
and `prompt.txt`. Output: `review.txt`; process diagnostics: `stderr.log`;
completion: `exit-code.txt` and `finished.txt` in that directory. Inspect process
state as well as these files before assuming it is still running or starting a
replacement. The Codex goal remains active unless the user explicitly requests
pausing it; asking about allowance alone does not change that status.
The initial invocation refused `--prompt` together with `--plan`; the corrected
runner uses prompt mode with explicit read-only scope. The user-systemd unit
`protomolt-goal4-kimi-review` owns the detached review, when successfully started.

## Checkpoint landing follow-up

The user subsequently authorized committing, pushing, merging to main and tagging
this moment. Final `./gradlew build :protomolt-acp-agent:acpProtocolTest` passed
(1494 tasks, 42 seconds; `/tmp/goal4-checkpoint-full-build-fixed.log`). This caught
and fixed a reflected self-profile restart regression: remote profile proxies
are excluded from local contributed RPC binding. The restart integration test
now invokes native RegisterWorker after the original endpoint has stopped.
That closes step 2 above for this source state; rebuilt-image/release gates
remain outstanding. Recheck the actual PR/remotes/tag before repeating landing.
