# Goal 6 first-run observations

## Automated rehearsal, 2026-10-01

The published `authoring-starter-d209f0551566` bundle was downloaded anonymously
into a new temporary directory and checked against its published SHA-256 file.
Compose started under a new project, `protomolt-goal6-first-run`, with fresh
volumes and loopback ports 18540 and 19540. The host already had Docker,
Compose 5.5.1, Node, Chromium, and an image cache. This is not a cold-machine
or unfamiliar-human setup measurement.

- Download started at 20:02:48 UTC.
- Health was observed UP at 20:03:44 UTC.
- Browser completion was observed at 20:03:53 UTC.
- Total observed elapsed time was 65 seconds, including gaps between commands.
  This is not isolated startup time, workflow latency, or a user completion-time
  benchmark. Those need separately instrumented measurements.
- The release's browser script returned `authoring: accepted`, `job: completed`,
  `provider: scripted`, and `liveModel: false`.
- Operation: `94181e0a-a892-4d68-96db-5a8701d7a72b`.
- Job: `1e9a71a1-3393-48bf-bb39-40a1fac899bd`.
- Visual inspection confirmed accepted state, completed execution, authorization
  and input fingerprints, and three successful contract checks.
- No paid model provider was used. Hosting cost and cold-download bytes were
  not measured. This result cannot support a live-provider cost claim.

Local raw evidence is in `/tmp/protomolt-goal6-first-run.VE8zdc`: checksum,
pull and startup logs, service status, timestamps, browser result, and screenshot
inside the extracted bundle directory. These temporary artifacts are not durable
public evidence. The immutable release and its prior architecture qualification
are linked from [the release report](goal5-release-qualification.md).

## Friction found and current changes

- The README did not direct new users to the qualified bundle. A first-workflow
  tutorial now links the exact assets, checksum, browser steps, expected service
  states, and failure diagnosis. It distinguishes the scripted demonstration from
  arbitrary workflow authoring. The tutorial still needs an unfamiliar reader.
- Two token types exist; only the browser launch token can launch this example.
  The walkthrough now names the correct token and explains the distinction.
- The console exposes a long typed deliverable before the launch controls. This
  is useful inspection data but may make the next action harder to find. No UI
  redesign has been made; evaluate this with the unfamiliar reader.
- Running `./gradlew :samples:runCelMapping` failed with
  `key 'fields' is not present in map`. The example accessed protobuf wrapper
  fields even though CEL adapts Struct/Value to maps/scalars. Changing the sample
  expressions to `input.enabled` and `input.name` made the command succeed and
  produce both copied and selected `Ada` values. Production CEL behavior was
  unchanged. The mapping guide now gives the command and expected result.

## Published library consumer rehearsal

The independent build under `examples/protobuf-toolkit` ran locally, including
after its build files and sources were copied outside the checkout. Both runs
passed mapping, selector, projection, validation, registry compatibility, and
OpenAPI checks. The final application additionally checks named validation
violations and the OpenAPI CEL disclosure. These runs do not establish clean
public-consumer resolution: the host has Gradle initialization scripts that
rewrite repositories and prepend mavenLocal, discovered during CI investigation.

The first dependency resolution used stale cached snapshots containing the old
`ai.pipestream.proto` package names. The current published mapper artifact was
inspected and contained `ai.protomolt.proto` classes. `--refresh-dependencies`
resolved the mismatch and the consumer compiled and ran. This limitation is
documented. The exercise proves local application behavior, not public artifact
provenance, an immutable library release, or cold dependency-cache timing.

The first hosted consumer check failed because `git.rokkon.com` does not resolve
from GitHub's runner. It resolves to a private address on the developer host.
The example is being switched to Central's public snapshot repository, whose
existing snapshot was stale. Current main has a green full CI run; its existing
manual snapshot publication workflow is being used to publish current libraries.
A clean Gradle-home run and the hosted consumer job must pass before describing
the standalone consumer as verified for external users.

Logs: `/tmp/protomolt-goal6-toolkit-consumer.log` (initial failure),
`/tmp/protomolt-goal6-toolkit-refresh.log`,
`/tmp/protomolt-goal6-toolkit-isolated.log`, and
`/tmp/protomolt-goal6-toolkit-final.log` (successful runs).

## MCP and remote ACP rehearsal

`scripts/service-workspace-smoke.mjs` passed against the published authoring
stack. It registered and inspected the separate fixture service through MCP,
invoked NormalizeText successfully, and refused an empty text request. It then
started the locally built ACP adapter in remote mode, inspected the same profile
and descriptor fingerprint, and verified valid and invalid invocations.

Profile: `tutorial-dc0d3d37-b7cd-4316-a892-b6212de9b588`.
Descriptor fingerprint:
`ebccb5035e4a8f3281bfbf5c8382a91b5417cd0ec514436803ccc4990e9fd972`.
Raw log: `/tmp/protomolt-goal6-service-smoke.log`.
The adapter uses baseline production code; only the acceptance client and guide
are new. This proves the included unary service path, not an arbitrary user's
endpoint, a schema-change scenario, or all streaming modes.

The fixture container was then stopped in this disposable project. A valid
ServiceInvoke request returned HTTP 200 with `ok: false` and `status: UNAVAILABLE`.
The fixture was restarted immediately afterward. Raw result:
`/tmp/protomolt-goal6-first-run.VE8zdc/unreachable-result.json`. This reinforces
why callers must inspect the operation result, not just the HTTP status.

The user has offered to arrange an unfamiliar tester. The
[tester checklist](../tutorials/adoption-checklist.md) records time, cached
prerequisites, assistance, and incomplete paths. No human results exist yet.

## Restart, refresh, and correction checks

After stopping and starting the whole first-run Compose project, the task list
retained the exact accepted task, attempt, revision, and review invocation.
The global cursor advanced from 7 to 9; reconnect activity can add events without
changing the accepted task. This check compared task records rather than requiring
a frozen global cursor. Before/after JSON is retained with the first-run artifacts.
It did not inspect the completed job through the browser's saved launch intent.

`ServiceWorkspaceActionsTest` passed 13 tests, with no failures or skips. Its new
schema-change case restarts a reflected endpoint with another service, proves
inspection retains the old contract before refresh, then checks `changed: true`,
a new fingerprint, retention of both artifacts, invocation of the newly discovered
method, and persistence after reopening the profile store. This is an in-process
test, not a user's network endpoint. Log: `/tmp/protomolt-goal6-schema-refresh.log`.

The timing-enabled MCP/ACP rehearsal passed after restart. On this local host,
MCP registration took 168.60 ms; valid/invalid invocation took 56.18/4.47 ms.
ACP valid/invalid invocation took 16.46/6.16 ms. These are single sequential
observations including protocol overhead with different warm-up states, not
evidence that one protocol is faster. No model calls were made. Raw result:
`/tmp/protomolt-goal6-service-timing.log`.

The existing correction command ran in a fresh workspace and returned accepted,
offline-replay=true, and receipt-verified=true. A separate invocation verified the
stored correction policy and evidence using the exported public trust snapshot.
Run: `contact-dc0f8d15-aed3-4629-a677-4cb650abe500`. Logs:
`/tmp/protomolt-goal6-correction.log` and
`/tmp/protomolt-goal6-correction-verification.log`. This uses the deterministic
fixture and demonstration trust, not a live model or independently trusted issuer.

## Remaining evidence

Clean public-library resolution; cold setup timings; independent human walkthrough;
documented-input rehearsal; a user's own external endpoint;
correction/coordination/integration latency and outcome measurements; optional
Kafka/connector walkthroughs; durable publication of tutorials and evidence.
Goal 6 is not complete.
