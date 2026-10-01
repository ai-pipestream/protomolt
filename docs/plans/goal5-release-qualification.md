# Goal 5 release qualification

The scripted workflow authoring starter passed native AMD64 and ARM64
qualification on 2026-10-01. It supports browser task creation, independent
verification, accepted-workflow launch and durable asynchronous execution.
The author is scripted. This release does not establish live-model authoring
quality, exactly-once remote effects, or a NAS deployment.

## Source and publication

- Source: `d209f0551566dbd121fabe434631301a646fb0f5`.
- Tag on Forgejo and GitHub: `authoring-starter-d209f0551566`.
- [Prerelease bundle](https://github.com/ai-pipestream/protomolt/releases/tag/authoring-starter-d209f0551566).
- [Publication and qualification run](https://github.com/ai-pipestream/protomolt/actions/runs/36904344137): all jobs passed.
- [Implementation CI](https://github.com/ai-pipestream/protomolt/actions/runs/36900263020) and
  [browser readiness fix CI](https://github.com/ai-pipestream/protomolt/actions/runs/36902733425): all checks passed before merge.
- PRs #387 and #388 merged with trees equal to their tested heads. Both main
  remotes were checked at the release source. Later documentation commits do
  not change the immutable release source.

Bundle SHA-256:
`b6e74da387095ab2b9626598b73330eb9830a07caf57492b96d3063987d9d7a8`.
The bundle includes `SOURCE.json`, the Compose file, browser check and `.env`.
The source record names all seven image digests and the Compose/browser hashes.
Authoring index: `sha256:47daac38428b7028cc7240ca9b7b718f8021abe5fbb2440e7ca02077cd96d7eb`.
Repository index: `sha256:5609827d930b7b17b378ac178fb5ca1ae6030d0770f475f703f8411a9e18a552`.
The manifest job compared each child digest with the qualified native receipt.

## Acceptance evidence

Both native jobs built installed launchers and images, verified revision labels,
and exercised the browser with the published digests. Each also passed
`AuthoringComposeReviewTest` and `AuthoringComposeCrashTest`: one test per class,
zero failures and zero skips on each architecture. The review test verifies an
infrastructure failure, explicit retry, acceptance, invalid deliverables and stale
review identity. The crash test kills the coordinator after a durable remote
write, restarts the fixture, and checks recovery without duplicate effects.

Separate fresh native runners downloaded the public ZIP without authentication,
checked the checksum and source metadata, pulled the pinned images anonymously,
and completed browser authoring and launch. Their browser results report
`authoring: accepted`, `job: completed`, `provider: scripted`, `liveModel: false`.
Artifacts named `authoring-public-amd64-*` and `authoring-public-arm64-*` retain
results and screenshots in the linked run. A local anonymous download also
passed the published checksum check.

The original named requirements have these executable checks:

- `artifact_and_inline_workflow_match`: `WorkflowAuthoringPreflight` and reviewer
  tests compare stored artifacts with the compiled source and inline workflow.
- `required_checks_independently_verified`: `WorkflowAuthoringReviewerTest`
  reruns fixtures and rejects forged reports and changed expected results.
- `receipt_names_same_run`: reviewer tests reject wrong receipts and altered or
  missing authoritative run evidence.
- `job_payload_reuse_conflicts`: `JdbcWorkflowRunStoreIT` checks matching retries,
  changed submissions and concurrent conflicting submissions with one winner.
- `external_completion_reuse_conflicts`: the same PostgreSQL suite checks
  competing responses, single events and delayed validation rejection.
- `restart_preserves_completed_steps`: installed-process and packaged crash tests
  check persisted checkpoints and stable remote operation keys after restart.

`AcceptedWorkflowKafkaBridgeTest` covers accepted source/input/job identity,
duplicates, conflicts and mutable resolver changes through a real broker.
`AuthoringRemoteProcessTest` exercises the installed coordinator. These local
checks passed without skips before publication. The jobs suite passed 122 tests;
the final authoring suite passed 192, plus the worker discovery recovery process
test. Buf lint and compatibility passed. Contract tests use the native validator;
state and evidence checks remain handler obligations.

## Qualification repairs

A deterministic test exposed RPC cancellation turning reserved preparation into
terminal failed evidence. The fix rejects cancelled requests before reservation
and detaches execution after reservation, while keeping workflow budgets and
coordinator task checks. Regression tests failed before the fix and passed after.
See [preparation semantics](goal5-preparation.md).

The first native publication attempt failed on AMD64 when Chromium exposed its
debug endpoint before a page existed. PR #388 made both browser scripts wait for
a usable page. The new source tag above passed a complete fresh run. No release
bundle was published from the earlier partial attempt.

## Use and limits

Follow [the starter instructions](../../deploy/authoring/README.md) from the
extracted release directory. Compose pulls images; users need no host JDK or
local image build. The browser token has the launch scope; the scripted author
cannot approve or launch its own candidate. Kafka remains an optional bridge
with separate credentials and topic setup. Job cancellation is unsupported.

Publication is complete; NAS deployment and live-provider qualification are
separate work. Historical planning notes describe earlier gaps and should be
read with this dated release record.
