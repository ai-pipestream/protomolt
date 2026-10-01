# Accepted workflow launch over Kafka

## Contract inventory and boundary

- Existing: `WorkflowAuthoringLaunchRequest` binds a caller-selected launch UUID,
  the accepted candidate identity, and a pinned input artifact.
- Existing: `WorkflowAuthoringService.LaunchAcceptedWorkflow` checks the
  `workflow-launch` scope, validates requests and successful responses, resolves
  trusted acceptance/evidence, and persists keyed authorization before submitting
  the exact executable source and input. Its successful response identifies a
  matching job, not completed execution.
- Existing: `WorkflowAuthoringLaunchResult` identifies that job and its
  authorization artifact. The bridge checks native validation, unknown fields,
  and equality of the returned job ID to the request's canonical launch UUID.
  Referenced authorization bytes remain a server-side verification obligation.
- New sample: a Kafka consumer forwards this existing request to the authenticated
  gRPC operation. There is no new protobuf contract or production service mount.

The existing jobs request topic carries `WorkflowRunRequest`, which resolves a
mutable workflow name. It cannot stand in for an accepted launch. Accepted
promotion writes immutable workflow versions, while that named resolver reads
different mutable JSON. The sample therefore uses a separate topic and the
existing accepted-launch RPC, without changing the named jobs consumer.

## Authority and transport

The dedicated topic must be private or protected by broker ACLs. Permission to
write it grants use of the bridge's configured launch service authority. This
does not attest individual producer identity. The bridge uses a credential with
the existing `workflow-launch` scope, supplied at runtime; credentials and raw
payloads must not appear in diagnostics. Remote gRPC connections require TLS;
plaintext is an explicit local development configuration.

Each record value is a bounded serialized `WorkflowAuthoringLaunchRequest`; its
key equals the canonical launch UUID. Unknown fields, malformed messages,
unsupported annotation rules, invalid identities and mismatched keys fail before
the RPC. Successful responses pass the corresponding checks before offset commit.
The server independently enforces contract and evidence checks.

## Replay, errors, and cancellation

Disable auto-commit and process one record at a time. Commit only the handled
partition's next offset after a validated successful RPC. A successful RPC and
offset commit are separate operations. An interrupted reply or failed commit can
replay the same request; the existing keyed authorization and job identity must
prevent duplicate work. Never replace its UUID on retry.

Any invalid record, conflict, denied credential, RPC failure or invalid response
stops processing before the offset commit. A commit failure can have an unknown
outcome; restarting may replay the last launch. Operators must resolve the failure
before restart. Cancellation cannot undo an already persisted launch.
No exactly-once transport or arbitrary external-effect guarantee is claimed.

## Acceptance evidence still required

1. Native valid/invalid request and response tests, including nested unknown
   fields, canonical key binding, size bounds, and no RPC/commit on rejection.
2. Real broker plus authenticated RPC: replay after a lost reply produces one
   authorization and job; changed intent under the same UUID is rejected without
   an offset commit. A denied credential also leaves the offset uncommitted.
3. Resolve and launch an accepted executable source and pinned input. Changing
   mutable named registry JSON must not change the submitted source or input.
4. Restart from committed offsets and test commit/transport failure recovery.
5. Package the sample in the image-only starter and qualify that package on the
   advertised platforms. Local tests alone do not make it publicly available.

This bridge supplements the separate workflow-executor crash test. It does not
replace that test or close the Compose, browser, or platform release gates.

## Local evidence (2026-10-01)

The real-broker adapter suite passed three tests with no skips. Its launch ledger
is explicitly a test fake. Separately, the installed-coordinator test passed with
real Redpanda, a launch-only credential, the production launcher, PostgreSQL, and
durable authorization storage. It checks denied credentials before authorization,
a simulated lost reply after server success, replay from the uncommitted offset,
the same returned job and authorization, changed-intent rejection, and immunity to
mutable named registry rotation. The RPC response loss is injected by the test
after the real server returns; this is not a broker or network fault injection.

Logs: `/tmp/goal5-kafka-real-launch-tests.log` and
`/tmp/goal5-kafka-real-launch-final.log`. The installed bridge launcher builds;
the integration test invokes the bridge class with a real gRPC client rather than
launching that bridge executable. Image publication and Compose qualification are
still outstanding.

After integrating the current author-recovery branch, both installed-coordinator
cases pass with no skips, including independent review failure and retry before
Kafka launch (`/tmp/goal5-kafka-launch-stack-tests.log`).
