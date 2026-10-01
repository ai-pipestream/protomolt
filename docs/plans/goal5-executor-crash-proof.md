# Installed workflow executor interruption

`AuthoringWorkflowWorkerCrashProcessTest` qualifies a different process from the
author recovery test. The installed coordinator JVM hosts `WorkflowRunWorker`.
The test obtains a durably accepted authored workflow, prepares a distinct input,
and launches its exact executable source through the authenticated launch RPC.

A test-owned PostgreSQL advisory-lock trigger blocks the write step's checkpoint
transaction. The test observes the durable external fixture record and the
blocked database session before forcibly terminating the executor JVM. It then
verifies that only the earlier normalize checkpoint and its events committed.

The fixture process is also forcibly stopped and restarted with the same record
directory. After explicitly expiring the dead claim in PostgreSQL, the coordinator
restarts against the same stores. Production lease recovery runs the job on
attempt 2. The test asserts the same accepted source, unchanged normalize
checkpoint, completed write checkpoint, expected event attempts, completed job,
and byte-identical persisted fixture record.

Lease eligibility is accelerated by test SQL; this is not evidence of waiting
through the default five-minute lease. The fixture provides durable idempotency;
this does not establish exactly-once effects for arbitrary remote services.

Local validation on 2026-10-01: one test, zero failures and zero skips, using
`./gradlew :samples:test --tests '*AuthoringWorkflowWorkerCrashProcessTest'`.
Log: `/tmp/goal5-workflow-executor-crash.log`. Hosted CI, merge, published images,
browser qualification, Kafka launch, and native platform checks remain separate.
