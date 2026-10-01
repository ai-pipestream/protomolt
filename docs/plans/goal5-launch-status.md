# Accepted workflow launch status

Status: reviewed contract draft; no handler, browser route or public mount.

## Operations

- Existing: launch intent, keyed authorization ledger, job row and six job states.
- New: GetWorkflowLaunchStatus, scoped to workflow-launch. Its request wraps the
  existing complete launch intent; no launch response or authorization receipt is
  needed to recover from a lost reply.
- Extended after implementation: the browser bridge gets one fixed status route.
  The session keeps worker-coordinate plus workflow-launch. Do not grant
  service-invoke or call get-job with a substituted identity.

Read the keyed authorization first. With no authorization, return not_authorized
without reading a job, even if an unrelated job has that UUID. With authorization,
validate its existing contract and exact saved-intent binding. Changed intent is a
conflict. Reconstruct the expected source/input from trusted authorization and
verify the matching job with the same rules as launch before projecting it.
Corrupt evidence fails closed. A missing job after matching authorization returns
authorized_not_queued and permits the caller to retry the exact launch request.
The status read never performs launch effects or external fixture calls.

The projection contains job identity, status, execution attempt, configured retry
limit and timestamps. It excludes input, output, checkpoints, raw errors and lease
credentials. COMPLETED means execution finished successfully; it does not establish
independent semantic truth. FAILED and DEAD are terminal failures in the current
worker. QUEUED, RUNNING and WAITING remain active. No cancellation API is added.

## Validation and evidence

Native rules require a saved intent, exactly one outcome, a recognized job state,
nonnegative attempts, a positive configured retry limit, timestamps, terminal-only
completion time, and job UUID equality with the saved intent. Generated and dynamic
fixtures must both pass. The handler rejects unknown fields and unsupported rules,
validates requests and successful responses, and caps messages at 4 MiB.

Ledger custody, exact request equality, job source/input binding and the no-job-read
absence rule are handler obligations. The response is an observation: concurrent
launch or job progress can make a later read differ. The browser must bind responses
to the saved request and current task selection before displaying them.

JSON Schema/OpenAPI can describe the wrapper, outcome alternatives, enum and
numeric bounds. CEL requirements use existing runtime-rule metadata and still
require native validation; schema validation cannot prove ledger or job custody.
No generator changes are included.

## Implementation and acceptance backlog

1. Scoped status action and optional gRPC/REST/MCP mount. Prove denied scopes have
   no store reads; absent authorization never reads an unrelated job; altered intent
   conflicts; corrupt authorization or mismatched job is DATA_LOSS; storage outages
   and deadlines remain distinct. Cover all six job states and pending insertion.
2. Fixed authenticated browser route and saved-request status display. Prove local
   pending launch recovery, stale selection rejection, bounded errors, and no broad
   job access. Verify HTTP plus rendered browser behavior against installed services.
3. Worker retry-ceiling regression: the current JDBC lease sweeper requeues expired
   RUNNING rows without checking max_attempts, and claim increments unconditionally.
   Decide and implement crash recovery at the final attempt with PostgreSQL tests.
   The status projection must report historical over-limit rows accurately meanwhile.
4. Timestamp behavior: terminal writes use separate clock_timestamp calls and other
   updates use transaction timestamps. Do not reject stored observations based on
   assumed ordering. If execution requires monotonic timestamps, change the store
   under its own PostgreSQL regression tests; do not hide rows in this projection.

These contracts do not complete Goal 5. Browser authoring entry, real worker crash
proof, Kafka example, image-only Compose and release/platform qualification remain.
