# Channel, claim-check, dead-letter, and replay recovery

Status: implemented and locally verified on `descriptor-flow-runtime`; not yet
claimed as merged or released. This follows the cluster-backed processor
directory and builds on the durable flow lifecycle and processor channel.

## Outcome

Every workflow edge will declare its delivery guarantee and storage policy.
Large protobuf payloads may move through digest-verified claim checks without a
second business representation. Typed outcomes, retry schedules, dead-letter
records, idempotent receiver state, descendant settlement, payload retention,
and replay frontiers will survive coordinator, worker, broker, and payload-store
failures.

Accepted work must be neither lost nor committed twice. Poison work must be
inspectable and replayable under its original workflow version and descriptor
identities. Recovery must fail loudly when those identities or bytes are not
available.

## Existing code to extend

- `EntityEnvelope` already carries exactly one inline `Any` payload or typed
  `ClaimCheck`, and binds both forms to exact schema identity, byte length, and
  SHA-256 digest.
- `PayloadResolver` is the current hydration seam. The default is inline-only
  and refuses unresolved claim checks.
- `DurableProcessorChannel` and `FileDurableProcessorChannel` implement
  deterministic delivery ids, lease-fenced claims, retries, completion,
  release, settlement, protobuf WAL recovery, CRC32C, and one-writer fencing.
- `DurableFlowCoordinator` now persists run history and the full pending,
  active, and settlement frontier. `settlement_started` closes cancellation's
  race with descendant commit.
- `HistoryEvent` carries the exact envelope at routing, invocation, output, and
  settlement boundaries. `replay` can start a new run from selected persisted
  routed-message events.
- `repo/service` has content-addressed blob operations and a document-specific
  claim-check implementation. `jobs/service` has transactional outbox and
  dead-letter patterns, but both have product-specific storage models that must
  not become the mesh contract.

## Non-negotiable invariants

1. A channel stores or transports protobuf bytes. It never converts a payload,
   outcome, dead-letter record, or recovery checkpoint through JSON.
2. The envelope's schema reference and payload digest remain authoritative
   across inline/externalized transitions. Hydration verifies type name,
   descriptor fingerprint, length, and digest before parsing.
3. Channel policy is compiled into the immutable workflow plan. Runtime class
   names and mutable operator defaults cannot silently change guarantees.
4. Acceptance means one durable owner has committed the work. Socket receipt,
   broker send initiation, or an object-store upload alone is not acceptance.
5. Retryability is a typed outcome and policy decision, not a boolean guessed
   from exception text.
6. Dead-lettering is a durable terminal routing decision with the original
   envelope, workflow version, processor frontier, outcome, and attempt record.
7. Replay creates a new run and preserves a provenance link. It never mutates
   or erases the source run or dead-letter record.
8. A claim-check payload is reclaimable only after every required descendant
   settles and the retention/hold policy permits deletion.
9. Reconciliation reports evidence before repair. No repair may delete a
   possible live payload merely because another store is temporarily absent.

## Contract changes

### Channel policy

Add a `ChannelPolicy` referenced by each `FlowEdge` and fingerprinted into
`CompiledFlowPlan`. The contract needs:

- delivery mode: bounded memory, local durable WAL, or transactional external;
- overflow action: backpressure, named durable spill target, or refusal;
- inline byte limit and claim-check payload-store profile;
- maximum attempts, retry policy reference, and dead-letter channel reference;
- ordering key and concurrency bound;
- persistence prohibition for deletion-sensitive paths;
- retention and legal-hold policy references;
- acknowledgement point and required completion policy.

Compilation refuses an undeclared durable spill, a memory-only path leading to
a persistence-requiring processor, an unavailable profile, unbounded memory,
or incompatible ordering and concurrency guarantees.

### Typed outcomes and retry advice

Replace `ProcessorFailure.retryable` with a compatible typed model. Reserve the
old field and add:

- `ProcessorOutcomeKind`: success, retryable, permanent, skipped, abandoned,
  cancelled;
- structured cause entries with bounded code, message, processor id, attempt,
  and optional evidence reference;
- `RetryAdvice`: no retry, fixed delay, exponential backoff, retry-after time,
  maximum attempts, and jitter policy;
- `SettlementEffect`: settle, release, dead-letter, compensate, or no effect.

The runtime persists the resolved outcome and advice before releasing or
dead-lettering a claim. Replaying the same completion under the same lease is
idempotent; a different outcome under one completion id is a conflict.

### Payload store

Introduce a transport-neutral `PayloadStore` SPI and protobuf service contract:

- `PutPayload` streams bytes and returns an immutable artifact reference;
- `GetPayload` supports bounded range reads and returns digest/length metadata;
- `AcquireLease` and `ReleaseLease` fence retention ownership;
- `HeadPayload` supports reconciliation without transferring bytes;
- `MarkEligibleForDeletion` and `PurgePayload` are separate, auditable steps.

Adapters may wrap repository blob operations or object storage, but the SPI is
namespace-scoped and does not expose document parts, buckets chosen by request,
credentials, or pre-signed URLs in an `EntityEnvelope`.

### Dead-letter and replay

Add one append-only `DeadLetterRecord` protobuf containing:

- dead-letter id and original delivery/run/invocation ids;
- workflow name, version, plan fingerprint, and deployment revision;
- processor/node/edge frontier;
- exact input envelope or its existing claim check;
- typed outcome, attempt history, first/last failure time, and policy identity;
- source history sequence and replay status;
- namespace, retention, and hold references.

Expose protobuf RPCs to list/read dead letters with cursors, request replay,
cancel a scheduled retry, and acknowledge or retain a resolved record. Replay
delegates to `DurableFlowCoordinator.replay`; it does not implement a second
executor. Missing source version, descriptor, processor contract, or payload
returns a named refusal.

### History and lifecycle

Extend the existing `HistoryEventKind` for staged, spilled, externalized,
hydrated, retry-scheduled, retry-started, dead-lettered, replay-requested,
payload-retained, and payload-purged facts. Keep `FlowHistory` as the only run
history. External channel, payload, and dead-letter stores retain their own
ownership ledgers, linked by stable ids.

Persist expected descendant-set membership and payload leases in
`FlowExecutionCheckpoint`. Each settlement transition removes one descendant
and releases only the leases it proves are no longer reachable.

## Storage adapters

Ship three real channel modes behind one conformance suite:

1. `BoundedMemoryProcessorChannel` for explicitly non-durable work. It has a
   fixed byte and item budget, backpressures, and loses unaccepted work on
   process exit by contract.
2. The existing `FileDurableProcessorChannel`, upgraded to typed outcomes,
   retry schedules, dead letters, and descendant/payload leases.
3. One transactional adapter. Prefer a PostgreSQL outbox first because the
   repository already demonstrates atomic state plus outbox transitions. Kafka
   publication is an asynchronous relay; its offset is not treated as atomic
   with arbitrary processor effects.

All three use the same `ProcessorWork`, completion, outcome, and settlement
messages. The external adapter may use database columns for indexing and
leases, but authoritative bodies remain protobuf binary.

## Landing sequence

### Phase 1: policy and typed outcome

Add contracts, compiler checks, stable refusal codes, and compatibility tests.
Upgrade worker completion/failure and run history before adding another
adapter, so every later path targets one semantic model.

### Phase 2: bounded memory and declared WAL channels

Implement the memory adapter and select memory or WAL from compiled edge
policy. Add byte-accounted backpressure and named spill. Prove that a memory-
only workflow cannot accidentally instantiate a durable path.

### Phase 3: claim-check hydration and retention

Implement `PayloadStore`, externalize above the compiled threshold, and hydrate
through `PayloadResolver`. Acquire payload leases before durable channel
acceptance. Persist lease ownership in the run frontier. Verify bytes before
processor parsing and release leases only through settlement.

### Phase 4: retry and dead-letter ownership

Persist retry schedule and attempt identity in the same channel transition that
releases ownership. At the policy ceiling or on a permanent outcome, append the
dead-letter record before settling the failed delivery. Add per-processor and
workflow-default dead-letter policy resolution at compile time.

### Phase 5: transactional external adapter

Implement state-plus-outbox atomic writes, a bounded relay, idempotent broker
keys, consumer lease fences, and offset/checkpoint recovery. Prove the adapter
against the same channel state-machine fixtures as memory and file WAL.

### Phase 6: recovery center RPCs and reconciliation

Mount cursor reads, retry cancellation, replay request, retention hold, and
report-only reconciliation. Add age-guarded repair only after report fixtures
prove classification under partial outages.

## Crash matrix

Tests must kill or suspend execution on both sides of every boundary:

- before and after inline payload externalization;
- before payload put, after bytes but before metadata, and after digest commit;
- before and after channel acceptance;
- before and after outbox commit and broker publish;
- before claim, after claim, after processor effect, and before completion;
- before and after retry scheduling;
- before and after dead-letter append;
- before and after each descendant settlement;
- before payload eligibility, tombstone, and physical purge;
- before replay-run creation and after creation but before first invocation.

For each point, restart the coordinator and worker and assert one durable owner,
one ordered run history, stable ids, no early payload deletion, and no duplicate
committed effect. Fault fixtures must include unavailable broker, unavailable
payload store, corrupt bytes, stale lease, stale outbox relay, and descriptor or
workflow version drift.

## Required conformance and refusal tests

- memory, WAL, and transactional adapter produce the same delivery state
  transitions and terminal processor outcomes;
- inline and claim-checked executions produce byte-identical processor inputs
  and flow outputs;
- digest, length, schema name, and descriptor fingerprint mismatches are four
  distinct refusals;
- full memory channel backpressures or uses only its named spill target;
- permanent failure dead-letters without retry; retryable failure obeys exact
  advice and the attempts ceiling;
- duplicate enqueue/completion/dead-letter/replay is idempotent only when bytes
  match;
- poison first-hop input cannot loop invisibly;
- cancellation before settlement releases work, while cancellation after the
  persisted settlement boundary is refused;
- replay after redeployment uses the original plan and payload; replay without
  any required identity is refused;
- reconciliation under one unavailable store reports `UNKNOWN`, not orphan;
- retention hold blocks eligibility and purge; expiry alone cannot override an
  unsettled descendant.

## Cost gates

Measure:

- allocation, latency, and throughput by inline payload size;
- the inline-to-claim-check crossover for local and remote processors;
- WAL and database write amplification per history and channel transition;
- broker relay batch size, duplicate rate, and recovery lag;
- payload range-read and full-hydration cost;
- dead-letter listing and history cursor latency at corpus scale;
- reconciliation time and memory by record count;
- cancellation and backpressure reaction time.

Set explicit item, byte, frame, retry, history, and cursor bounds in contracts
and tests. No adapter may hide an unbounded in-memory collection behind a
durable API.

## Done means

The feature is done when the same workflow passes on bounded memory, file WAL,
and one transactional adapter; large payloads survive by verified claim check;
typed retries and dead letters are durable; descendant settlement governs
retention; and a dead-letter or selected history frontier can start a new run
under the original exact identities after full process restart. A document-
specific blob wrapper, boolean retry flag, in-memory dead-letter list, or replay
that invokes another executor is not completion.
