# Durable publication recovery

Status: design requirements; automatic cross-host session recovery is not enabled.
This extends the repository composition goal without changing public contracts.

## Persisted preparation

Before the first uncertain admission or takeover, store a bounded, versioned
private record in shared SQL. Bind it to account, authenticated principal,
operation ID, canonical command bytes and digest. Preserve the owner nonce,
predecessor generation, requested lease, per-upload attempt IDs and lease tokens,
sampled drive/backend identities and placement snapshots. Commit fixed modes
durably before admission or any mode-dependent side effect. Configuration stores
credential references, never credentials. Restrict this capability-bearing record
to the repository's private SQL role; public callers cannot read or restore its
nonce/token values. Structural restoration does not authenticate a caller.
A retry of an uncertain write must discover the same record, not mint identities.

`DocumentPublicationSeeds` now makes the random identities explicit and validates
exact uploading-member coverage and distinct capabilities. Structural restoration
of those seeds grants no execution authority. Durable serialization and session
restoration remain to be implemented.

Persist the recovery-only stage-started marker before assessment CREATE. If a
crash leaves that marker but no discoverable assessment, refuse to stage again;
absence is not proof that no external or committed effect occurred. Do not save
borrowed payload buffers or transient resolver outputs. Before staging, a retry
may require the caller to supply inputs again. After staging, use exact retained
assessment coordinates. Read live leases and terminal receipts from SQL.

## Separate execution authority

An operation owner nonce is not an exclusive coordinator claim. Two replicas
restoring it would both satisfy today's operation fence. Add a per-session claim
with monotonically increasing epoch, private token and database-clock lease.
Acquisition, renewal and transfer use compare-and-set. An uncertain acquisition
acknowledgment is resolved using the original proposed token and epoch.

For registered sessions, every owner, attempt, assessment, decision and publication
mutation must check the current claim in its own SQL transaction after lock waits.
Enforce this at the shared mutation boundary, including direct ledger callers;
a Java flag or a preflight read is insufficient. Retain operation-generation
checks as a separate boundary. An execution claim never extends expired operation
ownership or bypasses explicit generation takeover.

Use short transactions. Establish and test one lock order for claim, operation,
attempt and publication rows before implementation. Do not hold database locks or
connections across provider I/O. Claims are per operation, with no global hot row.
A claim renewal must serialize safely with mutations and transfer.

Provider work already in flight can finish after claim loss. Before enabling live
transfer, establish how immutable attempt objects, provider version identity,
reconciliation and cleanup keep these late effects from being adopted or deleting
live content. SQL fencing alone cannot cancel an external PUT. Recovery must wait
for the declared expiry/quiescence boundary and fence stale completion; it must
not infer host death from a network timeout. This policy is still an implementation
and test prerequisite, not a solved capability.

## Required acceptance cases

- Round-trip private records with complete command/placement identity; reject
  truncation, version mismatch, altered binding and extra/missing capabilities.
- Kill the writer after real assessment COMMIT with its acknowledgment withheld.
  Another process recovers from shared SQL, returns the exact receipt and creates
  no new upload, schema resolution or attempt. No local handoff file is allowed.
- Lose admission, claim-transfer and operation-takeover acknowledgments separately;
  replay uses the original identities and cannot create a second logical result.
- Two live processes restore the same session. Only the current claim may mutate;
  block one behind a row lock, transfer ownership, then prove its write is refused.
- Delay a real provider PUT across claim expiry. Stale completion cannot publish,
  overwrite an adopted identity or cause unsafe cleanup. Reconciliation remains
  possible without treating uncertainty as success.
- Exercise cancellation, principal/ACL revocation, expired operation ownership,
  cleanup failure and terminal replay through the same claim-aware path.
- Measure independent operations with multiple JVMs and RustFS. Record database
  pool sizes and contention; correctness tests do not establish scalability.

## Other goal work remains active

Public materialization contracts, tenant-scoped schema resolution/cache,
restore/pruning, non-S3 durability and bounded hydration remain required alongside
recovery. Historical reads must retain schema assets independently of a live
registry. Optional JCR semantics require their separate capability assessment;
this private recovery journal is not a JCR transaction or workspace design.

## Initial claim primitive (V78 checkpoint)

V78 introduced a private, per-operation SQL claim without publication mutation
enforcement. The V79 section below describes the subsequent enforcement work.
The claim binds the key and command digest; acquisition uses epoch one and exact
retry never renews the lease. Renewal requires the live token/epoch. Transfer
requires an expired predecessor and advances exactly one epoch with a new token;
replaying that transfer returns its original still-live lease. Deletion is refused
until a retention protocol can prevent identity reuse.

The row is locked before database time is checked. A failed transaction-local
`lockLive` marks rollback-only even if its caller catches the exception. Snapshot
isolation is rejected. The predecessor epoch is sufficient for compare-and-set
because the row cannot be deleted/recreated and epochs advance exactly once.
This does not authenticate takeover: the future private session host must authorize
it before using the primitive. Claims left by failed pre-admission preparation are
retained; they are not silently replaced.

V79 adds claim propagation with operation authority, transaction-visible write
stamps and SQL mutation guards. Complete durable session preparation, automatic
session registration and full recovery-path qualification remain unfinished.
No provider transfer policy or hard-crash recovery capability is enabled yet.

## Explicitly claimed operation fences (V79)

The SQL enforcement boundary now exists for operations admitted with an execution
claim. An immutable per-operation scope chooses claimed or unclaimed admission.
Both first inserts compete on the same unique key; an unclaimed winner permanently
refuses later claim registration. This closes the absent-row race without a global
lock. Migration backfills separate V78 claims and operations, but refuses overlap
that would silently adopt a previously unprotected operation.

Every claim INSERT or UPDATE must present an explicit epoch/token pair. A trigger
validates and consumes that pair, clears it before storage, and stamps the actual
transaction ID. Supplying an old stamp, a current transaction ID, an old token or
a no-op renewal cannot create this proof. This protects stale or incorrect SQL
call paths inside the trusted service role; it does not protect against a database
administrator who can read bearer tokens or disable triggers.

Claimed operation admission, renewal and takeover carry a private claim alongside
the operation owner. Fencing locks and stamps the claim before the owner. Owner
writes and the shared dependent mutation guard require a live claim stamp in the
same transaction. Attempt mutation still independently requires the owner stamp.
The deferred success-completion guard rechecks claim liveness at finalization,
preserving its existing owner, member, projection and outbox checks. Explicitly
forcing deferred checks early does not promise wall-clock liveness at the later
COMMIT; the held claim lock prevents a concurrent takeover until transaction end.
Cleanup's preexisting recovery-only proof remains available and grants no write
or publication authority.

Tests use real PostgreSQL locking, migrations, upload admission and SQL publication
fixtures. The publication fixture's observations are synthetic; the separate
production-JAR storage gate covers the existing real-provider path. The original
no-op-stamp and missing-finalization-check failures have red test evidence.

This does not activate claims in `DocumentPublicationSession`, persist complete
session state, or enable failover. Before that activation, every recovery path must
carry the claim, provider effects across transfer must be qualified, and forced
crash tests must pass. V80 consolidates the claim fence into one client SQL call:
the database function checks isolation, locks, checks time after waiting, and
stamps the consumed proof. The original triggers remain active. A real Hibernate
statistics test establishes one prepared statement and an unchanged lease; this
is not a measurement of latency or a claim that the server executes one statement.
Added latency and multi-JVM capacity still require RustFS measurements.

## Durable preparation record requirements

The private codec must retain the full normalized publication intent as well as
its canonical digest. Canonical command bytes omit the operation ID; reconstructing
from a digest or assuming those bytes are the full intent would lose information.
Preserve the account/principal/operation key, owner nonce, per-upload attempt and
lease-token identities, lease duration and predecessor generation. Preserve every
sampled drive field, including nullable strings and the credential reference, plus
the backend generation and full nonsecret location identity/storage realm. Decode
must not look up current provider configuration to replace a historical snapshot.

Use a bounded, versioned deterministic encoding and check exact member/drive
coverage, duplicate entries and command/key binding before reconstruction. Decoding
grants no authority. Fixed modes, proposed claim-transfer identity and the sticky
stage-started marker require their own committed transitions; they must not be
lost through replacement of an immutable preparation blob. The complete journal
and its process-crash qualification remain the next implementation work.
