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

## Implemented claim primitive (2026-10-05)

V78 and `RepositoryExecutionClaimLedger` now provide a private, per-operation SQL
claim. They do not yet register publication sessions or fence publication writes.
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

Remaining integration: persist complete session preparation; propagate claim
identity with operation authority; add transaction-visible claim write stamps and
SQL guards for registered-session mutations; exercise claim transfer across every
owner/attempt/stage/decision/publication path. A lock-only call is not yet the
SQL-visible proof those guards require. No provider transfer policy or hard-crash
recovery capability is enabled by this primitive.
