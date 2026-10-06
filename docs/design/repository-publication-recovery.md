# Durable publication recovery

Status: design requirements; automatic cross-host session recovery is not enabled.
This extends the repository composition goal without changing public contracts.

## Initial registration activation requirements

The private initial registration path now commits claim acquisition and immutable
preparation together. It encodes the bounded preparation before taking SQL locks,
then holds the claim lock and current-transaction fence through insertion and the
last pre-commit cancellation check. Exact retry keeps the original token, seeds
and lease; it does not renew or replace them. Real PostgreSQL regression tests
prove rollback leaves neither row and a lost commit acknowledgment leaves both.
Cancellation at every explicit boundary has the same all-or-nothing behavior.
See the [atomic-registration evidence](../evidence/repository/2026-10-05-atomic-registration/README.md).

Ordinary runtime registration remains disabled. Fixed modes and owner admission
still commit later, so complete fresh-process recovery of interrupted registration
remains to be qualified. Explicit low-level claim-only primitives remain available;
such a row must never authorize inventing replacement seeds or an owner identity.

Scoped same-session journal access now uses a host-private, nonserializable
capability bound to account, authenticated principal, operation ID, command digest,
initial claim token/epoch and saved owner nonce. The actual caller remains separate
for authorization. Owned V82/V83 operations compare the capability against the
loaded preparation and live owner. Direct private bind/load/start and cross-process
bootstrap still require process authority; the capability grants no document access.

The private opt-in session checks current source READ, destination WRITE and
revision preconditions using its frozen prepared plan before registration and
before setting the sticky assessment-start flag. These checks run again on retry.
Scoped creation of an absent destination still requires a separate grant policy
and is refused. Preflight and journal writes are separate transactions: a concurrent
revocation after preflight can still precede a marker write. Authoritative checks
at upload, CREATE/decision and publication remain necessary. Known revocation
before preflight refuses the marker without setting the sticky flag.

Default manager construction still uses unjournaled sessions. An explicit private
opt-in now retains exact session identity from immediately before the first
potentially committing journal call through ambiguous errors, cancellation and lost
responses. Registered-command expiry and cleanup still require qualification. See the
[scoped journal evidence](../evidence/repository/2026-10-05-scoped-registration/README.md).

The opt-in `DocumentPublicationSessions.journaled` keeps reserve-before-construction
ordering. The registration marks itself as potentially committed immediately before
calling initial registration; the marker never resets after an exception. Errors
and cancellation retain the exact session, claim token, modes and seeds. A proven
pre-journal failure releases capacity only when no users or recovery call remain.
Concurrent same-key calls cannot evict an executing entry. This mode refuses the
older unjournaled `recover()` replacement path before changing a manager entry.
SQL-proven supersession and authorized terminal replay retain their existing roles.

Marking before the journal call is conservative: encoding or byte-budget failure
can retain an entry even without a SQL commit. That entry is still bounded by count
and command-byte capacity, but no exception-based eviction or timeout cleanup is
implied. Qualify explicit abandonment/cleanup before enabling this opt-in in a host.
Shutdown drains calls while preserving uncertain local entries; that is not proof
of restart recovery. See the
[manager retention evidence](../evidence/repository/2026-10-05-journaled-manager/README.md).

Fresh-process interrupted registration is separate from same-process retention.
The existing restoration handle covers V83-started stages only. A future protocol
must classify pair-only, modes-present/owner-absent and owner-present states from
SQL, recover exact preparation identity, and refuse expired/transferred claims.
Missing modes cannot be invented or inferred from current defaults. Establish the
trusted original-claim ownership protocol and explicit unresolved-state policy
before reconstructing a manager entry from such a partial registration.

## Original-owner restoration handle

`DocumentPublicationRestoration` is an internal stage-only handle. A trusted
coordinator must supply the exact claim-bearing original owner; the handle does
not retrieve execution tokens or establish that another coordinator has stopped.
It loads preparation, validates fixed modes, and loads the committed assessment
start under live claim/owner checks. The saved owner nonce, generation and command
must agree. Missing start state refuses restoration and never authorizes CREATE.

The handle retains its preparation byte reservation through use and releases it
on close. Concurrent resume and close are refused. Resume has no upload bodies,
resolver, admission or takeover input. It checks the discovered assessment UUID
and database deadline against the saved start before capturing retained content;
terminal results use ordinary authorized replay.

This is not automatic failover or a public restore API. Durable registration of
ordinary runtime sessions and complete successor coordination remain gates. The
qualification fixture additionally checks claim loss and a forced writer exit as
described below; those checks do not authorize arbitrary live-token discovery.

### Coordinator ownership of restored state

`DocumentPublicationSessions.resumeStarted` integrates the stage-only handle with
the existing bounded manager. Its supplied owner must match the command's account,
operation and digest before authorized terminal replay. Private process authority
does not waive those identity checks. A terminal receipt needs no preparation load.

For a pending operation, the manager reserves session and command capacity before
loading the journals outside its monitor. A restored entry has one active user,
retains the exact supplied owner, and cannot enter ordinary execution or takeover.
Uncertain failures retain its preparation reservation for retry. Terminal results
evict and close it after use. Shutdown refuses new calls, allows accepted calls to
finish, and drains restored handles only when all active calls leave; ordinary
unpersisted session identities retain their previous lifecycle behavior.

`DocumentPublicationRuntime` has a package-private bridge to this path. It does
not expose owner capabilities through protobuf, discover SQL tokens, register
ordinary sessions durably, or authorize successor execution. The current host
still needs an explicit ownership/bootstrap protocol before public activation.

The session and real-provider gates passed; the latter repeats forced-exit
reconciliation through this manager. See the
[manager evidence](../evidence/repository/2026-10-05-managed-restoration/README.md),
including the account-binding regression found during Sol review.

The original-owner checkpoint passed nine PostgreSQL journal cases and the
production-JAR LocalStack gate, including actual handle reconciliation and exact
durable rejection replay after lost assessment COMMIT acknowledgment. See
[the retained evidence](../evidence/repository/2026-10-05-original-owner-restoration/README.md).

### Claim-loss and forced-exit qualification

The production-JAR LocalStack gate passed these cases. See the
[retained evidence](../evidence/repository/2026-10-05-restoration-crash/README.md).

The claim-loss fixture restores under a live ten-second claim, waits using the
database clock, and confirms the operation owner is still live. The old handle
must fail with the claim fence before assessment reads or terminal decisions,
both after expiry and after an explicit epoch/token transfer. It does not execute
with the successor claim or delay an external write across transfer.

The forced-exit fixture creates provider content and a sealed assessment, then
halts the writer JVM after the real assessment COMMIT while its JDBC acknowledgment
is lost. The parent requires the designated exit code and reaps the writer before
starting a fresh reader. Only the public operation ID and normal runtime connection
configuration cross that boundary; there is no command or capability handoff file.
The reader uses explicitly test-only privileged SQL to recover the original live
owner/claim, then reads preparation and uses the restoration handle. Its rejection
receipt must bind the original assessment, manifest and deadline, with exact retry.

The privileged bootstrap is not a production API. Shared token visibility alone
does not prove the former owner process is dead. A network timeout, missing host
heartbeat or expired execution claim cannot substitute for this fixture's actual
process-exit proof. Production takeover still requires the late-provider-effect
policy and host ownership protocol.

### Late provider effects and reclamation policy

Claim fencing governs SQL state. It cannot revoke a provider request already
issued, and a timeout or provider client cancellation does not prove that the
provider did not store bytes. An attempt's immutable plan fixes its exact backend,
namespace, UUID-scoped keys and content hashes. Verification captures exact
provider versions.
Unknown PUT outcomes remain unverified; they cannot be promoted as successful
uploads. Stale completion must fail the current claim and selection fences.

Cleanup may report observed absence while a stale PUT is still pending. `ABSENT`
is therefore an observation, not permanent deletion or proof that every writer
has stopped. Retain the attempt plan and cleanup tombstone and keep scheduling
bounded exact-key passes, including after an ABSENT result. Each pass uses the
original backend identity and removes versions only for those exact abandoned
keys. Never replace that lookup with the current drive configuration or a broad
prefix deletion. Published history and assessment references exclude attempts
from reclamation; expiration alone does not permit deleting retained content.

`DocumentSelectedTransferIT` now delays a real LocalStack PUT before adapter
execution, expires and transfers the execution claim, and observes cleanup
absence before releasing the PUT. The stale verification fails, but real bytes
are present afterward. The attempt remains a cleanup candidate; another pass
removes those bytes and preserves a neighboring key/version. This tests the
claim-only storage primitive, not journaled successor coordination.

Before enabling automatic takeover, qualify the full coordinator with fresh
successor generation/attempt identities, disjoint provider keys, cancelled or
draining old work, and concurrent publication/reference retention. Do not assume
an old attempt can be adopted merely because the content hash matches. Establish
cleanup scheduling and durable tombstone retention in the managed host. Until a
separate proof bounds the last possible provider arrival, no finite recheck count
or quiet interval can justify forgetting an abandoned key. Eventual cleanup
depends on provider availability and writers eventually stopping; it is not a
fixed-time reclamation guarantee.

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
of those seeds grants no execution authority. Durable serialization and shared
preparation storage are implemented below; session restoration remains unfinished.

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

The [remaining work map](repository-remaining-work.md) records independent slices
and the publication prerequisites for restore, pruning and hydration.

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
lost through replacement of an immutable preparation blob. V81 implements the
preparation journal; the remaining transitions and process-crash qualification
are still required.

## Implemented preparation encoding

`DocumentPublicationPreparationRecord` now binds the private immutable preparation
and rebuilds it through existing coverage checks. `DocumentPublicationPreparationCodec`
uses versioned deterministic encoding with a 16 MiB envelope bound, 1 MiB text
bound, bounded collection counts, strict standard UTF-8 and explicit null fields.
The decoder checks expected journal key and command digest, reconstructs the full
intent, and rejects a byte sequence that differs from its canonical re-encoding.
It retains exact seconds/nanos and sampled configuration without provider lookup.

Seven new tests, plus twelve existing seed/session cases, pass. They include real
SQL admission from decoded identities and malformed/noncanonical/oversized input.
The existing live preparation coordinator is unchanged. This is an encoding
prerequisite, not a restored session. V81 below adds the persisted journal with
an independently retained full-blob digest and immutable row checks: the command
digest alone does not bind seeds or placements. Encoded byte caps are not heap
accounting; journal reads also need bounded concurrency/resources. Evidence is in
`docs/evidence/repository/2026-10-05-preparation-codec/`.

## Shared preparation journal (V81)

The private SQL journal stores exact command bytes and the bounded preparation
blob with independent SHA-256 checks. Inserts require a live execution claim.
Initial preparation precedes operation admission; recovery preparation names an
exact expired predecessor and a distinct next owner nonce. Existing records are
immutable. Exact retries remain valid after admission; conflicting records fail.

Command-only bootstrap requires trusted process authority and the recorded
principal. Account membership alone does not grant recovery access. This private
host operation is not a public document read or a substitute for current ACL
checks when the recovered operation runs. Private preparation loads require the
exact live claim both before fetching and after decoding. Decoding runs outside
database locks. The returned borrowed record grants no later mutation authority;
every subsequent mutation must independently fence its claim.

Encoded storage is bounded and covered by the injected shared byte budget before
allocation. A successful load retains its reservation until close; failure,
cancellation and claim transfer release it. This is byte accounting, not a bound
on all parsed heap. SQL immutability and full-blob integrity protect the retained
identities without consulting current provider configuration.

Automatic session restoration is still disabled. Fixed modes, sticky stage-started
state, retained assessment coordinates, provider effects across claim transfer,
and the forced-process-crash test remain required before activation. Preparation
records cannot yet be pruned because the retention protocol is unfinished.

## Next implementation boundary: durable session transitions

The current `DocumentPublicationSession.Execution.bindModes` fixes member modes
only in memory. `beginAssessmentStage` likewise sets an in-memory sticky flag;
`DocumentPublicationAssessmentExecution.stage` generates the assessment UUID and
retention deadline immediately before CREATE. These are the next persistence
boundaries, not evidence that automatic recovery already works.

Add a private transition journal linked to the exact preparation key and owner
nonce. Keep the immutable V81 preparation intact. Fix the complete member-to-mode
map once, with exact command-member coverage and a bounded canonical encoding.
An exact retry returns the saved choice; a changed choice fails even after an
uncertain acknowledgment. Persist this before operation admission or any
mode-dependent provider work.

Before assessment CREATE, durably record the proposed assessment UUID and exact
retention deadline with a sticky stage-started transition. Retrying the transition
must return those original coordinates and must never generate replacements.
Persisted intent to stage is not proof of a committed assessment. After an
uncertain CREATE, discover and verify the committed assessment against those
coordinates and the command/owner bindings. An absent or mismatching assessment
requires explicit reconciliation; it cannot authorize another CREATE. Retained
manifest identity comes from the committed assessment, not a caller assertion.

Each mutation must fence the live execution claim in its SQL transaction, then
lock the preparation/transition row; any operation-owner lock follows the claim
lock. Recheck liveness after lock waits. Reads that expose private recovery
coordinates require the same trusted-process authority as preparation loading
and a final live-claim check after decoding. No SQL connection or row lock crosses
provider I/O. Each operation has independent rows and bounded state.

Acceptance tests must cover exact retries after withheld acknowledgments, changed
modes, extra/missing members, wrong preparation/owner, stale claims, transfer while
waiting for a lock, cancellation and an irreversible stage-started marker. Add a
real assessment-COMMIT acknowledgment-loss case before integrating restoration.
Then kill the writer process and recover from shared SQL in a second JVM, without
a handoff file, upload or registry resolution. These steps remain gated by the
separate late-provider-effect and cleanup policy; this journal alone must not
enable automatic takeover.

SQL enforcement is an activation requirement, not only a Java call-order rule.
Admission and mode-dependent mutations must require the committed fixed-mode
record. Assessment CREATE must match the committed transition's exact owner
generation/nonce, command digest, assessment UUID and retention deadline. Its SQL
guards must follow claim, transition, then owner lock order; the current owner-first
creation path needs explicit adjustment. Sample the deadline using database time
in the transition transaction, at exact microsecond precision, and never refresh
it on retry. Discovery is owner-generation scoped: a new generation must not
silently adopt a predecessor's assessment. Resolve it under valid original-owner
authority or classify it as unresolved pending an explicit recovery protocol.

## Private fixed-mode journal (V82)

The first durable transition now persists the member-to-mode map in a separate
immutable JSONB row tied to the preparation and exact owner nonce. Binding loads
the retained command and requires exact member coverage. SQL checks mode spellings,
shape and size, requires a current transaction's live claim proof, and locks the
preparation. New choices must precede the corresponding owner admission; exact
retries remain allowed afterward. Same-operation writers serialize on the claim
row before inserting. Changed choices, UPDATE and DELETE are refused.

Private loading requires trusted process authority, verifies the saved owner and
revalidates every member against the retained command, then fences the claim again
before returning. SQL cannot parse protobuf command membership: a trusted direct
SQL writer can insert a syntactically valid wrong-member map, but the loader rejects
it as DATA_LOSS. A journal row alone must never be treated as validated authority.
Equality is JSONB/map equality, not a new canonical protobuf or byte format.
Serialized modes are capped at 1 MiB with a shared reservation; this does not account
for all parsed heap. Preparation loading retains its independent byte reservation.

Real PostgreSQL tests cover lost acknowledgment after commit, fresh-loader recovery,
changed modes, member coverage, process-only access, late first binding, stale
claims and two first writers contending on the actual claim lock. A deliberate
wrong-member SQL row proves load-time rejection. The selected mode, preparation and
claim suites pass; this is neither provider recovery nor a process-crash test.

Session admission has not switched to this journal. Sticky assessment coordinates,
SQL consumption guards, owner-transition rules and late provider effects still
precede automatic recovery activation. Existing public contracts are unchanged.

## Durable assessment-start identity (V83)

The private start journal records an assessment UUID, requested retention interval,
and a deadline sampled once from the database clock. It binds the exact preparation,
command digest and live operation-owner nonce/generation. Fixed modes must already
exist and pass the private loader's command-member checks. Changed identity or
retention is refused; exact retries return the original deadline without extending
it. UPDATE and DELETE are forbidden. A fresh journal can load the coordinates from
shared SQL without remembering the original proposed UUID.

SQL records the first insert's transaction ID and refuses assessment creation in
that same transaction. Thus the sticky marker must commit before CREATE; an exact
retry cannot rewrite the transaction ID or refresh the deadline. A real PostgreSQL
red test demonstrated the missing separate-transaction check before this guard.
For operations with fixed-mode rows, a new assessment-owner INSERT must match the
committed marker's ID, command, owner nonce and deadline. Missing or mismatched
markers fail. Creation checks the marker before taking the owner lock, and SQL
repeats the binding check for direct INSERT callers. Existing operations without
fixed-mode rows retain their explicit unjournaled behavior.

A start marker is not a committed assessment. Loading its coordinates does not
permit another CREATE after an uncertain outcome. Discovery must still verify the
original committed assessment; absence remains unresolved. The new loader requires
the original owner to remain live, so it does not adopt a predecessor's assessment
under a new generation. Automatic session restoration, SQL fixed-mode consumption
guards, successful retained-evidence integration, late provider effects and the
forced-process-crash qualification remain open before recovery activation.

### Journaled provider assessment qualification

The production-JAR fixture now has a separate journaled scenario, preserving the
existing short-lease and restart cases. It saves preparation and fixed modes before
claimed owner admission, builds upload attempts from the saved seeds, and uploads
real provider bytes. It commits the sticky start before invoking real assessment
creation. A JDBC wrapper delegates the actual COMMIT, then raises SQL state 08006
to withhold its acknowledgment. Fresh journal/discovery helpers recover the exact
coordinates, selected attempts and one sealed stage without retrying CREATE. The
existing retained replay and terminal rejection path checks exact receipt identity.

This fixture is same-process reconciliation under the original live claim/owner,
not writer death, claim transfer or automatic session restoration. Its trusted
process caller does not establish current ACL enforcement for a scoped client.
It does not claim zero additional provider reads or reload the entire preparation
as a new host would. Run evidence is recorded separately after qualification.

Another activation prerequisite is explicit mode consumption: the current fixture
passes the same mode map to V82 and assessment preparation, but V83 checks only
coordinates/command/owner, not equality with observed assessment modes. Before
restored sessions can execute, load and compare the retained V82 map at the owned
preparation/staging boundary and reject a mismatch before artifacts or stage writes.
Add a deliberately mismatched-mode fixture; SQL mode-dependent mutation guards
and the hard-crash/late-provider-effect requirements remain open.

The journaled provider fixture passed both LocalStack and RustFS correctness gates;
see `docs/evidence/repository/2026-10-05-journaled-assessment/`. The next activation
change must also distinguish an intentionally unjournaled operation from a partially
journaled one. V79 supports claim-only primitives, so a claim alone does not declare
journal activation. An exact V81 preparation is the current durable signal: when
it exists, absent V82 modes must refuse owner admission and assessment CREATE.
V83 currently uses mode-row presence as the discriminator, allowing that partial
state to reach the legacy path. Close this gap in SQL and shared Java creation.

## Journal consumption guards (V84)

An exact preparation now requires fixed modes and its saved owner nonce at owner
admission and ordinary owner updates. Claim-only operations without a preparation
remain explicit unjournaled primitives. Assessment creation uses preparation
presence, rather than mode-row presence, to require modes and the committed start;
a partial journal cannot silently take the legacy path. Recovery-only owner updates
retain their existing exception so cleanup after claim expiry does not require
new execution authority. A real red test caught that cleanup regression before
adding the exception; the final test also refuses ordinary writes after expiry.

The modes journal now has an internal pass/fail comparator for scoped callers.
It checks operation/claim authority, reads bounded immutable state, and compares
observed choices without returning private mode or preparation data. Its existing
public-to-the-package recovery load remains process-authority-only. Shared assessed
execution compares the owned observation before either accepted promotion or invalid
artifact staging. Candidate execution freezes the requested map and compares it
after authorized source capture, before preparation. Low-level assessment CREATE
checks the observed evidence's already-verified modes after a short full-authorization
preflight and reauthorizes failures before exposing them. CREATE still rechecks
full authorization in its own mutation transaction. No SQL connection spans journal
decoding or provider work.

The new provider fixture deliberately stores one opaque choice but observes both
members as typed under the typed-required policy. Direct assessment CREATE must
refuse the mismatch before a stage is inserted. This is not a high-level artifact
write-count test. The low-level publication commit helper still needs independent
mode binding for direct callers; these guards do not establish complete enforcement
of every internal write path. Automatic restored-session activation, process-crash
recovery and the late-provider-effect policy remain unfinished. No throughput claim
is made for the additional claimed-operation checks.

## Direct successful commit mode binding

The low-level publication commit now derives each member's mode from its validated
schema-proof set and complementary opaque-content set. After full authorization,
inside the already owner-fenced transaction, it compares that map with the exact
preparation and fixed-mode rows, including command digest and both owner nonces.
The query returns only a boolean. It opens no nested transaction, transfers no
private JSON or schema artifact, and introduces no provider I/O under a lock.
At most 64 validated member names are encoded, with an explicit 128 KiB wire bound.
Unclaimed operations incur no comparison query; claimed operations without a
preparation remain the documented claim-only primitive.

A real-provider regression first demonstrated a successful typed publication
under an opaque saved choice. The fixture now requires refusal, no terminal outcome
and no destination row, and separately proves a matching typed journal still
publishes with an exact replayable receipt. Upload attempts come from saved seeds.
This guards publication; operation-scoped schema artifacts may already have been
staged in an earlier transaction. Raw privileged SQL is not a substitute for the
validated commit API. Automatic session restoration, claim-transfer/late-provider
qualification and forced-process-crash recovery remain separate unfinished work.
