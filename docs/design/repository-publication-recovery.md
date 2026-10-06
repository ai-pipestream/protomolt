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

The production-JAR PostgreSQL/LocalStack matrix also exercises this opt-in with an
actual scoped caller: accepted existing-source update, typed rejection, uncertain
CREATE, CREATE rollback, decision acknowledgment loss and exact retries. Account
binding denial creates no journal/owner/start rows and releases manager capacity;
cross-account replay is refused as NOT_FOUND. No upload-backend or schema resolution
occurs before that initial denial. Source setup still uses process authority, and
no scoped creation or public authentication protocol is qualified by this fixture.
See [scoped provider evidence](../evidence/repository/2026-10-05-scoped-provider/README.md).

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

### Next host lifecycle increment

The next implementation should establish durable local drain evidence for deliberate
handoff, independently of fresh-successor execution. This is a design requirement,
not a claim that the current host implements it. Reuse the execution claim's exact
account/principal/operation, epoch and token, adding a host-generated coordinator
incarnation binding immutable within that claim epoch. A future transfer installs a
distinct successor incarnation under the same operation key. Keep this per operation: publication must not acquire
a global host row lock. A claim lease or readable token alone cannot identify a safe
replacement coordinator.

Use distinct `DRAINING` and `LOCAL_DRAINED` states. Closing admission is not the same
as having drained all work. The existing `DocumentPublicationRuntime.shutdownStep`,
session drain, upload coordinator drain and read lifecycle provide local lifetime
boundaries to compose. Before implementing the state transitions, classify every
claimed mutation as new admission, settlement of admitted work, or reclamation.
Do not indiscriminately reject in-flight renewals, observation flushes or terminal
publication at the start of graceful drain. Conversely, copied claim authority must
not admit new attempts after the admission fence. These decisions must be enforced
at SQL mutation boundaries, not solely by a runtime boolean.

Only the exact owning incarnation may record `LOCAL_DRAINED`, after successful local
shutdown and with the original claim binding rechecked. It then fences further owner
mutations and renewals. Lost acknowledgments require exact state confirmation; neither
an expired lease nor a stale DRAINING marker permits inventing a drain attestation.
Read-only inspection and reclamation use their separate authority. Reader incarnation
registration is a useful lifecycle model, but does not attest publication ownership.

`LOCAL_DRAINED` does **not** mean remote effects have settled. A local SDK timeout can
end a worker while its PUT remains active at the provider. Preserve unknown outcomes,
immutable attempt plans and cleanup tombstones after local drain. Successor execution
must use new operation-generation/attempt identities and disjoint provider keys;
cleanup continues against original backend identities. Do not release historical
references, schema claims or pruning guards merely because a coordinator drained.

Acceptance must include a real delayed provider call across a drain timeout, wrong
incarnation/claim refusal, settlement of work admitted before DRAINING, refusal of new
work after that boundary, exact retry after lost transition acknowledgment, and a
late remote write after a local timeout. A fresh successor coordinator and abrupt-death
recovery require separate qualification. Ordinary `ManagedDocumentServices` still
constructs the unjournaled runtime; keep automatic activation off until these paths
are composed and tested together.

### Retained inputs

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

## Interrupted registration inspection

`DocumentPublicationRegistrationInspection` now classifies the original registration
as no preparation, preparation only, modes bound, owner admitted, owner expired,
assessment started, or terminal. The caller supplies the original live execution
claim and actual private process authority. The inspector does not discover tokens,
renew a lease, transfer ownership, mint identities, or grant execution permission.
It uses bounded preparation/mode loading, then a short claim-before-owner fenced
transaction that checks immutable bindings and terminal presence. A registration
that advances while its payload is decoded requires a fresh inspection.

An admitted owner is live only at the database observation. An assessment-start
marker does not prove assessment CREATE committed. A terminal marker routes the
caller to separately authorized replay; the inspector returns no receipt. Claim-only
operations remain unjournaled primitives, never permission to invent missing seeds
or modes. Real PostgreSQL tests and limits are recorded in
[the inspection evidence](../evidence/repository/2026-10-05-registration-inspection/README.md).

Fresh-process resume of a partial registration still requires a reviewed coordinator
ownership and late-provider policy. Sharing a token does not prove the previous
process stopped. Retained-capacity cleanup and automatic runtime activation remain
unfinished.

## Pre-owner abandonment

The private `DocumentPublicationAbandonment` primitive records an immutable V85
marker bound to the initial preparation digest, owner nonce and original execution
claim. It requires the original claim to be live. The claim fence serializes marking
against command and owner admission. A command row, owner or assessment start
prevents abandonment; a committed marker prevents preparation/mode retries and
all subsequent command/owner admission for that operation identity.

This covers registrations that cannot yet have issued provider writes. It does
not abandon admitted owners or enable takeover. Cancellation before commit rolls
back the marker; cancellation after commit leaves an uncertain caller outcome.
New marker insertion still requires the original claim to be live. Private exact
confirmation now compares the immutable marker to the retained preparation digest,
nonce, command and original token without renewing or fencing the claim. It works
after claim expiry or transfer; an absent marker does not prove rollback.

The journaled session manager can explicitly abandon an idle retained registration.
It keeps the entry reserved while SQL runs and releases capacity only after confirmed
abandonment. Lost acknowledgments, expired claims without markers, cancellation and
active users preserve the entry. An admitted owner cannot use this path. This is
private host functionality, not an exposed cancellation RPC or default activation.

Replay now distinguishes `ABANDONED` without manufacturing a publication receipt.
It checks the canonical stored command and current READ access to its complete
destination/source set before exposing the marker or a command conflict. It must
not authorize only an alternative command supplied by the caller. Public runtime
execution reports FAILED_PRECONDITION and does not open storage or schema scopes.
A fresh authorized observation also permits eviction when an execution failure races
abandonment. Failed confirmation preserves the original failure and retained entry.

## Private coordinator registration identity (V89)

Each opt-in journaled manager mints an incarnation UUID. Initial claim insertion,
its immutable epoch-one coordinator binding, and preparation commit together.
The private journal API uses the actual claim INSERT result to forbid attaching a
manager to a pre-existing unbound claim. Exact retry checks the original token and
incarnation without renewing the lease. The legacy registration overload refuses
bound operations, including retries with the correct claim token.

Original-owner resume checks any binding for the operation, so an epoch transfer
cannot make a previously bound operation appear legacy-unbound. A different manager
cannot resume that live operation. Authorized terminal replay remains available to
a fresh manager because it does not issue provider writes or resolve schemas.

The SQL trigger independently enforces a live claim fence, matching epoch/token,
immutability, and insertion before preparation/operation admission. Creation-only
adoption is enforced by the private Java acquisition API; the trigger alone does
not prove a privileged SQL caller created the claim in the same transaction.
This is registration identity, not proof of process liveness, local drain or remote
provider quiescence. Durable drain, successor execution, late-provider cleanup and
ordinary runtime activation remain unfinished.

## SQL admission closure (V90)

`RepositoryCoordinatorDrain.begin` inserts an immutable per-operation marker under
the original incarnation's live claim fence. It requires private process authority
in the authenticated account/principal scope. An exact retry preserves the original
timestamp. `confirm` checks the immutable marker's epoch, token, incarnation and
command digest without renewing the claim; it remains usable after expiry or
transfer to resolve an uncertain commit reply. Absence is not a rollback proof.

The database classifies work at its existing boundaries:

- New preparation, modes, command/owner, upload attempt and assessment-start rows
  are admission. V90 rejects actual INSERTs after the marker. Existing conflict
  retries remain valid because these guards run AFTER INSERT.
- Advancing the operation owner generation is admission; same-generation renewals
  remain settlement. A low-level claim transfer cannot bypass closure because the
  drain lookup covers every marker for the operation, not just its current epoch.
- Existing attempt verification, observations, schema/evidence staging, assessment
  CREATE from an already committed start, and terminal decisions are settlement.
  The existing live-claim and owner fences still apply; V90 does not replace them.
- Read-only inspection, pre-owner abandonment and separately authorized cleanup
  remain distinct from admission. This marker releases no retained references.

This is a private SQL prerequisite, not the complete host DRAINING state. Already
staged attempts can still begin provider calls. The next integration must close
host-local provider-start permits, retain each permit through SDK completion, and
compose manager/publication/read/upload shutdown. No LOCAL_DRAINED state, automatic
successor, provider-quiescence claim or ordinary-runtime activation is added here.
A shared marker never permits deleting late-effect cleanup tombstones.

## Local provider-start admission

`DocumentUploadCoordinator` now admits each PUT/read-back transfer through a local
permit immediately before `DocumentPartTransfer.upload`. The existing bounded
part-worker capacity still limits concurrency. Admission and closure share a short
monitor; neither provider I/O nor SQL runs under that monitor. There is no new
per-part SQL round trip. A permit acquired before closure is admitted work even if
thread scheduling delays the first provider instruction. It remains held until the
entire transfer method returns, including read-back and failure paths.

`stopProviderStarts` refuses subsequent permits without setting the coordinator's
hard-cancellation flag. A refused queued part returns a distinct not-started result,
so it does not poison sibling workers or their observation flusher. Permitted calls
finish under the existing cancellation, heartbeat and claim checks. The coordinator
flushes completed observations before reporting incomplete staging as UNAVAILABLE.
Incomplete attempts retain their normal reconciliation/cleanup obligations; stop
neither retries a PUT nor discards its key or evidence.

Runtime close stops provider admission before closing scopes or sessions, because
session cleanup may block or fail. Existing hard close/interruption semantics remain
separate. Runtime shutdown still drains sessions, schema scopes, upload operations,
and readers before borrowed resources may close. `awaitProviderIdle` and the local
activity counters cover transfers only: queued work, SQL settlement, retained bytes
and remote effects after a provider timeout are outside that observation.

The real PostgreSQL/LocalStack tests hold return from actual PUT and bounded read
calls, preserving resources until release and allowing admitted work to verify.
A two-worker case observes a third part refused while another permitted transfer
is still held; both permitted observations persist, only two PUTs occur, and the
whole incomplete attempt fails explicitly. This is delayed transfer-return evidence,
not proof that a remote network request is still active or that all remote effects
are quiescent. See the [provider-start evidence](../evidence/repository/2026-10-06-provider-start-drain/README.md).

The opt-in journaled manager now composes this local gate with V90 for retained
nonterminal registrations. It closes provider starts before manager cleanup, closes
a shared registration barrier, then waits for both registration transactions to
leave that barrier before taking a bounded identity snapshot. An accepted call
that has not entered registration cannot register after the barrier closes.
Entries evicted with durable terminal proof are outside that snapshot.

Each marker requires supplied private process authority for the original principal
and exact operation identity. The manager reads the actual persisted claim lease;
it does not invent a lease or derive authority from a key. A failed lookup or marker
reply is resolved only by exact durable marker confirmation. Cancellation remains
effective during confirmation. An absent claim is reported as unresolved and its
local identity remains retained: absence after a failed JDBC call is not rollback
proof. The gate stays closed on partial failure, and another pass can confirm markers
already committed without changing their identity.

This is registration and SQL admission closure only. Full runtime shutdown must
precede LOCAL_DRAINED; no such state is recorded by this operation. Ordinary runtime
sessions remain unjournaled, and automatic successor execution is still disabled.

## Private journaled runtime composition

`DocumentPublicationRuntime.journaled` is a package-private host opt-in that reuses
the existing upload coordinator, execution engine and reader lifecycle. It selects
the journaled session manager and stores a trusted per-operation drain-authority
resolver. Public constructors and the managed service still use the ordinary
runtime. Request ownership never supplies this private process authority.

For a journaled instance, the existing shutdown entry point always closes provider
starts and scope/session admission, then drains registration scopes and records V90
markers for retained nonterminal identities. Active registrations or unresolved
claim absence return false before provider/read teardown. Authority, SQL and
cancellation failures remain visible and retryable with admission closed. After
marking, shutdown waits for session calls and schema scopes, drains uploads, then
runs reader cleanup. Terminal entries already evicted with durable proof require
no marker. Local wait budgets do not replace database statement/network timeouts.

An active call may finish work already admitted before drain, but cannot begin a
new assessment afterward. The runtime probe checks the actual SQL admission refusal
and absence of a new assessment start, stage or rejection receipt; it does not turn
that refusal into a semantic rejection. Lost CREATE/decision replies and retained
rollback cases use the same real provider/SQL paths as ordinary runtime execution.

Successful runtime shutdown is still not durable LOCAL_DRAINED. Service-owned
`ManagedSchemaAccess` may retain abandoned registry loads after publication scopes
have exited, and its separate close/idle protocol must complete before a host can
attest drain. A direct synchronous `Schemas` resolver must not leave untracked work.
Remote effects surviving an SDK timeout, cleanup tombstones, durable attestation,
successor identity and public activation remain separate required work.
See [runtime qualification](../evidence/repository/2026-10-06-journaled-runtime/README.md).

## Durable local-drain primitive (V91)

The private ledger can record an immutable local-drain attestation only after an
exact V90 admission-closure marker. It locks the current claim first and compares
the original operation, digest, epoch, token and coordinator incarnation against
the V89/V90 bindings. An expired but unchanged claim may record completed drain:
this path neither renews the lease nor stamps execution authority. A transferred
claim cannot create a new attestation for its predecessor. Exact confirmation of
an already committed attestation remains available after transfer and lost replies.

Once recorded, execution is closed across every claim epoch of that operation.
Both the shared mutation-fence check and same-epoch claim updates reject further
work. This also rejects a write fence stamped earlier in the attestation transaction.
A low-level epoch change does not implement the future successor protocol and
does not reopen execution. V90 still independently refuses new admissions.

V49 recovery-only ownership updates retain their separate purpose. They cannot
renew an owner or publish a result; schema/assessment cleanup still needs its own
retention proof. Local drain does not delete retained definitions, read pins,
historical references or provider tombstones. Pre-owner abandonment currently
requires execution authority and is consequently refused after local drain.

The internal managed journaled host now calls this primitive after sessions,
scopes, uploads, readers and service-owned schema workers have drained. Its trusted
authority resolver receives account, principal and operation UUID; it exposes no
claim token. Ordinary public builders remain unchanged. Exact confirmation uses a
transaction so the host's SQL timeout policy applies. A partial attestation failure
retains shared SQL resources and retries the immutable identity snapshot before
marking shutdown complete. Constructor observation failure leaves schema ownership
with the caller and cleans up newly registered readers.

Production-JAR qualification holds a real Git descriptor load after publication
cancellation, then injects a real PostgreSQL failure on the second of two local
attestations. The first marker survives and retry confirms its original timestamp.
This proves local managed-worker ordering, not remote provider quiescence or safe
successor execution. See [managed host qualification](../evidence/repository/2026-10-06-managed-local-drain/README.md).
See [SQL qualification](../evidence/repository/2026-10-06-local-drain/README.md).

### Retaining drain identities through session shutdown

The manager captures its immutable identity list once the registration barrier
closes and drains. A repeated drain uses that same list: an accepted operation can
finish and be evicted after capture without removing its identity from the shutdown
obligation. Exact SQL confirmation still runs on every attempt.

Restoration admission uses the same barrier around coordinator verification and
local reservation. Its identity comes from the verified owner claim and this
manager's incarnation, not from a later SQL lookup. A failed journaled restoration
retains its bounded entry even when no payload scope was created. An exact retry is
allowed. Shutdown closes a loaded restoration's payload scope but preserves the
owner identity for the snapshot. Non-journaled resource release remains unchanged.

SQL regression covers a real pre-owner rollback followed by accepted abandonment
and cache eviction during drain, and a same-incarnation owner takeover followed by
failed restoration and retry. Both bugs were reproduced before the fixes. A held
actual restoration claim-fence transaction now verifies that the registration barrier
prevents an early snapshot. The production-JAR fixture also loads a generation-2
restoration using an observed executor; its deliberately uncommitted assessment stage
is refused. Close releases the loaded bytes while retaining the original coordinator
identity for V90. This does not demonstrate restored publication or provider reads.
The host must additionally drain its schema workers before whole-host V91 activation;
these checks do not record local-drain attestations.

### Next successor increment: reserved graceful handoff

The first successor increment must preserve the current execution closure. V89
admits only an initial coordinator binding, V90 refuses admission after drain, and
V91 closes mutations across claim epochs. Advancing the claim epoch alone must
never bypass these guards.

Implement a private immutable graceful-handoff record before enabling successor
execution. Bind the original account/principal/operation and command digest to the
exact predecessor epoch, token and incarnation, plus a distinct successor token,
incarnation and epoch. Under the per-operation claim lock, require the exact
V89/V90/V91 chain, an expired predecessor claim lease and no terminal operation,
then reserve the successor and transfer
the claim atomically. Lock order remains claim, operation owner, dependent rows.
The caller retains proposed identities across uncertain acknowledgments; an exact
record confirms success without renewing or transferring again, even when the
successor lease has subsequently expired; confirmation grants no execution. A conflicting
proposal is refused. Expiry alone never substitutes for the predecessor's local
drain attestation. Require authenticated process authority at the Java boundary.

Initial acceptance must exercise concurrent proposals, wrong scope/digest/claim/
incarnation, missing drain, terminal operations, rollback, and lost-ack exact retry
against PostgreSQL. Prove the resulting successor still cannot admit attempts,
mutate results or start providers. Keep this private; a reservation is not an
available recovery API. Changing V89 requires a distinct successor-binding path,
not loosening its protection of initial binding.

The following increment must authorize a new owner generation and fresh per-member
attempts under that exact successor binding. Existing upload plans prefix physical
keys with fresh attempt UUIDs; preserve that separation and the SQL physical-key
uniqueness guard. Never reuse predecessor attempts or interpret a local drain as
permission to remove predecessor plans, historical references or cleanup tombstones.
A real delayed provider write must land at an old key after handoff without becoming
successor content or allowing old cleanup to delete new content. Only after that
execution path is qualified may admission/mutation guards recognize the successor.
Abrupt-death recovery requires a separate policy because it lacks local attestation.

The reservation portion is implemented below. The execution and late-effect
qualification portions remain required before automatic successor activation.

### V92 reservation implementation

The private handoff row binds the reserved successor without widening V89's initial
binding rules. Its insert trigger locks the predecessor claim, checks exact V91
identity, lease expiry and both terminal tables, then transfers the claim in the
same transaction as the immutable row. It locks an existing operation owner after
the claim. The V91 record transitively requires V90 and V89; its narrow foreign key
is supplemented by exact token/incarnation/digest comparison.

The Java entry requires process authority and exact caller scope. Plain INSERT
avoids the BEFORE-trigger side effects of ON CONFLICT DO NOTHING. Concurrent retries
that lose the predecessor check can only return success by reading the exact
committed proposal. Different proposals fail. A failed statement or transaction
rolls back the claim transfer. Confirmation can outlive the successor lease without
renewing it. The reservation stamps the new claim identity but V90/V91 still refuse
all new admission and execution; no runtime invokes this primitive yet.

#### Activation must distinguish pre-owner and admitted-owner states

V92 may reserve a successor before an operation owner exists. Existing V81 recovery
preparation for predecessor generation greater than zero requires an expired owner;
V82 requires modes before generation advancement, and `RepositoryOperationLedger`
then compares and advances that owner generation. These APIs cannot silently recover
a pre-owner generation-zero preparation with its old seeds.

The first executable path should therefore handle an existing nonterminal owner:
verify exact V92 identity and a live successor claim; read retained command and
preparation through trusted authority; reauthorize caller, current policy and storage
placement; persist fresh owner nonce and per-upload attempt identities for predecessor
generation g, with modes bound to the same successor; then atomically acquire owner
generation g+1. Any exception to V90/V91 must require this explicit generation
binding, never merely a higher epoch or the existence of V92. Do not call the current
unjournaled recovery path from a journaled manager.

Pre-owner recovery remains a required separate protocol; reservation alone grants
nothing, and the occupied generation-zero preparation cannot be overwritten or its
seeds reused. Both paths must preserve predecessor attempts, historical source pins,
schema retention and tombstones while checking current READ and admission policy.

##### Reduce new pre-owner windows at initial admission

The previous journaled session path created this gap through separate transactions:
registration acquired the claim/binding and saved preparation, bound modes, then
`DocumentPublicationSession.admit` created the operation and first owner. Mode
binding also reloaded preparation in separate transactions. Atomic admission
removes that split for new journaled session calls.

The journaled session implementation now composes initial claim, coordinator binding,
preparation, modes, operation and owner in one short SQL transaction. Encode and
bound preparation, command and modes before locking; perform no provider or schema
I/O in the transaction. Recheck current caller authorization and placement under
the established lock order. Reuse the existing guards and factor transaction-local
helpers rather than duplicating their SQL or weakening standalone journal APIs.

The retained session fixes the claim token, owner nonce, upload identities and mode
choices before attempting the transaction. An uncertain acknowledgment keeps those
identities; retry confirms exact committed state without lease renewal. Cancellation
after commit stays visible, and terminal results still require authorized replay.
Registration scope must span the composed operation so local drain cannot attest
while its owner admission remains in flight.

`DocumentPublicationRegistration.admitInitial` holds that scope across an early
authorization preflight, bounded encoding, and the atomic transaction. The preflight
refuses immediately denied calls before marking a session uncertain; authorization
is checked again after locking the claim. Preparation and mode insertion helpers
are shared with their standalone journals. Operation admission pre-encodes its
command before locks and delegates to the same owner-insertion logic as existing
callers. Public contracts and SQL migrations are unchanged.

Six rollback cases inject a real PostgreSQL trigger failure after each insertion
and verify all six row families remain absent, followed by exact retained-session
retry. Session tests verify complete admission after lost commit acknowledgments,
current caller denial, and a drain snapshot surviving terminal replay eviction.
Standalone legacy journals still qualify abandonment after a lost actual commit
reply and expiry. Initial-admission process tests hold the actual owner transaction
before and after JDBC commit, check zero versus all six row families independently,
kill and reap the JVM, and retry public command/payload files in a fresh process.
Before-commit death publishes as generation one; after-commit death waits for
expiry and recovers as generation two with fresh attempt/upload identities. Both
cases require one final attempt, selected versioned provider bytes and receipt
replay without storage calls. Performance and broader authorization/concurrency
qualification remain acceptance work.

Acceptance requires rollback faults after each SQL insertion before commit; lost commit
acknowledgment; cancellation before and after commit; concurrent exact/conflicting
registration; policy revocation at admission; local drain racing the composed call;
and a real process kill before and after commit. Assert absence of all new rows on
rollback and the complete exact tuple on commit, then measure transaction counts
and latency separately on RustFS. SQL atomicity alone does not establish performance.

This prevents new partial initial admissions only through the composed session
path. Existing registrations and callers of standalone private primitives remain
explicit recovery inputs. Do not delete their generation-zero evidence, mint a
fictional predecessor owner, or report the pre-owner recovery requirement complete.
The choice between a preparation identity independent of owner generation and a
separate bootstrap transition still needs review before extending that recovery path.

Sol's source review agrees with atomic initial admission as the next bounded
implementation. V84 already requires modes before owner insertion; the sequence
is claim, binding, preparation, modes, operation, then owner. Recheck authorization
after the claim fence and before domain writes. Before commit there is no durable
new journaled registration;
after commit there is a real generation-one owner eligible for the existing
post-owner recovery protocols. This does not resolve legacy generation-zero rows:
V85 abandonment is permanent and V93 requires an existing owner. Their recovery
must remain separately specified and tested.

#### Atomic generation-install implementation boundary

Do not chain `DocumentPublicationPreparationJournal.save`,
`DocumentPublicationModesJournal.bind` and `RepositoryOperationLedger.takeOver`:
each opens its own transaction, and each remains fenced after V92. One composed
transaction must install all three exact records or none.

The proposed private install record binds account/principal/operation, V92 successor
epoch/token/incarnation, predecessor owner generation and nonce, new owner nonce,
command digest, preparation digest, canonical modes digest and transaction XID.
Acquire claim then owner locks. Require a live exact successor claim, an expired
exact predecessor owner and no terminal result. Run provider/schema selection
outside SQL locks; recheck database-backed caller, policy, placement and source
identities under the short transaction. No provider or registry I/O belongs there.

Only preparation, modes and exact owner-takeover guards may recognize this
transaction's install record. Preserve each guard's existing payload and identity
checks, and require the actual inserted digests to match the install. A deferred
completeness constraint must refuse commit unless all three exact rows exist.
The affected paths are V81 preparation, V82 modes, V79 owner execution, V84 journaled
owner and V90 AFTER registration. Any claim stamping exception to V91 must be
limited to the exact install transaction and successor identity. Do not use a GUC,
a higher claim epoch or V92 existence as general permission.

General execution, command admission, assessment starts, attempts and terminal
writes remain closed at this increment. Cancellation must roll back every record;
lost commit acknowledgment must confirm the immutable install without renewal or
repeating writes. Tests must reject incomplete installs, digest/mode/owner mismatch,
expired or transferred successor, unrelated operations and mutations of old owners.
Only a later qualified execution path may use the installed generation.

### V93 atomic registration implementation

The install record now controls the exact preparation/modes/owner transaction.
Claim locking and a live exact V92 identity replace the proposed claim stamp: no
claim UPDATE is needed, so V91's claim-update rule remains unchanged. The deferred
check requires matching records and live claim at commit. Proofs expire with the
transaction and cannot authorize later work.

The Java plan encodes outside locks under a shared byte reservation. It compares
fresh owner, attempt and upload identities with the saved predecessor preparation,
whose digest is checked in SQL. This is registration, with current source READ
checks; it does not retain a prior schema verdict as current or authorize provider
work. Current WRITE/policy/schema/placement rechecks remain an execution prerequisite.
The old attempts, retained definitions and cleanup records are untouched.

### V94 successor execution identity

The database can bind an installed successor to its exact claim epoch/token,
coordinator incarnation, owner generation/nonce, command, preparation and modes.
Activation and the coordinator binding must commit in one transaction, after the
V93 install commits. Both leases must remain live through activation commit.
An activation record is immutable and is not a cached authorization or schema
verdict.

Only the current matching grant opens the execution fence. New assessment starts
and content attempts additionally require that this successor has not begun its
admission drain. Its admission drain permits settlement; its local-drain marker
closes execution. Preparations, modes and later owner generations still require
the separate installation protocol. A low-level transfer to another claim epoch
cannot inherit an earlier grant. Resume now selects the exact epoch binding and
cannot treat an operation with older bindings as an unbound operation.

This is a private SQL boundary. The private activation/session path below checks
current WRITE and source access and confirms exact activation after uncertain
commit acknowledgments without lease renewal. Public hosts do not activate
successors automatically. Schema/policy/placement rechecks remain at the existing
execution boundaries. Delayed predecessor provider effects and
cleanup isolation require separate qualification before automatic recovery.

### Private activation transaction

`RepositorySuccessorExecution.activate` confirms the exact V92 handoff and V93
install before inserting V94 and its coordinator binding in one transaction.
Preparation hashes use the existing codec; modes use the shared install encoder.
Encoding and authorization-plan construction happen outside SQL locks under a
shared byte reservation. SQL locks the current claim and owner before document
locks. A current revision or authorization failure rolls back both new records.

The host supplies two trusted identities: the process coordinator authorizes this
private operation, and the execution caller supplies current document permissions.
Both must match the operation principal and account. Passing a process caller for
execution is an explicit administrative bypass; a scoped caller never inherits the
coordinator's bypass. Scoped creation still needs the separate creation-grant work.
Normal preparation continues to reject claimed historical reuse before this path.

Successful return confirms that the activation committed. It does not return a
session or renew a lease. An exact retry can confirm the same fact after permission
revocation, drain or expiry. Actual execution must reacquire live fences and check
current policy, schema and placement. On a lost commit reply, only the exact saved
activation and binding resolve the failure; cancellation that prevents readback
preserves the original exception.

Public hosts are not wired to activate or resume successors yet. Late provider
effects and cleanup isolation remain required. The byte reservation
currently uses the maximum encoded preparation bounds (33 MiB per call), not exact
payload size; it is a conservative admission bound, not measured heap consumption.

Session integration attaches to the already installed owner and claim. It does not
call the ordinary recovery session's owner-takeover method, which would request
another generation. Registration and journal access check the exact claim epoch
and predecessor generation. Successful activation readback stays separate from
live claim/owner acquisition so expired or revoked work cannot resume from a receipt.

### Private successor session attachment

`DocumentPublicationSession.successor` now retains the installed preparation,
owner nonce, attempt and upload IDs, modes, claim epoch/token and coordinator
incarnation. It never mints replacement identities or calls initial acquisition,
mode insertion or owner takeover. Its registration is marked potentially committed
from construction, so a failed attachment cannot make the session discardable.

`RepositorySuccessorExecution.attach` verifies the exact activation, acquires the
current claim and terminal-aware owner, and checks current document permissions
and revisions. It does not renew either lease. Registration then loads the saved
modes through journal access bound to the exact epoch and predecessor generation.
An existing durable assessment start sets the session's sticky started state;
execution uses the existing assessment resume path rather than fresh CREATE.

Attachment uses the registration barrier and exposes the exact successor drain
identity, including after a failure. Pre-owner registration abandonment is not valid
for an installed successor; callers must use owner cancellation instead. Automatic
provider recovery remains unfinished. No public host factory
selects this successor session automatically.

### Manager-owned successor activation

The private `DocumentPublicationSessions.activateSuccessor` holds an active call
and the registration barrier across preparation, reservation, activation and live
attachment. It constructs the side-effect-free session outside the manager lock,
then publishes it with its exact drain identity under that lock before V94 SQL.
There is no observable successor placeholder without a session. Capacity refusal
therefore precedes activation; preparation failure cannot leave a new V94 grant.

Retries compare the full handoff proposal and fixed-size digests of both encoded
preparations and modes. Ordinary initial sessions cannot be substituted. During
activation the retained entry is unavailable to ordinary execution. After an
uncertain failure it remains retained for exact retry and shutdown reconciliation.

Shutdown can close the inner attachment admission after accepting the outer call.
The activation may have committed even though attachment returns cancellation or
UNAVAILABLE. The outer barrier prevents a premature drain snapshot, and the retained
session supplies the exact successor identity for V90/V91 reconciliation. This is
not a promise that accepted activation always completes, or proof of provider
quiescence. Full recovered provider publication and late predecessor effects remain
separate qualification requirements.

### Graceful successor provider qualification

The production-JAR `JournaledSuccessorPublicationProbe` exercises a scoped caller
updating an existing writable document. The original manager performs real uploads
and a retained-source read, then an injected resolver failure interrupts it before
publication. The probe retains the exact preparation and verifies the original
provider versions and hashes. It releases local work and SQL reader pins before
V91, waits for actual claim/owner expiry, and uses a fresh manager for V92/V93/V94.

The successor publishes through the real versioned provider and runtime validation.
Its selected attempt and physical objects are distinct, its revision references
those objects, and its Any descriptor binding matches the selected definition.
The predecessor's attempt/object/cleanup rows and readable provider versions remain
unchanged. Exact receipt replay performs no provider or resolver work.

This is graceful local quiescence with shared SQL and object storage. The predecessor
process is not killed; its provider handle remains open but locally drained during
handoff. This does not establish abrupt-death recovery, a delayed remote PUT after
local drain, multi-replica throughput, or automatic public-host recovery. The probe
uses LocalStack for correctness; it supplies no RustFS performance result.

### Delayed provider request transport seam

The test-only `DelayedS3PutGateway` buffers one bounded, signed HTTP PUT without
forwarding it. Its SDK invocation must time out and its inbound handler must drain
before the original request is sent to LocalStack. It preserves raw target, Host,
signed headers and payload, verifies the supplied payload checksum, rejects
unsupported framing and any second request, and observes the backend's actual
status and version. This is an intermediary holding a request, not a repository
worker paused before calling storage.

The test proves absence before forwarding, then reads the exact new provider
version. `S3ObjectReclaimer` observes absence once before arrival and removes the
late version on another pass without deleting a prefix-sharing neighboring key.
This physical adapter evidence does not establish SQL tombstone retention,
automatic retry policy, arbitrary SDK framing, or successor isolation. The fixture
explicitly uses one SDK attempt, a bounded non-streaming signed body and HTTP.
Production provider configuration is unchanged. Repository composition must retain
one exact registered backend identity across the original and successor paths.

### Graceful successor with a delayed predecessor PUT

`DocumentSuccessorLatePutIT` composes the transport seam with real PostgreSQL and
LocalStack. The proxy leaves the SDK endpoint and registered backend identity
unchanged. It captures the original signed request, observes an actual SDK timeout,
and waits for both local drain records and closure of the old client. After natural
lease expiry, a second manager publishes through V92/V93/V94. SQL-driven recovery
first records ABSENT; forwarding then creates a real provider version. A second
recovery pass deletes it while retaining the tombstone. The predecessor object has
no verification, provider-version binding or revision reference. The successor's
exact bytes and receipt remain readable.

The fixture uses admin authority and opaque admission. It does not establish
abrupt-death recovery, automatic public-host recovery, additional scoped/typed
coverage, arbitrary request framing or retries, or performance. Existing scoped
typed publication evidence is recorded separately.

### Undrained coordinator and crash recovery boundary

The V94 mutation guard required successor activation only when a local-drain record
existed. Inspection and a PostgreSQL regression found that a bound coordinator
without V90/V91 records could transfer an expired claim through the low-level
primitive and pass the general mutation fence. V95 closes that path: once any
coordinator binding exists, general mutation requires the exact original epoch-one
binding or a live exact successor execution grant. A new claim token alone cannot
authorize work. Initial registration and explicitly unbound legacy operations keep
their existing behavior; V93's atomic installation exceptions remain narrow.

This correction is a prerequisite, not abrupt-death recovery. The next protocol
must distinguish transfer of publication authority from proof that remote work has
stopped. A suspected-dead host may be partitioned or paused and later resume.

- A crash reservation must record its own reason and exact predecessor identity;
  it must never fabricate V90/V91 local-drain evidence. Serialize expiry, terminal
  outcome and replacement decisions under the claim-before-owner lock order.
- Preserve V93/V94's fresh owner nonce, attempt identities, preparation/mode binding,
  current caller authorization and exact-retry behavior. A recovery path must not
  activate through an ordinary low-level claim takeover.
- Late external writes can remain possible. Immutable attempt-specific keys,
  stale SQL rejection and retained cleanup tombstones must isolate them from the
  replacement. Test a paused old host resuming as well as a terminated process.
- Reader lifetime is separate. `DocumentReadRecovery` and
  `DocumentAssessmentReadRecovery` require QUIESCED; V32 permits only LOCAL_DRAIN
  evidence. Claim expiry cannot release these pins. Recovery must retain and report
  unresolved pins until a separately specified, authenticated mechanism proves
  the exact reader incarnation cannot resume. Coordinator and reader incarnations
  must not be assumed to be interchangeable.
- Pre-owner generation-zero preparations need their own transition. V93 requires
  an existing expired owner and cannot overwrite or reuse the occupied initial
  preparation and seeds. Preserve this gap explicitly.

Required crash tests include competing replacements, rollback and lost commit
acknowledgment, stale owner renewal/verification/publication, current permission
revocation, delayed provider effects, retained read/schema evidence, and a real
process termination followed by restart. A paused-but-live predecessor is required
to establish fencing independently of operating-system termination. Automatic host
recovery remains unavailable until the protocol and those cases are implemented.

#### Shared reservation foundation

V96 introduces `repository_coordinator_reservations` as the immutable identity
referenced by successor installation. Existing V92 handoffs are backfilled and new
handoffs publish their reservation in the same transaction as claim transfer. The
parent insertion guard requires exact matching source evidence; a caller cannot
insert a reservation alone. V92 retains its original local-drain foreign key and
validation. V93 retains a declarative foreign key, now targeting the common parent,
and compares the exact reserved successor tuple before installation.

V96 accepts only GRACEFUL. The predecessor's remote state is UNKNOWN even
for a graceful reservation: local drain cannot prove that a buffered provider write
will never arrive. Adding EXPIRED_UNQUIESCED requires a separate immutable source
record and validation path, not a relaxed V92 check. That path must bind the exact
expired owner generation/nonce as well as the expired coordinator claim and
recheck both under their locks. A V90-only interruption can use that future path;
a V91-attested predecessor uses the graceful path.

The subsequent Java change will give installation and its retry fingerprint a
common proposal including reservation kind. Graceful and uncertain reservation
entry points remain distinct. No schema-worker or reader lifetime is released by
either reservation, and no automatic recovery is enabled by V96.

Use distinct common-proposal variants rather than optional owner fields:
`Graceful` retains the existing handoff proposal; `ExpiredUnquiesced` requires the
previous owner generation and nonce. Both expose the exact coordinator and successor
tuples. Kind and old owner must participate in plan/fingerprint equality and exact
reservation confirmation. The future parent extension must require owner fields for
the uncertain kind and forbid them for graceful records. V93 must compare those
fields to the locked predecessor owner and retained preparation; Java plan creation
must make the same comparison before SQL.

A fresh recovery process cannot load the previous preparation using an expired
claim: `DocumentPublicationPreparationJournal.load` correctly requires a live
identity. Discover bounded command/identity metadata under private authority, reserve
the replacement with authoritative SQL checks, then load the retained predecessor
preparation under the exact live successor claim. Do not widen the loader to accept
expired claims or treat a historical reservation confirmation as permission to run.
Attachment must also confirm the reservation kind and old-owner fields, alongside
its existing live claim/owner and current authorization checks. V93/V94 readback
alone does not currently encode the proposal variant; manager fingerprint equality
is necessary but does not replace this durable attachment check.

#### Expired, unquiesced SQL reservation

V97 adds the private `repository_coordinator_expirations` source. Its insert locks
the exact current claim and then the operation owner, requires both leases expired,
checks the original coordinator binding and exact owner generation/nonce, rejects
terminal operations, and refuses a predecessor with current-epoch V91 evidence.
A V90-only predecessor is eligible. It transfers the claim and publishes the common
EXPIRED_UNQUIESCED reservation atomically, retaining UNKNOWN remote state. It never
records local drain, stops a remote process or releases a reader pin.

The parent now requires owner identity for this kind and forbids it for GRACEFUL.
V93 compares that identity to its proposed predecessor owner in addition to its
existing locked-owner and preparation checks. V95 still refuses general execution
without V94 activation. The common Java proposal and session attachment are
described below. This private SQL primitive alone is not an available automatic
recovery feature.

#### Replacement failure before activation

V92/V97 transfer the claim before V94 creates its coordinator binding. If the
replacement dies in that interval, V97 cannot transfer the next epoch: its required
current-epoch binding does not exist. Do not invent a binding or recycle the dead
replacement's incarnation to bypass that condition.

An explicit supersession protocol remains required for unactivated reserved epochs:

- Reservation-only: the old owner generation and preparation still exist; no V93
  installation has committed.
- Installed but not activated: V93 has advanced the owner and saved fresh
  preparation/modes, but there is no V94 execution grant or coordinator binding.

After exact claim expiry, determine the committed state under claim-before-owner
locks and bind a new proposal to the corresponding owner/preparation. Preserve
previous reservations and distinguish them from local-drain evidence. Use fresh
coordinator and eventual attempt identities. Tests must kill the replacement in
each window, include lost acknowledgments and competing third coordinators, and
prove stale participants cannot mutate or publish. Automatic recovery is incomplete
until these windows have a qualified path; V97 deliberately remains closed there.

#### Common Java reservation and attachment

`RepositoryCoordinatorReservation.Proposal` has sealed Graceful and
ExpiredUnquiesced variants. The expired variant requires owner generation/nonce;
plan construction compares both with the retained previous preparation. The full
variant participates in the manager's retry fingerprint. Installation and
activation confirm the corresponding immutable reservation, and the activation
read used during attachment checks it again under the live claim transaction.
Current caller authorization and live owner checks remain separate.

Graceful confirmation reads the canonical V92 child, which cannot represent an
expired reservation. This also permits migration fixtures to construct the older
graceful state before upgrading. Expired confirmation reads its immutable parent
kind, UNKNOWN remote state and required owner tuple, and requires its source row.
There is no exception-based fallback between kinds.

`RepositoryCoordinatorExpiration.reserve` requires private process authority. It
uses a plain insert and exact confirmation after uncertain acknowledgment. A later
exact confirmation may report a committed reservation after expiry but cannot renew
its lease or authorize execution. Cancellation after commit remains visible; a
separate retry can confirm the saved fact.

Java/SQL integration covers reservation, loading the old preparation under the
live successor claim, installation, activation and exact attachment. Automatic
host recovery and recovery from an unactivated replacement's death remain required
alongside reader/pin lifecycle handling. A separate process-death qualification is
described below.

#### Unquiesced replacement with a real delayed provider effect

`DocumentSuccessorLatePutIT` now runs both graceful and expired-unquiesced cases
against PostgreSQL and LocalStack. In the uncertain case, the first manager remains
open after its real SDK call times out. Its provider activity is zero before closing
the SDK; the proxy still holds the original signed request. No epoch-one V90 or V91
record exists. After natural claim, owner and attempt expiry, a second manager
reserves through V97. Retrying the old manager fails at its stale claim.

The replacement loads retained preparation under its live successor claim and uses
the ordinary installation, activation and manager publication path. SQL cleanup
first observes absence; the proxy then forwards the actual old PUT. The old object
remains unverified/unreferenced, another cleanup removes its version, and the
replacement's exact bytes and receipt survive. Test cleanup releases local drained
read handles without asserting coordinator drain or reader QUIESCED.

This is a fresh manager in the same JVM, using admin authority and opaque CORE
data. It does not prove process death, a provider SDK call still active at transfer,
scoped typed uncertain recovery, reader/schema worker quiescence, or performance.

The general preparation loader still rejects a pre-V94 read after V91 local drain.
The separate reserved read authority below now serves both graceful and expired
reservations. The execution fence remains unchanged.

#### Writer process death and public-request retry

`DocumentPublicationProcessRecoveryIT` starts a writer JVM against PostgreSQL and
versioned LocalStack. Its test-only store delegates a real PUT, then holds the
return before the repository records verification. The parent independently reads
the exact bytes from the provider and observes the unverified SQL object before
SIGKILL. It reaps the writer (exit 137) before starting the recovery JVM.

Both JVMs receive only the public intent and raw part payload. The replacement
validates the payload digest, discovers private predecessor identities from SQL,
waits actual claim/owner expiry, reserves V97 and loads retained preparation under
its live successor claim. V93/V94 then install and activate fresh identities before
ordinary manager execution publishes. Assertions cover a distinct attempt/key,
exact versioned successor bytes and revision binding, no old revision references,
an unchanged unverified predecessor object, receipt replay without BlobStore calls,
no old V90/V91 markers, and unchanged ACTIVE predecessor reader incarnations.

This is client resubmission after a completed provider write, using admin authority
and opaque CORE data. It is not automatic recovery, payload reconstruction from
orphan objects, a late remote PUT, a scoped typed case, or proof that old pins can
be released. The worker now uses the private production discovery API below;
host lifecycle integration remains unfinished. The
separate late-PUT test qualifies delayed effects and tombstone cleanup. Neither
LocalStack test establishes RustFS throughput or horizontal scaling.

#### Exact-operation recovery discovery

`RepositoryCoordinatorRecoveryDiscovery` reads one operation key and expected
command digest under private process authority for that operation's principal.
Its single fixed-size query uses existing keyed indexes and explicit SQL lock and
statement timeouts. It reads identity and journal metadata, not command bytes,
payloads or receipts. Cancellation and database errors remain visible.

The result distinguishes absent/unclaimed, terminal/abandoned, unbound, pre-owner,
unactivated reservation/install, local drain, missing journal, exhausted generation,
and live states. Structural states take precedence over lease liveness. A bound
coordinator with matching preparation/modes and both expired claim and owner
returns an `EXPIRED_BOUND` observation containing exact private identities with
redacted diagnostic strings.
All expiry comparisons use one database statement timestamp. An admission drain
(V90) alone does not imply the local-drain evidence required by V91.

Discovery neither locks ownership nor renews it. Observations can immediately
become stale; V97 must recheck the exact tuple under claim-before-owner locks.
An observation never authorizes execution or pin release. The process recovery
test uses this API instead of a test-only private-identity query. This is an
internal exact-operation building block, not a public endpoint, fleet scanner,
automatic retry scheduler. V98 extends it with metadata for unactivated replacement recovery.

#### Preparation reads without an execution fence

`RepositoryReservedPreparation` accepts private process authority, the current
execution caller, an exact committed reservation and an expected expired owner.
It locks claim then owner, confirms the exact live reserved successor and source
reservation, and refuses current-epoch installation, activation or coordinator
binding. It reads only the owner's preparation generation, with byte accounting
reserved before fetching it. SQL lock and statement timeouts are mandatory.

Decoding and checksum validation happen outside SQL locks using the same integrity
checks as the ordinary preparation journal. Before delivering the borrowed record,
the loader locks and rechecks the reservation/claim/owner and immutable hash, then
checks current read access for the host-supplied execution caller. Process authority
for bootstrap does not substitute for that caller's document access. Missing
preparation is an explicit failed precondition. All failure paths release the byte
reservation; a successful caller retains it until closing the borrowed value.

No write fence is stamped and no lease is renewed. V91 and the ordinary loader's
guards are unchanged. V93 installation and V94 activation still perform their own
checks. Both real delayed-provider variants now reload this way, and the scoped
typed production-JAR successor probe uses it for graceful recovery. These tests
do not establish an automatic host or a fresh-process graceful discovery path.
The separate process-death suite now covers unactivated replacement recovery for
admin/opaque publication, including deaths after reservation and installation.

V98 adds an immutable supersession source distinguishing reservation-only from
installed-but-unactivated state. Under claim-before-owner locks it verifies the
exact current parent successor identity, expired claim and owner, expected install
phase/hashes, and absence of current binding/activation before advancing one epoch.
A reservation-only retry loads the old owner's preparation; an installed retry
loads the installed owner's preparation. Older install rows remain intact. The
canonical child retains phase and preparation hashes; the common reservation
parent records the new successor and current owner. Discovery supplies the exact
proposal metadata but does not authorize execution.

The focused SQL suite covers both graceful and expired origins, repeated
replacement deaths, V93/V94 rollback races, competing proposals, uncertain
acknowledgments, immutable evidence and atomic parent-insertion failure. No
coordinator binding or local-drain evidence is fabricated. Automatic orchestration
and recovery of reader pins remain separate work.

## Managed recovery integration prerequisites

The first managed integration should recover an exact retried operation using the
authenticated caller, canonical command and complete resubmitted upload payloads.
It must not infer credentials from command ownership or infer missing payloads
from unverified predecessor objects. Fleet discovery and recovery without a client
retry require an explicit durable payload source and authenticated execution
identity source; neither is supplied by the preparation journal.

Recovery authority is a separate trusted host input from shutdown `DrainAuthority`.
Private discovery classifies the exact command, but only eligible expired bound
or unactivated states can enter V97/V98. Live, terminal, absent, missing-journal,
pre-owner and gracefully drained states need explicit handling through their own
protocols, never a general takeover fallback. Current host drive/backend gates and
the current caller's rights apply through loading, activation and execution.

The reserved loader must return both preparation and fixed modes. Capture both
under the exact reservation and owner, reserve their bounded bytes before reading,
decode outside SQL, then recheck identity and current authorization before delivery.
Missing, malformed, oversized or owner-mismatched modes cannot default to OPAQUE
or be reconstructed from the payload. A retried caller's modes must agree with
the retained modes before successor installation.

Before any reservation mutation, a bounded host entry must retain the proposed
successor token/incarnation. Before installation it must retain the complete plan,
including its fresh owner, attempt and upload identities. An uncertain reply
retries those exact identities; it does not mint another proposal. Current session
retention begins at activation, so it does not yet cover these earlier phases.
Capacity and shutdown must cover that entry and its bytes from creation through
transfer to the session manager or confirmed terminal cleanup. Process death uses
the explicit expired unactivated supersession protocol instead of local guesses.

After activation, use the ordinary scoped execution path and its existing schema
worker lifecycle. Neither lease expiry nor successor publication proves old
readers or remote provider effects have stopped. Pin reclamation and physical
cleanup remain independently fenced. This integration plan does not advertise an
automatic recovery scheduler or a new public publication RPC.

V101 also enforces mode continuity in SQL at installation. Every successor mode
hash must equal the exact predecessor generation/owner's retained map. Migration
refuses pre-existing inconsistent installations instead of rewriting them. A
current policy change may reject the retained mode; it cannot silently change
that operation's meaning. This is separate from bounded host proposal retention.

### Private recovery-attempt owner

`RepositoryRecoveryAttempts` now owns a bounded map before reservation mutation.
Each entry binds the exact command, process/scoped identity and credential binding,
one reservation proposal and, once minted, one installation plan. One active handle
per entry runs SQL outside the shared map monitor. Every advance receives fresh
coordinator and execution callers; account/group permissions are not cached as
grants. The original proposal and plan survive cancelled post-commit replies.
The owner applies configured SQL timeouts to reservation, loading and installation,
including confirmation reads. Activation and attachment use the session manager’s
separate transaction view; host construction must supply a bounded view there too.
The private owner constructor alone does not enforce that session requirement.
The retained preparation/modes and next preparation count against the shared byte
budget. Successful activation transfers ownership to the existing session manager,
clears the attempt's borrowed data and releases its leases.

This component is private and is not wired to a managed host. It handles eligible
expired bound and unactivated observations, not graceful-discovery proposals.
Cached preparation is not delivered again on installation retry: process-authorized
V93 metadata installation may proceed after execution-caller rights change, while
V94 activation and attachment recheck live execution rights before provider work.
Installation alone is not an execution grant.

Closing admission blocks new handles but permits accepted handles to finish.
Drain reports active calls separately from unresolved identities; it is neither
provider-quiescence evidence nor durable completion. Unresolved entries retain
their budget leases until an explicit transition settles them. Retained unactivated
supersession and claim-fenced owner retirement are described below. Terminal and
graceful reconciliation, plus trusted host lifecycle integration, remain required
before factory exposure. Closing this private owner is not resource-complete shutdown.

### Next lifecycle requirement: reconcile both local owners

An activation call can retain a session-manager entry before V94 commits. If that
call fails, superseding the recovery attempt alone would leave the old manager
fingerprint occupying the same operation key. Before host wiring, reconciliation
must account for both entries, with exclusive per-operation access and no shared
monitor held across SQL.

Distinguish local identity retirement, durable operation completion and provider
quiescence. A never-submitted proposal may be discarded locally. After a submitted
reservation has an uncertain outcome, serialize against the exact claim transaction
and inspect durable reservation evidence before deciding whether identity can be
released. Expired unactivated proposals require V98's exact phase, owner and install
hash checks. A committed V94 activation instead belongs to the bound-coordinator
protocol; it must not be treated as an unactivated reservation.

After a durable supersession, an old manager entry needs exact fingerprint matching,
zero active users and confirmed claim transfer before retirement. A committed
activation can transfer local ownership to the retained matching session, but
attachment still requires current authorization. None of these transitions permits
reader-pin release or deletion of unverified provider objects. Shutdown must report
local idleness and unresolved durable work separately from completed reconciliation.
These are design requirements; recovery-attempt entry reconciliation is not
implemented yet. Existing session retirement is described below.

The session-retirement primitive already exists: `retireSuperseded` exclusively
borrows the local entry, then checks its exact owner generation/nonce and command
under the SQL owner lock. It can be reused after V93 installs a replacement owner.
V98 reservation alone advances the claim but leaves that owner unchanged, so it
cannot satisfy this retirement check. A new fingerprint-retirement API is not
required for this path. Recovery-attempt lease reconciliation remains separate.

A further host-composition constraint is now explicit: reservation constructors
and SQL require a different successor incarnation. A session manager owns one
fixed incarnation, so it cannot directly supersede its own expired reservation.
Do not rotate that shared identity while other sessions still use it or relax the
SQL guard as a fallback. Automatic same-host recovery requires a reviewed lifecycle
for a fresh manager incarnation, or a separately designed per-operation incarnation
model. Existing two-coordinator supersession remains supported. This constraint
must be resolved before claiming automatic same-host recovery.

### Per-operation successor targets

The private manager now mints a `SuccessorTarget` containing its owning manager,
exact operation key, command digest and fresh incarnation. The bounded recovery
entry retains that target with its proposal before reservation SQL. The manager
keeps no separate target registry. A target grants neither a claim nor execution:
activation still requires trusted coordinator authority, the current execution
caller, the exact installed plan and all existing SQL fences.

Target activation rejects a foreign manager, different key/digest or different
incarnation before retaining a session or mutating SQL. The initial manager
incarnation stays fixed. A successor session retains its target incarnation in
its registration, so shutdown snapshots and local drain use the exact per-session
identity. The existing fixed-incarnation activation overload remains available for
its original callers. Neither Java nor SQL freshness guards are relaxed.

The private fixed-incarnation `resumeStarted` path remains restricted to its
original incarnation. It cannot adopt a target-bound owner by substituting a UUID
read from SQL. Existing target sessions retry through their retained registration;
a separate target-bound restoration API would require explicit lifecycle review.

This addresses incarnation selection for same-manager successor preparation, not
complete automatic recovery. A superseded session still requires durable owner
replacement and explicit retirement before a new target can occupy its operation
slot. Per-entry replacement and claim-fenced retirement are described below;
terminal reconciliation, scheduler, cleanup and resource-complete shutdown remain open.

### Retained unactivated supersession invariants

An explicit recovery-attempt supersession step must first recheck process authority
and the retained execution-caller identity. Fresh bounded discovery must identify
an expired unactivated predecessor equal to this entry's exact successor key,
digest, epoch, token and incarnation. A different winner or an activated coordinator
is not eligible for this transition.

Mint and retain one pending target/proposal before V98. While pending, ordinary
advance must refuse; retries must use that exact tuple rather than rediscovering
and minting another identity after an uncertain acknowledgment. Only after exact
V98 confirmation may the entry replace its old proposal/target, release borrowed
preparation and plan leases, and resume at RESERVED. Keep its command lease. The
retained SQL preparation and modes remain the source for the next installation.

After that installation permanently fences the old owner, use existing session
retirement before activation. An active local session must leave the attempt
INSTALLED and retryable. A false retirement result is not itself a success proof:
the existing activation fingerprint check must continue to reject a mismatched
retained session. Never interpret committed V94 with a lost reply as unactivated
V98 work. Host retry policy needs a finite attempt limit or deadline; a single
pending proposal bounds concurrent state, not the total number of generations.
The implementation and its verification boundaries are described below.

### Retained unactivated supersession implementation

`Attempt.supersedeExpired` now implements the reviewed single pending transition.
It accepts only fresh discovery of this entry's exact expired, unactivated successor.
It retains the replacement target/proposal before V98, refuses ordinary advance
while that proposal is pending, and confirms the same tuple on retry. Only a
confirmed V98 commit replaces the current proposal and releases its borrowed
preparation/plan; the command lease remains held. This is an explicit single step,
not an automatic loop or a grant to discard provider state.

After the new V93 installation, ordinary advance calls existing session retirement
before activation. An old mismatched or active session cannot be overwritten:
retirement's exclusive-use check and activation's fingerprint check remain in force.
An already activated coordinator must use its bound-coordinator recovery path,
even if its acknowledgment was lost and its lease has since expired. A different
coordinator's winner cannot be adopted as this entry's own successor.

Automatic host orchestration, graceful and terminal reconciliation, removal of
entries fenced by a foreign winner, shutdown coordination and retry policy remain
unfinished. Local closure may therefore still report unresolved entries with
retained byte leases. No reader pin or provider object is reclaimed by this step.

### Claim-fenced recovery-owner retirement

`Attempt.retireFenced` releases only this recovery owner's local identity and byte
leases. A bounded READ COMMITTED transaction locks the exact claim, checks its
digest and stored command, and requires the current claim to exclude both the
retained proposal and any pending replacement. Each proposed successor must be
at an earlier epoch, or at the current epoch with a different token. The latter
covers a proposed token that never committed because another coordinator won.
V78 prohibits reversing epochs, changing an existing token within its epoch, or
deleting a claim, so a successful fencing observation remains true after unlock.

An absent claim, earlier epoch, matching token or lease expiry alone is insufficient.
Cancellation or failure of the readback preserves the entry and its leases. After
a successful readback, the method marks RETIRED, removes the owner entry and releases
its borrowed bytes. Accepted handles still count as active until closed, including
when admission has already closed. Retried retirement is idempotent; advancing a
retired handle is an error.

A separately retained session is not removed. In particular, foreign V98 can fence
this claim before V93 changes the owner, leaving the old manager session retained.
That manager's existing owner-based retirement correctly remains false. The host
must still account for that session and its drain identities. Recovery-owner drain
counts cannot stand in for manager, worker or provider drain. Terminal/graceful
reconciliation and complete host lifecycle integration remain unfinished.

### Claim-fenced journaled-session cache retirement

`DocumentPublicationSessions.retireClaimFenced` now derives the exact claim tuple
from the retained registration and invokes the same `RepositoryClaimRetirement`
proof as the recovery owner. It requires private process authority and exclusive
local use of the cache entry. Unjournaled or never-submitted registrations supply
no identity; restoration-only entries are not handled by this method. Existing
owner-based retirement keeps its previous semantics.

After a foreign claim transfer, this explicit method can free a journaled session's
local slot even when the owner has not changed. The proof runs outside the manager
monitor. Cancellation or SQL failure retains the session and command capacity.
The method does not touch attempts, reader pins, provider tombstones, worker owners,
or V90/V91 markers. A caller must not treat an empty cache as worker quiescence.

The helper uses the supplied transaction view. Managed service construction supplies
a bounded view; arbitrary private/test journaled factories are still responsible
for doing so. No universal timeout guarantee follows from this helper alone.

This method accepts calls only while the manager is open. It cannot change an
already captured shutdown snapshot. Shutdown instead uses the separate identity
classification below, while continuing to drain local uploads, reads and schema
workers. Preserve UNKNOWN remote state and keep physical reclamation independently
fenced; local cache removal cannot supply either proof.

### Shutdown classification after claim transfer

`RepositoryShutdownClaim` now classifies retained identities separately from
DRAINING and LOCAL_DRAINED; neither claim expiry nor an exception proves completion.

Keep the immutable, identity-only registration snapshot across retries and cache
eviction. Re-resolve private process authority on every retry. A bounded SQL
transaction locks the current claim and checks the original key, command digest,
epoch and token. A missing claim remains unresolved; a changed digest is an error.
Permanent token exclusion is required before considering either fenced outcome:

- **Never bound:** no immutable coordinator binding matches the retained token and
  incarnation. Initial registration and V94 activation commit their binding
  atomically with authority. Once the claim permanently excludes that token, this
  registration cannot subsequently become active. A different winner's binding
  must never be adopted as this registration's identity.
- **Previously bound:** the original binding exists. Require exact immutable
  reservation evidence for its first handoff, including predecessor identity and
  digest. An unreviewed low-level claim transfer is not a reviewed shutdown handoff.
  Verify the reservation successor against the current claim or subsequent durable
  handoff chain; an inconsistent or incomplete chain remains unresolved.

Registration draining first classifies the current claim. A proven fenced identity
is counted separately, even if it has an old V90 marker. Unresolved identities keep
shutdown pending; current identities use exact V90 confirmation or insertion. SQL,
control and integrity failures propagate. A takeover racing marker insertion can
therefore fail that call; the next shutdown call takes a fresh proof. No broad
exception handler converts errors into successful shutdown. An expired current
claim without V90 still fails admission closure until a reviewed transfer resolves
it; expiry alone does not produce a fenced classification.

After session, scope, upload, read and external schema workers stop, final
attestation must inspect every snapshot identity again. Confirm exact existing V91
first. Otherwise accept a proven fenced classification without inserting V91, or
record V91 only for the original claim with exact V90. V97 can transfer a claim
between V90 and V91, so registration-phase classification is insufficient. A race
while inserting V91 requires fresh proof or propagation, never a silent fallback.

Real-database coverage now includes a failed initial registration losing to another
epoch-one token; failed activation followed by V98; V90 followed by V97 before V91;
an unreviewed bound transfer; a complete two-hop chain followed by an unreviewed
third hop; cancellation after proof commit; and existing snapshot retries after
cache eviction. The recursive SQL chain check returns one boolean to Java and
uses the supplied SQL timeout; it is linear in history length, not a fixed-memory
or fixed-work guarantee. Long-history qualification remains outstanding.

The packaged `FencedSchemaWorkerProbe` now holds a real Git descriptor load through
V90, natural lease expiry and V97 transfer. It invokes `managedJournaled` directly
with a short test lease; production lease defaults remain unchanged. Shutdown
stays incomplete until the worker finishes, then succeeds without V91 for the
fenced predecessor. Exact successor claim and UNKNOWN reservation state remain
unchanged. The test exercises real versioned provider uploads and retained reads
before schema resolution. It qualifies managed-runtime worker gating, not process
death, remote PUT quiescence, or complete recovery-owner integration into the host.
This work grants no physical reclamation right.

### Retained retry inputs before managed-host integration

`RepositoryRecoveryAttempts.resume(caller, command)` reopens only an existing
local entry. It performs no SQL or discovery and cannot mint a proposal. Missing
entries return empty; closed admission, a different canonical command or caller
identity, concurrent use and exhausted call capacity remain explicit errors.
`begin` uses the same synchronized lookup before considering a supplied discovery
observation, so a retained attempt wins even when that observation is stale.

The private `advance` contract now requires the resubmitted mode map. Modes are
outside the canonical publication command. The map is bounded before copying,
then validated by the existing mode encoder for exact member keys and byte size.
Malformed shape fails before SQL. After loading immutable retained modes, a valid
but different map fails with FAILED_PRECONDITION before V93 installation; the same
comparison occurs before V94 activation. The proposal and retained preparation
stay available for a corrected retry. Cached preparation is not a fresh caller
authorization grant; activation still applies its existing live authorization.

An acknowledged activated handle retains only its bounded fixed-mode map for
idempotent comparison until handle closure, then clears that reference. The
recovery entry and its byte leases are released as before. This is an internal
prerequisite for authenticated client-retry integration, not a new public endpoint
or automatic scheduler. Existing immutable mode storage and validation semantics
are unchanged; this adds a comparison with the prospective host retry's inputs.

### Post-close disposal of unactivated successors

Managed recovery integration exposed a distinct shutdown state: a V93-installed
successor whose V94 transaction rolled back can remain current and unbound. It
cannot create V90, and waiting for an external V98 is not a usable local shutdown
protocol. The real-database regression is recorded in
[unactivated shutdown evidence](../evidence/repository/2026-10-06-unactivated-shutdown/README.md).
The manager classification, revalidation and private recovery-owner disposal
described below are implemented. Managed-host integration remains outstanding.

Close recovery admission first, then wait for every accepted recovery handle.
Close publication and registration admission and complete the registration barrier.
The manager retains the complete successor identity before V94 SQL. Its immutable
shutdown snapshot retains the fixed-size successor fingerprint alongside that
identity, without another command or preparation copy.

An explicit local `DETACHED_UNACTIVATED` outcome can dispose of this retained
context after proving the exact current reservation, V93 installation, installed
owner and command and observing no V94. It is limited to a manager-owned successor
session and its fingerprint, not arbitrary unbound claims. Retain the outcome in
the closed manager across retries. It is neither permanent SQL fencing nor a
V90/V91 marker, and changes no claim, reservation, attempt, pin or UNKNOWN state.

Before releasing shared resources, still wait for session calls, uploads, reads
and external schema workers. Revalidate each detached identity: unchanged
unactivated SQL state or its exact late V94 activation is compatible with local
shutdown. That late activation cannot start Java/provider work because local
admission is closed and accepted calls have ended. A fresh host can later recover
it through the existing expired-bound protocol. A reviewed successor chain is
also acceptable; unrelated tokens, changed command or incomplete chains remain
unresolved. Do not turn a transient absent-row observation into a permanent fence.

Recovery-owner command/preparation/plan leases can then be detached only after
its accepted handles have ended and manager ownership of every possible V94
registration is accounted for. This transfers retry responsibility to durable
state; it does not declare logical completion, abandonment, quiescence or automatic
execution. No same-host resume is allowed after disposal. An uncertain late
metadata commit remains recoverable through its atomic journals.

The proof compares fingerprints by reading bounded stored mode JSON under the
claim lock, then normalizing its string values and key order outside SQL. Java's
compact mode JSON digest and PostgreSQL's jsonb-text mode digest are different
encodings. The SQL digest binds the journal to the install; the normalized Java
digest binds it to the manager's retained fingerprint. Byte-budget exhaustion
reports `CAPACITY_UNAVAILABLE`, leaving drain incomplete for retry after accepted
work releases capacity. It grants no substitute proof. Strict reviewed-chain
checks apply to retained successors even before the first detached observation.
Required cases include failed V94 rollback, lost V94 reply, a late activation after
local classification, current and pending V97/V98 replies, active recovery handles,
held schema/provider work, changed fingerprint and an incomplete successor chain.
Keep the existing initial-registration absent-claim behavior separate until its
own durable handoff proof is designed.

#### Recovery-owner disposal

Close recovery admission, then use its bounded wait for accepted `Attempt`
handles, notified by handle closure. `detachClosed` captures the manager's immutable
registration snapshot before disposing of owner entries. That snapshot barrier
performs no SQL or additional byte reservation, so owner-held preparation leases
cannot prevent it from recording manager ownership.

An internal callback records the exact submitted fingerprint after the manager
retains the complete successor and before any V94 SQL. It runs outside the manager
monitor and inside the activation cleanup block. Preserve that fingerprint across
pending supersession; a later retained activation may replace it only after the
old claim was fenced by the confirmed supersession.

For each possibly submitted V94, require an exact snapshot-owned fingerprint,
permanent claim-fence evidence, or exact durable successor terminal evidence.
The terminal check binds the current claim, reservation, install, activation,
coordinator binding, owner generation/nonce, preparation hashes and fixed modes
to exactly one success or rejection at that generation. It is a private metadata
check, with no client receipt decoding or current document disclosure policy.
An abandoned pre-owner registration is not a substitute for a successor's V94.
A missing cache entry or an `INSTALLED` phase alone is never disposal proof.

Earlier metadata-only phases can release local bytes after accepted calls end:
committed V97/V98/V93 state is recoverable and uncommitted state started no local
provider work. For a submitted entry absent from the snapshot, disposal can first
release duplicate preparation graphs while retaining command, proposal and fixed
fingerprint evidence. Closed, idle owners cannot retry, and this frees their large
preparation reservations before bounded terminal inspection. Shared capacity
pressure may still leave disposal incomplete; it grants no substitute proof.

The owner serializes disposal but does not hold its monitor while consulting
Sessions or SQL. Confirmed earlier entries may be released before a later failure;
the remaining entries stay retryable through `detachClosed`, not execution. After
detachment, continue the existing session, upload, read and external-worker
barriers; owner-byte release does not establish their completion. Acceptance
must cover active handles, uncertain reservation/install acknowledgments,
attempted activation, terminal eviction, pending supersession and held workers.

#### Managed retry integration in progress

The optional `RecoveryAuthority` supplies private process authority independently
of request credentials. Managed publication holds a bounded per-operation call
guard through routing and execution. Caller-authorized replay observation precedes
local recovery lookup and private discovery; unsupported partial states produce a
generic request error. Existing local proposals are resumed before interpreting
new discovery results. Recovery requires complete resubmitted upload declarations
and immutable validation modes. Eligible recovery snapshots the complete upload
set into private arrays and checks their actual checksums before succession.
Reserve the aggregate declared bytes before allocating copies, and hold that
reservation through synchronous publication. Publication continues to make its
separate worker-owned copies; recovery peak capacity therefore includes the
snapshot plus the existing upload reservation. Capacity exhaustion cannot justify
skipping verification or using caller buffers after preflight. Fresh, live and
terminal routing do not allocate this recovery snapshot. PostgreSQL tests verify
capacity refusal before succession, and the managed provider fixture mutates
caller bytes after preflight while publication succeeds using the private copy.

An authorized terminal observation makes a retained recovery entry permanently
ineligible for execution or supersession. It releases duplicate preparation and
plan buffers before attempting exact terminal/fence disposal proof. The result
distinguishes a nonterminal operation, a terminal operation still retaining proof
identity, and a retired entry. Both terminal states return to ordinary authorized
receipt replay, never to activation. Later retries reauthorize; shutdown can use
private disposal proof without disclosing a receipt. Cancellation after the
transition preserves the reconciliation-only state.

For recovery-enabled hosts, closing first rejects new outer publication calls.
Shutdown waits for already accepted calls before detaching recovery owners and
closing nested session/provider/schema admission. The legacy non-recovery closure
sequence is unchanged. Packaged tests exercise in-process managed publication,
terminal replay, accepted calls across outer close, and recovery from a naturally
expired predecessor that failed schema selection before assessment creation.
Cancellation after terminal observation, subsequent credential/read revocation,
and managed recovery of an unactivated successor still require acceptance evidence.
Publication RPC parity and transport shutdown are not established by these tests.

Normal request availability does not depend on private recovery authority.
Already authorized terminal observations go directly to ordinary receipt replay;
any locally retained recovery entry on that path remains owned until shutdown
disposal. For a non-observed operation, an exact caller-key absence query checks
claims, operations and preparation journals before returning to initial admission.
A concurrent insert is still arbitrated by atomic admission; absence observation
never authorizes an unfenced mutation. Ownerless partial state does not take this
shortcut. PostgreSQL routing tests and the real managed-service fixture verify
fresh publication and terminal replay with an unusable recovery authority callback.

#### Retained unactivated retry follow-up

New-host managed dispatcher tests cover both reserved and installed unactivated
successors: live state is rejected, natural expiry permits one exact supersession,
and activation occurs once. These PostgreSQL cases use synthetic payloads only
for metadata routing and do not establish provider publication for this branch.

Local retained attempts now reconcile at most one unactivated supersession before
advancing. Payload preflight precedes succession. An existing pending V98 proposal
is confirmed before fresh discovery; otherwise an expired unactivated observation
must match the exact proposed successor identity. Local PROPOSED state is not proof
that V97 did not commit: a lost acknowledgment may leave durable state ahead of the
local phase. If discovery still identifies the original predecessor, the proposal
has not displaced it and ordinary guarded advancement handles the request.

Before V98, compare the retry's modes against the immutable predecessor journal,
bound by account/principal/operation, owner generation/nonce, command hash and
preparation hash. This comparison returns no journal contents, authorizes the
current caller read set, and leaves the atomic V98 identity checks intact. Invalid
modes cannot confirm a pending proposal or advance a fresh-host supersession.
Targeted tests cover mismatch followed by corrected retry and pending lost replies.
The final aggregate run passed 60 focused PostgreSQL tests and the packaged
provider regression, with no failures or skips. See the
[retained evidence](../evidence/repository/2026-10-06-unactivated-reconciliation/README.md).

An arbitrary activation failure never authorizes supersession. A committed V94
with a lost reply is bound-owner reconciliation, not unactivated V98 recovery;
that distinct retained-owner path is implemented and qualified as described below.

#### Retained activated-owner retry

The implementation passed 63 focused PostgreSQL tests and the packaged regression. While an uncertain V94 activation's
claim and owner remain live, retry the same activation and session attachment;
there is no reason to mint another proposal. Once that bound owner expires,
attachment cannot proceed under the expired lease.

Before a new V97 proposal, require exact-key EXPIRED_BOUND discovery matching the
retained successor epoch, token, incarnation and command hash, plus the planned
owner generation and nonce. Confirm the exact activation binding against the
retained fingerprint and compare the immutable publication modes. A foreign
winner is a conflict. A rolled-back activation belongs to the unactivated V98
path instead.

Retain the fresh target and V97 proposal before SQL. A lost acknowledgment retries
that same proposal. Only confirmed reservation permits replacing Loaded/plan
buffers and moving back to RESERVED. Keep the old submitted fingerprint until
permanent claim fencing or replacement activation provides exact disposal proof.
The existing V93 installation and session retirement checks precede attachment of
the next V94 owner. Reservation does not establish remote-worker quiescence;
provider tombstones, abandoned worker owners and schema/read pins retain their
independent lifetime obligations.

Acceptance must include live activation readback, natural expiry followed by
V97/V93/V94, lost V97 acknowledgment, and wrong caller, changed modes or foreign
winner with no mutation. Verify old worker ownership and provider tombstones
through the transition. The same-key call guard must cover the full managed retry
and publication; it is local exclusion, not distributed authority. Sol reviewed
this next-step design against the existing session implementation.

The live-lease readback case now has a focused PostgreSQL regression:
[live activation evidence](../evidence/repository/2026-10-06-live-activation-readback/README.md).
It confirms exact attachment after cancellation following a committed V94, with
one durable installation and activation and no additional supersession.

Implementation must represent an uncertain new V97 as a distinct retained pending
state. Retry that exact proposal before inspecting the old activation again:
the reservation may already have committed. Both ordinary advancement and the
unactivated V98 path must reject this pending state. Claim-fenced retirement must
check both the old successor and the pending V97 successor; fencing only the old
claim does not settle the uncertain new reservation. Terminal and shutdown disposal
must clear pending metadata only under their existing complete-operation or exact
ownership proofs. Capacity exhaustion or unresolved activation evidence is no
proof, and a private identity read does not replace current caller authorization.

The implementation uses the existing retained Pending holder with distinct V97 and
V98 proposal types. Both proposal identities participate in fencing retirement;
unactivated reconciliation rejects a pending V97. See the
[bounded transition evidence](../evidence/repository/2026-10-06-expired-bound-reconciliation/README.md)
for current checks and remaining acceptance cases.

Competing bound-owner recovery now has two focused PostgreSQL cases, with and
without a retained V97 whose commit failed. An independently activated winner is
not adopted after expiry; rejection and local cleanup leave its exact claim,
owner and leases unchanged. Retry metadata and session retirement are separately
asserted. See [competing-owner evidence](../evidence/repository/2026-10-06-foreign-bound-winner/README.md).
This does not qualify real provider-worker drain or revoked-caller retries.

Scoped ACL tests now cover access removal before bound reservation and before
confirmation of a committed reservation with a lost reply. Recovery returns
NOT_FOUND without changing durable identity or discarding retained state. Restored
access permits confirmation but does not erase the command's stale mutation
revision: later activation rejects it. See the
[scoped recovery evidence](../evidence/repository/2026-10-06-bound-retry-acl/README.md).
Credential and creation-grant revocation remain distinct unqualified cases for
this composed path; these tests use a scoped caller without a credential binding.

Credential-backed creation retries passed 6 PostgreSQL cases covering key revoke,
key rotation and grant revoke, before reservation and after a lost commit reply.
Invalid keys return UNAUTHENTICATED; revoked grants return NOT_FOUND. Denial
preserves durable identity, leases, proposal and retained capacity. Private cleanup
preserves SQL state and the session. See the
[credential and grant evidence](../evidence/repository/2026-10-06-bound-retry-authority/README.md).
Transport authentication, provider workers, existing-document operations without
creation grants and concurrent authority changes at commit remain unqualified.

## Managed publication transport boundary

Status: contract design for the next implementation stage. No endpoint is mounted.
Keep `DocumentService.SaveDocument` semantics unchanged. Add a separate managed
publication operation using the existing `DocumentPublicationIntent`,
`DocumentPublicationResult` and `DocumentPublicationRejection`. Host configuration
must explicitly enable it after authentication and lifecycle qualification.

The request carries the intent, one explicit TYPED or OPAQUE choice per member,
and payload bytes indexed by member ID and full revision ordinal. Ordinals include
empty and reused slots; only upload declarations receive bytes. Zero-length upload
content is valid and differs from an absent payload. Check exact upload coverage,
duplicates, declaration lengths and actual SHA-256 before any storage side effect.
The intent remains the canonical command; mode choices retain their existing
immutable journal binding. A changed mode is not a new spelling of the same retry.

No request field chooses provider coordinates, credentials, process authority,
owner generations, retry reservations or descriptor implementations. The host
samples placements through the existing drive/backend identity path and opens
schema scopes through configured, authorized registry access. Existing member
schema conditions retain type name and descriptor fingerprint checks. Do not add
a second raw-descriptor or executable-code upload route. Container definitions
remain a host policy decision.

Use a bounded unary operation for the existing buffered publication path. Preserve
the 1 MiB intent limit, the host's 8 MiB document assembly limit and the existing
10 MiB transport envelope ceiling. Establish a checked aggregate upload budget
before allocating copies; per-field bounds alone cannot bound the request. Test
empty, exact-limit and oversized envelopes and limit admission before expensive
validation. This operation does not claim support for unbounded upload streaming.
Large-payload transport needs a separate resource and cancellation contract rather
than silently buffering arbitrary content or weakening provider limits.

A completed response contains exactly one existing committed or rejected receipt.
Only durable terminal rejection uses the rejected alternative. Authentication,
capacity, timeout, cancellation, operation-key conflicts and transient provider errors remain
transport errors. An ambiguous timeout does not establish rollback: retry the same
operation ID, command, mode choices and complete uploads under current authority.
Validate receipt shape and exact request/account/principal/member binding before
delivery, reusing the shared codecs; a valid protobuf alone is not commit proof.

The reusable Java publication interface belongs above byte storage and below the
transport. Both direct calls and the gRPC adapter must use the same bounded input
validation, current authorization, backend selection and execution facade. Keep
SQL/provider SDK dependencies out of that interface. The adapter translates
request/response and cancellation; it must not implement recovery or admission.

Acceptance requires real PostgreSQL and object storage on both invocation paths:
valid publication, typed rejection, opaque mode, receipt replay, altered retry,
missing/extra payloads, checksum mismatch, revoked credentials/grants/ACLs,
deadlines before and after commit, and shutdown with accepted calls. The host must
stop new transport admission while accepted publication calls retain their schema,
provider and SQL resources until completion or separately proven disposal.
Successful local calls do not qualify public transport availability.

Transport review refinements: a verified revision-precondition failure can be a
durable rejection, so use the recorded outcome rather than exception category.
Add a lost-final-reply test returning the original committed receipt. Upload
attributes must be fixed or host-derived, never new client metadata outside the
canonical intent. A host-owned selector for the container definition and immutable
drive/profile snapshots is required before mounting typed publication; occurrence
schema scopes alone do not supply those inputs.

Require complete indexed upload bodies on every Publish call, including receipt
retries. Validate coverage, aggregate size and checksums in the shared facade
before calling execution. The current runtime can return terminal receipts before
checking supplied modes or bodies; it does not establish this new facade's rule.
The facade must also compare mode choices against the retained journal for an
existing operation, under current authority, before terminal delivery. Missing
mode evidence must not trigger guessed defaults or a new write. A later explicit
receipt-query operation may omit upload bodies, but Publish does not.

The 10 MiB limit is the transport's per-message parser ceiling. Shared admission
can limit accepted requests and retained bytes but cannot retroactively prevent
protobuf parsing. Keep parser, intent, payload, assembly and response budgets
separate and test each boundary. This staged contract is not service availability.

The terminal-mode comparison needs a read-only journal operation tied to immutable
preparation/mode records, exact command identity and authorized receipt replay.
The existing live-owner mode check is not suitable after lease expiry. Validating
terminal delivery must not renew an owner or reserve another execution. This
comparison is a prerequisite for mounting the staged service. Sol reviewed the
unmounted contract and identified this implementation requirement.

### Terminal mode binding implementation

The opt-in `DocumentPublicationReplay.observe(caller, command, modes, control)`
compares terminal modes after authorized receipt observation in the same SQL
transaction. It binds the journal to the receipt generation, current owner token,
preparation nonce and canonical command. Missing or invalid journal bindings
produce DATA_LOSS; a valid different mode map produces FAILED_PRECONDITION.
Pending and unobserved operations remain observations, with no execution authority.
The check needs no live lease, provider call or full preparation decode. Its SQL
parameters include a bounded copy of the canonical command, at most 1 MiB;
concurrent request admission remains a host obligation.

Regression tests exposed an existing credential gap: observation of an operation
without a creation grant skipped credential validation. The shared authorization
path now checks any supplied credential in that branch. Missing authority records,
revoked keys and rotated old keys fail with UNAUTHENTICATED. Process callers and
keyless legacy trusted callers keep their existing behavior; the eventual public
host must authenticate its callers. This fixes both legacy and mode-aware replay.

This is an internal prerequisite. The new publication service is still unmounted,
and its shared facade must call this check after validating the complete input.
The tests cover real PostgreSQL receipts and journals with synthetic native
revision observations; they do not qualify provider or transport execution.

### Shared publication facade

`DocumentPublicationRepository` defines the common request/response boundary.
Journaled runtimes can create it with a trusted `PublicationSelector`. One runtime
call barrier spans input validation, byte reservations, replay, host selection,
execution and final receipt verification. A shared permit limit bounds facade
calls across all wrappers created from the same runtime. The facade reserves
retained input, canonical command allowance and upload conversion bytes before
allocation; these reservations remain held until synchronous execution returns.
Transport parsing and callback-delivery budgets remain separate host obligations.

The first authorized mode-aware receipt check precedes host selection. For a
nonterminal operation, the selector supplies immutable drive/profile snapshots,
fixed attributes, a container definition and a lazy Any schema-scope factory.
It must not return open resources without a separate lifecycle owner and must
clean up partial acquisition on failure. The existing execution path checks
placements and authority again under its normal admission locks. Final delivery
requires an exact match between the execution outcome and a fresh authorized
mode-aware durable receipt, covering a concurrent commit after the first check.

V1 can use a precomputed definition of the built-in Document descriptor for its
container. Any occurrence resolution remains caller-authorized registry work.
The service-owned drive/profile selector, RPC handler and client are still to be
connected and qualified. This factory alone does not mount a publication API.

### Service-owned publication selection

The service selects the configured backend generation/profile and account-scoped
SQL drive records. Request fields cannot choose credentials or alternate backend
configuration. The generated Document descriptor supplies the V1 container;
compiler artifact identity is packaged with repo-proto from the same Gradle
version provider that configures protoc. Provenance classifies that identity as
producer-reported, not a local compiler observation. Runtime identity is read from
the loaded protobuf class, avoiding Java constant inlining. Missing compiler
metadata fails initialization before creating the managed reader.

Drive selection uses bounded transactions and must apply account filtering before
backend validation. Batch selection must reject an incomplete ID set before
checking any returned backend; mixed owned/foreign and owned/missing sets must
have the same result. The target query count is one configured-profile query plus
one drive batch query, regardless of the number of distinct drives in a request.
This reduces database round trips; throughput still requires measurement.
