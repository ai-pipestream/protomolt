# Managed historical attempt ownership

Status: reviewed design; a private owner spans proposal, installation and execution. Public
routing and the complete managed recovery path are not implemented.
Build on `RepositoryHistoricalSuccessorActivation`, its accepted-call attachment,
and its classified capture disposal. Keep the public historical publication gate
until the complete managed path is qualified.

## Why another owner is needed

`RepositoryRecoveryAttempts` owns ordinary session targets and uncertain reservation
and installation state. Its session attachment cannot own historical source Work,
V109 captures or historical assessments. Reuse its identity and bounded-capacity
principles, not its ordinary-session attachment path.

A historical execution cannot be recreated on every client retry. The current
`DocumentHistoricalExecution` retains an acknowledged START permit, sticky CREATE
and publication attempt flags, and the private identity that binds its assessment.
Closing and reopening loses those facts. Reading a persisted START row does not
recreate permission to CREATE. A retry owner must retain the same execution object
while this generation can still continue.

A historical assessment is a separate resource. The owner must retain the exact
prepared assessment and successful CREATE result if a request ends before publication.
Keeping only the execution handle is insufficient. An uncertain CREATE/publication
must follow explicit reconciliation; it must not call the operation again blindly.
Do not replace an assessment after promotion or fabricate successful reconciliation.

## Two separate lifetimes

The initial single-generation owner uses the following call boundaries. The concurrent-generation design below replaces full-call operation exclusion for historical takeover.

Each accepted client call holds the managed runtime's existing call permit and
operation-key exclusion through routing and synchronous work. The runtime owns and
closes those permits. A private Attempt exclusively borrows one retained entry;
closing it releases only entry exclusion, never the runtime's call permit or guard.

Each retained entry owns a separate scope barrier and parent permit. Its cached
execution and assessment use children of that owner barrier. They can survive
between requests without keeping the runtime's client-call barrier permanently busy.
The two barriers must not be substituted for each other. The accepted-call attachment
API verifies that a supplied permit belongs to its expected barrier.

Close new request admission first. Wait accepted client calls before detaching entry
state. For each entry, close its owned assessment and execution, then close its parent
permit and wait for remaining owner-scope children. Close the retained source Work,
then classify and dispose the activation capture. Source/history drainage is still
required: an escaped worker Work permit can outlive the client Attempt or assessment.
Neither SQL locks nor the owner-map monitor may span those waits.

## Entry identity and bounded state

Use the account/principal/operation key for local exclusion, not a global operation
lock. An entry retains the exact canonical command, fixed mode map, proposal and
preparation fingerprints, original retention identity, and complete authenticated
caller identity including credential binding. For this first owner, identity means
exact immutable `RepositoryCaller` equality, including account and ACL sets, matching
the retained history's existing caller binding. A changed plan, caller or credential
cannot borrow an existing entry. Recheck current source authorization during each
operation; saved identity is not a cached permission grant.
If host-derived caller sets change, refuse continuation rather than silently rebinding
retained reads. Supporting identity refresh requires a separately reviewed change to
the source/history binding; ordinary recovery's weaker local identity comparison is
not sufficient authority to make that change here.

Bound both retained entries and active calls. Reserve memory for retained command,
plan and modes before transferring ownership. Assessments retain their existing
payload/schema reservations. Capacity refusal must precede new registration SQL.
Never evict uncertain entries to make room, mint replacement identities on lookup
failure, or turn a failed confirmation into absence.

Resume a retained entry before discovery or new proposal creation. A new source
capture can be transferred only to an entry that has none; retries cannot replace
an existing capture and its Work with a different source owner. The source provider
must account for failed or uncertain read-pin acquisition through the existing
reader lifecycle. Transfer rules must specify who cleans up when attachment fails.

Use two-phase acquisition: `beginInstalled` reserves the entry and byte budget before
source capture, then `attachSources` transfers the exact source owner, root Work and
history set. Until attachment succeeds, the caller owns their cleanup. A failed
attachment must leave both sides' ownership unambiguous; after successful attachment,
an activation failure leaves those resources with the entry for reconciliation.
Source capture can itself perform read-pin SQL. The capacity guarantee is before
new activation registration SQL, not a claim that all caller preparation was SQL-free.

Create and retain assessments through the entry's own execution. Store its successful
CREATE result in the same entry before returning it to the request. Do not expose a
general setter that accepts an arbitrary assessment or stage from another execution.
An uncertain CREATE leaves the original assessment and execution attempt flags intact.

## Implementation order and boundary

An installed-plan activation/attachment owner can be implemented and qualified first.
Name it accordingly: it does not own reservation or installation. It receives the
exact installed plan, captures sources, retains the activation across V94/V109 reply
uncertainty, and caches the execution and assessment for subsequent calls.

Before enabling managed recovery, extend ownership backward: mint and retain the
proposal before the first V97 operation and retain the exact plan across V93 reply
uncertainty. The public route must validate all resubmitted fresh payloads before
those mutations. Historical bytes come from exact authorized retained captures;
fresh bytes are explicitly supplied again, not inferred from an old upload.

Use coordinator authority only for private reserve/install/activate/dispose actions.
Use the authenticated execution caller for reads, START, CREATE, publication and
receipt delivery. Resolve authority afresh at each boundary. Terminal replay must be
authorized before returning a receipt or allocating a new historical attempt.

## Disposal and retry outcomes

After the owning call barrier drains, use the existing sticky activation closure and
claim-locked capture classifier. Registered state drains through V107 after actual
Work and read-pin release. Consistent absence performs local-only cleanup. Failed
confirmation or inconsistent evidence leaves the entry unresolved and retryable.

NO_CAPTURE leaves preactivation source resources with their explicit owner. The
managed owner must close source admission, wait Work/Uses, release its exact read
handles, and verify release before dropping that entry. Do not fence a shared reader
incarnation or release preparation roots as part of entry disposal.

A successful local cleanup is not a published result, remote quiescence or permission
to restart the same generation with new mutable state. Eviction while running needs
an explicit terminal/fenced outcome; uncertain CREATE/publication retains its protocol
state. Shutdown can dispose local ownership while leaving durable reconciliation to
subsequent authorized recovery, without claiming that recovery already succeeded.

## Acceptance evidence before public enablement

- Exact caller, plan and mode binding; same-key exclusion; capacity rejection before SQL.
- Same execution and assessment survive client Attempt closure. Runtime client-call
  count returns to zero while owner resources remain tracked separately.
- Acknowledged START can continue through CREATE on a later client call; successful
  CREATE can continue publication using the same retained assessment and stage.
- Lost activation, CREATE and publication replies preserve identities, forbid blind
  repetition, and reconcile exact durable outcomes. No cold receipt recreates Work.
- Missing/corrupt fresh payloads refuse before reservation or installation.
- Current credential/source revocation prevents continuation or receipt delivery.
- Shutdown refuses new calls, waits active calls and actual worker children, then
  disposes registered, rolled-back and never-created captures without false markers.
- Timeout or failed confirmation retains state for retry; all owned budgets and permits
  return only after successful disposal. Other operation keys remain usable.
- The same managed behavior passes library and gRPC qualification before the facade's
  historical-command gate is changed. Public documentation describes only that result.

The existing single-source scoped mixed-successor probe, accepted-call probe and
capture-disposal suites are prerequisites, not evidence of managed owner qualification.

## Private installed-plan checkpoint

`RepositoryInstalledHistoricalAttempts` now reserves entry capacity before source
transfer, retains the activation/execution/assessment and acknowledged CREATE result,
and separates request exclusion from retained scope permits. Exact local retries do
not reserve another preparation scratch lease. New entry fingerprinting still uses a
conservative roughly 49 MiB temporary encoding reservation; retained accounting uses
actual encoded preparation/mode sizes. Encoding currently holds the short-lived map
monitor; this has not been qualified under concurrent load.

Seven real PostgreSQL tests qualify entry identity/exclusion, byte-capacity refusal,
exact retries under budget pressure, failed source transfer, held-worker drainage,
cancellation, START continuation across client calls, and activation rollback/lost
reply cleanup. Source publication in these SQL fixtures uses synthetic provider
observations; it is not provider durability evidence. The 21 existing activation and
capture-disposal tests also pass. See the
[installed owner evidence](../evidence/repository/2026-10-07-installed-historical-owner/README.md).

The packaged `HistoricalInstalledOwnerProbe` now qualifies retained assessment plus
CREATE/publication across separate client calls with a scoped caller, real provider
upload/readback, exact receipt replay and final ownership drainage. Its standalone
request barrier exercises the private owner; it does not establish managed runtime
routing. See [multi-call publication evidence](../evidence/repository/2026-10-07-installed-historical-publication/README.md).

Still required before managed use: reconcile this owner's expired reservations and
uncertain attached activations, then wire and
qualify both library and transport entry points. The private owner now supports
authenticated terminal/fenced retirement, as qualified below. The facade gate remains unchanged.

## Implemented: reconcile an uncertain CREATE

Before CREATE SQL, retain its immutable proposal on the execution: acknowledged START
identity/deadline, manifest SHA and exact upload selections. A later call can reconcile
only this same handle and retained assessment, under its private assessment identity,
source Work, command, modes and current runtime observation. No arbitrary stage setter
or cold receipt may restore execution authority.

Use a dedicated transaction path, not `mutate()`: the latter acquires physical-origin
locks before its callback, whereas retained verification needs the assessment-owner
lock first. Keep the established order: registration/claim, operation owner/command,
current policy, full document/credential authorization, fixed modes, retained assessment
verification, then any required physical/capture checks. Recheck expiry and authority
at delivery. Publication repeats its own fences; stage reconciliation grants no
publication permission by itself.

Only an exact verified committed stage can populate the owner's stage field. Empty or
failed observation retains the attempted proposal and sticky CREATE flag; it never
authorizes another CREATE. Qualification must cover after-commit reply loss followed
by exact adoption/publication, rollback with empty observation and refused restaging,
changed selection/manifest, revoked authority, and expired/released assessments.

The private qualification adds direct rollback and lost-reply checks,
same-owner lost-reply adoption followed by publication, changed selections,
credential revocation, and expiry while waiting for a real PostgreSQL row lock.
The expanded packaged run passed with no failures or skips, including dedicated
changed-manifest and explicitly released-assessment cases. The former changes evaluation time through
normal assessment preparation, then retries the original evidence. The latter uses
the existing SQL recovery function after real expiry, then requires empty
reconciliation and refusal of another CREATE. Neither case bypasses SQL guards.
See [CREATE reconciliation evidence](../evidence/repository/2026-10-07-historical-create-reconciliation/README.md).

## Implemented: retire completed or permanently fenced entries

Normal service operation must release completed entries without waiting for host
shutdown. The private owner implements this after CREATE reconciliation, using the existing replay,
claim-fencing and capture-disposal primitives. A lease expiring or discovery returning
no result is not proof that an attempt can be discarded.

Keep the exact entry exclusively borrowed throughout proof and disposal. For terminal
retirement, observe the exact command through
`DocumentPublicationReplay.observe(caller, command)` using the entry's authenticated
caller, with explicit control checks before and after observation. A committed or terminated
outcome makes the entry permanently retirement-only before any drainage starts.
For superseded work, separately establish permanent fencing of the retained claim
with `RepositoryClaimRetirement`; do not infer fencing from a timeout.
The fenced-retirement entry point must first verify private process authority for
the entry's exact reservation; the claim-fencing helper does not authorize a caller.

The terminal outcome may belong to a later generation. Current SQL guards require
successors to preserve predecessor modes; an attempted changed-mode install must
refuse. Exact-command terminal replay establishes the global terminal fact for local
retirement and grants no publication authority. This does not permit changing modes
or bypassing their journal checks in publication.

After either proof, close the assessment and execution, drain their owner scopes,
close source Work and classify/dispose the exact activation capture. Reuse the
shutdown disposal order. No SQL lock or owner-map monitor may span worker or reader
waits. Durable terminal state and permanent fencing do not establish that local
workers have stopped using bytes.

After proof, cancellation, timeout or cleanup failure retains the entry, its
retirement-only state and remaining reservations for another disposal attempt.
A failed or cancelled observation before proof must not make the entry retirement-only.
Once the immutable
proof has been established, private process authority may finish local disposal
even if the original credential is subsequently revoked. Returning a receipt still
requires fresh caller authorization; cleanup authority never substitutes for it.

Remove the entry and end its borrowed-call accounting together only after successful
disposal. Closing the Attempt afterward must not decrement active calls twice.
Future public routing must replay terminal outcomes before beginning a replacement
entry, so retirement cannot recreate execution authority for a completed operation.

Acceptance tests must cover pending-state refusal, committed and rejected outcomes,
later-generation terminal outcomes, lost replies, held worker permits, timeout and
retry, revocation after proof, and two independent operation keys. Assert retained
budgets while blocked and released budgets only after actual drainage. This is a
requirement now covered for the private installed owner, not public runtime routing.
The later-generation case must first prove that SQL rejects changed successor modes,
then use a valid mode-preserving successor to establish the terminal outcome.

Ten retirement cases and 28 existing owner/activation/disposal cases pass against
real PostgreSQL. Source setup in these SQL fixtures uses synthetic provider observations.
The packaged provider gate separately proves committed publication, normal retirement,
and exact authorized receipt replay after local resources are released, including an
uncertain CREATE that was reconciled before publication. A cancellation test waits
for post-proof root Work closure with an actual child still held; cancellation retains
the entry and its budget until a later call completes drainage. See
[retirement evidence](../evidence/repository/2026-10-07-historical-retirement/README.md).

## Implemented: private preparation ownership

Extend this same per-key owner backward through proposed, reserved and installed
phases. Do not transfer a completed ordinary recovery entry into a second map: its
session attachment cannot own historical Work, and a second map would introduce a
gap in exclusion and retained execution identity.

Reserve entry capacity and bounded bytes before minting and submitting a reservation.
Retain exact caller/command, original retention identity, requested modes, proposal,
token and incarnation. Local resume precedes discovery. Reuse coordinator recovery
discovery, expiration/supersession reservations, reserved preparation and successor
installation; preserve their current SQL guards and authorization checks.

Retain loaded immutable preparation and the exact generated installation plan before
submitting V93. `RepositoryReservedPreparation.load` rejects an already installed
successor, so a lost installation reply must confirm the retained plan rather than
reconstruct it through discovery. Only confirmed installation permits capture
transfer into the same entry. Continue using its original execution, START, assessment
and sticky CREATE/publication state across later calls.

An unactivated successor requires a retained pending V98 proposal and exact predecessor
mode checks, including across lost replies. Any intermediate implementation lacking
that phase must explicitly refuse it; fresh discovery cannot silently replace a
retained proposal. Validate complete resubmitted payloads before reservation mutations
using the existing recovery payload checks; request payload snapshots stay request-scoped.

Qualification must cover lost reservation and installation replies with unchanged
identities, capacity refusal before reservation, exact-plan confirmation after install,
failed capture transfer without leaked ownership, unchanged execution across activation,
terminal/fenced disposal before installation, and shutdown with actual workers held.
These remain qualification requirements, not available public behavior.

`RepositoryHistoricalAttemptPreparation` now retains proposed/reserved/installed
state inside the existing owner's entry. `beginProposed` preserves exact caller,
command, requested modes and retention bytes, including when a retry decodes a new
Java object for the same retained record. Advance snapshots and checks resubmitted
upload bytes before reservation mutations. It verifies the exact durable preparation,
history projection and initial capture anchor using the same check as activation;
activation retains its original SQL locks. Fixed predecessor modes are checked before
reservation. The same loaded preparation and minted plan survive interrupted replies.

Eleven new SQL cases plus 38 owner/activation/disposal regressions pass. They include
actual post-commit JDBC reply loss with confirmation interrupted, exact identities on
retry, fresh-owner recovery of expired unactivated reservations/installations, wrong
retention and mode refusal before writes, metadata-only shutdown, and byte capacity
refusal. Source setup uses synthetic provider observations. The expanded production-JAR
gate separately qualifies the same owner from pre-reservation through source capture,
assessment, actual provider publication, receipt replay and terminal retirement.
Missing and corrupt resubmitted bytes cause zero reservation/installation writes.
Existing CREATE reply-loss reconciliation and revocation/expiry cases now use this
preparation path too. See [preparation evidence](../evidence/repository/2026-10-07-historical-proposed-owner/README.md).
This private qualification does not enable managed public recovery.

The tests exposed an object-identity comparison in historical source Work. The binding
now compares operation ID and canonical command bytes, so a decoded exact command is
valid but another operation with identical semantic content is not. Canonical command
bytes alone are insufficient because they intentionally exclude operation ID.

Current limitations: reservation preflight authorization and the private reservation
write are separate transactions. Later loading and activation reauthorize, and no
provider execution is granted by reservation. Decide and qualify the required
revocation boundary before public dispatch. Preparation currently retains the bounded
maximum next-record lease until disposal; lowering this to measured retained bytes is
a memory-capacity optimization still to assess, not a qualified throughput claim.

## Recover this owner's expired unactivated proposal

Permit a retained V98 transition only when no sources, activation, execution or
retirement proof have attached to the entry. This covers an uncertain V97 reply,
reserved work, and an installed plan that has not started activation. Fresh discovery
must identify exactly this entry's successor epoch, token and incarnation, together
with the expected predecessor owner/preparation/install tuple. A still-original
predecessor requires retrying the retained V97; it does not justify a fresh proposal.

Retain a pending supersession before its first SQL submission. On retry, confirm that
exact pending proposal before considering discovery. Require complete resubmitted
payloads, current caller authorization, exact historical retention, and fixed modes.
Only after V98 confirmation may the exclusively borrowed entry adopt the pending
proposal, release old preparation leases and clear its old installation plan. Resume
at RESERVED in the same map entry. A lost reply must not mint another identity.

Refuse that transition once any source or activation attaches. An uncertain V94 may
already be bound: reservation fencing neither drains its readers nor permits resetting
acknowledged START, assessment, CREATE or publication state. Recovery of those states
requires exact capture classification and actual local drainage, or terminal/fenced
retirement followed by a separately qualified successor path.

Test each unactivated phase with real expiry, lost V98 acknowledgment, refusal of a
foreign successor, retained budgets on refusal, and no new history capture before the
replacement installation is confirmed. These transitions are implemented privately; public routing remains disabled.

While a supersession is pending, refuse ordinary advancement, installed-plan delivery
and source transfer until it is confirmed. Fenced retirement must check both current
and pending successor identities; fencing the old claim alone must not discard a
potentially committed replacement. Keep a single authoritative current proposal in
the preparation helper rather than independently mutating an entry-level copy.

### Private self-supersession qualification (2026-10-07)

`reconcileUnactivated` now performs the transition above on the same exclusively
borrowed entry. A pending V98 proposal prevents advancement, plan delivery and source
attachment. Fenced retirement checks both current and pending claims. Exact V98
confirmation adopts the replacement, releases obsolete preparation metadata and
clears the old plan. Source attachment and execution still prohibit this transition.

Expired reservations use discovery plus V98's atomic claim/owner/preparation checks;
the live-lease preparation loader is deliberately not used before supersession.
An already retained installation is checked against its exact plan and V93 record.

The focused PostgreSQL qualification covers 56 distinct tests: 55 passed together,
then the 18-case preparation class passed after adding pending-replacement shutdown
(the other 38 regression cases were unchanged). Actual JDBC post-commit faults cover
V97, V93 and V98 replies; synthetic provider observations only establish archived
source fixtures. See [self-supersession evidence](../evidence/repository/2026-10-07-historical-self-supersession/README.md).

The packaged real-provider host now retains one owner through actual expiry and
replacement before fresh capture, publishes a mixed revision, verifies exact provider
bytes and receipt replay, and retires the owner. The complete gate passed in 11m13s
(670.386 seconds for its one aggregate test, no failures or skips). The additional
scenario runs in a separate database/JVM with a 90-second host limit and a 30-second
operation lease; earlier host limits are unchanged. See
[provider evidence](../evidence/repository/2026-10-07-historical-self-supersession-provider/README.md).
Attached V94 recovery, managed library/gRPC routing, and the earlier authorization
boundary decision remain outstanding. No new public API is enabled by this change.

## Design next: concurrent recovery generations within bounded ownership

Reviewed with Sol on 2026-10-07; this section specifies work still to implement.
The performance requirement is explicit: a fenced generation's slow local worker
must not force a successor to wait for that worker before making safe progress.
Memory, accepted work and cleanup remain bounded; expiry alone proves no drainage.

Existing behavior to retain:

- `RepositoryHistoricalSuccessorActivation.activateWithWork` already retains its
  tentative capture and confirms exact committed activation after a lost reply.
  Same-host live retries can reuse accepted Work without opening source admission.
- `RepositoryInstalledHistoricalAttempts` retains execution, acknowledged START,
  assessment and sticky CREATE/publication state across calls.
- `RepositoryHistoricalCaptureState` classifies exact capture registration under
  the claim lock. Disposal distinguishes absent local registration from committed
  capture; only actual local drainage or separate quiescence proof permits release.
- An immutable `RepositoryHistoricalActivationEvidence` receipt does not reconstruct
  accepted Work, native pins or execution authority on another host.

### Ownership and routing

Keep an operation slot keyed by account, principal and operation ID. Each retained
Entry has a stable local identity independent of SQL claim epoch: preactivation V98
can advance that epoch while preserving one Entry. The slot tracks current, at most
one proposed successor, and bounded disposal-only Entries. Every Entry retains its
own exact SQL current/pending proposals, caller binding, modes, retention digest,
preparation leases, capture and execution state.

Route key-based retries to retained pending/current state before fresh discovery.
Old generations are addressed through internal retained handles only for disposal.
Allocate the successor Entry, its proposal and budget before V97. While its outcome
is uncertain, suspend admission of new mutations to the old Entry; already accepted
work remains owned and SQL fences arbitrate its outcome. Do not clear old START,
CREATE, assessment or publication flags.

Exact V97 confirmation switches routing to the successor and makes the old Entry
disposal-only. Successor installation and fresh capture proceed independently of old
reader drainage. A lost V97 reply retains both Entries and the exact pending identity.
If old publication wins first, failed takeover is reconciled against authorized
terminal replay; a failed reservation is not permission to discard uncertain state.
If the prospective successor expires before attachment, V98 updates that same stable
Entry using the already qualified pending-proposal protocol.

Use short, versioned slot transitions and per-Entry call exclusion. No map/slot
monitor spans SQL, provider calls or local drainage. In particular, the existing
`RepositoryPublicationCalls` same-key permit spans a complete runtime call and
would serialize takeover behind a slow old call. Historical routing must not reuse
that exclusion unchanged; SQL claim fences remain the cross-process authority.

### Capacity, shutdown and acceptance

Capacity counts every current, proposed and draining Entry and every retained byte
lease. Refuse exhaustion before V97; never evict an old generation merely because a
new one can run. Keep a bounded explicit disposal path so drained generations do not
accumulate indefinitely. Closing host admission prevents new requests; accepted
calls remain owned through completion. After accepted-call drainage, close owner
admission and dispose all retained generations independently. A timeout preserves
unfinished Entries and budgets rather than reporting fictitious cleanup.

Required tests against real SQL, then packaged providers:

1. Hold old V94 Work while a new V97/V93/V94 generation publishes and its exact
   bytes and receipt are read back. Assert successor progress before releasing Work.
2. Inject V97 before-commit failure and after-commit reply loss. Retries preserve one
   prospective Entry and proposal and never recreate old START/CREATE state.
3. Race old publication with V97 using barriers. Exactly one durable outcome wins;
   receipt replay or successor publication matches that winner.
4. Exhaust entry and byte capacity: zero V97 writes, existing state unchanged.
5. Fenced old disposal waits for real Work, then records its own exact V107; new
   capture, provider versions and receipt remain intact.
6. Let a prospective successor expire. V98 changes SQL identity within its stable
   Entry; both uncertain claim identities remain accounted for until confirmation.
7. Close admission with old and new accepted work present. Refuse new calls, drain
   each generation, and preserve unresolved ownership when the wait times out.
8. For rolled-back V94, release local resources without a false capture-drain row.
   A restarted process never turns a cold activation receipt into execution rights.

Implement and qualify this owner structure before enabling managed historical
routing. It does not require redesigning protobuf contracts or relaxing ownership,
provider checks, retention or JCR capability boundaries.

### Concurrent-generation implementation checkpoint

The private owner now keeps two indexes: the selected operation-key retry route,
and all retained Entries keyed by stable local UUID. `beginSuccessor` verifies the
exact attached predecessor, modes and expired bound observation, then reserves a
new Entry and its byte budget before SQL. A pending successor becomes the selected
retry route and stops new mutations on the old Entry. Already accepted work stays
owned; SQL determines the takeover winner. An unchanged retry retains that pending
Entry even when fresh discovery now sees its committed reservation. Changed caller
or canonical command cannot adopt it.

After exact V97 confirmation, the predecessor is marked disposal-only. The successor
can install and capture fresh sources while old Work remains held. Private cleanup
can address the old Entry by its stable ID. Removing it does not remove the new retry
route. Capacity counts both generations; refusal restores the old route without a
reservation write. Shutdown attempts other ready Entries after a held generation
cannot drain, retaining unfinished Entries and their budgets.

Five new real-PostgreSQL cases and 56 preparation/activation/retirement regressions
passed together (61 cases, zero failures/errors/skips). They cover overlapping old
Attempt/Work with new V97/V93/V94 and START, post-installation capture, independent
shutdown disposal, capacity refusal, V97 rollback and lost commit reply, and a
changed-command retry. Setup uses synthetic provider observations, so these tests
alone make no provider-publication claim. The packaged-provider regression also
passed in 11m14s (one aggregate case, zero failures/errors/skips), covering the
existing real-provider scenarios. It does not yet qualify publication while an older
generation's Work remains held. Evidence is recorded in
`docs/evidence/repository/2026-10-07-historical-generations/README.md`.

The private owner is not yet the managed historical route. The full-call same-key
host exclusion remains unchanged. Publication while old Work remains held, the
publication-versus-V97 race, byte-capacity exhaustion, and a pending generation's V98
expiry in the presence of an older draining Entry remain required acceptance cases.
No public historical capability is advertised by this checkpoint.

### Publication/takeover race qualification: finalization and commit

The barrier position determines the expected winner. `DocumentPublicationCommit`
executes `SET CONSTRAINTS ALL IMMEDIATE`, then checks the historical stage, before
returning to `Tx.inTransaction` for JDBC commit. V79 checks live claim and owner
leases during that explicit finalization. It does not automatically repeat those
checks at JDBC commit after they have already fired. The previous description of
all post-expiry publication commits as invalid was incorrect.

Qualify these orders without changing production guards:

- Block publication before finalization on a real historical-origin row. Observe
  the publisher PID and claim/owner locks, wait for database-clock expiry, and prove
  V97 waits on that publisher via `pg_blocking_pids`. Release the origin; publication
  must fail its liveness checks and roll back. V97 may then proceed. Verify no old
  result remains and qualify the successor's provider publication separately.
- Gate publication at JDBC `beforeCommit`, after successful finalization and stage
  checks. Allow lease expiry, submit V97, and prove its exact lock wait. Releasing
  the publisher can commit a valid result; V97 must then reject the terminal
  operation without a replacement reservation. Verify the original receipt/revision.
- Gate publication before claim acquisition, expire the predecessor and commit
  V97 first. Releasing the old call must produce an exact-fence error. Verify the
  successor result and absence of any old publication.

The checkpoints below record qualification of these orders. A lease is not proof that a transaction
already holding the fencing locks has stopped. Every gate needs bounded waits,
original SQL errors and unconditional barrier release in `finally`.

### Provider-overlap qualification

The next fixture uses an independent production-JAR JVM/database with the existing
90-second host cap. It retains a real historical capture and forked Work in an old
private generation, waits for actual SQL lease expiry, then reserves and installs a
new generation across separate calls. Only afterward does the existing provider
publication path capture fresh sources, upload, create evidence, publish and verify
provider bytes and the exact receipt. The old Work stays held throughout. Cleanup
must first refuse fenced retirement while Work is held, then release it and retire
only that generation, preserving the successor retry identity and full budget return.

The initial full packaged-provider run passed in 12m08s (one aggregate case, zero
failures/errors/skips; 725.485 seconds). It verified all overlap publication, old
generation retirement and terminal host markers. Sol's failure-cleanup refinement
has since been applied: owner detachment failure no longer skips local reader
cleanup, secondary cleanup failures are suppressed, and actual drainage remains
mandatory before pin release or reader quiescence. The revised source also passed
the full gate in 12m08s (725.843 seconds, zero failures/errors/skips). Source hashes
and reports are archived in
`docs/evidence/repository/2026-10-07-historical-generation-overlap/`. Transaction
winner races and managed public routing remain separate work.

#### Reviewed transaction-gate implementation plan

Reuse the real origin-row blocker in `HistoricalPublicationExpiryProbe` for the
pre-finalization case. Keep the assessment deadline beyond the short claim lease
so the test identifies claim expiry rather than an unrelated stage timeout. Observe
both blocker edges: publisher to origin holder, and V97 contender to publisher.

For the post-finalization case, adapt `DocumentJdbcFaults.beforeCommit` and the
latch/PID handling in `HistoricalAuthorizationCommitGate`. Match exact operation,
owner and the success row's `creation_xid=pg_current_xact_id_if_assigned()`. Pass the
gated Tx only to `DocumentPublicationCommit`, keeping schema staging outside the
gate. Observe locks using an unwrapped connection. The before-claim case uses a
publication-only connection gate. No production timeout, lease or SQL guard change
is implied by these fixtures.

#### Post-finalization fixture qualification

`HistoricalPublicationCommitWinnerProbe` targets the exact operation-success row
and current transaction ID at JDBC commit. It records the publisher PID, checks
live claim and owner leases at entry, waits for PostgreSQL expiry, then submits
V97 through the production reservation adapter. The test requires a lock wait on
that publisher before releasing commit, terminal rejection of takeover, unchanged
reservation count and one publication result. The existing provider fixture then
checks bytes and the receipt. A separate database/JVM retains the 90-second cap.
Sol reviewed the implementation. The full packaged-provider gate passed in 13m03s,
with one aggregate case and zero failures/errors/skips (778.536 seconds). Evidence:
`docs/evidence/repository/2026-10-07-publication-commit-winner/README.md`.

The initial case qualified direct SQL arbitration. The local-owner extension adds
`beginSuccessor` while the publisher is gated, then advances that proposal after
the publication commits. Pre-finalization expiry and takeover-before-claim ordering
remain separate acceptance cases.

#### Local proposal cleanup after predecessor publication

`HistoricalPublicationLosingSuccessorProbe` retains a fork of accepted Work and
selects a separate proposal after PostgreSQL lease expiry. The existing direct V97
contender still proves the precise publisher lock wait. The local proposal does
not advance until publication completes: its authorization path can acquire document
locks before reaching V97, so it must not be described as the observed V97 waiter.

After provider readback, local `advancePreparation` must report terminal rejection.
No reservation, installation or activation may be added. The focused cases exercise
both retirement orders. The old generation stays owned until its worker drains;
retiring it first must preserve the pending successor route. Removing the proposal
first must preserve ID-based access to the old generation. V107 stays unchanged
while Work is held and increases once after the publisher drains. Removing the
uninstalled proposal adds no capture-drain attestation. Final checks require both
IDs removed, all retained byte reservations returned and the exact receipt replayable.

Both focused cases passed with 0 failures or skips in 2m07s. Sol reviewed the
extension. The full storage suite passed in 13m58s with no failures or skips. Evidence:
`docs/evidence/repository/2026-10-07-local-successor-terminal-retirement/README.md`.

Terminal proof requires the retained caller's current replay authority. Revocation
before proof cannot authorize receipt delivery or mint that proof. Private shutdown
cleanup exists; coordinator-only targeted terminal retirement after revocation is
not implemented and must not be inferred from the authorized case.

#### Focused historical runtime checks

For individual development iterations, `admissionHistoricalRuntimeTest` supplies
explicit JUnit entrypoints using the same production-JAR compiler and historical
host as `admissionStorageTest`:

```
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.commitWinner' \
  --max-workers=2 --console=plain
```

Other method names are `commitWinnerOldFirst`, `reconciliation`, `selfSupersession` and
`overlappingGenerations`. Each case uses real PostgreSQL and LocalStack in an
isolated host with the existing deadline and required-marker checks. The focused
task is optional and excluded from ordinary unit tests. It does not replace the
full `admissionStorageTest` dependency of `check`; all historical scenarios remain
mandatory there. The first focused commit-winner run passed in 1m17s. The complete focused run passed: 4 tests, no failures or skips, in 4m17s.
Evidence: `docs/evidence/repository/2026-10-07-focused-historical-runtime/`.

#### Claim expiry before finalization

`HistoricalPublicationClaimExpiryProbe` blocks the publisher on a real historical
origin attempt row. PostgreSQL confirms the publisher-to-origin and
reservation-to-publisher lock waits using backend PIDs. The test waits for both
claim and owner expiry, checks that the assessment and selected upload leases are
still live, and releases the origin lock.

Capture revalidation calls `RepositoryExecutionClaimLedger.lockLive` after origin
binding. That earlier check raises `Fenced`; this case does not reach V79 deferred
finalization. The first fixture expected the later SQL guard and failed. Production
code was unchanged when the assertion was corrected to the actual claim boundary.
The second run passed in 1m07s. Final-source qualification passed in 1m05s
with 1 test and no failures or skips. Sol reviewed the fixture and aggregate wiring.
Evidence: `docs/evidence/repository/2026-10-07-pre-finalization-claim-expiry/README.md`.
The aggregate rerun for this extension remains pending.

The test requires zero committed result/revision rows and confirmation of the
waiting reservation after rollback. The old private generation retires through
its exact fence and returns retained bytes. It leaves a durable successor
reservation. The continuation below qualifies successor publication.
The before-claim acquisition ordering is qualified by the focused case below.

#### Publication after claim-expiry rollback

`HistoricalPostRollbackPublicationProbe` uses the confirmed reservation and failed
plan as the immediate installation predecessor. The original preparation remains
the retention reference. Local capacity is reserved before installation; fresh
reader/capture ownership and provider reads follow installation. Upload bytes are
explicitly supplied again.

The normal private-owner path publishes the same command, checks provider bytes,
replays the exact receipt and returns retained memory. Assertions require new
claim/process/owner/upload identities, disjoint capture pins, 2 reservations and
installations, 3 STARTs, 2 CREATEs and 1 successful publication. Sol reviewed the
extension; focused qualification passed in 1m06s with no failures or skips.
See `docs/evidence/repository/2026-10-07-post-rollback-publication/README.md`.
The aggregate rerun remains pending.

#### Takeover before publication claim acquisition

`HistoricalPublicationBeforeClaimProbe` pauses the publication-only connection
before transaction admission. V97 commits after database lease expiry while the
publisher remains gated. The test confirms the replacement claim and verifies
that the publisher backend is idle, without a transaction or backend xid. The
assessment and selected verified upload remain live at release.

The resumed publisher must throw `RepositoryExecutionClaimLedger.Fenced`, leaving
no result or revision. `HistoricalPostRollbackPublicationProbe` then qualifies
successor installation, fresh capture and same-command provider publication.
Final focused qualification passed in 1m07s with no failures or skips. Sol reviewed
the fixture and aggregate mapping; the expanded full suite remains pending.
Evidence: `docs/evidence/repository/2026-10-07-takeover-before-claim/README.md`.

## Managed host integration: remaining requirements

Source checkpoint: `4997247d3458a6496b0de73515470d3d2774bfec`.
This section specifies work still required; it does not enable public historical
publication or claim that the private recovery tests cover the managed host.

### Initial generation

`RepositoryInstalledHistoricalAttempts.Entry` currently represents an installed
successor or a recovery proposal. `beginSuccessor` requires an installed plan.
Neither state owns the initial `DocumentPublicationRegistration.historical`
registration across client calls. Add initial registration as an explicit lifecycle
state in the same owner, with capacity reserved before opening or accepting the
initial read capture, not only before registration.
Do not manufacture a successor reservation or V93 plan for this state.

Retain the original preparation, exact caller and modes, registration, capture,
accepted Work, execution, assessment and CREATE result as they become available.
A timeout must retain uncertain registration or admission state for reconciliation.
Retries must preserve acknowledged START and uncertain CREATE/publication state.
Derive initial predecessor identity from its actual claim and owner; do not derive
it from a fabricated successor plan. An expired initial generation must support
selection of a successor while accepted old Work remains owned until drainage.
Keep selected-key routing separate from stable-ID disposal of every generation.

### Runtime routing and authorization

`DocumentPublicationFacade.publishDocument` currently rejects historical commands
before `DocumentPublicationRuntime.publishValidated` can replay a terminal result.
The runtime's accepted path still calls ordinary `recovery.prepare` and
`sessions.execute`. Retain that public restriction until the managed path passes
its acceptance tests; removing the restriction alone is not an implementation.

Historical dispatch must share the existing envelope validation, request permits,
byte accounting and authorized receipt replay. Branch before ordinary recovery and
session execution. Do not hold the ordinary full-call operation guard across old
and new historical generations: it would prevent the takeover this owner supports.
Use bounded per-generation borrowing and short selection operations instead.
Keep SQL, provider calls and drain waits outside the selection monitor.

Obtain fresh private recovery authority for the exact operation. A request caller
cannot grant itself coordinator authority. Recheck current source and receipt
permissions at their existing boundaries. A retained entry's caller binding must
not silently change when credentials, accounts or ACL identity change. Request-local
schema resolver handles must not escape their lifetime: resolve with a current
call scope or explicitly transfer ownership of a retained scope.

### Cold recovery

`DocumentHistoricalRetentionBinding.require` checks a supplied record; it does not
load the original retained preparation. `DocumentPublicationPreparationJournal.load`
requires a live claim. `RepositoryReservedPreparation.load` loads the immediate
predecessor after reservation, which is not necessarily the original retention
anchor. Add bounded, integrity-checked anchor discovery under explicit process
authority, with current execution-caller authorization before returning source
metadata. Follow persisted lineage rather than assuming generation zero.

Persisted START, capture or assessment evidence cannot reconstruct a live execution
permit. Cold recovery must install a successor and acquire fresh capture ownership
and provider reads. Preserve the original retained schema and source definitions
while binding the new execution to its own claim, owner and assessment identity.

### Shutdown and acceptance gates

Integrate the historical owner into `DocumentPublicationRuntime.shutdownStep`:
close outer admission, wait accepted calls, then dispose every retained generation
and its accepted Work before closing nested readers and providers. A timeout or
cleanup error must preserve owner state and allow a later shutdown step to retry.
A blocked old generation must not prevent disposal of an unrelated ready entry.

Required evidence before enabling the public route:

- Initial registration, retry and uncertain admission retain one exact execution;
  invalid requests have no registration or provider effects.
- START acknowledgement and uncertain CREATE/publication survive call boundaries;
  replay returns the exact durable receipt under current authorization.
- Initial-to-successor and successor-to-successor takeover allow new work while old
  accepted Work is held, without reusing claim, capture or assessment identities.
- Cold restart resolves the original retention anchor across multiple successors;
  missing, corrupt or unauthorized anchors fail explicitly without partial publish.
- Cancellation before and after commit, revoked source/read authority, capacity
  refusal and retryable shutdown preserve resources and receipt semantics.
- The same cases pass through the library and authenticated in-process gRPC paths
  against real PostgreSQL and an object provider. Separate assertions cover denied
  delivery, durable SQL state, provider bytes and eventual release of owned memory.

Implement initial ownership first, then cold anchor discovery and managed routing.
Keep each change independently reviewable. Public contract names, field tags,
imports and Any URLs remain unchanged.

Initial retirement and shutdown also need an explicit path. `Attempt.retire` and
`detachClosed` currently check
reservation authority and derives fencing identity from the successor reservation.
For an initial entry, authorize the exact operation through private process authority
and prove retirement against its actual retained claim, coordinator incarnation
and owner binding. Do not bypass terminal
replay authorization, treat lease expiry as fencing proof, or create a placeholder
reservation merely to reuse cleanup. Cover initial retirement before registration,
after uncertain admission, after terminal publication and after successor takeover.

### Initial capture disposal prerequisite

Stop registration admission and join the exact in-flight registration call before
classifying a tentative capture. A rollback can remove the entire initial claim;
PostgreSQL cannot lock a row that never committed. An absence query alone cannot
prove that a still-running registration will not commit later.

Classify the exact tentative capture, including initial claim token, coordinator,
preparation identity, modes and pins digest. Committed registration must match its
preparation, initial binding, sealed historical roots and initial capture batch,
including transaction identity and exact pin ownership. A later takeover changing
the current claim is not evidence that the original capture rolled back. Partial
or inconsistent evidence fails explicitly. A root-release receipt must not be
mistaken for proof that the original registration never happened.

After confirmed rollback, release only the local capture resources and record no
durable capture-drain receipt. After confirmed commit, complete or confirm the exact
durable drain receipt. Both paths must wait for actual accepted Work and pin Uses;
neither may fence unrelated readers. A different initial winner must not cause the
losing tentative capture to acquire the winner's identity or drain receipt.

### Initial capture classifier checkpoint

`RepositoryInitialHistoricalCaptureState` now supplies a private, process-authorized
classifier for a joined initial registration attempt. It compares the exact initial
preparation, modes, coordinator and capture identities. An unrelated same-key winner
cannot make a rolled-back capture registered. Partial evidence fails explicitly.

A changed current claim after takeover does not erase initial registration evidence.
For a completed root release, the classifier uses the existing terminal and release
inspectors to check the permanent receipt and capture fingerprint, including drain
records, rather than requiring deleted root rows. Classification neither records a
drain nor grants an execution handle. The initial owner must still enforce stop/join
before invoking it; that owner and managed routing remain unimplemented.

### Retained initial generation checkpoint

`beginInitial` now reserves an entry and byte capacity before capture admission.
It retains the exact preparation, caller, modes and coordinator incarnation, with
encoded-digest comparison for reconstructed retries. Initial entries occupy the
same capacity and selected-route/all-generation maps as successors.

`RepositoryInitialHistoricalAttempt` owns the registration within that entry.
Accepted source Work survives closure of new source admission. A returned uncertain
registration is classified before retry: committed registration loads only the
exact live initial claim and owner; rollback retries the retained proposal. The
entry retains one execution object, so subsequent calls retain acknowledged START
and the existing CREATE/publication flags. Cold persisted START alone still cannot
recreate that execution object.

Initial retirement and shutdown use operation-scoped process authority and the
actual initial claim, without a fabricated reservation. Disposal joins accepted
calls, closes execution, releases root Work and classifies the exact capture. Held
Work keeps the entry retained. Initial-to-successor takeover uses the existing V97
and V93 path; retiring the old entry preserves the selected successor route.

The new SQL tests cover retained START, source closure, before-commit rollback,
lost commit reply, reconstructed retry, capacity before capture, unused-slot
shutdown and initial-to-successor overlap. These fixtures supply historical source
provider observations; they do not qualify new initial-owner publication against a
real object provider. Packaged-provider coverage, cold retention-anchor discovery,
managed host routing and library/gRPC parity remain required before public enablement.

### Initial owner provider checkpoint

The packaged initial-owner probe now publishes through the real PostgreSQL and
LocalStack adapters without a successor reservation. Normal CREATE and a lost
CREATE reply both resume the same retained initial generation across calls. Exact
provider bytes, receipt replay, repeated-publication refusal and capture drainage
under held Work are asserted. The focused gate passes; evidence is recorded in
`docs/evidence/repository/2026-10-07-initial-owner-provider/`.

The full storage gate includes this host as an additional isolated database; its
result remains pending. This closes the private initial-owner provider coverage
item above, but does not enable public historical routing or prove cold anchor
loading, managed host composition or library/gRPC parity.

### Cold retention-anchor loader design

The next private component loads retention evidence for a reserved successor. It
must not return an execution, assessment, read pin or provider handle. Its inputs
are the committed reservation, the exact predecessor owner and preparation, a
host-supplied process authority, and the current execution caller. Reuse the
reserved-preparation state check instead of inventing a second reservation test.
The current predecessor record has its own memory lease. This loader runs before
the reserved successor is installed. The reservation check requires a live claim,
an expired predecessor owner and no install. An installed predecessor can be
inspected under a new reservation; an installed current reservation cannot be reused.

Discovery follows `repository_successor_installs`, whose immutable edges bind the
current preparation digest and owner nonce to the preceding preparation digest
and nonce. Every edge must agree on account, principal, operation and command.
Start at the supplied predecessor preparation, not the greatest generation in
SQL. Stop only at a matching sealed history set with its sealed initial capture
and owner binding; a missing edge or inconsistent anchor is an explicit failure.
Do not select generation zero by convention or accept a numerically older record
without an ancestry proof. Check any historical activation's recorded retention
identity against the discovered anchor when present.

Count the planned successor edge toward the activation limit of 64 install edges.
At most 63 persisted edges can separate the predecessor from the anchor. Exceeding
that bound is an unsupported recovery depth, never permission to drop ancestors.
This is a chain-length bound, not a limit on the number of independently retained
operations. Only the anchor preparation is decoded; intermediate edges and
preparations require bounded identity metadata. No recursive load of all records
or unbounded SQL result collection is needed.

The loader has two short database phases around bounded decoding:

1. Require private authority and the exact reserved state. Authorize pending
   observation of the complete command for the execution caller before revealing
   lineage or source metadata. Read immutable lineage metadata without taking
   preparation/root locks after document locks in this phase. Inspect the anchor's
   encoded size before fetching bytes. Reserve the bytes before allocation.
2. Decode outside database locks using the existing preparation codec and digest
   checks. Reenter the reserved-state check, recheck the immutable anchor digest,
   recheck the identity chain and verify live retained roots with
   `DocumentHistoricalRetentionBinding.require`,
   and repeat execution-caller authorization before returning the owned result.

Keep claim/owner checks before preparation/root locks and document observation
locks. Do not hold provider calls or Java decoding under those locks. A released
root set cannot support a fresh historical capture even if its permanent release
receipt is valid. Cancellation, budget refusal and every decode or authorization
failure close the byte lease. Closing the returned result releases its ownership;
subsequent capture and successor activation must perform their existing checks.
Delivery-time authorization is not an enduring grant for later provider access.

Required PostgreSQL cases for this component:

- Initial anchor and a chain with at least two installed successors resolve the
  same original preparation; the immediate predecessor differs from that anchor.
- An installed but unactivated predecessor resolves without fabricating activation
  evidence. Existing activation evidence, when present, must name the same anchor.
- Wrong reservation, principal, account, command, digest or owner nonce refuses
  delivery. Missing edges, malformed bytes, incomplete capture evidence and
  released roots cannot fall back to another generation.
- Current source READ denial and credential revocation refuse delivery, including
  a barrier-controlled revocation between the first phase and final authorization.
- Cancellation, byte-budget refusal, decode failure and explicit close return the
  budget to baseline. Test the planned edge too: 63 persisted edges fit, while 64
  persisted edges must fail before new capture.
- Snapshot counts prove the loader creates no claim, START, capture, activation,
  assessment, publication or drain receipt. A cold START never becomes live Work.

This is a design requirement, not an available API. Implement and review the
loader and these tests before composing it into managed historical routing.
Sol reviewed the design; the pre-install restriction and planned-edge count
are required parts of that review.

### Restart lookup implementation checkpoint

`RepositoryHistoricalRetentionLoader` now reads the original preparation through
verified install ancestry under an exact, uninstalled successor reservation. It
checks initial capture identity, pin count/digest and current source authority in
two database phases around decoding. Its closeable result owns only preparation
metadata and a byte lease. It cannot restore execution or provider handles.

SQL evidence covers two successors, damaged ancestry and captures, identity and
memory rejection, and access revocation, cancellation or installation between
read and delivery. Corruption tests first exposed missing capture and epoch checks;
the corrected cases pass. The evidence directory is
`docs/evidence/repository/2026-10-07-retention-loader/`.

This is a private checkpoint. Initial-anchor, unactivated-predecessor, ancestry
limit and released-root cases remain, along with managed routing and transport
qualification. Do not remove the public historical restriction at this checkpoint.

### Managed provider-read integration

The existing engine already implements exact historical reads in
`DocumentPartReader.readHistorical`, including backend/version resolution,
provider-worker pin ownership and delivery authorization. The coordinator's
`DocumentRetainedReader` port accepts a current `PinnedPlan` only. Managed
historical publication must reuse the engine reader through a separate small
port rather than importing engine classes into the container module or duplicating
provider logic in the runtime.

The port must accept a ledger-issued historical capture plus explicit selected
revision ordinals, returning verified bytes with their ordinal association and a
closeable owner. Read only selected fragments. Preserve provider generation,
namespace and version; never resolve through the current drive or latest object.
Cancellation must leave pins and bytes owned until actual provider work exits.
Recheck current source authority before delivery, including failures that would
otherwise reveal provider details. Keep the existing payload and concurrency caps.

`DocumentPublicationSchemaScopes` remains call-owned and thread-confined. The
historical assessment preparation resolves schema selections synchronously and
retains copied evidence, not that resolver callback. Complete that preparation
before closing the call scopes. A retry that needs new resolution opens scopes
for the current call; it cannot reuse a closed request's resolver.

This read port does not enable public historical publication. Generation routing,
recovery observation and retention loading, provider preparation, receipt handling
and shutdown still need composition and common library/transport qualification.

### Restart lookup qualification

The initial-anchor, unactivated-predecessor, 63/64-edge limit and terminal
root-release cases pass. Retaining metadata does not retain source references.
A permanent release returns retention-unavailable before live-root corruption
checks. The exact concurrent release classifier remains unchanged.

The combined 7-suite run passed 64 tests with no failures, errors or skips.
Reports and source hashes are in the retention-loader evidence `qualified/`
directory. This supersedes the missing private-loader cases listed above;
managed routing and public library/gRPC qualification remain unfinished.

### Managed runtime composition sequence

The provider-read interface is now exposed by `DocumentHistoricalRetainedReader`.
The engine implements it using its existing exact-ordinal read. Its focused test
uses PostgreSQL and LocalStack and reads the retained provider version after the
current key is overwritten. The expanded eight-host storage gate also passed at
`4e595c4a4b91762fbc16a4507adefd4a40bac2b7`; its report is under
`2026-10-07-initial-owner-provider/full-storage`. These results do not qualify the
managed historical entry point.

Implement the remaining composition in this order:

1. Add a private preparation component that borrows an already accepted historical
   Work and its captures. Deduplicate provider reads by source address, revision
   and complete-revision ordinal. Validate every selector against the ledger's
   prepared reference before issuing reads. Map the returned fragment to each
   destination member and ordinal without confusing source and target ordinals.
   Read only selected fragments through DocumentHistoricalRetainedReader. Keep
   batches open until budgeted fragment copying finishes; close all batches on
   failure without releasing the owner's source Work. Ordinary reuse and upload
   fragments retain their existing admission checks.
2. Add an explicit managed-host historical configuration. Existing factories keep
   their current behavior and reader bounds. The host supplies the historical
   reader and private operation-scoped authority; request fields cannot enable
   either. Reserve an owner slot before acquiring captures or starting provider
   reads. Attach captures and accepted Work before preparation. If attachment
   fails, retain responsibility for capture drainage and report cleanup failures.
3. Route historical commands before the ordinary operation-wide recovery guard.
   Keep the outer call, validation, payload budget and permit boundaries. Observe
   authorized durable replay before selecting a local generation. Initial retry
   must reuse its seeds, incarnation and preparation. Local successor selection
   must preserve all older generations for disposal. Cold recovery uses verified
   retained preparation, then fresh captures and activation; START alone cannot
   recreate process Work. Do not place provider I/O under a map-wide monitor.
4. Retain assessment, START, CREATE reconciliation and publication state through
   the existing attempt owner. Schema scopes remain owned by the current call.
   Return only a receipt re-observed under the caller's current authority. A
   timeout or lost reply cannot discard a possibly committed attempt.
5. During shutdown, stop outer admission, drain accepted calls, then detach every
   historical generation before shutting down readers and providers. A retained
   old worker must not prevent another ready generation from being examined.
   Timeout leaves the runtime retryable and its resources owned. Only complete
   drainage permits host quiescence and resource closure.

Before public enablement, qualify the same cases through the library and actual
in-process gRPC adapter: initial publication, exact retry, restart from retained
preparation, lost CREATE and commit replies, old/new generation overlap,
revocation before delivery, cancellation with a held provider worker, mixed
upload/current/historical input, shared source deduplication, capacity refusal,
and shutdown retried after worker exit. Assert bytes, versions, receipts, durable
counts and restored budgets. Test non-opted-in hosts explicitly. Keep the public
unsupported guard until this complete composition passes; adding the reader port
alone does not justify removing it.

### Reader-backed assessment ownership

The private retained attempt now accepts historical preparation through the reader
interface. The assessment wrapper forks accepted Work before provider access and
creates its fragment snapshot after mode and opaque-source preflight. That snapshot
transfers directly into the assessment; it is not copied through a temporary
payload map. Registration and source Work follow the existing assessment cleanup
path. Calls remain serialized per borrowed attempt/execution, without holding the
installed-attempt map monitor during provider work.

For managed capture composition, reuse DocumentReadLedger's existing ownership of
all issued handles, including captures with uncertain commit replies. On an
attachment failure, close acquired captures and leave failed SQL release owned by
the ledger's bounded releaseDrained/reconciliation paths. Do not discard handles
or create a competing cleanup registry. Runtime shutdown must detach historical
attempts before DocumentReadLifecycle closes reader admission and attests local
quiescence. Managed capture acquisition and this shutdown wiring remain unfinished.

### Historical upload coordinator composition

Ordinary DocumentUploadCoordinator admission rejects historical owners with execution
claims. The private historical stageUploads entry now supplies operation-specific
authority. Initial-owner and successor provider tests qualify that entry. Other
tests still use direct transfers and do not
provide evidence for this coordinator entry.

Reuse existing payload budgets, backend resolution, workers, flushing and heartbeat.
An internal transfer authority must apply historical authorization and ownership
checks in the same transaction as admission, renewals, observation verification,
final state verification and post-preparation checks. Extract EntityManager-bound
operations; avoid nested transactions. Provider I/O remains outside SQL. Check
current authority after workers exit. Uncertain writes require reconciliation with
the original attempt identities. Implicit retries are prohibited.

Fork source Work and a registration child under the execution monitor, then release
the monitor before running transfers. Background callbacks into synchronized mutate
would otherwise deadlock. Retain protection until workers, heartbeat and flusher
exit. Freeze DocumentHistoricalSuccessorBinding's verified activation receipt or
synchronize that check independently; preserve receipt equality.

Sol reviewed this design. Barrier tests remain required for
expiry, revocation, late observations, uncertain provider replies and shutdown.
Public historical publication remains disabled pending integration and conformance.

The successor binding now atomically pins the first verified activation receipt.
Each later registration check compares the complete receipt with that value;
capture checks read a single published value. PostgreSQL tests exercise concurrent
callers, but registration row locks can serialize receipt verification.

A transfer child must also reserve its own metadata budget before execution:
source-pin and mode bounds, plus successor binding bytes when applicable. Fork
accepted Work and registration while the parent execution monitor is acquired,
and clean up every acquired resource if construction fails. Release the monitor
before starting upload workers. Keep all three child resources until the
coordinator returns or throws after worker drainage. Parent close can then release
its own reservation without leaving the child's referenced metadata unaccounted.
The private stageUploads path now creates that child and applies the coordinator's
SQL lock and statement timeouts to its transactions. Admission, renewals,
observation verification and delivery use the existing complete historical
authorization transaction. Provider work runs outside the parent execution monitor.
The initial-owner provider probe now uses this path and verifies exact selection
replay before assessment publication, including lost CREATE acknowledgement.

The request Attempt monitor still serializes its operations. The checkpoint below
records the qualified shutdown and fault cases and the remaining coverage.
Selection equality alone does not count provider writes; the later fault tests
instrument the real adapter. The configured SQL limits
bound individual locks and statements, not total transfer duration. Public routing
remains disabled.

### Historical upload race qualification still required

Use barriers around real provider calls and real PostgreSQL transactions. Exercise
initial and activated successor executions separately. Record provider effects,
selected attempt rows, current claim identity, source captures and byte reservations.

- After an actual PUT but before its observation returns, revoke source READ or
  destination WRITE. Release the provider barrier. Staging must fail, and the late
  observation must not authorize assessment creation or publication. Account for
  the stored object as an uncertain or abandoned attempt, rather than deleting it
  while its provider worker is active.
- At that same barrier, expire or supersede the execution claim. The original
  worker must fail its current-owner check. A successor must use its own attempt
  and token; it cannot accept the predecessor's late observation.
- Close the parent execution while a direct private transfer is active. Its child
  must keep source protection, registration and metadata accounting until workers
  exit. Test the request owner separately: closing admission must prevent new
  requests, and detach must respect its timeout while the existing request runs.
- Cancel during provider I/O and after observation verification. Both paths must
  drain actual workers and release the child budget exactly once. Future
  cancellation alone cannot establish drainage.
- Block the registration row in another transaction. The configured SQL lock
  timeout must terminate transfer authorization without starting a provider write
  or leaking the child resources. Test a later callback independently from initial
  admission to qualify the worker cleanup path.
- Return an error after the real provider accepts a write. A retry must not issue
  an implicit replacement write. Reconciliation must use the original attempt
  identity and verify the real object before any successful replay.
- Commit observation verification in PostgreSQL, then lose its acknowledgement.
  Staging must report failure and drain. An exact replay must recover the same
  VERIFIED attempt and token without another PUT or a replacement selection. This
  differs from a lost provider reply before SQL verification has committed.
- Count real provider writes on a successful exact replay. Stable selection
  equality is already checked, but does not establish this provider-effect claim.

These are acceptance cases; the evidence below covers a subset. SQL timeouts apply to each
statement and lock; provider deadlines and total request cancellation are separate.
For revocation and takeover tests, commit the policy or claim change before releasing
the provider barrier, so the expected transaction ordering is explicit.

Qualification checkpoint:

- The full storage gate passed with the successor uploader, including mixed and
  historical-only revisions. See `2026-10-08-successor-upload-storage` evidence.
- SQL lifetime tests cover successor parent close, cancellation during admission
  and initial claim-lock timeout. They use no provider I/O. See
  `2026-10-08-historical-upload-lifetime`.
- Real initial-owner provider tests cover READ denial, cancellation after PUT,
  lost provider reply, verified replay with one PUT, and lost SQL verification
  acknowledgement. See `2026-10-08-historical-upload-provider-faults` and
  `2026-10-08-historical-verification-reply`.
- Initial-owner admission shutdown retains provider work and memory after future
  cancellation until actual completion. See `2026-10-08-historical-provider-shutdown`.
- Initial-owner lease expiry after a real PUT rejects verification and retry while
  preserving the stored object for reconciliation. See `2026-10-08-historical-upload-expiry`.
- Successor installation before the predecessor PUT response fences that response.
  After the predecessor fails, fresh capture and activation let the successor stage
  with its own attempt and token; replay adds no PUT. This is same-process staging,
  not publication or restart recovery. See `2026-10-08-historical-upload-takeover`.

The evidence directories are under `docs/evidence/repository/`. Remaining cases
include successor provider faults and expiry, destination-only revocation,
cancellation after verification and SQL timeout in a later callback.
Managed public routing and transport parity remain unfinished.

### Retained progress for managed dispatch

The managed driver needs local progress before choosing an action on a borrowed
attempt. `Attempt.progress` distinguishes attached sources, retained assessment,
execution state, acknowledged CREATE and disposal-only ownership. The execution
snapshot distinguishes an attempted START, observed START coordinates and a
positively acknowledged INSERT. Loading coordinates after a lost reply does not
grant CREATE authority. CREATE and publication attempt flags retain their existing
mutation boundaries; an acknowledged CREATE is recorded only after success or
positive reconciliation.

These immutable snapshots guide local routing only. They neither establish a live
claim nor authorize source reads, publication or receipt delivery. Each action
still checks its own authority and durable bindings. In particular, publication
attempted does not mean committed; the driver must observe the durable receipt.
A failed START can leave attempted true even when no insert committed. A later
successful insert can establish acknowledgement; loading an existing START cannot.
The PostgreSQL and packaged-provider checks are recorded in
`docs/evidence/repository/2026-10-08-historical-progress/`.

Runtime shutdown composition and the private initial-owner rejection path are
qualified below. Preserve the public historical restriction until the remaining
rejection cases, cold recovery and library/transport acceptance cases are complete.

### Historical runtime shutdown composition

The package-private `DocumentPublicationRuntime.historicalJournaled` factory now
owns a historical attempt registry alongside ordinary sessions. Existing public
factories and the facade's historical restriction are unchanged. The supplied
reader must implement historical, ordinary and assessment reads with one lifecycle.
The host supplies the exact-operation drain authority independently of requests.

For this composition, close stops outer admission first. Shutdown waits accepted
calls, then closes and detaches all historical generations before closing nested
provider admission, external workers and the shared reader. A failed authority
lookup or incomplete detach leaves those resources available for a later shutdown
pass. A held generation does not stop disposal of another ready generation.

The internal `withHistoricalAttempts` action now owns the runtime call scope from
entry through synchronous completion. Close refuses new actions; shutdown cannot
detach generations until an already accepted action returns, including between
individual registry borrows. Registry references and borrowed calls must remain
inside the action. Future facade dispatch must preserve its existing accepted-call
scope across routing rather than treating continuation as a fresh external call.
The factory does not yet dispatch historical requests or perform cold recovery.
Qualification is recorded under
`docs/evidence/repository/2026-10-08-historical-runtime-shutdown/`.
The subsequent accepted-action check is under
`docs/evidence/repository/2026-10-08-historical-runtime-scope/`.

### Private historical validation rejection

The retained attempt now composes assessment capture, whole-assessment replay and
the existing durable rejection gate. It requires the original acknowledged CREATE
and exact upload selections. Historical Work stays attached while a separate
assessment read owns its replay inputs through the decision transaction; reader
capacity must accommodate both. No source protection is released to make room.

For a pending decision, the gate locks the exact claim before the operation owner.
Historical registration, START, modes, sealed assessment, retained slot bindings
and capture checks remain part of the transaction. Preliminary authorized terminal
observation takes an owner shared lock while the exclusive claim lock is held;
registration and the owner write lock follow. Dedicated contention qualification
must cover that ordering rather than assuming it from sequential success.

Receipt replay runs before live mutation checks. The first retry test exposed a
terminal-owner write attempted by the wrapper; that preflight was removed. The
fixed real-provider test produces a rejection from invalid mixed historical/upload
content, confirms no publication, lets the claim and owner expire naturally and
retrieves the same rejection through the retained attempt. See
`docs/evidence/repository/2026-10-08-historical-rejection/`.

This is private initial-owner qualification. Successor rejection, lock contention,
revocation at decision/delivery, lost rejection acknowledgement and cancellation
still need historical-path coverage before managed/public enablement. The broad
storage regression is recorded separately when it completes.

Initial-owner rejection also passes a lost-commit-reply test against PostgreSQL
and LocalStack. Retry returns the original receipt with the provider reader
closed. Session cleanup and historical source retention are checked separately.
See `docs/evidence/repository/2026-10-08-historical-rejection-reply/`.
Restart recovery, successor rejection and decision races remain acceptance work.
Public routing remains disabled.

Successor rejection now passes with real PostgreSQL and LocalStack. The receipt
identifies the replacement generation and assessment; no revision is published.
Retry requires no provider read, and normal retirement releases the entry while
preserving receipt replay. See
`docs/evidence/repository/2026-10-08-successor-rejection/`.
This does not establish restart recovery or concurrent rejection behavior.

Successor receipt replay is also tested across separate accepted runtime calls,
with the provider reader closed. Credential revocation after retirement prevents
client delivery while authorized recovery preserves the original receipt. See
`docs/evidence/repository/2026-10-08-successor-rejection-delivery/`.
Revocation during a pending decision remains separate acceptance work.
