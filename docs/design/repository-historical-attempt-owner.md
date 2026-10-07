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
