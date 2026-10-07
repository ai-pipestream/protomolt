# Managed historical attempt ownership

Status: reviewed design; a private installed-plan owner is implemented. Public
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

Still required before managed use: reconcile uncertain CREATE into
an exact verified stage without reissuing CREATE; retire authenticated terminal/fenced
entries during normal service operation; own proposal/install uncertainty; and wire and
qualify both library and transport entry points. Currently entries remain until shutdown,
so capacity can fill over a long-running service. The facade gate remains unchanged.

## Next: reconcile an uncertain CREATE

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
