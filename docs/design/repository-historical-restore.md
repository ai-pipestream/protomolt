# Historical revision restore

Status: contract design; no restore API is available. This is a bounded document
revision operation, separate from interrupted-execution recovery and optional JCR
version restoration.

## Operation and identities

Restoring revision r1 while r3 is current publishes a new revision r4. It does not
change r1, reset a revision counter, or overwrite r3. The caller supplies the
expected current destination revision; a concurrent r4 publication makes that
condition stale and refuses the restore. Existing multi-member publication remains
the atomic visibility boundary, so this design does not force one document per
transaction.

Retain `PublicationReuse` unchanged. Its source is a current pre-change dependency;
`DocumentPublicationCommand.validateAggregate`, `DocumentAdmissionAuthorization`,
and `DocumentReuseAdmission.requireBoundSources` enforce that meaning. An older
same-address revision cannot be represented by changing that dependency check.

The proposed additional `DocumentPublicationPart` content alternative is
`historical_reuse`, with an exact source address, revision UUID, full manifest
ordinal, slot, and immutable `PublicationObjectIdentity`. The ordinal counts empty
and deleted positions, matching historical reads. All selectors must agree with
the retained manifest and catalog. No arbitrary physical key adoption, latest
revision fallback, source revision-number guessing, or provider remapping is allowed.
The first version preserves the source slot at the destination and permits only
same-account selection. Count historical selectors against existing aggregate source,
part and byte bounds; reject noncanonical UUIDs and size overflow.
Field numbers and the final protobuf message name remain subject to contract review.
Do not expose this alternative until shared handlers can enforce it.

Historical selection is not inserted into the current-source revision map. Other
explicit current dependencies retain their existing checks. The canonical command
must bind the exact selector and identity into retries, assessment evidence and
receipts. The destination's expected current revision remains independently bound.
Changed source, destination condition, selected slot or physical identity under the
same operation identity is an idempotency conflict.

## Reuse the existing read foundation

`DocumentReadLedger.captureHistorical` already captures an exact native revision
under current READ authorization and acquires retention pins. Its `PinnedHistory`
issues a `Use` whose `DocumentHistoricalReadPlan` contains immutable manifest
ordinals and original physical bindings. `authorizeDelivery` checks current access
again. Build on these lifetimes rather than creating a parallel reader registry.

Keep the Use through actual provider completion, including cancellation, and through
any returned byte batch. Closing an outer handle does not establish provider
quiescence. Existing bounded admission, drain and failed SQL-release handling must
remain in effect. A selector or retained plan is evidence, never write authority.
A future owned selector helper is justified only where it removes duplicated
selection and lifetime checks in the actual restore path.

## Admission, schema and ownership

Recheck current source READ, destination WRITE, expected destination revision and
current admission policy. Historical ownership is provenance, not a present grant.
The initial bounded implementation should update an existing destination; absent
creation remains dependent on its separately reviewed authorization policy.

For typed occurrences, use the historical bindings and exact complete retained
descriptor closure. Preserve unresolved/opaque classifications as such; an opaque
revision may have no descriptor closure. Never replace definition A with the
current registry definition B merely because their type URLs match. Registry
absence must not prevent reading retained A. Restoring opaque bytes under a current
policy requiring a resolved type must fail explicitly unless a separately specified,
identity-bound resolution operation supplies the missing definition.
`DocumentHistoricalSchemaRows.capture` currently refuses opaque revisions. An
explicit current-policy-allowed raw path is therefore required; do not catch that
refusal and silently report typed validation success.

`DocumentHistoricalSchemas.check` proves historical integrity using the recorded
policy. Its result is not a current admission verdict. Run current admission against
the selected bytes and exact retained definitions, and bind the new assessment to
the restore command, current policy, attempt and destination condition. Refuse an
incompatible policy; do not mutate historical evidence to make it pass.

Preserve original part provenance while recording the new revision's actor,
operation and derivation from the exact historical revision. A restore must not copy
historical ACLs over current ownership as a side effect. Metadata selection needs
an explicit reviewed policy before adding any metadata restoration option.

## Retention and commit

Physical read pins protect selected bytes during preparation. A restore must also
retain its exact schema closure through pending assessment and publication. Current
V48/V55 guards conservatively prevent schema mutation; that is not a pruning
protocol. Before relaxing them, one liveness decision must cover committed revisions,
pending restores/assessments, active reads, recovery and optional future graph
references.

Stage verification outside SQL locks, then atomically check current authorization,
destination conditions, original physical identities and retention witnesses while
publishing the new references and receipt. Reuse existing deterministic lock order;
no provider or registry I/O under publication locks. Bind the source revision UUID,
full manifest ordinal/slot, object ID, complete provider identity and DOCUMENT_HISTORY
reference in that transaction; hold the Use through its completion. A rolled-back
transaction leaves no new visible revision or success receipt. A lost commit
acknowledgment requires exact durable outcome reconciliation before retry.
Release pins only after work drains and either committed
references protect the objects or explicit failure cleanup owns them. Exact retries
must reconcile durable outcomes before repeating work.

## Implementation sequence and acceptance

1. Compile and validate the reviewed historical selector contract with complete
   imports. Exercise required alternatives, UUID/ordinal bounds, slot and identity
   shape through the real runtime validator. Handler checks establish membership,
   account authorization and source retention; annotations cannot establish these.
2. Extend canonical command analysis, source authorization, capture, assessment
   slots/evidence and commit reference checks together. Fail closed on unsupported
   alternatives. Audit all `hasReuse` and implicit upload-or-reuse branches, including
   schema manifests, digest/size calculations, journal codecs and replay.
3. Integrate current admission with exact retained schemas. Preserve byte reuse and
   original backend identity. No restore wrapper may grant process authority.
4. Run real SQL/provider cases locally and through shared transport: r1 selected
   while r3 is current; stale destination; wrong revision/ordinal/slot/object;
   cross-account denial; READ/WRITE revoked at capture and commit; policy changed;
   registry absent or containing B while retained A exists; opaque policy refusal;
   exact retry and changed-selector conflict; failed finalization; cancellation
   during provider reads; held-read cleanup race; shared bytes without reupload.
5. Qualify pruning and backup separately before enabling deletion. Restore matching
   SQL metadata, retained schemas and exact provider content into an isolated host.
   SQL dump success alone does not establish a recoverable repository.

## JCR boundary

The existing [JCR assessment](repository-jcr-compatibility.md) requires reusable
multi-object atomic commits, stable identity and reference retention. This document
preserves those extension points. It does not define frozen graphs, child identity
collision behavior, checked-in restrictions, JCR sessions/workspaces, or strong
reference restoration. Those belong to the optional content-repository extension;
base storage gains no JCR dependency or compliance claim.

## Contract staging checkpoint

The checkpoints below record their original boundaries. The later canonical-command
checkpoint moves refusal from construction to execution entry points; references
below to the command-construction guard describe the earlier checkpoints.

`PublicationHistoricalReuse` and the `historical_reuse` content arm (tag 5) now
exist. Real runtime validation covers generated and dynamic messages; JSON Schema
exposes ordinal bounds and required message fields, while CEL remains runtime-only.
The canonical command deliberately refuses this arm until shared execution handles
it. Existing v1 golden bytes/hash fixtures pass unchanged; older consumers will
reject the new field. Before activation, review command version and consumer
capability negotiation rather than assuming additive wire compatibility establishes
executable compatibility. [Evidence](../evidence/repository/2026-10-05-historical-contract/README.md).

## Exact source-selection checkpoint

`PinnedHistory.selectRetained` now checks a bounded batch of historical selectors
against its existing captured plan and caller-owned Use. It authorizes current READ
once for the batch before exposing binding mismatch details, compares full manifest
ordinals and every immutable physical coordinate, then rechecks control and Use
liveness before returning ledger-issued entries. It creates no replacement pin or
reader registry. Empty/deleted manifest positions cannot masquerade as compact
present-entry indexes. Matching uses binary search over the SQL-ordered entries.

The owner must retain the Use through provider completion and the eventual new
reference transaction; a returned entry cannot extend its own lifetime. Existing
historical references and reader pins remain the retention mechanism. Current-policy
schema admission, pending schema liveness and final historical reference checks are
still required before enabling restore execution. The command guard remains.
[SQL evidence and limits](../evidence/repository/2026-10-05-historical-selection/README.md).

## Retained-definition assessment checkpoint

`DocumentRetainedSchemaResolution` supplies exact occurrence-bound definitions to
a new assessment without consulting a registry or inheriting historical acceptance.
It supports selecting a subset and remapping ordinals while preserving root slots,
paths and value identities, including different definitions under one type URL.
Source-loading bounds are separate from the current destination policy. Source
ordinals cannot repeat; destination ordinals retain the protocol ceiling. The host
must authenticate the source, own artifact buffers and hold its Use, and verify
destination slot and physical identity. This resolver grants no read or write access.

Existing historical replay additionally requires the complete source asset union;
new assessment checks the selected union. Current-time CEL and current policy run
again. The library recognizes historical content size and hash, but command execution
still refuses historical reuse. SQL/host composition, current authorization at commit,
pending schema retention and publication recovery remain required before activation.

Selected decoding now also recognizes historical-reuse payload declarations and
checks their exact size and hash against retained evidence. This is a decoding
prerequisite, not source authorization or a restore operation.
[Materialization evidence](../evidence/repository/2026-10-05-historical-materialization/README.md).

The next integration must carry an owned historical source selection, keyed by
destination member and full ordinal, through preparation and commit. Reuse the
source `PinnedHistory.Use` and exact selector checks; keep historical sources out
of the current-source revision map. `DocumentUploadPlan`, publication input and
fragment capture, assessment slots/replay, and commit reference binding currently
assume upload/current reuse. Extend those together before removing the command
guard. Final publication must recheck current source READ, destination WRITE,
physical identity and retained references while the source Use remains held.

## SQL-backed current-policy assessment checkpoint

The private `PinnedHistory.assessRestore` now performs exact historical selection,
owns fragment copies and a source Use, captures retained schema assets under current
READ, and assesses one historical/empty-only member against a supplied current
policy. Stored root identity columns are checked independently of encoded evidence.
Every new result view reauthorizes the source; close releases assessment resources,
fragment reservations and the Use. This result does not authorize the destination
or fence the active policy. Opaque and multi-source/mixed-content preparation remain
outside this helper. Full command integration and atomic reference publication are
still required; the historical command guard remains enabled.
[SQL assessment evidence](../evidence/repository/2026-10-05-restore-assessment/README.md).

## Historical reference transaction checkpoint

`DocumentHistoricalReferenceAdmission` prepares exact, bounded source claims from
the live pinned selection before write locks. Inside the write transaction it
requires transaction-local origin/retention locks covering all selected objects,
then verifies sealed successful native revision membership, exact original physical
coordinates and DOCUMENT_HISTORY reference ownership. Historical r1 remains usable
after r3 becomes current. The helper neither requires nor grants a current-source
revision condition. Its failures mark the transaction rollback-only.

Before wiring it into the native writer, add historical source addresses to the
complete deterministic current-authorization lock set. Fence the operation and
active policy, authorize source READ and destination WRITE before exposing binding
failures, then acquire drive/origin/retention locks and publish all new references
atomically while the source Use remains held. Command analysis, assessment retention
and replay must recognize the historical alternative consistently before removing
the public command guard. [Transaction evidence](../evidence/repository/2026-10-05-historical-reference/README.md).

## Historical authorization lockset checkpoint

Authorization preparation now accepts validated pinned historical sources and
requires their selector set to match the complete plan's historical arms. It bounds
count and bytes and keeps their addresses separate from current revision conditions.
The final authorization step locks the combined set once, checks current historical
source READ before destination WRITE and revision conflicts, and applies no old-head
CAS to historical sources. Per-target ordinal binding remains the writer's duty.

The positive SQL tests exercise this internal lockset directly; the public historical
command guard still prevents an executable restore. Full prepared-command acceptance,
input/assessment/commit integration and receipt replay authorization remain required.
[Authorization evidence](../evidence/repository/2026-10-05-historical-authorization/README.md).

## Canonical historical command and replay checkpoint

Canonical commands now accept validated historical selectors, count their sources
and bytes against aggregate bounds, and require stable UUIDs and preserved slots.
Historical revisions remain separate from current-source revision preconditions.
Changing the selected revision changes command identity; changing only the operation
ID does not. Existing command golden fixtures remain unchanged.

Execution remains unsupported. Candidate/assessment preparation, fragment capture,
upload planning, schema preparation, staging and claim/operation admission explicitly
refuse before consuming historical content or persisting an execution. The generic
encoded operation API reserves the publication codec for typed admission, preventing
that lower-level route from bypassing this gate.

Successful replay, rejection replay and precondition checks lock historical source
addresses and authorize current READ before destination checks. They do not compare
the selected historical revision with the current head. Revocation is tested using
real SQL state and canonical commands. Full staging, retained-schema provenance,
commit reference binding and recovery integration remain activation prerequisites.

## Next staging integration

Step 1 below is implemented by V86, with a V85 upgrade fixture and real SQL
historical/current-source tests. See [staging evidence](../evidence/repository/2026-10-05-historical-staging/README.md).
The direct SQL tests do not establish canonical command-to-slot correspondence.
Cross-account, missing-history-reference and retiring-object isolation cases remain
required before execution activation. Step 2 and final publication remain open.

Sol's review identified two separately reviewable steps. Neither enables restore:

1. Add the SQL declaration `HISTORICAL_REUSE` and immutable source-node identity.
   Historical rows require source node, revision and full ordinal; existing
   NEW_CONTENT/REUSE rows retain their shape and current-reuse checks. Validate
   exact source account, sealed successful native revision, part/slot/object,
   non-retiring retention and DOCUMENT_HISTORY reference. Do not join the current
   revision pointer for historical reuse. Prove r1 can stage after r3 is current,
   malformed/cross-account identities refuse, and rollback leaves no references.
   Exercise migration with existing rows. All Java execution gates remain enabled.
2. Integrate slot preparation/binding, creation inserts, retained-slot verification
   and replay snapshot encoding together. Snapshot v1 records only whether a source
   exists; it cannot identify historical declaration or source node. Introduce an
   explicit v2 encoding with version-dispatched reads of retained v1 snapshots.
   Update retained evidence and replay input size/hash handling before permitting
   historical CREATE. Keep authorization and owned source Uses through publication.

Then integrate immutable physical references, active-policy fencing and failure
recovery. A schema migration alone is not a restore execution implementation.

## Versioned staged provenance

V87 accepts immutable slot snapshot versions 1 and 2. New writes use v2: tags
distinguish upload, current reuse and historical reuse; historical records include
the exact source node in addition to revision and full ordinal. Version 1 retains
its original encoding and cannot represent a historical source. Replay reads the
stored version before reserving and reading snapshot bytes, then compares exact
re-encoding and digest under the retained-owner lock. Existing snapshots are not
rewritten or interpreted using the newest codec by default.

Retained slot verification now understands historical selector and physical
identity, and retained root evidence/input checks use the historical payload hash
and size. The historical creation binder remains gated: this does not prove an
end-to-end historical stage. Next, bind a live pinned source selection to each
destination member/full ordinal before allowing historical preparation. Add the
SQL negative case for a historical slot paired with a v1 snapshot, including
budget release on refusal, alongside command-to-stage correspondence tests.

## Pinned slot-binding checkpoint

The explicit internal slot preparation/binding overload now matches the complete
distinct historical selector set to the command, requires live source Uses and
transaction-local origin/retention locks, and binds every destination member/full
ordinal to exact historical physical identity. One selected source may feed several
destinations. See [binding evidence](../evidence/repository/2026-10-05-historical-slot-binding/README.md).

Assessment creation still uses the ordinary gated preparation route. Its physical
binder must next resolve historical objects alongside uploads/current reuse and
pass the same lock proof to slot binding. No source pin may drain before the staged
references are durable; current authorization and active policy must be fenced in
that transaction. This prerequisite does not yet establish an executable restore.

## Shared physical binding checkpoint

The explicit historical upload-plan preparation now carries borrowed source Uses,
validates complete selector/account identity, and leaves historical revisions out
of current-source CAS. Ordinary preparation remains gated. The shared physical
binder recognizes historical objects separately from uploads and reads their actual
ledger coordinates. Its internal historical assessment path acquires the complete
origin/retention lock set and returns that exact transaction's proof with the bound
objects for slot binding. Ordinary publication and assessment entry points still
refuse historical commands.

The SQL fixture composes this binder and slot binding for r1 after r3 is current,
with both a zero-upload/current-reuse case and a new-upload case. It uses ordinary
operation selection rows and synthetic provider observations, not an admitted
historical command. Next, assessment creation must carry the exact historical
command, current ACL/policy fence, retained evidence and source Uses together.
The operation ledger's command equality check must not be bypassed to do so.

Source-reference verification currently occurs in both physical and slot binding.
Consolidate that only with an exact transaction-scoped checked-reference proof;
neither a cached boolean nor an old source authorization is sufficient. This change
adds no provider I/O under locks and establishes no new performance measurement.

## Exact historical operation admission

The package-private unclaimed assessment route now admits the actual canonical
historical command, after binding the trusted caller's principal/account to the
operation key and checking complete live pinned preparations. It reuses the typed
operation identity/idempotency machinery. Changed command bytes under the same key
conflict; a caller/account mismatch is refused before an operation row is inserted.
Admission alone does not grant current document READ or WRITE.

Historical member/upload selection preparation carries the same references into
the existing authorization lockset. Selection staging verifies the exact admitted
command, caller, source READ, destination WRITE and revision conditions before
writing its rows. Claimed historical owners are refused. The physical binder test
now uses that actual admitted historical command rather than an ordinary operation
as a fixture stand-in. Ordinary admission, claims, sessions and public runtime
execution remain gated.

The retained-definition loader now owns its SQL asset reservations and attempt-local
resolver separately from the single-member assessment. It borrows an exact live
source Use, checks selected entries and ordinal mappings, and authorizes SQL loading
with the caller bound to that historical capture. It cannot accept a replacement
caller. Closing it releases the resolver before SQL reservations without closing
the borrowed Use. See [loader evidence](../evidence/repository/2026-10-06-historical-schema-loader/README.md).

Next, wire this loader into whole-command assessment. Current policy must assess
all members at one evaluation time and the real runtime observer must produce its
manifest. Do not substitute the earlier single-member assessment's verdict or
fabricate runtime evidence. Then connect CREATE with exact command/ACL/policy
checks, the source Uses, historical physical/slot binding, and retained schemas.

## Whole-command content preparation

The explicit historical fragment capture path now checks complete pinned source
references before inspecting supplied payloads. It checks all members and ordinals,
reserves their aggregate copy size, copies and hashes every payload, and rechecks
source Uses before returning. A source closed during copying refuses the result and
releases the reservation. Upload, current reuse and historical reuse have explicit
size/hash handling; ordinary capture still rejects historical commands.

The explicit raw structural helper also checks complete references before and after
assembly and recognizes historical payload identities. This helper does not select
an admission mode, resolve descriptors, authorize delivery or grant a validation
verdict. Its result borrows snapshot bytes. Typed-required calls remain refused.
The caller must keep the snapshot and source Uses alive through all consumers.
See [content preparation evidence](../evidence/repository/2026-10-06-historical-content-preparation/README.md).

### Composite typed assessment design

Whole-command assessment must use one current policy, command digest and evaluation
time, with explicit per-member modes. Each historical target ordinal routes to one
exact pinned source revision and source ordinal. Ordinary parts route to the current
resolver. A member can require several retained scopes; do not silently merge their
type-URL indexes. All sources within that member must agree on the exact container
definition, including metadata and descriptor/source bytes. A mixed member's ordinary
container must agree too. Different members may use distinct container definitions.

Extend retained completeness checking to verify each source's mapped target roots,
consume every selected occurrence exactly once, and check exact selected reference
and artifact bytes. Keep the existing whole-member completeness check. The completed
assessment must equal the union of all retained and ordinary selections; subset
checks alone must not permit unaccounted schema assets.

These library checks now exist in `DocumentCompositeSchemaResolution`. Retained
scopes expose their selected ordinals, references and exact borrowed artifact bytes.
The composite resolver verifies complete disjoint historical routes, exact container
agreement and the final reference/artifact union. Ordinary occurrences are tracked
with the definition returned for each occurrence, then compared against canonical
assessment evidence. Shared assets cannot hide a skipped resolver call, and swapping
two definitions under the same type URL is refused even when the union is unchanged.
Source scopes and input bytes remain host-owned; the composite owns its metadata
scratch and does not close the sources. See [composite evidence](../evidence/repository/2026-10-06-composite-schema-resolution/README.md).

The internal whole-command assessment now derives mappings from authenticated pinned
selectors, binds the exact caller and command/member identity, and reauthorizes
inspection and replay. One supplied policy and evaluation time govern all members.
SQL integration tests also combine CORE and PARSED from two distinct historical
sources into one member without registry access. Both source Uses survive outer
handle closure, and revoking the second source prevents further replay. These tests
cover retained schema routing, not actual provider reads or publication authority.
The internal unclaimed owner now prepares its physical plan from the same borrowed
source references and can retain evidence through observed-runtime CREATE. The CREATE
transaction uses current policy, source READ and destination authorization, exact
historical physical binding, and the same transaction-local locks for slot provenance.
The ordinary runtime, claimed sessions and public transport remain unchanged. Atomic
reference publication and historical-specific failure/reconciliation qualification
remain gated.

Opaque restore needs explicit source classification and current-policy mode selection
before raw assembly. A failed typed load or assessment must never trigger raw mode.
For the first implementation, refuse typed-source-to-opaque downgrade. Require each
historical source of an opaque target member to have an explicit sealed native
OPAQUE admission, with matching account, node, revision, commit, admission and
operation-success bindings. Missing admissions, legacy unknown modes and inconsistent
bindings remain unsupported or corrupt, not implicit opaque data. Verify selected
ordinals against their pinned plans. Run classification under the captured caller's
current READ and live Use; it grants no target-policy exception or write authority.
Keep this scalar classification separate from typed descriptor loading so opaque
preservation does not deserialize an unknown Any or require a current registry.
The internal assessment implements this classification in
`DocumentHistoricalOpaqueAdmission`, under the exact captured Use and current READ.
It checks the stored retention manifest without loading descriptor artifacts or
decoding Any payloads. The assessment owner keeps the existing fragment hash checks,
target-policy gate and inspection/replay authorization. Acceptance covers explicit
opaque success, typed-source refusal, admission-less source refusal, current
typed-required policy refusal, revocation, cancellation and reservation cleanup.
A two-source opaque member containing a typed source now has a SQL host regression
covering the typed source in either position, the specific downgrade error, and
release of both source pins and payload reservations. Provider observations in this
fixture are synthetic. All distinct sources are classified before fragment capture. Source
READ checks belong at delivery and commit; deterministic evidence replay remains free
of provider and SQL calls. Keep source Uses through observed-runtime CREATE and atomic
reference publication. The public route remains disabled while those integrations
and their tests are unfinished.

The historical owner returns only the CREATE identity and keeps its sources through
final authorization. A post-commit access revocation can withhold that result; this is
an uncertain outcome, not proof of rollback. Retain the proposed assessment ID and
reconcile before retrying. The physical plan's references are borrowed and expire with
the historical owner. No independent evidence or source lifetime is transferred.

## Atomic publication implementation order

Keep promotion and publication inside the Historical owner. A promoted candidate
must not escape and outlive its source Uses. Recheck the exact caller and current
source READ, prepare strict accepted proofs, stage artifacts outside write locks,
then call an explicit historical commit path with the owner's exact physical-plan
references. Close the candidate before releasing the Uses and return only the
terminal result. Ordinary entry-point guards remain in place.

The historical schema-batch path must receive complete live source preparations,
not a boolean that bypasses execution checks. Retention and the schema manifest
must read size, hash and physical object identity from historical declarations.
The commit writer must retain the historical manifest's producer and timestamp
metadata rather than interpreting a historical part as a new upload. Snapshot
these entries from pinned plans before write locks, keyed by full source revision
and ordinal or exact selector. Node and slot alone are insufficient when a command
selects two revisions of the same source slot.

Preserve transaction ordering: owner and command, current policy, destination
WRITE and source READ, drive/backend identity, independent origin and retention
locks, schema artifact claims, candidate construction, admission/retention writes,
and terminal result. Current-reuse manifest loading remains separate from the
historical entry map. No provider IO, registry resolution or descriptor validation
belongs inside this write transaction. Sol reviewed these integration boundaries.

Qualification must cover an older retained revision published as a new revision,
original provider identity and producer metadata, mixed ordinary/historical parts,
two revisions of one node/slot, atomic rollback and lost-acknowledgment replay.
Successful historical assessment CREATE does not establish publication correctness.

### Internal atomic publication integration

`Historical.publish` now keeps strict promotion, schema staging and publication
inside the source owner. It returns only the durable result and closes promoted
content before the source Uses can be released. Promotion consumes the assessment;
if staging or commit fails, reconcile an uncertain outcome rather than calling
publish again on that consumed scope. A returned SQL success is not subsequently
reclassified as cancellation by an extra authorization callback.

The dedicated commit path requires the complete exact physical-plan source
preparations and refuses execution claims. Ordinary publication retains its
historical-command guard. Historical publication checks current authorization,
policy, independent source/retention locks, original physical identity and schema
claims in the existing transaction order. It preserves retained part timestamps
and producer metadata and writes a new revision and terminal result atomically.
The source Uses are checked again before immediate constraint validation.

Initial real-SQL cases cover typed and opaque publication, reuse after the head
advances, policy changes, and rollback after failure at terminal-success insertion.
Additional real-SQL qualification now covers two revisions of the same source
node/slot with distinct payloads, physical IDs and provenance, and mixed current
and historical reuse. READ revocation while publication waits at its policy lock
refuses the operation without advancing the published revision. See the
`2026-10-06-historical-publication-races` evidence record for controls and limits.
The production-JAR storage probe also exercises a command containing separate
fresh-upload, current-reuse and historical-reuse members against versioned S3.
Leaving the real upload unverified must leave all three destinations absent;
verification permits atomic publication and exact replay. Sealed physical origins
are checked against the selected upload attempt and retained selectors. In this
probe current and historical reuse select the same bytes; the multi-revision SQL
case above covers distinct versions. A separate PostgreSQL test now publishes
historical CORE and a fresh PARSED upload within one typed member. It refuses an
unverified upload, preserves historical manifest entries and physical identities,
checks the fresh upload's selected attempt and byte metadata, and replays both
roots using retained schemas without registry access. Provider observations in
that SQL test are synthetic. Subsequent `HistoricalMixedPublicationProbe` coverage
also exercises the same-member path against a real provider; its
`HISTORICAL_MIXED_MEMBER_PROVIDER_OK` marker is required by
`DocumentAssessmentStorageRuntimeTest`.
See [same-member evidence](../evidence/repository/2026-10-06-historical-mixed-member/README.md).
Public restoration, claimed-session activation and automatic
claim transfer remain disabled. These are required follow-ups, not an assertion
that the full repository goal is complete.

## Claimed historical execution plan

Source audit baseline: `50e01d7e2aea365ec298df8bca9ce00488a160db`.
This is the next implementation boundary, not an enabled capability.

The wire selector and canonical command already bind the historical revision and
exact object. No new restore RPC or selector field is needed for this boundary.
The missing part is the normal coordinator lifetime: initial claim, sealed
preparation, owner registration, restart, successor fencing, and durable replay.

At the audit baseline, `DocumentPublicationPreparationRecord` validated and reconstructed via
ordinary `DocumentOperationUploadAdmission.prepare`. Historical plans instead
require live source preparations from `prepareHistorical`. The preparation codec
already stores the canonical intent; it must not serialize live Uses or pretend
that decoding an intent recreates pins. `RepositoryExecutionClaimLedger` and the
ordinary facade explicitly refuse historical execution. Historical assessment
creation also refuses an execution claim. Remove these barriers only at their
reviewed shared boundary, not as an independent enablement patch.

The first implementation now separates inert validation: historical records use
the same private member/placement/attempt loop and lease rule without returning
a prepared plan. Owner/attempt/token coverage still comes from
`DocumentPublicationSeeds`. The existing codec can round-trip the intent with
or without fresh uploads; no format change or duplicated selector list is needed.
Ordinary records retain their existing full preparation validation. Record
`prepare()`, claim acquisition and public execution still refuse historical
commands. This is serialization groundwork, not durable registration or recovery.

### Durable intent and live authority

1. Separate pure preparation-shape validation from acquisition of live source
   references. Persist the existing exact command, placements, owner/attempt seeds,
   mode binding and predecessor generation. Its historical selectors are the
   authoritative source identities; do not add a second independently mutable
   selector list. Decode into inert data, not an executable session.
2. Reacquire each distinct `(source address, revision UUID)` through
   `DocumentReadLedger.captureHistorical` under the authenticated caller. Use
   `DocumentHistoricalAssessmentSources` to check complete selector coverage,
   ordinals, slots, bytes and original backend bindings. Current-head advancement
   is allowed; source pruning, changed physical identity or revoked READ is not a
   reason to substitute another version. Reuse the existing byte budget and limits.
3. Bind the initial claim, preparation digest, modes and owner atomically using
   the existing registration journal. Source preparation remains live through
   registration, assessment and commit. Registration checks current credential
   generation, source READ, destination WRITE/creation authority and placement.
   A retained preparation is neither an access grant nor proof that pins survived
   a crash. Acquire resources outside SQL; SQL verifies the captured witnesses.
   Verify exact sources with the existing historical reference checks in the
   operation/policy, destination/source revision, drive/backend, independent
   origin/retention lock order before registration commits.
4. On resumed execution, authenticate again, reconcile a terminal result first,
   verify the stored command/preparation/modes, and reacquire fresh pins before
   constructing a historical execution owner. Keep existing successor epochs,
   owner fencing, deterministic lock order and database lease time. A replaced
   owner must not publish even if its provider work eventually returns.
5. Extend historical assessment creation and publication to accept only the exact
   current claimed owner and sealed preparation. Keep promotion inside the source
   owner. V83 assessment-start identity is sticky within an owner generation:
   reuse its exact assessment only after fresh authorization and policy checks.
   If it is ineligible, fail/fence that generation and use an explicitly qualified
   new-generation or new-operation protocol; never silently create another
   assessment in the same generation. Reconcile uncertain CREATE or consumed
   promotion against the exact durable identity first. Retained definitions remain historical; a
   newer registry definition under the same URL cannot replace them.
6. Release Uses after work drains and committed references or recovery own the
   physical effects. Cancellation before commit cannot create success. An uncertain
   commit is resolved by durable outcome lookup, not by repeating a consumed
   assessment or copying the selected bytes to a new untracked location.

Before claimed restore is activated, durable pending preparations need indexed
source and schema retention across process death. An in-memory Use and an opaque
journal blob do not supply that protection. Bind retention to the exact sealed
preparation in the registration transaction, transfer it under successor fencing,
and release it only after terminal/abandoned-state recovery establishes safety.
Source pruning must remain disabled until this index and cleanup protocol are
qualified. Fresh execution still reacquires read pins and current permission;
durable retention grants neither. Coordinator process authority must remain
separate from the freshly authenticated execution caller; a journaled principal
or credential identifier cannot be borrowed as authority after revocation.

Source metadata/security remains provenance. Restoration updates a destination
under its current ownership; it does not install historical ACLs. Keep historical
restore distinct from optional JCR graph/version restoration. No JCR dependency
or document-only transaction restriction is introduced.

### Initial claimed assessment execution boundary

Source inspection at `775e2814a1751f2667598095856fd6a99bf668b0` confirms that
registration, capture retention and terminal root release are separate from
claimed assessment execution. The latter remains gated in registration start,
upload admission, assessment CREATE and historical commit. The following is an
implementation requirement, not a claim that this path is available.

The next implementation introduces one private, closeable execution handle,
constructed only by the registered historical owner. It carries the exact
preparation, prepared plan, owner generation/nonce, execution claim/token/epoch,
fixed modes and registered capture identity. It retains an accepted
`DocumentHistoricalAssessmentSources.Work` from the registration's existing
source owner. A decoded V81 preparation, matching selectors or journal access
alone cannot construct this handle. Construction requires successful durable
registration or reconciliation of its exact committed identity.

In particular, `historicalCapture` is assigned inside the registration transaction
and may remain non-null after rollback or a lost acknowledgement. It is not proof
of durable registration. Before minting the handle, confirm the exact live claim
and owner, V81 preparation, V82 modes, V103 retention set and V104/V105 capture
and owner records, including pin digest and the initial capture's registration
transaction binding. Reject incomplete or rolled-back registration. The first
implementation accepts only the initial owner generation. V109 successor
attachment requires a separately qualified fresh capture capability.

Do not reopen the same histories and treat the new Uses as the registered
capability. `Prepared` references borrow live Uses, and the existing historical
assessment checks exact reference equality. Share the registration's source
owner through assessment creation; a fresh capture requires its own qualified
capture-registration protocol. This initial boundary does not authorize restart
or successor execution merely because their selectors match.

The handle's acceptance and mutation checks have two distinct responsibilities:

- Before accepting work, validate the caller and exact in-memory identities,
  acquire a lifetime permit and reserve bounded scratch outside SQL. Reject new
  work after close begins. An already accepted operation retains its permit
  through the real provider/assessment worker completion and SQL response
  unwinding, including cancellation and uncertain acknowledgements.
- Within each mutation transaction, fence the live claim and owner and verify
  command, sealed preparation, modes and registered capture/owner bindings.
  Recheck current credential generation, source READ, destination WRITE or
  creation authority, placement and physical source witnesses. Follow the
  existing deterministic lock order; perform no provider or registry calls
  while SQL locks are held. A prior authorization check is not a transferable
  grant for a later mutation.

Extend existing shared methods through narrow handle-requiring entry points:

1. **Upload admission.** Keep the existing refusal for claimed historical callers
   without a handle. The handle-qualified path must additionally call
   `DocumentHistoricalReferenceAdmission.requireBoundSources` after the required
   origin/retention locks and before writing attempt or selection rows. Current
   ordinary reuse checks do not substitute for historical physical witnesses.
2. **Assessment start.** Add an owned historical start path using the existing V83
   journal. Repeated start proposals return the committed assessment UUID and
   deadline for that generation. Require the handle and exact stored modes;
   never bypass the ordinary method's historical gate globally. Before retrying
   CREATE, inspect that exact durable assessment identity under current authority.
3. **Assessment CREATE.** Pass the same source owner and handle through
   `DocumentPublicationAssessment.Historical` into shared creation. Retain V83
   identity/deadline enforcement, observed-mode comparison, current policy and
   historical part/slot binding. CREATE remains create-only. A duplicate is not
   implicit adoption; reconciliation must establish the exact stored assessment,
   its manifest and eligible state before reuse. An incompatible or consumed
   assessment cannot cause another UUID to be minted in the same generation.

Keep claimed historical commit and public session/facade gates closed until the
subsequent publication and recovery acceptance cases below pass. This staging
boundary is an intermediate implementation step toward public restore, not a
replacement for it. No proto change or new public service is needed.
The CREATE-only handle must not expose the existing historical `publish` path:
it promotes the assessment and stages schema artifacts before the commit method
rejects claimed owners. Keep it unreachable or refuse claimed publication at
method entry, before either effect. The downstream commit guard alone does not
protect the assessment from consumption on an unsupported call.

Qualification for this boundary must use real SQL and existing storage adapters:

- A registered historical owner admits and creates an assessment for the selected
  old revision, including a mixed command with fresh uploads. Advancing the source
  head does not change the selected bytes or retained schema identity.
- Missing handle, foreign source owner, altered preparation/capture/modes,
  expired or replaced claim/owner, changed source witness, revoked READ/WRITE,
  rotated credential and revoked creation grant fail without new attempt,
  selection or assessment rows and without unauthorized provider writes.
- Concurrent start proposals and lost start acknowledgements retain one V83 UUID
  and deadline. Lost CREATE acknowledgement reconciles the existing assessment;
  it does not run a second CREATE or treat a consumed assessment as reusable.
- Barriers exercise revocation and owner replacement on both sides of the SQL
  mutation boundary. Closing during accepted provider work retains resources
  until that work drains; new work is refused and budgets eventually return to
  zero. Do not use sleeps or retries to conceal cleanup races.
- Existing ordinary and unclaimed historical tests remain green; public claimed
  historical execution still fails at its declared gate. A refused publication
  must leave the assessment unconsumed and create no schema artifact claims.
  Subsequent success
  qualification must exercise real claimed publication before V52 terminal
  root release can be advertised for that path.

Two implementation constraints avoid unnecessary work and premature resource
release. Decode and compare bounded preparation/mode data and verify the full
initial capture when constructing the handle. For later mutations, compare exact
immutable journal/capture scalar identities under the live claim/owner fence;
do not repeatedly decode V81 or rehash every capture child. Current authorization,
placement and physical witness checks still run for each mutation, including
assessment-start INSERT. Preserve existing journal/owner lock ordering and take
complete document/source authorization and sorted drive locks before independent
origin and retention locks. New methods must not introduce an inverse lock path.

Also, retaining `Sources.Work` alone does not make current source convenience
methods safe after source admission closes: those methods acquire a fresh Work.
Thread the already accepted Work through assessment preparation, resolver
construction, caller checks and CREATE instead of reacquiring it. The
close-during-work tests must hold an actual schema/provider worker across close,
not merely hold a permit with no assessment work running. A rejected submission
must release its permit; a started worker releases it only on actual completion.

### Claimed CREATE transaction refinement (2026-10-07)

The retained-preparation checkpoint `dca42e4b3` does not enable claimed CREATE.
Its next integration must not call CREATE from `DocumentHistoricalExecution.mutate`:
that method locks reused/historical origins and retention without including fresh
selected upload attempts. Extending that set inside CREATE would acquire further
origin locks after retention locks. Reentrance of the historical subset does not
make the expanded ordering safe.

Use one caller-owned CREATE transaction. Resolve descriptors and perform bounded
encoding before opening it. Persist normalized schema claims in a separate SQL
transaction that checks and holds the same current authority through its commit;
external schema resolution must never run while SQL locks are held. Authorized
schema staging can survive a later CREATE refusal, but cannot confer authority to
create or publish an assessment. Check the live claim, exact preparation,
history header, fixed modes and committed assessment start; then owner/command,
current schema policy, complete caller authorization and sorted drive bindings.
Observed evidence must match the handle's canonical command and fixed modes.

For this claimed path, call `DocumentCommitParts.bindHistoricalAssessment` before
the assessment-owner INSERT. It discovers the complete historical and fresh
object/attempt union and locks origins before retention. Bind slots using its
transaction-local proof, then check the exact registered capture and active native
pins under those locks. Pass these bound slots to a shared SQL write suffix.
Do not reacquire a smaller origin set first or extend the set after retention.
The ordinary and unclaimed paths retain their existing INSERT-then-bind order.
The common writer must not open a transaction or confer caller authority.

Before enabling this path, verify owner INSERT triggers introduce no conflicting
cross-operation lock order, and add a real mixed-upload contention test alongside
policy-lock contention, rollback and lost-acknowledgement cases. A lost CREATE
acknowledgement requires exact-stage discovery and retained-evidence reconciliation;
neither absence nor a duplicate permits a new assessment UUID or implicit adoption.
This refinement was reviewed by Sol against the existing binder and slot code.
The private initial-handle CREATE implementation and mixed-origin contention
check are recorded in
`docs/evidence/repository/2026-10-07-claimed-historical-create/README.md`.
This does not qualify a new handle's retry after an uncertain acknowledgement,
restart/successor recovery, or public claimed publication.

Credential revocation and commit have distinct outcomes. If staging commits before
revocation, schema claims may remain while final CREATE is refused. If CREATE
commits first, the assessment stays durable even if reauthorization denies return
of its identity. Retained historical handles must recheck a supplied credential's
live generation as well as current READ policy; public READ cannot bypass a
revoked key. A revoked caller cannot discover or reconcile the result. An explicitly
authorized host with the same operation principal may reconcile the original
coordinates. Principal-only hosts remain a separate authentication model and gain
no key-specific grant. These are corrections to authorization behavior, not new
wire fields or a promise of recovery on behalf of a revoked caller.

### CREATE ownership across initial handles

V83 coordinates are not a transferable CREATE capability. Preserve `start()`'s
coordinate idempotence: competing handles load one committed UUID/deadline, a
rolled-back start may retry, and a lost start acknowledgement reloads its original
coordinates. Only the handle whose INSERT is positively acknowledged may CREATE.
Record an internal insert outcome from the SQL update count; do not infer the
winner from a proposed UUID or a pre-insert read. Arm its private permission only
after commit and final authorization return normally. Validate full persisted
scope, command, owner, retention and expiry on readback/conflict.

Repeated start on the acknowledged winning handle keeps the same permission.
A handle that only loads a start, including the original handle after losing its
start acknowledgement, is reconciliation-only. Refuse its CREATE before schema
claims even when it prepares its own valid assessment. A CREATE SQL attempt
consumes the local permission before entering the transaction; loaded coordinates
or empty discovery cannot restore it. Schema staging is a separate authorized,
idempotent transaction and is not represented by the CREATE-attempt marker.

Qualification must show two independent handles returning identical start
coordinates but only one able to CREATE; the other must write no schema claims.
Real before-commit rollback must allow a later acknowledged start to win. Real
after-commit acknowledgement loss must leave both coordinate recovery and new
handles unable to CREATE, while a positively committed assessment can still be
discovered and reconciled under current authority. Repeat, expiry, retention
mismatch and credential revocation checks must retain their existing semantics.

This is an intermediate ownership boundary, not complete crash recovery. If the
winning handle disappears before CREATE, another handle cannot restage within
that generation. The completed service must resolve that case through explicit
drain/abandonment or a qualified successor with fresh captured-source ownership;
the existing V109 attachment and successor fences require historical execution
qualification before they can serve that purpose. Keep the public path closed
until this recovery sequence and publication are proven. No protobuf change or
new repository-wide transaction boundary is introduced by the private permit.

### Acceptance before enabling public restore

- With r3 current, selecting r1 publishes a new r4; retained r1 and r3 are
  unchanged. Exact retries return the same terminal receipt without uploads.
- A changed selector, physical identity, mode, placement or destination condition
  under one operation identity is a conflict. Decode of tampered or mismatched
  preparation fails before provider work.
- Restart after sealed registration and before commit reacquires r1, even when the
  current head advances. Stale destination conditions still fail. Revoked READ,
  WRITE, credential generation or creation grant cannot be revived by the journal.
- Real SQL barriers prove both revocation/commit orderings and successor fencing;
  no provider/schema calls execute under SQL locks. Independent operations using
  the same credential can still overlap.
- Crash/uncertain commit, held provider read, cancellation and failed pin release
  preserve exact recovery ownership. A late predecessor cannot commit.
- Retained definitions decode and validate without the original registry. Changed
  current policy is evaluated explicitly; opaque content is not silently promoted
  to typed success. Both semantic and contract-invalid fixtures retain their
  distinct meanings.
- Run the same completed operation through library and authenticated gRPC, then
  remove the public historical guard. Until these cases pass, the guard remains.

The first deliverable is private claimed preparation/recovery with real SQL and
provider evidence, followed by shared session integration and transport parity.
Published examples must wait for the public acceptance cases. Pending-source
retention and safe cleanup are prerequisites for claimed restore activation;
broader backup qualification and progressive hydration remain required afterward.

### Pending source projection checkpoint (2026-10-07)

V103 adds an indexed set of distinct source `(node_id, revision_id)` pairs for an
exact preparation and predecessor generation. The header binds the preparation
and command digests, expected count and ordered source digest. A live execution
claim fences creation; the header and its children must seal in one transaction.
Sealed sets cannot be changed or deleted. Revision foreign keys retain the
existing revision closure, including its physical references and schema bindings,
under the current immutable revision guards. This is not a new physical owner.

New ordinary registrations atomically record an explicit empty set. Pre-migration
preparations remain unknown, including on exact retry; absence is never inferred
to mean no historical sources. SQL validates internal projection consistency but
does not decode the protobuf command. Java therefore reconciles the sealed set
against canonical command intent. A future pruning consumer must require that
reconciliation, not merely trust the sealed flag. A mismatch is data loss.

This checkpoint does not enable historical execution, source pruning or retention
release. Historical registration still requires complete prepared source proofs;
ordinary registration supplies none and cannot bypass that check. Release needs
a separate fenced protocol proving terminal or qualified abandoned state and
drained readers/owners. Expiry alone is insufficient. Keep lock acquisition in
claim/preparation-header, sorted revision, then origin/retention order; a future
pruner must not acquire these in reverse. The current guards already prevent
revision deletion, so this projection is groundwork for safe future pruning,
not evidence of a repaired current data-loss path.

The multi-revision SQL fixture now checks projection deduplication across
destinations, distinct revisions of one source node, SQL/Java digest agreement,
complete prepared source coverage and refusal after the source owner closes.
Its provider observations are synthetic; it does not qualify nonempty claimed
registration. See `docs/evidence/repository/2026-10-07-history-root-selection`.

The next private registration boundary must accept a preparation and live pinned
sources together. Pre-encode preparation and modes before SQL locks, then register
claim/binding, preparation, nonempty retention set, modes and operation owner in
one transaction with current authorization and exact physical witness checks.
Reuse existing claim and typed owner admission through narrow historical entry
points; keep ordinary entry-point guards and the public facade unchanged. Retain
source Uses through commit or uncertain-outcome reconciliation. Registration
qualification must precede session execution and recovery activation.

### Private historical registration checkpoint

`DocumentPublicationRegistration.historical` now prepares an initial registration
from the exact journal seeds and a live `DocumentHistoricalAssessmentSources`
owner. This remains package-private. The caller must retain that owner and the
registration object across uncertain responses; recreating the registration
would mint a different claim token. Closing the source owner makes retries fail.

Registration checks current source/destination authorization, selected drives
and backend profiles, then writes the preparation and retention roots before
locking physical origins and retention. Exact physical witnesses must pass before
modes and the operation owner commit. The entire transaction rolls back together.
Historical retries require canonical coverage; a legacy unknown source set cannot
be adopted. Ordinary claim acquisition and preparation retain their historical
guards. Assessment start and claimed historical execution remain refused.

This private boundary is not yet wired into the runtime or public transport.
Qualification still needs source authorization races, full physical-failure
coverage, recovery/successor attachment and a fenced retention-release protocol.
The registration itself neither executes provider work nor creates a successful
publication receipt. Selecting TYPED mode records intent, not successful validation.

### Retention release requirements before implementation

The V103 count and digest describe the original sealed set. They cannot prove
live retention after roots have been released. A future release migration must
introduce an immutable per-preparation release record and update canonical
coverage to distinguish live retention, released retention and legacy unknown
coverage. Creation identity remains available for audit. A release record and
removal of all child roots must commit together; guarded deletion and a deferred
zero-child check must reject incomplete release. Until then, deletion stays refused.

Require exact terminal success or rejection plus drained accepted work, source
Uses and schema workers, or the existing pre-owner abandonment proof. A claim
lease expiring, coordinator DRAINING state, local drain record or successor
installation alone is insufficient. Keep predecessor-generation roots until
terminal completion unless a separately qualified atomic transfer protects all
sources needed by the successor. Current read pins protect their own physical
objects; they do not replace proof of schema-worker quiescence or historical
schema retention. No release API or compliance claim follows from this design.

The release proof must identify the exact source pins borrowed by the preparation.
Waiting for every reader of a source revision, or for all work in a shared reader
incarnation, would couple unrelated operations. Record only distinct selected
objects with their reader incarnation, pin UUID, physical object UUID, source node,
source revision and publication revision. V103 already retains the complete source
revision and its schemas; recording unselected parts adds no retention guarantee.
Pin identities must originate from the captured history under a live Use, not from
caller-supplied identifiers. The internal projection is data, not release authority.

V104 implements immutable capture batches under the retained preparation. A batch
contains the selected pin identities, sorted by pin UUID and sealed against a
versioned SHA-256 digest. A fresh capture adds its own identities instead of
replacing earlier evidence. New pin rows are checked and locked after the complete
ordered origin and retention sets, matching normal pin release. Exact confirmation
compares the persisted canonical tuples without requiring released pins to reappear.
There is no foreign key to live pins, physical objects or source revisions: their
eventual release must not erase or be prevented by this association evidence.

New nonempty V103 sets must commit a sealed initial capture batch in their creation
transaction. Existing sets are not backfilled, so missing initial capture remains
unknown. Each preparation permits at most 16 batches, each bounded to 10,000 pins.
An exact retry consumes no new batch. A seventeenth fresh capture fails closed;
qualified batch retirement or a revised bounded recovery policy is required before
general repeated execution relies on this facility. Encoding is done before SQL
locks under an additional 4 MiB serialized-data allowance, not a heap-size guarantee.

The SQL guards check root membership and exact live pin identity. They do not parse
the canonical protobuf command to prove complete selected-object coverage. The
Java registration derives pins from complete prepared sources; any future release
must independently reconcile persisted batches against the canonical command's
distinct source node/revision/object set. An initial flag or matching count alone
is not a release proof. Association also does not establish drained provider or
schema work. Existing process-death recovery still requires durable reader
quiescence; without the required recovery evidence, keep the roots retained.

V105 binds new capture batches to their exact execution claim epoch/token and
coordinator incarnation. Its immutable owner row must be inserted while the batch
is unsealed in its creation transaction. The database verifies the live claim
write fence, the command digest in V103, and the exact durable coordinator binding.
A deferred check prevents committing a new batch without its owner. Closed current
coordinators and terminal operations cannot own fresh batches. Existing V104 batches
remain unowned; neither migration nor exact retry retroactively assigns them.

A batch belongs permanently to its original execution. A later claim must obtain
a fresh capture instead of reassigning that batch. The latest coordinator's local
drain cannot certify work from every earlier epoch. This association grants no
execution, release or quiescence authority; historical successor capture and the
batch-local drain protocol still need end-to-end qualification.

The private source lifetime now closes admission independently of accepted work.
Registration retains a Work through transaction completion and response unwinding;
returned member schema resolutions retain theirs until component cleanup finishes.
New admissions fail after close. `awaitDrained` requires all accepted Works to end
and borrowed Uses to close successfully. This does not release physical pins or
write a durable drain record. Factory setup is not an accepted registration and
may fail safely if its sources close during construction.

The running-worker cancellation test deliberately waits for actual worker exit,
not Future completion. Any future asynchronous submission must additionally handle
rejection or cancellation before the task starts, handing ownership to the worker
only when it actually starts; otherwise a queued task could leak its Work. That
submission integration is not implemented or qualified by this checkpoint.
See the [local lifetime evidence](../evidence/repository/2026-10-07-source-lifetime/README.md).

V106 makes a captured pin ID permanently non-reusable after its native pin is
released. An AFTER INSERT guard rejects any `document_read_pins` insertion whose
UUID already occurs in immutable V104 capture evidence, including a changed reader
or object tuple. The check uses the existing pin-ID index and runs after any unique
index wait on a concurrent deletion. Existing native pins survive migration; fresh
UUIDs and unrelated handles on the same reader remain usable. Without this guard,
an absence observation could become false after a drain record commits. This
protection does not itself record drain or release any history roots.

V107 adds private capture completion. Registration retains a process-local capability
with its exact batch digest, V105 owner tuple and originating source/history handles;
the immutable batch retains the selected pin identities. This capability is tentative
until registration commits. A failed acknowledgement does not discard it, but a
rolled-back registration cannot mint durable completion evidence.

Completion closes source admission and the captured histories, waits for Sources
and every captured history to drain, and releases each captured history. Shared
handles wait for their other Uses; unrelated handles on the reader are not fenced.
Waits observe local timeout, cancellation and deadline; the local timeout does not
replace JDBC timeouts. Bounded hosts retain transaction-local SQL timeout settings
for confirmation as well as insertion. Partial cleanup and uncertain marker replies
remain retryable. Confirmation returns the exact first committed receipt and never
turns a cancelled response into success.

The immutable marker locks claim, preparation set and batch in that order, checks
original owner/command identity and requires both native pins and their mirrors to
be absent. It neither stamps execution authority nor renews a lease. Completion can
follow claim expiry or a guarded SQL epoch transfer; neither event proves drain or
invalidates actual original-owner completion. Public historical takeover remains
gated. LOCAL attests the owning process's enrolled Work/Use lifetimes; arbitrary
external tasks still need explicit enrollment before submission.

A new QUIESCED recovery marker requires every exact batch reader permanently
QUIESCED and its pins already recovered. ACTIVE, FENCED and expired states remain
insufficient. V46/V47 still own native-pin recovery. Existing valid completion may
be confirmed through either path without changing its original evidence kind.
Migration invents no completion for existing batches. See the
[capture-drain qualification](../evidence/repository/2026-10-07-capture-drains/README.md).

Normal root release must consume this batch-local completion, including all actual
schema/provider workers enrolled in its lifetimes. V91's host-local marker alone is
insufficient. Other active handles in the same reader incarnation need not block
a qualified normal release.
Under the claim and preparation-set locks, release must also verify terminal or
qualified initial-abandonment identity, complete canonical coverage, all batches'
drain evidence, and absence of exact native pins and their DOCUMENT_READER mirrors.
Prevent new captures once terminal release begins. V85 is initial pre-owner
abandonment only, not a generic successor cancellation mechanism.

V108 now guards new V104 batches and V105 owners against V85 abandonment and
V52/V64 terminal outcomes. AFTER INSERT preserves confirmation of an existing
batch discarded by ON CONFLICT. The existing claim fence serializes closure with
capture insertion; a real lock-wait regression qualifies committed abandonment.
The follow-up fixture also qualifies closure after real V64 cancellation. V52
success coverage awaits the currently gated claimed historical commit path; a
bare success-row fixture is not a substitute for that path. Root release must
extend this guard to its future release receipt before that protocol is enabled.

The private `DocumentPreparationCaptureCoverage` helper now implements the
transaction-local inspection below: canonical selector matching, root integrity,
batch digest/object coverage, original owner/drain equality and absent pins. It
does not check terminality or release roots. Repeated selectors and internally
consistent incomplete/duplicate batches now have real SQL qualification. A later
capture's drain cannot substitute for an earlier capture's missing completion.
Cross-revision aliases now have a real historical publication fixture: one physical
object in two revisions produces two distinct captured tuples. A real populated
V103 migration now qualifies the legacy initial-batch gap: retained roots remain
exact, and a legitimate later capture with a completed quiescence receipt still
cannot substitute for the missing creation-time batch. See the
[migration evidence](../evidence/repository/2026-10-07-legacy-capture/README.md).
V109 now qualifies separate completion of two capture-owner epochs through the
private historical activation path described below. Release wiring remains gated.

### Historical successor activation and shared retention

Source review found a sequencing gap in ordinary activation.
`RepositorySuccessorInstall.install` can persist the next
historical preparation, fixed modes and owner. It does not create a V103 source
set or any capture evidence. `RepositorySuccessorExecution.activate` calls
`next.prepare()`, which refuses historical inputs. V105 requires the exact new
coordinator binding; V94 creates that binding only during activation. Advancing
the claim with SQL or fabricating a binding is not a valid multi-owner fixture.

The private implementation follows the activation/capture portion of this sequence.
It does not change the public protocol or enable historical staging and commit.
Fresh-process attachment and broader recovery qualification remain follow-up work.

1. Keep the original, canonically checked V103 retention set through operation
   terminality. Reserve the successor through V92/V97/V98 and install V93 using
   the existing reviewed protocol. Expired-unquiesced reservation must not be
   treated as completion of the predecessor's readers or remote provider work.
2. Reacquire the complete selected historical inputs under the current execution
   caller. Hold their source Work/Uses through transaction completion, including
   an uncertain commit reply. Use `prepareHistorical`, the recorded placements,
   fixed modes and the new attempt identities. Resolve or inspect remote data
   outside SQL locks; preserve existing byte, selector and capture-batch bounds.
3. In one private activation transaction, establish the exact V94 claim/owner
   identity, check current source/destination permissions and immutable backend
   placements, and insert its coordinator binding. Lock and validate the retained
   original preparation/header, then the sorted physical origins and retention
   records before validating every selected pin. Append a V104/V105 capture under
   that original header with the successor's epoch, token and incarnation. The
   capture is non-initial; its ownership never rewrites the initial capture.
   Execution identity, binding and capture must commit together or all roll back.
4. Persist an immutable historical-activation sidecar in that same transaction.
   Bind its account/principal/operation, V94 epoch/token/incarnation and successor
   preparation digest to the original V103 generation/preparation digest, command
   digest, and this activation's exact V104 batch digest. Validate the matching
   V105 owner and same-transaction batch creation; deferred completeness checks
   must prevent a partial historical activation from committing. V94 identity
   alone is insufficient historical activation evidence. This association must
   survive lost replies and process restart, not live only in a host object.
   Prove ancestry through immutable V93 predecessor-preparation bindings, not
   merely matching command text and an earlier generation. Walk only within an
   explicit bounded traversal budget; exhaustion or a missing link refuses
   attachment and preserves retention. A previously verified sidecar can anchor
   the next link only after its exact retained identities are checked. Also check
   live root coverage and complete selected-object coverage. Successor generations
   intentionally share the original header until terminal release; their absent
   V103 headers remain UNKNOWN, never empty. Ordinary attachment must not silently
   accept that state. A fresh attachment revalidates the durable association and
   registers its own capture before borrowing source data.
5. Retain the tentative capture capability before the activation transaction can
   commit. A lost reply must leave it available for exact confirmation or cleanup.
   Confirm the sidecar, execution identity and this capture's digest/owner tuple;
   execution identity alone cannot confirm that a competing caller's capture was
   recorded. A fresh process or fresh reader cannot inherit a previous process's
   local completion capability. Its new capture requires current authorization.
6. Drain the successor capture and the original capture independently. Only their
   own Sources/Uses and exact pins participate. After all capture batches have
   matching V107 completion, the retained-set inspector may qualify coverage;
   that still grants no root release without the separately checked terminal
   outcome. A successor's successful activation, lease expiry or completed capture
   must not close an unfinished predecessor capture.

Acceptance uses a real initial historical preparation and admitted owner, then
actual expired-unquiesced reservation, install and private activation. Leave the
initial reader live while the second owner completes its capture: retention
qualification must refuse. Release and quiesce the initial reader, record its
original-epoch completion, and require both batches to qualify without renewing
either lease. Also cover current access revocation, changed placements, a missing
or released retention anchor, the seventeenth batch, capture-insert rollback,
lost activation acknowledgement, concurrent exact activation, cancellation on
both sides of commit, and stale token/incarnation. Verify no partial V94 identity
or coordinator binding survives an unsuccessful activation. Include a missing or
mismatched sidecar, a broken or exhausted ancestry chain, cold confirmation after
a lost reply, and a fresh process registering a distinct capture. Use actual SQL guards
and captured handles, never fabricated successor grants.

V109 and `RepositoryHistoricalSuccessorActivation` implement the private atomic
activation boundary. The attempt retains its tentative capture before sidecar
insertion, including when the transaction rolls back or its reply is lost. Exact
retry confirms this capture's immutable binding without renewing leases. The
ancestry walk is bounded at 64 immutable V93 links, using the existing unique
operation/generation index. No remote provider or registry call runs inside these
SQL locks. The sidecar and capture use the same activation transaction; a deferred
constraint refuses a required binding that is missing at commit.

The new required-capture flag defaults to false for existing V94 rows. SQL does
not infer historical selectors from protobuf bytes. Future historical attachment,
execution and root release must therefore require the V109 sidecar explicitly;
a bare V94 execution row is not historical activation evidence. The ordinary
activation path still refuses a fresh historical command.

Qualification covers actual two-owner captures, separate drain receipts, omitted
or altered sidecar identities, rollback, lost reply and cancellation around commit.
`RepositoryHistoricalActivationEvidence.confirm` now returns immutable V94/V109
identity from a separate connection without source handles, session attachment,
lease renewal or local-drain authority. It binds command, epoch/token/incarnation,
successor and retained preparation digests, capture digest and activation transaction.
Missing activation returns absent; bare V94 execution is not historical evidence.
Exact readback remains valid after capture completion or credential/source revocation;
it does not authorize new reads or execution. Scoped activation tests separately
check current source READ, creation grant and credential revocation after capture.

A restarted process must not reuse the saved incarnation as process authority.
The original live manager can reconcile its own retained attempt after a lost
reply. Another process needs a fresh reservation, epoch, incarnation and source
capture once the prior claim and owner expire. Old capture evidence remains
required; only actual completion or durable reader quiescence can satisfy it.
The cold-confirmation tests use a separate entity-manager factory in the same JVM,
not a deployed restart or newly attached execution session.

A three-owner regression now waits for actual epoch-2 claim and owner expiry,
reserves a distinct epoch-3 incarnation, and follows two immutable V93 preparation
links back to the original generation-0 retention set. Completing epochs 3 and 2
still leaves coverage incomplete until epoch 1 releases its actual pins and gains
a QUIESCED receipt. All three batches then qualify without changing the current
leases. See the [confirmation and recovery evidence](../evidence/repository/2026-10-07-historical-confirmation/README.md).

Two fresh activation attempts with distinct captures now have a real PostgreSQL
contention regression. The winner is held before commit; `pg_blocking_pids` proves
the competitor waits on that exact session. Only one V94/V109 activation and new
capture batch commit; the losing attempt receives a duplicate-key failure before
registering its capture. The winner's exact retry and completion remain valid,
with unchanged leases. A second case aborts the first transaction before commit:
the waiting attempt commits its distinct capture, and the rolled-back attempt
cannot confirm that capture or mint a drain receipt for its uncommitted identity.
Neither case establishes deployed process recovery. See the
[concurrency evidence](../evidence/repository/2026-10-07-historical-concurrency/README.md).
The [rollback evidence](../evidence/repository/2026-10-07-historical-race-rollback/README.md)
records the second case and its explicit drain refusal.

The recovery bounds now have actual SQL qualification: 64 installed ancestry
edges activate, while edge 65 refuses atomically. Installed but unactivated
successors use V98 supersession and add no captures, so this reaches the ancestry
bound independently of the 16-batch limit. A separate case fills all 16 batches
with real activated owners and refuses capture 17 even after prior successors
drain. See the [bound evidence](../evidence/repository/2026-10-07-historical-recovery-bounds/README.md).

**Unresolved exhaustion handling:** either refusal leaves an installed current
owner/claim without an activation. Further retries cannot shorten ancestry or
erase permanent batches. Before enabling public historical recovery, implement
an explicit terminal outcome for bound exhaustion and qualify its cancellation,
idempotency, retained-source cleanup and receipt behavior. Do not describe these
limits as automatic recovery or self-healing. Deployed restart/session attachment
also remains open.
Keep claimed historical execution and root release gated.
Success publication and broader multi-generation recovery require their own
qualification; activation and capture registration alone do not prove them.

The reviewed next step retains the V103 header and adds a permanent per-preparation
release receipt. In one transaction, lock claim, V81 preparation, V103 set and V104
batches in digest order. Decode the bounded canonical preparation, match its hashes,
and compare every batch's distinct source node/revision/object tuples to the complete
canonical selection. Require the same-creation initial batch, every exact V105 owner
and every V107 completion. The coverage check now recomputes actual root
count/digest in the header's SQL snapshot; release must use it under the locks
above before deletion. Do not rely on a sealed header alone. Missing legacy coverage is UNKNOWN, not an empty
set. Require matching V52 success or V64 rejection with an applicable owner generation,
or exact V85 initial abandonment with no admitted owner/start. Expiry, coordinator
drain and successor installation alone are insufficient.

Insert the release receipt and delete all child roots atomically. The delete guard
must accept only that transaction's exact receipt, with a deferred zero-root check.
Coverage must distinguish UNKNOWN, LIVE_EXACT and RELEASED_EXACT while retaining the
original header as creation evidence. Test capture/release races, incomplete batches,
an undrained earlier epoch, legacy missing ownership, corrupt root rows, rollback and
lost acknowledgement before enabling this path. V111 now supplies the SQL release
foundation described below; the callable release protocol remains unavailable.
Neither V107 nor V108 alone authorizes root deletion.

#### Release identity and terminal applicability

This is the reviewed implementation contract for the next private release step,
not an available API. It changes no protobuf names, tags, imports or Any URLs.

The release receipt is permanent and keyed by account, principal, operation and
the target preparation's predecessor generation. It binds that preparation's SHA,
immutable command SHA, original root count/digest, the terminal evidence below,
and the release transaction ID. It must also retain a deterministic digest/count
of the capture batches and their exact owner/drain identities that were checked.
The receipt authorizes deletion only of that preparation's liveness roots; it
does not delete publication history, capture evidence, schema assets or provider
bytes, and it grants no execution or read authority.

For V52 success or V64 rejection, bind the exact outcome kind, owner generation,
result SHA and outcome creation transaction ID. Check the command codec/version
and hash against both the retained preparation and admitted operation. The outcome
generation must be at least the retained predecessor generation plus one. Match
the terminal owner generation and nonce to its own V81 preparation at generation
minus one. Conflicting success/rejection/abandonment evidence is corruption.
A reason-4 rejection additionally retains its exact V110 same-transaction pairing;
its inspected ancestry prefix is not proof of full ancestry or capture drainage.

Do **not** require a complete V93 ancestry walk to release a terminated operation's
retention. Execution eligibility and ending retention have different requirements.
After an ancestry-limit rejection there may be 65 or more installed, unactivated
attempts. Applying the execution traversal limit to cleanup would strand the
original roots. The sufficient release proof is the exact target preparation and
command, the operation-wide immutable terminal outcome, and complete drainage of
every capture belonging to that target preparation. V81/V93 terminal guards and
V108 capture closure prevent new work from entering that operation while release
holds the same claim lock. No ancestry omission may bypass validation of the
target preparation, its roots, capture identities, or terminal evidence itself.

V85 abandonment is a separate alternative, not a fabricated result receipt.
Require predecessor generation zero, exact original preparation SHA/owner nonce
and epoch-1 claim token/binding. Require absence of an admitted operation, owner,
assessment start, success and rejection. The original capture must still drain;
abandonment alone does not establish that a local reader has stopped.

The transaction locks claim, target V81 preparation, V103 header and V104 batches
in digest order. `DocumentPreparationCaptureCoverage` must reconcile the bounded
canonical command and complete selector/object sets under those locks, including
all initial and later captures. SQL independently rechecks sealed root count/SHA,
batch count and pin digests, V105 coordinator bindings, V107 completion identities,
and absence of native pins and their object-reference mirrors. A count of zero
captures, missing legacy ownership, or an undrained older epoch remains UNKNOWN
or incomplete; none is equivalent to successful drainage. Existing 16-batch,
10,000-pin/root and preparation serialization bounds remain unchanged.

Only after those checks may the same transaction insert the receipt and delete
all target child roots. The DELETE guard requires that exact receipt and header
identity with the current transaction ID; receipt mutation/deletion is forbidden.
A deferred check prevents a receipt committing with remaining child roots. The
original V103 header and V104/V105/V107 evidence survive. Capture admission also
checks the permanent release record, so release cannot be followed by a new batch.

Coverage becomes UNKNOWN, LIVE_EXACT or RELEASED_EXACT. RELEASED_EXACT requires
an exact immutable receipt/header/preparation binding and zero remaining roots.
A receipt with remaining roots, or missing roots promised by an existing sealed
header without a receipt, is DATA_LOSS. A missing legacy header remains UNKNOWN.
Idempotent release confirmation must remain possible after source revisions and
provider objects are independently pruned: it uses retained identities and the
release evidence, rather than re-reading now-absent source content. V104 already
deliberately has no live pin/object/revision foreign keys. New release evidence
must preserve that property.

The private `DocumentPreparationTerminalEvidence` inspector now qualifies the
immutable terminal alternative under claim/preparation locks. It verifies canonical
receipt identity and stored projections, including the reason-4 transaction pair,
or exact initial abandonment. It neither grants release nor changes retention.
Its success qualification uses normal journaled publication; claimed historical
publication remains gated. V111 now supplies permanent release receipts, SQL
capture/terminal checks and atomic root-deletion guards. Its PostgreSQL tests
cover initial abandonment, canonical cancellation, rollback and migration of
existing retained rows. `DocumentPreparationRootReleases` now supplies a bounded
private handler and release-specific UNKNOWN/LIVE_EXACT/RELEASED_EXACT inspection.
It confirms retries from permanent evidence without querying source publication
or part tables. The existing history-root coverage remains a live-execution proof;
its EXACT value never includes released retention. Concurrent releases now have
real claim-lock wait evidence for both commit and rollback. The actual 16-capture
and 65-edge cases prove release after every persisted capture drains, including
the original epoch. Multi-root partial deletion now rolls back, and complete
release leaves other preparations' roots intact. A competing capture waits behind
the real release lock and remains rejected after commit or rollback; its unrelated
reader pins survive. Actual pruning, remaining race orderings and other terminal
release paths remain unfinished; no release API is mounted. See
[multi-root and capture-race evidence](../evidence/repository/2026-10-07-multi-root-release/README.md),
[concurrency and limit evidence](../evidence/repository/2026-10-07-root-release-concurrency/README.md),
[private handler evidence](../evidence/repository/2026-10-07-root-release-handler/README.md),
[SQL release evidence](../evidence/repository/2026-10-07-root-release-sql/README.md) and
[terminal inspection evidence](../evidence/repository/2026-10-07-preparation-terminal-evidence/README.md).

Required qualification before mounting or advertising this behavior:

- Release after success, each applicable rejection including reason 4, and exact
  initial abandonment; an ancestry-limit terminal case must not need a full walk.
- Pending operation, expired lease alone, coordinator drain alone, stale terminal
  identity, conflicting outcomes and malformed canonical preparation cannot release.
- Every original and later capture drains; a missing batch/owner, live native pin,
  mirror, incomplete selector coverage or corrupted child digest blocks release.
- Capture versus release and two releases serialize on the real claim lock;
  demonstrate commit and rollback outcomes using actual SQL wait observations.
- Cancellation, deferred-check failure and lost commit acknowledgement preserve
  either the complete live set or a complete receipt with no roots, never a mixture.
- Exact retry after independently pruning released source history remains valid;
  changed identity is rejected, and another preparation's roots remain untouched.
- A database with pre-existing live headers/captures migrates without declaring
  legacy unknown evidence released or synthesizing drainage.

### Terminal handling for exhausted historical recovery

This is a private implementation under qualification, not an available operation.
The additive receipt reason `RECOVERY_LIMIT_EXCEEDED = 4` now compiles and passes
runtime generated/dynamic validation and canonical codec tests. V110 now allows
the reason only with an exact same-transaction recovery-limit sidecar. There is
a package-private Java decision handler, but no mounted operation. Descriptor compatibility does not mean old runtime validators
accept the new value: qualify and upgrade receipt readers before enabling writes.
See [contract evidence](../evidence/repository/2026-10-07-recovery-limit-contract/README.md).
The [SQL foundation evidence](../evidence/repository/2026-10-07-recovery-limit-sql/README.md)
qualifies paired decisions at both actual bounds, under-bound refusal, missing
receipt refusal, and exclusion of a late activation in the same transaction.
The [handler evidence](../evidence/repository/2026-10-07-recovery-limit-handler/README.md)
now qualifies both actual bounds through `RepositoryHistoricalLimitDecisions`,
under-bound absence of a decision, rollback before commit, lost acknowledgement
after commit, exact retry, and current READ-policy and credential checks on scoped
replay. Retries leave claim/owner leases unchanged. Explicit terminal cancellation
of a non-exhausted unactivated successor remains unimplemented. Request cancellation is separate from an explicit terminal
cancellation operation: a signal observed before commit may abort tentative
work, but a signal arriving after commit cannot undo a durable limit decision.
If the host cannot deliver that response, the caller must reconcile through
authorized exact replay. Neither a cancellation exception nor response loss alone
establishes whether the transaction committed. Controlled JDBC boundary cases
qualify these outcomes in the [cancellation evidence](../evidence/repository/2026-10-07-recovery-limit-cancellation/README.md),
without claiming interruption during an in-flight JDBC commit.
The [decision concurrency evidence](../evidence/repository/2026-10-07-recovery-limit-concurrency/README.md)
observes real claim-lock contention between two decisions: first commit returns
the same receipt to both, while first rollback lets the second create the sole
pair. Leases and retained captures are unchanged.
The [activation race](../evidence/repository/2026-10-07-recovery-limit-activation-race/README.md)
also observes V94 waiting on the deciding transaction: after the decision commits,
activation sees the terminal row and refuses without partial durable capture or
execution. An exhausted activation is not an eligible winner.
The [supersession race](../evidence/repository/2026-10-07-recovery-limit-supersession/README.md)
holds the decision past actual lease expiry while V98 waits on its claim lock.
Normal deferred validation rolls the decision back; supersession then succeeds.
The old plan cannot decide, but a genuine fresh installation can.
The [expired-replay evidence](../evidence/repository/2026-10-07-recovery-limit-expired-replay/README.md)
qualifies the converse: a decision committed while live replays after actual
expiry without renewal, V98 rejects the terminated operation, and discovery
offers no recovery candidate. The documented early-constraint timing case
remains distinct.
The [retention-corruption evidence](../evidence/repository/2026-10-07-recovery-limit-corruption/README.md)
checks altered root digests, missing roots, changed pin contents, changed owner
tokens and missing initial-capture evidence. The handler reports each error and
leaves no terminal pair or execution. Runtime guards are restored before each
attempt; this is isolated administrative fault injection, not a supported mutation.
The [ancestry-corruption evidence](../evidence/repository/2026-10-07-recovery-limit-ancestry/README.md)
keeps the newest install valid while damaging the older edge's command or
predecessor digest. Both produce an error before any terminal pair is recorded.
These checks do not authorize retained-source release; that remains the separate
atomic protocol above.
The [timing evidence](../evidence/repository/2026-10-07-recovery-limit-timing/README.md)
now covers lease expiry before rejection insertion, expiry before normal deferred
validation, and early constraint firing followed by an expired-lease commit.
The first two roll back both rows; the last preserves the already validated
terminal pair without granting execution or renewing leases.
Liveness is checked when the guards run;
forcing deferred constraints early can precede later wall-clock lease expiry.
Do not describe this as an unconditional commit-instant liveness guarantee.
The ordinary `DocumentPublicationRejections.cancel` path cannot resolve an
installed but unactivated successor: V95 requires exact activation before its
owner mutation. A SQL regression preserves this guard and checks ordinary
cancellation succeeds after genuine activation. Do not remove the V95 check or
fabricate V94 activation to terminate an exhausted operation.

The private limit-decision operation must maintain the following boundary:

1. Require process authority and an exact installed plan, original retention
   preparation and command. Serialize bounded identities before taking locks.
   A prior exception, SQL error text or an expired lease is not limit evidence.
2. Lock the current claim first, then the current owner. Check exact token,
   epoch, owner generation/nonce, live leases and committed V93 installation,
   including reservation, preparation digest and modes. A terminal receipt may
   replay after lease expiry through the existing authorized receipt-read path;
   a new decision requires current authority. If an installed owner has already
   expired, use the existing V98 supersession and V93 installation path to obtain
   a fresh live decision identity; do not renew or impersonate the expired one.
   Lock order must not reuse the
   owner-first ordinary cancellation transaction for this claimed path.
3. Require no V94 execution for this current epoch and no success or rejection.
   Validate the original retained preparation/header identity. Recheck the bound
   under those locks: either 16 permanent capture batches for that retention set,
   or an exact immutable ancestry prefix of 65 links proving it exceeds 64.
   Every inspected link must match command, generation and predecessor digest.
   Broken or missing links within that bounded prefix are corruption, not a
   resource-limit rejection. If the retained anchor is farther away, the remaining
   ancestry is explicitly unverified by this decision; do not assert complete
   ancestry or use the sidecar as proof for root release. Record the inspected
   depth, endpoint generation and endpoint digest in the sidecar.
   For capture exhaustion, validate sealed batch identities and their ownership;
   do not require completed drains merely to record a terminal outcome.
4. Insert an immutable decision sidecar bound to scope, command, current
   epoch/token/incarnation, installed owner and preparation, retained preparation,
   limit kind and observed bound. Commit a rejection receipt in the same SQL
   transaction. Deferred checks must prevent either half committing alone.
   The sidecar carries decision evidence only; it must confer no generic owner
   write fence, provider access, publication, source read or capture authority.
5. Extend the fixed rejection reason enum additively with
   `RECOVERY_LIMIT_EXCEEDED` and retain REJECTED disposition. Existing enum numbers,
   receipt fields, codecs, imports and Any URLs stay stable. Update the runtime
   validator, receipt codec and SQL reason checks together. Do not label an
   automatic limit decision EXPLICIT_CANCELLATION or ADMISSION_REJECTED.
6. Narrowly extend rejection insertion checks for this exact same-transaction
   sidecar; leave ordinary mutation fences unchanged. Current scoped receipt-read
   authorization still applies. Lost replies reconcile by immutable identities;
   a different command, installed plan or limit evidence must not borrow a result.
7. Retain all roots and captures after rejection. Their release remains the
   separate protocol above, requiring every original capture to drain or obtain
   actual quiescence evidence. Terminality alone is not permission to prune.

Acceptance before enabling this private decision path:

- Both real bound fixtures obtain one exact durable rejection without a V94/V109
  row for the current exhausted epoch (earlier activations remain),
  new captures, provider calls or lease renewal. Under-bound input, wrong retained
  generation, changed digest/modes, malformed ancestry, wrong caller and replaced
  or expired ownership refuse with no partial sidecar or receipt.
- SQL tampering cannot insert a sidecar without its matching rejection, reuse one
  in another transaction or cause unrelated owner/provider writes to pass.
- A race against activation, owner supersession or another decision has one
  coherent outcome with claim-before-owner locking. Cancellation before commit,
  rollback and lost acknowledgement preserve exact durable state on retry.
- Old receipts still decode; the new reason passes the real runtime validator,
  complete-import proto compilation and applicable lint/compatibility checks.
- All previous captures and roots remain until independently qualified release.
  Current grant/key revocation blocks external receipt delivery as elsewhere.

An explicit user cancellation of a non-exhausted installed successor is a distinct
unresolved operation. Do not make the limit operation a general cancellation
bypass. Public historical execution and automatic recovery remain gated while
these terminal and release requirements are unfinished.

### Next execution slice: historical successor CREATE ownership

After qualifying initial-handle CREATE ownership, extend private execution to an
exact activated historical successor. V83 is immutable staging intent, not a
transferable CREATE permit. V85 explicitly refuses pre-owner abandonment when an
owner or start exists; it cannot resolve a vanished initial handle after START.

For uncertain CREATE, use exact `DocumentAssessmentDiscovery.discover` followed
by `DocumentAssessmentReconciliation.observeRetained` under current authority.
A positive retained stage must match generation, assessment, selections, manifest
and deadline. Empty discovery is not rollback proof and must not rearm the old
handle or create another stage in the same generation.

When no retained stage can be reconciled, reuse successor reservation/installation
(V97/V93 and applicable supersession), retained preparation and modes, and the
fresh historical capture/activation protocol (V109). Existing activation evidence
does not itself supply an executable session or live source Uses. The next private
handle must be minted from the exact live activation and fresh capture, with current
source READ, destination WRITE, credential and policy checks. Cold confirmation
must never fabricate borrowed resources or reuse an old incarnation. Scope V83
start identity to the successor's predecessor generation, retaining acknowledged
INSERT ownership and sticky CREATE attempts. Drain earlier captures independently.

Acceptance for this next slice:

- Initial START committed, no assessment, winning handle lost: V85 refuses; an
  empty discovery result grants neither CREATE nor abandonment.
- After real expiry or qualified handoff, a genuine fresh successor obtains new
  capture/pins and activation; its new start/CREATE leaves the original V83 intact.
- A late old-owner CREATE is fenced. Successor success does not attest old-reader
  quiescence; original capture drain is separately proven.
- Lost CREATE acknowledgement reconciles only exact positive retained evidence;
  rollback/absent evidence never silently restages the old generation.
- Revoked keys/policies, changed source identities, missing retained schema assets,
  and failed activation leave no partially granted execution or new assessment.
- Cancellation and uncertain commit outcomes preserve exact recoverable identities;
  no lease renewal, source-release authority or public availability is implied.

This is planned work, not an enabled API. Preserve the separately reviewed recovery
limits, terminal decisions and source-release protocol; do not bypass them to make
successor execution work.

#### Successor execution binding and accepted activation work

Reuse the existing `DocumentHistoricalExecution` CREATE implementation through a
private validated initial/successor binding. It must distinguish the current
executable preparation and mode row (successor generation) from the original
retention preparation and capture root set. Parameterizing every check with a
single generation would be incorrect: current preparation ownership advances,
while original historical retention does not move to the successor's generation.

The activation attempt must retain accepted source Work before activation and
across uncertain acknowledgement. An internal accepted-work activation entry
point can fork this permit, verify its exact source owner and caller, and run the
same activation transaction without reopening closed source admission. Existing
capture-only callers keep their existing lifecycle. The caller-owned permit must
be closed on disposal; capture completion must remain pending while it is held.
The initial prerequisite is qualified separately from executable attachment.

Later attachment must verify the exact committed V94/V109 identity, next
preparation/modes, current claim/owner and retained-root/capture bindings, then
transfer a live child Work and accepted scope Call into the execution handle.
A cold activation receipt cannot supply either resource. Failed attachment closes
only tentative local resources and preserves uncertain durable identity for exact
confirmation; it must not silently mint another activation or abandon capture
cleanup. Post-commit authorization and lost-reply behavior require their own tests
before this becomes an executable successor path.

Accepted-work activation qualification passed 17 real PostgreSQL normal/scoped
tests with no failures or skips and Sol review. See
[accepted activation evidence](../evidence/repository/2026-10-07-historical-accepted-activation/README.md).
This does not qualify successor handle attachment or CREATE.
