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
Observed-runtime CREATE and atomic reference publication remain gated.

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
Acceptance must cover explicit opaque success, typed-source refusal, unknown source,
current typed-required policy refusal, revocation, cancellation and zero leaked
reservations. Sol reviewed this boundary; it is not implemented yet. Source
READ checks belong at delivery and commit; deterministic evidence replay remains free
of provider and SQL calls. Keep source Uses through observed-runtime CREATE and atomic
reference publication. The public route remains disabled while those integrations
and their tests are unfinished.
