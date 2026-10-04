# Independent revisions and retained part references

Status: Sol-reviewed implementation plan with the partial checkpoints below;
the complete cutover is not available. Initial source audit at `6f562450`.
This refines [repository composition](repository-composition.md#immutable-part-reuse-implementation-design)
and preserves the [optional JCR boundary](repository-jcr-compatibility.md).

Implementation checkpoint: V38 adds the three shadow tables and the legacy
publication bridge described below. It preserves full manifest positions, exact
managed object identities and immutable history, including deleted documents.
V22/V36 publication guards remain authoritative. Managed reads now
use the shadow's ordered parts and revision ID, validating them against that legacy
authority, and the read model carries per-part backend bindings. Independent-ID
allocation for new mixed revisions remains unimplemented. V39 derives document
retention ownership from sealed revision parts and current revision pointers,
preserving existing reference keys and V29 reclamation guards. It refuses missing
or extra references during migration rather than repairing them. This bridge does
not enable mixed publication or qualify shared-origin throughput.

V40 records immutable initial operation member selections, including placement for
zero-upload members. It retains an allowlisted drive snapshot and a versioned
configuration digest, with no copy of free-form metadata/options or credential
references. Existing attempts are not automatically selected. V42 retains those
rows as placement anchors and adds immutable selection history plus a current
pointer for each operation generation and member. Internal retry staging appends
the next selection and advances that pointer with an exact expected revision and
attempt. Terminal-result binding and provider recovery remain prerequisites for
activation.

### Retry selection boundary

- V42 copies each existing V40 anchor to selection revision 1, preserving its
  attempt, including explicit null attempts for zero-upload members. It does not
  select other attempts or copy selections between owner generations.
- A retry names a nonempty subset of uploading command members. It compares the
  persisted canonical codec, version, bytes and digest, rechecks authorization and
  source evidence for the full command, and stages only the named members.
  The original placement remains fixed; configuration drift requires a separate
  operation instead of silently changing where a retry writes.
- The owner write fence precedes document and drive locks. SQL locks the
  replacement attempt before checking its live state and absence of cleanup,
  then locks the selection pointer. The expected selection revision and attempt
  must both match. History and pointer changes commit with all new attempts or
  roll back together. History cannot commit without its pointer advancement.
- SQL proves owner and placement bindings, but does not decode the protobuf
  command. Canonical command/member semantics remain the responsibility of the
  internal Java entry point and trusted database writer boundary.
- Replacing a selection does not revoke the displaced attempt's lease or token.
  Before provider work or independent publication can be activated, those paths
  must fence the exact generation, member, selection revision and attempt.
  Durable outcomes must retain that same binding. Lost-acknowledgment recovery
  must read durable state rather than blindly issuing another replacement.
- Retry preparation encodes upload rows only for selected members, before SQL
  locks. The full command still undergoes authorization and source checks. Pointer
  replacement adds two client statements per retried member, bounded by the
  command's 64-member limit. Operations with different owners have no shared
  selection lock; one operation's retries serialize through its owner fence.
  Provider I/O remains outside this transaction. These are structural bounds,
  not evidence of qualified end-to-end throughput or tail latency.

### Selected-attempt renewal and verification

V43 requires the exact current attempt for NEW_CONTENT renewal, state changes and
post-seal object updates, including no-op updates. Initial plan declaration and
PLANNING-to-STAGING sealing still precede selection. The owner write fence is
acquired first, and the attempt row is locked before checking current selection,
lease and cleanup. A fresh indexed lookup sees a selection replacement performed
in the same transaction; a transaction-ID-only proof would miss that change.
An attempt can appear in only one selection-history row, preventing an old attempt
from being reselected at another revision. The Java boundary also requires the
explicit expected selection revision and worker token.

`DocumentSelectedAttemptLedger` is an internal lifecycle boundary. Its renewal
batch accepts 1–64 distinct attempts, locks them in PostgreSQL UUID order, checks
the entire expected set and renews atomically. Verification accepts 1–256 distinct
key observations and matches size, SHA-256 and content type against the staged
plan. Replaying verification requires identical nullable provider version and
ETag. Any mismatch rolls back the entire batch. VERIFIED remains a byte-evidence
state; it does not mean the document passed typed or semantic admission.

Both operations have a tested client-statement ceiling of nine per batch. SQL
trigger work remains proportional to affected rows, and batches for one operation
serialize through its owner fence. Inputs and JSON encoding are bounded before
SQL. This does not establish a latency or throughput target. Provider scheduling
must use an operation-wide concurrency window and byte budget rather than
multiply limits for every backend or member.

Internal `DocumentUploadCoordinator` now connects provider execution to selected
renewal and verification, with bounded workers, observation batches and heartbeat.
It is not a public typed publication path. A preflight selection check cannot
prevent replacement during a remote PUT. Attempt-specific keys
isolate late writes; the post-I/O verification fence must reject a displaced
worker. Lost PUT acknowledgments require exact-key, qualified read-back or an
explicit replacement followed by expired-attempt cleanup. Do not invent a receipt
or retry PUT blindly. No SQL transaction may span that provider I/O. The existing
FULL_REVISION writer and deletion-only recovery path remain unchanged.

## Current coupling that must change together

- `DocumentPublicationLedger.Publication` now carries an independent revision ID
  and per-part backend bindings. Its query reads ordered shadow revision parts
  while still checking the legacy FULL_REVISION publication authority. The reader
  resolves original bindings per part under one aggregate budget/window.
- `DocumentSourceSnapshot` now fences the current revision UUID and mutation
  revision, cross-checking the legacy publication bridge. New mixed or zero-upload
  revision publication still needs the independent authoritative commit path.
- V22 history is keyed by attempt UUID. Its guards require a live full attempt,
  exact ordered manifest and matching current document body. V36 deliberately
  prevents NEW_CONTENT attempts from entering that path.
- V39 derives document retention from sealed revision references and current
  revision pointers. Its native-reference predicate preserves archive reader
  protection from V28. V29 shares locks for archive readers, but durable reference
  changes still exclusively lock origin owners and physical retention rows.
- V25 registers physical coordinates before verification. Registration is not
  evidence of verified content. V10 raw keys remain quarantined because their
  old backend strings are not proof of registered physical identity.
- `DocumentPublicationBatch` composes SQL publication but requires an attempt
  for each destination. It does not establish typed admission, durable operation
  outcomes or the new cross-origin lock discipline.

These are internal Java/SQL extensions. Preserve protobuf names, numbers, imports,
manifest ordering and stored Any URLs. No new public RPC is needed for this cutover.

## Identity and ownership

A revision receives an immutable UUID independently of upload attempts, document
mutation counters and manifest version numbers. The document projection maps a
revision to its exact domain snapshot and ordered slots. The repository foundation
owns physical bindings and durable reference ownership; it must not require a
document CORE, graph address or JCR type to retain an object.

Keep the origin attempt on physical verification evidence. A revision references
that evidence; it never adopts or rewrites the origin attempt. A metadata-only
revision has references but no new attempt. Failed upload replacements remain
separate attempts selected explicitly by the owning operation.

The ordered document relation records revision UUID, manifest position, slot and
physical object identity for every PRESENT entry. EMPTY and DELETED entries remain
in the immutable manifest without invented physical objects. Use manifest position
and present-object ordering deliberately: V22 uses dense PRESENT order, whereas
NEW_CONTENT records its position in the complete command. They are not interchangeable.

Verified physical content includes exact optional provider version, size, checksum
and content type in addition to retained coordinates. An immutable verification
binding must be supported by actual verification evidence. Its presence alone does
not qualify a provider for immutable reuse. Absent versions and provider sentinel
versions remain distinct; neither means that another backend may be substituted.

## Complete revision preparation before publication

The next internal boundary is a non-publishing revision preparer. It resolves
verified upload observations and retained source identities into one immutable,
command-ordered draft. Repository orchestration belongs in `repo/engine`;
SQL evidence and fencing belong in `repo/container`, and deterministic fragment
checks belong in `repo/codec`. No provider I/O occurs in the
publication transaction. Publication must recheck owner/selection fences, source
revisions, authorization and physical retention before committing this draft.

Keep exact original fragment bytes and digests as physical evidence. Parse a
bounded semantic view for checks; never require byte equality after protobuf
parse/reserialize or overwrite original bytes with a normalized representation.
`DocumentPartCodec.assemble` is a merge utility, not an admission gate.
`DocumentFragmentConfinement` now implements the single-fragment field checks
below for the fixed `protomolt-document-parts/v1` policy. It accepts an already
bounded, parsed Document; it neither resolves descriptors nor persists policy
identity. `DocumentChunkSequence` now checks global ordered run names, partitions
and a caller-specified aggregate element bound. `DocumentRevisionAssembly` now
combines these checks with immutable original PRESENT-fragment bytes, unique slots,
exactly one CORE, aggregate byte/count bounds, bounded-depth parsing and cancellation
between fragments. Repository resolution, full manifest preparation and publication
wiring remain unimplemented. Byte bounds are not a heap limit; hosts must reserve
resources for protobuf object expansion and apply concrete policy. A single parse
is not interruptible through the between-fragment control callback.
`DocumentCommandContent` now binds materialized full-ordinal bytes to the canonical
member, checks declared sizes/digests and decoded ownership, and refuses an explicit
schema condition or host-required typed policy until schema admission is available.
It checks content only; no returned value proves physical provenance, authorization
or that a current selection is still live. The host supplies a stable bounded map
and resource reservation before invoking it.
Presence follows the codec: an explicitly present empty BlobBag
is a BLOBS fragment, while repeated PARSED/CHUNKS content requires entries.

For the initial Document layout, require one CORE, matching nonblank document IDs
in all PRESENT fragments, and field confinement before merging:

- CORE excludes `blob_bag`, `parser_results` and
  `search_metadata.semantic_results`. Other root and search-parent content belongs
  to CORE, including unknown fields under the pinned descriptor/layout policy.
- BLOBS and PARSED contain only the identity and their nonempty owned field.
- CHUNKS contains the identity and nonempty semantic results. Its search parent
  cannot introduce other fields. Root/search-parent unknown fields in non-CORE
  fragments are rejected as unowned content. Unknown fields within an owned
  subtree retain their original bytes; that does not establish semantic validity.
- Validate CHUNKS in global manifest order against the existing run rules:
  consecutive equal effective IDs stay together, blank IDs use global element positions,
  and repeated/generated names use the first unused suffix. Reject a run split
  across adjacent fragments and incorrect subkeys. Preserve command ordinals;
  compact upload ordinals are not complete revision positions. Existing behavior
  treats a generated `set-N` and an adjacent explicit ID of that same value as
  one run. The sequence checker preserves this collision rule rather than
  introducing a new layout identity.

Pin descriptor and layout identities with admission evidence; future code must not
reinterpret a historical unknown CORE field using today's layout. Derive document
metadata/manifest from the checked assembly and compare ownership/address against
the canonical command. Preserve explicit EMPTY entries without physical objects.
Zero-upload revisions still undergo complete preparation without invented attempts.
Apply aggregate byte, nesting, descriptor and execution bounds, cancellation and
shared payload reservations through materialization and validation.

Until descriptor resolution, validation and durable retention are implemented,
requests requiring structured-schema validation must fail closed. A structurally
confined fragment is not typed-valid. Nested Any paths follow explicit policy.
Keep this preparer internal until mixed publication, reads and retention activate
together; a draft cannot itself produce a successful operation outcome.

### Preparation bridge and resource lifetime

The existing engine depends on the container. Do not reverse that dependency to
reuse `DocumentPartReader`, move provider orchestration into the ledger, or invent
a `Publication` for unpublished content. Extend the existing bounded reader rather
than introducing another executor, concurrency limit or provider lookup path.

The container now issues `DocumentRetainedReadPlan` through its internal upload
admission boundary after authorization and durable source checks. It binds the
canonical command, authenticated principal and operation generation, plus member/full part ordinals,
source revision and slot, complete physical object identity and retained backend
binding. This first plan covers retained objects only; the preparation integration
must additionally bind fresh attempt, selection revision and token.
Construction is controlled by the ledger; a caller-supplied object declaration is
not equivalent evidence. Reuse the batched comparisons in `DocumentReuseAdmission`.
`DocumentSourceSnapshot` alone does not contain the complete command object claim,
and `DocumentPartReader.readSourceSlots` currently selects by slot only.

The engine consumes that plan through the reader's existing aggregate reservation
and scheduling window. Read the exact retained generation, namespace, key and
provider version; verify size, digest and content type against the plan.
`DocumentPublicationLedger.Part` now carries the retained content type from managed
SQL reads, and the reader checks the exact provider value. Legacy snapshots without
a retained type remain explicitly unknown; they do not satisfy an exact typed read
plan. `DocumentPartReader.readRetained` now consumes the ledger-issued plan for one
named member using its existing resolver, aggregate budget and scheduling window.
Its returned parts correspond positionally to that member's filtered plan entries;
the plan retains full ordinals across upload/EMPTY gaps. This low-level read is not
yet wired into assembly or a public repository operation. No lookup of today's
drive or fallback backend is allowed.

Fresh bytes remain under `DocumentUploadPayloads.Use`; do not fetch them again
solely for assembly. The current coordinator closes this use before returning
`Staged`, so integration requires a scoped engine callback or equivalent controlled
handoff inside the use lifetime. It must also remain inside owner/selection
heartbeat, cancellation and operation-permit lifetimes. The current heartbeat ends
before the final selected-attempt verification; appending a callback after that
verification without extending heartbeat coverage is insufficient. Zero-upload
preparation still needs owner renewal and must not take the current early return.
Run the handoff after upload acknowledgments and SQL verification, while heartbeat
and failure checks remain active; the returned `Staged` snapshot is not that fence.

Use one shared payload budget for uploads and retained reads, reserving additional
copies before allocation. `PayloadBudget` accounts for reservations, not actual JVM
heap or protobuf expansion. Design and test an explicit worst-case expansion
reservation or independent parser/heap bound before claiming aggregate decoded
memory control. `DocumentReadBatch.parts()` exposes
byte arrays, and closing its batch releases their reservation; those arrays cannot
escape as an unbudgeted draft. Drain actual provider workers before releasing any
reservation, even when cancellation has already completed a Future.

SQL source evidence is point-in-time evidence, not a reader pin. Existing retained
history does not prove a general read-versus-prune protocol. Before allowing a
source read concurrently with reference release, prove a durable protection path
and retain it through actual I/O completion. Re-fence source authorization/revision,
owner, selected attempts and physical retention after I/O. Final publication still
performs its own atomic fence; neither a read plan nor a checked draft grants it.

### Document reader protection before activation

V44 implements the native document pin table, mirror/reference guards and exact
single-pin release primitive. V45 adds bounded atomic batch release with complete
origin, retention and native-pin lock sets and identity checks before/after locking.
Internal whole-plan acquisition now validates all
claims, locks complete sorted origin/retention sets and inserts deduplicated pins
atomically. V46 adds recovery of exact pin batches only after durable QUIESCED
evidence; it does not establish that evidence. `DocumentReadLedger` now owns a
fresh incarnation, counts capture and protected-plan lifetimes, fences admission
and refuses local quiescence until all plans close and their Uses end. The engine's
protected `readRetained` overload transfers a setup Use to the returned batch;
provider calls that ignore cancellation retain it until actual return. This is an
opt-in internal composition, not the production host default. The raw-plan overload
still requires protection supplied by its caller. The owner uses the existing
`repository_reader_incarnations` lifecycle from V31/V32: fresh ACTIVE incarnations,
permanent fencing, and trusted LOCAL_DRAIN attestation before QUIESCED recovery.
Pins do not expire. A deadline, cancelled Future or expired operation lease cannot
prove that a provider has stopped using an object.

V44 adds document-specific native read pins and a DOCUMENT_READER generic reference
kind. Archive pins have archive-entry/version foreign keys and cannot represent
document identities. Each document pin binds its reader incarnation and exact
physical object independently of current and historical document rows. Validate
every source claim in the canonical plan before deduplicating protection by object;
one physical pin may record one witnessed source, while the canonical command
retains all source claims for later reauthorization. A pin does not attest that
every source remains authorized. Deleting a source revision must not cascade-delete
its active read protection. Add the corresponding native-reference predicate and
mirror guards without weakening the existing archive/current/history cases.

Acquire protection for the whole canonical retained plan in one short transaction:
fence the operation owner, require an active reader, lock/authorize the source and
destination revision set, validate every claim, then prelock all distinct document
origin attempts in PostgreSQL UUID order FOR SHARE followed by all distinct
retention objects in UUID order FOR SHARE. Reject retiring or reclaiming objects.
Insert one native pin/reference per distinct physical object, then recheck owner
expiry before commit. Claim validation, sorted locks and inserts must share this
transaction; a previously captured plan is insufficient. No provider call occurs
under these locks. An invalid final object rolls back the whole acquisition.

The generic reference guard now has a shared-lock branch for DOCUMENT_READER;
its default exclusive branch would otherwise upgrade these locks. Preserve the
existing durable document reference modes until their separate concurrency audit.
Trigger-acquired locks must obey the same complete origin-then-retention ordering.
Release uses that order and exact pin/incarnation/object identity. Failed releases
stay durably pinned for retry or proven-quiescent recovery; never log and forget them.

The owning host must account for active provider calls and every open protected
batch. Fence new pin admission before shutdown, stop new reads, drain actual
workers and batch owners, then attest local quiescence. `DocumentPartReader.awaitIdle`
alone is insufficient: it does not count returned `DocumentReadBatch` lifetimes.
A stale remote host or UNKNOWN incarnation needs external proof of shutdown;
elapsed time alone cannot authorize recovery.

The engine integration reuses the batch's existing synchronized admission
barrier: `enterWorker` rejects a closed batch before provider I/O, and release is
eligible only when the batch is closed and its entered-worker count is zero.
Queued tasks therefore need no separate lifetime counter as long as every provider
call remains behind this barrier. A separate setup lifetime must cover capture,
resolver work and ownership handoff, including failures before a batch exists.
Closing a batch or cancelling a Future remains prompt even when a provider ignores
interruption. A latch signals plan drain without running user callbacks on the
last worker. The coordinator retains the plan handle, awaits drain with a bounded
timeout and explicitly invokes `release` outside batch/lifecycle monitors. Release
and recovery serialize per handle; SQL failure propagates and permits retry.
The host must bound its waiting tasks, preserve failed-release handles and keep
its SQL pool alive through release or recovery. An exception from a cancelled
worker's Future is not an adequate reporting channel. `DocumentReadRecovery` now
discovers exact durable pin identities without the original in-memory handle, only
for already-QUIESCED readers. Each V47 call selects 1–10,000 claims in indexed
reader/pin order, then uses V46/V45 without acquiring pin locks ahead of origins.
It returns the observed claim count, which may overlap another worker's batch;
callers repeat bounded calls until zero. A failure during concurrent cleanup is
visible and requires fresh discovery, never a fabricated successful drain result.
The caller owns scheduling, deadlines and retry policy. Cleanup race qualification
and production host mounting remain unfinished; these APIs do not establish
remote-crash quiescence for ACTIVE, FENCED or UNKNOWN readers.

Acceptance requires atomic multi-object rollback, acquisition versus cleanup in
both lock orders, logical source deletion while a pin still blocks reclaim,
cancelled real provider calls that continue holding their pins until actual return,
failed release/retry, wrong-incarnation and unquiesced recovery refusal, and
idempotent recovery after verified local drain. Exercise overlapping 257-object
plans submitted in opposite orders to expose hidden per-row lock upgrades. Direct
SQL must reject structurally forged pin identities and detached generic references;
caller authentication still belongs to the host under the trusted database-writer
boundary. Record lock waits and
client statement counts as well as correctness; avoid an origin lock per object.

Current qualification covers legal document deletion with both HISTORY and READER
references surviving, plus pin admission versus retirement in both transaction
orders using observed PostgreSQL blocking. It does not establish sole-reader
reclamation safety: V38 forbids revision/part deletion, V39 retains the history
reference, and attempt cleanup refuses any published history. Removing only the
current document cannot produce a reader-only object. Keep that acceptance case
open until a reviewed history-pruning path exists; do not disable guards to claim
it passes. Pruning must first be assessed against retained-schema decoding,
restoration, references and the optional JCR capability boundary. This checkpoint
adds no pruning API and changes no transaction semantics.

### Implementation order and acceptance

1. Expose the controlled exact read plan and extend the reader. Real SQL/provider
   cases must reject a matching slot with a different object/version/type, retain
   original backend identity after drive changes, and release budgets on failure.
2. Add the scoped upload handoff and engine composition. Count real provider calls
   to prove no extra GET for fresh assembly; exercise cancellation and lease loss
   during reused reads, zero-upload members and admission capacity exhaustion.
3. Add durable source-read protection and race it against prune/cleanup before
   enabling those combinations. A final stale-source rejection cannot repair bytes
   reclaimed while an admitted reader is still using them.
4. Integrate typed validation and descriptor retention, then enable publication only
   with the mixed-read/retention/outcome cutover gates above. Structural assembly
   alone remains insufficient for a successful typed operation.

Required cases include noncanonical protobuf wire ordering, unknown CORE/owned
payload bytes, forged fields in CHUNKS, mismatched IDs/ownership, wrong global
chunk subkeys, exact retained versions across providers, zero-upload drafts and
cancellation/budget failures with no visible revision. Existing staging tests do
not substitute for this assembly evidence.

## Migration and activation sequence

The first migration uses a document-specific shadow projection:

- `document_revision_publications`: revision UUID, node UUID, original publication
  mutation revision, immutable body, original publication time and a unique nullable
  legacy attempt UUID.
- `document_revision_parts`: revision UUID, full manifest position, part/sub-key
  slot and verified physical object UUID for each PRESENT entry.
- `document_revision_current`: document node UUID and its selected revision UUID.

These names are proposed internal tables. They introduce no generic content node
model or JCR dependency. The later reusable reference foundation retains objects
for these domain owners without requiring document-specific fields. Historical
backfill may use the old attempt UUID as the revision UUID value, with an explicit
legacy-attempt column; future revision allocation must have no such dependency.

1. Add independent revision identity and ordered reference storage with an explicit
   legacy-history mapping. Backfill only managed verified FULL_REVISION histories.
   Use each history's own immutable body, including histories whose document no
   longer exists; do not reconstruct historical metadata from today's row. Preserve
   publication time, exact original backend identity, optional versions and order.
   Expand each historical manifest with ordinality and separately rank PRESENT
   entries to match compact legacy object ordinals. Check both full manifest
   positions and dense object counts; copying V36's legacy revision_ordinal alone
   would silently misplace entries around EMPTY/DELETED slots.
   Store the full position zero-based. Compare slot, exact key, size and hash
   against verified attempt objects and their V25 physical locations, not only
   foreign-key existence or aggregate counts.
   Legacy unbound rows remain unknown. Missing or inconsistent managed evidence
   aborts migration. Do not fabricate schema or admission provenance for old rows.
2. Maintain the legacy FULL_REVISION bridge transactionally while the new projection
   is introduced. It must have an exact consistency check, not best-effort dual
   writes. Existing strict V22/V36 publication checks remain in force. Mixed and
   zero-upload publication stay disabled until read and retention cutover is ready.
   The shadow migration adds no generic retention owner kinds or duplicate pins:
   existing V26/V29 retention remains authoritative until the coordinated cutover.
3. Move managed reads and captured source fences to independent revision identity.
   Each selected part resolves its original backend generation, profile, namespace
   and exact provider version. Group client resolution by identity without grouping
   away manifest order. Keep the existing aggregate payload budget and one bounded
   scheduling window across the whole selection; do not multiply concurrency or
   allocate one pool per backend. No SQL locks survive into provider calls.
4. Derive history/current retention from revision reference membership rather than
   origin-attempt equality. Install and validate the new native ownership predicates
   and pins before releasing legacy pins. Preserve archive version and reader paths.
   Origin cleanup must refuse any surviving reference even after its original
   document is gone. Adapt staging reuse proof to the new authoritative relation.
5. Enable mixed and zero-upload commits only with completed provider qualification,
   assembled content validation, descriptor retention and durable operation outcome
   binding. Reauthorize all sources/destinations and recheck revisions at publication.
   Commit metadata, ordered references, retention and outcome/outbox in one SQL
   transaction. Never acknowledge success before that transaction commits.

Schema snapshots and historical raw ownership are explicit gates, not inferred
from current document references. Raw-bearing history requires verified adoption
or a separate correctly retained raw identity path. Until that path exists, reject
unsupported history/restore operations explicitly. Do not remove quarantine merely
to make a migration or example pass. Document history pruning likewise requires
reader protection before permanent history retention can be relaxed.

## Concurrency and execution requirements

Prepare immutable command data, descriptor closure and provider observations before
publication. Provider reads, writes and retries never occur inside the metadata
commit transaction. Existing source-proof batching is a useful starting point,
not proof of the latency of a complete commit.

The new path must compute the complete lock set for every destination, source,
placement and old/new physical reference before changing publication state. Acquire
each lock class in a single documented order across the entire change set. A
per-member loop that locks an origin attempt after earlier members already acquired
retention locks is unacceptable. Trigger-induced locks count in this audit.

The existing Java FULL_REVISION batch path now prelocks current-pin rows, then
the complete new/displaced-current origin set before validating members. It locks
their retention rows only after validation and before the first publication write.
This uses three client statements for a batch of up to 64 destinations. It is a
baseline ordering fix, not the mixed-revision boundary: that boundary must include
all reused origins and enforce its complete lock-set admission for direct SQL too.

With today's exclusive durable-reference guards, prelock the sorted distinct union
of origin attempts from all old and new reference sets, then the sorted union of
physical retention rows before executing any per-row reference mirrors. Install
new history/current pins before releasing old current pins. This is the safety
baseline for cutover, not the final throughput claim.

Durable references currently take exclusive origin-attempt locks. Reuse of different
objects from one large original upload can therefore contend on that shared row.
Do not carry that pattern into the new foundation without measuring and justifying
it. Evaluate shared immutable-origin validation and shared reference acquisition
against exclusive retirement/reclamation, with stable global ordering and no lock
upgrades. Cleanup and publication must use the same discipline. Merely changing a
single trigger's lock mode is insufficient to establish safety.

V41 removes exclusive source locking from typed staging. `DocumentRevisionLocks`
classifies destinations as writes and source-only identities as reads, choosing
the strongest advisory mode across identity/hash collisions before any row lock.
It preserves global Java UUID row order through same-mode runs of at most 256 IDs;
read-only rows use FOR SHARE and destinations use FOR UPDATE. Destination-first
row locking would conflict with existing multi-document deletion, which follows
UUID row order without the advisory protocol. Current policy and raw mutation
revision checks remain protected, and requested revisions are compared only after
authorization. FULL_REVISION publication now uses the same shared-source/exclusive-
destination row protocol and explicitly rechecks source revisions before any
drive/attempt work. Native origin/retention guards remain exclusive. Publication
callbacks may update destination bindings/outbox but must not modify source-only
documents or upgrade their locks. Arbitrary `saveIfRevision` callbacks retain the
older exclusive path.

Performance acceptance includes independent destinations sharing one origin,
independent origins, deliberate same-destination conflicts, multi-destination
commits, readers and concurrent cleanup. Record throughput, p50/p95/p99 latency,
lock wait/hold time, client statements, SQL execution plans, WAL and peak memory.
Use the same hardware and interleaved baseline runs. Report environment and raw
measurements; set regression limits from the measured workload before activation.
Bound both old and new reference sets, not only newly uploaded parts.

## Required acceptance cases

- Populate several retained histories, including a deleted current document;
  migrate and compare exact ordered identities and bytes. Corrupt managed evidence
  must fail migration rather than becoming an unknown legacy row.
- Read a revision assembled from two original backends with overlapping key names.
  Preserve selection order, cancellation, payload budgets and exact version reads.
  An unavailable original backend fails without consulting the current drive.
- Publish metadata-only and schema-only revisions without PUT/copy/empty attempts;
  still perform all required schema, ownership and assembled-content validation.
- Reject forged references, stale source revisions, revoked policy, obsolete
  operation generations, retiring objects and unsupported provider reuse identity.
- Race reference acquisition against cleanup and reader acquisition against prune.
  Pause real adapters outside SQL to prove unrelated commits can still progress.
- Roll back a failing second member with no first-member visibility or receipt.
  Replay after an uncertain commit returns the original recorded outcome.
- Retain raw and descriptor dependencies for historical decoding and restoration;
  do not consult today's registry or drive to repair missing historical evidence.

The JCR extension may later compose multiple graph changes using these foundation
primitives. None of these revision IDs, document addresses, accounts or counters
claim JCR node identity, sessions, workspaces or version-history semantics.
