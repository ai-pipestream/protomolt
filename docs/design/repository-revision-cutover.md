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
between fragments. Its explicit `Limits.maxWireValues` policy also preflights every
fragment before allocating a decoded Document. One budget counts every field
occurrence plus packed scalar elements across fragments, including duplicate
singular fields, map-entry messages and unknown-group contents. Known messages are
traversed through the generated descriptor; opaque unknown bytes remain byte-bounded.
Fixed-width packed bodies are alignment-checked and skipped without decoding, and
packed varints have cancellation checks during scanning. Original bytes are untouched.
Repository resolution, full manifest preparation and publication
wiring remain unimplemented. Byte bounds are not a heap limit; hosts must reserve
resources for protobuf object expansion and apply concrete policy. Wire counts
bound structural allocation inputs, not exact heap, merge costs or later Any/schema
decoding. The extra scan has not received production latency qualification. A single parse
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
solely for assembly. The internal `stageAndPrepare` callback now runs after upload
acknowledgments and SQL verification, inside payload ownership, owner/selection
heartbeat, cancellation and operation-permit lifetimes. Its scoped View exposes
read-only buffers at full member/ordinal keys and is invalidated before releasing
the Use. Zero-upload preparation renews its owner without creating upload workers
or an observation flusher. Ordinary stage/retry also keep heartbeat active through
final verification.

After a custom callback, the coordinator rechecks canonical command, authorization,
retained source bindings, all initial member selections including zero-upload rows,
owner lease and exact selected-attempt revisions. These are separate transactions,
not an atomic admission/publication fence. Heartbeat tasks drain and sticky failure
is checked before returning. The callback must not publish, perform semantic review
or let borrowed buffers escape; it is trusted bounded preparation code. Engine
assembly integration, decoded-memory accounting and schema validation remain open.

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

## Descriptor retention design

Status: reviewed design for implementation. DocumentSchemaBinding and
DocumentPayloadCheck verify in-memory evidence; neither persists descriptors.
Typed publication remains disabled.

### Storage and identity

Use a bounded immutable SQL descriptor catalog for the first implementation.
Descriptors are repository metadata. This choice does not select a document byte
provider or require S3. Stage bytes in a separate short transaction before the
publication transaction. Publication writes references, not descriptor payloads.
Do not add registry or provider calls inside SQL transactions.

Catalog identity is `(account_id, artifact_sha256)`. Store exact serialized bytes,
length and creation time. Enforce a maximum artifact size at the Java and SQL
boundaries, with a proposed ceiling of 16 MiB matching the existing registry
artifact ceiling. Hosts can set lower limits. Verify SHA-256 in SQL; a duplicate
key must compare exact bytes and refuse a mismatch. Do not deduplicate across
accounts or expose artifact existence outside the authorized account.
Artifact hashes are not read credentials. Historical reads obtain artifacts through
an authorized revision/path binding; an internal catalog lookup must not become a
public fetch-by-hash endpoint.

A schema binding identifies the full message name, canonical descriptor-closure
fingerprint and exact artifact hash. These hashes have different meanings. File
order and unknown descriptor-set envelope fields can change artifact bytes without
changing the canonical fingerprint. Store the fingerprint algorithm version with
admission evidence. Verify complete imports and selected-type closure before
staging; a SQL hash check does not prove protobuf validity or rule support.

Bound typed paths and distinct artifact keys per operation/revision, plus aggregate
staged descriptor bytes. Reject an oversized plan before acquiring locks. The
artifact-size ceiling does not bound a batch containing many distinct schemas.
Qualify these aggregate limits before activation.

The catalog is repository-controlled. SchemaRegistryStore descriptor storage is
optional and cannot serve as the historical retention authority. Missing artifacts
fail reads; never consult the current registry or drive to fill a historical gap.

### Staging and publication ownership

Protect staged artifacts with claims tied to the canonical operation key, owner
generation and stable member/path identity. Stage and claim creation must be
atomic. Reuse an existing artifact only after an exact-byte match. The engine
retains the verified binding and validation-policy identity as preparation evidence.
Staging does not create a readable revision or a successful operation outcome.

Publication must recheck operation ownership, account, expected revisions and
current authorization. Link each required artifact in the same SQL transaction as
the revision, ordered content references, admission evidence, outcome and outbox.
Carry unchanged path bindings into the new revision explicitly. A missing claim,
expired owner, missing artifact or changed evidence must refuse publication.
Recovery can restage under a valid owner before retrying; it cannot invent success.

Binding ownership belongs to the immutable historical revision. Deleting the
current pointer or document must not remove descriptors still required by history.
A binding includes the rendition/part selector and an exact typed-payload path;
root structured_data and parser-result Any values need distinct bindings. Inventory
existing selector contracts before choosing an encoding. Do not use wildcards or
ambiguous concatenated strings as persisted path identity. Pin layout/schema
versions needed to interpret that path.

No binding is backfilled for V38 legacy projections. Their missing schema and
admission evidence stays unknown. New bindings activate only with the reviewed
independent revision publisher. SQL foreign keys and hashes protect storage
integrity, not the truth of a validation verdict. A trusted internal publication
boundary must bind engine evidence to the exact operation and candidate; direct
SQL must not manufacture validated revisions through unguarded inserts.

### Cleanup and concurrency

Staging claims require a finite recovery lifecycle. Cleanup may remove an artifact
only when it has no historical bindings and no live staging claim. Expiry is not
permission to detach a committed reference. Do not solve cleanup by retaining all
unreferenced staging rows forever.

The proposed lock order is operation owners, existing publication locks
(document/source, physical origins and retention, when applicable), artifact keys,
then claim/reference rows. Lock each complete key set in deterministic order.
Artifact acquisition never takes an operation-owner lock later. Use one SQL order
for compound owner keys and artifact keys, including explicit text collation;
Java string ordering must not silently substitute a different comparator.

Cleanup first discovers a bounded candidate set and all related claim-owner keys
without locks. It then locks those owners, locks the artifact keys and rereads
claims and references. If a new claim owner was not in the locked set, roll back
and rediscover; do not acquire that owner after the artifact lock. Missing owner
evidence fails closed. Recheck generation, expiry and terminal state under the
owner locks before removing obsolete claims. A live claim or historical reference
prevents deletion. A candidate exceeding the owner-set limit stays retained; do not
truncate the proof to make it eligible. Cleanup can still remove bounded batches
of individually proven obsolete claims, following owner-then-artifact-then-claim
lock order. It must not remove claims outside the locked/proven batch. Final artifact
deletion requires the full claim/reference absence proof. Rotate discovery fairly
so a popular artifact cannot starve unrelated recovery work or leave dead claims
permanently unreachable.

Publication and cleanup serialize on artifact identity so a reference cannot
commit to deleted bytes. Never check liveness and later delete outside the
protected transaction. Avoid a shared counter or registry-wide lock that serializes
unrelated artifacts. Foreign keys and conflict handling can take implicit locks; include those locks
in the audit. Prelock existing rows before claim/reference mutations. Create new
artifact rows in the same sorted key order, accounting for unique-key waits. Test
trigger, foreign-key and conflict paths with SQL barriers before enabling cleanup.

A SQL descriptor read returns the complete bounded artifact under a database
snapshot. Later parsing owns local immutable bytes; it does not borrow a provider
stream. On historical decode, rehash the exact artifact, rebuild the closed schema
and verify type/canonical identity before decoding. Current access policy still
applies. Host memory accounting and cancellation cover the artifact and decoded
messages. This design does not permit history pruning until the separate restore
and JCR assessments authorize it.

### Implementation and acceptance

1. Add catalog and staging-claim storage with real PostgreSQL tests for byte bounds,
   hash enforcement, immutable identity, same-account dedupe, account isolation,
   exact replay and conflicting bytes. Prove populated migration creates no legacy
   schema bindings.
2. Implement operation-scoped staging and bounded recovery. Race owner replacement,
   expiry, cleanup and publication using real SQL barriers. Verify rollback after
   claim insertion and restart recovery without in-memory handles.
3. Add native revision/path references with the independent publisher. Test multiple
   paths, carried-forward bindings, changed-schema-only revisions and atomic
   rollback when a later member fails. Descriptor bytes stay outside the commit
   transaction; references, evidence and outcome commit together.
4. Read and restore historical data with the registry unavailable. Reconstruct from
   persisted descriptor bytes and revision metadata, reject corrupted/missing
   artifacts, and retain descriptors after current-document deletion. Exercise
   current authorization and explicit legacy-unknown behavior.
5. Measure staging size/latency, metadata-commit p50/p95/p99, lock waits, WAL and
   peak memory with repeated schemas and independent accounts. Qualify the ceiling
   and dedupe behavior before activation. A future blob-backed artifact store needs
   an explicit lifecycle and capability; there is no automatic storage fallback.

### JCR boundary

Immutable artifacts, references, staged claims and atomic publication are reusable
repository primitives. Document paths are one consumer of them. They do not define
JCR node types, sessions, workspaces or version histories. The eventual content
extension can compose schema references within a larger atomic change set. Keep
JCR dependencies out of this catalog and preserve existing protobuf identities.

### Runtime Any resolution is required

Runtime type resolution is a core ProtoMolt feature. Typed archival admission must
support `Any` values, including nested values, when their definitions are available.
The current internal payload helper's blanket rejection of schemas containing Any
is a temporary implementation gap, not the intended contract. An absent optional
Any field does not require a payload type lookup.

Reuse `StoredSchemaSources` for stored schema versions and transitive references,
`ProtoSourceCompiler` for source-to-descriptor compilation, and the existing
`DescriptorLoader` type lookup for descriptor sources. The repository engine must
consume resolved immutable bindings without depending on a registry service or
its storage implementation. Generated Java classes are not required for dynamic
decoding; Java generation can consume the same definitions as a separate facility.
Its existing coverage must be inventoried before adding a new generation path.

A resolution session must bind each actual Any occurrence to its full type URL,
selected schema version or content identity, complete descriptor closure and
validated payload. Resolve a type once within that session and reuse its frozen
binding. A type URL alone does not select a schema version. Conflicting definitions
must fail explicitly rather than depend on lookup order or a later registry value.
Historical reads use retained bindings and bytes, without consulting a mutable
latest version in the registry.

Walk ordinary message fields, repeated fields and map values to find populated
Any envelopes. Resolve and validate embedded messages recursively. Preserve the
original bytes. Charge nested work against shared byte, depth, wire-value, schema
and resolution budgets; do not reset the budget at each envelope. Retain every
resolved closure needed to reconstruct the revision. Apply authorization to schema
resolution as well as document access. Registry I/O and compilation stay outside
publication SQL transactions.

Acceptance includes source-only definitions absent from the application classpath,
valid and invalid nested payloads, unset Any fields, missing definitions, mismatched
types, conflicting versions, recursion and aggregate limits, repeated lookup reuse,
and historical decode with the registry unavailable. Annotation rules on the outer
Any envelope and on each decoded payload must both run. These cases are required
before treating typed archival admission as complete.

### Definition exchange and locally generated SPI clients

ProtoMolt servers exchange protobuf definitions and service metadata so a receiving
server can resolve types at runtime and construct a client for the remote service.
They do not exchange executable libraries, classes, JARs or service implementations.

Reuse `GenerateStubsAction` and `WasmProtoc` to generate message classes and gRPC
client source locally from the received definitions. The receiving host selects
its own generators, compiler, dependencies and SPI adapter template. Compile and
load the generated client through a service-specific class loader, then expose it
through the local SPI. Calls execute on the remote service through gRPC; received
schema definitions do not authorize running remote implementation code locally.

`Composer.builder()` already discovers local `ServiceModule` providers through
`ServiceLoader`. The connection from generated client source to runtime compilation,
service-specific loading and SPI registration still needs implementation evidence.
Generated protobuf messages are not SPI providers by themselves. Share the SPI and
protobuf runtime through the parent class loader to preserve Java type identity.
Pin schema and endpoint identities and define client disposal and loader lifetimes.

Received definitions are input data. Use fixed compiler options, controlled source
paths, approved dependencies and host-owned generators; reject options requesting
external code, plugins or classpath changes. Never load a library supplied by the
peer. A class loader is not an execution sandbox.

Keep compilation optional and outside base storage. Descriptor-based Any admission
must work without Java generation. Both paths use the same selected schema identity
and validation rules. Acceptance must exercise two servers exchanging definitions,
local generation and compilation, SPI discovery, an actual gRPC call, version
isolation, rejection of executable peer artifacts, failures and loader cleanup.

### Existing reflection and dynamic invocation

The existing `ReflectionClient` discovers services and their descriptor closures
from live gRPC servers. `ReflectedAnyActionTest` exercises reflection, profile
registration and dynamic request/reply handling for a real in-process Any service.
`DynamicGrpcCalls` invokes methods without generated client classes. Reuse these
paths for peer discovery and invocation. The optional generated-class path does
not replace them, and its missing compilation/loading evidence does not mean
runtime type resolution is missing from ProtoMolt.

For archival admission, freeze the selected reflected definition and retain its
complete closure. Discovery of a current remote definition alone cannot recreate
an older revision after that definition changes. No executable peer library is
needed for the dynamic reflection path.

### Existing selection, mapping and projection tools

Reuse the existing descriptor and transform libraries:

- `MappingHelper` and `MappingHelperJsonSupport` expose descriptor field paths,
  field metadata and mapping validation for clients and UIs.
- `ProtoFieldMapperImpl.FieldAccessor` resolves dotted paths and unpacks Any values
  through `AnyHandler` and `DescriptorRegistry` during traversal.
- `MetadataExtractor` evaluates named CEL selectors with descriptor validation.
- `MessageProjection` uses the mapper and CEL to build annotated target messages;
  it also derives source and target field masks.

These APIs already provide discovery, selection and transformation. Repository
admission must use definitions frozen for the operation when composing them.
Do not create another general selector engine for archival work.

Persisted admission evidence has an additional requirement: identify each selected
occurrence in one exact revision. The mapper's dotted traversal refuses repeated
intermediate fields; a repeated field selected as a whole is not one occurrence.
CEL can select or transform values, but the returned value alone does not identify
its source occurrence. A field mask likewise selects fields, not individual list
positions or map entries. Preserve these existing semantics and add explicit
provenance only where the repository needs it. Review the representation for field
numbers, repeated positions, typed map keys and Any boundaries before changing
contracts. Do not use display strings or arbitrary CEL expressions as the sole
identity of a retained schema binding.

### Staging recovery ownership constraints

The existing operation fence cannot authorize deletion of expired staging claims:
V34 refuses renewal of an expired owner, and V35 requires a live owner stamped by
this transaction. Taking over an operation would make that fence available, but
also changes the operation generation and token and installs a lease. A background
artifact sweeper must not take business ownership merely to delete staging data.
Takeover remains appropriate for a coordinator that will recover the command.

Recovery needs proof that it locked the operation owner before touching artifacts
or claims, without granting publication authority or changing that owner's token,
generation or lease. Any recovery-only stamp must use the database transaction ID,
never a caller-supplied value, and must remain separate from the live write fence.
An expired owner's recovery stamp must not permit staging, publication or renewal.
Generation and expiry checks establish that a worker cannot use the claim.
Deletion also requires a retention decision. Retrying the immutable command may
still need descriptor bytes after the registry disappears. Require a replacement
retained claim for the recovering operation, or an explicit terminal retention
decision, before releasing the last staging protection. Published revision and
historical references remain protected.

Required tests include a live claim refusal, expired and obsolete claims, unchanged
business ownership after cleanup, attempted write-fence escalation, renewal and
takeover races with SQL barriers, rollback, repeated recovery and bounded progress.
Keep claim deletion separate from artifact deletion. Published revision references
must protect descriptors independently of operation leases. Artifact deletion
requires a fresh locked absence check for all claims and revision references.

Final artifact collection locks the artifact FOR UPDATE, checks for zero claims
and zero revision references, then deletes. This collector acquires no operation
owner locks and deletes no claims. Claim insertion and cleanup take artifact
KEY SHARE before changing claims, so the artifact lock serializes the absence
check with both. Concurrent staging may fail and retry; a dangling claim must
fail to commit. Test that race explicitly. This replaces complete-owner-set
discovery for final artifact deletion only. Claim cleanup still requires
owner-first proof and an explicit retention decision.

### Archived Any assets and compiler provenance

An archived Any value includes the original payload bytes, exact type URL and
complete retained descriptor closure. Schema dependencies and validation options
are part of those assets. Bind them to the immutable revision and occurrence so
future readers can reconstruct the message without the original registry,
application classes or remote service. Preserve the original wire bytes even when
a decoded message can also be serialized; reserialization need not reproduce the
original byte order or encoding.

Capture the protobuf compilation provenance with the schema assets: compiler name
and version, compiler artifact or build identity where available, input schema
identity, relevant compiler options, and the protobuf runtime version used at
admission. Preserve declared proto syntax or edition in the descriptor files.
Distinguish protoc generation, ProtoMolt source compilation through Wire, and
imported descriptors obtained through reflection. Include generator/plugin versions
when generated code is retained or offered for reproducible client generation.
Reuse existing codegen provenance rather than inventing a second version source.

A FileDescriptorSet does not supply trustworthy compiler provenance by itself.
For an imported artifact, retain supplied provenance with its origin and trust
status. When the producer does not supply a compiler version, record it as unknown;
do not substitute the current local compiler version for the remote producer.
Local compilation records the actual toolchain. Provenance is immutable admission
metadata and must participate in the binding evidence without redefining existing
canonical descriptor fingerprints or protobuf type URLs.

Acceptance must reconstruct and serialize an archived value from retained assets
after registry removal, preserve access checks, exercise unknown imported compiler
provenance, and reject unsupported descriptor syntax/edition explicitly. Retained
bytes and metadata enable recovery; they do not promise compatibility with every
future protobuf runtime. Keep a tested decoding path and record its toolchain.

The additive `RepositorySchemaAsset` contract now reuses
`PublicationSchemaCondition` for canonical identity and records the exact artifact
digest, preserved type URL, compilation origin and compiler evidence. Local source
compilation requires the retained source bundle digest and observed compiler
identity. Imported descriptors accept producer-reported compiler information or
an explicit unknown reason. Admission runtime identity is recorded separately.
Compiler options are inert provenance, not instructions to execute.

Validation fixtures exercise generated and dynamic messages through ProtoMolt's
runtime validator. JSON Schema emits bounds and patterns plus CEL metadata;
cross-field coherence remains a runtime constraint. This checkpoint does not
establish OpenAPI execution of those rules or change the generators. The host
must verify the retained bytes, source asset, tool identities and access policy.
Revision/occurrence binding, durable provenance storage and automatic toolchain
capture remain implementation work; the contract does not expose a new RPC.

Schema storage is shared within an account: the artifact catalog key is
`(account_id, artifact_sha256)`, and operation claims reference it. Revision
bindings must reference retained assets rather than copy the descriptor bytes
into each object. Deduplication currently operates on exact complete-closure
bytes, not on individual imported files. Two distinct closures can contain the
same imports, and different encodings of the same canonical schema can occupy
separate artifacts. Preserve exact-byte identity when considering later storage
optimizations.

DocumentSchemaAssetBinding checks parsed metadata, complete descriptor closure,
canonical identity and the exact byte digest together. It retains immutable
metadata without asserting compiler authenticity or source retention. Its future
payload consumer must compare the exact metadata type URL with the admitted URL.
It is an internal preparation primitive, not a persisted revision reference.

DocumentPayloadCheck.checkAssets now connects this binding to the existing bounded
payload validator. Root metadata must agree with the host's exact URL policy;
nested resolution must return metadata naming the exact requested URL. Successful
preparation retains an immutable URL-to-asset map alongside the original payload
and decoded value. Each URL is resolved once, while every occurrence is validated.
This map does not replace occurrence-specific revision evidence or authenticate
compiler claims. Publication, durable asset references and trusted host capture
remain pending.
The resolver supplies one schema version per exact URL for a check; resolving
different versions at different occurrences needs the future path binding. The
host must also bound aggregate descriptor and metadata memory returned by the
resolver, separately from the existing payload and schema-graph limits.

Opaque DocumentCommandContent preparation now derives a RepositoryAnyResolution
observation for a present root structured_data field. It hashes the exact Any.value
bytes after fragment integrity and decoded ownership checks, preserves the type
URL and records not_attempted. No registry call or inner-message deserialization
occurs. An absent field produces no observation; a present empty Any records the
empty value's digest. URLs exceeding the observation contract's 4096-code-point
bound fail explicitly. Hashing adds one linear pass over the packed value without
copying it into another byte array or opening a SQL transaction. Nested opaque Any
occurrences are not inspected by this path. The result remains internal preparation
evidence; it is not persisted or a validated-content claim.
The observation identifies the effective Any.value after outer-envelope parsing;
the retained immutable fragments identify the original wire, including duplicate
fields and encoding differences. These identities must not be conflated.
