# Independent revisions and retained part references

Status: Sol-reviewed implementation plan, not available behavior. Source audit at `6f562450`.
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
references. Existing attempts are not automatically selected. Replacement CAS,
terminal-result binding and recovery remain prerequisites for activation.

## Current coupling that must change together

- `DocumentPublicationLedger.Publication` carries one attempt, backend profile
  and namespace for all parts. Its query loads the attempt's complete verified
  object set. `DocumentPartReader` resolves that one backend for the entire read.
- `DocumentSourceSnapshot` fences the current attempt UUID. An independent
  revision, including one with no uploads, needs its own fence identity.
- V22 history is keyed by attempt UUID. Its guards require a live full attempt,
  exact ordered manifest and matching current document body. V36 deliberately
  prevents NEW_CONTENT attempts from entering that path.
- V26 retention derives document ownership from the object's origin attempt.
  The latest native-reference predicate is in V28; replacing only the earlier
  definition would lose archive reader protection. V29 shares locks for archive
  readers, but durable reference changes still exclusively lock origin owners
  and physical retention rows.
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
