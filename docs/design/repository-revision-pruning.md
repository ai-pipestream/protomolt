# Revision pruning and retained audit identity

Status: direction reviewed by Sol, 2026-10-08; implementation gates remain open.
No pruning endpoint or deletion permission
is enabled by this document. This is the next durability slice of
[repository composition](repository-composition.md), following the
[historical restore design](repository-historical-restore.md).

## Decision

Pruning releases a revision's content retention obligations. It must preserve
the revision identity, publication outcome, provenance and operation receipt.
A committed revision can therefore remain a valid historical fact after its
content is no longer available for reading or restoration.

Do not delete `document_revision_publications`, `document_revision_commits` or
`repository_operation_success` as the implementation of pruning. Introduce a
separate durable prune record, and distinguish immutable evidence of what was
published from live references that require payloads and descriptors to remain.
Retaining evidence does not by itself require retaining every payload forever.

This is a proposed change in internal retention semantics, not permission to
weaken existing SQL guards. Every affected native-reference check, mirror,
foreign key and historical reader must change together with its acceptance tests.
Existing protobuf names, field tags, import paths and Any type URLs remain intact.

## Why deleting revision rows is incorrect

The current sources establish several independent obligations:

- [V38](../../repo/container/src/main/resources/db/migration/repo/V38__document_revision_projection.sql)
  makes revision projections and their parts immutable. The current revision
  points to a retained publication.
- [V52](../../repo/container/src/main/resources/db/migration/repo/V52__independent_document_publication.sql)
  binds native commits to publications and stores `previous_revision` as a foreign
  key. Publication receipts survive the execution that created them.
- [DocumentPublicationReplay](../../repo/container/src/main/java/ai/protomolt/proto/repo/container/ledger/DocumentPublicationReplay.java)
  checks stored result members against those commit and publication rows. Missing
  rows are data loss, not an expired receipt.
- [DocumentHistoricalReadRows](../../repo/container/src/main/java/ai/protomolt/proto/repo/container/ledger/DocumentHistoricalReadRows.java)
  checks the full part set against the retained manifest. Removing parts without
  defining a pruned state would report corruption.
- [V55](../../repo/container/src/main/resources/db/migration/repo/V55__document_revision_schema_references.sql),
  V56 and V57 bind schema artifacts, evidence and assets to commits and parts.
  Artifact bytes currently live directly in the schema artifact catalog (V48).
  Keeping those foreign keys unchanged prevents actual artifact reclamation.
- [V66](../../repo/container/src/main/resources/db/migration/repo/V66__document_assessment_staging.sql)
  derives `DOCUMENT_HISTORY` reachability from sealed publications and their
  parts. A new tombstone alone would not stop those rows from retaining content.

Consequently, neither deleting retention mirrors alone nor merely recording an
intent is a completed pruning implementation.

## Observable behavior

A successful prune transaction makes the exact revision unavailable for new
content captures. Authorized historical reads and restore attempts return
`FAILED_PRECONDITION` with an explicit pruned-content reason before provider or
registry access. Missing content for a revision that is still retained remains
`DATA_LOSS`. Authorization runs first; a tombstone is not an existence oracle.

Exact publication retries continue to return their original receipt after
authorization. They do not recreate data, rerun admission or claim that bytes
remain available. New restoration creates a new revision and requires retained
source content; it cannot silently substitute a newer revision or schema.

An internal process-authority operation initially accepts one exact node/revision
and a stable request identity. The durable result binds the account, node,
revision, request digest and released reference set. Identical retry returns that
result; the same identity with different arguments conflicts. A timeout after
commit means unknown outcome until that identity is reconciled. Cancellation
after commit does not undo the committed prune.

Keep logical prune completion separate from physical reclamation. SQL records
the release and durable reclaim work atomically. Provider deletion occurs later,
using the exact stored backend generation, realm and provider object/version.
Unavailable providers retain retryable work and report failure explicitly.
If implementation needs an earlier committed fence, call it pending work; reserve
the final `PRUNED` result for the transaction that releases the live references.

## Atomic decision and concurrent reference acquisition

[DocumentRevisionLocks](../../repo/container/src/main/java/ai/protomolt/proto/repo/container/ledger/DocumentRevisionLocks.java)
already provides ordered document advisory locks and row locks.
[authorizeHistory](../../repo/container/src/main/java/ai/protomolt/proto/repo/container/ledger/DocumentAdmissionAuthorization.java)
takes a shared source-document lock before reading history;
[DocumentReadPins](../../repo/container/src/main/java/ai/protomolt/proto/repo/container/ledger/DocumentReadPins.java)
then locks physical origins followed by retention rows and inserts pins in that
transaction. Pruning should use the same document boundary in exclusive mode.

The proposed decision transaction must:

1. Bind its own idempotency identity without taking another operation's execution
   ownership. Require the established READ COMMITTED transaction semantics.
2. Acquire the candidate document's existing exclusive revision lock and row lock.
   Check account, exact sealed native revision, current-pointer exclusion and
   current policy. Do not take an operation/claim lock after this document lock.
3. Check durable source dependencies, including historical read pins, assessment
   source slots and unreleased preparation roots. Expiry is not release evidence.
   Initial eligibility is limited to supported sealed native revisions with the
   verified PRESENT CORE required by V52 and `DocumentCommitWriter`. Their whole
   capture pins at least that object. Recheck this invariant rather than treating
   malformed empty history as unreferenced. If a future contract admits zero-part
   history, add durable revision-level capture protection before activating it.
4. Lock the complete affected physical origin set, retention set and schema set
   in their established orders; recheck eligibility after waits. The mixed
   document/archive origin order must match V65's `lock_repository_retention_set`:
   archive origins first, document attempt origins next, then retention rows, each
   set ordered by PostgreSQL UUID order. Never interleave origins and retention.
5. Recheck the blockers under those locks, install the prune result and release
   only this revision's live references in one transaction. Enqueue exact reclaim
   candidates without doing provider or registry I/O in the transaction.

This order is a design constraint, not yet a proof of every acquisition path.
Before activation, inventory historical capture, preparation insertion, assessment
staging, publication, source restoration and direct SQL guards. Each must either
hold the same source-document fence and reject pruned state before acquisition,
or use a separately proven compatible fence. No trigger may acquire an earlier
lock after holding a later lock. A Java caller convention alone is insufficient
if another supported path can bypass it.

If capture wins, its durable protection blocks pruning until qualified release.
If pruning wins, the waiting capture observes pruned state and refuses before I/O.
Logical locks cover metadata decisions, not the lifetime of provider requests;
durable pins cover work after the capture transaction commits.

## Coverage and retention boundaries

V103's normalized preparation roots and sealed header bind a preparation digest,
command digest, root count and root-set digest. Use that proof and the qualified
root-release receipts. A preparation with missing coverage is unknown, not empty.
Terminal execution alone cannot discard its recovery dependencies.

Unknown legacy preparation coverage needs a bounded reconciliation protocol before
pruning can be generally useful. Decode and verify the persisted canonical record,
then register exact roots under the same acquisition fence. A scan performed before
the prune transaction is not authorization. Activation must prevent an older writer
from inserting an unclassified preparation after a coverage check. Choose and test
the database-enforced coverage/version barrier before enabling deletion; do not put
an unbounded scan of every command blob on each prune request.

The reviewed approach is canonical coverage certification, not header existence
alone. V103 does not decode protobuf: an internally consistent empty or unrelated
root set can still omit the command's actual sources. The private Java journal
must require `DocumentPreparationHistoryRoots.coverage` to return EXACT before
writing a certificate in the same transaction. Bind that certificate to preparation
identity, preparation digest, command digest, root count/digest and verifier version.
For a new root-owning preparation, the deferred constraint must require both the
certificate and sealed header. An install-only recovery successor is different:
its executable preparation refers to an older retained-root owner. Allow that
installation only through the exact same-transaction successor edge, including
owner and command/preparation digests, with V93's claim, modes and owner completion
checks. It must retain an unresolved entry and must not acquire an invented history
header or certificate. Installation does not grant activation or capture authority.
V118 implements new root-owner certification. Pruning is not enabled.

This proof relies on the trusted repository handler. Direct table DML using the
backend database role is privileged administration, not a supported publication
API. SQL checks identity, atomicity and relational invariants; it cannot attest
that Java decoded protobuf, and a same-role writer can forge a marker. Document
this boundary rather than describing the marker as independently verified evidence.
Supporting untrusted direct SQL publication would require a separate privilege
boundary or a database-side decoder.

Track pre-certification preparations in an account-indexed unresolved ledger,
populated once during migration. Reconcile bounded batches with the existing
preparation codec and digest checks. For live roots, require canonical coverage;
for released roots, use `DocumentPreparationRootReleases.requireReleased` to verify
the exact terminal receipt, canonical roots and qualified capture drains. Missing
headers, false empty sets, mismatched digests or absent release evidence stay
unresolved. Do not invent roots or infer release from expiry. Clear an unresolved
entry only in the transaction that records its verified certificate. Successors
need separate bounded lineage reconciliation through immutable install edges to
the original verified root owner, including its exact release proof when released.
An activation receipt alone does not replace that verification. V119 now records a
separate immutable lineage proof for install-only successors. The current canonical
preparation is decoded, then claim, current preparation and previous preparation
are locked in that order. Its exact V93 edge must match both preparation identities.
The immediate predecessor must already have a root certificate or lineage proof;
the inherited anchor, command, root fingerprint and depth must match. Each proof
advances exactly one generation, with a maximum depth of 64. An unresolved
predecessor keeps the successor unresolved. Missing both predecessor proof and
unresolved marker is corruption. Proof insertion and unresolved deletion commit
together. No root header, capture or execution right is created.

`DocumentPreparationCoverageReconciliation` now verifies one retained root owner
with a bounded decode reservation and explicit SQL timeouts. It rechecks immutable
identity under claim then preparation locks. Released roots go through the full
terminal/release verifier before certification; missing headers remain unknown.
`DocumentPreparationCoverageBatch` buffers at most 64 keys for one account and
operation principal, using UUID/generation keyset pagination. Each entry commits
separately. Failures propagate and leave the failing entry unresolved; retrying the
same cursor is safe because completed entries leave the unresolved index. A corrupt
entry currently stops that principal's batch, requiring investigation or explicit
cursor selection to inspect later entries. There is no silent skip.

The final unresolved lookup covers the whole account, including other principals
and keys before the cursor. Its result is an observation, not pruning authority:
new work may commit afterward. Additional lineage/release and concurrency
qualification and the final pruning transaction remain required. A lineage proof
attests canonical ancestry, not current live retention: root release may happen
later and pruning must separately evaluate the remaining owners.

Avoid an account counter lock held across document or physical-origin locks. A
READ COMMITTED indexed unresolved check is combined with the source document's
exclusive fence: new committed preparations must certify atomically, and any new
source root must acquire that same source fence. The reconciliation protocol and
future-writer guard need race tests before this becomes pruning authorization.

Shared physical objects and schema artifacts survive until their last live owner
releases them. Current revisions, archive references, reads, assessments and staged
schema claims remain independent owners. A pruned source's immutable provenance
does not automatically pin all source content forever; any consumer that requires
restoration must acquire an explicit retention obligation.

The existing diagnostic inventory always reports unresolved future content-repository
references. That warning must not be mistaken for a real reference, or silently
removed as though JCR were implemented. The base repository supports only declared,
qualified retention owners. An optional content-repository extension must register
its obligations transactionally before it can participate in pruning. Unknown or
unqualified installed retention capabilities disable pruning.

## Schema and part record layout

Keep immutable identities and hashes separate from reclaimable bytes. Physical
location rows are metadata; their foreign keys do not require the provider bytes
to survive forever. Preserve those identities and part evidence, and change the
live-reference predicate only through the qualified prune transaction.

The existing `DocumentAttemptCleanupLedger` is attempt-wide abandoned-upload
cleanup. Its claim lists keys, and it refuses an attempt while any of its objects
is referenced. It is not an exact-version, per-object history-pruning worker.
Do not route a partly shared attempt through that worker or remove its protections.
Published-history reclamation needs a claim bound to the eligible object/version;
other objects from the same original upload attempt must remain untouched.

Schema bytes need an actual storage split because V48 keeps `artifact_bytes` in
the identity row referenced by immutable revision evidence. Prefer preserving that
identity catalog while moving bytes to a separately reclaimable payload relation.
The reviewed migration must preserve digest and size verification, update every
artifact consumer and restore path, and make absent payload explicit. Do not retain
duplicate full descriptor sets on every revision.

The layout must preserve exact receipt replay, historical metadata inspection
and evidence hashes, while allowing last-owner payload reclamation. Migration over
existing populated revisions must demonstrate both properties. No nullable artifact
bytes or relaxed checksum constraint may accidentally make an unavailable schema
look like a usable retained artifact.

## JCR foundation boundary

Stable identity and multi-object transaction composition belong in the repository
foundation. Retention holds need a reusable transactional representation. JCR
sessions, workspaces, node/property types, strong references, version histories
and restoration semantics belong in the optional content-repository extension.
ProtoMolt account/workspace/version identifiers are not substitutes for them.
No JCR dependencies or compliance claims are introduced here.

## Implementation order and exit criteria

1. **Close the reference and lock inventory.** Enumerate every acquisition and
   release path, including the nonempty CORE invariant, older preparation records and SQL
   bypasses. Sol reviews the chosen payload/catalog layout and complete lock order.
2. **Introduce pruned state and fenced acquisition together.** Real PostgreSQL
   tests force both orderings of capture, preparation registration and assessment
   staging versus pruning. Current revisions and unknown coverage refuse. No public
   pruning API is advertised while this boundary is incomplete.
3. **Release native references and mirrors atomically.** Test two retained revisions
   sharing an object/artifact, release of only one owner, active reads, two preparation
   roots, final qualified release, transaction rollback and lost commit acknowledgement.
   Direct deletion attempts outside the qualified protocol must still fail.
4. **Reclaim and recover.** Real providers prove deletion of only the eligible exact
   version, provider outage/retry, crash between logical release and provider cleanup,
   duplicate workers and restart. Retained siblings remain readable and decodable.
5. **Verify callers and operations.** Library and gRPC tests distinguish pruned from
   corrupt content, preserve authorized exact receipt replay, enforce current ACLs,
   and refuse revoked callers before disclosing state. Tests include source restore
   racing pruning, no registry substitution and refusal of malformed empty history.
6. **Qualify migration and performance.** Migrate populated history, reconcile legacy
   coverage, run the storage regression gate, restore a matching backup into an
   isolated host, and measure RustFS latency/throughput with concurrent unrelated
   documents and multiple repository instances. Report measured limits separately
   from correctness. Avoid global serialization of ordinary reads/publications.

Existing PostgreSQL fixtures to extend are `DocumentRevisionRetentionInventoryIT`,
`DocumentHistoricalReaderPinsIT`, `DocumentPreparationHistoryRootsIT`,
`RepositorySchemaArtifactsIT` and `DocumentRevisionSchemaArtifactsIT`. They provide
setup and existing protection assertions; none currently proves the proposed prune
operation. Add forced race orderings rather than relying on timing-only stress.

A persisted intent, green narrow test or unreferenced mirror is not the exit
criterion. The slice is complete only when an eligible revision can release its
last payload/schema ownership, recovery can finish exact physical cleanup, retained
siblings and in-flight consumers remain safe, and immutable replay still works.

## Review checkpoint

The first implementation prerequisite is V116's nonblocking shared source-key fence
on read-pin and preparation-root INSERTs. Both read scopes are covered because a
CURRENT pin can survive a pointer move. [PostgreSQL evidence](../evidence/repository/2026-10-08-history-acquisition-fence/README.md)
covers both transaction orderings and complete registration rollback. This does
not establish a pruned-state check, release live references or permit deletion.

V117 applies the same acquisition fence to assessment source-slot INSERTs. It
resolves both REUSE and HISTORICAL_REUSE through the immutable source revision,
including REUSE's absent source_node. [Assessment fence evidence](../evidence/repository/2026-10-08-assessment-source-fence/README.md)
covers both orderings, transaction rollback and successful retry. Committed slots
still retain their sources until qualified release. Canonical preparation coverage
certification is the next activation prerequisite; a sealed V103 header alone is
not sufficient.

Sol reviewed the source obligations and this design on 2026-10-08. The review
supports preserving audit identities, splitting schema payloads, and avoiding the
attempt-wide cleanup path. It also confirmed the nonempty CORE invariant, so no
new capture table is required solely for hypothetical zero-part native history.
The implementation must still prove SQL acquisition fencing, native-reference and
mirror transitions, legacy coverage, and the complete lock order. This review is
not evidence that pruning, schema reclamation or their acceptance gates have landed.

## Implementation inventory after coverage reconciliation

Source review on 2026-10-08 established these coupled changes for the next migration.
This inventory is not an enabled prune API.

1. The active `repository_native_reference_exists` definition is in V66, including
   the ASSESSMENT alternative. Update only the live DOCUMENT_HISTORY predicate for
   completed prune records. V26's reference guard must continue rejecting deletion
   while a native owner exists. Preserve all unrelated owner alternatives.
2. V39's history mirror runs when a projection seals. Pruning therefore requires
   explicit removal of the exact revision/publication-revision mirror set. Preserve
   immutable publication, part, physical-location, commit and receipt records.
   Check the complete distinct object set before and after mirror removal; a missing
   reference is corruption, not an excuse to prune an incomplete set.
3. Use V65 `lock_repository_retention_set` for the complete object set after the
   document fence. That function orders archive origins, document attempt origins,
   then retention rows. Do not substitute an object-by-object loop. Acquire schema
   locks afterward in digest order. Recheck dependencies after any lock wait.
4. Extend V116 read-pin and preparation-root acquisition and V117 assessment-slot
   acquisition with exact revision-state checks under the existing source fence.
   Current-scope pins remain protected after a pointer move. V63 and V86 also require
   consistency with the changed live-reference predicate. A reference cannot be
   acquired between eligibility inspection and the completed prune transaction.
5. Add the authorized pruned-content refusal to `DocumentHistoricalReadRows`,
   `DocumentHistoricalReferenceAdmission` and `DocumentHistoricalSchemaRows`.
   Schema-only materialization is a separate entry path and must not load descriptors
   for pruned content. Keep receipt replay on immutable evidence rather than forcing
   content capture as a replay prerequisite.
6. V55/V56/V57 associations remain archival evidence. Update live-retention diagnostics
   to distinguish them from content ownership. V48 descriptor payload separation
   and schema reclamation still require a separate populated migration and tests;
   deleting physical history mirrors alone does not reclaim descriptor bytes.
7. Persist idempotent prune identity and exact reclaim candidates with reference
   release. Provider cleanup must resolve immutable physical identity to the exact
   provider version; `repository_physical_locations` alone does not contain every
   provider version field. Audit the archive/document source records before defining
   cleanup bindings. The existing attempt-wide cleanup worker remains unsuitable.

External branches based on 4f4d66bff cover additional reconciliation qualification,
RustFS scaling and isolated backup. The coordinating branch retains the pruning
migration, acquisition checks, reclamation protocol and integration review. No agent
should merge a schema or retention change merely because an older-base rehearsal
passes. Re-run the applicable acceptance cases after integration.
