# Repository operation inventory

Implementation baseline: `528117a2d48cda3b3abedadd75b5706d7ac68ca7`.
Design: [repository composition](repository-composition.md).
Classification describes intended behavior, not just protobuf edits. Extended
operations retain their existing wire identities. New operations listed below
are proposed responsibilities; names and fields require contract review before
being added with handlers.

## Existing gRPC operations

Every existing repository RPC is included below. Shared authorization is an
extension even where the message shape remains unchanged.

### DriveService

- **Extended: CreateDrive.** Account-scoped administrative authorization and
  provider-specific provisioning. Preserve existing drive identity.
- **Extended: GetDrive, ListDrives.** Authorized visibility and capability-aware
  readiness without constructing an unselected provider.

### DocumentService

- **Extended: SaveDocument.** Shared ownership and contract gate; retain partial
  part/chunk-set saves and copy-forward. Typed content admission must apply to
  local, gRPC and HTTP paths. Define retry identity beyond deterministic doc IDs.
- **Extended: GetDocument, GetDocumentByReference, GetDocumentManifest.** Shared
  current-access checks, integrity and explicit completeness semantics. Existing
  requested-part reads do not establish a hydration session.
- **Extended: ListDocuments.** Account and document authorization before returning
  metadata; pagination must not leak denied records.
- **Extended: DeleteDocument.** Authorization and race-safe lifecycle checks;
  preserve existing purge state and exact manifest key ownership.
- **Extended: GetBlob, PutBlob, DeleteBlob.** Explicit administrative/raw-storage
  authority; cannot bypass document policy through drive/object coordinates.
- **Extended: GetBlobForUpdate, CompareAndPutBlob.** Same authority boundary;
  byte/CAS semantics unchanged. Preserve 9,437,184-byte limit, strong quoted ETags,
  checksum checks and explicit unsupported-provider failure. These RPCs explicitly
  do not promise idempotency-key replay; a lost write acknowledgement can conflict.

### ArchiveService

- **Extended: CreateArchive.** Account authorization; explicit admission policy
  without silently weakening opaque/typed distinctions.
- **Extended: GetArchive, ListArchives, GetArchiveStats.** Shared authorized
  visibility; aggregate counters must not expose unauthorized archive data.
- **Extended: PutEntry, UploadRendition.** Typed admission where required, staged
  visibility, schema retention, version metadata and durable retry identity.
  Preserve opaque originals and existing rendition addressing.
- **Extended: GetEntry, GetEntryManifest, ListEntries, ListVersions.** Current
  authorization for historical content, retained schema identity and explicit
  absence of legacy metadata snapshots.
- **Extended: DeleteEntry, DeleteRendition, PruneVersions.** Authorization,
  concurrent-reference safety and protected deletion-policy checks. Existing
  redaction semantics need explicit treatment alongside immutable snapshots;
  do not assume DeleteRendition creates a new version only.
- **Extended: ClassifyEntry, BridgeEntry.** Use the same admission/ownership path
  for derived output, preserving input/output version and policy identity.

## Other entry points and unchanged semantics

- **Extended:** HTTP document/archive uploads use the same engine boundaries as
  their gRPC counterparts, including streaming limits and staged publication.
- **New, library and HTTP implementation under review:** shared raw-ingestion operation for the existing HTTP
  document-upload route. It owns immutable upload attempts, checksum verification,
  trusted managed-object bindings and committed-reference receipts. No new public
  RPC is required for this extraction. The two original HTTP replacement
  regression cases now pass against the shared ingestion handler.
  `RawIngestionRepository` and `RawIngestionOperations` now implement the library
  boundary for process-authorized callers on qualified non-expiring streaming
  stores. SQL/S3 tests cover immutable replacement, dedupe receipts, checksum and
  length rejection, borrowed streams, drive changes, ambiguous PUT acknowledgement,
  revision-conflict retries, empty content-derived IDs and live candidate leases
  after failed duplicate publication. Qualified S3/cache compositions wire physical
  cleanup and start recovery before serving uploads. Disabled compositions return
  HTTP 503 for valid upload requests. Attempt IDs are internal identities, not client
  idempotency keys. This work is locally tested, not deployed.
- **Extended, planned:** document save/copy/delete and raw cleanup maintain managed
  raw-object references transactionally. Caller-supplied storage coordinates never
  grant deletion authority. Shared objects require zero-reference cleanup, and
  legacy deterministic keys require explicit migration before immutable guarantees.
- **Extended, implemented for qualified managed records:** full saves and partial
  BLOBS copies retain/release admitted raw references atomically with document
  publication; dedupe validates them before updating its counter. Source fragment
  checksums and commit-time binding/backend checks reject corrupt or stale copies.
  Acquiring a fresh managed reference requires the trusted ingestion path; ordinary
  saves cannot acquire one by supplying storage coordinates.
- **Extended, implemented:** PutBlob, CompareAndPutBlob and DeleteBlob reject keys
  containing the reserved `.protomolt-managed` segment, including generated put
  keys. Read operations retain their existing authority requirements. The guard
  is shared by library and gRPC calls; it does not enable managed ingestion.
- **Extended, implemented:** provider discovery reports NON_EXPIRING_WRITES for
  S3 and TTL-zero Redis. This reports adapter expiry behavior, not a deployment
  durability guarantee. Existing protobuf descriptors remain unchanged.
- **New, provider implementation under test:** `ObjectReclaimer` is a separate
  lifecycle port on an opened byte store. S3 advertises PHYSICAL_RECLAMATION and
  removes exact-key versions and delete markers in bounded passes, then checks
  absence. Ordinary raw delete APIs do not acquire this authority. Other providers
  report unsupported. Qualified S3/cache service composition supplies this port. A successful
  pass does not rule out a later completion of an old PUT.
- **New, library recovery implementation under test:** `RawObjectRecovery` claims
  eligible ledger records, resolves their original backend profiles and performs
  reclamation outside SQL. Failures remain durable and propagate; stale cleanup
  tokens cannot complete a newer claim. Tombstones remain available for subsequent
  late-writer reconciliation. Service recovery scans at most 100 records older than
  one hour per configured sweep interval. Unconfigured historical generations fail
  durably; this host currently resolves only its configured generation.
- **New, cache reclamation composition:** `CachingBlobStore.reclaimer` combines
  authoritative physical cleanup with strict cache eviction and absence checking.
  Cache failures propagate for durable retry. Real S3/Redis tests cover a lost
  cache-delete acknowledgement and a late cache fill; ordinary cache mutation
  error behavior is unchanged by this separate lifecycle port.
- **New:** library repository interface and remote implementation, reusing the
  existing request/response vocabulary rather than creating a second wire model.
- **New:** provider factory discovery and explicit capability selection.
- **Unchanged:** protobuf packages, existing field tags, import paths and Any URLs;
  part encoding and current conditional-blob behavior; entry-local byte dedupe;
  distinction between pipeline document parts and archive renditions.
- **New:** admitted schema artifact retention and retrieval, version metadata
  snapshot, operation replay lookup, and archival export/restore support where
  existing operations cannot express the required guarantee. Decide exact RPC
  additions after reviewing available registry and receipt contracts.
- **New:** pending hydration revision begin, patch, inspect, finalize and cancel
  responsibilities. Named-component replacement only; arbitrary field merging is
  outside this goal. Normal reads remain on the last committed complete version.

## Existing identities to reuse

`grpc.service.v1.SchemaSource` accepts exactly one of a type name, inline sources,
or descriptor-set input. It is an acquisition description, not a persisted
immutable registry identity. `grpc.profile.v1.SchemaSource` records acquisition
kind, source reference, descriptor fingerprint and artifact reference. Avoid
pulling the full platform service implementation into repository libraries.

`mesh.v1.SchemaReference` already specifies full type name and a SHA-256 of the
descriptor closure sorted by file name and deterministically serialized. Reuse
that canonicalization rather than inventing an incompatible digest. Review the
dependency cost before importing the mesh message itself into repository proto.

`RenditionDescriptor.schema_subject` records a subject only; it is not enforced
and does not capture immutable dependency closure. Keep the old field's meaning.
`OwnershipContext` already names account, datasource, connector, source owner and
DocumentSecurity; reuse those identities for policy and historical provenance.

`receipt.v1.WorkRecord` already binds subjects, steps and content-addressed
artifacts, and supports prior-manifest revision links. Its signed-record issuer
and key requirements must not be fabricated for an unsigned admission result.
Use an optional receipt projection from persisted admission/provenance facts.
Review delegation's existing candidate/attempt/contract/evidence bindings before
introducing another semantic-review identity.

Archive content-root equality currently elides saves. It is not evidence of
metadata/schema/policy-aware dedupe. Expected-version checks prevent stale updates
but do not recover a lost acknowledgement. These distinctions need acceptance
tests before extending retry contracts.

## Regression evidence to preserve

### Next boundary: archive destructive admission

`ArchiveDeletionFailureIT` now reproduces swallowed delete failures over both the
library and real in-process gRPC transport with PostgreSQL and S3. Both cases are
intentionally red: injecting UNAVAILABLE into physical deletion still produces a
successful response. Do not describe these paths as recovered or retry-safe yet.

Two additional cases inject a PostgreSQL serialization failure before entry
deletion commits, using the real S3 adapter. Both library and gRPC calls fail,
and SQL restores the entry/version rows, but the referenced object is missing.
These cases are also intentionally red. A storage-error propagation fix alone
cannot repair this rollback window.

The next implementation must cover DeleteEntry, DeleteRendition and PruneVersions:

- Lock and compare sampled entry/version state before admitting deletion.
- Persist exact original storage coordinates and a durable operation identity
  before object I/O. Fence writes that could reintroduce affected references.
- Make affected content unavailable through normal reads while physical cleanup
  is pending, without falsely reporting that physical deletion completed.
- Complete counters, tombstones and responses from confirmed state; preserve
  retryable failures and handle a timeout after successful completion.
- Exercise partial provider failure, SQL failure, concurrent saves, shared
  retained objects, restart and retry through both invocation paths.

Changing `deleteQuietly` alone cannot satisfy this boundary: object-first deletion
can still damage retained manifests if a commit fails or a concurrent save wins.
Post-save pruning and upload cleanup also call that helper and require durable
orphan handling. Keep archive operations process-authorized until mutation and
current-policy guards support scoped callers.

V12 adds database-assigned revisions to entries and retained versions, including
existing rows. Metadata merges and saves compare the sampled entry revision under
the entry lock; stale metadata cannot overwrite a newer edit at the same version
number. Migration coverage includes direct SQL updates and delete/reinsert of both
row types. Restores must preserve or advance the revision sequence beyond restored
row revisions.

V13 binds that entry revision to its retained-version set: inserts, rewrites and
deletes advance each affected owner's revision in the same transaction. Statement
triggers cover bulk and direct SQL changes, and moving a version touches both
owners. Saves and metadata/classification merges therefore reject a snapshot
sampled before a committed retained-set change. Regression tests reproduce all
three previously accepted stale writes against PostgreSQL.

This still does not make object-first deletion safe: bytes can disappear before
the manifest transaction commits. Durable destructive admission must fence that
interval and prevent later writes from reviving references selected for cleanup.
Direct SQL that locks versions before entries can also deadlock with an engine
save; PostgreSQL aborts a participant. Consistent application lock ordering and
explicit retry/error coverage remain part of destructive admission.

#### Reviewed deletion implementation boundary (not available yet)

- **Extended requests:** DeleteEntry currently has only address (tag 1);
  DeleteRendition has address/rendition/reason (tags 1–3); PruneVersions has
  address/keep_latest (tags 1–2). None has an idempotency key. Add optional operation
  identity using new tags, preserving all existing identities. Normalize the
  command and bind its fingerprint to trusted caller, account and operation ID.
  Reusing that identity with a different command must conflict. An address cannot
  be the replay key because deletion and recreation can reuse it.
- **Extended responses and new lookup:** persist logical version/tombstone counts
  and the terminal response, so an identified retry returns the original outcome
  after a lost acknowledgement. Add operation identity/status without changing
  the meaning of existing final counters. Legacy calls without an ID may use an
  internal identity for recovery but cannot promise caller-level replay; retain
  the existing absent-entry result. Explicit operation lookup is needed when a
  caller has an ID and the request ends before a response arrives.
- **New durable archive operation ledger:** store normalized command scope,
  fingerprint, sampled revision, admission state, claim token, attempts, bounded
  error, final response and exact target objects. Reuse immutable backend profile
  generations and original-profile resolution from managed raw recovery. Reuse
  the document purge admission/drain pattern, not its document-specific table or
  drive-name-only addressing. Resolve credentials in the host, never in receipts.
  Existing archive manifests retain an object key but no original bucket/backend
  binding; copying today's mutable drive configuration is insufficient. Persist
  an immutable storage binding for each new physical object. Legacy objects need
  verified binding backfill before physical deletion, or fail closed.
- **Admission and visibility:** in one transaction under the entry lock, compare
  the sampled revision, persist cleanup targets, and hide affected content from
  ordinary reads. Save the logical removal/tombstone outcome for replay. Provider
  I/O starts only after admission commits and runs outside SQL transactions.
  Count physical objects only after confirmed absence. Failed cleanup remains
  discoverable and retryable with its original coordinates.
- **New archive object generation/reference fence:** current rendition keys are
  content-addressed and can be reused by a later write. A pre-delete reference
  check cannot stop an in-flight PUT finishing after cleanup or a new save
  publishing the same key during cleanup. Track immutable physical generations
  and admission leases before PUT; a deleting generation cannot gain references.
  Later valid writes need a distinct generation. Retain cleanup tombstones for
  late writes, as in managed raw ingestion. Legacy referenced keys need explicit
  migration/adoption; never pretend they already have these guarantees.
  Prefer unique per-write keys for new physical objects while carrying forward
  existing manifest references for unchanged content. Stop minting legacy
  deterministic keys after cutover. Content hashes remain dedupe/integrity facts,
  independent of physical identity.

Acceptance must include commit rollback before admission (no object I/O), crash
after admission, lost cleanup acknowledgement, restart through the original
backend, stale worker completion, delayed PUT after cleanup, delete/recreate at
the same address, identical-content rewrite during cleanup, shared retained
objects, and same-ID/different-command rejection. Run the shared operations over
both library and gRPC. This design adds no available RPC or runtime capability.

After destructive admission, add historical metadata/schema/policy snapshots.
Current historical archive reads expose current entry
metadata, and same-content saves can merge metadata without creating a version.
Typed admission must not inherit that ambiguity.

### Existing baseline cases

- `DocumentPartCodecTest`: full/core byte round trips, absent field preservation,
  chunk partitioning, root checksum, manifest JSON and path validation.
- `ArchiveServiceIT`: retained versions, unchanged rendition sharing, expected
  version conflict, streaming size validation, HTTP receipt, redaction tombstones,
  pruning shared objects, delete counters and unversioned archives.
- `ConditionalBlobContractValidationTest`: annotation and cross-field validation.
- `ConditionalBlobRpcRustFsIT` and `RemoteBlobStoreConditionalTest`: real storage
  conditional behavior and client response/identity checks respectively. Inspect
  individual coverage before claiming concurrent-write or failure recovery proof.

Gradle repo tests and Buf lint succeeded on the unchanged source baseline.
The forced fresh run of repo-proto, repo-container and repo-service tests also
passed (212 tasks executed, 1m 47s); log:
`/tmp/protomolt-repository-baseline-fresh.log` in the implementation workspace.
`scripts/check-proto-compatibility.sh 528117a2d48cda3b3abedadd75b5706d7ac68ca7`
passed before any protocol change. These checks do not prove the proposed gates,
historical snapshots or hydration behavior; those require new red/green tests.

### Remote byte coordinate migration

The extracted client and service now require explicit local-bucket to remote-drive
bindings. The single-drive Java constructor binds only that same logical bucket
name. The service checks loaded drive buckets against its configured map; startup
without a remote map fails before database acquisition. Operators must identify
existing bucket/drive pairs and keep the existing object keys and version IDs.
The additive `DriveProviderConfig.remote` arm (tag 3) persists endpoint and remote
drive name; existing tags and names are unchanged. Missing legacy bindings and
configuration drift fail before returning drive records. Runtime annotations
validate nonblank, bounded identity fields; equality to configuration is a handler
obligation. Legacy rows require an explicit verified backfill, not automatic
adoption. Many-to-one mappings are rejected. This is a deliberate change from ignored bucket
arguments. Remote namespace provisioning remains unfinished. Direct self-routing is checked
at listener startup for in-process names and local TCP addresses on the bound
port. Proxy and multi-node cycle detection is not implemented. This work does
not establish complete remote repository parity.
