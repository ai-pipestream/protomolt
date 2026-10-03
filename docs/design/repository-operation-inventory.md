# Repository operation inventory

Implementation baseline: `528117a2d48cda3b3abedadd75b5706d7ac68ca7`.
Design: [repository composition](repository-composition.md).
Architectural gate: [optional JCR 2.0 compatibility](repository-jcr-compatibility.md).
Assess new contracts and transaction boundaries against that requirement without
changing existing protobuf semantics or adding JCR dependencies to storage modules.
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

- **New, Java byte SPI:** `BlobStore.getBounded` reads a complete selected object
  version within a caller-supplied payload limit. Unsupported adapters fail
  explicitly; no ordinary-read fallback is permitted. The direct S3 adapter
  implements it and advertises `BOUNDED_READ`. Cache, Redis and remote adapters
  do not yet implement it. Existing GetBlob protobufs and the conditional-write
  bound are unchanged. Document staging requires this capability and bounds
  verification by the planned payload size. Document publication reads and legacy
  source reuse enforce recorded part sizes through this operation. Oversize is
  DATA_LOSS; unsupported providers fail with FAILED_PRECONDITION. Shared memory
  accounting across operations remains outstanding.
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

The new `archive_mutation.proto` defines reviewed request/receipt/lookup messages.
`archive_mutation_service.proto` adds the separately mounted `ArchiveMutationService`
without adding ignored fields to the existing RPCs.
`ArchiveMutationRequest` wraps exactly one existing destructive request and a
required operation UUID. `ArchiveMutationReceipt` separates immutable logical
counts from observed physical cleanup, with a command fingerprint, observation
time and monotonic status revision. `COMPLETED` means the latest durable cleanup
records confirm absence, not that lookup performed a fresh provider check.
Undetected late writes may exist until reconciliation; a cleanup claim can reopen
the physical status with a newer status
revision. Targets partition into pending and confirmed-absent objects. No-op
operations have a completed, zero-target receipt.

Handler obligations, beyond annotations:

- Use `(account, trusted stable principal, operation UUID)` as the exact lookup
  and idempotency key. Never accept the principal from the request. Reauthorize
  lookup under current policy; an operation UUID is not a bearer credential.
- Validate before admission, reject unknown command fields, normalize using the
  same address/rendition rules as execution, and fingerprint the deterministic
  selected-command encoding including its kind. Exclude operation ID, observed
  timestamps and runtime sampled revisions. Persist the normalized command too.
- Lock the entry and compare its sampled mutation revision, then commit the
  logical result, distinct cleanup targets and receipt together. Persist absent
  entry and other no-op results so replay cannot affect a later recreated entry.
- Replay never reruns the command. Return the original logical outcome with the
  latest physical observation. Same key/different command is CONFLICT. Enforce
  receipt address/kind/fingerprint equality with the admitted command in SQL.
- Cancellation before admission commits has no logical effect. Cancellation or
  lost acknowledgement after commit does not cancel cleanup; query or retry the
  same ID. Do not blindly rerun uncertain transactions. Stale cleanup claims
  cannot overwrite newer status observations or move confirmed counters backward
  without a newly recorded reconciliation observation.
- Invalid contracts are INVALID_ARGUMENT; unsupported annotation rules fail
  closed; policy denial is PERMISSION_DENIED. Storage outages remain discoverable
  as RETRY_REQUIRED with bounded error categories, never fabricated completion.

Runtime fixtures cover generated and dynamic messages, required/exclusive action
selection, nested request rules, bounds and receipt cross-field accounting. JSON
Schema exposes UUID format and CEL metadata; CEL accounting is runtime-only.
No portable OpenAPI parity is claimed. The new service has two new operations:
`ArchiveMutation` admits one of the three existing command shapes;
`GetArchiveMutation` returns its durable cleanup status. Required response
wrappers carry the same validated receipt. Existing destructive RPC contracts
remain unchanged and their implementation cutover is still pending.

`ArchiveMutationCommand`, `ArchiveMutationLedger` and V19 now implement internal
command validation and atomic admission persistence. The operation key serializes
retries; both the hash and exact command bytes must match. A new admission locks
and checks the sampled entry revision, runs the logical SQL callback, validates
its cleanup targets against prior references and remaining live references, and
commits the initial receipt and immutable target set with the change. Reciprocal
database triggers prevent an admitted target from acquiring new references.
Authorization remains the caller's obligation, including before replay/lookup.
Callbacks must maintain logical counters and perform no provider I/O or nested
transactions. Admission is entry-scoped, not a general JCR session transaction.

Real PostgreSQL tests cover concurrent replay, principal isolation, conflicting
commands, absent-entry replay after recreation, rollback/retry, stale revisions,
invalid outcomes, target scope/liveness, immutable rows and repinning after
admission. The repinning regression failed before the V19 trigger fix. Nested
invalid and unknown command fields exercise the runtime validation boundary.
The admission ledger's internal lookup returns the **initial** receipt only.
`ArchiveDestructiveMutations` implements logical entry deletion, version pruning
and rendition redaction inside that transaction. It recomputes manifest hashes
and sizes, verifies exact per-version SQL pins and original byte bindings, and
updates counters with the logical changes. Redaction retains object identity and
reason as provenance while removing live references; both row and protobuf
manifest checksum/size fields change together. Unbound content requires explicit
storage identity migration; no current-drive inference is performed.

V20 and `ArchiveMutationObservations` maintain a separate monotonic status record.
One SQL snapshot reads all target states, and unchanged observations retain their
revision/time. Observation writes serialize per operation; late reconciliation
can move completed status back to reclaiming. Lookup never executes provider I/O
or repeats a logical mutation. `ArchiveMutationOperations` is shared by the Java
SPI and the public optional `ArchiveMutationGrpcService` adapter. The adapter
requires an explicit trusted caller context; process authority is required in
both paths until archive ownership policy integration is complete. Authentication
alone does not grant this authority.

Real PostgreSQL/S3 tests exercise all three commands through the shared library
and in-process gRPC, retained-object sharing, checksum/size tombstone updates,
rollback without byte loss, invalid stored bindings, cleanup outage/retry,
concurrent status lookup and late-write reconciliation. A PostgreSQL test verifies
cancellation during the logical callback rolls back admission. Runtime fixtures
validate missing and malformed successful response receipts.

`RepoServices` now mounts the mutation adapter and managed archive writes when
`ManagedStoragePolicy` qualifies the configured backend. The reader and recovery
resolver require the exact original generation/profile/realm. Unary, streaming
and bridge writes use durable admission. Managed library access and gRPC service
exposure start lifecycle workers first; listener startup failure closes the
composition, including workers and providers. A real Netty/PostgreSQL/S3 test
admits an authenticated mutation, closes the host, and verifies cleanup after
restart through in-process startup without a separate lifecycle call.

Ownership enforcement and legacy identity migration remain unfinished. The three
old destructive RPCs and response messages, Java SPI methods, object-first engine
bodies, and public ledger helpers are removed. Request payloads retain their
names, imports and tags for ArchiveMutationRequest. This is an intentional
pre-release API break, authorized while there are no external users. No stored
rows or objects are rewritten or deleted by this cutover. No deployment is claimed.
The unwaived FILE compatibility comparison against `dd788630` reports exactly
the three RPC and three response-message removals. `buf.yaml` temporarily waives
only those deletion rule families for this intentional pre-release cutover;
remove the waiver after it lands in the base branch. Descriptor tests preserve
the command payload tags and assert the old endpoints and responses are absent.

ArchiveDeletionFailureIT originally exposed swallowed provider failures and SQL
rollback restoring rows whose bytes had already been deleted. Its replacement
cases qualify the identified API through Java and authenticated gRPC using real
PostgreSQL and S3: rollback preserves rows, pins and bytes with no receipt; the
same operation can retry; cleanup failures remain RETRY_REQUIRED until confirmed
reclamation. ArchiveServiceIT uses managed composition for all archive operations
and checks logical targets separately from physical completion.

**Generic byte-mutation guard:** BlobRepository PUT, conditional PUT and DELETE
reject the exact opaque path segments `archive`, `documents` and `.protomolt-managed` before
provider mutation. The effective generated PUT key is checked too. The archive
reservation covers keys already stored under earlier layouts, including drive
aliases sharing the namespace; it performs no data rewrite or SQL lookup.
Raw-key detection remains separate for the raw admission machinery. Administrative
GET and authoritative reads are unchanged. Direct BlobStore SPI access is a trusted
provider port, outside this repository API guard.

Twelve real PostgreSQL/S3 regression cases first demonstrated successful forbidden
writes/deletes, then pass with PERMISSION_DENIED: PUT/CAS/DELETE, Java/gRPC,
versioned/unversioned storage, and a drive alias. Post-refusal checks preserve
current bytes, provider version, ETag and normal archive reads. Further cases
cover unbound historical namespace keys, generated keys under reserved drive
prefixes, raw-key regression, and ordinary loose-blob round trips. Reserving an
exact `archive` segment makes unrelated pre-release loose blobs under that segment
read-only through this API; similar names and encoded text are not interpreted.

The document orphan reconciler now excludes that same archive namespace before
orphan classification, even for an armed zero-age sweep. It has no authority to
infer archive ownership from document rows. A real regression keeps a published
object, a verified candidate and unbound historical bytes intact, then proves
archive recovery can still reclaim the expired candidate. Namespace predicates
live in the provider-free codec module and are shared by engine and container.
The document purge consumer also refuses persisted batches containing archive
or managed-raw keys before deleting any key; tests cover both legacy and
generation-bound queued commands with mixed document/reserved targets. Failure remains recorded
with an explicit repair diagnostic. Document configurations must use prefixes
outside the reserved archive namespace; older conflicting purge records require
explicit repair rather than bypassing the guard. The general reconciler also
explicitly excludes managed-raw keys; a real armed-sweep test still deletes an
ordinary orphan while leaving both reserved namespaces intact.

Document parts are now excluded from that general sweep as well. Real local and
gRPC regressions paused after PUT but before SQL publication; previously an armed
sweep deleted the part and the save still returned success. Both now preserve
the part and normal reads. Current and historical document namespace keys are
quarantined without rewriting them. Generic blob mutation tests also cover
explicit document keys and generated keys under a reserved drive prefix.
This is a temporary safety boundary: the internal cleanup described below only
covers admitted, never-published attempts. Legacy abandoned parts remain excluded.
The dedicated attempt ledger, publication/cleanup fence and required interleavings
are specified in `repository-composition.md` under
Document part publication and reclamation. Existing exact document purge remains
active; its other races are not declared resolved by the sweep quarantine.

The internal `DocumentPartAttemptLedger` now admits a complete ordered plan under
a fresh caller-minted attempt UUID, original registered provider generation and
physical namespace. V21 preserves existing document rows and objects unchanged.
Admission, token-fenced lease renewal and measured-byte verification are new
internal operations; they perform no provider I/O. Immutable plans include source
revisions, expected digests/sizes and chunk order. Database guards reject unsealed
commits, plan edits and premature verification. Bounded SHA-256 reservation
indexes retain the exact storage coordinates; a digest collision refuses admission.
`VERIFIED` records byte verification reported by a trusted writer, not schema or
semantic acceptance. Internal publication and recovery now consume this foundation,
and qualified hosts now schedule recovery for abandoned attempts. Public document
saves are not wired to it, and it is not an available managed-write API.
End-to-end writer integration remains required before public managed writes.

V22 adds immutable document publication history and an active attempt reference,
without adopting existing rows. The package-private `saveVerifiedAttempt` now
shares the existing document/source revision locks and commits its document row,
publication and callback in one transaction. It checks the sampled drive and
selected provider identity, exact verified manifest and next document version.
Deferred database guards reject changed bound bodies, surviving-row unbinding,
invalid physical part sets, false aggregate fields, unbound saves naming admitted
objects and reuse of retired
publication facts. Status-only changes retain the binding. Tests prove rollback
of the row, binding, history and outbox together, including failure after the
callback, plus concurrent writers against the same revision.

The coherence probe reports missing bound parts without changing their manifests.
Legacy purge refuses admitted part keys before provider deletion and records the
failure. That refusal conservatively matches physical namespace and key across
registered realms because legacy commands do not carry a qualified realm.
PostgreSQL/LocalStack fixtures admit before PUT, read and hash the resulting bytes,
then publish through the internal transaction to exercise these guards. Public
full/partial save, original-profile reads, raw-reference integration and managed
reclamation remain unwired. These internal checks do not qualify a managed
document API, authorization, crash recovery or end-to-end transport behavior.

V23 reserves document keys permanently before provider I/O. Both attempt admission
and the existing full/partial save paths acquire the same digest-indexed key
reservation, with exact-key comparison on collision. Full saves reserve every
PUT destination; partial saves reserve PUT and COPY destinations together before
either starts. Reservation batches use a common digest/key order. A legacy key
cannot become a managed attempt key after a failed save, row deletion or queued
purge; reservations are not cleanup authority. This deliberately retains rows
for abandoned legacy attempts. Global exact-key scope is conservative because
older manifests do not identify an immutable backend realm.

The migration locks writer tables, coalesces duplicate legacy document references
and reserves document keys in purge snapshots while leaving raw blob keys alone.
Existing managed publications retain their admitted owner. Unbound ownership
collisions abort migration instead of silently adopting data. Regression tests
cover existing legacy manifests, both reservation orders, simultaneous contenders,
reversed batches and migration without changing existing document rows. Real
library/gRPC tests assert a committed reservation exists before PUT and COPY.

Deployment must drain pre-V23 writers and their provider I/O before managed writes
are enabled. Database migration locks protect the reference scan and trigger
installation; they cannot cancel a PUT already started by an older binary.
The public managed writer/read/recovery integration is still not enabled.

The original-backend document read path is under qualification. Its SQL snapshot
locks the current document revision after authorization and loads the immutable
attempt, profile, namespace and ordered verified objects. A changed or deleted
revision conflicts; it cannot be mistaken for an unbound legacy row. Bound reads
require an explicit resolver and verify the recorded provider version, eTag,
size and SHA-256 before decoding. They never retry against the current drive.
Provider I/O starts after the SQL transaction releases its locks.

Current tests cover drive changes and stale/deleted revisions in PostgreSQL,
and recorded-version reads, missing versions, checksum mismatch, malformed
protobuf and unavailable resolution against versioned LocalStack storage.
An interruption regression reproduced blocking executor shutdown after a real
GET. Cancellation now cancels outstanding tasks and returns without waiting for
an interrupt-insensitive provider; borrowed clients remain host-owned. Such
provider calls still require finite host-configured timeouts to release resources.

Bound publication tests now exercise local and in-process gRPC reads over real
PostgreSQL and versioned storage after the current drive changes. The Java read
interface accepts a transport-neutral `RepositoryReadControl`; existing two-argument
callers use its no-deadline default. The gRPC adapter captures its cancellation and
monotonic deadline, and the managed reader polls completion within that budget.
Implementations of `DocumentRepository` must implement the new three-argument
read methods. No protobuf field, method or type URL changes are involved.
Legacy unbound reads check cancellation before and after their existing blocking
storage call; they do not yet provide prompt cancellation during that call.
The gRPC cancellation and deadline tests assert server-side worker interruption,
not merely client completion. Their fault gate is after a real provider GET;
they do not establish that an in-flight HTTP request is interruptible.

S3 GET failures now retain provider-neutral error codes and their original causes.
Real SDK fault-response tests distinguish confirmed missing objects from missing
namespaces, denied access, outages, refused connections, API deadlines and socket
timeouts. This normalization currently covers GET, not all provider operations.

One shared `DocumentPartReader` enforces a concurrent GET budget across its
requests (32 by default, configurable at construction). Saturation returns
`RESOURCE_EXHAUSTED` without a waiting queue. Cancelled workers retain their slots
until the provider actually returns. A request's submission window never exceeds
that shared limit or 32. Hosts must share the reader across requests using the
same resource budget; constructing a reader per RPC defeats that bound. The S3
factory now configures finite whole-call, attempt, connection and socket timeouts;
hosts supplying external clients must configure their own finite timeouts.

Tests exercise cancelled calls retaining capacity, recovery after provider return,
ordered multipart selection with delayed fragments, and bound-read errors through
both the library and gRPC. This qualifies the internal read path, not a completed
managed document feature. Public managed write admission, host composition and
document attempt reclamation remain unfinished. Do not enable managed document
writes until those publication and recovery paths are implemented and qualified.

### Managed document writer and recovery sequence

The legacy save path must reject bound destinations and bound copy sources after
authorization, before dedupe bookkeeping or provider I/O. The existing SQL body
guard prevents unbound replacement at commit, but does not prevent an earlier
legacy PUT/COPY. This is an explicit transitional failure, not permission to read
from the current drive or silently downgrade a publication. The managed writer
will replace this rejection when qualified.
The preflight query is not a lock held across I/O. A publication racing after the
check can still leave an abandoned legacy object; fresh keys and final SQL
revision/publication guards prevent it from replacing the visible bound body.

The next implementation pieces reuse the current attempt, publication and receipt
models; they do not require a replacement document RPC:

1. An internal stager owns admission of a complete ordered plan and immutable
   payload snapshots. If admission is separated later, it must reload the sealed
   plan: the current `Attempt` value exposes only its count and location, not
   enough information to authorize arbitrary supplied keys. Record each source
   revision and the exact original backend/version for carried fragments. A
   current-drive COPY without a source version is insufficient for bound content.
2. Resolve and qualify the selected backend before I/O. Write fresh attempt keys,
   then GET the returned provider version and verify measured size, SHA-256 and
   provider identity before marking each part verified. An ambiguous PUT failure
   retains the admitted attempt; never infer absence or retry by overwriting that
   key. Byte verification remains separate from typed/schema admission.
3. Keep lease ownership valid throughout provider calls using renewal during I/O
   or a validated lease budget greater than the bounded operation duration plus
   margin. Renewal only before a long call is insufficient. Check the token after
   every call; expired ownership cannot be reacquired for publication. Cancellation
   leaves durable attempted-object records for cleanup rather than deleting inline.
4. Compose the internal recovery worker with a persisted deletion claim fenced
   against publication on the same attempt lock. Resolve original profiles,
   require no publication history,
   reclaim only exact admitted keys, retain failures and confirm physical absence.
   Permanent identity reservations and tombstones must support repeated checks for
   a late write after lease expiry. Test cleanup racing publication, lost provider
   acknowledgements, restart and late I/O before enabling public writes.
5. Publish through `saveVerifiedAttempt` with destination/source revision locks,
   target generation checks, raw-reference bindings and the outbox in one SQL
   transaction. Then qualify full and partial saves locally and over gRPC, including
   policy/drive changes, carried fragments, cancellation and commit ambiguity.
   Existing intake deduplication is not general idempotency-key replay.

The internal stager may be tested before recovery, but must remain unwired to
public writes while abandoned attempts cannot be recovered. Host composition is
the final enablement step. These locks and object lifecycle records are repository
foundation work; they do not establish JCR transient sessions, atomic multi-object
commits or workspace/version semantics. Keep that optional extension separate.

`DocumentPartStager` now implements the internal staging step in the container
module. It is package-private and has no public service caller. It copies and
checks the ordered payloads before admitting the complete plan, then performs
PUT and exact-version read-back verification outside SQL transactions. Verification
checks bytes, content type and provider identity. A shared renewal scheduler keeps
leases live during I/O; lease failure prevents further verification. Failures retain
the attempt ID and original cause, without retrying ambiguous PUTs or deleting bytes.

The stager limits active stages to 32 and copied payload bytes to a shared 256 MiB
default budget, configurable at construction. This does not change the byte SPI's
conditional-write bound. Closing prevents new stage registration and stops renewal;
an already registered stage can still leave an admitted attempt during shutdown.
Hosts must call `awaitIdle` and retain both borrowed provider and database resources
if it reports busy. The host must construct the handle and retained backend identity
from the same selected provider; comparing a supplied identity cannot prove which
physical service an arbitrary borrowed client reaches.

Real PostgreSQL and versioned-storage tests cover admission before PUT, exact-version
verification, partial progress after a lost acknowledgement, corrupt reads, caller
payload mutation, lease expiry, slow-call renewal, budget contention and shutdown.
These staging tests do not establish typed validation, copy-source authorization
or public write idempotency. The stager does not publish documents.

V24 adds internal abandoned-attempt recovery through `DocumentAttemptCleanupLedger`
and `DocumentAttemptRecovery`. A claim locks the attempt before checking lease
expiry and publication history. Publication takes the same lock and refuses any
cleanup tombstone. Every published attempt remains retained, even after deletion
of the current document; historical retention and reclamation are separate work.
Cleanup renews a token-fenced lease before each provider call and records a result
only while that claim remains current. An expired claim cannot report success.

The worker resolves the persisted backend generation and original profile, then
reclaims each exact admitted key, including unverified PUT outcomes. It uses the
provider's physical reclamation capability to remove versions and delete markers.
False results and exceptions remain retryable; original exceptions are returned,
while stored diagnostics omit provider messages. `ABSENT` records an observation,
not a permanent guarantee: immutable tombstones remain eligible for repeated
checks to remove late writes. Candidate filtering precedes the bounded scan limit.

PostgreSQL tests cover publication/cleanup contention, competing claims, expired
owners, direct SQL guards, and migration preservation. Versioned-storage tests
exercise physical deletion, unrelated-key preservation, late writes, unavailable
backends, unconfirmed deletion and lost acknowledgements. A client-reopen test
closes the database pool/entity manager and provider client after partial cleanup,
then opens fresh clients and resumes the expired claim from durable state. This
alone does not establish recovery after a process or host crash. Separate forked
JVM tests halt without shutdown hooks after a real first PUT returns (before its
verification record), and after a real first deletion (before recording cleanup
completion). The parent verifies the deliberate exit code, durable attempt state
and partial bytes, then recovers through fresh database/provider clients. These
tests qualify those two process-loss points; they do not simulate storage-server
failure, machine power loss, or a crash around SQL publication. A separate host test
closes and rebuilds `RepoServices`, enters through `repository()`, and verifies
scheduled cleanup against real SQL and storage. Publication crash qualification
and public writer integration remain outstanding. No managed document write endpoint
is enabled by this checkpoint.

`DocumentAttemptRecoveryService` provides bounded passes for a configured backend
generation. Candidate selection filters that generation before the batch limit;
unconfigured historical generations remain untouched. Construction checks the
retained profile and backing capabilities. The host must derive the supplied
identity and reclaimer from the same opened provider: the borrowed handle does
not itself expose a verifiable backend identity. Each result preserves its
original failure, if any. Shutdown interruption stops subsequent work.

Qualified `RepoServices` composition starts a separate document recovery loop
through library or transport access. It uses the selected backing handle and,
when configured, its cache-aware reclaimer. Each pass admits at most 100 attempts
with a ten-minute cleanup lease. First cleanup is eligible after writer expiry;
repeat checks wait one hour. Passes run on the configured sweep interval. Retry
and lost-claim outcomes are logged, including original failures. There is no
current-drive fallback and no adoption or deletion of legacy untracked parts.

`DocumentPartReader.readFragments` now provides carried partial-save fragments as
exact verified bytes from their original binding. It detaches provider buffers
before measuring size/hash and returns the selected slots in publication order,
using the same bounded fetches and cancellation checks as typed reads. Tests retain
noncanonical valid protobuf wire bytes that parsing and serialization normalize,
read the recorded version after a latest-version overwrite, and prove provider
buffer mutation cannot change returned fragments. This is storage-integrity
verification, not protobuf/schema admission or source authorization.

Before public writer integration, the engine must use these exact fragments;
assembling and splitting a message again is not proof of byte preservation.
Legacy carried fragments still need their own verified source path. The container writer
must stage the complete plan and use `saveVerifiedAttempt`, retaining the engine's
destination/source authorization, sampled revisions, raw-reference publication and
outbox in the guarded transaction. A same-body managed deduplication should retain
its current publication pin. A transport-neutral write control must check
cancellation in the eventual managed publication facade before its SQL commit.
Cancellation or a lost acknowledgement after commit is attempted is
ambiguous; do not create another attempt automatically. Host shutdown must also
wait for actual staging completion before releasing the borrowed database/provider.

`RepositoryOperationControl` now supplies cancellation and monotonic deadlines to
the existing document-save API; `RepositoryReadControl` extends it without changing
the read signatures. Two-argument Java saves remain available and delegate with
`NONE`; repository implementations must implement the controlled three-argument
method. Protobuf services and messages are unchanged. gRPC supplies the request's
captured context/deadline. Legacy saves check around provider calls, inside the
deduplication transaction, and inside the final row/raw-reference/outbox transaction.
No cancellation check runs after that transaction returns. Tests cover refusal
before PUT, cancellation/deadline after real PUT, and gRPC cancellation with the
server handler completing before the assertion that no row was published.
The gRPC case uses the host's virtual-thread executor model. A single-thread
embedding executor blocked in synchronous storage can delay cancellation dispatch
itself; hosts must leave executor capacity for transport cancellation callbacks.

The internal managed stager accepts a nonblocking cancellation check before
admission and around provider/verification boundaries. A cancellation after PUT
retains the admitted attempt and original cause for recovery. The future public
writer must preserve the cancellation/deadline code when unwrapping `StageFailure`.
These controls do not abort synchronous provider calls already in progress, and
legacy fan-out still waits for its outstanding calls. Explicit library controls
do not change a remote provider's RPC timeout. HTTP raw ingestion has not acquired
an HTTP cancellation signal in this change.

The internal `saveVerifiedAttempt` overload now checks cancellation before
transaction entry, after document/source locks, after drive/attempt validation,
and after its transactional callback. PostgreSQL tests observe an actual lock
wait before cancellation and prove no publication occurs after the lock releases.
Another test flushes the row/publication/outbox work, then cancels before commit
and proves all of it rolls back while the admitted attempt remains recoverable.
Cancellation during commit and lost commit acknowledgements still need dedicated
managed-writer tests; these checks deliberately do not run after commit returns.

The planned writer facade must validate the plan's account, node, namespace and
key prefix against its sampled drive and authorized candidate before admission
or PUT. Final publication guards alone are too late to prevent writes to the
wrong location. Its active-operation count must cover stage, candidate assembly
and publication, so shutdown cannot close resources in the gap between them.

Repository host shutdown interrupts lifecycle workers and gives them a shared
ten-second join budget. A timeout or interrupted join leaves providers, the ledger,
transports and worker handles retained, reports failure, and keeps the composition
closed to new access. Call `close()` again after workers stop to release resources.
This prevents a slow recovery call from using resources already closed by the host;
it does not guarantee that an arbitrary provider responds promptly to interruption.
After workers stop, the host stops every HTTP/gRPC transport before releasing
shared dependencies. Transport failure preserves their handles for another close
attempt. Both transports await their handler executors; shutdown requests alone
do not count as termination. Each executor gets ten seconds for graceful shutdown
and ten more after interruption. gRPC also awaits server termination. These are
per-transport waits, not a ten-second bound on the entire host shutdown.
The host retains transport handles before startup, so a failed bind or routing
check cannot discard cleanup ownership. HTTP startup failure also closes the host
composition, including lifecycle workers. Blocked-handler tests use real HTTP
and in-process gRPC transports and verify failure, resource retention and retry.

Keep archive operations process-authorized until current-policy guards support
scoped callers. Legacy write/staging cleanup remains separate follow-up work;
removing destructive RPCs does not qualify those older write paths.

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

Revision checks alone did not make object-first deletion safe. The identified
mutation path now removes that ordering and fences later reference publication.
Direct SQL that locks versions before entries can also deadlock with an engine
save; PostgreSQL aborts a participant. Consistent application lock ordering and
explicit retry/error coverage remain part of destructive admission.

#### Reviewed deletion implementation boundary (not available yet)

Physical key separation is implemented as a prerequisite: newly written rendition
objects receive unique write UUIDs, with the content hash retained in the key and
manifest. Unary saves and bridge output reuse a matching current rendition's
physical reference. Streamed dedupe returns the retained key and deletes only its
unused unique candidate; cleanup errors propagate. Delete/recreate at the same
address gets different keys in library and gRPC integration tests. Existing
manifest keys remain readable. Failed candidate registration/recovery, immutable
backend bindings and durable deletion below are still unfinished.

V14 and `ArchiveObjectLedger` provide the binding reservation foundation used by
managed archive uploads and reads. A binding records entry/account/
archive and original backend generation, storage realm, bucket and object key.
The profile supplies the realm; a composite foreign key prevents mismatching it.
Realm/bucket/key uniqueness prevents generation rotation from assigning the same
physical object twice. Bindings are immutable even through direct SQL and survive
entry deletion. Migration deliberately leaves legacy objects unbound. A stable
realm must identify one physical storage namespace across profile rotations.
Future version references must carry the binding's object ID; entry/key alone
cannot identify a binding across different storage realms. The engine must check
entry/account/archive and selected backend before registration; a binding ID is
not an authorization grant.

Managed composition reserves before PUT, fences upload leases, verifies consumed
bytes, binds version references transactionally, resolves original bindings for
reads and cleanup, and schedules abandoned-candidate recovery. The reused managed
profile implementation currently qualifies only S3; other providers need explicit
qualification without falling back to a current drive or S3 client.

V15 and `ArchiveUploadLedger` add internal STAGING/VERIFIED admission. Begin commits
the immutable binding and upload lease atomically. Renewal and verification lock
the upload row before sampling database wall-clock time, reject expired/wrong
tokens, require the admitted byte length, and preserve the first verified hash,
provider version and ETag. Identical verification replay is accepted only while
the lease remains live. SQL constraints/triggers also protect those identities.
Tests cover rollback of both reservation and admission, stale/wrong attempts,
length mismatch, verification replay and direct SQL mutation. Bare V14 bindings
are not promoted to upload admissions. Managed archive writers use this ledger; publication references and cleanup
claims are described below. Legacy writers remain unqualified.

**Extended manifest contract:** `RenditionManifestEntry.storage_object_id` is an
additive UUID string at tag 10. Existing field tags, imports, package and Any URL
are unchanged. Legacy absence remains valid and means unknown/unmanaged, never
an inferred current-drive binding. Runtime validate.v1 checks UUID syntax and a
CEL rule allowing an ID only for PRESENT content or DELETED provenance; EMPTY
and unspecified states cannot carry it. A tombstone's ID is provenance, not a
live byte reference. JSON Schema emits a UUID-format string and CEL extension;
the CEL extension still requires runtime execution. Standard JSON Schema format
handling also does not establish validate.v1 `ignore_if_zero` parity for an
explicit empty string. Generator parity is not claimed or changed here.

No writer populates this field yet. Publication must validate the complete
manifest, compare bound entry/account/archive/key and verified hash/size under
locks, require the active attempt token for first publication, and commit object
references with the version. Reusing already published content requires retained
reference ownership, not an indefinitely live upload lease. A byte-dedupe result
keeps the committed manifest's object ID/key and leaves the unused candidate for
cleanup. Normal reads must resolve the ID's original backend binding. Full proto
imports, lint, FILE compatibility and generated/dynamic runtime fixtures cover
the contract addition; they do not establish engine integration.

V16 adds transactional archive version/object references. `commitSave` validates
bound manifests using the runtime engine, checks binding scope/key and verified
hash/size, locks upload identities in UUID order, and requires a live token for
first publication. It changes VERIFIED to LIVE and inserts references in the
version transaction, before dropping a superseded version. Later versions reuse
LIVE objects only while retained references exist. A managed key cannot silently
lose its binding ID. Reference insertion failure rolls back the entry, version,
references and upload transition together. This is archive byte publication,
not typed payload admission or a universal content/JCR transaction boundary.

The old destructive methods and their SQL helpers have been removed. Use the
identified mutation API. Managed upload/read routing and original-profile recovery
are wired by qualified host composition. Carry-forward helpers preserve binding
identity. The command payloads and their field tags remain unchanged. Existing
unbound PRESENT content fails closed until verified binding migration; refusal
for all three commands is tested through Java and gRPC without changing stored
rows or bytes.

- **Reused command payloads:** DeleteEntry has only address (tag 1);
  DeleteRendition has address/rendition/reason (tags 1–3); PruneVersions has
  address/keep_latest (tags 1–2). The enclosing ArchiveMutationRequest requires
  operation identity. Normalize the
  command and bind its fingerprint to trusted caller, account and operation ID.
  Reusing that identity with a different command must conflict. An address cannot
  be the replay key because deletion and recreation can reuse it.
- **New receipt and lookup:** persist logical version/tombstone counts and the
  admission receipt, so an identified retry returns the original logical outcome
  after a lost acknowledgement, with a current durable cleanup observation.
  Retire calls without caller-supplied operation identity. Explicit lookup serves
  callers whose request ended before a response arrived.
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
- **Archive object generation/reference fence:** legacy deterministic rendition
  keys could be reused by a later write. A pre-delete reference
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

### Bound archive reads (internal composition)

V18 adds durable archive cleanup claims with fenced completion, bounded candidate
selection and retained tombstones. Publication and cleanup serialize on the same
upload row; SQL also refuses non-LIVE references and reclamation of referenced
objects. Unexpired uploads and retained version references are ineligible. Stale
DELETING work can be reclaimed by another worker; DELETED objects remain eligible
for periodic reconciliation because a late provider PUT can recreate bytes.
ArchiveObjectRecovery uses the original persisted backend profile and physical
reclaimer outside SQL transactions. Failures remain recorded and propagate.
Qualified host composition schedules recovery in two independent bounded loops.
Admitted mutation targets use the purge interval for retry; in-flight claims
without an error have a separate one-hour abandonment timeout. Failed claims
rotate by updated time so later targets can progress. The aged orphan/tombstone
loop uses reconcileMinAgeMs and the sweep interval, retaining capacity even while
mutations arrive. Workers stop before owned provider/database resources close.

Archive references are the complete pin set only for current archive objects.
Future JCR graph/frozen-version/restore references must join the liveness decision
or own separate physical objects. These entry-scoped commits do not implement a
general JCR session transaction.

Unary PutEntry has a managed composition using ArchiveObjectWriter.
Fresh candidates receive a durable reservation before checksummed provider I/O;
successful writes record byte identity and provider revision. The owning version
publishes object references and lease tokens in the same SQL transaction. Identical
content reuses its retained binding without another upload. Known revision
conflicts may retry with fresh reservations; arbitrary persistence failures and
lost provider acknowledgements propagate, leaving recorded candidates for recovery.
Managed mode does not physically delete superseded objects after publication.
Streaming writes reserve a final immutable key before consuming input. They
measure exact length and digest, renew the lease during consumption and verify
before publication, without staging-copy or whole-body buffering. Invalid and
deduplicated candidates stay recorded for recovery. Bridge writes admit generated
bytes and preserve the sampled source revision. Streaming dedupe and unchanged
bridge results confirm the retained version under an entry lock before returning;
real PostgreSQL races fail without those guards and pass with them. Legacy writers
still reject bound versions. Coverage uses real PostgreSQL/S3, library/in-process
gRPC, provider acknowledgement and SQL failure injection. This verifies byte
admission, not schema validity or semantic review.

Backend identity persistence is extended without protobuf changes. V17 preserves
existing generations, realms, profile values and foreign keys; legacy S3 rows
decode to the same canonical s3/v1 identity as new provider-produced descriptors.
New generations persist a provider/schema/location descriptor with no required
S3 columns. Generation immutability and conflicting registration checks remain.
BlobStoreProvider.managedIdentity is additive and defaults to unsupported. S3
implements it without opening a client and excludes credential options; the host
must still check retention capabilities. No other provider gains managed recovery
support merely because its identity can be persisted.

Java callers of ManagedBackendLedger.Profile should now use
Profile(BackendIdentity, storageRealm). The former S3 constructor and S3-specific
record accessors are removed; use identity().location(). Provider modules own canonicalization and validation;
the generic value is a trusted provider descriptor, not a credential sanitizer
for arbitrary user maps. Production composition uses the S3 provider factory.

GetEntry, ClassifyEntry and BridgeEntry now share an optional ArchiveObjectReader
for renditions with storage_object_id. It requires an exact retained version
reference to a LIVE upload, checks manifest scope/key/size/checksum, and resolves
the immutable backend generation and realm. Reads use the recorded provider
revision and verify returned bytes. Missing published bytes report DATA_LOSS;
missing resolver configuration fails explicitly. The host owns provider clients
and historical configuration lookup. Legacy renditions still use the current
drive because their original backend identity is unknown; this is not a verified
migration. Qualified host composition enables bound admission and the resolver;
legacy migration remains explicit. This is locally tested behavior, not evidence
of a live deployment.

The PostgreSQL lifecycle test covers unpublished, wrong-entry/wrong-version,
carried and removed references. The service regression uses real PostgreSQL and
versioned S3, with local and in-process gRPC reads, changed current drive settings,
provider overwrites, unavailable resolution, and deletion of the recorded revision.

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
