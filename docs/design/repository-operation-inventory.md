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
  Explicit library composition can route full and partial saves through the
  managed attempt writer with same-transaction raw references/outbox and existing
  full-save locked dedupe. Partial saves capture legacy or managed sources and
  preserve unchanged provenance and chunk-set order. Existing production host
  wiring remains unchanged pending broader partial-save, performance and lifecycle
  qualification.
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

- **Extended, engine error mapping:** managed writer failures retain recognized
  domain/provider codes through their wrapper, with attempt ID and phase in the
  error message and the original cause for local callers. Unclassified failures
  are UNKNOWN and require outcome reconciliation; no automatic retry is implied.
  Verified-read identity/checksum mismatch is DATA_LOSS. This adds no wire fields
  or retry RPC and does not enable the managed save route.
- **New, Java byte SPI:** `PayloadBudget.reserve` returns an idempotent closeable
  lease or throws explicit capacity exhaustion without waiting. The document
  writer accepts a shared budget for active staging inputs and verification
  results. Readers accept the same budget. Default constructors use private budgets;
  host-wide composition remains pending. No protobuf change.
- **Changed Java return type:** `DocumentPartReader.readFragments` and
  `readLegacyFragments` return `DocumentReadBatch`. Use `parts()` inside
  try-with-resources, keeping the batch open through reuse. Typed `read` closes its
  batch after assembly. Cancellation retains its reservation until entered provider
  workers exit. Existing protobuf names and responses remain unchanged.
- **New, Java lifecycle:** `DocumentPartReader.close` rejects new work;
  `awaitIdle` waits for entered resolver/read operations and actual provider
  workers. Timeout retains borrowed resources. Returned batches have independent
  caller-owned lifetimes and must still be closed.
- **New, Java source composition:** `DocumentPartReader.readSource` reads from a
  captured `DocumentSourceSnapshot`. Managed sources resolve their retained backend;
  legacy sources use the captured namespace and supplied qualified store. The
  returned batch remains open through writer publication. This helper does not
  authorize access or enable the public managed-save route.
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

## Reviewed redesign status

- **New, planned internal foundation:** immutable physical-object identities,
  retained references, reader lifetime and acquisition/reclamation coordination.
  No generic object catalog or commit API is advertised as implemented.
- **New, planned commit primitive:** bounded multi-object atomic changes with
  expected revisions, current authorization, reference updates, outbox and durable
  operation result. Existing archive admission/receipt behavior informs this port;
  its entry-specific contract is not a general session transaction.
- **Extended, planned document save:** changed-part uploads with immutable reuse,
  batched verification persistence and an operation heartbeat. The current copying
  path fails the intended efficiency requirements despite passing scoped tests.
- **Extended, planned byte provider contract:** opt-in verified upload evidence.
  S3 sends a checksum today but PutResult does not expose validated checksum proof.
  Ordinary providers retain an explicit verification policy; no silent downgrade.
- **Extended, current delete guard:** managed current/history publications cause
  FAILED_PRECONDITION before synchronous or queued purge admission. This preserves
  the source but does not implement managed deletion. Legacy deletion remains.
- **Unchanged wire contracts:** this redesign has not changed protobuf names,
  tags, imports or Any URLs. New transport fields require separate inventory and
  validation review. Progressive hydration remains gated on the full foundations.

Correctness includes latency, throughput and resource use. See the design's
operation-count targets and diagnostic evidence; no production speed guarantee
has been established. The V25 reference-table experiment is set aside and does
not constitute the generic retention foundation.

### Archive mechanisms available for reuse

The generic foundation should extract the archive mechanisms and their tests,
without exposing entry-specific ledger types as a universal repository contract:

- V14/ArchiveObjectLedger: immutable object UUID and original backend generation,
  realm, namespace/key identity, with unique physical coordinates.
- V15/ArchiveUploadLedger: durable pre-upload admission, database-clock lease
  fencing and immutable verified byte/provider identity.
- V16/ArchiveVersionBindings: retained-version references acquired before dropping
  superseded references during publication.
- V18/ArchiveCleanupLedger: reference acquisition and cleanup share an object
  lock; durable claims and tombstone reconciliation handle failed or late writes.
- ArchiveMutationLedger: scoped command fingerprint, immutable logical outcome
  and separate durable physical-cleanup observation.

Current archive identities require entry/account/archive scope and reference
triggers restrict ownership to that entry. Generic ownership must preserve those
checks through a domain adapter. Do not create independent catalogs that can each
reclaim the same physical coordinates: migrate or alias existing object IDs to a
shared object lock, or extend the existing cleanup authority to include all owners.
Migration needs exact evidence and race tests; duplicated liveness decisions are
not a safe transitional state.

At `3cdd28e4`, ArchiveObjectReader uses bounded GET with retained size and durable
reader pins. V28-V33 add lifetime protection, incarnation fencing, verified local
shutdown and bounded recovery traversal. Unproven crashed incarnations remain
protected; legacy unbound reads and aggregate response memory still need work.
ArchiveObjectWriter's checksum request does not
supply the proposed verified-write receipt. Preserve tested lifecycle behavior
while replacing these limits through the shared foundation.

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
Its subject alternatives currently cover workflow runs and delegation tasks,
not repository commits. An optional repository receipt projection requires an
explicitly reviewed subject extension; do not relabel a commit as a workflow.
Review delegation's existing candidate/attempt/contract/evidence bindings before
introducing another semantic-review identity.

Archive content-root equality currently elides saves. It is not evidence of
metadata/schema/policy-aware dedupe. Expected-version checks prevent stale updates
but do not recover a lost acknowledgement. These distinctions need acceptance
tests before extending retry contracts.

## Shared commit foundation: source inventory at 3cdd28e4

This is the implementation inventory for composition-design slice 2, not an
available multi-object API. Sol independently reviewed the existing contracts.
No new protobuf fields are introduced by this inventory.

### Existing boundaries and exact reuse

- [DocumentLedger](../../repo/container/src/main/java/ai/protomolt/proto/repo/container/ledger/DocumentLedger.java)
  `saveGuarded` locks source revisions and one destination, including a missing
  destination using sorted advisory-lock keys, then merges and runs its callback.
  `saveVerifiedAttempt` specifically adds the attempt-token, selected
  drive/backend, source-snapshot and next-manifest-version checks. Its history,
  current publication, native/common retention references and
  the caller's outbox callback participate in that one SQL transaction.
  `DocumentAtomicPublicationIT` already exercises rollback and reference guards.
  Preserve these checks; repeated calls to this method are separate commits.
- [DocumentPartPublication](../../repo/container/src/main/java/ai/protomolt/proto/repo/container/ledger/DocumentPartPublication.java)
  `validate` checks complete ordered PRESENT slots,
  counts, size, root checksum and CORE provider identity. It currently requires
  the manifest to match one verified attempt's objects. Immutable reuse needs
  an ordered revision-reference relation; simply allowing an old object key in
  the manifest would bypass the existing proof.
- [DocumentPublicationLedger](../../repo/container/src/main/java/ai/protomolt/proto/repo/container/ledger/DocumentPublicationLedger.java)
  `findForRead` rechecks the sampled document revision
  and returns original physical bindings. It is not a consistent read of several
  independently sampled current pointers. A multi-object commit needs a matching
  read-snapshot contract before claiming atomic visibility to callers.
- [ArchiveMutationCommand](../../repo/container/src/main/java/ai/protomolt/proto/repo/container/archive/ArchiveMutationCommand.java)
  supplies deterministic semantic command encoding;
  `ArchiveMutationLedger` scopes operation UUIDs to account and trusted principal,
  compares both canonical bytes and SHA-256 on replay, then commits logical
  mutation, immutable receipt and cleanup targets together. Its callback is
  entry-scoped. Reuse the identity/replay rules, not its deletion-specific kinds,
  counters or receipt states for a publication transaction.
- `JdbcEventOutbox.enqueue(EntityManager, DocumentEventRecord)` participates in
  the caller's transaction. `EventRelay` delivery is separate. Keep persistence
  atomic with publication while excluding Kafka delivery, provider I/O, model
  calls and schema-registry network access from the commit transaction.

### Wire and identity gaps that must remain explicit

- **Unchanged:** `SaveDocumentRequest` selectors, copy source and partial-save
  semantics; it currently has no operation UUID or expected revision. Its
  response has no recoverable commit receipt. Deterministic document identity
  and content dedupe are not substitutes for a lost-acknowledgement protocol.
- **Unchanged:** archive `PutEntryRequest.expected_version`, where zero retains
  its existing last-write-wins meaning. It has no operation UUID. Do not silently
  reinterpret zero as must-create or introduce stronger retry semantics under
  that field. Omitted renditions already reuse archive references within their
  existing entry/version rules.
- **Unchanged:** document manifest version, archive version, mutation revision,
  upload attempt/token, physical object UUID and reader incarnation. Each has a
  distinct lifetime. A generic commit ID must not replace any of them.
- **Unchanged:** archive rendition `schema_subject` is recorded but unenforced;
  it does not establish immutable schema admission. Mesh `SchemaReference`
  supplies type plus canonical descriptor-closure digest. Registry Java
  `SchemaReference` instead describes an imported file's subject/version. The
  new boundary must distinguish acquisition, immutable binding and import lookup.
- **New, pending review:** an internal unsigned multi-object logical outcome and
  authorized lookup by scoped operation identity. Neither ArchiveMutationReceipt
  nor WorkRecord can represent it unchanged. Any later protobuf envelope must
  be additive and compile with complete imports; preserve existing Any URLs.

### Adapter boundary and first acceptance unit

The minimal port belongs in `repo/spi`, which currently depends on `repo/proto`
and rejects SQL, Kafka and provider SDKs in its runtime gate. Immutable input
values describe a bounded change set, operation identity, expected revisions and
verified physical-reference/evidence identities. They must not expose Hibernate
entities, EntityManager, SQL callbacks, provider clients or executable closures.
Construction validates shape; the adapter still checks current authorization,
state, physical admission and evidence bindings inside the commit boundary.

The PostgreSQL implementation may initially reside in `repo/container` while
that existing module still owns the domain tables. This does not make the
adapter a minimal consumer: the module exposes Hibernate and includes Kafka.
Extraction needs published-POM/runtime gates before claiming dependency isolation.
Domain-specific transaction handlers belong behind that adapter, not in the port.
Use the existing native document/archive tables and retention authority; do not
create an unrelated generic shadow store that can disagree with those rows.

The first adapter acceptance unit is two verified document attempts, their two
destinations and a retained source revision, using existing publication validation.
The internal `DocumentPublicationBatch` now supplies one outer transaction;
`saveVerifiedAttempt` delegates a single member to it. Calling the single-member
method twice still opens two transactions. The batch acquires the union of
destination and source locks in global order, then publishes both. Shared immutable part
references remain a separate integration after this atomic foundation.
One short transaction must verify
all expected revisions and current policies, publish both revisions and their
references, persist their outbox records and the scoped logical outcome, or roll
back every change. Upload/validation work completes before that transaction.
Missing-row creation uses the same advisory-lock protocol as current writers;
lock-order interoperability must be tested with existing single-object paths.

The internal document batch is an initial adapter primitive, not the completed
commit port. It preserves existing protobuf contracts and SQL tables, validates
all source snapshots and expected revisions against the pre-change state, locks
selected drives in order, and validates every attempt before merging any row.
A source may also be a destination: its original retained history remains, while
its new current revision and the other destination publish together. This uses
already staged bytes; it does not introduce shared immutable part reuse.

Batches contain 1–64 distinct destinations and attempts. Multi-document batches
are additionally limited to 10,000 combined new/prior manifest parts and 10,000
source checks; existing single-document limits remain unchanged. These are
conservative admission ceilings, not latency-qualified production defaults.
Callbacks are internal transaction participants restricted to each member's
bindings and outbox; unrelated document/attempt locks and provider I/O are forbidden.
Existing trigger lock ordering is sufficient while physical objects remain
attempt-owned and overlapping destinations serialize before attempt locks.
Reassess that ordering when introducing cross-document physical reuse.

PostgreSQL fixtures cover two publications with a retained source and outbox,
rollback after both merges, source/destination overlap, stale source/destination,
changed drive, previously claimed cleanup, duplicate/oversized batches, and
opposing overlapping batches with an observed database wait. An independent
single publication completes while the overlapping batch remains blocked, in
both first-batch commit and rollback cases. This is concurrency evidence, not a
latency benchmark. Additional fixtures exercise missing-row batch/single creation
with observed database waits and both batch commit/rollback, plus cleanup waiting
on a batch that has published both members before its lease expires. Committed
history prevents cleanup; rollback lets the expired attempt be claimed. The
locked-prior aggregate-limit fixture accepts exactly 10,000 combined entries and
rejects 10,001 using legacy EMPTY-part manifests (no stored bytes are claimed).
Cleanup-first concurrent interleavings, policy races, durable outcome/idempotent replay, consistent multi-object reads,
and public-port/provider-neutral conformance remain outstanding.

Local validation on 2026-10-03: `:protomolt-repo-container:test --tests '*Document*'
 :protomolt-repo-service:test --tests '*Document*'` completed in 1m08s. JUnit XML
reports 200 container cases and 87 service cases: 285 passed, zero failures/errors,
and two opt-in benchmarks skipped. Sol reviewed the source/trigger lock ordering
and the implementation. This is local evidence; no CI, push, merge, deployment,
provider throughput qualification, or JCR compliance is implied.

The [durable command/replay design](repository-composition.md#durable-commit-command-and-replay-design)
now specifies command identity separately from staging evidence, durable admission
before provider I/O, immutable unsigned outcomes, and fenced terminal rejection.
This is reviewed design, not an implemented operation or a new advertised RPC.
Lease validity is checked at publication admission/history insertion/pointer
switch. A transaction already past those checks can finish after wall-clock expiry
while holding its owner fence; cleanup/takeover waits and observes the outcome.

**Extended internal behavior:** ordinary `DocumentLedger.save` now flushes and
refreshes its returned row inside the existing transaction. Previously it returned
zero/stale `mutationRevision` despite the database trigger assigning a new revision,
which could reject a subsequent guarded write. A focused PostgreSQL test failed
on revision zero before the fix and checks insert, update and follow-up guarded
save. This adds one same-transaction SELECT; guarded publication already refreshed
its result. It does not add provider I/O or a second transaction.

Follow-up validation on 2026-10-03 ran the same document-focused container/service
command after the revision-return fix and added race/boundary fixtures. It completed
in 1m16s: JUnit XML reports 207 container and 87 service cases, 292 passed,
zero failures/errors, and two opt-in benchmarks skipped. Sol reviewed the replay
design and the revision-return fix. That checkpoint had no replay API or SQL
operation ledger. The V34 slice below adds admission storage; terminal outcomes
and the executable replay API remain unimplemented.

### Internal operation admission storage (V34)

**New, internal only:** `RepositoryOperationLedger` reserves one encoded command
under `(account, trusted principal, operation UUID)`, observes it, renews its owner,
and performs generation-fenced takeover after expiry. It has no production caller,
public SPI method, RPC or terminal-outcome operation. Its package-private encoded
byte input is storage plumbing, not an executable command
API. Existing document/archive operations and protobuf names/tags remain unchanged.

V34 separates immutable `repository_operations` command rows from narrow mutable
`repository_operation_owners` rows. Foreign-key and deferred required-owner guards
make their first admission atomic; neither identity can be deleted or rewritten.
Owner renewal/takeover queries and triggers do not load, compare or hash command
bytes. One command is bounded to 1 MiB, account/principal to 200 characters, and
leases to one second through one day at the Java boundary. SQL also caps leases
at one day. Generation overflow fails rather than wrapping.

Admission compares codec, version, exact immutable bytes and their SHA-256 after
the conflict insert, using a separate locked read under READ COMMITTED. The stored
digest verifies encoded bytes. The typed document codec below includes its version
and semantic fields; opaque storage fixtures establish no semantic proof.
Exact retry does not renew a lease or steal another worker's token. The coordinator
retains its nonce across uncertain admission/takeover acknowledgement; matching
live ownership can be recovered without extending the lease. Lookup omits tokens,
but still requires coordinator authorization before any public exposure. Scope
isolation tests are not authorization tests.

The owner generation and token fence renewals, and takeover increments generation
while rotating the token. Lease checks use database time after row-lock waits.
Both initial admission and a conflict retry enforce READ COMMITTED; a focused test
first demonstrated that the retry path could bypass an owner-only isolation guard.
The command INSERT trigger now enforces the guard even on `ON CONFLICT DO NOTHING`.

Fixtures use opaque synthetic bytes to test storage identity only. Real PostgreSQL
cases cover conflicting/same-command admission waits with winner commit/rollback,
independent-key progress, exact replay through a fresh ledger, expired renewal,
takeover retry, renewal-versus-takeover waits, immutable SQL guards, incomplete
admission rollback, size/lease bounds and populated V33 migration. A maximum-size
command heartbeat has a client-statement/transaction budget; that is not provider
latency qualification. SQL trigger internals are outside the client-statement count.

This row pair does **not** record allowed upload scope, validate protobuf semantics,
authorize resources, bind evidence to members, publish domain rows or persist a
terminal outcome. Keep it disconnected from provider I/O until typed command and
upload-intent integration exist. The subsequent coordinator must publish domain
changes, native references, outbox and immutable outcome in the same transaction;
missing lookup results remain inconclusive during in-flight admission/commit.

Local V34 validation on 2026-10-03: `:protomolt-repo-container:test
:protomolt-repo-spi:checkRuntimeBoundaries` completed in 1m55s. JUnit XML contains
60 suites, 427 cases: 426 passed, zero failures/errors and one opt-in benchmark
skipped. All 28 admission cases passed; the maximum-payload renewal stayed within
one transaction and four prepared client statements. Sol reviewed the final
command/owner split and isolation guard. This is local validation, not hosted CI,
publication, deployment or a production latency claim.

Required cases before wiring production consumers:

- A stale second destination, changed source/policy, invalid reference or outbox
  failure leaves both destinations, references and logical outcome unchanged.
- Reversed input ordering cannot deadlock; independent change sets progress;
  create/create conflicts use the same durable identity checks as ordinary saves.
- Exact replay after a lost acknowledgement returns the stored logical result;
  reused operation identity with different semantic bytes conflicts. Lookup and
  replay reauthorize the current caller. No post-commit cancellation reports
  that the transaction rolled back.
- A concurrent reader obtains one consistent committed set. Choosing a snapshot
  and acquiring physical lifetime protection must not accidentally mix revisions
  or permit cleanup between sampling and use. Qualify this against the existing
  READ COMMITTED retention guards before fixing the read-port signature.
- A failure after one domain handler runs still rolls back both handlers, their
  references, outbox and receipt. Recovery handles staged bytes separately.
- Check minimal-port runtime and published metadata, complete proto imports,
  lint and compatibility; measure statement counts and same-object versus
  disjoint-object contention. Passing a two-row fixture is not scale qualification.

For the optional JCR extension, this supplies reusable atomic publication and
snapshot primitives only. Stable movable-node identity, session pending changes,
workspaces, types, graph references and version restoration remain extension
responsibilities under the existing compatibility assessment. Existing
address-derived document/archive IDs retain their current meaning.

Baseline verification on 2026-10-03: 64 existing document publication/part and
archive mutation tests passed. `bufLint` passed; repository, mesh and receipt
Java compilation was up to date. `scripts/check-proto-compatibility.sh 3cdd28e4`
freshly built the complete current/baseline descriptor sets and passed FILE
compatibility. This proves the inventory change preserves that starting wire
surface, not that a future commit API already passes compatibility.
`repo-spi:checkRuntimeBoundaries` passed. Generated Maven POM and Gradle API/runtime
metadata list only `protomolt-repo-proto` as a direct dependency; these were
generated locally, not published. The existing runtime gate checks transitive
forbidden modules. No implementation or conformance claim is made for the new
multi-object port.

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
saves in the production host are not wired to it. Explicit library composition
now integrates the managed full/partial writer, with selected real local/gRPC
coverage; this is an experimental path pending lifecycle and performance gates.

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
full/partial save and original-profile reading have explicit library composition;
raw-reference and source-revision races have selected local/gRPC coverage. Managed
delete/reclamation and production host wiring remain unfinished. This evidence
does not qualify the complete managed document API or its performance.

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
managed document feature. Opt-in full/partial write composition is tested, while
host composition and retained-document reclamation remain unfinished. Do not enable managed document
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

The following sequence records the implemented copy-based writer baseline. The
transaction/concurrency redesign in repository-composition.md supersedes it for
future work; preserve its failure guarantees without retaining unnecessary copying
or per-part transactions. It does not require renaming existing document RPCs:

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

#### Archive deletion and binding foundation

Physical key separation is implemented as a prerequisite: newly written rendition
objects receive unique write UUIDs, with the content hash retained in the key and
manifest. Unary saves and bridge output reuse a matching current rendition's
physical reference. Streamed dedupe returns the retained key and deletes only its
unused unique candidate; cleanup errors propagate. Delete/recreate at the same
address gets different keys in library and gRPC integration tests. Existing
manifest keys remain readable. Managed candidate registration/recovery, immutable
backend bindings and identified destructive admission are described below; these
are domain-specific foundations, not a completed generic repository transaction.

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

Managed archive writers populate this field. Publication must validate the complete
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
both library and gRPC. The identified ArchiveMutationService is implemented for
qualified bindings; the general retention/commit redesign remains planned.

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

### Typed document publication intent and admission

**New standalone contract:** `repo/v1/document_publication.proto` and the
`repo/spi` value `DocumentPublicationCommand`. **Extended internal operation:**
`RepositoryOperationLedger.admit` accepts that value, checks account/operation
binding, and stores its codec/version/canonical bytes through V34 admission.
**Unchanged:** all existing RPCs, document/archive requests, responses, protobuf
names, tags, imports and Any URLs. No publication RPC, executor or terminal
outcome is introduced. Admission still does not authorize provider I/O: durable
upload scope must be integrated first.

The intent describes 1–64 complete document revisions. It reuses `NodeAddress`,
`OwnershipContext`, `DocumentSecurity`, `DocumentPart` and `WriteProvenance`.
New fields name stable drive UUIDs, explicit row kind, crawl/cluster association
and source-blob lifecycle policy. `ownership.connector_id` is the sole connector
value; there is no competing legacy override. Filename derives from admitted
CORE content; current timestamps, root checksum, storage coordinates and provider
observations remain execution evidence. Creation/reprocess history is retained
server state, not a caller-authored replacement. Generic metadata is caller data;
it must never be interpreted as an undocumented override of typed fields.

New bytes specify size, lowercase SHA-256, representation type and optional
provenance. Explicit EMPTY is distinct from a zero-byte stored object and pending
hydration. Retained reuse specifies the original physical object UUID, backend
generation, realm, namespace, key, optional provider object version, size, digest
and content type, plus source address/revision/slot. Source and destination slot
must match; this operation does not rename chunk sets or cast one part into another.
Retained provenance and timestamps carry forward. A source that is also a batch
destination binds its pre-change revision. This shape supports future reuse; the
current attempt-owned publication adapter still cannot execute it.

The exact schema condition copies the type/descriptor-closure identity semantics
of mesh `SchemaReference` without importing its workflow/artifact dependencies.
Its closure is the defining file and transitive imports, sorted by file name and
serialized deterministically. Existing mesh identity stays unchanged. This is a
condition, not a registry locator or evidence of descriptor retention/validation.
Requested schema omission never disables a repository's required admission policy.

**Validation boundary.** The real runtime validator checks required messages,
version, UUID shape, explicit alternatives, positive revisions, digest/MIME shape,
metadata/count bounds, row-kind/lifecycle rules, CORE selection, chunk keys and
account equality. Fixtures use both generated and dynamic messages. The immutable
Java command additionally rejects unknown fields/enums recursively, NUL and
malformed UTF-16, duplicate members/destinations/slots, malformed legacy ACL/source
principals, conflicting source conditions and arithmetic overflow. Unknown fields
are not assumed rejected by ordinary protobuf parsing or the annotation validator.
All invocation paths must construct this value before durable typed admission.

A command is at most 1 MiB including its operation UUID, with at most 10,000 total
slots and 10,000 source checks across members (each reuse counts as one source
check). These ceilings apply together. Hash-based duplicate/condition checks are
linear in input count; only at most 64 members are sorted. No per-part SQL/provider
call occurs during construction. Declared content bytes are overflow-checked, not
capped at 1 MiB: transport, per-object size, active byte budget, provider capability
and deadline limits remain coordinator obligations. This is not a qualified
bounded upload plan or a measured latency guarantee.

**Canonical version 1.** Normalize operation UUID spelling and sort members by
stable ASCII member ID in the immutable executable intent. Exclude operation UUID
from canonical bytes; retain encoding version and every other field. Serialize
protobuf deterministically, including map keys. Preserve part order, explicit
source order, ACL order and optional presence. Do not trim coordinates, collapse
empty/absent optionals, reorder chunks, or substitute generated staging identities.
Noncanonical drive/object UUID spellings are rejected. Pin wire bytes and SHA-256
with the independently assembled `document-publication-v1.hex` fixture. Future
encoders must preserve v1, not silently reinterpret already admitted commands.

**Stateful checks and remaining implementation backlog:**

- Bind durable allowed upload scope and each member/slot/attempt generation to
  this command before provider I/O. Verify bytes and exact target configuration;
  reject stale owners and contradictory evidence. Test takeover and late evidence.
- Decode assembled content and enforce address, ownership, layout and exact schema
  conditions before semantic review/publication. Retain descriptor closure and
  admission evidence; test wrong Any type, missing imports, unsupported validation
  rules, descriptor drift and registry outage.
- Fence current authorization, requested ACL changes, source access and policy
  races for library and transport. Constructing ownership or a physical identity
  grants no permission. Test revoked historical/replay access and cross-account
  attempts; reject forbidden requested changes rather than silently rewriting them.
- Integrate retained physical references into native/common retention and the
  document batch. Test source/destination overlap, original provider versions,
  zero-copy updates, pruning races and original provenance retention.
- Commit domain rows, refs, outbox and immutable logical outcome together under
  the operation owner fence. Add reviewed successful-response/error contracts,
  lookup, cancellation and replay fixtures; no publication response exists yet.
- Qualify configured payload/concurrency limits and latency distributions against
  representative providers; retain the existing conditional payload bound.

**JSON Schema/OpenAPI:** generated schema records ordinary field/count rules and
CEL as `x-protomolt-cel`. CEL extensions are documentation, not executable JSON
Schema constraints. Cross-message uniqueness, aggregate limits, unknown-field
policy, canonical encoding, current authorization and byte/schema verification
require runtime/handler enforcement. Existing optional/oneof/numeric/format
translation gaps remain tracked generator work; no generator parity is claimed
or changed here. No service descriptor means no new OpenAPI operation.

**JCR assessment:** this intent is a document-domain composition of the bounded
multi-object foundation, not its universal transaction model. Stable JCR node
identity across moves, session changes, workspace semantics, references and
restoration remain in the optional content extension described in
[the compatibility assessment](repository-jcr-compatibility.md). No JCR types or
mesh/workflow runtime dependency enter the base SPI.

Local validation on 2026-10-03: all repo/proto and repo/spi tests plus
`RepositoryOperationAdmissionIT` passed: 28 + 17 + 30 cases, zero failures,
errors or skips. The admission cases use real PostgreSQL 18 in Testcontainers.
The initial combined run completed in 17 seconds; a final rerun after narrowing
the schema-projection assertions completed in 3 seconds with unchanged tasks
up-to-date. Full `bufLint`, complete-descriptor compatibility against `15e25698`,
and the repo/spi runtime dependency gate pass. Sol reviewed the contract, codec,
typed admission and remaining obligations without a blocking finding. These are
local contract/admission results, not provider performance qualification or proof
that the full repository goal is complete. No push, hosted CI, merge or deployment
is part of this checkpoint.

### Bounded document-attempt admission statements

**Extended implementation, unchanged operation:** `DocumentPartAttemptLedger.begin`
now prepares immutable JSON statement inputs before opening its transaction and
inserts parts/source dependencies in batches of at most 256 rows. All numbers are
encoded as decimal strings and parsed directly into PostgreSQL INTEGER/BIGINT;
no value passes through a floating-point JSON number. Ordered part ordinals,
source revisions, physical location registration, key reservation, unverified
state and the final atomic seal retain their existing meaning. The existing
conditional payload bound and public protobuf contracts are unchanged.

Client statement count is now `5 + ceil(parts/256) + ceil(sources/256)` in one
transaction. The five fixed calls resolve the original profile, insert the
attempt, reserve keys, seal the plan and read its result. A regression test first
observed 1,031 statements for 513 parts and 513 sources, failing a budget of 11.
The new path passes that budget. No schema migration or database guard was removed.
Permanent key reservation still runs in database digest order, and per-row
reservation/plan/catalogue triggers still enforce direct-SQL writes. Their work
remains proportional to row count and is not included in the client statement
count. This change reduces round trips, not the number of retained objects or
required integrity checks.

Real PostgreSQL fixtures cover 1/0, 256/256, 513/513 and 10,000/10,000 part/source
counts, including exact values above 2^53 and at Long.MAX_VALUE, escaped Unicode
coordinates, ordered slots and unverified catalogue identities. Injected database
failures during a later object or source batch roll back the attempt, all earlier
rows, physical locations and key reservations. These are synthetic metadata
claims, not verified provider bytes. Existing provider/publication tests remain
necessary.

One local diagnostic run measured 8.773, 59.768, 118.907 and 2,082.556 ms respectively
for those four cases, with 6, 7, 11 and 85 client statements. Timing includes Java
encoding and transaction completion but not test profile creation. This is a
single uncontrolled-host run, not an interleaved baseline comparison, lock-wait
measurement, production latency target or speedup claim. Count ceilings bound
metadata volume; the encoded strings are retained until admission returns and
heap/resource qualification at worst-case coordinate lengths remains outstanding.

Before adding the operation-owner lock, preserve this batch behavior and qualify
its transaction/lock duration. Operation ownership must be locked before any
attempt lock. The existing attempt plan requires an uploaded CORE; the typed
intent instead allows a retained CORE and may contain no new uploads. Do not
force an unchanged CORE upload or create an empty dummy attempt to bridge that
mismatch. Generalize upload admission to new bytes only, keep complete revision
validation at publication, and separately fence retained source references.
A reused CORE must still be present; an EMPTY CORE remains invalid.

Local regression validation on 2026-10-03:
`:protomolt-repo-container:test --tests '*Document*'
 :protomolt-repo-service:test --tests '*Document*'` completed in 1m09s.
JUnit XML reports 213 container and 87 service cases: 298 passed, zero
failures/errors, and two opt-in benchmark skips. This includes the new six
batch-admission cases and existing document staging/publication/cleanup/provider
fixtures. Sol reviewed the batching and retained SQL guards. No push, hosted CI,
merge, deployment, complete owner-binding integration or latency qualification
is implied. The measured two-second maximum-count case also exceeds the minimum
one-second lease: a future coordinator must choose adequate leases and recheck
ownership after admission, not assume the minimum works for every plan.

### Coordinator transaction participation and rollback reporting

**New internal entry:** `DocumentPublicationBatch.saveInTransaction` joins an
already-active writable transaction. It opens no transaction and does not commit.
**Extended internal helper:** `RepositoryOperationLedger.lockLiveOwner` locks the
owner row before domain locks, compares generation/token and checks database time
after any wait. Renewal reuses the helper without changing its SQL statement
budget. **Unchanged:** existing standalone publication, all public SPI/wire
contracts, physical retention and cleanup semantics. No operation-to-upload or
terminal-outcome binding is established by calling these helpers alone.

The participant performs the same preflight, sorted revision/drive locks,
verification, history/current-reference insertion, outbox callback and cancellation
checks as the standalone path. Candidates remain thread-confined. An exception
or Error marks the outer transaction rollback-only; an internal caller catching
the error cannot then commit partial results. Owner-fence failure similarly
vetoes the transaction. The coordinator remains responsible for acquiring the
owner fence first, binding the exact command/evidence, checking authorization,
choosing lease checkpoints and persisting the immutable outcome in this transaction.
Provider I/O remains outside it.

**Fixed transaction behavior:** real PostgreSQL tests exposed that Hibernate's
RESOURCE_LOCAL commit could silently roll back a transaction marked rollback-only,
while `Tx.inTransaction` returned its callback's result as success. `Tx` now checks
the rollback-only flag before commit and throws `RollbackException`. It explicitly
rolls back on Error, and preserves rollback failures as suppressed exceptions on
the initiating failure. This changes false-success reporting into explicit failure;
it does not imply that a connection failure during COMMIT proves rollback.

Fixtures verify two real document publications plus references/outbox commit or
roll back with one outer SQL transaction, rejection of a transactionless manager,
and caught participant/owner failures that still veto commit. Independent real
PostgreSQL tests cover a callback marking rollback-only and returning a value,
an Error after SQL writes, and server termination of only the test transaction's
backend connection to force rollback failure. They assert no committed probe rows
and preservation of the original plus rollback failures. No success-shaped
provider mock is used for these claims.

### Required storage facts for partial publication

Sol's source review found that permitting a CORE-less upload plan is insufficient:
V22 publication history/current pins treat an attempt as a complete revision;
V26 native reference proof retains the objects of that attempt. The next
implementation must separate upload ownership from complete revision ownership.

- Extend existing attempt admission with an explicit new-content plan kind and
  operation/member/owner-generation binding. Preserve full-revision admission
  for current callers and its exactly-one-uploaded-CORE rule. New-content attempts
  contain 1–10,000 new objects, possibly no CORE. Zero-upload revisions have no
  attempt. Bind the exact ordered upload subset and selected placement before I/O.
- A complete revision owns ordered physical-object references drawn from verified
  new objects and authorized retained source slots. Reuse the physical catalogue
  and common retention authority; do not manufacture copied attempt-object rows
  or introduce a second upload ledger. Preserve a present CORE and provenance.
- Extend native-reference proof for these revision/slot owners. V22's deferred
  consistency guard must recognize the exact new revision/current pin; V25's
  quarantine checks must recognize its legitimate registered keys; V24 cleanup
  must see newly published upload ownership. Preserve existing legacy guards.
  Current-policy authorization, metadata snapshots and source-revision fencing
  must apply to both new and reused content.
- Verify/renew new-content attempts under the operation generation fence in
  owner-before-attempt order. Define takeover/resume explicitly; an unexpired
  attempt lease does not authorize an old operation owner. Assess every SQL
  mutation path and recovery lock order before adding cross-table triggers.

These are identified implementation prerequisites, not completed behavior or a
new RPC. The foundation remains composable for the optional JCR extension;
no document-specific attempt identity becomes a universal content-repository
transaction or node identity.

Local validation on 2026-10-03: full `:protomolt-repo-container:test` and
`:protomolt-repo-service:test` completed in 2m56s. JUnit XML records 443 container
cases and 331 service cases: 771 passed, zero failures/errors and three skips.
The motivating caught-failure fixtures failed before the `Tx` fix because no
exception escaped the rollback-only transaction, then passed with it. The
independent connection-loss/Error/rollback-only PostgreSQL fixtures also pass.
Sol reviewed the participant, owner helper, transaction fix and required partial
publication facts without a blocking finding. No push, hosted CI, merge,
deployment or completion of the full eight-stage goal is claimed.

### New-upload selection from the admitted command

`DocumentUploadPlan` is a new internal, pure preparation operation. It retains
the complete validated publication command and each member's full ordered
revision while selecting only slots that declare new bytes. Retained and empty
slots do not create uploads. An upload keeps its original revision ordinal;
a member with no uploads has no attempt. Stable drive UUIDs select immutable
placement snapshots, and coordinator-minted attempt UUIDs do not change the
canonical command. Provider I/O, SQL admission, authorization, retention and
publication are outside this mapper.

Preparation rejects extraneous drive/attempt selections before copying maps
larger than the bounded member count. Placement coordinates are validated even
for reuse-only members. Work is proportional to command members, source checks
and parts, plus generated key lengths; it does not fetch content or descriptors.
The command's existing count and encoded command-byte bounds still apply; these
do not impose a declared-content byte cap. Tests cover 4,096 new
chunk slots alongside a retained CORE, exact 64-bit sizes, immutable snapshots,
shared-drive/shared-source members, zero-upload members and invalid selections.
These are pure synthetic mapping fixtures, not verified provider uploads or a
latency qualification. No database lock is acquired during preparation.

The legacy node ID encoding concatenates address coordinates with `|`, so
distinct tuples containing that delimiter can collide. This new path refuses
such addresses for destinations and sources instead of silently changing stored
identity. The legacy paths remain unchanged; a repository-wide identity encoding
and migration decision is outstanding. This guard is not JCR stable identity.

Before execution, implement durable NEW_CONTENT attempt admission bound to the
operation/member/owner generation, exact upload subset and original placement.
Recheck sampled drive/backend state under the admission transaction. Legacy
full-revision admission still requires an uploaded CORE and must not consume
these partial plans. Revision ownership, publication and terminal outcomes
remain separate unfinished work. No public API availability is claimed.

Local validation on 2026-10-03: the ten mapper tests, six real PostgreSQL batch
admission cases and 52 real PostgreSQL atomic publication cases passed, with no
skips, failures or errors (28-second Gradle run). Sol reviewed the mapper, tests
and inventory without a blocking finding. No hosted CI, push, deployment or
end-to-end performance qualification is claimed for this checkpoint.

### Composable attempt admission and remaining owner-fence boundary

Existing full-revision attempt admission is extended internally to participate
in a caller-owned transaction. `prepareAdmission` binds the immutable plan,
lease and encoded batches before acquiring database locks; callers cannot pair
encoded inputs with a different plan. `beginInTransaction` neither starts nor
commits a transaction. Its returned attempt is provisional until the outer
commit, and any participant failure marks the transaction rollback-only. The
existing `begin` method uses this same implementation and retains its statement
budget of five fixed calls plus one per object/source batch.

This enables one coordinator transaction to admit multiple members and their
key reservations and physical-location rows together. It does not enable
NEW_CONTENT plans, validate a publication command, enforce current policy or
establish an operation-owner fence. Existing exactly-one-uploaded-CORE admission
semantics remain intact.

The next owner-bound admission change must resolve these database boundaries:

- Persist immutable operation/account/principal/member/generation binding and
  exact command-derived scope with admission. Store compact upload ordinals and
  original revision ordinals separately. NEW_CONTENT permits zero or one uploaded
  CORE; its complete revision still requires CORE. Zero-upload members have no
  attempt. Preserve legacy sealing and all V23/V25 key/catalogue guards.
- Acquire operation ownership before attempt/domain locks in admission, renew,
  verification, publication and recovery paths. Adding an owner-lock query to a
  row trigger is insufficient: the triggering statement may already hold a
  target-row lock. Direct SQL mutation must not bypass the ordered fence. Assess
  restricted-role ordered routines or an equivalent enforceable boundary before
  enabling NEW_CONTENT; do not rely on callers following a comment.
- Legacy V22 publication requires a complete manifest with uploaded CORE. Both
  Java and SQL entry points must reject NEW_CONTENT until full revision/slot
  ownership can combine verified new bytes with retained references. V24 cleanup
  must reclaim abandoned new-content uploads while respecting that ownership.
- Prove takeover races and direct-SQL rejection, including verification, renewal
  and cleanup; measure contention under bounded timeouts. Passing single-writer
  fixtures is insufficient evidence for this owner-bound path.

Sol reviewed this admission extraction and the remaining lock-order assessment.
These are implementation prerequisites, not completed owner-bound behavior.

Local validation on 2026-10-04: all ten admission cases passed, including the
four new transaction-participation cases. The broader container `*Document*`
regression completed in 55 seconds: 25 suites, 231 cases, 230 passed, one skipped
and no failures/errors. The existing statement-count assertions passed through
10,000 objects and sources. These counts are not production latency evidence.
No push, hosted CI, merge or deployment is claimed.

### SQL owner-write proof, before upload integration

V35 adds an internal transaction-specific owner-write proof. The existing owner
guard remains authoritative for scope, generation and lease transitions. A new
trigger stamps every owner INSERT/UPDATE with the actual top-level transaction's
`xid8`, ignoring any caller-supplied value. Existing rows start with a null stamp.
The new Java `fenceLiveOwner` checks the exact account/principal/operation,
generation and token in one owner-only UPDATE, without extending the lease or
reading the command payload. The existing guard checks expiry after lock waits.

`require_repository_operation_write_fence` checks exact scope, generation, live
lease and current transaction ID with a plain indexed read. It acquires no owner
row lock. A dependent row trigger can therefore reject an unfenced write without
waiting for ownership after it has locked its own row. This is the selected
foundation for owner-first enforcement; production dependent tables are not yet
wired to it. A SELECT FOR UPDATE alone does not produce this proof.

The stamp proves prior owner-row write locking, not caller authorization or
possession of a token. A trusted database writer with UPDATE rights on the owner
table can obtain a stamp. Principal authentication and current-policy enforcement
remain coordinator obligations; this does not protect against a database
administrator disabling triggers. Every guarded boundary must still check the
bound generation and lease, and publication requires its final ownership check.
An initial stamp does not promise that a lease remains live until commit.

Cost: one owner-row UPDATE per coordinator transaction, with its WAL and row
version, shared by a batch of dependent writes. This serializes transactions for
the same operation, not unrelated operations. Do not stamp per object or claim
that reduced client calls eliminate trigger-side work. The existing renewal
method keeps its statement budget. Contended latency/WAL qualification remains
required when uploads and publication use this mechanism.

PostgreSQL documents that the transaction functions return the top-level ID even
inside subtransactions, and that rollback to a savepoint releases subsequent
locks and changes. The stamp rolls back with the owner row; retaining the same
top-level transaction ID after rollback does not preserve the proof. See
[transaction ID functions](https://www.postgresql.org/docs/18/functions-info.html#FUNCTIONS-PG-SNAPSHOT)
and [lock lifetime](https://www.postgresql.org/docs/18/explicit-locking.html).

Real PostgreSQL tests use a clearly test-only child table and guard to exercise
the primitive. They cover 256 guarded rows with one owner client statement,
cross-transaction replay, exact scope/generation, forged token, savepoint release
and rollback, refusal without waiting on a competing owner, independent-operation
progress, lease expiry, replacement while a writer actually waits on a lock, and
V34-to-V35 migration with an existing owner. They also demonstrate the trusted
database-writer limitation rather than claiming authorization. All 16 new cases
and 31 existing operation-admission cases passed locally on 2026-10-04.
NEW_CONTENT scope, production child guards and terminal outcomes remain pending.

The full container and service suites subsequently passed in 2m29s: 103 suites,
804 cases, 801 passed, three skipped, zero failures/errors. Sol's review requested
identifying the exact blocked backend in the lock-wait cases; that improvement
passed all 16 focused cases in a 14-second rerun. No production throughput,
hosted CI, push, merge or deployment is claimed by these local results.

### Operation-bound new-content attempt storage

V36 extends the existing attempt tables with `FULL_REVISION` and `NEW_CONTENT`
plan kinds. Existing rows and callers retain full-revision semantics. New-content
rows bind account, principal, operation, owner generation, member and selected
drive UUID immutably. This is a storage binding, not evidence that the command
authorized that drive or those slots. A future typed admission entry point must
compare the persisted command, complete member intent and sampled placement.
No service or provider-I/O path admits new-content attempts yet.

Objects retain compact upload ordinals and separately store original revision
ordinals. Migration backfills the latter from the former for existing objects.
New-content subsets remain nonempty, with unique slots and original positions;
they may contain zero or one uploaded CORE. Complete revisions still need CORE,
and reuse-only revisions have no attempt. Existing reservation, immutable
physical-location and common retention guards apply to these same object rows.
No copied retained objects or second upload ledger is introduced.

Attempt and object/source writes require the matching V35 owner-write proof.
Binding fields and original ordinals cannot change. Object/source guards use the
existing parent lookup and check the proof after locking the attempt, including
database-time liveness after a wait. The proof read acquires no owner lock; a
legitimate writer must already hold it from V35. Unfenced direct SQL can wait on
an attempt before failing, but cannot obtain ownership through this check.
Legacy publication rejects new-content rows before taking retention locks.
The Java attempt snapshot now includes `planKind`; its canonical constructor has
one additional String argument. Legacy Java renew/verify/publication entry points
explicitly refuse new-content attempts instead of treating them as full revisions.
Wire contracts are unchanged.

Takeover cannot adopt an old attempt: the stored owner generation is immutable.
Recovery must allocate a fresh attempt and fresh keys. More than one historical
attempt may exist for a member within one owner generation, because an expired
upload can need replacement while its operation lease remains live. The future
coordinator must select and reconcile the exact attempt; this schema alone does
not implement retry reconciliation or active-attempt selection.

Cleanup intentionally retains its attempt-only lock order. It accepts only an
expired, sealed, unpublished attempt; the attempt lease cannot then be renewed.
New-content publication is disabled, and generation takeover requires fresh keys.
Cleanup can therefore reclaim abandoned uploads even while an operation owner is
live, without acquiring an owner lock after the attempt lock. Common retention
still fences reclaiming objects. Reassess eligibility when complete revision
ownership is implemented; do not weaken it to enable publication.

Real PostgreSQL fixtures exercise the SQL lifecycle with synthetic unverified
byte declarations, not provider success or semantic validation. They cover sparse
positions, both uploaded-CORE counts, missing/stale owner proof, immutable fields,
rollback of reservations/catalogue rows, fresh attempts after takeover, cleanup
and late-write refusal, early legacy-publication refusal under a held attempt
lock, and migration with an existing attempt/physical object. The owner check is
folded into the existing parent guard, preserving one parent lookup for legacy
object/source writes. New-content writes add an owner-proof lookup. Unchanged
client statement counts do not qualify their latency cost.

Local validation on 2026-10-04: all 17 new SQL lifecycle cases passed. The final
full container/service regression passed in 2m30s: 104 suites, 821 cases, 818
passed and three skipped, with zero failures/errors. The older cleanup migration
fixture now compares every pre-existing field and separately checks the additive
plan-kind/binding/ordinal defaults. Sol reviewed the SQL guard preservation,
trigger order, recovery semantics and removal of redundant legacy parent reads.
No provider upload, public admission API, hosted CI, push, merge, deployment or
performance qualification is claimed.

### Typed internal upload admission

`DocumentOperationUploadAdmission` connects the validated publication command,
pure upload selector and V36 storage. Its private prepared value couples the
command to its exact generated upload subset, tokens and encoded batches; callers
cannot substitute an unrelated encoding. Preparation runs before database locks.
Admission checks the trusted caller scope, acquires the V35 owner fence and
compares the persisted codec/version, exact canonical bytes and digest. It then
locks and authorizes current document revisions before locking distinct sampled
drives in UUID order and comparing their registered backend profiles. It inserts only new-byte
objects with original revision ordinals and complete declared source conditions.
The existing key reservation/catalogue and 256-row batch insertion paths are
shared with full-revision admission, not duplicated into another ledger.

All members commit in one transaction. A later member failure rolls back earlier
attempts, key reservations and physical locations. Duplicate attempt IDs fail;
this method does not infer success, adopt prior bytes or reconcile a lost commit
acknowledgement. Reuse-only members still undergo placement checks and create no
attempt. Their sampled placement is not durably captured by an attempt row;
persisting that evidence for replay/publication remains an explicit prerequisite.

A final aggregate checks every newly inserted attempt's state and earliest lease
against database time, followed by the operation-owner check. This prevents a
slow later member from returning success with an already-expired earlier attempt.
It is a pre-commit liveness check, not a guarantee against expiry after that check;
each later provider boundary must recheck its authority. A real SQL delay fixture
first demonstrated the incorrect successful return, then passed with full batch
rollback after this check was added.

Scope limits: this remains package-private SQL staging. The qualified composition
supplies the backend selection; comparing it with the unchanged drive and immutable
registration does not independently derive provider identity from drive config.
Host authentication integration, retained physical-content validation, schema retention,
active-attempt selection/retry reconciliation and operation-bound provider writes
remain pending. The current-policy extension below consumes a caller already
authenticated by the host; it does not implement authentication or expose an RPC.
No successful receipt, semantic review, normal read or public upload API follows
from staging these synthetic byte declarations.

The real PostgreSQL fixtures cover exact typed command binding, stale drive/profile
refusal, missing registration, forged owner, zero-upload placement checks,
duplicate-attempt refusal, sparse subset persistence, later-member rollback and
lease expiry. At this initial checkpoint, before the authorization extension below,
one drive/member/source with 1 and 513 uploads required 12 and 14 client statements
respectively, each in one transaction; server-side trigger
work and provider latency are not included. All 11 new cases and ten legacy batch
admission cases passed locally on 2026-10-04. Sol reviewed the implementation and
the reproduced lease failure without a remaining blocking finding.

The broader container/service `*Document*` regression passed in 1m20s: 37 suites,
346 cases, 344 passed, two skipped and zero failures/errors. This is a document
regression run, not every repository test. No push, hosted CI, merge, deployment
or production performance qualification is claimed.

### Batched document revision locking

`DocumentLedger.lockRevisions` is **extended** for existing guarded writes and
multi-document publication. V37 adds `lock_document_revision_keys(bigint[])`;
no protobuf field, name, import, Any URL or public RPC changes. This is a
prerequisite for the pending typed-admission authorization/revision checks,
not evidence that those checks or provider writes are enabled.

The lock sequence remains all distinct signed-bigint advisory keys, followed by
document rows in Java UUID order. The advisory phase runs in one PL/pgSQL call
with an explicit ordered loop. Row reads use batches of 256 with array ordinality,
because PostgreSQL's UUID order differs from Java's signed-half order. Each row
query sorts the supplied immutable ordinality before `FOR UPDATE OF d`; it does
not move the locking clause into an unordered subquery. See PostgreSQL's
[locking-clause ordering](https://www.postgresql.org/docs/18/sql-select.html#SQL-FOR-UPDATE-SHARE).
Missing destinations remain protected by advisory keys. XOR key collisions only
serialize unrelated identities; they never substitute for document identity.

The internal boundary accepts at most 10,064 distinct identities (10,000 source
conditions plus 64 destinations), refuses oversized inputs before SQL, and
requires an active transaction. V37 independently bounds and validates its array
and requires READ COMMITTED. Empty Java input issues no statements. For nonempty
input the client statement bound is `1 + ceil(unique identities / 256)`, including
the advisory call. PostgreSQL still acquires O(N) locks and the method still
returns O(N) document snapshots; this is not a reduction to constant server work
or a byte-size bound on existing manifests. Transactions enter this method before
staging document mutations and retain their locks until commit/rollback.

Every native row result includes the actual locked database revision alongside
the Hibernate entity. A stale first-level-cache entity causes revision conflict,
before it can supply policy or content information. Matching cached entities are
valid. This avoids per-row refresh calls. V8's sequence revision covers SQL policy
updates as well as ORM/body changes. The enclosing participant must roll back
its transaction on failure; the low-level helper does not commit, retry, renew
leases or silently refresh stale caller state.

The new real PostgreSQL tests first reproduced excessive client statements at
257/10,000 identities and stale cached-row acceptance after external policy and
same-transaction SQL changes. They also exercise missing/stale sources, source
changes during a row-lock wait, absent-row creation, Java/PostgreSQL UUID-order
differences with colliding advisory keys, completion of the advisory phase before
row locking, independent-operation progress, overlapping reversed 257-row batches,
timeout rollback, invalid SQL arrays and no-SQL input bounds. Wait assertions
observe the specific contender's PostgreSQL backend PID, rather than assuming a
sleep means it reached a lock.

Local validation on 2026-10-04: the full container/service regression passed in
2m19s, with 106 suites, 846 cases, 843 passed and three skipped, no failures or
errors. Sol reviewed the implementation and concurrency assertions. Subsequent
test refinements explicitly observe acquired advisory locks before timeout and
row ordering across the 256/257 boundary; all 15 final focused cases passed in
17 seconds, including an eight-second controlled lock timeout.

The full-run diagnostic observed 41 client statements and approximately 419 ms
for locking 10,000 synthetic existing documents. Fixture insertion was timed
separately at approximately 1.56 seconds; the revised fixture read took 36 ms.
An earlier test helper's `ANY(CAST(:ids AS uuid[]))` query accounted for a roughly
45-second test delay: live database activity identified that query, and replacing
it with an `unnest` join removed the delay without disabling database guards.
That helper was only in this new test, not the production path. These are local
diagnostics, not quiet-host comparative benchmarks, p95/p99 targets or evidence
for large production manifests. Heap, lock-table capacity, database CPU/WAL,
concurrent-load latency and the complete admission/provider path remain open
qualification work. No public API availability, push, hosted CI, merge or
deployment is claimed by this checkpoint.

### Current policy and revision checks at typed staging admission

`DocumentOperationUploadAdmission.admit` is **extended** to require an explicit
host-authenticated `RepositoryCaller`; there is no caller-free or automatic
administrative overload. `DocumentAdmissionAuthorization` is a new internal
participant. The existing ACL evaluator moves from the engine's package-private
`DocumentAccessPolicy` to public `ai.protomolt.proto.repo.spi.DocumentAccessPolicy`.
The engine and container use the same READ/WRITE, typed identity, account, deny,
inheritance and malformed-policy rules. Its nine unit cases move with it. The SPI
still depends on protobuf contracts, with no SQL, provider SDK or engine dependency.
There are no protobuf/RPC changes and no new public upload service.

Before SQL, admission requires the caller's account membership (unless explicitly
administrative) and exact principal match with the durable operation owner. Missing
caller/account bindings fail with PERMISSION_DENIED; an unbound requested account
is masked as NOT_FOUND. Preparation does not authenticate anyone: hosts must also
apply scope checks before their own placement sampling or repository lookup.

Inside one fresh transaction the sequence is owner fence, exact canonical command,
all document revision locks, authorization and revision checks, drive/profile
checks, then attempt insertion. Explicit and reused sources are merged, checked
once per source revision, and visited in stable UUID order prepared before SQL.
Every locked row must match its complete four-field address, not just its derived
UUID. Sources must be available, have no pending purge, and grant READ, including
self-copy. Destinations require WRITE. Scoped creation remains refused until a
separate host-bound creation-grant contract exists; scoped existing destinations
must also be available and preserve ACL, selected drive name, datasource and
source-deletion policy. Administrative authority remains explicit and still
validates stored and proposed policy. As on the existing path, it may handle
legacy missing security; malformed stored policy is never repaired or bypassed
implicitly. Unresolved inherited policy still fails closed for scoped callers.

The complete source/destination authorization pass precedes **every** revision
comparison. A stale readable source cannot cause a conflict response before a
denied destination is checked. A Sol review identified the interleaved version
of this bug; a real SQL fixture reproduced it, and the two-pass implementation
corrects it. This masks denied revision state, not every policy problem:
malformed or unresolved inherited policy still produces FAILED_PRECONDITION
under the preserved policy semantics.

Zero-upload commands receive the same checks. Failure rolls back the owner fence
and any transaction work; the tests assert no attempt, key reservation or physical
catalogue entry survives. Policy revocation committed while admission waits on a
source row is observed and denied. Another fixture holds a drive lock and proves
denied access returns without waiting on that drive. Source revision checks and
protected mutation checks run before any new staging row can be created.

These are **staging** permissions. Publication must reacquire revisions and
reauthorize after provider work; a successful staging transaction is not enduring
authority to publish. Retained physical-object/slot proof, validated CORE ownership,
schema/descriptor retention, operation-bound verification/renewal, exact retry
selection, zero-upload placement durability and terminal publication/replay remain
unfinished. No semantic review, receipt, normal read or provider write is enabled
by this checkpoint. Large existing policy/manifest byte sizes and concurrent-load
latency remain resource qualification work; bounded row counts alone do not prove
bounded response latency or heap use.

Local validation on 2026-10-04: nine new refusal cases first reproduced successful
staging without the caller/policy/revision checks. The mixed stale-source/denied-
destination case separately reproduced the review finding before the two-pass
fix. `:protomolt-repo-spi:check`, `:protomolt-repo-engine:test`,
`:protomolt-repo-container:test` and `:protomolt-repo-service:test` then passed in
2m21s: 120 suites, 980 cases, 977 passed and three skipped, zero failures/errors.
The SPI and engine runtime dependency gates passed. Two final explicit-dependency
fixtures were added afterward; all 41 admission cases then passed in ten seconds.
Sol reviewed the final implementation and documented scope with no remaining
blocking finding.

For the tested one-member/drive/source cases, current authorization adds exactly
two client statements for batched document locking: 1 and 513 uploads now require
14 and 16 statements respectively in one transaction. Policy evaluation adds no
per-row SQL. These counts exclude server trigger work and are not latency
qualification. All work is local; no push, hosted CI, merge or deployment is
claimed by this checkpoint.

### Retained source binding at typed upload staging

This extends internal upload admission without changing protobuf contracts or
enabling a new public operation. After locked source authorization and revision
checks, admission now proves that each reuse claim matches the source's current
managed FULL_REVISION publication. The proof requires verified attempt/object
records, an unchanged published body, no cleanup claim, and both current and
historical retention references. It compares the exact object ID, backend
generation, realm, namespace, key, optional provider version, size, checksum and
content type. Catalogue registration alone is insufficient.

Policy-only revision changes are checked against the caller's current expected
revision without requiring the older publication revision to change. Explicit
source dependencies that do not reuse bytes need no managed publication. A legacy
row recreated at a formerly managed address cannot borrow the retained historical
binding. Current drive configuration does not remap stored source coordinates.

Preparation deduplicates claims and encodes batches before acquiring SQL locks.
Proof uses ceil(distinct reused sources / 256) plus ceil(distinct reused slots /
256) client statements. Each source body is compared once, independent of its
number of claimed parts. Integer sizes are encoded as decimal strings, preserving
64-bit precision. The tested one-source admission cases now use 16 statements for
one upload and 18 for 513 uploads. These are statement bounds, not throughput or
tail-latency qualification; full-body comparison, lock duration and memory still
need measurements under representative load.

No additional attempt or retention locks are acquired. This relies on the existing
FULL_REVISION immutable history, exclusive attempt keys and native retention
guards while source revision locks are held. Reassess that argument when adding
mixed revisions. Provider work is outside this staging transaction.

Eleven forged identity/slot cases were reproduced as incorrectly accepted with
the new proof disabled, then passed with it enabled. The final focused run passed
66 cases in 20 seconds, including 256/257 distinct sources, 256/257/513 claims,
sizes above 2^53, shared and contradictory claims, optional version presence,
policy changes and SQL retention guards. Test fixtures explicitly synthesize SQL
verification observations; they exercise real publication and retention guards
but do not claim to upload or verify provider bytes. Sol reviewed the final source
and found no blocking issue within this boundary.

Provider immutability qualification, independent revision identity and ordered
references, mixed/zero-upload publication, retained schema closure and retry
reconciliation remain unfinished. Passing this proof grants no enduring publish
authority: publication must reauthorize and recheck revisions after provider work.
No new semantic review, normal-read or receipt path is enabled.

The full container and service regression run passed on 2026-10-04 in 2m18s:
107 suites, 902 cases, 899 passed and three skipped, zero failures or errors.
No push, hosted CI, merge or deployment is claimed for this local checkpoint.

### Independent revision projection migration

V38 adds internal document revision headers, ordered PRESENT-part references and
current revision pins. This extends the existing FULL_REVISION publication path
with a transactional shadow projection; no protobuf/RPC, receipt, schema reference
or idempotency contract changes. Historical UUID values initially equal their
legacy attempt IDs, recorded separately as provenance. The schema's independent
revision identity is preparation for the reviewed cutover, not an implemented
mixed-revision publication API.

Backfill uses immutable history bodies, including histories with no surviving
document. Full manifest position and dense PRESENT-object ordinal are resolved
separately, so EMPTY/DELETED slots do not shift physical identities. Exact slot,
key, size, checksum and physical location must match verified managed evidence;
aggregate size, root checksum and CORE provider identity are checked as well.
Missing or inconsistent evidence aborts migration atomically. Unbound legacy
documents remain unadopted. New legacy writes maintain the projection in the same
transaction, and deleting a document removes its current pin while retaining history.

The population trigger closes part insertion before returning. Sol identified
an earlier same-transaction bypass where forcing a deferred check early allowed
later inserts without revalidation. A real PostgreSQL test reproduced that failure
before the synchronous seal fixed it. The default completeness check is immediate,
after population; explicitly deferred callers still cannot add parts once sealed.
One materialized expected relation supports each completeness proof, avoiding a
full manifest scan per part and deferred-event accumulation across the backfill.

No generic retention owner kinds or extra pins are added. Existing archive reader
and document retention guards remain in force. The shadow tables do not qualify
provider immutability, authorize physical reuse, retain schema/raw history, or enable
metadata-only writes, mixed publication, restoration or pruning. The coordinated
read/retention cutover and latency qualification remain required.

Local validation on 2026-10-04: the container/service regression passed in 2m17s,
108 suites and 909 cases (906 passed, three skipped, no failures/errors). Two final
fixtures then extended migration coverage to nine passing cases in eleven seconds:
explicit deferral of the completeness proof and a 513-part sparse revision migrated
and replaced through the live bridge. That diagnostic measured migration at 96 ms
and complete SQL fixture publication at 444 ms on the local run; these include test
setup work and are not controlled latency/throughput qualification. Sol reviewed
the migration and seal fix with no remaining blocking finding. No push, hosted CI,
merge or deployment is claimed.

### Per-part managed read bindings

Managed reads now resolve V38 ordered revision references while checking their
sealed header and current pin against the still-authoritative legacy publication.
Missing projection state for a managed row fails; it does not become an unbound
legacy read. Full manifest positions are checked separately from dense PRESENT
ordering. Captured source fences use revision IDs and indexed joins from the
already locked document. Distinct backend profiles are loaded in batches of 256;
a real SQL fixture proves 257 profiles take two client statements and an empty
selection takes none. Missing profiles fail without replacement coordinates.

Each read part carries its original generation, profile and namespace. Resolver
lookups are deduplicated by generation/profile only for the selected parts. The
reader reserves one aggregate payload budget before any resolver work and uses
one bounded scheduling window across the selection, preserving part order and
existing cancellation/drain rules. No SQL lock survives into provider reads.
Partial saves select source slots by part/sub-key rather than object key, so
overlapping keys in different namespaces do not collapse. Publication construction
rejects duplicate slots; missing selections fail explicitly.

Java API change: `DocumentPublicationLedger.Publication.revisionId()` replaces
`attemptId()`. `boundParts()` exposes each `BoundPart` with its `Binding`; the old
publication-wide generation/profile/namespace accessors are removed. `parts()`
remains a content-only view. The uniform-binding constructor remains for existing
FULL_REVISION producers. There are no protobuf name/tag/import/Any URL changes.

Real S3 tests use two versioned namespaces with overlapping keys, deliberately
overwrite latest values, and read the recorded versions in original order. They
also check selected/unselected unavailable bindings, one resolver call per distinct
backend identity and aggregate budget refusal before any resolver call. These are
constructed read snapshots backed by real provider bytes, not successful mixed SQL
publications or qualification of different provider implementations. Pure slot
tests cover overlap, ordering, missing and duplicate slots. Coordinated retention,
mixed/zero-upload publication, schema/raw history and latency qualification remain
unfinished.

Local validation for the per-part read change on 2026-10-04: the full engine,
container and service suites passed in 2m21s, 120 suites and 997 cases (994 passed,
three skipped, no failures/errors). The engine runtime dependency gate passed.
This is local verification, not hosted CI, merge or deployment evidence.

### Publication origin and retention lock ordering

The sanctioned Java FULL_REVISION publication batch now stabilizes current-pin
rows and locks the complete union of new and displaced-current attempts before
member validation. After validation it locks their retention rows, before inserting
any history/current references. Document and drive locks remain earlier phases;
each later phase uses PostgreSQL UUID ordering for its own complete set. This
extends internal publication coordination without changing protobuf contracts,
current reference ownership or provider execution.

The previous V26 ordering acquired new retention while inserting history, then
acquired the old origin during current-pin replacement. Real PostgreSQL fixtures
reproduced that ordering for single- and two-member publication: while the old
origin was held, an independent NOWAIT query could not acquire new retention.
The fixed path waits on that origin before taking any new retention. This is a
proven order violation, not evidence of a deadlock reachable through today's legal
FULL_REVISION public path. Multi-origin reuse would make the inverse ordering a
concrete risk, so this fix precedes revision-owned mirrors.

The helper adds three client queries per batch, independent of member count up
to 64: current pins, origins, and retention. The retention query returns counts
instead of all physical UUIDs and rejects missing retention rows. Existing part
budgets still apply; rows locked, server work and lock hold time are not constant.
Exclusive origin locking remains a throughput qualification concern. Shared
immutable-origin validation needs a separate reviewed change across acquisition,
retirement and reclamation rather than a local lock-mode substitution.

This helper intentionally covers existing FULL_REVISION publications only. It does
not add reused origins, change direct-SQL trigger ordering, or claim a general
multi-domain transaction proof. Future mixed publication must include every reused
origin/object and provide an enforceable admission boundary before new native
reference mirrors can be activated. Revision-owned retention remains unfinished.

Local validation on 2026-10-04: both new order cases first failed with PostgreSQL
NOWAIT retention-lock errors, then passed after the prelock change. All 57 atomic
publication cases passed, including 1/64-member three-statement checks and a
cross-member NOWAIT probe confirming the complete retention set is held before
the first publication callback. The container suite passed in the broader run;
that run's service suite hit a LocalStack startup failure (`Text file busy`, exit
126) before RemoteBlobStoreIT initialized. A service-only rerun passed in 1m02s
without application changes. Combined final container/service results: 108 suites,
921 cases, 918 passed, three skipped, no failures/errors. Sol reviewed the final
helper and tests without a blocking finding. No push, hosted CI, merge or deployment
is claimed.

### Revision-owned retention (V39)

Extended internal SQL behavior; no protobuf or public API change. Document native
reference checks now use sealed revision headers and ordered part membership.
History owners use revision identity; current owners use document identity plus
the retained publication revision. Existing FULL_REVISION bridge UUIDs equal their
legacy attempts, so the migration changes no reference keys and creates no pins.
It checks exact expected/actual reference sets in both directions before and after
replacing the document mirrors. Missing or extra pins fail migration atomically.
Archive-version and active-reader predicate branches remain unchanged.

History references are installed when the projection seals, after its parts exist.
Current references switch after the current revision pointer changes. Same-revision
upserts return immediately. Each switch locks the old/new origin union, then the
physical retention union in UUID order, and deletes old references in one set
statement. This also covers document deletion when the revision-current FK cascade
runs before the legacy-current cascade. Historical references survive deletion.
SQL work still scales with part count; this is not a constant-time operation.

V22/V36/V38 guards still restrict publication to complete legacy revisions. The
Java batch prelocks all members' origins and retention before publication; per-row
SQL mirrors do not prove global ordering for arbitrary multi-member direct SQL.
Independent revision creation, mixed-origin and zero-upload publication, shared
origin throughput qualification, pruning and operation replay remain unfinished.

Local validation on 2026-10-04: all 14 revision-projection migration cases passed,
including both FK cascade orders, exact retained-reference parity, rejection of
missing/extra references, same-pin updates, sparse 513-part histories and immutable
projection checks. Full container/service run passed in 2m30s: 108 suites, 925
cases, 922 passed, 3 skipped, no failures/errors. Sol reviewed the final SQL
and tests with no blocker. These results establish regression coverage; controlled
throughput and tail-latency measurements remain pending. Work remains local.

### Initial operation member selection (V40)

New internal persistence; no public operation, retry or replay endpoint. Typed
upload admission now records every member's initial selection in the same SQL
transaction as staging. The key includes account, principal, operation, owner
generation and member. Each row binds destination and sampled mutation revision,
selected drive/backend/realm/namespace, upload count and exact optional attempt.
Zero-upload members retain placement with no dummy attempt or physical object.
The SQL trigger requires the current owner write fence and exact live NEW_CONTENT
attempt binding when present. Java's private prepared command, canonical-command
comparison, policy checks and drive locks establish command membership; SQL alone
does not decode opaque protobuf command bytes or authorize arbitrary members.

The placement snapshot has an explicit field allowlist, a 16 KiB encoded-size
limit, and a separate 32-byte version-1 digest of the sampled drive configuration.
Free-form metadata, provider options and credential references are hashed rather
than copied. The fixed digest recipe lists fields explicitly so future drive
columns cannot silently change historical comparisons. The original immutable
backend generation remains the authority for provider identity. Storage namespace
is limited to 4096 UTF-8 bytes, consistent with the existing 1024-character Java
location bound. These internal records are not public response payloads.

Encoding happens before SQL locks. Persistence adds one client statement for up
to 64 members, independent of uploaded part count. Server work still includes a
drive digest per member and trigger verification; free-form source configuration
has no size limit yet. Large shared-drive configurations need measurement or an
admission bound before latency qualification. This does not improve origin-lock
contention or activate mixed publication.

Initial rows are immutable. A duplicate admission rolls back its fresh attempts
and reservations instead of replacing a selection. A populated V39 migration with
two staged same-member attempts leaves both unselected. Retry CAS/history,
selection lookup under ownership, adoption rules, terminal-result binding and
replay remain required. Future publication must prove exact canonical member and
upload cardinality; the presence of selection rows alone is insufficient.

Local validation on 2026-10-04: full container/service run passed in 2m36s, with
108 suites, 936 cases, 933 passed, 3 skipped and no failures/errors. Coverage
includes exact upload and zero-upload selection, owner/binding/digest rejection,
duplicate-admission rollback, immutable placement, configuration minimization and
populated migration without attempt adoption. Sol reviewed the final versioned
digest and migration fixture with no blocker. No provider I/O or performance
qualification is established by these SQL tests. Work remains local.

### Concurrent source reads during typed staging (V41)

Extended internal admission locking; protobuf and legacy publication behavior are
unchanged. Source-only documents use shared advisory and FOR SHARE row locks.
Destinations, including self-sources, use exclusive advisory and FOR UPDATE row
locks. Shared advisory keys are promoted before acquisition when any destination
aliases the key. The SQL helper sorts actual signed keys, acquires every advisory
lock, then Java locks rows in its global UUID order. Runs of one mode are batched
at 256 rows. This preserves order with the direct multi-document delete path;
locking all destinations before sources would introduce a deadlock.

The maximum row-statement count is bounded by source chunks plus twice the number
of destinations; with 10000 sources and 64 destinations, including the advisory
statement the bound is 169. Typical source/destination pairs use one extra query
compared with the previous all-exclusive batch. The 1/513-new-part admission gates
are now 18/20 client statements. This cost buys concurrent source access without
weakening policy locks or changing row order. Source ACL/status/deletion updates
still conflict with FOR SHARE. The raw mutation revision detects stale Hibernate
entities; authorization still precedes caller-requested revision comparisons.

Two real PostgreSQL tests first failed against the old exclusive helper: independent
destinations sharing a source timed out, and a source-row share probe failed after
an advisory collision. Tests now also cover a complete pair of admissions with
one held after selection insertion, direct policy/deletion conflicts, self-source
and alias promotion, mixed-mode row ordering against a direct writer, malformed
SQL arrays, cached-policy staleness and a large interleaved batch. Native retention
lock modes remain exclusive. FULL_REVISION publication subsequently adopted the
same shared-source protocol as described below; neither change qualifies mixed
publication or provider throughput.

Local validation on 2026-10-04: container/service regression passed in 2m31s:
108 suites, 944 cases, 941 passed, 3 skipped, no failures/errors. Sol reviewed the
final SQL, Java, tests and design with no blocker. The interleaved 10000-source,
64-destination case used 130 client statements (bound 169), taking 558.514 ms in
this uncontrolled run. That is a diagnostic measurement, not a throughput or
production tail-latency result. The held-selection test establishes concurrent
staging progress; provider I/O and mixed publication remain outside its scope.

### Shared source documents during FULL_REVISION publication

Extended existing publication behavior. The internal mixed lock helper is now
`DocumentRevisionLocks`, shared by typed staging and `DocumentPublicationBatch`.
Publication explicitly compares every locked source revision before drive/attempt
validation. Source snapshots continue to acquire shared row locks; self-sources
are destinations and receive exclusive locks from the start. The older generic
`DocumentLedger.lockRevisions` path is retained for arbitrary revision-save
callbacks. Protobuf contracts and native retention guards are unchanged.

The supported publication callback updates destination raw bindings and outbox;
it must not modify a source-only document or acquire a stronger source lock.
Raw-object reference maintenance still has its own locks, and the concurrency
cases here do not qualify shared raw-object contention or provider throughput.
FULL_REVISION owners cannot yet reuse another owner's physical origin. Changing
only the common retention guard would therefore neither demonstrate that future
concurrency nor remove the exclusive locks in the surrounding V22/V39 paths.
Independent revision publication still needs a complete shared-reference and
exclusive-retirement protocol with no lock upgrades before activation.

Real PostgreSQL tests hold one publication after native history/current insertion
and outbox enqueue, then require another destination reading the same source to
commit before release. Both commit and rollback cases first timed out against the
old exclusive source path, then passed after the change. The tests also verify
source UPDATE/DELETE conflicts, source revision changes during an observed row-lock
wait, and no history/reference/outbox leakage from an aborted publication. SQL
fixtures use synthetic verification observations; they do not represent new
provider qualification.

Validation on 2026-10-04 UTC: the final container/service run passed in 2m22s:
108 suites, 947 cases, 944 passed, 3 skipped, no failures/errors. All 60 atomic
publication cases passed. The first full run exposed pre-V41 migration fixtures
calling the mixed-mode SQL function. All-write sets now explicitly use the V37
exclusive batch function; source-only reads require V41. The populated migration
cases passed after this change. There is no database capability probe. Sol reviewed
the final dispatch and documentation with no blocker. Work remains local.

### Retry attempt selection (V42)

Extended internal upload admission; new SQL history/current-pointer persistence.
No protobuf or public RPC changed. V40 rows remain immutable placement anchors;
V42 backfills each exact initial choice, including null for zero-upload members.
Migration excludes concurrent anchor inserts until the backfill and initializer
are installed. It does not require historical selected attempts to remain live.

Internal retry names the expected selection revision and attempt for a subset of
uploading command members. It checks the persisted canonical command, current
policy, source bindings and placement, then stages only the selected uploads.
New attempts, history and pointer changes commit atomically. A conflict in a later
member rolls back earlier replacements. The SQL guard locks drive, attempt and
pointer in that order; the fresh initial-selection guard also locks drive and
attempt before foreign-key enforcement. Expired or cleanup-owned attempts cannot
be newly selected after a lock wait. History is immutable and cannot commit
without advancing its pointer.

Upload encoding is limited to retried members and runs before SQL locks. Stable
lease tokens stay in the prepared command. Replacement adds two client statements
per retried member, at most 64 members. Full-command authorization and retained
source checks still run. No provider I/O occurs inside the selection transaction.
Configuration hashing and end-to-end tail latency still require qualification.

PostgreSQL coverage includes exact CAS, stale expectations, two competing retries,
multi-member rollback, canonical-command mismatch, takeover generation isolation,
zero uploads, populated migration, consecutive replacements and missing-pointer
commit rejection. The cleanup race test holds an attempt lock before selection,
lets its lease expire, inserts a guarded cleanup row and then releases the lock.
Without the explicit shared attempt lock, replacement incorrectly committed; with
the lock it rechecks and rejects the attempt after waiting.

Provider writes, verification and renewal must still bind the exact current
selection before activation. Replacing a pointer does not revoke old tokens.
Terminal outcomes, ambiguous-acknowledgment recovery, independent mixed revisions
and typed schema admission remain unfinished; this checkpoint exposes none of
them as available behavior.

Final local validation: container and service suites passed in 2m41s, covering
108 suites and 964 cases: 961 passed, 3 skipped, no failures/errors. Sol reviewed
the final initial/retry lock ordering, subset behavior, migration and documentation
with no remaining blocker. No hosted CI, push, merge or deployment was performed.

### Selected-attempt lifecycle (V43)

New internal `DocumentSelectedAttemptLedger` renewal and verification batches;
extended SQL guards for NEW_CONTENT post-seal mutations. Existing FULL_REVISION
begin, renewal, verification and publication interfaces are unchanged. No new
protobuf fields, RPCs or provider execution path are exposed.

Renewal accepts 1–64 distinct expected selections under one operation owner.
It locks attempts in PostgreSQL UUID order, checks every member/revision/attempt/
token and current lease/cleanup state, then renews the batch atomically.
Verification accepts 1–256 distinct observations, encodes them before SQL and
updates only exact staged key/size/SHA-256/content-type matches. Already verified
parts also require identical nullable provider version and ETag. Missing keys,
identity mismatches and selection conflicts roll back the whole batch.

V43 extends the V36 guards without changing stored bindings or legacy behavior.
Declarations and PLANNING-to-STAGING sealing remain possible before selection;
post-seal writes require the current selection even when a caller bypasses the
Java helper. The proof is checked after acquiring the attempt row. A fresh lookup
also rejects an old attempt after a selection CAS in the same transaction.
The first red run disabled these trigger checks: displaced renewal and direct
verification incorrectly succeeded, including both same-transaction cases.

Real PostgreSQL cases cover bounded verification, partial completion, replay,
wrong token/revision/member/attempt, duplicate observations, identity mismatch
rollback, displaced direct writes, same-transaction replacement, cleanup winning
an attempt-lock wait, and 64-member renewal with whole-batch failure. These use
explicit synthetic observations and do not establish real-provider verification.
Both verification (1 and 256 observations) and renewal (64 members) pass a ceiling
of nine client statements per transaction. Server trigger work remains linear;
no end-to-end latency or throughput is qualified.

Provider execution still needs preflight/postflight integration, shared byte and
concurrency limits, cancellation and lost-acknowledgment reconciliation with real
adapters. Deletion-only recovery is not upload resumption. Typed admission,
retained descriptors, mixed revision publication and durable terminal outcomes
remain prerequisites before exposing the new operation path publicly.

Final local validation: container and service suites passed in 2m18s, covering
109 suites and 983 cases: 980 passed, 3 skipped, no failures/errors. Sol reviewed
the final SQL, Java batching, lock order, rollback tests and documentation with no
remaining blocker. No hosted CI, push, merge or deployment was performed.

### Provider transfer extraction and selected-attempt adapter evidence

Extended internal composition, no public contract change. `DocumentPartTransfer`
now owns the admitted single-part PUT and bounded read-back comparison shared by
future selected execution and the existing full-revision stager. It checks content
size, SHA-256, content type, provider version and ETag before returning evidence.
It accepts an individual planned part without manufacturing a legacy full plan.
The full stager retains admission, private payload copies, byte reservations,
worker limits, lease renewal, SQL verification and resource draining. Original
exceptions remain the cause of phase-specific staging failures. Read-back bytes
are hashed once rather than again when recording the already matched identity.

`DocumentSelectedTransferIT` explicitly composes the transfer with the selected SQL
lifecycle against PostgreSQL and the real versioned S3 adapter in LocalStack.
It transfers one non-CORE part of a command that also declares an uploaded CORE,
leaving the attempt STAGING. This does not yet prove a complete uploaded subset
with a retained CORE. Other cases hold an actual PUT acknowledgment across a
selection replacement, lose acknowledgment after the real PUT, and delay PUT
until after a cleanup ABSENT observation. Stale completions cannot be verified;
exact-key recovery uses the retained backend and reclaims late versions on a
subsequent pass. The tests do not fabricate successful provider observations.

At this extraction checkpoint, the operation-wide coordinator remained pending.
The internal coordinator added below shares byte/concurrency limits, batches its
heartbeat and flushes observations by count and age. These transfer tests qualify
specific recovery cases, not a public pipeline or end-to-end latency.

Local container and service suites passed in 2m17s: 110 suites, 987 cases,
984 passed and 3 skipped, with no failures or errors. Sol reviewed the transfer
extraction and adapter evidence with no remaining blocker. No hosted CI, push,
merge or deployment was performed.

### Bounded selected-upload payload preparation

Extended internal preparation only. `DocumentOperationUploadAdmission.Prepared`
can prepare and claim private payloads for an explicit member subset. The bundle
binds to that exact prepared plan and attempt IDs. Its keys use member identity
and complete revision ordinal, preserving sparse uploads without requiring CORE
bytes. Exact key-set equality rejects missing bodies and bodies for retained,
empty or unselected slots. Slot, size and declared checksum must match the plan;
the copied bytes are independently hashed before admission or provider I/O.

One shared `PayloadBudget` reservation covers twice the aggregate uploaded bytes
before any payload copy: input copies and bounded read-back capacity. Failed
preparation releases the reservation. A one-use claim transfers ownership to the
coordinator; the caller cannot close an active bundle. Only the coordinator may
lend its arrays to workers and close the claimed handle after all started work
and observation flushing drain. SDK buffers and caller-owned input are outside
this accounting. No new limit is added to the blob SPI's conditional writes.

Unit coverage checks sparse ordinals, retained-only members, wrong slots and
checksums, replacement-plan rejection, explicit subset binding, shared capacity,
partial-preparation failure and the admission facade. These synthetic payload
fixtures establish byte/declaration binding, not protobuf semantic validity. The
subsequent internal coordinator is described below.

Local payload and upload-plan tests passed: 17 cases, no failures or skips. Sol
reviewed resource ownership and command/attempt binding with no blocker. This
checkpoint did not rerun the full container/service suites or hosted CI, and was
not pushed, merged or deployed.

### Internal selected-upload coordinator

Extended internal staging, with no new RPC or publication path.
`DocumentUploadCoordinator` prepares and claims payloads, resolves each original
backend generation once, then invokes SQL admission itself. Backend identity and
required capabilities are checked before admission. Committed attempts must match
the exact prepared UUID set, captured location, realm and declaration count. Retry
uses only explicitly named members and their expected selection revisions.

One coordinator instance shares 32 operation permits and 32 provider-call permits
across its backends, plus the host-supplied byte budget. The host must share that
instance; creating one per backend would multiply these limits. One operation
heartbeat renews its owner and then the complete selected attempt set. Part
workers perform no per-part SQL renewal. The independent observation flusher has
a 256-entry queue and at most 256 pending rows; aggregate pressure flushes the
oldest partial batch even when no individual attempt has 256 observations.
Otherwise it flushes by maximum age and drains tails. Database latency can delay
an age-triggered flush; this is not a bounded-latency guarantee.

Observed worker, heartbeat and flusher failures stop new work; already-entered
provider calls or SQL transactions may still complete. Timed queue offers
recheck cancellation instead of waiting forever after a flusher failure. Every
started task drains before the private payload reservation is released. Closing
the coordinator stops new work; `awaitIdle` reports whether borrowed providers
and SQL resources are still in use. It never closes borrowed provider handles.
Loss of a PUT acknowledgment remains an error and never triggers another PUT.

The returned staging snapshot pairs each exact selection identity with its
verified attempt by UUID, never by separate list positions. It is not durable
publication success: a concurrent replacement may
occur immediately afterward, so publication must re-fence those identities.
Typed/schema admission, mixed revision publication and terminal outcomes remain
disabled. The currently tested paths cover 257 real objects, 64 members sharing a
backend, finite-age flushing during a blocked PUT, lease renewal during that wait,
caller interruption/draining, lost acknowledgment, explicit retries and unsupported
backend capabilities. Tests use PostgreSQL and the actual versioned S3 adapter in
LocalStack. Serialized fixture bytes establish transfer identity, not slot-level
protobuf semantic validity.

Remaining coordinator qualification includes a fully retained CORE with a sparse
upload subset, multiple distinct provider implementations sharing limits, forced
flusher failure under queue saturation, owner takeover/deadline races and query
count/latency measurements. This initial coordinator checkpoint lacked SQL lock
and statement timeouts. The scoped policy added below addresses those database
waits without claiming a whole-operation deadline.

Provider inventory for the remaining cross-provider case: the S3 registration
currently supplies all three capabilities required by this selected-upload path.
Redis supplies non-expiring writes only when TTL is zero, but does not advertise
bounded reads or physical reclamation. Qualifying Redis for this path requires
implementing and testing those behaviors; setting capability flags alone is not
sufficient. This restriction does not remove its existing byte-SPI operations.
The Redis review must also cover its literal namespace/key concatenation and
`$meta` suffix, separately issued byte/metadata commands and ignored requested
version in `get`. Add real-provider collision, concurrent read/write and explicit
unsupported-version cases before admitting it to retained document storage.

Local validation: the full container/service run passed in 2m37s, with 112 suites
and 1,001 cases (998 passed, 3 skipped, no failures/errors). The subsequent staged
result pairing correction passed all seven coordinator integration tests in 16s.
Sol reviewed failure draining, resource bounds and final result identity pairing;
the positional-list hazard it identified is corrected. No hosted CI, push, merge
or deployment was performed.

### Scoped upload SQL timeouts

Extended internal transaction execution; protobuf contracts are unchanged.
`DocumentUploadCoordinator` now requires explicit `SqlTimeouts` from its host.
Positive whole-millisecond lock and statement limits must satisfy lock <= statement
and each be at most one day. No production latency default is inferred from the
test values. Admission, owner renewal and selected-attempt renewal/verification
all use one borrowed `Tx` view with this policy.

The view applies PostgreSQL transaction-local settings before application SQL,
inside rollback handling. One configuration query is added per transaction.
Closing the view does not close its shared entity-manager factory. It rejects
nontransactional `readOnly` calls instead of silently leaving those queries
unbounded. Existing unscoped transaction consumers keep their current behavior.

Real PostgreSQL tests prove lock timeout (55P03), statement timeout (57014),
rollback of preceding writes and settings restoration on the same pooled
connection after both success and failure. A restricted database role makes
timeout setup itself fail; the application callback never runs. Coordinator
tests hold the owner row during admission and the attempt row during verification
after a real S3 upload. Staging fails while those locks remain held, releases
its drained payload reservation and records no false verification.

These are per-lock-acquisition and per-statement limits. They do not bound pool
checkout, network failure, total transaction/operation duration, or provider I/O.
Already-started work must still drain before borrowed resources can be released;
a lost commit acknowledgment still requires reconciliation, not a fabricated
outcome. Whole-operation deadlines and the remaining provider/failure cases
remain qualification work.

Local focused validation passed 14 tests: five transaction-policy cases and nine
coordinator integration cases. Sol reviewed the policy, wiring and evidence with
no blocker. No full-suite rerun or remote publication was performed for this
checkpoint. The one-connection test initially exposed a separate migration-pool
startup defect; at that checkpoint its fixture migrated with two connections and
then restricted the runtime pool to one. The subsequent startup fix below removes
that fixture workaround.

### Single-connection ledger startup

Fixed existing startup behavior; no repository operation or protobuf change.
Flyway previously borrowed its metadata and migration connections from the runtime
Hikari pool. A configured maximum of one therefore exhausted that pool during
fresh migration. `LedgerSingleConnectionStartupIT` reproduced the ten-second pool
checkout failure before the fix.

Flyway now owns separate migration connections using the same JDBC URL and
credentials. Their initialization explicitly retains READ COMMITTED isolation.
The runtime pool keeps its configured limit throughout startup; migration sessions
are additional temporary database connections, not runtime pool members. Future
connection/session settings must be assessed for both paths rather than assuming
Hikari-only settings automatically reach Flyway.

Real PostgreSQL tests prove fresh startup at maximum pool size one, migration,
restart with persisted data, and closure of migration and runtime connections.
The timeout-policy fixture now also starts with one connection rather than
shrinking a larger pool. The startup regression and five timeout-policy cases
passed locally in 10s. Sol reviewed the connection ownership and isolation change
with no blocker. A further real failed-migration case proves constructor failure
closes both connection paths; the final two startup cases passed in 8s.

The full container/service suites passed in 2m34s: 114 suites, 1,009 cases,
1,006 passed and 3 skipped, with no failures/errors. The added migration-failure
test ran afterward against the same production code. Sol also reviewed that
failure-path test with no blocker. No hosted CI, push, merge or deployment was
performed.

### Credential-to-ownership integration gap

New required host integration; existing shared ownership checks remain unchanged.
At `87331b50`, `RepoServices` uses `DocumentGrpcService`'s default caller binding,
which copies principal and process authority but supplies no account/ACL identities.
`RepoServiceMain` and `RepoServiceModule` pass the optional operator token and no
resolver to Netty startup. The interceptor maps that token to `Caller.operator()`;
without interception, `CallerContexts.current()` also defaults to operator.
`UploadHttpServer` checks one optional token, then uses the hardcoded
`http-upload` process caller. The lower-level actions caller and credential
resolvers do not carry repository account membership.

Consequently an operator token must not be described as automatic per-client
ownership. Scoped caller behavior is proven through injected bindings, including
`RepoServiceIT`'s policy server, but production credential mapping is still absent.
The host must bind each credential's effective account/ACL grants, preserve that
scope through delegation and transport adapters, and reject caller-supplied
ownership outside it. Required acceptance uses the actual host wiring with two
keys, forged account IDs, missing/revoked keys and attempted operator escalation.
Scoped creation needs an explicit creation grant. Resolve repository operation
capabilities, account IDs, typed ACL identities and key-specific limits at the
transport edge, preserving an effective grant or grant identity. Current actions
`Caller` resolution discards key identity; principal-only binding cannot recover
key-specific restrictions, and its method scopes are not repository account grants.
Require authenticated operator credentials for externally exposed HTTP raw upload
until its scoped contract exists; today's optional token does not guarantee this.
Keep raw keys outside repository and provider modules. See the composition design's trusted-caller
section; do not place credential storage in the byte SPI or provider modules.

### Redis v2 provider correctness

Extended existing byte operations without changing protobuf contracts. The old
split body/metadata layout allowed namespace/key and `$meta` collisions, torn
reads, glob interpretation of logical listing prefixes, ignored requested versions
and incorrectly sized stream writes. Real Redis regressions demonstrated these
failures before replacement.

The new versioned key tuple and single hash isolate identity components. Lua
operations bind bytes and metadata, reject oversized reads before fetching the
body and apply destination expiry on writes/copies. Stream lengths and allocation
bounds are checked before writes. Managed backend identity excludes credentials
and includes the v2 layout. Bounded reads and exact-key physical reclamation are
now explicit capabilities; conditional writes remain unsupported and their shared
payload bound is unchanged. Standalone Redis 7+ is the supported topology.

This is a breaking physical layout, with no legacy fallback or implicit retained
generation conversion. See `repo/blob/redis/README.md`. Persistence, eviction,
immutable repository key reuse and cross-provider coordinator qualification still
need evidence; these provider capabilities alone do not prove archival durability.

Account/principal ownership is distinct from credential identity. Key rotation
must preserve document ownership; the effective credential grant still determines
allowed operations and delegated scope. Production host integration described
above remains outstanding.

Sol's review identified unbounded custom metadata allocation. A real Redis
regression failed before adding a 256-entry / 64 KiB UTF-8 limit, including content
type, with malformed text rejected before EVAL. Rejection preserves existing
bytes. The Redis/cache/container/service suites and runtime dependency gates passed
in 2m37s before that refinement; the Redis/cache suites passed again in 6s after
it. No hosted CI, push, merge or deployment was performed.
The final Redis/cache rerun passed in 7s after moving metadata/address validation
ahead of body copying or stream consumption and adding an accepted exact-boundary
metadata fixture. Sol reviewed the resource-bound change with no blocker.

### Selected upload across different providers

Existing coordinator behavior now has a real mixed-provider integration case:
one operation stages two parts in versioned LocalStack S3 and two in Redis 7,
with PostgreSQL retaining separate generations, physical profiles and attempts.
The resolver is called once per generation. Successful staging preserves each
member's selected attempt; every planned key reads back from its intended provider
and is absent from the other provider. One shared payload budget returns to zero.

A resolver substituting the S3 identity for the retained Redis identity is
rejected before selection or attempt admission. No fallback to another provider
is allowed. Sol reviewed the test without a blocker. The ten coordinator cases
passed in 22s; the added no-attempt-row assertions were checked in a focused rerun.
No production behavior or public API changed. This proves transfer/routing, not
Redis restart durability, eviction safety, typed admission, retained CORE reuse
or publication. Those obligations remain open. No remote publication occurred.

### Observation queue failure propagation

Fixed an internal staging race without a public contract change. On SQL failure,
the flusher records the failure and clears its queue. That could unblock a timed
producer offer, which previously returned without checking the newly recorded
failure. `add` now checks operation failure after a successful offer as well as
before it. The coordinator already checks the shared failure before returning;
the observed defect was a producer returning normally, not a published revision
or a successful completed coordinator operation.

A real-provider regression builds 513 measured observations through S3 PUT and
bounded GET. It fills a batch, blocks the flusher on a real PostgreSQL attempt
row, confirms that blocker through `pg_blocking_pids`, fills the queue and proves
the extra producer retries its timed offer. SQL lock timeout must fail both
flusher and producer while no rows become verified and the uploaded bytes remain
available for recovery. The case failed before the fix and passed afterward with
all eleven coordinator cases in 26s. This directly tests the flusher's full-queue
failure path; coordinator payload-budget draining has separate integration tests.
Sol reviewed the test and fix without a blocker. Full container/service validation
passed: 114 suites, 1,012 cases, 1,009 passed and three skipped, no failures/errors.
No hosted CI, push, merge or deployment was performed.

### Retained CORE with a changed-part upload

Existing selected staging now has a real retained-content integration case.
`DocumentPartStager` writes and verifies a CORE in versioned S3, and the existing
full-revision publication path commits its actual measured identity to PostgreSQL.
A subsequent command targets that same document revision, reuses the exact CORE
identity and supplies only the ordinal-1 CHUNKS payload. An extra CORE payload is
rejected before creating an attempt or calling PUT. Valid staging performs exactly
one PUT and records one verified CHUNKS object. The original CORE remains both
historically readable and current at the same S3 version and bytes; the current
document manifest and mutation revision do not change during staging.

Sol reviewed the case and requested the current-version assertion in addition to
the pinned-version read. Twelve coordinator cases passed in 28s before that final
assertion; the strengthened retained-CORE case then passed in 14s. This is staging
evidence, not completion of mixed-revision publication,
schema admission, changed-part reads or retention cutover. No public API changed.

### Reusable descriptor fingerprint identity

Extracted the existing mesh descriptor fingerprint/closure algorithm into
`protomolt-descriptors` as `DescriptorFingerprints`. Existing `MeshDigest` methods
delegate unchanged, allowing repository typed admission to reuse canonical identity
without depending on mesh contracts. No protobuf identity, tag or URL changed.
A fixed-byte golden locks file ordering and digest; additional tests retain file
unknown fields, preserve the existing exclusion of set-envelope unknown fields,
and rebuild a multi-file descriptor closure for offline dynamic decoding.
The descriptor and mesh suites passed in 4s after the final fixture refinement.
Sol found no identity-change blocker. This helper is identity computation, not
validation of untrusted closure, a complexity bound or durable schema retention.

The cutover design now specifies the next non-publishing revision preparer:
bounded original-byte materialization, slot/field confinement, global chunk-run
checks, pinned layout identity and shared publication rechecks. Re-serialization
must not replace original evidence or be used as a byte-equality admission rule.
This preparer remains unimplemented. No hosted CI or remote publication occurred.

### Document fragment field confinement

New pure codec helper `DocumentFragmentConfinement` checks generated Document
fragments against the fixed `protomolt-document-parts/v1` policy before assembly.
It rejects mismatched document IDs, misplaced known fields, non-CORE unknown root
fields and CHUNKS parent fields outside semantic results. CORE and owned payload
subtrees may retain unknown fields. Presence matches the existing splitter,
including explicitly present empty BlobBag messages. No physical bytes are
rewritten, and the check does not require reserialized bytes to equal input bytes.

Tests cover actual split output, ownership/title injection, missing content,
unknown fields in allowed and forbidden positions, and a valid overlong varint
whose bytes change on reserialization. Sol reviewed the helper without a blocker.
The caller still must bound parsing, verify the generated descriptor against its
pinned identity and preserve original bytes. Whole-revision assembly, global chunk
subkeys/order, schema validation, authority and publication are not established by
this helper. It is not yet wired into repository admission or an advertised RPC.
The final codec suite and its runtime dependency boundary check passed in 2s.
No hosted CI, push, merge or deployment was performed.

### Ordered CHUNKS confinement

New `DocumentChunkSequence` checks one revision's CHUNKS in manifest order. It
applies fragment confinement, bounds the aggregate semantic-result count, rejects
adjacent fragments that split an effective-key run and validates exact generated
subkeys. Failed instances cannot be reused. It consumes parsed views without
rewriting physical bytes. Complete assembly/admission wiring remains outstanding.

The splitter and checker share `ChunkRunNames`, preserving first-unused suffix
behavior with a set and suffix cursor instead of repeated list scans. A seeded
5,000-name compatibility case compares against the original allocator. Existing
generated-name/explicit-ID collisions are retained and explicitly tested. Codec
and dependency-boundary checks passed in 2s. This establishes structural lookup
improvement, not measured end-to-end latency. No protobuf contract changed.
Sol reviewed the allocator, bounds and rejection behavior without a blocker. Run
naming is part of the pinned v1 fragment policy. No hosted CI or publication ran.

### Bounded structural revision assembly

New pure `DocumentRevisionAssembly` retains immutable ByteString fragments and
produces a parsed Document view after aggregate byte/count preflight, unique slots,
exactly one CORE, fragment confinement and ordered chunk validation. It bounds
parser recursion, checks complete consumption and checks cancellation between
fragments. Original bytes remain separate from reserialized semantic content.
Tests include complete split/assembly, CORE-only data, noncanonical encoding,
malformed input, end-group/trailing fields, wrong identity, depth, exact/over-limit
bytes/fragments/chunk elements and cancellation.

Sol reviewed the design and resource boundaries. A suspected end-group parsing
bypass was not reproduced by the generated Document parser: the malformed-input
test passed without the explicit last-tag/consumption checks. Those checks remain
as defensive boundary requirements, not a claimed red/green defect fix.
The codec suite and dependency gate passed in 2s. No provider qualification,
annotation/Any validation, descriptor retention, authority or publication follows
from a structural result. Host memory reservations must cover decoded expansion;
raw byte limits alone are not heap bounds. Repository preparation remains open.

### Canonical command to decoded content binding

New package-private `DocumentCommandContent` checks a canonical member's exact
full-ordinal materialized byte set, including retained declarations and EMPTY gaps.
It checks aggregate bounds before hashing/parsing, hashes ByteString buffer views
without cloning the entire body, invokes structural assembly and requires decoded
ownership to equal the command. A host-required schema policy or an explicit schema
condition fails unsupported until validation and retention are available. Omission
of the request field cannot bypass the supplied host requirement.

A review found and a deterministic regression reproduced a map-view race: checking
the live key set before copying could miss an extra copied ordinal. The exact-set
check now uses the same immutable snapshot used for hashing and assembly. The host
must still supply a stable bounded map; copying an adversarial growing collection
is not a heap bound. Seven content cases plus the codec suite passed in 3s, including
wrong decoded ownership, corrupt bytes, missing/sparse ordinals, retained digest
mismatch, schema-policy refusal, cancellation and limits. Sol reviewed the final
fix without a blocker. These tests do not fabricate provider success or establish
retained storage provenance. The helper is not yet integrated with live repository
preparation, authorization or publication. No remote publication occurred.

### Preparation integration boundary audit

The next preparation path extends existing behavior rather than adding a new RPC.
Source inspection found that bounded provider reads and their shared budgets live
in `repo/engine`, which already depends on `repo/container`. The revision cutover
design now places preparation orchestration in the engine and leaves durable
evidence/fencing in the container; its earlier container-orchestration instruction
would have encouraged a dependency cycle or duplicate provider I/O.

New internal work is an exact, ledger-issued preparation read plan and a scoped
fresh-payload handoff. Existing reader selection is extended to verify complete
command object identity and content type, not merely source slots. Upload staging
must extend heartbeat and private-byte lifetime through preparation, including
zero-upload members. Authentication, protobuf fields, public reads and publication
behavior are unchanged by this design checkpoint.

Existing `DocumentPartReaderIT` cases provide reusable regression coverage:
`mixedBindingsPreserveOrderVersionsAndOneAggregateBudget`,
`boundReadUsesOriginalNamespaceLocallyAndOverGrpcAfterDriveChanges`,
`readsRecordedVersionAfterLatestBytesAreReplaced`, and
`cancelledCallsRetainCapacityUntilProviderReturns`. These were inspected, not rerun
for this documentation change. They do not establish the new exact-plan boundary,
content-type check, scoped payload handoff or document reader-versus-prune safety.
Those acceptance cases remain required by the updated cutover design.
Sol reviewed the boundary against the implementation without a design blocker;
its memory-accounting clarification is included. Documentation whitespace checks
passed. No code, protobuf, hosted CI or deployed behavior changed.

### Retained content type on managed reads

Extended the existing managed publication read projection to carry the mandatory
SQL content type into `DocumentPublicationLedger.Part`. Missing or blank managed
evidence fails instead of becoming unknown. `DocumentPartReader` now checks the
provider's exact content type alongside length, digest, version and etag. The
seven-argument Java constructor preserves explicitly unknown legacy snapshots;
no protobuf field or provider request changed and no extra provider call was added.

Real PostgreSQL/S3 tests first reproduced acceptance of incorrect and missing
content types. Fault injection modifies only the type on a real bounded GET result.
Both cases now return DATA_LOSS through local and in-process gRPC invocation.
A positive managed-read assertion checks the retained SQL type. Legacy reads retain
their existing behavior because old manifests have no content-type evidence.
This closes a production read integrity gap; it does not implement the exact
preparation plan or establish typed schema validity.

All 55 managed/legacy reader cases passed, followed by 102 publication and revision
projection cases in 37s and the engine runtime dependency gate. An initial ledger
filter matched no tests; the corrected explicit class filters produced that ledger
result. Sol reviewed the final implementation without a blocker. No hosted CI,
push, merge or deployment ran.

### Ledger-issued retained read evidence

New internal `captureRetainedReads` returns an immutable `DocumentRetainedReadPlan`
bound to the canonical command, authenticated principal and owner generation. The
same SQL transaction checks live ownership, persisted command bytes, current
source/destination permissions and mutation revisions, full retained object claims,
and registered backend profiles. It checks owner expiry again after those waits.
Entries preserve command order and full ordinals, including gaps for fresh parts.
It creates no upload attempts or operation selections, including reuse-only cases.

Tests exercise eleven forged physical/slot claims, invalid callers before SQL,
source/destination denials and stale revisions, sparse ordinals, immutable entries,
policy revocation and reuse-only capture. The coordinator's existing real PostgreSQL
and S3 source fixture captures the exact published CORE identity and reads its
recorded version, checking hash and content type. Other admission fixtures remain
synthetic SQL declarations; they do not establish provider success.

The plan is point-in-time evidence, not a reader pin, fresh-selection fence,
semantic validation result or publication permission. Engine consumption, reader
protection, payload lifetime and post-I/O checks remain required. No new RPC or
protobuf field is introduced. Sol reviewed the SQL boundary; its public-accessor
finding was fixed by exposing principal text rather than an inaccessible ledger
key type. Account and operation UUID remain available from the canonical command.
The final 82 selected admission/coordinator cases passed in 27s. No hosted CI,
push, merge or deployment ran.

### Engine consumption of retained read plans

Extended `DocumentPartReader` with `readRetained(plan, memberId, control)`. It
validates member membership, selects the ledger-issued entries in command order,
and uses the existing bound-part reader with the exact key, size, digest, version,
content type and original backend binding. It neither invents a Publication nor
adds a second I/O scheduler. Returned parts align positionally with the filtered
plan entries; full ordinals remain in the plan. Keep the batch open through use.

The real PostgreSQL/S3 coordinator fixture now exercises this reader, including
unknown-member refusal, cancellation before backend resolution, a shared budget
held through batch lifetime, and release after an injected content-type mismatch
on a real bounded GET. A later drive namespace change and a real overwrite of the
latest object do not change the captured version's bytes. A test-only container
dependency on the engine supports this cross-module fixture; production dependency
direction is unchanged.

This remains a low-level unpinned read boundary. Assembly, scoped fresh-byte handoff,
reader-versus-prune protection and post-I/O SQL fencing remain outstanding. The
test does not claim concurrent-cleanup safety or a public mixed-revision API.
All 67 coordinator and managed/legacy reader cases passed in 32s, with the engine
runtime dependency gate. Sol found no blocker; multiple retained inputs with
intervening upload/EMPTY ordinals still need an end-to-end positional mapping case
when integrating assembly. No hosted CI, push, merge or deployment ran.

### Sparse retained reads and protection design

The missing positional-mapping case now uses real staged S3 objects and guarded
SQL publication. A command orders fresh CHUNKS, retained CHUNKS, EMPTY BLOBS and
retained CORE. Its captured ordinals are 1 and 3; the bounded reader returns the
two exact original bodies in that order, reversing their source publication order.
Exactly two bounded GETs occur, the aggregate reservation survives through batch
use, and no upload attempt is created. The source fixture was extracted for reuse
by the earlier CORE carry-forward case. These bytes prove storage/order behavior,
not semantic assembly or typed validation.

The cutover design now specifies document pins using the existing reader
incarnation/quiescence foundation, atomic whole-plan acquisition, sorted shared
origin/retention locks, independent retention through logical source deletion,
and release only after actual provider calls and protected batch owners drain.
This is a proposed implementation with explicit race/recovery/performance tests,
not a claim that document reader protection has landed.

All 13 coordinator cases passed in 29s after correcting a test compilation error
in a Part accessor. No production code, hosted CI, push, merge or deployment changed.
Sol reviewed the fixture and design; its distinction between deduplicated physical
pins and all canonical source claims is incorporated, along with the host/SQL
authentication boundary.

### Native document reader pins

V44 adds `document_read_pins`, the DOCUMENT_READER native/reference mirror, a
shared origin/retention lock branch, and exact single-pin release. Admission
requires an ACTIVE reader incarnation, a verified object in a sealed current
revision, existing history/current retention and an object not retiring or
reclaiming. Pins have no expiry and no source-row foreign key. Native identities
cannot be updated; detached mirror insertion and premature mirror deletion fail.
Exact release checks incarnation and object and permits acknowledgement retries.
Existing archive and durable document reference predicates/modes are preserved.

Real PostgreSQL cases cover mirrored ownership, wrong-incarnation release,
immutable identity, fenced admission, structurally forged source/mirror rejection,
source replacement, retirement refusal and overlapping shared-reader transactions.
The fixture supplies explicitly synthetic SQL content evidence and does not claim
provider I/O or typed validation. This migration is a foundation only: no document
read currently acquires these pins. Atomic whole-plan acquisition, lifetime/drain
ownership, quiescent recovery and cleanup races remain integration gates.

Sol reviewed the migration and tests without a blocker. Full container/service
reports contain 116 suites and 1,031 cases: 1,028 passed, three skipped, no failures.
The first run completed service tests but failed container test compilation on an
ambiguous lambda overload; after correction the container suite passed in 1m56s,
with service results up to date. No hosted CI, push, merge or deployment ran.

### Atomic retained-plan pin acquisition

New internal `capturePinnedReads` retains normal command/owner/authorization checks,
requires an ACTIVE reader and captures all exact source claims. `DocumentReadPins`
prepares pin UUIDs and deduplicated physical witnesses before SQL, then locks the
complete distinct origin set in PostgreSQL UUID order followed by all retention
objects in PostgreSQL UUID order. It inserts the whole pin set in one statement;
the ending owner check participates in the same transaction. All claims are
rechecked after origin locking, including future shared-object/non-witness sources;
deduplicating physical protection never removes canonical claim validation.

Tests verify two-object acquisition, complete native/mirror rollback when the last
object in SQL order is retiring, unknown-reader refusal and two members retaining
two claims but sharing one physical pin. Existing source/owner/authorization cases
remain in the affected suite. Sol reviewed the lock discipline; the additional
post-origin source proof was incorporated from that review. Host read lifetimes,
quiescence/recovery and overlapping large-plan races remain unqualified. No public
API or provider path uses this internal handle yet.
The final 91 admission/native-pin cases passed in 27s after fixing an ambiguous
test assertion overload. No hosted CI, push, merge or deployment ran.

### Atomic document pin release

V45 adds `release_document_read_pins` for 1–10,000 complete, unique pin claims;
multiple pins may protect the same object. The function validates extant identities,
requires registered document objects and durable origins, locks all origins then
retention objects then native pin rows in PostgreSQL UUID order, rechecks identities,
and deletes the matching native/mirror ownership in one transaction. Missing pins
allow lost-acknowledgement replay, but a still-existing mismatched pin aborts the
batch. `DocumentReadPins.release` encodes before opening the transaction and uses
one client statement; an empty captured set requires no SQL.

Real PostgreSQL tests cover a wrong identity preserving both pins, duplicate pin
and unregistered object refusal, successful release/replay, and concurrent reverse-
order requests for two pins sharing one object. The latter exercises overlap but
does not establish large-plan cleanup fairness or throughput. Caller drain proof,
host integration and recovery remain separate requirements: this release function
must never be used to infer that provider I/O has stopped.
All 94 admission/native-pin cases passed in 27s. Sol found no blocker; cross-object
partial-overlap and large-plan cleanup races remain qualification work. No hosted
CI, push, merge or deployment ran.

### Quiescence-gated document pin recovery

V46 adds `recover_quiesced_document_read_pins`, which requires an existing reader
in permanent QUIESCED state before invoking V45's atomic, identity-bound batch
release. ACTIVE, FENCED and unknown readers cannot recover pins; another quiesced
reader cannot release them either. `DocumentReadPins.recover` uses this gate and
preserves retry semantics. No time-based expiry or provider-drain inference is added.

Real PostgreSQL tests exercise refusal, retained references after refusal, successful
recovery and replay, including the Java captured-plan wrapper. Their direct SQL
quiescence attestation is synthetic lifecycle evidence, not proof that a host has
drained provider work. The 103 admission, document-pin and archive-incarnation cases
passed in 29s with no failures or skips. Actual read/batch lifetime ownership,
recovery discovery and cleanup races remain integration work. No hosted CI, push,
merge or deployment ran.
Sol found no blocking recovery-gate or identity issue. The cutover plan records the
reviewed batch-admission barrier and separate setup lifetime for the next integration;
the current provider path does not yet use this protection.

### Protected document read lifetimes

`DocumentReadLedger` registers a fresh reader incarnation and counts admission
before capture enters SQL. Each captured plan admits Uses until plan close or
host fencing. A Use can transfer its already admitted lifetime to a batch after
fencing without increasing the count or opening a gap. Closing plans and Uses
does only local accounting; a latch reports drain without executing callbacks on
provider threads. The coordinator explicitly releases after drain, with visible
SQL failures and serialized retry/recovery. Local quiescence requires fenced
admission and every captured plan to have drained, even if pin release failed.

The protected `DocumentPartReader.readRetained` overload holds the Use through
setup, resolver work, provider calls and returned batch ownership. The existing
batch admission barrier prevents closed batches from starting provider calls and
retains protection for workers already entered. Cancellation remains prompt.
The raw-plan overload is unchanged and still requires caller-supplied protection;
production host mounting and crash-recovery pin discovery are not implemented.

Real PostgreSQL cases cover fresh-identity refusal, failed capture, transfer after
fencing, multiple Uses, duplicate closes, release lock timeout with durable pins,
and recovery after local drain. A blocked SQL capture races host fencing: local
quiescence refuses until capture exits, then durable ACTIVE-state checking refuses
that capture. Real versioned S3 reads cover sparse fragment order, open-batch pin
retention, unknown-member and exhausted-budget cleanup, and cancelled reads whose
provider wrapper deliberately ignores interruption after actual GETs. Pins and byte
reservations remain held until those workers return; no synthetic provider success
is used. Fixtures prove storage lifetimes, not typed content validation.

Sol found no blocking race. Final qualification passed 240 cases in 16 suites
(container admission/coordinator, engine and service reader/legacy-reader suites),
with no failures or skips; the engine runtime boundary gate passed. Initial test
compilation exposed missing cancellation-control methods, which were corrected.
The final invocation completed in 33s. No hosted CI, push, merge or deployment ran.

### Bounded discovery for quiesced-reader recovery

V47 and `DocumentReadRecovery` recover durable document pins without an original
plan handle. The SQL gate requires permanent QUIESCED state even for an empty
reader and limits each discovery to 1–10,000 claims. A composite reader/pin index
replaces the reader-only index; discovery takes no native-pin row locks before
delegating to V46/V45's origin/retention/pin lock order. The returned count means
claims observed, not rows deleted. Concurrent workers may select the same claims;
zero establishes drain because a quiesced reader cannot acquire new pins.

Real PostgreSQL cases cover invalid Java and direct-SQL bounds, empty-reader gates,
257 claims in 64-claim batches across two physical objects, another reader's pins
remaining untouched, and two recoveries held after selecting the same claims.
An injected AFTER DELETE failure runs after the real mirror deletion; rollback
restores native pins and mirrors, and fresh discovery subsequently succeeds.
The quiescence fixture is direct SQL evidence, not proof of remote process shutdown.

Sol found no blocker. A losing recovery may still fail if concurrent cleanup removes
an identity after another worker releases its pin; fresh discovery is retryable,
and failures are never reported as a successful drain. This slice does not qualify
all cleanup races, implement recovery scheduling or mount the production host.
Final qualification passed 111 cases across native document pins, upload admission
and archive-reader incarnations with no failures or skips in 29s. An initial
ambiguous transaction lambda was corrected before the successful runs. No hosted
CI, push, merge or deployment ran.

### Reader admission, retirement and legal document deletion

Real PostgreSQL qualification now races pin admission with guarded retirement in
both transaction orders. The test observes the contender blocked by the holder
before committing it: an earlier pin survives retirement and remains releasable;
retirement committed first prevents a later pin. Retirement takes origin locks
before retention locks. These tests establish transaction ordering, not measured
production latency or throughput.

A separate case deletes through `DocumentLedger.deleteByNodeId` and proves that
exactly HISTORY and READER references remain for the object. A new reader cannot
pin the deleted current source. Existing reader release succeeds, but reclamation
still fails because history retains the bytes. V38's immutable history/parts and
the published-attempt cleanup guard prevent a supported sole-reader state today.
The cutover plan therefore retains sole-reader reclaim as an open acceptance case
for the future reviewed history-pruning path, including its JCR/restore assessment.
No guard is disabled, no pruning API is added and no provider deletion is claimed.

Sol found no blocker in the tests or their scope. Native-reader and revision-
projection suites passed 33 cases with no failures or skips. No hosted CI, push,
merge or deployment ran.

### Scoped fresh-upload preparation handoff

The internal `DocumentUploadCoordinator.stageAndPrepare` callback now runs after
real upload/read-back and SQL verification while private payload ownership, the
operation permit and owner/selected-attempt heartbeat remain live. Zero-upload
commands run the owner heartbeat and callback without inventing an attempt or
starting a flusher. Ordinary stage/retry keep heartbeat through final verification.

The callback receives a scoped read-only buffer view keyed by full member/ordinal,
not the payload owner. Its accessors are invalidated and internal references cleared
before the payload reservation closes. Borrowed buffers remain a trusted internal
contract: they cannot be retained beyond the callback without separate accounting.
This avoids an extra provider GET or an unreserved assembly copy. Decoded expansion
and engine assembly integration still require their own bounds and implementation.

Custom preparation repeats canonical command, authorization and retained-source
checks; compares every immutable initial member selection including zero-upload
rows; and renews the owner and exact selected attempts. Control is checked between
those transactions. These checks refresh staging context, not one atomic admitted
draft or publication fence. The heartbeat drains and its sticky failure is checked
before returning. The callback cannot publish or perform semantic review.

Real PostgreSQL/S3 tests verify owner and selected-attempt renewal while preparation
is held open, zero-upload reuse of retained content, exactly one PUT/read-back per
fresh part with no additional preparation GET, read-only/expired view behavior,
callback/cancellation/owner-expiry failures and selection replacement during the
callback. Failure releases memory but leaves verified attempt data for recovery.
The initial empty-CORE fixture was rejected by the runtime validator and replaced
with valid retained-source evidence; content-type fixture construction was corrected.
Sol found no blocker. All 117 coordinator, upload admission and payload cases passed
in 33s with no failures or skips. No hosted CI, push, merge or deployment ran.

### Structural wire budget before document decoding

`DocumentRevisionAssembly.Limits` now requires an explicit fifth argument,
`maxWireValues`; all in-repository Java callers were updated. No protobuf names,
tags, imports or Any URLs changed. `DocumentWireBudget` scans every fragment before
creating the decoded Document builder, using one aggregate counter for field
occurrences and packed scalar elements. It descends known message fields through
their generated descriptors, counts duplicate singular fields and map-entry fields,
and recursively counts unknown groups. Unknown length-delimited data stays opaque
and subject to the aggregate raw-byte limit. It does not copy field payloads.

Both packed and unpacked numeric encodings are accepted independently of the
descriptor's packing preference. Fixed-width packed lengths must align; packed
varints are scanned within their declared limit with cancellation checks. Truncated
or negative nested lengths, unmatched groups and excessive depth fail before
decoding. Original physical bytes remain unchanged, including noncanonical wire
representations accepted by the real parser.

Tests compare real protobuf parser behavior and cover packed/unpacked values,
unknown and nested groups, duplicate singular fields, wrong-wire known fields,
malformed nested lengths, map entries and one aggregate budget across fragments.
Sol found no blocking issue; its suggested nested-group and nested-length regression
cases were added. All 58 codec/content-check cases in seven suites passed with no
failures or skips, including the codec runtime dependency gate. The final invocation
completed in 2s. This is a structural allocation-input bound, not a heap, merge-cost,
Any-decoding or schema-validation guarantee; production scan latency remains
unqualified. No hosted CI, push, merge or deployment ran.

### Self-contained descriptor closure primitive

Added `ClosedDescriptorSet.load(ByteString, Limits)` in `protomolt-descriptors`.
This is a new Java utility; no protobuf names, tags, imports or Any URLs changed.
The existing classpath loader remains unchanged. Serialized bytes are bounded
before protobuf parsing, which retains its recursion guard. File and dependency
counts are bounded before graph assembly. An iterative dependency-first traversal
rejects cycles and limits import depth; declared import order is preserved for
public-import indexes. Every import, including well-known protobuf files, must be
present. Duplicate/unnamed files, duplicate imports and ambiguous full message
names across disconnected files or nested/package boundaries are rejected.

The loader does not resolve registry aliases, authenticate fingerprints, interpret
custom validation options, certify unsupported-rule handling, retain artifacts,
or provide a heap/latency guarantee. Artifact-wide uniqueness of other protobuf
symbol kinds is not certified. Runtime typed admission remains fail-closed until
its schema, validator and persistence integration is complete. Existing canonical
schema fingerprints and exact artifact-byte digests are still distinct identities.

Real protobuf tests cover retained-descriptor dynamic decoding, missing ordinary
and well-known imports, duplicates, unresolved field types, cycles, each resource
boundary, reverse-ordered deep chains, public-import ordering, malformed bytes and
excessive serialized nesting. Sol's review identified message-name ambiguity; the
implementation and regression cases address it. All 94 descriptor-module cases in
12 suites passed with no failures or skips in 2s. No hosted CI, push, merge or
deployment ran. The next integration gate is explicit schema/type identity and
validation-option handling, followed by durable retention and offline restoration.

### Validation options recovered from retained descriptors

Retained-descriptor tests now pass the strict closure loader's output to the real
ProtoValidator and ProtovalidateRuleSource. Valid data passes; numeric bounds and
cross-field CEL rules still reject invalid data after option extensions have been
serialized as unknown fields. This is local descriptor/validator integration, not
durable archival admission or a registry-outage restore proof.

Red tests exposed malformed outer validation options being treated as absent:
a known option number with the wrong protobuf wire type survives reparsing as an
unknown field, after which the previous code used default rules. OptionReparse now
recovers targeted option bytes and rejects any residual occurrence of that option
number. Field, message, real-oneof and predefined-rule declaration readers all use
the check, including when valid and invalid occurrences coexist. Unrelated options
are preserved. No protobuf declarations or wire identities changed.

Sol reviewed the fix; regression tests cover the malformed options and mixed
occurrences. The initial oneof fixture violated protobuf synthetic-oneof ordering
and was corrected. Validator construction alone does not compile these rules;
the integrated assertions invoke validate to exercise compilation. All 28 dialect
and 13 conformance-harness unit tests passed without failures or skips; this was
not a rerun of the external upstream conformance corpus.

Typed admission remains disabled. Before enabling it, close the separately
identified unsupported-rule paths: unknown fields inside recognized rule payloads,
unknown enum numeric values, and unrecognized or incorrectly encoded predefined
extensions. Preserve supported custom predefined rules while rejecting rules that
cannot be interpreted. Also bind schema/type identity, retain exact artifacts,
assemble admission evidence and prove historical decoding without the registry.

### Unsupported validation rules

Red tests confirmed that unknown Ignore/KnownRegex numbers passed validation.
The vendored schema uses proto2: generated parsers keep unknown enum numbers in
unknown fields. Tests encode those numbers in retained descriptors.

RulePayloads now checks validation instructions before translation. It rejects
unknown rule numbers and invalid encodings of known fields, including enum values,
collection rules and CEL definitions. Checks also run for unset data fields,
empty collections and IGNORE_ALWAYS. Unrelated custom options remain intact.

Custom rule numbers require an entry in the descriptor index for that rule type.
Their values must decode fully; singular values must be present. Empty packed
repeated values remain legal. This check does not inspect custom message-valued
CEL parameters. Schema-wide coverage and resource limits still need qualification.

Sol found no blocker. All 35 dialect tests and 13 conformance-harness unit tests
passed without failures or skips in 3s. Existing valid custom rules pass. The
external upstream test suite was not rerun. Typed archival publication remains
disabled until schema/type binding, retention and admission integration pass.
No protobuf definitions changed.

### Schema preparation independent of candidate values

ProtoValidator.prepareSchema is a new Java API. It checks all message types
reachable through ordinary message fields, including repeated values and map
entries. An identity set prevents cycles. Type and field limits apply to discovery
before any rules compile. Control checks run during discovery, between compilation
steps and before success. They do not impose a deadline on one CEL compilation.

Tests show that an empty parent can pass normal value validation without checking
invalid rules on an unset child. Explicit schema preparation rejects those rules
for singular, repeated and map fields. Recursive graphs, exact limits, exceeded
limits and cancellation/retry are covered. Existing validate behavior is unchanged.

Hosts must prepare Any payload types and extensions separately. Unreferenced
nested definitions are excluded. Compiled rules may leave the bounded cache;
preparation is not a receipt or a value check. Typed archival admission still needs
to bind this step to retained schema identity and candidate validation.

Sol found no blocker. All 226 validation-core, 40 dialect and 13 harness unit tests
passed with no failures or skips in 7s. No protobuf definitions changed. The
external upstream test suite was not run.

### Publication schema identity binding

DocumentSchemaBinding is an internal engine helper using the existing
PublicationSchemaCondition. It validates that condition, rejects unknown condition
fields, loads a bounded complete descriptor artifact, finds the exact full message
name and checks the canonical root-import closure fingerprint. Extra files outside
that closure are rejected. Nested message names are supported.

The binding preserves the original immutable artifact bytes and computes a separate
SHA-256 for those exact bytes. File ordering and unknown set-envelope fields can
change that hash without changing the canonical schema fingerprint. Both identities
retain their existing meanings; no protobuf definitions changed. The engine adds
only the descriptors utility dependency, without a registry or mesh client.

Sol reviewed the code and tests without a blocker. Tests cover nested types,
missing types/imports, descriptor changes, extra files, invalid conditions,
cancellation and distinct digest meanings. All 86 engine tests in 13 suites passed
with no failures or skips in 2s, including the runtime dependency gate.

The helper is not yet connected to admission. Any URL policy, schema preparation,
candidate validation and durable retention remain required steps. This binding is
not validation evidence, authorization or a publication receipt.

### Typed payload checks

MessageWireBudget moved from repo-codec to core/descriptors. Document assembly
reuses the same wire counter. Codec now depends on descriptors; the dependency gate
passes. The scanner limits occurrences and nesting, not exact heap use.

DocumentPayloadCheck requires an exact host-selected Any URL matching the schema.
It limits payload bytes, checks reachable types, prepares rules, scans wire format
and verifies complete decoding before invoking ProtoValidator. Errors and
cancellation prevent a checked result. Successful results retain original bytes.

The host supplies required rule dialects and manages byte and decoded memory.
Nested Any and extension-bearing schemas are unsupported, including unset fields.
Unknown candidate fields retain byte/occurrence limits but receive no rule checks.
Admission policy must decide whether to permit them. This internal helper provides
no semantic review, authorization, retention or receipt. Publication stays disabled.

Sol found no blocker. Tests exercise real length and CEL rules, exclusive choices,
invalid schema CEL, URL/type mismatch, malformed bytes, limits, duplicate tags,
unsupported schemas, cancellation and unknown fields. All 94 descriptor, 51 codec
and 95 engine tests passed without failures or skips in 3s. Both repository runtime
dependency gates passed. No protobuf definitions changed.

### Descriptor retention implementation decision

Reviewed the current physical-location kinds, retention mirrors, V38 revision
projection and optional registry artifact store. None supplies repository-owned
schema retention. The cutover design now selects a bounded SQL artifact catalog,
staging claims and immutable revision/path references. Descriptor bytes stage
before publication; reference insertion joins the atomic revision commit.

The design covers account isolation, historical ownership, explicit legacy gaps,
operation-first lock order, implicit SQL locks, bounded obsolete-claim cleanup and
aggregate limits. It preserves the optional JCR boundary and existing protobuf
identities. This is design work; no migration or retention API was implemented.

### Schema artifact staging checkpoint

V48 adds account-scoped immutable descriptor bytes and operation-generation staging
claims. Java stages a sorted batch under the existing owner fence. SQL verifies
byte identity and enforces per-artifact and accumulated per-generation limits.
Exact retries reuse claims, and a new artifact cannot commit without its creator's
claim. No public operation or protobuf identity changes.

Twelve new PostgreSQL tests and sixteen existing owner-fence tests pass. Coverage
includes exact descriptor bytes, account scope, replay at count and byte limits,
rollback, missing creator claims, invalid digest, expired and replaced owners, and
refusal of catalog/claim mutation. Cleanup, revision references, historical reads,
publication and performance qualification remain unfinished. This is internal
staging storage, not completed schema retention. Runtime Any resolution remains a
required integration, as recorded in the cutover design.

### Runtime Any validation and existing peer capabilities

The internal payload checker now accepts unset Any fields and resolves populated
Any envelopes through host-selected immutable schema bindings. It validates nested,
repeated and map payloads, caches each exact URL for the check, and shares byte,
wire-value, depth and schema limits across embedded messages. Original candidate
bytes remain unchanged. The returned URL map is in-memory evidence only; durable
per-path revision bindings and publication are still required.

All 101 engine tests pass, with no failures or skips, including runtime compilation
of a source-only type through the existing ProtoSourceCompiler and reconstruction
from retained descriptor bytes without another compilation. This is a helper test,
not proof of SQL-backed historical reads. The engine runtime dependency gate passes;
the source compiler is a test dependency only. Sol reviewed the recursive check and
found no blocker.

Existing service discovery already provides much of the peer workflow:
ServiceActionSupport.reflectAndStore obtains live reflected descriptors and writes
them through SchemaRegistryStore.putDescriptorSet. GitSchemaRegistryStore stores
those bytes at descriptors/sha256/<fingerprint>.pb and commits them. Service profiles
persist separately. ReflectedServiceActions and ReflectedMethodAction rebuild
methods and invoke them dynamically using DynamicGrpcCalls. No peer executable
library or generated Java class is required. The MCP host composes these facilities.

Tests cover reflection with dynamic Any invocation, registry-backed invocation,
profile restart and invocation, and Git descriptor persistence across restart.
The audit did not locate one test spanning reflection, Git commit, reopening both
stores and invocation together. Add that integration coverage rather than building
a duplicate discovery or persistence path.

GenerateStubsAction already emits source for Java, Kotlin, Python, C++, C#, Ruby,
PHP and Objective-C, plus grpc-java service stubs. Generated-source ZIP packaging
was reported by the user; its exact codegen download route is still being located.
Do not equate source generation or ZIP download with executable peer code loading.

### Direct SQL staging limit verification

Additional PostgreSQL cases fill a generation to the artifact-count or byte limit,
then attempt a direct claim INSERT for an artifact staged by another operation
in the same account. The claim trigger rejects both attempts. Previously committed
claims remain intact. All 14 staging tests pass; Sol found no blocker.

The cutover design records reuse of MappingHelper, the mapper's Any-aware field
access, MetadataExtractor's CEL selectors and MessageProjection. The remaining
path work is persisted occurrence provenance, not another selector engine.

### Reflection to Git restart proof

GitBackedReflectedAnyActionTest now exercises the combined existing path: discover
an in-process gRPC service through reflection, commit the descriptor bytes through
GitSchemaRegistryStore, reopen the registry and filesystem profiles, build a fresh
action catalog, and invoke an Any request/reply dynamically. It asserts the Git
commit and retained bytes, and verifies invocation makes no further reflection
calls. Only test dependencies changed; production code was reused.

All 25 service-workspace tests pass without failures or skips. This proves client
reconstruction from retained service definitions. Repository revision retention,
archival authorization and publication remain separate unfinished work.

### Recovery lock proof checkpoint

V49 adds a recovery-only operation lock proof. A dedicated update preserves the
owner token, generation, lease and write authority while recording the actual
transaction ID. Ordinary admission, renewal and takeover clear that recovery proof.
The nonlocking check uses the exact operation and transaction. Expired-owner
recovery does not grant live-write permission.

All 37 PostgreSQL cases pass: 7 recovery cases, 16 existing write-authority cases
and 14 schema-staging cases. Tests cover unchanged ownership, repeated calls,
rollback, proof isolation, forged identifiers and rejected renewal or staging after
expiry. A SQL barrier confirms that competing takeover waits on the recovery lock;
a separate test uses the Java takeover API and checks the resulting generation.
Sol found no blocker. Claim deletion, artifact collection and terminal retention
policy remain disabled and unfinished.

The design also requires compiler and runtime provenance with archived Any schema
assets. Remote compiler versions remain explicitly unknown when not supplied;
local toolchain versions must come from actual build metadata.

### Replaced schema claim release

V50 permits deletion of an obsolete generation's staging claim only when the same
operation's current generation has a claim for the exact artifact. The recovery
transaction locks the owner, selects a batch, locks selected artifacts in hash
order, then locks and deletes those claims. The trigger enforces recovery proof
and exact replacement even for direct DELETE statements.

Discovery starts with the current generation's maximum 64 claims and uses bounded
indexed probes for older matching claims. The batch limit is 1 to 256. An artifact
lock count check guards the complete selected set. This avoids scanning unrelated
unreplaced history before applying the batch limit.

All 42 PostgreSQL tests pass without failures or skips: 19 staging/release cases,
7 recovery cases and 16 write-authority cases. Coverage includes bounded repeat
calls, missing or wrong-operation replacement, direct DELETE, rollback, expired
current protection and a three-generation chain. Sol found no blocker.

This releases redundant claims while preserving retry assets. Current claims and
catalog artifacts remain protected. Terminal retention policy, artifact collection
and durable revision references are still unfinished.

### Retained descriptors for operation retry

RepositorySchemaArtifacts.readRetained reads an exact artifact only when the
current live owner identity and a claim from that same operation match. Claims
from prior generations are eligible for recovery reads. Another operation's claim
in the same account does not grant access. The method copies JDBC bytes, checks
size and SHA-256, and returns immutable bytes after the database read completes.
Missing, unclaimed and stale-owner reads return the same explicit unavailable
error. Publication still requires fresh ownership and admission checks.

All 22 staging/recovery PostgreSQL tests pass. New cases cover takeover followed
by reading prior-generation schema bytes, descriptor reconstruction and dynamic
payload decoding without a registry or generated class, restaging and release of
the obsolete claim. Tests also check scope, token, expiry and cancellation. A fresh
component uses the existing database factory; this is not a process-restart test.
Sol found no blocker. This supports operation retry, not historical document access
or completed revision-level schema retention.

### Schema asset and compilation provenance contract

Added RepositorySchemaAsset and compilation provenance messages, reusing
PublicationSchemaCondition. The contract records exact descriptor bytes by hash,
the preserved Any type URL, compiler identity/evidence and admission runtime.
Local compilation requires a source-bundle hash. Imported descriptors distinguish
producer-reported compiler metadata from an explicit unknown reason.

All 33 repository protobuf tests pass, including five new tests using the real
runtime validator with generated and dynamic messages. Buf lint for the new file
and workspace compatibility against the preceding HEAD pass. JSON Schema tests
verify bounds, patterns and exposed CEL metadata; cross-field rules require the
runtime validator. OpenAPI rule translation is not established by this checkpoint.
The new messages are not mounted RPCs. Trusted host capture, retained source
assets, revision/occurrence binding and durable provenance storage remain pending.

### Schema asset binding during payload preparation

DocumentSchemaAssetBinding verifies parsed metadata and exact descriptor bytes,
preserving the distinction between canonical schema identity and artifact digest.
DocumentPayloadCheck.checkAssets uses the existing bounded validator and rejects
root-policy or nested-resolution URL mismatches, including changed prefixes.
The result retains immutable metadata bindings and the original payload bytes.
All 106 engine tests and runtime dependency gates pass. Cases cover reordered
descriptor artifacts, unknown metadata fields, compiler options, bounds,
cancellation, repeated nested resolution, URL mismatches and invalid nested values.
These are internal preparation APIs; durable revision references, compiler trust
verification and per-occurrence binding remain incomplete.

### Opaque Any intake regression

Existing DocumentCommandContent behavior permits opaque structured_data when
host policy allows it and no explicit structured schema is requested. A regression
uses an arbitrary type URL and a deliberately undecodable packed value to prove
the content-only path preserves the Any without parsing its value. Required typed
policy and an explicit schema still fail pending full admission wiring. All eight
DocumentCommandContent tests pass. This proves content preparation, not durable
publication, status persistence or authorization. Future status metadata must
distinguish unattempted resolution, unavailable definitions and validation results.

### Any resolution observation contract

New RepositoryAnyResolution and RepositoryResolvedSchema messages add no RPC and
change no existing protobuf identity. Resolution records bind an exact type URL,
value-byte SHA-256 and size to one explicit outcome. Resolved references reuse
PublicationSchemaCondition plus exact descriptor artifact SHA-256. Descriptor
bytes and compiler metadata remain in shared assets. Unattempted lookup,
authoritative definition absence, lookup failure and access denial stay distinct.
No outcome claims that the value passed validation.

All 37 repository protobuf tests pass using generated and dynamic messages in
the new fixtures. Complete imports compile; new-file Buf lint and workspace
compatibility against the preceding HEAD pass. JSON Schema exposes size/pattern
rules and CEL metadata, while cross-field rules require the runtime validator.
OpenAPI execution of those rules is not established. Account/revision/attempt/path
binding, trusted observation capture, validation verdicts and persistence remain
implementation work. No availability claim is added to public API documentation.

### Opaque resolution capture during content preparation

DocumentCommandContent now emits an optional root structured_data observation
from checked materialized content. It computes the effective value SHA-256 and
size, preserves the type URL and sets not_attempted without registry access or
inner decoding. Absent and present-empty Any values remain distinct. The 4096
code-point URL bound fails explicitly. All nine content-preparation tests pass,
including real-validator verification of the generated observation. Sol found no
blocker. Original fragment identity, nested occurrences, persisted evidence and
typed admission remain separate obligations; no public operation is enabled.

### Independent publication integration baseline

Rechecked the live source call path and SQL activation gates. Publication still
runs through DocumentLedger.saveVerifiedAttempt and DocumentPublicationBatch;
DocumentAtomicPublicationIT is its integration suite, not an independent writer.
V22 deferred document consistency, V38 shadow guards, V39 retention mirrors and
both publication/source readers must change together for independent revisions.
In particular, a new revision pointer does not bypass V22's legacy body check.
The detailed activation inventory is in repository-revision-cutover.md.

All 76 DocumentRevisionProjectionIT and DocumentAtomicPublicationIT cases pass
against real PostgreSQL with populated migration fixtures and no skips. This is
a refreshed legacy regression baseline, not evidence that independent publication
is implemented. The full mixed/zero-upload and multi-destination scope remains.

### Independent publication origin and retention locks

Added a separate internal lock path for complete old/new physical sets without
requiring one upload attempt per destination. It stabilizes native current pointers,
locks the full origin union before retention objects and refuses missing or retiring
identities. A transaction-bound token compares the final destination/object/attempt
sets only after successful retention locking. The legacy FULL_REVISION path is unchanged.

The 80 atomic-publication/projection PostgreSQL cases pass. New cases verify old and
proposed origins and objects with zero fresh attempts, current-pointer protection,
missing objects, stale transaction tokens, incomplete lock phases and retirement
contention using actual database wait evidence. Fixture provider observations are
synthetic; these tests establish SQL behavior, not provider qualification. Independent
revision creation/sealing, metadata persistence, outcome/outbox and read activation
remain pending. Sol found no blocker in the scoped lock primitive.

### Successful publication result contract and command binding

Added DocumentPublicationResult/DocumentPublishedRevision without changing an
existing RPC or protobuf identity. The result binds the scoped operation, command
encoding version/hash, committing generation and full member/revision set. It does
not require an upload attempt and makes no typed-validation claim. The shared SPI
command now checks exact result correspondence and rejects unknown fields before
real runtime validation. Missing/extra/reordered members, wrong addresses, principal,
generation and command identity cannot pass that check.

All 67 protobuf/SPI tests and runtime dependency gates pass, including generated
and dynamic result validation and a 64-member result. JSON Schema exposes bounds
and CEL metadata; cross-field rules remain runtime constraints and no OpenAPI
generator change is included. Durable outcome insertion, atomic revision/outbox
binding and authorized replay remain unimplemented. Do not advertise this standalone
result definition as an available commit endpoint.
Complete protobuf imports compile, new-file Buf lint and workspace compatibility
against the preceding HEAD pass. Sol's canonical UUID concern was addressed with
exact lowercase UUID patterns and short-form rejection fixtures; the final 67-test
rerun passes.

### Bounded publication result storage encoding

Added a versioned DocumentPublicationResultCodec in the repository SPI. Encoding
requires complete command/member and committing-principal/generation matching.
Decoding checks the stored codec/version, byte bound and exact SHA-256 before a
descriptor-driven wire scan and generated parsing. The scan bounds field
occurrences before repeated-message allocation and runtime cross-field validation.
Decoding retains the stored identity; it does not consult or adopt a newer owner.

All 28 SPI tests and runtime dependency gates pass. Fixtures cover deterministic
encoding, mismatched identities, malformed bytes with a correct digest, excessive
empty member occurrences and the full 64-member result with maximum positive
counters. Sol reviewed the final codec with no remaining blocker. The added
descriptor dependency introduces no SQL, Kafka or storage SDK runtime dependency.
This is a storage codec only. Atomic outcome insertion, revision/outbox binding,
terminal-operation fencing and authorized replay remain pending.

### Publication event storage independent of delivery

Added an opt-in recorded-only saved-event factory and V51 outbox safeguards.
RECORDED events remain in SQL without entering the relay queue or being falsely
marked delivered. Their delivery state cannot be changed and they cannot be
deleted. Event identity and payload cannot be rewritten during relay updates.
New events receive the actual insertion transaction ID; migrated events retain
unknown insertion provenance. Existing publication callers keep their current
event policy; the native revision publisher is not activated by this change.

Two initial PostgreSQL regressions failed for the intended reasons: recorded-only
status was unavailable and existing event payloads could be overwritten. After
the change, all 21 event factory, PostgreSQL outbox and Kafka relay tests pass,
with no skips. Fixtures cover actual insertion stamping despite a supplied false
transaction ID, rollback, forbidden state/content changes and a populated V50-to-V51
migration. Sol found no blocker in this scope. The cutover design now specifies
bidirectional revision/outcome constraints, terminal-aware admission/replay,
recovery of unused schema claims and complete event-set binding. Those atomic
publication and replay behaviors still require implementation and qualification.
All 80 existing atomic-publication and revision-projection PostgreSQL tests also
pass against the new migration, with no skips.

### Independent revision SQL boundary

V52 adds internal immutable operation outcomes and per-member revision commits.
It extends publication consistency, physical-key quarantine, current-pointer
mirroring and read/source lookup to recognize sealed native revisions. It extends
owner admission, renewal and takeover to reject terminal operations while keeping
the existing recovery-only fence. Existing wire operations and protobuf identities
are unchanged. No production native publisher or authorized result-replay entry
point is available yet.

Real PostgreSQL fixtures cover a multi-member operation with mixed uploaded/reused
parts and a zero-upload member, exact stored result decoding, late-failure rollback,
missing outcomes/events, terminal writes after immediate constraint checks, later
policy changes, and retained event delivery-state updates. Physical observations
are synthetic and do not qualify a storage provider. Verified-but-unpublished keys
and keys belonging to a different document are rejected. A red timezone fixture
showed metadata timestamps changing with session settings; exact epoch-microsecond
fields fix that snapshot instability.

The final affected run passes 173 tests with no failures or skips, including legacy
atomic publication, revision projection, owner admission/write fencing, physical
location/key migration, retention and outbox tests. Sol reviewed the native SQL
boundary and the final regression additions without a remaining blocker. These
checks establish SQL linkage and physical evidence, not protobuf payload semantics,
typed schema retention, authorized replay, or concurrent throughput qualification.
The separate Kafka relay and recovery-fence run passes another 10 tests without
failures or skips; relay behavior remains unchanged.

### Authorized result replay and native source reuse

Added an internal DocumentPublicationReplay observer. It scopes lookup to the
authenticated principal/account and exact command, distinguishes not observed,
pending and committed, and rechecks current READ policy for every committed
destination under ordered locks. It validates the stored result with the real
codec and compares its members with immutable revision commits. Corruption is
DATA_LOSS; no error path substitutes an absent result or starts new work. Original
results remain replayable after owner expiry and later revisions, subject to
current authorization. This adds no public wire operation.

Extended DocumentReuseAdmission to accept sealed native sources through their
exact retained physical origins. A regression that publishes a second native
revision initially failed because this check required legacy upload history.
The native branch now requires committed authority; both branches require exact
verified objects plus history/current retention references. Legacy FULL_REVISION
checks remain intact. The canonical command check is shared from the operation
ledger by staging and replay.

The PostgreSQL suites cover operation and policy lock waits, revoked read access,
account/principal isolation, malformed stored wire with a correct digest, a valid
result naming the wrong revision, expired leases, later revisions and no duplicate
events. Shared direct-SQL setup lives in DocumentNativePublicationFixture; replay
and publication tests have separate classes. The production publisher, transport
entry point and typed schema-retention integration remain pending.
All 170 affected PostgreSQL tests pass without failures or skips, including native
publication, replay, reuse admission, upload admission, reader pins and operation
admission. Sol reviewed the replay and native reuse changes without a blocker.

### Native storage coordinates

V53 makes the legacy shared-prefix column nullable and requires new native revision
commits to leave it absent. Per-part retained locations remain authoritative. The
selected logical drive is not a substitute for those bindings. Deferred checks
refuse unbound null/blank-prefix rows, and legacy publication keeps a nonblank
prefix requirement. The migration preserves existing rows and historical bodies.

Legacy save paths now reject native destinations they cannot represent, before
provider work and under the deduplication lock. The response builder returns
UNSUPPORTED instead of crashing or inventing a shared prefix. These are internal
SQL and Java extensions; protobuf fields and the independent result remain unchanged.
Initial regressions reproduced the SQL NOT NULL failure and response-builder
NullPointerException. Fixtures now cover the populated V52 migration, native null
prefixes, refusal of fabricated prefixes, and refusal of unbound null/blank rows.
This prerequisite does not activate the production native publisher.
All 125 affected container/engine tests and 34 repository service integration
tests pass without failures or skips. Sol reviewed the migration, trigger order
and legacy response boundary without a blocker.

### Internal native publication checkpoint

`DocumentPublicationCommit` now connects checked `DocumentCommandContent`, current
upload selections, exact physical bindings and native revision commits. This is
an internal boundary, not an available RPC. It builds every candidate before writing
any destination and records the terminal result after all member revisions and
saved events. Typed-required intake still fails explicitly until retained schema
integration is complete.

`Prepared` must describe the currently selected attempt. After replacing an
attempt, rebuild the prepared plan with that attempt ID; an earlier plan cannot
publish the replacement's bytes. This preserves canonical command identity while
checking the current generated physical keys independently.

Current local evidence:

- `DocumentPublicationCommitIT` uses PostgreSQL and a versioned LocalStack S3
  adapter to upload and verify two documents, admit their actual protobuf bytes,
  commit native revisions, read exact provider versions and replay the durable
  result. Opaque `Any` values retain invalid inner wire bytes without inner
  deserialization and record resolution as not attempted.
- A PostgreSQL trigger injects failure while inserting the second member's commit.
  Neither document nor native commit becomes visible; the operation remains
  pending. Removing the fault allows the same staged operation to commit and
  replay successfully.
- Reuse-only and mixed revisions pass through the same Java publisher with a
  retained CORE. Its physical identity, last-write time and provenance stay
  unchanged; document version advances, and both original and new outcomes replay.
  These cases perform exact versioned retained reads directly in the test; they
  do not yet qualify the production retained-reader handoff.
- A replacement upload attempt succeeds with a rebuilt prepared plan while the
  original plan is rejected. Checked content remains bound to unchanged canonical
  command bytes, independently of the replacement physical attempt.
- Host-required typed intake rejects opaque content. Caller cancellation and
  thread interruption before commit leave the operation pending and retryable.
- Revoking destination access after staging prevents publication. Authorization
  refusal precedes revision-conflict disclosure to the denied caller; process
  authority still receives the stale-revision conflict and cannot publish it.
- A caller-side delivery failure injected after the real SQL commit is recovered
  through exact durable replay. A repeated publication is refused by the terminal
  operation guard; it does not create another revision or success outcome. This
  is not a network transport qualification.
- A PostgreSQL advisory-lock trigger pauses the first member's revision insert.
  Cancelling the caller before releasing that barrier rolls back the batch;
  neither document becomes visible and the same staged operation can retry.
- Retained manifest tests accept a producer field just below the per-entry JSON
  budget and reject an oversized entry with `RESOURCE_EXHAUSTED`. A different
  physical object for the requested source slot and conflicting identities for
  one source slot are rejected. These are SQL metadata tests with explicitly
  synthetic physical verification, not provider qualification.
- `DocumentCommitWriterTest` verifies caller-declared producer provenance is
  preserved and absent provenance remains absent. Its physical identities are
  synthetic and do not qualify a provider.

`DocumentRetainedManifestEntries` now projects only requested source slots using
their retained revision-part ordinal, with a V54 `(revision_id, object_id)` index.
It parses batches of at most 32 entries, rejects individual JSON entries over
1 MiB before returning them from SQL, and enforces a 64 MiB aggregate serialized
metadata budget. Slot, state, key, size and digest must match the command. These
limits cover this provenance projection, not the whole transaction. Admission
now locks source-only documents as scalar identity, revision, status and policy
views without hydrating their manifest. Destinations, including source/destination
overlaps, retain full row loading and prior-manifest parsing. The same globally
ordered advisory and row locks still apply; authorization precedes all revision
conflict reporting. Policy JSON and destination metadata still need aggregate
resource qualification.

Remaining for host integration: qualify the aggregate metadata budget and
aggregate policy/destination metadata use, production retained-reader handoff,
network acknowledgement loss and operating-limit performance. Typed admission,
retained-schema publication and the shared public library/transport boundary are
still unfinished. Existing SQL-only native publication fixtures do not substitute
for those integration cases.

### Shared admission module extraction

`repo/admission` now owns the existing package-private schema binding, schema asset
binding and recursive payload checker, plus their unchanged validation fixtures.
The former package was `ai.protomolt.proto.repo.engine`; the internal helpers now
use `ai.protomolt.proto.repo.admission`. No public Java API or protobuf identity
changed. Engine no longer carries their descriptor utility or source-compiler test
dependencies directly.

All 107 admission and engine tests passed with no failures or skips. Both runtime
dependency gates passed. The admission module excludes repository SQL/engine/service,
storage SDKs, Kafka, registry/compiler clients and transport implementations from
its production graph. Compiler-backed fixture tests remain test-only. This is a
module extraction, not an enabled typed publication API; the reviewed occurrence,
source-provenance and retention prerequisites remain in the cutover design.

### Occurrence evidence contract

Classification: new internal evidence contract; existing publication, read,
replay, receipt, idempotency and schema-resolution operations are unchanged.
`schema_occurrence.proto` adds `RepositorySchemaOccurrencePath`, its step and
map-key messages, and an explicit protobuf map-key type enum. It imports the
existing `RepositoryAnyResolution`/`RepositoryResolvedSchema` identities instead
of redefining artifact references. A path boundary requires the resolved outcome;
that observation still does not establish successful validation or retention.

Version 1 paths begin and end at a selected Any boundary. Field numbers, repeated
positions and typed map keys describe decoded occurrences. Runtime rules check
step selection, map-key kind and numeric width, valid field numbers, path bounds,
and boundary resolution. The real validator fixtures exercise generated and
dynamic messages, zero/false/empty keys, UINT64_MAX, reserved field numbers and
invalid or absent alternatives. JSON Schema records structural bounds and CEL
extensions; it does not execute cross-field CEL or establish OpenAPI parity.

An enclosing root locator remains required. CORE contains
`Document.structured_data` at field 4. PARSED contains parser shape via
`Document.parser_results` field 5 and its exact string key, then
`ParserResult.document` field 7 and `ParserDocument.shape` field 1. A generic
relative path must not be mistaken for either locator, a revision identifier or
an authorization capability. Reuse `DocumentPublicationSlot` when defining the
document-part wrapper; pin the containing layout and exact root access path.

Handler obligations remain: bound raw bytes, recursion and wire occurrences
before parsing; reject unknown evidence fields; validate transitions
against retained descriptors and candidate values; verify effective Any.value
bytes, size, URL and artifact/condition binding; apply aggregate UTF-8 text/step
bounds; reject duplicate paths; and bind account, operation/attempt, revision and
policy. The single-path codec below implements canonical encoding and decoder
re-encode checks; canonical evidence-set encoding remains unfinished.
The collector-to-protobuf bridge obtains actual value sizes from traversal;
it does not substitute an unknown size with zero. No public API consumes this
contract yet, and typed publication remains disabled.

Validation for this additive contract: 163 contract/admission/engine tests pass,
including both runtime dependency gates. Scoped Buf lint passes. Complete-import
descriptor compilation and unwaived FILE compatibility pass against checkpoint
`354d8fbbf80d17ff577992acf927d85e02388e51`. Sol reviewed the contract and fixtures.

### Completed-check occurrence projection

`DocumentSchemaOccurrenceProjection` converts a completed strict archival check
to immutable version 1 protobuf paths. It refuses the lower-level permissive
check, uses the exact resolved binding from the completed check, and validates
each output through the real runtime validator. The collector records each
effective Any.value size alongside its digest. Key conversion uses explicit enum
mapping, widens unsigned 32-bit values without sign extension, and preserves all
unsigned 64-bit bits. Cancellation and thread interruption stop conversion without
returning partial paths. A completed input remains reusable after cancellation.

This is an internal representation conversion. Ordinary protobuf serialization
round-trips the messages but is not yet a canonical identity encoding. There is
no untrusted-byte reader, root locator, revision binding or admission capability
in this helper. Compiler provenance, source retention and atomic publication
remain separate obligations. Evidence projection must not enable typed success
until those publication checks are implemented.

### Single-path occurrence codec

`DocumentSchemaOccurrenceCodec` defines codec `repository-schema-occurrence`,
version 1. Fields are written in ascending numeric order, repeated steps retain
their order, varints and length prefixes use their shortest protobuf encoding,
and semantic presence determines omission. Oneof zero, false and empty-string
values remain explicitly present. `signed_value` uses the contract's int64 wire
encoding, including ten-byte negative values, even when the original map key
type was sint or sfixed. Unknown fields, malformed UTF-16 strings, maps, packed
fields, extensions and unsupported scalar layouts are rejected. A fixed 225-byte
fixture pins the encoding independently of the protobuf deterministic-output flag.

Encoding measures byte size with bounded long arithmetic before allocating the
output. Decoding checks the 4 MiB byte ceiling and digest before a descriptor-aware
wire scan; at most 16384 wire values and 16 nesting levels are permitted. Parsing
uses the same recursion limit and must consume the entire input. Recursive unknown
field rejection and the runtime validator precede exact canonical re-encoding.
Alternate tag order, duplicate singular tags and overlong varints are rejected
even if their supplied digest matches and a normal protobuf parser accepts them.

These are single-path limits, not an operation's aggregate budget. The codec does
not sort or deduplicate a set, identify the containing root, match a path to live
candidate bytes, authorize access or retain schema assets. Its digest describes
evidence bytes, not a payload or admission verdict. Stored schema references still
require the enclosing revision/publication transaction and trusted candidate checks.

### Root discovery from retained fragments

New internal `DocumentAnyRootInventory` inspects exact CORE and PARSED fragment
bytes under an independently bound complete Document descriptor closure. This
implementation supports the current Document closure and
`protomolt-document-parts/v1` confinement policy; a different closure is rejected,
not interpreted through the current generated class. Historical support for other
layout versions requires an explicit implementation and retained definitions.

The inventory checks raw byte and wire-value/depth limits, parses dynamically
once, applies the shared confinement policy and expected document identity, and
rejects unknown fields on the selected root access paths. It records CORE field 4
and PARSED field 5/key/7/1 roots. Duplicate parser keys are rejected even for entries
without shapes; generated map collapse cannot choose a winner. Missing roots are
distinct from present empty Any envelopes. Payload definitions are not looked up,
and Any.value is not parsed: invalid inner wire and unavailable types remain
representable observations. Strict typed admission must separately validate them;
failure cannot silently become opaque success.

Results preserve the original ByteString, its exact SHA-256, slot, containing
schema binding, layout policy and immutable root access paths/envelopes. The raw
byte count is the retained ByteString size. These hashes do not authorize a
revision part, prove provider receipt identity or retain schema artifacts. Unknown
fields elsewhere in owned subtrees are not exhaustively validated by root discovery.
This helper covers the two named root families, not every possible Any in Document.

The shared confinement helper now also accepts Message, so the inventory can
check DynamicMessage directly. The existing Document overload and public consumers
remain intact. `repo/admission` adds only the pure `repo/codec` dependency. The
operation coordinator still needs aggregate root, parser-entry, byte and metadata
budgets across all fragments/members, plus accounting for assembly and inventory
allocations. Per-fragment limits are not an operation-wide memory bound.

Sol reviewed this slice. The contract, codec, admission and engine suites pass
228 tests, including runtime dependency gates. Fixtures use actual split output,
duplicate/default parser keys, absent/empty roots, malformed opaque payloads,
noncanonical original bytes, wrong slots/identity/schema and independent limits.
No typed publication or durable root-locator contract is enabled by this change.

### Root locator contract and projection

New `DocumentSchemaRootLocator` now defines the version 1 representation of those
locations. It reuses DocumentPublicationSlot and RepositoryResolvedSchema and
binds exact fragment SHA/size, containing Document closure, layout policy and root
access. Runtime CEL restricts version 1 to `protomolt-document-parts/v1`, CORE
field 4 or PARSED field 5/string key/7/1. Indexes and Any crossings cannot stand in
for those access patterns. Existing protobuf names, tags, import paths and URLs
remain unchanged; no existing operation, receipt or idempotency rule is modified.

`DocumentSchemaRootProjection` converts a member of an inventory's immutable root
list to that contract, checks the runtime annotations, and preserves the measured
fragment and containing-schema identities. This is representation conversion, not
authentication of an inventory supplied by another component. A production
boundary must match the selected root against exact fragment bytes and the relative
occurrence path's first Any boundary, then bind the locator to the immutable
revision-part row and physical object. Current-document lookup is insufficient
for historical roots. A canonical codec is added below; persisted root rows remain unfinished.

Generated/dynamic validation and actual split-fragment projection fixtures pass.
JSON Schema reports syntax and runtime CEL metadata, not execution of cross-field
rules or OpenAPI parity. The four affected suites pass 231 tests; scoped Buf lint
and complete-import FILE compatibility pass against `fab66378e81c713d0eb4a135d255203482ab41c3`.

### Required resolution and historical-read follow-up

The [contextual resolution design](repository-composition.md#contextual-any-resolution-and-optional-materialization)
adds completion gates, not available operations: occurrence-specific immutable
schema binding, preserve/materialize read modes, bounded contextual caching,
explicit failure outcomes, and historical restore tests with the registry absent
and process cache empty. Inventory every remaining document Any root before goal
closure, with coverage or an explicit typed-admission rejection. The current
CORE/PARSED root discovery remains the implemented root coverage. The contextual
checker below removes the internal URL-only resolution restriction. Unknown
schemas may remain opaque under an allowing contract;
required typed validation may not downgrade to opaque success.

### Immutable revision-to-schema retention prerequisite

V55 adds `document_revision_schema_artifacts`, linking an immutable native
revision to normalized account-scoped descriptor artifacts. Insertion requires
the live operation owner fence, matching account/principal/operation/generation,
the creating transaction's unsealed native projection, and a current-generation
staging claim. References are immutable and retain catalog rows independently of
claim lifetime. A revision can reference at most 64 artifacts. No existing
publication receives inferred or backfilled schema evidence.

Real PostgreSQL fixtures cover shared artifact bytes across revisions, scope and
owner rejection, sealing and terminal success, missing artifacts, rollback/retry,
lease expiry, and migration from populated V54 legacy/native publication history.
The fixture injects references before projection sealing; it is not a production
writer or a provider/typed-admission qualification.

Production insertion remains disabled. Complete root/path/candidate binding,
sorted artifact prelocking, aggregate operation evidence bounds, historical
decoding, and reference-aware terminal claim cleanup/GC remain required. A row in
this table proves retention only; neither it nor `structured_resolution` grants
typed-validation status. This migration changes no protobuf contract or public
operation behavior.

### Contextual occurrence schema checks

The internal admission checker now has a separate contextual entrypoint. Its
resolver receives the exact URL, immutable path to each nested Any, and effective
value digest/size. Every nested occurrence is resolved independently, including
occurrences sharing the root URL. Existing URL-only entrypoints retain their
once-per-URL behavior through explicit adapters.

Completed schema/asset indexes and occurrence projection use exact URL plus
artifact hash. Conflicting metadata for one identity is rejected. Fixtures use
two definitions with the same message name and opposing CEL rules in one
candidate; each validates and projects under its own bound definition. Wrong
selection fails validation, missing definitions fail strict admission, and
resolver outages propagate unchanged. Additional fixtures cover repeated root
URLs, immutable request paths and conflicting metadata.

This is bounded in-memory checking, not host-authorized registry resolution,
historical restoration or durable publication proof. The host still supplies
authorization, immutable version selection and aggregate resolver-memory limits.
No protobuf fields, public RPCs or production publication paths changed.

### Retained schema asset reader

`DocumentRetainedSchemaAssets` is an internal attempt-local source for exact
retained descriptor assets. Its scoped reader accepts only an artifact digest;
there is no registry/latest/compiler fallback. It verifies the exact bytes and
reconstructs complete imported descriptor closures through the existing binding
code. Missing or corrupt required assets report data loss. Reader access and
I/O failures, cancellation and configured descriptor resource limits remain
distinct and propagate rather than being cached as missing types.

The cache is serial and attempt-local, bounded by binding count and serialized
artifact bytes. Every result, including a cache hit, requires the caller's current
access/control callback. It rejects conflicting metadata for one URL/artifact
identity and caches only successful completed bindings. The scoped byte reader
must bound allocation before returning content; linked descriptor graphs and
metadata require separate host memory accounting.

This helper does not select or authorize historical revisions. The internal replay
checkpoint below matches recorded paths and compares rechecked evidence. Neither
this reader nor schema-reference rows establish complete historical restore
behavior on their own.

Filesystem fixtures reconstruct complete imports and two same-URL definitions
using fresh readers/descriptors, preserving compiler provenance and comparing
rechecked occurrence evidence. They also exercise repair/retry, access rechecks,
callback failure and resource bounds. The five affected suites pass 333 tests.
These are fresh-object tests, not a process-restart or SQL historical-read proof.

### Exact recorded-path replay

`DocumentSchemaReplay` checks one root against a bounded set of recorded occurrence
paths. Each path prefix, exact URL, effective value digest and size selects its
recorded descriptor artifact. The root must match too. Duplicate paths,
conflicting selections and metadata associations are rejected before resolution;
broken required metadata associations are data loss. The completed strict check
must reproduce the full path set, independent of input ordering. Extra evidence
cannot pass merely because every encountered occurrence had a resolution.

Persisted path bytes must first pass the occurrence codec's digest and canonical
decode checks. This helper accepts parsed objects and cannot attest to their
original wire encoding. Aggregate path count, encoded bytes and steps are bounded;
payload, descriptor and occurrence budgets still apply. It reruns the explicitly
supplied validator, without claiming to reproduce a historical runtime or judge.

The disk-backed mixed-version fixture now invokes this helper instead of choosing
schemas by test position during replay. Adversarial cases include changed payload,
missing/extra/duplicate paths, conflicting selections, wrong metadata, limits and
cancellation. All twelve protobuf map-key types replay with exact unsigned bits.
Revision selection/authorization, durable path-set storage, document-root binding,
SQL integration and process-restart restoration remain unfinished.

### Canonical document-root locator encoding

`DocumentSchemaRootCodec` uses the distinct `document-schema-root` format identity
at version 1. The occurrence and root codecs share a bounded numeric-tag-order
writer, with shortest varints, explicit oneof presence and recursive unknown-field
rejection. Existing occurrence wire bytes and format identity are preserved.
Both readers bound bytes, recursion and wire values before parsing, check the
supplied digest, validate the actual message annotations, and require exact
canonical re-encoding. A parsed protobuf round trip alone is insufficient.

The locator still binds fragment hash/size, containing schema, layout and root
access. Its encoding does not authenticate a revision or activate typed
publication. Complete evidence-set encoding and its atomic revision storage
remain separate work.

The five affected suites pass 338 tests, including the unchanged occurrence golden
bytes, actual split-fragment locator round trips, noncanonical wire alternatives,
unknown nested fields, parser/descriptor mismatch and cancellation. This is codec
qualification, not proof of persisted revision evidence or a historical read API.

### One-root schema evidence bundle

New additive `DocumentRootSchemaEvidence` groups the existing root locator and
relative occurrence paths. Version, required root, repeated bounds and exactly
one root-only path are runtime annotation checks exercised on generated and
dynamic messages. Existing operations, schema references, receipt/idempotency
bindings, protobuf names and field identities are unchanged. This is a new
representation, not a new public operation or admission verdict.

`DocumentRootSchemaEvidenceCodec` bounds the whole message before allocating
per-path encoded buffers: 4 MiB, 16384 wire values and depth 16, with additional
65536 aggregate path steps and 1 MiB path text. It sorts each path's V1 canonical
bytes using unsigned lexicographic order and rejects duplicate or conflicting
selector prefixes, duplicate paths and disagreement on the root boundary.
Decode requires the exact canonical representation and digest. The format is
`document-root-schema-evidence`, version 1; no descriptor bytes are duplicated
inside it. Assets and provenance remain normalized outside this one-root bundle.

The future publication handler must resolve its slot to exactly one immutable
revision-part object, compare original fragment hash/size and effective root
envelope, reproduce the entire path set through the trusted validator, establish
complete required root coverage and retain the exact union of required assets.
Those checks, revision policy binding and atomic persistence remain unfinished.
This contract does not impose a new document-only transaction model on the
reusable repository foundation or optional JCR extension.

JSON Schema exports repeated bounds and CEL metadata. CEL execution, canonical
ordering, selector identity, candidate completeness, memory budgets and SQL
binding are runtime/handler obligations, not claims of OpenAPI parity.

The five affected suites pass 346 tests. Scoped new-file Buf lint and complete
import compatibility against `4f65a42bea3d74956b6dac6d989371d4dcb3ec3e` pass.

### Strict evidence binding to one exact fragment

`DocumentFragmentSchemaReplay` inventories an exact CORE or PARSED fragment once,
using the caller-selected immutable slot, document ID and bound containing schema.
It requires a one-to-one match between every discovered root and a complete
recorded locator, including fragment hash/size, schema, layout and access path.
Duplicate, missing, extra or mismatched root evidence fails before payload schema
loading. Each effective root envelope then passes exact recorded-path replay;
the helper returns a list only after every root succeeds.

The fragment budget bounds root count and serialized bundle bytes. One decoded
payload-byte allowance covers every root and its nested Any decodes; completed
checks report their actual charged bytes before the next root starts. Exhausted
allowance fails closed. Wire/schema/path limits also apply per root, so their
aggregate upper bound is multiplied by the bounded root count. Descriptor and
metadata allocation still require the host's operation-wide accounting.

This is strict replay of all currently supported roots, not optional opaque
materialization. An empty discovered root set is not proof that a typed-required
document contract was met. A descriptor-graph regression now enumerates the
current Document entry points: `structured_data` and
`parser_results.value.document.shape`. It fails on another declared Any entry
point or an Any-bearing recursive route needing explicit discovery support.
Nested payload Any values remain the strict payload checker's responsibility.
Other archival message contracts, future schema layouts, complete document
coverage, revision authorization and atomic SQL evidence storage remain
publisher/integration obligations.

All five affected suites pass 351 tests. Actual split CORE/PARSED fixtures cover
complete locator matching, absent/extra evidence, raw content and identity
changes, aggregate budgets, exact exhaustion and cancellation during a later
root's retained-schema read. No public operation or SQL publication path changes.

### Atomic storage for revision-root evidence

V56 adds immutable `document_revision_schema_evidence` rows for an exact native
revision and part ordinal. Each row carries the operation owner and generation,
root-locator digest, original fragment hash/size, and versioned bundle bytes with
a verified digest. The physical-location and attempt-object joins require the
same verified CORE or PARSED source, slot and backend identity. Native projection
sealing additionally verifies the complete retained manifest. Insertion requires
the live operation write fence and the current unsealed publication transaction;
rollback removes evidence together with the revision. Existing revisions receive
no backfilled evidence.

Storage limits are 4 MiB per bundle, 1024 rows and 16 MiB per revision, and 4096
rows and 64 MiB per operation. The existing owner write lock serializes aggregate
checks across all member revisions. Stored generated lengths keep accounting
independent of bundle decoding. These are storage ceilings, not a promise that
every below-limit candidate fits the stricter validator and host memory budgets.
Near-limit insertion latency needs measurement before activating a typed writer:
per-row aggregates currently perform a bounded quadratic number of row visits.

The SQL columns are claims until the trusted publisher canonically decodes and
replays their bundles. SQL does not prove that the locator digest describes the
bundle, that every required root exists, or that all referenced assets and their
compiler/source provenance are retained. The future writer must prove those
relationships, authorization and policy binding before committing. Descriptor
bytes remain normalized in the existing artifact catalog; this table does not
duplicate them. Typed production publication remains disabled. No public
operation, protobuf identity, receipt or idempotency contract changes here.

Four targeted PostgreSQL suites pass 31 tests, including populated V55 migration,
exact fragment and owner rejection, immutable/sealed/terminal windows, transaction
rollback and retry, 16/64 MiB and 1024/4096-row limits, and an observed PostgreSQL
owner-lock wait from a second connection. The budget cases prove the exact limit
was reached before rejection; a generic database failure is insufficient. These
fixtures use synthetic physical observations and evidence bytes. They qualify
SQL storage guards, not provider integrity or successful typed admission. Sol
reviewed the migration and tests without a remaining correctness blocker.

### Canonical retained schema provenance metadata

`DocumentSchemaAssetCodec` introduces the internal `repository-schema-asset`
encoding at version 1 with a 512 KiB limit. It preserves the existing
`RepositorySchemaAsset` contract, including the selected type, exact descriptor
artifact digest, compiler evidence classification, inert compiler options,
admission runtime and optional retained-source digest. This is a representation
change only; operations, receipts and idempotency are unchanged.

String-to-string maps in the shared evidence writer sort by unsigned UTF-8 key
bytes. Each entry emits both key and value, including an empty value; non-map
repeated fields retain their original order. Canonical lengths are measured by
the writer, not inferred from generated-message serialization sizes. Unknown
fields, duplicate dynamic map keys and unsupported map layouts are rejected.
Decoding bounds the wire before parsing and requires the exact canonical bytes,
so a generated parser collapsing duplicate raw keys cannot make them acceptable.

The codec uses ProtoMolt's runtime validator for metadata annotations. Compiler
identity remains a claim until trusted admission establishes its evidence
classification. The codec does not resolve descriptors, prove the selected type
exists, authenticate a compiler, retain source bytes, authorize a revision or
activate typed publication. Durable metadata/source retention and binding to the
published revision's exact artifact union remain required integration work.

Five affected suites pass 357 tests, including six new metadata-codec tests.
Fixtures exercise local/imported provenance, generated/dynamic empty-value
equivalence, UTF-8 ordering distinct from Java UTF-16 ordering, duplicate map
entries, reversed wire order, omitted default values, invalid provenance,
encoding/digest mismatch, bounded decode and cancellation. Existing occurrence
and root codec regression tests remain green. Sol reviewed the code and tests
without a remaining blocker. These fixtures do not authenticate compiler claims.

### Normalized revision provenance and source bindings

V57 adds `document_revision_schema_assets`, associating an exact type URL and
descriptor artifact with canonical metadata and optional source artifacts. All
bytes use the existing account-scoped, immutable digest catalog. Composite
foreign keys require each role to have a retained artifact reference on that
same revision. A null source is permitted for imported definitions with no source
claim; trusted admission must require and verify the source reference whenever
the metadata claims one, including local compilation.

The association is immutable and requires the native commit owner, generation,
current transaction and unsealed projection. Metadata storage is bounded to
512 KiB. Type URLs retain their exact text, with a verified SHA-256 index key so
valid long URLs do not exceed PostgreSQL's B-tree entry limit. Readers must still
compare the exact URL; a collision on the composite URL-digest/descriptor identity
must refuse the association, never select a different definition. Different descriptor versions under one exact
URL remain distinct associations. Conflicting metadata for the same URL and
descriptor within one revision is not accepted.

The existing 64-artifact and 64 MiB staging budget applies cumulatively across
descriptor, metadata and source roles. Individual artifacts remain limited to
16 MiB; associations are limited to 64 per revision. These independent ceilings
do not promise 64 complete unique bindings: three unique artifacts per binding
would exhaust the artifact count after 21 bindings. Reuse reduces storage and
claim counts. Expanding the budget requires operation-wide resource review.

No source archive is extracted or executed by retention. Source bytes are inert;
future compilation must establish their exact format, input manifest and import
closure before claiming local observed provenance. The publisher must decode the
canonical metadata and compare its type URL, descriptor, source and condition
with these stored claims, retain the complete asset union, and establish policy
and candidate evidence before activation. V57 alone does not establish any of
those semantic relationships. Existing revisions receive no invented metadata.

Five targeted PostgreSQL suites pass 41 tests, including ten new association
tests. They cover shared normalized artifacts, each missing role reference,
optional and missing source/metadata cases, the exact metadata byte limit,
ownership and sealing, rollback/retry, populated V56 migration, 4096-character
Unicode URLs, multiple definitions under one URL and the exact 64-binding limit.
Explicit foreign-key names make the role-specific failures unambiguous. Sol
reviewed the migration and fixtures with no remaining blocker. These SQL tests
use synthetic bytes and do not establish trusted compiler or admission evidence.

### Verify retained associations against their actual assets

`DocumentRetainedSchemaAssets.resolve(Reference, control)` accepts an association
selected by the host's authenticated revision reader. It decodes canonical
metadata and compares the exact type URL, descriptor digest and optional source
digest, including source presence. The existing descriptor binder then verifies
the selected type and complete import closure. A claimed source must be present,
nonempty and match its digest. Source bytes remain inert: matching a digest does
not establish source format or prove that a compiler produced the descriptors.

Descriptor, metadata and source reads share one digest cache and serialized-byte
budget. Newly read assets remain provisional until all checks and final access
control succeed; a failed reference does not retain a partially completed cache
entry. The same digest used by several roles is counted once, with each role's
own format limit still checked. An exhausted budget prevents the next storage
read. The reader implementation must bound each allocation before returning
bytes; decoded metadata, linked descriptors and transient I/O copies still need
host memory accounting.

Missing required assets, digest corruption, invalid canonical metadata and a
reference/metadata mismatch are data loss. Configured resource limits remain
limit errors, while I/O, denial and cancellation propagate unchanged. Cached
reads recheck current access. The existing metadata-only overload retains its
descriptor-resolution behavior and does not claim source retention. This helper
does not select SQL revisions, authorize documents, attest compiler execution,
verify the complete revision evidence set or activate typed publication.

The next production integration is a single admission facade over these internal
helpers. It must accept bounded, authorized member fragments and a pinned policy
context, and return a complete immutable proof for the exact command member and
slot ordinals. `DocumentCommandContent.check` currently refuses required typed
admission. `DocumentCommitWriter.write` has the transaction hook immediately after
`document_revision_parts` insertion and before projection sealing, where the
complete V55/V56/V57 records belong. Stage the full asset union first; prelock its
digests in order after publication locks and recheck ownership, selected/reused
physical identities, command and policy under the commit fence. Typed activation
must change the existing OPAQUE-only SQL mode together with those proofs. An
authorized historical revision query is also needed; operation-owner retry
reads are not a substitute. This is remaining integration work, not availability.

Five affected suites pass 367 tests, including ten reference-reader tests backed
by real temporary files and complete descriptor imports. They cover decoding
with a fresh reader, absent and claimed sources, metadata/source corruption and
absence, exact association mismatches, failed-attempt retry, canonical metadata,
cross-role deduplication, aggregate byte bounds and access failure during checking
or before delivery. Sol reviewed the reader and tests; its missing successful
no-source case was added and passed. This is not a SQL/process-restart historical
restore qualification or proof of trusted compiler execution.

### Complete member schema admission proof

`DocumentSchemaAdmission.check` is the public, storage-independent verification
boundary for a complete publication member. It binds the command digest, policy
digest, explicit structured-root requirement, member and raw fragment ordinals.
It checks every nonempty declaration against the actual bytes, assembles the
Document, verifies ownership equality and refuses unknown fields or unsupported
Any locations. The fixed v1 validation profile uses ProtoMolt and Buf annotation
rules through ProtoMolt's runtime validator; callers cannot replace it with a
no-op validator or change its dialects through provider discovery.

Canonical root bundles must cover all discovered CORE and PARSED roots. All
retained associations, including optional claimed source bytes, are verified
before payload replay. The proof contains only the complete used association
set and its normalized descriptor/metadata/source artifact union. Fragment,
root, evidence, retained-asset and decoded-payload budgets apply across the
member; subordinate structural and schema ceilings are documented in `Limits`.
These serialized limits do not reserve heap or bound the reader's initial I/O
allocation. The host remains responsible for those reservations.

The immutable proof is privately constructed after final control checking. A
member without typed roots can pass when the structured-root requirement is
false; that proof is content verification, not a typed-admission verdict. The
publisher must compare the command, member, policy, requirement and validation
profile under its transaction fence, recheck authorization and physical object
identity, and retain the full artifact/reference/evidence set before sealing.
None of those publisher changes are activated by this facade. V52 remains
OPAQUE-only, and historical SQL reading still needs its own authorization path.

This boundary consumes candidate-supplied canonical evidence. Generating that
evidence from an initial registry-backed admission remains separate integration
work. Source integrity still does not establish trusted compilation. Before
activation, qualify near-limit memory and latency, including eager retained
reads and repeated canonical evidence encoding, against the host's operational
budgets. Reuse the shared `DocumentRevisionAssembly` checks when integrating
`DocumentCommandContent`; do not maintain two diverging assembly policies.

The affected module suites pass 377 tests, including 10 new facade tests;
unchanged tasks reuse Gradle verification outputs. The runtime dependency gate
passes with the explicit validation dialect module. Fixtures cover raw fragment
identity, root/reference coverage, source retention, aggregate budgets,
structured-root requirements, unknown fields and final access failure. A Buf
string constraint accepts a valid payload and rejects an invalid payload with
internally consistent candidate-supplied provenance. Sol reviewed the facade
and tests with no remaining blocker.

Initial publication needs an evidence-producing entrypoint in this facade when
the host resolver is integrated. It should select immutable definitions per
occurrence, check payloads, project canonical evidence and replay through `check`.
Hosts should reuse that operation instead of duplicating root discovery and
schema selection through public encoders. The consuming `check` remains an
independent verifier of persisted or imported evidence.

### Native publisher integration gate

The current main-source call graph has no caller of `DocumentPublicationCommit`
or `DocumentCommandContent.check`; container tests exercise both. Wiring a host
is remaining work. Existing authorization handles caller/account/document access,
not a schema-policy identity or freshness guard. The publisher constructor's
`requireTypedSchema` flag cannot authenticate the policy digest in an admission
proof. Add an authoritative policy snapshot and a commit-time guard before typed
activation; do not compute policy identity from untrusted request fields.

The host must stage the complete operation asset union before the commit
transaction. `RepositorySchemaArtifacts.stage` opens a transaction and must not
be nested inside `DocumentPublicationCommit.commit`. Within commit, retain the
existing owner, command, authorization, placement and physical-part checks. Lock
schema digests in sorted order across all members, verify current-generation
claims and current policy, and compare proof identity and raw slot hashes/sizes
with the selected upload or authorized reuse. Insert V55 references, V57 schema
associations and V56 root evidence after revision parts and before projection
sealing. Cancellation before terminal success must roll back the entire batch.

Typed mode needs a coordinated V52 migration and deferred completion checks;
proof existence does not authorize a mode change. SQL should enforce persisted
counts, scope and required bindings. Java proves canonical root coverage and
validation semantics. Preserve atomic multi-object publication while introducing
these checks, consistent with the repository foundation and optional JCR design.

### Produce evidence from host-selected definitions

`DocumentSchemaAdmission.prepareAndCheck` now accepts a complete member and an
explicit resolver. Each selection includes the raw-fragment root locator,
member ordinal, nested selector prefix, exact type URL and payload digest/size.
Earlier Any boundaries in the prefix include their selected descriptor identity.
A resolver can choose different descriptor versions for the same URL at different
occurrences. Bindings use the pair of URL and descriptor digest; storage assets
use their digest. Repeated selections cannot change metadata for one binding.

The producer validates payloads with the fixed profile, projects canonical root
and occurrence evidence, verifies claimed sources, and checks aggregate limits.
It then freezes the asset set and calls the independent consuming verifier.
That replay performs no registry selection. Null, denied, unavailable or invalid
definitions fail this strict path without an opaque fallback. A failed attempt
returns no proof and writes no storage. Resolver inputs and returned definitions
still require host authorization and allocation limits; compiler claims remain
separate from source integrity. This supplies the evidence-generation entrypoint
identified above, without activating the publisher or changing protobuf contracts.

A proposed policy catalog uses immutable account-scoped policy snapshots and an
active revision pointer. The snapshot must identify the validation profile,
structured-root requirement, schema eligibility and resource ceilings. Preparation
reads the authoritative snapshot; commit takes a shared pointer lock after the
operation fence and before document/drive locks, compares every proof and holds
that lock through terminal success. Concurrent writers can share the lock. A
policy update changes the pointer exclusively: an earlier update rejects stale
proofs, while an earlier writer lock permits that commit before the update.
This catalog and adapter remain to be implemented in the document repository,
with no dependencies added to byte storage or the optional JCR interface.

Activation must cover every publication entry point. An account requiring typed
admission cannot bypass policy through a legacy opaque writer. Opaque publication
remains permitted only where authoritative policy allows it. The current command
has one account, so one target policy pointer covers the member batch; source
ACL/revision checks still apply. Any future cross-account source or finer policy
scope needs an explicit scope and locking review. Required integration tests
include stale-policy rollback of all members, concurrent shared-lock writers,
a policy updater waiting for a writer, mixed-policy rejection, missing policy
and an attempt to use a legacy writer under a typed-required policy.

The evidence producer checkpoint passes 386 tests across the affected modules,
including 9 new preparation tests and the admission runtime dependency gate.
Unchanged module tasks reuse Gradle verification outputs. Tests cover source
integrity failure and repaired retry, resolver errors, Buf payload rejection,
aggregate limits and cancellation. The mixed-version fixture pairs nested fields
1 and 2 with distinct descriptor hashes under the same URL and checks those
pairs in canonical evidence. Resolver call counts prove that final replay does
not consult the registry. Sol reviewed production code and tests with no blocker.
These are library tests, not hosted publication or historical SQL qualification.

### Schema policy contract and library enforcement

`schema_policy.proto` adds `DocumentSchemaPolicy`, with immutable account identity,
encoding version, fixed validation profile, admission mode, resource limits and
explicit schema eligibility. Existing message names, tags and Any URLs are
unchanged. The eligible set can contain 1024 distinct exact URL/closure identities
within 512 KiB; that catalog limit is independent of the smaller per-member
binding count. Every nested payload uses the same eligibility check. The runtime
Document container is not a payload exemption: a Document used inside Any still
needs payload eligibility.

`DocumentAdmissionPolicy` normalizes allowlist order, rejects duplicates and
unknown content, and decodes exact canonical policy bytes. It supplies configured
limits and checks selected definitions before payload processing. The final proof
check independently verifies all occurrence identities, account, policy digest,
validation profile, root requirement and the exact limits used. A zero-root
content proof cannot become a typed verdict. `Proof.limits()` records those
limits so a claimed policy digest cannot hide weaker checking. The pure wrapper
still does not authenticate the active catalog pointer, command or caller.

V1 raw-fragment and decoded-payload ceilings are now each 256 MiB, matching the
existing bounded read selection default. Hosts can lower them and must reserve
memory for decoding and temporary copies. This restricts the new typed checking
path; it does not change provider conditional-write limits or enable unbounded
uploads. Operation-wide V48/V56 artifact and evidence limits still require
aggregation across members before staging and during commit.

OPAQUE_ALLOWED permits an explicit opaque operation. A member with a declared
schema still requires typed admission, and a failed typed attempt cannot retry
as opaque. TYPED_REQUIRED needs a complete proof with a payload root; when
require_structured_root is set, that includes CORE structured_data. Without that
flag, PARSED payloads can meet the root requirement. All publication entry points
must enforce these decisions when the policy catalog is activated.

Real validator fixtures cover generated and dynamic policy messages, oneof
presence, false permission flags, enums, every resource boundary, URL/schema
agreement and required fields. JSON Schema fixtures verify scalar bounds and
CEL metadata export. Cross-field eligibility, mode/root conditions and URL/schema
agreement still require runtime validation; canonical ordering, duplicates,
active-pointer freshness, proof correspondence, authorization and operation-wide
limits are handler obligations. No OpenAPI generator changes are included.

The contract compiles with complete imports, passes scoped Buf lint and passes
FILE compatibility against checkpoint b2ca8d7d. The next integration is the
account policy catalog and shared-lock freshness guard described above, followed
by native publication evidence writes. No policy administration RPC or typed
publisher is mounted by these definitions.

Operation classification for this checkpoint: canonical policy encode/decode,
policy-guided preparation and proof/policy correspondence checks are new library
operations. Existing member preparation and consuming verification are extended
with fixed byte ceilings and recorded checking limits. Public RPCs, opaque
storage operations, policy activation and transaction writers are unchanged.

The affected modules pass 397 tests, including 6 new contract tests and 5 new
policy library tests. The existing mixed-version fixture now also rejects a
nested disallowed fingerprint under an allowed URL, both during selection and
when independently checking a proof. Runtime dependency gates pass. Sol reviewed
the contracts, implementation and tests with no remaining blocker. These checks
do not qualify the planned SQL policy race or native publication activation.

### Persist policy snapshots and fence activation revisions

V58 adds immutable, account-scoped `document_schema_policies` and one
`document_schema_policy_current` pointer per account. Stored bytes must match the
SHA-256 key, fixed codec/version and 512 KiB limit. Pointer insertion starts at
revision 1; updates preserve the account and increment the revision exactly.
Pointers cannot be deleted and recreated to reset that identity. Returning to a
previous policy body creates another revision, so an old prepared selection
cannot pass through an A-to-B-to-A change.

The internal `DocumentSchemaPolicies` adapter provides new activate, read and
lock-current operations. Activation inserts a canonical snapshot and performs
compare-and-set on the expected pointer revision in one transaction. A stale
revision or cancellation rolls back both steps. An ambiguous activation result
requires reading the current pointer; the adapter does not retry against a newer
revision automatically. Trusted administration must authorize the caller before
this package-private adapter is invoked. The SQL guards protect consistency,
not the identity or authority of a policy administrator.

Preparation reads a decoded snapshot. The commit guard requires READ COMMITTED,
locks the pointer with FOR SHARE, checks revision and digest after any lock wait,
then decodes the immutable body while holding the pointer lock. It compares the
canonical bytes and account as well. Separating the pointer lock from body lookup
avoids relying on a joined row captured before a concurrent update. The native
publisher must call this guard after the operation/command fence and before
locking documents, drives and parts, retaining the shared lock through commit.
Activation acquires no document locks. Concurrent writers can share the pointer;
policy updates wait, and another account has an independent pointer.

The container has an implementation dependency on repo-admission for canonical
policy decoding. Byte providers and their SPI remain unchanged. This migration
adds no policy to existing accounts or revisions and does not activate any typed
writer. All-entry-point enforcement, administrator authorization, operation-wide
proof budgets and the transaction evidence insertion hook remain integration work.

Qualification passes 44 PostgreSQL tests across the policy catalog, concurrency,
schema association and native publication suites. Tests cover stale activation
rollback, account/content mismatch, A-to-B-to-A revision checks, mutation guards,
unsupported isolation, cancellation after snapshot insertion and pointer update,
and migration from populated V57 that creates no policies. Concurrency tests
observe real backend blockers: writers acquire shared locks concurrently, the
updater waits for both, another account progresses, and an earlier committed
update rejects a waiting stale writer. The blocker assertion accepts either
initial MultiXact holder, then verifies the remaining holder after one releases.
Sol reviewed code and tests with no blocker. Admission and engine runtime
dependency gates pass. Publication-path integration remains required.

### Complete-command schema preparation and staging

`DocumentSchemaBatch.prepare` is a new internal operation. It checks the complete
command against one selected account policy before staging: each supplied proof
must name the exact canonical command digest and full member, and must pass the
policy's independent proof checks. Required members cannot omit a proof. An
explicit null proof is an error even when opaque admission is permitted; unknown
member identifiers are rejected. An absent proof is allowed only as an explicit
opaque decision under policy, not as recovery from typed validation failure.

The immutable batch preserves each accepted proof and deduplicates their complete
artifact union. Operation bounds are 4096 roots, 64 MiB of encoded root evidence,
64 distinct artifacts and 64 MiB of unique artifact bytes. All checks happen
before staging. The new command-bound `RepositorySchemaArtifacts.stage` overload
checks account/operation scope, then fences ownership and compares the durable
command bytes and digest inside the staging transaction. Its generic byte-staging
overload remains unchanged. Cancellation or a failed command check rolls back
artifact rows and claims. A semantic command digest excludes operation ID, so it
cannot substitute for operation scope or owner-token checks.

The batch provides internal policy and artifact lock helpers for the forthcoming
publication integration. Artifact checks require current-generation claims and
lock immutable catalog and claim rows in digest order without fetching byte
payloads. These helpers are not yet called by the native publisher. Their
integration tests, aggregate admission boundary fixtures and all-path policy
enforcement remain outstanding; SQL staging limits already have separate tests.
Staging does not authorize document access or expose a revision. A policy change
after staging may leave retained staged claims, but must prevent stale publication.

The initial-policy absence race was also reviewed: use an account-scoped shared
publication fence and exclusive first-activation fence. Existing-pointer updates
serialize through the pointer row. Acquire exclusive advisory locks before row
locks, not in a pointer UPDATE trigger after its row lock; that reverse order
would deadlock with a native writer. The design records the legacy projection
boundary and the required real concurrency tests. This fence is not implemented
by this checkpoint and the catalog remains internal.

Qualification: 39 tests pass across `DocumentSchemaBatchTest` (6),
`DocumentSchemaCommandStagingIT` (4), `RepositorySchemaArtifactsIT` (22) and
`RepositoryOperationRecoveryFenceIT` (7). The preparation tests use real admission
proofs and cover independent command/member/policy mismatches, account mismatch,
proof-map snapshot behavior, cancellation and shared artifacts. PostgreSQL tests
cover exact command staging, no publication from staging, altered commands,
operation/token mismatch and rollback after artifact insertion. Sol reviewed the
implementation, fixtures and lock-order design with no remaining blocker for
this checkpoint. No protobuf contract or public entry point changed.

### Account policy activation and all body-write paths

V59 extends policy activation and existing body writers with a shared account
fence. Publications use the shared two-integer advisory namespace; activation
uses its exclusive mode before snapshot and pointer work. SQL first-pointer
INSERT also takes the exclusive fence. Existing-pointer UPDATE retains its row
serialization without acquiring an advisory lock late. All decisions compare
exact account IDs; hash collisions affect scheduling only. READ COMMITTED is
required so a waiting publication checks the committed policy after acquiring
its fence.

Extended operations and boundaries:

- `DocumentPublicationCommit` and `DocumentPublicationBatch` acquire the fence
  and reject configured policy before document locks. Batch destinations enter
  in stable account order. They still cannot publish typed revisions.
- `DocumentLedger.save`, guarded saves and locked-reference callbacks acquire
  the shared account lock before domain locks. Their mixed-purpose updates leave
  the body-versus-bookkeeping decision to SQL.
- SQL revision projection INSERT rejects any configured policy without an
  explicit binding. This covers both native and managed legacy publications.
- SQL document INSERT and body-changing UPDATE provide the same check for
  unmanaged legacy rows, which need not create a revision projection. Account
  retagging is rejected; transfer requires a new document address.
- Dedupe bookkeeping, lifecycle status and authorization-only mutations remain
  separate from content admission. They confer no typed verdict. Historical
  revisions are not rewritten when policy is activated.

An opaque-permitted policy also rejects an older unbound writer. Choosing opaque
admission must be a recorded decision under the selected policy, not an implicit
bypass. This remains an internal rollout: no default policy is inserted and no
public policy-administration API is enabled. Existing accounts without policy
continue to publish. Typed evidence persistence and the permitted bound-writer
path remain the next integration work.

Supported Java writers acquire account locks before document locks. A late SQL
check alone permits a lock-queue cycle: a legacy writer holding a document queues
behind an exclusive activation, which waits for a native writer already holding
the shared account lock and waiting for that document. Early Java acquisition
avoids that order. Direct SQL callers must follow the documented order too;
deadlock errors propagate and roll back rather than becoming successful writes.

Pre-V59 migration fixtures now use an explicit historical SQL publication helper
instead of calling today's Java publisher against an older schema. It still
exercises real revision, drive, attempt and retention locks and inserts the
actual historical rows; no missing-function fallback was added to production.

Qualification passes 84 tests across policy publication/concurrency/catalog,
schema artifact/evidence/association migrations, native publication, the real
provider-backed publication committer and the document ledger. A further 59 tests
pass across legacy revision projection migrations, part publication and the
attempt writer. New cases verify both first-activation race directions using
actual PostgreSQL blockers, shared writers, unrelated-account progress, rejected
unbound native/legacy/unmanaged publication, direct SQL rejection, immutable
account identity, unchanged historical rows and permitted dedupe bookkeeping.
Sol reviewed the implementation, lock order and historical fixture changes with
no remaining blocker. These checks establish enforcement, not a latency benchmark
or an enabled typed publisher.

The additional 64-case `DocumentAtomicPublicationIT` suite passes as well,
including concurrent batch publication, shared-source locks, transactional
rollback, cancellation and cleanup fencing: 207 affected tests total.

### Retaining a checked member's complete schema evidence

`DocumentSchemaRetention.prepare` and `write` are new internal storage operations.
Preparation takes a member proof from the complete checked command batch and
orders its fragment declarations, artifact digests, associations and root bundles.
It performs no registry access. The write requires an active transaction with the
owner, policy, selected physical objects and complete artifact set already locked.
It rechecks the durable command and current owner fence, then requires the exact
current-transaction, unsealed native revision for the member and destination node.

Before inserting evidence, the writer compares every persisted nonempty part with
the proof's complete member ordinals, part/sub-key, size and digest. Left joins keep
extra or invalid physical rows visible so qualification cannot silently filter
them out. Provider generation, realm, namespace, key, verification and account
must agree. Native sealing additionally enforces selected-attempt, manifest and
retention consistency. Descriptor validation and raw-byte hashing happened before
this transaction; their work is not repeated while holding SQL locks.

JDBC batches of at most 16 insert the exact normalized artifact references, schema
associations and root evidence into V55, V57 and V56 tables, respectively. Optional
source roles and multiple descriptor versions remain separate identities. JDBC
insert counts are checked; SQL errors and cancellation propagate. Any failure
marks the enclosing transaction rollback-only, including a failure caught by an
internal caller. Staged catalog bytes and claims remain available for retry after
a failed publication, while no revision or terminal success is committed.

The helper is not connected to the production publisher and grants no typed
admission verdict. The next binding must freeze exact expected evidence and
physical-part sets, selected policy revision/digest, body/metadata and the owner
attempt before document mutation, then enforce their equality at seal and terminal
commit. Counts alone are insufficient. The composition design records this
requirement for both typed and explicitly permitted opaque decisions. V59 still
rejects every configured-policy body writer without that integration.

The PostgreSQL fixture builds real document bytes, complete descriptors, a
canonical command and a proof through ProtoMolt's runtime validator. Physical
provider observations are explicitly synthetic SQL fixtures, not SDK success
claims. It leaves a native OPAQUE revision unsealed for the retention callback;
this tests storage consistency without advertising a typed publisher or replacing
real-provider qualification. The selected policy is internal preparation data,
not an activated account policy or an authorization grant.

Qualification: 39 tests pass across the six new retention cases, six batch
preparation cases, and existing V55/V56/V57 SQL suites (8/9/10). Retention tests
compare exact catalog bytes, associations and root evidence, reject a missing
physical fragment and wrong revision, preserve immutability after commit, and
roll back even when a caller catches a retention failure. Cancellation is injected
only after the test observes inserted artifact references in the same transaction.
Sol reviewed the helper, fixture and tests with no remaining blocker. This small
positive fixture has one payload root; multi-root ordering, full JDBC batch
boundaries, near-limit latency and policy-bound sealing remain integration work.

### Comparing the complete expected schema retention set

`DocumentSchemaManifest.prepare` and V60's
`document_schema_retention_manifest_v1` are new internal comparison operations.
The Java side freezes one checked member's nonempty physical slots and complete
artifact, association and root evidence identities. The SQL side reads the
corresponding native revision rows. Both sort arrays by the same identities,
include nullable source identities, and encode integer values as decimal strings
so JSON conversion cannot round a BIGINT. The manifest contains references, not
copies of descriptor or source bytes.

Preparation rejects missing or extra physical ordinals, mismatched part/sub-key,
size or digest, and a reused object with the wrong UUID. An explicitly permitted
opaque member has complete physical slots and empty schema sets. The SQL reader
rejects an unknown revision rather than returning an empty valid-looking object.
Both sides bound allocation before encoding and check the encoded byte limit.
SQL left joins preserve unqualified physical rows so they cannot disappear from
the comparison.

This is exact-set comparison, not an admission verdict. V52's physical binding,
selected-attempt and seal checks remain required; manifest equality does not
replace them. The future admission row must also bind body, metadata, policy,
command, member and owner attempt. V59's configured-policy rejection is unchanged,
and no public typed publisher is enabled.

Qualification: 26 tests pass (four manifest unit cases, three PostgreSQL manifest
cases, six retention cases and 13 native publication cases). SQL tests compare the
Java proof manifest before seal and after commit, reject missing evidence, reject
same-count substitutions in all four sets, and reject an unknown revision. Unit
cases cover typed and explicitly opaque preparation, wrong reuse identity,
missing/extra slots, wrong digest and cancellation. The real provider, multi-root,
near-limit allocation/latency and policy-bound admission checks remain separate
qualification work; this checkpoint does not claim those are complete.
Sol reviewed the Java/SQL parity and tests with no blocker. Its suggested
substitution coverage now includes every part, association and root field;
native physical sealing remains an independent required guard.

### Freezing a candidate's body and metadata before mutation

`DocumentAdmissionSnapshot.prepare` is a new internal read operation for the
forthcoming admission binding. It projects the complete candidate through
`document_publication_body` and `document_revision_metadata_v1` using a synthetic
`documents` composite. No document is inserted. Explicit SQL parameter casts and
the actual column types preserve integer precision, nullable fields and timestamp
conversion; the Java implementation does not duplicate the snapshot JSON format.
Comparisons use JSONB equality, not textual key order.

The native query explicitly uses `FlushModeType.COMMIT`: computing an admission
snapshot must not flush pending managed entity changes. Callers still prepare the
final candidate after authorization, physical selection and reuse resolution,
then freeze its body, metadata and manifest before merging any document. A later
admission guard must reject mutations that differ from these frozen snapshots.
The helper itself grants no authorization or typed admission verdict.

Metadata text is limited to the existing 1 MiB native revision bound. The new
internal admission body snapshot is bounded at 16 MiB. These are SQL snapshot
limits, not changes to the byte SPI's conditional payload bound. SQL checks sizes
before returning JSON text to Java. Oversized values are rejected explicitly.

Integration cases compare pre-write snapshots with actual committed native body
and metadata across UTC, New York and Kathmandu sessions, including nanosecond
inputs that round across a second boundary. Metadata rewrites compare the new row
against its pre-write snapshot while preserving the original revision metadata.
A dirty managed entity test proves snapshot preparation does not auto-flush and
that explicit flushing subsequently produces the expected metadata. Separate
synthetic codec cases exercise BIGINT values beyond double precision, explicit
nulls, and oversized body and metadata JSON; they do not claim those synthetic
inputs are validated repository documents. Sol reviewed the implementation and
tests with no blocker. Admission-row enforcement and production integration are
still required.
Qualification: 15 tests pass across six snapshot cases, six retention cases and
three manifest comparison cases. Both size-rejection branches are exercised.

### Binding admission to the exact publication transaction

V61 adds immutable `document_revision_schema_admissions` and extends the document,
native commit, projection and seal guards. `DocumentSchemaAdmissionBinding.insert`
is the new internal Java operation. It consumes the checked batch, finalized
candidate and selected physical parts, freezes the body/metadata and manifest,
and inserts without auto-flushing documents. A failure marks the transaction
rollback-only. The production native publisher and public policy administration
are not wired to this operation yet; this is not an advertised public typed API.

An admission records the allocated revision, account, principal, operation, owner
generation, member, node, selected attempt revision, previous document mutation
revision, canonical command digest, exact policy revision/digest and explicit
TYPED or OPAQUE decision. Policy references target immutable snapshots rather than
the mutable current pointer. The insertion guard requires the live owner fence,
shared account policy lock and current-pointer `FOR SHARE` lock. It checks the
command and selection, locks an existing destination row and requires its sampled
pre-change revision. Admission must precede the document mutation.

For configured policies, a body write now requires the same-transaction admission
with exact body, metadata and pre-change revision. One binding cannot authorize a
second mutation in its transaction. The previous unchanged-body exception remains
for ordinary metadata/bookkeeping changes when no admission exists for the node
in that transaction. Commit and projection guards bind the exact revision, owner,
member, selection, decision and snapshots; sealing compares all retained manifest
sets while preserving V52's independent physical and selected-attempt checks.
The policy pointer is rechecked at commit/seal/completion, including changes made
by the publishing transaction itself.

Deferred admission completion requires the sealed native revision and terminal
operation success in the same transaction. Unused headers cannot commit. Typed
decisions require nonempty schema evidence; explicit opaque decisions require
empty schema sets. SQL enforces the storage identities and transaction, while the
Java proof and checked policy determine protobuf validity and whether opaque
admission is permitted. A failed typed check never falls back to opaque.

Snapshots are bounded per member (16 MiB body, 1 MiB metadata, 16 MiB manifest),
and per operation (64 members, 64 MiB combined snapshot text). Stored generated
byte counts keep aggregate checks from repeatedly rendering earlier JSON values.
These internal admission limits do not change the byte SPI conditional payload
bound. Large-batch latency still requires measurement before production exposure.

The SQL fixture exercises real runtime schema proofs and active policies for
successful typed and explicit opaque decisions, preserving exact historical
policy and metadata after later changes. Rejection cases cover changed candidate
body/metadata, missing retained evidence, unused admissions, policy revision
changes (including same-transaction changes), repeated mutation and immutable
headers. Physical observations remain explicitly synthetic, and these cases do
not replace real-provider or transport conformance testing.

Qualification: 128 tests pass across the 10 new binding cases, policy publication
and concurrency suites (6/4), native publication (13), retention (6), snapshots
(6), manifests (3), atomic publication (64) and legacy revision projection (16).
Sol reviewed the final migration, Java helper, lock order and tests with no
blocker. The final migration includes the indexed node/transaction lookup and
stored snapshot sizes. Public caller integration, a populated bound-revision
upgrade/restore rehearsal, binding-specific cancellation and near-limit batch
measurements remain to be completed before this capability is exposed.

### Native publisher integration with checked schema batches

`DocumentPublicationCommit.commit` is extended with an internal checked-batch
entry. The existing unbound entry is unchanged and still refuses typed or
configured-policy publication. The new entry requires the exact canonical command,
staged schema assets for its owner generation, and checked ordinary content for
exactly the proofless members. Typed members derive their assembled document and
fragments directly from the immutable runtime proof. They do not resolve schemas,
deserialize again or repeat runtime validation under SQL locks. No second caller
content value can compete with the proof for a typed member.

The commit locks the selected policy before document locks, then locks the full
schema artifact union after physical binding. It prepares every candidate and
inserts every admission header before merging any document. The shared member
writer retains artifact references, schema associations and root evidence after
physical part rows and before sealing. TYPED requires its retention writer;
explicit OPAQUE requires no schema evidence. Seal and terminal checks remain the
same V61 checks. A stricter host requirement still rejects an opaque-only member.

For typed revisions, occurrence-specific retained evidence replaces the legacy
single-root `structured_resolution` observation. The publisher does not report
`not_attempted` for a value it has validated. Opaque publication preserves its
explicit not-attempted observation. Both decisions retain exact physical manifests
and the existing canonical result/replay binding.

Tests exercise the actual native committer for typed and explicit opaque members,
including an explicit structured-schema contract, replay, missing opaque content
and a stricter host policy. A LocalStack case uses real versioned provider writes
for a two-member operation: one member carries a checked StringValue payload and
the other preserves an unresolved opaque Any. It verifies both decisions, exact
result replay and subsequent provider-version/digest reads. These internal APIs
are not yet connected to the public repository host or policy administration;
public examples must not advertise that integration as available.

Recovery qualification injects a real PostgreSQL failure immediately before root
evidence insertion, after observing the admission, document, native commit and
unsealed projection in that same transaction. All publication/evidence rows roll
back; normalized staging bytes and owner claims remain. Retrying the same prepared
operation then publishes once and replays the exact result. This uses synthetic
physical observations for the focused SQL recovery case, separately from the
mixed real-provider case above.

Qualification: 100 tests pass across typed native publication (5), retention
failure/retry (1), provider-backed native commit (11), admission binding (10),
atomic publication (64) and content checks (9). Sol reviewed the integration and
recovery test with no blocker. Historical decoding through authorized retained
schema reads, host/policy administration integration, typed cross-transport
conformance and large-batch latency remain required work.

### Exact containing schema association

V62 adds the containing schema role to the immutable admission header. The checked
proof supplies its exact container reference; the header points to that revision's
retained association by type-URL digest and descriptor digest. The association
already binds the metadata and optional source artifacts. This distinguishes a
containing Document definition from payload aliases that share its descriptor.
New TYPED admissions require the pair, OPAQUE admissions require both fields to
be absent, and a deferred foreign key requires the association before commit.

Pre-V62 admissions retain an unknown container role. Migration does not infer one
from unordered associations or invent historical provenance. An authorized
historical reader must report that limitation when typed reconstruction needs the
container role. Raw preserved content remains a separate read capability. The
role binding does not itself implement historical authorization or decoding.

Qualification: 58 distinct tests pass across admission proofs (10), admission
binding (14), typed native publication (5), retention failure/retry (1),
provider-backed native commit (11), populated migration (2), retention (6),
snapshots (6) and manifests (3). Fault injection during fresh publication verifies
missing, half-present, opaque-supplied and unretained container associations fail
and roll back. Real V61 typed and opaque admissions migrate to V62 with NULL roles,
unchanged evidence bytes and manifests, and preserved terminal success. The
focused SQL fixtures use synthetic physical observations; the native commit suite
separately exercises versioned LocalStack storage. Sol reviewed production code,
the fault tests and populated upgrade fixtures with no blocker.

### Authorized historical schema replay against supplied bytes

`DocumentHistoricalSchemas.check` is a new internal operation. Current READ
authorization on the exact document precedes revision lookup. A bounded SQL
snapshot requires a sealed native revision, matching terminal success, typed
admission and the V62 container role. It copies only that revision's retained
artifact, association and evidence sets, with count and byte checks before bulk
copying. It also checks the stored manifest, account, transaction, body and metadata
bindings. The complete historical command and policy stay internal.

Outside database locks, the reader reconstructs the canonical command, selects
the exact member and decodes the historical policy. It replays every supplied
fragment and occurrence through the existing runtime validator using the retained
definitions, then compares the exact artifact and root sets. No registry lookup
or fallback occurs. Fragment identities already verified by admission are reused
when checking root records, avoiding repeated hashing of a large fragment for
each root. Current authorization is checked again before delivering either a
proof or detailed failure. Cancellation propagates without a proof.

Missing retained artifacts and invalid supplied fragments report DATA_LOSS.
Opaque revisions and legacy typed revisions with an unknown container role report
FAILED_PRECONDITION. Unauthorized callers receive the same NOT_FOUND response for
existing and nonexistent revisions. The current account policy does not replace
the retained historical policy during replay.

This is replay against supplied exact bytes, not a historical provider-read API.
The host still needs historical pins, protected provider reads, aggregate admission
for concurrent read memory, restart qualification and public transport integration.
The new tests use real PostgreSQL and runtime validation with explicitly synthetic
physical observations; they do not claim provider availability.

Qualification: 31 tests pass across historical replay (7), revocation during
replay (2), existing publication replay (8) and admission binding (14). A fresh
reader reconstructs the exact proof for a different, normally authorized principal
after the current policy advances. The two revocation tests were also run with
delivery reauthorization temporarily removed: one incorrectly returned a proof and
the other exposed DATA_LOSS details. Both failed as expected; restoring the gate
returned both to green. The final nine historical tests were rerun after the last
control-flow adjustment. Sol reviewed the SQL reader, replay boundary and tests
with no blocker.

### Historical scope for physical read pins

V63 extends `document_read_pins` and its insertion guard with CURRENT and
HISTORICAL scopes. Existing rows and callers default to CURRENT, preserving the
current-revision checks used by publication reuse. HISTORICAL can pin a superseded
sealed revision with its exact retained DOCUMENT_HISTORY reference. Both scopes
require the same active reader, verified origin/object, exact node/revision/part
identity, and open retention state. The host must acquire the complete origin set
before retention rows and separately authorize the current document.

The DOCUMENT_READER mirror, release functions and quiescent recovery functions
are unchanged. Pin identity and scope remain immutable. Releasing a drained pin
does not require its source revision to remain current. Existing historical
references still prevent reclamation after reader release; this does not implement
history pruning. The SQL capability is internal and does not yet add an authorized
historical Java read plan, provider I/O, or public read endpoint.

Qualification: 134 tests pass across existing reader pins (17), upload admission
(90), upload coordination (20), historical pin guards/lifecycle (5) and migration
and native-turnover compatibility (2). The new cases cover superseded legacy
revisions and native revisions whose objects are reused by a newer native
revision. CURRENT still refuses those old revisions. Tests verify exact reader
references, immutable scope, identity mismatches, retirement, reader fencing,
release rollback/idempotence and quiescent recovery. A live V62 pin migrates to
CURRENT with its identity intact and can release after upgrade. These are real
PostgreSQL cases with synthetic physical observations, not provider-read evidence.
Sol reviewed the migration and final tests with no blocker.

### Shared Java reader lifetime

`DocumentReadLedger.PinnedRead<P>` now owns the common use, transfer, drain,
release and recovery lifecycle. The current `PinnedPlan` specializes it with
`DocumentRetainedReadPlan`; existing `PinnedPlan.Use` consumers still compile.
`DocumentReadBatch` and the internal provider read helper accept the shared use
type. SQL pin acquisition, authorization and protobuf contracts are unchanged.
Historical plan capture still needs implementation; the shared handle alone is
not historical read authority.

Qualification: the same 134 distinct pin/admission/coordinator tests pass after
the extraction. The final provider helper change was followed by rerunning all
90 upload admission and 20 coordinator tests. These include capture blocked in
SQL during fencing, transfer after fencing, failed release recovery and ownership
through provider completion. Sol reviewed the lifecycle diff with no blocker.

### Authorized native historical capture

New Java operation `DocumentReadLedger.captureHistorical` checks current document
READ access before looking up a revision and atomically captures its complete
HISTORICAL pin set. It returns a ledger-issued `DocumentHistoricalReadPlan` with
the archived manifest, full part ordinals, physical object IDs and original
provider bindings. It accepts native typed and opaque revisions. Legacy history
is explicitly refused by this entry point, without guessing a native binding.
No protobuf operation or field changed.

Capture holds the active-reader lock before document locks, locks complete origin
and retention sets in database UUID order, and rolls back all pins on failure.
Manifest JSON is limited to 64 MiB before JDBC transfer and parsed once; part sets
retain the 10,000-entry bound. No registry or provider call occurs under these
locks. Normal READ access is sufficient without ownership of the old operation.
Pins confer physical protection only; delivery authorization, real historical
provider reads and process-restart qualification remain unfinished.

Qualification covers typed and opaque capture, denied and cross-account callers,
legacy refusal after migration, native-to-native replacement, transfer/drain,
and all-or-nothing refusal when the later sorted object is retiring. Physical
observations in these PostgreSQL fixtures are synthetic; these tests do not prove
provider availability. Existing publication/admission/coordinator cases also run
against the shared manifest validator.
There are 148 distinct passing tests: historical capture (5), historical migration
and turnover (2), upload admission (90), upload coordination (20), part publication
(26) and typed publication (5). The initial typed capture fixture omitted policy
activation and was corrected; all seven historical cases passed again after that
fix and the normal-reader authorization assertion.

### Protected historical provider reads

New Java operation `DocumentPartReader.readHistorical` consumes `PinnedHistory`
and reads its original backend generations, namespaces, keys and provider versions.
It verifies retained byte identities through the existing bounded provider reader
and returns raw fragments without decoding or reserializing protobuf content.
Unresolved `Any` bytes are preserved in this mode. The batch owns its payload
reservation and transferred pin use until both the caller and actual workers
finish. Hosts still close, drain and release SQL pins explicitly.

`PinnedHistory` retains the capture-time authenticated caller and address. Delivery
checks current READ authorization again before returning content or detailed
provider failures; callers cannot swap identity at that check. Cancellation and
deadline errors carry a generic status without provider details. Control is
checked again after SQL lock waits and transaction completion. Typed schema replay
composition, public transports and process-restart qualification remain separate
unfinished work; this Java reader is not a public historical endpoint.

Qualification uses real PostgreSQL and versioned LocalStack storage. Cases cover
old-version reads after a same-key provider overwrite, denied delivery after a real
GET on both success and injected failure, and a noncooperative worker that retains
pins and payload capacity until it exits. Two SQL contention tests cancel or
expire the request while delivery authorization waits on the document row.
Provider interception delegates the actual GET before injecting faults; it does
not substitute successful bytes.
The retained-publication cases also read the superseded native revision after
replacement, covering unchanged and replaced parts plus a 40-chunk document.
There are 42 distinct passing tests: provider publication/read (15), delivery
lock-wait control (2), historical capture (5), and upload coordination (20). The
provider suite passed again after adding superseded-read assertions. Sol reviewed
the implementation and failure-boundary tests with no blocker.

### Typed historical provider read composition

New Java operation `DocumentHistoricalReader.readValidated` combines the protected
provider batch with retained-schema replay. Its closeable result exposes the
document, address, exact revision, validation profile, command hash and policy
hash. Admission proof internals and sibling command members remain private.
No live registry input is accepted and no protobuf contract changed.

The shared payload budget reserves fragment copies before conversion and exact
command/policy, artifact and evidence byte groups before SQL transfers. Partial
reservations are released on failure; successful results retain them and the raw
provider batch until close. This is serialized-byte accounting, not a JVM heap
limit. The bound validation method rechecks control after error-authorization
waits and performs controlled current authorization before successful delivery.

Qualification: 28 distinct tests cover budget lifetime, partial-capacity failure,
cancellation after reservation, retained-policy replay, existing schema revocation
boundaries, and provider publication/read behavior. The real-provider typed case
checks the decoded document and provenance, deadline expiry after provider I/O,
missing retained assets as DATA_LOSS, opaque typed refusal, and explicit raw
preservation afterwards. The engine runtime-dependency gate passes.

An additional fresh-JVM test publishes a runtime-defined custom protobuf type
and reads it through `DocumentHistoricalReadWorker`, using real PostgreSQL and
versioned object storage without writer descriptors or fragment inputs. A second
fresh JVM fails with DATA_LOSS after removal of that exact retained descriptor.
Both children release their read pins. This establishes process-local cache
independence; the writer process remains alive.

The composed-reader revocation case uses real PostgreSQL locks to stop retained
schema capture after provider I/O and queue a current ACL update. It observes
final authorization waiting behind that update before committing revocation.
The result is generic NOT_FOUND with no cause, no delivered document and zero
remaining payload reservations or read pins. Full deployment restart and public
transport integration remain to be qualified; no public historical transport is
advertised.

### Native host and historical transport integration inventory

This records planned work after checkpoint `9d1f7c7f`; none of these entries
advertises a mounted historical endpoint.

- **Unchanged contracts:** `GetDocument`, `GetDocumentByReference` and
  `GetDocumentResponse` retain current-state and partial-assembly semantics.
  `DocumentPublishedRevision.revision_id` remains the immutable revision UUID;
  it is not a provider version, mutation counter or JCR version.
- **New shared coordinator over existing operations:** compose native command
  admission, selected schema policy, authorized descriptor resolution, upload
  staging and `DocumentPublicationCommit`. Reuse the canonical command and
  `DocumentPublicationResult` instead of creating another retry/receipt format.
  Qualify real publication and durable retry before mounting it.
- **Extended host composition:** `RepoServices` must own managed document reader
  resources, original-generation provider resolution, shared capacity and
  shutdown with quiescence-backed pin recovery. A restart alone does not prove
  that old readers have stopped. Its current constructor call supplies neither the managed
  reader nor writer to `DocumentOperations`. Native policy-bound publication
  requires its own coordinator; enabling the older writer does not satisfy it.
- **New shared historical operation:** take trusted caller identity, exact
  address/revision and an explicit raw-preservation or validated mode. Own the
  pinned plan and result through delivery. Use the retained manifest, original
  bytes and existing admission identity. Keep current authorization separate
  from the historical policy snapshot.
- **New additive transport contract, not yet defined:** expose the shared
  operation through a thin adapter after choosing response bounds/framing and
  resource lifetime. Reuse existing address, manifest and revision types.
  Keep protobuf names, existing tags, imports and Any URLs intact. The host must
  bind scoped account/ACL identity; request coordinates cannot establish access.

The [host integration sequence](repository-composition.md#host-integration-sequence-after-historical-replay-qualification)
defines the required local/gRPC conformance flow and the optional JCR boundary.

### Owned publication fragment snapshots

New internal `DocumentPublicationFragments.capture` preflights the complete
canonical member/nonempty ordinal sets and declared sizes, reserves the aggregate
serialized size from a caller-supplied `PayloadBudget`, then makes private byte
copies. Its closeable owner keeps those reservations through preparation and
commit; any failure releases them. No fixed schema-artifact limit is reused as
a document-byte limit. Capacity exhaustion is explicit RESOURCE_EXHAUSTED.

Input bytes require their own reservation and must stay stable during capture.
Returned immutable maps are borrowed: a candidate or proof using their bytes
must finish before the owner closes. This helper accounts only its fragment
copies. It neither reserves descriptor/proof/decoded memory nor establishes
authorization, provider durability, physical selection or schema validity.
Those remain coordinator/admission/commit obligations. No host or RPC is mounted.

Tests overwrite borrowed source arrays after capture, mutate the source maps,
and validate the independent snapshots with the real policy engine. They cover
incomplete sets, wrong sizes, exhausted capacity, cancellation after a copy,
immutable views and idempotent close. The real PostgreSQL/provider publication
test keeps the snapshot open through schema staging and native commit, releases
its budget, then verifies retained-schema replay from a fresh JVM.

### Evidence allowance before canonical encoding

Extended internal root-evidence encoding accepts the member's remaining byte
allowance. It measures and shape-validates the bundle, then rejects an excessive
size before allocating canonical occurrence-path buffers. Preparation supplies
the remaining allowance directly; accepted canonical bytes and hashes are
unchanged. Tests cover the exact boundary, one byte over and rejection before
duplicate-selector processing. Evidence projection and shape validation still
precede this guard; it is not complete admission-memory accounting.

### Owned canonical encoding and comparison scratch

New `DocumentAdmissionReservations` supplies nonblocking byte leases without a
storage or transport dependency. The internal canonical codec has an additive
owned encode path: validate/measure, reserve, allocate one private output array,
then keep the lease until the encoded owner closes. Failed encoding releases it.
The array is transferred to an immutable ByteString without a second full copy.

The additive budgeted decode path owns only the canonical comparison scratch and
closes it before returning, including on a noncanonical-wire failure. Existing
caller-accounted overloads remain explicit; reservation failure never selects
those overloads as a fallback. Neither path accounts for input bytes, parsed
messages, validators or descriptor/object heap. Full preparation and proof
ownership still need to propagate these reservations.

Canonical string-map sorting compares validated Unicode scalar values directly,
which preserves unsigned UTF-8 byte order without allocating encoded key buffers.
Tests compare against independently written canonical wire bytes across UTF-8
width boundaries and supplementary characters. Additional cases verify exact
serialized-byte peak, owned lifetime, prompt decode-scratch release, capacity
refusal, cancellation and idempotent close. Existing canonical format and hash
contracts remain unchanged.

### Owned root-evidence sorting and replay

The root-evidence codec now has additive owned encode and budgeted decode paths.
Each canonical occurrence buffer has a lease while it participates in sorting
and duplicate checks. All those leases close before allocating the final encoded
root evidence; only the final output lease escapes. Byte-bound checks precede
these allocations. UTF-8 text lengths are measured without creating encoded
string buffers.

Budgeted decode closes the generic canonical comparison buffer, then independently
checks occurrence ordering with owned sorting/output buffers and closes those
before returning. Tests use capacity equal to the larger of the path-buffer sum
and final output size, proving these phases do not overlap their reservations.
They also cover second-path capacity refusal, cancellation after an earlier path,
duplicate selectors and noncanonical ordering, with zero residual reservation.
Canonical bytes and hashes are unchanged. Full preparation/proof ownership is
still pending; these are internal codec building blocks.

### Fragment replay scratch lifetime

Fragment replay accepts an explicit reservation provider for canonical evidence
scratch. Sorting and output leases close before descriptor lookup; capacity or
cancellation failures propagate without selecting the caller-accounted overload.
Occurrence replay validates and measures paths without serializing a buffer that
would only be used to count bytes. Canonical wire validation remains the evidence
decoder's responsibility before replay.

Tests replay both parser roots with the real validator, compare the resulting
locators with the existing path, check the peak against the largest bundle, and
assert zero scratch bytes at descriptor lookup. Refused capacity and cancellation
after reservation leave zero leases and never reach schema loading. Input bytes,
retained schema assets and parsed results remain caller-owned; preparation and
the returned proof still need complete reservation propagation.

### Owned preparation and policy verification

New additive Java overloads on `DocumentSchemaAdmission.prepareAndCheck` and
`DocumentAdmissionPolicy.prepareAndCheck` accept `DocumentAdmissionReservations`
and return a closeable `PreparedProof`. The original caller-accounted overloads
remain available. No protobuf fields, import paths, hashes or RPCs change.

Preparation reserves before copying descriptors and optional sources, deduplicates
them by artifact digest, and owns generated canonical metadata. Initial evidence
stays reserved through independent replay. Replay reserves canonical comparison,
sorting, descriptor fingerprint and root-locator scratch, releases temporary
allocations, and retains final evidence with the asset copies. Only the independent
checker can construct the ordinary immutable proof. `PreparedProof.proof()` borrows
that proof until close; callers must finish all consumers first. Fragment inputs,
resolver allocations, policy bytes and parsed JVM object graphs have separate host
ownership and bounds. There is no claim that these leases measure total heap or
the complete SQL/provider/transport publication path.

Reservation failure is kept distinct from retained-data corruption, including
when the host throws `IllegalArgumentException`. The public preparation boundary
restores the original exception identity. Cancellation, invalid retained bytes,
resolver errors and policy verification failures close acquired owners; none
selects an unbudgeted retry or an opaque result.

Tests verify exact retained-byte totals, independent descriptor/source copies
after borrowed buffers are reclaimed, shared artifact deduplication across aliases,
failure and cancellation at every reservation point, post-preparation policy
verification failure, and idempotent close. The real PostgreSQL/versioned-provider
publication test holds both fragment and proof owners through staging and commit,
releases them, and validates the retained custom type in a fresh JVM. This extends
the existing historical replay qualification; a mounted native coordinator and
its complete end-to-end resource accounting are still pending.

### Owned upload-preparation handoff

New internal `DocumentUploadCoordinator.stageAndPrepareOwned` transfers an
independently owned callback result only after final authorization, selection and
operation-owner checks and worker/view draining succeed. A later failure closes
that candidate and preserves the primary exception, attaching any cleanup error
as suppressed. Before the callback returns, it owns its own failure cleanup and
must not retain the borrowed upload view. Successful callers own the returned
result through staging/commit and close it after its consumers finish.

Real-provider tests cover successful transfer, cancellation after callback return,
expired ownership and cleanup failure. They assert that upload reservations have
drained before transfer and that the candidate closes exactly once on rejection.
Expiry is injected explicitly in the test database; ordinary SQL renewal guards
reject shortening a live lease. The earlier expiry fixture was corrected to test
the intended post-preparation owner fence. The native publication coordinator
still needs to use this handoff with its combined fragment/proof owner.

### Combined publication candidate ownership

New internal `DocumentPublicationCandidate` owns the complete command's fragment
snapshot, typed proof owners, checked opaque content and schema batch. Every member
has an explicit typed or opaque mode; required typed policy and explicit schema
conditions reject opaque selection before copying bytes. The resolver receives
the command member and occurrence context for host-scoped schema access. A later
member failure releases earlier proofs and fragments without changing modes.

Opaque members require no descriptor lookup and use independently supplied assembly
limits. Typed members use the budgeted policy path, and batch verification now has
an additive overload for its canonical scratch reservations. Candidate accessors
are borrowed through close; source pins, authorization, current policy selection,
provider/SQL copies and parsed object heap remain separate host obligations.

Tests cover mixed modes, exact surviving reservation totals, preflight refusal,
later-member registry failure, and capacity exhaustion after fragment capture.
The real-provider historical test now prepares this candidate inside
`stageAndPrepareOwned`, holds it through schema staging and native commit, closes
it, and replays the retained custom type in a fresh JVM. This proves the composed
internal path. The production host still needs to supply this orchestration,
authorized schema/backend resolution and shutdown/recovery lifecycle; no new
public RPC is available from this change.

### Retained-read composition port

New `DocumentRetainedReader` is a container-owned host port accepting a
ledger-issued `PinnedPlan`, member identity and read control. Its closeable batch
exposes ordered borrowed `PartObject` values. `DocumentPartReader` and
`DocumentReadBatch` implement these interfaces directly; existing engine callers
retain their concrete return types. No production container-to-engine dependency
or protobuf change is introduced.

The real-provider retained-read tests invoke the port and verify reordered source
slots with upload/EMPTY ordinal gaps, exact bytes, capacity refusal, and reader
pins held through batch consumption or actual cancelled-worker exit. Host shutdown
and pin release remain explicit. This port supports the facade design recorded in
`repository-composition.md`; it does not itself mount native publication.

### Protected mixed publication inputs

New internal `DocumentPublicationInputs` combines a borrowed upload view with
protected retained-reader batches. Before provider I/O it verifies the canonical
command, operation ID, account, principal and owner generation, then matches the
complete upload and reuse slot sets. Canonical command bytes deliberately omit
the operation ID, so their equality alone is insufficient here.

Retained batch positions map to the selected entries' full revision ordinals;
upload and EMPTY gaps are preserved. Part kind, sub-key, declared digest and size
must match the selected source. The provider reader verifies actual bytes and
original physical bindings; candidate admission still checks the complete content.
The input owner keeps batches and a plan use alive until its consumer has copied
bytes into an owned candidate. It closes local resources on failure but leaves
SQL pin closure, drain, release and recovery with the coordinator.

Real-provider tests combine an upload with reordered retained parts and an EMPTY
slot, copy the complete inputs under a separate reservation, and verify the copies
after closing the borrowed inputs. A malformed batch shape injected after real
reads releases its reservations. A different operation with identical canonical
bytes and a different owner generation are refused before provider reads. This
adapter prepares the remaining facade wiring; it does not grant publication
authority or mount a service.

### Composed native publication preparation

New internal `DocumentPublicationPreparation` connects upload staging, protected
retained reads and owned candidate admission. It returns the candidate and the
exact staged upload selections together. The upload coordinator transfers that
owner only after its post-preparation fences succeed; rejection closes it. Request
deadline/cancellation checks and upload worker failure checks both remain active
while reading retained bytes and checking the candidate. Checked parser failures
retain their identity across the synchronous callback, rather than being labeled
as caller errors regardless of their source.

The native commit integration suite now uses this composition for opaque and
typed revisions, both retained-only and mixed with fresh uploads. An EMPTY slot
shifts the full ordinals. Tests verify unchanged physical provider versions for
reused bytes, new versions for uploads, atomic revision publication, exact
idempotent replay and superseded reads. The larger retained-parts case still
crosses the SQL batch boundary. A schema resolver failure during the composed
flow leaves the previous revision current, produces no successful operation
result and releases the shared serialized-byte reservations and source pins.

This is a private library composition, not the mounted host operation. The host
still owns authenticated placement and schema access, initial policy selection,
reader incarnation, pin drain/release and recoverable cleanup handles. Transport
error mapping must distinguish invalid candidate input from corrupt retained
content before a public adapter is mounted. Schema staging and the native commit
remain explicit after preparation; no protobuf fields or public RPCs change.

### Bounded ownership of document reader cleanup

Extended `DocumentReadLedger` retains successfully captured handles until their
SQL release or recovery succeeds. Capture admission reserves a slot before SQL;
failed captures return it. Closed but undrained handles and drained handles with
failed releases still consume capacity. The default is 32 outstanding reads per
ledger, with an explicit constructor limit for the host. This bounds handle
count, not total JVM heap or physical pin count; per-command limits and payload
reservations remain separate.

New `releaseDrained(limit)` retries a bounded selection of closed, actually
drained handles. It performs SQL outside the lifetime monitor, reports failures
with their causes, keeps failed handles for retry and rotates them behind other
candidates. Direct and sweep releases share the same per-handle serialization;
concurrent sweep calls count a successful local release transition only once.
Open plans are never closed automatically, and future cancellation is not proof
that a provider stopped. `outstandingReads()` includes captures in progress.

PostgreSQL tests cover capacity before and after drain, denied and concurrent
capture, competing release passes, real lock-timeout failures for multiple pin
sets, and successful bounded retries after the lock is removed. Caller references
are dropped after closing the plans in the SQL-failure test, so the ledger must
retain their cleanup handles itself.

This does not complete durable host recovery. An unknown SQL capture outcome
requires incarnation-scoped discovery after proven quiescence. A lost release
acknowledgment needs the explicit reconciliation below when V45's original
physical identities no longer exist. The host must compose these operations with
shutdown, deadlines and recovery scheduling before its lifecycle is complete.

### Explicit reconciliation of uncertain reader outcomes

New `DocumentReadLedger.reconcileDrained(limit)` confirms exact tracked handles
after durable quiescence. It does not delete pins or silently reinterpret a failed
release. `DocumentReadPins` checks native pin IDs and `DOCUMENT_READER` mirror
owner IDs independently of physical-object existence. Mismatched reader/object
bindings fail; a remaining matching representation keeps the handle and its
capacity. Only complete absence retires the local handle. Confirmation shares
the per-handle completion lock with release and recovery. Bounded passes rotate
unconfirmed handles so one pending set cannot starve later completed sets.

Real JDBC fault tests execute PostgreSQL commit before throwing a response error.
An uncertain capture returns no handle but leaves durable pins discoverable by
`DocumentReadRecovery`; an uncertain release leaves a tracked handle despite the
committed deletion. Both converge after fencing, actual local quiescence and
bounded recovery/reconciliation. Tests also cover partial recovery, another
reader's independent pins, and false confirmations caused by orphaned mirrors or
mismatched identities. A separately labeled catalog fault proves confirmation
does not require the old physical location rows; it is not evidence that history
pruning has been implemented. V45 and its rejection of unregistered release claims
remain unchanged. No protobuf or transport contract changes.

### Reader lifecycle composition

New `DocumentReadLifecycle` combines bounded cleanup passes and retryable shutdown
for one reader incarnation. Its worker-lifecycle port is implemented directly by
`DocumentPartReader`; the container still has no engine runtime dependency. A
host schedules `tick()` while serving and repeats `shutdownStep(waitBudget)` during
shutdown. The component owns no background thread or provider/database client.

Shutdown stops reader and ledger admission, closes captured plans to new uses,
waits for actual provider operations/workers and then all transferred batch uses,
and only then attests durable quiescence. A capture whose SQL response arrives
after shutdown started is registered closed; its in-flight capture count prevents
premature quiescence. Each shutdown step performs at most one durable recovery
batch and one local reconciliation batch. A positive recovery count requires a
later pass observing zero. False, interruption or an exception keeps shared
resources host-owned and shutdown retryable. The local wait budget is shared
between worker and batch waits; SQL timeouts remain separately configured.

Real-provider tests hold returned batches and deliberately keep cancelled S3
workers running; shutdown cannot complete or release pins until those owners end.
PostgreSQL tests cover normal cleanup without closing admission, interrupted waits,
cleanup lock timeouts followed by retry, and a capture gated after real commit but
before the handle reaches the ledger. JDBC fault injection is shared with the
acknowledgment-loss tests and delegates every database operation to PostgreSQL.
This qualifies the lifecycle component, not a complete `RepoServices` deployment:
production scheduling, authenticated publication setup, transport mounting and
full host restart qualification remain outstanding.

### Shared native publication execution

New internal `DocumentPublicationExecution` connects authorized replay, active
policy selection, protected source capture, owned preparation, schema staging
and atomic native publication. It accepts an already-admitted owner and prepared
plan; owner nonce, takeover, attempt identity retention and qualified placement
remain host admission obligations. It does not create replacement operations.
The owner account and operation must match the command before the replay path.

A committed replay rechecks current document access and request control, then
returns the exact stored result without payload, registry or provider work. New
execution closes its candidate after staging/commit and always closes the source
plan locally. The ledger retains SQL cleanup ownership for lifecycle passes;
cleanup is not performed as an untracked after-commit action. Schema and event
writes remain in their existing fenced transactions, including the event delivery
setting.

The real-provider native commit tests now invoke this execution path for typed
and opaque revisions, retained-only and mixed uploads, EMPTY ordinal gaps, and
schema resolution failure. They replay successfully after stopping the upload
coordinator and reader, reject mismatched owner accounts and denied callers,
check cancellation after replay lookup, and verify event delivery state. No
public transport is mounted by this change. Production admission/configuration,
error mapping, scheduling and full host restart qualification remain required.

### Retained operation-admission session

New internal `DocumentPublicationSession` authenticates account/principal scope,
then mints its owner nonce and upload-member attempt identities before operation
admission SQL. It retains the same `DocumentOperationUploadAdmission.Prepared`
object, including that object's private selection tokens and immutable placement
snapshot. Retry uses the same identities and rechecks caller bindings and request
control. Empty admission means no executable ownership was granted; it never
renews a lease or takes over an operation. Terminal outcomes require authorized
replay. The real-provider publication fixture now enters operation admission
through this component.

PostgreSQL tests lose the response after the actual admission commit, then verify
that retry returns the stored nonce and generation with the identical prepared
plan. They also cover cancellation after commit, rejected caller bindings without
an operation row, a competing session that receives no owner, and no implicit
lease extension. The host must retain this session across uncertain outcomes;
this component is not a persistent session registry or a restart/takeover manager,
and its admission method alone does not serialize publication execution. The
session execution scope below supplies that local guard.

Typed/opaque selections remain internal host policy choices, outside
`DocumentPublicationIntent`. Before mounting a caller-facing publication API,
retain the session's stable choices or derive them deterministically from its
command and selected policy. Any new caller-selectable preference that
changes admission meaning must become part of canonical request identity; do not
accept an unbound request flag. Existing structured-schema requirements and
commit-time policy fences remain in force. No protobuf fields change here.

### Serialized publication-session execution

The internal session execution path now holds a fail-fast local lease across
authorized replay, operation admission, preparation and commit. Concurrent callers
receive a conflict rather than queuing borrowed payloads. Closing an old lease
twice cannot unlock a newer execution. Cancellation during lease acquisition
releases it, and the outer execution scope releases it on success or failure.

Before admission, the session captures an immutable, complete member-to-admission-mode
map. Subsequent unfinished retries must use the same typed/opaque selections;
failure never silently switches typed admission to opaque admission. A committed
replay requires neither this map nor payload/schema inputs. Admission without an
owner and terminal admission races trigger another authorized result observation;
only a committed result can succeed, otherwise the call reports conflict without
provider work or takeover.

This lease serializes the retained session in this process. SQL ownership fences
remain responsible for competing sessions and hosts; the older internal owner-based
execution entry is not covered by the session lease. Durable session recovery,
bounded session retention, qualified host placement, production mounting and
transport conformance remain outstanding. No public contract changes.

### Bounded publication session retention

New internal `DocumentPublicationSessions` owns retry sessions by authenticated
account, principal and operation ID, with exact canonical-command conflict checks.
It enforces both a session-count limit and an aggregate estimate of retained
canonical and intent serialization bytes. Those estimates do not claim to measure
parsed Java heap. Payloads, resolvers and caller buffers are not retained.

New operations first check for an authorized committed outcome without requiring
current placement or a free session slot. Preparation reserves capacity under a
short registry lock, then constructs the session outside it. A concurrent request
for an entry still being prepared receives conflict. Failed preparation releases
its reservation because no operation-admission SQL has run. Existing sessions
keep their original placement and admission choices. Execution and all SQL/provider
I/O run outside the registry lock.

Exceptions retain the session and its capacity, including uncertain outcomes.
Only an execution returning an authorized committed result marks the entry terminal, and eviction
waits for every active invocation reference to leave. A full registry refuses new
work without evicting uncertain identities; it still permits committed replay.
This is an internal host component, not durable restart recovery. Explicit recovery
and retirement of abandoned pending sessions remain necessary before production
mounting; there is no expiry-based eviction or implicit takeover.

### Command-bound ownership recovery

The internal operation ledger now offers a typed publication takeover entry. It
checks command account/operation scope before SQL, then locks the owner and verifies
the exact immutable canonical command before either a generation change or an
idempotent next-nonce return. Caller authentication is still a host obligation;
the ledger's key and command do not authenticate a principal. The older raw internal
takeover entry is not the entry for document host recovery.

PostgreSQL tests cover a changed command after expiry, a changed command on the
same-nonce retry path, live-owner refusal, wrong scope, stale-owner fencing, and an
actual takeover commit whose acknowledgment is lost. Exact retry returns the stored
generation, token and lease without extending it. Real-provider publication tests
also refuse takeover of a committed operation. This does not yet expose recovery
through the host or retire an abandoned registry session.

### Retained recovery-session identities

An explicit internal `DocumentPublicationSession.recovering` factory now fixes
the expected predecessor generation, next owner nonce, fresh upload attempts,
qualified placement snapshot and complete copied admission-mode map before any
takeover SQL. Ordinary sessions still use initial admission and never implicitly
take over. Every recovery admission retries the same command-bound generation
transition; neither cancellation nor a lost acknowledgment changes its identities.
Unsupported predecessor generations and incomplete mode maps fail before SQL.

PostgreSQL tests inject acknowledgment loss and cancellation after actual takeover
commit. Reusing the same session recovers generation two with unchanged owner and
prepared-plan identity; caller map mutation cannot downgrade admission. The mixed
typed revision test also publishes through a recovered session against the real
object-store adapter, verifies fresh attempt IDs and disjoint upload keys, and
checks old-owner fencing after takeover. The committed result records generation
two and retains the existing authorized replay behavior.

This qualifies the retained recovery-session component. The registry transition
below supplies local exclusivity; host-controlled restart selection remains
unfinished. No recovery transport or automatic abandoned-session eviction is enabled.

### Exclusive registry recovery transition

The internal registry now offers an explicit recovery operation. It authenticates
scope and observes an authorized committed result before requiring a free slot or
placement. Pending recovery reserves an exclusive entry only when no invocation
holds it. Normal execution and another recovery cannot borrow that entry until
the transition finishes. Preparation and SQL remain outside the registry monitor.

An existing session's fixed admission modes must match. Pure preparation failure
keeps the previous session, or returns the reservation when there was none. The
registry installs the replacement's private identities before takeover SQL and
retains them after exceptions. Exact recovery retries accept only the original
predecessor generation and reuse the installed session, including its placement
and attempt identities. A successful takeover returns no publication result;
publication still runs through the ordinary guarded execution path. Committed
replay marks a retained entry terminal but waits for active references before
evicting it.

SQL fault tests hold an actual committed takeover before returning its response,
exercise competing registry calls, then lose the acknowledgment or cancel and
recover the same token, generation and lease. Tests also cover failed preparation, retained
capacity, mode downgrade rejection and generation-two typed publication through
the registry with real provider writes and persisted attempt/key assertions. No successful byte-provider substitute is
used in the ownership-only tests: those ports reject any attempted provider I/O.

This transition does not choose a predecessor generation for the host, reconstruct
a crashed host's session, or create an abort receipt. Those remain explicit recovery work before
production mounting; uncertain entries are never discarded by timeout or capacity.

### Durable publication-command reconstruction

New internal `DocumentOperationCommands` loads only the authenticated principal's
operation in an authorized account. It reconstructs the operation UUID from that
scoped key because the canonical semantic bytes intentionally omit it. Supported
rows pass the existing ProtoMolt command validator, exact canonical-byte equality,
stored SHA-256 and account checks. Unsupported codecs/versions report UNSUPPORTED;
malformed, noncanonical or mismatched supported data reports DATA_LOSS. SQL errors
and request cancellation are not rewritten as data corruption or absence.

PostgreSQL cases cover member ordering, unknown fields, an embedded operation ID,
invalid intents, malformed bytes, wrong accounts, unsupported encoding headers,
principal isolation and post-read cancellation. A separately labeled isolated
schema fault bypasses SQL immutability and digest constraints to verify the reader's
digest check. A fresh JVM reconstructs the command using only database connection,
caller scope and operation UUID. It receives no original request object or bytes.

The real-provider registry recovery test now executes a command reconstructed by
a new reader, then publishes its generation-two revision. Its payloads, selected
placements and schema resolver are still supplied by the fixture. This proves
durable command reconstruction and its use in recovery, not a complete host restart
or staged-payload recovery. The loader is internal: returning a command grants no
owner token, document access or permission to publish, and is not a public endpoint
for reading historical request metadata.

### Explicit advancement of an expired recovered owner

A retained recovery session can now advance one further generation after an
explicit host request. The database must first confirm its exact command,
generation and private nonce under the owner lock, with expiry evaluated using
database time after that lock is acquired. A live owner, a skipped generation,
or a nonce that never became owner cannot advance. This observation grants no
ownership; preparation runs after its transaction ends, and the replacement must
still win typed takeover CAS.

Registry exclusivity covers observation, preparation, installation and takeover.
Observation or preparation failure preserves the previous session. Once installed,
the replacement survives SQL failure or acknowledgment loss with its nonce and
attempt identities unchanged. Terminal races use authorized result replay. Count
and command-byte reservations remain attached to the same registry entry.

PostgreSQL tests prove live-owner refusal without changing its token or lease,
generation-three acknowledgment reconciliation, skipped-generation rejection,
foreign-owner refusal and preservation after failed preparation. Another real
ledger takes ownership after the expiry-observation transaction commits but before
the local takeover; the local CAS and its retry both refuse ownership. This adds
explicit advancement, not automatic retirement, abort receipts or crash recovery.
When another host wins, the losing entry retains its ungranted nonce and cannot
adopt that host's owner or advance on its behalf.

### Superseded local session reconciliation

The new internal `retireSuperseded` operation releases an idle session only after
the exact command and durable ownership prove its nonce can no longer own its
target generation. A higher durable generation or equal generation with another
nonce suffices. Missing or lower generations, and expiry of the same nonce, do
not. The registry excludes execution, recovery and another reconciliation for
that entry while checking SQL; unrelated entries remain usable.

PostgreSQL fixtures cover missing, earlier, equal and later generations, command
conflicts, principal isolation, concurrent exclusion, SQL acknowledgment loss,
and cancellation after the observation. Failed observations retain count and
command bytes; positive proof removes only that entry. Retrying normal execution
after retirement still cannot take ownership. This is local capacity management,
not an abort receipt, provider cleanup, automatic takeover or public endpoint.

### Rejection receipt contract and encoding

New: `DocumentPublicationRejection` and `DocumentPublicationRejectionCodec`.
Extended: `DocumentPublicationCommand` can verify a rejection against its exact
canonical identity and trusted principal/generation. Unchanged: the success result,
all existing field tags, protobuf import paths and Any URLs. No service method or
terminal rejection storage is added at this checkpoint.

Generated and dynamic-message fixtures exercise the real runtime validator for
required identity, bounds, defined enums and the bidirectional cancellation rule.
Codec fixtures cover identity mismatches, unknown fields, NUL strings, encoding
version, digest corruption, truncation, duplicate scalar fields and the wire-value
limit. JSON Schema exposes account length bounds and retains the disposition rule
as `x-protomolt-cel`; that rule and trusted command/owner binding remain runtime
obligations. This does not add an OpenAPI endpoint or change the generator.

The full imports compile; Buf lint passes for the new contract and the complete
descriptor inventory passes FILE compatibility against `c05ef2d5`. All 90
repo-proto/repo-spi tests and the SPI/engine runtime dependency checks pass.
The fenced storage, decision,
replay authorization and race-test backlog is recorded in the design's rejection
receipt section. A valid receipt shape alone must never be advertised as a durable
terminal outcome.

### Durable explicit cancellation and terminal replay

New: internal `DocumentPublicationRejections.cancel` and immutable V64 rejection
storage. Extended: operation write/owner guards, admission/renewal/takeover terminal
checks, current-policy replay and local session retirement. Unchanged: protobuf
contracts and successful publication encoding. No public cancellation RPC is added.

Cancellation opens a fresh owner-fenced transaction and returns any already
committed result instead of overwriting it. A stale owner cannot decide for its
replacement. SQL clock time supplies the immutable receipt; transient failures
and transport cancellation are not classified as rejection. The registry and
success-only executor propagate a typed authorized terminal receipt before byte or
schema-provider work, without treating it as a successful document publication.

Real PostgreSQL tests cover concurrent success/cancellation in both lock orders,
response loss, late cancellation, idempotency, same-transaction write refusal,
stale generation, immutable rows, retained staging records, authorized terminal
eviction, source revocation while waiting, corrupt receipt headers and migration
over successful native publication. Replay uses bounded lean metadata observations
for the full 10,064-node union; existing 10,000-source admission limits remain.
Rejected creation without a target requires process authority. Missing or revoked
existing targets do not become readable through rejection replay.

Schema-admission rejection, public mounting, complete cleanup
and restart recovery remain open work. No general exception-to-receipt conversion
or unverified abort result is introduced.

### Rechecked revision-precondition rejection

New: internal `rejectRevisionPreconditions` decision. Extended: native execution
can reconcile a direct revision conflict after rollback and resource unwinding.
Unchanged: protobuf contracts, V64 storage format, codec and public RPC inventory.

The decision checks destination revision/if-absent conditions and explicit/reused
source revisions under the complete current-authorized shared lock set. A confirmed
mismatch records PRECONDITION_NOT_MET; a match returns PENDING with no receipt and
preserves the original conflict in execution. Missing or denied existing objects
do not become rejection evidence. An existing success or terminal decision wins
exact replay; a newer owner fences the previous worker.

SQL tests cover each condition kind, full-set authorization, owner replacement,
success winning and both row-lock orders. Execution uses real PostgreSQL with
controlled faults and provider ports that fail if reached: a real stale candidate
is rejected before I/O, a direct conflict signal with matching conditions is not
enough, cancellation leaves no decision, and Hibernate-wrapped commit failures
remain failures rather than being classified from their nested causes.

The receipt binds the canonical conditions and deciding generation, but carries
no observed revision or per-member rejection detail. Schema-admission rejection,
public host mounting, full cleanup and restart recovery remain separate work.

### Explicit validation time and admission-rejection plan

Extended: `ProtoValidator.validate(Message, Instant)` supplies one evaluation
instant for field/message CEL, timestamp rules, nested values and collections.
Unchanged: the existing overload remains available and samples once per call;
rule caches and rule-source composition remain shared. New tests cover historical
instants, strict timestamp boundaries and concurrent use of one validator.

Still planned: an explicit verified-invalid candidate assessment, retained
candidate/policy/schema/evaluation evidence and an additive rejection-receipt
binding. Current preparation exceptions do not prove complete schema resolution
or command-byte verification. The design section "Schema-admission rejection
evidence" specifies the required ordering, failure categories and acceptance
cases. No admission-rejection RPC or automatic terminal classification is exposed
by the validator overload.

### Complete payload traversal with a bounded verdict

New: internal `DocumentPayloadAssessment` and contextual assessment entry point.
Extended: the validator can evaluate all rules while retaining only the first
violation. Existing checked-payload return types and public protobuf contracts
are unchanged. Payload-level assessment preserves exact contextual schema paths
and one evaluation instant; an earlier invalid value cannot hide a later missing
schema, unsupported rule or operational failure.

This is a library building block. Command-wide fragment verification, continued
assessment across all roots/members, frozen evidence replay and the durable
schema-admission rejection decision remain required. No public outcome endpoint
or automatic admission-rejection receipt is added by this change.

### Owned member schema assessment

New: `DocumentSchemaAssessment.prepare`, an owned library assessment spanning all
CORE/PARSED roots of one member. Refactored unchanged behavior: descriptor,
metadata and source normalization now lives in the shared
`DocumentSchemaDefinitions` helper. Extended: structural member validation can
use an explicit evaluation instant, and occurrence projection accepts completed
invalid-value traversals without constructing a successful payload proof.

The assessment verifies the whole member's fragment hashes before resolution,
continues after payload violations, shares budgets across roots and releases
owned schema/evidence reservations on failure or close. It does not authorize
policy, independently replay a proof, assess other operation members or emit a
terminal receipt. Those integrations remain required before public adoption.

### Operation-wide candidate assessment

New: internal `DocumentPublicationAssessment`, retaining a complete mode map,
policy selection, evaluation instant, all member assessments and normalized assets.
Extended: `DocumentAdmissionPolicy.assess` enforces member account and resolved
schema eligibility; fragment snapshot capture verifies the hashes of all private
copies before schema resolution. Refactored: successful proof batches and invalid
assessments share the same operation-wide root/evidence/artifact limit accumulator.

No publication or terminal receipt is emitted. A tentative value failure becomes
available only after every typed and opaque member completes its applicable
checks. Independent frozen-evidence verification and durable decision integration
remain open; current-policy and authorization fences are not replaced by an
in-memory selection. Existing protobuf names, fields and public RPCs are unchanged.

### Native runtime retained-assessment configuration

- **New:** `DocumentPublicationRuntime.Assessments` carries a trusted local runtime
  bundle path, evidence retention and minimum decision window. Both durations are
  positive exact microseconds within one day, with retention greater than the
  minimum window. The host must keep the observed runtime and bundle immutable.
- **Extended:** an explicit `DocumentPublicationRuntime` constructor overload
  requires a reader implementing source reads, assessment reads and lifecycle
  ownership. It observes the actual runtime before composing execution. Failure
  aborts construction and leaves the borrowed reader under caller ownership.
- **Extended:** configured `execute` promotes accepted assessments or retains,
  re-reads and verifies invalid evidence before a fenced durable rejection.
  `Rejected.receipt()` carries the existing durable receipt. After stage creation
  starts, session retries discover the original stage; no upload or schema
  resolution runs again. Absent uncertain stages require explicit recovery.
- **Unchanged:** the constructor without assessment configuration, existing public
  result/receipt encoding, Java takeover operation and all protobuf names, tags,
  imports and Any URLs. This adds no transport method or caller-supplied runtime
  attestation. It does not make private owner identity crash-persistent.

The production-JAR gate exercises the public configured runtime against PostgreSQL
and LocalStack. It covers missing-bundle startup refusal followed by use of the
same reader, accepted publication, retained rejection, lost stage/decision commit
acknowledgements, stage rollback, terminal session retirement, unresolved session
capacity and shutdown during admission. The fixture uses one session and one read
slot, then checks drained memory/read ownership. Sol reviewed this checkpoint.

### Schema-only and routing-metadata native revision coverage

**Unchanged contracts and implementation:** existing all-reuse publication can
replace a schema binding under an unchanged Any type URL and create a separate
routing-metadata revision without new payload writes. The added production-JAR
fixture checks A/B/B payload descriptor references, retained container references,
account-level artifact normalization, exact original physical IDs/part provenance,
zero upload attempts and immutable historical `cluster_id` snapshots. Historical
validated reads retain command/revision bindings and use stored schema assets.
A failing replacement contract produces rejection without advancing the current
revision. This adds acceptance evidence, not a restore API or hydration operation.

### Historical metadata projection and RustFS version prerequisite

**Extended:** `HistoricalDocumentRepository.RevisionRead` now requires explicit
historical metadata, shared by raw and validated reads. The ledger captures the
immutable commit snapshot with the manifest under current READ authorization.
`DocumentHistoryService.ReadRevision` adds response field 7 without changing
existing fields, names, imports or Any URLs. Older-peer absence is distinct from
legacy-unrecorded and never authorizes a current-metadata fallback.

**New internal codec:** `DocumentHistoricalMetadata` projects the existing v1 SQL
snapshot into `DocumentRevisionMetadata`. It preserves nullable text and integral
microseconds, rejects corrupt fields/account mismatch as data loss, and reports
unsupported encoding versions explicitly. No new restore, JCR, migration or
legacy-read operation is enabled. Encoded metadata has a 1 MiB per-read-slot bound;
the wire representation participates in the existing complete-response budget.

**Unchanged operation, added provider qualification:** the pinned RustFS adapter
now has a real versioning case covering exact old-version bounded GET following
same-key replacement and explicit new-version deletion. Native typed publication
and retained-assessment replica performance remain pending; the next workload and
measurement requirements are recorded in the composition design.

### Native RustFS backend parity

No repository operation changes. The opt-in `admissionRustFsTest` executes the
production-JAR native assessment/publication/recovery fixture against pinned
RustFS. The existing `admissionStorageTest` keeps its LocalStack default. Backend
selection is explicit and rejects unknown values. This adds provider parity
coverage; it does not establish measured native scaling or a new public API.

### Independent native process workload

No API changes. `nativeReplicaTest` adds real shared PostgreSQL/RustFS coverage for
1/2/4 production-JAR writers, unique typed creates, retained invalid-create
rejections, same-runtime exact retry and fresh-process history/terminal observation.
It requires 14 successful publications and seven rejections; rejected creates
must have no current document or published revision. The fixture uses trusted
process authority and does not qualify concurrent mixed traffic, scoped API-key
bindings, production transport or measured scaling.

### Cross-process revision precondition race

Unchanged native update/rejection operations receive a two-JVM shared-revision
acceptance case. Both writers reach schema resolution using the same expected
mutation revision. Exactly one publishes; the losing operation retains an explicit
precondition rejection and creates no revision. Same-runtime retries and fresh-JVM
terminal observations must match, and current-head/history checks retain exactly
the original revision plus the winner. This extends the native process fixture;
it is not a throughput or transport qualification.

### Native mixed traffic diagnostic

Unchanged repository contracts. `nativeReplicaBenchmark` exercises real historical
reads, typed publications and retained rejections concurrently across one/two/four
JVMs, with exact retry and per-window durable-result counts. It captures separate
operation latency and SQL/provider/lock/RSS diagnostics with explicit pool sizes.
The local low-load run passed but showed no speedup from replicas; it does not
qualify saturation, public transport or large payload performance. Evidence is in
`docs/evidence/repository/2026-10-05-native-mixed/README.md`.

### Optional decoding failure classification

**Extended internal Java behavior:** `MessageWireBudget` reports configured depth
and value-count limits with a dedicated exception. Malformed wire data remains a
protobuf parsing failure; cancellation propagates independently. This supports
explicit optional-materialization outcomes without matching exception text. No
protobuf field, operation or validation bypass is added. The occurrence-specific
decoder and authorized historical adapter are the next implementation boundary,
recorded in the composition design; they are not yet exposed as read operations.

### Internal optional Any decoder

**New internal helper, unchanged public operations:** `DocumentAnyMaterialization`
provides preserve and optional single-occurrence decoding over supplied immutable
bindings. It verifies selected URL/value identity, keeps nested Any bytes opaque,
and distinguishes resolution, parse and resource failures without an admission
verdict. The host still authenticates document paths, resolves retained references,
owns memory/lifetime and enforces access. No RPC, registry adapter, cache or
historical optional-mode operation is enabled. Sixteen decoder cases and the full
185-test admission suite passed, with the existing runtime dependency gate.

### Retained typed-root decoding adapter

**New internal adapter, unchanged public operations:** root selection now connects
strict CORE/PARSED fragment inventory, recorded root identity, exact retained
schema association and optional decoding. Preserve reads no payload schema assets;
required historical asset loss and typed value inconsistency fail explicitly.
The host still verifies stored evidence, authorization, physical revision ordinal
and ownership lifetime. Eight file-backed tests and all 193 admission tests pass;
SQL-host/public transport integration and nested traversal remain pending.

### Retained nested-path decoding

**New internal helper; unchanged repository RPCs:** a recorded path can now traverse
retained parent definitions to a nested Any using descriptor-checked field/index/
map-key steps. Exact references, occurrence identities and bounds are enforced;
required missing assets fail and no partial target escapes. The root and nested
readers share their retained-boundary decoder. Admission's map-entry shape check
was moved without changing its rules so both paths reuse it. Twenty-one new path
cases and all 214 admission tests pass. Host authorization, evidence membership,
physical ordinal binding and result ownership remain prerequisites, with SQL/public
integration still pending.

### Shared historical binding reconstruction

**Extracted internal implementation; unchanged public operations:**
`DocumentHistoricalSchemaBinding` reconstructs the retained command, member,
policy and container association before strict replay. It does not certify
fragment identity, evidence membership or current delivery authorization.
`DocumentHistoricalSchemas` retains its complete validation and authorization
checks. Nine focused PostgreSQL replay/revocation cases pass; optional selected
materialization still needs its own owned result and host integration.
