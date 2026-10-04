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
