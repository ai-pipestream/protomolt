# Repository composition and typed archival admission

Status: proposed design, ready for implementation planning. This document does
not introduce an API or claim that the missing behavior is available.

Source baseline: `fd0cf28769494b281e5ca3963bff1844b22be193`, whose tree matches
the merged delegation extraction at `528117a2d48cda3b3abedadd75b5706d7ac68ca7`.
Recheck main and affected contracts before implementation.

## Definition of completion

Correctness includes speed and latency, resource bounds, integrity, authorization
and recovery. Passing functional tests is insufficient if a small update causes
unnecessary full-document copying or repeated per-part coordination. Evaluate
these properties together before treating a component as complete.

Acceptance evidence must cover representative latency distributions and throughput,
provider call/byte counts, database transactions and waits, bounded memory and
queues, fairness under load, and behavior during contention and failure. Record
workload, provider, machine load and verification policy with results. Separate
measured outcomes from hypotheses. Preserve mandatory validation and durability;
reduce unnecessary work instead of removing guarantees.

The operation-count targets below are part of the design contract. Production
latency targets remain to be established on representative storage. The current
copy-based partial-save path is an experimental baseline and does not meet the
intended efficiency requirements.

## Purpose

Architectural requirement: support a future optional ProtoMolt-owned JCR 2.0
content repository with standard Java JCR and protobuf/gRPC access to the same
behavior. Follow the [compatibility and gap assessment](repository-jcr-compatibility.md)
before extending contracts or choosing document-specific transaction boundaries.
Keep JCR dependencies out of base storage modules. Existing accounts, workspaces,
IDs and version numbers are not assumed to have JCR semantics. Declare
capabilities and establish conformance before any
compliance claim. The potential gRPC transport must remain usable with other JCR
implementations. Continue storage correctness and recovery independently.

Pre-release design policy: there are no external users. Breaking Java and
protobuf changes are permitted when they simplify the design; do not retain
obsolete interfaces, compatibility shims or fallback behavior merely to preserve
an old API. Update affected callers and tests together. Stored objects, original
backend identities and recovery records still require deliberate handling;
permission to change contracts is not permission to silently lose data.
Keep provider I/O outside database transactions and provider dependencies out of
shared interfaces. Verify performance with measurements before claiming gains.

Make the repository usable as a Java library or a gRPC service with the same
document behavior. Applications select storage providers without inheriting all
provider dependencies. Typed protobuf admission adds schema resolution and
validation above byte storage. Other languages use the existing protobuf/gRPC
contracts; Java SPI is local composition, not a cross-language plugin protocol.

Keep the database where it provides transactions, metadata, version history and
ownership. Removing dependencies from small consumers does not require removing
the database from the repository implementation.

Storage selection is identity-based. Repository objects retain a backend
configuration generation and storage realm; the host resolves that identity to
the selected provider. S3 bucket, key, region and endpoint vocabulary belongs in
S3 configuration. Other providers must retain their own coordinates without
requiring S3 configuration or SDKs. Changing a current default affects new writes,
not existing bindings. Credential rotation may preserve an identity; a physical
namespace change requires a new generation and explicit migration. An unresolved
original backend is an error, never permission to try another backend.

Managed profiles now carry a versioned provider-owned nonsecret identity.
V17 preserves legacy S3 profiles without rewriting them; new profiles use the
generic representation. SQL object coordinates still use bucket/key terminology,
and non-S3 managed lifecycle qualification remains required. Generic profile
persistence alone does not establish provider durability or recovery support.

Embedding intent is defined by protobuf field annotations, including
[`index.chunking_policy.embedding`](../../search/index/spi/src/main/proto/ai/protomolt/proto/index/hints/v1/indexing_hints.proto).
Reuse those definitions; mappings/projections select or reshape annotated data
without requiring a second embedding declaration. Retained schema descriptors
must preserve these options. Future repository indexing must bind the admitted
content revision, annotation policy, projection and resolved model identity,
enforce current access policy, and recover indexing updates/deletions/restores.
That end-to-end integration remains unproven; keep search providers optional.

This extends [the archive design](archive.md) and follows
[the SPI composition rules](../architecture/spi-modularity.md). It does not merge
pipeline document parts into archive renditions or redesign every service.

## What exists, and what needs work

The following are source observations, not a new conformance or deployment claim.
These observations describe the baseline above; source links follow files as they
move. Implemented module extraction and lifecycle changes are recorded in
[the SPI composition notes](../architecture/spi-modularity.md). The remaining
contract and behavior requirements below still apply.

- [BlobStore](../../repo/blob/spi/src/main/java/ai/protomolt/proto/repo/blob/spi/BlobStore.java)
  already supplies an object-storage interface with explicit unsupported
  conditional operations. S3, Redis and cache adapters exist. Its
  [module](../../repo/container/build.gradle) also brings S3, Redis, SQL and Kafka
  dependencies; the interface is not yet a lightweight published dependency.
- [RemoteBlobStore](../../repo/blob/grpc/src/main/java/ai/protomolt/proto/repo/blob/grpc/RemoteBlobStore.java)
  implements that interface through DocumentService. It is packaged with the
  service, uses one configured drive, ignores the bucket argument, buffers unary
  content, and does not implement all BlobStore operations. A remote blob client
  is not a remote implementation of the complete repository API.
- [RepoServices](../../repo/service/src/main/java/ai/protomolt/proto/repo/service/RepoServices.java)
  owns composition and constructs an S3 client even when selecting another blob
  provider. Provider-specific drive provisioning also needs separation.
- [Document](../../repo/proto/src/main/proto/ai/protomolt/proto/repo/v1/document.proto)
  has `Any structured_data`, parser results with `Any shape`, and ownership.
  [DocumentPartCodec](../../repo/codec/src/main/java/ai/protomolt/proto/repo/codec/DocumentPartCodec.java)
  already assembles `<T extends Message>` through a supplied prototype. Its part
  operations use protobuf descriptors. This is typed parsing, not gRPC casting,
  and it is not an entirely reflection-free implementation.
- [Archive contracts](../../repo/proto/src/main/proto/ai/protomolt/proto/repo/archive/v1/entry.proto)
  already define versions, rendition manifests, hashes and provenance.
  `schema_subject` explicitly records a subject without enforcing it. A subject
  name alone does not pin an immutable schema or its imports.
- [ArchiveOperations](../../repo/engine/src/main/java/ai/protomolt/proto/repo/engine/ArchiveOperations.java)
  implements archive operations. Current entry metadata and immutable version
  manifests are distinct; historical versions do not snapshot every EntryInfo
  field. Deterministic entry identity alone does not guarantee idempotency for
  every mutation or retry.
- [Ownership and security](../../repo/proto/src/main/proto/ai/protomolt/proto/repo/v1/security.proto)
  are represented and persisted. The inspected repository handlers do not show
  evaluation of those document ACLs against the authenticated caller. Transport
  authentication is not proof of document authorization. This finding is scoped
  to repository paths, not a claim about every platform surface.
- [SchemaRegistryStore](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/SchemaRegistryStore.java)
  and [DescriptorSetArtifacts](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/DescriptorSetArtifacts.java)
  provide foundations to reuse. Do not introduce a competing schema registry.

Existing archive documentation describes some future behavior alongside current
behavior. In particular, archive mutation events and asynchronous archive purge
must not be inferred from the document store's outbox and purge implementation.

## Composition boundaries

Pre-release design policy: there are no external users requiring Java API or wire
compatibility shims. Prefer clean, usable boundaries and efficient execution over
preserving obsolete interfaces. Update all in-tree callers and fixtures together.
Stored-data integrity and explicit migration/recovery behavior remain requirements;
permission to break an API does not authorize silent redirection or data loss.

Use these dependency boundaries; module names below are proposed paths, not
published artifacts. Final moves must follow ADR-002 without split packages.

1. **Byte storage:** `repo/blob/spi` owns object coordinates, streams, integrity,
   conditional operations, capabilities and failures. It has no S3, Redis, SQL,
   Kafka, gRPC server or schema-registry dependency.
2. **Providers:** `repo/blob/s3`, `repo/blob/redis`, `repo/blob/cache` and
   `repo/blob/grpc` implement or decorate the byte port. Only the relevant
   provider carries its SDK. Azure is a future implementation, not a condition
   for finishing this extraction.
3. **Document mechanics:** `repo/codec` owns descriptor-driven parts and assembly;
   it depends on protobuf and required contracts, not provider SDKs or SQL.
4. **Repository behavior:** `repo/spi` defines document/archive operations using
   existing request/response contracts and explicit caller context. A repository
   implementation owns versions, manifests, metadata, authorization and admission.
   `repo/ledger` contains SQL persistence; `repo/engine` composes it with the byte
   port. Database dependencies here are intentional.
5. **Typed admission:** `repo/typed` supplies generic message bindings and uses
   the existing registry and runtime validator through explicit dependencies.
   It does not become a prerequisite for storing opaque originals.
6. **Transports:** `repo/grpc` implements the repository interface using generated
   clients. Existing `repo/service` handlers delegate to the shared engine; HTTP
   uploads also use that boundary. Neither transport duplicates repository policy.
7. **Optional integrations and assembly:** Kafka integration lives outside the
   engine's mandatory dependencies. The application selects a ledger, storage,
   admission policy, authorization policy and transports, and owns their lifecycle.

Two supported arrangements result: an application can call the repository engine
in process with S3 plus SQL, or call the same operations through a gRPC client.
The service hosting that gRPC API can use S3, Redis, or another conforming provider.
One transport wrapper serves all backends; separate S3-gRPC and Azure-gRPC copies
of repository behavior are unnecessary.

Keep the existing byte interface where its semantics fit. Add small optional
capabilities where necessary rather than forcing every backend to impersonate
S3. Distinguish conditional replacement, streaming reads/writes, listing,
server-side copy and administrative provisioning. A missing required capability
fails configuration or returns an explicit unsupported operation. Never emulate
atomic compare-and-set through a read followed by an unconditional write.

Resolve logical drive coordinates explicitly. The extracted remote adapter must
reject a mismatched coordinate or use a documented fixed-drive binding; silently
ignoring a caller's bucket is not the new contract. Detect direct self-routing at
configuration time and bound remote calls with deadlines to limit routing loops.

## SPI discovery and lifecycle

Load factories through ServiceLoader, following the existing registry-provider
pattern. Discovery lists trusted installed providers; it opens no connection and
starts no workers. The application explicitly selects provider IDs and options.
Duplicate IDs, absent selected providers, unknown options and construction failures
are errors. Never fall back to memory, another provider or weaker validation.

Selected providers declare capabilities and ownership of closeable resources.
Register cleanup immediately, close in reverse acquisition order, and retain
cleanup failures without losing the original failure. Borrowed channels/clients
remain caller-owned. Unselected providers need no credentials or live services.
Cache decorators must preserve authoritative reads and conditional-write semantics.

## Typed protobuf binding

A proposed `MessageBinding<T extends Message>` associates:

- the protobuf full message name and accepted Any type URL policy;
- a generated `Parser<T>` or default instance supplied by the application;
- an immutable registry schema reference and descriptor dependency closure;
- the validation policy/version required for admission.

These are design requirements, not a committed Java signature. Reuse existing
schema-reference messages where they represent the required identity; add fields
only after inventorying them. Java generic erasure cannot discover T. A parser
does not establish that the bytes satisfy the registered contract: confirm the
binding's descriptor identity against the pinned registry descriptor too.

Do not load Java classes named by untrusted type URLs. Generated bindings support
typed Java callers without server reflection. A descriptor-based dynamic path
supports registered messages with no generated Java class, using the same
admission policy. Preserve unknown fields and original bytes; parsing and
reserializing is not a byte-preservation guarantee.

The persisted schema identity includes registry scope, subject/version or other
immutable registry ID, full message name, and a verified digest of the complete
descriptor set including imports and validation options. Define canonicalization
and digest version before committing a wire field; test it with golden fixtures.
Retain the exact descriptor artifact as long as retained content needs it.
Resolve mutable aliases once at admission, never on historical reads.

## Admission and validation boundary

Expose two explicit policies: opaque archival intake and validated typed intake.
An archive may require typed intake for named renditions. Clients cannot bypass
that requirement by omitting schema fields, changing transport, or using raw
upload. Opaque originals remain legal where policy permits them, but must never
be represented as validated typed content.

For typed intake, the shared engine performs:

1. Authenticate the remote caller and authorize the target account/document.
   Embedded calls receive explicit trusted-process authority or caller context;
   absence of context must not accidentally grant authority.
2. Validate request structure and limits with ProtoMolt's runtime validator.
3. Resolve and verify the immutable schema binding, decode with bounded resources,
   match the message/type URL, and run annotation and cross-field validation.
4. Check handler obligations: current authorization, expected version, policy,
   schema eligibility, idempotency and retention state. Recheck mutable conditions
   at the commit boundary using version/policy guards.
5. Commit an admitted version and its validation record. Only then make it
   available to downstream typed processing or semantic review.
6. Validate successful responses before emitting them. An invalid response after
   commit is an internal failure, not an implied rollback; retry lookup must
   recover the committed operation.

Bound input size, nesting, descriptor complexity and validation execution. An
unsupported required rule is a refusal, not a warning or successful validation.
Separate contract validity from semantic correctness: a contract-valid court-case
extraction can still identify the wrong person. Semantic judgement remains a
separate operation and cannot silently rewrite an admitted version.

Large uploads may stage bytes before full validation. Staged bytes are not a
visible committed version, cannot be consumed by review/indexing, and have bounded
cleanup. An expired or failed staging upload must not become a valid rendition.
Do not pretend arbitrary protobuf validation can be performed incrementally on an
unbounded stream; typed admission has an explicit supported size limit.

## Versioned metadata, provenance and access

Retain existing ownership vocabulary: account, datasource, connector, source
owner and typed security. Do not introduce a competing tenant or principal model.

New admitted versions record a snapshot of descriptive/source metadata, source
ownership, schema identity and admission policy alongside content identity.
Current display labels may remain mutable, clearly identified as current state.
Current access policy governs reads of every historical version; restoring old
content must not restore permissions that were revoked later. Historical ACL
snapshots provide provenance, not authorization. Existing versions lacking a
snapshot remain explicitly unknown; do not manufacture historical values from
today's EntryInfo.

Preserve original and derived renditions separately. Derivations identify input
entry/version/rendition hashes, output hash, transform/workflow version, and
available run/attempt and actor identity. Reuse existing receipt/provenance models
instead of adding another general execution ledger. An admission record binds
content hash, schema digest, validation policy and version; any later semantic
evaluation additionally binds its candidate, attempt/revision and evidence.
Unknown attribution stays unknown. Hashes establish integrity, not authorship.

Enforce account isolation and document permissions in the shared engine for read,
list, write, copy, restore, prune and delete. List responses must not leak denied
metadata. Test deny precedence, inheritance and policy changes. Raw storage access
is an administrative capability and must not become a public authorization bypass.

### Trusted caller bindings and ACL decisions

Repository caller bindings carry exact account IDs and typed identities using the
existing `Principal` message. The host must resolve these from trusted policy or
credentials. Neither a principal name, an operation scope, a request's account ID,
nor `source_owner` establishes account membership. The existing two-argument
`RepositoryCaller` constructor supplies no account or ACL bindings. An operator
retains explicit process authority, distinct from ordinary account membership.

ACL matching requires both identity type and value, compared without case; account
IDs remain case-sensitive. Principal and group grants use the same typed identity
matching. A public rule requires both type and identity to be `public`, and cannot
grant access across accounts. READ and WRITE remain separate grants. Any matching
DENY overrides all matching grants, including grants inherited from a parent.

Inheritance must distinguish an unresolved parent from a resolved empty rule set.
When inheritance is enabled, an unresolved parent fails closed. Missing security
denies scoped callers; an empty policy grants nothing. Blank identities, missing
identity types, unknown/unspecified access enums and malformed public rules fail
closed. Operator bypass does not make malformed ownership or ACL data valid.

Persisted security JSON is parsed strictly. Before enabling scoped repository
access, operators must inspect missing ownership, unknown JSON fields and malformed
rules, recover their intended policy from an authoritative source, and explicitly
repair affected records. Do not backfill permissive ACLs or infer historical
ownership from a current login. Unknown security fields now raise a ledger error
instead of being discarded; retained bytes remain unchanged.

Document node, reference and manifest reads now evaluate the loaded row's current
policy before fetching content or exposing object coordinates. The row lookup is
the policy snapshot for that read: a later revocation affects the next read, and
does not promise cancellation of an already authorized read. Serialized ownership
inside the stored body is provenance and never supplies a read grant. Denied and
absent scoped point reads return the same NOT_FOUND status and description.

An account-bound caller needs no additional typed identity to match a public ACL;
public still does not cross accounts. Scoped inherited-policy reads remain
unavailable until parent policy resolution is implemented. Operators bypass
inherited grants while local policy and ownership still must be well formed.

Scoped document listings restrict SQL to the caller's bound accounts and evaluate
current ACLs before visible counts, offsets and continuation tokens. Omitting an
account filter lists across those bindings; naming an unbound account returns an
empty list. Each request scans matching rows in one database transaction, using a
fetch batch of 256 and detaching examined rows. At most 1024 account bindings are
accepted per listing. Exact visible totals require scanning every matching row;
the first page is not a constant-cost operation. Malformed or unresolved policy
fails the whole request, including when encountered beyond the returned page.
Offsets count visible rows, and policy changes between requests can shift pages;
tokens do not pin an old authorization snapshot. Count overflow fails explicitly.

Document writes now land under a unique attempt prefix beneath the stable node
root. Full saves and partial-copy destinations therefore do not overwrite objects
referenced by the previous manifest before the new row commits. Stored manifests
continue naming exact object keys, so existing layouts remain readable and the
reported storage prefix remains the stable node root. A carried CORE is read back
to verify its checksum and capture destination ETag/version metadata; this adds
one object read when a partial save copies CORE.

The final full/partial save now compares the destination's database-managed
mutation revision under a row lock before publishing. V8 backfills existing rows
and uses a sequence-backed trigger to advance the revision on every INSERT or
UPDATE, including direct SQL policy edits. Delete/reinsert receives a new revision.
Two guarded first writes also serialize on a transaction advisory lock because
there is no row to lock yet. Stale candidates return ABORTED; the API does not
silently retry with a newly privileged policy snapshot. Row and outbox changes
still share one transaction. Coherence-probe repairs also compare the sampled
revision, so maintenance cannot overwrite a concurrent policy edit. A conflict
leaves the current row unchanged and is reported as a skipped repair for a
subsequent probe; missing-object counts describe observations, not committed repairs.

Partial saves also hold the copy source's sampled revision through publication.
Source and destination locks use a consistent order, and a missing, replaced or
changed source aborts publication even if its bytes were already copied. Same-row
copies require both snapshots to agree. Failed attempts leave the committed
destination unchanged; their staged objects remain cleanup work. This checks source
stability in addition to the scoped source READ check below.

This destination revision check is a prerequisite for complete authorization.
Scoped saves now require an existing AVAILABLE destination with a current WRITE
grant before drive lookup, storage calls or dedupe bookkeeping. Dedupe rechecks
the preflight revision under its row lock. Partial copies additionally require
READ on the source, including self-copies: WRITE does not imply READ. Missing and
denied scoped coordinates use the same unavailable result. Scoped copies currently
stay within one owning account; membership in two accounts is not a cross-account
copy grant.

Scoped writes preserve the destination's current ledger ACL verbatim. Full saves
and partial saves that write CORE must supply that same ACL; changing it requires
process authority. A carried CORE may contain the source's ownership snapshot,
which remains provenance and cannot change current destination access. A scoped
caller cannot create a document by supplying its own WRITE rule, or revive a
tombstoned document through SaveDocument. Scoped creation, policy administration
and restoration need separately resolved trusted capabilities; they remain
unavailable in this checkpoint.

Scoped saves cannot change the drive or source-blob deletion policy, including
its reason stamp. They also preserve the ledger datasource ID, which determines
logical-document deletion groups. A written CORE must retain that datasource ID;
a carried CORE may still describe its source. These controls require process
authority and are checked before storage access, including on dedupe requests.
Because the existing deletion fields do not express patch presence, scoped clients
must echo their current values; omission does not mean "leave unchanged".
The existing `written_by` stamp remains a caller-supplied provenance assertion,
not verified actor identity or an authorization grant. Admission/evaluation actor
binding still belongs to the versioned-provenance work below.

Raw ledger maintenance writes remain administrative and do
not themselves implement the guarded repository API. Backups must preserve the
revision sequence state along with rows; restoring that state is still part of the
restore acceptance work. Failed or
superseded attempts leave unreferenced objects for lifecycle reconciliation; the
new paths do not promise permanent historical retention.

The complete ownership work remains unfinished: resolve inherited policy and
atomically guard policy revision with every mutation. Deletion, archive and drive
mutations still require process authority. Named
gRPC callers currently carry only name/scopes and remain denied unless the host
installs a trusted repository binding resolver. The document gRPC adapter accepts
such a resolver and rejects null, changed principal or changed process authority.
It is not a request-header override. Open listeners retain process authority and
require the trusted-network deployment boundary.

### Deletion admission and recovery

Document deletion now admits the sampled rows in one SQL transaction. Each row
must still match its sampled mutation revision before it is tombstoned and its
purge record is inserted. A logical selector covers those sampled identities;
rows created afterwards are not silently added to the command. Admission failure
rolls back every selected row and queue insertion before any storage deletion.

Synchronous deletion retains its existing manifest-part scope and waits on the
exact admitted purge IDs. Storage failures propagate and leave durable recovery
work; pending and failed documents are unavailable to normal reads. Recovery uses
the original completion mode. `DocumentDeleted` is emitted with actual synchronous
row removal, not repeated when another command already removed the row. Async
commands retain `PurgeRequested` and `DocumentPurged`. Request threads use a
separate JDBC queue handle, never the fleet's single-threaded Kafka consumer.

V9 adds a tombstone generation and completion metadata without changing protobuf
field identities. A body rewrite clears the generation; old work cannot remove a
new generation even if timestamps coincide. ACL edits after admission do not undo
the accepted command. Repeated deletes of the same tombstone share its generation
while retaining their own object scopes and command IDs. The recovery sweeper
locks and revalidates legacy rows before assigning a generation and enqueuing;
it does not replace existing admissions or reset exhausted retry state.

Migrated queue records have ASYNC mode and unknown generation/checksum. They keep
their previous timestamp guard only for legacy rows; they cannot remove a new
generation. Operators must review any remaining legacy cleanup rather than infer
its ownership from new data. Re-deleting a legacy tombstone with a pending legacy
command fails with FAILED_PRECONDITION until that command settles; it does not
silently cancel the earlier raw-blob cleanup or invent a generation binding.
Restore must preserve both the row generation and
its purge records; a generation without a durable admission is reported as an
error instead of manufacturing a cleanup scope.

The legacy async raw-blob path has an unresolved physical-data race: its upload
key is deterministic and may be overwritten between purge eligibility checking
and key-based deletion. Generation checks protect row removal, not a newer raw
upload at that key. Immutable raw objects and reference-aware cleanup are required
for legacy objects; the managed raw path below uses those mechanisms. ETag-only conditional deletion is
insufficient: another upload of identical content can reuse the same ETag.
Scoped deletion remains disabled
while its authorization and raw-byte protection are unfinished.

### Document part publication and reclamation

The current full and partial save paths write fresh part keys before
`DocumentLedger.saveIfRevision` publishes their manifest. Partial saves also copy
carried parts into the fresh attempt prefix. A document-manifest snapshot is not
authority to delete objects absent from that snapshot: a concurrent write can
land after the snapshot, be deleted by the sweep, and then publish successfully.
Increasing the sweep age or probing storage immediately before commit does not
fence the subsequent delete.

Two real PostgreSQL/S3 regressions pause after a successful provider PUT and
before publication, run an armed zero-age sweep, then resume the save. Both Java
and gRPC previously returned success after the part had been deleted. The
`documents` namespace is now quarantined from the general orphan sweep and from
generic blob mutations. It covers historical fixed keys and current attempt
keys. Exact document purge snapshots remain active. This removes the demonstrated
sweep race but **does not implement abandoned-part reclamation** or qualify the
remaining document purge/publication races. Unrelated loose keys under an exact
`documents` segment are also reserved by this pre-release API change.

The next implementation uses a dedicated document-part attempt ledger. Reuse the
raw/archive implementations' transaction, immutable backend-profile, lease-token,
reclaimer and late-write reconciliation patterns; do not put document parts into
tables whose owner identity is a raw upload or archive entry.

Managed document writes require an explicitly qualified composition with a
persisted immutable provider identity and an exact-object reclamation capability.
If either is unavailable, admission fails UNSUPPORTED before provider writes;
there is no automatic legacy-write path or current-backend recovery fallback.
Each additional provider must qualify this contract independently. This gate
applies when admission is wired, not to the quarantine-only implementation today.
Historical reads retain their explicit existing behavior; legacy adoption is a
separate operation and cannot infer coordinates from mutable drive configuration.

1. Register an immutable attempt UUID, destination node/account, sampled document
   and source revisions, original backend generation/realm and namespace before
   any PUT or COPY. Persist the exact planned part keys, part/sub-key identity,
   expected size and digest. Keys must be unique to this attempt. The complete
   plan includes copied parts; a prefix alone is not a physical cleanup scope.
2. Record verification for every planned PRESENT object. A successful COPY
   acknowledgement alone is not proof that its bytes match the sampled source
   manifest. Verify the required byte identity, including provider version where
   available. Renew a token-fenced lease during long writes and copies; expired
   or superseded attempts cannot publish. No SQL transaction spans provider I/O.
3. In the existing `saveIfRevision` transaction, lock the attempt, check its lease
   and complete verification, recheck destination/source revisions and applicable
   policy, and bind the exact manifest/object set to the document. Switch the
   document's attempt reference and its raw references atomically with its row and
   outbox event. There is one publication decision, not a second post-commit
   binding transaction. Preserve revision-conflict and ambiguous-commit behavior.
4. Cleanup locks that same attempt identity and checks references before claiming
   it DELETING. Publication cannot bind a claimed attempt. Perform exact-key
   physical reclamation outside SQL, persist failures and confirm absence before
   completion. Keep cleanup tombstones and reconcile late PUT/COPY completions.
   Thread interruption and lease expiry are not proof of physical cancellation.
5. Route document purge and superseded-attempt cleanup through this same ownership
   boundary. Define how already-started reads and copy sources are protected or
   fail explicitly during concurrent retirement; never turn missing bytes into
   successful empty content. Historical unbound part keys require explicit
   verified adoption or remain quarantined. Restore must preserve attempt rows,
   bindings, references and cleanup state together.

The first implementation unit is SQL attempt admission, immutable planned objects
and fenced verification, with migration/rollback/lease tests. It does not enable a
cleanup worker. Subsequent units wire full and partial publication, then guarded
reclamation and host recovery. Acceptance must cover snapshot-before-publication,
cleanup-claim-before-publication, publication-before-claim, expired/lost leases,
source changes during COPY, failed part writes, failed SQL publication, ambiguous
commit acknowledgements, cancellation, restart and late writes. Test both local
and transport paths against real storage. Remove the quarantine-only limitation
only after these gates pass; do not re-enable snapshot-based deletion.

Publication implementation decisions:

The low-level `DocumentAttemptWriter` composes staging and atomic publication.
It validates the selected drive, backend generation, address and every planned
key before attempt admission. Its shutdown barrier counts the complete operation,
including candidate construction and SQL publication. The host must close it and
successfully await idle before releasing its borrowed provider or database.
Failed writes retain their attempt ID and cause for recovery and commit-outcome
reconciliation. Engine integration must preserve cancellation, deadline and
revision-conflict status through that failure wrapper. This seam does not enable
managed saves or supply authorization, schema admission, or source-part planning.

`DocumentPartReader.readLegacyFragments` supplies exact verified source bytes for
future managed partial saves. It rejects invalid or duplicate selected slots,
missing SHA-256 digests and oversized declared selections before provider reads.
The default selection limit is 256 MiB and can be lowered by the host. CORE uses
its recorded provider version and ETag when known. Other legacy parts have no
recorded provider version: their currently readable bytes must match the saved
size and digest. Missing source objects remain `FAILED_PRECONDITION` so the
caller can retry as a full save; mismatched bytes are `DATA_LOSS`. Neither case
causes historical-version guessing or empty-content substitution.

This byte reader does not adopt an old row as a managed publication. Before
wiring it into saves, strictly parse the source manifest, authorize its sampled
row, confirm the same revision remains unbound, and retain source revision and
drive-state checks through publication. `DocumentSourceSnapshot` supplies the
physical-state portion: it reloads the authorized revision, requires an available
row and strictly parses the persisted manifest in one transaction. Legacy capture
checks the unbound state and selected drive; bound capture resolves its actual
publication and retained provider/part identities through the ledger in that
same transaction. It does not accept a caller-constructed publication record.

The writer requires exactly one matching snapshot for each planned source before
staging. Publication repeats the source revision, status and binding checks
under the existing sorted document locks, then locks the destination and legacy
source drives in UUID order and compares their full sampled routing/configuration
state. Bound sources use their retained publication rather than current drives.
These are physical source fences, not an authorization decision or a substitute
for policy-revision checks. They also do not establish that a supplied payload
was derived from the declared source; the shared engine owns that preparation.

The legacy selection limit is per call. The reader also reserves against a shared
payload budget, retained by returned batches until closed. The host composition
must supply the same budget to readers and writers across read-to-stage operations.
The stager's existing shared active-payload budget now reserves twice the total
input length before cloning or admitting an attempt: one allowance for private
input copies and one for bounded verification results. It reserves all parts
conservatively even when only a subset runs concurrently, and releases only after
started workers drain. This budget excludes caller-owned inputs, SDK internal
buffers and object overhead; it does not measure live JVM heap or force collection.
Hard aggregate memory guarantees require composition accounting in addition to
the individual provider response limits.
The byte SPI now has an explicit `getBounded` operation, implemented by the direct
S3 adapter with a streaming GET. It rejects declared oversize before reading,
checks unknown-length bodies while consuming them, and never returns a truncated
prefix. Zero permits an empty object; failures abort the acquired stream. The
limit covers payload bytes, not SDK overhead or aggregate memory. Other adapters
fail explicitly until implemented. Staging now requires this capability before
admission and bounds each verification read by that part's copied payload size.
An oversized stored version leaves the attempt unverified. Document publication
reads and legacy source reuse now bound each provider read by its recorded size.
Oversize is DATA_LOSS; unsupported bounded reads are FAILED_PRECONDITION with no
unbounded fallback. A part larger than the Java byte-array API can represent is
RESOURCE_EXHAUSTED. Supplying a common host budget remains an integration gate.
A cache's backing-provider capabilities must not be
treated as capabilities of the cache decorator itself.
The managed-publication reader continues using retained backend and per-part
provider identities. Both paths preserve fragment order and copy provider
buffers before measuring or returning bytes, without decoding and reserializing.

### Shared payload ownership

`PayloadBudget` in the byte SPI provides a nonblocking, overflow-safe byte
reservation with an idempotent closeable lease. `DocumentAttemptWriter` accepts
a borrowed budget so multiple writers can share a host limit. The convenience
constructor retains a private 256 MiB budget; using it does not establish a
host-wide cap. Staging now uses these leases for its combined input and
verification allowance. Capacity exhaustion occurs before attempt admission.

The Java fragment methods now return `DocumentReadBatch` instead of a raw list.
Callers use `batch.parts()` and must close the batch after source reuse/staging;
typed reads close their batch after assembly. The reader accepts a borrowed budget,
with a private 256 MiB default. Before dispatching any GET it reserves twice the
selected recorded sizes, covering provider results and detached copies. Both
allowances are conservatively retained until batch close and all entered workers
exit. Acquisition is nonblocking and overflow-safe. Staging reserves its input
copy and verification output against its supplied budget.
Source and staging reservations overlap deliberately; saturation fails explicitly
rather than waiting while holding another reservation.

Cancellation closes batch ownership but does not release its lease while entered
workers remain. Late results release on actual exit, and closed batches reject
workers that have not yet entered; cancelling a Future does not prove exit.
The reader now exposes `close()` and `awaitIdle(timeout)`. Close rejects new
operations and worker registration; the drain barrier waits for entered resolver
operations and actual provider workers before the host may release its borrowed
backend. A worker past its pre-call check may race with close and start a GET,
but stays counted until exit. A timeout means the backend must remain open.
Returned batches remain caller-owned and can outlive a drained reader; drain does
not release their payload reservations. Typed assembly after retrieval uses no
backend resource. Tests must cover
concurrent saturation before I/O, partial fan-out failure, uncooperative reads,
batch ownership through staging, repeated close, and release after success/error.
Arrays retained by a caller after closing its batch and SDK-internal buffering are
outside the guarantee. The batch and reservation mechanisms are implemented;
production host composition remains pending. `readSource` selects bytes from the
captured source snapshot, using retained publication identity for managed sources
and captured namespace/core identity plus a host-qualified store for legacy ones.
It makes no new authorization decision. The library integration test now carries
a managed source batch through a real writer publication with one shared budget:
exact combined capacity publishes version 2, while one byte less rejects before
attempt admission and preserves version 1. The batch reservation survives through
publication and releases on close. This proves the component handoff, not the
public SaveDocument composition: raw references, outbox, ownership fences, metadata
and host lifecycle still need to be integrated before that route is enabled.

### Save engine integration obligations

`DocumentSaveCandidate` now holds the existing pure row-construction behavior,
used by the legacy save path and available to the managed writer's candidate
factory. It preserves creation/reprocessing metadata and retains the destination's
datasource and stored security for scoped callers. The caller must already have
authorized the exact destination revision; this builder does not authorize a save.
The current transaction still publishes raw bindings and enqueues the saved event
with cancellation checks before and after those changes.

An explicit `DocumentOperations` constructor now accepts a borrowed managed writer
alongside the reader and retained generation. In that configuration, full saves
use the existing authorization and locked dedupe checks, then build an admitted
attempt and publish verified parts through `ManagedDocumentSave`. Raw references
and the saved event run in the writer's publication transaction. Existing host
constructors remain unchanged. This composition also supports partial saves from
legacy and managed sources: it captures the authorized source, reads selected
unchanged parts under the shared byte budget, and stages the complete revision
under fresh attempt keys. Source revision checks precede atomic publication.
Integration cases cover chunk-set order and preservation of unchanged provenance.
Real SQL/provider tests cover preserved raw references across drives for legacy
and managed sources, through library and in-process gRPC calls. They verify
managed publication, returned blob metadata and cleanup refusal while referenced.
Managed deletion remains unavailable: both delete modes now fail with
FAILED_PRECONDITION under the document row lock before admitting tombstones or
cleanup records. Retained publication history also causes refusal. Tests verify
that refusal preserves revision, status, raw references and source readability.
Legacy-source deletion proves the destination alone retains its copied raw bytes;
managed-source cases retain both references and do not establish physical purge.
Source revision races are exercised after the real source GET and after a
destination PUT, for legacy/managed sources through library and gRPC calls.
Preflight refusal admits no attempt; the later race leaves one unpublished attempt
for recovery. Neither exposes a destination row, publication or raw reference.
These cases change reprocessing metadata to advance the source revision; they do
not establish scoped ACL revocation behavior, which remains qualification work.
This is not yet the production host default or a complete managed-save feature.

Partial saves currently copy unchanged bytes. Before production wiring, measure
small updates to large documents, including bytes read/written and peak reserved
bytes. Reusing immutable objects could reduce this cost, but requires explicit
shared retention and reclamation rules; do not bypass integrity or recovery checks
to obtain lower latency.

Managed routing must preserve these behaviors explicitly:

- Full-save dedupe mutates reprocessing bookkeeping and raw references under the
  authorized revision lock. A managed publication must not become inconsistent
  with that changed row revision or history; qualify this path before enabling it.
- Partial saves keep canonical part order and existing chunk-set positions.
  Carried parts retain their update/provenance stamps, rewritten entries take
  current stamps, and absent parts remain explicit EMPTY entries.
- Source authorization precedes snapshot capture/read; destination and source
  revision fences must also cover the policy used for that authorization.
- The shared candidate builder feeds the managed writer. Raw-binding publication
  and the saved outbox event run on the writer's supplied EntityManager in its
  atomic publication transaction, never as a second transaction.
- Target/source and raw-drive validation both acquire PESSIMISTIC_READ locks.
  Opposing shared drive locks are compatible; the earlier proposed X/Y deadlock
  does not justify a union-order refactor. Preserve the sorted validation and
  reassess ordering if a future path adds exclusive drive mutation or upgrades.
- Carried BLOBS bindings must derive from the exact verified source batch.
  `ManagedRawBindings.copying(batch, source, account)` now parses that batch
  without another provider GET, rejects duplicate/changed/malformed BLOBS, then
  runs the existing exact-reference admission checks. The older store/drive
  overload remains for legacy saves and must not be used for managed composition.
- Capacity, cancellation, deadline and revision errors wrapped by WriteFailure
  now retain recognized domain meaning through `RepositoryErrors.call`, with
  attempt ID and phase in the public error message and the original wrapper as
  local cause. Staging verification mismatch is explicit DATA_LOSS. Unclassified
  state/identity fences are FAILED_PRECONDITION; only typed revision conflicts
  or explicit domain conflicts map to CONFLICT. Other unclassified
  failures remain UNKNOWN; this classification promises neither rollback nor
  retry safety. An ambiguous commit is reconciled, not blindly retried.
- Host composition supplies a shared budget and drains writers/readers before
  releasing providers and SQL resources. Passing a cache decorator requires its
  own qualified bounded-read support or explicit original-backing resolution.

Performance qualification remains required before switching application writes.
The first stager performed each part's PUT, exact read-back and SQL verification
sequentially; legacy part uploads use concurrent fan-out. Measure latency
(including p50/p95), throughput, SQL work and buffered bytes with representative
part counts and sizes on the same backend. The read-back and lease guarantees
must survive any bounded-concurrency or batching change. No performance parity
claim is supported by the correctness integration tests.

The opt-in diagnostic can be run from the repository root with
`PROTOMOLT_DOCUMENT_BENCHMARK=true ./gradlew :protomolt-repo-container:test --tests '*DocumentStagingBenchmarkIT'`.
It bypasses Gradle's test cache and up-to-date results and writes raw samples to
`repo/container/build/reports/document-staging-benchmark.csv`. It compares legacy
upload-only work with managed staging, not equivalent end-to-end save semantics.
Both paths validate actual stored bytes outside the timer; managed staging also
performs its required read-back verification inside the timer. Payloads are
synthetic protobuf fragments, not schema-admission fixtures. Provider timings are
cumulative call durations and may exceed wall time when calls overlap.

The [October 3 diagnostic samples](../evidence/repository/2026-10-03-document-staging.csv)
were captured on host `krick`, with PostgreSQL 18 and LocalStack 3.8 containers,
an unversioned namespace, and the implementation at `bd3e4bbf` plus the benchmark
harness. Each size/count/path has 12 measured samples after two warmups; path
order alternates. The host was not idle (roughly 25 percent CPU use), and p95
with 12 samples is the largest sample, so these are investigation evidence only.
Using nearest-rank percentiles, 32 parts of 262148 bytes each had managed p50 of 964 ms and legacy
upload-only work was 51 ms. Mean cumulative managed GET time was 740 ms, compared
with 120 ms for PUTs. Sequential read-back is therefore a concrete optimization
target in this environment; this does not establish its production contribution
or the speedup achievable through parallelism. Before enabling managed saves,
compare sequential and bounded-parallel managed staging with identical checks,
including cancellation, worker draining, memory bounds, lease loss and recovery.

The stager now uses four workers per attempt and a shared limit of 32 active
part sequences across its concurrent attempts. Each sequence still renews its
lease, PUTs, reads back the exact returned version and records measured identity.
Returned parts retain plan order. The cancellation check is nonblocking and
thread-safe because workers may call it concurrently. No transaction spans
provider I/O.

On failure or caller interruption, stop assigning new work and wait for every
started worker. A worker already past its active check can still start a late
call; its admitted key remains recoverable. Keep part permits, copied-byte budget,
the owner slot and heartbeat until workers finish. Restore caller interruption
after draining, and retain the first failure with subsequent failures suppressed.
Tests use versioned provider calls to establish overlap, the shared limit,
ordered results, corruption rejection, lease expiry, cancellation and shutdown.
The existing single-phase fault and abrupt-process-exit tests explicitly select
one worker so their injection points remain deterministic.

The [bounded-worker diagnostic](../evidence/repository/2026-10-03-document-staging-parallel.csv)
records 216 samples with the same fixtures and 12 samples per case. It rotates
legacy uploads, one managed worker and four managed workers on the same opened
provider. Both managed paths perform identical admission, lease, read-back and
verification work. For 32 parts of 262148 bytes, nearest-rank p50 was 1011 ms
with one worker and 315 ms with four; p95 was 1400 ms and 365 ms respectively.
For 32 parts of 4099 bytes, p50 was 195 ms and 101 ms. Single-part differences
are noise, since both settings launch one worker for one part. This supports
four as the initial bounded default, without establishing production performance
or parity with upload-only writes. The host remained shared and was observed at
roughly 56 percent CPU use during this run; a quiet-host qualification is still
required for performance claims. The benchmark command now runs all three paths.

- Keep the active attempt reference in a separate publication table keyed by
  document node, with a unique attempt reference. `saveIfRevision` already flushes
  and refreshes the document before running its callback. Updating the document
  again in that callback would advance its database mutation revision again and
  leave the returned row and outbox event stale. A mutable binding field on the
  public `DocumentRecord` also risks being cleared by ordinary `merge` calls.
- Share the existing sorted document advisory locks and destination/source row
  locks; then lock the admitted attempt. Recheck token, database-clock expiry,
  VERIFIED state, node/account, sampled destination revision and exact source
  revision map. Match every PRESENT manifest entry in order to the sealed plan,
  including slot, key, size and digest. Check manifest address, root checksum,
  total size and CORE provider identity. Per-part provider versions remain in
  the attempt ledger because the current manifest has no such fields.
  Managed publication parses persisted manifest JSON strictly. New bodies must
  be AVAILABLE with no pending purge. The transaction must derive the next
  manifest version from the locked prior row, not merely accept a positive
  version, and compare the selected drive's physical namespace/provider profile
  with the admitted generation. A drive name alone is not proof of location.
- Enforce final row/publication consistency with deferred database guards. The
  document flush precedes the publication switch, so an immediate cross-table
  check would reject a valid transaction. Removing a publication while its row
  survives must fail; deleting a row releases its reference in the same commit.
  Replacing a reference must pass the new attempt's publication gate. Preserve
  an immutable publication fact so a retired attempt cannot be published again.
- Status-only tombstones, purge failure and dedupe bookkeeping retain the same
  binding. `CoherenceProbe` currently marks missing parts in the manifest and
  saves it through ordinary `saveIfRevision`. For bound documents it must report
  corruption without rewriting that manifest until an explicit managed repair
  operation exists. Do not enable managed saves before this path and document
  purge respect the new ownership boundary.

The internal `requirePublishable` checks the candidate against the locked attempt
but does not create a publication, acquire document revision locks, authorize a
caller or emit an event. It is a precondition helper for that transaction, not a
standalone publication API. Its PostgreSQL fixtures use synthetic measured-byte
identities and do not qualify provider writes, recovery or end-to-end publication.

This is one document publication boundary. It does not supply JCR session saves,
multi-object content transactions, JCR workspaces, or version restoration.

### Managed raw uploads

V10 adds managed raw-object records and document reference tables without adopting
existing raw keys. `RawObjectLedger` provides leased STAGING/VERIFIED attempts,
LIVE bindings in the document publication transaction, and token-fenced cleanup
claims. Real PostgreSQL cases cover rollback, shared references, cross-account
rejection, expired leases, immutable metadata, cleanup retries and a collector
waiting on a publishing transaction's lock. DELETED records remain available for
reconciliation of late writes. Qualified HTTP ingestion and the managed recovery
worker now use these coordination primitives. The HTTP replacement regressions
below pass locally; this is not evidence of deployment.

The document engine now maintains those references during qualified full saves,
partial copies and dedupe. Ordinary requests can retain their destination's
admitted objects or inherit an authorized copy source's objects; naming a managed
key does not create a binding. The engine compares storage coordinates, version,
size, SHA-256 and MIME type with the retained record. Copy preparation verifies
the source BLOBS fragment against its manifest, and copy completion verifies the
destination bytes too. Publication rechecks the source reference set, backend
identity and locked drive configuration before replacing references in the same
transaction as the document and outbox. Dedupe performs these checks before
incrementing its counter. Removing BLOBS releases the current document's refs;
deleting one owner leaves shared raw objects pinned by the others.

Existing engine constructors continue supporting unmanaged documents. Managed
references require the host to supply an explicit qualified backend identity;
without it they fail as unsupported. This enables reference-preserving operations
over already admitted records. The shared ingestion operation and qualified service
composition establish that identity and enforce capability, lease and admission
requirements. Deployment retention remains an explicit operator obligation.
Real SQL/object-store tests cover library/gRPC
parity, metadata mismatch, unadmitted refs, cross-drive copies, shared retention,
corrupt fragments, and binding/provider changes during copying.

The real HTTP regression `rejectedReplacementPreservesCommittedDocumentAndRawBytes`
originally reproduced a bad-checksum replacement deleting the previously committed
raw object while GetDocument still returned its metadata. It now verifies the
committed object survives the rejected replacement. The accepted-replacement regression
also requires distinct physical keys and unchanged old bytes before reclamation.
This is not indefinite retention: garbage collection may remove an unreferenced
old object. A retained version or another document must pin it when continued
readability is required. Staging only until
checksum verification would fix the first case but leave publication and purge
races unresolved; the complete fix uses immutable per-attempt keys.

Move raw ingestion behind the shared repository engine. The engine mints a unique
key for both supplied and content-derived document IDs, records the attempt before
storage I/O, streams and hashes the bytes, verifies the declared checksum, and
publishes through the guarded document-save transaction. HTTP parses the request
and renders the resulting committed receipt. Logical document and blob IDs remain
stable; physical keys are not derived from those IDs. Existing protobuf identities
and public storage-reference fields remain unchanged.

Persist a managed raw-object record with its account, immutable storage location,
provider/configuration identity, size, computed checksum and attempt state. The
location includes the original bucket and key; later drive reconfiguration must
not redirect cleanup. Store provider version identity when available. Missing
original provider configuration fails cleanup explicitly rather than selecting a
replacement backend. Do not persist provider secrets in these records.

Use a document-to-raw-object reference table with real foreign keys. A storage
reference supplied in a protobuf payload does not confer deletion authority.
Only trusted ingestion or an authorized copy of an existing managed reference can
establish a binding. Copied BLOBS parts share the source binding after checking
source access and revision; byte references must match the bound record. Retained
versions and pending revisions will need their own foreign-key reference tables
before their retention features are enabled.

The shared raw-blob operations now reserve the exact slash-delimited
`.protomolt-managed` key segment. PutBlob (including generated effective keys),
conditional put and deletion reject it with PERMISSION_DENIED, including for
process-authority callers; administrative reads remain available. New managed
attempt keys use `<drive-prefix>/blobs/.protomolt-managed/v1/<uuid>.bin`. This is an
API mutation boundary, not a claim that direct provider users already honor the
managed lifecycle. Existing arbitrary keys using that segment become read-only
through these raw APIs and require an explicit migration path if present.
Remote repository providers need an explicit trusted
managed-write path through this reservation; until that exists, managed ingestion
on those backends must fail as unsupported. Providers must also guarantee that
referenced objects do not expire independently through a TTL. Redis/cache modes
with automatic expiry are not suitable without an explicit retention capability.

The byte-provider capability NON_EXPIRING_WRITES describes only ordinary adapter
PUT/COPY behavior: S3 advertises it, and Redis advertises it only with configured
TTL zero. Redis non-expiring rewrites clear inherited metadata expiry as well as
byte expiry. This capability alone does not qualify durability: external lifecycle
policies, Redis eviction/persistence and administrative mutation remain separate
deployment obligations. Cache compositions qualify the authoritative backing
store; cache entry expiry does not determine retention of backing bytes.

Production composition requires an explicit nonsecret backend-generation ID,
storage realm and retention qualification. Bind the generation in SQL to a
canonical nonsecret physical profile before enabling uploads. Reusing a generation
with changed routing must fail startup; credential rotation must not change the
identity. S3 and S3-cache share the authoritative backing profile. Historical
profiles distinguish `SDK_DEFAULT` endpoint resolution from an explicit endpoint
override; no AWS hostname is synthesized from the region. Historical
generations need an explicit resolver for cleanup; an unavailable original backend
must produce durable cleanup failure, never redirect deletion to the current one.
V11 and `ManagedBackendLedger` now provide immutable SQL generation bindings with
atomic insert-or-check registration. Real PostgreSQL tests exercise restart,
conflicting registrations and direct mutation refusal. Existing raw rows are not
automatically adopted or bound by this migration. The composition must register
its profile and resolve historical generations explicitly before using them.
Qualified service composition now binds the profile, supplies shared HTTP ingestion
and starts managed recovery before opening the upload listener. Set all three:
`DOCUMENT_PLATFORM_MANAGED_BACKEND_GENERATION`, `DOCUMENT_PLATFORM_MANAGED_STORAGE_REALM`
and `DOCUMENT_PLATFORM_MANAGED_RETENTION_QUALIFIED=true`. These are operator
qualification inputs, not automatic checks of storage lifecycle or durability.
S3 and S3-cache must advertise streaming, non-expiring writes and physical
reclamation; lifecycle recovery must be enabled. Redis and remote managed ingestion
remain unsupported. Missing qualification leaves the service usable but makes valid
document-upload requests return 503 before reading their body. Archive routing is
unchanged. Legacy HTTP constructors still compile and use that same refusal; hosts
must supply the `RawIngestionRepository` constructor to enable document uploads.
Recovery scans at most 100 records inactive for one hour on each configured sweep
interval. Tombstones remain for repeated reconciliation. This host resolves only
its configured generation; other generations remain durable failures requiring
their original backend configuration to be restored. No legacy key is adopted.

Identical reuploads compare computed content identity and all relevant document
metadata against the current managed binding. Normalize to the retained reference
only when they match, with the sampled document revision guarded through dedupe
and publication. Return the committed reference, which may differ from the upload
candidate. An ambiguous commit outcome must never trigger blind candidate deletion.
Legacy deterministic keys require explicit migration; they do not acquire an
immutability guarantee simply because new writers use unique keys.

Raw garbage collection is separate from document-part purge snapshots. In one SQL
transaction it locks the managed object, verifies no live owner references remain,
and claims it for deletion. All binding operations acquire the same lock and
reject claimed objects, preventing a new reference after the zero-reference check.
Actual storage deletion is retriable and performed outside the transaction. Shared
raw cleanup must remove all versions and delete markers for the exact managed key
on versioned S3, including versions left by ambiguous PUT retries. A key-only delete
marker is not physical reclamation. Verify absence before recording successful
cleanup, evict any cache entry, and retain failure state if the provider cannot
perform or verify that operation. Shared
raw content survives deletion of any one referencing document. Every future owner
table must participate in this zero-reference check before accepting references.

An expiring upload lease fences publication, not physical storage completion.
After expiry, a late PUT may still finish after cleanup has deleted its key. Keep
expired attempt identities and reconcile their keys repeatedly so those late bytes
are eventually removed; do not report expiry as proof that the writer stopped.
The managed namespace needs an explicit reconciliation policy because the current
general reconciler excludes raw blobs. Cleanup failures remain durable and visible.

Acceptance includes both HTTP regressions, identical reupload dedupe, derived IDs,
concurrent replacement and purge, shared-reference deletion, failed checksums,
failed/ambiguous publication, restart recovery, late PUT after expiry, cleanup
failure, drive reconfiguration, and explicit legacy migration. Exercise ingestion
through the shared library and HTTP with real SQL and object storage. This work
does not enable scoped creation or deletion before their authorization contracts
are complete.

## Partial updates and progressive hydration

Support this as a bounded extension after typed admission, not a prerequisite for
the initial module extraction. Existing
[document operations](../../repo/proto/src/main/proto/ai/protomolt/proto/repo/v1/document_service.proto)
already support selected-part writes, copy-forward of unwritten parts, selected
chunk-set replacement and partial reads. Reuse those mechanics. They do not by
themselves establish a durable hydration protocol or full-object validity.

The first extension accepts replacement of named parts, chunk sets or rendition
sub-keys into a pending revision based on an immutable committed version.
Unchanged content is referenced from that base. It does not mutate the original
committed version in place. Each patch carries a revision identity, expected
revision sequence, idempotency key, target and content hash. Explicit removal is
distinct from an omitted part, an empty value and a part that has not arrived.
Do not infer deletion from protobuf default values.

Keep pending hydration separate from PRESENT/EMPTY/DELETED byte lifecycle states.
A pending revision records required components or a sealed expected-component
manifest, received components, base version, ownership and expiry. An explicit
finalize operation seals the set; quiet time or receipt of an apparent last chunk
does not prove completeness. Late arrivals cannot modify a finalized revision.

Validate each patch envelope and any independently valid component on arrival.
Required-field and cross-component rules for the complete document run only after
assembly. A valid patch is not a valid complete document. Finalization verifies
component hashes, schema identity, completeness, current policy and base-version
preconditions, then validates the assembled object and publishes it atomically
through the ledger's version pointer. A concurrent committed update causes a
conflict; the first implementation does not automatically rebase or merge it.

Normal readers continue seeing the last complete version. An explicit authorized
partial-read operation may return a pending revision together with its missing
components and validation state. It must never look like a successful complete
typed read. Ordinary review, indexing and downstream processing consume only
finalized versions; any future partial-data consumer needs an explicit contract.

This allows eventual completeness when producers finish successfully; it cannot
promise convergence if required chunks never arrive. Abandoned revisions expire
or are cancelled with observable cleanup. Retries and out-of-order delivery use
patch identity and sequence rules without silently overwriting competing changes.
Keep referenced base content pinned until the revision completes or expires, and
apply current authorization to both pending reads and finalization.

Arbitrary field-level protobuf patches, repeated-field merge rules, concurrent
automatic reconciliation and CRDT semantics are deferred. If later needed, define
FieldMask presence, clear, map and repeated-field semantics as a separate contract.

Acceptance for this extension includes delayed/missing/duplicate chunks, conflicting
patches, stale bases, explicit deletion, expiry, restart recovery, invalid assembled
content, revoked access, and atomic reader visibility. Finalization must return
the same result after an idempotent retry, including a timeout after commit.

## Partial-update performance diagnostic

Run `PROTOMOLT_PARTIAL_BENCHMARK=true ./gradlew :protomolt-repo-service:test --tests '*DocumentPartialBenchmarkIT'`.
This opt-in task disables cached/up-to-date results. PostgreSQL 18 and LocalStack
3.8 store synthetic protobuf documents with 32 chunks. A single character changes
in one chunk. Full and partial updates start from equivalent fresh documents.
Setup and complete result verification are outside the timer. Execution order
alternates, with 2 warmups and 12 samples per case. Full requests contain all
chunks; partial requests contain one, so timing includes request processing.

[October 3 samples](../evidence/repository/2026-10-03-document-partial.csv)
come from `krick`, a shared 32-CPU host with observed load averages
14.46/22.86/20.26. Storage was unversioned. CSV chunk size denotes synthetic
config-string length, not serialized protobuf size. For 262144-character strings,
final part storage totals 8390579 bytes. Both paths issue 33 PUT calls. Full
replacement issues 33 verification GET calls totaling 8390579 bytes; partial
update issues 65 GET calls totaling 16518955 bytes. Nearest-rank p50/p95 were
326/380 ms for full replacement and 518/610 ms for partial update. With
4096-character strings, the corresponding times were 128/153 and 163/239 ms.
The p95 is the largest of 12 samples. These diagnostics do not establish
production throughput or latency guarantees.

Payload reservations observed during provider calls were 16781158 bytes for the
large full save and 33037910 bytes for partial save. This excludes request
messages, SDK buffers and other heap allocations. Results passed complete
stored-document verification and reservation-release checks.

The copying cost warrants immutable-part reuse design before production wiring.
Reuse and deletion need a shared retention model: revisions retain references to
immutable physical parts; attempts track newly written objects. Publication must
bind reused identities atomically after source revision and authorization checks.
Reclamation requires release of all current and historical references. Preserve
original backend/profile, version, digest and size for reused objects. Define
cross-provider behavior and assess JCR requirements before changing publication
or history contracts. Reuse is not implemented by this diagnostic.

## Transaction and concurrency design review

Status: Java physical-object values, the V25 location catalog and the V26 native
retention bridge are implemented. Independent cross-domain references, reader
pins, common cleanup orchestration and immutable-part reuse remain unfinished.
The earlier V25 reference-table experiment remains stashed; it is not the V25
location migration now in the tree. Design choices below take priority over
fitting a new API to that experiment.

Measured evidence establishes excessive storage I/O for partial updates. It does
not establish a database lock-wait percentage or connection-pool bottleneck.
Inspection identifies a coordination cost: DocumentPartStager.stagePart invokes
2 lease renewals and 1 verification transaction per part. These operations lock
the same attempt record. A 33-part save therefore executes 99 such transactions,
plus admission, publication and scheduled heartbeat work. Profile their cost;
do not attribute the measured latency to Java thread performance without data.

### Target operation flow

The foundation separates stored-content identity, repository revision identity
and commit identity. A commit accepts a bounded change set with expected revisions
for multiple repository objects. A document save composes this primitive. JCR
sessions may accumulate changes outside SQL and submit a change set later; this
does not yet define or implement JCR sessions.

1. Admit a durable operation identity, request fingerprint, new upload intents
   and ownership token. Record exact cleanup scope before provider writes.
2. Upload only changed content using bounded I/O workers. Maintain one operation
   heartbeat, independent of individual parts. Collect provider versions and
   verification evidence. Batch persistence of evidence at defined checkpoints;
   do not renew the same lease around each object operation.
3. Validate the proposed revision and typed content. Reused immutable bytes may
   retain byte-verification evidence, but schema/admission identity must match the
   new revision. Failed validation cannot publish content or reach semantic review.
4. Execute a short SQL commit: verify operation ownership, expected revisions,
   current authorization and retention eligibility; insert revision manifests,
   object/raw references and operation receipt; switch current revisions and
   enqueue events. Lock shared invariants in a deterministic order. No provider
   network call executes under these locks.
5. Dispatch events and reclaim eligible unreferenced content asynchronously.
   A synchronous response follows the SQL commit. Loss of the response is resolved
   through the operation identity and request fingerprint, not a blind new write.

Different objects should proceed independently. Updates competing for the same
expected revision conflict explicitly; automatic merging requires domain rules.
Cross-object invariants serialize only the affected change set. Admission and
publication are separate transactions; object bytes may exist before visibility.
The commit establishes repository visibility, not a distributed physical rollback
across SQL and heterogeneous storage providers.

### Failure and scheduling requirements

An expired owner cannot publish. Late uploads remain within recorded cleanup
scope and cannot regain ownership. A crash before evidence persistence may require
re-verification or reclamation, but cannot create a visible revision. Batched
verification must preserve all checks required for publication; batching is not
permission to trust an unchecked upload receipt.

Retention acquisition and cleanup claims need a shared concurrency protocol.
Once reclamation is admitted for an object identity, later commits cannot acquire
that identity. Cleanup proceeds outside SQL with durable retry state. Historical
references, including raw bytes and schema descriptors, participate in this rule.

Retained reads need an explicit lifetime: a read pin or equivalent epoch protects
physical identities during materialization. Reclamation must account for provider
calls that continue after client expiry. Missing or corrupt retained data produces
an explicit error.

Provider qualification may permit upload checksum evidence tied to the exact
returned version, avoiding a verification GET. Until that evidence is validated,
use bounded readback. An ETag is not sufficient checksum evidence. This decision
is separate from eliminating copies of unchanged parts.

Bound concurrency by provider connections, reserved bytes and admission capacity,
with fairness across operations. Virtual threads can host blocking adapters;
nonblocking adapters can implement the same completion/lifecycle contract. Select
between them using evidence from connection wait, worker queue, SQL lock wait,
provider duration, hashing and allocation measurements. Adding threads does not
remove unnecessary storage calls or serialize fewer SQL updates.

Required experiments compare the current path with changed-part-only writes and
batched verification, preserving failure tests. Include independent documents,
competing updates to one document, multi-object commits, slow providers and memory
pressure. Record operation latency distributions, SQL transaction counts and wait
times, provider calls/bytes, throughput, and reservation bounds. The design is not
production-qualified until these results and failure/recovery tests support it.

### Upload verification evidence at the provider boundary

S3BlobStore.putRequest already supplies the expected SHA-256 through the S3
checksumSHA256 request field. DocumentPartStager supplies that digest, then performs
bounded readback. BlobStore.PutResult exposes only ETag and version ID, so it cannot
express checksum evidence suitable for selecting a different verification path.
AWS documents full-object checksum handling for single PutObject requests in the
[PutObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html).
This is a protocol capability, not proof about every compatible endpoint.

Design a qualified verified-write result distinct from a plain write receipt.
Record algorithm, checksum scope, digest, observed byte count, physical identity
and evidence method. A supplied checksum or requested Content-Length is not
independent evidence of stored bytes. Validate supported checksum responses and
endpoint enforcement; reject mismatches. Missing evidence follows the explicitly
selected readback policy or fails qualification. ETag is not the checksum.

The optimized method is opt-in and fails if required response evidence is absent
or malformed. Ordinary writes can use the selected readback path; do not hide a
broken verified-write response by retrying a different mode. Measure the bytes
actually supplied to the request. A verified stream contract needs exact-length
and trailing-byte handling; otherwise limit this method to bounded immutable byte
inputs initially. Checksum qualification and version-retention qualification are
separate: successful checksum verification does not enable immutable reuse by
itself.

Single PUT qualification does not qualify multipart uploads or composite digests.
Qualification must also specify content-type/metadata guarantees. If admission
requires independent stored-metadata verification, retain that check instead of
claiming a checksum proves metadata. Storage verification does not replace
protobuf validation, semantic review or retained-read corruption detection.

Current S3 tests include matching and incorrect digests. The incorrect-digest test
accepts either S3Exception or SdkClientException, so passing it does not isolate
server rejection from client validation. Before enabling the optimized path, add
real endpoint evidence for mismatch rejection, correct response digest/version,
missing checksum evidence, wrong receipt, stream length mismatch and ambiguous
acknowledgement. Exercise configured endpoint identities individually. Keep this
provider optimization separate from revision reuse and from SQL batching.

### Commit identity and state boundaries

This is a semantic design, not a proposed protobuf schema. Review the existing
ArchiveMutationReceipt, WorkRecord projection and schema-reference inventory
before selecting transport messages. Preserve established wire names and tags.

Operation identity is distinct from an upload attempt, a physical object and a
repository revision. Scope an idempotency key to the account and trusted stable
principal. Persist the normalized semantic command and fingerprint. Client
expected revisions and requested placement are command inputs; deadlines and
transport details are not. Reusing a key with another command conflicts. A lookup
requires current authorization; an operation identifier grants no access.

An admitted operation records upload scope before I/O. Staging collects verified
inputs. Ready means inputs satisfy the recorded admission requirements, not that
publication succeeded. Committed means the revision changes, retention references,
outbox and immutable logical receipt committed together. Rejected/aborted means a
terminal decision prevented publication. Physical cleanup has separate durable
state. Known rejection requires a durable terminal receipt under the operation key.
Record it under the operation fence after ensuring no successful commit occurred.
A timeout or lost connection is an unknown observation, not proof of an
abort. Retry/lookup reconciles the same operation identity.

Lease renewal or takeover must fence stale owners. A newer attempt cannot publish
an earlier command result without verifying the persisted fingerprint and current
commit conditions. An expired token cannot commit, even if storage finishes later.
New physical attempts use fresh keys; expired upload keys cannot be reassigned.
A retry after commit returns the recorded outcome without repeating a mutation;
cleanup observation can advance independently of that immutable logical outcome.

The commit carries expected revisions for all affected repository objects and
policy facts needed to authorize the change. Source references are authorized in
their domain; physical object identifiers are not transferable permissions.
Default changes stay within one authorized account scope. Cross-account sharing
requires an explicit policy and acceptance cases, not matching checksums or keys.
Recheck policy facts that can change independently of a document revision.

Lock ordering applies to destination/source revision records, physical retention
identities and any mutable policy records. Atomic change sets have explicit count
and byte limits. Readers must see a committed revision or a consistent committed
snapshot of the change set, never a mixture assembled from separately sampled
current pointers. The chosen snapshot/isolation protocol needs concurrency tests.

### Operation-count acceptance targets

For a qualified retained document with 32 chunks and one changed chunk, assuming
that change produces one new physical fragment:

- Exactly one content PUT, plus verification of that changed object according to
  provider qualification. No download or upload solely to copy unchanged parts.
- A complete manifest assembled from verified identities. Additional content I/O
  required by schema or semantic rules is classified separately and remains
  subject to limits; omitting validation is not a performance optimization.
- A metadata-only revision performs no content I/O when the applicable admission
  policy permits reuse of existing content-validation evidence.
- Database work consists of operation admission, bounded evidence checkpoints and
  atomic publication, plus time-based heartbeat work. It does not require a fixed
  sequence of lease/verification transactions for each part. Bulk SQL still has
  per-object data volume; count statements and lock waits as well as transactions.
  Checkpoint count scales with new-part count divided by bounded batch size;
  heartbeat count scales with operation duration divided by heartbeat interval.
- A conflict leaves no visible revision changes. Objects uploaded before the
  conflict remain in durable cleanup scope. A repeated committed command performs
  no new upload or logical mutation.

These are design acceptance targets, not results of the existing implementation.
Benchmark independent operations, one contested revision and multi-object changes
separately. A throughput result cannot substitute for conflict correctness or
bounded resource use. Choose latency targets after representative provider tests;
do not derive production promises from the LocalStack diagnostic.

### Implementation slices after design agreement

These slices preserve the eight-stage objective. They do not declare the current
implementation complete or permit skipping ownership, typed admission, historical
metadata, recovery, or progressive-hydration gates.

The first slice has two initial Java values in repo/spi:
`PhysicalObjectLocation` retains the stable object ID and original generation,
realm, namespace and key; `PhysicalObjectIdentity` adds exact provider version
when qualified, measured size, SHA-256 and stored content type. They validate
shape only. They do not reserve storage, qualify a provider, authorize access,
publish content or retain objects. An absent version requires explicit immutable
key qualification and cannot represent unknown legacy data. V25 enforces
coordinate uniqueness across profile generations within a realm.
These types add no dependencies to repo/spi; its existing protobuf dependency
remains. Four value-contract tests and the runtime dependency gate pass, and
generated Maven/Gradle metadata still declares only repo-proto directly. This
does not prove the later database adapter or published-consumer acceptance cases.

The database migration must cover all existing storage authorities. V10 raw
objects identify a backend with a string that has no foreign key to the immutable
profile table introduced in V11. Older archive manifests also have keys without
V14 bindings. Neither source can be assigned an original realm from current drive
configuration. Quarantine unknown keys conservatively until verified adoption;
preserve V23's existing global reservations for legacy document keys.

The first catalog migration records locations, not a second cleanup lifecycle.
Backfill every admitted archive binding and document attempt object, including
bare reservations and staging rows. Include raw identities only when their
original profile is established. Reject cross-domain coordinate aliases and
digest collisions rather than merging records. Lock source tables during the
scan and install write-through guards in the same migration; drain older writers
before deployment. Existing cleanup remains authoritative until all acquisition
and cleanup paths use one catalog-row fence and account for existing references.
Do not enable common references or reuse before that cutover.

V25 now registers known archive and document-part locations in
`repository_physical_locations`, using the existing archive object UUID and a
new permanent document-part UUID. `PhysicalObjectLedger` returns those original
locations without granting access or cleanup rights. Existing admission paths
register locations in the same SQL transaction. No provider call occurs there.
Current cleanup ledgers remain the only cleanup authorities.

Raw keys remain quarantined even when their old backend string matches a profile
name; verified raw adoption is not implemented. Unknown document and archive keys
are also reserved conservatively across namespaces. Known and unknown claims use
one permanent key guard: known registrations share its lock, while quarantine
requires exclusive access. Distinct known namespaces can register the same key
concurrently once the guard exists. First creation of that guard serializes.
Source insertion, catalog registration and quarantine roll back together on a
conflict. Reservations survive deletion and cleanup; no release API exists yet.
Callers must still handle database transaction failures, including deadlock aborts
for conflicting batches; this migration does not add automatic retries.

`PhysicalLocationMigrationIT` covers populated backfill, unknown raw identity,
known cross-domain collisions, rollback, both registration/quarantine race orders,
and progress across distinct known namespaces. Archive classification uses each
rendition's binding and measured facts. PRESENT renditions require a retained
version reference; DELETED renditions may preserve known location provenance
without creating a reference. An unproved occurrence cannot borrow another
rendition's known location. Document lookups use the existing digest index plus
exact key comparison. These checks establish registration behavior, not provider
qualification, shared retention, or a latency target.

V26 mirrors existing `ARCHIVE_VERSION`, `DOCUMENT_HISTORY` and `DOCUMENT_CURRENT`
owners into `repository_object_references`. The mirror requires the exact native
owner; it cannot mint ownership from an object UUID. Native insertion, replacement
and release update the mirror in the same transaction. A retained native owner
prevents direct mirror release. No public reference API or independent reader,
raw or schema owner is enabled by this migration.

Existing archive cleanup, archive mutation targets and abandoned-document cleanup
set a permanent `repository_object_retention` fence. They lock the source owner
before the shared retention row; document changes lock their part rows in object
ID order. Native reference deletion follows that order too. Existing cleanup
tombstones are fenced during backfill. Failed publication rolls back reference
changes; a successful cleanup observation does not clear the fence. Old cleanup
ledgers still schedule and perform reclamation, and provider I/O stays outside SQL.

`RepositoryRetentionMigrationIT` checks populated archive reference and archive/
document cleanup upgrades. Publication tests check mirror replacement, rollback,
forged-owner rejection and history preservation when the current document is
removed. Archive race tests check the shared fence alongside native outcomes.
The abandoned-document cleanup path still excludes published attempts, so these
tests do not claim a reachable published-reference versus abandoned-cleanup race.
Document mirrors use set-based SQL within the publication transaction, adding one
history and one current reference per part, with row guards. Their statement and
lock costs must be measured before any performance acceptance or speedup claim.

Managed archive reads now call the provider's bounded read with the retained
object size. Oversized bodies fail with DATA_LOSS, unsupported bounded reads fail
with FAILED_PRECONDITION, and objects above the byte-array limit fail before
provider resolution. Length and digest checks still run on completed results.
The local/gRPC tests use real stored bytes and an oversized provider overwrite;
capability rejection is injected around the real adapter. This bounds one managed
object, not aggregate response memory. Legacy unbound archive reads still need
the same bound, and multi-rendition responses need a shared payload reservation.

V28 adds managed archive reader pins. Admission proves the exact retained version
under the source-owner and retention locks and returns original storage coordinates
in that same transaction. Each reader incarnation has a fresh UUID, separate from
the pin and object IDs. The reader holds its pin through backend resolution, the
synchronous bounded provider call, and byte verification. Cancellation cannot
release it while the call continues. Native pins survive logical version deletion;
shared references prevent physical reclamation until the last pin is released.

Failed release is visible and leaves durable protection for retry. A retry accepts
an absent pin after a potentially lost commit acknowledgement, but rejects a pin
owned by a different incarnation. Pins never expire by age. Crash recovery still
needs explicit evidence that the owning incarnation and its provider work have
stopped; no automatic crash-pin release API is implemented. Operators must not
delete pins based on elapsed time or a request timeout.

V27 separates logical retirement from physical reclamation. An admitted archive
mutation closes reference acquisition with a permanent retiring flag; the later
cleanup claim sets the permanent reclaiming flag. Reclaiming always implies
retiring. Document cleanup sets both because its eligible attempts are unpublished.
The migration reclassifies V26 target-only fences only for LIVE archive objects
with no cleanup claim evidence. DELETING and DELETED objects retain both fences,
including failed or ambiguous cleanup. Drain old processes and their provider I/O
before migration; no runtime operation can clear either fence.

Populated migration fixtures cover retained LIVE objects, target-only retirement,
failed DELETING claims, DELETED tombstones and abandoned document cleanup. Real
PostgreSQL races cover retirement/reference admission in both orders, commit and
rollback, and progress on an unrelated object while the target remains locked.
These are SQL lifecycle fixtures, not provider byte-verification evidence.

V28 cleanup claims lock the source and retention rows, then decline while any
shared reference remains. Candidate scans exclude pinned objects, leaving room for
other eligible work. No SQL transaction spans provider I/O. Each managed object
read adds one short acquisition transaction and one release transaction; these
include native/common reference maintenance. Their latency and contention costs
remain to be qualified under representative load; they are not a performance win.

An opt-in real-adapter read diagnostic is recorded in
[`2026-10-03-archive-read`](../evidence/repository/2026-10-03-archive-read/README.md).
It preserves raw per-operation and batch measurements, including warmups, and
compares pinned reads against a test-only unsafe unpinned baseline. This shared-host
run records seven client SQL statements and two transactions per pinned read, with
larger coordination cost for sixteen readers of one object. It motivates reviewing
shared admission/release locks and reducing client round trips; it does not prove
SQL lock-wait dominance or qualify production latency. Lifetime and recovery
guarantees remain mandatory for any optimization.

`ArchiveReadLifetimeIT` uses real PostgreSQL and S3 plus a delayed provider-call
decorator. Local and in-process gRPC cases prove logical deletion completes while
the provider call remains active, cleanup skips it, and reclamation proceeds after
completion. The cancellation cases include ignored thread interruption and gRPC
client cancellation. Container cases cover two reader incarnations, failed release,
direct mirror-release rejection, and pin/mutation commit/rollback races in both
orders with observed PostgreSQL lock waits. Completed archive service operations,
including failure cases, assert no leaked pins.

Operation inventory for this slice: protobuf operations and wire contracts are
unchanged; managed archive reads and cleanup are extended with lifetime protection;
internal `ArchiveReadLedger.acquire` and `Pin.close` are new. The Java
`ArchiveObjectReader` constructor now takes `ArchiveReadLedger` instead of
`ArchiveObjectLedger`; all in-tree callers are updated. `ArchiveObjectLedger.readable`
remains a metadata inspection API and does not protect provider I/O. This does not
add pins to legacy unbound archive reads or document reads, change authorization,
or implement incarnation-drain recovery.

`ArchiveRetentionConcurrencyIT` supplies the SQL baseline for that fence. It
observes actual PostgreSQL lock waits for reference-first and cleanup-first
transactions, then checks both commit and rollback outcomes. While a reference
transaction holds one object, cleanup of another key under the same profile,
realm and namespace must finish. These use synthetic SQL lifecycle fixtures;
they establish neither provider durability nor new catalog behavior. The
cleanup-first cases exercise the SQL state transition directly, while the
reference-first cases invoke `ArchiveCleanupLedger.claim`.

1. Define immutable physical identity and reference ownership before stabilizing
   the generic commit API. Build a small provider-neutral port and PostgreSQL
   adapter without exposing SQL, Kafka, SDK or JCR types in the port. Model exact
   backend/profile, namespace, key, version, digest and size. Represent current,
   history, raw, schema and active-reader ownership. First acceptance: reference
   acquisition races reclamation safely under real PostgreSQL, with populated
   migration fixtures and rejection of unknown legacy identity. Production
   publisher integration follows later; this slice tests the concurrency rule.
2. Build a bounded multi-object change-set commit port on that model. Preserve
   separate domain revision and operation identities, expected revisions, evidence
   and receipt lookup. Test atomic changes to multiple objects, references, outbox
   and receipt, including complete rollback. Reuse existing schema/receipt contracts
   before adding wire fields. The current repo/container exposes Hibernate and
   includes Kafka; keep those out of the minimal port. Host assembly supplies the
   PostgreSQL adapter through the port. Move existing engine/container coupling in
   tested increments. Dependency metadata/runtime gates must demonstrate separation.
   Outbox persistence is transactional; Kafka event delivery is outside the commit.
3. Add durable upload intents, one operation heartbeat, bounded evidence batches
   and stale-owner fencing. Crash tests cover receipt loss, late uploads and expiry.
   Establish SQL statement/transaction counts before adopting the new path.
4. Add reusable physical references for current revisions, retained revisions,
   raw content and schema artifacts. Protect active readers. Reference acquisition
   and reclamation use the same object-state fences. Validate migration, history
   retention/release, restoration and shared-object deletion before claiming reuse
   or managed deletion complete.
5. Route a single-part update through immutable references and the common commit
   primitive. Compare real I/O with the operation-count targets. Verify concurrent
   independent updates, conflicts on a shared revision, cross-object atomicity,
   current-policy changes and resource limits. Both library and gRPC invocation
   must exercise the same behavior.
6. Qualify provider checksum receipts separately; the ordinary verified-readback
   path remains explicit for providers without that capability. S3 qualification
   does not qualify Redis, a remote client, or another S3-compatible endpoint.
7. Finish typed admission, descriptor retention, metadata/provenance and lifecycle
   conformance; then qualify host composition and deployment. Progressive hydration
   follows these foundations and remains independently deliverable.

Initial work establishes physical identity and reference/reclamation concurrency,
followed by multi-object atomicity. Subsequent slices integrate domain behavior.
No experimental path becomes the advertised implementation on the basis
of a narrow unit test or LocalStack latency result.

The pre-redesign V25 experiment is preserved in local stash commit
`ada36843929cf34cc16f0d86ac48ea16cb59009e`. It couples publication references to
attempt objects and is not the approved generic object model. Before baseline
execution, clean generated repo/container resources so a previously copied V25
migration cannot remain on the runtime classpath. Do not reapply the experiment
without reviewing it against this design.

## Immutable part reuse: implementation design

Status: design for the next ledger change, not available behavior. This follows
the partial-update diagnostic and the existing JCR compatibility assessment.
Provider versioning supplies physical identity; repository revisions group parts
and retain references. Neither identity substitutes for the other.

### Physical identity and provider qualification

A retained object binding records backend generation/profile, namespace, key,
provider version, checksum, byte length and content type. S3 version IDs already
flow through the byte SPI and attempt ledger. A non-null version selects a
specific stored object; the literal S3 `null` version is not an immutable-version
guarantee. S3 lifecycle rules and explicit version deletion can remove retained
content, so versioning does not establish retention policy by itself.
See [S3 versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html)
and [version deletion](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html).

Continue allocating fresh keys for changed objects. Referencing an existing
version avoids the copy; overwriting its key is unnecessary. This also preserves
the abandoned-attempt reclaimer's whole-key scope. A future same-key write mode
would require version-specific reclamation and tests before activation.
Unversioned storage needs a qualified immutable-key policy for reuse. The
NON_EXPIRING_WRITES capability does not supply that policy. Providers without a
qualified reuse identity retain the explicit verified-copy path, with that
behavior recorded in operation evidence. Do not silently select another backend.

### Separate staged writes from committed references

V22 currently equates the ordered manifest with all objects written by one
attempt. DocumentPartPublication validates that equality, and
DocumentPublicationLedger loads parts from that attempt. Introduce an ordered
revision-reference relation so a committed revision can combine fresh verified
objects with references to previously admitted objects. An attempt still records
only its new writes, including uncertain writes requiring cleanup.

1. Add immutable physical bindings and ordered revision references. Backfill
   existing publication history from verified attempt objects, retaining exact
   identities and manifest order. Legacy unbound objects stay explicitly unknown.
   Check migration on populated data; reject missing or inconsistent evidence.
2. Change SQL publication guards and Java validation together. Validate the full
   ordered reference set, aggregates, CORE identity, manifest and root checksum.
   Fresh references require the live verified attempt. Reused references require
   a retained admitted binding and a source authorization/revision fence. Empty
   fresh-write sets must support metadata-only revisions without invented PUTs.
3. Resolve each reference through its retained storage binding during readback.
   A revision can contain several storage identities. Keep cross-provider copy
   explicit when destination policy requires local placement; otherwise validate
   permission to retain the remote binding. Preserve existing protobuf tags,
   names, import paths and Any URLs; internal SQL relations need no wire rename.
4. Publish current and historical references atomically with document metadata,
   raw-object retention, and outbox records. Existing current raw references do
   not establish historical retention. Add historical raw pins before releasing
   current bindings or permitting history restoration.
5. Reclamation must lock physical identity and reject any current or retained
   reference. Reference acquisition and reclamation share that lock discipline.
   Preserve durable cleanup fencing for late writes. A published origin attempt
   cannot be reclaimed merely because its original document was deleted.

Reuse preserves part provenance and timestamps. It does not authorize a new
account, validate a different schema, or prove semantic correctness. Bind typed
admission to the assembled revision and descriptor identity; changed schemas or
policies need their required checks even when byte identities remain unchanged.
The repository foundation supplies reusable object references and atomic change
sets; document revisions are one consumer. JCR graph/session/workspace semantics
remain in the optional extension, with no JCR dependency in byte modules.

### Acceptance sequence

- Migrate populated managed history; exact version reads and manifest order remain
  unchanged. Reject forged reuse, wrong account, unknown binding, wrong digest,
  unavailable original profile and a source revision changed before commit.
- Change one of 32 managed chunks: verify one new object write, no copied objects,
  complete result equality and retained original identities for the other parts.
  Separate required schema/admission reads from unnecessary storage copying.
- Test reuse after a later S3 version and a delete marker, plus missing exact
  versions and suspended/null-version policy. Repeat with a qualified non-S3
  provider; do not infer qualification from a successful S3 test.
- Race reference acquisition with release/reclamation and restoration. Exercise
  cancellation, ambiguous commit and restart with real adapters. Shared objects
  survive deletion of a referencing document until the final permitted release.
- Rerun the same partial-update diagnostic and report I/O, reservation and latency
  changes. Keep production managed-save wiring gated on deletion, retention,
  authorization, lifecycle and transport conformance tests.

## Archival durability and lifecycle

Preserve manifests, checksums, entry-local sharing, version policies and exact-key
deletion. Never delete an object still referenced by a retained version. Byte-store
version IDs, repository version numbers and strong ETags remain distinct concepts.
Do not substitute an ETag for a content checksum.

SQL and object storage are not one atomic transaction. Specify staging, manifest
commit, retries and orphan reconciliation for each write path; never hold a SQL
connection during object I/O. Failed cleanup is observable and reclaimable, not
swallowed. Export/import must preserve content, descriptors, manifests, provenance
and available metadata snapshots, verify checksums, and report missing artifacts.
Restore into an authorized destination under current access policy. Include a
backup/restore rehearsal against real storage in implementation acceptance.

### Managed deletion implementation requirements

Current publication history permanently protects published attempts from recovery
cleanup. Removing a current row cannot be treated as permission to erase history.
The managed deletion implementation must first define and test:

- Atomic logical deletion of the current row/publication and current raw bindings,
  preserving separately recorded historical part and raw-object ownership.
- Explicit version retention/release decisions, including authorization and
  concurrent readers/restoration. A deletion request must not silently discard
  retained history or report physical purge while any protected bytes remain.
- Durable reclamation claims for released objects using the retained backend
  generation/profile and exact identities, independent of current drive config.
- Retry, restart and cancellation behavior across logical commit and physical
  reclamation, including cleanup failure and shared objects retained by another
  document or version. Preserve existing wire contracts; assess whether their
  outcomes can represent these states before adding an operation.

The legacy purger's admitted-key refusal stays as defense in depth. Replacing it
with direct object deletion is not a managed-deletion implementation. Complete
these semantics before enabling managed saves in the production host.

Retention periods, legal holds, WORM enforcement, signatures and trusted timestamp
services are separate future capabilities. Retained versioning alone is not a
compliance claim. Provide an explicit deletion-policy seam; when protection is
configured but unsupported, startup or the destructive operation must fail closed.
Do not advertise those future capabilities as part of the first implementation.

## Failures, retries and operation identity

Use explicit domain failures with consistent transport translation: malformed
requests or content are invalid arguments; missing authentication and denied
access remain distinct; stale versions conflict; unavailable storage is retryable
only under the operation's retry rules; corrupt retained bytes are data loss;
unsupported capabilities/rules cannot count as success. Preserve causes internally
without exposing credentials or provider internals to clients.

Define idempotency for mutations using caller/account scope, operation identity
and a request fingerprint. Replaying the same key and fingerprint returns the
same outcome; changing the request under that key conflicts. Reuse existing keys
where sufficient. Do not assume deterministic entry IDs or content-only dedupe
cover metadata-only changes, changed schemas, or different admission policies.
Version dedupe identity must include semantic metadata and schema/policy identity
that the version promises to preserve, while byte dedupe may still reuse content.

Cancellation before commit aborts publication and schedules staging cleanup.
Cancellation or timeout after commit may leave a successful durable operation;
the client recovers through idempotent retry or operation lookup. Document which
point is authoritative. Internal retries are bounded, deadline-aware, and never
retry invalid contracts, denied access or conflicts as transient provider errors.

## Implementation sequence and acceptance

### Sol review decisions

The initial independent design and authorization reviews identified these
requirements, which refine the boundaries above:

- Pin bindings per rendition/sub-key and per typed payload path, including
  `Document.structured_data` and parser-result `Any` values. Outer-message
  validation alone cannot mark nested opaque payloads validated. Policy declares
  which paths require decoding and validation; retain carried-forward bindings.
- Store descriptor artifacts durably with repository-controlled retention and
  commit-time references. Registry descriptor storage is optional and cannot
  alone guarantee historical readability. Validate linked dependency closure,
  not merely descriptor-set parsing and checksum. Reuse the existing canonical
  descriptor fingerprint algorithm after golden-fixture verification.
- Historical reads expose a distinct version metadata snapshot alongside explicitly
  current entry metadata. Legacy snapshot absence is unknown, never today's
  metadata relabeled as historical. Restore selects the snapshot deliberately and
  applies current access policy.
- Hydration seals an ordered component manifest with stable slots and hashes.
  Part arrival order does not determine assembly order. The initial protocol
  replaces named components; insertion/reordering requires a new explicitly
  supplied manifest and revision precondition. Reassemble, validate and verify
  the resulting part layout before publication.
- Introduce explicit repository authority using existing typed principals and
  account membership resolved by a trusted host. Existing action caller names
  and scopes alone do not establish tenant membership. Never accept membership
  asserted in the request as authenticated authority. Legacy malformed or missing
  ownership fails closed for ordinary callers and requires explicit administrative
  migration; no automatic unrestricted operator fallback in the shared engine.
- Deletion must first durably remove visibility and record exact cleanup work,
  guarded against concurrent references; physical cleanup follows with observable
  retry state. Existing archive object-first deletion and logged cleanup failures
  need red tests. Do not report physical deletion complete until confirmed. This
  recovery work is in scope even though generalized archive Kafka events remain
  deferred. Preserve explicit redaction versus retained-version semantics.
- Signed WorkRecord currently supports specific subject kinds. Reusing its
  provenance patterns does not make archive admission a supported signed subject.
  Keep optional projection separate and add a reviewed subject extension only if
  required; never fabricate issuer/key identity.

### Delivery stages

Each item is a bounded change with its own review. This is the proposed scope for
a subsequent goal, not an active implementation goal.

1. **Contract inventory and regression baseline.** Classify every affected
   operation as unchanged, extended or new. Inventory schema references, receipt
   bindings and current idempotency before proposing fields. Compile complete
   protobuf imports, run lint/compatibility, and capture existing part/version
   fixtures. Preserve protobuf names, tags, import paths and Any URLs.
2. **Extract byte SPI, codecs and providers.** Prove minimal consumers exclude SQL,
   Kafka and unselected SDKs using published metadata and runtime dependency gates.
   Test provider discovery, bad configuration, unsupported capabilities, resource
   cleanup and concurrent conditional writes against real providers. Preserve
   the existing conditional payload bound unless separately justified.
3. **Share repository behavior across library and transport.** Move business
   operations behind the repository interface; keep gRPC/HTTP handlers thin.
   Extract the remote clients and provider-specific provisioning. Run the same
   repository conformance cases locally and over a real in-process gRPC transport,
   backed by SQL and object-store test containers. Prove non-S3 startup constructs
   no S3 client. Keep existing public consumers compiling or document Java moves.
4. **Ownership enforcement.** Add red tests for cross-account access, deny rules,
   listing, historical reads, policy races and destructive operations; implement
   shared authorization so both invocation paths pass them. Make migration from
   previously unenforced ACLs explicit, including malformed or missing ownership.
5. **Typed admission and schema retention.** Add additive reviewed contracts and
   bindings. Test valid/invalid requests and successful responses through the real
   validator: alternatives, bounds, cross-field rules, unsupported rules, wrong
   Any type, missing imports, descriptor mismatch and registry outage. Confirm
   rejected/staged content never reaches semantic review or normal reads. Retained
   descriptors must permit historical decoding when the original registry is gone.
6. **Version metadata and provenance.** Implement snapshots and derivation/admission
   identity. Test metadata-only/schema-only changes, legacy unknown snapshots,
   revoked historical access, unchanged byte reuse and stale evaluation bindings.
   Apply SQL migrations with existing rows and verify restoration semantics.
7. **Durability, failure recovery and documentation.** Test concurrent writes,
   idempotent retries, cancellation before/after commit, checksum corruption,
   partial storage failure, retained-object pruning, cleanup failure and restore.
   Use controlled fault injection around real adapters rather than success-shaped
   fake backends. Publish accurate library and gRPC examples and module dependency
   instructions only for completed behavior.

8. **Bounded progressive hydration, after the preceding foundations.** Implement
   pending revisions and explicit finalization using the existing part/rendition
   mechanics and the acceptance cases above. Keep this independently deliverable;
   if its contract requires a broader merge engine, defer that expansion rather
   than blocking the storage and typed-admission work.

For every new annotation, record runtime validation and JSON Schema/OpenAPI
coverage. Runtime-only rules and handler obligations must be explicit; do not
claim an exported schema enforces them. Generator expansion is separate work.

Completion requires the existing affected suite plus shared local/remote contract
tests, dependency exclusions, compatibility checks and an archival restore proof.
Local tests, hosted CI, merge, publication and deployment are separate outcomes.
Azure, new search integrations, archive Kafka events, unbounded streaming reads,
compliance certification and a broad service redesign are not hidden prerequisites.
