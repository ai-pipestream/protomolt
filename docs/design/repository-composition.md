# Repository composition and typed archival admission

Status: proposed design, ready for implementation planning. This document does
not introduce an API or claim that the missing behavior is available.

Source baseline: `fd0cf28769494b281e5ca3963bff1844b22be193`, whose tree matches
the merged delegation extraction at `528117a2d48cda3b3abedadd75b5706d7ac68ca7`.
Recheck main and affected contracts before implementation.

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

The selected-byte limit is per call;
it does not account for returned buffers retained by callers. The composition
must bound total source bytes held across concurrent read-to-stage operations.
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
RESOURCE_EXHAUSTED. Budgeting retained source buffers remains an integration gate.
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

The reader currently returns raw fragment lists, which have no release point.
Replace that managed API with an ordered, closeable fragment batch and inject a
single host-owned payload budget into both reader and stager. The composing engine
must keep the batch open through staging; typed reads close it after assembly.
Budget acquisition is nonblocking and overflow-safe, before GET or copying.
Reader reservations cover both the bounded provider result and detached copy
while they coexist; the batch retains the detached-copy reservation. Staging
reserves its input copy and verification output against the same shared budget.
Source and staging reservations overlap deliberately; saturation fails explicitly
rather than waiting while holding another reservation.

Cancellation closes ownership of completed results but must not release leases
held by running provider calls. Late worker results release their leases on actual
exit; cancelling a Future does not prove that exit. The reader also needs a close
and drain barrier before the host releases its borrowed backend. Tests must cover
concurrent saturation before I/O, partial fan-out failure, uncooperative reads,
batch ownership through staging, repeated close, and release after success/error.
Arrays retained by a caller after closing its batch and SDK-internal buffering are
outside the guarantee. Reader batches, shared reader reservations and the host
composition remain unimplemented; the budget primitive and writer injection alone
do not establish the cross-operation memory guarantee.

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
