# Repository composition and typed archival admission

Status: implementation in progress under the eight delivery stages below.
Individual checkpoints are recorded in the operation inventory and revision-cutover
design. The complete typed archival admission path is not yet available; this
document does not claim that unfinished behavior is implemented.

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

The reusable `ClosedDescriptorSet` loader now checks a serialized artifact against
explicit byte, file-count, dependency-edge and import-depth bounds. It requires
all imports in the artifact, including well-known types, rejects duplicate files
and ambiguous full message names, and links in dependency order without classpath
or registry fallback. This is an available closure primitive, not typed admission:
custom-option interpretation, unsupported-rule rejection, schema/type identity
binding, durable descriptor retention and historical restore integration remain
required. Existing registry artifact hash validation and permissive classpath
loading retain their separate purposes.

The [descriptor retention design](repository-revision-cutover.md#descriptor-retention-design)
chooses a bounded SQL artifact catalog with staging claims and immutable revision
references. It remains a design: current identity and payload checks are in-memory.

## Admission and validation boundary

Expose two explicit policies: opaque archival intake and validated typed intake.
An archive may require typed intake for named renditions. Clients cannot bypass
that requirement by omitting schema fields, changing transport, or using raw
upload. Opaque originals remain legal where policy permits them, but must never
be represented as validated typed content.

Opaque intake preserves an Any's exact type URL and value bytes without requiring
its message definition or deserializing the packed value. The surrounding request
and document envelope still require bounded parsing, ownership checks and byte
integrity verification. A stored opaque value may be read as bytes by an authorized
caller; it cannot satisfy an operation requiring validated typed input.

Keep resolution and validation separate in future admission metadata. If lookup
was skipped, record resolution as not attempted rather than claiming the
definition is missing. If lookup established that no definition was available,
record that outcome explicitly. A registry outage or denied access is a different
outcome, not evidence of a nonexistent type. Resolution alone does not establish
validation. Opaque intake must be explicitly allowed by policy; required typed
intake cannot silently degrade after a lookup, decode or validation failure.

Later typed processing resolves a pinned definition, retains the schema asset and
validates the original bytes before producing validation evidence. Attach new
evidence to the same immutable content identity through the version/provenance
model; do not rewrite earlier records to imply validation happened at intake.
Deserialization is demand-driven for operations that need fields, such as mapping,
projection, annotation validation or embedding extraction.

The additive RepositoryAnyResolution definition now records exact type URL,
value-byte digest and size, with one outcome: not attempted, resolved schema
reference, or a failed lookup classification. Definition unavailable means an
authoritative absence; lookup failure and access denial remain distinct. Resolved
references reuse PublicationSchemaCondition and the exact descriptor artifact
digest without embedding descriptor bytes or compiler metadata. This is a
resolution observation, not a validation record. Enclosing evidence must still
bind the account, operation/attempt, revision and occurrence; handlers verify the
bytes and actual outcome. No RPC or persistence path is mounted by the definition.
For resolved observations, the handler must match the retained asset metadata's
exact type URL and PublicationSchemaCondition as well as its artifact digest.
Durable validation evidence and revision wiring remain pending.

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

Credential integration requirement: externally exposed clients authenticate, and
the host binds effective account and ACL grants to that authenticated call. An API
key identifies the principal and its permitted scope; it does not make a requested
account ID authoritative. Credential-specific restrictions must survive resolution
even when several keys identify the same principal. Creation must check an explicit
creation grant and establish ownership from the authorized context. Existing
ownership and ACLs govern updates; another account ID in a request cannot transfer
ownership. Source-owner metadata remains provenance, not the authenticated creator.

Resolve a stable effective repository grant at the transport edge: permitted
operations, account IDs, typed ACL identities and key-specific limits. Preserve
that grant or its trusted identity before discarding credential context. The
current actions `Caller` loses key identity, so a later principal-only binding
cannot distinguish two keys with different grants. Method scopes alone do not
establish repository permissions. Never pass raw keys into repository or provider
modules.

Current host gap, audited at `87331b50`: `RepoServices` constructs the default
two-argument document caller binding. Standalone and module Netty startup supply
the optional operator token with no caller resolver. `ApiTokenServerInterceptor`
maps that token to process authority; an open listener's absent caller context also
defaults to operator. HTTP upload checks one optional token and invokes ingestion
as `http-upload` with process authority. These paths do not implement per-client
credential-to-account ownership. Scoped repository tests use injected trusted
bindings and are not proof of production credential wiring.

Before client-key exposure, wire a host-controlled credential binding through the
actual gRPC/HTTP entry points and test two keys with different account grants,
missing/revoked credentials, forged account IDs and attempted authority escalation.
Require an authenticated operator credential for externally exposed raw HTTP
upload until a scoped upload boundary is implemented. The current tokenless
listener grants operator authority without authenticating an operator. Missing
authentication on externally exposed endpoints must fail
closed; trusted in-process library calls receive their caller from the host.
Delegated agents retain the initiating caller's effective scope instead of gaining
coordinator authority. Feed and connector credentials likewise need explicit
datasource/account grants. All adapters must use the shared repository checks;
credential storage does not belong in the byte SPI or provider modules.

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
commit conditions. An expired token cannot newly pass publication admission,
even if storage finishes later. Expiry does not preempt a transaction that already
passed its publication checks while holding the database owner fence: it may
finish, and takeover/cleanup waits for its commit or rollback before deciding.
Document history insertion and current-pointer switching each check a live lease;
expiry before either check aborts publication. Expiry after both checks does not
retroactively undo it. Keep transactions short and explicitly bounded rather than
using lease expiry as a substitute for transaction timeout/cancellation.
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

### Durable commit command and replay design

This design extends the internal document batch; it is not an available API.
`DocumentPublicationBatch` currently has neither durable operation identity nor
an outcome lookup. Its callbacks are internal SQL participants. The eventual
`repo/spi` port must use immutable values and existing trusted `RepositoryCaller`
and operation-control types, with no SQL callbacks, Hibernate entities, Kafka,
provider SDKs or JCR dependencies. Ordinary document/archive methods retain
their current wire semantics until an additive reviewed boundary is implemented.

**One executable command.** A command contains an encoding version, one account,
an operation UUID and bounded changes with stable per-command member identities.
Every change specifies destination identity, explicit expected-absent or expected
revision, mutation kind, requested metadata/ownership, ordered part slots and
source revision dependencies. Do not overload a numeric zero across APIs whose
current zero semantics differ. Schema and policy preconditions are explicit
when requested. Unsupported change kinds and unknown semantic fields fail closed.
The implementation derives canonical bytes from this same validated immutable
model and executes it. Never accept a free-standing caller fingerprint or opaque
canonical byte array alongside a different executable command.

Canonical identity excludes the operation UUID itself and includes the encoding
version. The storage key is `(account, trusted stable principal, operation UUID)`;
the principal never comes from the payload. Store and compare both SHA-256 and
the exact canonical bytes, following the archive mutation precedent. Define field
normalization and ordering in the encoder specification with golden fixtures:
member ordering may be normalized by stable member identity, while part/chunk
order remains semantic. Each supported encoder version must retain its original
byte representation; an exact retry uses that version. Do not silently re-encode
old records using new rules.

For new bytes, semantic input includes slot, declared size/digest/content type,
requested logical content and any caller-selected placement. Server-generated
attempt UUID/token, physical object UUID/key, observed provider version/ETag,
lease and verification times are execution evidence. Bind that evidence durably
to the operation, member and attempt generation, and verify it against the
command before publication. Restarting staging does not change command identity.
New uploads cannot request arbitrary existing physical keys as an adoption shortcut.

Explicit reuse is different: the caller-selected immutable object identity,
original placement, qualified provider version (when present), size, digest and
content type are semantic, together with the authorized source revision and
destination slot. Reuse still requires native retention and permission; constructing
`PhysicalObjectIdentity` grants neither. A changed selected source version is a
different command even if its bytes happen to match.

Caller-supplied timestamps, metadata and explicit expected policy/admission
versions are semantic. Server commit/observation timestamps and the policy snapshot
resolved during execution are evidence. An exact schema condition uses a full
message type and canonical descriptor-closure fingerprint; a registry subject alone
is insufficient. Preserve recorded evidence independently of future registry access.

**Durable admission before staging.** Persist command identity and allowed upload
scope before provider I/O. A repeated key must compare against that admitted
command even before there is a terminal outcome. This prevents a failed or
abandoned attempt from freeing the key for a different command. Persisted ownership
generations fence takeover, late verification and publication by old workers.
Different commands conflict; an exact pending retry resumes or observes the same
operation rather than minting another logical operation.

**One atomic outcome.** The commit transaction locks the scoped operation record
before domain locks, checks the current owner generation and command/evidence
bindings, and rechecks mutable authorization and revision preconditions. It then
publishes the revisions, native/common references, transactional outbox and one
immutable logical outcome together. The internal batch now has a transaction-scoped
entry for this coordinator; nesting calls to `save(Tx, ...)` would open a second
transaction and is prohibited. Internal native successful publication now binds
the complete result to the committed revisions in that transaction. Internal
explicit cancellation and declared revision-precondition rejection have fenced
durable decisions; schema-admission rejection and public outcome endpoints remain
unimplemented.
Operation tracking is not a shadow
document store: existing domain rows and retention tables remain authoritative.

The unsigned logical outcome records the scoped operation identity, command
encoding version/digest, terminal disposition, a server publication timestamp
assigned inside the successful transaction (not an exact wall-clock commit instant),
and a bounded per-member result with exact destination revision/domain version
and retained admission/evidence identities. It must not contain lease tokens or
provider credentials. Physical cleanup observations are separate mutable state.
Archive deletion receipt enums and signed workflow/delegation `WorkRecord` subjects
cannot represent this outcome unchanged; do not invent a workflow subject or
signature. Final Java/wire types require their own reviewed contract and fixtures.

**Replay and uncertain observations.** A committed exact retry reauthorizes the
caller and returns the stored outcome without rerunning domain mutations or outbox
effects. It must not fail merely because the old staging lease expired or because
the current document has a later revision. Authorization for the original targets
and retained revisions still applies; outcome lookup must not disclose targets
whose access was revoked. The adapter must lock and recheck mutable policy facts
in a deterministic order, including policies changing independently of document
rows, and authorize historical targets/results before returning them. The exact
policy lock integration and races remain implementation prerequisites.

A lookup with no visible row means only that no record was visible at that instant.
It does not prove rollback while another transaction is committing. A lost commit
acknowledgement remains an unknown observation until reconciliation through the
same operation fence resolves it. No cancellation check after SQL commit may
report that publication rolled back. Pre-admission malformed requests can fail
without creating an operation; known post-admission rejection or cancellation is
terminal only when recorded under the operation fence after proving no commit
won. If publication fails after writes, roll back that transaction first. In a
fresh transaction, reacquire the operation-row fence and compare current state
and owner generation before recording rejection. A competing retry may commit
or take ownership in the gap; the rejected worker must return/observe that result
instead of overwriting it. A rejected worker cannot overwrite a commit or a newer
owner's decision.
Transient provider/SQL failures and disconnected clients do not establish rejection.

Implementation acceptance must cover response loss and exact replay; conflicting
commands under one key before and after staging; concurrent same-key callers;
rollback of every member/outbox/outcome; owner expiry/takeover and late evidence;
revoked lookup/replay access and policy races; replay after later revisions;
and lookup during an uncommitted operation. Command byte size, change/part/source
counts, outcome size, SQL statements and held-lock latency require explicit budgets
before public adoption. Current document batch limits are a starting guard, not
qualification of these new operations. Consistent multi-object read snapshots
remain separate work and must retain physical lifetime protection.

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

Managed archive readers also have a local shutdown barrier. `close()` stops new
reader admission; `awaitIdle(timeout)` waits for entered reads through their pin
release attempt. Host shutdown closes admission before draining workers and
transports, then waits for the reader before releasing shared providers and SQL.
A timeout or interruption retains those resources so shutdown can be retried.
No monitor is held across SQL, provider I/O or byte verification; the read path
adds only entry/exit accounting and no database or network calls.

Real PostgreSQL and S3-adapter integration cases retain an escaped library handle,
delay its provider read, and check timeout, interruption inside the reader drain,
and successful retry. Injected SQL release failure demonstrates that local
quiescence does not mean durable pins are absent: shutdown may finish while that
failed-release pin remains. This barrier covers managed archive object reads,
not whole multi-rendition operations, writes or legacy unbound reads. It is a
prerequisite for incarnation recovery, not an automatic crash recovery mechanism.

Operation inventory for reader shutdown: Java `ArchiveObjectReader.close()` and
`awaitIdle(Duration)` are new; host shutdown is extended. Protobuf operations,
names, tags, imports, Any URLs and receipt/idempotency bindings are unchanged.
Validation: `:protomolt-repo-service:test --tests '*Archive*' --tests
'*LifecycleShutdownTest' --tests '*UploadHttpShutdownTest'` passed 110 tests on
2026-10-03; the opt-in archive benchmark was skipped. All three reader shutdown
cases passed against PostgreSQL 18 and LocalStack 3.8. Sol reviewed the production
barrier, failure injection and scope statements with no remaining blocker.

#### Reader incarnation recovery protocol

The local reader barrier is necessary but insufficient for recovery. At V30,
`ArchiveReadLedger` accepts a caller-supplied UUID without durable registration;
SQL cannot reject further admission from an identity selected for recovery.
Neither a fresh host UUID nor a successful shutdown of another host proves that
the original reader stopped. The following protocol must precede pin recovery.

1. Register one fresh, immutable incarnation before exposing its reader. Retain
   a durable state row: ACTIVE, FENCED or QUIESCED. Registration must reject an
   existing identity, including a retired one; no upsert can reactivate it.
   Existing pins migrate to an UNKNOWN owner state, preserving every native and
   mirrored reference. UNKNOWN is never eligible for automatic release.
   Stop and drain V30 readers before rollout; changing SQL admission cannot stop
   provider calls that an older process already admitted.
2. Admission takes a shared lock on the ACTIVE incarnation before the existing
   source-owner and object-retention shared locks. Keep this inside the existing
   admission SQL call. Direct pin insertion must enforce the same guard. A fence
   takes only the incarnation's exclusive lock, changes ACTIVE to FENCED, and
   commits. Once that commit is visible no new pin may be admitted for that
   incarnation. A pin admitted before the fence stays protective.
3. FENCED means admission stopped; it does not mean provider I/O stopped. For a
   local graceful stop, close reader admission, persist the fence, then observe
   that exact reader's successful drain while its borrowed resources remain
   alive. Only the owning lifecycle coordinator can turn that observation into
   QUIESCED. Timeout, interruption or an ambiguous fence commit retains resources
   and pins. Retry uses the same incarnation and observes the durable state.
4. A remote crash requires independently verified termination of the exact host
   incarnation and its outstanding provider work. A hostname, reused PID,
   heartbeat age, expired lease, disconnected SQL session, canceled Future or
   newly started replacement process is insufficient. No generic public
   `markDead(uuid)` or `recover(uuid, true)` operation is acceptable. Until a
   deployment-specific termination authority is designed and tested, remote
   crash pins remain retained. Record the evidence source and subject; an
   arbitrary evidence string is not verification.
5. QUIESCED is permanent and authorizes bounded recovery of remaining pins,
   including failed releases. Recovery selects a bounded batch without first
   locking pin tuples, then processes one object per short transaction following
   the existing source-owner -> retention -> pin order. Each transaction removes
   its native and mirrored references atomically. Concurrent normal release is
   idempotent; wrong incarnation or
   object identity is an error. Keep the incarnation tombstone after its last
   pin disappears. Do not hold an incarnation-wide exclusive lock across the
   batch or any provider call.

The lifecycle coordinator must own the entire reader admission surface for a
registered incarnation. Returning a drain result from one of several readers
sharing a ledger is insufficient. Construction must make that ownership explicit
and prevent accidental identity reuse. Registration failure must prevent exposing
the reader; ambiguous registration must not trigger a replacement UUID and leave
an untracked owner. The incarnation UUID is public identity, not a quiescence
capability. Keep local completion behind the owning lifecycle object; bind any
external authority to the exact registered host generation and retain provenance.
Processes with unrestricted SQL ownership remain inside the trusted boundary;
a state column cannot independently verify a supervisor's claim. SQL state
protects admission, while the trusted lifecycle boundary establishes local
quiescence; neither replaces the
other. Ordinary pin release remains legal after fencing and needs no exclusive
incarnation lock. Terminal-state guards reject identity changes, deletion and
backward transitions. Recovery privileges must not be available through ordinary
repository caller operations.

Keep this foundation provider neutral and free of JCR dependencies. It manages
physical read lifetimes, not JCR sessions, workspaces, version histories or
multi-object publication. A future content repository can compose lifetime
protection with its own snapshot semantics; QUIESCED supplies none of those
semantics. The initial consumer is managed archive reads. Document, raw-content
and schema readers require their own admission integration before sharing the
mechanism; their coverage must not be inferred from the archive tests.

Operation inventory: registration, fencing, attested quiescence and bounded
recovery are proposed new internal Java/SQL operations. Pin admission and host
shutdown are proposed extensions. Pin release retains its exact-identity and
lost-acknowledgement semantics. No protobuf field, import, Any URL, schema
reference, receipt binding or public idempotency key changes in this slice.
Incarnation IDs are lifecycle identities, not account authorization or receipts.

Acceptance before enabling recovery:

- Apply migration to populated V30 data and prove all old pins and references
  survive as UNKNOWN; old identities cannot silently register as ACTIVE.
- Race admission with fencing in both orders, observing actual SQL lock waits.
  Commit and rollback cases must distinguish pre-fence admission from rejected
  post-fence admission. Direct insertion must obey the same boundary.
- Reject registration reuse and backward state transitions. Prove unrelated
  incarnations and objects continue while one incarnation is fenced.
- Keep the real delayed provider read active after fencing; recovery must reject
  it. Cancellation, timeout and interruption must not produce quiescence evidence.
- Bind local drain evidence to its owning reader and incarnation. Test failed
  pin release and crash between fence and drain. A crash after drain but before
  QUIESCED commits loses the local observation: retain pins until independent
  proof is available. A committed QUIESCED transition whose acknowledgement was
  lost remains recoverable by observing the durable state on retry.
- Race bounded recovery with ordinary release and physical cleanup; include
  duplicate recovery, wrong identities, SQL rollback and a failure midway through
  multiple batches. Unprocessed pins must remain protective and discoverable.
- Preserve one client statement/transaction for admission and one for release.
  Registration and lifecycle transitions occur outside the steady read path.
  Do not add per-read heartbeats or provider I/O under database locks. Measure
  p50/p95 latency and throughput for same-object and disjoint reads at 1/4/16
  readers against V30; a shared incarnation row is still a potential hot spot.

Implement durable registration/fencing and its migration/race tests first, then
bind local quiescence and bounded recovery. External crash authority is a separate
deliverable; incomplete deployment proof must not be hidden behind a TTL policy.
Sol reviewed this protocol on 2026-10-03. External termination attestation and
recovery for unproven crashed owners remain outstanding.

V31 implements the registration and fencing portion. New `ArchiveReadLedger`
instances register a fresh identity; duplicate or migrated UNKNOWN identities
fail construction. SQL admission and direct pin insertion require an ACTIVE row
under a shared lock before object locks. The permanent FENCED state blocks new
pins but leaves ordinary release legal. No QUIESCED state or recovery deletion
API is enabled. Migration preserves existing pins as UNKNOWN, and processes from
before V31 must be drained before rollout.

`ArchiveObjectReader.close()` now stops local admission and persists the fence
outside its lifecycle monitor. Failed SQL leaves admission closed and the host's
shared resources retained for retry. A successfully acknowledged fence is cached
so repeated shutdown remains safe after the ledger closes. `awaitIdle()` rejects
an incomplete fence, including a concurrent close still waiting on SQL. These
transitions add startup/shutdown SQL, not extra client calls per object read.
The constructor change is a Java lifecycle change; wire contracts, schema
references, receipt bindings and existing release idempotency remain unchanged.
The [V31 diagnostic](../evidence/repository/2026-10-03-reader-incarnation/README.md)
records the affected tests and raw latency/throughput measurements. Client SQL
counts remain two statements/two transactions per read. Shared-host timing is
not production latency qualification; quiet paired measurements remain pending.

V32 adds local quiescence attestation and bounded recovery. The uniquely
registered `ArchiveReadLedger` instance counts acquisition from entry through
the first completed `Pin.close()` attempt, including SQL release failure.
Successful admission transfers that lifetime to the returned pin; empty or
failed admission releases the local count without erasing any durable pin left
by an ambiguous commit. Fencing closes local admission before SQL. Only that
owning ledger may attest after its acknowledged fence and zero local lifetimes.
This also covers multiple object readers sharing a ledger. `Pin.close()` retains
its trusted contract: provider work must actually have finished before calling it.

The host persists QUIESCED after its reader drain, before closing shared SQL and
provider resources. Attestation failure retains those resources for retry. SQL
records immutable LOCAL_DRAIN provenance and a timestamp; repeated attestation
observes the same terminal state. UNKNOWN and FENCED owners remain ineligible
for recovery, and SQL has no timeout-based state transition. Privileged SQL
callers are trusted: a database function enforces state sequencing, not physical
proof of remote process termination.

`ArchiveReadRecovery.recover(limit)` selects at most 1,000 eligible pins and
processes each in a separate transaction using the existing exact-identity
release and mirrored-reference guards. One failed release does not undo earlier
commits or stop other selected candidates; failures are aggregated and reported.
The host logs failed recovery with durable pins still pending, then continues
unrelated archive cleanup. Remaining pins are selected again on later passes.
This bounds returned candidates and transactions, not the cost of scanning a
large registry; production-scale query and fairness qualification remain work.

Operation inventory: owning-ledger and reader local attestation, bounded reader
recovery and its SQL helpers are new. Host shutdown/reconciliation and local
acquisition/close accounting are extended. Protobuf operations, Any URLs,
schema references and receipt bindings are unchanged. Read admission/release
still use two client statements and two short transactions per successful read;
registration, fencing and attestation occur outside that steady path. Recovery
does not delete payloads directly or authorize access to retained content.
The [V32 evidence](../evidence/repository/2026-10-03-reader-quiescence/README.md)
records 178 passing regression cases, the separate real-provider benchmark and
Sol review. Measured reads retain two client statements/two transactions;
uncontrolled host timing remains diagnostic rather than latency qualification.

V33 prevents a permanently failing first recovery page from monopolizing a
running recovery instance. A keyset cursor advances past every attempted batch,
including failed releases, using immutable `(object_id,pin_id)` order. An empty
page after the cursor causes one query from the beginning; a short nonempty page
is not filled by wrapping. A call therefore selects at most its limit, issues
at most two candidate queries and never retries a selected pin twice. The
composite index replaces the existing object-only index, preserving its lookup
prefix without increasing the steady-state index count.

The cursor is instance-local scheduling state, not cleanup authority. All
releases still check durable QUIESCED state and exact identity. Failed pins
remain retained and are retried on later traversals. A restart begins again at
the first page. This proves progress through a finite or stable eligible set
while the instance lives; it does not promise a retry deadline under continuous
arrivals, repeated restarts or competing hosts. Join/filter scan cost at large
scale remains unqualified. No protobuf or public operation contract changes.
The real PostgreSQL test
`failingFirstPageDoesNotStarveLaterPinsAndIsRetriedAfterWrap` failed against
`b78ca15a` because its second pass reselected the poisoned first pin. With V33
and the cursor, it passes: two later pins are released, the failed pin is retried
after wrap and a fresh recovery instance still observes its durable failure.
The affected suite passed 66 container and 113 service tests on 2026-10-03; the
opt-in benchmark was skipped. Sol reviewed the cursor and index replacement
with no correctness blocker. This is scheduling/concurrency evidence, not a
new throughput or latency measurement.
Operation inventory: internal recovery scheduling is extended; registration,
fencing, attestation, release identity, wire schemas and receipt bindings are
unchanged. V33 changes only the database index supporting recovery order.

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

V29 changes only archive reader acquisition/release to shared source-owner and
retention row locks, including native pin and common-reference triggers. Distinct
reader pins for one object can therefore admit and release concurrently. Logical
mutation, cleanup, and other reference owners retain exclusive locks in the same
source-before-retention order. No shared-to-exclusive upgrade is permitted inside
a reader transaction. Native/common reader rows remain private to each pin.

Retention relies on fresh statement snapshots after lock waits. The Java ledger
pool explicitly selects READ COMMITTED, and SQL retention boundaries reject other
isolation modes, including direct SQL overrides. Tests cover rejected REPEATABLE
READ pin admission, state transitions, document cleanup and archive cleanup.
Reader-reader admission/release tests fail with the prior exclusive locks and pass
with V29. Additional races observe PostgreSQL lock waits and verify cleanup sees
the committed or rolled-back release; populated V28 migration fixtures preserve
existing pins and their reclamation protection. Drain older readers before this
lock-mode migration. Protobuf operations and payload-validation rules are unchanged.

The [V29 diagnostic rerun](../evidence/repository/2026-10-03-archive-read-shared-locks/README.md)
retains the same workload and all raw results. Sixteen-reader same-object medians
were lower, but some other cases regressed and host load differed substantially.
The race tests independently prove removal of reader-reader serialization. The
benchmark does not establish a production speedup; client statement/transaction
counts remain seven/two per pinned read, and crash-pin recovery is still pending.

V30 consolidates managed archive pin admission and release into one client SQL
statement each, retaining two separate short transactions around provider I/O.
The VOLATILE PL/pgSQL admission function executes separate lock and lookup commands
under READ COMMITTED, resolves the exact original readable snapshot, inserts the
pin and returns that snapshot. Missing or retired references return no row before
any pin is inserted. Existing native/common triggers remain authoritative.
Release checks the pin, reader incarnation and object identity, preserving retry
after a committed release whose acknowledgement was lost. No SECURITY DEFINER
privilege is added. These internal calls change no protobuf contracts or caller
authorization. No transaction spans provider I/O.

`ArchiveReadCallIT` verifies exact returned identity, rollback, invalid entry,
version and object, wrong release ownership, idempotent release and an explicit
two-client-statement/two-transaction budget. Database trigger and internal function
statements are not counted as client round trips. This does not make admission
free, remove SQL work or implement crash-pin recovery.

The [V30 rerun](../evidence/repository/2026-10-03-archive-read-calls/README.md)
confirms two client statements/two transactions in every pinned scenario. Latency
was mixed, with several regressions versus the preceding run and variation in
the unsafe baseline too. Reduced client statements are verified; a production
latency improvement is not. Raw measurements and limitations are retained rather
than using statement count as a substitute for performance qualification.

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

The [revision cutover plan](repository-revision-cutover.md) records the reviewed
Java/SQL dependencies, populated-history migration, coordinated read/retention
activation and concurrency/performance gates for implementing this design.

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

Each item has its own implementation and review evidence. These eight stages are
the active goal; individual checkpoints do not establish completion of a stage.

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

### Typed publication command checkpoint

The [operation inventory](repository-operation-inventory.md#typed-document-publication-intent-and-admission)
now records the additive `DocumentPublicationIntent` and immutable
`DocumentPublicationCommand`: complete imports, runtime shape validation,
linear aggregate checks, versioned canonical bytes, original physical reuse
identity and stable drive lookup. Internal V34 admission binds that command to
account/operation scope. This does not implement upload scope, current-policy
fencing, descriptor retention, retained-part publication, terminal outcomes,
lookup/replay authorization or an RPC. Those remain prerequisites to executing
or advertising the new boundary.

Content-size and active-resource policy still needs a coordinator decision;
the 1 MiB command bound is not a bound on declared content bytes. Correctness
includes meeting the operation-count and latency gates above, not merely passing
these command fixtures. JCR sessions must compose the repository foundation;
this document-specific command is not the universal content transaction API.

### Admission batching before operation ownership integration

The existing document-attempt path now uses bounded 256-row object/source SQL
batches, with JSON preparation before the transaction. Its client statement
budget is five fixed calls plus one per batch, preserving all row-level SQL
guards and sealing in one transaction. The operation inventory records the red
statement-count regression, boundary/failure fixtures and diagnostic timings.
This is a prerequisite improvement, not owner-bound upload admission itself.

The next binding must match only new-upload slots to the admitted command's
member, operation owner generation and selected original placement. Existing
attempts require an uploaded CORE, while the reviewed intent permits retained
CORE and zero-upload revisions. Generalize that admission constraint deliberately;
never satisfy it by copying unchanged content. Attempt verification/renewal and
publication must all fence operation takeover in owner-before-attempt lock order.
A document batch's transaction participant must accept that already-held owner
fence before taking revision locks, without starting a nested transaction.

### Contextual Any resolution and optional materialization

This is a design requirement, not an available read API. The original admission
entrypoints resolve once per exact type URL through a `Function<String, ...>`.
The internal contextual entrypoint now records separate occurrence bindings;
registry integration and the read modes remain unfinished. The URL alone does
not reliably identify a schema revision. Keep definitions immutable during an
attempt, but pin them by occurrence and exact artifact identity rather than
requiring every occurrence of a URL to share one definition.

The resolution request must carry the trusted account/registry context, selected
type URL, containing root and occurrence path, and an explicit schema artifact
binding or immutable registry version. The resulting evidence binds that
occurrence to the exact descriptor artifact and its complete import closure.
Two occurrences may use different definitions of the same full type name in
separate descriptor contexts. An unversioned lookup can be pinned once for an
attempt; it cannot infer which of several definitions the producer intended.
Ambiguity is an explicit outcome, never a mutable-latest or simple-name guess.
Existing occurrence projection and URL-keyed asset maps must change together.

Before policy activation reaches public callers, close the initial-activation
race across every body-publication path. The current-policy row cannot protect
an account that has no policy yet: there is no row to lock. There is also no
guaranteed account row in this ledger, and optional drive/document rows cannot
stand in for one. Use an account-scoped transaction advisory fence: publications
take a shared lock, and first activation takes an exclusive lock. Keep its
two-integer namespace separate from the existing document advisory key space;
hash collisions may delay unrelated accounts but must never grant access or
substitute account identity. All authorization and policy lookups still compare
the exact account string.

The native publisher acquires this fence after operation ownership and before
policy, document, drive and part locks. It then checks the selected policy
revision and canonical body. Ordinary publications can share the account lock.
Activation must acquire its exclusive fence before pointer or document locks;
it does not acquire document locks. Do not introduce the reverse order by
acquiring the exclusive fence in a pointer UPDATE trigger after PostgreSQL has
already locked that row. Existing-pointer changes additionally serialize through
the pointer row lock. Exercise both race directions with actual blocked backend
observations, including another account that continues to publish.

Enforce the publication boundary in SQL as well as the early Java check.
`document_revision_publications` receives both native and legacy revisions;
guarding only `document_revision_commits` misses legacy writers. A rejected
legacy projection must roll back its document mutation. Until a path can supply
the complete selected policy and proof binding, it must reject publication under
configured policy. This also applies to an opaque-permitted policy: an opaque
decision still needs an explicit policy binding, not an unexamined legacy write.
Unmanaged legacy rows can change without a revision projection. Guard body
INSERT/UPDATE on `documents` as well; a projection-only guard is insufficient.
An existing document's account identity cannot be retagged: transfer requires a
new addressed document. Dedupe counters, lifecycle status and authorization-only
changes do not constitute a new content admission. They retain their own access
checks and do not gain a typed-admission claim. Metadata snapshots and typed
publication still need their transaction integration.

V59 implements the initial-activation fence and rejects unbound body writes under
any configured policy, including opaque-permitted policy. The native and legacy
batch writers enter before domain locks; general ledger saves and locked-reference
callbacks take the shared account lock early, allowing the SQL body guard to
distinguish content from bookkeeping. This ordering also avoids a three-party
cycle involving a native writer, a legacy writer and a queued exclusive policy
activation. Direct SQL writers must follow the same account-before-document order;
the SQL guards preserve rollback safety but cannot undo locks a caller already
took in the wrong order. No public policy administration or typed publication is
enabled by this migration. The next integration replaces the rejection for a
writer that supplies the complete policy/proof binding; it must preserve these
guards for all unbound paths.

The permitted bound-writer path needs an immutable admission row before mutating
the document, since the V59 body guard runs before a revision commit row exists.
Allocate the revision UUID first. After owner, command, account policy and
physical selection locks, insert the admission binding for that exact revision,
node, member and owner generation in the same transaction. Bind both the selected
policy revision and immutable policy digest, the canonical command identity,
the expected publication body and metadata snapshot, and the exact selected
physical objects at their full member ordinals. Do not attach a historical row
to the mutable current-policy pointer with a foreign key; compare the pointer
under the held lock and retain the immutable snapshot reference.

Freeze complete expected sets of artifact digests, schema associations (exact
type URL, descriptor, metadata and optional source identities), and root evidence
(fragment ordinal, locator and evidence digests). Counts alone do not establish
completeness: replacing a root or schema with another of the same size/count
must fail. Use bounded normalized rows or a SQL-comparable manifest and compare
exact sets at sealing. Keep descriptor and source bytes normalized in the
existing catalog; the admission manifest contains identities rather than copies.

The document guard may allow a configured-policy write only with the exact
current-transaction admission binding for its account, node, body and metadata.
The projection guard must additionally match the allocated revision, member,
operation and decision. Deferred completion must require the corresponding sealed
native revision, exact physical-part and evidence sets, and terminal operation
outcome. An unused admission row, missing member, late substitution or caught
writer failure cannot commit. The evidence writer itself marks a failed outer
transaction rollback-only. SQL checks identity and storage consistency; the
trusted Java proof constructor remains responsible for descriptor-based rules.

Support both explicit decisions already defined by the policy contract. Typed
admission requires the complete verified proof and at least one supported payload
root. Explicit opaque admission is allowed only when the decoded selected policy
and member contract permit omission; it retains no typed verdict. A failed typed
attempt never chooses the opaque branch automatically. Existing unbound writers
continue to reject configured policy. V61 and the internal Java binding helper now
implement the admission-row and seal/terminal guards, including an exact pre-change
mutation baseline and policy rechecks. The internal native committer now accepts
checked schema batches, derives typed content from proofs, and retains their
evidence before sealing. Public repository host and authorized policy
administration integration remain pending; the internal APIs do not expose that
public capability by themselves.

Expose two materialization modes, independently of admission policy:

- **Preserve:** return the exact archived fragment or its authorized claim-check
  reference, with an effective Any envelope view and its serialized payload when
  requested. Do not resolve schemas or decode inner payloads. Current parsing
  does not retain individual raw Any envelope wire slices. Reconstructing an
  envelope is not a byte-for-byte substitute for the original fragment; returning
  exact envelope slices would require separate extraction/storage work.
- **Materialize when available:** resolve the pinned definition and decode a
  bounded dynamic view when requested. A genuinely unavailable definition leaves
  the original bytes/reference intact with an explicit unresolved status.
  Distinguish missing, ambiguous, denied, unavailable, corrupt-definition,
  malformed-payload and resource-limit outcomes. Do not turn errors into missing
  definitions or mark an opaque value as validated.

`Any.value` is already serialized protobuf bytes. A `DynamicMessage` requires a
descriptor; it needs no generated Java class. Without a definition there is no
trustworthy typed field view. Expanded protobuf JSON requires descriptors for
the Any types it expands. An unresolved value may instead use a separately
documented JSON envelope containing type URL, resolution status and base64 bytes
or a claim-check reference. Do not advertise that envelope as expanded ProtoJSON.
Mixed schema versions under the same full name require occurrence-specific JSON
conversion, not one global name-indexed type registry. Treat that output as a
custom representation with explicit schema identities unless a standards-compatible
conversion is demonstrated. Bound JSON expansion and avoid embedding large
payloads by default.

Admission remains a separate decision. Opaque archival can accept unresolved
payloads when the contract permits it, subject to ownership, integrity and size
rules. Required typed validation must resolve and validate every required
occurrence before semantic review; selecting preserve mode never bypasses it.
An optional decoded view may report a failure alongside readable opaque content,
but required field operations fail explicitly. No automatic remote code loading
or Java compilation is needed merely to preserve or dynamically decode Any.

Reuse the existing `SchemaRegistryStore`, descriptor-loader and resolver-provider
boundaries through adapters where their contracts fit. Introduce the contextual
resolution seam in a dependency-light module; registry I/O and caching belong in
implementations. Existing name-only `DescriptorRegistry` lookup catches loader
exceptions and negative-caches misses. It is not an acceptable archival resolver
until failure distinctions and immutable identity are preserved.

Cache immutable descriptor artifacts and linked descriptor contexts by exact
artifact hash and selected type, including compiler/configuration identity when
compiling source. Scope discovery results by registry and authorization context.
Recheck access on use and result delivery, including shared loads that finish
after access revocation; a cached artifact is not an authorization grant. Bound
cache bytes, entries and in-flight work, deduplicate concurrent identical loads,
and propagate cancellation without cancelling other callers' shared work.
Mutable discovery and authoritative negative results need bounded freshness and
refresh/invalidation. Never negative-cache outages or denied access as absence.
Historical reads use retained exact artifacts and imports, not registry latest;
their success must not depend on a warm process cache.
If a historical revision requires an exact retained artifact and it is missing
or corrupt, report data loss rather than ordinary unknown-type status. An
authorized preserve read may still retrieve intact raw content separately;
materialization must not hide the broken retention guarantee.

Before closing this repository goal, the acceptance inventory must include:

- Every document Any location: record which roots are discovered, intentionally
  opaque, or unsupported for typed admission. CORE structured data and PARSED
  parser shapes are the current implemented discovery coverage. Add fixtures for
  other identified roots or explicit rejection under typed-required policy; do
  not silently skip them. Nested occurrences within discovered roots remain part
  of strict checking.
- Historical restore tests after registry removal, cache clearing and process
  restart, including complete imports, old schema/compiler provenance, layout
  selection, revoked access, missing/corrupt retained assets and unchanged bytes.
  Historical support is not established by design text or catalog rows alone.
- Same URL with two explicitly bound definitions in one candidate; wrong binding,
  ambiguous unversioned lookup, registry changes during an attempt and replay of
  the recorded occurrence bindings. No descriptor cross-contamination.
- Both materialization modes against real serialized payloads: known and unknown
  Any, nested unresolved Any, malformed known payload, explicit JSON envelope,
  claim-check authorization, and required validation refusing unresolved values.
- Cache tests covering concurrent load deduplication, bounded eviction, newly
  registered types after a miss, outages, recovery, tenant isolation and revoked
  access. Measure warm/cold lookup counts and latency; preserve mode must perform
  no inner-payload decode or schema lookup.

These gates refine stages 5 through 7. Production typed publication stays disabled
until candidate binding, retention and validation evidence are complete. This
section does not activate a registry resolver, cache, read mode or new public RPC.

### Historical schema read boundary

The historical reader must authorize the current document before looking up a
requested revision or any retained schema bytes. Reuse the shared advisory lock
and lean `SourceView` row lock from `DocumentRevisionLocks.lockForAdmission`,
with the existing account, READ policy, available-state and pending-purge checks.
A caller who can read one document need not own its original publication
operation. Conversely, an operation owner alone has no historical read grant.

After authorization, require the requested revision to belong to that exact node
and account, with a sealed native projection, matching native commit and terminal
operation success. The historical revision need not be current. Read schema bytes
only through its V55 artifact references, V57 associations and V56 evidence. V62
identifies the exact containing schema association; a pre-V62 unknown role cannot
be inferred from descriptor equality or association order.

Read the immutable schema rows without adding artifact row locks after document
locks. Check aggregate counts and byte sizes before copying bytes, budgeting for
JDBC and protobuf copies as well as the retained 64 MiB artifact and 16 MiB
evidence limits. Keep the complete stored command and policy internal: access to
one member does not grant disclosure of sibling members or account policy details.
Descriptor decoding runs outside database locks, with current authorization
checked again before exposing decoded data or detailed errors to the caller.

A schema snapshot alone does not establish validated historical content. Acquire
reader pins for the exact historical provider objects, verify their retained
identities and hashes, then replay the recorded occurrence bindings against those
bytes. `DocumentReadPins.acquire` still captures current revisions. V63 extends
the SQL pin protocol with explicit CURRENT and HISTORICAL scopes; existing pins
and Java inserts default to CURRENT. HISTORICAL retains the sealed revision and
DOCUMENT_HISTORY checks while omitting the current-pointer/current-reference
requirement. `DocumentReadLedger.captureHistorical` now authorizes the current
document and captures a native revision's exact archived manifest and physical
bindings, with all HISTORICAL pins acquired in the same transaction. It supports
typed and opaque native revisions; legacy revisions are explicitly refused by
this Java entry point. The capture reads no schema artifacts or provider bytes.
It acquires the active-reader lock before document locks, then locks all origin
attempts before retention rows. A failed acquisition rolls back the entire pin set.
Manifests exceeding 64 MiB are refused before JDBC copies their JSON; part counts
remain bounded at 10,000. Preserve current-source checks for publication reuse.
The Java lifecycle now lives in `DocumentReadLedger.PinnedRead<P>`; current
publication reuse retains its `PinnedPlan` handle and `PinnedPlan.Use` source
syntax. Provider batches accept the shared use type, so historical plans can
retain the same transfer, drain and retryable release semantics. This extraction
does not itself authorize delivery of historical provider bytes. `PinnedHistory`
uses this lifecycle, while host integration must reauthorize after provider I/O.
`DocumentPartReader.readHistorical` reads those original bindings into a protected
raw batch and invokes `PinnedHistory.authorizeDelivery` before returning it. The
handle binds the authenticated caller from capture; delivery cannot substitute
another caller. Runtime read failures are reauthorized before their details are
returned. Cancellation and deadline failures expose only a generic status, with
no provider details. The host still owns plan close, drain and SQL release; a
cancelled provider worker retains its batch reservation and pin use until it exits.
Pins remain until provider work drains, including cancellation. Use the
current runtime against retained definitions without consulting registry latest;
do not claim to rerun a historical compiler or executable validator.

`DocumentHistoricalSchemas.check` now implements the internal authorization,
bounded SQL snapshot and runtime replay boundary for supplied exact fragment
bytes. It reads no registry and rechecks the stored command and historical policy.
`DocumentHistoricalReader.readValidated` now composes this replay with the
protected raw provider batch. It preserves full manifest ordinals when mapping
fragments, uses the retained policy and schema closure, and returns the validated
document with its address, revision and contract/policy hashes. It has no live
registry input. Opaque revisions and missing retained assets fail explicitly in
this mode; callers can separately choose raw preservation without a validation
claim.

The typed result owns the raw batch, fragment-copy reservations and retained
schema-byte reservations until close. Before loading each BYTEA group, the ledger
reserves twice its actual serialized size for JDBC and protobuf copies. Fragment
copy/replay bytes are reserved before conversion. Reservations fail without
waiting, and partial acquisition is released on any failure. As with the existing
`PayloadBudget`, this accounts serialized payload copies, not JVM object graphs
or SDK overhead. Hosts must size heap and descriptor limits separately.
The low-level `PinnedHistory.validateFragments` method accepts caller-owned
fragment copies; those copies and any provider batch remain the caller's lifetime
responsibility. The composed reader manages both automatically.

A fresh-JVM qualification reads a runtime-defined protobuf type from real
PostgreSQL and versioned object storage. The child receives only connection
configuration and revision identity, with no writer proof or descriptor input.
It validates the archived document using retained definitions. A second fresh
JVM reports DATA_LOSS after the test removes the exact custom descriptor asset;
both runs release their read pins. This proves independence from writer-process
descriptor caches, not a complete deployment restart.

A composed-read revocation test now blocks retained-schema loading with a real
PostgreSQL table lock after provider reads finish. It queues an ACL update behind
the snapshot's document lock, then proves final authorization waits for that
update to commit. Revoked access returns generic NOT_FOUND without a result or
underlying cause, and releases payload reservations and physical read pins.
The test uses real storage and SQL coordination, without production test hooks
or blocking read-control callbacks.

Full deployment-restart qualification and public transport integration remain
outstanding; these internal-reader tests do not qualify every host delivery path.

### Host integration sequence after historical replay qualification

Status: reviewed implementation sequence, not mounted functionality. Source
inspection at `9d1f7c7f` confirms that `RepoServices` constructs
`DocumentOperations` with a backend generation but without `managedParts` or
`managedWriter`. `DocumentSchemaPolicies`, `DocumentSchemaBatch`,
`DocumentUploadCoordinator` and `DocumentPublicationCommit` remain internal
ledger components. A publication fixture is not a host entry point.

1. Add the shared native publication coordinator over these existing components.
   It must own policy selection, immutable candidate preparation, authorized
   schema resolution, checked staging, operation ownership and the complete
   proof-bound commit. Keep SQL locks out of provider and resolver I/O. Reuse
   the canonical command and `DocumentPublicationResult`; a retry returns its
   durable original revision identities after current authorization. Do not
   mount policy activation before all configured-policy write paths are bound
   or explicitly refused. Keep the V59 unbound-write rejection in place.
2. Compose managed document resources in the host. Resolve each original backend
   generation to a configured provider identity before I/O; missing generations
   fail explicitly. Share bounded payload capacity, own worker shutdown and
   quiescence-backed pin recovery, and close borrowed providers only after
   readers drain. A crashed process alone is not proof that an old incarnation
   is quiescent; recovery requires the host's ownership/fencing evidence.
   Qualify startup and shutdown, not just constructor injection. Selecting a
   non-S3 provider must not construct an S3 client. The legacy attempt writer
   alone is not a substitute for the native policy/proof coordinator.
3. Add a shared historical operation over `captureHistorical`, `readHistorical`
   and `readValidated`. The coordinator owns capture, result lifetime, drain and
   release. Use the exact address and revision UUID returned by publication;
   never select a mutable latest revision as a fallback. Capture the immutable
   historical manifest from the plan. Raw preservation returns original fragment
   bytes without a schema-validity claim; validated delivery includes revision,
   command and policy identity and reports missing retained definitions as data
   loss. Current READ authorization governs both modes, including final delivery.
4. Add reviewed historical wire contracts and a thin gRPC adapter to that shared
   operation. Keep current `GetDocument`, `GetDocumentByReference` and their
   partial-assembly response semantics unchanged. Historical mode, revision,
   manifest and validation evidence need an explicit additive result. Reuse
   `NodeAddress`, `DocumentManifest` and existing revision identity semantics.
   Choose unary bounds or streaming framing before defining fields; account for
   response copies, cancellation, backpressure and the actual serialization
   lifetime. Returning a borrowed result after closing its budget is not a
   transport-memory guarantee. No fields or RPC names are allocated by this plan.

The host must supply authenticated account and ACL bindings. The default
`DocumentGrpcService` binding carries principal/process authority only; it must
not infer account membership from request fields or grant broad authority to
make the historical path work.

Acceptance follows one real flow: publish through the shared coordinator, receive
its revision identity, read that revision locally and through an in-process gRPC
server backed by PostgreSQL and a real provider adapter, then gracefully restart
the host and repeat without a live registry. Qualify crash recovery separately
with explicit quiescence proof before releasing old pins. Run the same cases for scoped access,
cross-account denial, revocation, typed and opaque data, superseded revisions,
missing definitions, cancellation, exhausted capacity and unsupported provider
capabilities. Verify that no response escapes on failed typed admission and no
pins or workers remain after shutdown. Existing raw-reader and replay tests are
prerequisite evidence, not substitutes for this shared-path suite.

This sequence preserves the optional JCR split in
[the compatibility assessment](repository-jcr-compatibility.md). The native
document batch is one domain operation over reusable commit/reference primitives;
it must not become the universal transaction boundary. Historical document reads
do not implement JCR workspace sessions, stable identity across moves or version
restoration, and the base modules gain no JCR dependency.

### Admission resource ownership for the native coordinator

The owned fragment snapshot covers copied serialized fragments only. Before the
coordinator returns an immutable candidate, it must also own schema and evidence
resources through staging and commit. Keep the reservation interface in
`repo-admission`; the host adapts `PayloadBudget` without adding byte-provider,
SQL or transport dependencies to the admission library.

The additive budgeted preparation entry point returns a closeable `PreparedProof`.
Its reservation scope passes explicitly through preparation, independent replay
and canonical codecs. Measure before allocation. Retained evidence and generated
metadata bytes stay reserved until the proof owner closes. Path-sort buffers,
root-locator encodings and canonical re-encodings used only for comparison are
scratch allocations and release promptly after their last use. Independent
replay can temporarily hold both original and re-encoded evidence; account for
that overlap rather than releasing the original prematurely. Failure and
cancellation must release all reservations without delivering a partial proof.

The canonical encoder transfers an exclusively owned output array to ByteString
without a second copy. String-map sorting avoids encoded UTF-8 key buffers.
Every nested codec call still needs explicit accounting; a callback only at the
top-level evidence encoder is insufficient. Resolver-provided schema
bytes are borrowed during preparation and copied under reservation before retention.
Descriptor fingerprint serialization also holds a temporary lease. Never
reinterpret a schema-artifact cap as a document cap or
reserve all policy maxima up front instead of charging actual allocations.

This measures serialized bytes and temporary buffers, not all JVM heap. Parsed
descriptors, protobuf builders and object graphs retain their structural limits
and host concurrency/heap requirements. The scope must distinguish scratch from
retained leases, and tests must prove peak accounting, prompt scratch release,
capacity failure before allocation, cancellation cleanup, and complete proof
lifetime. Proof preparation and policy verification implement these reservations
as described in the operation inventory. Fragment inputs remain caller-owned
through proof use; the returned proof reference is borrowed until its owner closes.
The host coordinator must compose fragment and proof owners through staging and
commit, account for provider/SQL/transport copies, and drain consumers before
closing them. That full host integration remains required.

### Production publication boundary and retained-reader port

Keep the native publication facade in `repo-container`, where it can compose
operation admission, policy selection, placement checks, reader-pin capture,
upload preparation, candidate ownership, staging and commit without exposing
their package-private handles. Owner tokens and executable plans are not public
request data. A separate shared publication operation should use the existing
canonical command and result contracts; it must not silently change the current
`DocumentRepository.saveDocument` or current-read semantics.

`RepoServices` is the composition root for the container and engine. Inject the
new `DocumentRetainedReader` port into the publication facade. The engine's
`DocumentPartReader` implements it directly, and its closeable batch implements
the port's borrowed-payload contract. The container gains no engine dependency.
Only the pinned read overload is part of this port; an unprotected plan must not
replace a ledger-issued lifetime. The host owns reader shutdown and backend handles.

For each member, associate the returned batch positions with the filtered
`DocumentRetainedReadPlan.Entry` sequence and its full revision ordinals. Preserve
upload/EMPTY gaps and original physical identities. Keep the batch and its plan
use open until candidate capture has copied the bytes under its own reservation.
Close batches, close the plan, await actual drain, and then release SQL pins.
A drain or release failure must leave a recoverable handle with the host; neither
a cancelled future nor a timeout proves provider quiescence.

The facade must select the current policy before I/O and re-fence it at commit.
Placement comes from an authorized host selector for the exact immutable backend
generation/profile, never from unchecked request coordinates or a current-drive
fallback. Maintain operation idempotency and owner reconciliation across unknown
outcomes; do not mint another operation merely because a response was lost.

Before mounting this operation, `RepoServices` must compose the qualified backend
resolver, shared payload budget, managed reader, reader incarnation and recovery
lifecycle. Its current document operation construction does not do that. Keep
the private library flow as the qualification target first, including mixed
upload/reuse/EMPTY ordinals, revocation, policy changes, cancellation and restart;
add the thin public transport only after those checks pass.

### Publication session recovery ordering

The bounded in-process session registry retains failed and uncertain entries.
Capacity pressure and lease expiry are not permission to discard their private
owner/attempt identities. A committed outcome can release the registry entry only
after its active invocation references leave; protected reader pins and provider
cleanup remain owned by their separate lifecycles.

Before implementing host takeover, authenticate the operation principal/account,
verify the same canonical command, and observe an authorized terminal result.
Mint and retain the next owner nonce before sending takeover SQL. Use the typed
ledger takeover entry: it checks the exact command under the owner row lock, then
performs generation CAS against an expired owner. An uncertain acknowledgment
retries that same expected generation and nonce. It must not mint another nonce,
change operation identity, renew implicitly, or treat an expired retry as success.
The new owner must prepare fresh attempt identities and recheck placement and
current admission policy before provider work. Old attempts cannot be adopted by
guessing their keys or copying their verification observations.

Replacing a local registry entry requires an exclusive recovery state so another
caller cannot concurrently execute the old session or evict its replacement.
Bound recovery preparation and retain its identities across failed SQL responses.
Worker cancellation is not proof of drain: SQL generation fences prevent stale
publication, while old physical attempts and retained-read pins still need their
existing drain/cleanup ownership. Restart recovery must reconstruct command and
recorded selection evidence from durable state, without assuming the in-process
registry survived. Internal typed/opaque choices must be recovered or selected by
an explicit deterministic policy; do not add an unbound caller downgrade flag.

The internal registry implements exclusive preparation and exact retry of an
explicitly selected recovery transition. It preserves the same recovered session
after acknowledgment loss or cancellation. An explicit next-generation request
must first confirm under the database owner lock that the retained nonce became
the expected owner and that its lease expired. The observation releases its lock
before preparation and grants no replacement ownership: the later typed CAS must
still win. Failure before replacement installation preserves the prior session;
uncertainty after installation preserves the replacement. This is not automatic
restart recovery or permission to skip generations.

An explicit internal reconciliation can retire an idle local session when the
database proves its nonce is permanently superseded: the durable generation is
higher than the session's target, or equal with a different nonce. Verify the
exact command under the owner lock. Missing or lower generations and an expired
matching nonce are not sufficient. Reserve the entry exclusively during the
observation without holding the shared registry monitor across SQL. Errors and
cancellation retain the entry; positive proof releases only local count and byte
capacity, without changing the durable operation or its cleanup obligations.
This does not admit a new execution or select recovery on the caller's behalf.

Aborted/rejected retirement requires the durable terminal decision described in
the commit design. It cannot be inferred from NOT_OBSERVED, PENDING, a timeout or
an expired lease. Authorized replay of a terminal rejection or cancellation can now retire
its local session after invocation references leave. Full restart reconstruction,
schema-admission rejection and host mounting remain required.

### Rejection receipt and terminal decision implementation

`DocumentPublicationRejection` is a separate additive receipt contract. The
successful `DocumentPublicationResult` and its encoding remain unchanged. A
rejection binds the account, authenticated principal, operation UUID, exact
canonical command codec/version/digest and deciding owner generation. It includes
a database-assigned decision time in epoch microseconds, and fixed disposition
and reason enums. Explicit cancellation requires ABORTED; admission and
precondition rejection require REJECTED. There is no arbitrary error text,
document snapshot, provider identity, lease token or invented revision result.

The runtime validator checks receipt shape, enum alternatives, bounds and the
disposition/reason rule. `DocumentPublicationCommand.requireRejection` additionally
checks the trusted command/principal/generation binding and rejects unknown fields
and NUL strings. `DocumentPublicationRejectionCodec` limits stored receipts to
4096 bytes and 32 wire values, verifies the encoding version and digest, and
requires exact canonical bytes on decode. Encoding a caller-constructed receipt
is not proof that any decision was recorded. No rejection RPC is mounted.

The complete terminal-decision implementation requires these shared behaviors;
the implemented explicit-cancellation subset is recorded below:

- Add an immutable rejection table beside `repository_operation_success`, keyed
  by the same account/principal/operation. Under the operation owner fence, prohibit
  success and rejection from coexisting, including within one transaction. Reuse
  the canonical command identity and store the bounded encoded receipt. The
  deciding transaction assigns the receipt time; callers cannot choose it.
- Extend the SQL write fence, terminal owner guard, Java admission/renewal/takeover
  checks and replay to recognize either terminal outcome. Recovery cleanup may
  continue with its separate recovery fence; it must not restore write permission.
- After publication rolls back, open a fresh decision transaction. Lock the owner
  first, compare the exact command and current generation, and return an already
  committed outcome if another worker won. A newer owner fences the stale worker.
  Recheck mutable rejection conditions under domain/policy locks before recording
  them. Do not classify arbitrary exceptions as deterministic rejection.
- Implement explicit cancellation as an authorized operation decision. Transport
  cancellation, timeout, lost acknowledgment and transient SQL/provider failure
  remain unknown or pending. No failed control check silently records an abort.
- Replay exact stored receipts with current account/principal and target access
  checks. Define missing-target behavior for rejected creation separately from
  deleted or revoked existing targets. Never skip authorization merely because
  the receipt has no result revisions. An authorized terminal observation can
  retire its local session after invocation references leave; physical cleanup
  remains separately owned.

Acceptance requires real PostgreSQL publication/rejection/cancellation races,
owner takeover between rollback and decision, response loss after decision commit,
duplicate decisions, command conflicts, renewal/write refusal after termination,
current-policy replay and revocation races, immutable receipt/digest checks,
migration over existing success rows, and cleanup after either terminal outcome.
Existing successful publication and provider tests must remain green. The receipt
contract fixtures do not establish any of those SQL or host behaviors.

#### Implemented internal explicit cancellation

V64 adds immutable `repository_operation_rejection` storage and extends the
existing owner/write guards to prohibit both terminal outcomes for one operation.
The shared write fence rejects later domain writes even within the same decision
transaction. Java admission, renewal and takeover recognize rejection as terminal.
The cleanup recovery fence remains available and grants no publication authority.

`DocumentPublicationRejections.cancel` is an explicit internal host operation.
It opens a fresh transaction, locks the owner, verifies the canonical command and
returns an existing authorized terminal outcome if one already won. Otherwise it
requires the exact live owner, obtains a write fence, rechecks current access and
records EXPLICIT_CANCELLATION/ABORTED. It reads the database clock in that
transaction and uses that value for both receipt and SQL header; the trigger's
transaction-time interval check is a sanity bound, not a protobuf decoder.
There is no post-commit control check. Failed acknowledgments require exact replay;
a cancelled transport control before the decision leaves no terminal receipt.

Rejection replay verifies encoding, digest, canonical command, generation and
stored time/disposition/reason headers. Conflicting terminal rows are corruption.
Current read access covers every destination, explicit source and retained-part
source. A missing destination is allowed only for an if-absent creation intent
with process authority; deleted expected-existing targets remain unavailable.
Authorization reads lean metadata under ordered shared locks, supporting the
bounded union of 10,000 sources and 64 destinations without loading manifests or
widening write-admission limits. Success-only internal execution reports an
authorized rejection with a typed terminal signal before provider work. The
registry releases the terminal entry after active invocation references leave.

Real PostgreSQL fixtures exercise both publication/cancellation lock orders,
stale owners, exact retry, response loss, cancellation after commit, same-transaction
write refusal, cleanup-fence access, source revocation while replay waits, header
corruption, old-schema success migration and provider-free terminal replay.
Pre-V64 migration fixtures seed genuine legacy admission rows under old SQL guards;
production code does not probe for missing tables or fall back to an older schema.
This does not implement general exception classification, schema-admission
evidence rechecks, full cleanup completion or a public cancellation/outcome RPC.

#### Declared revision and creation preconditions

`rejectRevisionPreconditions` reuses the fresh owner-fenced decision transaction.
It locks the complete destination/source union and authorizes every current view
before evaluating any mismatch. Only command-declared destination revisions,
if-absent creation conditions, explicit source revisions and reused-part source
revisions are checked. Matching conditions return PENDING without a receipt or
lease renewal. Missing expected-existing or revoked objects remain unavailable;
they do not become evidence for rejection. An authorized existing destination
does contradict an if-absent condition.

A verified mismatch records REJECTED/PRECONDITION_NOT_MET using the same immutable
encoding, terminal mutual exclusion and exact replay as cancellation. This records
that a declared condition failed at the fenced decision, bound to the canonical
command and deciding owner. It does not record which condition failed or the
observed row revision, and must not be described as detailed historical evidence.
Current-policy and schema-admission failures require their own evidence and are
not covered by this decision method.

Native execution catches only a direct `DocumentLedger.RevisionConflictException`
after the candidate transaction and local resource scopes have unwound. The fresh
decision may return a competing success, an authorized terminal receipt, or PENDING.
PENDING preserves the original conflict; cancellation or lost ownership prevents
the decision. SQL/commit wrappers are propagated without searching their causes
for a reason to reject. No timeout, registry outage or generic exception is turned
into a terminal receipt.

Real SQL fixtures cover matching and stale conditions for destinations, explicit
sources, reused parts and creation; full-set authorization before a visible
conflict; missing objects; stale ownership; and successful publication winning.
Lock-order tests verify that a committed update is seen after a wait, and that a
matching observation blocks a later writer until its transaction ends. It is an
observation, not a promise that a later publication will remain valid. Execution
tests prove stale conditions are rejected before provider work, while direct
conflict signals with matching conditions, cancelled decisions and wrapped SQL
failures leave no fabricated receipt.

#### Schema-admission rejection evidence

The next terminal decision requires an explicit assessment result. Neither a
`ValidationException` nor an `IllegalArgumentException` escaping preparation is
sufficient. The successful-proof preparation path resolves members incrementally;
validation can fail before another fragment's digest or nested Any definition has
been checked. Failed preparation returns no `PreparedProof`.

The canonical publication command already binds ordered members, slots, upload
or reused-object sizes and content hashes. It does not bind the chosen admission
modes, active policy revision, contextual schema selections or evaluation time.
`DocumentPublicationFragments.capture` now owns copies and checks every size and
hash before schema resolution. Member admission checks independently verify those
hashes again. A durable rejection must also bind the selected policy and schemas.

The assessment pipeline will establish these facts in order:

1. Verify the complete candidate's member/ordinal inventory and every fragment's
   size and hash against the exact canonical command. Preserve protected retained
   inputs and owned reservations until assessment consumers have drained.
2. Freeze the explicit modes, authorized policy selection and complete required
   schema closure, including each contextual nested Any occurrence. Verify assets,
   imports, rule support and eligibility before producing a value verdict. Missing
   definitions, unsupported rules and resolver failures are incomplete checks.
3. Evaluate supported constraints using one host-selected instant. Return an
   explicit verified-invalid outcome only for completed value-rule violations.
   Keep malformed wire, unknown fields/locations, capacity exhaustion, cancellation
   and provider/SQL failures outside this first terminal classification. Subsequent
   supported rejection categories need their own verified evidence and tests.
4. Retain bounded immutable evidence before a fresh SQL decision transaction.
   Acquire the operation owner fence, then the current policy fence, then the
   complete authorized document/source lock set in the existing order. Recheck
   the exact owner, command, policy revision/digest and candidate associations.
   Changed policy or lost authority prevents a stale terminal decision. Existing
   success or terminal receipt wins replay. No schema resolution, payload download
   or validation runs while those SQL locks are held.

The evidence must identify the command and member/ordinal payload identities,
explicit modes, policy revision and digest, container and contextual occurrence
schema identities (descriptor, metadata and optional source), validation profile,
rule implementation/catalog configuration, evaluation instant and a bounded,
sanitized failure code. Raw validator messages can contain payload values and
must not be copied into public receipts. The current admission configuration uses
ProtoMolt and Protovalidate rule sources with empty taxonomy/postal catalogs; it
must not imply that an external catalog was checked. Configurable catalogs will
need immutable identities of their own.

An additive receipt binding and durable evidence storage are still required.
A digest identifies evidence but cannot reproduce a verdict after its evidence
or candidate bytes have disappeared. Retention, authorization, cleanup and replay
must distinguish the durable decision from the period during which independent
re-evaluation is supported. Reuse normalized schema assets rather than copying
complete descriptors into every receipt. Do not emit ADMISSION_REJECTED until
these bindings and their required lifetimes are implemented and tested.

The validator now offers `validate(Message, Instant)` as the first prerequisite.
CEL `now`, relative timestamp rules, nested messages and collection elements all
use that exact instant; the existing overload samples once per call. Descriptor
and compiled-rule caches remain shared, but evaluation time is invocation-local.
Operation assessments now pin one instant across their member admission calls
and frozen replay; it is not yet recorded in a durable receipt. Historical
structural decoding remains distinct from re-evaluating time-sensitive rules.

Acceptance for the remaining implementation includes a stale policy activation
race, changed owner generation, altered candidate hash, mixed nested schema
versions, unsupported rules versus genuine violations, a missing later schema
after an earlier invalid value, lost SQL acknowledgement, retained-evidence
corruption, fresh-process replay without the registry, and cleanup only after the
declared retention boundary. Evaluation-time tests must use fixed instants and
cover relative timestamp bounds and CEL without sleeping.

#### Payload assessment building block

`DocumentPayloadCheck.assessContextualAssets` now completes one payload's bounded
structural and contextual schema traversal even after a value-rule violation.
It uses the supplied evaluation instant for every decoded Any boundary. The
result is either an accepted checked payload or a separate invalid assessment
with the original envelope, complete resolved asset/occurrence inventory and
first failure identity. Invalid data never inhabits `DocumentPayloadCheck` or
its `AssetResult` success type. Existing checking methods still throw on invalid
values and return only checked payloads.

A later missing definition, unsupported rule, malformed payload, exceeded bound,
resolver failure or cancellation prevents an assessment result, even if an earlier
value was invalid. `ProtoValidator.firstViolation` evaluates all applicable rules
while retaining at most one violation; later rule compilation/evaluation errors
still propagate. Payload assessment discards diagnostic message text and bounds
each retained path/rule string to 16,384 UTF-16 code units. Those remaining
identities can still include user-controlled names or map keys and are internal,
not public receipt text. Individual diagnostic construction, decoded object heap
and rule execution retain their existing caller-budget obligations.

Assessment borrows stable byte/schema assets, copies the bounded occurrence
inventory and does not retain every decoded nested graph for a second traversal.
It does not establish command-wide fragment hash verification, complete later
root/member resolution, policy eligibility/authorization, evidence retention or a
durable rejection. The operation-level assessment must compose all roots and
members before a tentative value failure becomes verified-invalid evidence.
Tests cover distinct definitions under one URL, invalid-first/missing-later and
invalid-first/unsupported-later sequences, malformed later bytes, cancellation,
occurrence exhaustion, fixed-time verdicts and later CEL evaluation errors.

#### Owned assessment across a member's roots

`DocumentSchemaAssessment.prepare` now verifies one member's complete fragment
inventory, sizes and hashes before resolving payload schemas. It applies the
explicit evaluation instant to structural member/document validation and every
CORE/PARSED payload root. Structural failures remain incomplete assessments;
only supported payload value violations become the optional failure identity.
The loop continues through later roots after a violation, and a missing definition,
unsupported rule, corrupt bytes, exceeded bound or cancellation prevents return.
An assessment without any payload root is refused.

The result owns normalized descriptor, metadata, optional source and final root
evidence bytes through an explicit close operation. Fragment bytes remain borrowed
and must stay stable/alive until that owner is closed. Root count, decoded bytes
and encoded evidence bytes are charged across accepted and invalid roots alike.
Schema assets use the same extracted `DocumentSchemaDefinitions` implementation
as successful preparation. Failure identity includes root ordinal and locator,
contextual occurrence path and bounded rule identity, without raw message text.
Closing releases reservations and invalidates accessors; all failed preparation
paths release their acquired leases.

Standard occurrence bundles describe complete schema resolution even when values
are invalid. They are not successful admission proofs: the assessment never calls
a success-only verifier to manufacture a `Proof`. Even a result with no payload
failure is not an independently replayed admission capability. The next integration
must assess every operation member, enforce the authoritative policy/eligibility,
replay frozen evidence at the same instant, and retain the required bindings before
recording a durable rejection. The assessment itself has no database, provider,
publication or public RPC side effect.

Tests cover invalid CORE followed by missing/unsupported PARSED schemas,
same-length corruption of a later fragment before any resolver call, complete
asset ownership after resolver buffers are reclaimed, refusal at every reservation,
later-root cancellation, aggregate budgets charged for invalid roots, fixed-time
verdicts across both root kinds and rejection of a rootless typed assessment.

#### Operation-wide assessment and policy selection

`DocumentPublicationAssessment.prepare` now owns the complete fragment snapshot
and all typed member assessments under one explicit mode map, immutable policy
selection and evaluation instant. Snapshot capture verifies every private copy's
size and content digest against the canonical command before the first schema
lookup. Member checks still independently verify those identities; the additional
hash pass is deliberate until a trusted verified-snapshot interface can eliminate
it without weakening the library boundary.

The operation loop continues after a payload violation. A later typed member's
missing/disallowed schema, an opaque member's structural or ownership mismatch,
cancellation or resource failure closes all member owners before releasing the
fragment snapshot, and returns no assessment. Opaque members remain explicitly
selected and do not trigger schema resolution or receive a typed verdict.
`DocumentAdmissionPolicy.assess` binds its own policy digest, limits and structured
root requirement and applies the account/eligibility checks to every resolved
occurrence. The retained selection is not proof of current policy authorization.

Successful proof batches and invalid-value assessments now share
`DocumentSchemaUnion`: at most 4,096 roots, 64 MiB encoded root evidence, 64 distinct
schema artifacts and 64 MiB in the deduplicated artifact union. These limits apply
in addition to per-member limits and live serialized-byte reservations. Duplicate
artifact identities must have identical bytes. The operation result retains the
full member assessments and normalized artifact union; only the first payload
failure is selected as its tentative diagnostic identity.
Consumers receive non-closeable member views. The operation retains exclusive
ownership, and closing it invalidates subsequent view access. Consumers must
still drain before close; previously borrowed byte references cannot be revoked.

This completes candidate assessment across current roots and members, not durable
admission rejection. Frozen-evidence verification is described below; durable
retention/binding contracts and the fresh owner/policy/document-fenced decision
are still required.
Tests cover a bad later fragment before every resolver, invalid-first/missing-later
and invalid-first/ineligible-later members, later opaque ownership failure, explicit
opaque mode, cancellation, shared capacity refusal, private snapshot lifetime and
32 invalid descriptor variants exceeding the operation artifact count.

#### Independent replay of a member assessment

`DocumentSchemaAssessmentReplay` now reconstructs a member assessment from its
canonical occurrence evidence, retained descriptors, metadata and optional source
archives. It has no live schema resolver. Each selection binds the member ordinal,
root locator, complete occurrence prefix, exact type URL and candidate value hash
and size. Duplicate roots and selections are refused before retained asset reads.
Every recorded selection must be consumed exactly once by the reconstructed
candidate, and its canonical root evidence and complete asset/reference union
must match. Asset reads remain scoped and authenticated by the caller.

Replay verifies the policy content digest and root requirement, evaluates at the
recorded instant, and compares the complete bounded first-failure identity. It
works for accepted and invalid values without turning an invalid result into an
admission proof. It reproduces the verdict under the current validation runtime;
it does not prove execution by a historical runtime or authorize publication.
Original fragments, parsed inputs and reader buffers remain caller-owned and
bounded. Canonical scratch and reassessment assets are reserved and released on
success, cancellation, missing/corrupt evidence and reservation failure.

The admission suite passes 149 tests, including eight replay cases covering both
verdicts after the original assessment closes, changed evaluation time, erased
failure identity, missing later-root evidence, duplicate roots/references, policy
mismatch, corrupt evidence, every missing/corrupt retained asset including source
archives, read cancellation and each reservation refusal point. Operation-level
replay is described below. Durable assessment bindings and the fresh fenced
terminal rejection decision remain unfinished.

#### Operation assessment replay

`DocumentPublicationAssessment.verifySchemas` applies frozen-evidence replay to
all typed members in canonical command order. It verifies each member's command,
member content and evaluation instant, then compares the complete artifact union
and first failure with the operation assessment. Explicit opaque members remain
outside schema replay; their original content checks still apply. Verification
uses only the parent's owned bytes and policy snapshot, with no registry, SQL or
provider calls. It does not authorize publication or persist a terminal decision.

Replay runs outside the object's monitor. A busy guard refuses another replay or
close until the current invocation drains, including cancellation and failure.
Scratch reservations use the same shared `PayloadBudget` as preparation and are
released per member. Hosts must provision headroom above the retained assessment;
capacity exhaustion remains an operational failure and never an invalid-value
verdict. The original assessment remains usable for an explicit retry after
capacity pressure or cancellation clears. Verification is explicit so ordinary
preparation does not automatically pay for a second traversal.

Tests cover accepted/invalid members without repeated resolver calls, mixed
opaque/typed membership, capacity exhaustion, cancellation with scratch live,
retry, concurrent close/replay refusal, and complete release after close. The
remaining terminal-decision work must invoke verification before persisting
assessment bindings and entering the fresh owner/policy/document fence.

#### Durable rejection evidence: inventory and storage boundary

The terminal receipt stays small and immutable. `DocumentPublicationRejection`
currently binds the scoped operation, command, deciding owner generation,
database decision time, disposition and fixed reason. `DocumentPublicationRejections`
emits cancellation and declared-precondition decisions only. The existing
`ADMISSION_REJECTED` enum does not establish an implemented admission-rejection
path. V64's 4 KiB receipt bound remains appropriate for a future manifest digest,
codec and version; the complete candidate does not belong inside the receipt.

The reusable reference contract is now `RepositorySchemaAssetReference`, matching
`DocumentSchemaAdmission.Reference`: exact type URL, descriptor artifact SHA,
metadata codec/version/SHA, and optional source archive SHA. Generated and dynamic
messages enforce the identity bounds through ProtoMolt's validator. Converting
from the wire refuses unknown fields and malformed identities. Reading an asset
still has to verify its digest, metadata association, complete import closure and
scope. The contract neither creates retention nor grants access. JSON Schema
exposes the URL length and digest pattern; metadata/source correspondence remains
a runtime check against the referenced assets. No RPC is introduced.

The next manifest reuses `DocumentRootSchemaEvidence` for complete contextual
occurrences and `RepositorySchemaAsset` for descriptor/compiler provenance.
References stay normalized instead of embedding descriptor bytes per candidate.
The operation's canonical command already supplies ordered members, slots and
upload/reuse hashes. The manifest must additionally bind explicit typed/opaque
modes, selected policy revision and digest, exact evaluation instant, validation
profile and observed rule implementation/catalog configuration, all typed root
and reference identities, and the bounded first internal failure. The protected
manifest may contain user-controlled paths; the public receipt must not expose
raw validator messages or those paths. Command/member/ordinal correspondence,
completeness and policy eligibility are handler checks, not shape validation.

Storage operations are classified as follows:

- Unchanged: successful revision schema retention and historical revision reads.
  Their rows reference committed revisions and must not be fabricated for rejected
  candidates. Reuse the account-scoped schema artifact catalog and immutable
  policy bytes, but not successful revision ownership rows.
- Extended: terminal receipt encoding gains a manifest binding only after its
  codec and durable owner exist. Current cancellation/precondition receipts retain
  their meaning. No admission rejection is emitted before the new evidence gate.
- New: operation-scoped assessment manifest staging, retained candidate ownership,
  authenticated re-evaluation and explicit expiry/cleanup. These remain pending.

A durable manifest owner must protect the exact candidate bytes and schema/evidence
assets before recording rejection. Existing staging claims alone are insufficient:
replaced-generation claim release does not define final evidence retention, and
unpublished-attempt cleanup can reclaim expired candidate uploads. A new retention
pin must participate in cleanup admission under the same attempt/physical-object
locks; inventing a successful publication-history row to stop cleanup is forbidden.
Reused source objects also require exact version retention independent of a later
source revision prune. A cleanup claim that already won cannot be rescued by
attaching a late evidence pin without proving the object still exists.

The decision transaction will require the verified manifest and complete retained
associations under the owner, current policy and authorized document/source fences.
Descriptor validation and provider reads stay outside those locks. Evidence pins
must not turn a failed terminal insert into an invisible permanent leak: abandoned
staging has its own bounded recovery, while a committed decision owns retention
until its explicit replay boundary. Receipt replay remains available after that
boundary; independent re-evaluation then reports unavailable instead of treating a
digest as recoverable data. Expiry is an explicit host policy, not an implicit
fallback to attempt lease expiry. No default replay duration is selected yet.

#### Assessment manifest contract

`document_assessment.proto` now defines a protected, operation-scoped
`DocumentPublicationAssessmentManifest`. It binds the canonical command, deciding
owner generation, selected policy revision/digest, exact evaluation instant,
runtime provenance and complete explicit member modes. Typed members reference
normalized container/payload schema associations and canonical root-evidence
bytes. A recorded failure must point to a root of a typed member. Its complete
occurrence path reuses `RepositorySchemaOccurrencePath`; root payload failures
include their Any boundary, just like nested payload failures.

Evaluation time uses seconds and nonnegative nanos with protobuf's calendar
bounds. It must not be truncated to SQL microseconds. Runtime provenance requires
content hashes for all declared implementation artifacts, fixes the current rule
source order and empty taxonomy/postal catalog configuration, and records an
observed JVM identity. The host producer must inventory the actual loaded code
closure, including rule adapters, the CEL bridge/engine and protobuf runtime.
Dependency declarations or caller-supplied version labels are not observations.
Exploded classes or missing version metadata require an explicit observed build
identity; they cannot silently become an unknown implementation. No runtime
identity producer or historical executable loader is implemented by this contract.

Shape annotations enforce required identities, exclusive modes, counts, bounds,
artifact hash presence and failure/member/root correspondence. Handler obligations
remain separate: unique canonical list order, all-and-only command membership,
complete schema/occurrence inventory, exact failure reproduction, runtime closure
completeness, authorization, storage retention and current policy/owner fencing.
An absent failure can describe accepted values; an admission-rejection handler
must require a reproduced failure. Opaque membership does not assert successful
schema validation. Descriptor and source bytes remain normalized, not embedded.

Generated and dynamic-message fixtures exercise these shape checks through the
real validator, including nanosecond boundaries and mismatched failure roots.
Fixture runtime identities are explicitly synthetic and are not runtime
attestations. JSON Schema exposes count bounds and retains cross-field CEL as
`x-protomolt-cel`; OpenAPI consumers still need server-side checks for those rules
and for every state-dependent obligation. No generator change is included.

The contract does not itself provide persistence. Canonical manifest encoding
and completed-assessment conversion are described below; the observed-runtime producer,
retained candidate/schema ownership, receipt binding and the fenced durable
rejection path remain required before advertising or emitting admission rejection.

#### Canonical assessment manifest codec

`DocumentAssessmentManifestCodec` now encodes owned canonical bytes and verifies
stored bytes before returning a parsed manifest. It reuses the bounded evidence
wire implementation: at most 4 MiB, 16,384 wire values and depth 16, with raw input
bounds before parsing, exact SHA-256, unknown-field rejection and canonical
protobuf encoding. These are allocation/work ceilings, not a promise that every
combination of per-field maxima fits. In particular, the wire-value bound normally
rejects large root lists before the separate 4,096-root operation ceiling.

Canonical ordering is member ID, runtime artifact name, payload schema exact URL
then descriptor digest, and root ordinal then evidence digest. Text comparisons
use unsigned UTF-8 order. Duplicate member IDs, runtime names, schema association
keys (including the container) and root references are rejected rather than
collapsed. The deduplicated schema artifact identity set has a 64-artifact cap.
Stored set order must already be canonical; decode does not silently repair it.
Loaded root-locator uniqueness and all referenced asset bytes remain replay checks.

A typed member can have no separate payload schema references when its payload
reuses the container association. A real admission/replay test now exercises a
Document payload inside Document's Any field with exactly one retained schema
association. The contract permits this case without duplicating metadata.

Canonical output is reserved until its `Encoded` owner closes; decode comparison
scratch releases before return. Input and parsed heap remain caller-owned and
bounded. A borrowed ByteString cannot be revoked after close, so callers must
finish all consumers before releasing its owner. Tests cover canonical set
permutations, exact nanos, conflicting duplicates, nested unknown fields,
corrupt/truncated/unsupported wire input, aggregate artifact/wire limits,
reservation refusal, cancellation and closure. The codec establishes identity,
not observed runtime provenance, policy authority or durable retention.

#### Binding completed assessments to manifests

`DocumentPublicationAssessment.encodeManifest` now projects its completed member
views into the manifest, verifies frozen schema replay and returns independently
owned canonical bytes. Projection preserves the command digest, scoped operation,
owner principal/generation, policy revision/digest, exact seconds/nanos, member
modes, container/payload associations, root evidence hashes and complete first
failure occurrence. A wrong owner account or operation is refused. Principal and
generation are recorded identities; this method does not prove a live SQL fence.

One busy guard spans projection, replay and encoding. Concurrent or reentrant
close/replay/encoding cannot release the source buffers or replace the active
work. Verification finishes before allocating the final encoded buffer so its
lease does not overlap replay scratch. Both use the original shared byte budget.
The returned buffer owns a separate lease and remains decodable after the parent
assessment closes; that does not retain the referenced candidate/schema bytes.

The runtime argument is currently an internal host-supplied declaration checked
for contract shape. This method does not establish that its artifacts were
observed. A trusted observed-runtime producer and matching against the actual
loaded implementations remain prerequisites for the durable rejection path.
Tests name their runtime identities as synthetic fixtures and do not claim
attestation. Explicit opaque members retain their mode without a typed verdict.

Tests bind real accepted/invalid member assessments to decoded manifests, preserve
nanosecond time, match protected failure paths and root hashes, prove independent
output lifetime and no repeated registry resolver calls, refuse wrong owner scope,
and exercise invalid runtime shape, cancellation, capacity pressure and reentrant
close/encode with retry. No terminal receipt or retention guarantee is introduced.

#### Observing runtime origin content

`DocumentRuntimeArtifact` is an internal bounded reader for a local class's
`CodeSource` origin or an explicitly enumerated build artifact. Regular artifacts
use their exact byte SHA-256. Exploded directories use tree/v1: a domain prefix,
relative file paths in Java UTF-16 order, UTF-8 path lengths/bytes, file sizes and
file content. Empty-directory topology is not part of the digest, although all
directories count toward traversal bounds. Relative paths must round-trip losslessly. The version label explicitly
identifies a file or tree content digest; it is not an inferred release version.
No missing package manifest version becomes a guessed Gradle dependency version.

The reader limits total bytes, visited entries including empty directories, and
depth. It refuses nonlocal/missing code origins, links, special files and empty
artifact directories. File identity/size/mtime checks before and after reads and
a second tree inventory detect changed input. One 8 KiB buffer is reused across
files. Tests compile a real Java class and load it from both a JAR and exploded
classes, then exercise resource changes, renamed files, links, bounds, missing
origins, cancellation and mutation between observation and read.

This observes filesystem content at the classloader-attributed origin, not JVM loaded-byte
attestation. Hosts must keep code immutable while an identity is used; an agent,
custom classloader or later file replacement can invalidate a stronger claim.
Separate classes/resources directories and every transitive dependency require
explicit inventory entries. A shaded JAR identifies a bundle unless packaging
supplies verified component provenance. This reader neither discovers that
closure nor proves that a declared list is complete.

The next runtime producer must generate a versioned production dependency
inventory from resolved build artifacts, compare actual loaded anchor origins
against it, include both fixed rule sources, the CEL bridge/engine, formats and
protobuf dependencies, and record observed JVM vendor/build identity. Existing
runtime dependency gates only reject unwanted modules and do not supply such a
content inventory. Runtime provenance is not yet sufficient to enable terminal
admission rejection; durable candidate/evidence retention and decision fencing
remain required as well.

#### Production admission artifact inventory

Run `./gradlew :protomolt-repo-admission:admissionRuntimeInventory` to produce
`repo/admission/build/admission-runtime/inventory.tsv` and its `artifacts/`
directory. The task includes the admission JAR and every resolved production
runtime artifact, excluding test dependencies. The admission runtime dependency
gate runs first. This is build evidence; it does not establish which classes a
running host loaded or prove that its validation implementation matches this set.

The UTF-8 TSV starts with `protomolt-admission-runtime-inventory/v1`. Each following
line has four tab-separated fields: resolved component identity plus artifact
filename, lowercase SHA-256, decimal byte length, and `artifacts/<sha256>.jar`.
Lines are sorted by Java string order of logical identity and end with LF.
Ambiguous identities and names longer than 200 characters or containing ISO
control characters are refused. Artifact filenames distinguish ordinary classifier
artifacts; unresolved identity collisions fail rather than collapse entries.

The task permits at most 64 regular JAR files and 1 GiB aggregate bytes. It uses
one 8 KiB copy buffer, checks source identity/size/mtime around each copy, hashes
the copied bytes, rehashes the temporary output and opens it as a ZIP before
publication. Existing content-addressed blobs must match their expected hash
and size; corruption fails rather than being overwritten. These checks assume
an immutable build input during observation, not an adversarial filesystem.

The manifest is replaced atomically only after every referenced blob is ready.
A failed run leaves the prior manifest in place; consumers must require a
successful inventory task for the current build, not infer success from file
existence. Unreferenced blobs from older or interrupted builds are not selected
by the manifest and are not automatically deleted. Gradle up-to-date skipping is
disabled so every invocation observes the actual files again.

Qualification checked all 38 resolved artifact hashes and lengths, identical
manifests across repeated runs, refusal of a deliberately corrupted output blob,
preservation of the prior manifest on failure, and successful rerun after exact
restoration.

`DocumentRuntimeInventory` reads this local build format with a 128 KiB manifest
bound, strict UTF-8 decoding, exact header/columns/hash paths, canonical decimal
sizes and sorted unique names. It preflights all rows and the 1 GiB aggregate
bound before opening artifacts. Distinct logical identities may share a blob;
each row still counts toward the aggregate bound. Each blob must have the exact
size and SHA-256 and open as a ZIP. Bundle and artifact directories, manifest and
blob paths reject final-component symlinks. The local directory tree must remain
immutable during use; these checks do not protect against concurrent replacement
of ancestor directories by an adversary.

The resulting immutable identity snapshot can check a caller-selected list of
loaded class anchors against their attributed JAR origins. Empty lists, repeated
classes, absent artifact names, missing origins and mismatched origin content are
refused. No classes are loaded from names or code supplied by the inventory.
Tests compile and load real fixture classes in isolated classloaders, change the
origin independently of a valid bundle, and exercise malformed inventories,
non-archive content, corruption, missing files, links and cancellation.

A self-consistent inventory is not trusted merely because its bytes verify.
It requires trusted build provenance. The reader does not enforce the production
anchor set, discover all classloader dependencies, or produce a completed runtime
identity for admission. Fixed production anchors, actual deployment classloader
composition and observed JVM/configuration identity remain to be wired and
qualified before terminal admission rejection is enabled.

The container's `admissionRuntimeTest` task qualifies the generated production
bundle and is included in `check`. It compiles a test-only probe against only the
inventory JARs with annotation processing disabled. A fresh URL classloader uses
the platform loader as parent, so neither ordinary test dependencies nor host
application classes can fill gaps. The thread context loader is also switched
for the invocation and restored afterward.

The probe invokes the exact `DocumentSchemaAdmission.VALIDATOR`, checks accepted
and rejected native annotations and Buf-compatible rules, and checks the specific
constant, string length and message CEL failures. The harness verifies attributed
origins for admission, validator, both rule sources, CEL bridge/compiler/runtime,
formats and protobuf classes. A second case runs the same compiled probe with a
complete bundle, removes only the rule-adapter JAR from a fresh loader, and requires
the missing-class failure even though that adapter exists on the test classpath.
The probe itself is a test fixture and is not part of the production inventory.
This proves that these validation paths run with the bundled dependencies and
that a missing adapter cannot use the surrounding test classpath. It does not
claim all code paths were exercised or that a deployed host uses this loader.

#### Standard classpath qualification

The runtime evidence path will support trusted, immutable, ordinary JAR classpath
deployments. It will verify the actual loader topology and observed JVM identity;
it will not infer that an arbitrary classloader, shaded executable, module-path
launch or caller-supplied version list has the same semantics. This evidence is
not a sandbox or an attestation against agents/instrumentation inside the host.
Unsupported layouts must fail explicitly at the runtime evidence boundary.

`DocumentRuntimeClasspath` now qualifies an explicitly enumerated JAR list.
Every distinct inventory content hash must match exactly one classpath artifact.
Additional host JARs are allowed, but cannot supply classes also present in the
inventory. Inventory JARs are scanned first so duplicate class providers cannot
hide through classpath order. Base and multi-release variants inside one JAR are
allowed; versioned classes in different JARs are conservatively treated as
collisions even for releases inactive on the current JVM. Module descriptors are
excluded from class collision checks.

The scanner permits at most 256 regular JARs and 1 GiB aggregate bytes, 250,000 ZIP
entries, 16 Mi UTF-16 characters of entry names and 4,096 characters per name.
Manifest decompression is bounded to 64 KiB. Manifest `Class-Path`, ambiguous
manifest entries, duplicate ZIP names, links and directories are refused.
Captured file size/mtime/identity are checked before and after ZIP observation,
including a tested change between hashing and scanning. These are drift checks
under an immutable-host assumption, not atomic filesystem snapshots. JDK ZIP
opening processes central-directory data before the entry counter can run; the
entry bound limits scanner iteration and maps, not every JDK allocation for a
hostile archive. Only trusted local build artifacts are accepted in this design,
not uploaded executable libraries.

The real 38-artifact production bundle passes this qualification. Tests also cover
missing artifacts, repeated paths/content, unrelated host classes, shadowing in
either order, multi-release collisions, hidden manifest dependencies, oversized
manifests, links, cancellation and file drift. The method currently accepts paths
and returns observed hash/path associations; it does not assert they came from
the effective classloader. The observer below supplies the restricted topology
and loaded-origin checks; binding its observation to assessment use remains
required before replacing the internal raw runtime declaration.

#### Host runtime observation

`DocumentAssessmentRuntimeObserver` produces an internally constructed observation
for a standard OpenJDK application classloader over immutable local JARs. It reads
the trusted launch `java.class.path` declaration, qualifies every inventory
artifact and checks the loaded origins of the fixed admission, validator, rule
source, CEL, formats, descriptors, repository codec/protobuf and protobuf runtime
anchors. Each anchor must use that application loader in an unnamed module.
Custom loaders, nonmatching context loaders, module launches, exploded classpath
entries and recognized startup instrumentation options are unsupported.

The observer reads the validation profile and catalog configuration by invoking
`DocumentSchemaAdmission.runtimeProfile()` on the loaded implementation. This
avoids relying on a consumer's inlined public string constant. The admission
validator explicitly constructs the ordered native and Buf-compatible sources
with empty taxonomy and postal catalogs. The observer accepts only the qualified
v1 profile and configuration. JVM identity records observed VM vendor/name,
`Runtime.version()` and VM build; it does not invent a JVM artifact digest.

Artifact hashing and archive scans happen when observing the runtime, outside SQL
locks and request latency paths. The observation's identity accessor rechecks
loader/context, classpath declaration and JVM identity. It does not rehash JARs
per request. The host must keep code/resources immutable and prevent untracked
instrumentation. Startup-option rejection does not establish absence of dynamic
agents. The mutable `java.class.path` property is a trusted launch declaration,
not access to the loader's internal URL state; loaded anchor origins cross-check
it but cannot attest every dynamically loaded byte.

An additional qualification test launches a fresh ordinary JVM with the 38
production admission JARs, the container JAR and a test probe JAR. It observes
runtime identity, runs the same real valid/invalid validation probe, refuses
changed thread context and classpath declarations, propagates cancellation and
allows retry after restoring context. No test framework is on that process's
classpath. The test disables dynamic attach/agent loading for its process; this
is test setup, not a claim about deployed hosts.

Host startup has not yet been wired to obtain this observation. Candidate/evidence
retention and durable rejection fencing remain outstanding. No terminal admission
rejection is enabled by this step.

#### Encoding with observed runtime identity

`DocumentPublicationAssessment.encodeManifest` now requires an observation and
returns a privately constructed `ObservedManifest`. Its busy guard spans the
initial context check, projection, independent replay, encoding and final context
check. If the final check fails, it closes the encoded output lease before freeing
the busy state. No provider or SQL work occurs in this method. The observation's
context check runs after the caller's control callback, with no callback after
comparison that could invalidate the check before returning.

The wrapper owns only the encoded buffer. Its bytes, digest and explicit check
accessors require an open owner and a matching live runtime context. Closing
always releases its reservation, even when the runtime context is no longer
supported. The original assessment may close independently. Borrowed bytes are
not self-authenticating and must not outlive the wrapper. A future durable handler
must accept this live wrapper, recheck it at consumption, retain candidate/schema
assets separately, and fence current authorization, policy and owner generation.

The lower-level `encodeDeclaredManifest` remains an internal codec/projection
path for synthetic provenance fixtures. It cannot construct an observed wrapper
and must never become an alternative durable-decision input. Its package-private
visibility is not a security boundary; future terminal-handler review must reject
generic raw-byte/declared-runtime overloads that bypass the observed path.

The fresh-JVM probe now includes repository SPI and byte SPI JARs and exercises a
real typed rejection together with an explicit opaque member. It verifies the
observed runtime, exact failure root/occurrence and evaluation instant; replay
does not call the registry resolver again. It changes the classpath declaration
after encoding while the output reservation is live and verifies rejection,
cleanup and retry. It also covers cancellation, reentrant encode/close refusal,
independent output lifetime, access refusal under a changed context loader, and
closing the output under that unsupported context without leaking its lease.

#### Assessment retention foundation: reviewed implementation direction

The next storage increment reuses the physical retention bridge rather than
introducing an independent attempt-only retention mechanism. V26 introduced
`repository_object_retention`, `repository_object_references` and exact native
owner verification. V27 separates retirement from reclamation; V29 requires
READ COMMITTED and source-owner locks before physical retention locks. V44 and
V63 show how reader ownership extends that foundation. These mechanisms already
cover archive and document physical locations independently of provider vocabulary.

The new native assessment owner will bind account, principal, operation,
generation, command, observed manifest and the exact selected attempts/source
versions. Its object associations will mirror an assessment owner kind into
`repository_object_references`. Repeated use of one physical object deduplicates
the physical reference while retaining every command-slot association. The
owner's internal reference generation must be distinguished from a document
revision, provider version or JCR version. No successful revision/history row is
created for a rejected candidate.

V66 implements the internal staging owner and physical bindings described below;
terminal ownership and a public API remain unfinished. The migration preserves existing owners/references and extends the
native-owner guard and reference-kind constraint together. Direct reference
insertion without an exact native assessment association must fail.

Acquisition requires the live operation write fence, exact current selection and
complete verified physical bindings. For new uploads, the attempt must be the
selected live VERIFIED `NEW_CONTENT` attempt in that operation generation.
Any cleanup tombstone permanently disqualifies it, including expired claims and
ABSENT observations. The physical retention rows must exist and remain open;
neither a SQL pin nor a digest proves that provider bytes exist. Candidate reads,
descriptor checks and observed-manifest replay occur before the final SQL fences.
No provider I/O, hashing or schema validation belongs inside the lock sequence.

Attempt cleanup needs an explicit admission check as well as the shared physical
reference guard. `DocumentAttemptCleanupLedger.claim` locks the attempt before
rechecking eligibility. Its BEFORE trigger must reject a new cleanup claim while
any of that attempt's physical objects has an assessment reference. The candidate
scan gets the same predicate only as an optimization. This is essential because
the existing cleanup tombstone and AFTER trigger irreversibly retire/reclaim the
attempt's objects. A claim that won first cannot be repaired by a late pin.

All native owner/object associations and reference mirrors are acquired atomically
for the bounded operation, not committed one document at a time. Preserve the
existing order: operation owner fence, complete document/source authorization
fences, deterministic source-owner locks, then deterministic physical-retention
locks. Recheck policy, selection and database time after waits. The implementation
must derive one ordering compatible with existing readers, cleanup and publication;
it must not add retention-row-to-attempt lock inversion. Unrelated objects must
remain independently writable and readable.

Within that outer fence order, acquire the assessment owner row before all
distinct physical source-owner rows in canonical source-kind/UUID order, then
physical retention rows in object UUID order, then reference/read-pin rows.
Prelock the complete source set before bulk mirror insertion: per-row reference
triggers alone do not establish a global order across mixed sources. Acquisition,
promotion, replay admission and reaping must agree on this sequence. Attempt
cleanup consults reference existence under its attempt lock and must not then
lock an assessment owner, which would introduce the reverse edge.

Expiry is eligibility for explicit recovery, not automatic disappearance of a
native reference. A clock comparison inside `repository_native_reference_exists`
must not silently invalidate ownership while mirrored rows or active readers
still exist. Recovery changes/removes the native association and its physical
reference in one transaction under the same owner/physical locks. Staging needs
an explicit bounded recovery deadline; terminal evidence needs the explicit host
replay deadline. No default replay period is selected. Transition from staging
to terminal ownership must never drop the last protecting reference in between.
A failed terminal insert must leave either recoverable staging or a full rollback.
Promote the same native owner and reference set in the decision transaction;
do not delete and reacquire references under a new owner kind. An original source
may have entered retirement after staging, legitimately refusing new references
while an existing assessment reference still protects the bytes. Promotion must
preserve that protection without reopening the retired source.

Reused source objects reveal a separate read gap: an original source can become
`retiring` while assessment references still prevent physical reclamation. Future
assessment replay therefore needs a distinct authorization and native-owner check
for its exact retained object, not a current-source or successful-history lookup.
It must not clear the retirement flag or grant new unrelated owners access. A
scoped read pin must protect already-authorized workers through drain/recovery
before assessment release permits reclamation. Revocation still controls new
reads; physical retention is never permission to disclose data.

Schema retention remains normalized in the existing account-scoped artifact
catalog. Assessment ownership must retain the exact descriptor, metadata, optional
source and root-evidence associations independently of replaced-generation staging
claims. The immutable policy snapshot is reused. The protected manifest's digest
is a binding, not storage ownership; the receipt may survive after evidence expiry,
but independent re-evaluation must then report evidence unavailable.

Implementation and acceptance order:

1. Native assessment owner, exact physical associations and reference mirrors:
   real PostgreSQL tests reject wrong account/generation/selection, incomplete
   physical bindings, raw reference forgery and preexisting cleanup/retirement.
   Migrate existing rows without inventing owners or reopening retired objects.
2. Cleanup coordination: race acquisition against claim commit and rollback in
   both orders; prove stale scans cannot bypass the SQL guard. Verify missing or
   expired cleanup leases never permit attaching evidence to a claimed attempt.
   Check lock waits do not serialize unrelated objects.
3. Recovery and transfer: rollback partial acquisition, lose acknowledgements,
   retry idempotently, replace the operation owner, expire abandoned staging and
   transfer to terminal ownership without a retention gap. Release native and
   mirrored ownership atomically; do not release active read workers by elapsed
   time alone. Retire an original source between staging and terminal promotion
   and prove the existing references survive without reacquisition.
4. Schema/evidence ownership and replay: retain exact normalized assets, prune or
   retire original sources, remove the registry, then reproduce the retained
   candidate from a fresh process. Test revocation, corrupt bytes and explicit
   replay expiry separately from receipt replay.
5. Terminal rejection: accept only the live observed-manifest wrapper, require a
   reproduced value violation and complete retained associations, then fence the
   owner, policy and authorized target/source set in one decision transaction.
   Provider failure, unsupported rules or missing evidence remain operational
   failures. Only after these gates pass may ADMISSION_REJECTED be emitted.

The reusable foundation here is atomic multi-object retention ownership, stable
physical identity and explicit acquisition/release. An optional content-repository
extension can build on it for JCR semantics. It does not establish JCR sessions,
workspaces, node identity across moves, node/property types, reference integrity
or version restoration. Existing account/workspace/version fields remain distinct
from those capabilities, and base storage gains no JCR dependencies.

#### Assessment retention lock set and association review

V65 adds `lock_repository_retention_set(uuid[])`, an internal exclusive lock
primitive for acquisition and recovery. It bounds the input at 10,000 identities,
deduplicates objects, checks complete physical/source/retention rows, and requires
READ COMMITTED. It locks archive sources in PostgreSQL UUID order, then document
attempts in that order, then physical retention rows in object UUID order. Empty
sets are supported by this generic primitive. Invalid or missing
identities fail; nothing is silently omitted. The function changes no ownership
or retirement state and intentionally permits already-retired objects so recovery
can release existing ownership. Acquisition must separately reject those states.
The returned count proves neither authorization nor selected-candidate validity.

Call it once for the complete set after the operation, policy, authorization and
native assessment-owner fences. It is not a shared-reader admission path. In
particular, do not call it for a mixed source set after
`DocumentPublicationLocks.lockIndependentOrigins`: that method already holds
document attempt locks and would violate archive-before-document ordering. All
logical document/source/current-pointer locks must precede physical source locks.
No provider I/O or schema checking belongs within these locks.

The current `PublicationReuse` contract names document revision conditions;
`DocumentReuseAdmission` requires a retained current `DOCUMENT_PART` source.
It cannot express archive reuse. The generic physical lock primitive supports
both source families without extending that command or claiming archive reuse.

The next native assessment tables must use a globally unique assessment UUID as
the mirror owner identity. An operation UUID alone is insufficient because the
operation namespace also includes account and principal, while physical mirrors
do not. Keep one native retained-object row per `(assessment_id, object_id)` and
separate sealed bindings for each PRESENT `(member_id, revision_ordinal)`.
Several candidate parts may legitimately reuse the same physical object.
NEW_CONTENT bindings must validate the exact selected attempt and selection
revision, account/principal/operation/generation/member, and full command
`revision_ordinal`; the upload's dense `ordinal` is a different coordinate.
Reused bindings must freeze the validated source revision, source slot and exact
object identity so later replay does not depend on a current pointer.

The helper is implemented. V66 adds the staging ownership described next;
terminal promotion and replay remain unfinished. No rejected-candidate retention
API or ADMISSION_REJECTED emitter is enabled by V65.

#### Assessment staging ownership and recovery

V66 introduces internal `document_assessment_owners`,
`document_assessment_slots` and `document_assessment_objects` tables. The owner
binds the admitted document-command codec/version/digest, scoped operation and
generation, manifest bytes/digest and explicit staging deadline. One staging
owner is allowed per operation generation. Its manifest is bounded at 4 MiB,
its declared PRESENT slot count at 10,000 and its initial retention deadline at
one day from insertion. There is no default deadline. These are staging limits,
not a terminal evidence retention policy.

The caller creates the owner and slots, then seals in the same transaction.
Sealing locks the complete distinct physical set with V65, rechecks the live
operation fence and deadline, and verifies exact selected NEW_CONTENT full-slot
ordinals or retained current document-source revision/ordinal/object bindings.
It also checks the reverse direction: every selected member and every selected
uploaded object must appear, so lowering the declared slot count cannot hide an
upload. A reuse-only member has a real zero-upload selection and no synthetic
attempt. Distinct native object rows mirror one `ASSESSMENT` reference per
physical object, with internal reference generation `1`.

A deferred constraint refuses an unsealed owner or incomplete association set
at commit. A sealed owner cannot gain new slots in a later transaction. Native
objects and mirrored references cannot be deleted while their owner remains
active. The SQL boundary proves storage associations, not canonical protobuf
meaning: the future trusted handler must verify the observed manifest against
the complete canonical command, including every declaration and byte identity,
and perform policy, source, target and authenticated-caller checks under the
complete logical lock set before staging. A raw SQL manifest is not runtime
validation evidence. V67 adds normalized artifact ownership below; exact
manifest-to-artifact equality remains a handler obligation.

Attempt cleanup now excludes assessment references in both its advisory candidate
scan and its eligibility check after locking the attempt. A SQL BEFORE trigger
also refuses direct cleanup claims under that same attempt lock. Acquisition
that commits first therefore protects the bytes; cleanup that commits first
leaves a permanent tombstone that later assessment acquisition cannot adopt.

`release_expired_document_assessment` is an internal recovery operation. It takes
the operation recovery fence, assessment-owner lock and full source/physical
lock set, then removes slots, native objects, mirrors and owner atomically.
Expiry alone changes none of those references. A partial release cannot commit,
and retry after a completed release reports that the owner is absent. Generation
replacement alone does not authorize release. No assessment reader admission or
terminal transfer is enabled; both must extend the release gate before use, with
reader drain and terminal retention deadlines respectively.

Operation inventory for this increment: physical batch locking and staging/seal/
expired-stage release are new internal operations; attempt cleanup and native
reference verification are extended; protobuf contracts, successful publication,
normal reads and external transports are unchanged. Terminal promotion,
independent replay, schema/evidence pruning and ADMISSION_REJECTED emission are
still unfinished. PostgreSQL lifecycle fixtures deliberately use synthetic SQL
byte declarations; they do not claim provider I/O or canonical handler admission.

`DocumentAssessmentRetentionIT` covers sparse upload ordinals, omitted selected
uploads despite a lowered count, stale generation/selection, incomplete seals,
retirement and cleanup tombstones, reference forgery, immutable associations,
expiry without automatic release, partial recovery rollback, and cleanup waiting
for acquisition commit or rollback. `DocumentAssessmentReuseIT` covers repeated
physical objects under a zero-upload selection and mismatched source ordinals.
The affected PostgreSQL run passed 66 cases including existing retention,
retirement, cleanup, lock-set and operation-bound attempt regressions. Additional
account/source-revocation cases and large-batch
latency qualification remain to be added before the complete assessment path is
considered finished.

#### Normalized assessment artifact ownership

V67 adds `document_assessment_artifacts`, which references the existing
account-scoped `repository_schema_artifacts` catalog. An assessment stores digest
references, not a private copy of every descriptor, metadata record or source
file. Root evidence is a separate set, retained by V68 rather than V67. The owner
declares a schema artifact count from zero through 64;
zero preserves existing physical-only staging. The final association set must
match that count and total no more than 64 MiB. Exact correspondence to the
canonical manifest, including asset roles and complete import/root coverage,
remains a mandatory handler check before a terminal decision can use the owner.

Association insertion follows physical sealing in the same creation transaction.
It requires the live operation fence, matching owner account and an exact
current-generation staging claim for each catalog digest. An older claim or
another operation's knowledge of a digest cannot substitute. Physical locks
precede catalog and claim locks; the caller inserts associations in digest order.
The association has no foreign key to the temporary claim. Releasing a replaced
generation's staging claims therefore cannot release an assessment's schema bytes.

Expired recovery now removes artifact references together with physical references
and the owner. It retains catalog bytes and other assessments' references.
Catalog pruning is still disabled; its future protocol must inspect assessment
references as well as staging and revision references. Retention is not authority
to read the schema or disclose document contents.

The operation inventory extends staging and expired-stage release with normalized
artifact ownership; it adds no transport or protobuf operation.
`DocumentAssessmentArtifactsIT` exercises shared catalog bytes, independent
assessment lifetimes, exact generation/operation/account claims, missing-artifact
rollback, insertion phase restrictions and partial release refusal. The added
`DocumentAssessmentRetentionIT` recovery races observe actual PostgreSQL waits:
cleanup proceeds after release commits and remains refused after release rolls
back. These fixtures prove storage lifecycle behavior, not descriptor validity,
registry-offline semantic replay or provider-byte availability.

The affected run passed 104 PostgreSQL cases across assessment, schema catalog,
revision-artifact, physical-retention and cleanup tests. The final seven-case
artifact run also passed after tightening the partial-release assertion and
adding a populated V66-to-V67 migration case. That case preserves the existing
owner fields and physical references, defaults artifact ownership to zero
without inventing associations, and exercises release after migration.

#### Scoped assessment evidence for the admission writer

`DocumentPublicationAssessment.withRetentionEvidence` keeps the assessment and
its reservations alive while a consumer borrows the observed manifest, schema
artifacts and root evidence. Independent replay and runtime observation precede
the callback. The scope checks exact command, owner, policy, evaluation time,
first failure, member modes, schema roles, artifact digests and root identities
against the canonical observed manifest. Root bindings also carry the full part
ordinal, locator digest and candidate fragment digest and size. Collection
structures are copied; retained payload bytes are borrowed without duplication.

The parent cannot close or start another verification during the callback.
Cancellation, callback failure and runtime-context changes release the encoded
manifest reservation and close the borrowed scope. Context is checked again after
the callback. Returned values must not escape the scope; holding a ByteString is
not independent admission proof or authorization.

This is an internal Java operation, not a SQL staging writer or public API.
The future writer must recheck operation, policy, authorization and selection
fences within its decision transaction before committing. A post-callback check
cannot replace those fences or undo a committed transaction.

Root evidence requires separate storage limits: at most 64 schema artifacts
totaling 64 MiB, and at most
4,096 root evidence entries totaling 64 MiB. Root evidence must not be forced
into the schema artifact count limit. V68 below adds durable part bindings and
atomic acquisition/release. The admission writer still needs exact manifest
correspondence and authorized replay alongside V67's normalized schema artifacts.
Terminal promotion, replay reader admission and
ADMISSION_REJECTED emission remain disabled pending that complete path.

The scoped-evidence change passed 25 assessment/runtime unit tests and three
production-bundle runtime tests. The fresh-JVM probe checks borrowed evidence,
reservation lifetime, parent-close refusal, callback cancellation, runtime drift,
reentrant scope closure and mismatched owner generation. These checks do not
establish durable root storage or SQL admission behavior.

#### Durable assessment root evidence

V68 adds `document_assessment_roots` with an exact foreign key to the assessment's
member and full part ordinal. Each immutable entry records the locator digest,
fragment digest and size, and encoded evidence with its codec, version and digest.
Insertion requires a sealed assessment in its creation transaction under the live
operation fence. The fragment identity must match the slot's verified physical
object. A zero-byte fragment is allowed; the evidence encoding itself must be
nonempty. Storage can bind any retained candidate slot, while supported root
discovery and semantic interpretation remain admission-handler responsibilities.

The owner declares an immutable root count from zero through 4,096. A deferred
check requires the exact count and at most 64 MiB of evidence; each encoding is
limited to 4 MiB. Budget aggregation reads stored lengths after assembly rather
than rescanning all prior evidence for every insertion. Trusted callers must
enforce their input and reservation bounds before assembly. These SQL limits do
not replace the admission engine's stricter per-member limits or canonical checks.

Root insertion uses source and retention locks already acquired by physical
sealing. Recovery removes root evidence before candidate slots in the same
transaction as schema and physical reference release. Expiry alone removes
nothing. Direct deletion without recovery, partial recovery and late insertion
are refused. Existing assessments migrate with zero declared roots; no semantic
evidence is synthesized for them.

This extends internal assessment acquisition and expired recovery. Public
protobuf/gRPC contracts, normal reads and successful revision behavior are
unchanged. The SQL fixtures use synthetic evidence bytes and prove storage
lifecycle only. They do not prove canonical admission, provider availability,
registry-offline replay or terminal decision safety. The next integration work
is the trusted writer connecting scoped Java evidence to these durable bindings.

The affected PostgreSQL run passed 35 cases: nine root-evidence cases, seven
schema-artifact cases, 17 physical-retention cases and two reuse cases. Root
coverage includes exact sparse ordinals and fragment identities, invalid codecs
and checksums, zero-byte fragments, incomplete/excess counts, independent schema
and root budgets, oversized evidence, partial recovery rollback, and a populated
V67-to-V68 migration that preserves existing schema and physical ownership.

#### Admission writer integration and retry boundaries

The writer must reuse `DocumentCommitParts` selection, reuse and physical identity
checks. Its assessment binding path takes V65's complete lock set for proposed
objects only; a stage does not replace current pointers or remove old destination
references. The successful-publication path continues to lock both old and new
objects. Both paths check exact selected attempts, current source projections,
provider identity, verified bytes and cleanup state after acquiring physical locks.

For a new stage, the writer acquires the operation fence, current schema policy,
complete logical destination/source authorization and drive locks, then inserts
the assessment owner before binding physical objects. Candidate slot insertion
and sealing follow. Schema and root evidence are attached in that same transaction,
and the scoped observed evidence is checked before commit. Provider reads,
registry resolution and semantic replay precede this transaction.

Replacing an upload attempt requires rebuilding the prepared plan as well as the
selected-attempt map. The plan includes attempt-derived object keys. Supplying
the previous plan after replacement must fail rather than silently retargeting
its physical claims. A REUSE association records the locked source revision UUID
and full source ordinal, not a revision guessed from the physical object: the
same object may occur in multiple revisions.

An ambiguous staging acknowledgement requires reconciliation under the operation
fence. V66 permits one assessment owner per operation generation. The writer must
compare the persisted canonical manifest and complete slot, schema and root sets
before returning an existing stage. Divergent evidence conflicts. It must not
delete and recreate an existing owner or reacquire its sources: they may have
retired while the assessment still legitimately retains them. Current caller
authorization and terminal-operation state must be checked before any replayed
result is disclosed. This reconciliation path remains to be implemented alongside
the writer; the binding helper alone does not make staging retry-safe.

The shared binder also requires exactly the uploading members in its selected
attempt map before physical lookup or locking. An extra entry could otherwise
cause a new attempt lock after candidate retention locks. Four PostgreSQL
assessment-binding cases cover complete verified uploads without publication,
wrong token rollback, displaced selection and excess selected entries. The final
affected run passed those four plus 23 publication-commit and six schema-retention
cases. These fixtures use synthetic provider observations. End-to-end assessment
writer coverage, including reuse-only candidates and staging reconciliation,
remains required.

`DocumentAssessmentSlots` supplies the candidate association projection for a new
stage. It prepares bounded source-node/part/object lookup batches before locks,
then runs after physical binding under the complete logical and physical fences.
Each reused slot takes its source revision UUID and full ordinal from exactly one
matching current source part. The physical object alone is insufficient, and
missing or duplicate source matches are refused. This also handles zero-upload
members: their selection revision remains part of the association.

Upload associations have no source revision fields; empty declarations create no
physical association. The result must match the complete nonempty command slot
set and complete member selection set. Its immutable records are input to V66
insertion, not independent authorization or evidence of a completed assessment.
The helper must not be used to reconstruct a previously retained stage from
current source pointers during retry reconciliation.
The writer must check exact operation identity and canonical command equality
between observed evidence, the physical plan and slot preparation. Equal physical
slots alone do not prove equal destination conditions, ownership or policy.

The projection run passed 22 PostgreSQL cases across slot projection, assessment
binding/reuse and native publication. Reuse-only and mixed candidates use sparse
source manifests whose full source ordinals differ from candidate and physical
upload ordinals. Wrong source-object claims and incomplete physical bindings
roll back the assessment owner. This adds an internal projection operation and
changes no protobuf contracts or external read/write availability.

#### Staging acknowledgement semantics and production-host qualification

New-stage admission and existing-stage acknowledgement are separate paths. New
admission requires current policy, write authority and revision conditions. An
existing acknowledgement discloses previously retained evidence under current
read authorization for the complete destination/source set. It must not rerun
current-source acquisition or treat policy/revision changes as proof that the old
transaction failed. This acknowledgement is not authority for a later terminal
decision, which still needs its own current authorization and policy checks.

The request retains one fixed staging deadline across retries. PostgreSQL stores
timestamps at microsecond precision, so the writer contract must normalize or
reject finer precision before the first attempt, then compare the exact retained
deadline on reconciliation. It must never recompute `now + TTL`, silently extend
the existing owner, or report an expired retained stage as usable. An expired
owner still protects bytes until explicit recovery; expiry is not evidence that
an uncertain commit rolled back. Different generations and terminal operations
need their explicit recovery/replay paths rather than implicit stage adoption.

`admissionStorageTest` adds a fresh standard-JVM host using only production JARs
and a small test probe JAR. It observes admission runtime identity, starts a real
PostgreSQL ledger with migrations and JPA validation, checks the observation
inside a SQL transaction, and runs the real typed/opaque assessment probe with
the SQL host initialized. JUnit is deliberately absent from the subprocess.
This initially qualified runtime composition. The creation probe described below
now also exercises assessment persistence; neither probe qualifies provider I/O.

The combined test exposed two integration failures missed by isolated tests:

- Hibernate's actual manifest is 171,463 bytes. Runtime manifest parsing now has
  a 256 KiB bound and still parses the complete manifest. Tests require rejection
  of a `Class-Path` header beyond the former 64 KiB boundary and of oversized
  manifests; archive, inventory and payload budgets are unchanged.
- The standalone admission dependency closure selected Guava 33.6.0 while the
  container host selected 33.7.1. Combining both closures correctly failed the
  shadowing check. Host inventories now traverse the admission subtree in the
  host's resolved Gradle graph and identify those actual artifacts. Standalone
  inventories continue to describe the standalone resolution. No version is
  substituted at runtime and duplicate class-provider refusal remains enforced.

The host inventory is generated by
`:protomolt-repo-container:admissionRuntimeInventory`; its integration test is
`:protomolt-repo-container:admissionStorageTest`, also included in `check`.
The standalone observer test remains `admissionRuntimeTest`. The writer's
transactional tests must build on this host qualification; manufacturing a runtime
observation in an ordinary test classloader is not acceptable evidence.

#### Atomic observed-assessment creation

`DocumentAssessmentCreation` is an internal, create-only participant. It binds
borrowed observed evidence to the exact operation generation and canonical upload
plan, then checks the live owner, current schema policy, complete authorization
and immutable drive/backend identity before acquiring candidate physical objects.
One transaction stores the owner, complete slots, sealed physical references,
normalized schema associations and root evidence. Deferred constraints and the
live owner/runtime context are checked before commit. Schema catalog claims must
already belong to the exact operation generation.

The caller supplies a fixed assessment UUID and retention deadline. Finer-than-
microsecond deadlines are rejected. A second create for the same operation and
generation conflicts even if it supplies a different UUID; it does not adopt or
replace the retained assessment. Reconciliation of an uncertain commit remains a
separate, unfinished operation. No public staging API, terminal admission rejection
or semantic-review handoff is enabled by this participant.

The writer reserves scratch capacity before entering SQL for two copies of the
manifest plus two copies of the largest root encoding. This bounds explicit array
copies and JDBC payload retention, not total JVM heap. Slot batches contain at
most 256 rows; root payloads execute individually. Schema resolution, validator
execution and independent evidence replay occur outside this transaction.

`AssessmentCreationProbe`, run by `admissionStorageTest`, uses the qualified
production-JAR runtime and real PostgreSQL with genuine observed validation
evidence. It covers typed/opaque candidates with both passing and failing
validation, wrong owner generation, deadline precision, rollback after physical
sealing when schema claims are absent, complete retained schema/root associations,
exact manifest persistence and duplicate-create conflicts preserving the original
owner. An injected PostgreSQL `57014` at root insertion proves rollback after
physical sealing and schema association: all five assessment tables and native
physical references remain empty for the failed assessment, and writer scratch
is released. This proves SQL-error rollback, not client interruption timing or
post-commit cancellation handling. Neither publication nor a terminal rejection is created. Reservations are
released when the scoped assessment closes. Physical provider observations in this
probe are explicitly synthetic; provider I/O, whole-writer reuse/race cases and
lost-acknowledgement reconciliation still need their own qualification.

#### Reconciliation binding requirement (not yet implemented)

The observed validation manifest does not identify selected physical upload
objects, selection revisions, or a reused source's revision UUID and full part
ordinal. Rebuilding these from current source pointers after an uncertain commit
would describe a different candidate. A receipt returned only after commit also
cannot cover a lost acknowledgement or process restart.

The reconciliation contract will acknowledge the original committed stage using
the caller's fixed assessment UUID. Creation must additionally seal a bounded,
canonical snapshot of every slot tuple in its first transaction: member,
candidate full ordinal, selection revision, physical object UUID, declaration,
and optional source revision UUID/full ordinal. Its encoding version and digest
must bind the complete tuple set to the assessment UUID, operation key/generation,
canonical command, observed manifest and fixed deadline. This is database-derived
staging provenance, not another observed validator claim. Legacy owners lacking
this provenance must be refused rather than implicitly adopted.

Reconciliation must compare the complete snapshot with relational slot rows and
all command-derived identities, plus the complete retained schema/root evidence.
Supplied upload attempts and selection revisions must match frozen history;
REUSE source revision/ordinal come from the original snapshot. It must not consult
current selection pointers, reacquire physical retention, or require schema
staging claims to remain present. This verifies the original stage; it does not
assert that a fresh stage would select the same source revision now.

The lock order is live operation fence, current read authorization for the full
destination/source set, then retained assessment owner. After waits, recheck live
ownership, absence of terminal outcomes, sealed/unreleased state, exact fixed
deadline and unexpired retention using database time. Revoked or unavailable
sources can prevent disclosure without invalidating the original commit. Recovery
continues to take the operation recovery fence before owner and physical locks.
Tests must cover lost acknowledgement, restart, moved source pointers, changed
selection, expired stages, revoked access, terminal outcomes and legacy refusal
before reconciliation is exposed.

`DocumentAssessmentSlotSnapshot` supplies the internal encoding primitive.
Creation now persists it; reconciliation remains unfinished. Version 1 uses the `PMAS`
magic and big-endian fixed-width numbers, length-prefixed UTF-8 identity strings,
raw UUIDs/digests, epoch-second/nanosecond deadline and a counted slot sequence.
Slots are sorted by ASCII member ID and candidate full ordinal. Upload tuples
have no source fields; reused tuples include source UUID and full ordinal.
Duplicate candidate slots, malformed source alternatives, malformed UTF-16 and
sub-microsecond deadlines are refused. The encoding accepts 1..10,000 slots and
at most 4 MiB. Its scoped reservation covers the exact encoding array, immutable
byte copy and bounded text scratch; downstream JDBC copies require their own
reservation. Reconciliation will re-encode retained relational rows for exact
comparison rather than parse untrusted embedded lengths. Unit tests pin the wire
layout and exercise identity changes, ordering, bounds and cancellation cleanup.

V69 adds `document_assessment_slot_snapshots`, one immutable, checksum-protected
encoding per assessment. Insert requires the live operation fence and the owner's
physical seal in the original creation transaction. Recovery removes the snapshot
inside the same fenced transaction as the other assessment associations; expiry
alone does not delete it. Migration preserves existing owners without inventing
snapshots or retroactively accepting late provenance. Snapshot absence explicitly
marks a stage that the future reconciler must refuse. Presence alone is insufficient:
SQL checks storage integrity, while the handler must verify codec/version and exact
canonical equality against the full identity and every retained slot.

The creator encodes the bound slots after inserting root evidence, reserves
additional nonblocking capacity for its encoding and JDBC copies, and inserts the
snapshot before deferred constraints and final ownership/context checks. Capacity
failure rolls back the transaction. The qualified production-JAR/PostgreSQL probe
compares the persisted encoding with a fresh encoding of the actual relational
slot rows and original identity. SQL lifecycle tests cover migration, late/pre-seal
insertion refusal, checksum failure, immutability, explicit expiry recovery and
rollback of snapshot deletion when a later recovery step fails. Synthetic bytes
in these SQL-only fixtures prove lifecycle guards, not canonical handler provenance.
