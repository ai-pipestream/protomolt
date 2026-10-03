# Repository composition and typed archival admission

Status: proposed design, ready for implementation planning. This document does
not introduce an API or claim that the missing behavior is available.

Source baseline: `fd0cf28769494b281e5ca3963bff1844b22be193`, whose tree matches
the merged delegation extraction at `528117a2d48cda3b3abedadd75b5706d7ac68ca7`.
Recheck main and affected contracts before implementation.

## Purpose

Make the repository usable as a Java library or a gRPC service with the same
document behavior. Applications select storage providers without inheriting all
provider dependencies. Typed protobuf admission adds schema resolution and
validation above byte storage. Other languages use the existing protobuf/gRPC
contracts; Java SPI is local composition, not a cross-language plugin protocol.

Keep the database where it provides transactions, metadata, version history and
ownership. Removing dependencies from small consumers does not require removing
the database from the repository implementation.

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

The complete ownership work remains unfinished: resolve inherited policy and
atomically guard policy revision with every mutation. Mutations still require
process authority. Named
gRPC callers currently carry only name/scopes and remain denied unless the host
installs a trusted repository binding resolver. The document gRPC adapter accepts
such a resolver and rejects null, changed principal or changed process authority.
It is not a request-header override. Open listeners retain process authority and
require the trusted-network deployment boundary.

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
