# Optional JCR 2.0 implementation: compatibility and gaps

Status: architectural requirement and initial assessment, not implemented or
JCR-compliant. Reviewed against JSR-283 and local source at `c8f6c043`; archive
publication/reference changes in the working tree remain under review.

ProtoMolt will own the underlying content-repository implementation. An optional
standard Java JCR adapter and protobuf/gRPC access must invoke the same behavior.
Existing repository protobuf contracts retain their identities and meanings.
No JCR dependency belongs in the byte SPI, codecs, providers or base storage
modules. This requirement does not delay current storage correctness/recovery.

## Compatibility assessment

- **Atomic changes:** `Tx` can group SQL changes, but current `ArchiveLedger`
  methods own individual transactions. Repeated per-entry saves cannot implement
  an atomic multi-node save. The foundation needs a composable commit boundary
  for metadata, object references, revisions, policy checks and durable outcomes.
  Stage bytes first; publish all references together. Failed publication must
  leave no partial visible graph. JCR session-save atomicity is distinct from
  optional JTA transaction support; do not infer either from having SQL.
  [Writing §10.11](https://developer.adobe.com/experience-manager/reference-materials/spec/jcr/2.0/10_Writing.html),
  [Transactions §21](https://developer.adobe.com/experience-manager/reference-materials/spec/jcr/2.0/21_Transactions.html).
- **Stable identity:** archive UUIDs derive from the logical address. Preserve
  that contract, but do not use address-derived IDs as the identity of movable
  content nodes. Allocate a separate stable node ID and maintain paths/parent
  edges independently. Referenceable JCR identifiers survive moves.
  [Repository model §3](https://developer.adobe.com/experience-manager/reference-materials/spec/jcr/2.0/3_Repository_Model.html).
- **Transient sessions:** upload leases and hydration revisions are not JCR
  sessions. The extension needs pending changes, reads of its own changes,
  refresh/discard behavior, conflict detection and atomic save. Failed save must
  preserve pending changes. Remote sessions need authenticated handles, expiry,
  explicit close and retry-safe commit outcomes, without holding a SQL transaction
  or provider connection throughout a user's editing session.
  [Writing §10](https://developer.adobe.com/experience-manager/reference-materials/spec/jcr/2.0/10_Writing.html).
- **Workspaces:** model persistent rooted content graphs and their session views,
  with explicit workspace identity and access rules. Existing accounts, agent
  workspaces and archive names are not JCR workspaces. Copy/clone and corresponding
  node behavior require separate contracts when those capabilities are selected.
  [Repository model §3](https://developer.adobe.com/experience-manager/reference-materials/spec/jcr/2.0/3_Repository_Model.html).
- **Types and references:** protobuf schemas/Any do not implement JCR node types.
  The extension needs namespaces, primary/mixin types, typed single/multivalued
  properties, child definitions and constraint rules. Strong/weak node references
  and their integrity semantics are distinct from storage-object retention pins;
  check graph references within the publication transaction.
  [Repository model §3](https://developer.adobe.com/experience-manager/reference-materials/spec/jcr/2.0/3_Repository_Model.html).
- **Version restoration:** archive version numbers and rendition manifests are
  useful storage primitives, not JCR version histories. Choose and declare simple
  or full versioning deliberately. Frozen state, on-parent-version behavior,
  checked-in restrictions, identity collisions and atomic restore need explicit
  implementation and tests; restoring bytes alone is insufficient.
  [Versioning §15](https://developer.adobe.com/experience-manager/reference-materials/spec/jcr/2.0/15_Versioning.html).

## Required architectural split

The **repository foundation** owns immutable byte identity and original storage
bindings, verified uploads, reference retention, recovery, reusable revisions,
transactional commit composition, current-policy checks and durable operation
identity. Archive/document transactions are domain wrappers over these primitives,
not the only possible unit of atomicity. Preserve deterministic lock ordering
across a multi-object change set and cancellation/retry semantics at commit.
Current archive binding ownership is entry-specific. Keep it an archive policy,
not a universal rule that every physical object belongs to one document/node.
Reusable storage identity and domain reference ownership must stay separable.

The **optional content-repository extension** owns the graph, workspace/session
model, node/property types, namespaces, graph-reference integrity, versioning and
restoration semantics. It provides query/export and other basic JCR behavior,
then separately selected optional capabilities. JCR API types belong in its Java
adapter; gRPC messages describe portable semantics rather than Hibernate entities,
ProtoMolt account assumptions or internal archive binding IDs.

Keep the potential **gRPC transport contribution implementation-neutral**: a server
adapter over a standard `javax.jcr.Repository` must be possible for other JCR
implementations. ProtoMolt's own content engine remains the implementation behind
its Java and gRPC surfaces. Transport tests must exercise another implementation
as well as ProtoMolt, with capability negotiation and explicit unsupported errors.
This would be a new transport, not an existing standardized JCR gRPC binding.

## Gates before implementation and compliance claims

Before extending contracts or choosing transaction boundaries, review atomic
change sets, stable identity, session/workspace scope, typed values/references,
restore, errors, retries and declared capabilities against this requirement.
The current archive upload/reference work is reusable; a public per-document
commit API must not become the universal content-repository transaction model.
Do not implement JCR features as incidental additions to archive deletion fixes.

Create a capability matrix and fixtures before implementing the extension.
JCR's basic acquisition/security, reading, query, export, type discovery and
permission/capability checks are not optional merely because write/versioning
features can be selected. Advertised optional features must satisfy their full
specified semantics. Require applicable JCR 2.0 conformance tests for the declared
capabilities, plus Java/gRPC parity, atomic failure/restore and cross-implementation
transport tests. No JCR compliance claim precedes that evidence.
[Repository compliance §24](https://developer.adobe.com/experience-manager/reference-materials/spec/jcr/2.0/24_Repository_Compliance.html).

Source pointers: [transaction wrapper](../../repo/container/src/main/java/ai/protomolt/proto/repo/container/ledger/Tx.java),
[archive ledger](../../repo/container/src/main/java/ai/protomolt/proto/repo/container/archive/ArchiveLedger.java),
[archive identity](../../repo/container/src/main/java/ai/protomolt/proto/repo/container/archive/ArchiveIds.java),
[implementation inventory](repository-operation-inventory.md).
