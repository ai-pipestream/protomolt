# Parts-based repository requirements

**Draft for user review, 2026-10-13. Not an implementation authorization.**

Baseline: `reconcile/parts-based-repository` at
`61955b4f974466783f1e7af42e31ba97016a417d`. Findings describe this checkout,
not every remote branch. Main and `ai-slop-archive-by-astra` remain untouched.

This document replaces conversational assumptions with requirements we can review.
“Required” records the user's established design. “Proposed” needs design review;
it does not describe a shipped feature. Source findings are not runtime proof.
The [toolkit audit](protobuf-toolkit-audit.md) and its
[source index](protobuf-toolkit-source-index.md) supply the broader inventory.

This was the result of drift from the requirments by allowing an LLM to invent code 
as it went along.  The codex client was the coordinator, but it made a lot of poor decisions:

* The point of this is that this is a repository service on a platform.  The platform 
is called protomolt - a protobuf based framework aimed at making a document processor,
an agent helper, a service bridge where grpc can act as a REST OpenAPI, ACP, MCP, and A2A 
services all through looking at a reflection of a grpc service.

Furtheremore, it allows for documents to be processed either async without the use of an LLM
or for LLM communication where the asker creates the protobuf contract.  

Protobuf definitions are expanded for not only a schema, but validations and providence as 
well as defining embeddings.

We support multiple embeddings.  We can take data from multiple sources and either make them 
an async input for a pipeline or through agent communcations.

The center of this often means saving data - we are using s3 for a block storange because it is indexpensvie.

But we are also able to "swap out" repository service impls.  For S3, metadata is stored in a database
and will be broadcasted as it updates via kafka.

We have a kafka schema registry that is compatible with confluennt and apicurio.  it honors the 
extensions we do for validation.

The repo service can save all or some parts of the document entity or metadata associated with it.

All data is saved and preserved.




## 1. Purpose and boundaries

ProtoMolt needs one repository service for document processing and RAG. A document
is composed of addressable parts. Processing stages can read only what they need,
replace selected parts, and carry unchanged parts forward without rewriting their
content. Protobuf content and its retained schema remain authoritative throughout.

There is one canonical public document service: **DocumentService**. Validation,
revision commits, receipts, provider access and recovery are components behind it,
not competing ways for applications to save a document. Multiple RPCs on that
service are appropriate; duplicate storage semantics behind separate APIs are not.

Required use cases:

1. Bulk operations through gRPC streams, with a correlated outcome for each item.
2. Partial updates of selected parts and selected chunk sets.
3. Reading a full document and listing its revision history.
4. Reading a particular immutable revision, including selected parts of it.
5. Addressing parts independently and composing documents from exact part revisions.

SQL owns ledger state, references, authorization-related records and simple lookup
metadata. Selected storage providers hold payloads. Search projections belong to
search components, except simple SQL lookups. S3 is a provider, not the repository
model. No new search system, agent coordinator or JCR implementation belongs in
this repair.

Special note: S3 could be *any* block storage and this should ride as a generic provider.  This is so we can move to azure storage in the future or another block storage API that might provide better features than S3.

JCR is off the table, after researching - it's a grabage format.  Metadata can be swapped out as SQL or MongoDB (we will plan for mongo after postgresql).  We will also consider graphql for capabilities in traversing the map.

## 2. What the current source actually provides

The current [DocumentService contract](../../repo/proto/src/main/proto/ai/protomolt/proto/repo/v1/document_service.proto)
has unary save, get, address-based get, manifest inspection, list and delete,
alongside blob operations. Save already has `parts_written`,
`copy_unwritten_parts_from`, `chunk_sets_written` and write provenance. It does
not currently expose the proposed streaming document operations or a durable
operation receipt in SaveDocumentResponse.

The [manifest contract](../../repo/proto/src/main/proto/ai/protomolt/proto/repo/v1/manifest.proto)
defines CORE, BLOBS, CHUNKS and PARSED, ordered entries, chunk sub-keys, provenance,
and PRESENT/EMPTY/DELETED states. Preserve these distinctions. Comments describe
PARSED as append-only; enforcement must be traced before claiming that guarantee.

The source audit found these concrete gaps:

- DocumentOperations' canonical save does not automatically acquire the separate
  schema-admission and durable-result guarantees merely by sharing a repository.
- Managed partial save reads carried parts and supplies their bytes to another
  write. This contradicts the API comment promising no transit for unchanged parts.
- DocumentPartReader and fragment encoding materialize complete selected payloads.
- Manifest/security/history/archive paths still use JSON as authority. Those must
  be repaired, not copied into the unified path.  All JSON needs to be backed by a strongly typed schema and have a schema registry defintion.  It must be compatible with 
- Existing retained history services and internal publication contracts remain.
  Their useful behavior must be mapped into the canonical service; their existence
  is not justification to reintroduce a second public save API.

These are bounded findings, not a claim that every method has been audited.

## 3. Identity and fidelity requirements

**REQ-01: Keep identities distinct.** A logical document address, immutable document
revision, part selector, immutable part revision, operation ID, attempt ID, schema
artifact identity and physical provider version serve different purposes. An
integer `doc_version`, an S3 version ID and a revision UUID are not interchangeable.
A routing hint is not storage identity. Physical locations resolve through the
configured backend identity and generation, never an implicit alternate provider.

**REQ-02: Retain authoritative bytes and schemas.** Original files remain exact
bytes. Typed part payloads remain protobuf with retained schema identity. Preserve
unknown fields, presence, exact integer values, Any payloads and custom options.
No internal protobuf-to-JSON-to-protobuf representation may substitute for storage,
validation, mapping or projection. An intentional Struct field is still a field;
it is not permission to flatten arbitrary messages into Struct.

Exact retained object bytes and logical protobuf equivalence are different claims.
Unchanged objects must retain their exact bytes. Parsing and re-encoding a modified
message need not preserve arbitrary original wire ordering. Tests must say which
property they assert. Never advertise byte-for-byte preservation based solely on
message equality.

**REQ-03: SQL projections are rebuildable.** JSON metadata lookup columns are
allowed only as documented derived views. Recovery and complete object reads must
not reconstruct authoritative protobuf from those views. Schema payloads remain
normalized by immutable identity, not duplicated into each document.

Acceptance: write, restart, read and restore messages with unknown fields, optional
default values, large integers, nested Any and validation annotations; assert exact
stored bytes where promised and typed equality where transformation is intended.
Corrupt or remove a lookup projection and show it cannot alter authoritative content.

## 4. Parts, updates and read semantics

**REQ-04: Preserve existing selection semantics.** Empty `parts_written` currently
means full save; nonempty means selected replacement with carry-forward. CHUNKS
supports selected sub-keys. Omitted, empty and deliberately deleted are distinct.
Reject duplicate or contradictory selectors instead of choosing an arbitrary winner.
Keep ordering and provenance for carried entries.

**REQ-05: Pin composition before validation.** Resolve each source document/part
reference to an immutable revision once. Check source access and destination access.
A concurrent source update must not change the composition halfway through a save.
Never fetch “latest” as a fallback for an unavailable retained version.

**REQ-06: Reuse unchanged content.** Prefer retaining an immutable object reference
when ownership, retention and backend rules permit. Shared objects require references
and reclamation fences that protect every live revision. If a physical copy is
required, it must bind an exact source version and verify the resulting identity.
Do not route carried parts through a byte array and upload them again by default.
Cross-account reuse is not implied by possession of a part ID.

**REQ-07: Reads identify their snapshot and hydration.** Resolve current once; exact
revision reads never substitute current. Return a manifest that distinguishes parts
not requested from empty/deleted parts. Manifest inspection must not fetch payloads.
History listing must be paginated with defined ordering and continuation behavior.
A large full-document read is a stream of a pinned manifest and selected content,
not a requirement to build one enormous in-memory Document.

Acceptance: update one chunk set while source and destination change concurrently;
assert the winning revision, unchanged object identities, absence of unnecessary
provider GET/PUT calls, preserved siblings, and correct history after restart.

## 5. Validation and dynamic types

**REQ-08: Validate the actual candidate.** Use existing descriptor-based validation,
CEL and admission tools. Validate request structure before payload/provider work
where possible. After source pinning, validate the resulting composed document,
not only the patch. Evaluate required cross-field and cross-part rules before commit.
Successful response envelopes must also meet their declared contract.

**REQ-09: Bind validation evidence.** Bind evidence to candidate content, exact
schema artifacts, policy/rule profile, operation and attempt/revision as applicable.
A changed candidate or schema requires new evidence. Cache descriptors by immutable
artifact identity; do not cache authorization or validation success by type name.
State-dependent authorization, expected-revision checks and commit fencing are
handler/transaction obligations, not protobuf annotations.

**REQ-10: Opaque is an explicit mode, never successful typed validation.** Retain
unresolved Any bytes and type URL when opaque archival is requested and permitted.
Record unresolved status. Typed use requires a descriptor and successful decoding;
DynamicMessage also needs a descriptor. Missing definitions, registry outages,
invalid bytes and unsupported rules must remain distinguishable. Historical typed
reads use retained definitions without substituting live registry results.

Current DocumentAnyRootInventory covers CORE structured_data and PARSED shape,
the declared Document Any roots identified in this audit. Nested occurrence traversal
and unknown-field policy need qualification, not an invented claim that all possible
future locations are already supported.

Acceptance: valid and invalid composed candidates; unsupported rules; unavailable
versus missing schemas; two historical revisions using different definitions under
one type URL; nested Any; explicit opaque storage and typed refusal. Invalid typed
candidates cannot commit or reach semantic review as contract-valid candidates.

## 6. Commit, receipts and recovery

**REQ-11: One document mutation has one durable outcome.** Reuse operation-ledger,
admission and commit primitives behind SaveDocument. Upload/stage new objects outside
long SQL lock windows. In a short transaction, verify ownership/fencing and expected
revision, then commit revision references, retained evidence, durable result and
outbox atomically. No provider network call while holding publication row locks.
A SQL rollback must not make an uploaded object visible as a committed document.

**REQ-12: Idempotency is content-bound.** Same operation identity and same canonical
intent returns the recorded result without duplicate publication. Reuse with changed
intent fails explicitly. Canonical intent includes selected parts, source revisions
and applicable policy; do not use JSON serialization as its canonical form.

**REQ-13: Cancellation and uncertainty are explicit.** Cancellation before commit
prevents later publication by an abandoned attempt. After commit, cancellation cannot
undo history or change a committed receipt into failure. A disconnected caller can
retrieve the durable outcome. Staged-object cleanup cannot reclaim referenced content.
Retries retain their exact target; unknown provider outcomes are not reported as success.

**REQ-14: Recovery preserves the same contract.** Restart and isolated restore must
recover revisions, parts, schema payloads, operation outcomes and pending cleanup.
Historical missing/corrupt content, backend outage and backend credential refusal
remain typed failures with safe public messages and preserved internal causes.
Do not mask these failures by changing credentials or fetching another version.

Proposed default: each streamed bulk item is its own atomic document mutation. Stream
completion does not imply an atomic transaction across all items. Multi-document
atomic commit is a separate decision; do not implement it accidentally or preclude it
in foundational ledger interfaces without assessment.

Acceptance: crash at stage/commit/response boundaries, replay, concurrent same-ID
requests, stale ownership, cancellation races, restore with pending deletion, and
post-commit access revocation. Assert SQL state, provider effects and returned receipts.

## 7. Large documents, flow control and horizontal scaling

**REQ-15: No invented document ceiling.** A transport frame limit, control-record
budget, provider object limit and total document size are separate constraints.
A 600 MB PDF is a required practical case. The user's approximately 2 GB requirement
must be tested with explicitly stated units and object shape, not claimed by setting
a constant. A raw file, aggregate parts and one encoded protobuf message are different
cases with different runtime constraints.

**REQ-16: Bound memory by active work, not total payload.** Stream source bytes and
part transfers with backpressure, incremental checksums and cancellation. Avoid full
payload copies at codec, provider, RPC and recovery boundaries. Release buffers and
provider resources on success, early close, cancellation and error. Define when a
stream's integrity is established; a prefix is not a verified complete object.

Some whole-message CEL/validation requires a decoded message. Do not claim those
rules stream automatically. Define the materialization requirement and resource
admission explicitly; reject inability to validate rather than silently skipping
rules. Raw source bytes behind claim checks must not require that materialization.

**REQ-17: Provider capabilities are truthful.** Reuse BlobStoreProvider,
OpenedBlobStore, BackendIdentity and optional capability ports. A provider-specific
limit does not become the platform limit. An unsupported operation fails before
partial publication. Redis's value constraint does not justify limiting S3-backed
documents. No Azure implementation is claimed by this audit.

**REQ-18: Replicas share durable coordination.** Correctness must not depend on a
single JVM lock or local cache. Concurrent repo hosts share operation/revision state
and fencing. Independent documents should not contend on a global publication lock.
Measure lock hold time, pool wait, allocation, throughput and latency using real
RustFS for performance; use LocalStack for relevant S3 correctness qualification.
No performance targets or scale-out gains are asserted before measurement.

Acceptance: large raw source transfer through the public API, selective read/update,
slow consumer, cancellation, provider truncation, memory pressure and two-host races.
Record peak memory and provider traffic. Include maximum supported control-record
and decoded-candidate cases when measuring transaction lock windows.

## 8. Proposed operation responsibilities, not final protobuf definitions

Keep the existing DocumentService and define the additions/refinements there:

- SaveDocument: composed mutation, explicit write precondition, validation mode,
  operation identity and durable receipt; preserve parts/chunk selection.
- SaveDocuments: bidirectional stream invoking the same mutation semantics per item,
  bounded in-flight work and correlated outcomes. Large content uses part streams or
  references; a stream of oversized unary-shaped messages is not a size fix.
- GetDocument / address-based get: current or exact revision, selective hydration.
- ReadDocument: streaming equivalent for large selected content from a pinned revision.
- Part write/read: stage and retrieve exact part content; staging alone does not
  publish a document. Bind staged handles to owner, backend and lifecycle.
- History and operation outcome lookup: expose retained revision and receipt behavior
  through the canonical service, reusing existing handlers where sound.
- Existing manifest, list, delete and blob operations: inventory callers and preserve
  required behavior. Do not discard them merely to shorten the service definition.

Exact message fields, tags, stream frame alternatives, terminal integrity messages
and error mapping are the next contract-review deliverable. They are deliberately
not fabricated here. Upload resumption is not promised by basic streaming.

## 9. Reuse first; add helpers only for demonstrated gaps

Keep DocumentPartCodec/PartLayout, mapper/CEL/projection libraries, ProtoValidator,
closed descriptor artifacts, RegistrySchemaResolver, DocumentSchemaAdmission,
retained schema storage, ledger/commit/receipt/recovery components and provider SPI.
Adapt these behind the one service; do not treat the old buffered publication input
as a mandatory intermediate representation.

Candidate helper changes needing a design review:

1. **Exact-object streaming read port in the storage SPI.** BlobStore currently
   returns byte[] for reads, though InputStream upload already exists. Specify the
   read handle's provider version, length, digest verification, close/cancel behavior
   and lifetime. Reuse existing provider ownership and capability patterns.
2. **Immutable part-reference composition.** Reuse ledger bindings and retention
   fences to carry content forward. Existing `copy(srcBucket, srcKey, dstBucket,
   dstKey)` lacks a source version and returned identity; do not assume it qualifies
   exact-version copying. Extend a capability only if physical copying is necessary.
3. **Canonical save admission adapter.** Compose existing admission/evidence and
   commit ports into DocumentOperations. Specify the candidate view needed for
   cross-part validation without introducing another public service or JSON bridge.
4. **Binary authoritative metadata codec/storage mapping.** Reuse generated messages
   and versioned schema artifacts. Replace the identified JSON authority paths;
   leave explicitly derived lookup projections distinguishable and rebuildable.

These are responsibilities, not instructions to create four new libraries. Put each
helper in an existing module unless a concrete dependency boundary justifies a split.

Remove only after the replacement behavior is proven: obsolete authoritative JSON
codecs, duplicate orchestration, unused buffered adapters and misleading current-use
docs. The duplicate publication transport is already removed on this branch. Keep
internal safeguards and regression tests until their assertions are transferred;
never delete a failing test simply because its old transport disappeared.

## 10. Decisions for our review

1. **Bulk atomicity:** per-document outcomes as proposed, or a demonstrated use case
   requiring an explicitly atomic multi-document batch?
2. **Large typed messages:** confirm whether the 2 GB requirement includes a single
   decoded typed message, in addition to raw files and aggregate document parts.
   This determines validation/materialization work, not an arbitrary new limit.
3. **Opaque admission:** proposed explicit per-request mode, constrained by host
   policy, with an unvalidated receipt status and no typed-success claim.
4. **Part ownership:** proposed immutable references within an authorized storage
   realm first. Cross-account composition needs an explicit ownership/retention rule.
5. **Partial hydration:** committed revisions remain coherent snapshots. Incomplete
   staged parts are not implicitly valid committed documents. A durable partially
   hydrated document would require an explicit state model; defer unless needed now.

Future optional JCR compatibility remains an architectural constraint, not current
implementation scope. Assess stable identity, atomic multi-object commits, sessions,
workspaces, types, references and version restoration before extending foundations.
Do not equate existing account/workspace/version fields with JCR semantics. Keep JCR
dependencies out of base storage and any transport reusable by other implementations.

## 11. Progress and qualification record

Completed source reading for this requirements draft:

- Public document and manifest contracts: existing selections and missing operation
  receipt/streaming fields identified.
- DocumentOperations, ManagedDocumentSave and DocumentPartReader call paths:
  carry-forward re-upload and buffering identified in the toolkit audit.
- BlobStore and all optional capability declarations: streaming write exists;
  streaming read and exact-version copy are not provided by the inspected SPI.
- OpenedBlobStore, BackendIdentity, ExactVersionReclaimer and PayloadBudget:
  reuse lifecycle, identity and exact-target reclamation; payload reservations do
  not measure all JVM/SDK memory.
- Admission, retained schemas, metadata and commit components: reusable boundaries
  and authoritative JSON defects recorded in the linked audit.

No production edits or tests were run for this draft. No performance, end-to-end
validation or implementation completeness is claimed. The source index is navigation,
not proof that every listed file was reviewed.

Before implementation: review this document together; trace any disputed requirement
against code; write the concrete protobuf changes and handler/library mapping; then
agree a small first slice with observable acceptance criteria. Each slice must record
request-to-storage-to-read/recovery evidence and explicit remaining gaps. Compilation
and green tests alone do not establish preservation of this architecture.
