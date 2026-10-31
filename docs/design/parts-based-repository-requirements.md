# Parts-based repository requirements

**Draft v2.5, 2026-10-14.** This rewrite folds in the decisions agreed in review (see
`reviews/parts-requirements-review.md`, "Decisions agreed with Kristian"). It replaces v1
at `docs/design/parts-based-repository-requirements.md`. This is the only repository
design source. Removed design docs and evidence (tag `docs-archive/2026-10-14`) are
not normative. Do not restore them. Old tests and evidence may still be consulted for
failure cases worth preserving.

## 1. What this is

ProtoMolt is a protobuf-based platform. It processes documents into an enterprise
semantic search index, helps agents, and bridges services: one gRPC service, read
through reflection, is exposed as REST/OpenAPI, MCP, ACP and other surfaces. Protobuf
definitions carry schema, validation rules, provenance and embedding hints. Documents
are processed asynchronously in pipelines, or through agent conversations in which the
asker supplies the protobuf contract.

The repository service stores those documents. It saves all of a document or only some
of its parts, keeps every revision, and announces each change on Kafka.

## 2. Glossary

| Term | Meaning |
|---|---|
| **Stacks** | Where bytes live: S3-compatible storage, Azure Blob, RustFS or others. Swappable. |
| **Catalog** | Where revisions, part references, operation outcomes and the event outbox live. PostgreSQL first, MongoDB planned. Swappable. |
| **Catalog entry** | A document's lookup fields, defined as a proto message and produced by a protomolt projection. |
| **Call number** | `NodeAddress`, the logical address of a document. |
| **Accession** | One committed revision of a document. It is what gets announced on Kafka. |
| **Part** | One independently stored piece of a document: CORE, BLOBS, CHUNKS, EMBEDDINGS or PARSED. |
| **Projection** | Only the protomolt descriptor mapping (`docs/transform/projection.md`). Never database-derived columns. |

## 3. Scope

**In scope:** one `DocumentService` contract; partial saves and reads; revision history;
bulk streaming; swappable stacks and catalog; typed validation; Kafka events.

**Out of scope:**
- JCR compatibility. Do not shape identities, sessions, workspaces or commit boundaries for it.
- Multi-document atomic commits.
- A new search system.
- Retention or pruning of old revisions (later feature, §13).

**Runtime:** protomolt is JDK-first with minimal dependencies. The repository service
uses no application framework (no Quarkus, no Spring), as `repo/README.md` states.
Quarkus was the pipestream standard, not protomolt's.
The PostgreSQL catalog uses JDBC + HikariCP + Flyway (selected by Kristian).
This decision does not authorize implementation in the current documentation review.

## 4. Use cases

1. Bulk saves and reads through gRPC streams, with one correlated outcome per document.
2. Partial update of selected parts and selected chunk sets.
3. Read a full document, or selected parts of it, and list its revision history.
4. Read an exact past revision, including selected parts of it.
5. Compose a document from exact part revisions of other documents.
6. Add or replace one segmentation or one embedding set without rewriting anything else.

## 5. Data model

### Parts
A document is split on protobuf field boundaries into parts, each stored as its own object:

- **CORE:** everything not assigned to another part.
- **BLOBS:** the blob bag (claim checks for raw files).
- **CHUNKS:** one object per segmentation, holding segment text and offsets only.
- **EMBEDDINGS:** one object per embedding set, keyed `<segmentation_id>/<embedding_set_id>`.
- **PARSED:** parser results.

The manifest lists parts in order, with each part's state (PRESENT, EMPTY or DELETED),
size, SHA-256 and provenance. Omitted, empty and deleted are distinct. Duplicate or
contradictory selectors in a request are rejected.

### Semantic results
`SemanticProcessingResult` is replaced by:

- **`Segmentation`:** how a text was cut. Fields: kind (sentence, token window,
  paragraph or semantic boundary), chunker config, source text (a field path or a
  derived text), and segments with offsets.
- **`EmbeddingSet`:** one model's vectors over one segmentation, with model, dimensions
  and similarity.
- **Binding.** A segmentation is immutable once committed. Its identity is its ID plus a
  content digest; changing its text or boundaries produces a new digest. Segments have
  stable IDs. The offset unit is declared on the segmentation, and the contract fixes the
  allowed units. An embedding set records the segmentation ID and digest it was computed
  against, and one vector per segment ID. A save that carries forward an embedding set
  whose segmentation digest no longer matches is rejected; vectors are never silently
  reattached to new text.
- **`Lineage`** on both: the producing step, the ids it was derived from, and when it was created.

A document may hold many segmentations. Example: sentences, embedded with model2vec, then
semantic chunks derived from those sentence vectors over normalized text, then embedded
with several models.

The source text binding records the SHA-256 and UTF-8 length of the actual resolved
text, not only its field path. Recompute it against the composed candidate. The
segmentation digest includes kind, source binding, offset unit and ordered segments
including segment metadata; it excludes top-level lineage/metadata. Verify ranges,
text correspondence and exact segment/vector coverage; transport validation alone
cannot establish these relationships.

### Identities
- **Call number** (`NodeAddress`): the logical document.
- **Revision:** a per-document revision number. Used for history, exact reads and expected-revision checks.
- **Operation ID:** supplied by the client, unique per account; makes saves idempotent.
- **Part and segmentation/embedding-set selectors.**

Stack references and backend details are internal, not public identities.

### Save ownership and provenance

Every save has a required SaveOwner: intake origin or producing step. Document
ownership remains the existing OwnershipContext; save ownership identifies the
writer, not a transfer of tenancy or ACLs. Intake datasource and optional connector
must match the document's ownership and authenticated caller's authority: an intake
owner that names a connector must name the document's connector, a document without a
connector takes no owner that names one, and an owner without a connector may write a
document that has one. This holds for process authority exactly as for a writer grant. A step
reuses WriteProvenance for recorded attribution. Module ID is required; node, graph
ID and graph version may be supplied but graph membership/version is not verified
in slice 1. A graph registry is not a prerequisite and no numeric/string workflow
version conversion is implied. A CORE part taken from another revision through
PartSource counts as a CORE write: its ownership becomes the revision's ownership, under
the same checks as a CORE PUT (account, datasource and connector stay fixed; the ACL may
change) (decision 47).

**Writer grants (W1).** A trusted administrator provisions an immutable WriterGrant
for one account and one credential (issuer and credential ID). Grants do not name a
generation, so they survive key rotation. It names either an intake datasource/optional connector or a
step module/optional node. An absent node restriction covers that module's nodes;
a present node requires an exact match. An absent connector restriction covers the
specified datasource; when present it must match the resolved connector. Neither
omission permits another account, datasource or module. Document ownership and
ACL checks remain mandatory. The request cannot install a grant or supply trusted
credential identity. Process authority is not a writer grant. A process-authority caller carries no credential binding, so no grant matches it. It may save only with an intake owner, which must match the document's ownership as for any intake save. It may not claim a step owner or submit opaque attestations, new or carried. The revision records process-authority evidence (the principal) in place of grant evidence.

The grant declares whether it permits creation and its exact opaque_type_urls
(no wildcard or prefix matching). Only step grants may list opaque types. A type
allowance establishes authorized attribution, never payload validity. Changes to
scope create a new grant ID; revocation is a separate monotonic catalog state.
IDs are never reused. Provisioning requires administrator authority, outside the
public DocumentService; no new grant-management RPC is introduced in this slice.

The server selects a matching active grant for the save and, where necessary, each
opaque producer. Multiple eligible grants are resolved by unsigned UTF-8 lexicographic grant ID,
not database row order. Grant selection is server evidence, not caller intent.
Every grant authorizing the new write (not historical producer evidence) is rechecked in the commit transaction against
revocation and credential validity. Credential validity means the caller's presented generation is the credential's current, unrevoked generation. A rotation or revocation committed before the save denies it. The evidence records the generation used. The generation is evidence, not retry identity. Commit and revocation must share an enforceable
transactional conflict boundary: revocation committed first prevents the save;
a save committed first remains valid historical evidence. A pre-upload check or
cached grant alone is insufficient. Do not hold this transaction during upload.

Retain the exact binary WriterGrant snapshot plus its SHA-256, ID and retained
schema artifact reference as evaluated.
The digest covers snapshot bytes, not mutable revocation state. Immutable snapshots
may be normalized by identity/digest, but revisions must retain their associations.
Historical verification uses retained evidence, not a live grant lookup. Revocation
does not delete prior evidence or rewrite producer ownership.

Authorized intake and steps may create documents. A step creating a derived document
records exact source document/revision lineage; PartSource remains a separate choice
about reusing bytes, not a mandatory copy operation. Part sources and lineage must name
a document in the saving document's account, and the caller must be able to read that
document's current revision, as for a read; otherwise the save is rejected with
SOURCE_NOT_FOUND (decision 37). Every creation under a step grant, derived or not,
requires `allow_create`; `derived_from` records lineage only and grants nothing
(decision 46). There is no intake-only creation restriction.
Changed parts record the SaveOwner; unchanged parts preserve their original writer.
A sourced part counts as changed: it records the SaveOwner and the new revision; the
bytes' producer stays recorded on the source revision.
The accession records its save owner and source lineage, including metadata-only saves.

**Delete and purge authority (decision 36).** A delete needs the document ACL write check
plus either an active intake WriterGrant whose datasource and connector cover the
document's ownership, or process authority for that intake. It is rechecked in the commit
transaction like a save's grant. A step grant cannot delete. Administrator status alone
does not authorize a delete. The tombstone records the evidence in its validation record,
as writer-grant evidence or process-authority evidence; it records no content validation.
Purge is administrator-only and requires the account ACL write check. Process authority
and writer grants cannot purge. Delete and purge authorization failures are
PERMISSION_DENIED and are not recorded, as for saves.

Missing owner is INVALID_ARGUMENT/MALFORMED_REQUEST. An owner with no grant in the
account is FAILED_PRECONDITION/OWNER_UNKNOWN; a revoked grant, invalid credential,
or caller without a matching grant is PERMISSION_DENIED. Error details must not
expose another credential's grants. Unavailable grant/catalog authority is
UNAVAILABLE, never a recorded terminal rejection. Authorization failures do not
reserve operation IDs. Schema-registry failures remain separate from grant lookup.

## 6. Stacks

- **Write-once.** Every newly allocated object gets a new key and is never overwritten.
  Reusing an existing object does not allocate or overwrite a key.
- **Stable references.** The catalog records each object as (stack ID, key, SHA-256,
  length). A stack ID is a catalog record naming one configured storage namespace
  (endpoint plus bucket or container). Pointing configuration at a different namespace
  creates a new stack ID, so historical references never silently follow it.
- **No reliance on provider versioning,** conditional writes or provider-specific copy behaviour.
- **Interface:** streamed put (returns SHA-256 and length), streamed get with optional
  byte range, head, delete, and optional server-side copy as a speed-up only.
- **Carry-forward by reference.** A new accession points at the same key for unchanged
  parts. Nothing is copied or re-uploaded.
- **Keep forever.** Delete is a catalog tombstone. Only an explicit administrator purge removes
  bytes (§5, decision 36). A purge first marks the document's references purged in the catalog. It deletes
  an object's bytes only when no remaining unpurged reference, from any document, still
  points at it. A save that carries forward a reference checks, in its commit
  transaction, that the object is not purged. A read already streaming a purged object
  either completes with verified original bytes or reports a clear failure. There are no
  reader pins.
- **Raw blob claim checks.**
  - Each raw blob in `Document.blob_bag` has its own logical binding to a stack object;
    identical bytes may share that object under the reuse rule below.
  - The catalog binds `blob_id` to its `StackRef` on the BLOBS part entry
    (`StoredPartEntry.blob_refs`, table `catalog_blob_refs`).
  - The server fills `Blob.storage_ref` from that binding. A client can't supply or invent a
    physical storage reference, and a client-supplied `storage_ref` is refused.
  - On read the server also fills `Blob.size_bytes`, `Blob.checksum` and
    `checksum_type = SHA256` from the binding, and `Blob.written` with the revision, commit
    time and step provenance of the write that bound it. A declared size or SHA-256 that
    disagrees with the bound object is refused on save, as is a client-supplied `written`.
  - A client can still reuse authorized existing content without re-uploading it, through
    the supported mechanisms: upload, a `PartSource` source, carry-forward of an unchanged
    part, or blob-level carry-forward: a blob in a BLOBS PUT with no `data` and no
    `storage_ref` that names a blob of the expected revision keeps that blob's binding and
    its `written` stamp. The server resolves and verifies each such reference.
  - **Digest-based PUT reuse.** For `WRITE_ACTION_PUT`, the server reuses identical
    `(SHA-256, length)` bytes from the same document's authorized base revision in the
    selected stack, or from an object newly written by this save in that stack. This
    applies to raw blobs and encoded part fragments, not a global or cross-document
    digest lookup. The server computes identity from the submitted bytes. A PUT still
    records a new revision, updates `written_in_revision` and owner metadata, validates
    the candidate, and emits `CHANGE_KIND_REPLACED` for an existing present part.
    Base references enter the commit's `carriedObjects` purge check; newly written
    shared objects are registered once as new objects. Storage reuse does not make
    a PUT an unchanged part or an operation replay.
  - Blob references count as references for purge (§6) and for the carry-forward purge
    check (§7) (decision 45).
- **No automatic sweeping in the first slice.** An upload can finish and pause before its
  commit, so "unreferenced and old" is not safe to delete. Objects from failed or
  abandoned saves are left in place and can be reported. Automatic cleanup is a later
  feature and needs pending uploads recorded in the catalog and fenced against it.

## 7. Catalog and commit

**Commit.** New part bytes are written to the stacks first. Then one catalog transaction:

1. looks up the operation ID; if an outcome is already recorded, stops and returns it (see Outcomes);
2. checks the document's current revision equals the caller's expected revision ("none" for a new document);
3. inserts the accession (revision row and part references);
4. records the operation outcome, unique by account and operation ID;
5. writes the outbox event;
6. moves the current-revision pointer.

**Outcomes:**
- **Idempotency comes before the revision check.** A retry carries its original, now
  stale, expected revision. It still receives its original outcome, never `ABORTED`.
- The same operation ID with the same intent returns the recorded outcome.
- The same operation ID with different intent fails.
- **Concurrent same-ID saves:** both may upload, and one commits. The other may fail on
  the revision check or on the unique constraint, depending on database ordering. Either
  way, before returning a revision conflict, the save re-reads the outcome for its
  operation ID. Identical intent returns that recorded outcome; different intent fails.
  Its uploaded objects stay unreferenced.
- A stale expected revision returns gRPC `ABORTED`, with the current revision in the
  error details. The client decides what to do. There is no server-side queueing or merge.

**Not used:** leases, execution owners, epochs, coordinator handoff, successor
generations, reader pins.

**Cancellation** is best effort. An outcome lookup by operation ID returns one of three
states:

- **committed**, with the recorded outcome (success, or a recorded failure such as a
  validation rejection);
- **no recorded outcome**, which does not mean the request failed: an outstanding
  request may still commit later;
- **lookup unavailable**, when the catalog cannot be reached.

**Retry identity.** Persisted request metadata, SaveOwner, exact source lineage and
opaque attestations are part of the intent. They must remain identical on retries.
Trace IDs, connection details and transport diagnostics belong in request context,
not persisted request metadata, and do not affect the intent. An authorized replay
returns the recorded result without requiring the original writer grant to remain
active or re-resolving historical authority, and without re-validating content against changed live schemas. Current access is still checked. Stored rejections for an existing document require current read access on replay and outcome lookup. A rejected create whose target still does not exist is accessible only to its original authenticated credential (issuer and credential ID, with a currently valid generation) or process authority; no live writer grant is required. If that target subsequently exists, its current read policy applies. Rejections retain this internal access context; legacy rejections lacking it are process-authority-only. The grant-free replay path applies only when an outcome is already recorded; when no outcome is recorded, the request is authorized in full as a fresh save.

Canonical intent encoding is versioned binary protobuf with operation-kind domain
separation. Emit fields in field-number order, sort metadata by unsigned UTF-8 key,
and normalize unspecified validation mode to TYPED. Preserve write/source order;
reject duplicate selectors. Normalize omitted attestation owner to SaveOwner and
sort attestations by their typed occurrence locator; duplicate locators are invalid.
Bind exact content SHA-256 and length, never generated storage keys or piece layout.
Reject unsupported command fields rather than silently omitting them from identity;
this does not authorize dropping unknown fields from document content. Canonical
encoding requires cross-language fixtures before implementation approval.
Concurrent outcome rereads require a fresh transaction snapshot after the winner
resolves. A failed transaction's snapshot is not proof that no outcome exists.

**Purge concurrency.** Commit and purge must share a transactional conflict point
that prevents new references to an object selected for deletion. A plain read of a
purged flag is insufficient. Specify and test the isolation/conflict behavior for
each catalog. Conditional SQL updates can take row locks; do not call this lock-free.

**Portability.** The commit uses only operations every supported catalog provides. A
store-specific feature such as row locks may improve performance but may not be needed
for correctness.

**Multiple hosts.** Repo hosts share the catalog. The compare-and-set is the only
coordination. There is no document-wide serialization across unrelated documents.
Shared database indexes, connections and storage still contend, and are measured.

**Catalog entries** are rebuildable from stored parts. They never act as the source of truth.

## 8. Reads

- A read resolves the current revision once, or reads an exact revision, never substituting one for the other.
- The returned manifest distinguishes parts not requested from empty or deleted parts.
- Manifest inspection fetches no payloads.
- History is paginated with defined ordering.
- A listing returns each document's lookup fields (`DocumentLookup`, projected from the CORE part and kept on the catalog entry) with its save owner, accepts typed exact-match filters on those fields, the save owner and the commit time, and orders by call number, commit time or title with the call number as tie-break. Filters are protobuf fields, never a query language.
- Large reads stream a pinned manifest and the selected content.
- A revision's retained schema descriptor set is readable by its digest, scoped to that revision and its document's read access; a digest the caller cannot reach and one that does not exist both answer `NOT_FOUND`, and the bytes are verified against the digest before they are returned.

## 9. Validation

- Validate request structure first, then the composed candidate: the changed parts
  together with the carried-forward parts. Use the existing descriptor validation,
  ProtoValidator and CEL.
- Unchanged parts keep their object references and are never re-uploaded. They are read
  only where an applicable validation rule needs their content, such as a cross-part CEL
  rule. Tests account for those reads explicitly.
- Validation runs on every save. Earlier validation results are not reused.
- Store the schema artifact IDs and validation result on the revision. Cache descriptors
  by artifact ID, never by type name.
- Historical reads decode with the schema retained at save time.
- **Typed by default.** An `Any` that cannot be resolved fails the save, with distinct
  errors for missing schema, registry outage and invalid bytes.
- **Explicit opaque occurrences.** OPAQUE_ALLOWED plus host policy permits only
  specifically attested occurrences. Other Any values written by the save remain
  typed. Store the terminal opaque Any's exact type URL and value bytes, SHA-256 and
  length without decoding its value. Failed typed validation never falls back to opaque.
- On initial creation, each new opaque occurrence requires a producing step owner
  authorized for the caller by an active step WriterGrant whose opaque_type_urls
  contains that exact type URL. Intake ownership alone cannot attest opaque content. Intake may
  supply a distinct producing-step attestation only when authorized to do so; otherwise
  the content must be typed. This permits responsible attribution without pretending
  that a grant validates the payload.
- On edits, only previously recorded opaque occurrences may remain opaque, unchanged.
  New or changed Any values must be typed. Each carried occurrence records its exact
  source address, revision and occurrence, with the same type URL and value digest/
  length. The server checks authorized retained evidence and byte identity; a client
  flag or digest claim is insufficient. Opaque content may move within a changed part
  only with that explicit source binding. Purged or missing sources cannot qualify it.
- Whole unchanged parts keep their references without re-decoding just to establish
  carry-forward. Typed sibling content may still be decoded if a cross-part rule needs
  it. Opaque payload bytes are never decoded, including for cross-part validation.
- Structure, envelope, ownership, source access and references are still validated.
  Missing or mismatched opaque ownership fails FAILED_PRECONDITION/OPAQUE_UNOWNED.
  Grant/catalog or schema-registry outages remain UNAVAILABLE and are not terminal
  recorded outcomes.
- Record the effective owner and evaluated WriterGrant evidence with each opaque occurrence.
  Carried occurrences preserve the original producer attribution and grant snapshot.
  A revoked original producer grant does not prevent authorized reuse of previously
  committed opaque content; the new save still needs its own active writer grant,
  current source access and opaque host policy. No re-attestation as newly produced
  content is allowed. The revision is OPAQUE_ACCEPTED, never TYPED_VALID. A rule needing opaque contents is explicitly
  NOT_EVALUATED. Typed reads refuse opaque content unless the caller explicitly asks
  for its opaque representation; that representation makes no typed-valid claim.

## 10. Data formats and events

- **Stored data is binary protobuf.** That includes the manifest, security policy and
  history metadata, which are JSON today and must be migrated.
- **JSON the platform produces to represent a protobuf message** is the canonical proto3
  JSON mapping of a message registered in the schema registry. External protocol
  envelopes keep their own required formats: MCP and A2A JSON-RPC, OpenAPI documents,
  JSON Schema.
  The registry is either protomolt's own, which can act
  as a Confluent- or Apicurio-compatible Kafka schema registry, or an external Confluent
  or Apicurio registry.
- **JSON input** that does not match its registered schema is rejected, never silently dropped.
- **Events.** Every accession writes exactly one durable outbox event, a registered
  protobuf `DocumentEvent`, in the commit transaction. It carries the call number, revision, previous revision, and the changed parts,
  segmentations and embedding sets. Delivery is at-least-once, so the
  same event may arrive more than once; consumers deduplicate by call number plus revision. Tombstones and purges emit their own events. All events have a stable event_id for
  delivery deduplication, derived from the call number and revision through a
  deterministic protobuf encoding, so two call numbers never share an ID whatever
  characters their fields hold. PurgeMarked and PurgeCompleted have distinct event IDs and
  share a purge_id for correlation; purge_id alone is not a deduplication key.

## 11. Size and streaming

- Large single protobuf messages are required, up to the protobuf and gRPC runtime
  limit of about 2 GiB. Measure the exact boundary, including framing overhead, before
  promising a number. Larger content is split across parts or segmentations. No small
  document limit (such as the old 8 MiB buffered limit) may be imposed.
- A host admission budget that content exceeds (fragment or decoded bytes, schema
  bindings, retained artifacts, reservations) is RESOURCE_EXHAUSTED and never a recorded
  outcome; the same operation ID commits on a host with room for the content.
- Raw files of any size are stored as bytes behind claim checks and never decoded.
- A 600 MB raw file must stream in and out with bounded memory.
- Uploads stream through the gRPC UploadDocument stream and through the HTTP upload route. Both
  are surfaces onto the same upload operation, with the same claim check, validation and
  accession behaviour.
- Stack limits, such as Redis's value cap, never become platform limits.

A single UploadDocument stream supplies one raw blob or one encoded codec fragment.
It is bidirectional and answers before any byte is staged. The first frame is the
header, and every admission check (authentication, ownership, owner match, writer
authority, conflict) runs on it. A header whose frame fits the host's maximum message
size may carry the bytes inline with a finished CORE; it commits in that round trip
and is answered saved. Otherwise the header declares a streamed body and is answered
accepted, after which ordered data frames follow, or replayed, when the header alone
fixes an intent already recorded under its operation ID, and no data is sent. A
streamed body may announce a trailer carrying the finished CORE (for example a
classification computed from the bytes); the trailer may change CORE content only,
and ownership derivation, owner match and writer authority run again on it before
the one commit. Client half-close ends the stream and is answered saved. Identity
comes only from the transport credential, never from fields in the request;
anonymous uploads are refused in the first answer. The server derives the fragment
schema from the part layout. Reject duplicate headers, data before a streamed header,
trailers the header did not announce, mismatched digest/length, and incomplete uploads. Other PUTs use the inline Document. Sequential uploads
and SaveDocuments are separate accessions, not atomic multi-payload streaming.
ReadDocument emits a manifest, verified part frames, and a completion frame. A prefix
is not success. Full blob success requires verified bytes and successful RPC completion;
a range is not whole-object integrity proof unless the implementation verifies it.
Streaming transport does not make whole-message CEL evaluation constant-memory;
measure decoded materialization separately from frame buffers.

## 12. Surfaces

gRPC first. `DocumentService` is the only public contract. JSON-facing surfaces are edge
adapters generated from proto descriptors; internal calls stay protobuf over gRPC.

- **Present:** dynamic gRPC with reflection, REST gateway, OpenAPI, JSON Schema, MCP, ACP,
  Kafka Connect, and the HTTP upload route.
- **Planned:** A2A and GraphQL, generated from the same contract.

The separate history and publication gRPC services are removed. History lives on `DocumentService`.

## 13. Later features (not now)

- **Retention.** Delete in-between revisions, keeping the newest and optionally the
  original. It must be an offline job that deletes a part's bytes only when no kept
  revision references them.
- **Composite object references (proposal, not approved for slice 1).** Ordered pieces
  on immutable stack identities, per-piece and whole-object digests, piece-level shared
  reference accounting, and range mapping require a separate reviewed extension.
  StackRef remains single-object in this contract. Do not silently accept multi-piece
  references or claim sequential uploads provide this feature.
- **MongoDB catalog.**
- **Azure stacks.**
- **A2A and GraphQL surfaces.**

## 14. What to keep and what to retire

Build to this document. Keep an existing component only if this design uses it.

**Sequencing.** Write and pass the replacement guarantees first. Retired code is
removed only after the replacement passes the §15 acceptance tests, including the
recovery cases. Before removing a test, check whether it covers a failure case this
document still requires; if so, port it.

**Keep:**
- parts codec and layouts;
- mapper, CEL and projections;
- ProtoValidator and descriptor tools;
- retained-schema resolver and artifact cache;
- blob provider SPI, reduced to the stack interface;
- outbox and event relay;
- HTTP upload route.

**Retire, with their migrations and tests:**
- publication sessions, preparations and journals;
- assessments and evidence lifecycles;
- coordinator reservations, handoff and drains;
- successor generations;
- reader pins and reader-host quiescence/termination;
- capture coverage;
- historical attempt owners;
- exact-version reclamation and provider-generation tracking;
- the history and publication gRPC services.

**Migrations.** The new catalog schema starts from a fresh Flyway baseline in place of
V1 to V120. The baseline applies only to a new, empty catalog. It must refuse to run
against an existing V1 to V120 database. Moving data from an old database, if ever
needed, is a separate explicit tool.

**Tests.** A test is kept only if it checks behaviour this document requires.

## 15. First slice: acceptance

All tests run on the existing docker compose infrastructure. Use LocalStack for S3
correctness and RustFS for performance measurements.

1. Saving one segmentation leaves every sibling part's stack reference unchanged, with no
   PUT on siblings. Any sibling GET is one an applicable validation rule requires, and the
   test asserts exactly those.
2. Adding an embedding set writes exactly one new embedding payload object. A new
   manifest and catalog rows are expected; no other part payload is written.
3. Two concurrent saves with the same expected revision produce one accession and one `ABORTED`.
4. A replay with the same operation ID returns the same outcome, even though its expected
   revision is now stale. Changed intent fails. Two concurrent saves with the same
   operation ID produce one accession, and both callers receive its outcome.
5. An invalid composed document doesn't commit.
6. An opaque save is accepted only when policy allows it. The opaque occurrences are
   recorded, and a rule needing their contents is recorded as not evaluated.
7. Exact int64 values survive save, restart and read. Fields from any registered,
   compatible `Document` version survive save, restart and read byte-for-byte: the
   save decodes against the latest registered version of the `Document` subject and
   records that schema artifact ID on the revision. Fields from no registered version
   are refused with SCHEMA_MISSING. Which versions can be registered is the subject's
   compatibility mode (`core/compat`, the Confluent set); the registry default is
   BACKWARD, which can be changed per subject; the `Document` subject is configured
   BACKWARD_TRANSITIVE (decision 34), so a new `Document` version is checked against
   every registered version. A save decodes typed content against the latest
   registered `Document` version; a field that version does not know, including one
   only an older version had, is refused with SCHEMA_MISSING and never dropped
   (decision 35). Each revision records the schema version it was saved with, and
   reads decode with that version.
8. History pages correctly, and exact-revision reads return the old content.
9. Each accession writes one outbox event listing what changed. An outbox relay retry
   produces duplicate delivery but no duplicate catalog event.
10. A 600 MB raw file streams in and out through both gRPC and HTTP with bounded heap,
    testing upload and download separately, each with backpressure/slow consumption
    and cancellation mid-stream.

**Recovery cases:**

11. Crash after object upload, before commit: no accession is visible, and a retry
    with the same operation ID succeeds.
12. Crash after commit, before the response: a retry returns the committed outcome
    for a caller that can still read the document; a save that removes the caller's
    own read access is confirmed by a principal that can read it.
13. Truncated or corrupt stored content fails the read with a typed error and is never
    returned as valid.
14. A retained schema that is unavailable fails the typed read with a distinct error,
    never a fallback to the live registry.
15. Carrying forward an embedding set whose segmentation changed is rejected.
16. Purging one document leaves an object it shares with another document readable.
17. An outcome lookup for a cancelled request that never committed returns "no recorded
    outcome".

18. Missing owner, no matching owner grant, credential mismatch, revoked grant and
    grant-authority outage are distinct failures; unauthorized attempts cannot claim
    an operation ID. Exercise commit/revocation in both orderings after a long upload.
19. Explicit opaque creation checks producer grant type allowance; edits preserve only
    verified prior opaque bytes. Changed/new opaque attempts fail, and no typed failure
    is converted to opaque success. Retained attribution survives grant revocation and live grant unavailability.
20. Persisted metadata/owner/lineage changes conflict on retry; a different transport
    trace ID does not. Authorized replay does not require the original write grant; current read access
    and caller authentication still apply.
21. Save/purge races on shared objects cannot commit a reference to deleted bytes.
    Exercise both transaction orderings, not just sequential flag checks.

22. Retained grant snapshot digest/ID mismatches fail as DATA_LOSS, without live
    substitution. Rotation keeps the credential's grants, and the new generation writes under them. An old or revoked generation is denied at commit, including when rotation or revocation commits during a long upload.
23. Overlapping grants select deterministically; intake cannot attest opaque content
    without an eligible step grant for its authenticated credential.
24. A process-authority intake save succeeds and records process-authority evidence. A process-authority save naming a step owner, or carrying any opaque attestation, is denied and does not reserve the operation ID.

## 16. Open questions

1. **Archive subsystem (answered, #535).** `ArchiveLedger` and its migrations form a
   second storage model with its own versions and renditions. Answer: keep `ArchiveService`
   as its own service, and store its entries as v2 documents. Archive reads are protomolt
   projections of v2 reads; archive writes are a thin translator into one `SaveDocument`
   or `UploadDocument`, gated by §9 validation. Versions are revisions, history stays in
   the catalog, and provider object versioning is not used (§6). The ledger, its
   migrations, and its own backup and purge paths retire once the replacement passes §15
   on archive traffic. The design of record, including the seven additive v2 changes it
   needs, is "The archive over v2 documents" in `docs/design/archive.md`.
2. **Consumers of the semantic shape.** Replacing `SemanticProcessingResult` (including
   `embedding_config_id`) needs an inventory of its consumers first: the chunker,
   embedders, search indexers, the Qdrant point mapper and Kafka Connect.

These can wait. The storage semantics above come first.

## 17. Document links and subject tags

Status: specification for #537, ahead of any proto change. The concept scheme
model these tags point into is specified in `docs/design/well-known-types.md`,
"Concept schemes (SKOS)". This section owns the repository side: how a document
declares links and tags, what the catalog records, and what reads return.

### Links are typed fields

A link is a field in the document's typed content (the CORE part, including a
typed `structured_data` payload) whose message type is `DocumentLink` and whose
descriptor carries the link field option. The descriptor, not the value, names the
relation, so the catalog projection records edges by walking the retained schema
the revision was validated with. Links live in a new file
`repo/v2/links.proto`, because both target kinds are repository identities
(`NodeAddress` and the #527 `DocumentFilter`):

```proto
// ai/protomolt/proto/repo/v2/links.proto
extend google.protobuf.FieldOptions {
  // Marks a DocumentLink field (singular or repeated) as a link.
  LinkField link = 59100551;
}

// What a link field means.
message LinkField {
  // The relation, a link_relation name: a Dublin Core term ("references",
  // "isPartOf", "replaces", "isReplacedBy", "requires", "source", ...), an IANA
  // registered relation ("alternate", "canonical", "describedby", ...), or an
  // absolute URI for an extension relation (RFC 8288).
  string relation = 1 [(ai.protomolt.proto.validate.v1.field) = {
    required: true
    string: {link_relation: true}
  }];
  // Protobuf full names of the structured_data types a static target may hold.
  // Empty means any target document, typed or opaque.
  repeated string target_types = 2 [(ai.protomolt.proto.validate.v1.field).repeated = {
    unique: true
    items: {string: {protobuf_fqn: true}}
  }];
}

// One link from the document that holds it to another document.
message DocumentLink {
  option (ai.protomolt.proto.validate.v1.message).cel = {
    id: "link-target-required"
    message: "a link names a static or a dynamic target"
    expression: "has(this.static_target) || has(this.dynamic_target)"
  };
  oneof target {
    // A fixed call number, optionally pinned to one revision.
    StaticTarget static_target = 1;
    // A catalog selector, resolved when the link is read.
    DynamicTarget dynamic_target = 2;
  }
  // Display text for the link; not part of its identity.
  optional string label = 3 [(ai.protomolt.proto.validate.v1.field).string = {min_len: 1, max_len: 1000}];
}

message StaticTarget {
  // The target's call number. Its account_id equals the linking document's.
  ai.protomolt.proto.repo.v1.NodeAddress address = 1 [(ai.protomolt.proto.validate.v1.field).required = true];
  // A pinned revision. Absent means the target's current revision.
  optional int64 revision = 2 [(ai.protomolt.proto.validate.v1.field).int64.gt = 0];
}

message DynamicTarget {
  // The #527 listing filter; at least one field set.
  DocumentFilter filter = 1 [(ai.protomolt.proto.validate.v1.field).required = true];
  // The order resolved targets come back in.
  DocumentSort sort = 2;
}
```

The extension number `59100551` is the next free block after `metric.v1`
(`59100541`/`59100542`). `link_relation` is a new Tier 1 named format
(`StringRules` field 37; well-known types, "Concept schemes (SKOS)"): a linear-scan check against the
bundled Dublin Core and IANA relation names, or a parse as an absolute URI. A
relation is a validated string, not an enum, so a new relation is not a schema
migration.

Schema rules, checked when rules compile (registration and validator build):

- A `DocumentLink` field without the `link` option fails compilation, and so does
  a `link` option on a field of any other type. Map fields cannot be links.
- One message may hold several link fields with the same relation; each field is a
  separate edge source.
- Inverse relations are not stored. A document that `isPartOf` another does not
  make the other `hasPart` it; the inverse is a backlink query (below). The
  bundled vocabulary pairs Dublin Core inverses (`isPartOf`/`hasPart`,
  `replaces`/`isReplacedBy`, `references`/`isReferencedBy`, `requires`/
  `isRequiredBy`) for display only.

### Occurrence identity

One link or tag occurrence is named by `(field_path, ordinal)`, and the same pair is
used in `catalog_document_links`, `catalog_document_concepts`, `ListLinks` and
`ResolveLink`:

- `field_path` is the field names from the CORE root, joined by `.`, with `[i]`
  after every repeated ancestor segment: `sections[0].cites`,
  `sections[3].appendix.sources`, `structured_data.chapters[1].cites`. The leaf
  link field carries no index.
- `ordinal` is the element index inside the leaf field when it is repeated, and 0
  when it is singular.
- A typed `google.protobuf.Any` is entered without a segment of its own: the path
  continues with the packed message's field names, as in `structured_data.cites`.
  A repeated `Any` ancestor takes `[i]` like any repeated field. An opaque `Any`
  occurrence (§9) is not decoded, so no link or tag inside it is projected.
- Map values are not entered. A `DocumentLink` or a `concept_schemes`-bound
  `ConceptRef` reachable only through a map value fails rule compilation, so no
  map key ever appears in an occurrence identity.

Two occurrences in one revision never share a pair, because every repeated level
on the way is indexed. Indices are positions in the stored revision, so an
occurrence identity names a place in one revision, not a link that persists
across revisions; inserting a section moves the indices of the sections after it.

### Static targets

A static target names a call number and optionally pins a revision.

- **Same account.** The target address's `account_id` must equal the linking
  document's account. A link is a reference, not a grant: cross-account links
  wait for the cross-account read model and are rejected until then.
- **Checked at save, not held.** Save validation reads the target's catalog rows as
  for a `PartSource` (decision 37): the target must exist, must not be purged, and
  the saving credential must be able to read it. When `target_types` is non-empty,
  the type of the revision the link addresses must be listed: the target's current
  revision for an unpinned link, the pinned revision for a pinned one. The current
  type is not substituted for a pinned revision's.
- **Type evidence per revision.** Each revision records `structured_data_type` on
  `catalog_revisions` (and on `RevisionRecord` and `StoredRevision`): the full name
  in the type URL of CORE's `structured_data` when that payload was admitted typed
  (§9), and absent when CORE has no `structured_data`, when the payload is an
  opaque occurrence, and for a tombstone. The check reads this column, never part
  bytes. A catalog rebuild recomputes it from each revision's CORE and retained
  schema, as it recomputes every other catalog projection. An absent type matches
  no `target_types` entry, so an opaque or untyped target is accepted only by a link
  field whose `target_types` is empty.
- **Rejection reasons.** A failure rejects the save with the new reason
  `LINK_TARGET_NOT_FOUND` (missing, purged and unreadable collapse to one reason,
  as `NOT_FOUND` does for reads) or `LINK_TARGET_TYPE` (type not allowed). A pinned
  revision must exist. A tombstoned target is accepted: the link is born dangling.
- **No commit coupling.** The target check is a point-in-time read outside the
  commit transaction. A target tombstoned or purged after the check makes the link
  dangling; it does not invalidate the linking revision. No lock is taken on the
  target and no target row is written.
- **Carried links.** A save that carries CORE forward unchanged does not re-check
  its links' targets; validation of the composed candidate still runs (§9), and a
  rule that needs the target's state is not part of this design.

### Dynamic targets

A dynamic target is a `DocumentFilter` plus optional `DocumentSort`, resolved when
the link is read and not materialized anywhere.

- Resolution runs the #527 listing in the linking document's account, under the
  reading credential's access, at read time. Two readers with different access see
  different targets; the same reader sees today's matches, not the matches at save.
- Resolution never includes tombstoned documents and pages like `ListDocuments`.
- A dynamic link writes no edge row, so it never appears as a backlink: a backlink
  would need every selector in the account evaluated against every save.
- Save validation checks only the filter's own declared rules (#527 contradiction
  rules included). The selector is not evaluated at save.

### Catalog edge table

Static links project into a catalog table on each accession, in the commit
transaction, next to the lookup rows:

```
catalog_document_links (
  document_id          bigint  -- linking document (catalog_documents)
  field_path           text    -- occurrence path, ancestor indices included,
                               -- e.g. "sections[0].cites"
  ordinal              int     -- element index in the leaf field, 0 if singular
  relation             text
  target_account_id    text
  target_graph_id      text
  target_graph_address_id text
  target_doc_id        text
  pinned_revision      bigint NULL
  PRIMARY KEY (document_id, field_path, ordinal)
)
index on (target_account_id, target_graph_id, target_graph_address_id,
          target_doc_id, relation)
```

- **Current revision only.** Rows describe the linking document's current revision,
  as `catalog_document_metadata` does. A new accession replaces the document's rows;
  its tombstone or purge clears them. Historical links are read from the historical
  revision's CORE with its retained schema.
- **Targets are addresses, not foreign keys.** The target columns are a call
  number. A link row never references `catalog_documents`, so tombstoning or
  purging a target writes nothing here and nothing cascades.
- **Rebuildable.** Every row is derived from the current CORE part and its retained
  schema (§7, "Catalog entries are rebuildable from stored parts"). A rebuild walks
  current revisions and rewrites the rows; a rebuilt table equals the committed
  one. The table is not the source of truth.
- **Dangling is computed, not stored.** Target state comes from a join to
  `catalog_documents` at read time.

### Reads

Two reads on `DocumentService`, both protobuf, both access-filtered:

- **`ListLinks(address, optional revision)`** decodes the selected revision's CORE
  with its retained schema and returns each link with its field path, relation and
  target. For a static target it adds the target state; for a dynamic target it
  returns the selector, and resolution is a separate paged call
  (`ResolveLink(address, revision, field_path, ordinal, page)`).
- **`ListBacklinks(target address, optional relation, page)`** reads
  `catalog_document_links` for edges into the target, and returns only edges whose
  linking document the reader can read. Order is linking call number, then field
  path, then ordinal.

Target state on a static link:

| State | Meaning |
|---|---|
| `target_tombstoned = true` | The target's current revision is a tombstone and the reader can read it. A pinned revision stays readable, since revisions are kept until purge. |
| `target_unavailable = true` | Missing, purged, or not readable by this reader: one state, so a link does not reveal what access hides. |
| neither | The target is current and readable. |

A link whose target is tombstoned or unavailable is **dangling**. Dangling links
are shown, not removed: deleting a document never edits, tombstones or purges the
documents that link to it.

### Events

A link change is a CORE change, announced by the linking document's normal
`DocumentEvent` (§10). A target's tombstone emits its own `Tombstoned` event and
nothing for its linkers; a consumer that tracks dangling links joins the event with
`ListBacklinks`.

### Attachments stay blobs

A file that shares the document's lifecycle (saved, revised and deleted with it) is
a blob in `Document.blob_bag`, behind its claim check (§6). A link is for another
document with its own call number, revisions and owner. An attachment is not
promoted to a linked document to give it a relation, and a link does not make its
target an attachment.

### Subject tags

A document is tagged with concepts through `ConceptRef` fields (well-known types,
"Concept schemes (SKOS)"), bound to their schemes in the schema. The repository's
part is the catalog:

- Tags project into `catalog_document_concepts (document_id, field_path, ordinal,
  scheme_id, concept_id)` on each accession, current revision only and rebuildable,
  as links do. Only concept ids are stored: no labels, no ancestors, no expansion.
- `DocumentFilter` gains a concept filter (`ConceptRef` plus a `ConceptExpansion`)
  that the catalog answers by expanding against the mounted schemes at query time,
  then matching stored ids. A scheme change needs no catalog rewrite.

### Implementation issues

Filed from this section once approved; each lands proto first.

1. `link_relation` format in `core/formats`, with the bundled Dublin Core and IANA
   relation tables and NOTICE attribution.
2. `repo/v2/links.proto`: `LinkField`, `DocumentLink`, targets; compile-time rules;
   occurrence identity and the map-value rejection; `LINK_TARGET_NOT_FOUND` and
   `LINK_TARGET_TYPE` reasons; per-revision `structured_data_type` on
   `catalog_revisions`, `RevisionRecord` and `StoredRevision`, rebuilt from CORE.
3. Save-time static target check and the `catalog_document_links` projection, with
   rebuild.
4. `ListLinks`, `ResolveLink` and `ListBacklinks`, with target state and access
   filtering. Dynamic resolution depends on #527.
5. `catalog_document_concepts` and the `DocumentFilter` concept filter (depends on
   the concept scheme mount).
