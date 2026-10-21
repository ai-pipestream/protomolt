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
  - A client can still reuse authorized existing content without re-uploading it, through
    the supported mechanisms: upload, a `PartSource` source, or carry-forward of an unchanged
    part. The server resolves and verifies each such reference.
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
- Uploads stream through gRPC client streaming and through the HTTP upload route. Both
  are surfaces onto the same upload operation, with the same claim check, validation and
  accession behaviour.
- Stack limits, such as Redis's value cap, never become platform limits.

A single UploadDocument stream supplies one raw blob or one encoded codec fragment:
exactly one header, ordered data, then client half-close. The server derives the
fragment schema from the part layout. Reject duplicate headers, mismatched digest/
length, and incomplete uploads. Other PUTs use the inline Document. Sequential uploads
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

1. **Archive subsystem.** `ArchiveLedger` and its migrations form a second storage model
   with its own versions and renditions. Fold raw-file archival into documents, or keep it
   separate and frozen?
2. **Consumers of the semantic shape.** Replacing `SemanticProcessingResult` (including
   `embedding_config_id`) needs an inventory of its consumers first: the chunker,
   embedders, search indexers, the Qdrant point mapper and Kafka Connect.

These can wait. The storage semantics above come first.
