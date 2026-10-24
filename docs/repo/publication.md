# Save and read document parts

`ai.protomolt.proto.repo.v2.DocumentService` is the canonical public
document API — the parts-based Repository Service v2 described in
[the design requirements](../design/parts-based-repository-requirements.md)
(v2.5; section references below point there). The wire contract is
[document_service.proto](../../repo/proto/src/main/proto/ai/protomolt/proto/repo/v2/document_service.proto)
with [manifest.proto](../../repo/proto/src/main/proto/ai/protomolt/proto/repo/v2/manifest.proto),
[errors.proto](../../repo/proto/src/main/proto/ai/protomolt/proto/repo/v2/errors.proto)
and [events.proto](../../repo/proto/src/main/proto/ai/protomolt/proto/repo/v2/events.proto);
host and storage configuration is in the
[repository guide](../../repo/README.md). The v1 `DocumentService` is
retired (#320 Steps 4–5); nothing in this guide refers to it.

A document is addressed by its **call number**, the four-segment
`NodeAddress` (`doc_id | graph_address_id | account_id | graph_id`), and
stored as independently addressable protobuf **parts**, each one write-once
stack object under the save's drive:

| Part | Selector | Content |
|------|----------|---------|
| CORE | `part: DOCUMENT_PART_CORE` | Everything not assigned to another part |
| BLOBS | `part: DOCUMENT_PART_BLOBS` | The blob bag: claim checks for raw files |
| PARSED | `part: DOCUMENT_PART_PARSED` | Parser results |
| CHUNKS | `segmentation_id: "<id>"` | One object per segmentation |
| EMBEDDINGS | `embedding_set: {segmentation_id, embedding_set_id}` | One object per embedding set |

A `PartSelector` names exactly one stored object. A request that names the
same selector twice, or names one part in two contradicting ways (a write
and a source, a write and a read selection), is rejected. CHUNKS and
EMBEDDINGS are only ever selected by their keys, never by the `part` arm.

Every commit is one catalog transaction: object writes land first on
write-once stacks, then the revision, its outcome and exactly one outbox
event commit atomically (compare-and-swap on the expected revision). A save
that fails before commit records nothing (some validation rejections are
the exception: they are recorded outcomes, see below).

## Saving

### `SaveDocument`

`SaveDocumentRequest` carries:

- **`operation_id`** — client-chosen, unique per account, non-blank, at
  most 200 characters. The terminal outcome is recorded under
  `(account_id, operation_id)`; retrying with the same ID and the same
  intent fingerprint replays the recorded outcome (`replayed = true`)
  instead of writing again. Reusing the ID with different content is a
  conflict. A caller that crashes without learning the result resolves the
  ambiguity with `GetOperationOutcome`, never by blind retry under a new ID.
- **`target`** — either a full `NodeAddress` (pipeline saves) or
  `use_datasource_id` (an intake save: the address becomes
  `(document.doc_id, datasource, ownership.account_id, "intake:<account>")`,
  which requires a CORE PUT since identity and ownership are CORE content).
- **`expected`** (`ExpectedRevision`) — `none` (the document must not exist
  yet) or an exact `revision`. There is no unconditional save: a mismatch
  fails `ABORTED` carrying a `RevisionConflict` detail (current revision,
  whether it is a tombstone), and records no outcome, so the standard
  read-modify-write retry re-derives the expectation and may reuse the same
  `operation_id`.
- **`document`** — content for every PUT in `writes`, and nothing else.
  Content addressed to a part not listed as PUT is rejected, never silently
  dropped.
- **`writes`** (`PartWrite`) — the parts this save changes, each with a
  `WriteAction`: `PUT` (new bytes from `document`), `EMPTY` (present and
  intentionally empty; no object), or `DELETE` (gone from this revision on;
  requires a `deleted_reason`).
- **`sources`** (`PartSource`) — parts taken from an exact revision of
  another document (cross-document sourcing): the new accession references
  the same stack object, nothing is copied. The source must share the
  saving document's `doc_id` and account, and the caller must be able to
  read the source's current revision, or the save is rejected with
  `SOURCE_NOT_FOUND` (no existence leak) — `SOURCE_PURGED` if the source
  was purged between check and commit.
- **`owner`** (`SaveOwner`) — who is writing: an intake origin (datasource,
  optional connector) or a producing step. Step saves are authorized
  against writer grants (`WriterGrant`, retained per revision as
  `WriterGrantEvidence`); document ownership itself never transfers.
- **`drive`**, **`validation_mode`** and **`opaque_attestations`** — the
  named storage for new objects, and admission (below).
- **`derived_from`** — exact source revisions a derived document claims
  lineage from (same account/readability rules as sources).
- **`metadata`** (field 99) — persisted request metadata; part of the retry
  identity, returned by `GetDocument`. Transport tracing does not belong
  here.

**Partial saves and carry-forward.** A save lists only the parts it
changes. Every part neither written nor sourced **carries forward
unchanged** from the expected revision — same object, same bytes, original
`written_in_revision` and writer in the manifest. Omitting a part is never
a delete; use `EMPTY` or `DELETE` to say so explicitly. This replaces the
v1 `parts_written`/`copy_unwritten_parts_from` model: the base is always
the single revision named by `expected`, so carry-forward can never mix
parts from different revisions, and a stale expectation fails instead of
silently rebasing.

**Raw blobs (claim checks).** File bytes never ride inside the document
payload. The BLOBS part is a bag of blob metadata; on save the server
extracts inline blob data to its own stack object and stores only the claim
check (`FileStorageReference`) in the fragment. Typed reads return blobs
with the claim check filled and the bytes elided; fetch the bytes with
`ReadBlob`. A repeated write of byte-identical content reuses the base
revision's object (still a `REPLACED` change with a new revision).

A blob in a BLOBS PUT that carries no `data` and is not the streamed upload
names a blob of the expected revision by its `blob_id`: the save keeps that
blob's binding without resending or copying its bytes, and the commit fails
as `SOURCE_PURGED` if the object was purged meanwhile. A `blob_id` the
expected revision does not have, or any such blob on a create, is a
malformed request. Reads fill each blob's `size_bytes`, `checksum` (SHA-256
hex) and `checksum_type` from its binding, and `written` with the revision,
commit time and producing step of the write that bound it; a carried blob
keeps its original `written`. A save that sets `written`, a `checksum_type`
other than SHA-256, or a `size_bytes` or `checksum` that disagrees with the
bytes is refused: as a malformed request for inline and carried blobs, and
as a recorded `UPLOAD_MISMATCH` for a streamed blob. `schema_subject` is
recorded as sent and not enforced.

**Response.** `SaveDocumentResponse` carries the `operation_id`, the
committed `Accession` (address, revision, previous revision, commit time,
per-part changes), and `replayed`.

**Save byte budget.** After splitting the request's `Document` into part
fragments and extracting any inline raw-blob bytes, a unary save reserves
those fragment and raw-blob byte lengths (plus transient retained-schema
evidence during admission) against a dedicated save-side byte budget of
`DOCUMENT_PLATFORM_READ_BUDGET_BYTES` until the commit finishes; on
`UploadDocument`, a streamed encoded part reserves per received 64 KiB
chunk while a streamed raw blob flows straight to the stack. Exhaustion
fails fast before any stack write (and on unary `SaveDocument`, before any
drive registration) with `RESOURCE_EXHAUSTED` (`Save buffer budget
exhausted`), leaving the `operation_id` unrecorded. Because the save-side
budget is a separate pool
of `DOCUMENT_PLATFORM_READ_BUDGET_BYTES` alongside the read-side
`ReadByteBudget` (`Read buffer budget exhausted`), concurrent read and save
paths can hold up to `2 * DOCUMENT_PLATFORM_READ_BUDGET_BYTES` in combined
reservations (and a unary save's reservation is taken after the request
message has been split and inline blobs extracted).

### Bulk and streamed saves

- **`SaveDocuments`** (bidirectional stream) — each item is a full
  `SaveDocumentRequest` plus a stream-local `request_id`. Every request
  yields exactly one response (`saved` or `ItemError`, correlated by
  `request_id`); one document's failure never fails the stream, and item
  side effects are exactly `SaveDocument`'s.
- **`UploadDocument`** (bidirectional stream): one `UploadHeader` (the
  `SaveDocumentRequest` to commit, what the bytes are, and how they
  arrive), answered before any byte is staged. An `inline_data` body
  commits in that round trip and is answered `saved`. A `streamed` body is
  answered `accepted` (data frames follow, then an `UploadTrailer` when the
  header set `core_in_trailer`, then half-close, answered `saved`) or
  `replayed` (the header's declared length and SHA-256 match an intent
  already recorded under its operation ID; no data is sent). Every admission
  check runs on the header, and owner match and writer authority run again
  on the trailer's finished CORE, which may not change the address,
  ownership or any other part. The payload is either a **raw blob** (`blob_id`:
  the save's document carries that blob's metadata without data, writes
  include BLOBS PUT, and the server fills the claim check) or **one encoded
  codec fragment** (`encoded_part.selector`: the stream supplies that
  selector's PUT, and the document must not also carry it inline).
  `declared_length` distinguishes an expected zero-byte object from unknown
  length; `expected_sha256` is verified while streaming. The HTTP
  `POST /v1/documents:upload` route is a surface onto this operation (see
  the repository README for its parameter form).

### Delete, purge, outcomes

- **`DeleteDocument`** writes a **tombstone accession**: itself a revision
  with an `operation_id` and an exact expected revision (deleting with
  `none` is malformed). No bytes are removed. Deleting a never-saved or
  purged document is `NOT_FOUND`; deleting an already-tombstoned document
  conflicts as `ABORTED` with `current_is_tombstone`.
- **`PurgeDocument`** is the administrative erasure: idempotent by
  `purge_id`, it marks the document's references purged in one catalog
  transaction and deletes only objects no unpurged reference still points
  at (a later call resumes an interrupted purge). It requires a
  credential-bound administrator; process authority cannot purge, and the
  mounted service denies all purges until an admin identity is bound.
- **`GetOperationOutcome`** is three-state: `committed` (an `Accession` or
  a recorded `SaveRejection` — validation rejections are committed
  outcomes), `none` (nothing recorded; an outstanding request may still
  commit), or the status `UNAVAILABLE` (the lookup itself cannot answer).

## Reading

All reads resolve the revision once (`RevisionSelector`: unset or
`current` = latest; an exact revision that does not exist is `NOT_FOUND`,
never substituted), check the caller's account and document ACL, and
re-verify retained bytes against the manifest before answering. Reading a
tombstoned document's current revision is `NOT_FOUND`; its earlier
revisions remain readable when addressed exactly.

**Selection** (`PartSelection`) is shared by `GetDocument`, `ReadDocument`
and bulk reads: unset or empty means every part; `selectors` names parts
explicitly; `all_segmentations` / `all_embedding_sets` include every keyed
object without naming IDs the caller has not discovered yet (use
`GetManifest` to discover them). The returned manifest always lists every
part of the revision in codec order with state (`PRESENT`/`EMPTY`/`DELETED`),
size, SHA-256, `written_in_revision`, writer, and `content_included`
marking what the selection actually returned.

- **`GetDocument`** — unary; returns the document assembled from the
  selected parts only, the manifest, the revision's `RevisionValidation`,
  and the persisted request `metadata`.
- **`ReadDocument`** — server stream: one manifest frame pinned to the
  resolved revision, then each selected part's stored fragment bytes in
  ≤64 KiB `PartData` frames (server-side verified as a whole before its
  first frame), then `ReadComplete`. A prefix without the completion frame
  is not success. Fragment bytes are the stored codec fragments, not a
  reassembled document. Verified reads reserve against a host byte budget;
  exhaustion fails fast with `RESOURCE_EXHAUSTED`.
- **`ReadDocuments`** — bulk form of `ReadDocument`: request-items in,
  frames/errors out, correlated by `request_id`. Its part bytes are
  claim-checked like `GetDocument` (blob `storage_ref`s filled), unlike the
  unary stream's stored fragments.
- **`ReadBlob`** — one raw file by `blob_id`, with optional `offset`/`length`
  byte range (0 length = to the end). The `BlobHeader` frame carries the
  resolved revision, total length, whole-object SHA-256, MIME type and
  filename; a full read ends with a SHA-256 check, and a mismatch ends the
  stream with `DATA_LOSS`.
- **`GetManifest`** — manifest + validation record only; fetches no
  payloads (`content_included` false throughout). Each manifest has the
  accession's `committed_at`, and each part entry has `written_at`, the
  commit time of the revision in `written_in_revision`.
- **`ListRevisions`** — history newest-first, `page_size` 1–1000 with
  opaque page tokens; each summary names revision, predecessor, commit
  time, committing operation, owner, tombstone flag, validation verdict,
  and the parts that revision changed.
- **`ListDocuments`** — current documents in one account, optionally scoped
  by `graph_address_id` (datasource) and/or `graph_id`;
  `include_tombstoned` adds documents whose current revision is a
  tombstone. Paginated like `ListRevisions`. There is no cross-account or
  drive-scoped listing: drives are a write-time storage choice, not part of
  a document's identity.

### Links

A link is a `DocumentLink` field in CORE, including the typed
`structured_data` payload, whose descriptor carries the
`(ai.protomolt.proto.repo.v2.link)` option (`repo/v2/links.proto`). The option
names the relation and, optionally, the `structured_data` types a static target
may hold. A `DocumentLink` field without the option, the option on any other
field, or a link anywhere under a map value refuses the save as
`VALIDATION_FAILED`.

A link is named by `(field_path, ordinal)`: field names from the CORE root
joined by `.`, with `[i]` after each repeated ancestor
(`structured_data.sections[0].cites`), and the index inside a repeated link
field. A typed `Any` nested in `structured_data` is entered without a segment
of its own (`structured_data.nested.cites`); an opaque one is not read. On a save that writes or sources CORE, each static target must be in
the same account, exist unpurged, be readable by the saving caller and hold
the pinned revision when one is named, or the save is rejected
`LINK_TARGET_NOT_FOUND`; when the field lists `target_types`, the addressed
revision's admitted type must be listed, or it is `LINK_TARGET_TYPE`. Both are
recorded `FAILED_PRECONDITION` outcomes. A tombstoned target is accepted and
the link dangles. A dynamic target's filter must set a field and be
consistent; it is not evaluated at save. A pinned revision must be a
document revision, not a tombstone. A link must also fit the catalog's edge
table: a field path of at most 1024 UTF-8 bytes, and a relation plus target
call number of at most 2048; a larger one is refused `VALIDATION_FAILED` with
rule `link-catalog-storable`. Revisions committed before the link migration
have no recorded `structured_data_type` until `CatalogEntryRebuild` runs, so a
link with `target_types` to such a document is refused `LINK_TARGET_TYPE`
until then.

- **`ListLinks`** — a revision's links, decoded with its retained schema. A
  static link reports `target_tombstoned` or `target_unavailable` (missing,
  purged or unreadable, one state) for this reader; a dynamic link returns
  its selector.
- **`ResolveLink`** — one page of a dynamic link's targets: its filter and
  sort run as `ListDocuments` in the linking document's account under the
  reader's access, tombstones excluded. A static link is `FAILED_PRECONDITION`.
- **`ListBacklinks`** — static links into a document from current revisions
  the reader can read, optionally of one relation, ordered by linking call
  number, field path and ordinal.

### Concept tags

A concept tag is a value of a `ConceptRef` field in any part, including the
typed `structured_data` payload in CORE and parser outputs in PARSED, whose
field rules bind it to concept schemes (`concept_schemes`, well-known types
"Concept schemes (SKOS)"); only CORE's are catalogued. A save decides each
tag's membership against the concept schemes the host mounts;
`DocumentSaves` takes the host's `ConceptSchemeCatalog`. The stock repository
loads configured scheme files at boot from
`DOCUMENT_PLATFORM_CONCEPT_SCHEMES`, or its alias
`PROTOMOLT_REPO_CONCEPT_SCHEME_PATHS`. Embedding hosts can provide
`RepoServiceConfig.conceptSchemePaths` or call `RepoServices.withConceptSchemes`.
Without configured paths or supplied mounts, no schemes are mounted.
A concept the mounted scheme lacks is `concept.member` and a
bound scheme the host has not mounted is `concept.unmounted`, both
`VALIDATION_FAILED`. A tag is read as the canonical `ConceptRef` and held to
its own rules, whatever a self-contained payload schema declares under that
name: a value whose ids are not strings at fields 1 and 2 is `concept.shape`,
and one that does not re-parse cleanly or breaks the canonical rules fails the
save as content. Reads and catalog
rebuilds decode stored content without deciding membership again, so a host
that has not mounted a scheme still reads what was admitted. Tags in a typed
nested `Any` are read as links are, without a path segment for the `Any`. A
tag a map value reaches through a typed `Any`, or a CORE tag whose field path
is over 1024 UTF-8 bytes (`concept-catalog-storable`), fails the save.

A tag in another part, such as a parser's output in PARSED, is checked the
same way (membership against the mounts, the canonical `ConceptRef`) and stored
with that part, but it is not catalogued, so a concept filter does not match
it. PARSED is the parser's claim-check output; the mapping step carries an
interpretation worth filtering on into CORE. During a rolling upgrade over a
shared store, a host built before `#656` that reads a PARSED part containing a
bound `ConceptRef` saved by a newer host refuses it in
`DocumentReads.decodeRetained` (`ContentRefused`, surfacing as `DATA_LOSS`), so
readers should be upgraded before writers emit bound `ConceptRef` tags in
PARSED.

Tags project into `catalog_document_concepts` under the same
`(field_path, ordinal)` identity as links: current revision only, replaced by
a save that writes or sources CORE, kept by one that carries it, cleared by a
tombstone or purge, and rebuilt by `CatalogEntryRebuild`. Only the tagged ids
are stored.

`DocumentFilter.concept` (a `ConceptRef` plus a `ConceptExpansion`) is
expanded against the mounted schemes when each page runs (`DocumentLists`
takes the host's `ConceptSchemeMounts`), then matched against the stored
pairs, in `ListDocuments` and in `ResolveLink` for a dynamic link whose filter
has one. The page's `concept_evidence` names each consulted scheme's mounted
version and the mappings skipped. A host without mounts, or a queried scheme
that is not mounted, is `FAILED_PRECONDITION`; a concept the scheme lacks, or
an expansion over the mount's bound, is `INVALID_ARGUMENT`. A scheme change
needs no catalog rewrite.

## Admission: schema, validation and opaque content

Every save is admitted before anything is written:

1. **Structure** — proto validation (required fields, CEL message rules)
   plus service checks (selector uniqueness, content only for PUTs,
   write/source overlap). Failures are `INVALID_ARGUMENT` with a
   `SaveRejection` (`MALFORMED_REQUEST`) detail and record no outcome.
2. **Schema (CEL) admission** — the composed candidate (written + sourced +
   carried parts, so cross-part rules see the whole document) is decoded
   fragment-by-fragment under retained schemas, every `Any` occurrence is
   resolved and recorded (`ResolvedOccurrence`), and document rules are
   evaluated by CEL. Semantic parts get their own checks: a segmentation's
   segments must match the text they span (its digest is computed
   server-side), and an embedding set must bind to the segmentation digest
   and segment IDs it claims. Legacy `semantic_results` writes are refused.
3. **Authorization** — the caller's account, the document ACL, and the
   writer grant for step saves (checked live at commit, retained as
   `WriterGrantEvidence`).

Two recorded verdicts exist. **`TYPED_VALID`** means every `Any` resolved,
decoded and validated. **`OPAQUE_ACCEPTED`** is only reachable with
`validation_mode = VALIDATION_MODE_OPAQUE_ALLOWED` *and* host policy
permitting it: each unresolvable occurrence must be named by an
`OpaqueAttestation` (typed `OccurrenceLocator` steps + exact type URL),
attributed to a producing step under an active writer grant, or carried
forward byte-identically from a previously committed occurrence
(`carried_from`); the bytes are stored undecoded and never reported as
typed-valid. Unattested opaque content is a validation rejection. Reads of
a part holding opaque occurrences fail typed reads unless `allow_opaque`
is set, and corrupt retained grant evidence fails reads closed
(`DATA_LOSS`).

Validation rejections (schema, semantic, payload rules) are **recorded
outcomes**: they commit a `SaveRejection` under the operation ID and answer
`FAILED_PRECONDITION`, so a retry with the same ID and intent replays the
rejection instead of re-running admission. Malformed requests, permission
failures and revision conflicts record nothing — with one recorded
exception: `OPAQUE_NOT_ALLOWED` maps to `PERMISSION_DENIED` and **is**
recorded, whereas `WRITE_DENIED` is unrecorded.

A field violation's `field_path` is rooted at the part's container message
in every part, the way link and concept-tag paths are: a payload rule that
fails inside CORE's typed `structured_data` reports
`structured_data.<field>`, and one inside a parser's output reports
`parser_results["<parser>"].document.shape.<field>`. Crossing a nested `Any`
adds no segment of its own.

Errors are gRPC statuses carrying the `errors.proto` detail messages
(`SaveRejection`, `RevisionConflict`, `OperationReuse`, `ItemError`, field
violations) in the status details; on bulk streams the same refusal is one
item's `ItemError`, never a stream failure.
