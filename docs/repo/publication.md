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

**Response.** `SaveDocumentResponse` carries the `operation_id`, the
committed `Accession` (address, revision, previous revision, commit time,
per-part changes), and `replayed`.

### Bulk and streamed saves

- **`SaveDocuments`** (bidirectional stream) — each item is a full
  `SaveDocumentRequest` plus a stream-local `request_id`. Every request
  yields exactly one response (`saved` or `ItemError`, correlated by
  `request_id`); one document's failure never fails the stream, and item
  side effects are exactly `SaveDocument`'s.
- **`UploadDocument`** (client stream) — one `UploadHeader` (the
  `SaveDocumentRequest` to commit, plus what the bytes are), then data
  frames, then half-close. The payload is either a **raw blob** (`blob_id`:
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
  payloads (`content_included` false throughout).
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
failures and revision conflicts record nothing.

Errors are gRPC statuses carrying the `errors.proto` detail messages
(`SaveRejection`, `RevisionConflict`, `OperationReuse`, `ItemError`, field
violations) in the status details; on bulk streams the same refusal is one
item's `ItemError`, never a stream failure.
