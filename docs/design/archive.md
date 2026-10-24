# The archive: entries, renditions, and retained versions

The [repository requirements](parts-based-repository-requirements.md)
record the next storage-module boundaries, schema enforcement, ownership checks
and metadata-versioning work. It distinguishes existing behavior from additions;
those additions are not yet available APIs.

The archive is the repository family's generic document store: named
collections of entries, each entry a set of independently addressable
**renditions** — the raw file, a parsed protobuf, markdown, NDJSON, parquet,
anything — with attached metadata, retained version history, and three ways
in (gRPC unary, gRPC client-streaming, HTTP POST). It runs on the same
engine as the pipeline document store — drives, blob stores, the Postgres
ledger, the transactional outbox — and is served by the same
`protomolt-repo-service` binary, standalone or embedded.

The design premise: when you have a single document you tend to grow it.
A source file arrives; a parse produces a protobuf; a conversion produces
markdown; an extraction produces structured metadata. Those are one entity
wearing different formats, and the archive models exactly that — one entry,
many renditions, every state change versioned and evented.

## One spine

The archive has exactly one identity model:

    account → archive → entry → version → rendition

An **archive** is an account-scoped named collection bound to one drive (the
drive resolves storage coordinates; nothing but repo-service speaks the
object store). An **entry** is one logical document inside an archive,
addressed by a caller-chosen `entry_id`. A **version** is one immutable
stored state of an entry. A **rendition** is one named byte object (or
sub-keyed object family) inside a version.

The entry's storage identity is a deterministic name-based UUID over
`(account_id, archive, entry_id)`. It is simultaneously the ledger primary
key, the object-key prefix, and the natural event partition key. Identity is
never random: re-saves are idempotent by construction, retries converge on
the same row and the same keys, and the same coordinate can never mint two
entries.

There is no node tree, no path hierarchy, no second document model, and no
search syntax in the contract. Hierarchy, when a consumer wants one, is a
metadata convention (`path` is just a metadata key); search is the search
service's job, fed by archive events. The storage contract stays a storage
contract.

## Renditions

A rendition is opaque bytes with a descriptor. The archive never parses,
splits, or interprets rendition content — that is what keeps it generic, and
what keeps a C++ or Rust implementation of the same contract honest.

Each rendition carries:

- **`name`** — the open vocabulary (`original`, `parsed`, `markdown`,
  `metadata.parquet`, whatever the producer means). Names are caller-chosen
  slugs; the archive imposes no closed enum.
- **`media_type`** — the IANA media type of the bytes.
- **`schema_subject`** — optional: for protobuf renditions, the schema
  registry subject that pins which message the bytes decode as. The archive
  records the pin; it does not enforce it. Enforcement, when wanted, is a
  registry-fed gate composed in front of the door, not storage's job.
- **`sub_key`** — optional: multi-object renditions (one object per page,
  per chunk set, per partition) store one object per sub-key under one
  rendition name.

Every rendition object in a version's manifest carries its size, SHA-256,
object key, write provenance, and a three-state lifecycle:
`PRESENT` (bytes exist), `EMPTY` (the write carried no bytes; no object
exists), `DELETED` (bytes deliberately removed; size, hash, and a named
reason are retained as provenance). The map still lists the file, the file
is gone, and the hash proves what it was — that is what makes redaction and
retention auditable.

Provenance is stamped at write time and never invented: the writer's
declared identity (`module`, `actor`) rides the manifest entry it wrote;
re-referenced renditions keep their original stamp; unknown stays blank.

## Versions and entry-local content addressing

Versioning is a **per-archive policy**, declared at archive creation:

- **`VERSIONING_NONE`** — an entry has one retained state. Saves bump the
  version counter (the optimistic-concurrency token) and superseded objects
  are deleted as part of the save's settling. The counter still tells you
  how many times the entry changed; the bytes do not accumulate.
- **`VERSIONING_RETAINED`** — every save lands a new immutable version.
  Old versions list, read, and prune explicitly. Nothing is silently
  rewritten.

The mechanism that makes retained versioning cheap is **entry-local content
addressing**. A rendition object's key is derived from its own content
hash, scoped under the entry:

    <drive.prefix>/archive/<accountId>/<archive>/<entryUuid>/<rendition>[/<subKey>]/<sha256>

A version's manifest references objects by these keys. When a new version
changes one rendition out of five, the four unchanged renditions have
unchanged hashes, therefore unchanged keys, therefore the new manifest
simply re-references the existing objects — no copy, no new bytes. When the
bytes did change, the new object lands beside the old one and both versions
stay fully readable.

Content addressing is deliberately **entry-local**, not store-global.
Global content-addressed stores need reference-counted garbage collection —
a coordination protocol with its own failure modes. Entry-local sharing
bounds every deletion question to one entry's own version manifests: prune
version N by deleting the keys in N's manifest that no other retained
manifest of the same entry references. That is a scan over one entry's
rows, not a store-wide GC.

A save whose root checksum (SHA-256 over the ordered rendition hashes)
matches the entry's current version is elided entirely and answers
`deduplicated=true` with the existing coordinates.

## The manifest is the contract

Each version's manifest is the authoritative list of that version's
objects: rendition descriptors, states, sizes, hashes, exact object keys,
provenance. It lives in the ledger (a JSONB column beside the version row)
because it must be transactionally consistent with the version's existence.

Everything operational derives from the manifest's exact key list, never
from prefix listing: deletion deletes the manifest's keys, reconciliation
diffs the bucket against the union of manifest keys, and a reader holding a
manifest knows byte-for-byte what a version contains before fetching
anything. Prefix sweeps are never safe — prefixes nest.

## Three doors, all built

The contract publishes no capability the service does not implement. Every
RPC below ships with the service, or it is not in the proto.

**Unary `PutEntry`.** Metadata plus inline renditions in one request, one
new version. The payload transits memory, so this is the door for small and
medium bodies. Rendition writes are verified: the server computes each
SHA-256 and sends it as the object store's checksum trailer, so the store
rejects landed bytes that mismatch.

**Client-streaming `UploadRendition`.** The bulk door over gRPC. The first
frame is a header — entry address, rendition descriptor, declared
`size_bytes` (required; the honest mirror of the HTTP route's
`Content-Length` contract), optional expected SHA-256 — and every following
frame is a chunk. The server streams the body through a digest as it
uploads. With a declared hash, bytes stream directly to their final
content-addressed key with the checksum trailer enforcing it. Without one,
bytes stream to a staging key under the entry (`.../staging/<uploadId>`),
and the completed digest settles them onto the final key by server-side
copy. A declared size or hash that does not match what arrived fails the
call and best-effort deletes the landed object. A crashed upload leaves a
staging object with no owner — an orphan by the standing rule, reclaimed by
the reconciler's min-age sweep. Completion lands one new version whose
manifest re-references every other current rendition, so a large upload
never copies its siblings.

**HTTP `POST /v1/archive:upload`.** The zero-dependency door: the request
body is the bytes, identity rides query parameters or headers (query wins) —
`account_id`, `archive`, `entry_id`, `rendition` (default `original`),
`filename`, `Content-Type`, optional `X-Content-Sha256`. `Content-Length`
is required (411 without it); the body streams through a digest straight to
storage with no buffering; a checksum mismatch is a 400 and the landed
object is best-effort deleted. Success returns a JSON receipt: entry UUID,
version, rendition, size, SHA-256, object key. A producer that must know
nothing about protomolt uses this door and still gets every guarantee.

## Reads

**Unary `GetEntry`.** One version's manifest plus the selected renditions'
bytes in one response. The bytes transit memory, so this read suits small
renditions.

**Server-streaming `ReadRendition`.** One rendition object (name and
sub-key) of one version, whole or as a byte range (`offset`, `length`). The
first frame is a header with the version, the manifest's descriptor, the
whole object's size and SHA-256, the served range and `whole_object`; data
frames of at most 64 KiB follow, sent only while the client's flow control
allows, so a slow client stalls the provider read instead of queueing the
object. A whole-object read is hashed as it streams and the stream ends in
`DATA_LOSS`, not success, when the digest or length disagrees with the
manifest. A range read is checked for its length only and is never integrity
proof for the object. Bound objects read their exact published version under
a read pin held until the stream ends; each open stream reserves a two-frame
window on the host read byte budget. The bounded unary profile does not
serve it.

## Metadata

An entry carries typed first-class fields — `title`, `filename`,
`content_type`, `source_uri`, `source_modified_at` — plus the extension
map (`map<string, string>`, field 99 by repository convention) for the long
tail. Structured metadata that deserves a schema is not squeezed into the
map: it is stored as a rendition with a `schema_subject` pin, which keeps
it validatable, evolvable, and independently fetchable. The map is for
labels; renditions are for data.

## Events

Archive mutation events are a deliberate future extension, designed here
and absent from the contract until built (rule 7). They ride the engine's
transactional-outbox pattern: the outbox row commits in the same
transaction as the ledger mutation, a relay publishes to Kafka through the
protomolt serde, delivery is at-least-once with event-id dedupe, and
events carry identifiers and state transitions, never bodies — a consumer
that needs bytes calls back through the API, because events that carry
whole documents race their own truth. The vocabulary: `EntrySaved` (with
the new version and root checksum), `RenditionDeleted` (the tombstone,
with its reason), `EntryDeleted`, `VersionPruned`. What stands between
the design and the door is generalizing the relay, which is typed to the
document store's event message today.

## Stats

The archive counts what it does, exactly, in the ledger: per-archive
transactional counters (entries, versions, retained bytes, current-version
logical bytes) and a per-rendition-name breakdown (object count, bytes),
maintained in the same transaction as the mutation they describe.
`GetArchiveStats` reads them with strong consistency — the ledger is the
contract, the way a receipt is: what the store claims, the store can prove
from its own rows. Operation telemetry (reads, latencies, cache behavior)
is deliberately not ledger state; it belongs to the metrics lane, fed by
events and process metrics, and never blocks or widens a storage
transaction.

## Durable deletion and recovery

ArchiveMutationService accepts an operation UUID and one delete-entry,
delete-rendition, or prune-versions command. It commits the logical changes,
counters, immutable command identity, and exact cleanup targets in one SQL
transaction. Physical cleanup follows outside that transaction at each object's
original backend identity. A provider failure records a retryable observation;
it cannot roll back the logical admission or report physical completion.

The receipt separates logical removal from pending and confirmed-absent objects.
Repeating the same operation and command returns its original logical outcome
with the current durable observation, even after the entry address is recreated.
Reusing the UUID for another command conflicts. GetArchiveMutation permits lookup
after a lost response. COMPLETED describes recorded confirmation, not a fresh
provider probe; reconciliation handles late writes using retained cleanup state.

Publication pins and cleanup claims fence each other. An object selected for
cleanup cannot acquire a new retained reference. Legacy content without verified
immutable storage bindings is refused, never resolved through today's drive.
The earlier object-first RPCs and SQL helpers have been removed. Their request
payload messages remain inputs to the identified mutation contract.

Managed composition and explicit caller authority are required. Scoped ownership
checks are still pending. Generic blob mutations reserve the exact `archive`
path segment, protecting both earlier and current archive layouts from overwrite
or deletion through that API, including drive aliases. Administrative reads
remain available. Direct provider SPI access stays trusted; the guard is a
repository API boundary, not a storage-provider permission system.

## The archive over v2 documents (specification, #535)

This section answers requirements §16 question 1 and is the design of record
for the archive's storage. `ArchiveService` stays its own service with its own
read budget and contract. Its entries, renditions and versions are stored as
v2 documents through `DocumentService`; the `ArchiveLedger` storage model, its
migrations and its separate backup and purge paths are retired once the
implementation passes the requirements' §15 acceptance on archive traffic.
Until then the sections above describe the running service. Provider object
versioning is never used: stack keys are write-once (requirements §6).

The archive keeps three layers apart:

| Layer | Mechanism | Where the mapping lives |
|---|---|---|
| Reads | protomolt projection of v2 read responses | `(sources)`, `(from)`, `(default_from)` on the archive view messages |
| Writes | thin command translator into one v2 save | Java, one class per archive command |
| Gate | v2 save validation (§9) and `validate.v1` on the projected view | the v2 contract and the archive view `.proto` |

### 1. Identity: archive coordinates onto call numbers

An entry address maps onto one `NodeAddress`, field by field, with no hashing:

| `NodeAddress` | Value | Why |
|---|---|---|
| `account_id` | `EntryAddress.account_id` | the tenant root, as everywhere |
| `graph_id` | `"archive:" + account_id` | a reserved graph namespace, parallel to `intake:<account>` |
| `graph_address_id` | `EntryAddress.archive` | the archive name, a slug |
| `doc_id` | `EntryAddress.entry_id`, verbatim | `Document.doc_id_derivation.method = CALLER_PROVIDED` |

The archive host writes only under `archive:<account>` graphs, and a save from
any other surface naming an `archive:` graph is refused by name, exactly as a
pipeline save naming an `intake:` graph is. The mapping is a pure field copy,
so every archive view field that names the entry is a `(from)` path and a read
needs no lookup table. Event IDs, outcome keys and partitions already derive
from the call number through deterministic protobuf encoding (§10), which
holds whatever characters `entry_id` carries.

`EntryInfo.entry_uuid` was the ledger's primary key and object-key prefix.
Under v2 neither exists: the call number is the identity and stack keys are
server-allocated. The implementation reserves the field (and
`PutEntryResponse.entry_uuid`, `UploadRenditionResponse.entry_uuid`) rather
than keep a second identity alive, and the `archive.snapshot_identity` rule on
`VersionManifest` drops its `entry_uuid` clause.

Ownership: each archive is bound at creation to one intake datasource of its
account (a new required `Archive.datasource_id`). Entries carry
`OwnershipContext{account_id, datasource_id}` and archive saves name
`SaveOwner.intake{datasource_id}`, so v2's writer-grant and process-authority
rules (§5) authorize archive writes with no archive-specific authority model.
A `WriteAttribution.module` maps to `SaveOwner.step{module_id}` and then needs
a step `WriterGrant`, as any step save does. `WriteAttribution.actor` has no v2
counterpart: the authenticated caller is already recorded as grant or
process-authority evidence, so the implementation removes `actor` from the
write contract instead of storing a self-declared name beside it.

### 2. Renditions onto blobs and parts

Every rendition is one stored object of one revision. The mapping from
`(name, sub_key)` is deterministic and total:

| Rendition | v2 storage | `blob_id` / selector |
|---|---|---|
| `parsed`, no sub-key | the PARSED part | `PartSelector{part: DOCUMENT_PART_PARSED}` |
| any other name, no sub-key | one raw blob in `blob_bag.blobs.blob` (BLOBS part) | `blob_id = name` |
| any other name with a sub-key | one raw blob in `blob_bag.blobs.blob` | `blob_id = name + "/" + sub_key` |

- `name` and `sub_key` are `path_safe_name`, which admits no `/`, so the
  separator is unambiguous and `blob_id` stays within the catalog's 1024-byte
  bound (two 200-byte halves plus one byte).
- The archive always writes the `blobs` arm of `BlobBag`, even for one blob,
  so readers have one shape. A document whose BLOBS part uses the singular
  `blob` arm was not written by the archive and is refused as an archive entry
  by name.
- `media_type` maps to `Blob.mime_type`; the `original` rendition's
  `filename` maps to `Blob.filename`. `PutEntryRequest.filename` describes
  the original file, so a request that sets it with no `original` rendition
  written or carried is `INVALID_ARGUMENT` naming `filename`.
  `UploadRenditionHeader.filename` likewise names the original file: a nonblank
  value requires the `original` rendition, is limited to 500 code points by both
  validation dialects, and participates in retry identity. Blank preserves a
  stored filename. The filename is stored on the streamed blob in the same revision
  and is available to characterization.
- `parsed` is the PARSED part, not an opaque blob. Its bytes are the encoded
  PARSED fragment (a `Document` carrying `doc_id` and `parser_results` only;
  v2 fragment confinement requires and stores `doc_id`, which equals the
  entry's `entry_id`), typed-validated by v2 like any parser output. `parsed`
  with a sub-key, or `parsed` bytes carrying any field other than `doc_id` and
  `parser_results`, or a `doc_id` other than the entry's, is `INVALID_ARGUMENT` naming
  the rendition. Every other rendition stays opaque bytes the archive never
  interprets.
- `schema_subject` maps to a new optional `Blob.schema_subject`, recorded and
  not enforced, as today. Registry enforcement stays a gate composed in front
  of the service.
- `content_profile` (`ai.protomolt.proto.asset.v1.ContentProfile`) on a
  rendition is stored on `Blob.metadata` (`google.protobuf.Struct`) under field
  `archive.content_profile` as a string `Value` holding the Base64-encoded
  deterministic binary protobuf bytes of the `ContentProfile`. Because
  blob-level carry-forward preserves `Blob.metadata`, carried renditions keep
  their `content_profile` across subsequent saves unless that rendition slot is
  overwritten.
- An inline rendition written with zero bytes (`RenditionContent.data` empty on
  `PutEntry`, or a 0-byte derived rendition from `BridgeEntry`) binds a 0-byte
  object on the `BLOBS` part and projects to `RENDITION_STATE_EMPTY` with an
  empty `sha256`.

Entry metadata maps onto CORE:

| Entry field | CORE field |
|---|---|
| `title` | `search_metadata.title` |
| `content_type` | `search_metadata.source_mime_type` |
| `source_uri` | `search_metadata.source_uri` |
| `source_modified_at` | `search_metadata.last_modified_date` |
| `created_at` | `search_metadata.creation_date`, stamped on the creating save and carried forward |
| `metadata` (field 99) | `search_metadata.metadata` (user keys; keys starting with `archive.` are reserved and refused at the door with `INVALID_ARGUMENT`) |
| `classification` | `search_metadata.metadata["archive.classification"]` (Base64-encoded deterministic binary protobuf `Classification`) and `search_metadata.metadata["archive.classification_state"]` (`ClassificationState` enum name, indexed into `DocumentLookup.metadata` for `ListEntries` filtering) |
| `declared`, `origin` | `structured_data`, an `Any` of `ArchiveEntryFacts`, typed-validated |

**v2 prerequisites.** The archive's existing guarantees need seven additive
changes to the v2 contract, specified here so the implementation issues can
cite them:

1. **Blob-level carry-forward.** In a BLOBS PUT, a blob with no `data` and no
   `storage_ref` whose `blob_id` names a blob of the expected revision keeps
   that blob's existing binding: no bytes sent, no copy, the reference enters
   the commit's carried-object purge check (§6, §7). Today every non-uploaded
   blob in a BLOBS PUT needs inline data, so adding a markdown rendition beside
   a 600 MB original would resend the original. This is a fourth reuse
   mechanism beside upload, `PartSource` and unchanged-part carry-forward.
2. **Server-filled blob facts.** On read the server fills `Blob.size_bytes`,
   `Blob.checksum` and `checksum_type = SHA256` from the claim-check binding,
   as it fills `storage_ref`. On save, a declared size or SHA-256 that
   disagrees with the bound object is refused. The binding also records the
   revision and commit time that bound it and the step provenance when a step
   wrote it, exposed read-only on a new `Blob.written`
   (`BlobWrite{revision, committed_at, ai.protomolt.proto.repo.v1.WriteProvenance step}`)
   and kept across carry-forward, so a re-referenced rendition keeps its
   original stamp. A client-supplied `Blob.written` is refused like
   `storage_ref`.
3. **`Blob.schema_subject`**, above.
4. **`DocumentManifest.committed_at`**, the accession's commit time, so a
   manifest read can say when its revision landed without a history read.
5. **`PartManifestEntry.written_at = 11`** (fields 9 and 10 are `opaque` and
   `content_included`; `StoredPartEntry.entry` is this message, so the stored
   form gains it too): the commit time of the revision that wrote the part.
   It follows `written_in_revision` exactly: set at commit when a part is
   written, kept across carry-forward, and restamped with the sourcing
   commit's time when the part comes from `PartSource`. A PARSED part written at
   revision 1 and carried through revision 2 then still reports revision 1's
   time; `DocumentManifest.committed_at` is the current accession's time and
   is not a substitute.
6. **Recorded unchanged outcomes.** `SaveDocumentRequest` gains
   `UnchangedPolicy unchanged` (`UNCHANGED_POLICY_UNSPECIFIED = 0`: a new
   revision for every PUT, as today; `UNCHANGED_POLICY_RECORD_OUTCOME = 1`).
   It is part of the save's intent. Under `RECORD_OUTCOME`, a save is
   unchanged only when every `PartWrite` is `WRITE_ACTION_PUT` of a part the
   expected revision holds as `PART_STATE_PRESENT`, each PUT's stored
   fragment SHA-256 and length equal that part entry, every raw blob binds
   the expected revision's object, and the save names no `PartSource`,
   `derived_from` or opaque attestation. Any `WRITE_ACTION_DELETE` or
   `WRITE_ACTION_EMPTY`, of a part in any state, and any PUT of a part the
   expected revision holds as `PART_STATE_EMPTY` or `PART_STATE_DELETED`,
   commits a new revision and never takes the unchanged path: a DELETE-only
   save, or an unchanged CORE PUT beside the removal of a populated PARSED
   part, applies the removal. For an unchanged save the commit transaction
   runs steps 1, 2 and 4 of §7 only: it records the operation outcome as a
   new arm of the `RecordedOutcome.result` oneof, `Accession unchanged = 5`
   beside `Accession accession = 1` and `SaveRejection rejection = 2`,
   holding the expected revision's `Accession` (the message's
   `recorded-outcome-required` rule admits it as a third arm). The save is
   answered with a new `SaveDocumentResponse.unchanged = true`, a separate
   response field. It inserts no accession, writes no outbox event and
   leaves the current-revision pointer alone. Because the unchanged arm is
   the persisted outcome, a same-key retry replays it as
   `SaveDocumentResponse.unchanged = true`, which the archive answers as
   `deduplicated = true`, and does not advance `current_revision`.
   Authorization, the in-transaction grant recheck and validation of the
   request structure run exactly as for any save, and the expected-revision
   check makes it race-safe: a writer that commits first turns the same
   request into an `ABORTED` or, with a fresh expected revision, a real
   revision. Uploaded objects of an unchanged save stay unreferenced, as for
   any save that does not commit them (§6).
7. **Recorded request metadata.** `RecordedOutcome` gains
   `map<string, string> request_metadata = 4`: the persisted request metadata
   (§7) of the save that recorded the outcome, on the accession, unchanged and
   rejection arms alike. A rejection has no revision to read metadata from, so
   this is the only place a recorded rejection keeps its intent. It is read
   under the same access rules as the outcome itself (§7, "Retry identity").

### 3. Read side: projections over v2 reads

Each archive read is one v2 read per revision it serves, then one projection
into the archive view. Every per-rendition fact, including when a carried
part or blob was written, is on that revision's manifest and blob rows
(prerequisites 2 and 5), so no read walks history. Nested projections (#568)
let a list of one message type build a list of another: a field whose message
type declares `(sources)` gets a child projection, applied element by element.
#568 also converts enum values across enum types by number (a `PartState`
value into a `RenditionState` field) and a numeric `default_from` literal
into an enum value by number; a number with no value in the target enum fails
the projection naming the field. The view messages declare their sources and
mappings on the `.proto`:

```proto
// Rows of VersionManifest.renditions: one per raw blob and one for PARSED.
message RenditionManifestEntry {
  option (ai.protomolt.proto.projection.v1.sources) = {
    source: "ai.protomolt.proto.repo.v1.Blob"
    source: "ai.protomolt.proto.repo.v2.PartManifestEntry"
  };
  RenditionDescriptor rendition = 1 [(ai.protomolt.proto.projection.v1.from) = {
    cel: "source"  // the child RenditionDescriptor projects from the same element
  }];
  // PartState and RenditionState share numbers; a blob in the document is PRESENT.
  RenditionState state = 2 [
    (ai.protomolt.proto.projection.v1.from) = {paths: {path: "state"}},
    (ai.protomolt.proto.projection.v1.default_from) = {literal: {number_value: 1}}
  ];
  int64 size_bytes = 3 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "size_bytes"}
  }];
  string sha256 = 4 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "checksum" path: "sha256"}
  }];
  // Blob rows: written.committed_at (prerequisite 2); PARSED: written_at (prerequisite 5).
  google.protobuf.Timestamp written_at = 6 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "written.committed_at" path: "written_at"}
  }];
  string deleted_reason = 7 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "deleted_reason"}
  }];
  WriteAttribution written_by = 8 [(ai.protomolt.proto.projection.v1.from) = {
    cel: "source"
  }];
}

message RenditionDescriptor {
  option (ai.protomolt.proto.projection.v1.sources) = {
    source: "ai.protomolt.proto.repo.v1.Blob"
    source: "ai.protomolt.proto.repo.v2.PartManifestEntry"
  };
  string name = 1 [
    (ai.protomolt.proto.projection.v1.from) = {cel: "source.blob_id.split('/')[0]"},
    (ai.protomolt.proto.projection.v1.default_from) = {
      cel: "source.selector.part == 4 ? 'parsed' : ''"
    }
  ];
  string sub_key = 2 [(ai.protomolt.proto.projection.v1.from) = {
    cel: "source.blob_id.contains('/') ? source.blob_id.split('/')[1] : ''"
  }];
  string media_type = 3 [
    (ai.protomolt.proto.projection.v1.from) = {paths: {path: "mime_type"}},
    (ai.protomolt.proto.projection.v1.default_from) = {
      cel: "source.selector.part == 4 ? 'application/x-protobuf' : ''"
    }
  ];
  string schema_subject = 4 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "schema_subject"}
  }];
}

message WriteAttribution {
  option (ai.protomolt.proto.projection.v1.sources) = {
    source: "ai.protomolt.proto.repo.v1.Blob"
    source: "ai.protomolt.proto.repo.v2.PartManifestEntry"
  };
  string module = 1 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "written.step.module_id" path: "owner.step.module_id"}
  }];
}

message VersionManifest {
  option (ai.protomolt.proto.projection.v1.sources) = {
    source: "ai.protomolt.proto.repo.v2.GetDocumentResponse"
  };
  EntryAddress address = 1 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "manifest.address"}
  }];
  int64 version = 2 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "manifest.revision"}
  }];
  string root_checksum = 3 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "validation.candidate_sha256"}
  }];
  // total_bytes = 4 is host-filled: a sum over the rows, and CEL has no fold.
  google.protobuf.Timestamp created_at = 5 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "manifest.committed_at"}
  }];
  // Raw blobs first, in stored order, then the PARSED part when present.
  repeated RenditionManifestEntry renditions = 6 [(ai.protomolt.proto.projection.v1.from) = {
    cel: "dyn(source.document.blob_bag.blobs.blob) + dyn(source.manifest.parts.filter(p, has(p.selector.part) && p.selector.part == 4))"
  }];
  EntryInfo metadata_snapshot = 7 [(ai.protomolt.proto.projection.v1.from) = {
    cel: "source"
  }];
}

message EntryAddress {
  option (ai.protomolt.proto.projection.v1.sources) = {
    source: "ai.protomolt.proto.repo.v1.NodeAddress"
  };
  string account_id = 1 [(ai.protomolt.proto.projection.v1.from) = {paths: {path: "account_id"}}];
  string archive = 2 [(ai.protomolt.proto.projection.v1.from) = {paths: {path: "graph_address_id"}}];
  string entry_id = 3 [(ai.protomolt.proto.projection.v1.from) = {paths: {path: "doc_id"}}];
}

message EntryInfo {
  option (ai.protomolt.proto.projection.v1.sources) = {
    source: "ai.protomolt.proto.repo.v2.GetDocumentResponse"
    source: "ai.protomolt.proto.repo.v2.DocumentSummary"
  };
  EntryAddress address = 1 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "manifest.address" path: "address"}
  }];
  int64 current_version = 3 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "manifest.revision" path: "revision"}
  }];
  string title = 4 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "document.search_metadata.title" path: "lookup.title"}
  }];
  string filename = 5 [(ai.protomolt.proto.projection.v1.from) = {
    cel: "source.document.blob_bag.blobs.blob.filter(b, b.blob_id == 'original').map(b, b.filename).join('')"
  }];
  string content_type = 6 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "document.search_metadata.source_mime_type" path: "lookup.source_mime_type"}
  }];
  string source_uri = 7 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "document.search_metadata.source_uri" path: "lookup.source_uri"}
  }];
  google.protobuf.Timestamp source_modified_at = 8 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "document.search_metadata.last_modified_date" path: "lookup.last_modified_date"}
  }];
  google.protobuf.Timestamp created_at = 9 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "document.search_metadata.creation_date" path: "lookup.creation_date"}
  }];
  google.protobuf.Timestamp updated_at = 10 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "manifest.committed_at" path: "committed_at"}
  }];
}

// ListVersions rows: one per revision, projected from history.
message VersionSummary {
  option (ai.protomolt.proto.projection.v1.sources) = {
    source: "ai.protomolt.proto.repo.v2.RevisionSummary"
  };
  int64 version = 1 [(ai.protomolt.proto.projection.v1.from) = {paths: {path: "revision"}}];
  int64 previous_version = 2 [(ai.protomolt.proto.projection.v1.from) = {paths: {path: "previous_revision"}}];
  google.protobuf.Timestamp created_at = 3 [(ai.protomolt.proto.projection.v1.from) = {paths: {path: "committed_at"}}];
  string module = 4 [(ai.protomolt.proto.projection.v1.from) = {paths: {path: "owner.step.module_id"}}];
  bool deleted = 5 [(ai.protomolt.proto.projection.v1.from) = {paths: {path: "tombstone"}}];
}
```

The snippet shows the mappings only; the implementation keeps every existing
`validate.v1` annotation on these messages and every field comment.

Calls, each one v2 read pinned to one revision (§8):

| Archive call | v2 call | Projection |
|---|---|---|
| `GetEntryManifest(v)` | `GetDocument` at revision `v`, selection CORE + BLOBS, PARSED manifest only | `GetDocumentResponse` to `VersionManifest`; `info` from the current revision, the same read when `v` is current |
| `GetEntry(v)` | as above, then `ReadBlob` per selected PRESENT rendition (PARSED part bytes for `parsed`) | the same projection; bytes are attached, never projected |
| `ReadRendition` | `ReadBlob` (or the PARSED part frames from `ReadDocument`) | header projected from the manifest row; bytes stream through unchanged |
| `ListVersions` | `ListRevisions` | each `RevisionSummary` to `VersionSummary` |
| `ListEntries` | `ListDocuments(account_id, graph_id = "archive:<account>", graph_address_id = archive, filter)` | each `DocumentSummary` to `EntryInfo` through its `DocumentLookup` (#527) |

- A BLOBS fragment holds blob metadata only, so reading it fetches no raw
  bytes and `GetEntryManifest` stays a manifest inspection.
- `ListVersions` returns `VersionSummary` rows, not full manifests. A revision
  summary carries no rendition rows, and building them would read every
  revision's BLOBS fragment. A caller that wants a version's manifest asks
  `GetEntryManifest(v)`. This replaces `ListVersionsResponse.versions`
  (`repeated VersionManifest`) and needs the buf breaking waiver in the
  contract PR.
- `ListEntries` filters and sorts through the typed `DocumentFilter` and
  `DocumentSort`, not a query language. When
  `ListEntriesRequest.classification_state` is set, the translator attaches a
  `MetadataMatch` on `archive.classification_state` equal to the requested
  `ClassificationState` enum name. `ListEntriesRequest.include_manifests`
  is served by `GetEntryManifest` per row until the catalog listing returns
  BLOBS rows itself.
- `root_checksum` becomes the revision's `candidate_sha256`: one digest of
  the composed candidate, computed by v2, rather than a second digest the
  archive maintains. `RenditionManifestEntry.object_key`,
  `storage_object_id` and `UploadRenditionResponse.object_key` are not
  populated: stack references are internal (§5, §6).
- Four fields stay host-filled, each named in the view `.proto` with its
  reason: `EntryInfo.metadata` (projection v1 refuses map fields;
  `DocumentLookup.metadata` is filled the same way, with reserved `archive.*`
  keys stripped); `VersionManifest.total_bytes` (a sum over the `PRESENT`
  rows, and CEL has no fold); `EntryInfo.classification` (decoded from
  `search_metadata.metadata["archive.classification"]` on document reads and
  `lookup.metadata["archive.classification"]` on listing rows); and
  `RenditionManifestEntry.content_profile` (decoded from
  `Blob.metadata.fields["archive.content_profile"]`).
- `ListEntries` rows have no `filename`: `DocumentLookup` projects CORE only,
  and the filename is on the `original` blob. A listing that needs it adds a
  BLOBS-derived lookup field to `DocumentLookup`, which is #527's contract.

**Validation of the view.** Every projected message is validated against its
own `validate.v1` annotations (`path_safe_name`, `mime_type`, `sha256_hex`,
`slug`, length bounds, the CEL rules on `VersionManifest` and
`RenditionReadHeader`) before it is returned. A projection that produces an
invalid view is `DATA_LOSS` naming the field and the rule, because the stored
revision passed v2 validation and the view must not invent a different truth.
No protovalidate, buf.validate or hand-written regex is involved.

**What the implementation pins first.** The `renditions` rule concatenates two
lists of different message types under `dyn`. The implementation's first test
projects a revision with two blobs and a PARSED part through the real view
`.proto` and asserts every row, including a raw blob row whose `state` comes
from the `number_value: 1` fallback and a PARSED row whose `state` converts
from `PartState`. A second test writes PARSED at revision 1, carries it
through revision 2, and asserts the PARSED row's `written_at` is revision 1's
commit time. If the CEL runtime refuses the heterogeneous
list, the PARSED row moves to its own `parsed` field on `VersionManifest`
(still a projection), not into code.

### 4. Write side: translator into one v2 save

Projections are one-way and whole-message; `UploadRendition` is a client
stream of bytes. A write projection would cover only `PutEntry`'s header and
leave the stream in code, two mechanisms for one write path. Each archive
write is instead one small hand-written translator that builds exactly one v2
command, and v2 save validation (§9: structure, then the composed candidate
with descriptor validation and CEL) is the only gate. Nothing bypasses it, and
the translator holds no rules v2 does not also enforce.

`PutEntry` becomes one `SaveDocument`:

| `SaveDocumentRequest` | From |
|---|---|
| `operation_id` | `PutEntryRequest.operation_id`, or derived (section 5) |
| `target.address` | the call number of section 1 |
| `expected` | `expected_version > 0`: `revision = expected_version`; `0`: the current revision read by the translator; for a deleted entry, the deletion's revision, since v2 keeps the tombstone as the current revision and answers `none` with `ABORTED`; `none` only when `ListRevisions` finds no revision |
| `document` | CORE fields of section 2; `blob_bag.blobs.blob` with every written rendition inline and every other current blob listed by `blob_id` only (prerequisite 1); `parser_results` from `parsed` |
| `writes` | `PUT` CORE; `PUT` BLOBS when any blob rendition is written; `PUT` PARSED when `parsed` is written |
| `owner` | `SaveOwner` of section 1 |
| `validation_mode` | `TYPED` |
| `unchanged` | `UNCHANGED_POLICY_RECORD_OUTCOME` (prerequisite 6) |
| `metadata` | `archive.content_sha256` (section 5) |

`UploadRendition` becomes one `UploadDocument`: the header frame carries the
same `SaveDocumentRequest` with `UploadHeader.blob_id` naming the streamed
blob (or `encoded_part{selector: PARSED}` for `parsed`), `declared_length =
size_bytes` and `expected_sha256` unchanged. A rendition of at most 1 MiB is
held in memory and sent as `inline_data` with its finished CORE, one round
trip. A larger one sends its header with a `streamed` body before any chunk
and forwards chunks as data frames only after v2 answers `accepted`; for the
primary rendition the header sets `core_in_trailer` and the finished CORE
follows in an `UploadTrailer`. Its
`expected` follows the `PutEntry` row above, deleted entries included. v2
already streams through a digest, refuses length and digest mismatches and
leaves an uncommitted object unreferenced, so the archive's staging key and
settle copy are retired.

- **Outcome lookup comes first.** Every write resolves its `operation_id`
  (section 5) and asks `GetOperationOutcome(account_id, operation_id)` before
  it reads anything else. A recorded outcome is the answer, compared by the
  archive intent it recorded (prerequisite 7), never by a revision read:
  - `request_metadata["archive.content_sha256"]` equal to this request's
    content digest: a committed accession is returned with `replayed = true`
    and the revision that operation committed; an unchanged outcome
    (`RecordedOutcome.unchanged`, prerequisite 6) is returned with
    `replayed = true` and `deduplicated = true` and the revision it matched; a recorded rejection
    is returned as recorded. No current-revision read and no new save.
  - A different or missing digest, on any arm: `ALREADY_EXISTS` naming
    `operation_id`. A rejection is replayed only to the request that caused
    it.
  - A derived (blank) `operation_id` with `expected_version = 0` whose
    recorded committed or unchanged revision is no longer the entry's current
    revision: `FAILED_PRECONDITION` naming the recorded version and the
    current one (or "a deletion at version N" when the entry was deleted),
    and telling the caller to send `expected_version` equal to the current
    version, or a new `operation_id`, to write the content again. A derived ID
    cannot tell a retry from a repeat of the same write, so a success-shaped
    replay would hide that the content is not current, and saving again would
    overwrite the intervening writer. A recorded revision that is still
    current replays as above. Caller-supplied IDs and nonzero
    `expected_version` always replay as above.
  Content dedup against the current revision is not an outcome lookup and
  never stands in for one.
- **Last write wins** (`expected_version = 0`) runs only when no outcome is
  recorded: the translator reads the current revision once and saves against
  it under the same `operation_id`. A writer that commits first makes the save
  `ABORTED`, returned as it is; the translator never retries on the caller's
  behalf. When two attempts of one operation race (a retry while the first is
  still in flight), one commits; the other fails on the revision check or as
  an operation reuse, and the translator repeats the outcome lookup once
  before answering, so the loser replays the winner's outcome.
- **The translated save records the archive content digest.**
  `SaveDocumentRequest.metadata["archive.content_sha256"]` is the section 5
  `content_sha256`. It is persisted request metadata (§7), so it is part of the
  v2 intent, and v2 records it with the operation outcome on every arm
  (prerequisite 7), including rejections, which have no revision to read it
  from. The replay check above therefore compares content the same way for
  caller-supplied and derived IDs.
- **Deduplication is a recorded outcome.** Every translated save sets
  `unchanged = UNCHANGED_POLICY_RECORD_OUTCOME` (prerequisite 6). When the
  save meets prerequisite 6's unchanged predicate (PUTs only, each matching a
  present part of the expected revision), v2 commits the operation
  outcome only, in the same transaction, after the same authorization and
  expected-revision checks as any save, and the archive answers
  `deduplicated = true` with that revision. The archive does no dedup of its
  own and never answers `deduplicated` without a recorded outcome, so a lost
  `deduplicated` response is recovered by the outcome lookup like any other.
- **Pinned cases.** The implementation tests each of these for both a derived
  and a caller-supplied `operation_id` unless a case names one:
  - writer A saves revision 1 with `expected_version = 0` and loses its
    response, writer B saves different content at revision 2, and A's retry
    leaves revision 2 current: with a supplied ID it replays revision 1
    (`replayed = true`); with a derived ID it is `FAILED_PRECONDITION` naming
    versions 1 and 2, and a further attempt with `expected_version = 2` writes
    A again as revision 3;
  - A's request matches revision 1 and deduplicates (`deduplicated = true`,
    no new revision), the response is lost, B saves revision 2, and A's retry
    leaves revision 2 current: with a supplied ID it replays the deduplicated
    outcome at revision 1; with a derived ID it is `FAILED_PRECONDITION` as
    above;
  - with a derived ID, B deletes the entry instead of saving, and A's retry
    is `FAILED_PRECONDITION` naming the deletion's version;
  - a request rejected by v2 validation, retried unchanged, replays the
    rejection;
  - the same retries with no intervening writer;
  - a supplied ID reused with different content after a committed, an
    unchanged and a rejected outcome, each `ALREADY_EXISTS`;
  - B deletes the entry, and a `PutEntry` or `UploadRendition` with
    `expected_version = 0` and a new ID commits a revision over the deletion
    (the translator sends the deletion's revision), never `ABORTED`;
  - an `UploadRendition` with a supplied ID and no `expected_sha256`, retried
    with other bytes of the same length, is `ALREADY_EXISTS` and stores
    nothing; retried with the same bytes, it replays;
  - a save whose CORE PUT matches the expected revision beside a DELETE or
    EMPTY of a populated PARSED part, and a DELETE-only save, each commit a
    new revision with the removal applied and `deduplicated = false`.
- **`VersionManifest.metadata_snapshot`** is the CORE of that revision,
  projected, so every version keeps the metadata it was saved with.
- **Characterization and `ClassifyEntry`.** At the save doors (`PutEntry` and
  `UploadRendition`) and on `ClassifyEntry`, any supplied `declared`
  (`FormatFact`) is validated against its `validate.v1` rules and must carry a
  non-blank `filename` on its active format arm (`INVALID_ARGUMENT` otherwise);
  any supplied `origin` (`ObjectStoreOrigin`) is validated the same way. The
  entry's primary rendition is `original` when present, or the first present
  non-`parsed`, non-bridge-derived rendition in `(name, sub_key)` order. On
  `PutEntry` and `UploadRendition` (the whole rendition when it is sent
  inline, otherwise captured through `WindowCapture` over the upload stream,
  keeping only the 64 KiB leading and 128 KiB circular trailing windows in
  memory, with the result sent in the `UploadTrailer`; nothing is staged on
  disk), the
  primary rendition's byte windows are characterized with
  `Characterizer.identify` and resolved with `Classifications.resolve` against
  the effective `declared` and `origin` facts. When the resolved
  `Classification` (ignoring `classified_at`) and the primary rendition's bytes
  match the current revision's stored classification, the stored
  `Classification` (with its existing `classified_at` timestamp) is preserved so
  an unchanged save produces a byte-identical `CORE` fragment and deduplicates
  under `UNCHANGED_POLICY_RECORD_OUTCOME`. `ClassifyEntry` reads the current
  document (`CORE` and `BLOBS`), streams the primary blob through `ReadBlob`
  into `WindowCapture` when a primary rendition is present, resolves the
  classification state machine, and saves `CORE` with `ExpectedRevision` set to
  the read revision, `UNCHANGED_POLICY_RECORD_OUTCOME`, and a deterministic
  `operation_id` (`operation_kind = "archive.classify-entry"`).
- **Bridging (`BridgeEntry`).** `BridgeEntry` reads the current document and
  requires a bridgeable classification state (`DECLARED`, `IDENTIFIED`, or
  `VERIFIED`; `UNCLASSIFIED` and `CONFLICTED` fail with `FAILED_PRECONDITION`).
  Every requested `BridgeKind` must be in `Bridges.applicableTo(format)`
  (`INVALID_ARGUMENT` otherwise), and the entry must have a present primary
  rendition (`FAILED_PRECONDITION` otherwise). For each requested (or all
  applicable) `BridgeKind`, `BridgeEntry` consults `BridgeEngine`: an
  unregistered kind records `BRIDGE_STATUS_DEFERRED` with
  `Bridges.deferralReason(kind, format)`; a registered bridge reads the primary
  blob via `ReadBlob` (at most once per `BridgeEntry` call) and runs
  `Bridge.derive`. A throwing bridge records `BRIDGE_STATUS_FAILED` with the
  error detail. When `BRIDGE_KIND_DOCUMENT_TEXT` produces 0 bytes and
  `BRIDGE_KIND_OCR_TEXT` is not already queued and is registered in the
  `BridgeEngine`, `BRIDGE_KIND_OCR_TEXT` is appended to the run. If no bridge
  produced output, no save is issued and the current version is returned.
  Otherwise all produced renditions (and any measured `ContentProfile` encoded
  into `Blob.metadata["archive.content_profile"]`) are written in a single
  `SaveDocument` (`PUT` `BLOBS`, carrying every non-overwritten blob forward by
  `blob_id`, `ExpectedRevision` set to the read revision,
  `UNCHANGED_POLICY_RECORD_OUTCOME`, and a deterministic `operation_id` under
  `operation_kind = "archive.bridge-entry"`). When v2 records an unchanged
  outcome (`SaveDocumentResponse.unchanged = true`), each produced bridge
  reports `BRIDGE_STATUS_UNCHANGED` and the version stays at the read revision;
  otherwise each reports `BRIDGE_STATUS_PRODUCED` (with degradation warnings in
  `detail` when present) at the newly committed revision.

### 5. Deterministic `operation_id`

v2 saves need an operation ID for idempotency (§7) and archive writes have
none. Both write requests gain an optional field, and both responses echo it:

```proto
message PutEntryRequest {
  // ... fields 1 to 11 and 99 unchanged ...
  // Retry identity. Blank: derived deterministically (UUIDv5) from the entry
  // address, the requested expected_version and the request content; never
  // random.
  string operation_id = 12 [
    (ai.protomolt.proto.validate.v1.field) = {
      string: {min_len: 1, max_len: 200}
      ignore_if_zero: true
    }
  ];
}

message UploadRenditionHeader {
  // ... fields 1 to 7 unchanged ...
  // Retry identity, as on PutEntryRequest. Blank requires expected_sha256.
  string operation_id = 8 [
    (ai.protomolt.proto.validate.v1.field) = {
      string: {min_len: 1, max_len: 200}
      ignore_if_zero: true
    }
  ];
  // Optimistic concurrency, as on PutEntryRequest: when non-zero, the upload
  // succeeds only if the entry's current version equals this value; zero is
  // last write wins.
  int64 expected_version = 9 [
    (ai.protomolt.proto.validate.v1.field) = {
      int64: {gte: 0}
    }
  ];
}

// PutEntryResponse and UploadRenditionResponse gain:
//   string operation_id = 7;  the operation the save was recorded under
//   bool replayed = 8;        the recorded outcome of an earlier identical save

// The UUIDv5 name. Binary protobuf, fields in number order (§7).
message ArchiveOperationKey {
  string operation_kind = 1;        // "archive.put-entry", "archive.upload-rendition", "archive.classify-entry", or "archive.bridge-entry"
  EntryAddress address = 2;         // account_id, archive, entry_id
  int64 expected_version = 3;       // as the caller sent it (0 for last write wins), or the read revision for ClassifyEntry / BridgeEntry
  string content_sha256 = 4;        // the request content digest below
}
```

- **Derivation.** `operation_id = UUIDv5(ARCHIVE_OPERATION_NAMESPACE,
  encode(ArchiveOperationKey))` per RFC 9562: SHA-1 over the namespace bytes
  followed by the key's binary encoding. The namespace is one fixed UUID
  constant in the contract, never configuration. The key is protobuf, not a
  joined string, so no character in an `entry_id` can make two keys encode
  alike, and `operation_kind` gives the domain separation §7 requires.
- **`expected_version` is the caller's requested value**, `0` for last write
  wins, never the revision the translator later reads. A retry is the same
  request, so it derives the same ID whatever happened to the entry in
  between, and the outcome lookup of section 4 finds the original save. v2
  binds the expected revision it receives into the save's intent
  (`SaveIntents`), which is why the lookup runs before the current-revision
  read: a recorded outcome is never compared for content against a newly read
  revision (only its currency is checked, for a derived last-write-wins ID),
  and with no recorded outcome there is no earlier intent to conflict with.
- **`content_sha256`** for `PutEntry` is the SHA-256 of the binary encoding of
  the request with `operation_id` and `expected_version` cleared, renditions
  sorted by `(name, sub_key)` and metadata sorted by unsigned UTF-8 key (§7
  canonicalization). For `UploadRendition` it is the same digest of the header
  with `operation_id` and `expected_version` cleared (the key holds
  `expected_version`) and `expected_sha256` kept, since the header frame must carry the operation
  ID before any byte arrives. A blank `operation_id` with a blank
  `expected_sha256` is `INVALID_ARGUMENT` naming both fields: without either,
  the content is unknown when the ID is needed, and a random ID is never the
  fallback.
- **A supplied `operation_id` with a blank `expected_sha256`** leaves the
  upload's content digest blind to its bytes. When the outcome lookup finds
  a recorded outcome for that digest, the translator uploads nothing: it
  digests the data frames as they pass and replays the outcome only when
  their SHA-256 and length equal the rendition the outcome names. Other
  bytes are `ALREADY_EXISTS` naming `operation_id`. An upload of this kind
  that loses a race to an attempt of the same operation is checked the same
  way before the winner's outcome is replayed.
- **Cross-language fixtures** pin the encoding and three derived IDs before the
  implementation is approved, as §7 requires for canonical intent encoding.
- A supplied `operation_id` shares the account's v2 operation namespace with
  `DocumentService` saves. Reusing one for a different intent fails loudly, as
  any v2 save does.

### 6. `VERSIONING_NONE`, history and events

**`VERSIONING_NONE` is a view policy.** Under §6 every revision is kept until
an administrator purge, so no archive policy can drop old revisions. An
archive with `VERSIONING_NONE` shows the current revision only:
`ListVersions` returns one row, and `GetEntry`, `GetEntryManifest` and
`ReadRendition` at a version other than the current one are
`FAILED_PRECONDITION` naming the archive's policy (rule 6: the version exists,
the archive does not serve it). Historical revisions stay in the v2 catalog,
readable through `DocumentService` by a caller with read access, and they
count toward stored bytes. A save deletes no superseded object.
`VERSIONING_RETAINED` serves every revision. Changing the policy changes what
the archive shows, never what is stored. Deleting old bytes is §13 retention,
an offline job, and arrives for documents and archives together.

**Events are document events.** The archive keeps no outbox of its own. Each
archive save is one accession and writes the one v2 `DocumentEvent` (§10) in
its commit transaction, keyed by the call number, deduplicated by
`(address, revision)` and by `event_id`. Archive consumers read the repository
event topic, select `graph_id` starting `archive:`, and project each event
into an archive event view whose `(sources)` is `DocumentEvent`:

| Archive event | `DocumentEvent` kind | Carries |
|---|---|---|
| `EntrySaved` | `accessioned` | address (section 1), `version = revision`, `previous_version`, changed parts |
| `EntryDeleted` | `tombstoned` | address, version, reason |
| `EntryPurged` | `purge_marked`, `purge_completed` | address, `purge_id` |
| `RenditionDeleted`, `VersionPruned` | none yet | arrive with per-blob deletion and §13 retention |

Events carry identifiers and state transitions, never bodies. An accession
names the BLOBS part as changed, not the individual rendition; the
per-rendition change is the difference between the manifests of `version` and
`previous_version`, both named in the event. This retires the "generalize the
relay" item under Events above: there is one relay and one event type.

**Archive definitions (`CreateArchive`, `GetArchive`, `ListArchives`).**
`DocumentArchiveBindings` records each archive definition as a CORE-only v2
document in the reserved `archive-def:<account_id>` graph
(`graph_address_id = "archives"`, `doc_id = <archive_name>`) with
`ExpectedRevision.none` so duplicate names fail atomically with
`ALREADY_EXISTS`. The archive's `archive.drive_name`, `archive.datasource_id`,
`archive.description`, `archive.versioning`, and user `meta.<key>` entries live
in `search_metadata.metadata` so `ListArchives` projects directly from
`ListDocuments` lookup rows and `find(account, archive)` binds entry writes to
the archive's `drive_name`.

**`GetArchiveStats` and `ListEntriesResponse.total_count`.**
`DocumentArchive.getArchiveStats` computes exact per-archive, per-rendition,
and per-`ClassificationState` (`ArchiveStats.classification_states`) counters
over the `archive:<account_id>` graph documents and revision manifests
(counting only the current revision when the archive policy is
`VERSIONING_POLICY_NONE`), and `ListEntriesResponse.total_count` reports the
total matching live entries (respecting any `classification_state` filter)
before pagination.

### 7. `ArchiveMutationService` over v2

`DocumentArchive` and `DocumentArchiveMutationGrpcService` serve
`ArchiveMutationService` over `DocumentService`:

- **`DeleteEntry` -> `DeleteDocument` (tombstone).** `DeleteEntry` resolves the
  entry's section 1 call number, checks `GetOperationOutcome` first for an
  idempotent replay of `operation_id`, and otherwise sends `DeleteDocument`
  at the entry's current revision (or the caller-supplied `expected_version`).
  Under keep-forever, logical deletion commits a tombstone revision and removes
  no physical object bytes, so the returned `ArchiveMutationReceipt` reports
  `kind = ARCHIVE_MUTATION_KIND_DELETE_ENTRY`,
  `state = ARCHIVE_MUTATION_STATE_COMPLETED`, `entry_deleted = true`,
  `versions_removed = 0`, `versions_tombstoned = 0`, and zero object counters
  (`objects_targeted = 0`, `objects_pending = 0`,
  `objects_confirmed_absent = 0`), with `command_sha256` equal to the lowercase
  hex SHA-256 of the deterministic protobuf encoding of `ArchiveMutationRequest`
  with `operation_id` cleared. Once tombstoned, `GetEntry` and
  `GetEntryManifest` at `version = 0` return `NOT_FOUND`, `ListEntries` and
  `GetArchiveStats` omit the deleted entry, and a retry with the same
  `operation_id` replays the recorded receipt.
- **`GetArchiveMutation` -> `GetOperationOutcome`.** `GetArchiveMutation` reads
  the account's recorded outcome for `operation_id` from `DocumentService`: a
  committed tombstone accession under `archive:<account_id>` projects to the
  same completed `ArchiveMutationReceipt`, a recorded archive mutation rejection
  (tagged with `archive.mutation_kind`) is rethrown with its recorded refusal
  status, and an unknown or non-mutation operation ID (including a committed or
  rejected `PutEntry` or `UploadRendition`) returns `NOT_FOUND`.
- **`DeleteRendition` and `PruneVersions` refused by name with
  `FAILED_PRECONDITION`.** Under v2 keep-forever retention, neither destructive
  byte removal across versions (`DeleteRendition`) nor version pruning
  (`PruneVersions`) has a v2 counterpart until per-blob purge or §13 retention
  lands. After request and `operation_id` validation (without checking archive
  binding or entry existence first, since the operation is unconditionally
  unsupported under keep-forever), both record a refusal outcome in
  `DocumentService` under `(account_id, operation_id)` (retaining
  `archive.mutation_kind`, `archive.mutation_sha256`, and
  `archive.mutation_refusal`, and confirming the recorded rejection via
  `GetOperationOutcome` so retries and `GetArchiveMutation` replay the refusal
  and conflicting `operation_id` reuse fails with `ALREADY_EXISTS`) and refuse
  by name with gRPC status `FAILED_PRECONDITION`:
  - `DeleteRendition`: `"DeleteRendition is not supported by the archive over v2 documents under keep-forever retention; destructive byte removal across versions requires per-blob purge or section 13 retention"`
  - `PruneVersions`: `"PruneVersions is not supported by the archive over v2 documents under keep-forever retention; removing retained versions requires section 13 retention"`

### Cutover & Remaining Follow-Ups

- **Host cutover and ledger retirement (#632).** No live deployment holds legacy
  `archive_*` ledger rows at cutover, so `RepoServices` and
  `RepoBoundedArchiveMain` mount `DocumentArchiveGrpcService` and
  `DocumentArchiveMutationGrpcService` directly, `UploadHttpServer` routes
  `POST /v1/archive:upload` exclusively through `DocumentArchive`
  (`RenditionUploads`) into `UploadDocument`, and Flyway migration `V121`
  drops the retired `archives` and `archive_*` ledger tables alongside the
  `ArchiveGrpcService`, `ArchiveMutationGrpcService`, `ArchiveOperations`, and
  `ArchiveLedger` stack without shipping an unexercised data migrator.
- **`DeleteRendition` and `PruneVersions` byte removal.** `DeleteEntry` and
  `GetArchiveMutation` run over `DeleteDocument` and `GetOperationOutcome`
  (section 7 above); `DeleteRendition` and `PruneVersions` are refused by name
  with `FAILED_PRECONDITION` until per-blob purge or §13 retention lands.

## Rules of the house

The archive's standing rules, stated once here:

1. **Deterministic identity everywhere.** Name-based UUIDs over logical
   coordinates; never random ids in storage paths or partition keys.
2. **The manifest's key list is the only source of object truth.** No
   prefix-derived operations.
3. **Never hold a database connection across object-store IO.**
   Transactions are scoped to SQL; blob IO happens outside them. Every pool
   sizing and consumer-shape decision follows from this.
4. **Provenance is never invented.** Unknown attribution stays blank.
5. **Storage mechanism appears in exactly one place.** Drives bind
   coordinates and credentials references; the contract echoes
   descriptors, never credentials.
6. **Reads fail honestly.** A row whose bytes are gone is
   `FAILED_PRECONDITION` with the manifest's account of why — never an
   opaque `NOT_FOUND`.
7. **No door without a handle, no handle without a door.** The proto
   publishes only implemented capability. Aspirations live in this
   document, not in the contract.
8. **Sanitize every path segment.** Object-key segments admit
   `[A-Za-z0-9._-]` only; caller-chosen identifiers never reach a signed
   request raw, because signature canonicalization must not diverge on
   exotic bytes.

## Relationship to the document store

`DocumentService` — the pipeline's claim-check store with its four
fixed parts cut along the platform `Document` message's field boundaries —
remains what it is: a specialized store whose parts, partial saves, and
copy-forward semantics are load-bearing for the document pipeline. The
archive is its generic sibling on the same engine, not its replacement.
The two share drives, blob stores, the outbox machinery, the reconciler,
and one process. The direction runs the other way: archive entries are
stored as v2 documents and archive renditions are views of their blobs and
PARSED part, as "The archive over v2 documents" specifies.

## Future work

- **Archive mutation events** — the outbox vocabulary above, once the
  relay generalizes beyond the document store's event type.
- **Async purge queue for archives** — the tombstone lifecycle, when bulk
  deletion under load demands it.
- **Azure blob store** — the `BlobStore` port's next provider, landing with
  the planned `repo/blob-store` module split.
- **Registry-enforced rendition schemas** — a gate that validates a
  protobuf rendition against its pinned subject at the door.
- **A conformance kit** — a black-box suite over the archive contract
  (save/read/version/prune semantics, dedupe, verified writes, event
  emission) so a second implementation in another language proves itself
  against the same pins the Java service passes.
