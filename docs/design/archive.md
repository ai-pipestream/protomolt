# The archive: entries, renditions, and retained versions

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

## Deletion and the orphan rule

The store is intentionally non-ACID across the object store and the ledger,
under the standing rule: **an object with no live ledger row that owns it
is an orphan, and orphans are reclaimable.**

`DeleteEntry` deletes objects first (best-effort, from the union of every
retained manifest's keys — exact keys, never a prefix sweep), then removes
the rows. A partial failure leaves orphans, which is the named, bounded,
observed failure mode: the storage reconciler already diffs bucket listings
against manifest ownership with a min-age guard and dry-run default, and
the archive's keys participate in the same sweep. `PruneVersions` removes
old retained versions by the entry-local reference scan described above.

Two-phase tombstoned deletion with the async purge queue — the document
store's `PENDING_PURGE` lifecycle — is a deliberate future extension for
the archive, not part of the first contract. The synchronous path plus the
reconciler is honest and complete; the queue is an optimization for bulk
deletion under load.

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
and one process. A future in which the document store's parts are
re-expressed as well-known archive renditions is plausible and deliberately
unforced.

## Future work

- **Archive mutation events** — the outbox vocabulary above, once the
  relay generalizes beyond the document store's event type.
- **Streaming reads** — a server-streaming download door, once the blob
  port grows a streaming get; today reads are unary and inline, sized
  accordingly.
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
