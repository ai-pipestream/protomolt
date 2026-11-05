# repo/ — the claim-check document store

The platform's document store: durable bodies in object storage, a Postgres
ledger that indexes them, and a small set of transport surfaces (gRPC, plus a
streaming HTTP upload route) — no framework, blocking style, virtual threads.
Every `DOCUMENT_PLATFORM_*` variable the service reads is listed, with
defaults and failure behavior, in the
[environment-variable reference](../docs/operations/environment-variables.md).

## What it is

The canonical public API is the parts-based
`ai.protomolt.proto.repo.v2.DocumentService` (`AccessionGrpcService`, the
parts-based Repository Service v2; see
[the design requirements](../docs/design/parts-based-repository-requirements.md)
and the [current document guide](../docs/repo/publication.md)).
`RepoServices` (full profile) mounts it alongside the frozen `Archive*` and
`Drive*` services; the v1 document service, its ledger stack and its protos
are retired (#320 Steps 4-5, #430). The separate publication transport is
removed; internal publication and recovery components remain.

When any part of the pipeline needs to persist a `Document` (the platform's
core document protobuf), it comes here. The service writes each part as a
write-once object to S3-compatible object storage and records the revision
in the Postgres v2 catalog (`repo_catalog` schema) that references those
objects. On read it does the reverse: resolve the manifest, fetch and
verify the selected objects, hand back the assembled document. Postgres is
the card catalog, S3 is the stacks. The older Hibernate ledger in
`repo/container` now serves only the frozen archive and drive subsystems.

Two design choices explain almost everything else.

First, a stored document is not one blob. It is split along clean protobuf
field boundaries into independently addressable **parts** — CORE, BLOBS,
CHUNKS (one object per segmentation), PARSED, EMBEDDINGS (one object per
embedding set) — each written as its own object under a shared prefix, with a
manifest that lists them. A reader fetches only
the parts it needs (CORE without multi-megabyte BLOBS), and the full document
reassembles byte-for-byte from the parts.

Second, object bytes are **write-once** and the catalog commit is one
transaction. Fragments are streamed to the stack first; the revision, its
recorded outcome and exactly one outbox event then commit atomically with
compare-and-swap on the expected revision. A failed save can leave
unreferenced stack objects behind - never a revision pointing at missing
bytes - and purge is the two-phase path that reclaims objects once no
unpurged reference points at them.

## Tenets

1. **No framework.** No Quarkus, no Spring. Pure Java 25, constructor
   wiring, a small `main`. `RepoServices` is the SPI.
2. **Virtual threads everywhere.** Blocking style, one virtual thread per
   unit of work (per gRPC call, per upload, per part IO). No reactive types
   in APIs.
3. **Generic names.** `Document`, `DocumentService`, `Drive` — the platform
   is going to a client.
4. **Deterministic UUIDs.** Node ids and blob ids are name-based UUIDs over
   logical coordinates (`doc_id | graph_address_id | account_id | graph_id`).
   Identity is never random; re-saves are idempotent by construction.
5. **Nothing but repo-service speaks S3.** Drives bind
   (bucket, prefix, region, credentialsRef) and are the only thing that
   resolves to object-storage coordinates. Everyone else sees drives and
   `FileStorageReference`s.

## Modules

| Module | Gradle project | Role |
|--------|----------------|------|
| `repo/proto` | `:protomolt-repo-proto` | The wire contract: `Document`, manifest, `DocumentService`, `DriveService` |
| `repo/codec` | `:protomolt-repo-codec` | Document part layouts, splitting and typed assembly |
| `repo/blob/spi` | `:protomolt-repo-blob-spi` | JDK-only byte storage contracts and provider discovery |
| `repo/blob/s3`, `repo/blob/redis`, `repo/blob/cache` | `:protomolt-repo-blob-s3`, `:protomolt-repo-blob-redis`, `:protomolt-repo-blob-cache` | Object storage providers and cache decorator |
| `repo/spi` | `:protomolt-repo-spi` | Caller identity and grants (`RepositoryCaller`, `DocumentAccessPolicy`, `PurgeAuthorization`), read/save controls, domain errors, and the frozen archive/drive repository interfaces |
| `repo/admission` | `:protomolt-repo-admission` | v2 candidate admission: retained-schema fragment decoding, CEL document rules, semantic (segmentation/embedding) checks, opaque-occurrence attestation |
| `repo/schema/registry` | `:protomolt-repo-schema-registry` | Retained schema assets the admission and historical decoding paths resolve against |
| `repo/engine` | `:protomolt-repo-engine` | Drive operations over the container ledger and selected byte storage |
| `repo/container` | `:protomolt-repo-container` | The drive and reader-host Postgres ledger (Hibernate + HikariCP + Flyway) |
| `repo/service` | `:protomolt-repo-service` | The host: mounts the v2 `AccessionGrpcService`, `DocumentArchiveGrpcService`, `DocumentArchiveMutationGrpcService`, and drive gRPC services, the streaming HTTP upload routes, per-account dynamic drives and the v2 outbox relay (`RepoServices` wiring) |
| `repo/stack` | `:protomolt-repo-stack` | v2 write-once stacks: streamed puts that return a `StackRef`, reads verified against it |
| `repo/catalog` | `:protomolt-repo-catalog` | v2 catalog over JDBC + HikariCP + Flyway: revisions, part references, outcomes, writer grants, outbox (mounted in the dedicated `repo_catalog` schema) |
| `repo/accession` | `:protomolt-repo-accession` | v2 `ai.protomolt.proto.repo.v2.DocumentService` (`AccessionGrpcService`): all 13 RPCs — save/read/manifest/revisions/outcome, streamed upload/read + blob reads, delete/list/purge, bulk streams and the Kafka outbox relay; mounted by full-profile `RepoServices` |
| `repo/archive` | `:protomolt-repo-archive` | The archive over v2 documents (`DocumentArchiveGrpcService` and `DocumentArchiveMutationGrpcService`): `ArchiveService` entry reads projected from `DocumentService` reads through the view `.proto`'s projection options, `PutEntry`/`UploadRendition` translated into one v2 save each, `ArchiveMutationService` over `DeleteDocument`/`GetOperationOutcome`, and outcome replay under deterministic UUIDv5 operation IDs; mounted by `RepoServices` and `RepoBoundedArchiveMain` |

## Repository v2 (parts-based)

The v2 `DocumentService` stores each document revision as a manifest over
write-once stack objects: `DocumentSaves` splits the document through the
codec (CORE, BLOBS, PARSED, one CHUNKS per segmentation, one EMBEDDINGS per
embedding set), writes each fragment to the save's drive stack under the
`documents` namespace, and commits the revision in one catalog transaction
(compare-and-swap on `ExpectedRevision`, idempotent by `operation_id` intent
fingerprint). Reads re-verify retained bytes against the manifest before
answering. Drives resolve per account at save time from the v1 drive ledger
(`stack-<driveId>` registrations, `default` falling back to `intake`), and a
cold host re-resolves a persisted stack by drive primary key. Purge is a
separate two-phase resumable operation behind a `PurgeAuthorization` port
(fail-closed deny-all until an admin identity is bound; process authority
cannot purge). Co-mounted pipeline steps save with process authority and are
rebound by `RepoServices` to a credential-bound step caller with an
auto-installed writer grant, so step saves pass the same `WriterAuthority`
and document-ACL checks as key-bound writers.

## Storage model

Every document is addressed by the canonical **`NodeAddress`** - the four
segments `doc_id | graph_address_id | account_id | graph_id`. Manifests,
reads, sources, deletes and list echoes all speak `NodeAddress`; derived
`node_id` strings are echoes, never truth, and the raw `doc_id` never
reaches an object key. Intake documents are addressed at the datasource
and carry the account's intake graph (`intake:<accountId>`); pipeline
documents are addressed at a graph node and carry the owning graph.

Payload bytes live only in object storage, in two shapes:

- **Part fragments** - each part of a revision is one write-once stack
  object (`StackRef`: stack, key, length, SHA-256) under the save drive's
  `documents` namespace. The catalog manifest records every part's state,
  size, digest and writer; readers fetch only the selected parts and
  re-verify their bytes against the manifest before answering. Unchanged
  parts carry forward by reference - the same object serves every revision
  that did not rewrite it.
- **Raw blobs** - the file bytes behind a document's blob bag, written by
  `UploadDocument` or the HTTP upload route as their own stack objects.
  The stored BLOBS fragment references them by `FileStorageReference` (the
  claim check); `ReadBlob` streams them back with an end-to-end SHA-256
  check.

Rewriting byte-identical content reuses the existing object (still a new
revision); there is no separate dedupe response - idempotent retries are
the `operation_id` outcome replay described in the
[document guide](../docs/repo/publication.md).

The archive subsystem (`repo/archive`) runs over v2 `DocumentService`
revisions and raw blobs (`archive-def:<accountId>` for archive definitions and
`archive:<accountId>` for archive entries); drives bind (bucket, prefix,
region, credentialsRef) and are the only thing that resolves to object-storage
coordinates.

## Delete and purge

`DeleteDocument` writes a **tombstone accession** - a revision like any
other, with its own `operation_id` and exact expected revision. No bytes
are removed: earlier revisions stay readable by exact address, and the
current read answers `NOT_FOUND`.

`PurgeDocument` is the administrative erasure, idempotent by `purge_id`.
The catalog transaction marks every reference of the document purged and
counts the objects that no unpurged reference (from any document, via
carry-forward or sourcing) still points at; those objects are then deleted
from their stacks, and a later call with the **same** `purge_id` resumes an
interrupted purge (`PostgresPurges.mark` returns the recorded plan for a
known `(account_id, purge_id)`; a new `purge_id` would re-mark the document
with an empty plan and never execute the pending deletions). Purge requires a credential-bound administrator through
the `PurgeAuthorization` port: process authority cannot purge, and the
mounted host denies all purges until an admin identity is bound.

## Kafka eventing (transactional outbox)

When `DOCUMENT_PLATFORM_KAFKA_BOOTSTRAP_SERVERS` is set, every accession
writes exactly one outbox event (a `DocumentEvent` of kind `Accessioned`:
address, revision,
changed parts with SHA-256s, validation verdict - see
`repo/proto/.../v2/events.proto`) **in its catalog commit transaction**, so
an event can never drift from the state change it describes. When it is
unset, nothing changes: no outbox relay, no producer.

The `repo-v2-outbox-relay` lifecycle loop claims pending events in commit
order, publishes each to the `document-events` topic through the protomolt
serde (registry-framed when `DOCUMENT_PLATFORM_SCHEMA_REGISTRY_URL` is set),
and marks them relayed in batches per pass; failed publishes record an
attempt and retry on a later pass. Semantics:

- **At-least-once.** Publish precedes the relayed stamp, so a crash
  mid-pass republishes up to a pass of events on restart. Consumers must
  dedupe on the event identity.
- **No backfill.** Enabling Kafka on an existing deployment starts the
  stream from that moment; earlier revisions produce no events. Replay
  history from `ListRevisions`, not from the topic.

## Building and testing

From the repository root:

```bash
./gradlew :protomolt-repo-accession:build :protomolt-repo-catalog:build :protomolt-repo-service:build
buf lint   # proto contract lint (repo/proto is a buf module)
```

Tests are testcontainers integration tests (Docker required): `postgres:18-alpine`
for the ledger, LocalStack for S3, the full stack booted through
`RepoServices` with no mocks.

- `ArchiveCatalogIT` — the catalog view over a real archive: one listing
  carries the manifests a projection needs, and the rows it produces are
  the facts the archive already stored.
- `ArchiveBridgeIT` — bridging end to end: a classified container derives
  its member listing beside the untouched original, a declared CSV
  derives its schema, a re-run is idempotent by content, a scanned
  document escalates to OCR on the prose bridge's finding, a bridge this
  host does not run reports `DEFERRED`, a broken container fails loud and
  lands nothing, and an unclassified or conflicted entry is refused by
  its state.
- `ArchiveClassificationIT` — the classification state machine behind the
  doors: verification and conflict from real bytes, grammar refusals
  naming the rule, HTTP-door identification, after-the-fact
  `ClassifyEntry`, exact per-state counts.
- `ArchiveServiceIT` — the archive end to end: retained versions with
  entry-local object sharing, whole-save dedupe, all three doors in (unary,
  client-streaming with and without a declared hash, HTTP POST), exact
  counters, rendition tombstones, pruning, both versioning policies, and
  the contract's refusals.
- The v2 accession suites (`repo/accession`) — `AccessionGrpcIT`,
  `DocumentSavesIT`, `SemanticPartsIT`, `OpaqueSavesIT`,
  `StreamingUploadAndReadIT`, `BulkStreamsGrpcIT`, `DocumentDeletesIT` /
  `DocumentListsIT` / `DocumentPurgesIT`, the outbox ITs
  (`OutboxOrderIT`, `OutboxKafkaIT`) and `RealisticPipelineAndEdgeCasesIT`:
  the 13-RPC surface over Postgres, S3, and Redpanda with no mocks,
  including save-intent goldens that pin fragment bytes and retry identity.
- `UploadHttpServerIT` — the streaming upload route over a real HTTP server:
  multi-MB streaming, byte-exact round trips, dedupe, the 411/400/404
  contract, checksum-mismatch rejection.
- `SeedAccountDrivesIT` — the seeded default account: `intake`/`pipeline`
  drives appear with the provisioning defaults, seeding is idempotent across
  repeat runs, and an unset variable is a no-op.
- `RedisBlobStoreIT` — the shared byte adapter against pinned `valkey/valkey:9.1.3-alpine`:
  verified writes, the two-entries-per-object mapping, 1000-key pipelined
  deletes, SCAN listing, TTL expiry.
- `CacheCloseTest` / `CacheProviderIT` — the caching decorator's
  read-through/write-through semantics over in-memory stores.

## Running

`RepoServiceMain` boots from the environment and serves gRPC (Netty) plus,
unless disabled, the HTTP upload route. To embed in-JVM instead, use
`RepoServices.build(config)` + `startInProcess(name)`.

| Env var | Default | Meaning |
|---------|---------|---------|
| `DOCUMENT_PLATFORM_GRPC_PORT` | `9090` | gRPC listen port |
| `DOCUMENT_PLATFORM_HTTP_PORT` | `8080` | HTTP listen port for the upload routes, `/console` and the console's Connect gateway at `/connect`; `0` or `off` disables all of them |
| `DOCUMENT_PLATFORM_JDBC_URL`, `DOCUMENT_PLATFORM_USERNAME`, `DOCUMENT_PLATFORM_PASSWORD`, `DOCUMENT_PLATFORM_POOL_SIZE` | local Postgres | Ledger database (see `LedgerConfig`; only the URL carries the `JDBC_` infix) |
| `DOCUMENT_PLATFORM_S3_ENDPOINT` | _(provider default)_ | S3 endpoint override for S3-compatible stores (LocalStack, SeaweedFS, MinIO); path-style is forced when set |
| `DOCUMENT_PLATFORM_S3_REGION` | `us-east-1` | S3 region |
| `DOCUMENT_PLATFORM_S3_ACCESS_KEY` / `_SECRET_KEY` | _(SDK default chain)_ | Static credentials; both or neither. Blank = the AWS default credentials chain (IRSA/instance profile in prod) |
| `DOCUMENT_PLATFORM_DEFAULT_BUCKET_BASE` | `documents` | Provisioned drives without an explicit bucket get `<base>-<accountId>-<name>` |
| `DOCUMENT_PLATFORM_BLOB_STORE` | `s3` | Blob-store selection: `s3` (default, direct object storage), `valkey` (preferred Redis-compatible byte provider), `s3-valkey-cache` (preferred S3 + cache composition), `redis` (objects live in Redis; not constructable for v2 stacks, whose streamed reads `RedisBlobStore` does not implement), `s3-redis-cache` (S3 of record behind a Redis read-through/write-through cache; supported, the cache forwards streamed reads to S3). The former `repo` / `repo-inprocess` delegation modes are retired (Step 5A) and fail construction with `Remote repository blob storage is retired` |
| `DOCUMENT_PLATFORM_VALKEY_URI` / `DOCUMENT_PLATFORM_REDIS_URI` | `redis://localhost:6379` | Connection URI (`redis://` or `rediss://`) for all Valkey/Redis byte and cache modes; conflicting alias values fail |
| `DOCUMENT_PLATFORM_VALKEY_TTL_SECONDS` / `DOCUMENT_PLATFORM_REDIS_TTL_SECONDS` | `3600` | Per-object TTL (0 = no expiry); cache-entry TTL in either S3 cache mode |
| `DOCUMENT_PLATFORM_VALKEY_MAX_OBJECT_BYTES` / `DOCUMENT_PLATFORM_REDIS_MAX_OBJECT_BYTES` | `8388608` | Largest object admitted to the adapter (0 retains its ceiling); either S3 cache mode bypasses the cache for larger objects |
| `PROTOMOLT_API_TOKEN` | _(required to open network listeners)_ | Bearer token for gRPC and HTTP (repo-service mounts no MCP surface) |
| `DOCUMENT_PLATFORM_READ_BUDGET_BYTES` | `268435456` | Per-pool byte ceiling for verified document/blob reads (`Read buffer budget exhausted`) and for a separate v2 save-admission reservation pool of the same size (`ReadByteBudget.forSaves`, reporting `Save buffer budget exhausted`), so concurrent reads and saves can hold up to `2 * DOCUMENT_PLATFORM_READ_BUDGET_BYTES` combined. Streaming `ReadDocument` reserves each non-opaque part's catalog length while decoding and streaming it (a bounded 131,072-byte chunk buffer for opaque parts); `ReadBlob` streams in 64 KiB chunks without a catalog-length reservation; unary `GetDocument` and bulk `ReadDocuments` reserve each selected part's catalog length; unary v2 saves reserve split part fragments and extracted inline raw-blob bytes from admission until commit, and `UploadDocument` reserves per received 64 KiB encoded-part chunk |
| `DOCUMENT_PLATFORM_S3_CONDITIONAL_WRITES` | `false` | Require S3 `If-None-Match` semantics for write-once stack-object creation |
| `DOCUMENT_PLATFORM_LIFECYCLE_ENABLED` | `true` | Run the background loops when `startLifecycle()` is called (archive mutation recovery, archive reconciliation, the v2 outbox relay) |
| `DOCUMENT_PLATFORM_PURGE_INTERVAL_MS` | `5000` | Lifecycle cadence: archive mutation-recovery lookback and idle sleep; also the v2 outbox relay's idle sleep |
| `DOCUMENT_PLATFORM_SWEEP_INTERVAL_MS` | `60000` | Archive-reconciliation loop cadence |
| `DOCUMENT_PLATFORM_RECONCILE_MIN_AGE_MS` | `3600000` | Min-age guard for archive reconciliation (objects younger than this are never reconciled) |
| `DOCUMENT_PLATFORM_KAFKA_BOOTSTRAP_SERVERS` | _(none)_ | Kafka bootstrap servers; outbox rows are always written in the commit transaction, and leaving this unset only disables the outbox relay/publisher |
| `DOCUMENT_PLATFORM_KAFKA_TOPIC` | `document-events` | The document-events topic the v2 outbox relay publishes to |
| `DOCUMENT_PLATFORM_SCHEMA_REGISTRY_URL` | _(none)_ | Confluent-compatible schema registry for the relay's serde; unset = registry-free (frames stamp schema id 0, which only protomolt consumers resolve) |
| `DOCUMENT_PLATFORM_SEED_ACCOUNT_ID` | _(none)_ | Standalone default account: at boot the service idempotently ensures this account's `intake` and `pipeline` drives exist; unset = no seeding. Permanent namespace once used — never change it against an existing store (see below) |

### Standalone default account

Deployments without an account-service can name ONE seed account via
`DOCUMENT_PLATFORM_SEED_ACCOUNT_ID`. At boot — after the ledger is migrated,
before the servers start — `RepoServices.seedAccountDrives()` idempotently
ensures that account's two provisioning-time drives exist: `intake`
(`DriveType.INTAKE`) and `pipeline` (`DriveType.PIPELINE`), each logging
created-vs-found. Reboots and repeat runs are safe: drive ids are
deterministic, so the second pass simply finds the rows.

`account_id` stays explicit and required on every request — the variable only
pre-creates the drives, it never defaults request fields. Embedded hosts
(`startInProcess`) opt in by calling `seedAccountDrives()` themselves;
`RepoServiceMain` calls it automatically.

**Warning:** treat the seed account id as a permanent namespace once it has
been used. The account id is baked into identity hashes and S3 prefixes —
changing it against an existing store strands every object and row written
under the old id. Pick it once and keep it.

## API surface

### gRPC `DocumentService` v2 (`repo/proto`, `repo/v2`)

The 13-RPC parts-based contract - `SaveDocument`, `SaveDocuments`,
`UploadDocument`, `DeleteDocument`, `PurgeDocument`,
`GetOperationOutcome`, `GetDocument`, `ReadDocument`, `ReadDocuments`,
`ReadBlob`, `GetManifest`, `ListRevisions`, `ListDocuments` - documented
in the [current document guide](../docs/repo/publication.md): partial
saves by `PartSelector` with carry-forward from the expected revision,
cross-document `PartSource` references, selective verified reads, and
claim-check raw blobs. The v1 document surface (`SaveDocument` /
`GetDocumentByReference` / `PutBlob` and friends) is retired.

Generic blob PUT, conditional PUT and DELETE on the remaining byte-storage
surfaces reject keys containing the exact path segment `archive`,
`documents` or `.protomolt-managed`, including keys generated from a drive
prefix. These namespaces belong to repository publication and cleanup. The
reservation protects historical archive keys and applies through drive
aliases as well. Administrative reads remain available. Keys are opaque:
`archive-backup` is not reserved, and percent-encoded text is not decoded.
This intentionally makes any pre-release loose blob under those exact
reserved segments read-only through the generic API; existing bytes are not
rewritten. The general orphan sweep also excludes these namespaces. Archive
and managed-raw recovery use their own ledgers; explicit v2 document purge
still processes admitted snapshots (see Delete and purge above).

### gRPC `ArchiveService` (`repo/proto`, `archive/v1`)

The generic document archive on the same engine: account-scoped archives
bound to a drive, entries with open named **renditions** (opaque bytes with
a media type and an optional schema-subject pin), attached metadata, and a
per-archive versioning policy (`NONE` or `RETAINED`). Rendition objects are
entry-local content-addressed — an unchanged rendition across versions is
one shared object — so retained history costs only what actually changed,
and every deletion question stays bounded to one entry's own manifests.
The design of record is [docs/design/archive.md](../docs/design/archive.md).

- `CreateArchive` / `GetArchive` / `ListArchives` — archive lifecycle.
- `PutEntry` — metadata plus inline renditions, one new version; verified
  writes; a root-checksum match dedupes the whole save.
- `UploadRendition` — **client-streaming** bulk door: a header frame
  (address, descriptor, required declared size, optional expected SHA-256),
  then chunks; digest computed while streaming; one new version that
  re-references every other current rendition.
- `GetEntry` / `GetEntryManifest` — whole or rendition-filtered reads at
  any retained version; stored bytes are digest-checked against the
  manifest, and a missing body fails `FAILED_PRECONDITION` by name.
- `ListEntries` / `ListVersions` — paginated listings.
- `ArchiveMutation` on `ArchiveMutationService` — an operation UUID plus one
  delete-entry, delete-rendition, or prune-versions command. `DeleteEntry`
  commits a v2 tombstone revision through `DocumentService.DeleteDocument`
  (leaving stored object bytes intact under keep-forever retention).
  `DeleteRendition` and `PruneVersions` are refused by name with
  `FAILED_PRECONDITION` under keep-forever retention and record their refusal
  under the operation UUID.
- `GetArchiveMutation` — reads the account's recorded `DocumentService`
  operation outcome (`GetOperationOutcome`) for the operation UUID: a committed
  `DeleteEntry` tombstone projects to a completed `ArchiveMutationReceipt`, a
  recorded mutation refusal replays its refusal status, and an unknown or
  non-mutation operation ID returns `NOT_FOUND`. Retry the same command with
  the same UUID to replay the recorded receipt or refusal; reusing the UUID for
  a different command fails with `ALREADY_EXISTS`. These operations require
  explicit authenticated authority and a qualified managed-storage host.
- `GetArchiveStats` — exact per-archive counters (entries, versions,
  retained/current bytes, per-rendition breakdown, and per-state
  classification counts) computed by `DocumentArchive.getArchiveStats` over
  live v2 documents in `archive:<account_id>` and their projected revision
  manifests.
- `ClassifyEntry` — declare (or re-declare) the entry's format and have
  characterization re-read the primary rendition's bytes; returns the
  state machine's resolution.
- `BridgeEntry` — run the bridges the entry's classification makes
  applicable, landing their derived renditions beside the untouched
  original; returns one outcome per bridge considered.

Entries carry an asset **classification** — the state machine from
[docs/design/asset-formats.md](../docs/design/asset-formats.md):
`UNCLASSIFIED`, `DECLARED`, `IDENTIFIED`, `VERIFIED`, or `CONFLICTED`,
never a silent default. The save doors accept a declared `FormatFact`
(validated against the format's own rules — a `.zip` name can never claim
tar — and a declaration must name its file) and an `ObjectStoreOrigin`
(coordinates required); characterization reads the primary rendition's
bytes through the asset family's one detection seam, and listings filter
by state.

A classified entry can then be **bridged**: `BridgeEntry` derives
renditions under the platform's well-known names (`members` for a tar or
zip, `schema` for a delimited table, NDJSON file, or Avro container)
beside the original, each pinning its own `schema_subject` so the output is
schema-validated data rather than loose bytes. Bridging is gated on the
state machine — an `UNCLASSIFIED` or `CONFLICTED` entry names no single
format and is refused `FAILED_PRECONDITION` — and every bridge reports
what it did: `PRODUCED`, `UNCHANGED` (content addressing makes a re-run
idempotent), `DEFERRED` (applicable, but the work needs a reader or a
service this host does not carry, named in the detail), or `FAILED` with
the reason verbatim. Derived
renditions never become the rendition an entry is characterized from.

A host that can reach a parser passes its own `BridgeEngine` to
`RepoServices.build`, adding the text and OCR bridges. A PDF the text
bridge recovers no prose from escalates to OCR on that finding alone, and
the empty `text` rendition lands as empty rather than absent.

The catalog view over an archive rides
[`asset/catalog`](../asset/README.md): `AssetCatalogRows.of(info, manifest)`
projects one flat row per asset, whose fields declare their own index
hints and metric members, so "how many Parquet datasets", "how much is
unclassified", and the OCR quality distribution are ordinary metric
queries over an ordinary subject.

### HTTP `POST /v1/archive:upload`

The archive rendition route shares the document upload HTTP server. The body is
raw rendition bytes; `account_id`, `archive`, `entry_id`, `rendition` (default
`original`), `sub_key`, `schema_subject`, `module`, `operation_id` and
`expected_version` ride query parameters or corresponding `x-` headers (query
wins). `Content-Length` is required; `Content-Type` describes the rendition and
`X-Content-Sha256` optionally declares its digest. A blank operation ID requires
the digest and derives a deterministic retry ID.

The v2 path goes through `RenditionUploads` into `UploadDocument`. A rendition of
at most 1 MiB is sent inline in the header frame and commits in one round trip. A
larger one sends its header first and reads no body byte until v2 answers
`accepted`, so a refused caller gets its 401 or 403 before the body moves; it then
waits for downstream readiness before reading another 64 KiB chunk. Nothing is
staged on disk. Early replay or refusal
answers without draining the remaining request body and closes the HTTP connection.
Success is binary `UploadRenditionResponse` with `Content-Type: application/protobuf`;
errors are binary `google.rpc.Status` with the same content type and the matching
HTTP status. This replaces the former hand-built JSON receipt, including its
invented `deduplicated` field. The receipt uses revision, size, SHA-256,
candidate root checksum, operation ID and replay status; legacy physical-key and
entry-UUID fields remain absent.

`filename`, `title` and `source_uri` are refused rather than silently
ignored; set entry metadata through `PutEntry`. Scoped uploads stamp READ and
WRITE rules only for the caller's ACL identities. An identityless writer creates
no such rules; its successful commit receipt does not grant subsequent read access.
Archive definitions grant public READ within their account, never public WRITE.

### HTTP `POST /v1/documents:upload`

The streaming raw-upload route, served by the v2 `UploadHttpHandler`
(`repo/accession`): the request body streams into the document's drive
stack through `DocumentUploads` — **no buffering in memory** — and lands as
a raw blob of a new revision saved through the same accession pipeline as
gRPC `UploadDocument`. Authentication comes first: every request must
present the configured API token as an `api_token` header or as
`Authorization: Bearer <token>`; anything else is `401` before routing.

Identity comes from query parameters or headers (query takes precedence):

| Param | Header | Required |
|-------|--------|----------|
| `account_id` | `x-account-id` | yes |
| `datasource_id` | `x-datasource-id` | yes |
| `drive` | `x-drive-name` | yes |
| `filename` | `x-filename` | yes |
| `operation_id` | `x-operation-id` | no (blank generates a random UUID) |
| `doc_id` | `x-doc-id` | no (see identity rules below) |
| `blob_id` | `x-blob-id` | no (blank derives from the document ID and filename) |
| `connector_id` | `x-connector-id` | no |
| `content_type` | `Content-Type` | no (default `application/octet-stream`) |
| `expected_revision` | `x-expected-revision` | no (integer `> 0` pins that exact revision; omitted or `0` means `ExpectedRevision.none`, a create) |
| `sha256` | `X-Content-Sha256` | no (declared digest, verified while streaming) |

**`Content-Length` is required** — chunked transfer and absent, blank, or
duplicated length headers are refused (`411`, duplicates `400`) before any
byte is read, because the length bounds the streamed write.

Identity and retry rules: with no `doc_id`, the document ID derives
deterministically from the declared `sha256` when one is supplied, else
from the supplied `operation_id`, else it is a fresh random UUID — so a
retry that repeats neither creates a new document on every attempt.
Retries should supply a stable `operation_id`: a retry with the same
`operation_id` and the same intent replays the recorded outcome and answers
with `replayed: true` instead of writing a duplicate revision. Updates to
an existing document should also pin `expected_revision` to the revision
they mean to advance; a concurrent write then conflicts (`409`) instead of
being overwritten.

Success is `200` with a JSON receipt:

```json
{
  "operation_id": "…",
  "doc_id": "…",
  "revision": 1,
  "replayed": false,
  "size_bytes": 8388608,
  "sha256": "…"
}
```

Errors are `{"error": "…"}` JSON: `400` (missing required parameter,
invalid `expected_revision` or `Content-Length`, unknown drive, digest or
length mismatch against the streamed bytes), `401` (missing or invalid API
token), `403` (the caller may not write this document), `404` (wrong path,
or a referenced document does not exist), `405` (non-POST), `409`
(revision or operation conflict), `411` (missing `Content-Length`), `429`
(capacity exhausted), `503` (stack or catalog unavailable).

## Retired: the dogfood blob store

`RemoteBlobStore` (`:protomolt-repo-blob-grpc`) — byte storage delegated to
another repo-service over the v1 `DocumentService` gRPC API, selected by
`DOCUMENT_PLATFORM_BLOB_STORE=repo` / `repo-inprocess` — was retired in
Step 5A (#388). The module, its tests, and the service modes are gone;
configuring either mode now fails fast at `RepoServices` construction.
Consumers that need byte storage compose a provider (`s3`, `redis`,
`s3-redis-cache`) directly; document access goes through the v2
`DocumentService`.

## Valkey and Redis-compatible blob storage

Valkey is the preferred Redis-compatible server. Explicitly select `valkey` for
bounded byte storage or `s3-valkey-cache` for an expendable cache over S3. The
legacy `redis` and `s3-redis-cache` modes remain supported. S3 remains the default
and the authoritative store for general streaming/managed storage.

Both names reuse `RedisBlobStore` in `repo/blob/redis`, with a single atomic hash
per encoded object. They preserve the existing physical keys, `redis/v2` and
`redis/v3` backend identities, and persisted drive provider `redis`. Selecting
Valkey does not change an endpoint or migrate data. Use `redis://` or `rediss://`.
The adapter still buffers payloads and has no native streamed reads; it does not
qualify as an unrestricted v2 stack. See the [provider documentation](blob/redis/README.md)
for limits, identity rules, real-server qualification and migration boundaries.

Prefer `DOCUMENT_PLATFORM_VALKEY_*` environment variables. Their `REDIS_*` aliases
remain accepted; conflicting values are rejected. `CachingBlobStore` keeps the
backing store authoritative, writes there first, and forwards streaming reads to
it. Cache loss does not lose authoritative objects. An optional
[document-platform override](../deploy/document-platform/compose.valkey.yml)
explicitly selects S3 with Valkey cache without changing the base deployment.

## Drive provider_config and the field-99 convention

`Drive.provider_config` (and `CreateDriveRequest.provider_config`) carries
the drive's pronounced per-provider knobs as a `DriveProviderConfig` —
a `oneof` of `S3DriveConfig` / `RedisDriveConfig` plus an `options` map.
The oneof is for the pronounced knobs; the tuning long tail goes in the
`options` map at field 99. It is persisted verbatim on the drive row (a
`provider_config` jsonb column, Flyway V2) and echoed on Get/List.

Field-99 convention (all repo protos): field 99 is the extension-metadata
map (`map<string, string> metadata`) on every message that carries one.
`Struct` fields with distinct names (`Blob.metadata`,
`SearchMetadata.custom_fields`) are for genuinely nested values and keep
their own field numbers.

## Future work

The `BlobStore` port currently lives in `repo/container` next to its
implementations (`S3BlobStore`, and `RedisBlobStore` as the second provider).
The storage layer still splits into `repo/blob-store/spi` (the port),
`repo/blob-store/s3`, `repo/blob-store/redis` etc. — one new implementation
per backend, no sweep through the handlers — when the next real backend
(Azure) lands.

### Multiple drive backends

One service can provision and route multiple `s3`, `valkey`, and `redis` drives.
`CreateDrive` remains restricted to process authority. Its `provider_config`
selects the endpoint and provider settings; a drive without that configuration
inherits the host backing, including an optional S3 read-through cache. New
explicit configurations share a provider handle when their complete resolved
options match. The host closes pooled handles once, after request processing
stops, including cleanup after partial startup failure.

S3 uses `s3.endpoint_override`, `s3.force_path_style`, and the drive's `region`.
A different endpoint requires an explicit `credentials_ref`; host credentials
are never implicitly sent to another endpoint. Supported references are:

| Reference | Resolution |
| --- | --- |
| `host:default` | Explicitly use this host's configured S3 static credentials or default SDK chain. |
| `env:ARCHIVE` on S3 | Read `ARCHIVE_ACCESS_KEY_ID` and `ARCHIVE_SECRET_ACCESS_KEY` from the host environment. Both are required. |
| `profile:archive` on S3 | Select the named AWS SDK host credential profile. |
| `env:SCRATCH` on Redis/Valkey | Read required `SCRATCH_PASSWORD` and optional `SCRATCH_USERNAME`; inject them only into the private provider connection options. |

Missing or unsupported references fail; there is no credential fallback. A
per-drive Redis URI must not contain user information or a password, because
provider configuration is returned by the drive metadata API. Secrets resolved
from references are neither written into drive records nor catalog endpoints.
S3 `options` may set `conditional-writes` and the four existing provider timeout
options (`api-call-timeout-ms`, `api-attempt-timeout-ms`,
`connection-timeout-ms`, `socket-timeout-ms`); other keys are refused.

New stack records use the resolved provider and endpoint. S3 namespaces include
the region and addressing mode, so SDK-default endpoints in different regions
cannot alias a stack. Redis namespaces also
encode the configured key prefix and write policy, with the database in the
endpoint, so two physical namespaces cannot
share a stack accidentally. Existing host-default stack records retain their
original IDs and identities, including the older Redis identity emitted by the
single-backend host. Compatibility applies only to the exact host-default
backing and exact old recorded namespace; it does not authorize repointing a
retained stack. Changing an endpoint or physical namespace requires a new drive
and an explicit data migration. New Redis/Valkey endpoints normalize equivalent
host, port, and database spellings; both provider aliases register one catalog
stack for the same physical namespace, including concurrent registration.
Indexed stack identities are bounded to 2,048 UTF-8 bytes and namespaces to
1,024 characters before catalog writes; these are configuration bounds, not
document payload limits, and
oversized per-drive configuration is refused as invalid input.

Selected-part saves retain untouched part and raw-blob references across drives.
A fresh host resolves historical objects from their recorded stack IDs. Purge
uses each stack's provider reclamation port; incomplete physical cleanup leaves
the durable purge pending for retry. Versioned S3 cleanup includes old versions
and delete markers. Shared references remain readable until their final owner
is purged.

This implementation preserves keep-until-administrator-purge semantics.
Explicit Redis/Valkey repository drives require `ttl_seconds = 0`, non-expiring
writes, and physical reclamation. Managed streaming hosts also require streaming
write capability; Redis/Valkey do not advertise it and cannot replace qualified
S3 archival backing there. Redis object limits still apply. A non-expiring Redis
configuration alone is not a durability guarantee: the operator must configure
persistence and an eviction policy consistent with retaining repository bytes.
The existing host-default and cache configuration paths retain their prior
behavior; this feature does not newly qualify expiring host defaults for archival
retention.

Temporary TTL drives from issue #775 remain separate design work: their expiry
contract must define reads after expiry, catalog cleanup, retained references,
and promotion to durable storage before the repository's keep-forever contract
can be changed. No temporary-retention exception is implemented here.
