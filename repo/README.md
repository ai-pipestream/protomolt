# repo/ — the claim-check document store

The platform's document store: durable bodies in object storage, a Postgres
ledger that indexes them, and a small set of transport surfaces (gRPC, plus a
streaming HTTP upload route) — no framework, blocking style, virtual threads.

## What it is

When any part of the pipeline needs to persist a `Document` (the platform's
core document protobuf), it comes here. The service writes the document body
to S3-compatible object storage and records a row in Postgres that points at
those objects. On read it does the reverse: look up the row, fetch the
objects, hand back the assembled document. Postgres is the card catalog, S3
is the stacks.

Two design choices explain almost everything else.

First, a stored document is not one blob. It is split along clean protobuf
field boundaries into independently addressable **parts** — CORE, BLOBS,
CHUNKS (one object per chunk set), PARSED — each written as its own object
under a shared prefix, with a manifest that lists them. A reader fetches only
the parts it needs (CORE without multi-megabyte BLOBS), and the full document
reassembles byte-for-byte from the parts.

Second, the store is **intentionally non-ACID**. The object write and the
ledger row that references it are not committed atomically. The standing rule
that makes that safe: an object with no live ledger row that owns it is an
orphan, and orphans are reclaimable. Deletes and the purge path are built
around that rule.

## Tenets

1. **No framework.** No Quarkus, no Spring. Pure Java 21+, constructor
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
| `repo/container` | `:protomolt-repo-container` | The storage engine: part codec, `BlobStore` port + `S3BlobStore`/`RedisBlobStore`/`CachingBlobStore`, part fan-out IO, the Postgres ledger (Hibernate + HikariCP + Flyway) |
| `repo/service` | `:protomolt-repo-service` | The service set: gRPC impls, the streaming HTTP upload route, `RepoServices` wiring, the dogfood `RemoteBlobStore` |

## Storage model

Every stored state is one ledger row addressed by the canonical
**`NodeAddress`** — the single addressing concept of the whole API. It
carries the four segments `doc_id | graph_address_id | account_id |
graph_id`, hashed into a deterministic `node_id`; manifests, by-reference
reads, partial-save copy sources, deletes and list echoes all speak
`NodeAddress`, and `node_id` strings are derived echoes, never truth.
Intake rows are addressed at the datasource and
carry the account's intake graph (`intake:<accountId>`); pipeline rows are
addressed at a graph node and carry the owning graph. The raw `doc_id` never
reaches an object key — keys are built from the deterministic UUIDs.

Payload bytes live only in object storage:

- **Part objects** at `<drive.prefix>/documents/<accountId>/<nodeId>/...`
  (`core.pb`, `blobs.pb`, `chunks/<subKey>.pb`, `parsed.pb`), written by the
  part fan-out with per-part SHA-256 manifest entries.
- **Raw upload blobs** at `<drive.prefix>/blobs/<accountId>/<blobId>.bin`,
  staged by the HTTP upload route; the `Document`'s `blob_bag` references
  them by `FileStorageReference` (the claim check).
- **Content-addressed blobs** at `<drive.prefix>/blobs/<uuid-of-sha256>`,
  written by `PutBlob` when no explicit key is given — identical puts land
  idempotently on one object.

Intake dedupe: a re-save whose split root checksum matches the AVAILABLE
row's checksum skips the object writes, marks the row re-processed, and
answers `deduplicated=true` with the existing coordinates. `force_save`
bypasses the skip with a store-verified write (the SHA-256 rides the PUT as
the checksum trailer, so the store rejects landed bytes that mismatch).

## The purge lifecycle (two-phase delete)

`DeleteDocument` with `purge_storage=false` is a **two-phase delete**.

**Phase A** (synchronous, in the gRPC call): the row tombstones to
`PENDING_PURGE` — metadata-only, fast, and deliberately NOT bumping
`updated_at` — and one `document_purges` record per row is enqueued **in the
same transaction** (Flyway V3). The record snapshots every object key to
delete (the manifest's PRESENT part keys plus, for INTAKE rows, the derived
raw-blob key `blobs/<accountId>/<blobId>.bin`), so Phase B never recomputes
keys. The document is immediately unreadable; its objects still exist.

**Phase B** (async, `RepoServices.startLifecycle()`): the purger
(`S3Purger.drainOnce`) claims a batch of PENDING records
(`SELECT ... FOR UPDATE SKIP LOCKED` — competing purgers never double-take
from the same select, and every terminal transition is conditional on
PENDING, so a re-claim settles to one winner) and per record:

1. re-reads the document row under a row lock and applies the **staleness
   guard**: row AVAILABLE (revived), or `updated_at` strictly after the
   record's `requested_at` (body re-staged after the delete was requested —
   the revive-before-PUT race) → the purge is **VOID**, objects and row left
   alone. The guard is re-checked under a fresh lock in the same transaction
   that removes the row, so a revive landing mid-drain still wins.
2. batch-deletes the snapshot keys from the record's bucket (drive resolved
   via `drive_name`; `deleteAll`, NoSuchKey-is-success — idempotent
   throughout), removes the row, marks the record **PURGED**. A row already
   gone still gets its snapshot objects deleted.
3. Any store/DB error (including a missing drive or partial batch failure) →
   `markFailed`: attempts + 1 and back to PENDING, until **10 attempts** land
   the record in **FAILED** and the row in `PURGE_FAILED`. The FAILED record
   IS the dead-letter queue for now — recovery is operator territory.

The **sweeper** (`PurgeSweeper.sweepOnce`) periodically rescans for rows
stuck in `PENDING_PURGE` with no PENDING purge record (crashed Phase A, or
pre-lifecycle tombstones) and enqueues a record for each (snapshot from the
row's manifest, `requested_at` = sweep time so a later revive voids it). It
deliberately never re-enqueues FAILED records.

The **reconciler** (`StorageReconciler.reconcile(store, bucket, prefix,
minAge, dryRun)`) diffs the bucket listing against what the ledger owns —
every row's manifest-PRESENT keys (rows of every status; a PENDING_PURGE
row's keys are owned until the purger lands), plus the `blobs/` namespace by
convention (content-addressed raw blobs are untracked by the ledger; their
lifecycle is `DeleteBlob`'s). Orphans older than `minAge` (default 1h — the
in-flight-upload guard) are reported, or deleted when armed. **Dry-run by
default.** The report carries the exact orphan count, a bounded key sample
(50), the scanned count and the skipped-too-young count.

The **coherence probe** (`CoherenceProbe.probe(store, sampleSize)`) is the
other direction: for a bounded sample of AVAILABLE rows, every
manifest-PRESENT key is HEAD-probed, and confirmed-missing objects get their
manifest entry tombstoned to `PART_STATE_DELETED` with
`deleted_reason = "COHERENCE_PROBE"` (size/sha/key retained). The row stays
AVAILABLE by design — the remaining parts are still valid — and `updated_at`
does not move (bookkeeping, not a body rewrite). Reconciler and probe are
library calls; when `DOCUMENT_PLATFORM_RECONCILE_ENABLED` is set, a slow
periodic loop reconciles every drive (dry-run logging unless
`DOCUMENT_PLATFORM_RECONCILE_DRY_RUN=false`). No RPCs yet.

`purge_storage=true` keeps its synchronous behavior: objects first
(best-effort), then hard-delete the rows — no queue involved.

## Kafka eventing (transactional outbox)

When `DOCUMENT_PLATFORM_KAFKA_BOOTSTRAP_SERVERS` is set, the service emits
document lifecycle events to Kafka through a **transactional outbox**. When
it is unset, nothing changes: no outbox writes, no relay loop, no producer
(zero overhead).

Every lifecycle commit point writes one row to `document_events_outbox`
(Flyway V4) **in the same transaction** as the ledger mutation, so an event
can never drift from the state change it describes:

| Event | Commit point |
|-------|--------------|
| `DocumentSaved` | Full/partial save upsert (revives included; checksum dedupe hits elide the write and fire nothing) |
| `DocumentDeleted` | Hard delete (`purge_storage=true` row removal) |
| `PurgeRequested` | Soft delete (tombstone + purge enqueue, Phase A) |
| `DocumentPurged` | Purge finalization (row removal + PURGED transition, Phase B) |

The row's payload is the serialized `DocumentEvent` protobuf (see
`repo/proto/.../document_events.proto`): a wrapper with an `event_id` (the
outbox row id) and a `oneof` of the four event messages, each carrying the
canonical `NodeAddress`, the manifest root checksum where meaningful, and
timestamps. A virtual-thread **relay** (`EventRelay`, started by
`startLifecycle()`) claims PENDING rows (`FOR UPDATE SKIP LOCKED`), publishes
each to the single **`document-events`** topic keyed by **doc_id** (so one
document's events are partition-ordered), waits for the broker ack, and marks
the row PUBLISHED. Publication goes through the protomolt serde
(`sink/kafka/serde`), pinned to the `DocumentEvent` type against the packaged
descriptor set, so every record is validated and Confluent-framed. By default
no schema registry is involved and frames stamp schema id 0, which only
protomolt consumers resolve; set `DOCUMENT_PLATFORM_SCHEMA_REGISTRY_URL` to
point the relay's serde at a Confluent-compatible registry (the
`DocumentEvent` subject registered under `<topic>-value`) and frames carry
the registry-assigned id, so standard Confluent tooling can read the topic.

Semantics and operational notes:

- **At-least-once.** Publish precedes the PUBLISHED transition, so a relay
  crash mid-flight republishes on restart. Consumers must dedupe on
  `DocumentEvent.event_id`.
- **Retry and DLQ.** A failed publish increments `attempts`; at 10 attempts
  the row lands FAILED, which IS the dead-letter queue for now (operator
  territory; the relay never re-enqueues it).
- **PUBLISHED rows are retained**, not deleted (same choice as the purge
  queue's PURGED rows).
- **No backfill.** Enabling Kafka on an existing deployment starts the event
  stream from that moment: rows saved or deleted before the first configured
  boot produce no events. If consumers need history, replay from storage,
  not from the topic.

## Building and testing

From the repository root:

```bash
./gradlew :protomolt-repo-proto:build :protomolt-repo-container:build :protomolt-repo-service:build
buf lint   # proto contract lint (repo/proto is a buf module)
```

Tests are testcontainers integration tests (Docker required): PostgreSQL 17
for the ledger, LocalStack for S3, the full stack booted through
`RepoServices` with no mocks.

- `RepoServiceIT` — the gRPC surface end to end: drives (including the
  `provider_config` jsonb round trip), full/partial saves, dedupe,
  delete/purge, the tombstone→enqueue→drain lifecycle, `PutBlob`/`GetBlob`/
  `DeleteBlob`.
- `S3PurgerIT` / `PurgeSweeperIT` / `JdbcPurgeQueueIT` — the two-phase
  delete: drain semantics, the staleness guard and revive race, the
  attempts→DLQ ladder (poison-key failing store), SKIP-LOCKED claiming.
- `StorageReconcilerIT` / `CoherenceProbeIT` — the orphan diff (min-age
  guard, dry-run rail, `blobs/` convention) and the manifest-repair probe.
- `DocumentEventOutboxIT` / `EventRelayIT` - the Kafka outbox: same-tx
  enqueue and rollback atomicity, SKIP-LOCKED claim order, the
  attempts→FAILED ladder, relay publish/consume round trip over Redpanda,
  dead-broker and crash-mid-flight recovery.
- `KafkaEventingIT` - eventing end to end through `RepoServices` (Postgres,
  LocalStack, Redpanda): save/delete commit points write the outbox
  atomically, the relay publishes, and a `ProtoMoltProtobufDeserializer`
  consumer reads and revalidates the events.
- `UploadHttpServerIT` — the streaming upload route over a real HTTP server:
  multi-MB streaming, byte-exact round trips, dedupe, the 411/400/404
  contract, checksum-mismatch rejection.
- `RemoteBlobStoreIT` — the dogfood blob store over the in-process gRPC
  transport.
- `SeedAccountDrivesIT` — the seeded default account: `intake`/`pipeline`
  drives appear with the provisioning defaults, seeding is idempotent across
  repeat runs, and an unset variable is a no-op.
- `RedisBlobStoreIT` — the Redis blob store against `redis:7-alpine`:
  verified writes, the two-entries-per-object mapping, 1000-key pipelined
  deletes, SCAN listing, TTL expiry.
- `CachingBlobStoreTest` — the caching decorator's read-through/write-through
  semantics over in-memory stores.

## Running

`RepoServiceMain` boots from the environment and serves gRPC (Netty) plus,
unless disabled, the HTTP upload route. To embed in-JVM instead, use
`RepoServices.build(config)` + `startInProcess(name)`.

| Env var | Default | Meaning |
|---------|---------|---------|
| `DOCUMENT_PLATFORM_GRPC_PORT` | `9090` | gRPC listen port |
| `DOCUMENT_PLATFORM_HTTP_PORT` | `8080` | HTTP upload listen port; `0` or `off` disables the HTTP server |
| `DOCUMENT_PLATFORM_JDBC_URL` / `_USERNAME` / `_PASSWORD` / `_POOL_SIZE` | local Postgres | Ledger database (see `LedgerConfig`) |
| `DOCUMENT_PLATFORM_S3_ENDPOINT` | _(provider default)_ | S3 endpoint override for S3-compatible stores (LocalStack, SeaweedFS, MinIO); path-style is forced when set |
| `DOCUMENT_PLATFORM_S3_REGION` | `us-east-1` | S3 region |
| `DOCUMENT_PLATFORM_S3_ACCESS_KEY` / `_SECRET_KEY` | _(SDK default chain)_ | Static credentials; both or neither. Blank = the AWS default credentials chain (IRSA/instance profile in prod) |
| `DOCUMENT_PLATFORM_DEFAULT_BUCKET_BASE` | `documents` | Provisioned drives without an explicit bucket get `<base>-<accountId>-<name>` |
| `DOCUMENT_PLATFORM_BLOB_STORE` | `s3` | Blob-store selection: `s3` (direct object storage), `repo` (delegate bytes to another repo-service over gRPC), `repo-inprocess` (same, in-process transport), `redis` (objects live in Redis), `s3-redis-cache` (S3 of record behind a Redis read-through/write-through cache) |
| `DOCUMENT_PLATFORM_REPO_TARGET` | _(none)_ | Required for the `repo` modes: `host:port` for `repo`, an in-process server name for `repo-inprocess` |
| `DOCUMENT_PLATFORM_REPO_DRIVE` | `default` | The drive the repo-backed store addresses on the remote service |
| `DOCUMENT_PLATFORM_REDIS_URI` | `redis://localhost:6379` | Redis connection URI (`redis://[:password@]host:port[/db]`) for the `redis` and `s3-redis-cache` modes |
| `DOCUMENT_PLATFORM_REDIS_TTL_SECONDS` | `3600` | Per-object TTL in Redis (0 = no expiry); the cache-entry TTL in `s3-redis-cache` mode |
| `DOCUMENT_PLATFORM_REDIS_MAX_OBJECT_BYTES` | `8388608` | Largest object admitted to Redis (0 = unbounded); the cache ceiling in `s3-redis-cache` mode — larger objects bypass the cache |
| `DOCUMENT_PLATFORM_LIFECYCLE_ENABLED` | `true` | Run the background purge loops (purger + sweeper) when `startLifecycle()` is called |
| `DOCUMENT_PLATFORM_PURGE_INTERVAL_MS` | `5000` | Purge-drain idle backoff; a non-empty drain loops again immediately |
| `DOCUMENT_PLATFORM_SWEEP_INTERVAL_MS` | `60000` | Sweeper rescan interval (also the reconcile loop's cadence) |
| `DOCUMENT_PLATFORM_RECONCILE_ENABLED` | `false` | Run the slow periodic storage-reconcile loop over every drive |
| `DOCUMENT_PLATFORM_RECONCILE_DRY_RUN` | `true` | Periodic reconcile reports only; `false` arms orphan deletion |
| `DOCUMENT_PLATFORM_RECONCILE_MIN_AGE_MS` | `3600000` | Min-age guard for the periodic reconcile (in-flight-upload protection) |
| `DOCUMENT_PLATFORM_KAFKA_BOOTSTRAP_SERVERS` | _(none)_ | Kafka bootstrap servers; unset = eventing off (no outbox writes, no relay, no producer) |
| `DOCUMENT_PLATFORM_KAFKA_TOPIC` | `document-events` | The document-events topic the relay publishes to |
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

### gRPC `DocumentService` (`repo/proto`)

- `SaveDocument` / `GetDocument` / `GetDocumentByReference` /
  `GetDocumentManifest` / `DeleteDocument` / `ListDocuments` — the
  claim-check document API (full and partial saves, partial reads).
- `GetBlob` — raw blob fetch by `FileStorageReference`.
- `PutBlob` — store raw blob bytes on a drive. Unary: the payload transits
  the service in memory, so this is for small/medium blobs — bulk bytes
  belong on the HTTP route. The write is verified: the server computes the
  SHA-256 and the store rejects landed bytes that mismatch it. A blank
  `object_key` generates a content-addressed key
  (`<drive.prefix>/blobs/<uuid-of-sha256>`), so identical puts are
  idempotent.
- `DeleteBlob` — delete by `FileStorageReference`; idempotent
  (`deleted=false` when absent).

### HTTP `POST /v1/documents:upload`

The streaming raw-upload route. The request body flows through a SHA-256
`DigestInputStream` straight into object storage — **no buffering in
memory** — then the assembled `Document` runs through the same intake save
as gRPC `SaveDocument`.

Identity comes from query parameters or headers (query wins):

| Param | Header | Required |
|-------|--------|----------|
| `account_id` | `x-account-id` | yes |
| `datasource_id` | `x-datasource-id` | yes |
| `drive` | `x-drive-name` | yes |
| `filename` | `x-filename` | yes |
| `content_type` | `Content-Type` | no (default `application/octet-stream`) |
| `connector_id` | `x-connector-id` | no |
| `crawl_id` | `x-crawl-id` | no |
| `doc_id` | `x-doc-id` | no (blank derives a name-based UUID from the content SHA-256) |

**`Content-Length` is required** — a deliberate contract: the S3 sync client
streams a PUT only with a known length, so chunked/absent-length requests
get `411 Length Required` before any byte is read.

An `X-Content-Sha256` header is verified against the digest computed while
streaming; a mismatch is a 400 and the landed object is best-effort deleted.

Success is `200` with a JSON receipt:

```json
{
  "node_id": "3f8a…",
  "doc_id": "…",
  "deduplicated": false,
  "size_bytes": 8388608,
  "sha256": "…",
  "storage_ref": {"drive_name": "intake", "object_key": "intake/blobs/<accountId>/<blobId>.bin"}
}
```

Errors: 400 (names the offending parameter, or a checksum mismatch), 404
(unknown drive), 405 (non-POST), 411 (absent/invalid Content-Length), 502
(backing-store or intake-save failure).

## The dogfood blob store

`RemoteBlobStore` (`repo/service/.../client`) implements the
`BlobStore` port over a `DocumentService` gRPC stub: any protomolt consumer
that speaks the port can use a repo-service as its byte store instead of
carrying an S3 SDK. `put`→`PutBlob`, `get`→`GetBlob` (NOT_FOUND maps to
`BlobNotFoundException`), `delete`→`DeleteBlob`. Documented gaps:
`headObject` is a full fetch whose bytes are discarded (the v1 API has no
cheaper probe), `copy` is a client-side get+put (no server-side copy across
the API yet), and `list`/`deleteAll`/`headBucket` throw
`UnsupportedOperationException`. Unary gRPC means the payload is in memory on
both ends — huge payloads belong on the HTTP upload route.

`DOCUMENT_PLATFORM_BLOB_STORE` picks the deployment shape:

- **`s3`** (default) — the service talks to object storage directly. Zero
  behavior change from the original wiring.
- **`repo`** — bytes delegate to another repo-service at
  `DOCUMENT_PLATFORM_REPO_TARGET` over gRPC.
- **`repo-inprocess`** — same delegation over the in-process transport (the
  embedded/test shape; the target is an in-process server name).

The repo modes must point at a DIFFERENT service set — pointing one back at
itself would recurse `PutBlob` into itself.

## The Redis blob stores

`RedisBlobStore` (`repo/container/.../blob`) is the port's second provider:
blocking Jedis on virtual threads, sample-grade but honest. Redis has no
buckets, so the physical key is `<keyPrefix><bucket>/<key>` — the bucket is a
namespace label, which keeps per-bucket `list`/`deleteAll` working and
buckets collision-free. Each object is two entries: the bytes, plus a
`$meta` hash (content type, etag, last-modified). Verified writes are
client-side: the SHA-256 is computed before the write and a mismatch rejects
the put (Redis has no server-side checksum trailer like S3's). Listing is
`SCAN MATCH` — a full-database walk, which is the sample-grade part.

`CachingBlobStore` is the read-through/write-through decorator:
`CachingBlobStore(backing, cache, ttlSeconds, maxCacheableBytes)`. The cache
is always expendable and the backing store is truth — every write lands on
backing first and is mirrored into the cache best-effort; reads serve cache
hits and populate on misses; `list`/`headBucket` never touch the cache.

Two more `DOCUMENT_PLATFORM_BLOB_STORE` modes wire them:

- **`redis`** — objects live in Redis outright.
- **`s3-redis-cache`** — S3 (of record) behind a Redis cache; the Redis TTL
  and max-object-bytes props become the cache TTL and ceiling.

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
