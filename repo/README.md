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
| `repo/container` | `:protomolt-repo-container` | The storage engine: part codec, `BlobStore` port + `S3BlobStore`, part fan-out IO, the Postgres ledger (Hibernate + HikariCP + Flyway) |
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

## Building and testing

From the repository root:

```bash
./gradlew :protomolt-repo-proto:build :protomolt-repo-container:build :protomolt-repo-service:build
buf lint   # proto contract lint (repo/proto is a buf module)
```

Tests are testcontainers integration tests (Docker required): PostgreSQL 17
for the ledger, LocalStack for S3, the full stack booted through
`RepoServices` with no mocks.

- `RepoServiceIT` — the gRPC surface end to end: drives, full/partial saves,
  dedupe, delete/purge, `PutBlob`/`GetBlob`/`DeleteBlob`.
- `UploadHttpServerIT` — the streaming upload route over a real HTTP server:
  multi-MB streaming, byte-exact round trips, dedupe, the 411/400/404
  contract, checksum-mismatch rejection.
- `RemoteBlobStoreIT` — the dogfood blob store over the in-process gRPC
  transport.

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
| `DOCUMENT_PLATFORM_BLOB_STORE` | `s3` | Blob-store selection: `s3` (direct object storage), `repo` (delegate bytes to another repo-service over gRPC), `repo-inprocess` (same, in-process transport) |
| `DOCUMENT_PLATFORM_REPO_TARGET` | _(none)_ | Required for the `repo` modes: `host:port` for `repo`, an in-process server name for `repo-inprocess` |
| `DOCUMENT_PLATFORM_REPO_DRIVE` | `default` | The drive the repo-backed store addresses on the remote service |

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

## Future work

The `BlobStore` port currently lives in `repo/container` next to its only
implementation (`S3BlobStore`). As more backends land, the storage layer
splits into `repo/blob-store/spi` (the port), `repo/blob-store/s3`, and
`repo/blob-store/azure` — one new implementation per backend, no sweep
through the handlers.
