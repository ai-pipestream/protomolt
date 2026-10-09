# Historical read failure classification

Provider failures during historical reads are translated once, at the engine boundary,
so that library callers and gRPC callers receive the same repository-domain code for the
same physical condition. The translation lives in `RepositoryErrors.historicalProvider`
and `HistoricalReadFailures.translate` (`repo/engine`) and is applied by every public
entrypoint of `DocumentHistoricalOperations`: `readRaw`, `readValidated` and both
`readMaterialized` overloads. Nothing in a provider adapter, the SQL ledger, the SPI or the
transports changes. Archived runs are in
[docs/evidence/repository/historical-read-failures](../evidence/repository/historical-read-failures/README.md).

## Entrypoints and where provider I/O happens

Source anchors are the current `repo/engine` and `repo/container` classes.

| Entrypoint | Provider I/O | SQL | Deferred work after return |
|---|---|---|---|
| `DocumentHistoricalOperations.readRaw` | `DocumentPartReader.readHistorical` reads every present fragment through `BlobStore.getBounded` on virtual-thread workers, before the method returns | capture (`DocumentReadLedger.captureHistorical`: authorization, plan, pins), delivery reauthorization | `RawRead.authorizeDelivery` is SQL only; `RawRead.close` releases the payload lease and the pin use locally; no provider call |
| `DocumentHistoricalOperations.readValidated` | the same raw read first, through `DocumentHistoricalReader.readValidated` | capture, retained schema replay (`PinnedHistory.validateFragments`, `DocumentHistoricalSchemas.check`), delivery reauthorization | `ValidatedRead.document` returns the already decoded proof; `close` releases validation leases, copy lease and the raw batch; no provider call |
| `DocumentHistoricalOperations.readMaterialized` (both overloads) | one ordinal through `DocumentPartReader.readHistorical(history, ordinal, control)` | capture, `PinnedHistory.materializeFragment` (`DocumentHistoricalMaterializer.read`: snapshot, decode, reauthorization), delivery reauthorization | `Result.view` reauthorizes through SQL on every call; `close` releases decoded copies and the pin use; no provider call |

Every provider byte is read before the entrypoint returns. There is no lazy provider read
or provider-side close in a returned result, so a provider that fails after return cannot
surface through `fragments()`, `document()`, `view()` or `close()`. The container suite
proves this with a recording wrapper whose call count stays constant across result use
and close.

## What the provider boundary throws

`BlobStore.getBounded` of the S3 adapter (`S3ObjectReads.read`):

| Provider condition | Adapter exception |
|---|---|
| `NoSuchKey`, `NoSuchVersion` (404) | `BlobStore.BlobNotFoundException` |
| object longer than the recorded size | `BlobStore.BlobReadLimitException` |
| 403 (signature mismatch, unknown key id, denied policy) | `BlobStoreException(PERMISSION_DENIED)` |
| 401 | `BlobStoreException(UNAUTHENTICATED)` |
| 400 | `BlobStoreException(INVALID_ARGUMENT)` |
| 301, 307, other 404 (for example `NoSuchBucket`) | `BlobStoreException(FAILED_PRECONDITION)` |
| `RequestTimeout`, 408, 504, SDK call or attempt timeout, socket timeout | `BlobStoreException(DEADLINE_EXCEEDED)` |
| 429 | `BlobStoreException(RESOURCE_EXHAUSTED)` |
| other 5xx, connection refused, other I/O failure | `BlobStoreException(UNAVAILABLE)` |
| body ending before `Content-Length`, by I/O failure or end of stream while the streamed body is read | `BlobStoreException(UNAVAILABLE)` with the message `Object provider ended the response body early: received n of m declared bytes in the single body read the SDK does not retry` |
| I/O failure on an interrupted thread | `BlobStoreException(CANCELLED)` |
| negative `Content-Length`, or a body longer than `Content-Length` | `BlobStoreException(DATA_LOSS)` |
| other 4xx | `BlobStoreException(UNKNOWN)` |

`DocumentPartReader.readPart` already converts `BlobNotFoundException` (DATA_LOSS for a
retained revision), `BlobReadLimitException` (DATA_LOSS), `UnsupportedOperationException`
(FAILED_PRECONDITION) and every byte, digest, content-type, version-id and ETag
disagreement (DATA_LOSS). `BlobStoreException` passes through `readPart`, the worker
`ExecutionException` unwrapping, `readHistorical`'s reauthorization and
`DocumentHistoricalOperations` unchanged. That is the gap: a library caller receives
`BlobStoreException`, and the gRPC services, which forward only `RepositoryException` and
`StatusRuntimeException`, answer a generic `INTERNAL "Historical read failed"`.

## Translation table

`RepositoryErrors.historicalProvider(BlobStoreException)`; applied to historical reads only.
Messages are constants; the adapter exception and its SDK cause stay on the cause chain,
which `GrpcErrors.map` never serializes.

| `BlobStoreException.Code` | `RepositoryException.Code` | Message | Reason |
|---|---|---|---|
| `PERMISSION_DENIED`, `UNAUTHENTICATED` | `FAILED_PRECONDITION` | Original document backend refused the historical read | the host's provider credential is refused; the caller is already authorized by the ledger, so `PERMISSION_DENIED` would misattribute the refusal to the caller, and the condition is not transient |
| `UNAVAILABLE` | `UNAVAILABLE` | Original document backend is unreachable | transient; retry is reasonable |
| `DEADLINE_EXCEEDED` | `UNAVAILABLE` | Original document backend did not answer in time | a provider-side timeout is a backend availability failure; the caller's own deadline stays `DEADLINE_EXCEEDED` through `RepositoryReadControl.check` |
| `RESOURCE_EXHAUSTED` | `RESOURCE_EXHAUSTED` | Original document backend read capacity is exhausted | provider throttling; retry later |
| `CANCELLED` | `CANCELLED` | Historical document read cancelled | the adapter observed an interrupted thread; no JDBC reauthorization is started on it |
| `NOT_FOUND` | `DATA_LOSS` | Published document part is missing from its original backend | the revision is authorized and retained; its physical version is absent; never a lookup elsewhere |
| `DATA_LOSS` | `DATA_LOSS` | Historical document part is damaged at its original backend | corruption is not a retryable outage |
| `FAILED_PRECONDITION` | `FAILED_PRECONDITION` | Original document backend cannot serve the retained revision | redirect or missing namespace at the bound endpoint |
| `INVALID_ARGUMENT` | `FAILED_PRECONDITION` | Original document backend rejected the retained object coordinates | the request is built from retained coordinates, not caller input |
| `UNIMPLEMENTED` | `UNSUPPORTED` | Original document backend does not support historical reads | same mapping as the write path |
| `ABORTED` | `CONFLICT` | Original document backend aborted the historical read | same mapping as the write path |
| `INTERNAL` | `INTERNAL` | Original document backend failed internally | |
| `UNKNOWN`, `ALREADY_EXISTS`, `OUT_OF_RANGE` | `UNKNOWN` | Original document backend rejected the historical read | no read meaning; not promoted to a retry promise |

`HistoricalReadFailures.translate(RuntimeException)` applies that table and, for the same
reason (a provider or resolver may raise them outside `readPart`), also maps
`BlobNotFoundException` to DATA_LOSS, `BlobReadLimitException` to DATA_LOSS,
`UnsupportedOperationException` to FAILED_PRECONDITION, `CancellationException` to
CANCELLED and `PayloadBudget.CapacityExceededException` to RESOURCE_EXHAUSTED. A
`RepositoryException` is returned unchanged. Every other exception is returned unchanged:
`PersistenceException` and `LedgerException` from SQL, `IllegalStateException` from a
closed ledger or a broken invariant, and `IllegalArgumentException` from a malformed
address keep their type for library callers and keep the generic `INTERNAL` answer on gRPC.
Translating them would turn SQL outages or bugs into provider classifications.

Order inside each entrypoint: translate, then `control.check()`, then the cancellation
or expiry shortcut, then delivery reauthorization. A caller whose READ access is revoked
while the provider fails therefore receives the ledger's `NOT_FOUND`, never the provider
code; the failing read's pin use and payload lease are released in `finally` with
`HistoricalReadFailures.release`, which keeps the primary failure and attaches a close
failure as suppressed rather than replacing it. No close in the chain
(`PayloadBudget.Lease`, `PinnedRead.Use`, `DocumentReadBatch`, `DocumentHistoricalValidation`,
`DocumentHistoricalMaterialization`) throws in practice; the suppression path is unit tested
with a synthetic close failure.

## Classifications that do not come from the provider

Unchanged, recorded here because the suites assert them next to the provider cases.

| Condition | Code | Source |
|---|---|---|
| caller outside the account, document unknown, revision unknown, status not AVAILABLE | `NOT_FOUND "Document is unavailable"` | `DocumentAdmissionAuthorization.authorizeHistory` |
| caller binding fails to preserve the authenticated principal | `PERMISSION_DENIED` | the gRPC services' `caller()` |
| no authenticated caller on gRPC | `UNAUTHENTICATED` | `ApiTokenServerInterceptor`, the services' `caller()` |
| cancelled or expired control | `CANCELLED`, `DEADLINE_EXCEEDED` | `RepositoryReadControl.check`; message replaced by "Historical read cancelled or expired" after provider work |
| payload or response budget exhausted | `RESOURCE_EXHAUSTED` | `DocumentPartReader.readFragments`, `HistoricalDocumentResponses.capture` |
| retained generation not mounted in this host | `FAILED_PRECONDITION "Original document backend is unavailable"` | `ManagedDocumentServices.requireOriginal` |
| archive binding under a generation or realm this host does not serve | `FAILED_PRECONDITION "Original archive backend is not configured on this host"` | `ArchiveObjectReader`, translating the host resolver's typed `UnservedBackendGenerationException` (`ManagedArchiveServices`) after authorization and the pin; the archive twin of the row above, pinned by `ArchiveBackendGenerationHostIT` (`repo/service`) and the archive qualification's new-generation case |
| digest, size, content type, version id or ETag disagreement | `DATA_LOSS "Document part disagrees with its published byte or provider identity"` | `DocumentPartReader.readPart` |
| retained schema bytes or evidence damaged | `DATA_LOSS` | `DocumentHistoricalSchemas.check`, `DocumentHistoricalMaterializer.read` |
| selected occurrence unknown | `NOT_FOUND "Historical occurrence is unavailable"` | `DocumentHistoricalMaterializer.read` |
| ledger admission closed by host shutdown | `IllegalStateException "Reader admission is closed"` (library), `INTERNAL` (gRPC) | `DocumentReadLedger.beginCapture`; unchanged, not a provider refusal |

## Wire mapping

`GrpcErrors.map` carries each `RepositoryException` code to the gRPC status of the same
name, with `CONFLICT` to `ABORTED` and `UNSUPPORTED` to `UNIMPLEMENTED`, and the message as
the status description. Parity between the library and the in-process transport is
therefore exact once the engine throws a `RepositoryException`; the suites assert code and
description equality rather than relying on this table.

## Suites

| Class | Module | Real integrations | Purpose |
|---|---|---|---|
| `HistoricalReadFailureTest` | `repo/engine` | none | translation table, pass-through of non-provider failures, close suppression, and regression tables that pin `RepositoryErrors.call` and `RepositoryErrors.managedFailure` for every `BlobStoreException` code and every other mapped exception |
| `HistoricalReadFailureIT` | `repo/container` | PostgreSQL 18, LocalStack 3.8 and pinned RustFS 1.0.0-beta.11-preview.1 | a typed revision published through `DocumentPublicationRuntime` to a versioned bucket, then: successful raw, validated and materialized reads; deleted exact provider version with a newer version present; unknown version id; wrong provider credential (RustFS, which validates signatures; LocalStack accepts any static credential and therefore cannot inject this case); unreachable endpoint; provider failures while READ access is revoked; cancellation during provider I/O; labeled synthetic injection of every `BlobStoreException` code and of altered bytes through a wrapper over the real adapter; library and in-process gRPC parity for every case; budgets, pins, slots and call capacity released; receipt replay unchanged |
| `HistoricalReadFailureHostIT` | `repo/service` | PostgreSQL 18, pinned RustFS behind an in-test TCP proxy | the production host (`RepoServices.build` with historical access and schema access): successful reads, endpoint unreachable and reachable again, deleted exact version with a newer version present, wrong credential in a second host over the same ledger; library and in-process gRPC parity; no provider access for an unauthorized caller |

Run:

```
flock -w 1800 /tmp/protomolt-repository-qualification.lock ./gradlew \
  :protomolt-repo-engine:test --tests '*HistoricalReadFailure*' \
  :protomolt-repo-container:test --tests '*HistoricalReadFailure*' \
  :protomolt-repo-service:test --tests '*HistoricalReadFailure*' \
  --max-workers=2 --console=plain
```

## Why DATA_LOSS is unreachable for damaged bytes on RustFS

The archive qualification's corrupt-payload case and `RustFsDamagedObjectReadIT`
(`repo/blob/s3`) flip one byte of an object's data file on the stopped volume of the pinned
image `rustfs/rustfs:1.0.0-beta.11-preview.1` and read it back. Observed on the wire, with
the SDK, the `aws` CLI and curl's SigV4 client against the same object:

| Request | Answer |
|---|---|
| HEAD, by version id or latest | `200 OK`, `Content-Length`, ETag, version id: no signal |
| `GetObjectAttributes` | the stored checksum and size: no signal |
| GET, by version id, latest, with `x-amz-checksum-mode: ENABLED`, or any `Range` (including a range entirely before the flipped byte) | `200 OK` or `206 Partial Content` with the complete header set and the requested `Content-Length`, then zero body bytes and an orderly close; no error status, no error body, and no trailer, since the body is `Content-Length` delimited |
| a second and third GET | identical; nothing changes server side |
| RustFS log | `bitrot reader hash mismatch`, `Erasure decode failed during GetObject ... bytes_written: 0`, `HTTP transport failed ... error from user's Body stream`; the hash covers the whole data block, which is why a range before the flipped byte fails too |

A client that sees `200`, the headers and a body that ends short of `Content-Length` sees
exactly what a connection dropped mid-body leaves behind. Treating that as DATA_LOSS would
classify every mid-body transport failure as corruption, so the adapter keeps UNAVAILABLE
and names the fact instead: `Object provider ended the response body early: received 0 of
1048576 declared bytes in the single body read the SDK does not retry`, with the HTTP
client's `IOException` (`Premature EOF` from the URL-connection client) as cause. The engine's
DATA_LOSS for damaged content comes only from the digest and size checks on delivered bytes
(`DocumentPartReader.readPart`, `ArchiveObjectReader`), and this provider never delivers a
byte of a block whose hash mismatches, so that branch is unreachable for it.

The two read paths differ only in where the short body is seen. `getBounded` streams the
body outside the SDK's retry loop: one body read, the counts above in the adapter message.
`get` uses `getObjectAsBytes`, where the SDK reads the body inside its retry loop and gives
up after four attempts; the adapter's cause chain then carries
`RetryableException: Failed to read response. (SDK Attempt Count: 4)` and no byte counts,
because the SDK does not expose them.

A byte flipped 64 bytes before the end of a 2 KiB object's `xl.meta` (an inline object below
the 512 KiB threshold has no part file) is answered differently: HEAD still `200`, GET
`503 Service Unavailable` with the XML error `SlowDown`, "Resource requested is unreadable,
please reduce your request rate". That is the same status and error code the provider uses
for throttling, so it maps to UNAVAILABLE through the 5xx row and is not promoted either;
distinguishing it by message text would be a heuristic.

A closed port is the other UNAVAILABLE: `SdkClientException` with connection refused on the
cause chain and no byte counts in the message. Both unit and RustFS tests assert the two
messages are distinct.

## Injection labels

- Real provider: credential refusal, connection refused, deleted version, unknown version
  id and the successful reads are real responses of the pinned images through the
  production S3 adapter.
- Synthetic over real: `RecordingStore` wraps the real adapter to count and record
  requests, to raise a chosen `BlobStoreException` code, to return altered bytes, to
  revoke READ access during a read, or to cancel the control during a read. These cover
  provider codes the pinned images do not emit on demand and timings that cannot be
  reached from outside. A versioned bucket cannot have the bytes of a retained version
  altered in place, so damaged bytes are injected only through this wrapper and the
  digest check in `DocumentPartReader.readPart` is what refuses them.
- The TCP proxy in the host suite forwards bytes unchanged; closing its listener produces
  a real connection refusal in the SDK.
