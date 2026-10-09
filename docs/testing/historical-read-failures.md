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
| I/O failure on an interrupted thread | `BlobStoreException(CANCELLED)` |
| content length disagreeing with the body | `BlobStoreException(DATA_LOSS)` |
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
flock -w 600 /tmp/protomolt-repository-qualification.lock ./gradlew \
  :protomolt-repo-engine:test --tests '*HistoricalReadFailure*' \
  :protomolt-repo-container:test --tests '*HistoricalReadFailure*' \
  :protomolt-repo-service:test --tests '*HistoricalReadFailure*' \
  --max-workers=2 --console=plain
```

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
