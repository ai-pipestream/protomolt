# Archive read failure classification

Base: `5f8acfb413febd0a3ce1233d06d9a143d1220f86` (main) plus the merges of
`agent/historical-read-errors`, `agent/publication-mode-efficiency` and `agent/archive-recovery`.
Branch: `agent/repository-train-followups`. The procedure and classifications are in
[docs/testing/archive-backup-qualification.md](../../../testing/archive-backup-qualification.md)
and [docs/testing/historical-read-failures.md](../../../testing/historical-read-failures.md).

Two findings of the review of those three branches are closed here:

- A bound archive read under a backend generation the host does not serve escaped as an
  untranslated `IllegalStateException` from the host's resolver in `ManagedArchiveServices`,
  on the library path and as a generic `INTERNAL` on gRPC. It is now `FAILED_PRECONDITION`
  "Original archive backend is not configured on this host" with the typed
  `UnservedBackendGenerationException` as cause, translated once in `ArchiveObjectReader`.
- A byte-flipped object on RustFS reached the engine as `UNAVAILABLE` with no fact telling it
  apart from an outage. The wire investigation below shows the provider gives no definitive
  damage signal, so the classification stays `UNAVAILABLE` and the S3 adapter's message now
  carries the received and declared byte counts.

Every gradle run used the shared host lock, with the wait recorded separately from the test
outcome in each run's `lock-wait.txt`:

```
flock -w 1800 /tmp/protomolt-repository-qualification.lock ./gradlew <tasks> --max-workers=2 --console=plain
```

## Environment

`environment.txt`: Java, OS, Docker server, image ids of `postgres:18-alpine`,
`localstack/localstack:3.8` and `rustfs/rustfs:1.0.0-beta.11-preview.1`, git head and status
at archive time, load average and the other containers on the host. Other agents' suites ran
on the same host between runs, serialized by the lock; no performance claim is made.

## Wire investigation of the damaged object (`wire-observations.log`, `wire-observations.sh`)

A disposable RustFS container on its own named volume, a versioned bucket, a 1 MiB object
(`part.1`, above the 512 KiB inline threshold) and a 2 KiB inline object, written with the
`aws` CLI; one byte flipped at offset 4096 of `part.1` on the stopped volume through a helper
container of the same image; the container restarted and probed with curl's SigV4 client, the
`aws` CLI and, in `RustFsDamagedObjectReadIT`, the SDK and the production adapter. The
container is created and removed by the script; the secret is redacted from the log.

| Probe | Observed |
|---|---|
| baseline GET and HEAD before the flip | `200 OK`, `content-length: 1048576`, bytes equal, SHA-256 equal |
| HEAD after the flip | `200 OK` with the same `content-length`, ETag and version id |
| GET by version id, latest, with `x-amz-checksum-mode: ENABLED` | `200 OK`, full header set, `content-length: 1048576`, then zero body bytes and an orderly close: curl `(18) end of response with 1048576 bytes missing`; botocore `IncompleteRead(0 bytes read, 1048576 more expected)`; SDK `RetryableException: Failed to read response. (SDK Attempt Count: 4)` |
| GET `Range: bytes=0-1023` (before the flipped byte) and `Range: bytes=4000-5000` | `206 Partial Content`, `content-length` 1024 and 1001, zero body bytes, same close |
| second and third GET | identical to the first |
| `GetObjectAttributes` | ETag, size and the stored checksum, no error |
| RustFS log | `bitrot reader hash mismatch ... data_len=1048576`, `Erasure decode failed during GetObject ... bytes_written: 0`, `HTTP transport failed ... error from user's Body stream` |
| the intact 2 KiB sibling | reads byte-equal throughout |
| a byte flipped 64 bytes before the end of the 2 KiB object's `xl.meta` | HEAD `200`; GET `503 Service Unavailable` with the XML error `SlowDown`, "Resource requested is unreadable, please reduce your request rate" |

No error status, error body or trailer accompanies the damaged part: the provider verifies
the whole data block's hash before emitting any byte, so the wire picture is a complete
success header followed by a body that ends short of `Content-Length`, which is also what a
connection dropped mid-body leaves. The inline case does carry an error status, but it is the
provider's throttling status and code. Neither is a signal a transport failure or an
overloaded provider cannot produce, so DATA_LOSS is not reachable for this provider and the
adapter reports UNAVAILABLE with the counts. A restarted RustFS also answers `503` with
`x-rustfs-readiness-pending` for a short time, which is why the tests wait for a signed
request to succeed rather than for `/health`.

## Runs

Counts are tests/failures/errors/skipped; `suites.tsv` lists every suite of every run. Each
run directory holds `lock-wait.txt`, `gradle-run.log` and the JUnit XML of the suites it ran.

| Run | Sources | Tasks | Lock wait | Gradle exit | Result |
|---|---|---|---|---|---|
| `run-red` | base plus the new suites and the typed exception class alone | service `*ArchiveBackendGenerationHostIT`; blob-s3 `*RustFsDamagedObjectReadIT`, `*S3BoundedReadFailureTest` | 0 s | 1 | `ArchiveBackendGenerationHostIT` 1/1/0/0: the library read raised `IllegalStateException: Original archive backend is not configured on this host` from `ManagedArchiveServices`; `RustFsDamagedObjectReadIT` 1/1/0/0: `BlobStoreException(UNAVAILABLE): Object provider read could not complete <- IOException: Premature EOF`, no byte counts; `S3BoundedReadFailureTest` 6/2/0/0 on the two short-body messages |
| `run-green` | both fixes, first version of the adapter read loop | engine `*HistoricalReadFailure*`; container and service `*Archive*` `*HistoricalReadFailure*`; the whole blob-s3 module | 0 s | 1 | 59 suites, 359 tests, 2 failures, both in blob-s3: `S3BoundedReadStreamTest.falseSmallLengthDoesNotBypassConsumptionLimit` (the loop stopped at the small declared length and answered DATA_LOSS where the contract is `BlobReadLimitException`) and `S3BoundedReadFailureTest.stalledBodyKeepsTimeoutMeaningAfterHeaders` (a socket timeout inside the body read was reported as the short body instead of DEADLINE_EXCEEDED); every other suite passed, including `ArchiveBackendGenerationHostIT` and `RustFsDamagedObjectReadIT` |
| `run-adapter-units` | corrected read loop: drain to the limit before classifying an over-long body, let timeouts and interrupts keep their codes | blob-s3 `*S3BoundedReadFailureTest`, `*S3BoundedReadStreamTest`, `*S3ReadFailureTest` | 0 s | 0 | 24/0/0/0 |
| `run-final-green` | final sources | the same tasks as `run-green` | 0 s | 0 | BUILD SUCCESSFUL in 8m 4s: 59 suites, 359 tests, 0 failures, 0 errors, 11 skipped (the 10 `ArchiveBackupQualificationIT` cases, which skip without the init script by design, and the opt-in `ArchiveReadBenchmarkIT`) |
| `run-final-qualification` | final sources | `-I .../qualification.init.gradle :protomolt-repo-container:test --tests '*ArchiveBackupQualificationIT'` | 0 s | 0 | BUILD SUCCESSFUL in 1m 48s: 10/0/0/0, zero skips; the four cases of interest are archived with markers, redacted command logs, host logs and results |

An earlier qualification run on the sources of `run-green` (before the adapter correction,
which touches neither the short-body path nor any archive path) passed 10/0/0/0 as well; it is
not archived because the final run supersedes it on identical cases.

## Classification of the four negative cases (final run, `run-final-qualification/negative-*`)

| Case | Library code and message | Cause chain | gRPC |
|---|---|---|---|
| new-generation | `FAILED_PRECONDITION` "Original archive backend is not configured on this host" | `RepositoryException(FAILED_PRECONDITION) <- UnservedBackendGenerationException` | `FAILED_PRECONDITION` with the same description (`new-generation.transport.read_refused`) |
| wrong-credentials | `PERMISSION_DENIED` (adapter message "Object provider rejected the read") | `RepositoryException(PERMISSION_DENIED) <- BlobStoreException(PERMISSION_DENIED) <- S3Exception` (403 SignatureDoesNotMatch) | unchanged |
| inconsistent-snapshot | `FAILED_PRECONDITION` | `RepositoryException(FAILED_PRECONDITION) <- BlobStoreException(FAILED_PRECONDITION) <- NoSuchBucketException` | unchanged |
| corrupt-payload | `UNAVAILABLE`; the adapter message on the chain is "Object provider ended the response body early: received 0 of 1048576 declared bytes in the single body read the SDK does not retry" | `RepositoryException(UNAVAILABLE) <- BlobStoreException(UNAVAILABLE) <- IOException` | unchanged |

In every case the pending mutation's recovery lane records BACKEND_RECLAMATION_FAILED and
RETRY_REQUIRED, manifests still read, and no byte is deleted.

## Document-side twin

`DocumentAttemptRecoveryService` builds its resolver with the same shape and the same
`IllegalStateException`, but `DocumentAttemptRecovery.recover` catches every runtime failure
and returns it as `Result.failure` with outcome RETRY; `reconcilePass` has no throwing public
path and the host lifecycle loop logs the result. The raw-object reclaimer's resolver in
`RepoServices` is only called from its lifecycle lane, which catches and logs. The document
read resolver (`ManagedDocumentServices.requireOriginal`) already raises
`FAILED_PRECONDITION`. None of them is changed.
