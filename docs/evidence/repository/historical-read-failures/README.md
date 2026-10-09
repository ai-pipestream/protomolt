# Historical read failure classification

Base: `5f8acfb413febd0a3ce1233d06d9a143d1220f86` (main). Branch: `agent/historical-read-errors`.
Date: 2026-10-09. The failure matrix, the mapping table and the suite mapping are in
[docs/testing/historical-read-failures.md](../../../testing/historical-read-failures.md).
This closes finding 4 of the
[backup rehearsal](../external-backup-rehearsal/README.md): a provider refusal during a
historical read reached library callers as `BlobStoreException` and gRPC callers as a
generic `INTERNAL "Historical read failed"`.

Every run used the shared host lock:

```
flock -w 600 /tmp/protomolt-repository-qualification.lock ./gradlew <tasks> --max-workers=2 --console=plain
```

Lock wait is recorded per run (`LOCK_ACQUIRED ... waited_seconds=`), separately from the
test outcome. Another agent's RustFS scaling benchmark series held the lock between runs
and ran on the same host; load averages are in each run's `gradle-run.log` (`LOAD_BEFORE`,
`LOAD_AFTER`). No timeout was raised to turn a failure green; two release checks in the
container suite were changed from instantaneous to bounded after the first green run showed
that a transport call's engine result and response snapshot are closed by the server thread
after the client already holds the answer (see the runs below).

## Environment

`environment.txt`: Java, OS, Docker server, image ids of `postgres:18-alpine`,
`localstack/localstack:3.8` and `rustfs/rustfs:1.0.0-beta.11-preview.1`, git head and
status at archive time, load average and the other containers running on the host.

## Runs

| Run | Sources | Tasks | Lock wait | Gradle exit | Result |
|---|---|---|---|---|---|
| `run-red` | base engine, new suites | container and service `*HistoricalReadFailure*` | 0 s | 1 | `HistoricalReadFailureIT` 2 tests, 2 failures (LocalStack and RustFS legs); `HistoricalReadFailureHostIT` 1 test, 1 failure |
| `run-green` | fix applied | engine, container and service `*HistoricalReadFailure*` | 164 s | 1 | engine 7/0/0/0, host 1/0/0/0, container LocalStack leg passed, RustFS leg failed on an instantaneous response-budget check after a successful transport call |
| `run-a` | bounded response-budget check | the same plus `GrpcErrorsTest` | 170 s | 1 | container RustFS leg failed on an instantaneous payload-budget check after a successful selected transport call (the materialization service sends inside its try-with-resources) |
| `run-a2` | bounded payload-budget check | the same | 189 s | 0 | container 2/0/0/0 (27.5 s), host 1/0/0/0 (cached from run-a), engine 7/0/0/0 (cached), `GrpcErrorsTest` 10/0/0/0 |
| `run-b` | final sources | `--no-build-cache`, `cleanTest` on all three modules, every engine test, container `*HistoricalReadFailure*` `*DocumentHistorical*` `*DocumentPublicationCommitIT*` `*RepositoryHistoricalGenerationsIT*`, service `*HistoricalReadFailure*` `*GrpcErrorsTest` `*ManagedDocumentHostIT` `*ManagedSchemaHostIT` `*DocumentPartReaderIT` `*ReaderHostCompositionIT` | 236 s | 0 | BUILD SUCCESSFUL in 3m 10s: engine 15 suites, 95/0/0/0; container 29 suites, 229/0/0/0 (`DocumentPublicationCommitIT` 33, `DocumentHistoricalRestoreAssessmentIT` 42, `DocumentHistoricalSelectionIT` 31, `HistoricalReadFailureIT` 2); service 7 suites, 78/0/0/0 (`DocumentPartReaderIT` 41, `LegacyDocumentPartReaderIT` 14, `HistoricalReadFailureHostIT` 1) |
| `run-c` | final sources | `:protomolt-repo-container:admissionStorageTest` | 600 s timeout (flock exit 1, Gradle not started, `run-c/gradle-run-lock-timeout-1.log`), then 158 s | 0 | BUILD SUCCESSFUL in 19m 39s: `DocumentAssessmentStorageRuntimeTest` 5/0/0/0 (1177 s) |

Counts are tests/failures/errors/skipped. Each run directory holds `gradle-run.log` and the
JUnit XML of the suites it executed.

## What the red run observed on the base

`run-red/junit-service-HistoricalReadFailureHostIT.xml`: the production host, endpoint made
unreachable, `readRaw` threw
`BlobStoreException: Object provider read could not complete <- SdkClientException: Unable to execute HTTP request: Connection refused (SDK Attempt Count: 4) <- ConnectException`
through `HistoricalDocumentRepository.readRaw`. Every baseline read, the unauthorized-caller
check and the pre-cancellation check passed before that point.
`run-red/junit-container-HistoricalReadFailureIT.xml`: on both LocalStack and RustFS the
first injected adapter code reached the caller as `BlobStoreException` from
`DocumentPartReader.readPart`. The suites stop at the first failing case, so the later cases
were observed only after the fix.

## Case outcomes after the fix (run-a2 and run-b)

Library code and message equal the in-process gRPC status code and description for every
row; "real" means a real response of the pinned image or socket, "injected" means the
labeled wrapper over the real adapter.

| Case | Kind | Raw, validated and materialized |
|---|---|---|
| successful retained reads | real, both providers and the host | bytes, document and selected occurrence equal through both transports; no provider call behind a returned result |
| foreign caller, provider would refuse | real ledger refusal | `NOT_FOUND "Document is unavailable"`; zero provider requests |
| cancelled control before provider access | injected control | `CANCELLED`; zero provider requests |
| every `BlobStoreException.Code` | injected | the table in the testing note; adapter failure on the cause chain |
| altered bytes, altered version id | injected over a real read | `DATA_LOSS "Document part disagrees with its published byte or provider identity"` |
| READ revoked while the provider fails | injected | `NOT_FOUND "Document is unavailable"`, provider detail suppressed |
| cancellation, expiry after provider bytes arrived | injected control | `CANCELLED "Document read cancelled"`, `DEADLINE_EXCEEDED "Document read deadline exceeded"`; the gRPC deadline case releases its snapshot, budget, pins and the single call slot |
| wrong credential | real, RustFS (container) and host | `FAILED_PRECONDITION "Original document backend refused the historical read"`, cause `BlobStoreException(PERMISSION_DENIED)`; LocalStack accepts any static credential and cannot inject this case |
| unreachable endpoint, then reachable again | real (closed loopback port; host: proxy listener closed) | `UNAVAILABLE "Original document backend is unreachable"`, cause `BlobStoreException(UNAVAILABLE)`; baseline reads equal afterwards |
| unknown version id | real, both providers | `DATA_LOSS "Published document part is missing from its original backend"`: both pinned images answer `NoSuchVersion` |
| deleted exact version, newer version present | real, both providers and the host | `DATA_LOSS "Published document part is missing from its original backend"`; every request after the deletion carries the recorded version id, the provider serves the newer bytes for an unversioned read, the intact revision still reads |
| receipt replay after the failures | real | equal to the original publication result, library and host, including the wrong-credential host |

Budgets, SQL read pins, outstanding captures, reader workers and slots are zero after every
case; the backend resolver only ever receives the recorded generation and profile; the
schema resolution count stays at the publication count.

## Not covered

- Damaged bytes under a retained version: a versioned bucket cannot alter a retained
  version in place, so the digest refusal is proven only through the wrapper.
- Redis profiles, the remote blob adapter and the cache decorator: not exercised; they
  reach the same engine boundary and the same table.
- Host shutdown refusals (`IllegalStateException "Reader admission is closed"`) and SQL
  failures keep their current shape by design; they are not provider conditions.
- Hosted CI; local runs only.
