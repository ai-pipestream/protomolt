# Local repository performance with RustFS

The opt-in replica, archive-read and partial-update diagnostics now use the
repository's existing RustFS deployment pin, `1.0.0-beta.11-preview.1`. LocalStack
correctness fixtures are unchanged. The three benchmark classes run sequentially
in one Gradle test fork, and enabled benchmarks cannot use cached/up-to-date test
results. This is a local desktop-oriented reference, not production qualification.

Each environment file records the Docker image tag and local image ID, endpoint
and actual storage mounts. These runs used Docker local volumes mounted at `/data`.
The image ID is not a registry manifest digest. Containers have no explicit
CPU/memory limits; host load and storage configuration remain uncontrolled.

## Replica workload

`replicas` preserves 9,984 raw request rows, 48 client windows and 24 mixed-window
lock captures. All six 1/2/4-process SQL-budget configurations passed byte equality,
history, retry-manifest, conflict-winner and exact final counter checks. Measured
mixed throughput ranged from 225 to 276 requests/second. Additional replicas did
not consistently improve throughput. All 1,976 distinct sampled waiters in the
measured mixed windows involved the archive-statistics statement. Observations
are distinct `(window,sample,pid)` tuples, not request counts or wait duration;
multiple blocker rows do not count a waiter twice. The raw archive retains empty
polls. `summary.csv` excludes warmup -1 and divides total mixed operations by total
mixed window time, including driver assertions.

The storage change did not remove the observed SQL coordination bottleneck.
These sequential runs do not constitute a controlled RustFS-versus-LocalStack
comparison. Neither backend's measurements establish service CPU saturation or
production capacity. The workload still excludes destructive operations and
public typed-document publication.

## Archive reads and partial updates

`archive-reads` retains batch and per-read samples for independent/shared objects,
4 KiB/256 KiB payloads, and 1/4/16 readers. The intentionally unsafe unpinned path
is a diagnostic comparison only; it is not a proposed production configuration.
All payload, provider-call, SQL-operation and drained-resource assertions passed.

The first partial-update run failed during PUT with an S3 400 response. A targeted
assertion then demonstrated that newly generated attempt keys contained `//`:
`ManagedDocumentSave` added a trailing slash and `DocumentPartCodec.objectKey`
added another. Removing the trailing slash in the two new-key construction sites
allowed the same RustFS workload to pass. No codec-wide normalization, retained
address rewriting or compatibility fallback was added. Existing keys remain
opaque and readers/cleanup still use their stored addresses exactly.
After the fix, 59 related tests passed: 14 legacy part-reader cases, 41 managed
part-reader cases, three managed-write failure cases and one save-candidate case.
There were no failures, errors or skips in that regression run.

`partial-updates` records the successful rerun: 48 measured samples, with two
warmups and twelve samples for each size/path. Full and partial cases start with
fresh equivalent documents; one character changes in one of 32 chunks. The
synthetic chunk-string lengths are 4,096 and 262,144 characters, not serialized
payload sizes. Small full/partial p50 was 349/394 ms; large full/partial p50 was
556/573 ms. These are single-run diagnostics. This older copy-based path still
writes all 33 parts; it does not qualify the new immutable-part reuse or typed
publication coordinator.

Replica and archive-read evidence predates the two-line key fix; that fix affects
new managed document saves, not their archive operations. `sources.sha256` records
the final fixture and fixed writer sources. The failed writer's SHA-256 was
`62a5c1f9b797b63c35a9af4527e70b4c4e49218192b601787b605635b163e379`, from base commit
`dfdc0a92`. Full benchmark-command attempts: first compile failed on a Docker Java
metadata accessor; after correcting it, two benchmarks passed and partial staging
failed; the separator assertion reproduced red; the fixed partial run passed.
No failed attempt is counted as a successful complete suite.

```sh
PROTOMOLT_REPLICA_BENCHMARK=true PROTOMOLT_REPLICA_WORKERS=32 PROTOMOLT_ARCHIVE_READ_BENCHMARK=true PROTOMOLT_PARTIAL_BENCHMARK=true ./gradlew :protomolt-repo-service:test --tests '*RepositoryScaleBenchmarkIT' --tests '*ArchiveReadBenchmarkIT' --tests '*DocumentPartialBenchmarkIT' --console=plain
PROTOMOLT_PARTIAL_BENCHMARK=true ./gradlew :protomolt-repo-service:test --tests '*DocumentPartialBenchmarkIT' --console=plain
```

These are local test results. They do not establish hosted CI, deployment,
conformance for every RustFS/S3 operation, or completion of the repository goal.
