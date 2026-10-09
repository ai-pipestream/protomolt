# Review follow-up, 2026-10-07

An independent review re-ran every gate at `701b87864` (staging, both
consumer modes, owned-module tests): all exit 0, 241 tests, 0 failures,
errors or skips, and consumer-matrix inventories identical to the recorded
ones. It then fixed the places where the qualification worked around a
failure instead of surfacing it. Tests ran from a detached worktree with the
fixes applied on top of `701b87864`; the fixes were committed unchanged.

## Fixes

- Every Docker-backed IT in `repo/blob/{s3,redis,cache}` used
  `@Testcontainers(disabledWithoutDocker = true)` and would skip, not fail,
  on a host without Docker. The guard is removed from all seven (including
  the two new ones), so a missing Docker fails the gate. The three modules
  were run again after this change with `--rerun-tasks`: s3 74, redis 54,
  cache 5 tests, 0 failures, 0 skipped
  (`provider-module-tests-no-skip-guard.log`). `repo/container` and
  `repo/service` ITs still carry the guard; those modules are outside this
  assignment.
- `S3LocalStackConditionalIT` now runs on `localstack/localstack:4.13`
  (`sha256:46302bcb…`), which enforces `If-Match`. The 3.8 gap probe is
  replaced by real qualification: a stale matching write conflicts on every
  retry without changing bytes or version, a writer holding the current ETag
  succeeds, and an eight-way matching race has exactly one winner. Running
  the original gap probe on 4.13 failed with `BlobConflictException`, which
  is how the enforcement was established.
- `S3ProviderLifecycleTest` no longer accepts either of two exceptions after
  close. The measured failure is the SDK's `RejectedExecutionException` from
  its terminated timeout scheduler (untranslated by the adapter); the test
  pins exactly that, and passed three consecutive `--rerun-tasks` runs.
- `RedisPublishedConsumer` no longer catches and retries any
  `RuntimeException` from the store under test while waiting for the
  container. Readiness is `docker exec redis-cli ping` returning `PONG`; the
  provider's first operation must succeed with no retry.
- `CacheProviderIT` closes the Redis handle even when closing the S3 handle
  throws.
- The boundary classifier now covers the in-house JDBC, Kafka and server
  modules and the remaining third-party SQL, Kafka and server families in
  the version catalog. `verifyForbiddenDependencyDetection` asserts a
  representative coordinate per family and two legitimate client
  coordinates that must stay unclassified. No consumer row changed.
- `BrokenDiscoveryProbe` requires the `ServiceConfigurationError` to name the
  deliberately bogus registration rather than accepting any such error.

## Commands and exit statuses

Same commands as the parent README with repository
`<scratchpad>/maven-review-fixes` and candidate
`0.1.0-provider-qualification-review-fixes`; the POM-only run added
`--rerun-tasks`.

```
stage exit 0
gradle-module exit 0
pom-only exit 0
modules exit 0
```

JUnit totals: spi 7, s3 74, redis 54, cache 5, grpc 11, repo/spi 40,
codec 51 = 242 tests, 0 failures, 0 errors, 0 skipped. The S3 count rises by
one because the gap probe became two qualification tests.

## Provider option defaults removed

Neither provider defaults an option any more. S3 requires `credentials-mode`
and the four timeouts (previously static keys and 300000/60000/10000/60000 ms
when absent); Redis requires `write-policy` (previously `replace`). A missing
option fails with `IllegalArgumentException` before any client or pool is
acquired, covered by `S3ProviderTest.everyOptionIsRequiredWithNoDefault` and
`RedisProviderTest.writePolicyIsRequiredWithNoDefault`. An explicitly empty S3
`endpoint` still selects the regional AWS endpoint; it is a value the caller
must pass, not a default for a missing one. The repository host
(`RepoServices`) now passes the former values explicitly, and every test and
probe that opens a provider passes the full option set. The Java constructors
of `RedisBlobStoreConfig` are unchanged.

## Repository host Docker guard

`disabledWithoutDocker` is also removed from the 21 Testcontainers ITs in
`repo/container` and `repo/service`.

## Final run with every change

Candidate `0.1.0-provider-qualification-review-nodefaults`: staging exit 0,
Gradle-module consumer check exit 0, POM-only consumer check (with
`--rerun-tasks`) exit 0, matrix inventories unchanged. Module tests
(`no-defaults-all-repo-tests.log`, `--rerun-tasks --continue`):

```
repo/blob/spi        7 tests, 0 failures, 0 skipped
repo/blob/s3        81 tests, 0 failures, 0 skipped
repo/blob/redis     55 tests, 0 failures, 0 skipped
repo/blob/cache      5 tests, 0 failures, 0 skipped
repo/blob/grpc      11 tests, 0 failures, 0 skipped
repo/spi            40 tests, 0 failures, 0 skipped
repo/codec          51 tests, 0 failures, 0 skipped
repo/container    1831 tests, 17 failures, 1 skipped
repo/service       490 tests, 1 failure, 3 skipped
```

The four skips are opt-in benchmarks gated by
`@EnabledIfEnvironmentVariable` (`PROTOMOLT_*_BENCHMARK=true`); performance is
outside this assignment.

The 18 host failures are not caused by this branch. Running only those ITs at
`a9fefffe9`, which has no change to `repo/container` or `repo/service`,
produces the same 17 + 1 failures in the same classes
(`preexisting-host-failures-at-a9fefffe9.log`):
`DocumentOperationCommandsIT`, `DocumentRevisionSchema{Artifacts,Assets,Evidence}IT`,
`RepositoryCoordinator{Handoff,Supersession}IT`,
`RepositorySuccessor{Activation,Install}IT` and `ArchiveDeletionFailureIT`.
Causes seen: relation `repository_preparation_history_sets` does not exist,
"Publication codec requires typed command admission", "Repository execution
scope is absent", and an archive metadata version constraint.
