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

## Not changed, recorded for the coordinator

- Production option defaults remain: S3 `credentials-mode` absent means
  static keys, S3 timeouts default when absent, an empty S3 endpoint means the
  SDK default endpoint, and Redis `write-policy` defaults to `replace`. These
  are part of the provider options contract used by `repo/container`, so
  removing them is a contract decision outside this assignment.
