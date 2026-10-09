# Historical cleanup fairness and retry

Parent: `4b45f4521a82e991be18eb28de379a05c53ac0e1`.
Sol reviewed the production rotation, retained-worker tests and public cleanup
authority failure scenario. The source checkout is `historical-cleanup-fairness`;
the separate full regression on the parent remains independently tracked.

## Bounded maintenance

`retireReady(limit, ...)` previously restarted at the first ready generation on
every pass. If the first `limit` entries retained workers or repeatedly failed
authority lookup, later ready entries could remain unreclaimed indefinitely.

The selected ready entries now move to the tail of the existing ordered generation
map before disposal begins. Rotation occurs under the short registry monitor;
authority lookup, SQL and resource disposal still happen outside it. Retry routing
uses a separate map and is unchanged. Entry identities, ownership, retained bytes
and disposal proof requirements are unchanged. Each candidate is rechecked before
borrowing, including when a concurrent maintenance call has changed its state.

Two real-PostgreSQL cases failed before this fix. Both establish actual terminal
outcomes, retain source workers for two generations, and release only the second
worker. With a one-entry limit, the first pass either waits for the held worker or
reports an injected authority failure. The second pass must retire the ready later
generation. The held generation remains retained until its worker is released;
another pass then retires it and returns the byte budget to zero.

The lifecycle fixtures use synthetic provider observations for source setup, as
declared in their test class. They prove SQL/ownership lifecycle and scheduling,
not provider throughput or successful data publication.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalAttemptRetirementIT' \
  --tests '*RepositoryInstalledHistoricalAttemptsIT' \
  --tests '*RepositoryInitialHistoricalAttemptsIT' \
  --tests '*RepositoryHistoricalInstalledCancellationIT' \
  --max-workers=2 --console=plain
```

Passed in 1m7s: 28 tests, zero failures, errors or skips. The original two failures
and final four XML reports are archived, with their Gradle logs.

## Public cleanup authority failure

The separate production-JAR runtime probe uses real PostgreSQL and LocalStack S3.
After a public historical publication commits, the host's exact-key cleanup
authority resolver is made to throw a specific exception. Library and authenticated
in-process gRPC terminal retries remain available. Maintenance reports that same
exception and retains the generation, read captures and reserved bytes. Clearing
the fault allows the next maintenance pass to retire it; another remote operation
then succeeds. No extra PUT, selection or schema resolution occurs during the
failed cleanup and terminal replays.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.initialOwner' \
  --tests '*HistoricalRuntimeQualificationTest.overlappingGenerations' \
  --max-workers=2 --console=plain
```

The public case proves authority lookup failure and retry, not a failed SQL
pin-release transaction. The fairness proof is the separate bounded SQL test,
not a public multi-operation load test. Neither establishes throughput or a
cleanup latency guarantee. Public historical factory exposure still requires its
remaining authorization, recovery, deadline and SQL cleanup acceptance cases.

The runtime command passed in 2m11s: two aggregate JUnit cases, zero failures,
errors or skips. `public-gradle.log.gz` and `public-test.xml.gz` preserve this
initial-owner/public-cleanup and overlapping-generation run against the rotation.
