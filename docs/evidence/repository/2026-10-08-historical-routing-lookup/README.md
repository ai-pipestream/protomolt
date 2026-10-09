# Selected historical generation routing

Base: `434c219861daca924fbc8f8c81b1890fca7db7a9`, with the lookup and test changes
in the commit containing this evidence.

The selected-generation lookup returns local routing facts without borrowing the
attempt, accessing SQL, or changing retention. Exact caller and command identity
are checked. The result can become stale and grants no mutation authority.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryInitialHistoricalAttemptsIT' \
  --tests '*RepositoryInstalledHistoricalAttemptsIT' \
  --tests '*RepositoryHistoricalAttemptRetirementIT' \
  :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.overlappingGenerations' \
  --max-workers=2 --console=plain
```

Passed in 1m59s. The three SQL suites contain 25 tests, with no failures, errors
or skips. They cover absent, borrowed and attached lookup states, unchanged
retention, conflicting command/caller identity, and closed-registry rejection.

The overlap probe was then strengthened to keep the predecessor attempt borrowed
through successor reservation. The final probe was rerun with:

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.overlappingGenerations' \
  --max-workers=2 --console=plain
```

Passed in 1m4s: one aggregate test, no failures, errors or skips, suite time
62.65 seconds. It uses the production JAR with PostgreSQL and LocalStack. It
asserts that lookup can identify the borrowed predecessor, successor reservation
can proceed, and the predecessor cannot mutate after local supersession. Existing
checks continue through successor installation and publication, preserving old
history until its provider work drains.

The archive contains the SQL XML, both command logs and the final runtime XML.
Public historical dispatch remains gated; this test does not qualify that API.
