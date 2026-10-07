# Private historical attempt retirement

Base `22fe8642ce184923c9fba038681d55bf683a67e6`, with sources identified by
`sources.sha256`. Sol reviewed the implementation, provider probes and final
cancellation test without a blocking finding. No protobuf changes.

The installed owner can retire an exactly authorized terminal operation or a
permanently fenced claim. Expiry alone refuses. Proof makes the entry disposal-only;
worker drainage precedes capture release, byte refunds and map removal. Cancellation
or timeout retains cleanup state. Private cleanup after proof does not grant receipt
delivery authority when the original credential has been revoked.

## Local qualification

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalAttemptRetirementIT' \
  --tests '*RepositoryInstalledHistoricalAttemptsIT' \
  --tests '*RepositoryHistoricalCaptureDisposalIT' \
  --tests '*RepositoryHistoricalSuccessorActivationIT' \
  --max-workers=2 --console=plain
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalAttemptRetirementIT' --max-workers=2 --console=plain
```

All commands exited 0. Initial focused run: 37 cases, zero failures/errors/skips,
1m21s. The packaged production-JAR gate passed in 10m20s: one aggregate test,
618.046 seconds, zero failures/errors/skips. It requires retirement markers in both
the initial host and supplemental historical reconciliation host, with actual provider
publication/readback and unchanged receipt replay after retirement.

After that gate, one cancellation test was added without changing production code,
the runtime driver or packaged probes. The final retirement suite passed in 40s:
10 cases, 37.353 seconds, zero failures/errors/skips. Together with the 28 unchanged
regressions this qualifies 38 distinct focused cases; it is not a single 38-test run.
`packaged-sources.sha256` records the pre-cancellation source identity; the final
hash list differs only for the expanded test class. The packaged gate was not rerun
for that test-only addition. Both sets of XML and Gradle logs are archived.

The focused tests cover pending refusal without disabling execution, immediate and
held-worker retirement, terminal commit reply loss, credential revocation before and
after proof, expiry versus permanent fencing, a later-generation terminal outcome,
and two independent operation keys sharing historical input. The later-generation
test first proves that V101 rejects changed modes, then installs a mode-preserving
successor. The cancellation case observes actual root Work closure after proof while
a child remains active; it requires retained bytes/pins, zero premature drain markers,
refused mutation and successful fresh-control cleanup after the child exits.

These SQL fixtures use synthetic provider observations for source setup. Real provider
evidence comes from the packaged gate. SQL lock timeouts in fixtures, production
deadlines and host process caps were not relaxed for this checkpoint.

## Limits

This is a private installed-plan owner. Reservation/install uncertainty, managed
historical routing and library/gRPC qualification remain incomplete. Public routing
must replay terminal outcomes before allocating a replacement entry. No public
historical API was enabled. This evidence does not establish hosted CI, throughput,
horizontal scaling, pruning completeness, JCR compliance or completion of the goal.
