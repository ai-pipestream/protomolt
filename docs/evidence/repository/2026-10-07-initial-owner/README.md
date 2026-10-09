# Retained initial historical generations

Base: `dcdc3e98717a71c61678a258af63771f948a201f`. Sol reviewed the initial
lifecycle, accepted Work paths, retirement and reconstructed-retry correction.

```
./gradlew :protomolt-repo-container:test --tests '*RepositoryInitialHistoricalAttemptsIT' --tests '*RepositoryInstalledHistoricalAttemptsIT' --tests '*RepositoryHistoricalGenerationsIT' --tests '*DocumentPreparationCaptureDrainIT' --tests '*DocumentHistoricalExecutionIT' --tests '*DocumentHistoricalRegistrationAuthorizationIT' --max-workers=2 --console=plain
```

Initial entries reserve capacity before capture and retain registration, Work and
one execution handle across calls. JDBC faults exercise rollback and lost commit
acknowledgement. Shutdown waits for held Work and releases only its exact capture.
A real V97/V93 successor starts while initial Work remains held; retirement removes
the initial generation without losing the selected successor route.

Review found that record object equality rejected reconstructed exact retries.
The implementation now compares canonical preparation digests with bounded scratch
memory. A second suggested issue concerned non-ASCII mode keys. The attempted test
was correctly rejected by the existing ASCII member-ID contract; it was not evidence
of a production bug. `invalid-unicode-fixture.xml.gz` preserves that failed fixture.
The final retry fixture uses a permitted ASCII ID, and the contract is unchanged.

These are PostgreSQL lifecycle tests. Historical source publication observations
are fixture supplied. They do not establish provider-backed publication through the
new initial entry, managed host routing, or public historical availability.

Final source: 68 tests across six suites, 0 failures/errors/skips, Gradle 1m44s.
The initial-owner suite has 6 passing cases (16.202s), timestamp
2026-10-07T23:26:29.892Z. All six XML reports, Gradle output and source hashes are
archived here. The full packaged storage regression is a separate pending gate.
