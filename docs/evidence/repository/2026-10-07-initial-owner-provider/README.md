# Initial historical owner: provider qualification

Base: `54438e48429f0e8bb5e69c19f73d5c1e95bf1831`. Source hashes identify the
uncommitted test changes exercised by this run. Production sources are unchanged.

Command:

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

Exit 0; BUILD SUCCESSFUL in 26s. One JUnit test, zero failures, errors or skips;
23.818s suite time, timestamp 2026-10-07T23:33:28.345Z. The archived XML and command
log are the terminal reports. This is correctness evidence, not a performance run.
The test uses PostgreSQL 18 and LocalStack 3.8, plus the packaged runtime bundle.

The host requires normal initial publication and lost-CREATE-reply reconciliation
markers. Both scenarios allocate the retained initial entry before its own capture,
resume across calls, upload through the provider, verify exact version bytes and
receipt contents, refuse repeated publication, and require one START, CREATE and
publication. Neither creates a successor reservation or installation. Held source
Work prevents capture drainage; releasing it permits one exact drain and receipt
replay, with memory returned to baseline.

The surrounding fixture prepares a source revision and command before the new
entry's capture. This does not claim that all fixture reads occur after reservation.
The private owner is exercised directly; public historical routing, cold anchor
loading, managed host shutdown and library/gRPC parity are not qualified here.

The full storage gate now includes an independent initial-owner host database.
Its result is pending; the focused test alone does not establish the full gate.
Two preceding attempts failed compilation in the test harness (source-list omission
and a checked exception in a resolver lambda); both were corrected before this run.
