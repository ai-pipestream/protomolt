# Initial historical capture classification

Base: `9fe0e9ac05d310951b5f624ade7e62e0078fbc43`. Sol reviewed the production
classifier, release confirmation and test additions. No public contract changed.

```
./gradlew :protomolt-repo-container:test --tests '*DocumentPreparationCaptureDrainIT' --tests '*DocumentPreparationRootReleasesIT' --max-workers=2 --console=plain
```

The private classifier distinguishes a rolled-back local capture from an exact
committed initial registration. It requires process authority and an already joined
registration call. Wrong modes and partial capture evidence fail explicitly.

Tests use real PostgreSQL with controlled JDBC faults before and after commit.
They cover held Work, independent readers, a different same-key initial winner,
actual V97 takeover, terminal cancellation and V111 root release, and deliberate
corruption of a batch, source-pin rows and capture ownership. The V111 path checks
exact terminal evidence and the permanent capture fingerprint, including V107 drain
records. Confirmation takes row locks but performs no mutation.

Historical source fixtures supply publication provider observations. These tests
make no object-provider durability or throughput claim. The initial-entry owner,
its stop/join enforcement and public historical routing remain separate required work.

Final source passed all 41 tests with no failures, errors or skips in 42s.
Capture suite: 32 cases, 39.001s, timestamp 2026-10-07T23:16:55.453Z.
Root-release suite: 9 cases, 13.374s, timestamp 2026-10-07T23:16:55.451Z.
Archived XML and Gradle output accompany the exact source hashes.
