# Focused historical runtime qualification

Base: f62552f5df4669c8f3cf199e2e41a446304b74ae. Sol reviewed the extraction.
Source hashes identify the tested changes.

Command:

```
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --max-workers=2 --console=plain
```

Passed: 4 tests, 0 failures, 0 errors, 0 skips. Gradle: 4m17s;
JUnit: 253.603s, timestamp 2026-10-07T22:10:21.834Z.
The earlier individual commitWinner run passed in 1m17s.

Cases cover reconciliation, self-supersession, overlapping generations and
publication winning after SQL finalization. Each uses PostgreSQL and LocalStack
with probes compiled from the observed production JARs.

The shared compiler was compared with the original extracted block. The aggregate
host body is unchanged. The optional task is excluded from unit tests; check still
requires admissionStorageTest. The full aggregate was not rerun for this extraction;
its prior 13m03s result is recorded in 2026-10-07-publication-commit-winner.
This evidence does not establish completion of the repository goal.
