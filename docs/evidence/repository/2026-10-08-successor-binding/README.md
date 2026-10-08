# Concurrent successor receipt binding

Base: 398d1f190. The adjacent XML records 11 real PostgreSQL tests, with zero
failures, errors or skips. Gradle exited 0 in 32 seconds.

Command:

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryHistoricalSuccessorPinsIT' --max-workers=2 --console=plain
```

The successor binding atomically records the first verified activation receipt.
Later checks compare the complete receipt. Four concurrent callers share one
binding in the activation/capture test; registration row locks may serialize
verification. This is not a lock-free throughput measurement.

Sol reviewed the source and test change with no blocking findings. Historical
upload child ownership and runtime integration remain unfinished. Public
historical publication remains disabled.
