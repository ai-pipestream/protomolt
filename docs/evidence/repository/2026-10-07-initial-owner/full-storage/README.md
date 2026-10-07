# Storage regression for initial ownership

Tested clean source commit: `54438e48429f0e8bb5e69c19f73d5c1e95bf1831`.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

Exit 0; BUILD SUCCESSFUL in 15m46s. One aggregate JUnit case, zero failures,
errors or skips; suite time 944.066s, timestamp 2026-10-07T23:28:51.816Z.
The command log and terminal XML are archived alongside this note.

This tests the production initial-owner checkpoint and the existing packaged
storage scenarios. The additional initial-owner provider host introduced in
`ec27ac5b5` is not included here; its focused evidence is recorded separately.
No throughput or latency qualification is claimed from this regression run.
