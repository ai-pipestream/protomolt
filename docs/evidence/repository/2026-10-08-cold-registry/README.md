# Cold recovery registry integration

Base: 5d4b61856. The registry admits cold proposals without a supplied retention
record. It reserves capacity and command memory before SQL, preserves retry
identity, and lets preparation own the recovered anchor. Source attachment and
later generation takeover use the resolved original anchor.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryHistoricalPreparationIT' --tests '*RepositoryHistoricalGenerationsIT' --tests '*RepositoryHistoricalGenerationLimitsIT' --max-workers=2 --console=plain
```

Exit 0 in 2m14s: 36 tests, zero failures, errors or skips. Archived XML contains
28 preparation, five generation and three generation-limit cases. An earlier
preparation run passed 27 cases before the final takeover case was added.

New coverage includes cold reservation/install acknowledgments lost after actual
SQL commits, retry across separate calls, shutdown in all three phases, budget
and capacity limits, changed modes/lease, warm-route collision, and exact decoded
command retry. Admission assertions verify no reservation or installation writes.
The takeover case installs and activates a cold generation, waits for its actual
lease expiry, activates another generation, disposes the predecessor, and repeats
START on the surviving generation. Final assertions check capture and memory
release. PostgreSQL is real; archived source fixtures use synthetic provider
observations.

Sol reviewed production and tests with no blocking findings. This evidence covers
local registry composition. Fresh-registry recovery through multiple successors,
real-provider execution after restart, separate-process recovery, and transport
parity remain open. Public historical routing remains disabled.
