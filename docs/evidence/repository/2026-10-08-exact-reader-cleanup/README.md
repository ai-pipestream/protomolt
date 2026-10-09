# Exact reader resource cleanup

Code checkpoint: `750bdca53` on `agent/host-reader-composition`.

```sh
./gradlew :protomolt-repo-container:test --tests '*ArchiveExternalQuiescenceIT' --tests '*DocumentAssessmentReadSessionIT' --tests '*ArchiveReadQuiescenceIT' --max-workers=2 --console=plain
```

23 tests passed, no failures, errors or skips; 24-second build. Real PostgreSQL
runs V115 and the existing guards. Managed-child termination supplies the test
receipt. Synthetic SQL resources do not qualify a production provider's shutdown.

Checks cover partial archive cleanup failure, retry, foreign retention blocking
physical deletion, invalid batch limits, and rejection of empty recovery for
ACTIVE, FENCED or unknown readers. Document cleanup uses a shared limit of one
across native parts and assessment sessions. Foreign sessions and LOCAL_DRAIN
provenance remain intact. Existing global archive and assessment tests pass.
Sol reviewed the implementation with no blocker.

Exact archive recovery has no per-reader cursor; a persistent failure can delay
later work with a one-item batch. Errors remain visible. Automatic supervisor
scheduling and load qualification remain separate work. No protobuf changed.
