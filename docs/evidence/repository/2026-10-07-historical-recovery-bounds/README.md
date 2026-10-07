# Historical recovery bounds

Base `9a3fbadd2d5a28c085a76c8a6343e16ceb1b6246` plus the test fingerprint
retained here. Production and protobuf sources are unchanged.

`RepositoryHistoricalRecoveryBoundIT` uses PostgreSQL 18, actual lease expiry,
normal reservation/installation APIs and enabled SQL guards throughout.

- Original capture plus 15 activated successors fill the 16-batch limit.
  All 15 successor captures drain. Capture 17 still refuses: completed batches
  remain permanent evidence. No new V94, coordinator binding, V109 sidecar or
  batch survives. Existing leases, 15 drain receipts and 16 batches remain.
- A chain of installed but unactivated successors uses discovery, V98 supersession
  and V93 installation to reach 64 edges while retaining only the original batch.
  Historical activation succeeds at 64 and its capture drains. A real V97/V93
  successor creates edge 65, whose activation fails at the explicit V109 ancestry
  guard. The attempted activation/capture roll back; earlier execution, sidecar,
  batches, drain and current leases remain unchanged.

The capture-bound test first passed separately in 41 seconds. The final two-test
command exited 0 in 1 minute 52 seconds, with zero failures, errors or skips:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalRecoveryBoundIT' --max-workers=2 --console=plain
```

The final XML is in `focused-results.tar.gz`. Sol reviewed both constructions
and identified the following remaining product gap: refusal leaves an installed
operation that cannot activate. More recovery epochs cannot remove either bound.
An explicit terminal outcome and safe retained-source cleanup must be implemented
and tested before public historical recovery is enabled. These tests establish
atomic refusal, not automatic recovery from exhaustion.

Source publication uses synthetic provider observations. This does not establish
provider performance, deployed recovery, historical publication or root release.
