# Installed historical cancellation boundary

Base `8261ae10429cb298d0417f3638fd03b2c5cec7d1`, plus the test fingerprint
retained here. No production or protobuf changes.

The first experiment attempted existing explicit cancellation against an actual
installed, unactivated historical successor. It failed at the owner fence with
`Coordinator successor requires exact activation`, confirming the V95 boundary.
This is a missing terminal path, not a reason to remove the execution guard.

The committed regression asserts this refusal, unchanged leases, no rejection,
no activation and unchanged capture state. A positive control genuinely activates
the same successor, then cancels, replays its receipt exactly and drains its
capture. It preserves the existing authority boundary; it does not implement
terminal recovery-limit handling.

The final command exited 0 in 20 seconds: 13 tests, zero failures/errors/skips.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalInstalledCancellationIT' \
  --tests '*DocumentCaptureAdmissionClosureIT' --max-workers=2 --console=plain
```

The two XML reports are in `focused-results.tar.gz`. Tests use real PostgreSQL 18;
source publication has synthetic provider observations. The design document now
specifies a separate same-transaction limit sidecar and terminal receipt, including
ownership, replay, lock order and validation acceptance criteria. That operation
and retained-root release remain unimplemented.
