# Historical runtime accepted-call scope

Base: 5a4067618. The PostgreSQL/LocalStack packaged aggregate passed in 33 seconds,
exit 0, one aggregate test with zero failures/errors/skips. Source hashes and XML
are adjacent.
Sol reviewed the scoped action and probe with no blocking findings.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

The historical shutdown probe now creates its owners inside the runtime's
synchronous historical action. It closes runtime admission before the action
returns, then verifies a zero-wait shutdown cannot detach either generation even
though no registry borrow remains active. A new action is refused with UNAVAILABLE
without executing its callback. After the original action returns, the previously
qualified authority-failure, held real read batch and cleanup-retry cases run.

The fixture keeps a registry reference after the action solely to inspect its
drain counts. Production actions must not export registry references or borrowed
calls. Public historical dispatch remains disabled; eventual facade integration
must continue its existing accepted scope rather than reopen admission on retry.
