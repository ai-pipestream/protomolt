# Historical rejection before cancellation

Base: 19714db2e. Real PostgreSQL and LocalStack. The existing rejection commit
fault now permits a test callback before the real JDBC commit. It selects the
transaction by account, principal, operation, generation, assessment and
creation_xid, after provider validation and rejection insertion have completed.

The callback starts cancellation through an independent transaction. PostgreSQL
must report one waiter blocked by the rejection backend, and the cancellation
future must remain unfinished. The callback then permits commit. SQLSTATE 08006
injection removes the commit acknowledgement from the rejection request.

Cancellation and authorized replay must return the same assessment rejection.
SQL must contain one decision and no published revision. Existing assertions
cover closed-reader retry, lease expiry and resource cleanup. The barrier uses
an isolated operation; it counts the waiter without capturing the cancellation
backend PID. The SQL lock timeout bounds cancellation if the test fails.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

Exit 0 in 1m3s: one aggregate, zero failures, errors or skips. The aggregate
requires the new race marker. Sol found no blocker. Production code is unchanged.
Cancellation-before-rejection and revocation during a pending decision remain
separate acceptance cases.
