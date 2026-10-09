# Cancellation during historical assessment replay

Base: 37e832b05. Real PostgreSQL and LocalStack. After the real provider returns
an assessment batch, a wrapper commits explicit cancellation before delivering
the batch to validation. Replay must report TerminalOperationException. The batch,
assessment session and temporary memory must be released.

Retry with the provider reader closed must return the explicit cancellation
receipt. An independent transaction confirms the same receipt, one rejection and
zero published revisions. Historical source protection remains until shutdown;
final cleanup must restore the initial memory budget.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

Exit 0 in 1m3s: one aggregate, zero failures, errors or skips. The aggregate
requires HISTORICAL_CANCELLATION_BEFORE_REJECTION_OK. Sol found no blocker.
Production code is unchanged. This is cancellation during provider replay, not
simultaneous SQL decision queues. Pending-decision revocation remains unqualified.
