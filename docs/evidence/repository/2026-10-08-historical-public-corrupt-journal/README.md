# Public cold recovery refuses a corrupt preparation

Parent: `1fdaf1d5d7ac0e1345575dee701388d06c0787ac`.

The writer exits with `Runtime.halt(23)` after reserving a successor. The driver
confirms process and SQL-session termination before starting a new recovery JVM.
That process receives only the caller request and upload bytes. It loads neither
the predecessor's Java objects nor a cached preparation.

The fixture changes one byte in the exact operation's generation-zero preparation,
preserving its stored digest. Normal SQL prevents this state. In this isolated test
database, one transaction disables the immutable preparation trigger, drops the
digest constraint, changes the row, reinstates the constraint as NOT VALID and
reenables the trigger. Subsequent writes remain guarded. Repair restores the original
bytes and validates the constraint in one transaction.

Library and authenticated in-process gRPC calls must both return DATA_LOSS with
the exact preparation integrity error. Refusal leaves the damaged row unchanged
and creates no successor installation, assessment owner, revision commit or success.
Selector, schema, historical provider and upload backend counters stay zero.
After repair, the same runtime publishes once and returns the durable receipt.
Actual provider readback checks both new upload bytes and the original historical
object identity. Shutdown releases local read pins and byte reservations.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.coldPublicCorruptPreparation' \
  --tests '*HistoricalRuntimeQualificationTest.coldPublicProcessRestart' \
  --tests '*HistoricalRuntimeQualificationTest.coldProcessRestart' \
  --max-workers=2 --console=plain
```

Final run passed in 3 minutes 46 seconds: seven cases, zero failures, errors or
skips. This includes all three existing private and public crash phases plus the
new corrupt-preparation case. The final log and XML are archived here. Sol reviewed
the final assertions and the managed-host integration requirements without a blocker.

The archived message-assertion failure is a fixture error: the remote client wraps
the RPC failure, while its gRPC cause retains the exact status description. The
corrected assertion checks that description and code; the library assertion checks
its direct exception message. All six existing restart cases passed in that run.
Sol also caught and reviewed the correction of a driver condition that would have
required private-path markers from ordinary public restart cases.

This test covers corrupt preparation bytes after a reserved-phase process crash.
It does not qualify every journal table, arbitrary administrative changes, network
socket failures, storage durability, throughput or horizontal scaling. The
historical factory remains package-private pending managed-host integration.
