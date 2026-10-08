# Public historical recovery after uncertain commit acknowledgement

Parent: `e8fd82f9bc62b9639043af8c44e56fc4546d2cce`.

A public predecessor completes a real LocalStack S3 upload while its reply is delayed.
After its lease expires, a public successor encounters an injected acknowledgement
loss on either reservation or installation. The fault matches the exact operation
and transaction: reservation uses PostgreSQL xmin, installation uses install_xid.
It commits first, cancels immediate confirmation through that request's control,
and raises SQL state 08006. Cancellation matters because a lost reply alone can be
recovered by the internal confirmation path without returning to the public boundary.

The failed call releases its borrow while retaining a distinct, unattached successor
identity and the committed recovery row. It creates no assessment, revision commit
or success, and performs no additional provider write. Changed destination condition,
historical object identity, mode, account binding and credential generation are then
refused through library and authenticated in-process gRPC without selector, schema
or storage work. The durable recovery row stays identical.

A valid gRPC retry resumes the same local successor identity and durable row, with
exactly one reservation and installation. It publishes once while the predecessor
reply remains delayed. Releasing that reply fences the predecessor; the successor
receipt stays unchanged. Actual provider readback verifies the publication, and
runtime cleanup returns generation slots and byte reservations.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.publicRecovery' \
  --max-workers=2 --console=plain
```

Final run passed in 30 seconds: one aggregate case, zero failures, errors or skips.
Sol reviewed matching, retained identities and cleanup without a blocker; its
suggestions to assert released borrow and absent publication rows were added.
Original final log and XML are archived here.

This covers committed reservation/install with acknowledgement loss plus cancelled
immediate confirmation, followed by retry in the same runtime. It does not cover
all rollback, corrupt-journal, cold-process or network failure combinations. Cold
restart has separate evidence; public corrupt-journal qualification remains open.
