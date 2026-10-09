# Historical publication commits before ACL administration

Parent: `c92640328392511fc740afcf32a60ca77e413749`.
Sol reviewed the SQL ordering, receipt checks and resource cleanup without a blocker.

The production-JAR host runs READ and WRITE cases through library and authenticated
in-process gRPC, with PostgreSQL and LocalStack S3. A JDBC gate selects only the
exact operation success row created by the current transaction. Before commit,
PostgreSQL must identify the publisher PID as a blocker of the exact ACL writer PID;
the success row must remain invisible to an independent transaction.

Publication commits first. The ACL writer then commits before the JDBC publication
reply returns. Removing READ denies delivery and retry with NOT_FOUND despite the
durable publication. Removing WRITE preserves exact committed receipt delivery and
replay because READ remains available. The receipt is compared with SQL result_bytes;
each operation has one success, assessment owner and revision commit afterward.

Failure cleanup releases the JDBC gate before joining worker threads. The original
ACL is restored only after those threads finish. A separate host mode avoids adding
these cases to the existing initial-owner runtime budget.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.publicCommitWinner' \
  --max-workers=2 --console=plain
```

Passed in 29 seconds: one aggregate case, zero failures, errors or skips. All four
READ/WRITE library/gRPC markers are required. The first run completed the scenarios
but the driver demanded unrelated recovery markers; the new-mode check was corrected.
Original final log and XML are archived here.

Credential and schema-policy publication-first races remain separate work. The
transport identity is a test binding; external identity providers, network failures,
performance and horizontal scaling are outside this evidence.
