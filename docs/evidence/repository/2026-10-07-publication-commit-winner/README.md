# Publication wins after explicit finalization

Base: `53241493b16369b665f98dce9090f1579cb3b891`. Sol reviewed the fixture.
No production code or schema changed.

```
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

Exit 0, BUILD SUCCESSFUL in 13m03s. One aggregate JUnit case, zero failures,
errors or skips; 778.536 seconds, timestamp 2026-10-07T21:52:25.963Z.
Source hashes were verified after completion.

A dedicated database and 90-second production-JAR host exercise real PostgreSQL,
provider upload and byte readback. A JDBC proxy pauses only the transaction with
the exact operation-success row, owner and current transaction ID. Publication
has already completed explicit constraint finalization and its historical stage
check at this point. Both leases are checked live when the gate opens.

After database-clock expiry, the test submits V97 and observes its INSERT waiting
on the exact publisher PID through pg_blocking_pids. Publication then commits.
The contender reports the terminal-operation SQL error, creates no reservation,
and cannot replace the result. Existing checks verify the committed receipt,
provider bytes, one revision set and resource return after terminal retirement.

The driver requires HISTORICAL_POST_FINALIZATION_PUBLICATION_WINS_OK and
HISTORICAL_PUBLICATION_COMMIT_WINNER_HOST_OK. Child logs are periodic snapshots;
terminal XML, exit status and required-marker assertions establish completion.

This qualifies post-finalization SQL arbitration through the reservation adapter.
It does not cover a selected local successor entry, expiry before finalization,
takeover before claim acquisition, managed public recovery, performance or the
complete repository goal. LocalStack supplies S3 correctness coverage here.
