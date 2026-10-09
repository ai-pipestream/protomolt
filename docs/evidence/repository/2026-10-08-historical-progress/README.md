# Historical attempt progress

Base: 96fc69f0e. Gradle passed in 1 minute 4 seconds, exit 0. Reports contain
15 PostgreSQL execution tests and one PostgreSQL/LocalStack packaged aggregate;
zero failures, errors or skips. Source hashes and XML reports are adjacent.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentHistoricalExecutionIT' :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

SQL tests distinguish untouched START, rollback, committed START with a lost
acknowledgement, observed coordinates and acknowledged insertion. A retry after
rollback can acknowledge a new insert. A retry after a committed lost reply loads
the original coordinates without granting CREATE authority. Existing concurrency,
retention and expiry cases also run. The SQL fixtures use synthetic provider
observations and do not establish provider behavior.

The packaged test uses real PostgreSQL and LocalStack. It verifies retained source
and assessment phases across borrows, CREATE acknowledgement versus a lost reply,
positive CREATE reconciliation, and the publication-attempt flag. It also executes
the initial-owner upload fault scenarios already required by the aggregate.

Sol reviewed the changes with no blockers. These are local routing snapshots, not
authorization or terminal receipt evidence. START attempted can remain true after
rollback or a precondition failure. Preparation/install progress and a complete
historical rejection flow remain required for managed dispatch; public routing
is still disabled. This is focused evidence, not a full storage-suite result.
