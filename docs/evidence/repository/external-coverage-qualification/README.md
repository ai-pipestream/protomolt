# Reconciliation and retained-root release qualification

Base: `4f4d66bffe0025783df178b82a35212424faabf6` on `agent/historical-cleanup-fairness`.
Branch: `agent/repository-coverage-qualification`. This checkpoint adds tests and
evidence only. It changes no production Java, SQL, fixture or Gradle file, and it
neither creates nor enables a prune operation. A certificate or lineage proof still
grants no read, write, execution, capture, root release or provider deletion right.

The acceptance mapping, the commit-order expectations and the inventory of races
that remain untested are in
[docs/testing/repository-coverage-qualification.md](../../../testing/repository-coverage-qualification.md).

## What the suites prove

Four new classes under `repo/container/src/test/java/.../ledger/`, prefixed
`RepositoryCoverageQualification`, plus one helper of the same prefix:

- `ReleaseRaceIT` (8 cases): a populated V117 historical preparation is migrated to
  the current schema with a real terminal receipt (abandonment or canonical
  cancellation) and a real capture drain. One operation's actual SQL commit is held;
  the other is shown blocked on the execution-claim row lock through
  `pg_blocking_pids`; the barrier then commits or aborts. Reconcile-first keeps its
  `LIVE_ROOTS` certificate after release; release-first yields `RELEASE_RECEIPT` from
  the exact receipt. Both rollback-first orders converge on retry.
- `ReleasedLineageIT` (2 cases): a successor is installed, activated with its own
  capture, cancelled through the live owner, drained and released by the qualified
  root release. Lineage then anchors to that released root, under a V117 anchor
  migrated forward (`RELEASE_RECEIPT`) and under the current writer (`LIVE_ROOTS`).
- `AncestryLimitIT` (1 case): 65 successors installed through expiry, discovery,
  supersession and the V93 install with one-second leases. Depths 1 through 64
  verify; generation 65 is refused with `FAILED_PRECONDITION` and stays unresolved.
  Two clearly labeled adversarial SQL inserts show the table constraint and the
  trigger refuse a depth-65 or understated row.
- `BatchIT` (2 cases): the first entry's proof commits, the second entry's
  certification commit is replaced by a serialization failure, the third never runs;
  the same cursor retried resumes at the first unresolved key without a duplicate
  proof. The whole-account observation still reports another principal's row and a
  lower key installed between the scan and the observation; keyset order is asserted
  on fixed operation ids.

## Real, synthetic and injected

- Real: every SQL effect, lock wait, trigger, constraint, terminal receipt, capture
  drain, root release, successor install, activation and lineage proof.
- Synthetic: the source publications come from the existing fixtures, so provider
  observations are fixture supplied and nothing here measures object storage.
- Injected: a held JDBC commit, an aborted JDBC commit (`SQLSTATE 40001`), and the
  two adversarial SQL inserts named above. No trigger or constraint was disabled.

## Runs

Environment: Linux 7.0.0-34, 32 CPUs, 121 GB RAM, load average about 8 from
unrelated work during both runs, Docker 29.8.1, OpenJDK 25.0.3, Gradle 9.6.1,
Testcontainers 2.0.5, Hibernate 7.4.10.Final, PostgreSQL JDBC 42.7.13.
Container: `postgres:18-alpine`, digest
`sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873`.
Source SHA for both runs: `4f4d66bffe0025783df178b82a35212424faabf6` plus the
uncommitted test and documentation files that this checkpoint commits.

Run A, `run-a-qualification/`, 2026-10-09T12:05:54Z to 12:07:20Z, exit 0:

```
flock -w 600 /tmp/protomolt-repository-qualification.lock ./gradlew \
 :protomolt-repo-container:test --tests '*RepositoryCoverageQualification*' \
 --max-workers=2 --console=plain
```

| Suite | Tests | Failures | Errors | Skipped | Seconds |
|---|---|---|---|---|---|
| RepositoryCoverageQualificationReleaseRaceIT | 8 | 0 | 0 | 0 | 10.2 |
| RepositoryCoverageQualificationReleasedLineageIT | 2 | 0 | 0 | 0 | 5.5 |
| RepositoryCoverageQualificationAncestryLimitIT | 1 | 0 | 0 | 0 | 76.3 |
| RepositoryCoverageQualificationBatchIT | 2 | 0 | 0 | 0 | 9.3 |

Run B, `run-b-existing-suites/`, 2026-10-09T12:07:32Z to 12:08:54Z, exit 0:

```
flock -w 600 /tmp/protomolt-repository-qualification.lock ./gradlew \
 :protomolt-repo-container:test \
 --tests '*DocumentPreparationCoverageCertificatesIT' \
 --tests '*DocumentPreparationCoverageLineageIT' \
 --tests '*DocumentPreparationCoverageReconciliationIT' \
 --tests '*DocumentPreparationRootReleaseIT' \
 --tests '*DocumentPreparationRootReleaseConcurrencyIT' \
 --tests '*RepositoryHistoricalSuccessorActivationIT' \
 --tests '*RepositorySuccessorInstallIT' \
 --max-workers=2 --console=plain
```

| Suite | Tests | Failures | Errors | Skipped | Seconds |
|---|---|---|---|---|---|
| DocumentPreparationCoverageCertificatesIT | 5 | 0 | 0 | 0 | 11.2 |
| DocumentPreparationCoverageLineageIT | 8 | 0 | 0 | 0 | 24.2 |
| DocumentPreparationCoverageReconciliationIT | 8 | 0 | 0 | 0 | 8.8 |
| DocumentPreparationRootReleaseIT | 8 | 0 | 0 | 0 | 8.4 |
| DocumentPreparationRootReleaseConcurrencyIT | 2 | 0 | 0 | 0 | 3.2 |
| RepositoryHistoricalSuccessorActivationIT | 12 | 0 | 0 | 0 | 26.5 |
| RepositorySuccessorInstallIT | 11 | 0 | 0 | 0 | 26.2 |

Each directory holds the compressed JUnit XML copied before any later run and a
`run.meta` with the start, end, exit code, SHA and host load. An earlier run of the
new suite failed one case on a fixture collision (the publication fixture's drive
name is unique per account, so a second `input(c)` in one context cannot be used);
the test was corrected to reuse one template and the archived run is the fresh,
complete rerun. No timeout was raised and no assertion was weakened.

## Limits

- The `RACE_TIMEOUTS` lock wait of 15 seconds applies only to the operation that is
  deliberately queued behind a held commit; the blocked state is observed within
  milliseconds and the barrier then opens, so the bound is a safety net.
- Run time of the 65-link case is dominated by 64 one-second lease expiries and is
  not a throughput measurement. No benchmark or scaling claim follows from any run.
- The lost-acknowledgement and competing-reconciler cases belong to the
  coordinator's separate recovery suite and are not reproduced here.
- Local runs only. This is not a hosted-CI result, a main merge or a deployment.
