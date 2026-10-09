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

Four classes under `repo/container/src/test/java/.../ledger/`, prefixed
`RepositoryCoverageQualification`, plus one helper of the same prefix:

- `ReleaseRaceIT` (9 cases): a populated V117 historical preparation is migrated to
  the current schema with a real terminal receipt (abandonment or canonical
  cancellation) and a real capture drain. One operation's actual SQL commit is held;
  the other is shown blocked on the execution-claim row lock through
  `pg_blocking_pids`; the barrier then commits or aborts. Reconcile-first keeps its
  `LIVE_ROOTS` certificate after release; release-first yields `RELEASE_RECEIPT` from
  the exact receipt. Both rollback-first orders converge on retry. A ninth case never
  opens the barrier in time: the waiting reconciler fails on PostgreSQL's lock
  timeout, state and budget are intact, every latch and worker closes, the gated
  backend ends with no open transaction, and both operations then complete.
- `ReleasedLineageIT` (2 cases): a successor is installed, activated with its own
  capture, cancelled through the live owner, drained and released by the qualified
  root release. Lineage then anchors to that released root, under a V117 anchor
  migrated forward (`RELEASE_RECEIPT`) and under the current writer (`LIVE_ROOTS`).
- `AncestryLimitIT` (2 cases): 65 successors installed through expiry, discovery,
  supersession and the V93 install with one-second leases. Depths 1 through 64
  verify; generation 65 is refused with `FAILED_PRECONDITION` and stays unresolved.
  A separate, clearly labeled adversarial SQL case rebuilds the same fixture and shows
  the table constraint and the trigger refuse a depth-65 or understated row.
- `BatchIT` (2 cases): the first entry's proof commits, the second entry's
  certification commit is replaced by a serialization failure, the third never runs;
  the same cursor retried resumes at the first unresolved key without a duplicate
  proof. The account-level observation still reports another principal's row and a
  lower key installed between the scan and the observation; keyset order is asserted
  on fixed operation ids.

## Real, synthetic and injected

- Real: every SQL effect, lock wait, lock timeout, trigger, constraint, terminal
  receipt, capture drain, root release, successor install, activation and lineage proof.
- Synthetic: the source publications come from the existing fixtures, so provider
  observations are fixture supplied and nothing here measures object storage.
- Injected: a held JDBC commit, an aborted JDBC commit (`SQLSTATE 40001`), a barrier
  deliberately kept closed past a one-second lock wait, and the two adversarial SQL
  inserts named above. No trigger or constraint was disabled.

## Runs

Environment: Linux 7.0.0-34, 32 CPUs, 121 GB RAM, load average about 8 from
unrelated work during every run, Docker 29.8.1, OpenJDK 25.0.3, Gradle 9.6.1,
Testcontainers 2.0.5, Hibernate 7.4.10.Final, PostgreSQL JDBC 42.7.13.
Container: `postgres:18-alpine`, digest
`sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873`.

Runs C and D are the current result. They ran on source `97653f5e` plus the
uncommitted changes that the follow-up commit records (the separate adversarial case
and the bounded-wait case). Runs A and B are the earlier complete runs on source
`4f4d66bf` plus the first commit's files and are kept as archived history.

Run C, `run-c-qualification/`, 2026-10-09T12:16:00Z to 12:18:36Z, exit 0:

```
flock -w 600 /tmp/protomolt-repository-qualification.lock ./gradlew \
 :protomolt-repo-container:test --tests '*RepositoryCoverageQualification*' \
 --max-workers=2 --console=plain
```

| Suite | Tests | Failures | Errors | Skipped | Seconds |
|---|---|---|---|---|---|
| RepositoryCoverageQualificationReleaseRaceIT | 9 | 0 | 0 | 0 | 12.1 |
| RepositoryCoverageQualificationReleasedLineageIT | 2 | 0 | 0 | 0 | 5.7 |
| RepositoryCoverageQualificationAncestryLimitIT | 2 | 0 | 0 | 0 | 146.5 |
| RepositoryCoverageQualificationBatchIT | 2 | 0 | 0 | 0 | 9.3 |

Run D, `run-d-existing-suites/`, 2026-10-09T12:18:36Z to 12:20:00Z, exit 0:

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
| DocumentPreparationCoverageCertificatesIT | 5 | 0 | 0 | 0 | 12.1 |
| DocumentPreparationCoverageLineageIT | 8 | 0 | 0 | 0 | 25.3 |
| DocumentPreparationCoverageReconciliationIT | 8 | 0 | 0 | 0 | 9.5 |
| DocumentPreparationRootReleaseIT | 8 | 0 | 0 | 0 | 8.9 |
| DocumentPreparationRootReleaseConcurrencyIT | 2 | 0 | 0 | 0 | 3.0 |
| RepositoryHistoricalSuccessorActivationIT | 12 | 0 | 0 | 0 | 26.9 |
| RepositorySuccessorInstallIT | 11 | 0 | 0 | 0 | 25.3 |

Run A (`run-a-qualification/`, 12:05:54Z to 12:07:20Z, 13 tests) and run B
(`run-b-existing-suites/`, 12:07:32Z to 12:08:54Z, 54 tests) both exited 0 with zero
failures, errors or skips. Each directory holds the compressed JUnit XML copied before
any later run and a `run.meta` with start, end, exit code, SHA and host load.

Before run A, one run of the new suite failed a single case on a fixture collision:
the publication fixture's drive name is unique per account, so a second `input(c)` in
one context cannot be used. The test was corrected to reuse one template; every
archived run is a fresh, complete run. No timeout was raised and no assertion weakened.

## Exit criteria

| Criterion | Evidence |
|---|---|
| Every required case has real passing assertions | runs C and D: 69 tests, 0 failures, 0 errors |
| Zero unexpected skips | `skipped="0"` in every archived XML |
| Bounded waits | lock waits are `SqlTimeouts(2 s, 10 s)` ordinarily, 15 s only for the deliberately queued operation, 1 s in the bounded-wait failure case; every latch await is 10 or 15 s; the failure case asserts the timeout arrives within 8 s |
| No resource leaks | every case asserts zero reserved budget bytes; race cases assert worker termination and no open transaction on the gated backend; rigs, factories and executors close in try-with-resources |
| Exact failure codes and side effects | listed per case in the testing note |
| Shared production defects | none found; every refusal was documented handler or SQL-guard behavior, so no red test or proposed diff is owed |
| Competing reconcilers and lost acknowledgement | assigned to the coordinator's uncommitted `DocumentPreparationCoverageRecoveryIT` by the handoff and deliberately not recreated |
| No benchmark or scaling claim | run times are dominated by one-second lease expiries and are not measurements |

## Limits

- The 15-second `RACE_TIMEOUTS` lock wait applies only to the operation that is
  deliberately queued behind a held commit; the blocked state is observed within
  milliseconds and the barrier then opens, so the bound is a safety net.
- Local runs only. This is not a hosted-CI result, a main merge or a deployment.
