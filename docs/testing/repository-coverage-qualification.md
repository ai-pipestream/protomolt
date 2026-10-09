# Repository coverage qualification

Status: tests and evidence committed on `agent/repository-coverage-qualification`
from base `4f4d66bffe0025783df178b82a35212424faabf6`; local runs only. Archived
results are under
[docs/evidence/repository/external-coverage-qualification](../evidence/repository/external-coverage-qualification/README.md).

This note maps each acceptance criterion of the reconciliation and retained-root
release assignment to its runnable case, records the exact failure codes and side
effects the cases assert, and inventories the race cases that remain untested.
Pruning is not enabled and nothing here is pruning authority. Design context:
[revision pruning](../design/repository-revision-pruning.md), source anchors
`DocumentPreparationCoverageBatch`, `DocumentPreparationCoverageReconciliation`,
`DocumentPreparationCoverageLineage`, `DocumentPreparationRootReleases`,
`DocumentPreparationTerminalEvidence`, migrations V118 and V119.

## Suites

All classes live in `repo/container/src/test/java/ai/protomolt/proto/repo/container/ledger/`.

| Class | Cases | Purpose |
|---|---|---|
| `RepositoryCoverageQualificationSupport` | helper | commit gate, `pg_blocking_pids` wait, SQL readers, legacy root-owner writer, supported successor-install step |
| `RepositoryCoverageQualificationReleaseRaceIT` | 8 | reconciliation versus root release, both commit orders, commit and rollback, two terminal kinds |
| `RepositoryCoverageQualificationReleasedLineageIT` | 2 | lineage anchored to a genuinely released root, legacy and current anchor |
| `RepositoryCoverageQualificationAncestryLimitIT` | 1 | 64 accepted, 65 refused, adversarial SQL guard checks |
| `RepositoryCoverageQualificationBatchIT` | 2 | partial batch commit and retry, whole-account observation and keyset order |

Run them with:

```
flock -w 600 /tmp/protomolt-repository-qualification.lock ./gradlew \
 :protomolt-repo-container:test --tests '*RepositoryCoverageQualification*' \
 --max-workers=2 --console=plain
```

## Criterion mapping

### 1. Reconciliation versus root release in both commit orders

Case: `ReleaseRaceIT.reconciliationAndReleaseConvergeInEitherCommitOrder`,
parameters `first` in {RECONCILE, RELEASE}, `abortFirst` in {false, true},
`terminal` in {ABANDONMENT, REJECTION}.

Fixture: `historicalInitial` under schema target 117, a real terminal
(`DocumentPublicationAbandonment.abandon`, or owner admission plus
`DocumentPublicationRejections.cancel`), the real capture drain
(`DocumentPreparationCaptureDrain.recover` returning `QUIESCED`), then Flyway to
current. Before the race the test asserts one unresolved row, no certificate, one
live root, one drained capture and one pin batch.

Mechanism: the first operation runs on an entity-manager factory whose JDBC commit
is intercepted by `DocumentJdbcFaults.beforeCommit`. The gate fires only for the
connection that sees its own uncommitted certificate (`xmin=pg_current_xact_id()::xid`)
or its own uncommitted release receipt (`creation_xid=pg_current_xact_id()`). While
held, other sessions still see the pre-race state. The second operation is then
shown waiting with `wait_event_type='Lock'` on the
`repository_execution_claims ... FOR UPDATE` statement and the gate's backend in
`pg_blocking_pids`. The gate then commits, or replaces the commit with
`SQLSTATE 40001` so the first transaction rolls back.

| Order | First outcome | Second outcome | Final proof |
|---|---|---|---|
| reconcile first, commit | `VERIFIED_LIVE` | release receipt | `LIVE_ROOTS` |
| reconcile first, rollback | injected failure, nothing written | release receipt; fresh reconcile `VERIFIED_RELEASED` | `RELEASE_RECEIPT` |
| release first, commit | release receipt | `VERIFIED_RELEASED` | `RELEASE_RECEIPT` |
| release first, rollback | injected failure, roots still live | `VERIFIED_LIVE`; fresh release succeeds | `LIVE_ROOTS` |

Converged checks in every case: `ALREADY_CERTIFIED` on retry; the retried release
equals the receipt; roots 0, releases 1, certificates 1, lineage 0, unresolved 0,
capture drains 1, pin batches 1, headers 1; certificate preparation, command, root
count and roots digest equal the record's canonical values and the receipt; the
release row's stored capture fingerprint equals its SQL recomputation and
`receipt.capturesSha256()`; the terminal kind column and `receipt.terminal()` match
the fixture's abandonment token and owner nonce or the generation-1 rejection;
claim and owner leases unchanged; both payload budgets at zero reserved bytes. The
executor, factory and gate close in `finally`, including on assertion failure.

### 2. Successor lineage anchored to a genuinely released historical preparation

Case: `ReleasedLineageIT.successorLineageAnchorsToGenuinelyReleasedHistoricalRoot`,
`legacyAnchor` in {true, false}.

Flow: `installedHistoricalSuccessor` with a one-minute lease, real activation of
that successor with its own capture (`RepositoryHistoricalSuccessorActivation`),
cancellation through the live owner at generation 2, completion of the activation
capture, reader drain of the initial capture, `recover` returning `QUIESCED`, then
`DocumentPreparationRootReleases.release` with `captureCount()` 2 and a `REJECTION`
terminal at generation 2. The legacy variant then migrates from 117 and shows the
successor is `PENDING_SUCCESSOR` until the root is `VERIFIED_RELEASED`; the current
variant starts from the writer's `LIVE_ROOTS` certificate.

Checks: lineage anchor generation 0, anchor digest equal to the certificate's
preparation digest and the receipt, depth 1, root count 1, roots digest equal to
the certificate and the release row, previous digest equal to the original record,
successor digest equal to the installed plan; retries return `ALREADY_CERTIFIED`
and `ALREADY_VERIFIED_SUCCESSOR`; the release retry returns the same receipt; pin
batches, capture drains, headers, releases, installs, rejections, activations and
executions are unchanged by reconciliation; no header for generation 1; leases
unchanged; budget at zero.

### 3. The 64-link ancestry limit at its boundary

Case: `AncestryLimitIT.sixtyFourLinksVerifyAndTheSixtyFifthIsRefusedWithoutClearingUnresolved`.

The fixture derives 65 installs through the supported protocol: lease expiry,
`RepositoryCoordinatorRecoveryDiscovery` reporting `INSTALLED_NOT_ACTIVATED`, an
immutable `SupersededUnactivated` reservation and `RepositorySuccessorInstall.install`,
with one-second leases. No lease or recovery limit is removed. The 64-link
activation limit in `RepositoryHistoricalRetentionLoader` does not bound
installation, so generation 65 is reachable legitimately and the handler guard is
tested directly rather than through adversarial SQL alone.

Checks: generations 1 through 64 return `VERIFIED_SUCCESSOR` with depth equal to
generation, anchor 0 and the exact previous digest; generation 65 fails twice with
`FAILED_PRECONDITION` and message `Preparation coverage ancestry exceeds 64 links`,
keeps its unresolved row and writes no lineage row; generation 64 retries as
`ALREADY_VERIFIED_SUCCESSOR`; a batch reaching generation 65 propagates the same
code and a page beyond it still observes `unresolved=true`. Two clearly labeled
adversarial inserts follow: a depth-65 row satisfying every trigger check fails on
the `depth` check constraint, and a depth-64 row at generation 65 is refused by the
trigger. Effects, leases and budgets are unchanged.

### 4. Partial-batch completion, retry and whole-account observation

Cases: `BatchIT.earlierProofCommitsLaterCommitFailurePropagatesAndRetryNeitherDuplicatesNorLoses`
and `BatchIT.wholeAccountObservationSeesOtherPrincipalsAndLowerKeyWorkCommittedAfterTheScan`.

Keys are fixed uuids `...0001`, `...0002`, `...0003`, `...0005`, so PostgreSQL uuid
order is the asserted keyset order. In the first case, three legacy root owners are
migrated; the certification commit of the second is replaced by `SQLSTATE 40001`.
The batch throws with the injected message; the first owner has one certificate and
no unresolved row, the other two keep unresolved rows and have no certificate. The
same cursor retried on a healthy connection returns entries `[0002, 0003]` as
`VERIFIED_LIVE`, no next cursor and `unresolved=false`; every owner has exactly one
certificate; a further retry returns no entries and changes nothing.

In the second case, two owners under `principal`, one under `other`, and a lower
key whose generation 0 certified atomically under the current writer. A page of one
returns `[0002]` with cursor `(0002,0)` and `unresolved=true`. The second page
returns `[0003]`; an `afterCommit` hook fires once after that certificate commits
and performs a real V93 install of `(0001,1)` before the final observation, which
reports `unresolved=true`. The remaining rows are exactly `other/0005/0` and
`principal/0001/1`. The `other` principal's scan certifies its root and still
observes the lower key; a fresh scan for `principal` returns `(0001,1)` as
`VERIFIED_SUCCESSOR` and finally `unresolved=false`, with four certificates and one
lineage row.

### 5. Inventory of remaining race cases

Listed below with the source and the closest existing test pattern. These are
supplementary to the runnable cases above, not substitutes for them.

## Failure codes asserted

| Code or text | Where |
|---|---|
| injected `SQLSTATE 40001`, message `gated commit deliberately aborted` | race rollback orders, batch commit failure |
| `FAILED_PRECONDITION` `Preparation coverage ancestry exceeds 64 links` | handler and batch at generation 65 |
| `violates check constraint ... depth` | adversarial depth-65 SQL insert |
| trigger refusal (`RuntimeException`) | adversarial understated-depth SQL insert |
| `PENDING_SUCCESSOR` | successor before its released anchor is verified |

No test raised a timeout, weakened an assertion or skipped. `RACE_TIMEOUTS` (15 s
lock wait, 30 s statement) applies only to the operation queued behind the held
commit and is a safety net; the ordinary suites keep `SqlTimeouts(2 s, 10 s)`.

## Remaining race cases

| Race | Why it matters | Source | Closest existing pattern |
|---|---|---|---|
| Two reconcilers of the same key, and a lost acknowledgement after the certificate commit | correctness of `ALREADY_CERTIFIED` under real contention and after a dropped reply | `DocumentPreparationCoverageReconciliation` lines 262-311 | coordinator's uncommitted `DocumentPreparationCoverageRecoveryIT`; deliberately not reproduced here |
| Reconciliation of root g concurrent with lineage certification of g+1 | both lock the claim first, then preparations; the serialization order is asserted only sequentially | `DocumentPreparationCoverageLineage.certify` lock order comment | `ReleasedLineageIT` (sequential) |
| Root release concurrent with lineage certification of a descendant | release deletes roots while lineage inherits the anchor's root fingerprint from the certificate, not the live rows | `DocumentPreparationRootReleases.release`, `DocumentPreparationCoverageLineage` | `ReleaseRaceIT` gate pattern, applied to a chain |
| Successor install concurrent with reconciliation of the same operation | install updates `repository_operation_owners` and inserts a preparation while the root reconciler holds claim and preparation locks | `RepositorySuccessorInstall.install`, V93 `require_repository_successor_complete` | `BatchIT` lower-key hook, moved inside a held reconcile |
| Capture append concurrent with live-root certification | both take the claim lock; the certificate does not record captures, so the later release must still count the appended drain | `DocumentPreparationSourcePins.insert`, `DocumentPreparationCaptureCoverage.lockAndRequireDrained` | `DocumentCaptureAdmissionClosureIT.captureWaitingOnAbandonmentClaimLockSeesCommittedClosure` |
| Abandonment or cancellation committing while reconciliation waits on the claim | reconcile-live does not inspect the terminal; a terminal committed first must not change the `LIVE_ROOTS` outcome, and a release in between must flip it to `RELEASE_RECEIPT` | `DocumentPublicationAbandonment.abandon`, `DocumentPreparationTerminalEvidence.lockAndRequire` | `ReleaseRaceIT` with the gate on the abandonment commit |
| Batch entry certified by another process between scan and entry | entry should report `ALREADY_CERTIFIED` rather than fail; only sequential coverage exists | `DocumentPreparationCoverageBatch.reconcile` | `BatchIT` with a second batch on another factory |
| Same-operation higher generation installed inside the current page after the scan | the page ends before the new key; the observation must still report it | `DocumentPreparationCoverageBatch` final `EXISTS` | `BatchIT` whole-account case (lower key only) |
| Held commit outlasting `lock_timeout` on the waiting reconciler | the bounded wait must surface a timeout, leave the unresolved row and release the budget | `SqlTimeouts.apply`, `Tx.inTransaction` | `ReleaseRaceIT` with the barrier never opened before the bound |
| Cancellation of the read control between batch entries | `control.check()` between entries must stop without touching later keys and keep earlier commits | `DocumentPreparationCoverageBatch` loop | `DocumentPreparationCoverageReconciliationIT.callerMismatchAndCancellationCannotClearUnresolved` |
| Connection pool exhaustion or connection loss mid-batch | the failing entry must propagate; later entries must remain unresolved | `Tx`, Hikari pool of three in `DocumentNativePublicationFixture.context` | none |
| New source-root acquisition against a future prune decision (future-writer race) | the pruning design requires it before deletion is enabled; it has no reconciliation-side counterpart yet | `docs/design/repository-revision-pruning.md`, V116, V117 | `2026-10-08-history-acquisition-fence`, `2026-10-08-assessment-source-fence` |
| Process death between certificate commit and caller observation | SQL state is covered by the coordinator's lost-acknowledgement case; the operator retry contract is not exercised end to end | `DocumentPreparationCoverageBatch` javadoc | `DocumentPublicationProcessRecoveryIT` worker pattern |

## Integration notes

- Owned files: the five test classes above, this note and the evidence directory.
  No shared production, SQL, fixture or Gradle file changed, and no shared-file
  request is open.
- No production defect was found by these cases. Every refusal observed was the
  documented behavior of the handlers and SQL guards.
- The base branch on Forgejo moved to `97e6b9ab` after this work started. The base
  commit is an ancestor of that head and this branch adds only new paths that the
  head does not touch, so no merge conflict exists.
- Hosted CI, main merge and deployment remain out of scope for this checkpoint.
