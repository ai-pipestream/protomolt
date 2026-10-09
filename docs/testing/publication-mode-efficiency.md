# Publication-mode validation efficiency

`DocumentPublicationModesJournal.requireObservedModes` compares the modes a
scoped publication observed with the modes fixed in the private journal. It runs
once per journaled publication, after the pinned reads are captured and before the
candidate preparation or the assessed promotion. This note records the database
work of that path, the safety purpose of each statement, the single-transaction
form it now has, its lock order and revocation windows, the tests that pin it, and
the measurement. Evidence is under
[`docs/evidence/repository/publication-mode-efficiency/`](../evidence/repository/publication-mode-efficiency/README.md).

## Statement map

One call with an execution claim and a saved preparation. The claim-only case
returns after the sizes read.

| # | Statement | Site | Safety purpose |
|---|---|---|---|
| 1 | `SELECT lease_until FROM fence_repository_execution_claim(...)` | `RepositoryOperationLedger.fenceLiveOwner` via `RepositoryExecutionClaimLedger.lockLive` | locks the claim row `FOR UPDATE`, checks digest, epoch, token and lease after the lock, stamps the fence; linearizes against takeover and recovery |
| 2 | `UPDATE repository_operation_owners SET write_fence_xid=... RETURNING` | `fenceLiveOwner` | proves the owner generation and token are live after the lock and stamps this transaction for the owner guards |
| 3 | `SELECT ... FROM repository_operations` | `RepositoryOperationLedger.requireCommand` | the stored canonical command and digest equal the caller's command |
| 4 | `SELECT octet_length(p.preparation_bytes), octet_length(m.modes::text) ... LEFT JOIN repository_publication_modes` | `DocumentPublicationModeValidation.capture` | sizes of both immutable rows before any byte is fetched; reserves exactly those bytes; claim-only when no preparation exists |
| 5 | `SELECT p.preparation_bytes, p.preparation_sha256, p.owner_nonce, p.command_sha256, m.owner_nonce, CASE WHEN octet_length(m.modes::text)<=:reserved THEN m.modes::text END ... LEFT JOIN` | `capture` | both rows in one read; the modes text is fetched only within the reservation |
| | decode preparation: size, digest, codec, command digest, predecessor, owner nonce | `DocumentPublicationPreparationJournal.decode` | canonical preparation validation, refused as `DATA_LOSS` before any mode check |
| | absent modes | `requireObservedModes` | `FAILED_PRECONDITION` "Fixed publication modes are absent" |
| | decode modes: owner nonce, string values, keys equal the command members | `decodeModes` | malformed journal refused as `DATA_LOSS` |
| | compare with the observed map | `requireObservedModes` | `FAILED_PRECONDITION` "Observed publication modes differ from fixed modes" |
| | commit | `Tx.inTransaction` | one commit; a refusal rolls the fence stamps back |

Before this change the same call used two transactions and ten statements: the
first transaction held statements 1 to 3, a second `fence_repository_execution_claim`
from the preparation journal's own capture, a preparation size read, a
preparation row read and a separate modes read; the decode and comparison ran
outside any transaction; a second transaction then repeated statements 1 to 3 to
re-establish live authority at delivery. Each transaction is a pool acquisition
and a synchronous WAL commit, because the claim fence is an `UPDATE`.

## Why the split is not essential

The second transaction existed because the decode ran outside the fence: between
the first commit and the delivered result a takeover or expiry could have
happened, and the trailing fence refused delivery in that window. The decode is
pure CPU over bytes that are already reserved and bounded (preparation at most
16 MiB by the codec and the column check, modes at most 1 MiB by the column
check and 10,000 entries by the trigger); it touches no provider and no other
connection. Running it inside the fenced transaction closes the window instead of
re-checking it: no authority change can interleave between the read and the
delivered result, because a takeover needs the claim row this transaction holds
`FOR UPDATE`. The comparison result still grants nothing; the candidate
preparation and the terminal commit fence the owner again, and the terminal
commit re-verifies the binding with `requireBoundModes` under the owner lock.

Lock order is unchanged: claim row (fence function, `FOR UPDATE`), then the owner
row (`UPDATE ... RETURNING`), then plain reads of the immutable preparation and
modes rows. No lock is taken on those rows. Lease expiry after the fence inside
the transaction is not refused by this call; it is refused by the next fencing
transaction, exactly as expiry after the former trailing fence was. The window is
the two reads plus the decode. A takeover cannot occur inside the window; an
expiry can, and the delivered result is then stale for at most that window.

Reservations are exact: the preparation's `octet_length` and the modes text's
`octet_length` (UTF-8 bytes of the JSONB text rendering), taken from the sizes
read before the bytes read, released when the call ends. The modes text is
bounded in SQL by the reserved size, so an unreserved byte is never fetched; a
modes row whose text exceeds the column bound is reported as invalid after the
preparation is validated, which keeps the error order.

## Tests

`PublicationModeEfficiencyIT` (real PostgreSQL through Testcontainers,
`PublicationModeEfficiencyJdbc` counts pool acquisitions, commits, rollbacks and
every execute with its SQL and can block or fail a named statement or the commit):

| Case | Asserts |
|---|---|
| `validComparisonUsesOneFencedTransactionAndFiveStatements` | 1 acquisition, 1 commit, 0 rollbacks, statements 1 to 5 in that order, reservation equal to both row sizes while the bytes are held, zero afterwards |
| `mismatchRollsBackTheFenceWithoutACommit` | exact message and code, 0 commits, 1 rollback, 5 statements, the next fence still succeeds |
| `claimOnlyComparisonReadsSizesOnlyAndCommitsOnce` | 4 statements, 1 commit, a one-byte budget suffices |
| `absentAndMalformedStoredRowsKeepClassificationAndOrderAndRollBack` | missing, wrong members, non-string value, wrong nonce, damaged preparation, damaged preparation plus missing modes: code, message and order, 0 commits, 1 rollback, budget zero |
| `insufficientBudgetForEitherRowReleasesTheOtherReservation` | capacities below the preparation, equal to it and one byte short of both rows: `CapacityExceededException`, 4 statements, zero reserved afterwards |
| `scopeAndAuthorizationRefusalsTouchNoConnection` | foreign account, foreign principal, foreign operation id, pre-cancelled control: refused before any acquisition |
| `takeoverWaitsOnTheValidationFenceAndRevokesTheNextTransaction` | barrier after statement 5; the claim expires while held; a takeover is shown waiting on the claim row lock in `pg_stat_activity`; the comparison commits; the takeover then wins; the owner's next fence and next comparison are `Fenced` |
| `takeoverBeforeTheFenceRefusesWithoutReadingOrReserving` | a takeover before the call: `Fenced` after statement 1 only, no reservation, 1 rollback |
| `ownerExpiryAfterTheFenceIsCaughtByTheNextFence` | barrier after statement 5; owner lease expires while held; the comparison commits; the next fence and next comparison are `OwnerFencedException` |
| `cancellationIsObservedInsideTheFenceAndAfterCommit` | cancelled at the barrier: `CANCELLED`, 0 commits, 1 rollback; cancelled during commit: `CANCELLED` after 1 commit |
| `sqlFailurePropagatesAndReleasesEveryReservation` | injected `SQLException` on statement 5 and on commit: propagates unchanged, both reservations were live at the fault and are zero afterwards, a retry succeeds |
| `boundModesComparisonIsOneStatementUnderTheOwnerLock` | `requireBoundModes` is one statement; mismatch `FAILED_PRECONDITION`, other generation and malformed observed binding `DATA_LOSS` |
| `decodeWindowInsideTheFenceIsMeasured` | the window between statement 5 and commit is recorded for the fixture, and the parse of a 1 MiB modes object is timed |

`DocumentPublicationModesJournalIT` keeps its cases; three were renamed because
their pinned contract changed: `scopedModeComparisonUsesOneCommitAndExactRowBudget`
(one commit, mismatch rolls back), `insufficientModesBudgetReleasesPreparationReservation`
(the modes reservation now follows the preparation's) and
`deliveredComparisonGrantsNoAuthorityAfterChanges` (an authority change after the
single commit is refused by the next fence, not by the delivered comparison).

## Measurement

The evidence README holds the source identities, the traced per-operation counts
and the three-repetition comparison. In short: per publication the traced
window shows 16 → 15 scoped commits, 16 → 15 pool acquisitions and 177 → 172
execute calls; per rejection 35 → 33 and 300 → 290. Throughput and latency
moved in the same direction in every configuration but inside the
run-to-run spread, so the verified claim is the round-trip reduction, not a
latency improvement. The decode window inside the fence is a few milliseconds
at the fixture size and about 10 ms for a modes object at the 1 MiB column
bound; the 16 MiB preparation bound was not timed under the lock.

Run the focused suite with:

```
flock -w 600 /tmp/protomolt-repository-qualification.lock ./gradlew \
 :protomolt-repo-container:test --tests '*PublicationModeEfficiencyIT' \
 --tests '*DocumentPublicationModesJournalIT' --max-workers=2 --console=plain
```

## Remaining bottlenecks

The publication still commits 15 times. The paired pre/post reads outside this
path (`DocumentOperationUploadAdmission.captureReads`, `DocumentPublicationReplay.observe`,
`DocumentSelectedAttemptLedger.renewOwnerAndSelections`, two commits each) and the
single-purpose `renew`, `recheckInitialSelections`, `preflight` and
`DocumentSchemaPolicies.read` transactions are the next candidates; they belong
to other classes and are not changed here. RustFS PUT latency on the critical
path and per-process pool admission are unchanged.
