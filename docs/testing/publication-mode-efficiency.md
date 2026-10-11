# Publication-mode validation efficiency

`DocumentPublicationModesJournal.requireObservedModes` compares the modes a
scoped publication observed with the modes fixed in the private journal. It runs
once per journaled publication, after the pinned reads are captured and before the
candidate preparation or the assessed promotion. This note records the database
work of that path, the safety purpose of each statement, the single-transaction
form it now has, its lock order and revocation windows, the tests that pin it, and
the measurements, including the lock window at the 16 MiB preparation bound. Evidence is under
[`docs/evidence/repository/publication-mode-efficiency/`](../evidence/repository/publication-mode-efficiency/README.md).

## Statement map

One call with an execution claim and a saved preparation. The claim-only case
returns after the sizes read; a refusal by the decode or the comparison returns
after statement 5 and rolls back.

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
| 6 | `SELECT lease_until FROM fence_repository_execution_claim(...)` | `fenceLiveOwner`, after the comparison | the claim is still live by `clock_timestamp()` after the decode; a claim lease that expired while decoding is refused (`Fenced`) |
| 7 | `UPDATE repository_operation_owners ... AND lease_until > clock_timestamp() RETURNING` | `fenceLiveOwner`, after the comparison | the owner lease is still live after the decode; expiry while decoding is refused (`OwnerFencedException`) |
| 8 | `SELECT ... FROM repository_operations` | `requireCommand`, after the comparison | the command binding the former trailing transaction re-checked |
| | commit | `Tx.inTransaction` | one commit; a refusal rolls the fence stamps back |

Before this change the same call used two transactions and ten statements: the
first transaction held statements 1 to 3, a second `fence_repository_execution_claim`
from the preparation journal's own capture, a preparation size read, a
preparation row read and a separate modes read; the decode and comparison ran
outside any transaction; a second transaction then repeated statements 1 to 3 to
re-establish live authority at delivery. Each transaction is a pool acquisition
and a synchronous WAL commit, because the claim fence is an `UPDATE`.

## Why the split is not essential, and what replaced the trailing transaction

The second transaction existed because the decode ran outside the fence: between
the first commit and the delivered result a takeover or expiry could have
happened, and the trailing fence refused delivery in that window. The decode is
pure CPU over bytes that are already reserved and bounded (preparation at most
16 MiB by the codec and the column check, modes at most 1 MiB by the column
check and 10,000 entries by the trigger); it touches no provider and no other
connection, so it can run inside the transaction. Inside the transaction a
takeover cannot interleave, because it needs the claim row this transaction
holds `FOR UPDATE`. Lease expiry can: the claim and owner checks compare
`lease_until` with `clock_timestamp()`, so a lease that passes while the decode
runs is not seen by statements 1 and 2. Statements 6 to 8 repeat the claim, owner
and command checks after the comparison, in the same transaction, so the
delivered result was live at a point after the decode, which is the contract the
former trailing transaction gave. The comparison result still grants nothing;
the candidate preparation and the terminal commit fence the owner again, and the
terminal commit re-verifies the binding with `requireBoundModes` under the owner
lock.

Current-authority boundary of a successful call: the claim and owner were live,
and the command bound, at statement 8, which is the same point the former
trailing transaction established before its commit. An expiry or takeover after
the commit is refused by the caller's next fencing transaction, as before. The
next steps at the three call sites are unchanged: `DocumentAssessmentCreation`
proceeds to slot and evidence preparation; both `DocumentPublicationExecution`
sites proceed to `DocumentSchemaBatch.stage` (its own owner-fenced staging
transaction), upload staging through `DocumentUploadCoordinator` (which opens with
the fenced admission and renewal transactions) and the terminal commit (which
fences the owner and re-verifies the binding).

Lock order is unchanged: claim row (fence function, `FOR UPDATE`), then the owner
row (`UPDATE ... RETURNING`), then plain reads of the immutable preparation and
modes rows, then the same claim and owner rows again, which this transaction
already holds. No lock is taken on the preparation or modes rows. The locks are
held from statement 1 to the commit; the measured length of that window at the
16 MiB bound is in the Measurement section. Only the same operation's claim and
owner rows are held: a takeover or renewal of this operation waits, other
operations do not.

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
| `validComparisonUsesOneTransactionFencedBeforeAndAfterTheDecode` | 1 acquisition, 1 commit, 0 rollbacks, statements 1 to 8 in that order, reservation equal to both row sizes while the bytes are held, zero afterwards |
| `mismatchRollsBackTheFenceWithoutACommit` | exact message and code, 0 commits, 1 rollback, 5 statements, the next fence still succeeds |
| `claimOnlyComparisonReadsSizesOnlyAndCommitsOnce` | 4 statements, 1 commit, a one-byte budget suffices |
| `absentAndMalformedStoredRowsKeepClassificationAndOrderAndRollBack` | missing, wrong members, non-string value, wrong nonce, damaged preparation, damaged preparation plus missing modes: code, message and order, 0 commits, 1 rollback, budget zero |
| `insufficientBudgetForEitherRowReleasesTheOtherReservation` | capacities below the preparation, equal to it and one byte short of both rows: `CapacityExceededException`, 4 statements, zero reserved afterwards |
| `scopeAndAuthorizationRefusalsTouchNoConnection` | foreign account, foreign principal, foreign operation id, pre-cancelled control: refused before any acquisition |
| `takeoverWaitsOnTheValidationLocksAndTheSecondFenceRefusesTheExpiredClaim` | barrier after statement 5; the claim expires while held; a takeover is shown waiting on the claim row lock in `pg_stat_activity`; after release the comparison is refused `Fenced` at statement 6 with 0 commits and 1 rollback, the owner's `write_fence_xid` is unchanged, and the takeover then wins with epoch 2 |
| `takeoverAfterTheValidationCommitsIsRefusedByTheNextFence` | the comparison commits; the claim then expires and is taken over; the owner's next fence is `Fenced` |
| `takeoverBeforeTheFenceRefusesWithoutReadingOrReserving` | a takeover before the call: `Fenced` after statement 1 only, no reservation, 1 rollback |
| `leaseExpiryDuringTheDecodeWindowIsRefusedBeforeCommit` (owner, claim) | barrier after statement 5; the owner or claim lease expires while held; after release the comparison is refused (`OwnerFencedException` at statement 7, `Fenced` at statement 6), 0 commits, 1 rollback, budget zero, `write_fence_xid` unchanged; the next fence and next comparison are refused the same way |
| `cancellationIsObservedInsideTheFenceAndAfterCommit` | cancelled at the barrier: `CANCELLED`, 0 commits, 1 rollback; cancelled during commit: `CANCELLED` after 1 commit |
| `sqlFailurePropagatesAndReleasesEveryReservation` | injected `SQLException` on statement 5, on statement 6 and on commit: propagates unchanged, both reservations were live at the fault and are zero afterwards, a retry succeeds |
| `boundModesComparisonIsOneStatementUnderTheOwnerLock` | `requireBoundModes` is one statement; mismatch `FAILED_PRECONDITION`, other generation and malformed observed binding `DATA_LOSS` |
| `decodeWindowInsideTheFenceIsMeasured` | the window between statement 5 and statement 6 is recorded for the fixture, and the parse of a 1 MiB modes object is timed |
| `maximumPreparationLockWindowIsMeasured` | a canonical preparation of exactly 16,777,216 bytes (see Measurement): six calls, each 1 acquisition, 1 commit, 8 statements, reservation equal to both rows while held and zero after; prints the lock window, decode window and thread allocation per call |
| `heldMaximumValidationBlocksOnlyItsClaimAndTheTakeoverWaitIsBounded` | the same 16 MiB shape held after statement 5: a comparison on another claim in the same schema commits meanwhile; the held claim expires; a takeover waits on its row lock; after release the 16 MiB decode runs, statement 6 refuses `Fenced`, 0 commits, `write_fence_xid` unchanged, and the takeover wins; prints the other claim's call time and the takeover's completion after release |

`DocumentPublicationModesJournalIT` keeps its cases; three were renamed because
their pinned contract changed: `scopedModeComparisonUsesOneCommitAndExactRowBudget`
(one commit, mismatch rolls back), `insufficientModesBudgetReleasesPreparationReservation`
(the modes reservation now follows the preparation's) and
`deliveredComparisonGrantsNoAuthorityAfterChanges` (it holds the caller after the
single commit, so an authority change there is refused by the next fence; a change
before the commit is refused by statements 6 to 8, pinned in `PublicationModeEfficiencyIT`).

## Measurement

The current evidence for this source is
[`requalification/`](../evidence/repository/publication-mode-efficiency/requalification/README.md);
the evidence README one level up records the earlier source `1c9faac60`, before
the post-decode recheck. In short, against main with the change reverted: per
publication the traced window shows 16 → 15 scoped commits, 16 → 15 pool
acquisitions and 177 → 175 execute calls; per rejection 35 → 33 and 300 → 296.
Throughput and latency means moved in the same direction in every configuration
but inside the run-to-run spread, so the verified claim is the round-trip
reduction, not a latency improvement.

Lock window at the bound: with a canonical 16,777,216-byte preparation (seventeen
drives whose metadata fill the codec's 1 MiB per-text bound) the claim and owner
rows of that one operation are held about 60 to 115 ms per call, of which the
decode is 44 to 74 ms; the thread allocates about 230 MiB per call; the exact
reservation is 16,777,588 bytes. While such a call is held, a comparison on
another claim completes normally, and a takeover of the held claim completes
about 60 ms after release, once the decode ends and the recheck refuses the
expired claim. At the fixture size the decode window is 0.4 to 1.9 ms, and a
1 MiB modes object parses in 2 to 5 ms. The bound is held only by a preparation
with very large drive metadata; it delays only that operation's own takeover or
renewal and is short against the one-second minimum lease, so the decode stays
inside the transaction.

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
