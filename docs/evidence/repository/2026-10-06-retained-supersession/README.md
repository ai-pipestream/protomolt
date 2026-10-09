# Preserve pending supersession across uncertain replies

Base: `7f8ca12e5c98d083b8e4aceedd12eb44c2a859ef`.

The private recovery attempt can explicitly supersede its own expired unactivated
successor. It binds fresh discovery to the exact retained successor, stores one
pending target/proposal before V98, and refuses ordinary advance until that exact
proposal is confirmed. Only confirmed V98 releases the old borrowed preparation
and plan; the command lease stays held. New installation then uses existing local
session retirement before fingerprint-checked activation.

The three positive cases use real PostgreSQL and expire actual leases. They cover
reservation-only, installed, and failed V94 commit with a retained local session.
Cancellation after the V98 commit retains the old proposal and byte budget. Closing
and reopening the call handle preserves the pending tuple; exact retry confirms the
same committed token and activates the replacement. Old session capacity is safely
reused, with no remaining owner byte reservation on this successful path.

A committed V94 case loses its reply and then expires. Discovery explicitly reports
EXPIRED_BOUND, and unactivated supersession is refused without writing V98. Foreign
winner cases cover both discovery mismatch and a previously attempted V98 whose
commit rolled back before another coordinator won. The pending retry refuses the
foreign binding instead of minting or adopting another identity. These refused
paths intentionally retain their budgets and report `Drain(0,1)` after closure.
They do not pretend that closure completed reconciliation.

The affected run passed 45 cases without skips: 11 recovery-owner, 2 target,
14 local recovery and 18 SQL supersession cases (`affected-green.tar.gz`). The
foreign-winner case was subsequently expanded to distinguish pending and nonpending.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryRecoveryAttemptsIT' --tests '*DocumentSuccessorTargetIT' --tests '*DocumentPublicationRecoveryIT' --tests '*RepositoryCoordinatorSupersessionIT' --console=plain
```

Sol reviewed the implementation and refusal paths with no blocker. The tests perform
no provider I/O and prove neither reader quiescence nor pin reclamation. Host wiring,
foreign/terminal reconciliation, graceful recovery, shutdown composition and bounded
automatic retry policy remain open. The three-second positive fixture leases may
need a longer interval on a heavily loaded CI host; production lease rules were not
relaxed and no database clock was mocked.

The expanded owner suite (12 cases) and packaged production-JAR storage-runtime
case passed without skips. `packaged-green.tar.gz` preserves those results:

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryRecoveryAttemptsIT' :protomolt-repo-container:admissionStorageTest --console=plain
```

These are local checks, not hosted CI, merge or deployment evidence.

The final foreign-winner test also attempts the losing coordinator's old V94
activation when there is no pending proposal. SQL rejects the stale claim without
committing an execution record. Both parameterized foreign-winner cases passed
after this assertion (`final-fence-green.tar.gz`):

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryRecoveryAttemptsIT.differentCoordinatorWinnerCannotReplaceRetainedIdentity' --console=plain
```
