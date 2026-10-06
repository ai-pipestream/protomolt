# Fenced shutdown regression

Base: `f682c1ff4d1bb293a223e2814c0d1e64046ec579`.

The new `DocumentSuccessorTargetIT` case
`supersededUnactivatedSessionCanFinishShutdownWithoutInventingDrainMarkers`
uses PostgreSQL and real V93/V98 transactions. A locally retained successor never
activates, expires, and is superseded by another coordinator. Before the fix, shutdown
throws `RepositoryExecutionClaimLedger.Fenced` in `drainRegistrations` instead of
classifying the displaced registration and completing local drain.

The initial targeted run had one expected failure, no skipped cases. `red.tar.gz`
preserves its XML; this archive is not green qualification. Subsequent passing
runs below exercise the implementation. The recovery design records remaining
qualification work.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentSuccessorTargetIT.supersededUnactivatedSessionCanFinishShutdownWithoutInventingDrainMarkers' --console=plain
```

## Implementation and focused checks

`RepositoryShutdownClaim` checks the retained digest and claim under its SQL lock.
An unregistered loser cannot become active after permanent fencing. A bound
coordinator needs the complete immutable reservation chain to the current claim.
Session shutdown counts fenced identities separately from V90 markers and rechecks
before final V91 attestation. Unreviewed transfers remain unresolved. The runtime
still waits its local workers, and respects unresolved final attestation.

The initial affected suite passed 38 cases (`initial-green.tar.gz`): 3 successor
target, 25 journaled session and 10 successor manager cases. Added bound-transfer
and same-epoch winner checks passed 3 cases (`bound-green.tar.gz`). The complete
V97/V98 chain, subsequent unreviewed gap and real post-commit cancellation passed
one additional case (`chain-green.tar.gz`). No cases were skipped. A test-only
AssertJ generic-inference compile error was corrected before the chain run.

Sol found no production blocker. Race-time SQL failures propagate; a later
shutdown retry rechecks identity. SQL timeouts bound elapsed database work, not
history length or fixed intermediate memory. This does not qualify a real held
schema worker combined with takeover, long histories, performance, or complete
automatic host recovery. Those items remain in the repository work list.

## Final verification

The final run passed 59 cases: 27 journaled-session, 10 successor-manager,
3 successor-target, 18 expiration and 1 packaged storage-runtime test, with no
failures, errors or skips. `final-green.tar.gz` contains the XML. Packaged coverage
includes the existing real managed Git-worker drain and partial-attestation retry;
it does not yet combine that held worker with the new fenced identity path.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentSuccessorTargetIT' --tests '*DocumentJournaledSessionsIT' --tests '*DocumentSuccessorManagerIT' --tests '*RepositoryCoordinatorExpirationIT' :protomolt-repo-container:admissionStorageTest --console=plain
```
