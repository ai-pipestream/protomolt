# Initial historical capture confirmation

Baseline: `bd1a88de5aa80185525286356ea27c5447d7bf18`. Tested sources are
fingerprinted in `source-sha256.txt` and committed with this evidence.

`DocumentPreparationSourcePins.requireInitial` confirms an existing capture. It
does not insert or repair capture evidence, mint an execution handle, or enable
claimed historical execution. It requires initial generation/epoch identity,
the exact live claim and coordinator, a sealed initial batch created in the
retention set's transaction, exact owner/child tuples and count, and live
HISTORICAL read pins held with shared row locks. Terminal/abandoned/drained or
released state is refused.

The caller must already hold the claim before physical origin/retention locks
and retain live source Work. Full canonical V81 and root verification, operation
ownership and current ACL/credential checks remain separate handle-construction
obligations. This helper's successful return is not authorization. Its claim
fence serializes capture/closure changes; it never reacquires the claim after
locking live pins.

## Executed checks

The first attempt failed compilation because the test named a nonexistent
`RepositoryOperationLedger.FencedException`. Correcting it to the existing
`RepositoryExecutionClaimLedger.Fenced` produced seven passing tests in 16
seconds. The compiler failure is retained in `compile-failure.log`; the initial
run log and XML are in `initial/`.

Final command:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentInitialCaptureConfirmationIT' \
  --tests '*DocumentPreparationSourcePinsIT' \
  --tests '*DocumentPreparationCaptureDrainIT' \
  --max-workers=2 --console=plain
```

Exit 0 in 45 seconds, 36 executed tests, zero failures/errors/skips: ten new
confirmation cases and 26 existing capture-drain cases. The SourcePinsIT filter
matched no class; no coverage is attributed to it. Actual XML files are retained
in `final/`.

The new cases cover valid repeat confirmation, missing owner/batch, wrong claim
token, changed child tuple with unchanged count, noninitial flag, changed
creation transaction, real abandonment, released live pins and a real V107 drain.
Corruption cases explicitly use transaction-local replication mode to bypass
immutable-row triggers. They do not represent supported mutation APIs. Missing
evidence remains missing after refusal. SQL uses PostgreSQL 18; initial source
provider observations come from the existing synthetic publication fixture.

Sol reviewed the capture-only separation and implementation with no blocker.
The composed handle must still prove lock ordering, full canonical bindings,
authorization and released-root refusal. Local tests/review do not establish
hosted CI, merge, deployment or public historical execution.
