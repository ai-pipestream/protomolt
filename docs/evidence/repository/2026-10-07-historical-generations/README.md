# Bounded concurrent historical generations

Base: `aaab6c3fa07fac289cdd6c38ae17a5487f07b03c`. Sol reviewed the implementation
and tests. Review found and fixed a same-operation changed-command retry bypass;
new tests exercise that refusal. No protobuf contract or SQL migration changed.

## Behavior

The owner retains every Entry under a stable local UUID while separately selecting
the operation's retry Entry. `beginSuccessor` checks the attached predecessor and
exact expired claim/owner observation, allocates capacity and bytes before SQL, and
retains the old Entry. New old-generation mutation admission stops while takeover
is pending. Already accepted work remains owned and governed by SQL claim fences.
After confirmed V97, the predecessor is disposal-only. Successor preparation,
fresh capture and activation can proceed without waiting for old Work to drain.

Retry selects the retained successor before discovery; unchanged identity survives
V97 rollback or a lost commit reply. Changed caller/command cannot adopt it. Cleanup
addresses old generations privately by ID, preserving the new retry route. Capacity
counts all retained generations. Shutdown attempts every ready Entry even when an
older one cannot yet drain; unresolved Entries and byte leases remain retained.

## Focused qualification

```
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalGenerationsIT' \
  --tests '*RepositoryHistoricalPreparationIT' \
  --tests '*RepositoryInstalledHistoricalAttemptsIT' \
  --tests '*RepositoryHistoricalAttemptRetirementIT' \
  --tests '*RepositoryHistoricalSuccessorActivationIT' \
  --tests '*RepositoryHistoricalCaptureDisposalIT' \
  --max-workers=2 --console=plain
```

Exit 0, BUILD SUCCESSFUL in 1m48s. 61 cases, zero failures/errors/skips: five new
generation cases and 56 existing cases. Real PostgreSQL 18; archived-source setup
uses synthetic provider observations and does not establish provider I/O by itself.

New cases prove:

- An old borrowed Attempt and Work fork stay held while the same owner reserves,
  installs and activates a successor and obtains START. New source capture happens
  only after replacement installation; the old capture remains unreleased.
- Fenced retirement waits for old Work. When it finally retires, ordinary retry
  still selects the successor's stable ID.
- Shutdown releases the ready successor's history while keeping the blocked old
  Entry. A second cleanup after old Work closes returns the full byte budget.
- Entry-capacity refusal leaves the old route/budget intact and writes no V97 row.
- Actual JDBC before-commit failure and after-commit lost reply preserve one pending
  successor. The committed case keeps the exact SQL claim on retry. A changed
  canonical command with the same operation ID refuses without consuming more
  retained bytes. Cleanup by old ID leaves the new route intact.

## Packaged-provider regression

`./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain`
completed with exit 0, BUILD SUCCESSFUL in 11m14s. The aggregate JUnit case took
671.357 seconds, with zero failures, errors or skips (timestamp
2026-10-07T21:03:47.405Z). It exercised the existing PostgreSQL, LocalStack S3 and
Redis packaged-host scenarios, including self-supersession. This is correctness
regression evidence, not a RustFS performance measurement or proof of provider
publication while an older generation's Work remains held.

The terminal XML and Gradle log are archived here. Child logs are periodic snapshots;
the aggregate test's successful required-marker assertions and process exit checks
are the authoritative completion evidence. Source hashes match the focused run.

## Remaining scope


This is private ownership infrastructure, not enabled managed historical routing.
The full-call same-key runtime guard is unchanged. Actual provider publication with
old Work held, publication/takeover transaction races, byte-capacity refusal, and
pending V98 expiry while an older generation drains remain qualification work.
The deferred finalization guard rechecks live claim/owner leases: publication held
past expiry must roll back; it must never be presented as a valid old-generation win.
No scalability, pruning, JCR, hydration or complete-goal claim follows from this gate.
