# Private historical registration qualification

Base `1dd49f259f73a072b2bd29166fa7c9c9c9de16c7`, plus this checkpoint's changes.
Sol reviewed implementation, lock ordering and fault tests with no blocker for
the private registration boundary. No public historical execution is enabled.

## Local verification

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalMultiRevisionPublicationIT' \
  --tests '*DocumentPreparationHistoryRootsIT' \
  --tests '*DocumentPublicationPreparationCodecIT' \
  --tests '*DocumentJournaledSessionsIT' \
  --max-workers=2 --console=plain
```

Exit 0, 53 seconds, 41 tests with no failures, errors or skips. XML trailing
whitespace normalized. `initial.log` records the preceding passing run of the
multi-revision, preparation-journal and execution-claim suites before fault cases
were added. These are local results, not hosted CI or deployment evidence.

The multi-revision fixture uses real PostgreSQL and production schema retention,
selection, pinning and registration code. Provider observations in that fixture
are synthetic, as its class comment states. Two historical revisions of one node,
and historical plus current reuse, both register with repeated selectors.

JDBC wrappers delegate to the real connection. Before COMMIT, a fault waits until
the new operation owner exists in that transaction, then refuses commit. All
eight row families are absent when read from another connection. On retry, the
real COMMIT succeeds before a lost-acknowledgment exception is injected. All eight
families are present, with the expected root count. The next exact retry recovers
the same owner and preserves claim/owner lease timestamps. Closing the source
owner makes subsequent retries fail without lease renewal. Canonical root
coverage is exact. Assessment start and ordinary preparation still refuse
historical execution.

## Remaining qualification

The broader `:protomolt-repo-container:admissionStorageTest --max-workers=2
--console=plain` regression passed (exit 0); `storage.log` and its XML retain the
result (7 minutes 45 seconds, one harness test, no skips). It exercises the
production-JAR storage runtime and required probe markers,
not public historical execution. Source authorization races, additional physical witness failures,
manager ownership through process recovery, successor attachment, release and
pruning remain pending. Mode storage is admission intent, not a validated or
semantically accepted document. No successful publication receipt is produced by
this registration path.

The first authorization fixture failed with `RevisionConflictException`: it
changed the current security policy, which advances the document mutation
revision, then reused an old destination condition after restoring the policy.
The corrected fixture samples the actual revision after initial policy setup and
separates initial denial from denial after successful registration. No production
revision guard was weakened. The failed run is retained in
`authorization-fixture-failure.log`.

The corrected run passed in 56 seconds, exit 0, 44 tests, no failures or skips:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalRegistrationAuthorizationIT' \
  --tests '*DocumentScopedRegistrationIT' \
  --tests '*RepositoryExecutionClaimLedgerIT' \
  --tests '*DocumentPublicationPreparationJournalIT' \
  --max-workers=2 --console=plain
```

The new two-case test uses a non-operator caller and an existing destination.
It captures a historical source with READ and WRITE, then removes READ while
leaving WRITE. Initial denial creates none of the eight registration row families;
denial after successful registration preserves the claim and owner leases. The
policy edit also advances the mutation revision, so NOT_FOUND proves current
authorization wins before stale-condition disclosure. This uses fixture SQL for
the policy edit, not a production policy-management API or a concurrency race.
Sol reviewed the corrected test with no blocker for this scope.
