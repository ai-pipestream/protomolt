# Claimed historical publication qualification

Base: `4b8ad159eca36e41f4d6f8c57e540d738629cbbe`. This is a private
execution path, not an advertised public feature. Source hashes accompany each
run; results for an earlier source do not qualify later changes.

## Implementation

A sealed internal transaction-fence interface is implemented only by a live
historical execution handle. Publication binds the exact retained CREATE manifest
and START, stages under current policy and authorization, then promotes once.
It reuses DocumentPublicationCommit's revision and V52 receipt writer.

Commit orders registration/claim, owner, policy/full authorization, retained
assessment, complete physical origins, capture pins, artifacts and writes. After
deferred constraints, a database-clock check requires the exact stage to remain
sealed, unreleased and unexpired. Post-promotion failure requires reconciliation;
there is no automatic retry or second promotion.

Opaque assessments use an explicit authorized no-artifact transaction for CREATE
and publication. RepositorySchemaArtifacts retains its nonempty-batch contract.
Sol reviewed the production path and the qualification fixtures without a remaining
production blocker. Runtime qualification is recorded separately below.

## Verified evidence

All packaged runs use:

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

- Files at this directory's root: run 74361, typed reuse-only successor publication,
  exact replay and duplicate-attempt refusal. Full driver passed in 8m 2s; XML
  reports one test, zero failures/errors/skips. This predates the opaque changes.
- `explicit-opaque-and-lost-ack/`: run 91545, full driver passed in 8m 2s; XML
  reports one test, zero failures/errors/skips, including restart/cleanup. The
  host proves explicit opaque historical publication with no schema-artifact
  claims, and lost publication COMMIT acknowledgment with durable receipt replay.
  The JDBC fault matches account/principal/operation/generation and current
  transaction ID, delegates the real commit, then raises SQLState 08006. Assertions
  compare replay to persisted receipt bytes, require one success and revision,
  refuse a repeated attempt, and verify released payload ownership.
- `expiry-with-budget/`: run 26180, initial SQL/provider host completed with
  `OBSERVED_SQL_HOST_OK`, including the expiry case and later bounded-host checks.
  Source hashes were reverified. The full driver subsequently passed in 8m 22s;
  archived XML reports one test, zero failures/errors/skips, including restart
  and cleanup. This predates the credential-revocation fixture.
- `credential-revocation/`: run 9856, initial SQL/provider host completed with
  `OBSERVED_SQL_HOST_OK`, including `CLAIMED_HISTORICAL_PUBLICATION_REVOKED_OK`.
  The exact scoped credential is revoked through the real authority API after
  promotion, before publication SQL acquires locks. Assertions require one-shot
  injection, durable revocation, UNAUTHENTICATED publication and observation,
  unchanged current head/outbox, no publication rows, preserved staging claims,
  sticky retry refusal and released payload ownership. This is revocation-first
  coverage, not a simultaneous race. The full driver passed in 8m 22s; its XML
  reports one test, zero failures/errors/skips, including restart and cleanup.
- `regressions/`: 43 tests passed in 54 seconds, with zero failures/errors/skips:
  DocumentPublicationCommitIT (32), DocumentHistoricalPublicationIT (5),
  DocumentHistoricalPublicationAuthorizationIT (2), DocumentAssessmentPreflightIT
  (4). Source hashes still matched the credential-revocation run.

The focused regression command was:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPublicationCommitIT' \
  --tests '*DocumentHistoricalPublicationIT' \
  --tests '*DocumentHistoricalPublicationAuthorizationIT' \
  --tests '*DocumentAssessmentPreflightIT' --max-workers=2 --console=plain
```

The expiry case holds an actual historical origin row after candidate promotion.
It requires PostgreSQL to report the exact publisher blocked by that holder both
before and after database-clock expiry, then requires the final
`Publication assessment is no longer live` refusal. It checks unchanged current
revision/mutation and outbox count, no operation success/revision rows, pending
replay, sticky refusal and payload cleanup. Worker shutdown is bounded; rollback
cleanup preserves the original failure.

## Preserved failed runs and harness correction

- `opaque-fixture-failure/`, run 32711: the fixture selected a typed historical
  revision for opaque mode. Production correctly refused a downgrade.
- `legacy-opaque-fixture-failure/`, run 84384: the fixture selected the original
  unbound publication, which lacks an explicit retained admission. Production
  correctly refused to infer opacity from missing evidence. The corrected fixture
  creates an opaque revision through DocumentPublicationRuntime before the typed
  revisions and uses the actual returned identity. No admission guard was relaxed.
- `expiry-host-timeout/`, run 61272: the expiry scenario passed, but the aggregate
  host reached its 180-second cap during later bounded-host startup. This remains
  a failed full run, not a suite pass.

Sol compared timestamps: the prior green host reached bounded startup at roughly
160 seconds; adding the deliberate 20-second expiry wait moved the same phase to
180 seconds. The driver now budgets 180 + 30 seconds for that added wait and
bounded setup. Operation/provider deadlines, zero-exit requirements and every
marker assertion remain unchanged. This does not establish the cause or resolution
of the separate hosted Java25 timeout on the integration candidate.

## Remaining qualification

Publication-time source-policy revocation, publication-wins races, mixed
claimed publication, broader recovery discovery and public integration remain.
Credential revocation before commit locks is qualified as described above.
This checkpoint does not close pruning, provider qualification,
performance, JCR compatibility or progressive hydration requirements.
