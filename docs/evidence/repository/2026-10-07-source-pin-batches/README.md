# Durable historical source pin batches

Validated working-tree changes based on
`fa46c25fd948f12008cb40b3f0d6d168407bb694`. These receipts accompany the V104
migration, internal association helper, and registration wiring in the same commit.
Sol reviewed this evidence-only prerequisite without a blocker. It does not enable
retention release or claimed historical execution.

## Focused PostgreSQL qualification

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentHistoricalMultiRevisionPublicationIT' --tests '*DocumentHistoricalRegistrationAuthorizationIT' --tests '*DocumentPreparationHistoryRootsIT' --tests '*DocumentPublicationPreparationJournalIT' --max-workers=2 --console=plain
```

Exit 0, 41 seconds, 30 tests: multi-revision (2), registration authorization (2),
history roots (3), preparation journal (23). No failures, errors or skips.
XML trailing whitespace was removed for repository hygiene.

The historical registration cases use real PostgreSQL with synthetic provider
observations. They verify:

- Source associations, roots, preparations, claims and owners roll back together
  when JDBC commit fails, and survive a lost acknowledgment together.
- Exact retry retains one batch and leaves both claim and owner leases unchanged.
- A forced early deferred check refuses a new historical set without its initial
  pin batch. Normal registration completes that batch before commit.
- Unsealed batches, mismatched count/digest, forged live pins, evidence deletion
  and sealed-header mutation are refused without partial committed associations.
- Repeated selectors deduplicate physical pins; two source revisions remain distinct.
- Fresh captures append evidence. Normal live-pin release succeeds while evidence
  stays, and exact batch confirmation still succeeds afterward.
- The sixteenth total batch succeeds, a seventeenth fresh batch fails, and an
  exact confirmation at the quota succeeds. Only the original batch is initial.
- Migration from V103 preserves rows without inventing initial pin evidence.
  The deliberately forged V103 command projection remains refused by Java.

The first focused run failed because a new test called the publication lock helper
with an empty destination set. The helper correctly rejected it. The fixture now
supplies the actual destination nodes; production checks were not weakened. Its
log is retained as `initial-fixture-failure.log`.

## Production JAR storage regression

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

Exit 0, 7 minutes 43 seconds. The one JUnit orchestration test completed in
461.541 seconds with no failures, errors or skips. It runs the observed production
JAR host and child-process restart/recovery probes against actual PostgreSQL,
versioned LocalStack and Redis containers. This checks migration packaging and
existing admission/storage behavior; the focused cases above qualify the new
private pin associations. LocalStack is a correctness fixture, not a performance
measurement. The lease-wait recovery case was allowed to complete normally.

## Scope

V104 proves immutable capture identities and exact retry behavior. SQL membership
checks do not decode protobuf commands to prove selected-object completeness.
Future release must reconcile canonical command coverage, terminal/abandonment
identity, accepted work drainage and all relevant capture batches. No public API,
backup/restore qualification, performance claim or JCR capability follows from
these focused tests. There is no capture-batch retirement implementation yet.
The next release prerequisite is exact claim epoch/token and coordinator binding
for each batch, plus a batch-local source/worker drain proof. V104 does not yet
provide those identities. Older records must not be assigned an assumed owner.
