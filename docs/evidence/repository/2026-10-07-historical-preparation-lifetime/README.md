# Retained historical assessment preparation

Base: `ad28648c8a522b826fd9828fb05278e1de926415`, branch
`refactor/repository-composition`. Sol reviewed the final source diff and found
no remaining blocker in ownership transfer, cleanup, authorization or early refusal.

The historical execution handle prepares an assessment using a child of its
accepted source work and registration call. Closing source admission does not
interrupt that accepted work. A returned assessment retains its own lifetime
after the execution handle closes. Cleanup releases assessment resources, source
work and registration in that order. Current authority is checked before and
after resolution; a revoked credential cannot receive successful findings.

Four new PostgreSQL-backed cases cover closed admission, admission closed while
a resolver is held, credential revocation during resolution and resolver failure.
They use actual descriptor assessment and durable credentials. Retained provider
observations in this fixture are synthetic, not provider performance evidence.
Existing historical publication, mixed-member, restore assessment and source
lifetime suites are included in the final run.

The initial run had 53 tests and four failures: the new fixture changed SQL
ownership after archiving a protobuf with different ownership. The implementation
correctly refused that mismatch. The fixture now sets ownership in the original
document before archival. The corrected run passed all 53 tests.

Final run: **64 focused tests plus one production-JAR storage driver test**, with
zero failures, errors or skips. Total Gradle duration: **8m37s**. The storage
driver exercises its existing real-provider probe bundle; it is not counted as
individually reported JUnit tests. Runtime inventory: 38 artifacts, 15,992,702 bytes.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalPreparationLifetimeIT' \
  --tests '*DocumentHistoricalRestoreAssessmentIT' \
  --tests '*DocumentHistoricalPublicationIT' \
  --tests '*DocumentHistoricalMixedMemberPublicationIT' \
  --tests '*DocumentHistoricalSourceDrainIT' \
  --tests '*DocumentHistoricalSourceLifetimeTest' \
  :protomolt-repo-container:admissionStorageTest \
  --max-workers=2 --console=plain
```

Compressed logs and XML distinguish initial, corrected and final runs.
`source-sha256.txt` identifies the tested sources. These are local results;
hosted CI, merging and deployment are separate.

Claimed historical CREATE remains explicitly unsupported and now refuses before
retention evidence or schema staging. CREATE must lock and verify the current
schema policy; preparation alone does not establish that authority. Actual
claimed provider execution, CREATE/reconciliation and publication remain work
for subsequent checkpoints. No protobuf contract or migration changed.
