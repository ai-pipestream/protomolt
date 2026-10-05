# Original-owner stage restoration

Local qualification on 2026-10-05, based on `a1565284` plus this checkpoint.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentAssessmentStartJournalIT' --console=plain
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

The PostgreSQL suite passed nine cases. New cases cover retained preparation byte
ownership, idempotent close, absent start refusal, wrong owner, scoped-caller
refusal, and cleanup at every observed cancellation checkpoint.

The production-JAR PostgreSQL/LocalStack gate passed. Its new scenario creates
real provider content, loses the acknowledgment after an actual assessment COMMIT,
and restores from the shared journals under the supplied original live claim.
Wrong assessment IDs and deadlines fail before provider reads. Reconciliation
produces a durable admission rejection whose receipt binds the original assessment
ID, manifest and database deadline. Exact retry returns the same rejection signal
without additional assessment reads. Reservations and read pins drain.

Initial fixture runs exposed missing Java declarations and an incorrect success
expectation for a rejected candidate; those test errors were corrected before the
retained passing result. The specific rejection signal must carry the SQL receipt;
unrelated exceptions and normal success fail the fixture.

Sol reviewed the production and fixture changes without a blocker. This is local
correctness evidence, not RustFS performance, hosted CI, deployment, forced-process
crash recovery, or safe automatic claim transfer. Managed-host integration and
claim loss between restoration and resume remain unqualified.
