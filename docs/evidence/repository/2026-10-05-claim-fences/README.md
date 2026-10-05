# Claim-aware operation SQL fencing

Validation command:

```
./gradlew :protomolt-repo-container:test --tests '*RepositoryClaimMutationFenceIT' --tests '*RepositoryExecutionClaimLedgerIT' --tests '*DocumentOperationUploadAdmissionIT' --tests '*DocumentPublicationRecoveryIT' :protomolt-repo-container:admissionStorageTest --console=plain
```

New integration cases exercise the same owner nonce after claim transfer, missing
claim context, independent operation expiry/takeover, claim expiry after owner-row
lock waits, stamp expiry at dependent checks, claimed upload admission requiring
both stamps, immutable scope selection in both first-insert race orders, and V78
migration with existing records versus ambiguous overlap. Exact PostgreSQL PIDs
and `pg_blocking_pids` establish contention.

Sol identified two defects during review. `red-tokenless-stamp.xml` proves that
an initial no-op claim UPDATE could create authority without presenting its token.
Consumed epoch/token proof fields now reject that update, copied stamps, supplied
transaction IDs, wrong epochs and old tokens; exact proof works and is cleared
before storage. `red-deferred-finalization.xml` proves the success finalizer initially
omitted claim liveness. Its corrected test delays before SET CONSTRAINTS, and the
new finalizer refuses that expired claim. An earlier test delayed after the fixture
had already run deferred constraints; it was not evidence of finalizer behavior
and has been replaced. Fixture setup also needed real member selections for the
new operation. Neither fixture correction changes production semantics.

This is SQL fencing for explicitly claimed operations. Automatic session recovery
is not enabled; current publication sessions still use unclaimed admission. The
proof does not protect against a malicious database owner who can read private
tokens or disable triggers. SQL fixtures alone do not qualify provider safety or
performance. The provider gate exercises the existing production JAR path; it does
not establish shared-session crash recovery or RustFS capacity.

Final gate: BUILD SUCCESSFUL in 2m 51s. All 123 focused PostgreSQL cases passed
(11 mutation-fence, 7 claim, 91 upload, 14 recovery), with zero failures/errors/skips.
The separate production-JAR PostgreSQL/LocalStack storage test also passed. Its
JUnit XML and runtime inventory are retained here. The inventory contains 38
artifacts totaling 15,733,834 bytes. These tests establish correctness only.
