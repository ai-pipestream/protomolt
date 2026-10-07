# Successor publication after rollback

Base: 38dd2f6abc5cbd58f7eb2c2a4dfa3774485d036d. Sol reviewed the fixture.
Production implementation is unchanged.

```
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.claimExpiresBeforeFinalization' --max-workers=2 --console=plain
```

Final: 1 test, 0 failures/errors/skips; Gradle 1m06s, JUnit 64.067s,
timestamp 2026-10-07T22:45:05.156Z. Initial run passed in 1m07s.

After rollback and takeover, the same operation installs a successor using the
original retention reference. A fresh capture rereads immutable provider versions;
the caller supplies upload bytes again. PostgreSQL and LocalStack support the
complete publication and receipt checks.

Assertions cover distinct claim, process, owner, upload and capture identities;
2 reservations, 2 installations, 3 STARTs, 2 CREATEs, and 1 publication. Cleanup
returns retained memory and the receipt identifies the successful generation.

The mandatory aggregate requires this result but has not been rerun for this
extension. Before-claim arbitration and public historical routing remain open.
