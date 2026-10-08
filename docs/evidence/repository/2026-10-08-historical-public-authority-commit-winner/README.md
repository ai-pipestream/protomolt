# Historical publication before credential and policy administration

Parent: `17be60382fd408c18cd7546d7dc7d844bcd99d68`.

The existing READ/WRITE publication-first cases now also cover credential revocation
and admission-policy replacement through library and authenticated in-process gRPC.
Every operation uses a fresh registered credential. Revoked credentials stay revoked.

HistoricalObservedWriter records the actual JDBC connection used by one transaction
in RepositoryCredentialAuthorities.revoke or DocumentSchemaPolicies.activate.
PostgreSQL must identify the publisher PID as its blocker before publication commits.
The administrator finishes before the JDBC publication reply returns to the runtime.
The observer refuses additional connection acquisition while the transaction runs.
All database and storage effects use PostgreSQL and LocalStack S3.

Credential revocation denies receipt delivery and retry with UNAUTHENTICATED after
the result has committed. Policy replacement preserves exact authorized delivery
and replay. SQL retains the same result and exactly one success, assessment owner
and revision commit per operation. Policy content is restored through CAS at a later
revision after the worker threads finish. Test factories close after runtime cleanup.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.publicCommitWinner' \
  --max-workers=2 --console=plain
```

Passed in 31 seconds: one aggregate case, zero failures, errors or skips. The driver
requires all eight READ/WRITE/credential/policy library/gRPC markers. Final log and
XML are archived here. Subsequent source edits changed diagnostics and removed an
unused import only.

This is publication-first SQL ordering and postcommit receipt authorization. External
identity providers, network failures, performance and scale-out are outside this
in-process transport evidence. Recovery-journal qualification remains separate work.

Sol reviewed the connection observation, lock ordering, fresh credentials and
policy restoration without a blocker. Shared diagnostics now describe authority
changes rather than only ACL changes.
