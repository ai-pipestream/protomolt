# Deterministic publication preparation

The focused PostgreSQL suite passed: 103 tests, zero failures/errors/skips.

```
./gradlew :protomolt-repo-container:test --tests '*DocumentPublicationSeedsIT' --tests '*DocumentPublicationSessionIT' --tests '*DocumentOperationUploadAdmissionIT' --console=plain
```

Four new cases cover reconstruction of exact owner/attempt/lease identities into
real SQL rows, command and caller binding, immutable capability maps, and rejected
missing/extra/aliased identities. Existing session and upload cases remain green.
An initial test incorrectly assumed the fixture's non-mixed command uploaded both
members; it actually reused both. The final test explicitly constructs two upload
members. No production workaround was made for that fixture error.

Sol reviewed the preparation slice with no blocker. The new identities are private
and confer no restored execution authority. There is no durable session store,
automatic failover, hard-crash qualification or new public API in this change.
See `docs/design/repository-publication-recovery.md` for those prerequisites.
This suite is correctness evidence, not a provider or performance measurement.
