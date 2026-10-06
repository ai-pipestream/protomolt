# Coordinator SQL admission closure

Base: `a0dd65e92694aed1863789a30d878895fcb6d444`, plus the accompanying source changes.
V90 is a private SQL prerequisite. It does not implement provider-start closure,
LOCAL_DRAINED, automatic transfer or default runtime activation.

Validation:

- The historical V84 abandonment fixture initially failed twice because it used
  today's registration API, which requires V89. Seeding the old database through
  its original claim and preparation primitives fixes the fixture without adding
  a production missing-table fallback. The abandonment, reuse and retention-inventory
  upgrade regression then passed in 29 seconds.
- The drain, claim mutation, abandonment and upload admission regression passed
  126 cases in 37 seconds on the final source. Compressed XML contains original test outputs.
- The drain suite covers V89-to-V90 migration across four registration phases,
  exact registration/start retries, continued owner renewal and terminal cancellation,
  denied new upload attempts, wrong identity, cancellation around commit, immutable
  markers, lost real commit acknowledgment, confirmation after expiry/transfer, and
  refusal of owner-generation advancement. Provider bytes are not exercised here.

The lock-order case observes a modes admission SQL
statement blocked on the held drain transaction through pg_stat_activity and
pg_blocking_pids before permitting COMMIT. Sol reviewed this stronger test and the
implementation without a remaining blocker. An earlier test revision matched the
table name rather than the actual fence-function query; that observer failed until
corrected. No production fencing rule was relaxed to make the test pass.

Final focused command:

```
./gradlew :protomolt-repo-container:test --tests '*RepositoryCoordinatorDrainIT' --tests '*RepositoryClaimMutationFenceIT' --tests '*DocumentPublicationAbandonmentIT' --tests '*DocumentOperationUploadAdmissionIT' --console=plain
```

All throughput, provider-start, successful provider-backed settlement during drain,
local shutdown, successor execution and pruning claims remain outside this evidence.

`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain` also passed
against V90. The compressed provider XML records the aggregate production-JAR
PostgreSQL/LocalStack regression. These ordinary publication cases do not insert a
drain marker; they establish compatibility of existing provider paths with the new
migration, not successful provider settlement while draining. The only later Java
edit reindents the lock-observer test without changing its SQL or assertions.
