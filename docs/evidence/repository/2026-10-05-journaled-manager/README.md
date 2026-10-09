# Journaled manager retention

Source base: `978a1f7c2b82c515541b213d9effce958ff1cd36`. Run on October 5, 2026,
America/New_York (October 6 UTC).

The red run added only the opt-in constructor/factory and journaled session
selection before fixing retention or blocking unjournaled replacement. Seven
assertions failed: invalid pre-registration modes retained capacity, and six
lost-acknowledgment/cancellation cases reached the incompatible old recovery path.
`red.xml` and `red.log.gz` record that intermediate implementation, not a claim
that an undefined factory compiled against the unchanged base.

The registration now sets a monotonic, visible marker before `acquireInitial`.
The manager only evicts pre-journal failures when no users/recovery remain; after
the marker it preserves exact identity on every exception. Opt-in `recover()`
refuses before the old entry-replacement branch. Default manager wiring is unchanged.

Focused command:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentJournaledSessionsIT' \
  --tests '*DocumentPublicationRecoveryIT' \
  --tests '*DocumentScopedRegistrationIT' \
  --tests '*DocumentPublicationSessionIT' --console=plain
```

Result: 48 tests passed, none skipped (10 manager, 14 existing recovery, 7 scoped,
17 session). `green-*.xml` and `green.log.gz` retain the result. The new cases use
real PostgreSQL and controlled JDBC acknowledgment/cancellation faults. Actual
missing-policy refusal stops them after registration, before provider work; provider
ports throw if reached and never simulate success. Retained object observations in
the SQL fixtures are synthetic and do not establish provider correctness.

The tests verify exact claim token/lease and preparation bytes on retry, initial
pair/modes/owner commit uncertainty, pre-journal input and scoped permission denial,
byte-capacity refusal, same-key concurrency, shutdown/drain retention, and refusal
of the incompatible recovery path. `retireSuperseded` false leaves the identity
intact. The monotonic marker is conservative: an internal pre-SQL allocation or
encoding failure can retain capacity. Explicit cleanup and fresh-process partial
registration remain open requirements; shutdown does not establish restart recovery.

The production-JAR gate also passed in 2m36s:

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

`provider.xml` records the wrapper test and `provider.log.gz` its build output.
Its real PostgreSQL/LocalStack execution matrix now routes acceptance, rejection,
CREATE rollback, lost CREATE acknowledgment and lost decision acknowledgment
through the opt-in manager. Retries supply no replacement placements or bodies;
callbacks refuse repeated uploads or schema resolution. Terminal entries release
capacity; uncertain staging retains its entry. Existing direct-session variants
remain covered. These provider scenarios use process authority; scoped provider
qualification and ordinary host activation remain outstanding. `source.sha256`
identifies the implementation and fixture files used for these runs.
