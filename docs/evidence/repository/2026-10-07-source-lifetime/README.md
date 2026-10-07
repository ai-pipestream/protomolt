# Historical source lifetime qualification

Base: `e0b2bd178b05c0459f9035a450b90a365ef85311`, branch
`refactor/repository-composition`, with this checkpoint's uncommitted source changes.
`source-sha256.txt` identifies the reviewed source files. Two Javadoc clarifications
followed the test run; executable source was unchanged. Sol reviewed the change
read-only and found no remaining blocker after the cleanup fix.

Command, exit 0, 24 seconds:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalSourceLifetimeTest' \
  --tests '*DocumentHistoricalSourceDrainIT' \
  --tests '*DocumentHistoricalMultiRevisionPublicationIT' \
  --tests '*DocumentHistoricalRegistrationAuthorizationIT' \
  --tests '*DocumentPreparationPinOwnerMigrationIT' \
  --tests '*DocumentPublicationScopeCallsTest' \
  --max-workers=2 --console=plain
```

14 tests passed, zero failures/errors/skips:

- Lifetime mechanism: 4, including actual worker exit after future cancellation,
  release failure/retry, admission closure and all-component cleanup on failure.
- Source drain integration: 3, using real PostgreSQL 18. Registration is held
  before commit or after actual commit before acknowledgement; source/history
  closure cannot release its borrowed Use. SQL visibility distinguishes the two
  gates. Returned schema resolution retains its Use and memory reservations until
  cleanup, using retained descriptors without ordinary registry access.
- Multi-revision publication: 2.
- Revoked historical registration authorization: 2.
- Populated capture-owner migration: 1.
- Existing publication scope calls: 2.

The initial run failed compiling the new cleanup test because Java could not infer
the functional interface of mixed lambdas and Work in `List.of`. An explicit
`AutoCloseable` type parameter fixed it. This is a test compilation correction,
not a demonstrated production red/green regression. The initial log is retained.
JUnit XML trailing whitespace was normalized; test values were not changed.

The publication fixture supplies synthetic provider observations. These tests
prove actual SQL and local source lifetime behavior, not object-store durability,
provider performance, or remote worker drain. No full storage suite was rerun for
this checkpoint. No protobuf changes were made.

Still required: async queued-before-start cancellation handoff, binding local drain
to exact V105 batch owners, physical native-pin/mirror release evidence, terminal
or qualified abandonment checks, canonical command coverage and crash recovery.
No durable drain marker, retention-root release or public historical execution is
enabled by this change. Factory construction requires open sources and can safely
fail if source admission closes concurrently before registration is accepted.
