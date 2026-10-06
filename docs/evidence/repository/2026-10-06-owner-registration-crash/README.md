# Owner admission across process death

The registration harness now covers six process-death positions: before/after
initial preparation, fixed modes and owner admission. Real PostgreSQL JDBC hooks
halt the writer immediately around the target COMMIT. The owner hook requires the
claim, preparation, mode, owner and admitted-command rows on that connection, so
earlier registration transactions finish normally.

Before owner COMMIT, a fresh reader observes MODES_BOUND with no command or owner.
After COMMIT it observes OWNER_ADMITTED and verifies the exact admitted command,
generation one and the original preparation's owner nonce. Original-authority retry
returns that same owner and lease. The parent compares the writer's transaction
lease timestamp with the fresh reader's timestamp; the original claim lease and
token also remain unchanged. No assessment, upload/selection attempt or terminal
result is created, and read reservations drain.

The writer is reaped and its input deleted before the reader starts. The reader
receives only the operation ID and expected phase. Its direct private SQL lookup is
test-only evidence, not a production capability-discovery API, coordinator
quiescence protocol or permission to transfer ownership.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentInitialRegistrationCrashIT' \
  --tests '*DocumentPublicationRegistrationInspectionIT' \
  --tests '*DocumentPublicationPreparationJournalIT' \
  --tests '*DocumentPublicationModesJournalIT' --console=plain
```

36 tests passed with no skips in 45 seconds. Sol reviewed commit targeting and
identity/lease assertions without a blocker. Source provider observations are
synthetic fixture data; no provider-crash behavior or automatic resume is claimed.
Production activation, hosted CI, merge and deployment remain separate gates.
