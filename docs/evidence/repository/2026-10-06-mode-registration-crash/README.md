# Mode binding across process death

The initial registration crash harness now also stops a writer JVM immediately
before and after the real mode-binding COMMIT. Its hook waits for both initial
registration rows and the mode row on the committing connection, so it does not
interrupt the earlier claim/preparation transaction.

The parent reaps the writer and deletes its private request before starting the
reader with only the operation ID and expected phase. Before-COMMIT death preserves
the preparation but no mode row: inspection returns PREPARATION_ONLY. After-COMMIT
death preserves the exact TYPED/OPAQUE choices: inspection returns MODES_BOUND.
The reader retries only the map loaded from SQL; it never invents missing choices.
Exact preparation hashes and original claim token/epoch/lease checks still apply.
The parent compares the writer's pre-binding lease timestamp with the fresh reader's
SQL timestamp, independently checking that mode binding did not renew the lease.
That timestamp is test evidence; no private authority is passed to the reader.
No owner, admitted command, assessment start, upload/selection attempt or terminal
result is created. All read reservations drain.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentInitialRegistrationCrashIT' \
  --tests '*DocumentPublicationModesJournalIT' \
  --tests '*DocumentPublicationPreparationJournalIT' \
  --tests '*DocumentPublicationRegistrationInspectionIT' --console=plain
```

34 tests passed in 36 seconds, none skipped, including all four process-death
positions. Real PostgreSQL is used; source provider observations are synthetic
fixture data. This qualifies durable registration boundaries, not provider crash
behavior, automatic takeover, host quiescence or ordinary session activation.
Sol reviewed the four-phase commit hooks and fresh-reader boundaries; its suggested
pre-binding lease comparison is included. Hosted CI, merge and deployment are separate gates.
