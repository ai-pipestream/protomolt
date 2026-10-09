# Exact historical operation admission

The private assessment path records the actual canonical historical command.
Authenticated account and principal checks precede operation insertion. Selection
staging checks current source READ and destination authorization, owner lifetime,
exact command identity and complete live historical references.

Real PostgreSQL cases bind r1 after r3 becomes current alongside current reuse or
an upload. Invalid account and principal tests verify no operation was inserted;
invalid account staging verifies no selection was inserted. Exact retries preserve
the owner, changed commands conflict, and closed source Uses prevent admission and
binding. The physical binder fixture now uses the historical command's own owner.
The upload branch uses synthetic provider observations through the selected-attempt
ledger; it does not establish provider byte correctness.

```sh
./gradlew :protomolt-repo-container:test \
 --tests '*DocumentHistoricalSelectionIT' \
 --tests '*DocumentHistoricalAuthorizationIT' \
 --tests '*RepositoryOperationAdmissionIT' \
 --tests '*DocumentOperationUploadAdmissionIT' \
 --tests '*DocumentJournaledPublicationSessionsIT' --console=plain
```

Result: 161 tests, zero failures/errors/skips; build completed in 45 seconds.
Local log: `/tmp/protomolt-historical-exact-admission-qualified.log`.
Sol reviewed the identity checks and found no blocker. `git diff --check` passed.
This is local qualification, not hosted CI, merge or deployment.

Whole-command assessment with retained schemas, observed-runtime CREATE and receipt
recovery remain unfinished. Claimed operations, sessions and public historical
execution remain disabled. Document access requires separate authorization.
