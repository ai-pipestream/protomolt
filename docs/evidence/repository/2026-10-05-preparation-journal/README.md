# Shared preparation journal

Local gate, 2026-10-05, real PostgreSQL 18 test containers. Publication fixture
observations are synthetic; this is not provider or forced-crash qualification.

`red.log` captures two failures before the fixes: a claim transferred after the
SQL read could still receive decoded preparation, and a same-principal account
member could retrieve the private recovery command without process authority.

`green.log` runs:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPublicationPreparationJournalIT' \
  --tests '*DocumentPublicationPreparationCodecIT' \
  --tests '*RepositoryExecutionClaimLedgerIT' \
  --tests '*RepositoryClaimMutationFenceIT' --console=plain
```

37 cases pass: 10 journal, 7 codec, 9 claim, 11 mutation-fence. Build time: 29s.
The journal cases cover retained identities reaching real upload admission,
fresh journal instances, exact retries, conflicting records, immutable SQL rows,
expired-predecessor requirements, command bootstrap authorization, real claim
transfer during load, cancellation before delivery and after save commit,
capacity refusal, whole-blob corruption and reservation cleanup.

Automatic recovery remains disabled. Modes, stage markers, assessment coordinates,
provider effects across transfer and process-crash qualification remain open.
No latency, horizontal capacity, hosted CI, merge or deployment claim is made.
