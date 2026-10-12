# Schema payload rehearsal baseline (PR 310)

2026-10-12. The in-container backup rehearsal (`RepositoryBackupRehearsalIT`)
passes 10 of 12 cases both on the untouched base and on the schema-payload
branch. The same two negatives fail on both with byte-identical recovered-host
check signatures, so they are pre-existing and unrelated to the payload split.
Root cause is not established; they are not classified as environmental.

- Base: `bed8cf29d` (Forgejo/GitHub main at assignment creation), run in an
  isolated worktree, since removed.
- Branch: `agent/schema-payload-separation` at the repaired V120 head
  (implementation commit recorded in `RESULTS.md`).

Command (both runs, under the repository qualification lock):

```sh
./gradlew -I repo/container/src/test/resources/backup-recovery/rehearsal.init.gradle \
  :protomolt-repo-container:test \
  --tests 'ai.protomolt.proto.repo.container.ledger.RepositoryBackupRehearsalIT' \
  --max-workers=2 --console=plain
```

Failing cases on both runs (see the archived JUnit XML beside this record):

- `inconsistentCatalogAndObjectSnapshotRefuses`
- `wrongProviderCredentialsRefuse`

Both fail in the recovered host's `history_refused` check: the test expects a
provider-level `BlobStoreException` ("rejected the read") but observes
`FAILED_PRECONDITION` before provider access. The failing signatures are
archived in `base-check-failures.txt` and `branch-check-failures.txt`; the two
files are identical.

Schema-relevant rehearsal coverage passes on the branch: both positive restore
runs, plus the `corrupt-descriptor` and `missing-descriptor` negatives against
the new payload relation.

Follow-up ownership: coordinator to triage/assign. This assignment makes no
change in the affected read/provider area.
