# Repository regression baseline repair

Branch `agent/repository-regression-repair`, base `ad28648c8a522b826fd9828fb05278e1de926415`
on `refactor/repository-composition`. Local Docker evidence (PostgreSQL 18 and
LocalStack Testcontainers) on the krick workstation; not hosted CI, a merge, a
release or a deployment. Every run below records HEAD, working-tree status, the
exact command and exit status in `head.txt`/`exit.txt`, the Gradle console in
`gradle.log`, and the JUnit XML in `test-results-xml.tar.gz`.

| Run | Tree | Command | Exit | Result |
|---|---|---|---|---|
| `01-base-focused` | untouched base | focused nine families (below) | 1 | 98 tests, 12 failures, 0 skips |
| `02-focused-repaired` | base + this branch's test changes | same focused command | 0 | 98 tests, 0 failures, 0 skips |
| `03-full-container` | same | `:protomolt-repo-container:test --max-workers=2 --console=plain` | 0 | 219 classes, 1946 tests, 0 failures, 1 skip |
| `04-full-service` | same | `:protomolt-repo-service:test --max-workers=2 --console=plain` | 0 | 55 classes, 490 tests, 0 failures, 3 skips |

Focused command for runs 01 and 02:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentOperationCommandsIT' --tests '*DocumentRevisionSchemaArtifactsIT' \
  --tests '*DocumentRevisionSchemaAssetsIT' --tests '*DocumentRevisionSchemaEvidenceIT' \
  --tests '*RepositoryCoordinatorHandoffIT' --tests '*RepositoryCoordinatorSupersessionIT' \
  --tests '*RepositorySuccessorActivationIT' --tests '*RepositorySuccessorInstallIT' \
  :protomolt-repo-service:test --tests '*ArchiveDeletionFailureIT' --max-workers=2 --console=plain
```

The four skips are the pre-existing benchmark suites gated by environment
variables (`DocumentStagingBenchmarkIT`, `ArchiveReadBenchmarkIT`,
`DocumentPartialBenchmarkIT`, `RepositoryScaleBenchmarkIT`); none is touched here.
The container default task also excludes the opt-in `admissionRuntimeTest`,
`admissionStorageTest`, `admissionRustFsTest` and `scopedPublicationTest` tasks.
They were not run: no runtime-hosted or production behavior changed in this branch.

## Disposition of the 18 historical failures

`per-case-table.txt` lists each case with its run-01 failure message and run-02
status. Only test sources and the two shared test fixtures changed; no production
Java, SQL migration, constraint, guard, ACL or execution-claim check was edited.

| # | Case | Base `ad28648c` | Defect | Repair |
|---|---|---|---|---|
| 1 | `DocumentOperationCommandsIT.refusesUnsupportedOrCorruptStoredCommands` (7: malformed, member-order, unknown-field, operation-id, wrong-account, invalid-intent, version) | still failing, `Publication codec requires typed command admission` in fixture setup | fixture | The untyped ledger admission is a real guard (V79 typed-admission boundary) and must refuse publication-codec bytes. The test now asserts that refusal, then persists the payload through a clearly labelled corruption fixture (direct SQL row plus owner, digest constraint satisfied), confirms the row exists, and then verifies the loader's own refusal code (`DATA_LOSS`/`UNSUPPORTED`). The `codec` case keeps the real untyped path. |
| 2 | `DocumentRevisionSchemaArtifactsIT.wrongAccountPrincipalOrGenerationCannotBorrowTheWriteFence`, `DocumentRevisionSchemaAssetsIT.wrongScopeAndGenerationCannotUseCurrentOwnerFence` | still failing, `Repository execution scope is absent` raised by `require_repository_execution_claim` before the write-fence check | fixture | Since V79 the write fence first requires an execution scope; a never-admitted foreign account or principal stops at that earlier guard, so the intended fence check was not reached. The tests now admit real operations under the foreign account and the foreign principal (same operation id) so their scopes exist and exactly one binding differs, and assert `live owner write fence` for account, principal and generation. A fourth variant with no scope at all asserts the earlier `Repository execution scope is absent` refusal explicitly. Both tests also assert no schema reference and no commit row survived. |
| 3 | `DocumentRevisionSchemaEvidenceIT.rawFragmentHashSizeOrdinalAndSupportedPartAreChecked` | still failing, same absent-scope cause | fixture | Same repair: foreign-account and foreign-principal scopes are established, variants 4-6 reach the fence, and a new variant 7 (unscoped account) pins the absent-scope guard. Hash, size, ordinal and sub-key checks (variants 0-3) are unchanged. |
| 4 | `RepositoryCoordinatorHandoffIT.exactRetryAfterSuccessorExpiryDoesNotRenewOrOpenExecution(true)` | still failing, `relation "repository_preparation_history_sets" does not exist` from the V103+ journal writer on a V95 schema | fixture | Initial acquisition now goes through `LegacyPublicationPreparationFixture.acquireInitial`, which seeds the V95 schema with that era's writer (real claim ledger, coordinator binding and preparation row; no history set) and uses the real journal only at V103+. The test then migrates with the real Flyway chain and asserts: schema reached V103+, V103 invented no history set for the legacy preparation, and claim epoch/token/lease, owner binding and the preparation row are byte-identical before and after migration, before the current handoff API is exercised. |
| 5 | `RepositoryCoordinatorSupersessionIT.upgradesExistingReservationsBeforeSupersession(false/true)` | already passing on this base | n/a | Fixed before this branch by `LegacyPublicationPreparationFixture` (commits `e0b2bd178`, `3a4e3da33`), used via `RepositorySuccessorInstallIT.plan`. Verified by execution in runs 01-03. |
| 6 | `RepositorySuccessorActivationIT.exactRetryKeepsLeasesAndRejectsChangedPlan(95/96)` | already passing on this base | n/a | Same; verified by execution. |
| 7 | `RepositorySuccessorInstallIT.migrationChecksAlreadyInstalledModes(false/true)` | already passing on this base | n/a | Same; the inline schema-version switch in `plan` was moved into the shared version-aware fixture so the install and handoff families share one implementation. |
| 8 | `ArchiveDeletionFailureIT.boundReadsUseOriginalBackendIdentityAndNeverTheCurrentDrive` | still failing, `Archive metadata snapshot must identify its containing version` (V102 trigger) | fixture | The test rebuilt version 2 from version 1's manifest and left the frozen metadata snapshot claiming `currentVersion=1`; V102 correctly refuses that. The fixture now captures a snapshot that identifies version 2 (same address, entry UUID and descriptive fields) and asserts the stored version-2 snapshot is exactly version 1's snapshot with only the version identity advanced. The bound read then still proves the ORIGINAL backend generation and profile are resolved and the original bytes are returned after the drive is repointed and after a later provider revision overwrote the key. |

What the repaired tests now prove:

- A publication-codec command row can only be created by typed admission; damaged
  or foreign rows that exist anyway are refused by the restart loader with the
  precise code, after the row demonstrably exists.
- A wrong account, wrong principal or wrong owner generation cannot borrow the
  live owner write fence for schema artifact, asset or evidence references even
  when its own operation legitimately exists; a nonexistent scope is refused earlier.
- A preparation registered at schema V95 survives the real V96 to V111 migration
  with its claim, leases, owner identity and preparation row intact, gains no
  invented history projection, and the current handoff retry contract holds on it.
- An archive version's metadata snapshot is frozen to its own version, and reads
  of retained content bind to the original backend identity, never the current drive.

No production defect was found; every failure was a fixture that no longer
matched a stricter, intentional guard (V79 execution scope, V102 snapshot
identity, V103 history projection) or called a newer writer against an older schema.
