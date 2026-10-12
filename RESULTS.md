# RESULTS: schema payload separation (V120)

## Base / head / branch

- Base: bed8cf29de6bad75dea11372e1b7efedf35af943 (Forgejo/GitHub main at assignment creation)
- Branch: agent/schema-payload-separation
- Implementation head: eee885bec5474574adaa3713fb1b2ae56e3ba81c
- Repair head (staging no-recreate fix + regression + rehearsal evidence):
  38afa609477c318e17d4d4095d88f7b4ddfc5193
  (this file is committed on top; see branch tip)
- Migration reserved: V120 (main tip is V119; PR308 untouched)

## Changed behavior

Descriptor bytes move from the `repository_schema_artifacts` identity catalog into
a normalized `repository_schema_artifact_payloads` relation keyed by the existing
`(account_id, artifact_sha256)` identity. Catalog identities, staging claims, and
V55/V56/V57 associations are preserved. Payload rows are immutable: no delete
permission, no reclamation, no pruning. A missing retained payload is an explicit
error, never an empty schema or a registry substitution.

SQL (V120 `repository_schema_artifact_payloads`):
- New payload table with PK `(account_id, artifact_sha256)`, `payload_bytes`,
  generated `size_bytes` (1..16 MiB), digest CHECK, FK to the catalog.
- Backfill from catalog bytes; per-row CHECKs verify digest/size, count plus
  identity checks prove completeness. Redundant catalog columns dropped only after.
- INSERT-only payload guard plus a deferred catalog trigger requiring the payload.
- `stage_repository_schema_artifact`, the claim guard, the V57 asset guard, and
  the V67 assessment-completeness check read the payload relation with identical
  limits (64 artifacts, 64 MiB batch, digest verification, collision refusal).
- The claim guard's missing-payload failure is now explicit
  (`Schema artifact payload is missing`) instead of a bare strict-select error.
- Unchanged: claim release, V55/V67 guards, V60 manifest, V68/V69 release
  (identity locks and digest-only reads; verified to hold no byte/size use).

Java: staging, retained reads, historical schema rows, assessment replay, retained
evidence, assessment creation, and publication artifact locking read the payload
relation. No Java signature, protobuf, endpoint, or provider changes.

Tests and fixtures: old-target fixture seeding keeps a pre-V120 lock probe for the
V61/V87 upgrade tests; the in-container rehearsal's pinned migration level moves
119 -> 120. New coverage: populated V119->V120 upgrade, payload immutability,
explicit missing-payload errors, concurrent and lock-timeout-forced identical
staging, and repeated-startup idempotence.

## Costs: migration vs steady state

- Migration (one time, inside the Flyway transaction at startup): one full
  backfill of catalog bytes with per-row digest/size CHECKs plus count/identity
  verification. The migration takes SHARE ROW EXCLUSIVE on the catalog and claims
  during the backfill, and DROP COLUMN takes ACCESS EXCLUSIVE on the catalog until
  transaction end: readers and writers on those tables block for the whole upgrade
  transaction. Plan a quiesced upgrade window. Old binaries must not run against
  the migrated schema (they query the dropped columns); upgrade code and schema
  together, with no rolling mix. Cost is linear in retained descriptor bytes.
- Steady state: one PK join to the payload row per size probe, lock probe, or
  byte fetch; same row counts, same bounds, same digest order. No new I/O under
  SQL locks and no account-global serialization.

## Review findings

- Coordinator review of PR 310 found that staging recreated a missing retained
  payload after a catalog conflict. Repaired in the stage function: the catalog
  INSERT reports whether this call created the identity
  (`ON CONFLICT DO NOTHING RETURNING`); only the creator writes the payload row,
  while an existing identity requires and verifies its retained payload and
  otherwise refuses with `Schema artifact payload is missing` before any claim
  write. Same-owner retry and different-owner staging of a corrupted identity
  both refuse; duplicate and concurrent identical staging semantics and lock
  order are unchanged. Covered by
  `stagingMustNotSilentlyRepairMissingRetainedPayload` (proven red, then green).
- The deferred `repository_schema_payload_required` trigger cannot fail first
  through any supported path (the claim guard checks payload existence at claim
  time and holds KEY SHARE to commit); it stays as the table-level invariant for
  the catalog=>payload edge.
- `readRetained` distinguishes a missing claim/owner (`unavailable`) from a
  present identity with lost bytes (`Retained schema payload is missing`).
- Lock order preserved: catalog, payload, then claims in caller digest order.
- Intermediate failures during development were all in new/updated test code
  (assertion shape, an old-target probe, a pinned count) and were fixed in the
  tests or made explicit in SQL; no production expectation was weakened.

## Commands and exit codes

All under `flock -w 1800 /tmp/protomolt-repository-qualification.lock` with
`--max-workers=2 --console=plain`:

1. `./gradlew :protomolt-repo-container:test --tests RepositorySchemaArtifactsIT
   --tests DocumentRevisionSchemaArtifactsIT` — exit 0, BUILD SUCCESSFUL, 36/36.
2. Same task for 9 schema/history/assessment classes — exit 0, 59/59.
3. Same task for 13 commit/drain/historical/typed classes — exit 0, 88/88.
4. Full `:protomolt-repo-container:test` — exit 0, BUILD SUCCESSFUL in 29m19s,
   252 classes, 2216 tests, 0 failures/errors/skips.
5. In-container rehearsal (`-I .../rehearsal.init.gradle ... --tests
   RepositoryBackupRehearsalIT`) — exit 1, 10/12; the 2 failures
   (`inconsistentCatalogAndObjectSnapshotRefuses`, `wrongProviderCredentialsRefuse`)
   fail identically on the untouched base (see limitations).
6. Base comparison of (5) in an isolated worktree at bed8cf29 — exit 1, same
   10/12 with the same 2 failures and identical CHECK_FAILED signatures.
7. Standalone `verification/repository-recovery/run.sh --out <empty> --runs 1
   --skip-negatives --xid-burn 0` — exit 0, REPOSITORY_RECOVERY_REHEARSAL_OK,
   restore at V120 with `restore.materialized.offline_decode` OK.
8. `:protomolt-repo-container:check -x :protomolt-repo-container:admissionStorageTest`
   — exit 0, BUILD SUCCESSFUL.
9. `compileJava` in `verification/repository-recovery` — exit 0.
10. Repair verification, same lock/workers: new
    `stagingMustNotSilentlyRepairMissingRetainedPayload` alone against the
    unfixed function — exit 1, red as required.
11. Focused suites after the repair (`RepositorySchemaArtifactsIT`,
    `DocumentRevisionSchemaArtifactsIT`) — exit 0, 37/37.
12. All 22 affected schema/history/assessment/commit/drain/typed classes after
    the repair — exit 0, 147/147.
13. Full `:protomolt-repo-container:test` after the repair — exit 0, BUILD
    SUCCESSFUL in 29m18s, 252 classes, 2217 tests, 0 failures/errors/skips.
14. In-container rehearsal on the repaired head — exit 1, 10/12 with the same
    2 pre-existing failures.
15. Base comparison rerun in an isolated worktree at bed8cf29 — exit 1, 10/12
    with the same 2 failures and byte-identical check signatures; raw XML and
    signatures preserved in
    `docs/evidence/repository/2026-10-12-schema-payload-rehearsal-baseline/`.
16. Standalone scoped rehearsal on the repaired head
    (`--runs 1 --skip-negatives --xid-burn 0`) — exit 0,
    REPOSITORY_RECOVERY_REHEARSAL_OK.

## Test counts / failures / skips

- Focused new/updated suites after repair: 37 + 147 = 184 tests, 0 failures,
  0 skips (plus the earlier 183-test pre-repair baseline, superseded).
- Full container module after repair: 2217 tests, 0 failures, 0 errors, 0 skips.
- In-container backup rehearsal: 10/12 on branch and on base (2 pre-existing
  failures with identical signatures; root cause not established).
- Standalone recovery rehearsal (scoped): pass on the repaired head.

## Real-provider evidence

- All suites run against real PostgreSQL 18 (Testcontainers `postgres:18-alpine`).
- Both rehearsals restore a populated backup into an isolated PostgreSQL
  instance; the standalone run proves retained descriptors still decode
  (`restore.materialized.offline_decode`, catalog digests=4) on the new layout.

## Remaining limitations

- Two in-container rehearsal negatives (`inconsistentCatalogAndObjectSnapshotRefuses`,
  `wrongProviderCredentialsRefuse`) fail identically on the untouched base and on
  this branch: the recovered host expects provider-level refusal but observes
  FAILED_PRECONDITION before provider access. Pre-existing and outside schema
  storage; root cause is not established, so they are not classified as merely
  environmental. Raw comparison evidence (base/head SHAs, commands, failure
  signatures) is preserved in
  `docs/evidence/repository/2026-10-12-schema-payload-rehearsal-baseline/`.
  Follow-up ownership: coordinator to triage/assign; this assignment makes no
  change in that area.
- The standalone `--skip-negatives` run is scoped evidence for the new layout,
  not a complete recovery gate. The in-container corrupt/missing-descriptor
  negatives cover the new layout and pass; full standalone negatives remain for
  the recovery workstream.

## PR URL

https://git.rokkon.com/ai-pipestream/protomolt/pulls/310

## Local / branch / CI / merge

- Local: green except the 2 pre-existing rehearsal negatives (proven on base).
- Published branch: agent/schema-payload-separation on origin, then github.
- Hosted CI: coordinator-owned (not run from here).
- Merge: coordinator-owned (not merging).
