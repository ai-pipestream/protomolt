# RESULTS: schema payload separation (V120)

## Base / head / branch

- Base: bed8cf29de6bad75dea11372e1b7efedf35af943 (Forgejo/GitHub main at assignment creation)
- Branch: agent/schema-payload-separation
- Implementation head: eee885bec5474574adaa3713fb1b2ae56e3ba81c
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
  verification; the migration takes SHARE ROW EXCLUSIVE on the catalog and claims
  (same berth as V55), so concurrent staging waits for the upgrade, not for other
  readers. Cost is linear in retained descriptor bytes.
- Steady state: one PK join to the payload row per size probe, lock probe, or
  byte fetch; same row counts, same bounds, same digest order. No new I/O under
  SQL locks and no account-global serialization.

## Review findings

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

## Test counts / failures / skips

- Focused new/updated suites: 36 + 59 + 88 = 183 tests, 0 failures, 0 skips.
- Full container module: 2216 tests, 0 failures, 0 errors, 0 skips.
- In-container backup rehearsal: 10/12 (2 pre-existing environmental failures,
  proven identical on base).
- Standalone recovery rehearsal (scoped): pass.

## Real-provider evidence

- All suites run against real PostgreSQL 18 (Testcontainers `postgres:18-alpine`).
- Both rehearsals restore a populated backup into an isolated PostgreSQL
  instance; the standalone run proves retained descriptors still decode
  (`restore.materialized.offline_decode`, catalog digests=4) on the new layout.

## Remaining limitations

- The two rehearsal negatives above fail identically on base and branch in this
  environment (recovered host expects provider-level refusal, observes
  FAILED_PRECONDITION before provider access). Pre-existing, outside schema
  storage, needs owner triage in another workstream.
- Standalone rehearsal negatives were skipped (`--skip-negatives`) as the scoped
  rehearsal; the in-container corrupt/missing-descriptor negatives cover the new
  layout and pass.

## PR URL

https://git.rokkon.com/ai-pipestream/protomolt/pulls/310

## Local / branch / CI / merge

- Local: green except the 2 pre-existing rehearsal negatives (proven on base).
- Published branch: agent/schema-payload-separation on origin, then github.
- Hosted CI: coordinator-owned (not run from here).
- Merge: coordinator-owned (not merging).
