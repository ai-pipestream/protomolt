# Recovery negative-test repair: wrong-credentials and inconsistent-snapshot

2026-10-12/13. Repair of the two red backup-recovery negatives in
`RepositoryBackupRehearsalIT` (`inconsistentCatalogAndObjectSnapshotRefuses`,
`wrongProviderCredentialsRefuse`), assigned to Kimi on branch
`agent/recovery-negative-tests`.

- Base: `8f79765942e03712599f9171968357ec53630428` (Forgejo/GitHub main when the
  worktree was created; unchanged at submission).
- Head: `b60d9f94ccc502813068f82a52ec562e70692df3` (the fix commit; this record
  rides along on the branch tip).
- Prior preserved baseline (read-only, copied with provenance):
  `/work/worktrees/protomolt/schema-payload-separation/docs/evidence/repository/2026-10-12-schema-payload-rehearsal-baseline/`
  documented the identical two failures on base `bed8cf29` and on schema branch
  `38afa609`, 10/12 passed both times.

## Root cause (both failures): an obsolete test expectation, not a production regression

The recovered host asserted the pre-translation contract: a raw
`BlobStoreException` whose message contains `rejected the read`. One day after
the rehearsal landed, commit `e00894b404ae6c886b862710d04a4ce7118b9fa1`
("fix(repo): map historical read provider failures to repository codes",
2026-10-09) deliberately changed the historical-read boundary:

- `repo/engine/.../DocumentHistoricalOperations.java:175-185` — every failure
  raised after capture is classified by `HistoricalReadFailures.translate`
  before delivery reauthorization.
- `repo/engine/.../HistoricalReadFailures.java:26-42` — a `BlobStoreException`
  becomes `RepositoryErrors.historicalProvider(provider)`; the provider failure
  stays on the cause chain.
- `repo/engine/.../RepositoryErrors.java:58-87` — the translation table, with
  the design rule in the javadoc: the caller is already authorized by the
  ledger, so a provider refusal is a host/backend condition — a refused backend
  credential is `FAILED_PRECONDITION`, never the caller's `PERMISSION_DENIED`.
- `repo/engine/.../HistoricalReadFailureTest.java` — the whole table, constant
  messages and cause retention are locked by `HistoricalReadFailureTest`,
  including `mapped.getMessage()` never containing provider detail
  (`s3://`, `secret`, `bucket`).

The two rehearsal checks were written on 2026-10-08 (`bbff47177`,
`dc0056ae4`) and never updated to the typed contract, so they observed the
typed code `FAILED_PRECONDITION` and failed their string assertions. The
earlier reading of that as "pre-provider refusal" was wrong in both cases —
proven below by the retained cause chains.

### wrongProviderCredentialsRefuse (host mode `wrong-credentials`)

Call path, all assertions now green (`fixed-checks.txt`):

1. Direct provider GET with the wrong secret -> S3 `403 SignatureDoesNotMatch`
   (`wrong-credentials.provider_refuses`).
2. Host composition succeeds without a provider call
   (`wrong-credentials.host`) — the credential is only exercised by reads.
3. `readRaw`/`readValidated` for every recorded revision reach the provider and
   fail there. Chain, recorded per revision:

   `RepositoryException(FAILED_PRECONDITION: Original document backend refused the historical read)`
   `<- BlobStoreException(PERMISSION_DENIED: Object provider rejected the read)`
   `<- S3Exception(403 SignatureDoesNotMatch)`

   `S3ObjectReads` (`repo/blob/s3/.../S3ObjectReads.java:52-66`) maps 403 to
   `PERMISSION_DENIED`; `historicalProvider` maps that to `FAILED_PRECONDITION`
   with the constant public message. No alternate credential, fallback store or
   cached success exists in the mode: the host holds only the wrong secret, and
   every revision read refuses. The typed code is `FAILED_PRECONDITION`, not a
   caller-authorization code — backend credentials are not conflated with
   caller authorization.

### inconsistentCatalogAndObjectSnapshotRefuses (host mode `inconsistent-snapshot`)

The driver substitutes run-2's provider archive into a resealed run-1 set, so
preflight passes and the host must refuse. Seed bucket names are qualified per
capture (`RepositoryBackupRehearsalSeedHost.java:40-41,60`: account
`rehearsal-<account>-<suffix>`, bucket = account name), so the substituted
volume has no such namespace. Provider behavior on the pinned image was probed
directly (`provider-probe.txt`): a version-exact GET against a missing bucket
returns `NoSuchBucket` (404), not `NoSuchVersion`.

1. Direct provider GETs of all 3 recorded versions -> `NoSuchBucketException`
   each (`inconsistent-snapshot.provider_versions_absent`).
2. `readRaw`/`readValidated` reach the provider and fail there. Chain:

   `RepositoryException(FAILED_PRECONDITION: Original document backend cannot serve the retained revision)`
   `<- BlobStoreException(FAILED_PRECONDITION: Object provider rejected the read)`
   `<- NoSuchBucketException(404 NoSuchBucket)`

   `S3ObjectReads` maps a 404 that is not `NoSuchVersion` to blob
   `FAILED_PRECONDITION`; `historicalProvider` keeps `FAILED_PRECONDITION` with
   the constant message. Missing exact versions are never served current or
   substitute bytes.
3. Retained catalog evidence stays intact: receipt replay is byte-equal for all
   three revisions (`inconsistent-snapshot.replay.*`) and the pending
   operation's coverage state matches the record
   (`inconsistent-snapshot.catalog_intact`) — these checks were previously
   unreachable behind the failing assertion.

Contrast with the passing `missing-version` mode: there the namespace exists
and one exact version was deleted, so RustFS returns `NoSuchKey`, which
`S3ObjectReads` maps to `BlobNotFoundException` and
`DocumentPartReader.readPart` (`repo/engine/.../DocumentPartReader.java:570-574`)
to `DATA_LOSS`. The mapping difference is the provider's answer, not test
interpretation.

## Fix (test-only; production already behaves correctly)

- `repo/container/src/test/resources/backup-recovery/RepositoryBackupRehearsalRecoveredHost.java`
  - New `providerRefusal(...)` helper: asserts the typed repository code, the
    constant public message, the retained `BlobStoreException` code and
    constant message, and the provider's HTTP status/error code at the root of
    the cause chain.
  - New `chain(...)` helper: renders that causal evidence into the check detail
    (types and codes only for the provider link; no provider messages, so no
    endpoint or credential material reaches the markers).
  - `wrong-credentials` now asserts the typed chain for `readRaw` and
    `readValidated` on every revision (was: raw-only `BlobStoreException` text).
  - `inconsistent-snapshot` asserts the typed chain for both reads and records
    the direct-probe outcome kinds (all `NoSuchBucketException`).
- `repo/container/src/test/resources/backup-recovery/RepositoryBackupRehearsalContentChecks.java`
  - New `thrown(...)` capture helper beside `outcome(...)`.

No production file changed; `HistoricalReadFailureTest` already locks the
translation this repair asserts end-to-end. No sleeps, retries, disabled
assertions, skipped negatives or deadline changes were introduced; the
ordinary-test exclusion and the init-script inclusion are untouched.

## Runs (all under `flock -w 1800 /tmp/protomolt-repository-qualification.lock`)

1. Baseline, base commit, focused 4 tests (run-1, run-2, both negatives):
   2 failures matching the preserved schema-payload baseline byte-for-byte
   (`base-check-failures.txt`, `base-RepositoryBackupRehearsalIT.xml`,
   `base-environment.txt`; run root
   `repo/container/build/backup-rehearsal/2026-10-12T23-57-33.970043737Z`).
2. Fixed, focused 4 tests, same command: 4 passed, 0 failed
   (`fixed-checks.txt`, `fixed-focused-RepositoryBackupRehearsalIT.xml`,
   `fixed-environment.txt`; run root
   `repo/container/build/backup-rehearsal/2026-10-13T00-09-18.837479188Z`).
   Commands:
   `./gradlew -I repo/container/src/test/resources/backup-recovery/rehearsal.init.gradle :protomolt-repo-container:test --tests '*RepositoryBackupRehearsalIT.rehearsalRunOne' --tests '*RepositoryBackupRehearsalIT.rehearsalRunTwo' --tests '*RepositoryBackupRehearsalIT.inconsistentCatalogAndObjectSnapshotRefuses' --tests '*RepositoryBackupRehearsalIT.wrongProviderCredentialsRefuse' --max-workers=2 --console=plain`
3. Fixed, full class (12 tests), `--tests 'ai.protomolt.proto.repo.container.ledger.RepositoryBackupRehearsalIT'`:
   **12 tests, 0 failures, 0 errors, 0 skipped**; all 12 outcome records pass
   (`fixed-full-RepositoryBackupRehearsalIT.xml`, `full-checks-summary.txt`;
   run root `repo/container/build/backup-rehearsal/2026-10-13T00-11-03.743108692Z`).

No infrastructure failures occurred in any run; the only red results ever
observed were the two obsolete assertions. The `missing-version`,
`corrupt-descriptor`, `missing-descriptor`, `wrong-endpoint`, `new-generation`,
`wrong-database-credentials` and `identity-gap` negatives and both positives
were unaffected by the change.

## Limitations

- Evidence covers `RepositoryBackupRehearsalIT` only. The full container suite
  and `admissionStorageTest` were not run; no production path changed, so no
  expansion was warranted.
- The typed assertions pin the pinned RustFS image's answers
  (`NoSuchBucket`/404, `SignatureDoesNotMatch`/403). A provider image change
  that alters those answers would fail the checks with the actual chain
  printed, which is the intended evidence.
