# Archive backup and recovery qualification

Base: `5f8acfb413febd0a3ce1233d06d9a143d1220f86` (main). Branch: `agent/archive-recovery`.
Archived run: commit `6c5cff4275362b3e084c7a1e1951387b943ccfff` (test sources), tree status in `environment.txt`.

Two complete clean-room rehearsals restored a QUIESCED backup of a seeded repository
holding archive entries (PostgreSQL 18 ledger plus a pinned RustFS volume) into fresh
stores and verified it in a fresh production-JAR host, followed by eight negative JUnit
cases (twelve injected scenarios) on disposable copies of run-1. The procedure and the
criterion mapping are in
[docs/testing/archive-backup-qualification.md](../../../testing/archive-backup-qualification.md).
This is backup verification of one deployment shape for archive entries, not production
backup readiness, not pruning and not cross-provider recovery.

## Command and environment

```
flock -w 600 /tmp/protomolt-repository-qualification.lock \
  ./gradlew -I repo/container/src/test/resources/archive-backup-qualification/qualification.init.gradle \
  :protomolt-repo-container:test --tests '*ArchiveBackupQualificationIT' --max-workers=2 --console=plain
```

| Item | Value |
|---|---|
| PostgreSQL | `postgres:18-alpine`, image `sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873` |
| RustFS | `rustfs/rustfs:1.0.0-beta.11-preview.1`, image `sha256:ea50257bc5e281e83170f49b84a109432d930f93bf5051c77d09105c5a62746b` |
| Host JVM | Java 25.0.3, production JARs plus the compiled host classes; `runtime-inventory.tsv` lists every classpath JAR by SHA-256, `host-sources.tsv` the host sources (own and reused), `driver-sources.tsv` the driver sources, reused driver helpers and init script |
| Docker | server 29.8.1, Linux 7.0.0-34-generic amd64 |
| Lock | `lock-wait.txt`: requested 2026-10-09T19:20:57Z, acquired 2026-10-09T19:38:02Z (the first 600 s wait timed out at 19:30:57Z, the second acquired; `lock-waits.txt`), released 2026-10-09T19:39:46Z; lock waiting is recorded separately from the test outcome and is not part of the suite time |
| Host load | `environment.txt` and `lock-wait.txt` carry the load averages; other agents' benchmark series ran on the same host before and after, serialized by the lock; no performance claim is made |

## Result

`gradle-run.log`: BUILD SUCCESSFUL in 1m 43s (suite time 103 s); `junit-results.xml`: 10 tests, 0
failures, 0 errors, 0 skipped.

| Suite | Tests | Failures | Errors | Skipped |
|---|---|---|---|---|
| `ArchiveBackupQualificationIT` (JUnit, driver) | 10 | 0 | 0 | 0 |
| run-1 driver markers (`run-1/markers.log`) | 21 | 0 | 0 | 0 |
| run-1 seed host (`run-1/backup/identities/seed-results.xml`) | 102 | 0 | 0 | 0 |
| run-1 recovered host (`run-1/recovered/results.xml`, READY) | 185 | 0 | 0 | 0 |
| run-2 driver markers | 21 | 0 | 0 | 0 |
| run-2 seed host | 102 | 0 | 0 | 0 |
| run-2 recovered host (READY) | 185 | 0 | 0 | 0 |
| negative recovered hosts (six cases that compose a host) | 117 | 0 | 0 | 0 |
| negative driver markers (eight cases) | 71 | 0 | 0 | 0 |

Every `CHECK_OK` line in a `markers.log` is an asserted fact; a failed check ends the case.
Host logs (`seed-host.log`, `recovered-host.log`), redacted command logs (`commands.log`),
`identities.json` (nonsecret), `manifest.json`, `SEALED`, `schema/catalog.tsv` and
`catalog/fingerprints.json` are archived; the SQL dump, the provider volume archive, the
protobuf records and the Docker logs are not.

## What the rehearsals observed

- Seed through the production host: host-provisioned versioned buckets; alpha
  `report.pdf#2026` v1 to v3 (the `original` object referenced by versions 1, 2 and 3,
  `markdown` replaced in v2 and re-referenced by v3, a 1 MiB `attachment.bin` streamed into
  v3), an elided fourth save; alpha `notes#1` with an EMPTY rendition; beta
  `subject-record` v1 and v2 sharing `original`, then `DeleteRendition(scratch, RTBF)`
  admitted with two targets pending; beta unversioned `draft` whose v1 is gone. Ten
  physical objects, every one LIVE with a provider version id under the run's generation.
- The cut: no client backend, no ACTIVE incarnation (two QUIESCED), no document or archive
  read pin, no staging attempt, no upload outside LIVE; 92 tables fingerprinted; the
  admitted mutation's two targets recorded as durable pending work; `pg_dump` exit 0;
  fingerprints unchanged after the dump; both containers stopped cleanly; volume archived
  and the set sealed (24 components).
- Restore: `pg_restore --exit-on-error` exit 0; next transaction id past the stored
  maximum (0, see the testing note); sequences equal (`archive_mutation_revision_seq` 31);
  V119; all 92 table fingerprints equal; archive state equal (10 uploads, 1 admitted
  mutation, 2 pending targets, 11 version references, 7 versions, 4 entries).
- Recovered host, before opening: every binding, entry row, version row and admission row
  equal to the seed record; all ten objects read by recorded version id with matching
  SHA-256, ETag and size; the production observation of the mutation still ADMITTED with
  two pending targets at status revision 1.
- Recovered host, open: a fresh ACTIVE incarnation pair; every version of every entry
  byte-equal with equal manifest and current metadata; listings and stats equal; the access
  matrix equal (operator OK; member, stranger, outsider PERMISSION_DENIED on reads, writes
  and mutations; cross-account address NOT_FOUND); transport UNAUTHENTICATED without the
  token and equal to the record with it; the pending mutation COMPLETED through the
  recovery lane (status revision 2, two confirmed absent, both targets DELETED after one
  attempt, no version left under their keys, the shared `original` still LIVE and read at
  both versions); the elided save and the mutation replay return the recorded outcomes.
- Fresh write: version 4 with a new `markdown`, `original` and `attachment.bin`
  re-referenced by their recorded bindings, sequence and entry revision past the restored
  values, versions 1 to 3 re-read. Lifecycle: `DeleteRendition(markdown, RETENTION)`
  tombstoned four versions and targeted three objects; cleanup COMPLETED (status revision
  3); each version's manifest equals the recorded manifest with the tombstone applied; the
  surviving renditions read byte-equal; `original` (versions 1 to 4) and `attachment.bin`
  (versions 3 and 4) stay LIVE on the provider; alpha counters: 2 entries, 5 versions,
  retained bytes equal current bytes equal `original` plus `attachment.bin` plus the notes
  body. READY written last.
- Source provider volume digest and every file of the sealed set unchanged afterwards.

## Negative cases

| Case | Injection | Observed |
|---|---|---|
| mixed-captures, missing-component, corrupt-checksum, unsealed | copies of run-1's set damaged | preflight refused before any resource; message names the cause |
| nonempty-target | existing restore directory | refused; file intact |
| inconsistent-snapshot | run-2's provider archive in run-1's set, resealed | all 10 recorded versions absent (NoSuchBucket); every `GetEntry` FAILED_PRECONDITION with `BlobStoreException(FAILED_PRECONDITION) <- NoSuchBucketException`; listings and stats read; the mutation replays; cleanup RETRY_REQUIRED with `BACKEND_RECLAMATION_FAILED`, targets DELETING; no READY |
| missing-version | newer version put under version 1's `markdown` key, recorded version deleted | `GetEntry(v1)` DATA_LOSS via `BlobNotFoundException <- NoSuchKeyException`; manifest intact; `original` of v1 and versions 2 and 3 byte-equal; the newer version never substituted |
| corrupt-payload | one byte flipped at offset 4096 of `attachment.bin`'s `part.1` on the extracted restored volume (file digest recorded before and after) | the provider aborts the read of the recorded version (SDK gives up after four attempts); `GetEntry(v3)` and the single-rendition read UNAVAILABLE via `BlobStoreException(UNAVAILABLE) <- IOException`; manifest intact; `original` and `markdown` of v3 and versions 1 and 2 byte-equal |
| wrong-endpoint | recorded generation, different endpoint | `Managed backend generation is already bound to another physical profile`; host never composed |
| new-generation | new generation at the recorded endpoint | host composes; every bound read `IllegalStateException: Original archive backend is not configured on this host`; manifests read; cleanup RETRY_REQUIRED with `BACKEND_RECLAMATION_FAILED`; all 10 objects still on the provider |
| wrong-credentials | wrong provider secret | direct GET 403 SignatureDoesNotMatch; host composes; every bound read PERMISSION_DENIED via `BlobStoreException(PERMISSION_DENIED) <- S3Exception`; cleanup RETRY_REQUIRED; all 10 objects intact |
| wrong-database-credentials | wrong ledger password | host exit 1 on `password authentication failed` before any check (`recovered/results.xml` contains zero cases); no READY |

Injections are explicit catalog, set or provider damage outside the supported writers,
applied only to disposable copies; nothing substitutes, falls back or advertises readiness.

## Earlier attempts on the same sources

Kept for the record (`attempts/`): attempt 1 and 2 failed to compile (a shell escape in the
injection script; the reused ledger helper needs the document fixture compiled with it);
attempt 3 failed in the seed host because the replay check read a record written later;
attempt 4 failed in the recovered host because the entry rows were recorded before the
elided save revised them (a recording-order mistake, not a restore difference: the
driver's table fingerprints were equal); attempt 5 and the first wait of attempt 7 timed
out on the shared lock (600 s) while other agents' runs held it; attempt 6 passed 9 of 10
with corrupt-payload expecting DATA_LOSS where the provider aborts the read (UNAVAILABLE);
attempt 7 passed 10 of 10 on uncommitted sources; attempt 8 is the archived run on the
committed sources. No timeout was raised and no expectation was loosened: the
corrupt-payload expectation was replaced by the observed exact classification with its
provider cause required.

## Affected archive suites

Run on the same tree (commit `6c5cff427`) under the shared lock, lock acquired without
waiting at 2026-10-09T19:40:26Z, BUILD SUCCESSFUL in 7m 29s
(`affected-archive-suites/gradle-run.log`, `results.tsv`, `lock-wait.txt`):

```
flock -w 600 /tmp/protomolt-repository-qualification.lock \
  ./gradlew :protomolt-repo-container:test --tests '*Archive*' \
  :protomolt-repo-service:test --tests '*Archive*' --max-workers=2 --console=plain
```

37 classes, 260 tests, 0 failures, 0 errors, 11 skipped. The 11 skips are two
pre-existing gates, both named in `results.tsv`: `ArchiveBackupQualificationIT` (10 cases)
matches `*Archive*` and reports itself skipped because this command does not apply the
init script, which is the documented opt-in behavior of this suite; and
`ArchiveReadBenchmarkIT` (1 case) runs only with `PROTOMOLT_ARCHIVE_READ_BENCHMARK=true`.
The other 36 classes (249 tests) passed with no skip: every archive ledger, upload,
mutation, cleanup, reader, retention, bridge, catalog, classification, service, bounded and
Redis archive suite in `protomolt-repo-container` and `protomolt-repo-service`.

## Gaps

- Archive ACLs do not exist; the matrix pins the process-authority contract.
- No `xid8` value is stored by archive tables; the counter check is trivial here.
- The ordinary `test` task reports the class skipped; excluding it needs a
  `build.gradle` line (shared file, requested from the coordinator).
- Not covered: Redis profiles, LocalStack, online capture, provider identity remapping,
  hosted CI, performance, `PruneVersions`.
