# Isolated repository backup and recovery rehearsal

Base: `4f4d66bffe0025783df178b82a35212424faabf6` (`agent/historical-cleanup-fairness`, V119).
Branch: `agent/repository-backup-rehearsal`. Date: 2026-10-08. Archived run: commit
`f698103f1` with a clean tree (`environment.txt`, `driver-sources.tsv`, `host-sources.tsv`).

Two complete clean-room rehearsals restored a QUIESCED backup of a seeded repository
(PostgreSQL 18 ledger plus a pinned RustFS volume) into fresh stores and verified it in a
fresh production-JAR host with no schema registry, followed by ten negative JUnit cases
(fourteen injected scenarios) on disposable copies. The procedure is documented in
[operations/repository-backup-recovery.md](../../../operations/repository-backup-recovery.md).
This is backup verification of one deployment shape, not production backup readiness,
not pruning, and not cross-provider disaster recovery.

## Provenance

GitHub PR #413 (`agent/repository-backup-restore`, head `610eb15c2`, base
`refactor/repository-composition`, still a draft, 136 commits behind this base, predating
V112 through V119) holds an earlier offline rehearsal as a separate Gradle project. Its
Docker, backup-set and content-check approach was reused by re-implementation inside the
ledger test package with that provenance noted in each file header; nothing was
cherry-picked or merged. New here: a second account, a nested Any attachment type in a
separate descriptor file, a journaled pending operation with V118 certificate, V119 lineage
and unresolved states, per-table catalog fingerprints, the V118/V119 invariant comparison,
and the inconsistent-snapshot, missing-descriptor, wrong-credentials and identity-gap cases.

## Command and environment

```
flock -w 600 /tmp/protomolt-repository-qualification.lock \
  ./gradlew -I repo/container/src/test/resources/backup-recovery/rehearsal.init.gradle \
  :protomolt-repo-container:test --tests '*RepositoryBackupRehearsal*' --max-workers=2 --console=plain
```

| Item | Value |
|---|---|
| PostgreSQL | `postgres:18-alpine`, image `sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873` |
| RustFS | `rustfs/rustfs:1.0.0-beta.11-preview.1`, image `sha256:ea50257bc5e281e83170f49b84a109432d930f93bf5051c77d09105c5a62746b` |
| Host JVM | Java 25.0.3, production JARs plus the compiled host classes; `runtime-inventory.tsv` lists every classpath JAR by SHA-256, `host-sources.tsv` the host sources |
| Docker | server 29.8.1, Linux 7.0.0-34-generic amd64 |
| Host load | an unrelated RustFS scaling benchmark series (another agent) ran before and after this run on the same host; the shared advisory lock serialized the runs; load averages are in `environment.txt` |

## Result

Final run (`gradle-run.log`, `junit-results.xml`): lock acquired 2026-10-08T12:22:18Z,
BUILD SUCCESSFUL in 2m23s; suite time 140.7s.

| Suite | Tests | Failures | Errors | Skipped |
|---|---|---|---|---|
| `RepositoryBackupRehearsalIT` (JUnit, driver) | 12 | 0 | 0 | 0 |
| run-1 driver markers (`run-1/markers.log`) | 21 | 0 | 0 | 0 |
| run-1 seed host (`run-1/backup/identities/seed-results.xml`) | 92 | 0 | 0 | 0 |
| run-1 recovered host (`run-1/recovered/results.xml`, READY) | 137 | 0 | 0 | 0 |
| run-2 driver markers | 21 | 0 | 0 | 0 |
| run-2 seed host | 92 | 0 | 0 | 0 |
| run-2 recovered host (READY) | 137 | 0 | 0 | 0 |
| negative recovered hosts (7 cases that compose a host) | 73 | 0 | 0 | 0 |

Driver markers per case are in each `markers.log`; every `CHECK_OK` line is an asserted
fact, and a failed check ends the case. Host logs (`seed-host.log`, `recovered-host.log`)
and redacted command logs (`commands.log`) are archived; the SQL dump, the provider volume
archive, the protobuf receipts and Docker logs are not.

Earlier attempts on the same sources, kept for the record: attempt 1 (11:30Z) failed in
the driver's expectation that one V118 certificate exists, when every managed publication
journals its own (four exist); attempt 2 (11:33Z) passed 9 of 12, failing on the
mixed-capture refusal text (size differs before the digest is compared) and on the two
descriptor cases, whose observed classifications are recorded in the findings below;
attempt 3 (11:41Z) passed 11 of 12, failing only the corrupt-descriptor root expectation;
attempt 4 (11:47Z, commit `bbff47177`) passed 12 of 12 and was reviewed by Sol; attempt 5
(12:17Z) failed in the driver on renamed invariant keys after the review fixes; attempt 6
(12:22Z, commit `f698103f1`) is the archived run. No timeout was raised. Three early
expectations were replaced by the observed behavior (the certificate count, the mixed-set
refusal text, the per-occurrence materialization shape) and the DataLoss acceptance in the
corrupt-descriptor case records a classification difference (finding 1) rather than hiding
it; after the review the negative cases require the observed outcome classes.

## Consistency boundary

QUIESCED, never online. After the seed host exited with code 0, `pg_stat_activity`
showed no client backend other than the capture connection, no ACTIVE reader incarnation
(both seed incarnations QUIESCED) and no read pin. The pending operation's claims,
preparations and expired leases are durable journal rows, kept as data. Every base table
(92) was fingerprinted (count plus an order-independent digest of all row text), then
`pg_dump --format=custom` ran, both containers were stopped cleanly, the RustFS volume was
archived from its stopped state, and the set was sealed.

## Positive rehearsal observations (run-1 and run-2)

- Seeded through the production host: two accounts, versioned buckets, typed policies,
  `alpha-v1` (public read), `alpha-v2` (public grant revoked), `beta-v1`; each revision
  retains a root `Record` occurrence (closure of 6 files including `timestamp.proto`) and a
  nested `Attachment` occurrence (3-step path, separate 1-file artifact).
- Pending operation: journal generation 0 certified atomically (V118, zero historical
  roots), graceful handoff installing generation 1 (verified by V119 lineage), supersession
  installing generation 2 (left unresolved); no execution, no success row; exact replay
  observes PENDING.
- Restore: `pg_restore --exit-on-error` exit 0; next transaction id above the largest stored
  `xid8` in both runs (a cluster not past the stored values is refused, see finding 6);
  sequences equal to the manifest; Flyway V119;
  every one of the 92 tables' row fingerprints equal to the source capture, so V118
  certificates/unresolved rows and V119 lineage rows and V93 install edges (including
  `install_xid`) came back verbatim; V118/V119 invariants equal to the source.
- Recovered host (fresh JVM, no registry): row counts, incarnation states, backend
  profile, sequences, catalog digests equal the seed record; raw fragments and validated
  documents byte-equal for all three revisions with recorded command, policy, metadata and
  manifest digests; root and nested occurrences decoded with `ClosedDescriptorSet` from the
  retained artifacts with the recorded digests and tool identity; all provider objects read
  by bucket, key and recorded version id with matching SHA-256, ETag and size; all three
  receipts replay byte-equal; access matrix unchanged (member OK; same-account stranger,
  foreign account and the other account's member NOT_FOUND on every revision); transport
  refuses without the operator token; zero live schema resolutions during verification.
- Pending operation after restore: chain rows equal the seed record; replay still PENDING;
  recovery discovery observes INSTALLED_NOT_ACTIVATED; generation 2 then verifies against
  the restored install edge (V119 depth 2, no header, no execution); a third successor is
  reserved and installed through the reserved-predecessor loader without execution.
- Fresh write: a typed publication with no registry is refused; a registry rebuilt from the
  retained catalog bytes (both artifact digests equal) admits `alpha-v3` with mutation 4
  above the restored sequence 3; old bindings unchanged; every old revision re-reads.
- Source provider volume digest and every file of the sealed set (fingerprinted right after
  sealing) unchanged afterwards.

## Negative cases

| Case | Injection | Observed |
|---|---|---|
| missing-component, corrupt-checksum, unsealed | copy of run-1's set damaged | preflight refused before any resource; message names the cause |
| mixed-captures | run-2's provider archive placed in run-1's set | preflight refused (size differs from the manifest) |
| nonempty-target | existing restore directory; occupied volume | refused; file intact; volume reported occupied |
| inconsistent-snapshot | run-2's provider archive in run-1's set, resealed | all 3 recorded versions absent; raw and validated reads refuse (`BlobStoreException: Object provider rejected the read`); receipts and the pending chain still replay from the catalog; no READY |
| missing-version | newer version put under the key, recorded version deleted | raw, validated, root and nested materialized reads of `alpha-v1` DATA_LOSS; the newer version never substituted; `alpha-v2` and `beta-v1` byte-equal |
| corrupt-descriptor | attachment artifact bytes damaged (V48 digest CHECK dropped on the copy), live registry with the correct definition armed | raw read byte-equal; validated read refuses with the admission module's `DataLoss` type (see gap below); nested materialization DATA_LOSS; the intact root still decodes its own retained definition; live registry never consulted |
| missing-descriptor | attachment artifact rows deleted with referential triggers disabled, live registry armed | raw byte-equal; validated DATA_LOSS; root and nested materialization DATA_LOSS; live registry never consulted |
| wrong-endpoint | recorded generation, different endpoint | `Managed backend generation is already bound to another physical profile`; host never composed |
| new-generation | new generation at the recorded endpoint | host composes; every history read FAILED_PRECONDITION |
| wrong-credentials | wrong provider secret | direct GET 403 signature mismatch; the host composes (no provider call at composition); every history read `BlobStoreException: Object provider rejected the read` |
| wrong-database-credentials | wrong ledger password | host exit 1 on `password authentication failed`; no READY |
| identity-gap | the recorded version read from a restored volume (digest equal) and put into a fresh provider | same digest, different provider-issued version id; GET by the recorded version id on the fresh provider fails |

Injections against the restored copies are explicit catalog or provider damage outside the
supported writers; no production fallback exists for them.

## Findings and gaps

1. **Classification gap (production, reported, not fixed here).** A retained artifact
   whose bytes no longer match its digest makes the validated historical read throw
   `ai.protomolt.proto.repo.admission.DocumentRetainedSchemaAssets.DataLoss`
   (package-private, extends `IllegalStateException`) instead of
   `RepositoryException(DATA_LOSS)`. `DocumentHistoricalSchemas.check` maps
   `InvalidProtocolBufferException | IllegalArgumentException` to DATA_LOSS and rethrows
   other runtime exceptions unchanged. A missing artifact and the materialized reads are
   classified correctly. Reproducer: the `corrupt-descriptor` case
   (`recovered/markers.log`, `corrupt-descriptor.validated_classification.*`). Proposed
   fix: expose the admission data-loss type (or translate it inside
   `DocumentSchemaAdmission.check`) and map it in `DocumentHistoricalSchemas.check`
   exactly as `DocumentHistoricalMaterializer` maps
   `DocumentSchemaMaterialization.DataLoss`. Sol's review refines the location: add a
   public typed data-loss failure at the admission boundary and map it in
   `DocumentHistoricalSchemas`, rather than exposing the package-private internal type.
   Refusal is explicit either way; only the code differs. Production files were not edited.
2. **Two refusal shapes for damaged closures.** A *missing* artifact row makes the
   revision's retained snapshot incomplete, so every typed read of the revision refuses,
   including the root occurrence whose own artifact is intact. *Corrupt* bytes are detected
   only where they are used: the validated read and the nested occurrence refuse, while the
   intact root still materializes its own retained definition. Both are explicit and neither
   consults the armed live registry. The harness records each shape as observed; an earlier
   draft assumed one shape for both and was corrected after the first runs.
3. **Provider identity is not portable.** Version ids are provider-issued; the volume
   snapshot preserves them, an object copy does not (`identity-gap`). Remapping provider
   identities behind receipts has no protocol and was not invented here.
4. **Provider refusals surface as `BlobStoreException`** from raw history reads rather
   than a `RepositoryException` code (inconsistent-snapshot, wrong-credentials). Explicit,
   but a second classification observation for the coordinator; Sol's review places the
   translation at the engine/SPI boundary in `DocumentHistoricalOperations` through the
   existing `RepositoryErrors` mapping, not in the S3 implementation.
5. **Not covered:** archive entries, Redis profiles, LocalStack, online capture, hosted CI,
   performance. The init script is required because the plain `test` task has no admission
   bundle; without it JUnit disables the class with the stated reason and Gradle reports
   all 12 cases as skipped with a green build (checked 2026-10-08T11:51Z). A plain green
   run is therefore not rehearsal evidence; only a run with the init script is. This is
   opt-in evidence, not continuing CI coverage.
6. **Transaction counter advancement is refused, not automated.** A restored cluster
   whose next transaction id is not past every stored `xid8` would let a new transaction
   equal a restored `install_xid`; the restore stops there with an explicit message. The
   first draft automated `pg_resetwal`; neither archived run needed it, so that path was
   never exercised and was removed rather than shipped unqualified. The manual procedure
   follows the PostgreSQL `pg_resetwal` documentation and remains outside this runbook.

## Review

Sol (Codex `gpt-5.6-sol`, read-only over the checkout) reviewed commit `bbff47177`
and returned three blockers, five should-fix items and two notes. All were applied in
the follow-up commit before the archived run: the staging-attempt count is asserted and
the backend check plus every table fingerprint are repeated after the dump; the
unexercised `pg_resetwal` path is replaced by a refusal; command results are redacted when
created so a failed `docker run` cannot place a password or secret key in a failure
message, JUnit output or `summary.json`; the identity-gap case reads the recorded
version's bytes from a restored volume, checks their digest and copies those bytes; the
V118/V119 invariants compare digests, nonces and counts across certificate, header,
preparation, install edge, anchor and predecessor and forbid the certificate-and-lineage
overlap; the negative cases require the observed outcome classes (FAILED_PRECONDITION,
DATA_LOSS, `BlobStoreException` rejection, NoSuchKey/NoSuchVersion) instead of any
failure; the untouched-backup proof compares against a fingerprint taken right after
sealing; `driver-sources.tsv` binds the evidence to the driver sources and init script;
the case counts and the "never silently passes" wording were corrected. Sol's placement
advice for the two classification observations is recorded in findings 1 and 4. The
earlier verdict ("not ready for coordinator integration") predates these changes; the
coordinator decides on the archived run below.

Independent verification of the reviewed tree (2026-10-08T12:34Z, same host, Kimi agent):
the documented command was rerun end to end on the reviewed sources — BUILD SUCCESSFUL in
2m22s, 12 tests, 0 failures, 0 errors, 0 skipped, all twelve outcomes `passed: true`
(`verification-2026-10-08T1240Z/summary.json`, `junit-results.xml`, `environment.txt`,
`driver-sources.tsv`). This is a third complete execution of both clean-room rehearsals,
not a replay of archived results. The same session removed six unused duplicate driver
sources under `backup-recovery/driver/ai/` (an abandoned parallel draft swept into
`bbff47177` by an overlapping editor; never listed in
`RepositoryBackupRehearsalProbeCompiler.SOURCES`, never compiled, never referenced); the
verification run above was made after that removal.
