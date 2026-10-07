# Offline repository backup and recovery rehearsal

Qualification of the procedure in
[repository-backup-recovery.md](../../../design/repository-backup-recovery.md)
and the operator guide
[operations/repository-backup-recovery.md](../../../operations/repository-backup-recovery.md),
run by `verification/repository-recovery/run.sh` on 2026-10-07 against the
candidate base `ad28648c8a522b826fd9828fb05278e1de926415` plus this branch.

Command (one invocation, two fresh positive runs, then every negative case):

```
verification/repository-recovery/run.sh --out /tmp/repository-recovery-<ts>
```

The output tree was archived here with `verification/repository-recovery/archive-evidence.sh`.
Backup payloads (SQL dump, provider volume archive, protobuf receipts) and
Docker logs are not archived; every identity they carry is in
`identities.json`, `manifest.json` and the markers.

## Resources

| Resource | Pin |
|---|---|
| PostgreSQL | `postgres:18-alpine`, image `sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873` |
| RustFS | `rustfs/rustfs:1.0.0-beta.11-preview.1`, image `sha256:ea50257bc5e281e83170f49b84a109432d930f93bf5051c77d09105c5a62746b` |
| Helper | `alpine:3.20` (tar in and out of volumes) |
| Host JVM | Java 25, production JARs only; `runtime-inventory.tsv` lists every classpath JAR by SHA-256 |
| Admission bundle | `repo/container/build/admission-transport-runtime` built by the root build |

Each run used its own Docker network, volumes, containers, database
credentials, provider keys and operator token. Secrets were passed only through
the environment and are redacted in `commands.log`.

## Positive runs

Both runs executed the same sequence and passed every check:

| Phase | Checks | run-1 | run-2 |
|---|---|---|---|
| Seed host (live, registry closed before historical reads) | 55 | pass | pass |
| Harness (capture, source removal, restore, untouched proofs) | 17 | pass | pass |
| Recovered host (fresh JVM, no registry, READY last) | 80 | pass | pass |

Observations common to both runs (`run-*/markers.log`, `run-*/recovered/markers.log`):

- Quiescence: after the seed host exited, `pg_stat_activity` showed no client
  backend other than the capture connection; `pg_dump` exit 0; both containers
  reported `exited` before the provider volume was archived.
- Source unreachable before restore: containers removed, JDBC connection
  refused, recorded provider port free.
- Restore into a new database and a new provider volume at the recorded
  endpoint origin; `pg_restore --exit-on-error` exit 0; next transaction id
  1580 above the largest stored `xid8` 1012, so no `pg_resetwal` advancement
  was needed in either run; sequences equal to the manifest
  (`document_mutation_revision_seq=2`, `archive_mutation_revision_seq=8`);
  Flyway level V111; identity-table row counts equal to the manifest.
- Restored reader incarnations were all QUIESCED; the recovered host registered
  its own new ACTIVE incarnations and revived none.
- Raw fragments and validated documents of both revisions byte-equal to the
  seed record, with recorded command, policy, metadata and manifest digests.
- Materialized typed occurrence decoded with `ClosedDescriptorSet` from the
  retained descriptor artifact (five files including `google/protobuf/timestamp.proto`),
  artifact digest equal to the recorded one, admission tool identity preserved.
- Archive versions 1 and 2 byte-equal with frozen metadata snapshots, shared
  `original` object key across versions, deduplicated save still deduplicated.
- All six recorded provider objects read directly by bucket, key and recorded
  version id with matching SHA-256, ETag and size.
- Receipt replay of both recorded publication requests byte-equal.
- Access matrix unchanged: member reads both revisions; a same-account caller
  without the granted identity and a foreign-account caller get NOT_FOUND for
  both revisions, because revision 2 revoked the public grant; the transport
  refuses calls without the operator token (UNAUTHENTICATED).
- Live schema resolution attempts during historical verification: 0.
- Fresh writes: a typed publication with no registry was refused; a registry
  rebuilt from the retained catalog bytes (same artifact digest) admitted
  revision 3 with mutation revision 3 above the restored sequence value 2; the
  archive committed version 3; sequences advanced; restored bindings unchanged;
  every old revision and version re-read byte-equal afterwards.
- After verification the source provider volume digest and every backup
  component digest were unchanged.

## Negative cases

Each case restored a disposable copy of run-1's sealed backup set into its
own resources (`negative-*/markers.log`, `negative-*/recovered/markers.log`).

| Case | Injection | Observed | Resources created | Original backup |
|---|---|---|---|---|
| missing-component | `provider/rustfs-data.tar` deleted from the copy | refused in preflight: required component missing | 0 | unchanged |
| corrupt-checksum | one byte flipped in `sql/ledger.dump` | refused in preflight: component checksum differs | 0 | unchanged |
| unsealed | `SEALED` deleted | refused in preflight: backup set is not sealed | 0 | unchanged |
| nonempty-target | existing file in the restore directory; occupied volume | refused: restore target is not empty; file intact; occupied volume reported, never extracted into | 0 for the directory case | unchanged |
| wrong-endpoint | recovered host configured with a different endpoint port under the recorded generation | `IllegalStateException: Managed backend generation is already bound to another physical profile`; host never started; no READY | restore only | unchanged |
| new-generation | new generation at the recorded endpoint | host binds the new generation, then `readRaw` and `getEntry` refuse with "Original document/archive backend is not configured on this host"; no READY | restore only | unchanged |
| missing-version | recorded version id of revision 1's core object deleted on the disposable restored RustFS | see `negative-missing-version/recovered/markers.log`; revision 2 still reads; no READY | restore only | unchanged |
| corrupt-descriptor | catalog row of the retained artifact replaced with damaged bytes (immutability trigger disabled by the test on the disposable copy) | raw read still byte-equal (explicit opaque access); validated and materialized reads DATA_LOSS; no READY | restore only | unchanged |

No case fell back to another backend, served a newer version in place of a
missing one, invented typed content, or advertised readiness.

## Boundary noted

A provider-version loss is detected by the recovered host's verified read,
not by preflight: the volume archive digest is intact, only the content inside
the restored provider is damaged. The host stays unready until verification
completes, as the design note states.
