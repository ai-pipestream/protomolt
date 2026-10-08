# Backup and recovery follow-ups: provisioned versioning and the xid advancement branch

Follow-up to [2026-10-07-backup-recovery](../2026-10-07-backup-recovery/README.md),
run on 2026-10-08 from the branch that closes the two gaps that evidence listed.

Command (unchanged):

```
verification/repository-recovery/run.sh --out /tmp/repository-recovery-<ts>
```

## What changed

- `S3NamespaceProvisioner.ensureNamespace` now enables bucket versioning on the
  bucket it creates, or on an existing bucket that does not report it, and
  refuses the namespace unless S3 reports versioning ENABLED. The seed host no
  longer creates buckets by hand: both drives go through `DriveService.CreateDrive`
  and the host's provisioner, and the seed checks through the plain S3 API that
  both provisioned buckets report versioning ENABLED (`seed.drive.provisioned_versioned`).
  `S3NamespaceProvisionerIT` covers a new bucket, an existing unversioned bucket
  and a suspended bucket on LocalStack.
- The second positive run consumes 8192 transaction ids on the source cluster
  before seeding (`--xid-burn`, default 8192), so the stored `xid8` values exceed
  what the restored cluster allocates and the restore has to take the
  `pg_resetwal` branch. The harness asserts which branch ran (`restore.xid_branch`)
  and fails if the expected one did not.
- The first run of the branch exposed a harness defect: after the `pg_resetwal`
  restart, Docker published a different ephemeral host port and the harness kept
  the old one, so the restored database never became "ready". The harness now
  resolves the port again after the restart. The archived run is the one after
  that fix.

## Results

| Run | Branch | Harness | Seed | Recovered host | READY |
|---|---|---|---|---|---|
| run-1 | no advancement (next xid 1580 above max stored 1012) | 18 pass | 56 pass | 80 pass | yes |
| run-2 | advanced with `pg_resetwal` (next xid 9208 above max stored 9206, after consuming 755 to 8948) | 19 pass | 56 pass | 80 pass | yes |

After the advancement the recovered host read the same content, replayed the
receipts, committed a new revision with mutation revision above the restored
sequence, and its own transactions ran above every restored `xid8`
(`restore.xid_past_stored` current=9210, maxStored=9206).

All eight negative cases passed with the same observations as before
(`negative-*/markers.log`).

Module tests on the same tree: `:protomolt-repo-blob-s3:test` and
`:protomolt-repo-engine:test` (172 tests, including the new provisioner IT),
and the service drive and archive ITs (`ArchiveServiceIT`, `ArchiveManagedUploadIT`,
`ManagedArchiveHostIT`, `RawIngestionIT`, `ManagedRawDocumentIT`,
`DocumentRecoveryHostIT`, 95 tests), all passing. `ArchiveManagedUploadIT`
relied on an unversioned namespace to overwrite published bytes in place; it now
creates that bucket outside the provisioner and says so, because the provisioner
would version it and the overwrite would no longer reach the published version.
