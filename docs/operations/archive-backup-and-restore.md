# Archive backup and cold restore

This guide is the operator procedure for an **offline** (quiesced) backup of a
repository deployment's surviving archive stack and its **cold restore** into
fresh resources. Every step below is the one proven by the qualification suite
`ArchiveBackupRestoreIT` (`:protomolt-repo-service`), which rehearses the full
cycle against real PostgreSQL 18 and RustFS and fails loudly when any piece is
missing. It is a cold procedure: it is not an online or incremental backup
service, not a public restore API, and not provider identity remapping.

What a backup set must cover:

1. The archive ledger tables in PostgreSQL — `archives`, `archive_entries`,
   `archive_versions`, `archive_object_bindings`, `archive_object_uploads`,
   `archive_version_object_refs`, `archive_read_pins`, `archive_mutations`,
   `archive_mutation_targets`, `archive_mutation_observations`, the stats
   tables, `managed_backend_profiles`, and the retention bridge rows —
2. the `drives` rows naming each archive's bucket and prefix,
3. the `repo_catalog` schema used by the v2 parts-based `PostgresCatalog`,
4. the provider objects the archive ledger references, with their
   provider-issued version identity.

## Why the provider backup is a volume snapshot

Bound archive reads do not fetch "the current bytes" of an object. The ledger
records, per object, the bucket, key, expected size, SHA-256, and the
provider-issued `provider_version` and ETag from the verified upload
(`archive_object_uploads`), and a read fetches exactly that version
(`BlobStore.getBounded`) and re-hashes the bytes before serving them. Copying
objects into a new bucket assigns new version ids and breaks that binding, so
an S3-level object sync is **not** a restore of this stack. The offline backup
preserves provider identity by archiving the RustFS data volume itself,
stopped, and serving the restored copy at the **same endpoint** the ledger's
backend generation is bound to (`managed_backend_profiles` records the
endpoint, region and path style; a host booted against a different endpoint
refuses the recorded generation). Never overwrite the source volume: restore
always extracts into a fresh volume.

## Backup (quiesced cut)

1. Quiesce the deployment: stop ingest, then close the repo-service host
   cleanly (`RepoServices.close()` drains reads, stops the lifecycle lanes and
   releases its pools). Prove quiescence before capturing anything:
   - `SELECT count(*) FROM archive_read_pins` is 0,
   - `SELECT count(*) FROM archive_object_uploads WHERE state NOT IN
     ('LIVE', 'DELETED')` is 0 — no upload or cleanup in flight. `STAGING`,
     `VERIFIED` and `DELETING` rows are active work and make a cut unsafe;
     `LIVE` rows and retained `DELETED` cleanup tombstones are durable data,
   - no client backends remain in `pg_stat_activity`
     (`backend_type = 'client backend'`).
   Mutations admitted but not yet reclaimed are durable ledger rows, not
   process activity; they are captured as data and complete after the restore.
   The same holds for completed cleanups: a `DELETED` upload row is the
   tombstone record of a finished reclamation. It travels with the backup, and
   the restore verifies the reclaimed object stays absent from the provider
   instead of requiring it to exist.
2. Record the ground truth: per-table fingerprints over both schemas
   (`public` and `repo_catalog`), every sequence's `last_value` (including
   `archive_mutation_revision_seq`), the drive rows, and the object inventory
   from `archive_object_uploads` joined to `archive_object_bindings`.
3. Dump the whole database: `pg_dump --format=custom` (the dump spans both
   schemas, so the v2 catalog travels with the ledger).
4. Re-run the fingerprints: nothing may have changed across the dump window.
5. Stop PostgreSQL and RustFS. Archive the RustFS data volume from its stopped
   state (`tar` of the volume root), digest both artifacts
   (`sql/ledger.dump`, `provider/rustfs-data.tar`), and record the manifest:
   image tags, the backend generation / realm / endpoint, table fingerprints,
   sequences, row counts, and the archive state. Mark the set `SEALED`.

## Cold restore

1. Preflight: the set must be sealed and every artifact's digest must match the
   manifest. Refuse mixed, damaged or unsealed sets before creating anything,
   and never restore into a non-empty target.
2. Create a fresh Docker volume and extract the provider tar into it. Start
   RustFS on that volume **at the recorded endpoint** (same host port); the
   endpoint is part of the bound backend identity.
3. Create a fresh PostgreSQL cluster and `pg_restore --exit-on-error` the dump
   into its database. Treat any `ERROR`/`WARNING` as a failed restore.
4. Compare the restored database with the capture before any host opens:
   identical table fingerprints, identical sequences, identical drive rows, and
   every recorded entry version and object row present. A mismatch fails the
   restore loudly — the fingerprint gate names the table, the row-level gate
   names the affected entry.
5. Boot the repo-service host with the same managed backend generation and
   realm against the restored database and endpoint. The backend binding check
   accepts the identical physical profile; the recovery lanes resume, and a
   mutation left pending at the cut now completes through the normal recovery
   path.
6. Verify service-level identity before returning to production:
   - every archived entry and every retained version reads byte-for-byte, each
     rendition's bytes hashing to the SHA-256 recorded in the ledger;
   - every `LIVE` provider object in the object inventory reads by its
     recorded version id, and every retained `DELETED` tombstone's reclaimed
     object stays absent;
   - listings and per-archive stats equal the capture;
   - replayed saves and mutations return the recorded outcomes
     (deduplicated saves, the recorded mutation receipt);
   - a fresh write advances versions and the mutation revision sequence past
     the restored values.

## What failure looks like (proven by the suite)

- **An object missing from the restored bucket**: the pre-boot provider sweep
  fails naming the entry, rendition, bucket, key and version id; a host read of
  that rendition is `DATA_LOSS` ("published archive object is missing"), and no
  newer provider bytes are ever substituted. Sibling renditions and other
  versions are unaffected.
- **A ledger row missing from the restored database**: the fingerprint
  comparison fails naming the table, and the row-level check names the affected
  entry and version; a host read of that version is `NOT_FOUND` while the
  surviving versions read normally.
- **A provider restored at a different endpoint**: the host refuses the
  recorded backend generation at boot (`ManagedBackendLedger.bind`); nothing is
  served.

## Running the qualification

The suite is an ordinary integration test — no init script, no opt-in
property; Docker availability is the only requirement:

```
./gradlew :protomolt-repo-service:test --tests "*ArchiveBackup*" --max-workers=2
```

Evidence (manifest, fingerprints, object inventory) lands under
`repo/service/build/archive-backup-qualification/<timestamp>/` on success and
failure.

## Limitations

Offline only; one deployment shape (PostgreSQL 18, single-volume RustFS with
versioned buckets); provider identity is provider-issued and preserved only by
the volume snapshot; no identity remapping, no online or incremental capture,
no Redis-backed profile, no hosted-database variant, and no performance claim.
