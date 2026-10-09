# Archive backup and recovery qualification

This note covers exactly what `ArchiveBackupQualificationIT` qualifies: an offline
(QUIESCED) backup of one repository deployment holding archive entries, PostgreSQL ledger
plus one pinned RustFS volume, restored into fresh stores and verified by a fresh
production-JAR host. It extends the document rehearsal in
[repository-backup-rehearsal.md](../operations/repository-backup-rehearsal.md), whose
offline-volume procedure, transaction-counter check, backup-set layout and driver helpers
it reuses unchanged. It is a qualification harness, not an online backup service, not a
public restore API, not provider identity remapping and not a statement of production
backup readiness. Archived results are under
[docs/evidence/repository/archive-backup-qualification](../evidence/repository/archive-backup-qualification/README.md).

## Inventory: what the archive protocol supports

Everything below was located in source; nothing is assumed.

| Item | Source | Fact |
|---|---|---|
| Archive shapes | `repo/proto/.../archive/v1/archive.proto`, `entry.proto` | account-scoped `Archive` bound to one drive with `VERSIONING_POLICY_NONE` or `RETAINED`; `EntryAddress` (account, archive, entry_id) with a deterministic entry UUID (`ArchiveIds.entryUuid`); `VersionManifest` per immutable version with renditions in PRESENT, EMPTY or DELETED state, object key, `storage_object_id`, write attribution and a frozen `metadata_snapshot` (V102) |
| Archive version numbers | `archive_versions.version`, `archive_entries.current_version`, `archive_mutation_revision_seq` (V12, V13) | per-entry counters and a trigger-stamped ledger revision; neither is a provider identity |
| Provider identity | `archive_object_bindings` (V14: generation, realm, bucket, key, immutable), `archive_object_uploads` (V15, V16, V18: state, sha256, `provider_version`, `etag`) | the S3 version id and ETag the provider issued at upload, recorded by `ArchiveUploadLedger.verify`; bound reads resolve the recorded generation and realm (`ArchiveObjectReader`) and fetch by bucket, key and `provider_version` with `getBounded` |
| Shared references | `archive_version_object_refs` (V16) mirrored into `repository_object_references` as `ARCHIVE_VERSION` (V26), retention row `repository_object_retention` (V27) | an unchanged rendition is re-referenced by later versions, never copied (`ArchiveOperations.shareRetainedObject`); the physical object is retained while any version references it |
| Ownership | `RepositoryErrors.requireProcessAuthority` on every `ArchiveOperations` and `ArchiveMutationOperations` door | only process authority is admitted; account membership and ACL identities grant nothing (archive ACLs are listed as pending in `docs/design/archive.md`); identity is address-derived, so an entry addressed under another account is NOT_FOUND |
| Receipts and durable identity | `archive_mutations` (V19, immutable admission with command bytes and receipt), `archive_mutation_targets` (V19), `archive_mutation_observations` (V20) | `ArchiveMutationService` admits one logical mutation per (account, principal, operation id); replays return the recorded logical admission with the current physical observation; `PutEntry` has no receipt table, its replay is the root-checksum elision (`deduplicated=true`, retained manifest returned) |
| Pending operation | `ArchiveMutationLedger.execute`, `ArchiveCleanupLedger`, `ArchiveObjectRecovery`, lifecycle lane `repo-archive-mutation-recovery` in `RepoServices.startLifecycle` | an admitted delete or redaction leaves `objects_pending > 0` in state ADMITTED until the lane claims each target (`DELETING`), reclaims it at the exact original backend (`S3ObjectReclaimer`: every version under the key) and records `DELETED`; a failed attempt records `BACKEND_RECLAMATION_FAILED` and the observation reports RETRY_REQUIRED |
| Document-to-archive references | V26, V65, V70 lock sets; `repository_physical_locations.source_kind='ARCHIVE'` | the retention lock sets order archive owners before document owners, but no supported operation creates a document that references an archive object (no Java caller uses the `ARCHIVE` source kind); the qualification therefore uses the supported archive-internal dependencies: shared rendition objects across retained versions and the mutation target's retention fence |
| Physical backend binding | `managed_backend_profiles` (V11, V17), `ManagedBackendLedger.bind` | the host binds its configured generation to the recorded endpoint origin, region, path style and realm, and refuses another physical profile under the same generation |

## Seed

Two accounts (`abq-alpha-<run>`, `abq-beta-<run>`), each with a host-provisioned drive whose
bucket the production provisioner creates with versioning enabled (verified through the
plain S3 API), and a retained archive `qualification-docs` in each. Every payload is a
labeled synthetic fixture (`archive-backup-qualification synthetic fixture`).

| Entry | Content |
|---|---|
| alpha `report.pdf#2026` | v1: `original` (2 KiB) and `markdown` with title, filename, content type, source URI and metadata map; v2: unchanged `original` re-referenced, new `markdown`; v3: a 1 MiB `attachment.bin` streamed through `uploadStream` with a declared digest, both siblings re-referenced; a fourth identical save is elided (`deduplicated=true`, version stays 3, title changes) |
| alpha `notes#1` | v1 with a PRESENT `original` and an EMPTY `empty` rendition |
| beta `subject-record` | v1 `original` and `scratch`; v2 new `scratch`, `original` re-referenced; then `DeleteRendition(scratch, RTBF)` admitted through `ArchiveMutationService`: two versions tombstoned, two physical targets pending |
| beta `qualification-scratch/draft` | `VERSIONING_POLICY_NONE`: v1 then v2; v1 is not retained (NOT_FOUND), its object stays LIVE and unreferenced for aged reconciliation |

The seed host parks both archive cleanup lanes (one-hour intervals) so the admitted
mutation is still pending at the cut; the recovered host runs mutation recovery every
second. Both are ordinary `RepoServiceConfig` values, not fixture hooks.

Recorded before the cut: every `GetEntry` response per version, every version listing,
per-archive stats, the dedupe request, the mutation request and receipt, every physical
binding (generation, realm, bucket, key, provider version id, ETag, digest, size, state,
references, retention fence, mutation-target flag), entry and version rows, the admission
rows, the access matrix, sequences, row counts, reader incarnations and upload states
after the host closed.

## Consistency boundary

The cut is QUIESCED and proven, with the document rehearsal's checks plus the archive's:
after the seed host exits with code 0, `pg_stat_activity` shows no client backend other
than the capture connection, no ACTIVE reader incarnation, no document or archive read
pin, no staging part attempt, and every `archive_object_uploads` row is LIVE (no
STAGING, VERIFIED or DELETING row: no upload or cleanup in flight). The two pending
mutation targets are durable rows, kept as data. Every base table is fingerprinted,
`pg_dump --format=custom` runs, the backend check and fingerprints are repeated after the
dump, both containers are stopped, the RustFS volume is archived from its stopped state
and the set is sealed with the archive state recorded in the manifest.

## Restore and verification

Preflight, new volumes, provider volume extracted and served at the recorded endpoint,
new PostgreSQL, `pg_restore --exit-on-error`, the transaction-counter check (a cluster not
past every stored `xid8` is refused, as in the document rehearsal), sequences, migration
level V119, every table fingerprint, and the archive state (pins, upload states, admitted
mutations, pending targets, references, versions, entries) compared with the capture.

The recovered host, a fresh JVM on the production JARs, then verifies in this order:

1. Before any host opens: row counts, reader incarnations, backend profile, sequences,
   bindings, entry and version rows, admission rows and cleanup state equal the seed
   record; every recorded LIVE object, including both pending targets, reads from the
   restored provider by its recorded version id with the recorded SHA-256, ETag and size;
   the production `ArchiveMutationObservations` reports the mutation ADMITTED with two
   pending targets at status revision 1, equal to the recorded receipt.
2. Every recorded version of every entry reads byte-equal (renditions, manifest with
   frozen snapshot, current metadata); listings and stats equal; the access matrix equals
   the seed; the authenticated in-process transport refuses without the token and serves
   the recorded entry and receipt with it.
3. The pending mutation completes through the recovery lane: COMPLETED with two
   confirmed-absent targets, logical admission unchanged, both target rows DELETED after
   one attempt each, no version left under either key on the provider, the shared
   `original` still LIVE and readable at both versions. Replay of the dedupe request and of
   the mutation request return the recorded outcomes.
4. A fresh write: version 4 of alpha `report.pdf#2026` with a new `markdown`; the
   `original` and `attachment.bin` bindings are re-referenced (same `storage_object_id`),
   `archive_mutation_revision_seq` and the entry revision advance past the restored
   values, versions 1 to 3 still read.
5. An authorized lifecycle operation: `DeleteRendition(markdown, RETENTION)` on that
   entry tombstones four versions and targets three distinct objects; cleanup completes;
   every version's manifest equals the recorded manifest with the tombstone applied by
   the production manifest mechanics, the surviving renditions read byte-equal, the shared
   `original` (versions 1 to 4) and `attachment.bin` (versions 3 and 4) stay LIVE on the
   provider, the three markdown objects are gone, and the counters match.
6. Only then READY is written.

## Negative cases

Each case restores a disposable copy of run-1's sealed set. The expected classification
is the exact existing one, observed at the library boundary (`RepositoryException.code()`
or the raw exception type) and recorded with its cause chain in the markers.

| Case | Injection | Required observation |
|---|---|---|
| mixed-captures, missing-component, corrupt-checksum, unsealed, nonempty-target | run-2's provider archive in run-1's set; `sql/ledger.dump` removed; one byte of the provider archive flipped; `SEALED` removed; occupied restore directory | preflight refuses before any resource is created; the target stays untouched |
| inconsistent-snapshot | run-2's provider archive in run-1's set, resealed | every recorded version absent (NoSuchBucket); every `GetEntry` FAILED_PRECONDITION (`BlobStoreException(FAILED_PRECONDITION)` from the provider's 404 on a missing bucket); manifests, listings and stats still read; the mutation replays; recovery records BACKEND_RECLAMATION_FAILED and RETRY_REQUIRED, targets stay DELETING; no READY |
| missing-version | a newer version put under the markdown key of version 1, then the recorded version deleted | `GetEntry(v1)` DATA_LOSS (`BlobNotFoundException`: RustFS answers NoSuchKey for the deleted version id); the manifest is intact; the sibling `original` of the same version and versions 2 and 3 read byte-equal; the newer version is never substituted |
| corrupt-payload | one byte of the 1 MiB `attachment.bin` data file flipped on the extracted restored volume before RustFS starts | RustFS detects the damaged part and aborts the response for the recorded version id (the SDK gives up after four attempts); `GetEntry(v3)` and the single-rendition read are UNAVAILABLE with `BlobStoreException(UNAVAILABLE) <- IOException` in the cause chain, so no bytes are served; manifest intact; siblings and other versions read byte-equal. The DATA_LOSS branch of `ArchiveObjectReader` (bytes delivered with a wrong digest) is not reachable on this provider |
| wrong-endpoint | recorded generation at a different endpoint | `ManagedBackendLedger.bind` refuses: already bound to another physical profile; the host never composes |
| new-generation | a new generation at the recorded endpoint | the host composes; every bound read fails with the resolver's `IllegalStateException` (`Original archive backend is not configured on this host`), which `RepositoryErrors.call` does not translate (baseline, recorded for the error-translation work); manifests still read; recovery records BACKEND_RECLAMATION_FAILED and RETRY_REQUIRED; every recorded byte is still on the provider |
| wrong-credentials | wrong provider secret | direct GET 403; the host composes without a provider call; every bound read PERMISSION_DENIED with `BlobStoreException(PERMISSION_DENIED)` in the cause chain; recovery RETRY_REQUIRED; every recorded byte intact |
| wrong-database-credentials | wrong ledger password | host exit nonzero on `password authentication failed`; no READY |

## Running

The lock wait bound exceeds the longest single holder of the shared lock,
`admissionStorageTest` at about twenty minutes; a shorter bound expires behind it and
produces no result, which is a lock timeout to record, not a test outcome.

```
flock -w 1800 /tmp/protomolt-repository-qualification.lock \
  ./gradlew -I repo/container/src/test/resources/archive-backup-qualification/qualification.init.gradle \
  :protomolt-repo-container:test --tests '*ArchiveBackupQualificationIT' --max-workers=2 --console=plain
```

The init script wires the module's ordinary `test` task the way the document rehearsal's
does: it adds the admission inventory bundle and the production host classpath, marks the
task never up to date, re-includes the class that `build.gradle` excludes from the ordinary
`test` task, and sets the opt-in property. The ordinary task therefore never selects the
class and never reports it skipped; selecting it without the script finds no tests and
fails, as `RepositoryBackupRehearsalIT` does. With the script every case runs and a passing
run has zero skipped cases. `-Pprotomolt.qualificationKeep=true` leaves
each case's containers and volumes. Evidence lands under
`repo/container/build/archive-backup-qualification/<timestamp>/`.

Sources: driver `ArchiveBackupQualificationIT` and `ArchiveBackupQualificationProbeCompiler`
in `repo/container/src/test/java/.../ledger/`; hosts and helpers under
`repo/container/src/test/resources/archive-backup-qualification/`; reused unchanged from
the document rehearsal: `RepositoryBackupRehearsalStores`, `Shell`, `Catalog`, `BackupSet`
(driver) and `Failure`, `Checks`, `Json`, `Ledger` (hosts).

## Limitations

Offline only; one deployment shape (PostgreSQL 18, RustFS single volume, versioned
buckets); provider identity is provider-issued and preserved only by the volume snapshot;
no identity remapping, no online or incremental capture, no Redis profile, no LocalStack,
no hosted CI, no performance claim. Archive ACLs do not exist, so the ownership matrix pins
the process-authority contract, not an account-scoped one. The transaction-counter
advancement branch is refused, not exercised. Pruning is not enabled and nothing here is
pruning authority; `PruneVersions` is not used.

## Criterion mapping

| Criterion | Case and marker |
|---|---|
| Inventory of shapes, versions, ownership, receipts, references, physical binding | the inventory table above; seed markers `seed.report.*`, `seed.notes.v1`, `seed.subject.*`, `seed.draft.unversioned`, `seed.ledger.*` |
| Seed through production code: two accounts, current and retained history, metadata, at least two entries, more than one payload version | `ArchiveBackupQualificationSeedHost` (seed host markers, `seed-results.xml`): four entries, seven retained versions, ten physical objects, provisioned versioned buckets |
| Shared-reference or retention dependency | `seed.ledger.report_sharing`, `seed.ledger.pending_targets`, `restore.bindings.*`, `after-write.lifecycle.bindings` |
| Genuinely pending operation, recovery classification, retry behavior | `seed.pending.admitted`, `restore.pending.observation`, `restore.pending.completed`, `restore.pending.targets_reclaimed`; retry behavior under a missing or refusing backend in `<mode>.pending.retry_required` and `<mode>.pending.targets_retained` |
| QUIESCED backup with drained workers, no transactions or live I/O, catalog and store consistency | driver markers `capture.quiescent`, `capture.catalog_fingerprinted`, `capture.post_dump_quiescent`, `capture.stopped`, `capture.provider_archive`, `capture.sealed` |
| Provider version identity through the offline volume | `restore.provider_identity`, `restore.provider.*` (every object by recorded version id, SHA-256, ETag, size) |
| Transaction-counter check | `restore.xid_epoch`, `restore.xid_past_stored` |
| Restore into fresh PostgreSQL, store and JVM | `restore.pg_restore`, `restore.catalog_identity`, `restore.archive_state`, `restore.fresh_incarnation` |
| Bytes, digests, metadata | `restore.entry.<tag>.v<n>.bytes`, `.manifest`, `.present`, `.info`; `restore.versions.*`; `restore.stats.*` |
| Ownership and denial matrix | `restore.access.*`, `restore.access_matrix_unchanged`, `restore.transport.*` |
| Replay where supported | `restore.replay.put.alpha-report`, `restore.replay.mutation.beta-subject`, `restore.transport.mutation_lookup` |
| Subsequent legitimate write | `after-write.new_version`, `after-write.sequence_advanced`, `after-write.readable`, `after-write.listing`, `after-write.entry.*` |
| Authorized lifecycle operation without losing a retained sibling | `after-write.lifecycle.admitted`, `.completed`, `.v1` to `.v4`, `.bindings`, `after-write.lifecycle.provider.*`, `.stats` |
| Negative: missing exact provider version | `missingProviderVersionRefusesWithoutSubstitution`: `missing-version.read_refused` (DATA_LOSS), `.siblings_readable`, `.unaffected.*` |
| Negative: corrupt payload | `corruptPayloadRefusesWithoutServingBytes`: `corrupt-payload.read_refused` (UNAVAILABLE with the provider cause), `.rendition_refused`, `.manifest_intact`, `.siblings_readable` |
| Negative: mismatched catalog and store sets | `mismatchedOrDamagedBackupSetsAreRefusedAtPreflight` (`mixed-captures.refused_before_resources`) and `resealedMismatchedCatalogAndStoreRefuseArchiveReads` (`inconsistent-snapshot.read_refused.*`, `.pending.retry_required`) |
| Negative: wrong provider binding | `wrongEndpointUnderRecordedGenerationRefuses` (`wrong-endpoint.refused`) and `newGenerationAtRecordedEndpointRefusesReadsAndReclamation` (`new-generation.read_refused.*`, `.pending.retry_required`, `.bytes_intact.*`) |
| Negative: wrong credentials | `wrongProviderCredentialsRefuseReadsAndReclamation` (`wrong-credentials.provider_refuses`, `.read_refused.*`, `.pending.retry_required`, `.bytes_intact.*`) and `wrongDatabaseCredentialsRefuseTheHost` |
| Two complete clean-room rehearsals | `rehearsalRunOne`, `rehearsalRunTwo` (independent stores, credentials, generation and backup set each) |

## Baseline classifications recorded for the error-translation work

These are observations of the current production code, asserted exactly; they are not
changed here.

- A bound archive read under a generation the host does not serve fails with the
  resolver's `IllegalStateException` ("Original archive backend is not configured on this
  host") from `ArchiveOperations.getEntry`; `RepositoryErrors.call` maps
  `IllegalArgumentException` and provider exceptions but not this one.
- A provider refusal during a bound read surfaces as `RepositoryException` with the
  `BlobStoreException` code (PERMISSION_DENIED for 403, FAILED_PRECONDITION for a missing
  bucket's 404, UNAVAILABLE when the provider stops the response), with the provider
  exception in the cause chain.
- A missing object or version is DATA_LOSS from `ArchiveObjectReader`; RustFS answers
  NoSuchKey rather than NoSuchVersion for a deleted version id, and both map to
  `BlobNotFoundException`.
- Physical cleanup under any of these conditions records `BACKEND_RECLAMATION_FAILED`
  (RETRY_REQUIRED) and retries on the next pass; it never deletes bytes it could not
  address at the recorded backend.

## Observations outside the qualified scope

- No archive table stores an `xid8` value, so `restore.xid_epoch` compared the restored
  cluster's next transaction id with 0 in both runs; the check is load-bearing only for
  document publication journals, which this seed does not create.
- When a recovered host exits through a failed check, the interrupted lifecycle lanes log
  "Closed by interrupt" JDBC warnings during shutdown; the passing hosts show none. This is
  shutdown noise of the lanes' one-second intervals, not a data effect.

## Affected archive suites

The archive suites of `protomolt-repo-container` and `protomolt-repo-service` were run on
the same tree to show the behaviors the qualification relies on are the current ones:

```
flock -w 1800 /tmp/protomolt-repository-qualification.lock \
  ./gradlew :protomolt-repo-container:test --tests '*Archive*' \
  :protomolt-repo-service:test --tests '*Archive*' --max-workers=2 --console=plain
```
