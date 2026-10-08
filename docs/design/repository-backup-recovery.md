# Repository offline backup and recovery

Status (2026-10-07): qualified rehearsal design for one deployment shape. The
harness under `verification/repository-recovery/` implements exactly what this
note claims and nothing more. This is not a public restore RPC, an online backup
service, a provider migration tool, a source-history pruner or a JCR
implementation. The claimed historical execution paths that currently refuse
(`repository-historical-restore.md`) stay refused; this rehearsal never calls
them.

## Backup boundary

One supported repository deployment is reconstructed from three durable stores.
Everything below was located in source, not assumed.

### SQL ledger (PostgreSQL, Flyway level V111)

All rows of the `repo` migration set, including `flyway_schema_history`, and the
two sequences. There are no identity or serial columns; every key is minted by
the application or derived from content.

| Component | Where | Notes |
|---|---|---|
| Sequence state | `document_mutation_revision_seq` (V8), `archive_mutation_revision_seq` (V12) | Trigger-stamped `mutation_revision`; the restored value must be at or past the source value (`repository-composition.md` "Backups must preserve the revision sequence state"). |
| Backend identity, generation, profile | `managed_backend_profiles` (V11, V17) | Immutable rows: generation → provider, `identity_json` (endpoint, region, path-style), storage realm. The host binds its configured identity to the generation at start and refuses a mismatch. |
| Object registry and provider identities | `raw_objects`, `document_raw_refs`, `document_part_attempts`, `document_part_attempt_objects`, `archive_object_bindings`, `archive_object_uploads`, `archive_version_object_refs`, `repository_physical_locations`, `repository_object_keys` | Each committed object records generation, realm, namespace (bucket), key, sha256, size, content type, `provider_version` (S3 version id) and etag. These are the exact provider identities that must still resolve after restore. |
| Logical content state | `drives`, `documents`, `archives`, `archive_entries`, `archive_versions` (metadata snapshot frozen by V102), `archive_stats`, `archive_rendition_stats`, `document_revision_*`, selections, purges, outbox | `archive_stats` are exact counters, not derived (V75). `document_revision_*` projections are retention-authoritative since V39. |
| Retained schema and descriptor assets | `repository_schema_artifacts` (V48, bytea catalog keyed by account and sha256), `document_revision_schema_artifacts` (V55), `document_revision_schema_evidence` (V56), `document_revision_schema_assets` (V57), `document_schema_policies` and `_current` (V58), `document_revision_schema_admissions` (V61) | All descriptor bytes live in SQL, not in the object provider and not with every object. Recorded compiler and admission tool identities (`SchemaCompilationProvenance`, `SchemaToolIdentity`) are inside the `repository-schema-asset` metadata artifact bytes referenced by `metadata_sha256`. |
| Receipts, operations, claims | `repository_operations`, `_operation_owners`, `_operation_success`, `_operation_rejection`, `archive_mutations`, `archive_mutation_targets`, `archive_mutation_observations`, `repository_execution_claims`, `_execution_scopes`, `_credential_authorities`, `_creation_grants` | Leases and deadlines (`lease_until`, `claim_until`, `retain_until`, `expires_at_epoch_micros`) are restored verbatim. Restore never resets them. |
| Preparation, coordinator and capture journal | V81 to V98 and V103 to V111 tables | Append-only evidence; restored verbatim. |
| Reader pins, incarnations, cleanup state | `repository_reader_incarnations`, `archive_read_pins`, `document_read_pins`, `document_assessment_*`, `document_part_attempt_cleanup`, `repository_object_retention`, `repository_object_references` | Restored verbatim. A restored ACTIVE incarnation belongs to a process that no longer exists; nothing in this procedure fences or quiesces it, and no cleanup is triggered by restore. |
| Transaction ids | about 22 `xid8` columns (`creation_xid`, `projection_xid`, fence xids, ...) | Same-transaction proofs compare these to `pg_current_xact_id()` in BEFORE INSERT/UPDATE triggers; read paths join on equality between restored rows. A logical dump restores the values verbatim. The restored cluster's next transaction id must be strictly greater than every stored `xid8` so a new transaction can never equal a restored proof; the restore checks this and advances the counter with `pg_resetwal` when needed. |

### Object provider (pinned RustFS, versioned S3)

Every bucket the drives reference (`documents/...`, `archive/...`,
`.protomolt-managed`) with all object versions. SQL pins exact version ids and
etags, so the restore must preserve version ids byte for byte.

Bucket versioning is provisioned by the host, not left to the operator:
`S3NamespaceProvisioner.ensureNamespace` creates a missing drive bucket, enables
versioning on it (or on an existing bucket that does not report it) and refuses
the namespace unless S3 reports versioning ENABLED. Before this change the
provisioner created unversioned buckets, S3 returned no version id, and the
ledger recorded `provider_version` as NULL (the first rehearsal attempt showed
exactly this); such objects were protected only by never-reused keys. The seed
host now creates its drives through the ordinary provisioning path and verifies
through the plain S3 API that both buckets report versioning ENABLED, and the
restore reads every committed object by its recorded version id.

Qualified provider: `rustfs/rustfs:1.0.0-beta.11-preview.1` (the same pin as
`AssessmentStorageBackend`), single volume `/data`, `RUSTFS_VOLUMES=/data`, Linux
x86-64 host with Docker. Storage format: one directory per object key holding
`xl.meta` (inline data for small objects, `part.N` files otherwise); version ids
are recorded inside `xl.meta`. The harness records the exact image id in the
manifest and the evidence.

Experiment (2026-10-07, scratch): a versioned bucket with two versions of one key
and one other key; container stopped; `tar` of the volume; untar into a fresh
volume; fresh container on that volume. `ListObjectVersions` returned the same
three version ids and `GetObject(versionId=...)` returned the original bytes of
each version. An ordinary GET followed by PUT would have minted new version ids
and is not used anywhere in this procedure.

### Schema assets

Retained schema assets are SQL rows (above). The backup additionally exports the
normalized catalog digest inventory (`schema/catalog.tsv`: account, sha256,
size) so a restore can prove the catalog came back exactly and so a missing
catalog is a refused component, not a silent gap.

### Derived caches (excluded, rebuilt)

`CachingBlobStore` contents, `DocumentSchemaArtifactCache`, registry descriptor
loads, external Confluent or Apicurio discovery, retention inventory query
results, indexes and `GENERATED STORED` columns, Hibernate and statement caches.
None of these is a retention mechanism (`repository-schema-cache.md`).

### Not stored in the repository

Provider and database credentials (no row holds a secret; `repository_credential_authorities`
holds tombstones only). The restore reads credentials from explicit runtime
inputs. Build-time `compiler.properties` of the built-in contract is part of the
production JARs, which the evidence inventories by sha256.

## Consistent offline cut

The capture is a global, test-owned stop. No online protocol is invented.

1. The only writer is the seed host JVM. It closes its `RepoServices` (which
   drains schema workers, lifecycle threads, provider handles and readers) and
   exits. The harness records the exit code; a nonzero exit aborts the capture.
2. Quiescence evidence, not just an empty queue: after the JVM exit the harness
   queries `pg_stat_activity` and refuses to continue while any non-dumper
   backend is connected to the ledger database.
3. `pg_dump --format=custom` of the ledger database runs inside a container of
   the same pinned PostgreSQL image, over the run-scoped Docker network. The
   dump carries rows, both sequences (`setval`), functions and triggers (created
   post-data, so restored rows are not re-guarded).
4. The source PostgreSQL container is stopped (clean shutdown) and the RustFS
   container is stopped. Only then is the RustFS volume archived with `tar`
   from a helper container. No provider effect can arrive after the snapshot:
   the sole writer exited before step 3 and the provider process was stopped
   before its bytes were read.
5. The schema catalog inventory is exported from the dump target database
   state captured in step 3 (read-only query before the stop).
6. Every component file is digested (SHA-256) into `manifest.json` together
   with nonsecret identity: format version, image ids, Flyway level, backend
   generation, realm and identity fields, sequence values, row counts of the
   identity tables and the seeded identities. `SEALED` is written last and
   holds the manifest digest. A backup without `SEALED` is unsealed and refused.

Restore does not revive expired authority: leases and claims are restored with
their recorded deadlines and the restored host is started with a fresh reader
incarnation. Nothing resets `lease_until`, `claim_until` or `retain_until`.

## Restore procedure (what the harness does)

Preflight, before any resource is created: `SEALED` present and matching the
manifest digest; manifest format known; every required component present with
the recorded digest and size; the restore directory, database volume and
provider volume targets empty. Any failure refuses with a nonzero exit, creates
nothing and leaves the backup directory and the source volumes byte-identical
(the harness digests them before and after each refusal).

1. The source containers are stopped and removed. The harness proves the
   original resources are unreachable (Docker inspect fails, the endpoint port
   refuses connections) before restoring.
2. A new provider volume is created and the RustFS archive is extracted into it.
   A new RustFS container starts on that volume. Because `managed_backend_profiles`
   records the endpoint origin (`http://127.0.0.1:<port>`), the restored
   container is published on the same host port after the harness has proven
   the port is free. This is explicit configuration of the recorded identity,
   not a fallback: the restored host is handed the recorded endpoint, region,
   path style, generation and realm from the manifest and binds them through
   `ManagedBackendLedger.bind`, which refuses any other physical profile.
3. A new PostgreSQL container with a new volume is initialised, the role and
   database are created from explicit runtime inputs, and `pg_restore
   --exit-on-error` loads the dump. Exit codes and stderr are captured; any
   error refuses the restore.
4. Transaction-id epoch check: the maximum stored `xid8` across every `xid8`
   column is compared with `pg_current_xact_id()`. If the restored cluster is
   not strictly past it, the container is stopped, `pg_resetwal -x` (and `-e`
   for a nonzero epoch) advances the counter, the container restarts, and the
   check is repeated. Both branches are exercised on every invocation: the
   first positive run seeds on a fresh cluster and needs no advancement; the
   second consumes `--xid-burn` (default 8192) transaction ids before seeding,
   so its stored `xid8` values exceed what the restored cluster allocates and
   the advancement must run. The harness asserts which branch ran in each case
   and the recovered host verifies all content afterwards either way.
5. Sequence check: both sequences are read and must equal the manifest values.
6. The recovered host is a fresh JVM on the production JARs only (no test
   framework on the classpath), with a `ManagedSchemaAccess` that throws on any
   live resolution and no registry directory. It performs every verification
   below and only then writes `READY`. A host whose verification fails never
   advertises ready.

## Positive rehearsal checks

Seeded through production paths (`RepoServices.build(config, bridges,
historicalAccess, schemaAccess, publicationOptions)` over real SQL and RustFS):

- a drive created through `DriveService.CreateDrive` over in-process gRPC with an
  `api_token`;
- a typed document whose `structured_data` is an `Any` of a dynamically built
  type that imports `google/protobuf/timestamp.proto` and `buf/validate/validate.proto`
  with a CEL rule, so retained descriptors carry imports and validation runs;
- two committed revisions with different data; the second tightens the stored
  security so a non-member caller is denied;
- an archive entry with two versions where one rendition is unchanged (shared
  object) and one changes, plus a deduplicated third save;
- recorded before shutdown: receipts, document bytes, revision ids, mutation
  revisions, command and policy digests, raw fragment hashes, archive manifests,
  object keys, provider version ids and etags (read from the ledger, never
  inserted), schema artifact digests and tool identities, sequence values.

Verified after restore in the fresh host: raw fragments byte-equal to the
recorded hashes for both revisions; validated reads equal to the recorded
documents with the recorded command and policy digests; materialized typed
occurrence decoded from the retained descriptor artifact whose sha256 equals the
recorded artifact; archive versions 1 and 2 readable with their frozen metadata
snapshots and shared object key; provider version ids and etags of every bound
object equal to the recorded ones; receipt replay of both recorded requests
byte-equal; non-member caller denied (NOT_FOUND) and unauthenticated gRPC
refused (UNAUTHENTICATED); a new document revision and a new archive version
committed through the ordinary paths with a mutation revision strictly greater
than every restored value and a new revision id, after which the old versions
still read.

## Negative cases

Each case restores a disposable copy from the same sealed backup; no case shares
a volume or database with another or with the positive rehearsal.

| Case | Injection (owned by the run) | Expected observation |
|---|---|---|
| Missing component | provider archive removed from a copy of the backup | preflight refuses, no container created |
| Corrupt checksum | one byte of the SQL dump flipped in a copy | preflight refuses before any resource |
| Unsealed set | `SEALED` removed from a copy | refused |
| Nonempty target | restore into a directory or volume that already holds data | refused; target untouched |
| Missing provider version | the recorded version of revision 1's core part deleted by version id on the disposable restored RustFS | raw and validated reads of revision 1 fail (NOT_FOUND or DATA_LOSS); the latest revision still reads; nothing substitutes the newer version |
| Corrupt retained descriptor | catalog bytes damaged on the disposable restored SQL copy with the immutability trigger disabled by the test | validated read fails with DATA_LOSS; raw (opaque) read still returns bytes; no typed JSON is produced |
| Wrong backend identity | restored host configured with a different endpoint port under the recorded generation; and with a new generation at the recorded endpoint | first: `ManagedBackendLedger.bind` refuses ("already bound to another physical profile"); second: reads refuse because the original generation is not configured on this host |
| Revoked access | second revision's tightened security and a scoped non-member caller | NOT_FOUND after restore; owner still reads |

Refusals preserve the actual failure class and exit code; the harness archives
them.

## Limitations stated

- Offline only. The cut requires every writer process stopped. No online or
  incremental backup, no PITR, no cross-provider or provider-neutral format. The
  provider archive is RustFS's own volume format and is only valid for the same
  image.
- Endpoint identity is part of the recorded backend identity. A restore to a
  different origin needs a new generation and a migration of bindings, which does
  not exist; the harness does not remap identities.
- Checksums detect corruption; they do not authenticate a backup. Encryption and
  authenticity policy are outside this rehearsal.
- A component mismatch that only a verified post-restore read can detect (for
  example a provider version missing from a volume whose archive digest is
  intact) is detected by the host's verification, not by preflight. The host
  stays unready until verification completes.
- Restored reader incarnations and claims are not released by restore.
- Not covered: JCR restoration, public historical restoration RPCs, claimed
  historical execution, pruning, Redis-backed deployments, LocalStack.
