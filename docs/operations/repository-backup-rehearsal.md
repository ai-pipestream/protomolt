# Repository backup and recovery rehearsal

This guide covers exactly what `RepositoryBackupRehearsalIT` qualifies: an offline
(QUIESCED) backup of one repository deployment, PostgreSQL ledger plus one pinned RustFS
volume, and its restore into new, disposable stores verified by a fresh production-JAR
host. It is a rehearsal harness and the tested procedure behind it. It is not an online
backup service, not a public restore API, not a provider migration and not a statement of
production backup readiness. The design obligations it checks are in
[repository-composition.md](../design/repository-composition.md) ("Backups must preserve
the revision sequence state along with rows") and
[repository-revision-pruning.md](../design/repository-revision-pruning.md) (V118/V119
coverage states). The procedure and backup-set layout descend from the draft harness on
GitHub PR #413 (`agent/repository-backup-restore`, head `610eb15c2`, base
`refactor/repository-composition`), which predated V112 through V119 when this harness
was written; it was re-implemented inside the ledger test package rather than
cherry-picked. #413 later landed separately as the standalone
`verification/repository-recovery/` harness, whose operator guide is
[repository-backup-recovery.md](repository-backup-recovery.md). The two harnesses check
the same offline boundary from different entry points; this one runs as a JUnit suite
through V119 with the negative cases listed below.

## Supported scope

| Item | Supported |
|---|---|
| Mode | Offline only: every writer process exited and its SQL sessions gone before capture |
| Ledger | PostgreSQL `postgres:18-alpine`, Flyway level V119; dump and restore run inside that image |
| Provider | `rustfs/rustfs:1.0.0-beta.11-preview.1`, single `/data` volume, versioned buckets |
| Identity | The restored provider must be served at the recorded endpoint origin, region and path style under the recorded backend generation and storage realm |
| Content | Typed documents in two accounts, current and historical revisions, retained descriptors including a nested Any type, receipts, a pending journaled operation with V118/V119 states, restored verbatim |
| Not supported | Online or incremental backup, point-in-time recovery, cross-provider or provider-neutral formats, endpoint or generation remapping, LocalStack, Redis-backed deployments, archive entries, backup authenticity or encryption |

## Consistency boundary

The snapshot is QUIESCED, never online. The harness proves the cut rather than assuming it:

1. The only writer is the seed host JVM. It closes its `RepoServices` (drains schema
   workers, lifecycle threads, provider handles and readers) and exits with code 0.
2. After the exit, `pg_stat_activity` must show no client backend other than the capture
   connection, `repository_reader_incarnations` must hold no ACTIVE row,
   `document_read_pins` must be empty and no part attempt may be PLANNING or STAGING.
   Durable journal state (the pending operation's claims, preparations and expired leases)
   is data, not process activity, and is kept.
3. Every base table is fingerprinted (row count plus an order-independent digest of all row
   text), together with sequences, the largest stored `xid8` and the V118/V119 invariants.
4. `pg_dump --format=custom` runs from a sibling container of the same image. Afterwards the
   backend check is repeated and every table fingerprint must be unchanged, and the restore
   later compares the restored tables against the same fingerprints: a write landing in the
   dump window would be detected and the set refused, never captured silently.
5. PostgreSQL and RustFS are stopped (clean `exited` state), then the RustFS volume is
   archived with `tar` from a helper container. No provider effect can arrive after the
   cut: the sole writer exited before the dump and the provider process was stopped
   before its bytes were read.
6. Component digests, image ids, backend identity, sequences and row counts go into
   `manifest.json`; `SEALED` holds the manifest digest and is written last.

Independent SQL dumps and object copies taken while writers run would not be a consistent
cut; this procedure does not claim online point-in-time recovery.

## Provider identity

S3 version ids are provider-issued. The provider snapshot is therefore the stopped RustFS
volume itself, restored byte for byte into a new volume and served again at the recorded
endpoint; the catalog's `provider_version` and `etag` values are then read back by exact
version id. The harness also demonstrates the gap: copying an object's bytes into a fresh
provider mints a different version id (`identity-gap` case), so GET-old/PUT-new cannot
satisfy the recorded identities. Remapping provider identities behind receipts is not
implemented and is not attempted here.

## Running the rehearsal

Prerequisites: Docker reachable by the invoking user (both pinned images are pulled by
exact tag if absent), the Java 25 toolchain, and no other repository rehearsal on the host
using the same advisory lock.

```shell
flock -w 600 /tmp/protomolt-repository-qualification.lock \
  ./gradlew -I repo/container/src/test/resources/backup-recovery/rehearsal.init.gradle \
  :protomolt-repo-container:test --tests '*RepositoryBackupRehearsal*' --max-workers=2 --console=plain
```

The init script wires the module's ordinary `test` task without editing its build file:
it adds the observed admission transport runtime bundle (`admissionTransportRuntimeInventory`)
and the production host classpath (module JAR plus `assessmentTransportRuntime`, the same
closure `admissionStorageTest` uses), and marks the task never up to date so a rehearsal is
always a fresh run. `build.gradle` excludes the rehearsal from the ordinary `test` task and
the init script re-includes it, so the same command without `-I` matches no tests and the
build fails; it never reports skipped cases. A passing rehearsal has zero skipped cases in its
archived results.

Options: `-Pprotomolt.rehearsalKeep=true` leaves each case's containers and volumes for
inspection (they are otherwise removed at the end of each case). Evidence is always
preserved under `repo/container/build/backup-rehearsal/<timestamp>/`.

## What one positive run does

1. Starts source PostgreSQL and RustFS on fresh named volumes with run-scoped credentials.
2. Runs the seed host JVM on the production JARs plus the compiled host classes. It
   creates versioned buckets and drives for two accounts, installs typed policies, publishes
   `alpha-v1` (public read), `alpha-v2` (public grant revoked, new data) and `beta-v1`,
   each a dynamic `Record` carrying a nested `Any` of a separately defined `Attachment`
   type, and journals a publication that never completes: initial claim (V118 certificate),
   two coordinator handoffs installing unactivated successors (V93), the first verified by
   V119 lineage, the second left unresolved. It records receipts, documents, raw fragment
   digests, validated bindings, root and nested occurrence selections, provider version ids
   and etags, catalog digests, sequences, row counts and the coverage chain.
3. Verifies everything once while live with the registry closed, then exits.
4. Takes the QUIESCED cut above and seals the set.
5. Removes the source containers and proves the database refuses connections and the
   recorded provider port is free; digests the stopped source volume.
6. Restores: preflight, new volumes, provider volume extracted, RustFS on the recorded
   port, new PostgreSQL, `pg_restore --exit-on-error`, transaction-id check (the restore is
   refused if the restored cluster is not past every stored `xid8`), sequence and
   migration-level checks, per-table fingerprint comparison against the source capture,
   V118/V119 invariant comparison by identity.
7. Runs the recovered host JVM with no schema registry. It compares row counts, reader
   incarnations, backend profile, sequences, catalog digests and migration level; reads
   raw and validated history for every revision; decodes the root and nested occurrences
   from the retained artifacts; reads every provider object by recorded version id; replays
   every receipt byte-equal; checks the access matrix (member, same-account stranger,
   foreign account, cross-account member); exercises the authenticated transport; verifies
   the pending operation is still pending, discoverable and verifiable (V119 depth 2) and
   can take another successor without executing; then commits a new revision through a
   registry rebuilt from the retained catalog bytes and re-reads the old ones. Only then
   it writes `READY`.
8. Proves the source volume and every backup component are unchanged.

## Backup set layout

```
backup/
  manifest.json            format, images, backend identity, migration level, sequences, row counts, component digests
  SEALED                   digest of manifest.json, written last
  sql/ledger.dump          pg_dump custom format (rows, sequences, functions, triggers)
  provider/rustfs-data.tar stopped RustFS volume, numeric ownership preserved
  schema/catalog.tsv       normalized schema artifact digests (account, sha256, size)
  catalog/fingerprints.json per-table row fingerprints, sequences, max xid8, V118/V119 invariants
  identities/              seed-recorded identities and protobuf receipts (evidence, nonsecret)
```

This layout is private to this procedure. It is not a wire contract and carries no
credentials: database password, provider secret and operator token are generated per run,
passed only through the child JVM environment and redacted from `commands.log`.

## Restoration order by hand

The harness is the tested path; these are the steps it performs, in the order that must
be kept.

1. Stop every repository host, worker and cleanup process that can reach the ledger or the
   provider. Confirm with `pg_stat_activity` that no client backend remains and that no
   reader incarnation is ACTIVE.
2. `pg_dump --format=custom --no-owner --no-privileges` of the ledger database from a
   container of the same PostgreSQL image.
3. Stop PostgreSQL and RustFS. Archive the RustFS volume with `tar --numeric-owner -cf`
   from a helper container. Record SHA-256 digests of every file and seal the set.
4. To restore, verify the seal and every digest first. Refuse a nonempty target directory
   or volume.
5. Extract the provider archive into a new volume and start RustFS on it at the recorded
   endpoint origin. The original provider must already be stopped; the recorded origin is
   part of the backend identity and the host refuses any other.
6. Start a new PostgreSQL, create the role and database from your own inputs, and
   `pg_restore --exit-on-error --no-owner --no-privileges`.
7. Compare the restored cluster's next transaction id with the largest value in any
   `xid8` column. If it is not strictly greater, do not start a host. V119 lineage proofs
   and V93 install edges compare `install_xid` with the current transaction; a cluster whose
   counter has not advanced could refuse valid proofs or admit an invalid same-transaction
   claim. Advancing the counter is a manual `pg_resetwal` procedure following the PostgreSQL
   documentation's safe-value rules for `-x` and `-e`; the harness refuses this state and has
   not exercised that procedure (both archived runs restored a cluster already past the
   stored values), so it is not part of the qualified runbook.
8. Compare sequences and migration level with the manifest, and the per-table fingerprints
   with `catalog/fingerprints.json`; then start the host with the recorded generation,
   realm and endpoint. Verify reads before announcing readiness.

## Negative cases

Each case restores a disposable copy of run-1's sealed set into its own stores.

| Case | Injection | Required observation |
|---|---|---|
| missing-component, corrupt-checksum, unsealed, mixed-captures | component removed, one byte flipped, `SEALED` removed, run-2's provider archive placed in run-1's set | preflight refuses before any resource is created |
| nonempty-target | existing file in the restore directory; occupied volume | refused; target untouched |
| inconsistent-snapshot | run-2's provider archive in run-1's set, resealed | every recorded version absent; history reads refuse; receipts and the pending chain still replay from the catalog; no READY |
| missing-version | newer version put under the same key, then the recorded version of `alpha-v1`'s CORE deleted | raw, validated and materialized reads of `alpha-v1` refuse; the newer version is never substituted; other revisions still read |
| corrupt-descriptor | retained attachment artifact bytes damaged (digest CHECK dropped on the copy) while a live registry holding the correct definition is armed | raw read still byte-equal; validated and nested materialized reads DATA_LOSS; root occurrence still materializes; the live registry is never consulted |
| missing-descriptor | retained attachment artifact rows deleted with referential triggers disabled, live registry armed | as above |
| wrong-endpoint | recorded generation at a different endpoint | `ManagedBackendLedger.bind` refuses: already bound to another physical profile |
| new-generation | new generation at the recorded endpoint | host binds; every history read refuses because the original generation is not configured |
| wrong-credentials | wrong provider secret | direct GET refused by the provider (403, signature mismatch); the host composes without a provider call; every history read refuses with the provider rejection |
| wrong-database-credentials | wrong ledger password | host exits nonzero on authentication failure; no READY |
| identity-gap | the recorded version's bytes read from a restored volume and put into a fresh provider | same digest, different provider-issued version id; a GET by the recorded version id on the fresh provider fails |

No case falls back to another backend, serves a newer version in place of a missing one,
invents typed content, or advertises readiness.

## Rerunning without touching real data

Every resource a run creates carries a `protomolt-rehearsal-<id>` prefix: network,
volumes, containers, buckets, accounts, generation. Nothing reads or writes a deployed
ledger or provider; the hosts are handed only the run's own endpoints through the
environment. A run removes only what it created. The evidence directory is kept on
success and failure.

## Backup verification versus production backup readiness

This rehearsal verifies that a QUIESCED backup of this deployment shape restores with
exact identities. It does not establish production readiness: it does not schedule or
retain backups, does not authenticate or encrypt them, does not cover online capture, large
volumes, cross-provider recovery, Redis profiles, archive entries or performance, and it
has not been run on hosted CI. Those remain separate work.
