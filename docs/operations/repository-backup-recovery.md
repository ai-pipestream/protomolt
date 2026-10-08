# Repository offline backup and recovery rehearsal

This guide covers exactly what `verification/repository-recovery/` qualifies: an
offline backup of one repository deployment (PostgreSQL ledger plus one pinned
RustFS volume) and its restore into new resources, verified by a fresh
production-JAR host. It is a rehearsal harness and the tested procedure behind
it. It is not an online backup service, not a public restore API, not a
provider migration and not a JCR or claimed-historical-execution feature. The
design and the backup boundary are in
[repository-backup-recovery.md](../design/repository-backup-recovery.md).

## Supported scope

| Item | Supported |
|---|---|
| Mode | Offline only: every writer process stopped and drained before capture |
| Ledger | PostgreSQL `postgres:18-alpine` (dump and restore run inside that image) |
| Provider | `rustfs/rustfs:1.0.0-beta.11-preview.1`, single `/data` volume, versioned buckets |
| Identity | The restored provider must be reachable at the recorded endpoint origin, region and path style under the recorded backend generation and storage realm |
| Content | Typed documents with retained descriptors, archive versions, receipts, sequences, claims and pins, restored verbatim |
| Buckets | Provisioned by the host with versioning enabled and verified; an existing bucket is adopted by enabling versioning, or refused if S3 does not report it enabled |
| Not supported | Online or incremental backup, point-in-time recovery, cross-provider or provider-neutral formats, endpoint or generation remapping, LocalStack, Redis-backed deployments, backup authenticity or encryption |

## Prerequisites

- Docker reachable by the invoking user. The three images are pulled if absent.
- Java 25 for the Gradle toolchain. No PostgreSQL client tools are needed on the
  host; `pg_dump`, `pg_restore` and `pg_resetwal` run in the pinned image.
- A new or empty output directory. The harness refuses anything else.

## Running the rehearsal

From a clean checkout:

```shell
verification/repository-recovery/run.sh --out /tmp/repository-recovery-$(date +%s)
```

The script builds the observed admission runtime bundle with the root build,
builds the harness through its composite build on the same JARs, and runs two
positive rehearsals followed by every negative case. Options: `--runs N`,
`--skip-negatives`, `--keep` (leave containers and volumes for inspection).
The second positive run consumes `--xid-burn` transaction ids (default 8192)
before seeding so that its restore has to take the `pg_resetwal` branch; the
first run covers the branch where no advancement is needed. With
`PROTOMOLT_GRADLE_LOCK=<file>` set, the Gradle steps run under `flock` on that
file so parallel builds of one checkout on the same machine are serialized.
Exit code 0 and the final line `REPOSITORY_RECOVERY_REHEARSAL_OK` mean every
check passed; any failure leaves a nonzero exit and the failing check in
`markers.log` and `results.xml` of the run directory.

Each positive run creates its own Docker network, volumes and containers named
`protomolt-recovery-<id>-...`, its own database credentials, provider keys and
operator token (generated per run, passed only through the environment, redacted
from logs and never written to the manifest).

## What a positive run does

1. Starts source PostgreSQL and RustFS, then a seed host JVM on the production
   JARs that creates drives, publishes two typed revisions of one document
   (dynamic type with imported `timestamp.proto` and a CEL rule; the second
   revision revokes the public grant), creates an archive entry with two
   versions sharing one rendition object, and records every identity the
   responses and the ledger report into `backup/identities/`.
2. Waits for the seed host to exit, proves no other client backend is connected,
   runs `pg_dump --format=custom`, stops both containers, archives the stopped
   RustFS volume with `tar`, writes `manifest.json` and seals the set.
3. Removes the source containers and proves the database refuses connections and
   the provider port is free.
4. Restores: preflight (seal, manifest digest, required components, empty
   targets), new volumes, provider volume extracted, new RustFS on the recorded
   port, new PostgreSQL, `pg_restore --exit-on-error`, transaction-id epoch check
   (advancing with `pg_resetwal` only when the restored cluster is not past
   every stored `xid8`), sequence check.
5. Runs the recovered host JVM with no schema registry. It verifies raw and
   validated historical reads, the materialized typed occurrence decoded from
   the retained descriptor artifact, archive versions and frozen metadata,
   provider bytes and version ids read directly with the recorded version id,
   receipt replay, the access matrix, the authenticated transport, then commits
   a new document revision (through a registry rebuilt from the retained catalog
   bytes) and a new archive version, re-reads the old versions and only then
   writes `READY`.

## Backup set layout

```
backup/
  manifest.json          format, images, backend identity, migration level, sequences, row counts, component digests
  SEALED                 digest of manifest.json, written last
  sql/ledger.dump        pg_dump custom format (rows, sequences, functions, triggers)
  provider/rustfs-data.tar
  schema/catalog.tsv     normalized schema artifact digests (account, sha256, size)
  identities/            seed-recorded identities and protobuf receipts (evidence, nonsecret)
```

This layout is private to this procedure. It is not a wire contract and carries
no credentials.

## Restoring by hand

The harness is the tested path; these are the steps it performs, for an operator
who has to run them individually. Replace names as needed and keep the order.

1. Stop every repository host, worker and cleanup process that can reach the
   ledger or the provider. Confirm with `pg_stat_activity` that no client
   backend remains.
2. `pg_dump --format=custom --no-owner --no-privileges` of the ledger database
   from a container of the same PostgreSQL image.
3. Stop PostgreSQL and RustFS. Archive the RustFS volume with
   `tar --numeric-owner -cf` from a helper container. Record SHA-256 digests of
   every file and seal the set.
4. To restore, verify the seal and every digest first. Refuse a nonempty target
   volume or directory.
5. Extract the provider archive into a new volume and start RustFS on it at the
   recorded endpoint origin. The original provider must already be stopped;
   the recorded origin is part of the backend identity and the host refuses any
   other.
6. Start a new PostgreSQL, create the role and database from your own inputs,
   and `pg_restore --exit-on-error --no-owner --no-privileges`.
7. Compare the restored cluster's next transaction id with the largest value in
   any `xid8` column. If it is not strictly greater, stop the cluster and run
   `pg_resetwal -e <epoch> -x <xid>` on the data directory as the `postgres`
   user, creating the `pg_xact` segment first if it does not exist, then start
   it again and repeat the comparison. The harness performs and checks this
   step on its second positive run.
8. Compare both sequences with the manifest, then start the host with the
   recorded generation, realm and endpoint. Verify reads before announcing
   readiness.

## Rollback and cleanup

Every resource a run creates is removed on completion unless `--keep` was given.
To roll back a manual restore, stop and remove the restored containers and
volumes; the backup set and the source volumes are never modified by a restore
or by a refused restore, which the harness proves by digesting them before and
after. A restored cluster is a new cluster: nothing in it points back at the
source.

## Evidence

Each run directory holds `commands.log` (every external command with its exit
code, secrets redacted), `seed-host.log`, `recovered-host.log`, `markers.log`,
`results.xml`, the Docker logs of each container, the sealed backup set and the
restore directory. `summary.json` at the output root lists the outcome of every
run and negative case. The dated evidence folder under `docs/evidence/repository/`
archives these for the qualified runs.
