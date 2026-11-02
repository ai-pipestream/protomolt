# Local catalog maintenance

Implementation slice: one explicitly selected document revision. The starting
checkout is main `7aa5f2526`; this is the first recovery-tooling slice, not a backup
or restore system.

Required behavior: verify a revision's manifest and CORE using its retained
schemas, or explicitly rebuild that revision's derived catalog projections.
Verification must not rewrite catalog entries, part bytes, schema artifacts,
operation outcomes or outbox events. A historical revision must never replace the
current entry. Missing/corrupt retained material must fail before a repair.

Reuse and call path: `RepoServiceMain` command dispatch → local
`RepoCatalogMaintenance` → `RepoMaintenanceComposition` (ledger database, drive
ledger, selected blob store, parts catalog, drive-to-stack resolution shared with
the serving host) → `CatalogEntryRebuild` → `DocumentReads`/retained schema
admission and verified stack reads → existing catalog `rewriteRevisionType` and
conditional `rewriteEntry`. No new public RPC, schema resolver, persistence format
or backup authority.

Acceptance: production host saves over PostgreSQL and S3; damage derived lookup
and revision-type rows; audit leaves those rows unchanged; explicit repair restores
them while exact stored manifest/schema/CORE bytes (including unknown fields),
operation outcomes and outbox records survive. Historical repair cannot overwrite a
new current entry. Corruption and insufficient read budget fail without repair.

## Commands and authority

Use the installed repository-service distribution with the same database, stack,
namespace and read-budget environment as the repository being maintained:

```bash
bin/protomolt-repo-service verify-core ACCOUNT GRAPH DATASOURCE_OR_NODE DOCUMENT REVISION
bin/protomolt-repo-service rebuild-entry ACCOUNT GRAPH DATASOURCE_OR_NODE DOCUMENT REVISION
```

All four address components are mandatory. `REVISION` is a positive exact revision;
there is no latest alias, account sweep or implicit repair. The first command audits;
the second explicitly writes derived projections after successful verification.
Malformed arguments fail before repository initialization. Failure exits nonzero
with its cause; success writes a single result line to standard output. Normal host
logging may appear separately.

This is a privileged **local operator** tool. Possession of the configured SQL and
stack credentials is its authority, just as for a database maintenance process; it
is not an account-user command and does not evaluate document ACLs. Keep those
credentials within the operator boundary. The command opens the catalog and the
stores only (`RepoMaintenanceComposition`): the ledger database with its normal
migrations, the drive ledger, the selected byte store and the parts catalog. It
does not build the serving host, so it starts no gRPC/HTTP or in-process listener,
account seeding or lifecycle loop, loads no concept-scheme files, wires no saves or
accession service, registers no internal step credential, and constructs no Kafka
outbox publisher even when `DOCUMENT_PLATFORM_KAFKA_BOOTSTRAP_SERVERS` is set. A
concept-scheme path that the host could not load does not stop the command.
Under qualified managed storage it registers its own reader incarnation, as every
host does, and fences it on exit, but no reader-host execution. Running the command
beside a live host shares that host's database and stores. Audit-only mode
makes no document/projection writes; it is not a promise of a SQL read-only
connection during initialization. No API token is needed because no network
surface is opened.

A verified non-tombstone reports `core=VERIFIED_WITH_RETAINED_SCHEMAS`. This means
manifest identity, CORE length/digest, retained CORE schemas and occurrence evidence
were checked; explicitly opaque occurrences remain opaque and are never described
as typed-valid. A tombstone reports `core=TOMBSTONE_NO_CORE` instead.

Repair first writes the named revision's derived structured-data type, then uses
the existing conditional transaction to replace the current lookup/link/concept
rows **only while that exact revision is still current and unpurged**. A historical
revision or concurrent newer accession reports `current_entry=NOT_CURRENT`; a purge
after the type repair reports `current_entry=PURGED`. The command does not substitute the newer
revision. These are two transactions; if the second fails, the first may already
have committed. Repeating the exact command is safe and creates no accession,
operation outcome or outbox event.

## Limits

This command checks one CORE and its retained decode evidence, under the configured
read memory budget. It does not check PARSED, CHUNKS, EMBEDDINGS or raw blob bytes,
verify every historical revision, validate every catalog column, or test the entire
storage estate. It does not delete, sweep, restore or rewrite authoritative objects.
Missing retained schemas are errors, never replaced with a live registry schema.

Production recovery needs a coordinated operator-owned SQL and object-store backup,
followed by verification of every required part and retained artifact. The isolated
drill below qualifies that sequence for one fixture. These commands repair derived
rows in an existing catalog; they are not a restore mechanism or a portable
repository export format.

## Verification

The focused suite uses real PostgreSQL and LocalStack S3. It covers a separate JVM
running `RepoServiceMain`, audit/repair, historical-current separation, missing
retained schema failure/recovery, bounded read refusal, exact streamed CORE bytes,
and preserved schema/manifest/outcome/outbox records. Its custom payload includes
an int64 field declared in the retained dynamic schema but unknown to the generated
message used by the fixture to encode it. Truly undeclared fields remain subject to
the existing save admission rules; this tool does not weaken those rules.

```bash
flock /work/.gradle-krick.lock ./gradlew \
  :protomolt-repo-service:test --tests '*RepoCatalogMaintenanceTest*' \
  :protomolt-repo-service:integrationTest --tests '*RepoCatalogMaintenanceIT*' \
  checkZeroRegex checkTestNaming \
  :protomolt-repo-accession:javadoc :protomolt-repo-service:javadoc \
  --max-workers=2
```

Result for this slice: 2 unit tests and 5 integration tests passed, zero failures or
skips; both guards and Javadoc tasks passed. This is local qualification, not a
production backup restore or deployment.

`RepoMaintenanceCompositionIT` covers the composition boundary. With an
unreachable Kafka bootstrap address and a concept-scheme path that does not exist,
the serving `RepoServices.build` constructs a Kafka producer (visible as a
`kafka.producer` MBean) or fails on the scheme file, while the maintenance
composition verifies a saved revision with no producer MBean, no producer,
transport or lifecycle thread, and no internal step credential in the catalog. A
child JVM running `RepoServiceMain verify-core` under the same environment exits
zero with its result line.

```bash
flock /work/.gradle-krick.lock ./gradlew \
  :protomolt-repo-service:integrationTest --tests '*RepoMaintenanceCompositionIT*' \
  --max-workers=2
```


## Isolated offline restore drill

`RepoBackupRestoreIT` adds recovery qualification from starting commit `3c7239777`.
Required behavior: recover two revisions, all five part families, original raw blob
bytes, retained schemas and operation outcomes after complete catalog and object
loss. A CORE-only second save must retain the other four parts; retrying either
original operation after restore must return its original outcome without creating
new revisions or events.

Reuse and call path: public `DocumentService` saves → existing accession catalog,
S3 objects and retained schema artifacts → closed services → PostgreSQL `pg_dump`
→ exact S3 bucket/key/bytes/metadata backup → empty test database and object store
→ PostgreSQL `psql` restore and exact object replacement → fresh `RepoServices`
without the custom schema mounted → public historical/current reads, individual
selected-part streams, raw blob stream, retained descriptors and operation outcomes
→ `RepoCatalogMaintenance verify-core` for both revisions. The fixture uses the
existing semantic digest/part codec and does not introduce a repository backup
format, provider fallback, public API or new schema resolver.

The drill owns dedicated PostgreSQL 18 and LocalStack 3.8 Testcontainers. Every
service is closed before the backup. It exports a standard SQL dump to a temporary
file, copies object bytes into the test fixture, deletes every test object version
and delete marker (rather than merely hiding current objects), drops
and recreates the test database, then restores both. No destructive operation uses
an externally configured database or object endpoint. The empty database is checked
before restore; SQL-only restoration must fail CORE verification until objects are
restored. Successful verification must preserve exact manifests, schema artifacts,
stack records, object references, catalog entries, outcomes and outbox payloads.

The recorded endpoint, bucket, namespace and keys remain identical throughout.
This models loss and recovery of an existing namespace; it does **not** qualify
migration to another endpoint/bucket or rewriting stable stack identity. It also
does not qualify online snapshot consistency, point-in-time recovery, provider
version-history restoration, physical container replacement, or a production
backup schedule. Object bytes are small test fixtures held in memory; this is not
a large-store backup implementation. `verify-core` keeps its CORE-only scope; the
drill separately reads and compares every selected part and raw blob through the
public API. The custom typed payload retains its exact int64 value and Any bytes
without requiring a live schema mount after restoration.

Run the additional drill with:

```bash
flock /work/.gradle-krick.lock ./gradlew \
  :protomolt-repo-service:integrationTest --tests '*RepoBackupRestoreIT*' \
  checkZeroRegex checkTestNaming --max-workers=2
```

Local qualification: the new restore drill and five existing maintenance integration
tests passed, alongside two maintenance unit tests (8 total, zero failures, errors
or skips). Zero-regex, Testcontainers naming and service runtime-boundary checks
passed. This is an isolated Testcontainers drill, not a production restore.
