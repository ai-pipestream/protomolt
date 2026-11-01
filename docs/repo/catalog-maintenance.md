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
`RepoCatalogMaintenance` → `RepoServices.v2Composition` →
`CatalogEntryRebuild` → `DocumentReads`/retained schema admission and verified stack
reads → existing catalog `rewriteRevisionType` and conditional `rewriteEntry`.
No new public RPC, schema resolver, persistence format or backup authority.

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
credentials within the operator boundary. The command opens the usual service
composition, including normal database migrations and internal credential setup,
but starts no gRPC/HTTP listener, account seeding or lifecycle loop. Audit-only mode
makes no document/projection writes; it is not a promise of a SQL read-only
connection during host initialization. No API token is needed because no network
surface is opened.

A verified non-tombstone reports `core=VERIFIED_WITH_RETAINED_SCHEMAS`. This means
manifest identity, CORE length/digest, retained CORE schemas and occurrence evidence
were checked; explicitly opaque occurrences remain opaque and are never described
as typed-valid. A tombstone reports `core=TOMBSTONE_NO_CORE` instead.

Repair first writes the named revision's derived structured-data type, then uses
the existing conditional transaction to replace the current lookup/link/concept
rows **only while that exact revision is still current and unpurged**. A historical
revision, concurrent newer accession or purge reports
`current_entry=NOT_CURRENT_OR_PURGED`. The command does not substitute the newer
revision. These are two transactions; if the second fails, the first may already
have committed. Repeating the exact command is safe and creates no accession,
operation outcome or outbox event.

## Limits

This command checks one CORE and its retained decode evidence, under the configured
read memory budget. It does not check PARSED, CHUNKS, EMBEDDINGS or raw blob bytes,
verify every historical revision, validate every catalog column, or test the entire
storage estate. It does not delete, sweep, restore or rewrite authoritative objects.
Missing retained schemas are errors, never replaced with a live registry schema.

A complete disaster-recovery drill still needs a coordinated operator-owned SQL and
object-store backup restored into isolated infrastructure, then all required parts
and retained artifacts verified. This slice repairs derived rows in an existing
catalog; it is not that restore mechanism or a portable repository export format.

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
