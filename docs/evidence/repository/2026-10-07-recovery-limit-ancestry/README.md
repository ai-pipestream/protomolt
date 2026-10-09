# Older installation corruption blocks recovery-limit decisions

Base: `8a6d97463f594bd271acf930ecb5f75986d82475`. Tests and documentation only;
production code, migration and protobuf contracts are unchanged.

Two PostgreSQL 18 cases build 16 real retained capture batches and two genuine
V93 installation edges, using actual expiry and V98 supersession between them.
The isolated fixture then corrupts the older edge's predecessor-preparation digest
or command digest. Its immutable guard is disabled only for that administrative
mutation and re-enabled in the same transaction before invoking the handler.

The newest installed plan still confirms exactly. The production decision handler
must therefore inspect older ancestry; it reports FAILED_PRECONDITION with
`ancestry is incomplete or differs`. No decision/rejection pair, execution,
historical activation, additional batch or drain appears. Claim/owner leases
remain unchanged and the payload budget is fully released.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalLimitAncestryIT' \
  --max-workers=2 --console=plain
```

Both tests passed with zero failures/errors/skips in 30s. `results.tar.gz` holds
JUnit XML, `gradle.log` the build output, and `source-sha256.txt` the tested source
fingerprints. Sol reviewed the test and confirmed that it exercises older ancestry
rather than merely invalidating the immediate plan; no blocker was found.

These are representative older-link corruption cases, not an exhaustive database
corruption suite. Initial provider observations are synthetic; SQL and runtime
handlers are real. No deployment or public recovery is qualified. Root release
still requires a separate atomic terminal/abandonment proof, complete retained
coverage and every capture's drain evidence. A V110 rejection alone grants no
permission to delete retained roots.
