# Exhausted activation versus a terminal recovery decision

Base: `7f7218814a591a56a9aa4278fe36602da3578033`. Production code, SQL and
protobuf contracts are unchanged. The added case extends
`RepositoryHistoricalLimitConcurrencyIT`; source fingerprints are included.

At the actual 16-batch capture bound, a decision transaction is held immediately
before JDBC commit after its sidecar/rejection pair exists tentatively. A second
transaction invokes the real historical activation handler with a fresh local
source capture. PostgreSQL reports its V94 INSERT waiting on the first backend
via `pg_blocking_pids`. Releasing the first transaction commits the terminal
decision. The activation then fails with `Closed successor cannot activate`.

This checks terminal visibility after the lock wait under READ COMMITTED, not
only Java call ordering. The decision receipt replays exactly. One terminal pair
exists, with no V94 execution, V109 historical activation, seventeenth durable
capture batch or capture drain. Claim/owner leases remain unchanged and the
shared payload budget returns to zero. A local source reader is intentionally
open during the attempt and closed by the fixture; the assertion concerns
absence of partial **durable** activation/capture state.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalLimitConcurrencyIT' \
  --max-workers=2 --console=plain
```

All three cases passed with zero failures/errors/skips in 40s: the new activation
race and the two decision commit/rollback races. `results.tar.gz` contains the
JUnit XML and `gradle.log` the build output. Sol reviewed the race and its
READ COMMITTED visibility expectations with no blocker.

The database and handlers are real; initial provider observations are synthetic.
This is not a storage performance or live network test. At an exhausted bound,
activation is never a valid competing winner. The existing bound tests already
exercise failed activation followed by a successful decision; this new case
adds a decision-first lock wait. Expired-owner supersession, corruption,
expired-lease replay and source-root release remain separate qualifications.
No public historical execution or deployed recovery is enabled here.
