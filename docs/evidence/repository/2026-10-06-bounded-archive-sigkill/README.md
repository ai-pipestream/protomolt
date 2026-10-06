# Bounded archive service death before publication

`BoundedArchiveProcessIT` launches the production main in a separate JVM with
PostgreSQL and Redis containers. The new case holds the existing entry's SQL
publication lock, observes the candidate's waiting SQL statement through
`pg_blocking_pids`, and requires a separately committed VERIFIED upload. An
independent Redis read checks its exact bytes before killing the service JVM.

SIGKILL must yield Linux exit 137 and fail the pending RPC with UNAVAILABLE.
After restart, the old committed value remains readable. Retrying the interrupted
request creates version 2 with a different physical object. Stale expected version
1 remains ABORTED; matching content with expected version 2 deduplicates.

The test waits through the production five-minute lease without altering timestamps.
The abandoned object must remain VERIFIED before expiry and never gain an archive
version reference. Normal restarted-host recovery must mark it DELETED and a direct
Redis read must report absence. The current version must remain readable and its
matching save must still deduplicate. This is service-process recovery: neither
PostgreSQL nor Redis is killed, and no provider success is fabricated.

Sol reviewed the publication barrier and lease semantics. The first run caught a
test assumption that stale expected-version saves would deduplicate; the corrected
test explicitly requires ABORTED and uses the current expected version for dedup.

```sh
./gradlew :protomolt-repo-service:test \
  --tests '*BoundedArchiveProcessIT' --tests '*RepoBoundedArchiveMainTest' --console=plain
```

All 10 process/launcher tests passed in 5 minutes 42 seconds. Log:
`/tmp/protomolt-bounded-archive-sigkill-qualified.log`.
The final focused rerun also passed with an explicit post-cleanup retained-version-1
read, proving that both the earlier retained bytes and current version survive
abandoned-object reclamation. Log: `/tmp/protomolt-bounded-archive-sigkill-history.log`.
Hosted CI, merge, deployment, provider-crash durability and document execution
claim transfer remain separate gates.
