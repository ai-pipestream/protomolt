# Concurrent standalone archive bootstrap

The test launches two production `RepoBoundedArchiveMain` JVMs against the same
PostgreSQL and Redis instances. They use the same account/drive and provider
identity but different default storage locations. A fixture trigger takes an
advisory transaction lock inside each actual drive INSERT. The observer requires
two matching blocked INSERT statements in `pg_stat_activity` and `pg_blocking_pids`
before releasing the lock, so an ordinary sequential lookup cannot satisfy the test.

Both processes must become ready. SQL contains exactly one drive row at one winning
location. One process creates an archive and writes an entry; the other reads it and
retries the write with the same complete result and `deduplicated=true`. The stored
drive location remains unchanged. Cleanup releases the lock, stops the child
processes and removes the fixture trigger/function.

The focused concurrent-bootstrap test passed in 15 seconds. Log:
`/tmp/protomolt-concurrent-archive-bootstrap.log`. Sol reviewed the barrier,
assertions and cleanup. No production behavior change was needed.

The full launcher regression then passed all nine tests with zero failures, errors
or skips in 42 seconds:

```sh
./gradlew :protomolt-repo-service:test \
  --tests '*BoundedArchiveProcessIT' --tests '*RepoBoundedArchiveMainTest' --console=plain
```

Log: `/tmp/protomolt-concurrent-bootstrap-qualified.log`.

This is Linux process qualification with real adapters, not a throughput or replica
scaling claim. Delayed Redis replies, exhaustive unmounted RPCs, forced-kill recovery
and minimal runtime dependency graphs remain separate requirements.

The provider review also verified the selected Jedis 8.0.1 constructor's two-second
socket timeout from the locally resolved dependency bytecode. Do not extend that
production timeout solely to manufacture a successful ten-second provider delay.
The next provider-I/O test needs a shorter embedded drain deadline; longer faults
must test failure and reconciliation.
