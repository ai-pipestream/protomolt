# Redis process-crash qualification

Source base: `665a9b9f6c15d148183f8f8190b20d54bdaff668`, with the test identified
by `source.sha256`. Run on October 5, 2026, America/New_York (October 6 UTC).

Command: `./gradlew :protomolt-repo-blob-redis:check --console=plain`.
Result: 36 tests, zero failures/errors/skips; runtime dependency boundary gate
passed. The two new restart tests use real Redis containers and fresh SPI handles.
`check.log.gz`, `test-suites.txt` and the restart suite XML retain the results.

Image: `redis:7-alpine`, resolved image/digest
`sha256:e7723ff73d963f5cc6d9c4643ea3d989527a402a319239054e9472a7fb9219a2`.
The fixture verifies running-server configuration, kills the Redis process with
SIGKILL, checks exit 137, and restarts the same container/storage. It neither saves
a snapshot nor requests a graceful shutdown before either crash.

With AOF and `appendfsync always`, acknowledged writes and copies retain exact
bytes, content type, ETag, packed custom metadata and non-expiring TTL. An object
first proven present after restart is then reclaimed; another crash/restart proves
the deletion persists while unrelated objects remain. With persistence disabled,
non-expiring objects disappear, demonstrating the limit of that capability flag.

This is process-crash evidence while the Docker host and storage stay running.
It does not qualify power loss, replication, failover, backups, eviction policy
changes, delayed provider effects, or managed repository lifecycle. No production
capability or service activation changed. No performance claim is made.
