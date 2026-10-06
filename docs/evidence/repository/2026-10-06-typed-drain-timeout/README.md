# Typed repository drain timeouts

The standalone archive launcher needs to keep accepted work alive through a timed
shutdown without retrying arbitrary cleanup failures. A package-private
`RepositoryDrainTimeoutException` now distinguishes expiration of lifecycle-worker,
archive-RPC, archive-put and archive-read waits. Existing `IllegalStateException`
callers remain compatible. Interruption preserves its flag and cause; provider
release failures propagate unchanged.

The focused suite checks every timeout phase, retention until actual completion,
and the distinct interruption and release-failure paths. The archive host/transport
cases use real PostgreSQL and Redis; the lifecycle-worker test explicitly models
an uncooperative worker with a latch. Sol reviewed the exception boundaries and
suggested the included reader-phase assertion.

```sh
./gradlew :protomolt-repo-service:test \
  --tests '*LifecycleShutdownTest' \
  --tests '*BoundedArchiveTransportIT' \
  --tests '*BoundedArchiveHostIT' \
  --tests '*ManagedArchiveReadShutdownIT' --console=plain
```

The focused suite passed. Log: `/tmp/protomolt-typed-drain-timeout-qualified.log`.
The production launcher, shutdown retry loop and child-process SIGTERM test remain
pending. The new type alone proves neither process recovery nor crash durability.
It does not promise all transports remain open, since one drain check runs after
transport shutdown. Hosted CI and deployment are separate from this verification.
