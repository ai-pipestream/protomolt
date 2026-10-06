# Bounded archive transport drain

The bounded host now drains admitted archive RPCs, puts and reads before closing
gRPC transport and its executor. Admission closes first. A timeout retains all
resources for retry; a second idle check precedes provider release. Interrupts
remain visible with the original phase-specific diagnostics.

The new regression failed against the previous ordering: a 100ms close remained
inside transport shutdown rather than returning its archive drain failure within
two seconds. Red log: `/tmp/protomolt-archive-drain-red.log`.

With the fix, a real authenticated Netty call backed by PostgreSQL and Redis stays
alive beyond the former ten-second transport cancellation interval. The fixture
pauses after the actual Redis write returns, holding the operation's completion.
Timed close returns a drain failure, new RPCs receive UNAVAILABLE, and the provider
and byte reservations remain owned. Releasing the pause produces a successful
version-one response; a later close releases all resources. This is not a delayed
Redis network command or a child-process SIGTERM test.

```sh
./gradlew :protomolt-repo-service:test \
  --tests '*BoundedArchiveTransportIT' \
  --tests '*BoundedArchiveHostIT' \
  --tests '*GrpcServerLifetimeTest' \
  --tests '*ManagedArchiveReadShutdownIT' --console=plain
```

All 24 tests passed, with zero failures, errors or skips, in 25 seconds. Log:
`/tmp/protomolt-archive-drain-qualified.log`. Sol reviewed the ordering, queued-call
rejection, retry and resource ownership with no blocker. The final run also covers
existing read-shutdown interruption and failure behavior.

The standalone archive launcher still needs implementation and process-level
qualification. No RustFS performance, hosted CI, deployment or crash-durability
claim follows from this checkpoint.
