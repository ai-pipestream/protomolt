# Historical publication after writer process exit

Base: 07bf06677. The new restart case passed alone in 28s. The final regression
run passed in 2m: three aggregate tests, zero failures or skips.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.coldProcessRestart' --tests '*HistoricalRuntimeQualificationTest.coldOwner' --tests '*HistoricalRuntimeQualificationTest.selfSupersession' --max-workers=2 --console=plain
```

The writer uses real PostgreSQL and LocalStack, commits initial historical
capture/registration and START, then exits with `Runtime.halt(23)` without cleanup.
The driver waits for that exact process and its uniquely named SQL sessions to
exit before starting the recovery JVM. The writer checks its actual database
application name before the checkpoint. XML includes the driver's confirmed
exit/session boundary and recovery-success markers.

The owner-readable request file contains only the protobuf command and resubmitted
upload bytes. An exact property whitelist excludes retention records, placements,
seeds, descriptors, historical bytes and authority. The synthetic scoped credential
binding is injected separately by the test host and checked against SQL authority.
Recovery does not reprovision the object-store namespace.

The recovery JVM starts with an empty registry, discovers the expired bound
predecessor, and loads the original anchor through cold preparation. It captures
sources and reads their exact provider versions with size/checksum checks.
Historical schema resolution must use retained definitions; the only fresh schema
resolution is for the resubmitted upload. The shared publication verifier asserts
CREATE, one publication, exact receipt replay, terminal retirement, and release
of the recovery process's read and metadata resources.

The mandatory storage driver includes this same process pair. The full aggregate
has not been rerun for this change. Sol reviewed the design and implementation
without blocking findings. Two initial fixture compilation errors were corrected:
an overloaded transaction lambda and a checked exception inside the resolver.

Scope: crash after initial START, followed by a new process completing the mixed
publication. This does not qualify every crash phase, cleanup of the crashed
writer's old reader incarnation, managed public routing, transport parity or
performance. These remain separate acceptance items.
