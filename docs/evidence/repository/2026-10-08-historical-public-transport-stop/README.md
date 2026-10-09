# Historical public deadline and service close

Parent: `bb93b546adb1cc97f43d72717a82abcea62dc46f`.
Test-only changes; Sol reviewed the deadline source, producer lifetime, admission
close and cleanup assertions without finding a blocker.

Both cases use the packaged production runtime, real PostgreSQL and LocalStack S3,
with authenticated in-process gRPC and test identity binding. The provider observer
holds the first actual PUT reply; it does not synthesize successful storage.

## Deadline

A ten-second remote client timeout supplies the gRPC deadline. Local read control
is `NONE`, so this is not a local cancellation test. The probe first requires that
the actual PUT completed and its reply is held, then requires DEADLINE_EXCEEDED and
server-context cancellation. Service capacity, delivery bytes and historical read
captures remain held; another RPC receives RESOURCE_EXHAUSTED. No assessment or
revision commit exists. After releasing the real reply, the producer drains, the
object remains unverified, no assessment/commit appears and all resources return.

## Orderly service admission close

While an accepted RPC holds the real PUT reply, service and runtime admission are
closed. Neither can report full drainage; delivery bytes, historical captures and
the accepted call remain live. A new RPC receives UNAVAILABLE. Releasing the reply
lets the original RPC return a valid committed receipt with one assessment, one
revision commit and a verified upload. Service and runtime drainage then return
all reservations and historical captures.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.initialOwner' \
  --max-workers=2 --console=plain
```

Passed in 1m21s: one aggregate JUnit case, zero failures, errors or skips. The driver
requires both new markers, `HISTORICAL_PUBLIC_RPC_DEADLINE_DRAIN_OK` and
`HISTORICAL_PUBLIC_RPC_CLOSE_DRAIN_OK`. Original log and XML are archived here.

The deadline must leave enough time to reach the held PUT; an unusually slow host
may fail that prerequisite rather than qualify this scenario. The close case is
service admission close, not network listener shutdown or a socket disconnect.
Channel/server teardown follows the lifecycle assertions. These tests establish
neither throughput nor scale-out, and the historical factory remains package-private
pending the other public acceptance cases.
