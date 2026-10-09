# Bounded unary archive admission

Local verification on 2026-10-05:

```
./gradlew :protomolt-repo-engine:test \
  --tests '*ArchivePutAdmissionTest' --tests '*ArchiveAuthorityTest' \
  :protomolt-repo-service:test --tests '*RedisArchiveLifecycleIT' \
  --tests '*ArchiveManagedUploadIT' --tests '*ArchiveBridgeIT' --console=plain
```

58 tests passed with zero failures, errors or skips, Gradle 20 seconds. Runtime
dependency boundary checks are prerequisites of these test tasks. Sol reviewed
the admission scope, explicit unsupported paths and real-provider fixture with
no material blocker.

The new gate preflights every inline rendition and complete request before engine
copies or SQL/provider work. It shares a payload budget and active-call limit.
Closing admission refuses new calls; an accepted call retains its slot and bytes
until its scope closes. Unit cases cover metadata size, empty-rendition count,
payload bounds, idempotent scope close and close/drain behavior.

Real PostgreSQL/create-only Redis cases exercise local and in-process gRPC saves,
unchanged-byte reuse and historical reads. A later oversized rendition refuses the
whole request before any PUT and without creating the actual derived entry UUID.
Delayed real PUT cases independently exhaust shared byte capacity and active slots;
closing the gate leaves its reservation held until the provider call returns.
The lost-acknowledgment fixture leaves real unpublished bytes recoverable while
releasing the completed caller's reservation. Streaming and bridge-generation
entry points refuse before reading or producing bytes. Existing managed streaming
and bridge fixtures continue to pass through the original constructors.

The serialized-request-plus-four-payload reservation is an allowance, not exact
heap accounting. Requests have already been decoded; transport buffers, SDK heap,
retained manifests and reads are not fully covered. The host must impose provider
limits, transport admission and close/drain ordering before activation. These are
library/in-process tests, not a managed Redis host, distributed benchmark, deployment
durability qualification or a claim that all repository memory is bounded.
