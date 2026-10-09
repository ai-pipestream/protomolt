# Internal authenticated bounded archive transport

The dedicated Netty mount requires an API token and exposes only ArchiveService.
Ten reviewed synchronous unary methods share one ingress gate across listeners.
Bridge and streaming requests fail at headers. Authentication precedes admission.
Ingress and archive writes reserve from the same PayloadBudget.

Real PostgreSQL/Redis tests verify remote/local retry and historical-read behavior,
authentication during saturation, cancelled writes retaining both reservations,
shutdown waiting for provider completion, rejected streaming, malformed protobuf,
plain/gzip oversize, and capacity shared by two listeners. Retrying after cancelled
publication performs no additional physical write. A descriptor inventory test
requires review when the service adds a method.

```sh
./gradlew :protomolt-repo-service:test \
 --tests '*BoundedArchiveTransportIT' \
 --tests '*UnaryRequestAdmissionTest' \
 --tests '*BoundedArchiveHostIT' \
 --tests '*ManagedArchiveHostIT' \
 --tests '*RedisArchiveLifecycleIT' \
 --tests '*RedisServiceCompositionIT' --console=plain
```

Result: 38 tests, zero failures/errors/skips, 26 seconds. Local log:
`/tmp/protomolt-bounded-transport-qualified.log`. Sol found no blocker in the final
review. An initial test compilation failed on an overloaded method reference;
an explicit lambda corrected it. `git diff --check` passed.

The factory and transport mount remain internal. Public environment configuration,
HTTP admission, complete repository parity, response/read-memory limits and Redis
deployment durability remain open. Input reservations do not measure decoded heap
or prior network buffering. Compressed-size parser errors retain gRPC 1.84's UNKNOWN
status; uncompressed oversize reports RESOURCE_EXHAUSTED. Both prevent storage work.
No hosted CI, merge or deployment occurred.
