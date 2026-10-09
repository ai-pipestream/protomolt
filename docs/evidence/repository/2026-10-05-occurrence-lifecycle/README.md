# Selected-history transport lifecycle

The production-JAR correctness harness exercises PostgreSQL and a real object
provider through in-process gRPC. LocalStack evidence is in localstack.log/xml.

Held-send tests cover explicit client cancellation and deadline expiration after
real retained-content loading and response verification. Both keep response byte
reservations and the single call slot until the producer exits, then prove a real
retry succeeds and reservations drain. The aggregate-limit case injects invalid
output after a real read to exceed the serialized 8 MiB cap; it is not a valid
oversized archived schema fixture.

Sol reviewed the tests with no blocking findings. These cases do not establish
Netty behavior, API-key authentication, managed-host exposure, full transport
conformance, horizontal throughput or deployment readiness.

Both local gates passed: admissionStorageTest in 2m15s and admissionRustFsTest
in 2m11s. RustFS parity output is in rustfs.log/xml. These are correctness runs;
RustFS remains the target for separate performance measurements.
