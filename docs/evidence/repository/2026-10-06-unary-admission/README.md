# Unary admission before protobuf decoding

The internal interceptor reserves shared capacity before request parsing. Only
explicitly listed synchronous unary methods are eligible. Streaming methods fail
without creating their observer. Terminal listener callbacks release capacity
after synchronous work returns, including cancellation and shutdown paths.

Real Netty tests use a synthetic protobuf echo service. They cover success, gzip,
compressed and uncompressed size errors, malformed protobuf, duplicate messages,
cancellation before half-close, cancellation during a blocked handler, server
shutdown, capacity rejection before decoding, and stream rejection before handler
creation. This is transport evidence, not repository/provider integration.

Redis host tests additionally cover physical identity conflict and cleanup after
capability rejection. The original backend binding remains usable after a conflict.

```sh
./gradlew :protomolt-repo-service:test \
 --tests '*UnaryRequestAdmissionTest' \
 --tests '*BoundedArchiveHostIT' \
 --tests '*RedisServiceCompositionIT' \
 --tests '*RedisArchiveLifecycleIT' \
 --tests '*ManagedArchiveHostIT' --console=plain
```

Result: 30 tests, zero failures/errors/skips, 27 seconds. Local log:
`/tmp/protomolt-unary-admission-qualified.log`. Sol reviewed the interceptor and
lifecycle tests with no blocker. No hosted CI, merge or deployment occurred.

The first compressed-size test expected RESOURCE_EXHAUSTED, but gRPC 1.84 reports
UNKNOWN for that parser exception. The test now records this behavior and verifies
no handler execution or leaked reservation. Uncompressed oversize remains
RESOURCE_EXHAUSTED. This was a corrected expectation, not a runtime error-mapping fix.

The guard remains unmounted. The next test must use authenticated archive RPCs,
real Redis writes, a shared ingress/write budget and shutdown during provider work.
Raw transport buffers, decoded heap, detached workers and HTTP admission are outside
this prerequisite. The allowance reserves two maximum messages per call to cover
gRPC unary protocol-violation detection.
