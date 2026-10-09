# Bounded Redis read shutdown

`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`
passed in 7m44s. Sol reviewed the gate and lifecycle assertions.

The fixture completes a real Redis bounded read, then holds the result inside
the provider call. A 200ms shutdown times out with resources retained. The caller
receives CANCELLED while the provider call remains active; SQL still answers and
the provider close count stays zero. Releasing the call permits a second shutdown
to close the provider exactly once. The full restart and cleanup regression passes.

This qualifies library historical-read shutdown, not gRPC cancellation or active
publication shutdown. The gate preserves interruption. It does not require Redis
to start new I/O after cancellation. No production code changed.
