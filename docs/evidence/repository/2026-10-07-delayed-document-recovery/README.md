# Delayed document write after durable cleanup

`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`
passed in 7m45s. The three focused `RedisDelayedRequestGateTest` cases passed
in 8s. Sol reviewed the integration and proxy lifecycle handling.

The parent proxy captures the original Redis conditional-write request, matching
its physical key. The actual client times out; no document revision advances and
no bytes arrive early. The original host exits. Redis restarts and a fresh JVM
waits for real lease expiry, then confirms durable ABSENT cleanup and physical
absence. Only then does the parent deliver the queued request to Redis.

The late bytes match the original payload, differ from both committed versions,
and remain unverified and unreferenced. The current revision UUID and mutation
number stay unchanged. Exact recovery removes the late bytes again. Both retained
histories still decode with the original live schema registry gone. The cleanup
lookup uses the original generation and physical identity.

This is a delayed transport delivery of the original request, not another adapter
PUT. The proxy deliberately retains the frame across the Redis restart and sends
it over a fresh connection. It supports only unauthenticated RESP2 Redis DB 0.
The final reclaim is a direct recovery call; scheduled recheck timing remains
unqualified. No production code changed.

The first full run completed the repository checks but failed proxy shutdown on
idle client resets. The fixture now counts those only before a request's first
byte. Real PING/reset and partial-request/reset tests distinguish normal idle
connection closure from errors that must fail the fixture. The passing full run
recorded two idle disconnects.
