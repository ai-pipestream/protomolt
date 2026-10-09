# Delayed Redis request fixture

`./gradlew :protomolt-repo-container:test --tests '*RedisDelayedRequestGateTest' --console=plain`
passed in 7s against Redis 7. Sol reviewed the fixture and its limitations.

A bounded RESP2 proxy captures one complete conditional-write request emitted by
the production Redis adapter. Capture matches the physical key; an unrelated
write passes through. The caller times out and disconnects. After a direct
provider reclaim confirms absence, the proxy forwards the original bytes once
and checks the real Redis success response. The expected object appears and can
be reclaimed without removing its neighbor. No response is fabricated and no
second adapter PUT reconstructs the request.

Only unauthenticated Redis DB 0 is supported. AUTH, SELECT and HELLO are rejected.
The captured wire frame is limited to 2 MiB; delivery and close are serialized.
The parser also has depth, header, bulk and array limits.

This is a transport prerequisite. It does not yet test SQL attempt tombstones,
repository publication fences, retained revisions or scheduled recovery timing.
The production-JAR host integration is the next step.
