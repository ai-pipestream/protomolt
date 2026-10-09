# Redis historical transport

`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`
passed in 3m40s with real PostgreSQL, Redis and Git resources.

The internal host mounts publication and history services. After the live schema
resolver closes, authenticated in-process gRPC returns the original validated
document. The test compares revision identity, command and policy hashes, metadata
and manifest with the local repository. RAW delivery matches every local fragment
ordinal and byte sequence. Missing authentication and missing mode are rejected.
Sol reviewed the test with no blocker.

This adds tests only. It does not qualify Netty, scoped credentials, Redis restart
persistence or the materialization RPC. The bounded-document profile is internal.
