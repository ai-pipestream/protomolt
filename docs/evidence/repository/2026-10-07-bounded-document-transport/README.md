# Redis publication over in-process gRPC

`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`
passed in 3m40s with real PostgreSQL, Redis and Git resources.

Tests cover operator-token authentication, remote replay of a local receipt,
fresh remote typed publication, local replay of the remote receipt and the
1 MiB upload limit. The oversized request passes the default protocol validator
with matching size and checksum. Local rejection checks the specific limit
error; gRPC checks INVALID_ARGUMENT. Every authenticated call has a new deadline.
Sol reviewed identity mapping and upload coordinates.

No production behavior changed. Netty, scoped credentials, remote history and
Redis restart durability remain unqualified. The profile remains internal.
