# Shared archive lifecycle on Redis

Base `6997778c85b0cd9fbb9c4cbd33e97fd1d8c92d94`. October 5, 2026 local,
October 6 UTC. PostgreSQL 18 and standalone Redis 7 real containers. No production
code changes or simulated provider successes.

```sh
./gradlew :protomolt-repo-service:test \
  --tests '*RedisArchiveLifecycleIT' --tests '*RedisServiceCompositionIT' --console=plain
```

Five new lifecycle cases and two existing composition cases passed with no skips,
failures or errors in 11 seconds. An initial test compilation error from overloaded
assertion inference was fixed before these runs. New coverage:

- Unary publication, repeated-content reuse and historical bytes through the same
  shared library operations, directly and over in-process gRPC.
- Actual provider success followed by injected caller-acknowledgment loss leaves
  SQL STAGING and physical bytes, with no published entry; expiry and recovery reclaim it.
- A real PUT held before provider execution resumes after lease expiry and cleanup.
  SQL verification refuses it. A second pass discovers the DELETED tombstone and
  reclaims the late bytes. This is a deterministic in-process schedule, not a killed JVM.
- A real bounded read holds its pin through logical deletion; cleanup skips it.
  Once provider read and pin release finish, cleanup can delete the bytes. Throwing
  after actual reclamation leaves DELETING and retry succeeds against actual absence.

Resolver assertions verify original backend generation, realm and v3 identity.
Readers fence admission, drain and attest local quiescence before shared resources
close. Existing composition tests still cover a Redis host that must not construct
S3 and refuse unsupported managed activation.

The fixture manually composes archive libraries, uses process authority, and does
not enable managed Redis in `RepoServices`. This is not API-key/tenant authorization,
streaming support, process-crash durability, throughput, backup or fleet scheduler
qualification. The provider is not configured for persistence in this fixture.
