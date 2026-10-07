# Repository provider qualification

Status: executed at the branch tip of `agent/repository-provider-qualification`
(base `4db6d6c35bc227658e2341c750e9248a8aee3041`). Evidence:
[2026-10-07-provider-qualification](../evidence/repository/2026-10-07-provider-qualification/README.md).
This document qualifies the independently reusable repository libraries using
their published artifacts, real provider implementations and production SPI
discovery, following [repository composition](repository-composition.md). It
does not claim whole-stack conformance, production readiness, linear scale, or
full repository-host startup on a non-S3 backend; composition and recovery
remain with the coordinator.

## Method

All consumer rows resolve from a filesystem Maven repository staged by
`gradle/repository-consumer.init.gradle` at a unique candidate version; no
project dependencies are substituted. Every row runs in both metadata modes
(Gradle module metadata and Maven-POM-only), each mode in its own build
directory so one mode's up-to-date outputs can never prove the other
(`verification/repository-consumer/README.md`). Boundary decisions use
group:artifact identity. Probes launch from the resolved published classpaths
against packaged JARs. Real backends: `redis:7-alpine` containers for Redis,
`localstack/localstack:4.13` for S3 conditional-write qualification
(`localstack/localstack:3.8` remains for the unconditional S3 and cache
suites), and the deployment-pinned RustFS image (existing
`RustFsConditionalBlobStoreIT`). No performance claim is made or required.

## Published consumer matrix

Resolved external module counts (the consumer root project excluded) are
identical in both metadata modes for every row — verified by diffing the
recorded inventories in
[`consumer-matrix/`](../evidence/repository/2026-10-07-provider-qualification/consumer-matrix/).
"Allowed families" are the provider SDK families a row may legitimately carry;
SQL/ORM/pooling/migration, Kafka, server/engine and Azure are rejected in
every row.

| Consumer row | Declared artifacts | Resolved modules | Allowed provider families | Result |
|---|---|---|---|---|
| byte SPI alone | `protomolt-repo-blob-spi` | 1 | none | JDK-only pinned: exactly the one artifact, no external dependency |
| repository SPI alone | `protomolt-repo-spi` | 36 | none | protobuf/gRPC client libraries legitimate; no storage SDK |
| codec alone | `protomolt-repo-codec` | 36 | none | protobuf + descriptors only |
| repository SPI + codec | repo-spi + repo-codec | 37 | none | as above |
| S3 provider alone | `protomolt-repo-blob-s3` | 49 | `aws-sdk` | no Redis/SQL/Kafka/server; AWS SDK confined to this row |
| Redis provider alone | `protomolt-repo-blob-redis` | 9 | `redis-sdk` | no AWS/Azure/SQL/Kafka/server |
| cache alone | `protomolt-repo-blob-cache` | 3 | none | slf4j + byte SPI only; no cache-provider SDK |
| cache with selected Redis provider | cache + redis artifacts | 10 | `redis-sdk` | explicit selection; no AWS |
| blob gRPC client alone | `protomolt-repo-blob-grpc` | 38 | none | gRPC/protobuf client libraries legitimate |
| publication gRPC client alone | `protomolt-repo-publication-grpc` | 38 | none | no engine/server/SQL/Kafka/storage SDKs |

Families rejected per row, by group:artifact identity: SQL/ORM/pooling/
migration (`org.postgresql`, `org.hibernate[.orm]`, `com.zaxxer`,
`org.flywaydb`, `org.liquibase`, `org.xerial`, `com.h2database`,
`org.mariadb.jdbc`, `com.mysql`, `org.jooq`, `io.agroal`, and in-house
`ai.pipestream:*-jdbc`), Kafka (`org.apache.kafka`, `io.confluent`, Kafka
artifacts of `io.apicurio`, in-house `protomolt-kafka-*` and
`protomolt-config-kafka`), server/engine (`io.vertx`, `io.micronaut`,
`io.quarkus`, `org.springframework.boot`, in-house `protomolt-server-*`,
`*-service`, `protomolt-repo-container`, `protomolt-repo-engine`,
`protomolt-serve`, `protomolt-agent-host`), AWS (`software.amazon.awssdk`),
Azure (`com.azure*`), Redis (`redis.clients`). `verifyForbiddenDependencyDetection`
asserts the classification of a representative coordinate from each family,
plus two legitimate client coordinates that must stay unclassified.
Azure has no implementation anywhere in the platform; no row allows it, so its
absence is a standing guard, not a leak finding. Protobuf, gRPC client stubs
and CEL/protobuf-validation libraries are legitimate in client rows and are
not flagged merely because a server could also use them.

The emitted POMs are inspected directly
([`published-metadata/pom-dependencies.txt`](../evidence/repository/2026-10-07-provider-qualification/published-metadata/pom-dependencies.txt)):
the byte SPI POM declares zero dependencies; provider SDKs appear only in
their own modules' POMs (`software.amazon.awssdk:s3` compile scope in the S3
adapter, `redis.clients:jedis` runtime scope in the Redis adapter); every
module carries the `published-with-gradle-metadata` marker and a `.module`
file, which is what makes the two metadata modes genuinely different
resolution paths over the same artifacts.

## Supported entry points

- Discovery: `BlobStores.discover()` (ServiceLoader over packaged
  `META-INF/services` entries) and `BlobStores.of(...)`; selection via
  `providerIds()`, `managedIdentity(id, options)` (no I/O), and
  `open(id, options[, requiredCapabilities])`. The packaged discovery probe
  asserts exactly `{s3, redis}` load from the staged JARs.
- S3 options, all required with no defaults: `endpoint` (explicitly empty
  selects the regional AWS endpoint), `region`, `path-style`,
  `conditional-writes`, `credentials-mode` (`static` with `access-key`/
  `secret-key`, or `default-chain`), and four timeouts ordered
  connection/socket ≤ API attempt ≤ API call. Capabilities: `BOUNDED_READ`,
  `LIST`, `SERVER_SIDE_COPY`, `STREAMING_WRITE`, `NON_EXPIRING_WRITES`,
  `PHYSICAL_RECLAMATION`, plus `AUTHORITATIVE_CONDITIONAL_READ` and
  `ATOMIC_CONDITIONAL_WRITE` only with `conditional-writes=true`.
- Redis options, all required with no defaults: `uri`, `ttl-seconds`,
  `max-object-bytes`, `key-prefix`, and `write-policy` (`replace` or
  `create-only`). Capabilities:
  `LIST`, `OBJECT_EXPIRY`, `BOUNDED_READ`, `AUTHORITATIVE_CONDITIONAL_READ`,
  `PHYSICAL_RECLAMATION`, `ATOMIC_CONDITIONAL_WRITE` (replace policy), and
  `NON_EXPIRING_WRITES` only with TTL zero.
- Cache: `CachingBlobStore(backing, cache, ttlSeconds, maxCacheableBytes)` is
  a library composition, deliberately not a ServiceLoader provider; the
  backing store is truth on every operation.
- Blob gRPC client: `RemoteBlobStore(stub, drive | bucketDrives, timeout)` —
  a client library with per-RPC deadlines and explicit unmapped-bucket
  rejection; `getBounded` remains fail-closed unsupported.

## Configuration and lifecycle findings

- Discovery opens nothing: both factories validate options and construct
  without I/O; an unselected provider is never constructed (pinned in
  `BlobStoresTest`).
- Unknown provider, duplicate IDs, malformed IDs, null handles and
  misdirected backend identities are explicit errors; there is no fallback
  provider, in-memory backend or default bucket anywhere in the selection
  path.
- Invalid or missing configuration is rejected before acquisition, without
  echoing credentials, for both factories (`S3ProviderTest`,
  `RedisProviderTest`, `RedisBlobStoreConfigTest`).
- An unsupported required capability fails `open` and releases the selected
  handle before returning control (`RedisProviderTest`,
  `S3ProviderLifecycleTest`).
- Startup failure is a first-operation failure by design (both SDKs connect
  lazily): unreachable endpoints surface explicitly — `BlobStoreException`
  `UNAVAILABLE` from the S3 adapter, `JedisConnectionException` from the Redis
  adapter — and the handle still closes cleanly
  (`S3ProviderLifecycleTest`, `RedisProviderTest`).
- Owned-resource cleanup: close is idempotent; after close the handle refuses
  access and the owned client/pool fails observably (S3: closed-client /
  terminated-executor failure; Redis: "Pool not open"). The S3 composed
  cleanup closes client then credentials and retains the primary failure with
  the secondary suppressed, exercised through a narrowly instrumented real
  client (delegating proxy, injected close failure only) in
  `S3ProviderLifecycleTest.cleanupFailureRetainsPrimaryAndSuppressedFailures`.
- Backend identity is nonsecret and I/O-free: S3 (`endpoint`, `region`,
  `path-style`, with the `SDK_DEFAULT` sentinel distinct from any explicit
  origin) and Redis (`scheme/host/port/database/key-prefix`, credentials
  excluded). The discovery probe asserts identity fields carry no credential
  material from the packaged JARs.

## Conditional-write and concurrency qualification

- Redis (real `redis:7-alpine`): Lua-atomic compare/write. Duplicate
  create-if-absent conflicts; a stale matching ETag conflicts; concurrent
  creates and replacements each have exactly one winner whose bytes read back
  (`RedisConditionalWriteIT`). The ETag is content-derived SHA-256, **not** a
  mutation epoch: an ABA content cycle is not detectable, pinned honestly in
  `contentEtagIsNotAnEpochAndDoesNotDetectAba`. Redis provides no version
  identity; `get(..., versionId)` is an explicit unsupported operation.
- S3 on LocalStack 4.13, versioning enabled (`S3LocalStackConditionalIT`):
  `If-None-Match: *` is enforced (duplicate creates conflict and an eight-way
  create race has exactly one winner) and `If-Match` is enforced (a stale
  precondition conflicts on every retry without mutating bytes or version,
  and an eight-way matching race against one snapshot has exactly one
  winner). Refused writes never mutate the object; the 9 MiB conditional
  bound is pinned with a boundary refusal that leaves stored bytes untouched.
  LocalStack 3.8, which the suite first used, ignores `If-Match` (a stale
  precondition succeeds), so it cannot qualify matching writes; the suite
  pins 4.13 for that reason. RustFS matching-write qualification
  ([`RustFsConditionalBlobStoreIT`](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/RustFsConditionalBlobStoreIT.java))
  is unchanged.
- S3 backend version identity: with versioning enabled, every write returns a
  distinct opaque `versionId` that reads back exact historical bytes. It is a
  provider identity, not a repository document revision number, and is never
  parsed.
- Cache with explicitly selected real providers (`CacheProviderIT`): S3
  backing + Redis front cache is write-through with read-through population;
  a poisoned cache entry proves plain `get` is cache-served while the backing
  store stays authoritative; copy/delete evict. With Redis backing, the
  authoritative conditional read bypasses the cache, conditional writes land
  on the backing store and evict, and concurrent conditional writes through
  the decorator keep exactly one winner. Known limitation retained from the
  design: a failed eviction can leave a stale plain-read entry until
  tombstone reconciliation; conditional reads are never cache-served.
- Non-S3 published consumer: `RedisPublishedConsumer` runs from a classpath
  containing only the byte SPI and Redis provider JARs, asserts AWS and Azure
  SDK classes are absent, then starts a real Redis container and executes
  create/read/replace/conflict/delete through discovery-selected handles.

## Negative-fixture proof of the gates

- Forbidden dependency detection: a disposable publication
  (`ai.pipestream.negative:protomolt-repo-blob-spi-injected`, group outside
  the exclusive filter, repository under the mode build directory) injects
  `redis.clients:jedis` as a runtime dependency; the same classifier used by
  the positive rows must and does flag it (`NEGATIVE-FIXTURE ... detected as
  expected` in both modes). The staged candidate repository is never
  contaminated.
- Broken service registration: the published Redis provider JAR repackaged
  without `META-INF/services` yields no discoverable provider and selection
  fails "not installed"; a registration naming a missing class surfaces
  `ServiceConfigurationError`. Fixtures carry `DELIBERATE-TEST-FIXTURE`
  markers and are test inputs only.

## Requirement status

| Handoff requirement | Status |
|---|---|
| Inventory the actual consumer matrix | **Strengthened.** Was three ad-hoc name-list checks; now ten consumer rows plus five probe classpaths, group:artifact families, recorded declared/resolved inventories, both metadata modes proven identical. |
| Strengthen published-artifact verification | **Strengthened.** Per-mode build isolation, POM/module-metadata inspection, packaged-JAR discovery probes, forbidden-dependency and broken-registration negative fixtures, artifact hashes recorded. |
| Provider selection and lifecycle | **Satisfied and strengthened.** Selection/identity/refusal coverage pre-existed; added startup-failure, close-failure retention, unsupported-capability release for S3, after-close observable failure for Redis, and cache-with-real-provider composition. |
| Real Redis cases | **Already satisfied**, kept green (testcontainers `redis:7-alpine`, including persistence/crash evidence). |
| S3 correctness/conditional cases on LocalStack with versioning | **Strengthened.** New versioned-bucket suite on LocalStack 4.13 qualifies both `If-None-Match` and `If-Match`; LocalStack 3.8 does not enforce `If-Match` and is not used for conditional qualification. |
| Concurrent conditional writes | **Satisfied and strengthened.** Redis races pre-existed; added LocalStack create and matching races, and a cache-decorator race; RustFS matching qualification unchanged. |
| Payload bound and no-retry semantics | **Already satisfied**, re-proven on LocalStack: 9 MiB accepted, +1 refused without mutation; conflicts never become success through retry. |
| Non-S3 consumer with AWS absent | **Strengthened.** Now an executed probe on the published classpath, not only a metadata assertion. |
| Fix demonstrated defects | **None demonstrated in production code.** The only main-code change is a test-enabling visibility widen of `S3BlobStoreProvider.close` (private → package) so the real cleanup path can be driven; behavior is unchanged. |
| Published instructions | **Strengthened.** README rewritten with exact commands, gate inventory and mode-isolation rules. |

## Remaining gaps and prerequisites

- Full repository-host startup on a non-S3 backend requires
  `repo/container` and composition work outside this assignment's ownership;
  this qualification covers the provider libraries and their published
  consumers, not the host wiring. No host claim is made.
- `repo/publication/grpc` has no in-module unit tests; its public surface is
  compile-probed from the published artifact here, and transport behavior is
  owned by the packaged storage suites (coordinator scope).
- `RemoteBlobStore` does not implement `getBounded` (fail-closed SPI
  default); callers needing bounded reads from a remote store must not assume
  it.
- Redis ETag ABA blindness and the cache stale-eviction window are documented
  semantics, not defects; both have explicit design mitigations (reconcile
  after uncertain acknowledgment; tombstone reconciliation).
- Azure support does not exist and was not invented; the family guard simply
  keeps it that way.
- Performance benchmarking was out of scope; no latency or throughput claim
  is made from LocalStack or container timings.
