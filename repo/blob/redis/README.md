# Valkey and Redis-compatible byte provider

Valkey is the preferred server for this provider. Include
`protomolt-repo-blob-redis` and explicitly select `valkey` through the byte SPI.
The existing `redis` selection remains supported. Both factories reuse the same
`RedisBlobStore`, Jedis client and Lua operations; no data migration or server
detection occurs. Use `redis://` or `rediss://` URIs for either server.

Configuration requires `uri`, `ttl-seconds`, `max-object-bytes`, `key-prefix`
and `write-policy`.
Zero TTL disables expiry; zero size configuration still has a 512 MiB per-value
adapter ceiling. InputStream writes transfer through a fixed 64 KiB window and
reject short, extra or checksum-mismatched bytes before the Lua command can execute.
Whole-object and range streams consume one atomic Redis response incrementally;
closing early abandons the socket rather than draining the object. `objectLength`
uses HSTRLEN without fetching the payload. The explicit byte-array GET/PUT APIs
remain buffered; use the stream APIs for transfers.
Custom metadata is limited to 256 entries and 64 KiB of total UTF-8 bytes,
including names, values and content type. Malformed text is rejected before any
write. These limits are independent of the body limit.

Each object uses one hash containing its bytes and metadata. Lua operations keep
reads, bounded size checks, writes and copies atomic with respect to concurrent
Redis commands. Scripts do not promise rollback after server errors. Listing
uses SCAN and is not a snapshot. The caller's logical prefix is matched literally.
Valkey Cluster and Redis Cluster are not supported by this implementation.

## Create-only physical keys

The required `write-policy` option is `replace` or `create-only`. `create-only`
selects a separate physical layout with backend identity `redis/v3`; `replace`
retains `redis/v2` and its existing keys.
The v3 key prefix is `protomolt:redis:v3-create-only:`; each following component
uses the same canonical encoding as v2. Identical endpoint, configured prefix,
namespace and logical key do not alias between policies. There is no automatic
migration or cross-layout fallback.

Create-only requires zero TTL and caps writes/copies at 9 MiB, or a smaller
configured limit. PUT and COPY atomically refuse existing targets, including an
identical retry; self-copy conflicts. Streamed InputStream writes use the same
policy. Matching conditional replacement is unsupported, so this mode does not
advertise `ATOMIC_CONDITIONAL_WRITE`; authoritative reads remain available.
Both policies advertise streaming writes within their existing provider limits.

This protects against replacement through the selected adapter, not direct Redis
administration. Reclamation deletes the key; a delayed write can recreate it.
The repository must qualify writer drain/fencing and repeat cleanup before managed
archival activation. Create-only is not a retention, tombstone or durability claim.

The provider exposes listing, expiry, bounded reads, streaming writes and physical reclamation.
Replacement mode additionally exposes atomic conditional writes.
It exposes non-expiring writes only when the configured TTL is zero. Explicit
version requests are unsupported; they never fall back to the current object.
`getForUpdate` returns bytes and their ETag from one bounded Redis operation.
`conditionalPut` atomically creates an absent object or replaces one with a matching
ETag. Both operations retain the shared 9 MiB conditional payload bound; writes
also obey `max-object-bytes`. A failed condition changes neither metadata nor TTL.
Malformed existing objects fail explicitly rather than being treated as absent.

ETags identify body content, not a mutation epoch: an A-to-B-to-A change produces
the original tag, and metadata-only changes do not change it. Ordinary writes and
copies can still overwrite keys. This capability therefore does not establish
immutable archival identity. An acknowledgment lost after a write requires
authoritative reconciliation; a retry conflict is not automatically success.
The test injects a caller-acknowledgment failure after the real adapter returns;
it does not simulate a dropped Redis network response.
Reclamation observes exact-key absence; a late write requires another cleanup
pass. Capability flags do not establish archival durability. Persistence, eviction,
administrative mutation and repository key reuse policy require separate review.

## Process-crash coverage

`RedisPersistenceIT` kills the real selected server process with SIGKILL and starts the same
container again. With AOF enabled, `appendfsync always`, disabled snapshotting and
`maxmemory-policy noeviction`, acknowledged writes and copies retain their bytes,
content type, ETag, custom metadata and non-expiring TTL. The test first recovers
an object, reclaims it, then repeats the crash to verify that deletion persists.
With both AOF and snapshotting disabled, the same non-expiring adapter loses its
objects on restart. Configuration is asserted against the running server.

This covers process failure while the Docker host and storage remain running.
It does not qualify host power loss, replication/failover, backup restoration,
external eviction/configuration changes, or managed archival publication. The
service's managed-storage qualification guard remains unchanged.

## Layout change

The default v2 physical key encodes the configured prefix, namespace and logical key as
separate canonical base64url UTF-8 components. One object's metadata cannot collide
with another object's name. Managed identity includes the endpoint, database,
configured prefix and layout version; it excludes credentials and runtime limits.
Credential rotation therefore does not change physical identity.

There is no fallback to the old concatenated keys or `$meta` hashes. Existing data
requires an explicit migration using the old reader or a deliberate rebuild.
Retained backend generations must not be relabeled as v2 while their objects use
the old layout. No automated migration is supplied in this change.


## Selection and retained identity

The `valkey` factory declares `redis` as its canonical identity provider. Thus
`BlobStores.managedIdentity("valkey", options)` equals the existing `redis`
identity for the same endpoint, database, prefix and write policy. Physical keys,
`redis/v2` and `redis/v3` identities, and persisted `RedisDriveConfig` remain
unchanged. A different endpoint still changes identity; this alias does not
redirect historical references or migrate Redis data files.

Repository configuration accepts `valkey` and `s3-valkey-cache` alongside the
legacy `redis` and `s3-redis-cache` modes. Prefer the `DOCUMENT_PLATFORM_VALKEY_URI`,
`DOCUMENT_PLATFORM_VALKEY_TTL_SECONDS` and `DOCUMENT_PLATFORM_VALKEY_MAX_OBJECT_BYTES`
environment variables; the corresponding `REDIS` variables remain accepted.
Supplying both spellings with different values fails without echoing credentials.

S3 remains the default authoritative store. The shared Valkey/Redis adapter streams
through bounded client buffers for both aliases. This does not remove Redis's
per-value limit or qualify server-side memory use: the existing hash/Lua operations
still hold the value on the server. General managed storage still requires S3 and lifecycle recovery;
the existing bounded archive profile remains separately limited. S3 with a Valkey
cache keeps S3 authoritative and forwards streamed reads to S3. An empty cache can
be rebuilt; an authoritative store must not be discarded or silently redirected.

## Server qualification

The default integration target is `valkey/valkey:9.1.3-alpine`, pinned from the
[official stable 9.1 release page](https://valkey.io/download/9.1/).
Keep its Lua engine enabled: EVAL and cmsgpack are required. The same tests can
exercise a Redis server with explicit environment overrides, including the server
executable used by the crash test:

```sh
flock /work/.gradle-krick.lock ./gradlew \
  :protomolt-repo-blob-spi:test :protomolt-repo-blob-redis:test \
  :protomolt-repo-blob-redis:integrationTest --rerun \
  :protomolt-repo-blob-cache:integrationTest --rerun --max-workers=2

PROTOMOLT_REDIS_TEST_IMAGE=redis:7.2.12-alpine \
PROTOMOLT_REDIS_TEST_SERVER=redis-server \
flock /work/.gradle-krick.lock ./gradlew \
  :protomolt-repo-blob-redis:integrationTest --rerun --max-workers=2
```

The `--rerun` task option ensures changing the test server reruns the suite.
`ValkeyProviderIT` proves cross-alias reads, copies and reclamation through real
providers for both write policies, alongside the existing atomicity, conditional
write, expiry, metadata, size-bound and process-crash suites. These tests establish
only their asserted behavior, not cluster/failover or host-power-loss durability.

Before migrating existing authoritative data, check the source server version.
The [Valkey migration guide](https://valkey.io/topics/migration/) covers Redis OSS
through 7.2; Redis 7.4+ persistence files are incompatible. This change provides no
file conversion, cutover, rollback or administrative data migration.

## Bounded transfer qualification

`RedisStreamingIT` forks a JVM with a 48 MiB maximum heap and transfers a 128 MiB
object through InputStream PUT, whole-object GET, a small range and stream-to-PUT
promotion. It verifies exact bytes and SHA-256, plus early close. Separate cases
pin pre-commit input failures, create-only conflicts, empty ranges, snapshot reads,
URI authentication/database selection, TTL and malformed-object refusals.
The transport reuses Jedis socket configuration, including rediss hostname
verification; the bounded-heap test exercises the plaintext local server.
This is client-memory qualification, not a Redis server-memory or durability claim.
