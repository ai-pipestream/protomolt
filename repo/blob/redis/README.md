# Redis byte provider

This provider implements byte storage for standalone Redis 7 or later. Include
`protomolt-repo-blob-redis` to discover the `redis` provider through the byte SPI.
Configuration requires `uri`, `ttl-seconds`, `max-object-bytes` and `key-prefix`.
Zero TTL disables expiry; zero size configuration still has a 512 MiB per-value
adapter ceiling. Stream writes buffer the declared payload and reject short or
extra bytes before writing. They are not an unbounded streaming upload path.
Custom metadata is limited to 256 entries and 64 KiB of total UTF-8 bytes,
including names, values and content type. Malformed text is rejected before any
write. These limits are independent of the body limit.

Each object uses one hash containing its bytes and metadata. Lua operations keep
reads, bounded size checks, writes and copies atomic with respect to concurrent
Redis commands. Scripts do not promise rollback after server errors. Listing
uses SCAN and is not a snapshot. The caller's logical prefix is matched literally.
Redis Cluster is not supported by this implementation.

The provider exposes listing, expiry, bounded reads and physical reclamation.
It exposes non-expiring writes only when the configured TTL is zero. Explicit
version requests and conditional writes are unsupported; they never fall back to
the current object. The shared conditional payload bound is unchanged.
Reclamation observes exact-key absence; a late write requires another cleanup
pass. Capability flags do not establish archival durability. Persistence, eviction,
administrative mutation and repository key reuse policy require separate review.

## Process-crash coverage

`RedisPersistenceIT` kills the real Redis process with SIGKILL and starts the same
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

The v2 physical key encodes the configured prefix, namespace and logical key as
separate canonical base64url UTF-8 components. One object's metadata cannot collide
with another object's name. Managed identity includes the endpoint, database,
configured prefix and layout version; it excludes credentials and runtime limits.
Credential rotation therefore does not change physical identity.

There is no fallback to the old concatenated keys or `$meta` hashes. Existing data
requires an explicit migration using the old reader or a deliberate rebuild.
Retained backend generations must not be relabeled as v2 while their objects use
the old layout. No automated migration is supplied in this change.
