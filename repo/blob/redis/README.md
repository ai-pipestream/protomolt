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

## Create-only physical keys

Optional `write-policy=create-only` selects a separate physical layout with backend
identity `redis/v3`. The default `replace` retains `redis/v2` and its existing keys.
The v3 key prefix is `protomolt:redis:v3-create-only:`; each following component
uses the same canonical encoding as v2. Identical endpoint, configured prefix,
namespace and logical key do not alias between policies. There is no automatic
migration or cross-layout fallback.

Create-only requires zero TTL and caps writes/copies at 9 MiB, or a smaller
configured limit. PUT and COPY atomically refuse existing targets, including an
identical retry; self-copy conflicts. Buffered InputStream writes use the same
policy. Matching conditional replacement is unsupported, so this mode does not
advertise `ATOMIC_CONDITIONAL_WRITE`; authoritative reads remain available.
It also does not advertise streaming writes. The capability description below
applies to the default replacement mode.

This protects against replacement through the selected adapter, not direct Redis
administration. Reclamation deletes the key; a delayed write can recreate it.
The repository must qualify writer drain/fencing and repeat cleanup before managed
archival activation. Create-only is not a retention, tombstone or durability claim.

The provider exposes listing, expiry, bounded reads, atomic conditional writes and physical reclamation.
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

The default v2 physical key encodes the configured prefix, namespace and logical key as
separate canonical base64url UTF-8 components. One object's metadata cannot collide
with another object's name. Managed identity includes the endpoint, database,
configured prefix and layout version; it excludes credentials and runtime limits.
Credential rotation therefore does not change physical identity.

There is no fallback to the old concatenated keys or `$meta` hashes. Existing data
requires an explicit migration using the old reader or a deliberate rebuild.
Retained backend generations must not be relabeled as v2 while their objects use
the old layout. No automated migration is supplied in this change.
