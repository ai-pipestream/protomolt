# Repository schema resolution and cache

Implementation plan; this does not declare a shared repository cache available.
The cache complements retained archival assets and never replaces them.

The first ownership primitive, `DocumentSchemaArtifactCache` in `repo/admission`,
now holds digest-verified immutable byte copies with bounded entries and byte
capacity. It provides leased lookup and insertion, not registry integration.
Insertion reserves twice the input size for copying; retained entries reserve
their exact serialized size. Concurrent copies consume capacity independently.
The adapter must compose artifact digests into the complete definition identity
below, authorize selection, and hold all leases through its consumers' copies.

## Existing seams

`DocumentSchemaAdmission.Resolver.select(Selection)` already receives the member
ordinal, root locator, occurrence prefix, exact type URL and value digest. Its
host captures authorization context. Preserve this occurrence-aware selection;
do not wrap it in a cache keyed only by type URL.

`SchemaRegistryStore` provides versioned discovery and content-addressed descriptor
artifacts. Check `supportsDescriptorSets()` before interpreting an empty artifact
result: the interface's default empty result means unsupported as well as absent.
Confluent and Apicurio loaders have caches, but those caches do not establish
repository authorization, capacity or archival retention guarantees.

## Two separate operations

1. Authorized discovery chooses an exact definition for an occurrence. The host
   supplies the registry instance, account/security scope and selection policy.
   Initially do not cache discovery or negative answers. Repeat authorization
   when an operation selects a definition; a prior authorized lookup is no grant.
   Bind that context from host-authenticated identities, not caller-supplied cache
   labels. `SchemaRegistryStore` has no caller or tenant parameter and supplies no
   authorization boundary on its own.
2. Immutable artifact lookup can reuse verified bytes for that exact definition.
   Start with a cache scoped to one host-bound registry/security context. Its key
   includes exact type URL, descriptor digest, metadata codec/version/digest and optional source
   digest. A registry numeric ID alone is not globally unique. Validate bytes
   against their declared identity before insertion. Never use the latest schema
   to fill a missing retained historical artifact.

The adapter composes these operations behind the existing resolver. Keep registry
integration dependencies in a separate adapter leaf, outside repository admission.
Caching definition bytes does not cache a validation verdict: candidate bytes,
policy and validation runtime remain inputs to every admission check.

## Ownership, capacity and concurrent calls

Cache entries own byte reservations; leases pin entries while callers copy or use
them. `Definition` contains shared `ByteString`s: its entry lease must survive until
admission completes its budgeted copy, or the adapter must return a separately
budgeted owned copy. Returning shared bytes after releasing their lease is invalid.
Evict only unpinned entries. Bound total bytes, individual artifact size,
entry count, concurrent loads and waiters. Account separately for linked descriptor
heap; serialized-byte accounting alone is not a heap bound. First implementation
may cache only bytes and retain existing per-check linking limits.

Coalesce concurrent loads only for the same exact key and security context.
Each waiter keeps its own deadline and cancellation. One canceled waiter must not
cancel another caller's load. Cancel underlying work when no waiters remain if
the provider supports cancellation; retain its capacity until completion otherwise.
Never hold a cache mutex or SQL transaction across registry I/O or descriptor
linking. Refuse capacity exhaustion explicitly rather than making an unbounded
uncached retry. Shutdown rejects new work and drains owned loads and leases.

## Missing definitions and opaque data

An authoritative missing definition, unsupported artifact storage, denied access,
registry outage, corrupt bytes and cancellation are distinct outcomes. Do not
negative-cache errors or turn them into an opaque admission success.
Only the selected healthy store's authoritative answer establishes absence.

Opaque preservation can retain an unresolved Any envelope when the selected
repository policy permits it. It does not require decoding its value bytes.
Typed admission still requires a complete definition and runtime validation.
Selected JSON or DynamicMessage materialization also requires descriptors; when
they are unavailable, return the explicit unresolved result supported by that
operation, or its declared error, rather than inventing fields. Cache ownership
must not become the retention mechanism for committed schema assets.

## Acceptance cases and delivery order

First implement bounded immutable byte ownership and leases, then the registry
adapter, then concurrent-load sharing. Tests must use real descriptor bundles and
the existing registry store contract fixtures where applicable.

- Two occurrences with the same type URL but different selected definitions keep
  independent identities and decode against their own definitions.
- Different registry/security contexts cannot reuse discovery grants. Revocation
  refuses new authorized selection even when artifact bytes are cached.
- Concurrent equal-key loads share bytes; unequal keys do not. Cancel one waiter,
  exhaust capacity, evict an idle entry and close during loading; all reservations
  must drain without invalidating active leases.
- Corrupt artifacts never enter the cache. Outages and unsupported storage cannot
  become authoritative absence. A later successful request can retry a failure.
- Historical reads succeed from retained definitions with the registry unavailable
  and the cache empty; a cache hit cannot make pruning delete retained assets.
- Raw opaque preservation and typed decoding exercise different policy outcomes.
  No cache hit bypasses candidate validation or successful-response checks.

Record RustFS throughput and latency separately from these correctness checks.
Report warm and cold caches, hit/miss counts, registry calls, retained bytes and
concurrency; do not infer horizontal scaling from a warm single-process cache.
