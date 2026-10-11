package ai.protomolt.proto.repo.blob.spi;

/** Optional operations an application may require before using a selected store. */
public enum BlobCapability {
    /** Complete-object GET with a caller-supplied limit enforced before unbounded materialization. */
    BOUNDED_READ,
    AUTHORITATIVE_CONDITIONAL_READ,
    ATOMIC_CONDITIONAL_WRITE,
    LIST,
    SERVER_SIDE_COPY,
    STREAMING_WRITE,
    OBJECT_EXPIRY,
    /**
     * Normal PUT/COPY does not apply an object TTL. This does not qualify external
     * lifecycle policies, eviction, persistence settings or administrative deletion.
     */
    NON_EXPIRING_WRITES,
    /** Exact-key physical reclamation includes all historical versions and markers. */
    PHYSICAL_RECLAMATION,
    /**
     * Deletion of exactly one immutable provider version of one object, never
     * the whole key. Distinct from {@link #PHYSICAL_RECLAMATION}: whole-key
     * availability never implies exact-version support. A handle advertises this
     * only with a wired {@link ExactVersionReclaimer} port; re-wrapping a handle
     * through a constructor that cannot wire the port drops the capability
     * rather than advertising it against an unsupported port.
     */
    EXACT_VERSION_RECLAMATION
}
