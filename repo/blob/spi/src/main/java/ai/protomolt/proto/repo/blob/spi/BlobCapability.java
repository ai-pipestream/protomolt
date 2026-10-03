package ai.protomolt.proto.repo.blob.spi;

/** Optional operations an application may require before using a selected store. */
public enum BlobCapability {
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
    PHYSICAL_RECLAMATION
}
