package ai.protomolt.proto.repo.blob.spi;

/** Optional operations an application may require before using a selected store. */
public enum BlobCapability {
    AUTHORITATIVE_CONDITIONAL_READ,
    ATOMIC_CONDITIONAL_WRITE,
    LIST,
    SERVER_SIDE_COPY,
    STREAMING_WRITE,
    OBJECT_EXPIRY
}
