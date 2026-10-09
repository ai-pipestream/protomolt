package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.spi.DocumentPublicationInput;

/** Validated internal assembly limits shared with the public bounded document options. */
record BoundedDocumentProfile(int maxObjectBytes, long payloadBudgetBytes) {
    BoundedDocumentProfile {
        if (maxObjectBytes < 1 || maxObjectBytes > DocumentPublicationInput.MAX_UPLOAD_BYTES)
            throw new IllegalArgumentException("Document object limit must be positive and at most 8 MiB");
        if (payloadBudgetBytes < 64L * 1024 * 1024)
            throw new IllegalArgumentException("Document budget must cover the 64 MiB runtime admission allowance");
    }

    void validate(RepoServiceConfig config) {
        if (!RepoServiceConfig.BLOB_STORE_REDIS.equals(config.blobStore())
                || !config.managedStorage().retentionQualified() || !config.lifecycleEnabled())
            throw new IllegalArgumentException("Bounded documents require qualified Redis and lifecycle recovery");
        if (config.redisTtlSeconds() != 0 || config.redisMaxObjectBytes() < maxObjectBytes
                || config.redisMaxObjectBytes() > 9L * 1024 * 1024)
            throw new IllegalArgumentException("Bounded documents require zero TTL and a provider object cap within 9 MiB");
    }
}
