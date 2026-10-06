package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.engine.ArchivePutAdmission;
import java.util.Objects;

/** Shared internal state for local and authenticated Netty archive admission. */
record BoundedArchiveProfile(ArchivePutAdmission.Limits limits, PayloadBudget budget, int maxActive) {
    BoundedArchiveProfile {
        Objects.requireNonNull(limits);
        Objects.requireNonNull(budget);
        if (maxActive < 1 || maxActive > 1024)
            throw new IllegalArgumentException("Archive put concurrency must be between 1 and 1024");
    }

    ArchivePutAdmission openAdmission(RepoServiceConfig config) {
        if (!config.managedStorage().retentionQualified() || !config.lifecycleEnabled()
                || !RepoServiceConfig.BLOB_STORE_REDIS.equals(config.blobStore()))
            throw new IllegalArgumentException("Bounded archive qualification requires managed Redis and lifecycle recovery");
        if (config.redisTtlSeconds() != 0 || config.redisMaxObjectBytes() < 1
                || config.redisMaxObjectBytes() > 9 * 1024 * 1024
                || limits.maxObjectBytes() > config.redisMaxObjectBytes())
            throw new IllegalArgumentException("Bounded Redis requires zero TTL and an object limit within 9 MiB");
        return new ArchivePutAdmission(limits, budget, maxActive);
    }
}
