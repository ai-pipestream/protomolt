package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.engine.DriveProvisioner;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.DriveCacheConfig;
import ai.protomolt.proto.repo.v1.S3DriveConfig;
import java.util.Objects;
import java.util.function.Consumer;

/** Verifies inherited host-default settings; explicit drive backends use {@link DriveBackingPool}. */
final class SelectedDriveBackend implements Consumer<DriveRecord> {
    private final RepoServiceConfig config;
    private final String provider;

    SelectedDriveBackend(RepoServiceConfig config) {
        this.config = config;
        this.provider = config.driveProvider();
    }

    @Override public void accept(DriveRecord drive) {
        var stored = drive.readProviderConfig();
        DriveProvisioner.requireSelectedProvider(!config.hasS3Backing()
                && ("redis".equals(drive.provider) || "valkey".equals(drive.provider))
                ? drive.provider : provider, drive.provider, stored);
        if (drive.credentialsRef != null && !drive.credentialsRef.isBlank()) {
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Per-drive credential resolution is not configured");
        }
        if ("s3".equals(provider) && drive.region != null && !drive.region.isBlank()
                && !drive.region.equals(config.s3Region())) {
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Drive region differs from the selected backend");
        }
        if (stored == null) return;

        if (!stored.getOptionsMap().isEmpty()) {
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Per-drive provider options are not supported by this assembly");
        }
        boolean matches = switch (stored.getConfigCase()) {
            case S3 -> matchesS3(stored.getS3());
            case REDIS -> stored.getRedis().getUri().equals(config.redisUri())
                    && stored.getRedis().getTtlSeconds() == config.redisTtlSeconds()
                    && stored.getRedis().getMaxObjectBytes() == config.redisMaxObjectBytes()
                    && stored.getRedis().getKeyPrefix().isEmpty();
            case CONFIG_NOT_SET -> true;
            default -> false;
        };
        if (!matches) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Drive configuration differs from the selected backend");
    }

    private boolean matchesS3(S3DriveConfig s3) {
        if (!Objects.equals(s3.getEndpointOverride(), config.s3Endpoint() == null ? "" : config.s3Endpoint())
                || s3.getForcePathStyle() != (config.s3Endpoint() != null)) {
            return false;
        }
        if (!s3.hasReadThroughCache()) {
            return true;
        }
        if (!config.hasRedisCache()) {
            return false;
        }
        DriveCacheConfig cache = s3.getReadThroughCache();
        String cacheProvider = cache.getProvider().isEmpty() ? "valkey" : cache.getProvider();
        return cacheProvider.equals(config.redisCompatibleProvider())
                && cache.getUri().equals(config.redisUri())
                && cache.getTtlSeconds() == config.redisTtlSeconds()
                && cache.getMaxCacheableBytes() == config.redisMaxObjectBytes()
                && cache.getKeyPrefix().isEmpty()
                && cache.getCredentialsRef().isEmpty();
    }
}
