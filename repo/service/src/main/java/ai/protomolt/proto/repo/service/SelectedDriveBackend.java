package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import java.util.Objects;
import java.util.function.Consumer;

/** Verifies stored drive settings describe the one backend this service actually uses. */
final class SelectedDriveBackend implements Consumer<DriveRecord> {
    private static final ai.protomolt.proto.validate.ProtoValidator VALIDATOR =
            ai.protomolt.proto.validate.ProtoValidator.create();
    private final RepoServiceConfig config;
    private final String provider;

    SelectedDriveBackend(RepoServiceConfig config) {
        this.config = config;
        this.provider = RepoServiceConfig.BLOB_STORE_S3_REDIS_CACHE.equals(config.blobStore())
                ? "s3" : config.blobStore();
    }

    @Override public void accept(DriveRecord drive) {
        if ((RepoServiceConfig.BLOB_STORE_REPO.equals(provider)
                || RepoServiceConfig.BLOB_STORE_REPO_INPROCESS.equals(provider))
                && !config.repoBucketBindings().containsKey(drive.bucket))
            throw new ai.protomolt.proto.repo.spi.RepositoryException(ai.protomolt.proto.repo.spi.RepositoryException.Code.FAILED_PRECONDITION, "Drive bucket has no configured remote binding");
        var stored = drive.readProviderConfig();
        DriveProvisioner.requireSelectedProvider(provider, drive.provider, stored);
        if (drive.credentialsRef != null && !drive.credentialsRef.isBlank()) {
            throw new ai.protomolt.proto.repo.spi.RepositoryException(ai.protomolt.proto.repo.spi.RepositoryException.Code.FAILED_PRECONDITION, "Per-drive credential resolution is not configured");
        }
        if ("s3".equals(provider) && drive.region != null && !drive.region.isBlank()
                && !drive.region.equals(config.s3Region())) {
            throw new ai.protomolt.proto.repo.spi.RepositoryException(ai.protomolt.proto.repo.spi.RepositoryException.Code.FAILED_PRECONDITION, "Drive region differs from the selected backend");
        }
        boolean remote = RepoServiceConfig.BLOB_STORE_REPO.equals(provider)
                || RepoServiceConfig.BLOB_STORE_REPO_INPROCESS.equals(provider);
        if (remote && (stored == null || !stored.hasRemote()))
            throw new ai.protomolt.proto.repo.spi.RepositoryException(ai.protomolt.proto.repo.spi.RepositoryException.Code.FAILED_PRECONDITION, "Remote drive has no persisted binding; explicit migration is required");
        if (stored == null) return;

        if (!stored.getOptionsMap().isEmpty()) {
            throw new ai.protomolt.proto.repo.spi.RepositoryException(ai.protomolt.proto.repo.spi.RepositoryException.Code.FAILED_PRECONDITION, "Per-drive provider options are not supported by this assembly");
        }
        boolean matches = switch (stored.getConfigCase()) {
            case S3 -> Objects.equals(stored.getS3().getEndpointOverride(),
                    config.s3Endpoint() == null ? "" : config.s3Endpoint())
                    && stored.getS3().getForcePathStyle() == (config.s3Endpoint() != null);
            case REDIS -> stored.getRedis().getUri().equals(config.redisUri())
                    && stored.getRedis().getTtlSeconds() == config.redisTtlSeconds()
                    && stored.getRedis().getMaxObjectBytes() == config.redisMaxObjectBytes()
                    && stored.getRedis().getKeyPrefix().isEmpty();
            case REMOTE -> VALIDATOR.validate(stored.getRemote()).valid()
                    && stored.getRemote().getTarget().equals(config.repoTarget())
                    && stored.getRemote().getDriveName().equals(config.repoBucketBindings().get(drive.bucket));
            case CONFIG_NOT_SET -> true;

        };
        if (!matches) throw new ai.protomolt.proto.repo.spi.RepositoryException(ai.protomolt.proto.repo.spi.RepositoryException.Code.FAILED_PRECONDITION, "Drive configuration differs from the selected backend");
    }
}
