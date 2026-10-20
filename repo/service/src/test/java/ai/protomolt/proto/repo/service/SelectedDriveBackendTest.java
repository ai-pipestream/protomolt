package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.DriveProviderConfig;
import ai.protomolt.proto.repo.v1.RedisDriveConfig;
import ai.protomolt.proto.repo.v1.RemoteDriveConfig;
import ai.protomolt.proto.repo.v1.S3DriveConfig;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class SelectedDriveBackendTest {
    private static RepoServiceConfig config(String mode) {
        return RepoServiceConfig.fromEnvironment(Map.of(RepoServiceConfig.ENV_BLOB_STORE, mode,
                RepoServiceConfig.ENV_S3_ENDPOINT, "http://storage:9000",
                RepoServiceConfig.ENV_S3_REGION, "us-east-1"));
    }

    private static DriveRecord drive(String provider, DriveProviderConfig settings) {
        var drive = new DriveRecord();
        drive.provider = provider;
        drive.region = "us-east-1";
        if (settings != null) drive.writeProviderConfig(settings);
        return drive;
    }

    private static void refused(SelectedDriveBackend backend, DriveRecord drive) {
        assertThatThrownBy(() -> backend.accept(drive)).isInstanceOfSatisfying(RepositoryException.class,
                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
    }

    @Test void refusesCredentialReferencesAndDifferentRegionsButAllowsUnspecifiedSettings() {
        var backend = new SelectedDriveBackend(config("s3"));
        var drive = drive("s3", null);
        backend.accept(drive);
        drive.credentialsRef = "secret-reference";
        refused(backend, drive);
        drive.credentialsRef = " ";
        backend.accept(drive);
        drive.region = "eu-west-1";
        refused(backend, drive);
        drive.region = null;
        backend.accept(drive);
    }

    @Test void s3AndCacheRequireTheSelectedEndpointAndAddressingMode() {
        var good = S3DriveConfig.newBuilder().setEndpointOverride("http://storage:9000").setForcePathStyle(true);
        for (var mode : java.util.List.of("s3", "s3-redis-cache")) {
            var backend = new SelectedDriveBackend(config(mode));
            backend.accept(drive("s3", DriveProviderConfig.newBuilder().setS3(good).build()));
            refused(backend, drive("s3", DriveProviderConfig.newBuilder()
                    .setS3(good.clone().setEndpointOverride("http://other:9000")).build()));
            refused(backend, drive("s3", DriveProviderConfig.newBuilder()
                    .setS3(good.clone().setForcePathStyle(false)).build()));
            refused(backend, drive("s3", DriveProviderConfig.newBuilder().setS3(good)
                    .putOptions("timeout", "5").build()));
        }
        var aws = new SelectedDriveBackend(RepoServiceConfig.fromEnvironment(Map.of()));
        aws.accept(drive("s3", DriveProviderConfig.newBuilder().setS3(S3DriveConfig.getDefaultInstance()).build()));
    }

    @Test void redisRequiresEverySettingToMatchAndRejectsPrefixAndProviderOverrides() {
        var config = config("redis");
        var backend = new SelectedDriveBackend(config);
        var good = RedisDriveConfig.newBuilder().setUri(config.redisUri())
                .setTtlSeconds(config.redisTtlSeconds()).setMaxObjectBytes(config.redisMaxObjectBytes());
        backend.accept(drive("redis", DriveProviderConfig.newBuilder().setRedis(good).build()));
        for (Consumer<RedisDriveConfig.Builder> change : java.util.List.<Consumer<RedisDriveConfig.Builder>>of(
                b -> b.setUri("redis://other:6379"), b -> b.setTtlSeconds(1),
                b -> b.setMaxObjectBytes(1), b -> b.setKeyPrefix("other:"))) {
            var changed = good.clone();
            change.accept(changed);
            refused(backend, drive("redis", DriveProviderConfig.newBuilder().setRedis(changed).build()));
        }
        refused(backend, drive("s3", null));
        refused(backend, drive("redis", DriveProviderConfig.newBuilder().setS3(S3DriveConfig.getDefaultInstance()).build()));
        refused(backend, drive("redis", DriveProviderConfig.newBuilder().setRedis(good).putOptions("pool", "2").build()));
    }

    @Test void persistedRemoteIdentityNeverFallsThroughAsLocalSettings() {
        var remote = DriveProviderConfig.newBuilder().setRemote(RemoteDriveConfig.newBuilder()
                .setTarget("old-host").setDriveName("old-drive")).build();
        for (var provider : java.util.List.of("s3", "redis"))
            refused(new SelectedDriveBackend(config(provider)), drive(provider, remote));
    }
}
