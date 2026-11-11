package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.v1.DriveCacheConfig;
import ai.protomolt.proto.repo.v1.DriveProviderConfig;
import ai.protomolt.proto.repo.v1.RemoteDriveConfig;
import ai.protomolt.proto.repo.v1.S3DriveConfig;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.protovalidate.ProtovalidateRuleSource;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class RemoteDriveConfigTest {
    @Test void validatesPersistedRemoteIdentityThroughRuntimeEngine() {
        var validator = ProtoValidator.create();
        var valid = RemoteDriveConfig.newBuilder().setTarget("repo.example:9090").setDriveName("archive").build();
        assertThat(validator.validate(valid).valid()).isTrue();
        assertThat(validator.validate(valid.toBuilder().clearTarget().build()).valid()).isFalse();
        assertThat(validator.validate(valid.toBuilder().setDriveName(" ").build()).valid()).isFalse();
        assertThat(validator.validate(valid.toBuilder().setTarget("x".repeat(4097)).build()).valid()).isFalse();
        assertThat(validator.validate(valid.toBuilder().setDriveName("x".repeat(1025)).build()).valid()).isFalse();
    }

    @Test void validatesPerDriveReadThroughCacheConfigThroughRuntimeEngines() throws Exception {
        var validCache = DriveCacheConfig.newBuilder()
                .setProvider("valkey")
                .setUri("redis://127.0.0.1:6379")
                .setTtlSeconds(60)
                .setMaxCacheableBytes(1024)
                .setKeyPrefix("drive-a:")
                .setCredentialsRef("env:DRIVE_CACHE")
                .build();
        var uncachedS3 = S3DriveConfig.newBuilder()
                .setEndpointOverride("http://127.0.0.1:4566")
                .setForcePathStyle(true)
                .build();
        var cachedS3 = uncachedS3.toBuilder().setReadThroughCache(validCache).build();

        for (var rules : List.of(new ProtomoltRuleSource(), new ProtovalidateRuleSource())) {
            var validator = ProtoValidator.create(List.of(rules));

            assertThat(validator.validate(uncachedS3).valid()).isTrue();
            assertThat(validator.validate(cachedS3).valid()).isTrue();
            assertThat(validator.validate(validCache).valid()).isTrue();
            assertThat(validator.validate(validCache.toBuilder().clearProvider().build()).valid()).isTrue();
            assertThat(validator.validate(validCache.toBuilder().setProvider("redis").build()).valid()).isTrue();
            assertThat(validator.validate(DriveProviderConfig.newBuilder().setS3(cachedS3).build()).valid()).isTrue();

            var dynamicValid = DynamicMessage.parseFrom(
                    DriveCacheConfig.getDescriptor(), validCache.toByteString());
            assertThat(validator.validate(dynamicValid).valid()).isTrue();

            for (var invalidCache : List.of(
                    DriveCacheConfig.getDefaultInstance(),
                    validCache.toBuilder().setProvider("memcached").build(),
                    validCache.toBuilder().clearUri().build(),
                    validCache.toBuilder().setUri("   ").build(),
                    validCache.toBuilder().setUri("x".repeat(4097)).build(),
                    validCache.toBuilder().setTtlSeconds(0).build(),
                    validCache.toBuilder().setTtlSeconds(-1).build(),
                    validCache.toBuilder().setMaxCacheableBytes(0).build(),
                    validCache.toBuilder().setMaxCacheableBytes(-10).build(),
                    validCache.toBuilder().setKeyPrefix("x".repeat(513)).build(),
                    validCache.toBuilder().setCredentialsRef("x".repeat(513)).build())) {
                assertThat(validator.validate(invalidCache).valid())
                        .as("generated cache %s", invalidCache)
                        .isFalse();
                var dynamicInvalid = DynamicMessage.parseFrom(
                        DriveCacheConfig.getDescriptor(), invalidCache.toByteString());
                assertThat(validator.validate(dynamicInvalid).valid())
                        .as("dynamic cache %s", invalidCache)
                        .isFalse();
                var invalidS3 = uncachedS3.toBuilder().setReadThroughCache(invalidCache).build();
                assertThat(validator.validate(invalidS3).valid())
                        .as("nested S3DriveConfig %s", invalidCache)
                        .isFalse();
            }
        }
    }
}
