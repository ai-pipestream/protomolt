package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.v1.RemoteDriveConfig;
import ai.protomolt.proto.validate.ProtoValidator;
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
}
