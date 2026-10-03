package ai.protomolt.proto.repo.service;

import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ManagedStoragePolicyTest {
    @Test void absentQualificationIsDisabled() {
        assertThat(ManagedStoragePolicy.fromEnvironment(Map.of())).isEqualTo(ManagedStoragePolicy.disabled());
    }

    @Test void qualificationRequiresGenerationAndRealm() {
        for (Map<String, String> env : java.util.List.of(
                Map.of(ManagedStoragePolicy.ENV_RETENTION, "true"),
                Map.of(ManagedStoragePolicy.ENV_RETENTION, "true", ManagedStoragePolicy.ENV_GENERATION, "nas-v1"),
                Map.of(ManagedStoragePolicy.ENV_GENERATION, "nas-v1"),
                Map.of(ManagedStoragePolicy.ENV_REALM, "account-a"),
                Map.of(ManagedStoragePolicy.ENV_RETENTION, ""),
                Map.of(ManagedStoragePolicy.ENV_GENERATION, " "),
                Map.of(ManagedStoragePolicy.ENV_REALM, " "),
                Map.of(ManagedStoragePolicy.ENV_RETENTION, "tru"))) {
            assertThatThrownBy(() -> ManagedStoragePolicy.fromEnvironment(env)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test void acceptsExplicitQualificationWithoutCredentialFields() {
        var policy = ManagedStoragePolicy.fromEnvironment(Map.of(ManagedStoragePolicy.ENV_RETENTION, "true",
                ManagedStoragePolicy.ENV_GENERATION, "nas-v1", ManagedStoragePolicy.ENV_REALM, "account-a"));
        assertThat(policy).isEqualTo(new ManagedStoragePolicy("nas-v1", "account-a", true));
    }

    @Test void rejectsEndpointLikeIdentifiersWithoutEchoingTheirValues() {
        String sensitive = "https://user:password@example.com";
        assertThatThrownBy(() -> new ManagedStoragePolicy(sensitive, "realm", true))
                .isInstanceOf(IllegalArgumentException.class).hasMessageNotContaining(sensitive);
    }
}
