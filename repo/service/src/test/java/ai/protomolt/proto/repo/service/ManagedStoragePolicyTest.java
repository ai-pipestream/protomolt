package ai.protomolt.proto.repo.service;

import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ManagedStoragePolicyTest {
    @Test void unqualifiedRedisIsRefusedBeforeDatabaseOrProviderAcquisition() {
        var config = new RepoServiceConfig(0,
                new ai.protomolt.proto.repo.container.ledger.LedgerConfig("jdbc:postgresql://127.0.0.1:1/unavailable", "unused", "unused"),
                "http://127.0.0.1:1", "us-east-1", "unused", "unused", "policy-test", 0,
                "redis", null, null, "redis://127.0.0.1:1", 0, 1024)
                .withManagedStorage(new ManagedStoragePolicy("redis-test", "test-realm", true));
        var providers = ai.protomolt.proto.repo.blob.spi.BlobStores.of(java.util.List.of());
        assertThatThrownBy(() -> new RepoServices(config, ai.protomolt.proto.asset.bridge.BridgeEngine.standard(), providers))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Managed storage requires an S3 backing store and enabled lifecycle recovery");
    }

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
