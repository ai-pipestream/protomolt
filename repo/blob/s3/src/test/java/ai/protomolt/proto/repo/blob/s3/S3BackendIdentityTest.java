package ai.protomolt.proto.repo.blob.s3;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class S3BackendIdentityTest {
    @Test void selectedFactoryProducesTheIdentityAndPreservesIpv6Origins() {
        var providers = ai.protomolt.proto.repo.blob.spi.BlobStores.of(java.util.List.of(new S3BlobStoreProvider()));
        var identity = providers.managedIdentity("s3", Map.of("endpoint", "http://[::1]:9000/", "region", "us-east-1", "path-style", "true"));
        assertThat(identity.location().get("endpoint")).isEqualTo("http://[::1]:9000");
        assertThat(S3BackendIdentity.of(identity.location().get("endpoint"), "us-east-1", true)).isEqualTo(identity);
        assertThatThrownBy(() -> providers.managedIdentity("absent", Map.of())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void canonicalLocationExcludesCredentialsAndSurvivesTheirRotation() {
        var options = new HashMap<>(Map.of("endpoint", "https://STORE.example:443/", "region", "us-east-1",
                "path-style", "true", "access-key", "first", "secret-key", "first-secret"));
        var provider = new S3BlobStoreProvider();
        var identity = provider.managedIdentity(options);
        assertThat(identity.provider()).isEqualTo("s3");
        assertThat(identity.schema()).isEqualTo("s3/v1");
        assertThat(identity.location()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "endpoint", "https://store.example", "region", "us-east-1", "path-style", "true"));
        options.put("secret-key", "rotated-secret");
        options.put("access-key", "rotated-key");
        assertThat(provider.managedIdentity(options)).isEqualTo(identity);
        assertThatThrownBy(() -> identity.location().put("endpoint", "https://other.example"))
                .isInstanceOf(UnsupportedOperationException.class);
        options.put("endpoint", "https://other.example");
        assertThat(provider.managedIdentity(options)).isNotEqualTo(identity);
    }

    @Test void sdkResolutionAndExplicitLocationRemainDifferent() {
        var provider = new S3BlobStoreProvider();
        var implicit = provider.managedIdentity(Map.of("endpoint", "", "region", "us-east-1", "path-style", "false"));
        assertThat(implicit.location().get("endpoint")).isEqualTo(S3BackendIdentity.SDK_DEFAULT);
        assertThat(implicit).isNotEqualTo(S3BackendIdentity.of("https://s3.us-east-1.amazonaws.com", "us-east-1", false));
        assertThat(implicit).isNotEqualTo(S3BackendIdentity.of(S3BackendIdentity.SDK_DEFAULT, "us-west-2", false));
    }

    @Test void ambiguousOrSensitiveOriginsFailWithoutEchoingValues() {
        for (String endpoint : java.util.List.of("https://user:secret@store.example", "https://store.example?token=secret",
                "https://store.example#secret", "https://store.example/secret", "https://store.example:99999")) {
            assertThatThrownBy(() -> S3BackendIdentity.of(endpoint, "us-east-1", true))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageNotContaining("secret").hasNoCause();
        }
        assertThatThrownBy(() -> new S3BlobStoreProvider().managedIdentity(Map.of("endpoint", "", "region", "us-east-1")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
