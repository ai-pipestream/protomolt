package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStores;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3ProviderTest {
    private static Map<String, String> options() {
        return new HashMap<>(Map.of("endpoint", "http://127.0.0.1:1", "region", "us-east-1",
                "access-key", "test", "secret-key", "test-secret", "path-style", "true",
                "conditional-writes", "false"));
    }

    @Test void discoveryAndOpeningDoNotContactTheEndpoint() throws Exception {
        var providers = BlobStores.discover();
        assertThat(providers.providerIds()).containsExactly("s3");
        try (var opened = providers.open("s3", options())) {
            assertThat(opened.store()).isInstanceOf(S3BlobStore.class);
        }
    }

    @Test void refusesUnknownAndInvalidConfiguration() {
        var providers = BlobStores.discover();
        var unknown = options();
        unknown.put("unknown", "value");
        assertThatThrownBy(() -> providers.open("s3", unknown)).isInstanceOf(IllegalArgumentException.class);
        var invalid = options();
        invalid.put("conditional-writes", "yes");
        assertThatThrownBy(() -> providers.open("s3", invalid)).isInstanceOf(IllegalArgumentException.class);
        var credentialUrl = options();
        credentialUrl.put("endpoint", "http://user:secret@localhost");
        assertThatThrownBy(() -> providers.open("s3", credentialUrl))
                .isInstanceOf(IllegalArgumentException.class).hasMessageNotContaining("secret");
    }

    @Test void explicitDefaultChainDoesNotResolveCredentialsDuringAcquisition() throws Exception {
        var options = options();
        options.remove("access-key");
        options.remove("secret-key");
        options.put("credentials-mode", "default-chain");
        options.put("endpoint", "");
        options.put("path-style", "false");
        try (var opened = BlobStores.discover().open("s3", options)) {
            assertThat(opened.store()).isInstanceOf(S3BlobStore.class);
        }
    }
}
