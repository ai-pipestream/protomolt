package ai.protomolt.proto.repo.blob.spi;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlobStoresTest {
    @Test void identityResolutionRejectsUnsupportedAndMisdirectedProvidersWithoutOpeningClients() {
        var unsupported = new BlobStoreProvider() {
            public String id() { return "unsupported"; }
            public OpenedBlobStore open(Map<String, String> options) { throw new AssertionError("Identity lookup acquired resources"); }
        };
        var misdirected = new BlobStoreProvider() {
            public String id() { return "misdirected"; }
            public OpenedBlobStore open(Map<String, String> options) { throw new AssertionError("Identity lookup acquired resources"); }
            public BackendIdentity managedIdentity(Map<String, String> options) {
                return new BackendIdentity("other", "other/v1", Map.of("namespace", "elsewhere"));
            }
        };
        var providers = BlobStores.of(List.of(unsupported, misdirected));
        assertThatThrownBy(() -> providers.managedIdentity("unsupported", Map.of()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> providers.managedIdentity("misdirected", Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("different backend identity");
    }

    @Test void emptyInstallationHasNoImplicitBackend() {
        var providers = BlobStores.discover();
        assertThat(providers.providerIds()).isEmpty();
        assertThatThrownBy(() -> providers.open("memory", Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void factoryFailureIsPreservedWithoutOpeningAnotherProvider() {
        var calls = new AtomicInteger();
        var failure = new IllegalStateException("acquisition failed");
        var selected = new BlobStoreProvider() {
            public String id() { return "selected"; }
            public OpenedBlobStore open(Map<String, String> options) { throw failure; }
        };
        var unselected = new BlobStoreProvider() {
            public String id() { return "unselected"; }
            public OpenedBlobStore open(Map<String, String> options) {
                calls.incrementAndGet();
                throw new AssertionError("Unselected factory invoked");
            }
        };
        var providers = BlobStores.of(List.of(selected, unselected));
        assertThatThrownBy(() -> providers.open("selected", Map.of())).isSameAs(failure);
        assertThat(calls.get()).isZero();
    }

    @Test void rejectsMalformedIdentityAndNullHandle() {
        var malformed = new BlobStoreProvider() {
            public String id() { return "Bad ID"; }
            public OpenedBlobStore open(Map<String, String> options) { throw new AssertionError(); }
        };
        assertThatThrownBy(() -> BlobStores.of(List.of(malformed)))
                .isInstanceOf(IllegalArgumentException.class);
        var broken = new BlobStoreProvider() {
            public String id() { return "broken"; }
            public OpenedBlobStore open(Map<String, String> options) { return null; }
        };
        assertThatThrownBy(() -> BlobStores.of(List.of(broken)).open("broken", Map.of()))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("no handle");
    }

    @Test void validatesBackendIdentityAndStrongEtagGrammar() {
        var valid = new BackendIdentity("s3-1", "s3-1/v10", Map.of("endpoint-url", "https://store.example"));
        assertThat(valid.provider()).isEqualTo("s3-1");
        assertThat(valid.schema()).isEqualTo("s3-1/v10");

        for (String badProvider : List.of("", "1s3", "-s3", "S3", "s3_1", "a".repeat(33))) {
            assertThatThrownBy(() -> new BackendIdentity(badProvider, badProvider + "/v1", Map.of("k", "v")))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        for (String badSchema : List.of("s3", "s3/", "s3/v", "s3/v0", "s3/v01", "s3/v1234567890", "other/v1", "s3/V1")) {
            assertThatThrownBy(() -> new BackendIdentity("s3", badSchema, Map.of("k", "v")))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        for (String badKey : List.of("", "1k", "Key", "k_1", "a".repeat(65))) {
            assertThatThrownBy(() -> new BackendIdentity("s3", "s3/v1", Map.of(badKey, "v")))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        assertThat(BlobStore.requireStrongEtag("\"abc!#~\"")).isEqualTo("\"abc!#~\"");
        for (String badEtag : List.of("", "\"\"", "abc", "W/\"abc\"", "\"ab\"c\"", "\"ab c\"", "\"" + "a".repeat(1023) + "\"")) {
            assertThatThrownBy(() -> BlobStore.requireStrongEtag(badEtag))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
