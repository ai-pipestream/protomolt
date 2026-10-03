package ai.protomolt.proto.repo.blob.spi;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlobStoresTest {
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
}
