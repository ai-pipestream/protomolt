package ai.protomolt.proto.repo.blob.spi;

import java.lang.reflect.Proxy;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OpenedBlobStoreExactVersionTest {
    private static BlobStore storeStub() {
        return (BlobStore) Proxy.newProxyInstance(OpenedBlobStoreExactVersionTest.class.getClassLoader(),
                new Class<?>[] { BlobStore.class },
                (proxy, method, args) -> { throw new UnsupportedOperationException(method.getName()); });
    }

    @Test void advertisedCapabilityRequiresExplicitPort() {
        assertThatThrownBy(() -> new OpenedBlobStore(storeStub(), () -> { },
                Set.of(BlobCapability.PHYSICAL_RECLAMATION, BlobCapability.EXACT_VERSION_RECLAMATION),
                name -> { }, (bucket, key) -> true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("EXACT_VERSION_RECLAMATION requires a reclaimer port");
    }

    @Test void absentCapabilityGetsUnsupportedPort() throws Exception {
        try (var opened = new OpenedBlobStore(storeStub(), () -> { }, Set.of())) {
            assertThatThrownBy(() -> opened.exactVersionReclaimer().reclaim("bucket", "key", "version"))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Exact-version reclamation is unsupported");
        }
    }

    @Test void wiredPortIsReturned() throws Exception {
        ExactVersionReclaimer port = (bucket, key, versionId) -> ExactVersionOutcome.ABSENT;
        try (var opened = new OpenedBlobStore(storeStub(), () -> { },
                Set.of(BlobCapability.EXACT_VERSION_RECLAMATION),
                name -> { }, (bucket, key) -> true, port)) {
            assertThat(opened.exactVersionReclaimer()).isSameAs(port);
            assertThat(opened.exactVersionReclaimer().reclaim("bucket", "key", "version"))
                    .isEqualTo(ExactVersionOutcome.ABSENT);
        }
    }

    @Test void closedHandleRefusesPortAccess() throws Exception {
        var opened = new OpenedBlobStore(storeStub(), () -> { }, Set.of());
        opened.close();
        assertThatThrownBy(opened::exactVersionReclaimer)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Storage handle is closed");
    }
    @Test void uncertaintyRetainsExactFailureAndRejectsMissingDiagnostics() {
        var cause = new java.io.IOException("private backend detail");
        var failure = new BlobStoreException(BlobStoreException.Code.UNAVAILABLE, "Provider unavailable", cause);
        var result = ExactVersionOutcome.uncertain(ExactVersionOutcome.Phase.DELETE, failure);
        assertThat(result.failure()).isSameAs(failure);
        assertThat(result.failure().getCause()).isSameAs(cause);
        assertThatThrownBy(() -> ExactVersionOutcome.uncertain(ExactVersionOutcome.Phase.DELETE, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ExactVersionOutcome(ExactVersionOutcome.Status.DELETED,
                ExactVersionOutcome.Phase.DELETE, failure)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void wrapperExplicitlyForwardsExactPort() throws Exception {
        ExactVersionReclaimer port = (bucket, key, version) -> ExactVersionOutcome.ABSENT;
        try (var original = new OpenedBlobStore(storeStub(), () -> {},
                     Set.of(BlobCapability.EXACT_VERSION_RECLAMATION), name -> {}, (bucket, key) -> true, port);
             var wrapper = new OpenedBlobStore(original.store(), () -> {}, original.capabilities(),
                     original::ensureNamespace, original.reclaimer(), original.exactVersionReclaimer())) {
            assertThat(wrapper.capabilities()).contains(BlobCapability.EXACT_VERSION_RECLAMATION);
            assertThat(wrapper.exactVersionReclaimer()).isSameAs(port);
        }
    }
}
