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

    @Test void capabilityIsDroppedRatherThanAdvertisedWithoutPort() throws Exception {
        try (var opened = new OpenedBlobStore(storeStub(), () -> { },
                Set.of(BlobCapability.PHYSICAL_RECLAMATION, BlobCapability.EXACT_VERSION_RECLAMATION),
                name -> { }, (bucket, key) -> true)) {
            assertThat(opened.capabilities()).contains(BlobCapability.PHYSICAL_RECLAMATION)
                    .doesNotContain(BlobCapability.EXACT_VERSION_RECLAMATION);
            assertThatThrownBy(() -> opened.exactVersionReclaimer().reclaim("bucket", "key", "version"))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Exact-version reclamation is unsupported");
        }
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
}
