package ai.protomolt.proto.repo.blob.spi;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class BoundedReadDefaultTest {
    @Test void unsupportedAdapterNeverFallsBackToAnUnboundedRead() {
        var store = (BlobStore) Proxy.newProxyInstance(BlobStore.class.getClassLoader(), new Class<?>[]{BlobStore.class},
                (proxy, method, args) -> {
                    if (method.isDefault()) return InvocationHandler.invokeDefault(proxy, method, args);
                    throw new AssertionError("Unexpected provider call: " + method.getName());
                });
        assertThatThrownBy(() -> store.getBounded("namespace", "key", null, 10))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.getBounded("namespace", "key", null, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
