package ai.protomolt.proto.registry;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RegistrationHistoryTest {
    @Test
    void aListedVersionMustNotDisappearFromCompatibilityHistory() {
        SchemaRegistryStore inconsistent = (SchemaRegistryStore) Proxy.newProxyInstance(
                SchemaRegistryStore.class.getClassLoader(),
                new Class<?>[] {SchemaRegistryStore.class}, (proxy, method, args) -> {
                    return switch (method.getName()) {
                        case "versions" -> List.of(1);
                        case "version" -> Optional.empty();
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
        assertThatThrownBy(() -> RegistrationSupport.history(inconsistent, "example.proto"))
                .isInstanceOf(RegistryStoreException.class)
                .hasMessageContaining("example.proto")
                .hasMessageContaining("1");
    }
}
