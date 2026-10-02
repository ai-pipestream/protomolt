package ai.protomolt.proto.registry;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaRegistryStoresTest {
    @Test
    void coreDiscoversMemoryWithoutGitAndRequiresExplicitSelection() {
        var stores = SchemaRegistryStores.discover();
        assertThat(stores.providerIds()).containsExactly("memory");
        assertThatThrownBy(() -> stores.open("git", Map.of(), null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not installed");
        assertThatThrownBy(() -> stores.open("", Map.of(), null))
                .isInstanceOf(IllegalArgumentException.class);
        try (var store = stores.open("memory", Map.of(), null)) {
            assertThat(store).isInstanceOf(InMemorySchemaRegistryStore.class);
        }
        assertThatThrownBy(() -> stores.open("memory", Map.of("directory", "unused"), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void duplicateAndInvalidIdsFailBeforeOpeningStorage() {
        var opened = new AtomicInteger();
        var provider = provider("test", opened, null);
        assertThatThrownBy(() -> SchemaRegistryStores.of(List.of(provider, provider)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Duplicate");
        assertThatThrownBy(() -> SchemaRegistryStores.of(List.of(provider("Bad ID", opened, null))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(opened).hasValue(0);
    }

    @Test
    void discoveryDoesNotOpenProvidersAndSelectionDoesNotFallback() {
        var opened = new AtomicInteger();
        var unused = new AtomicInteger();
        var failure = new RegistryStoreException("unavailable");
        var stores = SchemaRegistryStores.of(List.of(provider("broken", opened, failure), provider("unused", unused, null)));
        assertThat(opened).hasValue(0);
        assertThatThrownBy(() -> stores.open("broken", Map.of(), null)).isSameAs(failure);
        assertThat(opened).hasValue(1);
        assertThat(unused).hasValue(0);
    }

    @Test
    void selectedMemoryProviderUsesTheCallersWriteGate() {
        SchemaRegistryStore.WriteGate reject = (subject, mode, history, text, refs, resolver) -> List.of("policy rejected");
        try (var store = SchemaRegistryStores.discover().open("memory", Map.of(), reject)) {
            assertThatThrownBy(() -> store.register("test.proto", "syntax = \"proto3\"; message Test {}", List.of()))
                    .isInstanceOf(IncompatibleRegistrationException.class);
            assertThat(store.subjects()).isEmpty();
        }
    }

    private static SchemaRegistryStoreProvider provider(String id, AtomicInteger opened, RuntimeException failure) {
        return new SchemaRegistryStoreProvider() {
            public String id() { return id; }
            public SchemaRegistryStore open(Map<String, String> options, SchemaRegistryStore.WriteGate gate) {
                opened.incrementAndGet();
                if (failure != null) throw failure;
                return new InMemorySchemaRegistryStore(gate);
            }
        };
    }
}
