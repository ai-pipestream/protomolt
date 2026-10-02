package ai.protomolt.proto.registry;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigSupportTest {
    @ParameterizedTest
    @NullAndEmptySource
    void missingEnvelopeIsAnExplicitInputError(String envelope) {
        try (var store = new InMemorySchemaRegistryStore()) {
            assertThatThrownBy(() -> ConfigSupport.gate(store, envelope))
                    .isInstanceOf(InvalidConfigException.class)
                    .hasMessageContaining("envelope");
        }
    }

    @Test
    void invalidJsonRetainsItsParseCause() {
        try (var store = new InMemorySchemaRegistryStore()) {
            assertThatThrownBy(() -> ConfigSupport.gate(store, "{"))
                    .isInstanceOf(InvalidConfigException.class)
                    .hasCauseInstanceOf(com.fasterxml.jackson.core.JsonProcessingException.class);
        }
    }

    @Test
    void invalidMessageRetainsItsParseCause() {
        try (var store = new InMemorySchemaRegistryStore()) {
            store.register("config.proto", "syntax = \"proto3\"; package test; message Config { int32 limit = 1; }", List.of());
            assertThatThrownBy(() -> ConfigSupport.gate(store,
                    "{\"messageType\":\"test.Config\",\"config\":{\"unknown\":1}}"))
                    .isInstanceOf(InvalidConfigException.class)
                    .hasCauseInstanceOf(com.google.protobuf.InvalidProtocolBufferException.class);
        }
    }

    @Test
    void aListedSubjectWithoutALatestVersionIsCorruptState() {
        assertThatThrownBy(() -> ConfigSupport.resolveType(store(Optional.empty()), "test.Config"))
                .isInstanceOf(RegistryStoreException.class).hasMessageContaining("broken.proto");
    }

    @Test
    void anUncompilableSubjectIsNotReportedAsTypeNotFound() {
        var invalid = new StoredSchema("broken.proto", 1, 1, "not protobuf", List.of(), "unused");
        assertThatThrownBy(() -> ConfigSupport.resolveType(store(Optional.of(invalid)), "test.Config"))
                .isInstanceOf(RegistryStoreException.class)
                .hasMessageContaining("broken.proto")
                .hasCauseInstanceOf(ai.protomolt.proto.sources.ProtoCompilationException.class);
    }

    private static SchemaRegistryStore store(Optional<StoredSchema> latest) {
        return (SchemaRegistryStore) Proxy.newProxyInstance(ConfigSupportTest.class.getClassLoader(),
                new Class<?>[] {SchemaRegistryStore.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "subjects" -> List.of("broken.proto");
                    case "latest" -> latest;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
