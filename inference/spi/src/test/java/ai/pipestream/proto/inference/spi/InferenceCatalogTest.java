package ai.pipestream.proto.inference.spi;

import ai.pipestream.proto.inference.v1.ModelCapabilities;
import ai.pipestream.proto.inference.v1.ModelEntry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InferenceCatalogTest {

    private final InferenceCatalog catalog = new InferenceCatalog();

    private static ModelEntry entry(String id, String provider) {
        return ModelEntry.newBuilder()
                .setId(id)
                .setProvider(provider)
                .setEndpoint("http://example.test:9300")
                .setCapabilities(ModelCapabilities.newBuilder().setMaxContextTokens(8192).build())
                .build();
    }

    @Test
    void registerAndGetRoundTrips() {
        ModelEntry entry = entry("judge", "openvino");
        catalog.register(entry);
        assertThat(catalog.get("judge")).isEqualTo(entry);
        assertThat(catalog.generation()).isEqualTo(1);
    }

    @Test
    void duplicateIdFailsLoud() {
        catalog.register(entry("judge", "openvino"));
        assertThatThrownBy(() -> catalog.register(entry("judge", "openvino")))
                .isInstanceOf(InferenceException.class)
                .hasMessageContaining("duplicate");
    }

    @Test
    void incompleteEntriesFailLoud() {
        assertThatThrownBy(() -> catalog.register(ModelEntry.getDefaultInstance()))
                .isInstanceOf(InferenceException.class)
                .hasMessageContaining("no id");
        assertThatThrownBy(() -> catalog.register(
                ModelEntry.newBuilder().setId("x").build()))
                .isInstanceOf(InferenceException.class)
                .hasMessageContaining("no provider");
        assertThatThrownBy(() -> catalog.register(
                ModelEntry.newBuilder().setId("x").setProvider("openvino").build()))
                .isInstanceOf(InferenceException.class)
                .hasMessageContaining("no endpoint");
    }

    @Test
    void unknownIdFailsLoud() {
        assertThatThrownBy(() -> catalog.get("nope"))
                .isInstanceOf(UnknownModelException.class)
                .hasMessageContaining("nope");
        assertThatThrownBy(() -> catalog.remove("nope"))
                .isInstanceOf(UnknownModelException.class);
    }

    @Test
    void listFiltersByProviderAndSorts() {
        catalog.register(entry("b-model", "openvino"));
        catalog.register(entry("a-model", "openvino"));
        catalog.register(entry("z-model", "nvidia"));
        assertThat(catalog.list("")).extracting(ModelEntry::getId)
                .containsExactly("a-model", "b-model", "z-model");
        assertThat(catalog.list("nvidia")).extracting(ModelEntry::getId)
                .containsExactly("z-model");
    }

    @Test
    void removeBumpsGeneration() {
        catalog.register(entry("judge", "openvino"));
        catalog.remove("judge");
        assertThat(catalog.generation()).isEqualTo(2);
        assertThat(catalog.list("")).isEmpty();
    }
}
