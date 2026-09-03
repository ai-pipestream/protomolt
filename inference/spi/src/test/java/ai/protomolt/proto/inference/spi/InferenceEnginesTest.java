package ai.protomolt.proto.inference.spi;

import ai.protomolt.proto.inference.v1.ChatTurn;
import ai.protomolt.proto.inference.v1.DescribeModelRequest;
import ai.protomolt.proto.inference.v1.GenerateRequest;
import ai.protomolt.proto.inference.v1.GenerateResponse;
import ai.protomolt.proto.inference.v1.GenerateStreamRequest;
import ai.protomolt.proto.inference.v1.ListModelsRequest;
import ai.protomolt.proto.inference.v1.ModelCapabilities;
import ai.protomolt.proto.inference.v1.ModelEntry;
import ai.protomolt.proto.inference.v1.Role;
import ai.protomolt.proto.inference.v1.StructuredOutputConstraint;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InferenceEnginesTest {

    /** A recording fake: proves the facade resolves and delegates. */
    static final class FakeProvider implements InferenceProvider {
        ModelEntry lastModel;
        GenerateRequest lastRequest;

        @Override
        public String id() {
            return "fake";
        }

        @Override
        public GenerateResponse generate(ModelEntry model, GenerateRequest request) {
            this.lastModel = model;
            this.lastRequest = request;
            return GenerateResponse.newBuilder()
                    .setText("fake answer")
                    .setModel(request.getModel())
                    .setProvider(id())
                    .build();
        }

        @Override
        public void generateStream(ModelEntry model, GenerateStreamRequest request,
                                   ChunkObserver observer) {
            throw new InferenceException("fake does not stream");
        }
    }

    private final FakeProvider fake = new FakeProvider();
    private final InferenceCatalog catalog = new InferenceCatalog();
    private final InferenceEngines engines = new InferenceEngines(catalog, List.of(fake));

    private static ModelEntry entry(String id, String provider) {
        return ModelEntry.newBuilder()
                .setId(id).setProvider(provider).setEndpoint("http://example.test")
                .build();
    }

    private static GenerateRequest request(String model) {
        return GenerateRequest.newBuilder()
                .setModel(model)
                .addMessages(ChatTurn.newBuilder().setRole(Role.ROLE_USER).setContent("hi"))
                .build();
    }

    @Test
    void generateResolvesAndDelegates() {
        engines.register(entry("judge", "fake"));
        GenerateResponse response = engines.generate(request("judge"));
        assertThat(response.getText()).isEqualTo("fake answer");
        assertThat(response.getProvider()).isEqualTo("fake");
        assertThat(fake.lastModel.getId()).isEqualTo("judge");
        assertThat(fake.lastRequest.getMessages(0).getContent()).isEqualTo("hi");
    }

    @Test
    void registerRejectsUnloadedProvider() {
        assertThatThrownBy(() -> engines.register(entry("judge", "not-loaded")))
                .isInstanceOf(InferenceException.class)
                .hasMessageContaining("not-loaded");
    }

    @Test
    void unknownModelFailsLoud() {
        assertThatThrownBy(() -> engines.generate(request("ghost")))
                .isInstanceOf(UnknownModelException.class)
                .hasMessageContaining("ghost");
    }

    @Test
    void structuredOutputRequiresCatalogCapabilityBeforeDelegation() {
        engines.register(entry("judge", "fake"));
        GenerateRequest structured = GenerateRequest.newBuilder(request("judge"))
                .setStructuredOutput(StructuredOutputConstraint.newBuilder()
                        .setName("example_Form")
                        .setJsonSchema("{\"type\":\"object\"}"))
                .build();

        assertThatThrownBy(() -> engines.generate(structured))
                .isInstanceOf(InferenceException.class)
                .hasMessageContaining("structured-output capability");
        assertThat(fake.lastRequest).isNull();
    }

    @Test
    void capableModelDelegatesStructuredOutputConstraintUnchanged() {
        engines.register(ModelEntry.newBuilder(entry("judge", "fake"))
                .setCapabilities(ModelCapabilities.newBuilder().setStructuredOutput(true))
                .build());
        StructuredOutputConstraint constraint = StructuredOutputConstraint.newBuilder()
                .setName("example_Form")
                .setJsonSchema("{\"type\":\"object\"}")
                .build();

        engines.generate(GenerateRequest.newBuilder(request("judge"))
                .setStructuredOutput(constraint).build());

        assertThat(fake.lastRequest.getStructuredOutput()).isEqualTo(constraint);
    }

    @Test
    void duplicateProviderIdsFailLoud() {
        assertThatThrownBy(() -> new InferenceEngines(catalog, List.of(fake, new FakeProvider())))
                .isInstanceOf(InferenceException.class)
                .hasMessageContaining("fake");
    }

    @Test
    void listAndDescribeWork() {
        engines.register(entry("judge", "fake"));
        assertThat(engines.listModels(ListModelsRequest.getDefaultInstance()).getModelsList())
                .extracting(ModelEntry::getId).containsExactly("judge");
        assertThat(engines.describe(DescribeModelRequest.newBuilder().setModel("judge").build())
                .getEntry().getProvider()).isEqualTo("fake");
    }

    @Test
    void serviceLoaderDiscoversTheTestProvider() {
        // test-provider.META-INF/services registers TestDiscoveredProvider.
        InferenceEngines discovered = new InferenceEngines(catalog);
        assertThat(discovered.providers()).containsKey("test-discovered");
    }
}
