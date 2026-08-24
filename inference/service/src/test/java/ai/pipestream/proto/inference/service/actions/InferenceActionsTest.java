package ai.pipestream.proto.inference.service.actions;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.inference.spi.ChunkObserver;
import ai.pipestream.proto.inference.spi.InferenceCatalog;
import ai.pipestream.proto.inference.spi.InferenceEngines;
import ai.pipestream.proto.inference.spi.InferenceException;
import ai.pipestream.proto.inference.spi.InferenceProvider;
import ai.pipestream.proto.inference.v1.FinishReason;
import ai.pipestream.proto.inference.v1.GenerateRequest;
import ai.pipestream.proto.inference.v1.GenerateResponse;
import ai.pipestream.proto.inference.v1.GenerateStreamRequest;
import ai.pipestream.proto.inference.v1.ModelCapabilities;
import ai.pipestream.proto.inference.v1.ModelEntry;
import ai.pipestream.proto.inference.v1.Usage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InferenceActionsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static final class StubProvider implements InferenceProvider {
        @Override
        public String id() {
            return "stub";
        }

        @Override
        public GenerateResponse generate(ModelEntry model, GenerateRequest request) {
            if (request.getMessages(request.getMessagesCount() - 1).getContent().equals("boom")) {
                throw new InferenceException("backend exploded");
            }
            return GenerateResponse.newBuilder()
                    .setText("answer")
                    .setModel(request.getModel())
                    .setProvider(id())
                    .setFinishReason(FinishReason.FINISH_REASON_STOP)
                    .setUsage(Usage.newBuilder().setPromptTokens(7).setCompletionTokens(1).build())
                    .build();
        }

        @Override
        public void generateStream(ModelEntry model, GenerateStreamRequest request,
                                   ChunkObserver observer) {
        }
    }

    private final InferenceEngines engines = engines();

    private static InferenceEngines engines() {
        InferenceCatalog catalog = new InferenceCatalog();
        InferenceEngines engines = new InferenceEngines(catalog, List.of(new StubProvider()));
        engines.register(ModelEntry.newBuilder()
                .setId("judge").setProvider("stub").setEndpoint("stub://local")
                .setCapabilities(ModelCapabilities.newBuilder()
                        .setMaxContextTokens(262144).setStreaming(true).setThinking(true)
                        .setStructuredOutput(true).build())
                .putLabels("machine", "krick-1")
                .build());
        return engines;
    }

    private static ObjectNode input(Map<String, Object> fields) {
        return MAPPER.valueToTree(fields);
    }

    @Test
    void generateHappyPathCarriesProvenance() throws ActionException {
        ObjectNode out = dispatch(new GenerateAction(engines), input(Map.of(
                "model", "judge",
                "messages", List.of(Map.of("role", "user", "content", "verdict?")),
                "temperature", 0.2)));
        assertThat(out.get("ok").asBoolean()).isTrue();
        assertThat(out.get("text").asText()).isEqualTo("answer");
        assertThat(out.get("provider").asText()).isEqualTo("stub");
        assertThat(out.get("finishReason").asText()).isEqualTo("stop");
        assertThat(out.at("/usage/promptTokens").asLong()).isEqualTo(7);
    }

    @Test
    void generateUnknownModelAnswersOkFalse() throws ActionException {
        ObjectNode out = dispatch(new GenerateAction(engines), input(Map.of(
                "model", "ghost",
                "messages", List.of(Map.of("role", "user", "content", "hi")))));
        assertThat(out.get("ok").asBoolean()).isFalse();
        assertThat(out.get("error").asText()).contains("ghost");
    }

    @Test
    void generateProviderFailureAnswersOkFalse() throws ActionException {
        ObjectNode out = dispatch(new GenerateAction(engines), input(Map.of(
                "model", "judge",
                "messages", List.of(Map.of("role", "user", "content", "boom")))));
        assertThat(out.get("ok").asBoolean()).isFalse();
        assertThat(out.get("error").asText()).contains("backend exploded");
    }

    @Test
    void generateRejectsBadInput() {
        assertThatThrownBy(() -> dispatch(new GenerateAction(engines), input(Map.of(
                "model", "judge",
                "messages", List.of(Map.of("role", "wizard", "content", "hi"))))))
                .isInstanceOfSatisfying(ActionException.class, e ->
                        assertThat(e.getMessage()).contains("wizard"));
        assertThatThrownBy(() -> dispatch(new GenerateAction(engines), input(Map.of(
                "model", "judge", "messages", List.of()))))
                .isInstanceOf(ActionException.class);
    }

    @Test
    void generateRejectsRuleViolations() {
        // A temperature above the contract's declared maximum fails validation, not the
        // model. Dispatching through a catalog because that is where the envelope is checked
        // against the verb's request message; calling execute directly is below that line.
        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create())
                .replace(new GenerateAction(engines));
        assertThatThrownBy(() -> catalog.execute("inference-generate", input(Map.of(
                "model", "judge",
                "temperature", 3.0,
                "messages", List.of(Map.of("role", "user", "content", "hi"))))))
                .isInstanceOfSatisfying(ActionException.class, e ->
                        assertThat(e.getMessage()).contains("temperature must be within"));
    }

    @Test
    void unavailableWithoutEngines() {
        assertThatThrownBy(() -> dispatch(new GenerateAction(null), input(Map.of(
                "model", "judge",
                "messages", List.of(Map.of("role", "user", "content", "hi"))))))
                .isInstanceOfSatisfying(ActionException.class, e ->
                        assertThat(e.getMessage()).contains("not configured"));
        assertThatThrownBy(() -> dispatch(new ListModelsAction(null), input(Map.of())))
                .isInstanceOf(ActionException.class);
        assertThatThrownBy(() -> dispatch(new DescribeModelAction(null), input(Map.of("model", "judge"))))
                .isInstanceOf(ActionException.class);
    }

    @Test
    void listModelsRendersEntriesAndGeneration() throws ActionException {
        ObjectNode out = dispatch(new ListModelsAction(engines), input(Map.of()));
        assertThat(out.get("ok").asBoolean()).isTrue();
        JsonNode entry = out.get("models").get(0);
        assertThat(entry.get("id").asText()).isEqualTo("judge");
        assertThat(entry.at("/capabilities/maxContextTokens").asLong()).isEqualTo(262144);
        assertThat(entry.at("/capabilities/structuredOutput").asBoolean()).isTrue();
        assertThat(entry.at("/labels/machine").asText()).isEqualTo("krick-1");
        assertThat(out.get("catalogGeneration").asLong()).isEqualTo(1);
    }

    @Test
    void describeModelRoundTripsAndFailsFalseOnUnknown() throws ActionException {
        ObjectNode found = dispatch(new DescribeModelAction(engines), input(Map.of("model", "judge")));
        assertThat(found.get("ok").asBoolean()).isTrue();
        assertThat(found.at("/entry/provider").asText()).isEqualTo("stub");
        ObjectNode missing = dispatch(new DescribeModelAction(engines), input(Map.of("model", "ghost")));
        assertThat(missing.get("ok").asBoolean()).isFalse();
        assertThat(missing.get("error").asText()).contains("ghost");
    }

    /**
     * Dispatches the way every surface does: through a catalog holding the verb, which is
     * where the request contract is checked before the verb runs.
     */
    private static ObjectNode dispatch(ProtoAction verb, ObjectNode input)
            throws ActionException {
        return ActionCatalog.defaults(ActionContext.create())
                .replace(verb).execute(verb.name(), input);
    }

}
