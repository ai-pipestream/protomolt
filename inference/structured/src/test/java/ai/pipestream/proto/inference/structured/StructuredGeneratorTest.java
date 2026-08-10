package ai.pipestream.proto.inference.structured;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.inference.spi.ChunkObserver;
import ai.pipestream.proto.inference.spi.InferenceCatalog;
import ai.pipestream.proto.inference.spi.InferenceEngines;
import ai.pipestream.proto.inference.spi.InferenceException;
import ai.pipestream.proto.inference.spi.InferenceProvider;
import ai.pipestream.proto.inference.structured.testdata.TestForm;
import ai.pipestream.proto.inference.v1.AttemptOutcome;
import ai.pipestream.proto.inference.v1.FinishReason;
import ai.pipestream.proto.inference.v1.GenerateRequest;
import ai.pipestream.proto.inference.v1.GenerateResponse;
import ai.pipestream.proto.inference.v1.GenerateStreamRequest;
import ai.pipestream.proto.inference.v1.GenerateStructuredRequest;
import ai.pipestream.proto.inference.v1.GenerateStructuredResponse;
import ai.pipestream.proto.inference.v1.ModelCapabilities;
import ai.pipestream.proto.inference.v1.ModelEntry;
import ai.pipestream.proto.inference.v1.StructuredAttempt;
import ai.pipestream.proto.inference.v1.Usage;
import ai.pipestream.proto.validate.ProtoValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the coordinator against an in-process scripted provider: no
 * container, no GPU, no network. The fake replays a queue of raw response
 * texts (or failures) and counts its invocations so pre-invocation rejections
 * are provable.
 */
class StructuredGeneratorTest {

    private static final String TARGET_TYPE =
            "ai.pipestream.proto.inference.structured.testdata.v1.TestForm";
    private static final String STRUCTURED_MODEL = "structured-model";
    private static final String TEXT_ONLY_MODEL = "text-only-model";
    private static final String VALID_FORM_JSON = "{\"name\": \"Ada Lovelace\", \"age\": 36}";

    private ScriptedProvider provider;
    private StructuredGenerator generator;

    @BeforeEach
    void setUp() {
        provider = new ScriptedProvider();
        InferenceCatalog catalog = new InferenceCatalog();
        InferenceEngines engines = new InferenceEngines(catalog, List.of(provider));
        engines.register(entry(STRUCTURED_MODEL, true));
        engines.register(entry(TEXT_ONLY_MODEL, false));

        DescriptorRegistry descriptors = new DescriptorRegistry();
        descriptors.register(TestForm.getDescriptor());

        generator = new StructuredGenerator(engines, descriptors);
    }

    @Test
    void invalidJsonThenValidMessageSucceedsWithFullProvenance() throws Exception {
        provider.script("this is not json", VALID_FORM_JSON);

        GenerateStructuredResponse response = generator.generate(request(STRUCTURED_MODEL).build());

        assertThat(provider.invocations()).isEqualTo(2);
        assertThat(response.getTargetType()).isEqualTo(TARGET_TYPE);
        assertThat(response.getModel()).isEqualTo(STRUCTURED_MODEL);
        assertThat(response.getProvider()).isEqualTo("scripted");
        assertThat(response.getModelVersion()).isEqualTo("scripted-v1");

        assertThat(response.getAttemptsCount()).isEqualTo(2);
        StructuredAttempt first = response.getAttempts(0);
        assertThat(first.getAttempt()).isEqualTo(1);
        assertThat(first.getOutcome()).isEqualTo(AttemptOutcome.ATTEMPT_OUTCOME_PARSE_FAILED);
        assertThat(first.getResponseText()).isEqualTo("this is not json");
        assertThat(first.getFeedback()).isNotBlank();
        StructuredAttempt second = response.getAttempts(1);
        assertThat(second.getAttempt()).isEqualTo(2);
        assertThat(second.getOutcome()).isEqualTo(AttemptOutcome.ATTEMPT_OUTCOME_SUCCEEDED);
        assertThat(second.getFeedback()).isEmpty();

        assertThat(response.getMessage().getTypeUrl())
                .isEqualTo("type.googleapis.com/" + TARGET_TYPE);
        assertThat(response.getMessage().unpack(TestForm.class))
                .isEqualTo(TestForm.newBuilder().setName("Ada Lovelace").setAge(36).build());

        // Each scripted response reports 10 prompt + 5 completion tokens.
        assertThat(response.getTotalUsage())
                .isEqualTo(Usage.newBuilder().setPromptTokens(20).setCompletionTokens(10).build());

        assertThat(response.getPromptFingerprint()).matches("[0-9a-f]{64}");
        assertThat(response.getSchemaFingerprint()).matches("[0-9a-f]{64}");
        assertThat(provider.lastRequest().hasStructuredOutput()).isTrue();
        assertThat(provider.lastRequest().getStructuredOutput().getName())
                .matches("[A-Za-z0-9_-]{1,64}")
                .doesNotContain(".");
        assertThat(provider.lastRequest().getStructuredOutput().getJsonSchema())
                .contains("\"$schema\"");
    }

    @Test
    void validationFailureRendersViolationPathAsFeedback() {
        provider.script("{\"name\": \"Ada Lovelace\", \"age\": 200}", VALID_FORM_JSON);

        GenerateStructuredResponse response = generator.generate(request(STRUCTURED_MODEL).build());

        assertThat(response.getAttemptsCount()).isEqualTo(2);
        StructuredAttempt first = response.getAttempts(0);
        assertThat(first.getOutcome()).isEqualTo(AttemptOutcome.ATTEMPT_OUTCOME_VALIDATION_FAILED);
        assertThat(first.getFeedback()).contains("\"age\"");
        assertThat(response.getAttempts(1).getOutcome())
                .isEqualTo(AttemptOutcome.ATTEMPT_OUTCOME_SUCCEEDED);
    }

    @Test
    void missingRequiredFieldFailsValidation() {
        provider.script("{\"age\": 30}", VALID_FORM_JSON);

        GenerateStructuredResponse response = generator.generate(request(STRUCTURED_MODEL).build());

        StructuredAttempt first = response.getAttempts(0);
        assertThat(first.getOutcome()).isEqualTo(AttemptOutcome.ATTEMPT_OUTCOME_VALIDATION_FAILED);
        assertThat(first.getFeedback()).contains("\"name\"");
    }

    @Test
    void unknownTargetTypeFailsBeforeInvocation() {
        GenerateStructuredRequest request = request(STRUCTURED_MODEL)
                .setTargetType("ai.pipestream.proto.inference.v1.NoSuchType")
                .build();

        assertThatThrownBy(() -> generator.generate(request))
                .isInstanceOfSatisfying(StructuredGenerationException.class, e -> {
                    assertThat(e.getAttempts()).isEmpty();
                    assertThat(e.getTargetType())
                            .isEqualTo("ai.pipestream.proto.inference.v1.NoSuchType");
                });
        assertThat(provider.invocations()).isZero();
    }

    @Test
    void modelWithoutStructuredOutputFailsBeforeInvocation() {
        GenerateStructuredRequest request = request(TEXT_ONLY_MODEL).build();

        assertThatThrownBy(() -> generator.generate(request))
                .isInstanceOfSatisfying(StructuredGenerationException.class,
                        e -> assertThat(e.getAttempts()).isEmpty());
        assertThat(provider.invocations()).isZero();
    }

    @Test
    void unknownModelFailsBeforeInvocation() {
        GenerateStructuredRequest request = request("no-such-model").build();

        assertThatThrownBy(() -> generator.generate(request))
                .isInstanceOfSatisfying(StructuredGenerationException.class,
                        e -> assertThat(e.getAttempts()).isEmpty());
        assertThat(provider.invocations()).isZero();
    }

    @Test
    void persistentGarbageExhaustsExactlyThreeAttempts() {
        provider.script("garbage one", "garbage two", "garbage three", "never reached");

        assertThatThrownBy(() -> generator.generate(request(STRUCTURED_MODEL).build()))
                .isInstanceOfSatisfying(StructuredGenerationException.class, e -> {
                    assertThat(e.getAttempts()).hasSize(3);
                    assertThat(e.getAttempts()).allSatisfy(attempt ->
                            assertThat(attempt.getOutcome())
                                    .isEqualTo(AttemptOutcome.ATTEMPT_OUTCOME_PARSE_FAILED));
                    // The final attempt carries no feedback: nothing follows it.
                    assertThat(e.getAttempts().get(2).getFeedback()).isEmpty();
                });
        assertThat(provider.invocations()).isEqualTo(3);
    }

    @Test
    void explicitMaxAttemptsTwoStopsAtTwo() {
        provider.script("garbage one", "garbage two", "never reached");

        GenerateStructuredRequest request = request(STRUCTURED_MODEL).setMaxAttempts(2).build();

        assertThatThrownBy(() -> generator.generate(request))
                .isInstanceOfSatisfying(StructuredGenerationException.class,
                        e -> assertThat(e.getAttempts()).hasSize(2));
        assertThat(provider.invocations()).isEqualTo(2);
    }

    @Test
    void maxAttemptsAboveThreeIsRejectedByRequestValidation() {
        GenerateStructuredRequest request = request(STRUCTURED_MODEL).setMaxAttempts(4).build();

        assertThatThrownBy(() -> generator.generate(request))
                .isInstanceOfSatisfying(StructuredGenerationException.class,
                        e -> assertThat(e.getAttempts()).isEmpty());
        assertThat(provider.invocations()).isZero();
    }

    @Test
    void providerFailureAbortsWithoutRetryAndKeepsAttempts() {
        provider.script("garbage one");
        provider.fail(new InferenceException("backend unreachable"));

        assertThatThrownBy(() -> generator.generate(request(STRUCTURED_MODEL).build()))
                .isInstanceOfSatisfying(StructuredGenerationException.class, e -> {
                    assertThat(e.getCause()).isInstanceOf(InferenceException.class);
                    assertThat(e.getAttempts()).hasSize(1);
                    assertThat(e.getAttempts().get(0).getOutcome())
                            .isEqualTo(AttemptOutcome.ATTEMPT_OUTCOME_PARSE_FAILED);
                });
        // The provider failure is not retried: two invocations, one per script entry.
        assertThat(provider.invocations()).isEqualTo(2);
    }

    @Test
    void successfulProvenanceUsesTheCatalogIdentityNotProviderClaims() {
        provider.spoofProvenance("other-model", "other-provider");
        provider.script(VALID_FORM_JSON);

        GenerateStructuredResponse response = generator.generate(request(STRUCTURED_MODEL).build());

        assertThat(response.getModel()).isEqualTo(STRUCTURED_MODEL);
        assertThat(response.getProvider()).isEqualTo("scripted");
    }

    @Test
    void responseContractRejectsInvalidPersistedProvenance() {
        provider.script(VALID_FORM_JSON);
        GenerateStructuredResponse response = generator.generate(request(STRUCTURED_MODEL).build());
        ProtoValidator validator = ProtoValidator.create();

        assertThat(validator.validate(response).valid()).isTrue();
        assertThat(validator.validate(response.toBuilder()
                .setPromptFingerprint("not-a-sha256").build()).valid()).isFalse();
        assertThat(validator.validate(response.toBuilder()
                .setTargetType("not a protobuf type").build()).valid()).isFalse();
        assertThat(validator.validate(response.toBuilder()
                .clearModel().build()).valid()).isFalse();
        assertThat(validator.validate(response.toBuilder()
                .clearProvider().build()).valid()).isFalse();
    }

    @Test
    void oversizedProviderOutputFailsWithoutCreatingInvalidEvidence() {
        provider.script(VALID_FORM_JSON + " ".repeat(1_048_577));

        assertThatThrownBy(() -> generator.generate(request(STRUCTURED_MODEL).build()))
                .isInstanceOfSatisfying(StructuredGenerationException.class, e -> {
                    assertThat(e.getMessage()).contains("evidence limit");
                    assertThat(e.getAttempts()).isEmpty();
                });
        assertThat(provider.invocations()).isEqualTo(1);
    }

    private static GenerateStructuredRequest.Builder request(String model) {
        return GenerateStructuredRequest.newBuilder()
                .setTargetType(TARGET_TYPE)
                .setModel(model);
    }

    private static ModelEntry entry(String id, boolean structuredOutput) {
        return ModelEntry.newBuilder()
                .setId(id)
                .setProvider("scripted")
                .setEndpoint("in-process://scripted")
                .setCapabilities(ModelCapabilities.newBuilder()
                        .setStructuredOutput(structuredOutput))
                .build();
    }

    /**
     * The scripted in-process provider: replays queued raw response texts (or
     * queued failures) and counts invocations. Not registered with
     * ServiceLoader; wired through the explicit {@link InferenceEngines}
     * constructor.
     */
    private static final class ScriptedProvider implements InferenceProvider {

        private final Queue<Object> script = new ArrayDeque<>();
        private final AtomicInteger invocations = new AtomicInteger();
        private GenerateRequest lastRequest;
        private String responseModel;
        private String responseProvider;

        void script(String... responses) {
            script.addAll(List.of(responses));
        }

        void fail(InferenceException failure) {
            script.add(failure);
        }

        void spoofProvenance(String model, String provider) {
            responseModel = model;
            responseProvider = provider;
        }

        int invocations() {
            return invocations.get();
        }

        GenerateRequest lastRequest() {
            return lastRequest;
        }

        @Override
        public String id() {
            return "scripted";
        }

        @Override
        public GenerateResponse generate(ModelEntry model, GenerateRequest request) {
            invocations.incrementAndGet();
            lastRequest = request;
            Object next = script.poll();
            if (next == null) {
                throw new InferenceException("scripted provider ran out of responses");
            }
            if (next instanceof InferenceException failure) {
                throw failure;
            }
            return GenerateResponse.newBuilder()
                    .setText((String) next)
                    .setModel(responseModel == null ? model.getId() : responseModel)
                    .setProvider(responseProvider == null ? id() : responseProvider)
                    .setModelVersion("scripted-v1")
                    .setFinishReason(FinishReason.FINISH_REASON_STOP)
                    .setUsage(Usage.newBuilder().setPromptTokens(10).setCompletionTokens(5))
                    .build();
        }

        @Override
        public void generateStream(ModelEntry model, GenerateStreamRequest request,
                ChunkObserver observer) {
            throw new InferenceException("the scripted provider does not stream");
        }
    }
}
