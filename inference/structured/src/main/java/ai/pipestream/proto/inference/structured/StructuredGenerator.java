package ai.pipestream.proto.inference.structured;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.inference.spi.InferenceEngines;
import ai.pipestream.proto.inference.spi.InferenceException;
import ai.pipestream.proto.inference.v1.AttemptOutcome;
import ai.pipestream.proto.inference.v1.ChatTurn;
import ai.pipestream.proto.inference.v1.DescribeModelRequest;
import ai.pipestream.proto.inference.v1.GenerateRequest;
import ai.pipestream.proto.inference.v1.GenerateResponse;
import ai.pipestream.proto.inference.v1.GenerateStructuredRequest;
import ai.pipestream.proto.inference.v1.GenerateStructuredResponse;
import ai.pipestream.proto.inference.v1.ModelEntry;
import ai.pipestream.proto.inference.v1.Role;
import ai.pipestream.proto.inference.v1.StructuredAttempt;
import ai.pipestream.proto.inference.v1.StructuredOutputConstraint;
import ai.pipestream.proto.inference.v1.Usage;
import ai.pipestream.proto.prompt.PromptPacket;
import ai.pipestream.proto.prompt.PromptRenderer;
import ai.pipestream.proto.prompt.RenderPromptRequest;
import ai.pipestream.proto.prompt.ViolationFeedbackRenderer;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The structured-generation coordinator: fills one protobuf message type with
 * a catalog model and returns the validated form with its complete attempt
 * provenance.
 *
 * <p>The flow is fail-fast, in order: the request is validated against its
 * declared rules; the target type must resolve in the descriptor registry; the
 * model must exist in the catalog and declare the structured-output capability
 * — all before any model invocation. Each attempt renders the prompt packet's
 * instructions as the system turn, calls {@link InferenceEngines#generate},
 * parses the response as strict protobuf JSON (unknown fields rejected), and
 * validates the parsed message against the target type's rules. A failure is
 * rendered back to the model as the only retry signal; a provider
 * {@link InferenceException} aborts immediately and is never retried. The
 * attempt budget is the request's {@code max_attempts} (default 3), hard-capped
 * at 3.</p>
 *
 * <p>Instances are thread-safe and stateless beyond their collaborators.</p>
 */
public final class StructuredGenerator {

    /** The attempt ceiling: both the default and the hard cap. */
    private static final int MAX_ATTEMPTS = 3;
    /** Mirrors StructuredAttempt.response_text's validated maximum. */
    private static final int MAX_ATTEMPT_TEXT_LENGTH = 1_048_576;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final InferenceEngines engines;
    private final DescriptorRegistry descriptors;
    private final PromptRenderer promptRenderer;
    private final ProtoValidator validator;

    /**
     * Creates the coordinator with a default prompt renderer.
     *
     * @param engines the inference facade that resolves and executes catalog models
     * @param descriptors the registry target types resolve against
     */
    public StructuredGenerator(InferenceEngines engines, DescriptorRegistry descriptors) {
        this(engines, descriptors, PromptRenderer.create());
    }

    /**
     * Creates the coordinator with an explicit prompt renderer.
     *
     * @param engines the inference facade that resolves and executes catalog models
     * @param descriptors the registry target types resolve against
     * @param promptRenderer renders the target descriptor into the prompt packet
     */
    public StructuredGenerator(InferenceEngines engines, DescriptorRegistry descriptors,
            PromptRenderer promptRenderer) {
        this.engines = Objects.requireNonNull(engines, "engines");
        this.descriptors = Objects.requireNonNull(descriptors, "descriptors");
        this.promptRenderer = Objects.requireNonNull(promptRenderer, "promptRenderer");
        // forMessageType adds nothing here: message-level CEL typing happens per
        // message type inside the validator, so one shared instance covers the
        // request and every parsed form.
        this.validator = ProtoValidator.create();
    }

    /**
     * Fills the request's target type with the request's model.
     *
     * @param request the structured-generation request
     * @return the validated, packed form with attempt history and provenance
     * @throws StructuredGenerationException on an invalid request, an unknown
     *     target type, an unknown or incapable model, a provider failure, or
     *     exhaustion of the attempt budget
     */
    public GenerateStructuredResponse generate(GenerateStructuredRequest request) {
        validateRequest(request);
        String model = request.getModel();
        String targetType = request.getTargetType();
        Descriptor descriptor = descriptors.findDescriptorByFullName(targetType);
        if (descriptor == null) {
            throw new StructuredGenerationException(
                    "unknown target type '" + targetType
                            + "': not registered in the descriptor registry",
                    model, targetType, List.of());
        }
        return generateResolved(request, descriptor);
    }

    /**
     * Fills an explicitly resolved target descriptor. Chain and recipe callers use
     * this overload because inline schemas are deliberately scoped to one action and
     * are not installed into the host's shared descriptor registry.
     *
     * @param request the validated structured-generation request
     * @param descriptor the exact target descriptor resolved with the chain
     * @return the validated, packed form with attempt history and provenance
     * @throws StructuredGenerationException when the request target and descriptor
     *     disagree, or generation otherwise fails
     */
    public GenerateStructuredResponse generate(GenerateStructuredRequest request,
                                                Descriptor descriptor) {
        validateRequest(request);
        Objects.requireNonNull(descriptor, "descriptor");
        if (!request.getTargetType().equals(descriptor.getFullName())) {
            throw new StructuredGenerationException(
                    "resolved target descriptor '" + descriptor.getFullName()
                            + "' does not match request target '"
                            + request.getTargetType() + "'",
                    request.getModel(), request.getTargetType(), List.of());
        }
        return generateResolved(request, descriptor);
    }

    private void validateRequest(GenerateStructuredRequest request) {
        Objects.requireNonNull(request, "request");
        ValidationResult requestResult = validator.validate(request);
        if (!requestResult.valid()) {
            throw new StructuredGenerationException(
                    "invalid structured-generation request: " + formatViolations(requestResult),
                    request.getModel(), request.getTargetType(), List.of());
        }
    }

    private GenerateStructuredResponse generateResolved(GenerateStructuredRequest request,
                                                         Descriptor descriptor) {
        String model = request.getModel();
        String targetType = request.getTargetType();

        ModelEntry entry = describeModel(request);
        if (!entry.getCapabilities().getStructuredOutput()) {
            throw new StructuredGenerationException(
                    "model '" + model + "' does not declare the structured-output capability",
                    model, targetType, List.of());
        }

        PromptPacket packet = renderPacket(descriptor, request);
        StructuredOutputConstraint constraint = StructuredOutputConstraint.newBuilder()
                .setName(schemaName(targetType))
                .setJsonSchema(packet.getResponseJsonSchema())
                .build();

        int maxAttempts = request.getMaxAttempts() == 0
                ? MAX_ATTEMPTS
                : Math.min(request.getMaxAttempts(), MAX_ATTEMPTS);

        List<ChatTurn> conversation = new ArrayList<>();
        conversation.add(turn(Role.ROLE_SYSTEM, packet.getInstructions()));
        conversation.add(turn(Role.ROLE_USER, "Fill the " + targetType
                + " form. Respond with only the JSON document, with no commentary."));

        List<StructuredAttempt> attempts = new ArrayList<>();
        Usage.Builder totalUsage = Usage.newBuilder();

        for (int attemptNumber = 1; attemptNumber <= maxAttempts; attemptNumber++) {
            GenerateResponse response = invoke(request, constraint, conversation, attempts,
                    attemptNumber);
            String text = response.getText();
            if (text.codePointCount(0, text.length()) > MAX_ATTEMPT_TEXT_LENGTH) {
                throw new StructuredGenerationException(
                        "provider output on attempt " + attemptNumber
                                + " exceeds the structured evidence limit of "
                                + MAX_ATTEMPT_TEXT_LENGTH + " characters",
                        model, targetType, attempts);
            }
            accumulate(totalUsage, response.getUsage());
            boolean lastAttempt = attemptNumber == maxAttempts;

            ParseResult parsed = parse(descriptor, text);
            if (parsed.form() == null) {
                String feedback = lastAttempt ? "" : parseFeedback(parsed.error());
                attempts.add(attempt(attemptNumber, AttemptOutcome.ATTEMPT_OUTCOME_PARSE_FAILED,
                        text, feedback, response));
                if (!lastAttempt) {
                    conversation.add(turn(Role.ROLE_ASSISTANT, text));
                    conversation.add(turn(Role.ROLE_USER, feedback));
                }
                continue;
            }
            DynamicMessage form = parsed.form();

            ValidationResult result = validator.validate(form);
            if (!result.valid()) {
                String feedback = lastAttempt ? "" : ViolationFeedbackRenderer.render(result);
                attempts.add(attempt(attemptNumber, AttemptOutcome.ATTEMPT_OUTCOME_VALIDATION_FAILED,
                        text, feedback, response));
                if (!lastAttempt) {
                    conversation.add(turn(Role.ROLE_ASSISTANT, text));
                    conversation.add(turn(Role.ROLE_USER, feedback));
                }
                continue;
            }

            attempts.add(attempt(attemptNumber, AttemptOutcome.ATTEMPT_OUTCOME_SUCCEEDED,
                    text, "", response));
            return GenerateStructuredResponse.newBuilder()
                    .setMessage(Any.pack(form))
                    .setTargetType(targetType)
                    .setModel(model)
                    .setProvider(entry.getProvider())
                    .setModelVersion(response.getModelVersion())
                    .addAllAttempts(attempts)
                    .setTotalUsage(totalUsage)
                    .setPromptFingerprint(sha256Hex(packet.getInstructions()))
                    .setSchemaFingerprint(sha256Hex(packet.getResponseJsonSchema()))
                    .build();
        }

        StructuredAttempt last = attempts.get(attempts.size() - 1);
        throw new StructuredGenerationException(
                "structured generation of '" + targetType + "' with model '" + model
                        + "' exhausted " + maxAttempts + " attempt(s); last outcome: "
                        + last.getOutcome(),
                model, targetType, attempts);
    }

    private ModelEntry describeModel(GenerateStructuredRequest request) {
        try {
            return engines.describe(DescribeModelRequest.newBuilder()
                    .setModel(request.getModel())
                    .build()).getEntry();
        } catch (InferenceException e) {
            throw new StructuredGenerationException(
                    "cannot preflight model '" + request.getModel() + "': " + e.getMessage(),
                    e, request.getModel(), request.getTargetType(), List.of());
        }
    }

    private PromptPacket renderPacket(Descriptor descriptor, GenerateStructuredRequest request) {
        RenderPromptRequest.Builder renderRequest = RenderPromptRequest.newBuilder()
                .setTargetType(request.getTargetType());
        if (request.hasPersona()) {
            renderRequest.setPersona(request.getPersona());
        }
        return promptRenderer.render(descriptor, renderRequest.build(),
                descriptor.getFile().getFullName());
    }

    private GenerateResponse invoke(GenerateStructuredRequest request,
            StructuredOutputConstraint constraint, List<ChatTurn> conversation,
            List<StructuredAttempt> attempts, int attemptNumber) {
        GenerateRequest generateRequest = GenerateRequest.newBuilder()
                .setModel(request.getModel())
                .addAllMessages(conversation)
                .setTemperature(request.getTemperature())
                .setTopP(request.getTopP())
                .setMaxOutputTokens(request.getMaxOutputTokens())
                .setStructuredOutput(constraint)
                .build();
        try {
            return engines.generate(generateRequest);
        } catch (InferenceException e) {
            throw new StructuredGenerationException(
                    "provider failed on attempt " + attemptNumber + " of model '"
                            + request.getModel() + "': " + e.getMessage(),
                    e, request.getModel(), request.getTargetType(), attempts);
        }
    }

    /** The outcome of a strict parse: the form, or the parser's error message. */
    private record ParseResult(DynamicMessage form, String error) {
    }

    /**
     * Parses the response as strict protobuf JSON for the target type; the
     * default parser rejects unknown fields.
     */
    private static ParseResult parse(Descriptor descriptor, String text) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        try {
            JsonFormat.parser().merge(text, builder);
        } catch (InvalidProtocolBufferException e) {
            return new ParseResult(null, e.getMessage());
        }
        return new ParseResult(builder.build(), null);
    }

    /** The parse rejection rendered back to the model before the next attempt. */
    private static String parseFeedback(String parseError) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("valid", false);
        root.put("instruction", "The response was not valid JSON for the target type. "
                + "Resubmit the complete form as a single JSON document, with no commentary.");
        root.put("parse_error", parseError == null ? "" : parseError);
        return root.toString();
    }

    private static StructuredAttempt attempt(int attemptNumber, AttemptOutcome outcome,
            String responseText, String feedback, GenerateResponse response) {
        return StructuredAttempt.newBuilder()
                .setAttempt(attemptNumber)
                .setOutcome(outcome)
                .setResponseText(responseText)
                .setFeedback(feedback)
                .setUsage(response.getUsage())
                .setFinishReason(response.getFinishReason())
                .build();
    }

    private static ChatTurn turn(Role role, String content) {
        return ChatTurn.newBuilder().setRole(role).setContent(content).build();
    }

    private static void accumulate(Usage.Builder total, Usage usage) {
        total.setPromptTokens(total.getPromptTokens() + usage.getPromptTokens());
        total.setCompletionTokens(total.getCompletionTokens() + usage.getCompletionTokens());
    }

    private static String formatViolations(ValidationResult result) {
        StringBuilder sb = new StringBuilder();
        for (ValidationResult.Violation violation : result.violations()) {
            sb.append(" [").append(violation.path()).append("] ")
                    .append(violation.ruleId()).append(": ").append(violation.message());
        }
        return sb.toString();
    }

    /** Produces an OpenAI-compatible schema name while retaining deterministic identity. */
    private static String schemaName(String targetType) {
        String normalized = targetType.replaceAll("[^A-Za-z0-9_-]", "_");
        if (normalized.length() <= 64) {
            return normalized;
        }
        return normalized.substring(0, 55) + "_" + sha256Hex(targetType).substring(0, 8);
    }

    private static String sha256Hex(String text) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(text.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16));
                hex.append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available on this JVM", e);
        }
    }
}
