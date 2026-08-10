package ai.pipestream.proto.chain;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.cel.CelProtoMapper;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.grpc.recipe.RecipeValidation;
import ai.pipestream.proto.grpc.recipe.ArtifactRepository;
import ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe;
import ai.pipestream.proto.grpc.recipe.v1.RunEvidence;
import ai.pipestream.proto.grpc.recipe.v1.StepEvidence;
import ai.pipestream.proto.grpc.recipe.v1.StepStatus;
import ai.pipestream.proto.shapes.MessageScope;
import ai.pipestream.proto.shapes.ScopedProtoMapper;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Offline replay of a recorded recipe run: given the recipe, its {@link RunEvidence}, the
 * schema the run was checked against, and the artifact store holding the fixtures, verify
 * that the recording is exactly what the recipe produces — with no server and no network.
 *
 * <p>Replay re-derives every step request from the recorded scope (the input fixture plus
 * every prior step's recorded response) using the same scoped mapper the live runner uses,
 * and compares it against the recorded request. Gates are re-evaluated against the recorded
 * scope, recorded responses are re-validated when the step declares validation, and the
 * evidence's recipe and descriptor fingerprints must match what is being replayed. Any
 * alteration — request, response, gate verdict, schema, or recipe content — fails with the
 * step and the mismatch named.</p>
 *
 * <p>The verdict is data, not an exception: expected mismatches return a failed
 * {@link ReplayResult}. Only store and parse failures throw.</p>
 */
public final class RecipeReplay {

    private RecipeReplay() {
    }

    /** One step's replay verdict; {@code detail} names the mismatch when not ok. */
    public record StepReplay(String stepName, StepStatus recordedStatus, boolean ok,
                             String detail) {
    }

    /** The run's replay verdict: ok only when every check passed, else the first failure. */
    public record ReplayResult(boolean ok, String failure, List<StepReplay> steps) {

        static ReplayResult failed(String failure, List<StepReplay> steps) {
            return new ReplayResult(false, failure, List.copyOf(steps));
        }

        static ReplayResult ok(List<StepReplay> steps) {
            return new ReplayResult(true, "", List.copyOf(steps));
        }
    }

    /**
     * Replays {@code evidence} against {@code recipe}.
     *
     * @param schema    the descriptors the run was checked against; every method and type
     *                  reference in the recipe must resolve here, and its fingerprint must
     *                  match the evidence's dependency fingerprints
     * @param artifacts the store holding the recorded fixtures
     * @throws IOException when a referenced artifact is missing or unreadable
     */
    public static ReplayResult replay(GrpcRecipe recipe, RunEvidence evidence,
                                      List<FileDescriptor> schema,
                                      ArtifactRepository artifacts) throws IOException {
        Objects.requireNonNull(recipe, "recipe");
        Objects.requireNonNull(evidence, "evidence");
        Objects.requireNonNull(artifacts, "artifacts");
        // Both sides answer to the contract before any comparison happens.
        RecipeValidation.validate(recipe);
        RecipeValidation.validate(evidence);

        List<StepReplay> steps = new ArrayList<>();
        if (!evidence.getRecipeName().equals(recipe.getName())
                || !evidence.getRecipeFingerprint().equals(RecipeValidation.fingerprint(recipe))) {
            return ReplayResult.failed(
                    "evidence was recorded for different recipe content", steps);
        }
        String schemaFingerprint = ChainRecipeCompiler.descriptorFingerprint(schema);
        for (var dependency : evidence.getDependenciesList()) {
            if (!dependency.getDescriptorFingerprint().equals(schemaFingerprint)) {
                return ReplayResult.failed("dependency " + dependency.getAlias()
                        + " names a different descriptor set than the replay schema", steps);
            }
        }

        DescriptorRegistry registry = DescriptorRegistry.create();
        for (FileDescriptor file : schema) {
            registry.registerFile(file);
        }
        ScopedProtoMapper mapper = new ScopedProtoMapper(registry);

        Descriptor inputType = findMessage(schema, recipe.getInputType());
        if (inputType == null) {
            return ReplayResult.failed(
                    "input type " + recipe.getInputType() + " not in the replay schema", steps);
        }
        Map<String, Message> scope = new LinkedHashMap<>();
        scope.put("input", parse(artifacts, evidence.getInputArtifact(), inputType,
                "run input"));
        // Mirrors the live runner: a gate-skipped step binds its name but never becomes
        // the result the chain returns.
        Message last = scope.get("input");

        for (int i = 0; i < recipe.getStepsCount(); i++) {
            var step = recipe.getSteps(i);
            if (i >= evidence.getStepsCount()) {
                return ReplayResult.failed("evidence ends before step " + step.getName(), steps);
            }
            StepEvidence recorded = evidence.getSteps(i);
            if (!recorded.getStepName().equals(step.getName())) {
                return ReplayResult.failed("evidence step " + i + " is "
                        + recorded.getStepName() + " but the recipe's is " + step.getName()
                        + "; the recipe changed under the evidence", steps);
            }
            if (!recorded.getMethod().equals(step.getMethod())) {
                return fail(steps, step.getName(), recorded.getStatus(),
                        "recorded method " + recorded.getMethod()
                                + " differs from the recipe's " + step.getMethod());
            }
            MethodDescriptor method = ChainDefinition.resolveMethod(schema, step.getMethod());

            if (recorded.getStatus() == StepStatus.STEP_STATUS_SKIPPED) {
                String detail = verifySkip(step, scope, method);
                if (detail != null) {
                    return fail(steps, step.getName(), recorded.getStatus(), detail);
                }
                scope.put(step.getName(),
                        DynamicMessage.getDefaultInstance(method.getOutputType()));
                steps.add(new StepReplay(step.getName(), recorded.getStatus(), true, ""));
                continue;
            }
            if (recorded.getStatus() != StepStatus.STEP_STATUS_SUCCEEDED) {
                // A failed or cancelled step ends the run; nothing after it can be replayed.
                steps.add(new StepReplay(step.getName(), recorded.getStatus(), true, ""));
                return terminalTail(recipe, evidence, i, steps);
            }
            if (recorded.getGrpcStatusCode() != 0) {
                return fail(steps, step.getName(), recorded.getStatus(),
                        "succeeded step records gRPC status " + recorded.getGrpcStatusCode());
            }

            // The recorded request must be exactly what the mapping derives from the
            // recorded scope; a drifted fixture or a changed rule both surface here.
            Message expected;
            try {
                expected = buildMessage(mapper, scope, method.getInputType(),
                        step.getRulesList(), step.getCelRulesList());
            } catch (Exception e) {
                return fail(steps, step.getName(), recorded.getStatus(),
                        "request mapping could not be re-derived: " + e.getMessage());
            }
            if (recorded.hasRequestArtifact()) {
                Message request = parse(artifacts, recorded.getRequestArtifact(),
                        method.getInputType(), "request of step " + step.getName());
                if (!request.equals(expected)) {
                    return fail(steps, step.getName(), recorded.getStatus(),
                            "recorded request differs from what the recipe's mapping "
                                    + "derives from the recorded scope");
                }
            } else if (step.getCompletion()
                    == ai.pipestream.proto.grpc.recipe.v1.StepCompletion.STEP_COMPLETION_LIVE) {
                return fail(steps, step.getName(), recorded.getStatus(),
                        "a live step records no request artifact");
            }

            if (!recorded.hasResponseArtifact()) {
                return fail(steps, step.getName(), recorded.getStatus(),
                        "succeeded step records no response artifact");
            }
            Message response = parse(artifacts, recorded.getResponseArtifact(),
                    method.getOutputType(), "response of step " + step.getName());
            if (step.getValidateResponse()) {
                ValidationResult result = ProtoValidator
                        .forMessageType(method.getOutputType()).validate(response);
                if (!result.valid()) {
                    return fail(steps, step.getName(), recorded.getStatus(),
                            "recorded response fails validation: " + result.violations());
                }
            }
            scope.put(step.getName(), response);
            last = response;
            steps.add(new StepReplay(step.getName(), recorded.getStatus(), true, ""));
        }

        if (evidence.getStepsCount() > recipe.getStepsCount()) {
            return ReplayResult.failed("evidence records more steps than the recipe", steps);
        }
        if (evidence.hasOutputArtifact()) {
            Message expected;
            try {
                expected = recipe.hasOutput()
                        ? buildMessage(mapper, scope, outputType(schema, recipe),
                                recipe.getOutput().getRulesList(),
                                recipe.getOutput().getCelRulesList())
                        : last;
            } catch (Exception e) {
                return ReplayResult.failed(
                        "output mapping could not be re-derived: " + e.getMessage(), steps);
            }
            Descriptor outputDescriptor = recipe.hasOutput()
                    ? outputType(schema, recipe)
                    : last.getDescriptorForType();
            Message output = parse(artifacts, evidence.getOutputArtifact(),
                    outputDescriptor, "run output");
            if (!output.equals(expected)) {
                return ReplayResult.failed(
                        "recorded output differs from what the recipe derives", steps);
            }
        }
        return ReplayResult.ok(steps);
    }

    /** A skipped step must have a gate, and the gate must evaluate false on the scope. */
    private static String verifySkip(ai.pipestream.proto.grpc.recipe.v1.RecipeStep step,
                                     Map<String, Message> scope, MethodDescriptor method) {
        if (step.getWhen().isBlank()) {
            return "step is recorded skipped but declares no gate";
        }
        try {
            boolean go = evaluator(scope, method.getInputType())
                    .evaluateBooleanOrFail(step.getWhen(), Map.copyOf(scope));
            return go ? "step is recorded skipped but its gate evaluates true" : null;
        } catch (Exception e) {
            return "gate could not be re-evaluated: " + e.getMessage();
        }
    }

    /** Nothing may be recorded after a failed or cancelled step. */
    private static ReplayResult terminalTail(GrpcRecipe recipe, RunEvidence evidence,
                                             int failedIndex, List<StepReplay> steps) {
        if (evidence.getStepsCount() > failedIndex + 1) {
            return ReplayResult.failed(
                    "evidence records steps after a terminal step", steps);
        }
        return ReplayResult.ok(steps);
    }

    private static ReplayResult fail(List<StepReplay> steps, String name,
                                     StepStatus status, String detail) {
        steps.add(new StepReplay(name, status, false, detail));
        return ReplayResult.failed("step " + name + ": " + detail, steps);
    }

    /** The same scoped mapping the live runner uses, rule for rule. */
    private static DynamicMessage buildMessage(ScopedProtoMapper mapper,
                                               Map<String, Message> scope, Descriptor type,
                                               List<String> rules,
                                               List<ai.pipestream.proto.grpc.recipe.v1
                                                       .CelMappingRule> celRules)
            throws Exception {
        MessageScope.Builder messageScope = MessageScope.builder();
        scope.forEach(messageScope::add);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
        mapper.map(messageScope.build(), builder, rules);
        if (!celRules.isEmpty()) {
            new CelProtoMapper(mapper.fieldMapper(),
                    evaluator(scope, type), "target", Map.copyOf(scope))
                    .map(builder, celRules.stream()
                            .map(RecipeReplay::toRecord).toList());
        }
        return builder.build();
    }

    private static ai.pipestream.proto.cel.CelMappingRule toRecord(
            ai.pipestream.proto.grpc.recipe.v1.CelMappingRule rule) {
        return new ai.pipestream.proto.cel.CelMappingRule(
                rule.getFilter().isBlank() ? null : rule.getFilter(),
                rule.getSelector().isBlank() ? null : rule.getSelector(),
                rule.getTarget(), rule.getFallbackList());
    }

    private static CelEvaluator evaluator(Map<String, Message> scope, Descriptor targetType) {
        CelEnvironmentFactory factory = CelEnvironmentFactory.builder();
        scope.forEach((name, message) ->
                factory.addMessageVar(name, message.getDescriptorForType()));
        factory.addMessageVar("target", targetType);
        return new CelEvaluator(factory.build());
    }

    /** Loads one fixture; the store has already re-hashed it, so bytes are authentic. */
    private static DynamicMessage parse(ArtifactRepository artifacts,
                                        ai.pipestream.proto.grpc.recipe.v1.ArtifactReference
                                                reference,
                                        Descriptor type, String what) throws IOException {
        ArtifactRepository.StoredArtifact artifact = artifacts.find(reference.getSha256())
                .orElseThrow(() -> new IOException(
                        "evidence references missing artifact " + reference.getSha256()
                                + " (" + what + ")"));
        try {
            return DynamicMessage.parseFrom(type, artifact.content());
        } catch (Exception e) {
            throw new IOException("recorded " + what + " does not parse as "
                    + type.getFullName(), e);
        }
    }

    private static Descriptor outputType(List<FileDescriptor> schema, GrpcRecipe recipe) {
        Descriptor type = findMessage(schema, recipe.getOutput().getType());
        if (type == null) {
            throw new IllegalArgumentException("output type " + recipe.getOutput().getType()
                    + " not in the replay schema");
        }
        return type;
    }

    /** The message with the given fully qualified name, nested types included. */
    private static Descriptor findMessage(List<FileDescriptor> schema, String fullName) {
        for (FileDescriptor file : schema) {
            Descriptor found = findIn(file.getMessageTypes(), fullName);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static Descriptor findIn(List<Descriptor> messages, String fullName) {
        for (Descriptor message : messages) {
            if (message.getFullName().equals(fullName)) {
                return message;
            }
            Descriptor nested = findIn(message.getNestedTypes(), fullName);
            if (nested != null) {
                return nested;
            }
        }
        return null;
    }
}
