package ai.pipestream.proto.chain;

import ai.pipestream.proto.grpc.profile.ServiceProfileValidation;
import ai.pipestream.proto.grpc.recipe.RecipeValidation;
import ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe;
import ai.pipestream.proto.grpc.recipe.v1.RecipeOutput;
import ai.pipestream.proto.grpc.recipe.v1.RecipeStep;
import ai.pipestream.proto.grpc.recipe.v1.ServiceDependency;
import ai.pipestream.proto.grpc.recipe.v1.StepCompletion;
import ai.pipestream.proto.grpc.recipe.v1.StructuredGenerationSpec;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Duration;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Compiles a resolved {@link ChainDefinition} into the durable {@link GrpcRecipe} contract,
 * losslessly: every mapping rule, gate, deadline, and validation flag carries over, and the
 * result always passes {@link RecipeValidation} before it is returned.
 *
 * <p>Chains name concrete targets; recipes name service profiles. The compiler bridges that
 * gap by deriving one dependency per distinct service full name: the alias and profile name
 * are the service's fully qualified name (stable and path-safe), the endpoint is the step's
 * target sanitized into a path-safe placeholder, and the descriptor fingerprint is the
 * SHA-256 of the chain's files serialized in name order. Binding those placeholders to real
 * profiles and endpoints is the promoter's act, not the compiler's.</p>
 *
 * <p>Compilation is deterministic: identical chains compile to identical bytes, and
 * reordering the chain's file list does not change dependency fingerprints.</p>
 */
public final class ChainRecipeCompiler {

    private ChainRecipeCompiler() {
    }

    /**
     * Compiles {@code chain} into a validated recipe.
     *
     * @throws IllegalArgumentException when any chain identity (name, step name) is not
     *         path-safe or the result otherwise violates the recipe contract
     */
    public static GrpcRecipe compile(ChainDefinition chain) {
        Objects.requireNonNull(chain, "chain");
        String fingerprint = descriptorFingerprint(chain.files());

        // One dependency per distinct service, in first-use order: steps keep referencing
        // their own service even when several services share one target. Structured
        // steps share one deterministic dependency anchor whose fingerprint is the same
        // chain file set - the target type's file is part of it, so compile and replay
        // fingerprint over the very descriptors the coordinator filled.
        Map<String, ServiceDependency> dependencies = new LinkedHashMap<>();
        List<RecipeStep> steps = new ArrayList<>(chain.steps().size());
        for (ChainDefinition.Step step : chain.steps()) {
            if (step.structured() != null) {
                dependencies.computeIfAbsent(ChainDefinition.Step.STRUCTURED_DEPENDENCY,
                        name -> ServiceDependency.newBuilder()
                                .setAlias(name)
                                .setServiceProfile(name)
                                .setEndpoint("local")
                                .setDescriptorFingerprint(fingerprint)
                                .build());
                steps.add(RecipeStep.newBuilder()
                        .setName(step.name())
                        .setDependency(ChainDefinition.Step.STRUCTURED_DEPENDENCY)
                        .setStructured(StructuredGenerationSpec.newBuilder()
                                .setTargetType(step.structured().targetType().getFullName())
                                .setModel(step.structured().model())
                                .setMaxAttempts(step.structured().maxAttempts())
                                .build())
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build());
                continue;
            }
            String service = step.method().getService().getFullName();
            dependencies.computeIfAbsent(service, name -> ServiceDependency.newBuilder()
                    .setAlias(name)
                    .setServiceProfile(name)
                    .setEndpoint(endpointName(step.target()))
                    .setDescriptorFingerprint(fingerprint)
                    .build());
            steps.add(compileStep(step, service));
        }

        GrpcRecipe.Builder recipe = GrpcRecipe.newBuilder()
                .setName(chain.name())
                .setInputType(chain.inputType().getFullName())
                .setDeadline(millis(chain.deadlineMs()))
                .addAllDependencies(dependencies.values())
                .addAllSteps(steps);
        if (chain.output() != null) {
            recipe.setOutput(RecipeOutput.newBuilder()
                    .setType(chain.output().type().getFullName())
                    .addAllRules(chain.output().rules())
                    .addAllCelRules(chain.output().celRules().stream()
                            .map(ChainRecipeCompiler::compileRule).toList())
                    .build());
        }
        GrpcRecipe built = recipe.build();
        // The contract speaks last: any identity the chain carried that a repository or MCP
        // resource could not hold fails here, at compile time, with the field named.
        RecipeValidation.validate(built);
        return built;
    }

    private static RecipeStep compileStep(ChainDefinition.Step step, String service) {
        RecipeStep.Builder builder = RecipeStep.newBuilder()
                .setName(step.name())
                .setDependency(service)
                .setMethod(service + "/" + step.method().getName())
                .addAllRules(step.rules())
                .addAllCelRules(step.celRules().stream()
                        .map(ChainRecipeCompiler::compileRule).toList())
                .setValidateResponse(step.validate())
                .setCompletion(step.external()
                        ? StepCompletion.STEP_COMPLETION_EXTERNAL
                        : StepCompletion.STEP_COMPLETION_LIVE);
        if (step.when() != null && !step.when().isBlank()) {
            builder.setWhen(step.when());
        }
        if (step.deadlineMs() > 0) {
            builder.setDeadline(millis(step.deadlineMs()));
        }
        return builder.build();
    }

    private static ai.pipestream.proto.grpc.recipe.v1.CelMappingRule compileRule(
            ai.pipestream.proto.cel.CelMappingRule rule) {
        ai.pipestream.proto.grpc.recipe.v1.CelMappingRule.Builder builder =
                ai.pipestream.proto.grpc.recipe.v1.CelMappingRule.newBuilder()
                        .setTarget(rule.targetPath())
                        .addAllFallback(rule.textRuleFallback());
        if (rule.filterExpression() != null) {
            builder.setFilter(rule.filterExpression());
        }
        if (rule.selectorExpression() != null) {
            builder.setSelector(rule.selectorExpression());
        }
        return builder.build();
    }

    /**
     * The chain's schema as one stable fingerprint: files serialized in name order, so the
     * hash depends on content alone, never on declaration order in the caller's list.
     */
    static String descriptorFingerprint(List<FileDescriptor> files) {
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addAllFile(files.stream()
                        .map(FileDescriptor::toProto)
                        .sorted(Comparator.comparing(proto -> proto.getName()))
                        .toList())
                .build();
        return ServiceProfileValidation.sha256(set.toByteArray());
    }

    /**
     * A target as a path-safe endpoint placeholder. Chains carry addresses
     * ({@code localhost:9090}); the recipe contract carries names, so anything outside the
     * name alphabet folds to a dash. This is a stand-in the promoter rebinds, not a
     * connection detail.
     */
    static String endpointName(String target) {
        String sanitized = target.replaceAll("[^A-Za-z0-9._-]", "-");
        if (sanitized.isEmpty() || !Character.isLetterOrDigit(sanitized.charAt(0))) {
            sanitized = "e" + sanitized;
        }
        return sanitized.length() > 128 ? sanitized.substring(0, 128) : sanitized;
    }

    private static Duration millis(long deadlineMs) {
        return Duration.newBuilder()
                .setSeconds(deadlineMs / 1_000)
                .setNanos((int) (deadlineMs % 1_000) * 1_000_000)
                .build();
    }
}
