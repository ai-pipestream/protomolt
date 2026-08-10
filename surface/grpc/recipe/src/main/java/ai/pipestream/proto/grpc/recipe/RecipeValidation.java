package ai.pipestream.proto.grpc.recipe;

import ai.pipestream.proto.grpc.profile.ServiceProfileValidation;
import ai.pipestream.proto.grpc.recipe.v1.ArtifactReference;
import ai.pipestream.proto.grpc.recipe.v1.CelMappingRule;
import ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe;
import ai.pipestream.proto.grpc.recipe.v1.RecipeOutput;
import ai.pipestream.proto.grpc.recipe.v1.RecipeStep;
import ai.pipestream.proto.grpc.recipe.v1.RunEvidence;
import ai.pipestream.proto.grpc.recipe.v1.RunStatus;
import ai.pipestream.proto.grpc.recipe.v1.ServiceDependency;
import ai.pipestream.proto.grpc.recipe.v1.StepCompletion;
import ai.pipestream.proto.grpc.recipe.v1.StepEvidence;
import ai.pipestream.proto.grpc.recipe.v1.StepStatus;
import ai.pipestream.proto.grpc.recipe.v1.StructuredAttemptEvidence;
import ai.pipestream.proto.grpc.recipe.v1.StructuredGenerationEvidence;
import ai.pipestream.proto.grpc.recipe.v1.StructuredGenerationSpec;
import ai.pipestream.proto.grpc.recipe.v1.VersionedRecipe;
import ai.pipestream.proto.inference.v1.AttemptOutcome;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/** Structural and safety validation shared by recipe repositories, actions, and compilers. */
public final class RecipeValidation {

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");
    private static final Pattern FINGERPRINT = Pattern.compile("[0-9a-f]{64}");
    private static final Pattern MEDIA_TYPE = Pattern.compile(
            "[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,126}/[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,126}");
    private static final int MAX_DEPENDENCIES = 64;
    private static final int MAX_STEPS = 256;
    private static final int MAX_RULES = 1_024;
    /** Maximum catalog model id length on a structured-generation step. */
    public static final int MAX_MODEL_LENGTH = 256;
    /** Hard attempt ceiling of the structured-generation coordinator. */
    public static final int MAX_STRUCTURED_ATTEMPTS = 3;
    /** Maximum diagnostic or provenance text retained in one recipe record. */
    public static final int MAX_TEXT_LENGTH = 16_384;

    /** Maximum serialized recipe or promoted-recipe envelope size. */
    public static final int MAX_RECIPE_BYTES = 1024 * 1024;

    /** Maximum serialized run-evidence size. Artifact bytes are always stored separately. */
    public static final int MAX_RUN_EVIDENCE_BYTES = 4 * 1024 * 1024;

    /** Maximum single artifact size accepted by a conforming repository. */
    public static final int MAX_ARTIFACT_BYTES = 16 * 1024 * 1024;

    private RecipeValidation() {
    }

    /** Validates a draft or executable recipe. */
    public static void validate(GrpcRecipe recipe) {
        require(recipe != null, "recipe must not be null");
        require(recipe.getSerializedSize() <= MAX_RECIPE_BYTES,
                "recipe exceeds the maximum serialized size of " + MAX_RECIPE_BYTES + " bytes");
        validateName(recipe.getName(), "recipe.name");
        validateText(recipe.getDescription(), "recipe.description");
        validateType(recipe.getInputType(), "recipe.input_type");
        require(recipe.getDependenciesCount() > 0,
                "recipe.dependencies must not be empty");
        require(recipe.getDependenciesCount() <= MAX_DEPENDENCIES,
                "recipe.dependencies exceeds the maximum of " + MAX_DEPENDENCIES);
        require(recipe.getStepsCount() > 0, "recipe.steps must not be empty");
        require(recipe.getStepsCount() <= MAX_STEPS,
                "recipe.steps exceeds the maximum of " + MAX_STEPS);
        validatePositiveDuration(recipe.getDeadline(), "recipe.deadline");

        Set<String> dependencyAliases = new HashSet<>();
        for (ServiceDependency dependency : recipe.getDependenciesList()) {
            validateDependency(dependency);
            require(dependencyAliases.add(dependency.getAlias()),
                    "duplicate dependency alias: " + dependency.getAlias());
        }

        Set<String> stepNames = new HashSet<>();
        for (RecipeStep step : recipe.getStepsList()) {
            validateName(step.getName(), "step.name");
            require(stepNames.add(step.getName()), "duplicate step name: " + step.getName());
            validateName(step.getDependency(), "step.dependency");
            require(dependencyAliases.contains(step.getDependency()),
                    "step dependency is not declared: " + step.getDependency());
            if (step.hasStructured()) {
                validateStructuredSpec(step.getStructured());
                require(step.getMethod().isEmpty(),
                        "step.method must be empty when step.structured is set");
                require(step.getWhen().isBlank(),
                        "structured steps do not support step.when gates");
                require(step.getRulesCount() == 0 && step.getCelRulesCount() == 0,
                        "structured steps declare no mapping rules; the coordinator "
                                + "fills the target type directly");
                require(!step.getValidateResponse(),
                        "step.validate_response is meaningless on a structured step; "
                                + "the coordinator always validates its output");
                require(step.getDeadline().getSeconds() == 0
                                && step.getDeadline().getNanos() == 0,
                        "structured steps do not support step.deadline");
            } else {
                validateMethod(step.getMethod(), "step.method");
                validateText(step.getWhen(), "step.when");
                validateRules(step.getRulesCount(), step.getRulesList(), "step.rules");
                validateCelRules(step.getCelRulesList(), "step.cel_rules");
                validateDuration(step.getDeadline(), "step.deadline");
            }
            require(step.getCompletion() == StepCompletion.STEP_COMPLETION_LIVE
                            || step.getCompletion() == StepCompletion.STEP_COMPLETION_EXTERNAL,
                    "step.completion must be live or external");
        }

        if (recipe.hasOutput()) {
            validateOutput(recipe.getOutput());
        }
    }

    /** Validates an immutable promoted recipe and its content fingerprint. */
    public static void validate(VersionedRecipe versioned) {
        require(versioned != null, "versioned recipe must not be null");
        require(versioned.getSerializedSize() <= MAX_RECIPE_BYTES,
                "versioned recipe exceeds the maximum serialized size of "
                        + MAX_RECIPE_BYTES + " bytes");
        require(versioned.hasRecipe(), "versioned recipe content must be present");
        validate(versioned.getRecipe());
        validateName(versioned.getVersion(), "versioned_recipe.version");
        validateFingerprint(versioned.getRecipeFingerprint(),
                "versioned_recipe.recipe_fingerprint");
        require(fingerprint(versioned.getRecipe()).equals(versioned.getRecipeFingerprint()),
                "versioned_recipe.recipe_fingerprint does not match recipe content");
        validateTimestamp(versioned.getCreatedAt(), "versioned_recipe.created_at", true);
    }

    /** Validates bounded run evidence and every referenced artifact. */
    public static void validate(RunEvidence evidence) {
        require(evidence != null, "run evidence must not be null");
        require(evidence.getSerializedSize() <= MAX_RUN_EVIDENCE_BYTES,
                "run evidence exceeds the maximum serialized size of "
                        + MAX_RUN_EVIDENCE_BYTES + " bytes");
        validateName(evidence.getRunId(), "run.run_id");
        validateName(evidence.getRecipeName(), "run.recipe_name");
        if (!evidence.getRecipeVersion().isBlank()) {
            validateName(evidence.getRecipeVersion(), "run.recipe_version");
        }
        validateFingerprint(evidence.getRecipeFingerprint(), "run.recipe_fingerprint");
        require(evidence.getStatus() == RunStatus.RUN_STATUS_RUNNING
                        || isTerminal(evidence.getStatus()),
                "run.status must be recognized");
        validateTimestamp(evidence.getStartedAt(), "run.started_at", true);
        validateTimestamp(evidence.getCompletedAt(), "run.completed_at",
                isTerminal(evidence.getStatus()));
        validateOrder(evidence.getStartedAt(), evidence.getCompletedAt(), "run");
        require(evidence.getDependenciesCount() > 0,
                "run.dependencies must not be empty");
        require(evidence.getDependenciesCount() <= MAX_DEPENDENCIES,
                "run.dependencies exceeds the maximum of " + MAX_DEPENDENCIES);
        Set<String> aliases = new HashSet<>();
        for (ServiceDependency dependency : evidence.getDependenciesList()) {
            validateDependency(dependency);
            require(aliases.add(dependency.getAlias()),
                    "duplicate run dependency alias: " + dependency.getAlias());
        }
        require(evidence.hasInputArtifact(), "run.input_artifact must be present");
        validate(evidence.getInputArtifact());
        if (evidence.hasOutputArtifact()) {
            validate(evidence.getOutputArtifact());
        }
        require(evidence.getStepsCount() <= MAX_STEPS,
                "run.steps exceeds the maximum of " + MAX_STEPS);
        Set<String> stepNames = new HashSet<>();
        for (StepEvidence step : evidence.getStepsList()) {
            validate(step);
            require(stepNames.add(step.getStepName()),
                    "duplicate run step name: " + step.getStepName());
        }
        validateText(evidence.getFailureSummary(), "run.failure_summary");
    }

    /** Validates a content-addressed artifact reference. */
    public static void validate(ArtifactReference reference) {
        require(reference != null, "artifact reference must not be null");
        validateFingerprint(reference.getSha256(), "artifact.sha256");
        require(MEDIA_TYPE.matcher(reference.getMediaType()).matches(),
                "artifact.media_type must be a bounded type/subtype");
        require(Long.compareUnsigned(reference.getSizeBytes(), MAX_ARTIFACT_BYTES) <= 0,
                "artifact.size_bytes exceeds the maximum of " + MAX_ARTIFACT_BYTES);
    }

    /** Validates a path-safe recipe, version, run, dependency, or step identity. */
    public static void validateName(String value, String field) {
        require(value != null && NAME.matcher(value).matches(),
                field + " must be a single path-safe name");
    }

    /** Computes the lowercase SHA-256 identity of serialized recipe content. */
    public static String fingerprint(GrpcRecipe recipe) {
        require(recipe != null, "recipe must not be null");
        return ServiceProfileValidation.sha256(recipe.toByteArray());
    }

    private static void validateDependency(ServiceDependency dependency) {
        validateName(dependency.getAlias(), "dependency.alias");
        validateName(dependency.getServiceProfile(), "dependency.service_profile");
        validateName(dependency.getEndpoint(), "dependency.endpoint");
        validateFingerprint(dependency.getDescriptorFingerprint(),
                "dependency.descriptor_fingerprint");
    }

    private static void validateOutput(RecipeOutput output) {
        validateType(output.getType(), "output.type");
        validateRules(output.getRulesCount(), output.getRulesList(), "output.rules");
        validateCelRules(output.getCelRulesList(), "output.cel_rules");
    }

    private static void validate(StepEvidence step) {
        validateName(step.getStepName(), "step_evidence.step_name");
        require(step.getStatus() == StepStatus.STEP_STATUS_SUCCEEDED
                        || step.getStatus() == StepStatus.STEP_STATUS_FAILED
                        || step.getStatus() == StepStatus.STEP_STATUS_SKIPPED
                        || step.getStatus() == StepStatus.STEP_STATUS_CANCELLED,
                "step_evidence.status must be terminal");
        if (step.hasStructured()) {
            require(step.getMethod().isEmpty(),
                    "step_evidence.method must be empty when structured evidence is present");
            validateStructuredEvidence(step.getStructured(), step.getStatus());
        } else {
            validateMethod(step.getMethod(), "step_evidence.method");
        }
        validateTimestamp(step.getStartedAt(), "step_evidence.started_at", true);
        validateTimestamp(step.getCompletedAt(), "step_evidence.completed_at", true);
        validateOrder(step.getStartedAt(), step.getCompletedAt(), "step_evidence");
        if (step.hasRequestArtifact()) {
            validate(step.getRequestArtifact());
        }
        if (step.hasResponseArtifact()) {
            validate(step.getResponseArtifact());
        }
        require(step.getGrpcStatusCode() >= 0 && step.getGrpcStatusCode() <= 16,
                "step_evidence.grpc_status_code must be between 0 and 16");
        validateText(step.getSummary(), "step_evidence.summary");
    }

    private static void validateStructuredSpec(StructuredGenerationSpec spec) {
        validateType(spec.getTargetType(), "step.structured.target_type");
        require(!spec.getModel().isBlank()
                        && spec.getModel().length() <= MAX_MODEL_LENGTH,
                "step.structured.model must be non-blank and at most "
                        + MAX_MODEL_LENGTH + " characters");
        require(spec.getMaxAttempts() >= 0
                        && spec.getMaxAttempts() <= MAX_STRUCTURED_ATTEMPTS,
                "step.structured.max_attempts must be between 0 and "
                        + MAX_STRUCTURED_ATTEMPTS);
    }

    private static void validateStructuredEvidence(StructuredGenerationEvidence evidence,
                                                   StepStatus status) {
        validateType(evidence.getTargetType(), "structured_evidence.target_type");
        require(!evidence.getModel().isBlank()
                        && evidence.getModel().length() <= MAX_MODEL_LENGTH,
                "structured_evidence.model must be non-blank and at most "
                        + MAX_MODEL_LENGTH + " characters");
        validateFingerprint(evidence.getPromptFingerprint(),
                "structured_evidence.prompt_fingerprint");
        validateFingerprint(evidence.getSchemaFingerprint(),
                "structured_evidence.schema_fingerprint");
        // Empty attempt history is allowed only on a step that failed before the
        // first model invocation (no generator configured, preflight rejection).
        require(evidence.getAttemptsCount() <= MAX_STRUCTURED_ATTEMPTS,
                "structured_evidence.attempts exceeds the maximum of "
                        + MAX_STRUCTURED_ATTEMPTS);
        long promptTokens = 0;
        long completionTokens = 0;
        for (int i = 0; i < evidence.getAttemptsCount(); i++) {
            StructuredAttemptEvidence attempt = evidence.getAttempts(i);
            require(attempt.getAttempt() == i + 1,
                    "structured_evidence.attempts must be sequentially numbered "
                            + "from 1; entry " + i + " claims attempt "
                            + attempt.getAttempt());
            require(attempt.getOutcome() != AttemptOutcome.ATTEMPT_OUTCOME_UNSPECIFIED,
                    "structured_evidence.attempts outcomes must be defined");
            promptTokens += attempt.getUsage().getPromptTokens();
            completionTokens += attempt.getUsage().getCompletionTokens();
        }
        require(evidence.getTotalUsage().getPromptTokens() == promptTokens
                        && evidence.getTotalUsage().getCompletionTokens() == completionTokens,
                "structured_evidence.total_usage must equal the sum of per-attempt usage");
        if (status == StepStatus.STEP_STATUS_SUCCEEDED) {
            require(evidence.getAttemptsCount() > 0,
                    "a succeeded structured step records at least one attempt");
            require(evidence.getAttempts(evidence.getAttemptsCount() - 1).getOutcome()
                            == AttemptOutcome.ATTEMPT_OUTCOME_SUCCEEDED,
                    "a succeeded structured step's last attempt must be SUCCEEDED");
            require(evidence.getValidationPassed(),
                    "a succeeded structured step records validation_passed true");
        }
    }

    private static void validateCelRules(Iterable<CelMappingRule> rules, String field) {
        int count = 0;
        for (CelMappingRule rule : rules) {
            count++;
            require(count <= MAX_RULES, field + " exceeds the maximum of " + MAX_RULES);
            validateText(rule.getFilter(), field + ".filter");
            validateText(rule.getSelector(), field + ".selector");
            require(!rule.getTarget().isBlank(), field + ".target must not be blank");
            validateText(rule.getTarget(), field + ".target");
            validateRules(rule.getFallbackCount(), rule.getFallbackList(), field + ".fallback");
        }
    }

    private static void validateRules(int count, Iterable<String> rules, String field) {
        require(count <= MAX_RULES, field + " exceeds the maximum of " + MAX_RULES);
        for (String rule : rules) {
            require(rule != null && !rule.isBlank(), field + " entries must not be blank");
            validateText(rule, field);
        }
    }

    private static void validateType(String value, String field) {
        require(value != null && !value.isBlank()
                        && value.codePoints().noneMatch(Character::isWhitespace),
                field + " must be a fully-qualified protobuf message name");
    }

    private static void validateMethod(String value, String field) {
        require(value != null && !value.isBlank() && value.indexOf('/') > 0
                        && value.indexOf('/') == value.lastIndexOf('/')
                        && value.indexOf('/') < value.length() - 1
                        && value.codePoints().noneMatch(Character::isWhitespace),
                field + " must use Service/Method form");
    }

    private static void validateFingerprint(String value, String field) {
        require(value != null && FINGERPRINT.matcher(value).matches(),
                field + " must be a lowercase SHA-256 fingerprint");
    }

    private static void validateText(String value, String field) {
        require(value != null && value.length() <= MAX_TEXT_LENGTH,
                field + " exceeds the maximum length of " + MAX_TEXT_LENGTH);
    }

    private static void validatePositiveDuration(Duration value, String field) {
        validateDuration(value, field);
        require(value.getSeconds() > 0 || value.getNanos() > 0, field + " must be positive");
    }

    private static void validateDuration(Duration value, String field) {
        require(value.getSeconds() >= 0 && value.getNanos() >= 0
                        && value.getNanos() < 1_000_000_000,
                field + " must be a non-negative valid duration");
    }

    private static void validateTimestamp(Timestamp value, String field, boolean required) {
        boolean present = value.getSeconds() != 0 || value.getNanos() != 0;
        require(!required || present, field + " must be present");
        if (present) {
            require(value.getNanos() >= 0 && value.getNanos() < 1_000_000_000,
                    field + " must be a valid timestamp");
        }
    }

    private static void validateOrder(Timestamp start, Timestamp end, String field) {
        boolean hasEnd = end.getSeconds() != 0 || end.getNanos() != 0;
        require(!hasEnd || compare(start, end) <= 0,
                field + ".completed_at must not precede started_at");
    }

    private static int compare(Timestamp left, Timestamp right) {
        int seconds = Long.compare(left.getSeconds(), right.getSeconds());
        return seconds != 0 ? seconds : Integer.compare(left.getNanos(), right.getNanos());
    }

    private static boolean isTerminal(RunStatus status) {
        return status == RunStatus.RUN_STATUS_SUCCEEDED
                || status == RunStatus.RUN_STATUS_FAILED
                || status == RunStatus.RUN_STATUS_CANCELLED;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
