package ai.protomolt.proto.grpc.workflow;

import ai.protomolt.proto.formats.Formats;
import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.BranchFailurePolicy;
import ai.protomolt.proto.grpc.workflow.v1.CelMappingRule;
import ai.protomolt.proto.grpc.workflow.v1.FanOutSpec;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowOutput;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowStep;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.grpc.workflow.v1.RunStatus;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.StepEvidence;
import ai.protomolt.proto.grpc.workflow.v1.StructuredGenerationSpec;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.grpc.workflow.v1.VersionedWorkflow;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.Duration;

import java.util.HashSet;
import java.util.Set;

import static ai.protomolt.proto.grpc.workflow.ContractChecks.require;

/**
 * Structural and safety validation shared by workflow repositories, actions, and compilers,
 * following the {@code ClusterValidation} conventions: the contract's own {@code validate.v1}
 * annotations (name families, fingerprints, media types) run first through
 * {@link ProtoValidator}, which recurses into nested messages, then the checks annotations
 * cannot express (duplicates, declared-dependency references, fingerprint agreement, step
 * shape exclusivity) run here.
 */
public final class WorkflowValidation {

    static final int MAX_NAME_LENGTH = 128;
    private static final int MAX_DEPENDENCIES = 64;
    private static final int MAX_STEPS = 256;
    private static final int MAX_RULES = 1_024;
    private static final int MAX_EDGE_SOURCES = 64;
    /** Hard item ceiling of a fan-out step. */
    public static final int MAX_FANOUT_ITEMS = 1_024;
    /** Hard in-flight branch ceiling of a fan-out step. */
    public static final int MAX_FANOUT_CONCURRENCY = 64;
    private static final int MAX_ITEMS_PATH_LENGTH = 512;
    private static final int MAX_COLLECT_INTO_LENGTH = 256;
    /** Maximum catalog model id length on a structured-generation step. */
    public static final int MAX_MODEL_LENGTH = 256;
    /** Hard attempt ceiling of the structured-generation coordinator. */
    public static final int MAX_STRUCTURED_ATTEMPTS = 3;
    /** Maximum diagnostic or provenance text retained in one workflow record. */
    public static final int MAX_TEXT_LENGTH = 16_384;

    /** Maximum serialized workflow or promoted-workflow envelope size. */
    public static final int MAX_WORKFLOW_BYTES = 1024 * 1024;

    /** Maximum serialized run-evidence size. Artifact bytes are always stored separately. */
    public static final int MAX_RUN_EVIDENCE_BYTES = 4 * 1024 * 1024;

    /** Maximum single artifact size accepted by a conforming repository. */
    public static final int MAX_ARTIFACT_BYTES = 16 * 1024 * 1024;

    private WorkflowValidation() {
    }

    /** Validates a draft or executable workflow. */
    public static void validate(Workflow workflow) {
        require(workflow != null, "workflow must not be null");
        require(workflow.getSerializedSize() <= MAX_WORKFLOW_BYTES,
                "workflow exceeds the maximum serialized size of " + MAX_WORKFLOW_BYTES + " bytes");
        ContractChecks.annotations(workflow);
        validateText(workflow.getDescription(), "workflow.description");
        validateType(workflow.getInputType(), "workflow.input_type");
        require(workflow.getDependenciesCount() > 0,
                "workflow.dependencies must not be empty");
        require(workflow.getDependenciesCount() <= MAX_DEPENDENCIES,
                "workflow.dependencies exceeds the maximum of " + MAX_DEPENDENCIES);
        require(workflow.getStepsCount() > 0, "workflow.steps must not be empty");
        require(workflow.getStepsCount() <= MAX_STEPS,
                "workflow.steps exceeds the maximum of " + MAX_STEPS);
        validatePositiveDuration(workflow.getDeadline(), "workflow.deadline");

        // The annotation pass above already validated every dependency and step field;
        // only the cross-item and cross-field invariants remain.
        Set<String> dependencyAliases = new HashSet<>();
        for (ServiceDependency dependency : workflow.getDependenciesList()) {
            require(dependencyAliases.add(dependency.getAlias()),
                    "duplicate dependency alias: " + dependency.getAlias());
        }

        Set<String> stepNames = new HashSet<>();
        for (WorkflowStep step : workflow.getStepsList()) {
            require(stepNames.add(step.getName()), "duplicate step name: " + step.getName());
            require(dependencyAliases.contains(step.getDependency()),
                    "step dependency is not declared: " + step.getDependency());
            if (step.hasStructured()) {
                validate(step.getStructured());
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
            if (step.hasEdge()) {
                validate(step.getEdge());
                require(step.getRulesCount() == 0 && step.getCelRulesCount() == 0,
                        "step.rules and step.cel_rules must be empty when step.edge is set; "
                                + "the edge owns request mapping");
            }
            if (step.hasFanOut()) {
                require(step.hasEdge(), "step.fan_out requires step.edge; the items "
                        + "resolve against the edge's produced message");
                validate(step.getFanOut());
            }
        }

        if (workflow.hasOutput()) {
            validateOutput(workflow.getOutput());
        }
    }

    /** Validates an immutable promoted workflow and its content fingerprint. */
    public static void validate(VersionedWorkflow versioned) {
        require(versioned != null, "versioned workflow must not be null");
        require(versioned.getSerializedSize() <= MAX_WORKFLOW_BYTES,
                "versioned workflow exceeds the maximum serialized size of "
                        + MAX_WORKFLOW_BYTES + " bytes");
        ContractChecks.annotations(versioned);
        require(versioned.hasWorkflow(), "versioned workflow content must be present");
        validate(versioned.getWorkflow());
        require(fingerprint(versioned.getWorkflow()).equals(versioned.getWorkflowFingerprint()),
                "versioned_workflow.workflow_fingerprint does not match workflow content");
        ContractChecks.timestamp(versioned.getCreatedAt(), "versioned_workflow.created_at", true);
    }

    /** Validates bounded run evidence and every referenced artifact. */
    public static void validate(RunEvidence evidence) {
        require(evidence != null, "run evidence must not be null");
        require(evidence.getSerializedSize() <= MAX_RUN_EVIDENCE_BYTES,
                "run evidence exceeds the maximum serialized size of "
                        + MAX_RUN_EVIDENCE_BYTES + " bytes");
        ContractChecks.annotations(evidence);
        require(evidence.getStatus() == RunStatus.RUN_STATUS_RUNNING
                        || isTerminal(evidence.getStatus()),
                "run.status must be recognized");
        ContractChecks.timestamp(evidence.getStartedAt(), "run.started_at", true);
        ContractChecks.timestamp(evidence.getCompletedAt(), "run.completed_at",
                isTerminal(evidence.getStatus()));
        ContractChecks.order(evidence.getStartedAt(), evidence.getCompletedAt(), "run");
        require(evidence.getDependenciesCount() > 0,
                "run.dependencies must not be empty");
        require(evidence.getDependenciesCount() <= MAX_DEPENDENCIES,
                "run.dependencies exceeds the maximum of " + MAX_DEPENDENCIES);
        Set<String> aliases = new HashSet<>();
        for (ServiceDependency dependency : evidence.getDependenciesList()) {
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
            StepEvidenceValidation.validate(step);
            require(stepNames.add(step.getStepName()),
                    "duplicate run step name: " + step.getStepName());
        }
        validateText(evidence.getFailureSummary(), "run.failure_summary");
    }

    /** Validates a content-addressed artifact reference. */
    public static void validate(ArtifactReference reference) {
        require(reference != null, "artifact reference must not be null");
        ContractChecks.annotations(reference);
        require(Long.compareUnsigned(reference.getSizeBytes(), MAX_ARTIFACT_BYTES) <= 0,
                "artifact.size_bytes exceeds the maximum of " + MAX_ARTIFACT_BYTES);
    }

    /** Validates a workflow, version, run, or step identity against the slug contract. */
    public static void validateName(String value, String field) {
        require(value != null && value.length() <= MAX_NAME_LENGTH && Formats.isSlug(value),
                field + " must be a lowercase slug name");
    }

    /**
     * Validates a reference name - a dependency alias, service profile, or endpoint. These
     * carry service fully-qualified names and sanitized endpoint placeholders, so the contract
     * is the wider path-safe family rather than the slug family.
     */
    public static void validateReference(String value, String field) {
        require(value != null && value.length() <= MAX_NAME_LENGTH
                        && Formats.isPathSafeName(value),
                field + " must be a path-safe reference name");
    }

    /** Computes the lowercase SHA-256 identity of serialized workflow content. */
    public static String fingerprint(Workflow workflow) {
        require(workflow != null, "workflow must not be null");
        return ServiceProfileValidation.sha256(workflow.toByteArray());
    }

    /**
     * Computes the lowercase SHA-256 identity of a step's serialized edge contract:
     * the {@code TypedEdge} bytes with the {@code FanOutSpec} bytes appended when the
     * step fans out. Deterministic because both messages are map-free and field order
     * is fixed. Recording and replay derive the edge fingerprint here, so a drifted
     * edge surfaces identically on both sides.
     */
    public static String edgeFingerprint(WorkflowStep step) {
        require(step != null && step.hasEdge(), "step must carry an edge");
        byte[] edge = step.getEdge().toByteArray();
        if (!step.hasFanOut()) {
            return ServiceProfileValidation.sha256(edge);
        }
        byte[] fanOut = step.getFanOut().toByteArray();
        byte[] both = new byte[edge.length + fanOut.length];
        System.arraycopy(edge, 0, both, 0, edge.length);
        System.arraycopy(fanOut, 0, both, edge.length, fanOut.length);
        return ServiceProfileValidation.sha256(both);
    }

    /** Validates one named and fingerprinted service dependency. */
    public static void validate(ServiceDependency dependency) {
        require(dependency != null, "dependency must not be null");
        ContractChecks.annotations(dependency);
    }

    private static void validateOutput(WorkflowOutput output) {
        validateType(output.getType(), "output.type");
        validateRules(output.getRulesCount(), output.getRulesList(), "output.rules");
        validateCelRules(output.getCelRulesList(), "output.cel_rules");
    }

    /** Validates a typed inter-step edge contract. */
    public static void validate(TypedEdge edge) {
        require(edge.getSourcesCount() >= 1, "step.edge.sources must not be empty");
        require(edge.getSourcesCount() <= MAX_EDGE_SOURCES,
                "step.edge.sources exceeds the maximum of " + MAX_EDGE_SOURCES);
        for (String source : edge.getSourcesList()) {
            validateName(source, "step.edge.sources");
        }
        validateType(edge.getProduceType(), "step.edge.produce_type");
        validateRules(edge.getRulesCount(), edge.getRulesList(), "step.edge.rules");
        validateCelRules(edge.getCelRulesList(), "step.edge.cel_rules");
        if (!edge.getProjectTo().isEmpty()) {
            validateType(edge.getProjectTo(), "step.edge.project_to");
        }
    }

    /** Validates a bounded fan-out contract. */
    public static void validate(FanOutSpec fanOut) {
        require(!fanOut.getItems().isBlank()
                        && fanOut.getItems().length() <= MAX_ITEMS_PATH_LENGTH
                        && fanOut.getItems().codePoints().noneMatch(Character::isWhitespace),
                "step.fan_out.items must be a non-blank dotted field path of at most "
                        + MAX_ITEMS_PATH_LENGTH + " characters");
        require(fanOut.getMaxItems() >= 1 && fanOut.getMaxItems() <= MAX_FANOUT_ITEMS,
                "step.fan_out.max_items must be between 1 and " + MAX_FANOUT_ITEMS);
        require(fanOut.getMaxConcurrency() >= 1
                        && fanOut.getMaxConcurrency() <= MAX_FANOUT_CONCURRENCY,
                "step.fan_out.max_concurrency must be between 1 and "
                        + MAX_FANOUT_CONCURRENCY);
        require(fanOut.getFailurePolicy() == BranchFailurePolicy.BRANCH_FAILURE_POLICY_FAIL_FAST
                        || fanOut.getFailurePolicy()
                                == BranchFailurePolicy.BRANCH_FAILURE_POLICY_CONTINUE,
                "step.fan_out.failure_policy must be FAIL_FAST or CONTINUE");
        validateType(fanOut.getCollectType(), "step.fan_out.collect_type");
        require(!fanOut.getCollectInto().isBlank()
                        && fanOut.getCollectInto().length() <= MAX_COLLECT_INTO_LENGTH
                        && fanOut.getCollectInto().codePoints()
                                .noneMatch(Character::isWhitespace),
                "step.fan_out.collect_into must be a non-blank field name of at most "
                        + MAX_COLLECT_INTO_LENGTH + " characters");
    }

    /** Validates a structured-generation specification. */
    public static void validate(StructuredGenerationSpec spec) {
        require(spec.getMode() != ai.protomolt.proto.inference.v1.StructuredGenerationMode.UNRECOGNIZED,
                "step.structured.mode must be a defined mode");
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

    /** Validates a fully-qualified protobuf message name. */
    public static void validateType(String value, String field) {
        require(value != null && !value.isBlank()
                        && value.codePoints().noneMatch(Character::isWhitespace),
                field + " must be a fully-qualified protobuf message name");
    }

    /** Validates a method reference in Service/Method form. */
    public static void validateMethod(String value, String field) {
        require(value != null && !value.isBlank() && value.indexOf('/') > 0
                        && value.indexOf('/') == value.lastIndexOf('/')
                        && value.indexOf('/') < value.length() - 1
                        && value.codePoints().noneMatch(Character::isWhitespace),
                field + " must use Service/Method form");
    }

    /** Validates a lowercase SHA-256 fingerprint. */
    public static void validateFingerprint(String value, String field) {
        require(value != null && Formats.isSha256Hex(value),
                field + " must be a lowercase SHA-256 fingerprint");
    }

    /** Validates bounded diagnostic or provenance text. */
    public static void validateText(String value, String field) {
        require(value != null && value.length() <= MAX_TEXT_LENGTH,
                field + " exceeds the maximum length of " + MAX_TEXT_LENGTH);
    }

    /** Validates a duration that must be positive. */
    public static void validatePositiveDuration(Duration value, String field) {
        validateDuration(value, field);
        require(value.getSeconds() > 0 || value.getNanos() > 0, field + " must be positive");
    }

    /** Validates a non-negative well-formed duration. */
    public static void validateDuration(Duration value, String field) {
        require(value.getSeconds() >= 0 && value.getNanos() >= 0
                        && value.getNanos() < 1_000_000_000,
                field + " must be a non-negative valid duration");
    }

    private static boolean isTerminal(RunStatus status) {
        return status == RunStatus.RUN_STATUS_SUCCEEDED
                || status == RunStatus.RUN_STATUS_FAILED
                || status == RunStatus.RUN_STATUS_CANCELLED;
    }
}
