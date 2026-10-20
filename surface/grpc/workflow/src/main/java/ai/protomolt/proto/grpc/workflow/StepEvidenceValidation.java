package ai.protomolt.proto.grpc.workflow;

import ai.protomolt.proto.formats.Formats;
import ai.protomolt.proto.grpc.workflow.v1.BranchEvidence;
import ai.protomolt.proto.grpc.workflow.v1.EdgeEvidence;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.grpc.workflow.v1.StepEvidence;
import ai.protomolt.proto.grpc.workflow.v1.StepStatus;
import ai.protomolt.proto.grpc.workflow.v1.StructuredAttemptEvidence;
import ai.protomolt.proto.grpc.workflow.v1.StructuredGenerationEvidence;
import ai.protomolt.proto.inference.v1.AttemptOutcome;
import ai.protomolt.proto.inference.v1.FinishReason;
import ai.protomolt.proto.inference.v1.Usage;

import java.util.HashSet;
import java.util.Set;

import static ai.protomolt.proto.grpc.workflow.ContractChecks.require;

/**
 * The per-step part of run-evidence admission: one recorded step's status, shape
 * exclusivity, edge and fan-out branch evidence, and structured-generation attempt
 * history. {@link WorkflowValidation#validate(RunEvidence)}
 * owns the run-level checks and calls this once per recorded step, after the run's
 * annotation pass has already covered every annotated field.
 */
final class StepEvidenceValidation {

    private static final int MAX_PROVIDER_LENGTH = 128;
    private static final int MAX_MODEL_VERSION_LENGTH = 512;

    private StepEvidenceValidation() {
    }

    static void validate(StepEvidence step) {
        // step_name is covered by the RunEvidence annotation pass, which recurses here.
        require(step.getStatus() == StepStatus.STEP_STATUS_SUCCEEDED
                        || step.getStatus() == StepStatus.STEP_STATUS_FAILED
                        || step.getStatus() == StepStatus.STEP_STATUS_SKIPPED
                        || step.getStatus() == StepStatus.STEP_STATUS_CANCELLED,
                "step_evidence.status must be terminal");
        if (step.hasStructured()) {
            require(step.getMethod().isEmpty(),
                    "step_evidence.method must be empty when structured evidence is present");
            validateStructuredEvidence(step.getStructured(), step.getStatus());
        } else if (step.hasEdge()) {
            // A structured fan-out step records no method and no structured
            // evidence (there is one generation per branch, not one per step);
            // every other edge step records its method as usual.
            if (!step.getMethod().isEmpty()) {
                WorkflowValidation.validateMethod(step.getMethod(), "step_evidence.method");
            }
            validateEdgeEvidence(step.getEdge(), step.getStepName(), step.getStatus());
        } else {
            WorkflowValidation.validateMethod(step.getMethod(), "step_evidence.method");
        }
        ContractChecks.timestamp(step.getStartedAt(), "step_evidence.started_at", true);
        ContractChecks.timestamp(step.getCompletedAt(), "step_evidence.completed_at", true);
        ContractChecks.order(step.getStartedAt(), step.getCompletedAt(), "step_evidence");
        if (step.hasRequestArtifact()) {
            WorkflowValidation.validate(step.getRequestArtifact());
        }
        if (step.hasResponseArtifact()) {
            WorkflowValidation.validate(step.getResponseArtifact());
        }
        require(step.getGrpcStatusCode() >= 0 && step.getGrpcStatusCode() <= 16,
                "step_evidence.grpc_status_code must be between 0 and 16");
        WorkflowValidation.validateText(step.getSummary(), "step_evidence.summary");
    }

    static void validateEdgeEvidence(EdgeEvidence evidence, String stepName,
                                     StepStatus status) {
        WorkflowValidation.validateFingerprint(evidence.getEdgeFingerprint(),
                "edge_evidence.edge_fingerprint");
        require(evidence.getSourceCount() >= 1,
                "edge_evidence.source_count must be at least 1");
        require(evidence.getItemCount() >= 0
                        && evidence.getItemCount() <= WorkflowValidation.MAX_FANOUT_ITEMS,
                "edge_evidence.item_count must be between 0 and "
                        + WorkflowValidation.MAX_FANOUT_ITEMS);
        require(evidence.getBranchesCount() <= WorkflowValidation.MAX_FANOUT_ITEMS,
                "edge_evidence.branches exceeds the maximum of "
                        + WorkflowValidation.MAX_FANOUT_ITEMS);
        if (evidence.getItemCount() == 0) {
            require(evidence.getBranchesCount() == 0,
                    "edge_evidence.branches must be empty when item_count is 0");
        } else {
            require(evidence.getBranchesCount() == evidence.getItemCount(),
                    "edge_evidence.branches must cover every item: abandoned branches "
                            + "are recorded, not dropped");
        }
        if (status == StepStatus.STEP_STATUS_SUCCEEDED
                && evidence.getBranchesCount() == 0) {
            require(evidence.getValidationPassed(),
                    "a succeeded edge step without branches records validation_passed "
                            + "true");
        } else if (status == StepStatus.STEP_STATUS_SKIPPED) {
            require(evidence.getValidationPassed(),
                    "a gate-skipped edge step never evaluated its value; "
                            + "validation_passed stays true");
        }
        Set<String> branchIds = new HashSet<>();
        for (BranchEvidence branch : evidence.getBranchesList()) {
            require(isBranchId(branch.getBranchId()),
                    "edge_evidence.branches.branch_id must be '<step-name>#<index>'");
            require(branch.getBranchId().startsWith(stepName + "#"),
                    "edge_evidence.branches.branch_id must name its own step: "
                            + branch.getBranchId());
            require(branchIds.add(branch.getBranchId()),
                    "duplicate branch id: " + branch.getBranchId());
            int index = Integer.parseInt(
                    branch.getBranchId().substring(branch.getBranchId().lastIndexOf('#') + 1));
            require(index < Math.max(evidence.getItemCount(), 1),
                    "edge_evidence.branches.branch_id index exceeds the item count: "
                            + branch.getBranchId());
            require(branch.getStatus() == StepStatus.STEP_STATUS_SUCCEEDED
                            || branch.getStatus() == StepStatus.STEP_STATUS_FAILED,
                    "edge_evidence.branches.status must be SUCCEEDED or FAILED");
            if (branch.hasResponseArtifact()) {
                WorkflowValidation.validate(branch.getResponseArtifact());
            }
            WorkflowValidation.validateText(branch.getSummary(),
                    "edge_evidence.branches.summary");
        }
    }

    static void validateStructuredEvidence(StructuredGenerationEvidence evidence,
                                           StepStatus status) {
        require(status != StepStatus.STEP_STATUS_SKIPPED,
                "structured_evidence cannot be attached to a skipped step");
        WorkflowValidation.validateType(evidence.getTargetType(),
                "structured_evidence.target_type");
        require(!evidence.getModel().isBlank()
                        && evidence.getModel().length() <= WorkflowValidation.MAX_MODEL_LENGTH,
                "structured_evidence.model must be non-blank and at most "
                        + WorkflowValidation.MAX_MODEL_LENGTH + " characters");
        require(evidence.getProvider().length() <= MAX_PROVIDER_LENGTH,
                "structured_evidence.provider must be at most "
                        + MAX_PROVIDER_LENGTH + " characters");
        require(evidence.getModelVersion().length() <= MAX_MODEL_VERSION_LENGTH,
                "structured_evidence.model_version must be at most "
                        + MAX_MODEL_VERSION_LENGTH + " characters");
        WorkflowValidation.validateFingerprint(evidence.getPromptFingerprint(),
                "structured_evidence.prompt_fingerprint");
        WorkflowValidation.validateFingerprint(evidence.getSchemaFingerprint(),
                "structured_evidence.schema_fingerprint");
        // Empty attempt history is allowed only on a step that failed before the
        // first model invocation (no generator configured, preflight rejection).
        require(evidence.getAttemptsCount() <= WorkflowValidation.MAX_STRUCTURED_ATTEMPTS,
                "structured_evidence.attempts exceeds the maximum of "
                        + WorkflowValidation.MAX_STRUCTURED_ATTEMPTS);
        require(evidence.getAttemptsCount() > 0
                        || status == StepStatus.STEP_STATUS_FAILED
                        || status == StepStatus.STEP_STATUS_CANCELLED,
                "empty structured_evidence.attempts is allowed only on a failed "
                        + "or cancelled pre-invocation step");
        validateUsage(evidence.getTotalUsage(), "structured_evidence.total_usage");
        long promptTokens = 0;
        long completionTokens = 0;
        for (int i = 0; i < evidence.getAttemptsCount(); i++) {
            StructuredAttemptEvidence attempt = evidence.getAttempts(i);
            require(attempt.getAttempt() == i + 1,
                    "structured_evidence.attempts must be sequentially numbered "
                            + "from 1; entry " + i + " claims attempt "
                            + attempt.getAttempt());
            AttemptOutcome outcome = attempt.getOutcome();
            require(outcome != AttemptOutcome.ATTEMPT_OUTCOME_UNSPECIFIED
                            && outcome != AttemptOutcome.UNRECOGNIZED,
                    "structured_evidence.attempts outcomes must be defined");
            require(attempt.getFinishReason() != FinishReason.UNRECOGNIZED,
                    "structured_evidence.attempts finish reasons must be defined values");
            validateUsage(attempt.getUsage(),
                    "structured_evidence.attempts[" + i + "].usage");
            require(i == evidence.getAttemptsCount() - 1
                            || outcome != AttemptOutcome.ATTEMPT_OUTCOME_SUCCEEDED,
                    "only the last structured_evidence attempt may be SUCCEEDED");
            require(status == StepStatus.STEP_STATUS_SUCCEEDED
                            || outcome != AttemptOutcome.ATTEMPT_OUTCOME_SUCCEEDED,
                    "a failed or cancelled structured step cannot record a "
                            + "SUCCEEDED attempt");
            try {
                promptTokens = Math.addExact(promptTokens,
                        attempt.getUsage().getPromptTokens());
                completionTokens = Math.addExact(completionTokens,
                        attempt.getUsage().getCompletionTokens());
            } catch (ArithmeticException overflow) {
                throw new IllegalArgumentException(
                        "structured_evidence attempt usage sum overflows int64", overflow);
            }
        }
        require(evidence.getTotalUsage().getPromptTokens() == promptTokens
                        && evidence.getTotalUsage().getCompletionTokens() == completionTokens,
                "structured_evidence.total_usage must equal the sum of per-attempt usage");
        if (status == StepStatus.STEP_STATUS_SUCCEEDED) {
            require(!evidence.getProvider().isBlank(),
                    "a succeeded structured step records a provider");
            require(evidence.getAttemptsCount() > 0,
                    "a succeeded structured step records at least one attempt");
            require(evidence.getAttempts(evidence.getAttemptsCount() - 1).getOutcome()
                            == AttemptOutcome.ATTEMPT_OUTCOME_SUCCEEDED,
                    "a succeeded structured step's last attempt must be SUCCEEDED");
            require(evidence.getValidationPassed(),
                    "a succeeded structured step records validation_passed true");
        } else {
            require(!evidence.getValidationPassed(),
                    "a failed or cancelled structured step records validation_passed false");
        }
    }

    private static void validateUsage(Usage usage, String field) {
        require(usage.getPromptTokens() >= 0,
                field + ".prompt_tokens must not be negative");
        require(usage.getCompletionTokens() >= 0,
                field + ".completion_tokens must not be negative");
    }

    /** A fan-out branch id: a slug step name, {@code #}, then one to four digits. */
    static boolean isBranchId(String value) {
        int hash = value.lastIndexOf('#');
        if (hash <= 0 || hash > WorkflowValidation.MAX_NAME_LENGTH
                || !Formats.isSlug(value.substring(0, hash))) {
            return false;
        }
        int digits = value.length() - hash - 1;
        if (digits < 1 || digits > 4) {
            return false;
        }
        for (int i = hash + 1; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }
}
