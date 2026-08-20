package ai.pipestream.proto.workflow;

import ai.pipestream.proto.grpc.workflow.v1.ArtifactReference;
import ai.pipestream.proto.grpc.workflow.v1.RunEvidence;
import ai.pipestream.proto.grpc.workflow.v1.StepEvidence;
import ai.pipestream.proto.receipt.Completeness;
import ai.pipestream.proto.receipt.CompletenessStatus;
import ai.pipestream.proto.receipt.RecordArtifact;
import ai.pipestream.proto.receipt.RecordStep;
import ai.pipestream.proto.receipt.RecordSubject;
import ai.pipestream.proto.receipt.StepOutcome;
import ai.pipestream.proto.receipt.WorkRecord;
import ai.pipestream.proto.receipt.WorkRecords;
import com.google.protobuf.Timestamp;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Projects terminal run evidence into a work-record manifest. The
 * projector adds nothing the run did not produce: every manifest field
 * maps to an evidence field, a fingerprint, or a counter that already
 * exists, and the completeness claim is evaluated against
 * {@link #POLICY}, the committed statement of what a complete
 * workflow-run record carries.
 */
public final class WorkRecordProjector {

    /** The evidence policy's identity. */
    public static final String POLICY_ID = "workflow-run-evidence";

    /** The evidence policy's version. */
    public static final String POLICY_VERSION = "1";

    /**
     * The evidence policy: the denominator every completeness claim on a
     * workflow-run record is evaluated against. The policy digest signed
     * into each record is the SHA-256 of exactly this text.
     */
    public static final String POLICY = """
            A complete workflow-run record carries: the workflow name, \
            fingerprint, and run identity; one entry per recorded step with \
            its outcome, timing, and content-addressed request and response \
            artifacts where the step exchanged messages; model identity and \
            token usage for model-driven steps; and the run's input and \
            output artifacts by digest. A record is partial when the run \
            ended without completing, and its missing reasons say what the \
            evidence cannot show.""";

    private static final int MAX_SUMMARY = 4096;

    private WorkRecordProjector() {
    }

    /** SHA-256 of {@link #POLICY}; the digest each record signs. */
    public static String policySha256() {
        return WorkRecords.sha256Hex(POLICY.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * The identity a record is issued under.
     *
     * @param recordId the record's own name
     * @param issuer the issuing party
     * @param keyId the key that will sign the manifest
     * @param issuedAt the claimed issuance time
     * @param priorManifestSha256 the manifest digest this record re-issues,
     *         or empty for a first issue
     */
    public record Issuance(String recordId, String issuer, String keyId,
                           Timestamp issuedAt, String priorManifestSha256) {
        public Issuance {
            Objects.requireNonNull(recordId, "recordId");
            Objects.requireNonNull(issuer, "issuer");
            Objects.requireNonNull(keyId, "keyId");
            Objects.requireNonNull(issuedAt, "issuedAt");
            Objects.requireNonNull(priorManifestSha256, "priorManifestSha256");
        }
    }

    /**
     * Projects terminal run evidence into a manifest. Evidence still
     * running is refused — a record claims what a run produced, and a
     * live run is still producing; a failed or cancelled run projects as
     * a partial record with reasons, never as a refusal.
     */
    public static WorkRecord project(RunEvidence evidence, Issuance issuance) {
        Objects.requireNonNull(evidence, "evidence");
        Objects.requireNonNull(issuance, "issuance");
        WorkRecord.Builder manifest = WorkRecord.newBuilder()
                .setManifestVersion(WorkRecords.MANIFEST_VERSION)
                .setRecordId(issuance.recordId())
                .setIssuer(issuance.issuer())
                .setKeyId(issuance.keyId())
                .setIssuedAt(issuance.issuedAt())
                .setSubject(RecordSubject.newBuilder()
                        .setKind(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN)
                        .setWorkflowName(evidence.getWorkflowName())
                        .setWorkflowVersion(evidence.getWorkflowVersion())
                        .setWorkflowFingerprint(evidence.getWorkflowFingerprint())
                        .setRunId(evidence.getRunId()))
                .setCompleteness(completeness(evidence))
                .setPriorManifestSha256(issuance.priorManifestSha256());
        for (StepEvidence step : evidence.getStepsList()) {
            manifest.addSteps(projectStep(step));
        }
        if (evidence.hasInputArtifact()) {
            manifest.addArtifacts(artifact(evidence.getInputArtifact()));
        }
        if (evidence.hasOutputArtifact()) {
            manifest.addArtifacts(artifact(evidence.getOutputArtifact()));
        }
        return manifest.build();
    }

    private static Completeness completeness(RunEvidence evidence) {
        Completeness.Builder completeness = Completeness.newBuilder()
                .setPolicyId(POLICY_ID)
                .setPolicyVersion(POLICY_VERSION)
                .setPolicySha256(policySha256());
        switch (evidence.getStatus()) {
            case RUN_STATUS_SUCCEEDED -> completeness
                    .setStatus(CompletenessStatus.COMPLETENESS_STATUS_COMPLETE);
            case RUN_STATUS_FAILED -> completeness
                    .setStatus(CompletenessStatus.COMPLETENESS_STATUS_PARTIAL)
                    .addMissingReasons(evidence.getFailureSummary().isBlank()
                            ? "the run failed before completing"
                            : "the run failed: " + bounded(evidence.getFailureSummary(), 512));
            case RUN_STATUS_CANCELLED -> completeness
                    .setStatus(CompletenessStatus.COMPLETENESS_STATUS_PARTIAL)
                    .addMissingReasons("the run was cancelled before completing");
            default -> throw new IllegalArgumentException(
                    "only terminal run evidence projects into a work record; run '"
                            + evidence.getRunId() + "' is " + evidence.getStatus());
        }
        return completeness.build();
    }

    private static RecordStep projectStep(StepEvidence step) {
        RecordStep.Builder projected = RecordStep.newBuilder()
                .setName(step.getStepName())
                .setMethod(step.getMethod())
                .setOutcome(outcome(step))
                .setSummary(bounded(step.getSummary(), MAX_SUMMARY));
        if (step.hasStartedAt()) {
            projected.setStartedAt(step.getStartedAt());
        }
        if (step.hasCompletedAt()) {
            projected.setCompletedAt(step.getCompletedAt());
        }
        if (step.hasRequestArtifact()) {
            projected.setRequestArtifact(artifact(step.getRequestArtifact()));
        }
        if (step.hasResponseArtifact()) {
            projected.setResponseArtifact(artifact(step.getResponseArtifact()));
        }
        if (step.hasStructured()) {
            projected.setPromptTokens(step.getStructured().getTotalUsage().getPromptTokens())
                    .setCompletionTokens(
                            step.getStructured().getTotalUsage().getCompletionTokens())
                    .setModel(step.getStructured().getModel())
                    .setModelVersion(step.getStructured().getModelVersion());
        }
        return projected.build();
    }

    private static StepOutcome outcome(StepEvidence step) {
        return switch (step.getStatus()) {
            case STEP_STATUS_SUCCEEDED -> StepOutcome.STEP_OUTCOME_SUCCEEDED;
            case STEP_STATUS_FAILED -> StepOutcome.STEP_OUTCOME_FAILED;
            case STEP_STATUS_SKIPPED -> StepOutcome.STEP_OUTCOME_SKIPPED;
            case STEP_STATUS_CANCELLED -> StepOutcome.STEP_OUTCOME_CANCELLED;
            default -> throw new IllegalArgumentException("step '" + step.getStepName()
                    + "' carries no recorded status");
        };
    }

    private static RecordArtifact artifact(ArtifactReference reference) {
        return RecordArtifact.newBuilder()
                .setSha256(reference.getSha256())
                .setMediaType(reference.getMediaType())
                .setSizeBytes(reference.getSizeBytes())
                .setRedacted(reference.getRedacted())
                .build();
    }

    private static String bounded(String value, int max) {
        if (value.length() <= max) {
            return value;
        }
        int cut = max;
        if (Character.isHighSurrogate(value.charAt(cut - 1))) {
            cut--;
        }
        return value.substring(0, cut);
    }
}
