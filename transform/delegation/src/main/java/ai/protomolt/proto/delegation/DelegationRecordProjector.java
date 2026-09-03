package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.DelegateRequest;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.receipt.Completeness;
import ai.protomolt.proto.receipt.CompletenessStatus;
import ai.protomolt.proto.receipt.RecordArtifact;
import ai.protomolt.proto.receipt.RecordStep;
import ai.protomolt.proto.receipt.RecordSubject;
import ai.protomolt.proto.receipt.StepOutcome;
import ai.protomolt.proto.receipt.WorkRecord;
import ai.protomolt.proto.receipt.WorkRecords;
import com.google.protobuf.Timestamp;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * Projects a task's delegation transcript into a work-record manifest: the
 * receipt a delegation hands over, the way a workflow run hands over its
 * evidence. The projector adds nothing the transcript did not record.
 *
 * <p>The record's steps are the task's lifecycle milestones — offer,
 * acceptance, each candidate with its review outcome, and the terminal fact —
 * with the reviewer's recorded words as their summaries. Full fidelity rides
 * as an artifact: the transcript's own deterministic bytes are referenced by
 * digest, so a relying party holding the transcript can check it against the
 * record, and one holding only the record knows exactly which transcript it
 * attests.
 */
public final class DelegationRecordProjector {

    /** The evidence policy's identity. */
    public static final String POLICY_ID = "delegation-task-evidence";

    /** The evidence policy's version. */
    public static final String POLICY_VERSION = "1";

    /**
     * The evidence policy: the denominator every completeness claim on a
     * delegation-task record is evaluated against. The policy digest signed
     * into each record is the SHA-256 of exactly this text.
     */
    public static final String POLICY = """
            A complete delegation-task record carries: the task, worker, and \
            offered-spec identity; one step per lifecycle milestone (offer, \
            acceptance, each completion candidate with its review outcome, \
            and the terminal fact) with the recorded words as summaries; the \
            accepted candidate's referenced artifacts by digest; and the \
            transcript's own deterministic bytes by digest. A record is \
            partial when the task ended without acceptance, and its missing \
            reasons say what the evidence cannot show.""";

    /**
     * The transcript artifact's media type. The manifest's media-type rule
     * admits no parameters, so which message the bytes decode as is stated by
     * the policy text rather than a type parameter.
     */
    static final String TRANSCRIPT_MEDIA_TYPE = "application/x-protobuf";

    private static final int MAX_SUMMARY = 4096;

    private DelegationRecordProjector() {
    }

    /** SHA-256 of {@link #POLICY}; the digest each record signs. */
    public static String policySha256() {
        return WorkRecords.sha256Hex(POLICY.getBytes(StandardCharsets.UTF_8));
    }

    /** The identity a record is issued under; the workflow projector's shape. */
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
     * Projects one task's transcript entries into a manifest. A task still in
     * flight is refused — a record claims what a delegation produced, and a
     * live task is still producing; a cancelled, failed, or expired task
     * projects as a partial record with reasons, never as a refusal.
     *
     * @param taskId the task the entries record
     * @param entries the task's transcript entries, in recorded order
     */
    public static WorkRecord project(String taskId, List<TranscriptEntry> entries,
                                     Issuance issuance) {
        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(entries, "entries");
        Objects.requireNonNull(issuance, "issuance");
        TaskSpec spec = null;
        String workerId = "";
        CompletionCandidate accepted = null;
        CompletionCandidate open = null;
        WorkRecord.Builder manifest = WorkRecord.newBuilder()
                .setManifestVersion(WorkRecords.MANIFEST_VERSION)
                .setRecordId(issuance.recordId())
                .setIssuer(issuance.issuer())
                .setKeyId(issuance.keyId())
                .setIssuedAt(issuance.issuedAt())
                .setPriorManifestSha256(issuance.priorManifestSha256());
        Completeness.Builder completeness = null;
        for (TranscriptEntry entry : entries) {
            if (entry.hasCoordinatorFrame()) {
                DelegateResponse frame = entry.getCoordinatorFrame();
                switch (frame.getPayloadCase()) {
                    case OFFER -> {
                        spec = frame.getOffer().getSpec();
                        workerId = entry.getWorkerId();
                        manifest.addSteps(step("offer",
                                StepOutcome.STEP_OUTCOME_SUCCEEDED,
                                spec.getObjective()));
                    }
                    case REVISION_REQUESTED -> {
                        manifest.addSteps(step(
                                "revision-r" + frame.getRevisionRequested().getRevision(),
                                StepOutcome.STEP_OUTCOME_FAILED,
                                frame.getRevisionRequested().getFeedback()));
                        open = null;
                    }
                    case ACCEPTED -> {
                        manifest.addSteps(step("accepted",
                                StepOutcome.STEP_OUTCOME_SUCCEEDED,
                                frame.getAccepted().getVerdict()));
                        accepted = open;
                        completeness = terminal(
                                CompletenessStatus.COMPLETENESS_STATUS_COMPLETE, "");
                    }
                    case CANCELLATION -> {
                        manifest.addSteps(step("cancelled",
                                StepOutcome.STEP_OUTCOME_CANCELLED,
                                frame.getCancellation().getReason()));
                        completeness = terminal(
                                CompletenessStatus.COMPLETENESS_STATUS_PARTIAL,
                                "the task was cancelled before acceptance");
                    }
                    case EXPIRED -> {
                        manifest.addSteps(step("expired",
                                StepOutcome.STEP_OUTCOME_FAILED, ""));
                        completeness = terminal(
                                CompletenessStatus.COMPLETENESS_STATUS_PARTIAL,
                                "the lease expired before acceptance");
                    }
                    default -> {
                        // Admissions, renewals, and messages are recorded
                        // context, not milestones; the transcript artifact
                        // carries them in full.
                    }
                }
            } else if (entry.hasWorkerFrame()) {
                DelegateRequest frame = entry.getWorkerFrame();
                switch (frame.getPayloadCase()) {
                    case ACCEPT -> manifest.addSteps(step(
                            "accept-attempt-" + frame.getAccept().getAttempt(),
                            StepOutcome.STEP_OUTCOME_SUCCEEDED, ""));
                    case COMPLETION -> {
                        open = frame.getCompletion();
                        manifest.addSteps(step(
                                "candidate-r" + frame.getCompletion().getRevision(),
                                StepOutcome.STEP_OUTCOME_SUCCEEDED,
                                frame.getCompletion().getSummary()));
                    }
                    case FAILED -> {
                        manifest.addSteps(step("failed",
                                StepOutcome.STEP_OUTCOME_FAILED,
                                frame.getFailed().getError()));
                        completeness = terminal(
                                CompletenessStatus.COMPLETENESS_STATUS_PARTIAL,
                                "the worker reported failure before acceptance");
                    }
                    case CANCELLED -> {
                        manifest.addSteps(step("cancelled",
                                StepOutcome.STEP_OUTCOME_CANCELLED,
                                frame.getCancelled().getNote()));
                        completeness = terminal(
                                CompletenessStatus.COMPLETENESS_STATUS_PARTIAL,
                                "the worker withdrew before acceptance");
                    }
                    default -> {
                        // Heartbeats, progress, checkpoints, and messages are
                        // recorded context, not milestones.
                    }
                }
            }
        }
        if (spec == null) {
            throw new IllegalArgumentException("task '" + taskId
                    + "' has no recorded offer; there is no contract to attest");
        }
        if (completeness == null) {
            throw new IllegalArgumentException(
                    "only a terminal task projects into a work record; task '" + taskId
                            + "' is still in flight");
        }
        manifest.setSubject(RecordSubject.newBuilder()
                .setKind("delegation-task")
                .setTaskId(taskId)
                .setWorkerId(workerId)
                .setSpecSha256(WorkRecords.fingerprint(spec)));
        manifest.setCompleteness(completeness.build());
        if (accepted != null) {
            for (ArtifactReference reference : accepted.getArtifactsList()) {
                manifest.addArtifacts(artifact(reference));
            }
        }
        byte[] transcriptBytes = WorkRecords.deterministicBytes(
                Transcript.newBuilder().addAllEntries(entries).build());
        manifest.addArtifacts(RecordArtifact.newBuilder()
                .setSha256(WorkRecords.sha256Hex(transcriptBytes))
                .setMediaType(TRANSCRIPT_MEDIA_TYPE)
                .setSizeBytes(transcriptBytes.length));
        return manifest.build();
    }

    private static Completeness.Builder terminal(CompletenessStatus status, String reason) {
        Completeness.Builder completeness = Completeness.newBuilder()
                .setPolicyId(POLICY_ID)
                .setPolicyVersion(POLICY_VERSION)
                .setPolicySha256(policySha256())
                .setStatus(status);
        if (!reason.isEmpty()) {
            completeness.addMissingReasons(reason);
        }
        return completeness;
    }

    private static RecordStep step(String name, StepOutcome outcome, String summary) {
        return RecordStep.newBuilder()
                .setName(name)
                .setOutcome(outcome)
                .setSummary(summary.length() <= MAX_SUMMARY
                        ? summary : summary.substring(0, MAX_SUMMARY))
                .build();
    }

    private static RecordArtifact artifact(ArtifactReference reference) {
        return RecordArtifact.newBuilder()
                .setSha256(reference.getSha256())
                .setMediaType(reference.getMediaType())
                .setSizeBytes(reference.getSizeBytes())
                .setRedacted(reference.getRedacted())
                .build();
    }
}
