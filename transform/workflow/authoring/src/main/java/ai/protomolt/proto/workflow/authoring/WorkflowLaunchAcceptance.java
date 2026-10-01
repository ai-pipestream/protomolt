package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.delegation.CandidateReviewer.ReviewContext;
import ai.protomolt.proto.delegation.DelegationReducer;
import ai.protomolt.proto.delegation.DeliverableContracts;
import ai.protomolt.proto.delegation.TranscriptRepository;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.util.Objects;
import java.util.UUID;

/** Evidence of durable terminal acceptance; independent authoring review is still required. */
final class WorkflowLaunchAcceptance {
    private final WorkflowAcceptedCandidate identity;
    private final ReviewContext context;
    private final WorkflowAuthoringDeliverable authored;
    private final Timestamp acceptedAt;

    private WorkflowLaunchAcceptance(WorkflowAcceptedCandidate identity, ReviewContext context,
            WorkflowAuthoringDeliverable authored, Timestamp acceptedAt) {
        this.identity = identity;
        this.context = context;
        this.authored = authored;
        this.acceptedAt = acceptedAt;
    }

    WorkflowAcceptedCandidate identity() { return identity; }
    ReviewContext context() { return context; }
    WorkflowAuthoringDeliverable authored() { return authored; }
    Timestamp acceptedAt() { return acceptedAt; }

    static WorkflowLaunchAcceptance inspect(TranscriptRepository repository, String taskId) {
        Objects.requireNonNull(repository);
        UUID.fromString(taskId);
        var transcript = repository.load().orElseThrow(() -> invalid("no durable transcript"));
        var reduced = new DelegationReducer().reduce(transcript);
        if (!reduced.clean()) throw invalid("durable transcript has lifecycle findings: " + reduced.findings());
        var task = reduced.tasks().get(taskId);
        if (task == null || task.phase() != DelegationReducer.Phase.ACCEPTED) {
            throw invalid("task is not durably accepted");
        }
        TaskSpec spec = null;
        CompletionCandidate candidate = null;
        TranscriptEntry offerEntry = null;
        TranscriptEntry candidateEntry = null;
        TranscriptEntry accepted = null;
        for (var entry : transcript.getEntriesList()) {
            if (entry.hasCoordinatorFrame() && entry.getCoordinatorFrame().getTaskId().equals(taskId)) {
                var frame = entry.getCoordinatorFrame();
                if (frame.hasOffer() && frame.getOffer().getAttempt() == task.attempt()) {
                    spec = frame.getOffer().getSpec();
                    offerEntry = entry;
                }
                if (frame.hasAccepted() && frame.getAccepted().getAttempt() == task.attempt()
                        && frame.getAccepted().getRevision() == task.candidateRevision()) {
                    accepted = entry;
                }
            }
            if (entry.hasWorkerFrame() && entry.getWorkerFrame().getTaskId().equals(taskId)
                    && entry.getWorkerId().equals(task.holder()) && entry.getWorkerFrame().hasCompletion()) {
                var value = entry.getWorkerFrame().getCompletion();
                if (value.getAttempt() == task.attempt() && value.getRevision() == task.candidateRevision()) {
                    candidate = value;
                    candidateEntry = entry;
                }
            }
        }
        if (spec == null || candidate == null || accepted == null
                || !accepted.getWorkerId().equals(task.holder())) {
            throw invalid("accepted offer, worker candidate and coordinator entry do not match");
        }
        // Validate the selected envelopes too: unknown protocol fields must not
        // disappear when extracting the offered contract or candidate payload.
        WorkflowLaunchValidation.validate(offerEntry);
        WorkflowLaunchValidation.validate(candidateEntry);
        WorkflowLaunchValidation.validate(accepted);
        if (!spec.hasContract() || !spec.getContract().getTypeName().equals(
                WorkflowAuthoringDeliverable.getDescriptor().getFullName())) {
            throw invalid("task does not offer the authoring result contract");
        }
        var violations = DeliverableContracts.check(spec.getContract(), candidate.getResult());
        if (!violations.isEmpty()) throw invalid("accepted result violates its offered contract: " + violations);
        WorkflowAuthoringDeliverable authored;
        try {
            authored = candidate.getResult().unpack(WorkflowAuthoringDeliverable.class);
        } catch (InvalidProtocolBufferException e) {
            throw invalid("accepted result is not an authoring deliverable");
        }
        WorkflowLaunchValidation.validate(authored);
        var frame = accepted.getCoordinatorFrame();
        if (!frame.hasSentAt() || !Timestamps.isValid(frame.getSentAt())) {
            throw invalid("acceptance has no valid timestamp");
        }
        var identity = WorkflowAcceptedCandidate.newBuilder().setTaskId(taskId)
                .setAttempt(task.attempt()).setRevision(task.candidateRevision())
                .setTaskSpecSha256(WorkflowLaunchValidation.sha256(spec))
                .setCandidateSha256(WorkflowLaunchValidation.sha256(candidate))
                .setAcceptedEntrySha256(WorkflowLaunchValidation.sha256(accepted)).build();
        WorkflowLaunchValidation.validate(identity);
        return new WorkflowLaunchAcceptance(identity,
                new ReviewContext(taskId, task.holder(), spec, candidate), authored, frame.getSentAt());
    }

    private static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException(reason);
    }
}
