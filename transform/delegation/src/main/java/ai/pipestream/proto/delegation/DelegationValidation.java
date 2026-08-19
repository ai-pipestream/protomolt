package ai.pipestream.proto.delegation;

import ai.pipestream.proto.delegation.v1.AcceptanceCheck;
import ai.pipestream.proto.delegation.v1.AdmissionDecision;
import ai.pipestream.proto.delegation.v1.BlockedReport;
import ai.pipestream.proto.delegation.v1.Cancellation;
import ai.pipestream.proto.delegation.v1.CheckEvidence;
import ai.pipestream.proto.delegation.v1.CheckVerdict;
import ai.pipestream.proto.delegation.v1.Checkpoint;
import ai.pipestream.proto.delegation.v1.CheckpointReference;
import ai.pipestream.proto.delegation.v1.CommitReference;
import ai.pipestream.proto.delegation.v1.CompletionAccepted;
import ai.pipestream.proto.delegation.v1.CompletionCandidate;
import ai.pipestream.proto.delegation.v1.DelegateRequest;
import ai.pipestream.proto.delegation.v1.DelegateResponse;
import ai.pipestream.proto.delegation.v1.FailureReport;
import ai.pipestream.proto.delegation.v1.Lane;
import ai.pipestream.proto.delegation.v1.ProgressEvent;
import ai.pipestream.proto.delegation.v1.RevisionRequested;
import ai.pipestream.proto.delegation.v1.TaskOffer;
import ai.pipestream.proto.delegation.v1.TaskMessage;
import ai.pipestream.proto.delegation.v1.TaskMessageKind;
import ai.pipestream.proto.delegation.v1.TaskReject;
import ai.pipestream.proto.delegation.v1.TaskSpec;
import ai.pipestream.proto.delegation.v1.Transcript;
import ai.pipestream.proto.delegation.v1.TranscriptEntry;
import ai.pipestream.proto.delegation.v1.WorkerCapability;
import ai.pipestream.proto.delegation.v1.WorkerHello;
import ai.pipestream.proto.grpc.workflow.WorkflowValidation;
import ai.pipestream.format.Formats;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;

import java.util.HashSet;
import java.util.Set;

/**
 * Structural and safety validation for the delegation contract, mirroring the validate.v1
 * annotations on {@code delegation.proto} the way {@link WorkflowValidation} mirrors the
 * workflow contract. Lifecycle, sequencing, and evidence-completeness checks are the
 * {@link DelegationReducer}'s job; this class only guarantees a frame or transcript is
 * well-formed enough to carry and to reduce.
 */
public final class DelegationValidation {

    /** The sender/recipient identity that names the coordinator in a task message. */
    public static final String COORDINATOR = "coordinator";

    /** Maximum serialized size of one frame. */
    public static final int MAX_FRAME_BYTES = 1024 * 1024;

    /** Maximum entries in one recorded transcript. */
    public static final int MAX_TRANSCRIPT_ENTRIES = 65_536;

    private static final int MAX_CAPABILITIES = 64;
    private static final int MAX_CHECKS = 64;
    private static final int MAX_REFERENCES = 64;
    private static final int MAX_SCOPE_ITEMS = 256;
    private static final int MAX_NEEDS = 32;
    private static final int MAX_ATTEMPT = 1_024;
    private static final int MAX_NAME_LENGTH = 128;

    private DelegationValidation() {
    }

    /** Validates one worker-to-coordinator frame: envelope plus payload. */
    public static void validate(DelegateRequest frame) {
        require(frame != null, "frame must not be null");
        require(frame.getSerializedSize() <= MAX_FRAME_BYTES,
                "frame exceeds the maximum serialized size of " + MAX_FRAME_BYTES
                        + " bytes");
        validateEnvelope(frame.getFrameId(), frame.getTaskId(), frame.getSeq(),
                frame.hasSentAt(), frame.getSentAt());
        switch (frame.getPayloadCase()) {
            case HELLO -> {
                require(frame.getTaskId().isEmpty(),
                        "frame.task_id must be empty on the session-scoped hello");
                validate(frame.getHello());
            }
            case ACCEPT -> {
                requireTaskScoped(frame.getTaskId());
                validateAttempt(frame.getAccept().getAttempt(), "accept.attempt");
            }
            case REJECT -> {
                requireTaskScoped(frame.getTaskId());
                validate(frame.getReject());
            }
            case HEARTBEAT -> {
                requireTaskScoped(frame.getTaskId());
                validateAttempt(frame.getHeartbeat().getAttempt(), "heartbeat.attempt");
                bounded(frame.getHeartbeat().getNote(), 1_024, "heartbeat.note");
            }
            case PROGRESS -> {
                requireTaskScoped(frame.getTaskId());
                validate(frame.getProgress());
            }
            case CHECKPOINT -> {
                requireTaskScoped(frame.getTaskId());
                validate(frame.getCheckpoint());
            }
            case BLOCKED -> {
                requireTaskScoped(frame.getTaskId());
                validate(frame.getBlocked());
            }
            case FAILED -> {
                requireTaskScoped(frame.getTaskId());
                validate(frame.getFailed());
            }
            case CANCELLED -> {
                requireTaskScoped(frame.getTaskId());
                validateAttempt(frame.getCancelled().getAttempt(), "cancelled.attempt");
                bounded(frame.getCancelled().getNote(), 1_024, "cancelled.note");
            }
            case COMPLETION -> {
                requireTaskScoped(frame.getTaskId());
                validate(frame.getCompletion());
            }
            case TASK_MESSAGE -> {
                requireTaskScoped(frame.getTaskId());
                validate(frame.getTaskMessage(), frame.getTaskId());
            }
            default -> require(false, "frame.payload must be set");
        }
    }

    /** Validates one coordinator-to-worker frame: envelope plus payload. */
    public static void validate(DelegateResponse frame) {
        require(frame != null, "frame must not be null");
        require(frame.getSerializedSize() <= MAX_FRAME_BYTES,
                "frame exceeds the maximum serialized size of " + MAX_FRAME_BYTES
                        + " bytes");
        validateEnvelope(frame.getFrameId(), frame.getTaskId(), frame.getSeq(),
                frame.hasSentAt(), frame.getSentAt());
        switch (frame.getPayloadCase()) {
            case ADMISSION -> {
                require(frame.getTaskId().isEmpty(),
                        "frame.task_id must be empty on the session-scoped admission");
                validate(frame.getAdmission());
            }
            case OFFER -> {
                requireTaskScoped(frame.getTaskId());
                validate(frame.getOffer());
            }
            case RENEWAL -> {
                requireTaskScoped(frame.getTaskId());
                validateAttempt(frame.getRenewal().getAttempt(), "renewal.attempt");
                require(frame.getRenewal().hasExpiresAt(),
                        "renewal.expires_at must be set");
                validateTimestamp(frame.getRenewal().getExpiresAt(),
                        "renewal.expires_at");
            }
            case EXPIRED -> {
                requireTaskScoped(frame.getTaskId());
                validateAttempt(frame.getExpired().getAttempt(), "expired.attempt");
                bounded(frame.getExpired().getReason(), 2_048, "expired.reason");
            }
            case CANCELLATION -> {
                requireTaskScoped(frame.getTaskId());
                validate(frame.getCancellation());
            }
            case REVISION_REQUESTED -> {
                requireTaskScoped(frame.getTaskId());
                validate(frame.getRevisionRequested());
            }
            case ACCEPTED -> {
                requireTaskScoped(frame.getTaskId());
                validate(frame.getAccepted());
            }
            case TASK_MESSAGE -> {
                requireTaskScoped(frame.getTaskId());
                validate(frame.getTaskMessage(), frame.getTaskId());
            }
            default -> require(false, "frame.payload must be set");
        }
    }

    /** Validates a recorded transcript: bounds plus every entry's lane and frame. */
    public static void validate(Transcript transcript) {
        require(transcript != null, "transcript must not be null");
        require(transcript.getEntriesCount() <= MAX_TRANSCRIPT_ENTRIES,
                "transcript.entries exceeds the maximum of " + MAX_TRANSCRIPT_ENTRIES);
        for (TranscriptEntry entry : transcript.getEntriesList()) {
            validate(entry);
        }
    }

    /** Validates one transcript entry: lane, stream identity, and lane/frame agreement. */
    public static void validate(TranscriptEntry entry) {
        require(entry != null, "entry must not be null");
        require(entry.getLane() == Lane.LANE_WORKER
                        || entry.getLane() == Lane.LANE_COORDINATOR,
                "entry.lane must be WORKER or COORDINATOR");
        validateName(entry.getWorkerId(), "entry.worker_id");
        if (entry.getLane() == Lane.LANE_WORKER) {
            require(entry.hasWorkerFrame(),
                    "entry.worker_frame must be set when lane is WORKER");
            validate(entry.getWorkerFrame());
        } else {
            require(entry.hasCoordinatorFrame(),
                    "entry.coordinator_frame must be set when lane is COORDINATOR");
            validate(entry.getCoordinatorFrame());
        }
    }

    /** Validates a worker hello. */
    public static void validate(WorkerHello hello) {
        require(hello != null, "hello must not be null");
        validateName(hello.getWorkerId(), "hello.worker_id");
        require(hello.getProtocolVersion() == 1,
                "hello.protocol_version must be 1, the only defined version");
        bounded(hello.getProvider(), 128, "hello.provider");
        bounded(hello.getModel(), 256, "hello.model");
        bounded(hello.getModelVersion(), 512, "hello.model_version");
        require(hello.getCapabilitiesCount() <= MAX_CAPABILITIES,
                "hello.capabilities exceeds the maximum of " + MAX_CAPABILITIES);
        Set<String> names = new HashSet<>();
        for (WorkerCapability capability : hello.getCapabilitiesList()) {
            validateName(capability.getName(), "hello.capabilities.name");
            require(names.add(capability.getName()),
                    "duplicate capability name: " + capability.getName());
            bounded(capability.getDescription(), 2_048,
                    "hello.capabilities.description");
        }
    }

    /** Validates an admission decision. */
    public static void validate(AdmissionDecision admission) {
        require(admission != null, "admission must not be null");
        require(admission.getAdmitted() || !admission.getReason().isBlank(),
                "admission.reason must say why when the worker is rejected");
        bounded(admission.getReason(), 2_048, "admission.reason");
        require(admission.getAdmitted()
                        == Formats.isUuid(admission.getSessionId()),
                "admission.session_id must be a uuid exactly when the worker is"
                        + " admitted");
    }

    /** Validates a task offer, including the embedded spec and lease. */
    public static void validate(TaskOffer offer) {
        require(offer != null, "offer must not be null");
        validateAttempt(offer.getAttempt(), "offer.attempt");
        require(offer.hasSpec(), "offer.spec must be set");
        validate(offer.getSpec());
        WorkflowValidation.validatePositiveDuration(offer.getLeaseDuration(),
                "offer.lease_duration");
        require(offer.hasExpiresAt(), "offer.expires_at must be set");
        validateTimestamp(offer.getExpiresAt(), "offer.expires_at");
        if (offer.hasResumeFrom()) {
            validate(offer.getResumeFrom());
        }
    }

    /** Validates a task spec: objective, scope, constraints, checks, and context. */
    public static void validate(TaskSpec spec) {
        require(spec != null, "spec must not be null");
        require(!spec.getObjective().isBlank(), "spec.objective must not be blank");
        WorkflowValidation.validateText(spec.getObjective(), "spec.objective");
        require(spec.getAllowedScopeCount() <= MAX_SCOPE_ITEMS,
                "spec.allowed_scope exceeds the maximum of " + MAX_SCOPE_ITEMS);
        spec.getAllowedScopeList()
                .forEach(scope -> bounded(scope, 512, "spec.allowed_scope"));
        require(spec.getConstraintsCount() <= MAX_SCOPE_ITEMS,
                "spec.constraints exceeds the maximum of " + MAX_SCOPE_ITEMS);
        spec.getConstraintsList()
                .forEach(constraint -> bounded(constraint, 2_048, "spec.constraints"));
        require(spec.getRequiredChecksCount() >= 1,
                "spec.required_checks must declare at least one acceptance check; a"
                        + " worker saying done is never sufficient");
        require(spec.getRequiredChecksCount() <= MAX_CHECKS,
                "spec.required_checks exceeds the maximum of " + MAX_CHECKS);
        Set<String> names = new HashSet<>();
        for (AcceptanceCheck check : spec.getRequiredChecksList()) {
            validateName(check.getName(), "spec.required_checks.name");
            require(names.add(check.getName()),
                    "duplicate required check name: " + check.getName());
            bounded(check.getDescription(), 2_048, "spec.required_checks.description");
        }
        require(spec.getContextCount() <= MAX_REFERENCES,
                "spec.context exceeds the maximum of " + MAX_REFERENCES);
        spec.getContextList().forEach(WorkflowValidation::validate);
        if (spec.hasDeadline()) {
            WorkflowValidation.validatePositiveDuration(spec.getDeadline(),
                    "spec.deadline");
        }
    }

    /** Validates a checkpoint resume pointer. */
    public static void validate(CheckpointReference reference) {
        require(reference != null, "checkpoint reference must not be null");
        validateAttempt(reference.getAttempt(), "resume_from.attempt");
        require(reference.getCheckpointSeq() >= 1,
                "resume_from.checkpoint_seq must be at least 1");
        bounded(reference.getResumeToken(), 512, "resume_from.resume_token");
        require(!reference.getResumeToken().isBlank(),
                "resume_from.resume_token must not be blank");
    }

    /** Validates a worker reject report. */
    public static void validate(TaskReject reject) {
        require(reject != null, "reject must not be null");
        validateAttempt(reject.getAttempt(), "reject.attempt");
        require(!reject.getReason().isBlank(), "reject.reason must not be blank");
        bounded(reject.getReason(), 2_048, "reject.reason");
    }

    /** Validates a progress event. */
    public static void validate(ProgressEvent progress) {
        require(progress != null, "progress must not be null");
        validateAttempt(progress.getAttempt(), "progress.attempt");
        require(progress.getProgressSeq() >= 1,
                "progress.progress_seq must be at least 1");
        require(!progress.getMessage().isBlank(), "progress.message must not be blank");
        bounded(progress.getMessage(), 4_096, "progress.message");
    }

    /** Validates a checkpoint. */
    public static void validate(Checkpoint checkpoint) {
        require(checkpoint != null, "checkpoint must not be null");
        validateAttempt(checkpoint.getAttempt(), "checkpoint.attempt");
        require(checkpoint.getCheckpointSeq() >= 1,
                "checkpoint.checkpoint_seq must be at least 1");
        bounded(checkpoint.getResumeToken(), 512, "checkpoint.resume_token");
        require(!checkpoint.getResumeToken().isBlank(),
                "checkpoint.resume_token must not be blank");
        if (checkpoint.hasState()) {
            WorkflowValidation.validate(checkpoint.getState());
        }
        bounded(checkpoint.getNote(), 1_024, "checkpoint.note");
    }

    /** Validates a blocked report. */
    public static void validate(BlockedReport blocked) {
        require(blocked != null, "blocked must not be null");
        validateAttempt(blocked.getAttempt(), "blocked.attempt");
        require(!blocked.getReason().isBlank(), "blocked.reason must not be blank");
        bounded(blocked.getReason(), 4_096, "blocked.reason");
        require(blocked.getNeedsCount() <= MAX_NEEDS,
                "blocked.needs exceeds the maximum of " + MAX_NEEDS);
        blocked.getNeedsList()
                .forEach(need -> bounded(need, 1_024, "blocked.needs"));
    }

    /** Validates a failure report. */
    public static void validate(FailureReport failed) {
        require(failed != null, "failed must not be null");
        validateAttempt(failed.getAttempt(), "failed.attempt");
        require(!failed.getError().isBlank(), "failed.error must not be blank");
        bounded(failed.getError(), 8_192, "failed.error");
    }

    /** Validates a coordinator cancellation. */
    public static void validate(Cancellation cancellation) {
        require(cancellation != null, "cancellation must not be null");
        validateAttempt(cancellation.getAttempt(), "cancellation.attempt");
        require(!cancellation.getReason().isBlank(),
                "cancellation.reason must not be blank");
        bounded(cancellation.getReason(), 2_048, "cancellation.reason");
    }

    /** Validates a revision request. */
    public static void validate(RevisionRequested requested) {
        require(requested != null, "revision_requested must not be null");
        validateAttempt(requested.getAttempt(), "revision_requested.attempt");
        validateRevision(requested.getRevision(), "revision_requested.revision");
        require(!requested.getFeedback().isBlank(),
                "revision_requested.feedback must not be blank");
        bounded(requested.getFeedback(), 8_192, "revision_requested.feedback");
        require(requested.getFailedChecksCount() <= MAX_CHECKS,
                "revision_requested.failed_checks exceeds the maximum of " + MAX_CHECKS);
        requested.getFailedChecksList().forEach(
                check -> validateName(check, "revision_requested.failed_checks"));
    }

    /** Validates a completion acceptance. */
    public static void validate(CompletionAccepted accepted) {
        require(accepted != null, "accepted must not be null");
        validateAttempt(accepted.getAttempt(), "accepted.attempt");
        validateRevision(accepted.getRevision(), "accepted.revision");
        require(!accepted.getVerdict().isBlank(), "accepted.verdict must not be blank");
        bounded(accepted.getVerdict(), 2_048, "accepted.verdict");
    }

    /**
     * Validates a completion candidate: revision, evidence shape, and the output
     * references that make "done" reviewable. Whether the evidence covers the offer's
     * required checks is the {@link DelegationReducer}'s job; it needs the transcript.
     */
    public static void validate(CompletionCandidate candidate) {
        require(candidate != null, "completion must not be null");
        validateAttempt(candidate.getAttempt(), "completion.attempt");
        validateRevision(candidate.getRevision(), "completion.revision");
        require(!candidate.getSummary().isBlank(), "completion.summary must not be blank");
        bounded(candidate.getSummary(), 4_096, "completion.summary");
        require(candidate.getEvidenceCount() >= 1,
                "completion.evidence must not be empty");
        require(candidate.getEvidenceCount() <= MAX_CHECKS,
                "completion.evidence exceeds the maximum of " + MAX_CHECKS);
        Set<String> names = new HashSet<>();
        for (CheckEvidence evidence : candidate.getEvidenceList()) {
            validate(evidence);
            require(names.add(evidence.getCheckName()),
                    "duplicate evidence for check: " + evidence.getCheckName());
        }
        require(candidate.getCommitsCount() + candidate.getArtifactsCount() > 0,
                "completion must reference at least one commit or artifact; saying done"
                        + " is not evidence");
        require(candidate.getCommitsCount() <= MAX_REFERENCES,
                "completion.commits exceeds the maximum of " + MAX_REFERENCES);
        candidate.getCommitsList().forEach(DelegationValidation::validate);
        require(candidate.getArtifactsCount() <= MAX_REFERENCES,
                "completion.artifacts exceeds the maximum of " + MAX_REFERENCES);
        candidate.getArtifactsList().forEach(WorkflowValidation::validate);
    }

    /**
     * Validates a task message against its envelope's task. The message is
     * non-transitioning, so this checks structure only: identity, participants,
     * kind, bounded text, and artifact references.
     */
    public static void validate(TaskMessage message, String envelopeTaskId) {
        require(message != null, "task message must not be null");
        require(Formats.isUuid(message.getMessageId()),
                "task_message.message_id must be a uuid: " + message.getMessageId());
        validateName(message.getSender(), "task_message.sender");
        validateName(message.getRecipient(), "task_message.recipient");
        require(!message.getSender().equals(message.getRecipient()),
                "task_message.recipient must differ from task_message.sender");
        require(Formats.isUuid(message.getTaskId()),
                "task_message.task_id must be a uuid: " + message.getTaskId());
        require(message.getTaskId().equals(envelopeTaskId),
                "task_message.task_id must equal the frame's task_id");
        require(message.getKind() != TaskMessageKind.TASK_MESSAGE_KIND_UNSPECIFIED
                        && message.getKind() != TaskMessageKind.UNRECOGNIZED,
                "task_message.kind must be a defined kind");
        require(message.getReplyTo().isEmpty()
                        || Formats.isUuid(message.getReplyTo()),
                "task_message.reply_to must be a uuid or empty: " + message.getReplyTo());
        require(!message.getText().isBlank(), "task_message.text must not be blank");
        bounded(message.getText(), 8_192, "task_message.text");
        require(message.getArtifactsCount() <= MAX_NEEDS,
                "task_message.artifacts exceeds the maximum of " + MAX_NEEDS);
        message.getArtifactsList().forEach(WorkflowValidation::validate);
        require(message.hasSentAt(), "task_message.sent_at must be set");
        validateTimestamp(message.getSentAt(), "task_message.sent_at");
    }

    /** Validates one piece of per-check evidence. */
    public static void validate(CheckEvidence evidence) {
        require(evidence != null, "evidence must not be null");
        validateName(evidence.getCheckName(), "evidence.check_name");
        require(evidence.getVerdict() == CheckVerdict.CHECK_VERDICT_PASSED
                        || evidence.getVerdict() == CheckVerdict.CHECK_VERDICT_FAILED,
                "evidence.verdict must be PASSED or FAILED");
        require(evidence.hasRanAt(), "evidence.ran_at must be set");
        validateTimestamp(evidence.getRanAt(), "evidence.ran_at");
        bounded(evidence.getDetail(), 4_096, "evidence.detail");
        require(evidence.getArtifactsCount() <= MAX_NEEDS,
                "evidence.artifacts exceeds the maximum of " + MAX_NEEDS);
        evidence.getArtifactsList().forEach(WorkflowValidation::validate);
    }

    /** Validates one commit reference. */
    public static void validate(CommitReference commit) {
        require(commit != null, "commit reference must not be null");
        require(!commit.getRepository().isBlank(),
                "commit.repository must not be blank");
        bounded(commit.getRepository(), 512, "commit.repository");
        require(Formats.isSha1Hex(commit.getCommit()),
                "commit.commit must be a full lowercase SHA-1: " + commit.getCommit());
        bounded(commit.getSubject(), 256, "commit.subject");
    }

    private static void validateEnvelope(String frameId, String taskId, long seq,
                                         boolean hasSentAt, Timestamp sentAt) {
        require(Formats.isUuid(frameId),
                "frame.frame_id must be a uuid: " + frameId);
        require(taskId.isEmpty() || Formats.isUuid(taskId),
                "frame.task_id must be a uuid or empty: " + taskId);
        require(seq >= 1, "frame.seq must be at least 1");
        require(hasSentAt, "frame.sent_at must be set");
        validateTimestamp(sentAt, "frame.sent_at");
    }

    private static void requireTaskScoped(String taskId) {
        require(!taskId.isEmpty(), "frame.task_id must name the task on task frames");
    }

    private static void validateAttempt(int attempt, String field) {
        require(attempt >= 1 && attempt <= MAX_ATTEMPT,
                field + " must be between 1 and " + MAX_ATTEMPT);
    }

    private static void validateRevision(int revision, String field) {
        require(revision >= 1 && revision <= MAX_ATTEMPT,
                field + " must be between 1 and " + MAX_ATTEMPT);
    }

    private static void validateName(String value, String field) {
        require(value.length() <= MAX_NAME_LENGTH && Formats.isSlug(value),
                field + " must be a lowercase slug name: " + value);
    }

    private static void validateTimestamp(Timestamp value, String field) {
        require(Timestamps.isValid(value), field + " must be a valid timestamp");
    }

    private static void bounded(String value, int max, String field) {
        require(value.length() <= max,
                field + " exceeds the maximum of " + max + " characters");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
