package ai.protomolt.proto.delegation.contract;

import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.AdmissionDecision;
import ai.protomolt.proto.delegation.v1.BlockedReport;
import ai.protomolt.proto.delegation.v1.Cancellation;
import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.Checkpoint;
import ai.protomolt.proto.delegation.v1.CheckpointReference;
import ai.protomolt.proto.delegation.v1.CommitReference;
import ai.protomolt.proto.delegation.v1.CompletionAccepted;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.CandidateReviewIdentity;
import ai.protomolt.proto.delegation.v1.DelegateRequest;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.FailureReport;
import ai.protomolt.proto.delegation.v1.Lane;
import ai.protomolt.proto.delegation.v1.ProgressEvent;
import ai.protomolt.proto.delegation.v1.RevisionRequested;
import ai.protomolt.proto.delegation.v1.ReviewDeferred;
import ai.protomolt.proto.delegation.v1.ReviewFailed;
import ai.protomolt.proto.delegation.v1.ReviewStarted;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.TaskMessage;
import ai.protomolt.proto.delegation.v1.TaskMessageKind;
import ai.protomolt.proto.delegation.v1.TaskReject;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.formats.Formats;

import java.util.HashSet;
import java.util.Set;

/**
 * Structural and safety validation for the delegation contract, mirroring the validate.v1
 * annotations on {@code delegation.proto} the way {@link WorkflowValidation} mirrors the
 * workflow contract. Lifecycle, sequencing, and evidence-completeness checks are the
 * delegation lifecycle reducer's job; this class only guarantees a frame or transcript is
 * well-formed enough to carry and to reduce.
 *
 * <p>The shared atoms live in {@link ValidationChecks} and the review-identity binding
 * checks in {@link ReviewValidation}; both are package-private, so the public entry points
 * and the order of their checks are unchanged.
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
    private static final int MAX_DESCRIPTOR_SET_BYTES = 4 * 1024 * 1024;
    private static final int MAX_JSON_SCHEMA_LENGTH = 262_144;

    private DelegationValidation() {
    }

    /** Validates one worker-to-coordinator frame: envelope plus payload. */
    public static void validate(DelegateRequest frame) {
        ValidationChecks.require(frame != null, "frame must not be null");
        ValidationChecks.require(frame.getSerializedSize() <= MAX_FRAME_BYTES,
                "frame exceeds the maximum serialized size of " + MAX_FRAME_BYTES
                        + " bytes");
        ValidationChecks.validateEnvelope(frame.getFrameId(), frame.getTaskId(),
                frame.getSeq(), frame.hasSentAt(), frame.getSentAt());
        switch (frame.getPayloadCase()) {
            case HELLO -> {
                ValidationChecks.require(frame.getTaskId().isEmpty(),
                        "frame.task_id must be empty on the session-scoped hello");
                validate(frame.getHello());
            }
            case ACCEPT -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                ValidationChecks.validateAttempt(frame.getAccept().getAttempt(), "accept.attempt");
            }
            case REJECT -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                validate(frame.getReject());
            }
            case HEARTBEAT -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                ValidationChecks.validateAttempt(frame.getHeartbeat().getAttempt(), "heartbeat.attempt");
                ValidationChecks.bounded(frame.getHeartbeat().getNote(), 1_024, "heartbeat.note");
            }
            case PROGRESS -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                validate(frame.getProgress());
            }
            case CHECKPOINT -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                validate(frame.getCheckpoint());
            }
            case BLOCKED -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                validate(frame.getBlocked());
            }
            case FAILED -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                validate(frame.getFailed());
            }
            case CANCELLED -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                ValidationChecks.validateAttempt(frame.getCancelled().getAttempt(), "cancelled.attempt");
                ValidationChecks.bounded(frame.getCancelled().getNote(), 1_024, "cancelled.note");
            }
            case COMPLETION -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                validate(frame.getCompletion());
            }
            case TASK_MESSAGE -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                validate(frame.getTaskMessage(), frame.getTaskId());
            }
            default -> ValidationChecks.require(false, "frame.payload must be set");
        }
    }

    /** Validates one coordinator-to-worker frame: envelope plus payload. */
    public static void validate(DelegateResponse frame) {
        ValidationChecks.require(frame != null, "frame must not be null");
        ValidationChecks.require(frame.getSerializedSize() <= MAX_FRAME_BYTES,
                "frame exceeds the maximum serialized size of " + MAX_FRAME_BYTES
                        + " bytes");
        ValidationChecks.validateEnvelope(frame.getFrameId(), frame.getTaskId(),
                frame.getSeq(), frame.hasSentAt(), frame.getSentAt());
        switch (frame.getPayloadCase()) {
            case ADMISSION -> {
                ValidationChecks.require(frame.getTaskId().isEmpty(),
                        "frame.task_id must be empty on the session-scoped admission");
                validate(frame.getAdmission());
            }
            case OFFER -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                validate(frame.getOffer());
            }
            case RENEWAL -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                ValidationChecks.validateAttempt(frame.getRenewal().getAttempt(), "renewal.attempt");
                ValidationChecks.require(frame.getRenewal().hasExpiresAt(),
                        "renewal.expires_at must be set");
                ValidationChecks.validateTimestamp(frame.getRenewal().getExpiresAt(),
                        "renewal.expires_at");
            }
            case EXPIRED -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                ValidationChecks.validateAttempt(frame.getExpired().getAttempt(), "expired.attempt");
                ValidationChecks.bounded(frame.getExpired().getReason(), 2_048, "expired.reason");
            }
            case CANCELLATION -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                validate(frame.getCancellation());
            }
            case REVISION_REQUESTED -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                validate(frame.getRevisionRequested());
            }
            case ACCEPTED -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                validate(frame.getAccepted());
            }
            case REVIEW_STARTED -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                ValidationChecks.requireNoUnknown(frame, "review frame");
                validate(frame.getReviewStarted());
                ValidationChecks.require(frame.getReviewStarted().getIdentity().getTaskId().equals(frame.getTaskId()),
                        "review_started.identity.task_id must match frame.task_id");
                ValidationChecks.require(frame.getReviewStarted().getStartedAt().equals(frame.getSentAt()),
                        "review_started.started_at must match frame.sent_at");
            }
            case REVIEW_FAILED -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                ValidationChecks.requireNoUnknown(frame, "review frame");
                validate(frame.getReviewFailed());
                ValidationChecks.require(frame.getReviewFailed().getIdentity().getTaskId().equals(frame.getTaskId()),
                        "review_failed.identity.task_id must match frame.task_id");
            }
            case REVIEW_DEFERRED -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                ValidationChecks.requireNoUnknown(frame, "review frame");
                validate(frame.getReviewDeferred());
                ValidationChecks.require(frame.getReviewDeferred().getIdentity().getTaskId().equals(frame.getTaskId()),
                        "review_deferred.identity.task_id must match frame.task_id");
            }
            case TASK_MESSAGE -> {
                ValidationChecks.requireTaskScoped(frame.getTaskId());
                validate(frame.getTaskMessage(), frame.getTaskId());
            }
            default -> ValidationChecks.require(false, "frame.payload must be set");
        }
    }

    /** Validates a recorded transcript: bounds plus every entry's lane and frame. */
    public static void validate(Transcript transcript) {
        ValidationChecks.require(transcript != null, "transcript must not be null");
        ValidationChecks.require(transcript.getEntriesCount() <= MAX_TRANSCRIPT_ENTRIES,
                "transcript.entries exceeds the maximum of " + MAX_TRANSCRIPT_ENTRIES);
        for (TranscriptEntry entry : transcript.getEntriesList()) {
            validate(entry);
        }
    }

    /** Validates one transcript entry: lane, stream identity, and lane/frame agreement. */
    public static void validate(TranscriptEntry entry) {
        ValidationChecks.require(entry != null, "entry must not be null");
        ValidationChecks.require(entry.getLane() == Lane.LANE_WORKER
                        || entry.getLane() == Lane.LANE_COORDINATOR,
                "entry.lane must be WORKER or COORDINATOR");
        ValidationChecks.validateName(entry.getWorkerId(), "entry.worker_id");
        if (entry.getLane() == Lane.LANE_WORKER) {
            ValidationChecks.require(entry.hasWorkerFrame(),
                    "entry.worker_frame must be set when lane is WORKER");
            validate(entry.getWorkerFrame());
        } else {
            ValidationChecks.require(entry.hasCoordinatorFrame(),
                    "entry.coordinator_frame must be set when lane is COORDINATOR");
            validate(entry.getCoordinatorFrame());
        }
    }

    /** Validates a worker hello. */
    public static void validate(WorkerHello hello) {
        ValidationChecks.require(hello != null, "hello must not be null");
        ValidationChecks.validateName(hello.getWorkerId(), "hello.worker_id");
        ValidationChecks.require(hello.getProtocolVersion() == 1,
                "hello.protocol_version must be 1, the only defined version");
        ValidationChecks.bounded(hello.getProvider(), 128, "hello.provider");
        ValidationChecks.bounded(hello.getModel(), 256, "hello.model");
        ValidationChecks.bounded(hello.getModelVersion(), 512, "hello.model_version");
        ValidationChecks.require(hello.getCapabilitiesCount() <= MAX_CAPABILITIES,
                "hello.capabilities exceeds the maximum of " + MAX_CAPABILITIES);
        Set<String> names = new HashSet<>();
        for (WorkerCapability capability : hello.getCapabilitiesList()) {
            ValidationChecks.validateName(capability.getName(), "hello.capabilities.name");
            ValidationChecks.require(names.add(capability.getName()),
                    "duplicate capability name: " + capability.getName());
            ValidationChecks.bounded(capability.getDescription(), 2_048,
                    "hello.capabilities.description");
        }
    }

    /** Validates an admission decision. */
    public static void validate(AdmissionDecision admission) {
        ValidationChecks.require(admission != null, "admission must not be null");
        ValidationChecks.require(admission.getAdmitted() || !admission.getReason().isBlank(),
                "admission.reason must say why when the worker is rejected");
        ValidationChecks.bounded(admission.getReason(), 2_048, "admission.reason");
        ValidationChecks.require(admission.getAdmitted()
                        == Formats.isUuid(admission.getSessionId()),
                "admission.session_id must be a uuid exactly when the worker is"
                        + " admitted");
    }

    /** Validates a task offer, including the embedded spec and lease. */
    public static void validate(TaskOffer offer) {
        ValidationChecks.require(offer != null, "offer must not be null");
        ValidationChecks.validateAttempt(offer.getAttempt(), "offer.attempt");
        ValidationChecks.require(offer.hasSpec(), "offer.spec must be set");
        validate(offer.getSpec());
        WorkflowValidation.validatePositiveDuration(offer.getLeaseDuration(),
                "offer.lease_duration");
        ValidationChecks.require(offer.hasExpiresAt(), "offer.expires_at must be set");
        ValidationChecks.validateTimestamp(offer.getExpiresAt(), "offer.expires_at");
        if (offer.hasResumeFrom()) {
            validate(offer.getResumeFrom());
        }
    }

    /** Validates a task spec: objective, scope, constraints, checks, and context. */
    public static void validate(TaskSpec spec) {
        ValidationChecks.require(spec != null, "spec must not be null");
        ValidationChecks.require(!spec.getObjective().isBlank(), "spec.objective must not be blank");
        WorkflowValidation.validateText(spec.getObjective(), "spec.objective");
        ValidationChecks.require(spec.getAllowedScopeCount() <= MAX_SCOPE_ITEMS,
                "spec.allowed_scope exceeds the maximum of " + MAX_SCOPE_ITEMS);
        spec.getAllowedScopeList()
                .forEach(scope -> ValidationChecks.bounded(scope, 512, "spec.allowed_scope"));
        ValidationChecks.require(spec.getConstraintsCount() <= MAX_SCOPE_ITEMS,
                "spec.constraints exceeds the maximum of " + MAX_SCOPE_ITEMS);
        spec.getConstraintsList()
                .forEach(constraint -> ValidationChecks.bounded(constraint, 2_048, "spec.constraints"));
        ValidationChecks.require(spec.getRequiredChecksCount() >= 1,
                "spec.required_checks must declare at least one acceptance check; a"
                        + " worker saying done is never sufficient");
        ValidationChecks.require(spec.getRequiredChecksCount() <= MAX_CHECKS,
                "spec.required_checks exceeds the maximum of " + MAX_CHECKS);
        Set<String> names = new HashSet<>();
        for (AcceptanceCheck check : spec.getRequiredChecksList()) {
            ValidationChecks.validateName(check.getName(), "spec.required_checks.name");
            ValidationChecks.require(names.add(check.getName()),
                    "duplicate required check name: " + check.getName());
            ValidationChecks.bounded(check.getDescription(), 2_048, "spec.required_checks.description");
        }
        ValidationChecks.require(spec.getContextCount() <= MAX_REFERENCES,
                "spec.context exceeds the maximum of " + MAX_REFERENCES);
        spec.getContextList().forEach(WorkflowValidation::validate);
        if (spec.hasDeadline()) {
            WorkflowValidation.validatePositiveDuration(spec.getDeadline(),
                    "spec.deadline");
        }
        if (spec.hasContract()) {
            validate(spec.getContract());
        }
    }

    /**
     * Validates a deliverable contract: the descriptor set is present and bounded, the type
     * name is a full proto name, and the rendered schema is bounded. Whether the set links
     * and declares that type is {@link DeliverableContracts}' job; this only guarantees the
     * contract is well-formed enough to carry.
     */
    public static void validate(DeliverableContract contract) {
        ValidationChecks.require(contract != null, "contract must not be null");
        ValidationChecks.require(!contract.getDescriptorSet().isEmpty(),
                "contract.descriptor_set must not be empty");
        ValidationChecks.require(contract.getDescriptorSet().size() <= MAX_DESCRIPTOR_SET_BYTES,
                "contract.descriptor_set exceeds the maximum of "
                        + MAX_DESCRIPTOR_SET_BYTES + " bytes");
        ValidationChecks.require(!contract.getTypeName().isBlank(),
                "contract.type_name must name the deliverable message");
        ValidationChecks.bounded(contract.getTypeName(), 512, "contract.type_name");
        ValidationChecks.require(Formats.isProtobufFqn(contract.getTypeName()),
                "contract.type_name must be a full proto message name: "
                        + contract.getTypeName());
        ValidationChecks.bounded(contract.getJsonSchema(), MAX_JSON_SCHEMA_LENGTH,
                "contract.json_schema");
    }

    /** Validates a checkpoint resume pointer. */
    public static void validate(CheckpointReference reference) {
        ValidationChecks.require(reference != null, "checkpoint reference must not be null");
        ValidationChecks.validateAttempt(reference.getAttempt(), "resume_from.attempt");
        ValidationChecks.require(reference.getCheckpointSeq() >= 1,
                "resume_from.checkpoint_seq must be at least 1");
        ValidationChecks.bounded(reference.getResumeToken(), 512, "resume_from.resume_token");
        ValidationChecks.require(!reference.getResumeToken().isBlank(),
                "resume_from.resume_token must not be blank");
    }

    /** Validates a worker reject report. */
    public static void validate(TaskReject reject) {
        ValidationChecks.require(reject != null, "reject must not be null");
        ValidationChecks.validateAttempt(reject.getAttempt(), "reject.attempt");
        ValidationChecks.require(!reject.getReason().isBlank(), "reject.reason must not be blank");
        ValidationChecks.bounded(reject.getReason(), 2_048, "reject.reason");
    }

    /** Validates a progress event. */
    public static void validate(ProgressEvent progress) {
        ValidationChecks.require(progress != null, "progress must not be null");
        ValidationChecks.validateAttempt(progress.getAttempt(), "progress.attempt");
        ValidationChecks.require(progress.getProgressSeq() >= 1,
                "progress.progress_seq must be at least 1");
        ValidationChecks.require(!progress.getMessage().isBlank(), "progress.message must not be blank");
        ValidationChecks.bounded(progress.getMessage(), 4_096, "progress.message");
    }

    /** Validates a checkpoint. */
    public static void validate(Checkpoint checkpoint) {
        ValidationChecks.require(checkpoint != null, "checkpoint must not be null");
        ValidationChecks.validateAttempt(checkpoint.getAttempt(), "checkpoint.attempt");
        ValidationChecks.require(checkpoint.getCheckpointSeq() >= 1,
                "checkpoint.checkpoint_seq must be at least 1");
        ValidationChecks.bounded(checkpoint.getResumeToken(), 512, "checkpoint.resume_token");
        ValidationChecks.require(!checkpoint.getResumeToken().isBlank(),
                "checkpoint.resume_token must not be blank");
        if (checkpoint.hasState()) {
            WorkflowValidation.validate(checkpoint.getState());
        }
        ValidationChecks.bounded(checkpoint.getNote(), 1_024, "checkpoint.note");
    }

    /** Validates a blocked report. */
    public static void validate(BlockedReport blocked) {
        ValidationChecks.require(blocked != null, "blocked must not be null");
        ValidationChecks.validateAttempt(blocked.getAttempt(), "blocked.attempt");
        ValidationChecks.require(!blocked.getReason().isBlank(), "blocked.reason must not be blank");
        ValidationChecks.bounded(blocked.getReason(), 4_096, "blocked.reason");
        ValidationChecks.require(blocked.getNeedsCount() <= MAX_NEEDS,
                "blocked.needs exceeds the maximum of " + MAX_NEEDS);
        blocked.getNeedsList()
                .forEach(need -> ValidationChecks.bounded(need, 1_024, "blocked.needs"));
    }

    /** Validates a failure report. */
    public static void validate(FailureReport failed) {
        ValidationChecks.require(failed != null, "failed must not be null");
        ValidationChecks.validateAttempt(failed.getAttempt(), "failed.attempt");
        ValidationChecks.require(!failed.getError().isBlank(), "failed.error must not be blank");
        ValidationChecks.bounded(failed.getError(), 8_192, "failed.error");
    }

    /** Validates a coordinator cancellation. */
    public static void validate(Cancellation cancellation) {
        ValidationChecks.require(cancellation != null, "cancellation must not be null");
        ValidationChecks.validateAttempt(cancellation.getAttempt(), "cancellation.attempt");
        ValidationChecks.require(!cancellation.getReason().isBlank(),
                "cancellation.reason must not be blank");
        ValidationChecks.bounded(cancellation.getReason(), 2_048, "cancellation.reason");
    }

    /** Validates a revision request. */
    public static void validate(RevisionRequested requested) {
        ValidationChecks.require(requested != null, "revision_requested must not be null");
        ValidationChecks.validateAttempt(requested.getAttempt(), "revision_requested.attempt");
        ValidationChecks.validateRevision(requested.getRevision(), "revision_requested.revision");
        ValidationChecks.require(!requested.getFeedback().isBlank(),
                "revision_requested.feedback must not be blank");
        ValidationChecks.bounded(requested.getFeedback(), 8_192, "revision_requested.feedback");
        ValidationChecks.require(requested.getFailedChecksCount() <= MAX_CHECKS,
                "revision_requested.failed_checks exceeds the maximum of " + MAX_CHECKS);
        requested.getFailedChecksList().forEach(
                check -> ValidationChecks.validateName(check, "revision_requested.failed_checks"));
        ValidationChecks.validateOptionalCanonicalUuid(requested.getReviewInvocationId(),
                "revision_requested.review_invocation_id");
    }

    /** Validates a completion acceptance. */
    public static void validate(CompletionAccepted accepted) {
        ValidationChecks.require(accepted != null, "accepted must not be null");
        ValidationChecks.validateAttempt(accepted.getAttempt(), "accepted.attempt");
        ValidationChecks.validateRevision(accepted.getRevision(), "accepted.revision");
        ValidationChecks.require(!accepted.getVerdict().isBlank(), "accepted.verdict must not be blank");
        ValidationChecks.bounded(accepted.getVerdict(), 2_048, "accepted.verdict");
        ValidationChecks.validateOptionalCanonicalUuid(accepted.getReviewInvocationId(),
                "accepted.review_invocation_id");
    }

    /** Validates a candidate review identity: task, worker, attempt, revision, invocation, digests. */
    public static void validate(CandidateReviewIdentity identity) {
        ReviewValidation.validate(identity);
    }

    /** Validates a review start: identity, timestamps, window bound, and retry linkage. */
    public static void validate(ReviewStarted started) {
        ReviewValidation.validate(started);
    }

    /** Validates a review failure: identity plus a defined failure code. */
    public static void validate(ReviewFailed failed) {
        ReviewValidation.validate(failed);
    }

    /** Validates a review deferral: the identity of the invocation left open. */
    public static void validate(ReviewDeferred deferred) {
        ReviewValidation.validate(deferred);
    }

    /**
     * Validates a completion candidate: revision, evidence shape, and the output
     * references that make "done" reviewable. Whether the evidence covers the offer's
     * required checks is the delegation lifecycle reducer's job; it needs the transcript.
     */
    public static void validate(CompletionCandidate candidate) {
        ValidationChecks.require(candidate != null, "completion must not be null");
        ValidationChecks.validateAttempt(candidate.getAttempt(), "completion.attempt");
        ValidationChecks.validateRevision(candidate.getRevision(), "completion.revision");
        ValidationChecks.require(!candidate.getSummary().isBlank(), "completion.summary must not be blank");
        ValidationChecks.bounded(candidate.getSummary(), 4_096, "completion.summary");
        ValidationChecks.require(candidate.getEvidenceCount() >= 1,
                "completion.evidence must not be empty");
        ValidationChecks.require(candidate.getEvidenceCount() <= MAX_CHECKS,
                "completion.evidence exceeds the maximum of " + MAX_CHECKS);
        Set<String> names = new HashSet<>();
        for (CheckEvidence evidence : candidate.getEvidenceList()) {
            validate(evidence);
            ValidationChecks.require(names.add(evidence.getCheckName()),
                    "duplicate evidence for check: " + evidence.getCheckName());
        }
        ValidationChecks.require(candidate.getCommitsCount() + candidate.getArtifactsCount() > 0,
                "completion must reference at least one commit or artifact; saying done"
                        + " is not evidence");
        ValidationChecks.require(candidate.getCommitsCount() <= MAX_REFERENCES,
                "completion.commits exceeds the maximum of " + MAX_REFERENCES);
        candidate.getCommitsList().forEach(DelegationValidation::validate);
        ValidationChecks.require(candidate.getArtifactsCount() <= MAX_REFERENCES,
                "completion.artifacts exceeds the maximum of " + MAX_REFERENCES);
        candidate.getArtifactsList().forEach(WorkflowValidation::validate);
        if (candidate.hasResult()) {
            ValidationChecks.require(candidate.getResult().getTypeUrl().contains("/"),
                    "completion.result.type_url must be a type URL: "
                            + candidate.getResult().getTypeUrl());
        }
    }

    /**
     * Validates a task message against its envelope's task. The message is
     * non-transitioning, so this checks structure only: identity, participants,
     * kind, bounded text, and artifact references.
     */
    public static void validate(TaskMessage message, String envelopeTaskId) {
        ValidationChecks.require(message != null, "task message must not be null");
        ValidationChecks.require(Formats.isUuid(message.getMessageId()),
                "task_message.message_id must be a uuid: " + message.getMessageId());
        ValidationChecks.validateName(message.getSender(), "task_message.sender");
        ValidationChecks.validateName(message.getRecipient(), "task_message.recipient");
        ValidationChecks.require(!message.getSender().equals(message.getRecipient()),
                "task_message.recipient must differ from task_message.sender");
        ValidationChecks.require(Formats.isUuid(message.getTaskId()),
                "task_message.task_id must be a uuid: " + message.getTaskId());
        ValidationChecks.require(message.getTaskId().equals(envelopeTaskId),
                "task_message.task_id must equal the frame's task_id");
        ValidationChecks.require(message.getKind() != TaskMessageKind.TASK_MESSAGE_KIND_UNSPECIFIED
                        && message.getKind() != TaskMessageKind.UNRECOGNIZED,
                "task_message.kind must be a defined kind");
        ValidationChecks.require(message.getReplyTo().isEmpty()
                        || Formats.isUuid(message.getReplyTo()),
                "task_message.reply_to must be a uuid or empty: " + message.getReplyTo());
        ValidationChecks.require(!message.getText().isBlank(), "task_message.text must not be blank");
        ValidationChecks.bounded(message.getText(), 8_192, "task_message.text");
        ValidationChecks.require(message.getArtifactsCount() <= MAX_NEEDS,
                "task_message.artifacts exceeds the maximum of " + MAX_NEEDS);
        message.getArtifactsList().forEach(WorkflowValidation::validate);
        ValidationChecks.require(message.hasSentAt(), "task_message.sent_at must be set");
        ValidationChecks.validateTimestamp(message.getSentAt(), "task_message.sent_at");
    }

    /** Validates one piece of per-check evidence. */
    public static void validate(CheckEvidence evidence) {
        ValidationChecks.require(evidence != null, "evidence must not be null");
        ValidationChecks.validateName(evidence.getCheckName(), "evidence.check_name");
        ValidationChecks.require(evidence.getVerdict() == CheckVerdict.CHECK_VERDICT_PASSED
                        || evidence.getVerdict() == CheckVerdict.CHECK_VERDICT_FAILED,
                "evidence.verdict must be PASSED or FAILED");
        ValidationChecks.require(evidence.hasRanAt(), "evidence.ran_at must be set");
        ValidationChecks.validateTimestamp(evidence.getRanAt(), "evidence.ran_at");
        ValidationChecks.bounded(evidence.getDetail(), 4_096, "evidence.detail");
        ValidationChecks.require(evidence.getArtifactsCount() <= MAX_NEEDS,
                "evidence.artifacts exceeds the maximum of " + MAX_NEEDS);
        evidence.getArtifactsList().forEach(WorkflowValidation::validate);
    }

    /** Validates one commit reference. */
    public static void validate(CommitReference commit) {
        ValidationChecks.require(commit != null, "commit reference must not be null");
        ValidationChecks.require(!commit.getRepository().isBlank(),
                "commit.repository must not be blank");
        ValidationChecks.bounded(commit.getRepository(), 512, "commit.repository");
        ValidationChecks.require(Formats.isSha1Hex(commit.getCommit()),
                "commit.commit must be a full lowercase SHA-1: " + commit.getCommit());
        ValidationChecks.bounded(commit.getSubject(), 256, "commit.subject");
    }
}
