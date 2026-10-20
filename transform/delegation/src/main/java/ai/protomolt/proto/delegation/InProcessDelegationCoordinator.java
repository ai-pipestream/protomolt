package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.lifecycle.InMemoryTranscriptRepository;

import ai.protomolt.proto.delegation.lifecycle.TranscriptRepository;

import ai.protomolt.proto.delegation.lifecycle.DelegationReviewBindings;

import ai.protomolt.proto.delegation.lifecycle.DelegationReducer;

import ai.protomolt.proto.delegation.contract.DelegationValidation;

import ai.protomolt.proto.delegation.contract.DeliverableContracts;

import ai.protomolt.proto.delegation.CandidateReviewer.ReviewDecision;
import ai.protomolt.proto.delegation.DelegationCoordinatorState.Session;
import ai.protomolt.proto.delegation.DelegationCoordinatorState.TaskRuntime;
import ai.protomolt.proto.delegation.v1.AdmissionDecision;
import ai.protomolt.proto.delegation.v1.AgentDelegationServiceGrpc;
import ai.protomolt.proto.delegation.v1.Cancellation;
import ai.protomolt.proto.delegation.v1.CompletionAccepted;
import ai.protomolt.proto.delegation.v1.CheckpointReference;
import ai.protomolt.proto.delegation.v1.DelegateRequest;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.Lane;
import ai.protomolt.proto.delegation.v1.RevisionRequested;
import ai.protomolt.proto.delegation.v1.ReviewStarted;
import ai.protomolt.proto.delegation.v1.RetryCandidateReviewRequest;
import ai.protomolt.proto.delegation.v1.RetryCandidateReviewResponse;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.TaskMessage;
import ai.protomolt.proto.delegation.v1.TaskMessageKind;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import ai.protomolt.proto.formats.Formats;
import ai.protomolt.proto.validate.ProtoValidator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * In-memory delegation coordinator for embedded servers, tests, and MCP long polling.
 * The wire transcript remains the source of truth and is checked by
 * {@link DelegationReducer} after every accepted frame.
 *
 * <p>The coordinator is a single-monitor state machine: every projection mutation happens
 * under the one {@code lock}, whether driven by the live path or by transcript replay at
 * construction. The mutable projection lives in {@code DelegationCoordinatorState}, replay
 * in {@code DelegationReplay}, the candidate-review runtime in
 * {@code CoordinatorReviewRuntime}, and the lease clock in {@code CoordinatorLeaseSchedule};
 * those package-private collaborators share the coordinator's monitor and never create one
 * of their own. Frame emission, transcript persistence, and review application remain here
 * behind {@code CoordinatorFrames}, so transcript authority and reducer ordering are
 * unchanged by the extraction.
 */
public final class InProcessDelegationCoordinator
        extends AgentDelegationServiceGrpc.AgentDelegationServiceImplBase
        implements AutoCloseable {

    private static final System.Logger LOG =
            System.getLogger(InProcessDelegationCoordinator.class.getName());

    private final Object lock = new Object();
    private final AdmissionPolicy admissionPolicy;
    private final CandidateReviewer reviewer;
    private final Clock clock;
    private final TranscriptRepository transcripts;
    private final boolean scheduleLeaseExpiries;
    private final boolean scheduleReviewDeadlines;
    private final java.time.Duration reviewWindow;
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private final ExecutorService runtimeTasks = Executors.newVirtualThreadPerTaskExecutor();
    private final DelegationReducer reducer = new DelegationReducer();
    private final DelegationCoordinatorState projection = new DelegationCoordinatorState();
    private final CoordinatorFrames frames = new CoordinatorFrames() {
        @Override
        public void emit(Session session, String taskId, int attempt,
                         DelegateResponse.Builder payload) {
            InProcessDelegationCoordinator.this.emit(session, taskId, attempt, payload);
        }

        @Override
        public void emit(Session session, String taskId, int attempt,
                         DelegateResponse.Builder payload, Runnable afterPersist) {
            InProcessDelegationCoordinator.this.emit(session, taskId, attempt, payload,
                    afterPersist);
        }

        @Override
        public void applyReview(TaskRuntime task, ReviewDecision decision,
                                String invocationId) {
            InProcessDelegationCoordinator.this.applyReview(task, decision, invocationId);
        }
    };
    private final CoordinatorReviewRuntime reviewRuntime;
    private final CoordinatorLeaseSchedule leaseSchedule;

    /** Material for a first offer, supplied only after committed replay has been ruled out. */
    public record InitialOffer(TaskSpec spec, java.time.Duration leaseDuration,
            String startBindingSha256) {
        public InitialOffer {
            Objects.requireNonNull(spec, "spec");
            Objects.requireNonNull(leaseDuration, "leaseDuration");
            Objects.requireNonNull(startBindingSha256, "startBindingSha256");
        }
    }

    /** Creates a coordinator that admits workers and leaves candidates for manual review. */
    public InProcessDelegationCoordinator() {
        this(AdmissionPolicy.allowAll(), CandidateReviewer.manual(), Clock.systemUTC(),
                new InMemoryTranscriptRepository());
    }

    /** Creates a coordinator with explicit admission and review policies. */
    public InProcessDelegationCoordinator(AdmissionPolicy admissionPolicy,
                                          CandidateReviewer reviewer) {
        this(admissionPolicy, reviewer, Clock.systemUTC(),
                new InMemoryTranscriptRepository());
    }

    /** Creates a coordinator with an injectable clock for deterministic lease tests. */
    public InProcessDelegationCoordinator(AdmissionPolicy admissionPolicy,
                                          CandidateReviewer reviewer, Clock clock) {
        this(admissionPolicy, reviewer, clock, new InMemoryTranscriptRepository());
    }

    /**
     * Creates a coordinator with an explicit durable transcript repository. Any transcript
     * already present is validated and restored into the live in-memory projection.
     */
    public InProcessDelegationCoordinator(AdmissionPolicy admissionPolicy,
                                          CandidateReviewer reviewer, Clock clock,
                                          TranscriptRepository transcripts) {
        this(admissionPolicy, reviewer, clock, transcripts,
                java.time.Duration.ofSeconds(300), true, true);
    }

    public InProcessDelegationCoordinator(AdmissionPolicy admissionPolicy,
            CandidateReviewer reviewer, Clock clock, TranscriptRepository transcripts,
            java.time.Duration reviewWindow) {
        this(admissionPolicy, reviewer, clock, transcripts, reviewWindow, true, true);
    }

    // Allows tests to hold the expiry timer idle while advancing the injected clock.
    InProcessDelegationCoordinator(AdmissionPolicy admissionPolicy,
                                   CandidateReviewer reviewer, Clock clock,
                                   TranscriptRepository transcripts,
                                   boolean scheduleLeaseExpiries) {
        this(admissionPolicy, reviewer, clock, transcripts,
                java.time.Duration.ofSeconds(300), scheduleLeaseExpiries, true);
    }

    InProcessDelegationCoordinator(AdmissionPolicy admissionPolicy,
            CandidateReviewer reviewer, Clock clock, TranscriptRepository transcripts,
            java.time.Duration reviewWindow, boolean scheduleLeaseExpiries,
            boolean scheduleReviewDeadlines) {
        this.admissionPolicy = Objects.requireNonNull(admissionPolicy, "admissionPolicy");
        this.reviewer = Objects.requireNonNull(reviewer, "reviewer");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.transcripts = Objects.requireNonNull(transcripts, "transcripts");
        this.scheduleLeaseExpiries = scheduleLeaseExpiries;
        this.scheduleReviewDeadlines = scheduleReviewDeadlines;
        this.reviewWindow = Objects.requireNonNull(reviewWindow, "reviewWindow");
        if (reviewWindow.getNano() != 0 || reviewWindow.getSeconds() < 1
                || reviewWindow.getSeconds() > 3600) {
            throw new IllegalArgumentException("review window must be whole seconds in 1..3600");
        }
        this.reviewRuntime = new CoordinatorReviewRuntime(lock, projection, reviewer, clock,
                reviewWindow, scheduleReviewDeadlines, runtimeTasks, reducer, frames);
        this.leaseSchedule = new CoordinatorLeaseSchedule(lock, projection, clock,
                scheduleLeaseExpiries, runtimeTasks, frames);
        synchronized (lock) {
            transcripts.load().ifPresent(transcript ->
                    DelegationReplay.restore(reducer, projection, transcript));
            reviewRuntime.recordInterruptedReviews();
        }
    }

    @Override
    public StreamObserver<DelegateRequest> delegate(
            StreamObserver<DelegateResponse> responseObserver) {
        return delegateInternal(responseObserver, false);
    }

    /** Opens a bridge-owned worker only if no connected session owns its identity. */
    StreamObserver<DelegateRequest> delegateIfNoConnectedSession(
            StreamObserver<DelegateResponse> responseObserver) {
        return delegateInternal(responseObserver, true);
    }

    /** Exact live observer identity, not merely a matching worker hello. */
    boolean ownsCurrentSession(String workerId, StreamObserver<DelegateResponse> observer) {
        synchronized (lock) {
            projection.requireOpen();
            Session current = projection.sessions.get(workerId);
            return current != null && current.connected && current.admitted
                    && current.responses == observer;
        }
    }

    private StreamObserver<DelegateRequest> delegateInternal(
            StreamObserver<DelegateResponse> responseObserver, boolean conditionalOpen) {
        Objects.requireNonNull(responseObserver, "responseObserver");
        return new StreamObserver<>() {
            private Session session;
            private boolean ended;

            @Override
            public void onNext(DelegateRequest frame) {
                if (ended) {
                    return;
                }
                try {
                    CoordinatorReviewRuntime.ReviewDispatch dispatch = null;
                    synchronized (lock) {
                        projection.requireOpen();
                        DelegationValidation.validate(frame);
                        if (session == null) {
                            if (!frame.hasHello()) {
                                throw new IllegalArgumentException(
                                        "the first worker frame must be hello");
                            }
                            session = openSession(frame.getHello(), responseObserver, conditionalOpen);
                        } else if (frame.hasHello()) {
                            throw new IllegalArgumentException(
                                    "hello may only be the first frame on a stream");
                        }
                        // A later hello can replace this stream's session. Reject an
                        // old sender before recording even a valid next-sequence frame;
                        // closing the old stream must not disconnect its replacement.
                        if (!frame.hasHello() && (projection.sessions.get(session.workerId) != session
                                || !session.connected || !session.admitted)) {
                            throw new IllegalArgumentException(
                                    "worker stream is superseded or not admitted");
                        }
                        if (recordWorkerFrame(session.workerId, frame)) {
                            dispatch = handleWorkerFrame(session, frame);
                        }
                    }
                    if (dispatch != null) reviewRuntime.dispatchReview(dispatch);
                } catch (WorkerRegistrationConflictException e) {
                    ended = true;
                    responseObserver.onError(Status.ALREADY_EXISTS
                            .withDescription("worker already has a connected session").asRuntimeException());
                } catch (IllegalArgumentException e) {
                    ended = true;
                    markDisconnected(session);
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription(e.getMessage()).asRuntimeException());
                } catch (RuntimeException e) {
                    ended = true;
                    markDisconnected(session);
                    String cause = e.getClass().getSimpleName();
                    if (e.getMessage() != null) {
                        cause += ": " + e.getMessage();
                    }
                    LOG.log(System.Logger.Level.WARNING,
                            session == null
                                    ? "worker stream failed before a session opened: " + cause
                                    : "worker stream failed for worker " + session.workerId
                                            + ": " + cause,
                            e);
                    responseObserver.onError(Status.INTERNAL
                            .withDescription("delegation coordinator failed: " + cause)
                            .withCause(e).asRuntimeException());
                }
            }

            @Override
            public void onError(Throwable throwable) {
                ended = true;
                synchronized (lock) {
                    if (session != null) {
                        session.connected = false;
                    }
                }
            }

            @Override
            public void onCompleted() {
                ended = true;
                synchronized (lock) {
                    if (session != null) {
                        session.connected = false;
                    }
                }
                responseObserver.onCompleted();
            }
        };
    }

    /**
     * Offers a new task attempt to an admitted worker.
     *
     * @return the emitted offer
     */
    public TaskOffer offer(String workerId, String taskId, TaskSpec spec,
                           java.time.Duration leaseDuration) {
        return offer(workerId, taskId, spec, leaseDuration, null);
    }

    /**
     * Offers a new task attempt with an optional checkpoint from a prior attempt.
     *
     * @return the emitted offer
     */
    public TaskOffer offer(String workerId, String taskId, TaskSpec spec,
                           java.time.Duration leaseDuration,
                           CheckpointReference resumeFrom) {
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(leaseDuration, "leaseDuration");
        if (leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
        // A contract is linked here rather than at the first candidate: an offer naming a
        // type its own descriptor set does not declare is refused while the caller is still
        // holding it, and the schema every worker reads is rendered onto the offered spec.
        TaskSpec offeredSpec = DeliverableContracts.rendered(spec);
        synchronized (lock) {
            projection.requireOpen();
            Session session = projection.requireAdmittedSession(workerId);
            TaskRuntime existing = projection.tasks.get(taskId);
            if (existing != null && !existing.attemptTerminal()) {
                throw new IllegalStateException("task already has an open attempt");
            }
            int attempt = existing == null ? 1 : existing.attempt + 1;
            return commitOffer(session, workerId, taskId, existing, attempt,
                    offeredSpec, leaseDuration, resumeFrom, "", null);
        }
    }

    /**
     * Creates exactly one first offer for a caller-persisted task UUID. An exact retry
     * reads the original committed offer under the publication lock; it never calls
     * {@code newOffer}, checks current admission, renews the lease or publishes a frame.
     * The predicate must be pure and verify the domain-separated request/spec/lease
     * binding. The supplier must be read-only; both run while publication is locked.
     */
    public TaskOffer offerOnce(String workerId, String taskId,
            Predicate<TaskOffer> matchesCommitted, Supplier<InitialOffer> newOffer) {
        if (workerId == null || workerId.length() > 128 || !Formats.isSlug(workerId)) {
            throw new IllegalArgumentException("workerId must be a lowercase slug");
        }
        if (taskId == null || !Formats.isUuid(taskId) || !isLowercaseUuidAlphabet(taskId)) {
            throw new IllegalArgumentException("taskId must be a canonical lowercase UUID");
        }
        Objects.requireNonNull(matchesCommitted, "matchesCommitted");
        Objects.requireNonNull(newOffer, "newOffer");
        synchronized (lock) {
            projection.requireOpen();
            TranscriptEntry firstOffer = null;
            for (TranscriptEntry entry : projection.entries) {
                if (entry.hasCoordinatorFrame()
                        && entry.getCoordinatorFrame().getTaskId().equals(taskId)
                        && entry.getCoordinatorFrame().hasOffer()) {
                    firstOffer = entry;
                    break;
                }
            }
            if (firstOffer != null) {
                TaskOffer committed = firstOffer.getCoordinatorFrame().getOffer();
                if (!firstOffer.getWorkerId().equals(workerId) || committed.getAttempt() != 1
                        || !Formats.isSha256Hex(committed.getStartBindingSha256())
                        || !matchesCommitted.test(committed)) {
                    throw new TaskStartConflictException("task UUID already has a different first offer");
                }
                return committed;
            }
            // A task with no first offer is not an unused UUID. In particular, do
            // not adopt a generic or otherwise malformed historical task record.
            if (projection.tasks.containsKey(taskId) || projection.entries.stream().anyMatch(entry ->
                    (entry.hasCoordinatorFrame() && entry.getCoordinatorFrame().getTaskId().equals(taskId))
                    || (entry.hasWorkerFrame() && entry.getWorkerFrame().getTaskId().equals(taskId)))) {
                throw new TaskStartConflictException("task UUID is already in use");
            }
            Session session;
            try {
                session = projection.requireAdmittedSession(workerId);
            } catch (IllegalStateException unavailable) {
                throw new TaskStartAdmissionException("worker is not currently admitted and connected", unavailable);
            }
            InitialOffer initial = Objects.requireNonNull(newOffer.get(), "newOffer returned null");
            if (initial.leaseDuration().isZero() || initial.leaseDuration().isNegative()) {
                throw new IllegalArgumentException("leaseDuration must be positive");
            }
            if (!Formats.isSha256Hex(initial.startBindingSha256())) {
                throw new IllegalArgumentException("start binding must be a lowercase SHA-256 digest");
            }
            TaskSpec rendered = DeliverableContracts.rendered(initial.spec());
            DelegationValidation.validate(rendered);
            return commitOffer(session, workerId, taskId, null, 1, rendered,
                    initial.leaseDuration(), null, initial.startBindingSha256(), matchesCommitted);
        }
    }

    private TaskOffer commitOffer(Session session, String workerId, String taskId,
            TaskRuntime existing, int attempt, TaskSpec spec, java.time.Duration leaseDuration,
            CheckpointReference resumeFrom, String startBindingSha256,
            Predicate<TaskOffer> matchesStartBinding) {
        Instant expiry = clock.instant().plus(leaseDuration);
        TaskOffer.Builder offerBuilder = TaskOffer.newBuilder()
                .setAttempt(attempt)
                .setSpec(spec)
                .setLeaseDuration(DelegationTimes.toProtoDuration(leaseDuration))
                .setExpiresAt(DelegationTimes.toTimestamp(expiry));
        if (resumeFrom != null) offerBuilder.setResumeFrom(resumeFrom);
        if (!startBindingSha256.isEmpty()) offerBuilder.setStartBindingSha256(startBindingSha256);
        TaskOffer offer = offerBuilder.build();
        DelegationValidation.validate(offer);
        if (matchesStartBinding != null && !matchesStartBinding.test(offer)) {
            throw new IllegalArgumentException("new offer does not match its start binding");
        }
        TaskRuntime task = existing == null ? new TaskRuntime(taskId) : existing;
        emit(session, taskId, attempt,
                DelegateResponse.newBuilder().setOffer(offer), () -> {
                    task.workerId = workerId;
                    task.attempt = attempt;
                    task.offer = offer;
                    task.phase = DelegationReducer.Phase.OFFERED;
                    task.expiry = expiry;
                    task.leaseGeneration++;
                    projection.tasks.put(taskId, task);
                });
        leaseSchedule.scheduleExpiry(taskId, attempt, task.leaseGeneration, expiry);
        return offer;
    }

    /** Applies an external review decision to its identified completion candidate. */
    public void review(String taskId, int attempt, int revision, ReviewDecision decision) {
        if (attempt < 1 || revision < 1) {
            throw new IllegalArgumentException("review attempt and revision must be positive");
        }
        synchronized (lock) {
            projection.requireOpen();
            TaskRuntime task = projection.requireTask(taskId);
            if (task.phase != DelegationReducer.Phase.CANDIDATE) {
                throw new IllegalStateException("task has no candidate under review");
            }
            if (task.attempt != attempt || task.candidate.getRevision() != revision) {
                throw new IllegalStateException("review does not match the open candidate");
            }
            reviewRuntime.settleExpiredReview(task);
            String invocation = reviewRuntime.currentReview(task).status()
                    == DelegationReducer.ReviewStatus.RUNNING
                    ? reviewRuntime.currentReview(task).started().getIdentity().getInvocationId() : "";
            applyReview(task, Objects.requireNonNull(decision, "decision"), invocation);
        }
    }

    /** Retries only the latest failed invocation of the current candidate. */
    public RetryCandidateReviewResponse retryCandidateReview(RetryCandidateReviewRequest request) {
        requireNative(request, "invalid review retry request");
        CoordinatorReviewRuntime.ReviewDispatch dispatch;
        RetryCandidateReviewResponse response;
        synchronized (lock) {
            projection.requireOpen();
            for (TranscriptEntry entry : projection.entries) {
                if (!entry.hasCoordinatorFrame()
                        || !entry.getCoordinatorFrame().hasReviewStarted()) continue;
                ReviewStarted existing = entry.getCoordinatorFrame().getReviewStarted();
                if (!existing.getRetryId().equals(request.getRetryId())) continue;
                var identity = existing.getIdentity();
                if (!identity.getTaskId().equals(request.getTaskId())
                        || identity.getAttempt() != request.getAttempt()
                        || identity.getRevision() != request.getRevision()
                        || !existing.getPreviousInvocationId().equals(
                                request.getExpectedInvocationId())) {
                    throw new IllegalArgumentException("review retry id was used for another request");
                }
                return retryResponse(request, identity);
            }
            TaskRuntime task = projection.requireTask(request.getTaskId());
            if (task.phase == DelegationReducer.Phase.CANDIDATE) reviewRuntime.settleExpiredReview(task);
            DelegationReducer.ReviewState review = reviewRuntime.currentReview(task);
            if (task.phase != DelegationReducer.Phase.CANDIDATE
                    || task.attempt != request.getAttempt()
                    || task.candidate.getRevision() != request.getRevision()
                    || review.status() != DelegationReducer.ReviewStatus.FAILED
                    || !review.started().getIdentity().getInvocationId().equals(
                            request.getExpectedInvocationId())) {
                throw new IllegalArgumentException("review retry does not match the latest failed candidate");
            }
            var previous = review.started().getIdentity();
            var identity = DelegationReviewBindings.identity(reviewRuntime.selectedOfferEntry(task),
                    reviewRuntime.selectedCandidateEntry(task), UUID.randomUUID().toString());
            if (!identity.toBuilder().setInvocationId(previous.getInvocationId()).build()
                    .equals(previous)) {
                throw new IllegalStateException("current review identity is inconsistent");
            }
            ReviewStarted started = reviewRuntime.newReviewStart(identity,
                    request.getExpectedInvocationId(), request.getRetryId());
            Session session = projection.requireAdmittedWorker(task.workerId);
            long seq = session.peekNextCoordinatorSeq(task.taskId, task.attempt);
            DelegateResponse frame = reviewRuntime.reviewStartFrame(task, seq, started);
            append(TranscriptEntry.newBuilder().setWorkerId(task.workerId)
                    .setLane(Lane.LANE_COORDINATOR).setCoordinatorFrame(frame).build());
            session.commitCoordinatorSeq(task.taskId, task.attempt, seq);
            deliver(session, frame);
            response = retryResponse(request, identity);
            dispatch = new CoordinatorReviewRuntime.ReviewDispatch(task.taskId, started);
        }
        reviewRuntime.dispatchReview(dispatch);
        return response;
    }

    private static RetryCandidateReviewResponse retryResponse(RetryCandidateReviewRequest request,
            ai.protomolt.proto.delegation.v1.CandidateReviewIdentity identity) {
        RetryCandidateReviewResponse response = RetryCandidateReviewResponse.newBuilder()
                .setRequest(request).setIdentity(identity).build();
        requireNative(response, "invalid review retry response");
        return response;
    }

    /**
     * Sends a non-transitioning task message from the coordinator to one worker.
     * The message is recorded and sequenced like any other frame but never moves
     * the lifecycle; the task must exist and the worker must be admitted. A
     * disconnected worker reads it from the event feed when it reconnects.
     *
     * @return the emitted message, with its generated id and timestamp
     */
    public TaskMessage sendMessage(String workerId, String taskId, TaskMessageKind kind,
                                   String text, String replyTo,
                                   List<ArtifactReference> artifacts) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(text, "text");
        Objects.requireNonNull(artifacts, "artifacts");
        synchronized (lock) {
            projection.requireOpen();
            Session session = projection.requireAdmittedWorker(workerId);
            projection.requireTask(taskId);
            TaskMessage message = TaskMessage.newBuilder()
                    .setMessageId(UUID.randomUUID().toString())
                    .setSender(DelegationValidation.COORDINATOR)
                    .setRecipient(workerId)
                    .setTaskId(taskId)
                    .setKind(kind)
                    .setReplyTo(replyTo == null ? "" : replyTo)
                    .setText(text)
                    .addAllArtifacts(artifacts)
                    .setSentAt(DelegationTimes.toTimestamp(clock.instant()))
                    .build();
            // Messages sequence in the task's attempt-0 scope on both lanes.
            emit(session, taskId, 0, DelegateResponse.newBuilder().setTaskMessage(message));
            return message;
        }
    }

    /** A snapshot of one worker stream's registration state. */
    public record WorkerView(String workerId, boolean admitted, boolean connected,
                             WorkerHello hello) {
        public WorkerView {
            Objects.requireNonNull(workerId, "workerId");
            Objects.requireNonNull(hello, "hello");
        }
    }

    /**
     * One (task, attempt) sequence scope of a worker stream. Session frames
     * (hello, admission) use the empty task id and attempt 0; task messages
     * sequence in their task's attempt-0 scope.
     */
    public record Scope(String taskId, int attempt) {
        public Scope {
            Objects.requireNonNull(taskId, "taskId");
        }
    }

    /**
     * One worker's sender-side high-water marks, rebuilt from the recorded
     * transcript: the last recorded frame sequence of each worker-lane scope, and
     * the last recorded progress and checkpoint sequence of each task scope. A
     * replacement stream for the same worker id seeds its counters from these
     * values so it continues the scopes instead of rewinding them; a worker the
     * transcript has never seen gets empty maps and starts every scope at 1.
     */
    public record WorkerResumption(Map<Scope, Long> sequences,
                                   Map<Scope, Integer> progressSequences,
                                   Map<Scope, Integer> checkpointSequences) {
        public WorkerResumption {
            sequences = Map.copyOf(Objects.requireNonNull(sequences, "sequences"));
            progressSequences = Map.copyOf(
                    Objects.requireNonNull(progressSequences, "progressSequences"));
            checkpointSequences = Map.copyOf(
                    Objects.requireNonNull(checkpointSequences, "checkpointSequences"));
        }

        /** The last recorded sequence of one scope, 0 when the scope has no frames. */
        public long lastSequence(Scope scope) {
            return sequences.getOrDefault(scope, 0L);
        }

        /** The last recorded progress sequence of one scope, 0 when none. */
        public int lastProgressSequence(Scope scope) {
            return progressSequences.getOrDefault(scope, 0);
        }

        /** The last recorded checkpoint sequence of one scope, 0 when none. */
        public int lastCheckpointSequence(Scope scope) {
            return checkpointSequences.getOrDefault(scope, 0);
        }
    }

    /**
     * Rebuilds one worker's sender-side sequence state from the recorded
     * transcript. The transcript is the source of truth: a frame the coordinator
     * never accepted is not in it, so a replacement stream seeded from this
     * resumption can neither replay a recorded frame nor skip one. Seeding a
     * replacement stream from the resumption is what lets a same-worker
     * re-registration after a coordinator restart or a stream failure continue
     * the transcript's sequence scopes instead of rewinding them.
     */
    public WorkerResumption workerResumption(String workerId) {
        Objects.requireNonNull(workerId, "workerId");
        synchronized (lock) {
            projection.requireOpen();
            return DelegationReplay.workerResumption(projection.entries, workerId);
        }
    }

    /** Returns the current worker registrations, in first-seen order. */
    public List<WorkerView> workers() {
        synchronized (lock) {
            projection.requireOpen();
            return projection.sessions.values().stream()
                    .map(session -> new WorkerView(session.workerId, session.admitted,
                            session.connected, session.hello))
                    .toList();
        }
    }

    /** Cancels the current offer or lease. */
    public void cancel(String taskId, String reason) {
        synchronized (lock) {
            projection.requireOpen();
            TaskRuntime task = projection.requireTask(taskId);
            if (task.phase != DelegationReducer.Phase.OFFERED
                    && task.phase != DelegationReducer.Phase.LEASED
                    && task.phase != DelegationReducer.Phase.CANDIDATE) {
                throw new IllegalStateException("task has no open attempt to cancel");
            }
            Cancellation cancellation = Cancellation.newBuilder()
                    .setAttempt(task.attempt)
                    .setReason(reason)
                    .build();
            emit(projection.requireAdmittedWorker(task.workerId), task.taskId, task.attempt,
                    DelegateResponse.newBuilder().setCancellation(cancellation), () -> {
                        task.phase = DelegationReducer.Phase.CANCELLED;
                        task.leaseGeneration++;
                    });
        }
    }

    /**
     * Expires every live lease whose declared expiry is at or before {@code now}. The
     * lease clock covers the worker's obligation only: a submitted candidate waits for
     * its review without a deadline, so a task in the candidate phase never expires
     * here, and a revision request re-arms the lease when it hands the task back.
     */
    public int expireLeases(Instant now) {
        Objects.requireNonNull(now, "now");
        synchronized (lock) {
            projection.requireOpen();
            return leaseSchedule.expireLeases(now);
        }
    }

    /**
     * The deliverable types the offered tasks declare, as a proto3 JSON type registry.
     *
     * <p>A deliverable's type is declared by the offer, not by this process's build, so a
     * caller sending or reading one as JSON needs the types the live tasks name and nothing
     * else can supply them. A contract that does not link contributes no type: every
     * candidate against it is refused by the reducer anyway.
     *
     * @return a registry over every distinct deliverable type the coordinator's tasks declare
     */
    public JsonFormat.TypeRegistry deliverableTypes() {
        List<Descriptor> types = new ArrayList<>();
        Set<String> named = new HashSet<>();
        synchronized (lock) {
            projection.requireOpen();
            for (TaskRuntime task : projection.tasks.values()) {
                if (task.offer == null || !task.offer.getSpec().hasContract()) {
                    continue;
                }
                try {
                    Descriptor type = DeliverableContracts
                            .compile(task.offer.getSpec().getContract()).descriptor();
                    if (named.add(type.getFullName())) {
                        types.add(type);
                    }
                } catch (IllegalArgumentException e) {
                    // A contract that does not link names no resolvable type.
                }
            }
        }
        return JsonFormat.TypeRegistry.newBuilder().add(types).build();
    }

    /** The current offer's contract for one task, if its offer declared one. */
    public Optional<ai.protomolt.proto.delegation.v1.DeliverableContract> deliverableContract(
            String taskId) {
        synchronized (lock) {
            projection.requireOpen();
            TaskRuntime task = projection.tasks.get(taskId);
            if (task == null || task.offer == null || !task.offer.getSpec().hasContract()) {
                return Optional.empty();
            }
            return Optional.of(task.offer.getSpec().getContract());
        }
    }

    /** Returns the current replayable transcript. */
    public Transcript transcript() {
        synchronized (lock) {
            projection.requireOpen();
            return Transcript.newBuilder().addAllEntries(projection.entries).build();
        }
    }

    /** Returns all events after a cursor, optionally restricted to one task. */
    public List<Event> eventsAfter(String taskId, long afterCursor) {
        synchronized (lock) {
            projection.requireOpen();
            return projection.events.stream()
                    .filter(event -> event.cursor > afterCursor)
                    .filter(event -> taskId == null || taskId.isEmpty()
                            || event.taskId().equals(taskId))
                    .toList();
        }
    }

    /**
     * Blocks until a matching event appears after {@code afterCursor} or the timeout
     * elapses. Blocking is safe on a virtual thread and maps directly to an MCP
     * long-poll tool.
     */
    public Optional<Event> waitForEvent(String taskId, long afterCursor,
                                        java.time.Duration timeout)
            throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }
        long remaining = timeout.toNanos();
        long deadline = System.nanoTime() + remaining;
        synchronized (lock) {
            while (true) {
                projection.requireOpen();
                Optional<Event> event = firstEvent(taskId, afterCursor);
                if (event.isPresent() || remaining <= 0) {
                    return event;
                }
                long millis = remaining / 1_000_000L;
                int nanos = (int) (remaining % 1_000_000L);
                lock.wait(millis, nanos);
                remaining = deadline - System.nanoTime();
            }
        }
    }

    /** Reduces the current transcript for diagnostics or persistence gates. */
    public DelegationReducer.Result state() {
        synchronized (lock) {
            projection.requireOpen();
            return reducer.reduce(
                    Transcript.newBuilder().addAllEntries(projection.entries).build());
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (projection.closed) {
                return;
            }
            projection.closed = true;
            projection.sessions.values().stream().filter(session -> session.connected)
                    .forEach(session -> session.responses.onCompleted());
            projection.sessions.values().forEach(session -> session.connected = false);
            lock.notifyAll();
        }
        runtimeTasks.shutdownNow();
    }

    private Session openSession(WorkerHello hello,
                                StreamObserver<DelegateResponse> responses,
                                boolean conditionalOpen) {
        Session current = projection.sessions.get(hello.getWorkerId());
        if (conditionalOpen && current != null && current.connected) {
            throw new WorkerRegistrationConflictException();
        }
        AdmissionPolicy.Decision decision = admissionPolicy.admit(hello);
        Session session = new Session(hello, responses);
        session.admitted = decision.admitted();
        Session previous = projection.sessions.put(hello.getWorkerId(), session);
        if (previous != null) {
            previous.connected = false;
        }
        AdmissionDecision.Builder admission = AdmissionDecision.newBuilder()
                .setAdmitted(decision.admitted())
                .setReason(decision.reason());
        if (decision.admitted()) {
            admission.setSessionId(UUID.randomUUID().toString());
        }
        DelegationReplay.restoreCoordinatorSequences(projection, session);
        // The hello is recorded by the caller before task handling. Admission must
        // therefore be emitted after recordWorkerFrame returns. A pending response is
        // held on the session for that ordering.
        session.pendingAdmission = admission.build();
        return session;
    }

    private boolean recordWorkerFrame(String workerId, DelegateRequest frame) {
        ByteString bytes = frame.toByteString();
        ByteString prior = projection.workerFrames.get(frame.getFrameId());
        if (prior != null) {
            if (!prior.equals(bytes)) {
                throw new IllegalArgumentException(
                        "frame id was already used with a different payload");
            }
            return false;
        }
        // The timer can wake late. Check a new acceptance or candidate against the
        // current clock while holding the same lock that publishes the frame.
        // A committed identical retry returned above must remain replayable.
        TaskRuntime task = projection.tasks.get(frame.getTaskId());
        if (task != null && !task.expiry.isAfter(clock.instant())
                && ((frame.hasAccept() && task.phase == DelegationReducer.Phase.OFFERED
                        && frame.getAccept().getAttempt() == task.attempt)
                    || (frame.hasCompletion() && task.phase == DelegationReducer.Phase.LEASED
                        && frame.getCompletion().getAttempt() == task.attempt))) {
            throw new IllegalArgumentException("task lease has expired");
        }
        TranscriptEntry workerEntry = TranscriptEntry.newBuilder()
                .setWorkerId(workerId)
                .setLane(Lane.LANE_WORKER)
                .setWorkerFrame(frame)
                .build();
        if (frame.hasCompletion()) {
            if (task == null) throw new IllegalArgumentException("completion names no task");
            Session session = projection.requireAdmittedWorker(workerId);
            TranscriptEntry offerEntry = reviewRuntime.selectedOfferEntry(task);
            var identity = DelegationReviewBindings.identity(offerEntry, workerEntry,
                    UUID.randomUUID().toString());
            ReviewStarted started = reviewRuntime.newReviewStart(identity, "", "");
            long seq = session.peekNextCoordinatorSeq(task.taskId, task.attempt);
            DelegateResponse startFrame = reviewRuntime.reviewStartFrame(task, seq, started);
            TranscriptEntry startEntry = TranscriptEntry.newBuilder()
                    .setWorkerId(workerId).setLane(Lane.LANE_COORDINATOR)
                    .setCoordinatorFrame(startFrame).build();
            appendAll(List.of(workerEntry, startEntry));
            session.commitCoordinatorSeq(task.taskId, task.attempt, seq);
            projection.workerFrames.put(frame.getFrameId(), bytes);
            task.phase = DelegationReducer.Phase.CANDIDATE;
            task.candidate = frame.getCompletion();
            deliver(session, startFrame);
            return true;
        } else {
            append(workerEntry);
        }
        projection.workerFrames.put(frame.getFrameId(), bytes);
        return true;
    }

    private CoordinatorReviewRuntime.ReviewDispatch handleWorkerFrame(Session session,
            DelegateRequest frame) {
        if (frame.hasHello()) {
            emit(session, "", 0, DelegateResponse.newBuilder()
                    .setAdmission(session.pendingAdmission));
            session.pendingAdmission = null;
            leaseSchedule.resumeRestoredLeases(session);
            return null;
        }
        if (!session.admitted) {
            throw new IllegalArgumentException("worker was not admitted");
        }
        TaskRuntime task = projection.requireTask(frame.getTaskId());
        if (!task.workerId.equals(session.workerId)) {
            throw new IllegalArgumentException("task is assigned to another worker");
        }
        switch (frame.getPayloadCase()) {
            case ACCEPT -> task.phase = DelegationReducer.Phase.LEASED;
            case REJECT -> {
                task.phase = DelegationReducer.Phase.REJECTED;
                task.leaseGeneration++;
            }
            case HEARTBEAT -> leaseSchedule.renew(task);
            case PROGRESS, CHECKPOINT -> {
                // The transcript and event feed carry the full update.
            }
            case BLOCKED -> {
                task.phase = DelegationReducer.Phase.BLOCKED;
                task.leaseGeneration++;
            }
            case FAILED -> {
                task.phase = DelegationReducer.Phase.FAILED;
                task.leaseGeneration++;
            }
            case CANCELLED -> {
                // Cancellation is already terminal when the coordinator emits it.
            }
            case TASK_MESSAGE -> {
                // Non-transitioning: the transcript and event feed carry the
                // message; the lifecycle does not move. Sender authenticity is
                // a reducer finding, so a forged frame never lands here.
            }
            case COMPLETION -> {
                return new CoordinatorReviewRuntime.ReviewDispatch(task.taskId,
                        reviewRuntime.currentReview(task).started());
            }
            default -> throw new IllegalArgumentException("unexpected worker payload");
        }
        return null;
    }

    private void applyReview(TaskRuntime task, ReviewDecision decision, String invocationId) {
        Session session = projection.requireAdmittedWorker(task.workerId);
        switch (decision) {
            case ReviewDecision.Accept(String verdict) -> {
                CompletionAccepted payload = CompletionAccepted.newBuilder()
                        .setAttempt(task.attempt)
                        .setRevision(task.candidate.getRevision())
                        .setVerdict(verdict)
                        .setReviewInvocationId(invocationId)
                        .build();
                emit(session, task.taskId, task.attempt,
                        DelegateResponse.newBuilder().setAccepted(payload), () -> {
                            task.phase = DelegationReducer.Phase.ACCEPTED;
                            task.leaseGeneration++;
                        });
            }
            case ReviewDecision.Revise(String feedback, List<String> failedChecks) -> {
                RevisionRequested payload = RevisionRequested.newBuilder()
                        .setAttempt(task.attempt)
                        .setRevision(task.candidate.getRevision())
                        .setFeedback(feedback)
                        .addAllFailedChecks(failedChecks)
                        .setReviewInvocationId(invocationId)
                        .build();
                emit(session, task.taskId, task.attempt,
                        DelegateResponse.newBuilder().setRevisionRequested(payload),
                        () -> task.phase = DelegationReducer.Phase.LEASED);
                leaseSchedule.rearmLease(task, session);
            }
            // The candidate stays open for an explicit review action; the
            // lifecycle does not move.
            case ReviewDecision.Pending _ -> {
            }
        }
    }

    private void emit(Session session, String taskId, int attempt,
                      DelegateResponse.Builder payload) {
        emit(session, taskId, attempt, payload, () -> { });
    }

    private void emit(Session session, String taskId, int attempt,
                      DelegateResponse.Builder payload, Runnable afterPersist) {
        long seq = session.peekNextCoordinatorSeq(taskId, attempt);
        DelegateResponse response = payload
                .setFrameId(UUID.randomUUID().toString())
                .setTaskId(taskId)
                .setSeq(seq)
                .setSentAt(DelegationTimes.toTimestamp(clock.instant()))
                .build();
        DelegationValidation.validate(response);
        append(TranscriptEntry.newBuilder()
                .setWorkerId(session.workerId)
                .setLane(Lane.LANE_COORDINATOR)
                .setCoordinatorFrame(response)
                .build());
        session.commitCoordinatorSeq(taskId, attempt, seq);
        afterPersist.run();
        deliver(session, response);
    }

    private void deliver(Session session, DelegateResponse response) {
        if (session.connected) {
            try {
                session.responses.onNext(response);
            } catch (RuntimeException deliveryFailure) {
                // The transcript and live projection have already committed. A broken
                // stream cannot undo the decision; the worker can replay the event feed.
                session.connected = false;
                LOG.log(System.Logger.Level.WARNING,
                        "worker response stream failed for worker " + session.workerId
                                + "; recorded frame remains available for replay");
            }
        }
        // A dead stream gets nothing; the recorded frame is on the event feed and the
        // next stream for this worker continues the coordinator sequence past it.
    }

    private void append(TranscriptEntry entry) {
        appendAll(List.of(entry));
    }

    private void appendAll(List<TranscriptEntry> batch) {
        projection.requireOpen();
        Transcript candidate = Transcript.newBuilder()
                .addAllEntries(projection.entries)
                .addAllEntries(batch)
                .build();
        DelegationReducer.Result result = reducer.reduce(
                candidate);
        if (!result.clean()) {
            DelegationReducer.Finding finding = result.findings().getFirst();
            throw new IllegalArgumentException(finding.kind() + ": " + finding.error());
        }
        try {
            transcripts.save(candidate);
        } catch (RuntimeException uncertain) {
            projection.publicationFailed = true;
            throw new IllegalStateException("transcript publication is unavailable", uncertain);
        } catch (Error uncertain) {
            projection.publicationFailed = true;
            throw uncertain;
        }
        for (TranscriptEntry entry : batch) {
            projection.record(entry);
        }
        lock.notifyAll();
    }

    private static void requireNative(Message value, String description) {
        if (value == null || !nativeValid(value)) throw new IllegalArgumentException(description);
    }

    private static boolean nativeValid(Message value) {
        if (!value.getUnknownFields().asMap().isEmpty()
                || !VALIDATOR.validate(value).valid()) return false;
        for (var field : value.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getKey().isRepeated()) {
                for (Object nested : (List<?>) field.getValue()) {
                    if (!nativeValid((Message) nested)) return false;
                }
            } else if (!nativeValid((Message) field.getValue())) return false;
        }
        return true;
    }

    private void markDisconnected(Session session) {
        if (session == null) {
            return;
        }
        synchronized (lock) {
            session.connected = false;
        }
    }

    private Optional<Event> firstEvent(String taskId, long afterCursor) {
        return projection.events.stream()
                .filter(event -> event.cursor > afterCursor)
                .filter(event -> taskId == null || taskId.isEmpty()
                        || event.taskId().equals(taskId))
                .findFirst();
    }

    /**
     * Lowercase hex or dash with the canonical dashed-UUID length. Combined with
     * {@link Formats#isUuid} (whose hex alphabet is case-insensitive) this pins the
     * canonical lowercase form; together they accept exactly the set the former
     * {@code [0-9a-f-]{36}} check accepted.
     */
    private static boolean isLowercaseUuidAlphabet(String value) {
        if (value.length() != 36) {
            return false;
        }
        for (int i = 0; i < 36; i++) {
            char c = value.charAt(i);
            if ((c < '0' || c > '9') && (c < 'a' || c > 'f') && c != '-') {
                return false;
            }
        }
        return true;
    }

    /** One cursor-addressable transcript event. */
    public record Event(long cursor, TranscriptEntry entry) {
        public Event {
            Objects.requireNonNull(entry, "entry");
        }

        /** Task id, empty for session events. */
        public String taskId() {
            return entry.getLane() == Lane.LANE_WORKER
                    ? entry.getWorkerFrame().getTaskId()
                    : entry.getCoordinatorFrame().getTaskId();
        }

        /** Worker stream that carried the event. */
        public String workerId() {
            return entry.getWorkerId();
        }
    }
}
