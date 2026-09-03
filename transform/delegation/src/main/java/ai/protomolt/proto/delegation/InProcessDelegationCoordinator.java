package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.CandidateReviewer.ReviewDecision;
import ai.protomolt.proto.delegation.v1.AdmissionDecision;
import ai.protomolt.proto.delegation.v1.AgentDelegationServiceGrpc;
import ai.protomolt.proto.delegation.v1.Cancellation;
import ai.protomolt.proto.delegation.v1.CompletionAccepted;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.CheckpointReference;
import ai.protomolt.proto.delegation.v1.DelegateRequest;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.Lane;
import ai.protomolt.proto.delegation.v1.LeaseExpired;
import ai.protomolt.proto.delegation.v1.LeaseRenewal;
import ai.protomolt.proto.delegation.v1.RevisionRequested;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.TaskMessage;
import ai.protomolt.proto.delegation.v1.TaskMessageKind;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * In-memory delegation coordinator for embedded servers, tests, and MCP long polling.
 * The wire transcript remains the source of truth and is checked by
 * {@link DelegationReducer} after every accepted frame.
 */
public final class InProcessDelegationCoordinator
        extends AgentDelegationServiceGrpc.AgentDelegationServiceImplBase
        implements AutoCloseable {

    private static final StreamObserver<DelegateResponse> DISCONNECTED_RESPONSES =
            new StreamObserver<>() {
                @Override
                public void onNext(DelegateResponse value) {
                    // Restored events remain available through the cursor-addressable feed.
                }

                @Override
                public void onError(Throwable throwable) {
                }

                @Override
                public void onCompleted() {
                }
            };

    private final Object lock = new Object();
    private final AdmissionPolicy admissionPolicy;
    private final CandidateReviewer reviewer;
    private final Clock clock;
    private final TranscriptRepository transcripts;
    private final ExecutorService runtimeTasks = Executors.newVirtualThreadPerTaskExecutor();
    private final DelegationReducer reducer = new DelegationReducer();
    private final Map<String, Session> sessions = new LinkedHashMap<>();
    private final Map<String, TaskRuntime> tasks = new LinkedHashMap<>();
    private final Map<String, ByteString> workerFrames = new HashMap<>();
    private final List<TranscriptEntry> entries = new ArrayList<>();
    private final List<Event> events = new ArrayList<>();
    private long cursor;
    private boolean closed;

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
        this.admissionPolicy = Objects.requireNonNull(admissionPolicy, "admissionPolicy");
        this.reviewer = Objects.requireNonNull(reviewer, "reviewer");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.transcripts = Objects.requireNonNull(transcripts, "transcripts");
        transcripts.load().ifPresent(this::restore);
    }

    @Override
    public StreamObserver<DelegateRequest> delegate(
            StreamObserver<DelegateResponse> responseObserver) {
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
                    synchronized (lock) {
                        requireOpen();
                        DelegationValidation.validate(frame);
                        if (session == null) {
                            if (!frame.hasHello()) {
                                throw new IllegalArgumentException(
                                        "the first worker frame must be hello");
                            }
                            session = openSession(frame.getHello(), responseObserver);
                        } else if (frame.hasHello()) {
                            throw new IllegalArgumentException(
                                    "hello may only be the first frame on a stream");
                        }
                        if (recordWorkerFrame(session.workerId, frame)) {
                            handleWorkerFrame(session, frame);
                        }
                    }
                } catch (IllegalArgumentException e) {
                    ended = true;
                    markDisconnected(session);
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription(e.getMessage()).asRuntimeException());
                } catch (RuntimeException e) {
                    ended = true;
                    markDisconnected(session);
                    responseObserver.onError(Status.INTERNAL
                            .withDescription("delegation coordinator failed")
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
        synchronized (lock) {
            requireOpen();
            Session session = requireAdmittedSession(workerId);
            TaskRuntime existing = tasks.get(taskId);
            if (existing != null && !existing.attemptTerminal()) {
                throw new IllegalStateException("task already has an open attempt");
            }
            int attempt = existing == null ? 1 : existing.attempt + 1;
            Instant expiry = clock.instant().plus(leaseDuration);
            TaskOffer.Builder offerBuilder = TaskOffer.newBuilder()
                    .setAttempt(attempt)
                    .setSpec(spec)
                    .setLeaseDuration(toProtoDuration(leaseDuration))
                    .setExpiresAt(toTimestamp(expiry));
            if (resumeFrom != null) {
                offerBuilder.setResumeFrom(resumeFrom);
            }
            TaskOffer offer = offerBuilder.build();
            DelegationValidation.validate(offer);
            TaskRuntime task = existing == null ? new TaskRuntime(taskId) : existing;
            emit(session, taskId, attempt,
                    DelegateResponse.newBuilder().setOffer(offer), () -> {
                        task.workerId = workerId;
                        task.attempt = attempt;
                        task.offer = offer;
                        task.phase = DelegationReducer.Phase.OFFERED;
                        task.expiry = expiry;
                        task.leaseGeneration++;
                        tasks.put(taskId, task);
                    });
            scheduleExpiry(taskId, attempt, task.leaseGeneration, expiry);
            return offer;
        }
    }

    /** Applies an external review decision to the current completion candidate. */
    public void review(String taskId, ReviewDecision decision) {
        synchronized (lock) {
            TaskRuntime task = requireTask(taskId);
            if (task.phase != DelegationReducer.Phase.CANDIDATE) {
                throw new IllegalStateException("task has no candidate under review");
            }
            applyReview(task, Objects.requireNonNull(decision, "decision"));
        }
    }

    /**
     * Sends a non-transitioning task message from the coordinator to one worker.
     * The message is recorded and sequenced like any other frame but never moves
     * the lifecycle; the task must exist and the worker must be admitted and
     * connected.
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
            requireOpen();
            Session session = requireAdmittedSession(workerId);
            requireTask(taskId);
            TaskMessage message = TaskMessage.newBuilder()
                    .setMessageId(UUID.randomUUID().toString())
                    .setSender(DelegationValidation.COORDINATOR)
                    .setRecipient(workerId)
                    .setTaskId(taskId)
                    .setKind(kind)
                    .setReplyTo(replyTo == null ? "" : replyTo)
                    .setText(text)
                    .addAllArtifacts(artifacts)
                    .setSentAt(nowTimestamp())
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
            Map<Scope, Long> sequences = new HashMap<>();
            Map<Scope, Integer> progress = new HashMap<>();
            Map<Scope, Integer> checkpoints = new HashMap<>();
            for (TranscriptEntry entry : entries) {
                if (entry.getLane() != Lane.LANE_WORKER
                        || !entry.getWorkerId().equals(workerId)) {
                    continue;
                }
                DelegateRequest frame = entry.getWorkerFrame();
                Scope scope = new Scope(frame.getTaskId(), workerAttemptOf(frame));
                sequences.merge(scope, frame.getSeq(), Math::max);
                if (frame.hasProgress()) {
                    progress.merge(scope, frame.getProgress().getProgressSeq(), Math::max);
                }
                if (frame.hasCheckpoint()) {
                    checkpoints.merge(scope, frame.getCheckpoint().getCheckpointSeq(),
                            Math::max);
                }
            }
            return new WorkerResumption(sequences, progress, checkpoints);
        }
    }

    /** Returns the current worker registrations, in first-seen order. */
    public List<WorkerView> workers() {
        synchronized (lock) {
            return sessions.values().stream()
                    .map(session -> new WorkerView(session.workerId, session.admitted,
                            session.connected, session.hello))
                    .toList();
        }
    }

    /** Cancels the current offer or lease. */
    public void cancel(String taskId, String reason) {
        synchronized (lock) {
            TaskRuntime task = requireTask(taskId);
            if (task.phase != DelegationReducer.Phase.OFFERED
                    && task.phase != DelegationReducer.Phase.LEASED
                    && task.phase != DelegationReducer.Phase.CANDIDATE) {
                throw new IllegalStateException("task has no open attempt to cancel");
            }
            Cancellation cancellation = Cancellation.newBuilder()
                    .setAttempt(task.attempt)
                    .setReason(reason)
                    .build();
            emit(requireAdmittedSession(task.workerId), task.taskId, task.attempt,
                    DelegateResponse.newBuilder().setCancellation(cancellation));
            task.phase = DelegationReducer.Phase.CANCELLED;
            task.leaseGeneration++;
        }
    }

    /** Expires every live lease whose declared expiry is at or before {@code now}. */
    public int expireLeases(Instant now) {
        Objects.requireNonNull(now, "now");
        synchronized (lock) {
            int expired = 0;
            for (TaskRuntime task : tasks.values()) {
                if ((task.phase == DelegationReducer.Phase.LEASED
                        || task.phase == DelegationReducer.Phase.CANDIDATE)
                        && !task.expiry.isAfter(now)) {
                    expire(task, "lease deadline elapsed");
                    expired++;
                }
            }
            return expired;
        }
    }

    /** Returns the current replayable transcript. */
    public Transcript transcript() {
        synchronized (lock) {
            return Transcript.newBuilder().addAllEntries(entries).build();
        }
    }

    /** Returns all events after a cursor, optionally restricted to one task. */
    public List<Event> eventsAfter(String taskId, long afterCursor) {
        synchronized (lock) {
            return events.stream()
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
                Optional<Event> event = firstEvent(taskId, afterCursor);
                if (event.isPresent() || remaining <= 0 || closed) {
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
            return reducer.reduce(Transcript.newBuilder().addAllEntries(entries).build());
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            sessions.values().stream().filter(session -> session.connected)
                    .forEach(session -> session.responses.onCompleted());
            sessions.values().forEach(session -> session.connected = false);
            lock.notifyAll();
        }
        runtimeTasks.shutdownNow();
    }

    private Session openSession(WorkerHello hello,
                                StreamObserver<DelegateResponse> responses) {
        AdmissionPolicy.Decision decision = admissionPolicy.admit(hello);
        Session session = new Session(hello, responses);
        session.admitted = decision.admitted();
        Session previous = sessions.put(hello.getWorkerId(), session);
        if (previous != null) {
            previous.connected = false;
        }
        AdmissionDecision.Builder admission = AdmissionDecision.newBuilder()
                .setAdmitted(decision.admitted())
                .setReason(decision.reason());
        if (decision.admitted()) {
            admission.setSessionId(UUID.randomUUID().toString());
        }
        restoreCoordinatorSequences(session);
        // The hello is recorded by the caller before task handling. Admission must
        // therefore be emitted after recordWorkerFrame returns. A pending response is
        // held on the session for that ordering.
        session.pendingAdmission = admission.build();
        return session;
    }

    private boolean recordWorkerFrame(String workerId, DelegateRequest frame) {
        ByteString bytes = frame.toByteString();
        ByteString prior = workerFrames.get(frame.getFrameId());
        if (prior != null) {
            if (!prior.equals(bytes)) {
                throw new IllegalArgumentException(
                        "frame id was already used with a different payload");
            }
            return false;
        }
        append(TranscriptEntry.newBuilder()
                .setWorkerId(workerId)
                .setLane(Lane.LANE_WORKER)
                .setWorkerFrame(frame)
                .build());
        workerFrames.put(frame.getFrameId(), bytes);
        return true;
    }

    private void handleWorkerFrame(Session session, DelegateRequest frame) {
        if (frame.hasHello()) {
            emit(session, "", 0, DelegateResponse.newBuilder()
                    .setAdmission(session.pendingAdmission));
            session.pendingAdmission = null;
            resumeRestoredLeases(session);
            return;
        }
        if (!session.admitted) {
            throw new IllegalArgumentException("worker was not admitted");
        }
        TaskRuntime task = requireTask(frame.getTaskId());
        if (!task.workerId.equals(session.workerId)) {
            throw new IllegalArgumentException("task is assigned to another worker");
        }
        switch (frame.getPayloadCase()) {
            case ACCEPT -> task.phase = DelegationReducer.Phase.LEASED;
            case REJECT -> {
                task.phase = DelegationReducer.Phase.REJECTED;
                task.leaseGeneration++;
            }
            case HEARTBEAT -> renew(task);
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
            case COMPLETION -> handleCandidate(task, frame.getCompletion());
            default -> throw new IllegalArgumentException("unexpected worker payload");
        }
    }

    private void handleCandidate(TaskRuntime task, CompletionCandidate candidate) {
        task.phase = DelegationReducer.Phase.CANDIDATE;
        task.candidate = candidate;
        CandidateReviewer.ReviewContext context = new CandidateReviewer.ReviewContext(
                task.taskId, task.workerId, task.offer.getSpec(), candidate);
        runtimeTasks.submit(() -> {
            ReviewDecision decision;
            try {
                decision = reviewer.review(context);
            } catch (Exception e) {
                synchronized (lock) {
                    if (task.phase == DelegationReducer.Phase.CANDIDATE
                            && task.candidate.equals(candidate)) {
                        task.reviewFailure = e;
                    }
                }
                return;
            }
            synchronized (lock) {
                if (!closed && task.phase == DelegationReducer.Phase.CANDIDATE
                        && task.candidate.equals(candidate)) {
                    applyReview(task, decision);
                }
            }
        });
    }

    private void applyReview(TaskRuntime task, ReviewDecision decision) {
        Session session = requireAdmittedSession(task.workerId);
        switch (decision) {
            case ReviewDecision.Accept(String verdict) -> {
                CompletionAccepted payload = CompletionAccepted.newBuilder()
                        .setAttempt(task.attempt)
                        .setRevision(task.candidate.getRevision())
                        .setVerdict(verdict)
                        .build();
                emit(session, task.taskId, task.attempt,
                        DelegateResponse.newBuilder().setAccepted(payload));
                task.phase = DelegationReducer.Phase.ACCEPTED;
                task.leaseGeneration++;
            }
            case ReviewDecision.Revise(String feedback, List<String> failedChecks) -> {
                RevisionRequested payload = RevisionRequested.newBuilder()
                        .setAttempt(task.attempt)
                        .setRevision(task.candidate.getRevision())
                        .setFeedback(feedback)
                        .addAllFailedChecks(failedChecks)
                        .build();
                emit(session, task.taskId, task.attempt,
                        DelegateResponse.newBuilder().setRevisionRequested(payload));
                task.phase = DelegationReducer.Phase.LEASED;
            }
            // The candidate stays open for an explicit review action; the
            // lifecycle does not move.
            case ReviewDecision.Pending _ -> {
            }
        }
    }

    private void renew(TaskRuntime task) {
        java.time.Duration lease = java.time.Duration.ofMillis(
                Durations.toMillis(task.offer.getLeaseDuration()));
        Instant next = task.expiry.plus(lease);
        LeaseRenewal renewal = LeaseRenewal.newBuilder()
                .setAttempt(task.attempt)
                .setExpiresAt(toTimestamp(next))
                .build();
        emit(requireAdmittedSession(task.workerId), task.taskId, task.attempt,
                DelegateResponse.newBuilder().setRenewal(renewal), () -> {
                    task.expiry = next;
                    task.leaseGeneration++;
                });
        scheduleExpiry(task.taskId, task.attempt, task.leaseGeneration, next);
    }

    private void scheduleExpiry(String taskId, int attempt, long generation,
                                Instant expiry) {
        runtimeTasks.submit(() -> {
            try {
                java.time.Duration delay = java.time.Duration.between(
                        clock.instant(), expiry);
                if (!delay.isNegative() && !delay.isZero()) {
                    Thread.sleep(delay);
                }
                synchronized (lock) {
                    TaskRuntime task = tasks.get(taskId);
                    if (!closed && task != null && task.attempt == attempt
                            && task.leaseGeneration == generation
                            && !task.expiry.isAfter(clock.instant())
                            && (task.phase == DelegationReducer.Phase.LEASED
                            || task.phase == DelegationReducer.Phase.CANDIDATE)) {
                        expire(task, "lease deadline elapsed");
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private void expire(TaskRuntime task, String reason) {
        LeaseExpired expired = LeaseExpired.newBuilder()
                .setAttempt(task.attempt)
                .setReason(reason)
                .build();
        emit(requireAdmittedSession(task.workerId), task.taskId, task.attempt,
                DelegateResponse.newBuilder().setExpired(expired));
        task.phase = DelegationReducer.Phase.EXPIRED;
        task.leaseGeneration++;
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
                .setSentAt(nowTimestamp())
                .build();
        DelegationValidation.validate(response);
        append(TranscriptEntry.newBuilder()
                .setWorkerId(session.workerId)
                .setLane(Lane.LANE_COORDINATOR)
                .setCoordinatorFrame(response)
                .build());
        session.commitCoordinatorSeq(taskId, attempt, seq);
        afterPersist.run();
        session.responses.onNext(response);
    }

    private void append(TranscriptEntry entry) {
        Transcript candidate = Transcript.newBuilder()
                .addAllEntries(entries)
                .addEntries(entry)
                .build();
        DelegationReducer.Result result = reducer.reduce(
                candidate);
        if (!result.clean()) {
            DelegationReducer.Finding finding = result.findings().getLast();
            throw new IllegalArgumentException(finding.kind() + ": " + finding.error());
        }
        transcripts.save(candidate);
        entries.add(entry);
        events.add(new Event(++cursor, entry));
        lock.notifyAll();
    }

    private void restore(Transcript transcript) {
        DelegationValidation.validate(transcript);
        DelegationReducer.Result result = reducer.reduce(transcript);
        if (!result.clean()) {
            DelegationReducer.Finding finding = result.findings().getFirst();
            throw new IllegalArgumentException("stored transcript is invalid: "
                    + finding.kind() + ": " + finding.error());
        }
        for (TranscriptEntry entry : transcript.getEntriesList()) {
            entries.add(entry);
            events.add(new Event(++cursor, entry));
            if (entry.getLane() == Lane.LANE_WORKER) {
                DelegateRequest frame = entry.getWorkerFrame();
                workerFrames.put(frame.getFrameId(), frame.toByteString());
            }
            restoreRuntime(entry);
        }
    }

    private void restoreRuntime(TranscriptEntry entry) {
        if (entry.getLane() == Lane.LANE_WORKER) {
            restoreWorkerRuntime(entry.getWorkerId(), entry.getWorkerFrame());
        } else {
            restoreCoordinatorRuntime(entry.getWorkerId(), entry.getCoordinatorFrame());
        }
    }

    private void restoreWorkerRuntime(String workerId, DelegateRequest frame) {
        if (frame.hasHello()) {
            Session session = new Session(frame.getHello(), DISCONNECTED_RESPONSES);
            session.connected = false;
            sessions.put(workerId, session);
            return;
        }
        TaskRuntime task = requireRestoredTask(frame.getTaskId(), workerId);
        switch (frame.getPayloadCase()) {
            case ACCEPT -> task.phase = DelegationReducer.Phase.LEASED;
            case REJECT -> task.phase = DelegationReducer.Phase.REJECTED;
            case BLOCKED -> task.phase = DelegationReducer.Phase.BLOCKED;
            case FAILED -> task.phase = DelegationReducer.Phase.FAILED;
            case COMPLETION -> {
                task.phase = DelegationReducer.Phase.CANDIDATE;
                task.candidate = frame.getCompletion();
            }
            case HEARTBEAT, PROGRESS, CHECKPOINT, CANCELLED -> {
                // These frames do not independently change the reconstructed phase.
            }
            case TASK_MESSAGE -> {
                // Non-transitioning on the live path, so nothing to reconstruct:
                // the restored entry is already in the transcript and event feed.
            }
            case HELLO, PAYLOAD_NOT_SET -> throw new IllegalArgumentException(
                    "stored transcript contains an unexpected worker payload");
        }
    }

    private void restoreCoordinatorRuntime(String workerId, DelegateResponse frame) {
        if (frame.hasAdmission()) {
            Session session = sessions.get(workerId);
            if (session == null) {
                throw new IllegalArgumentException(
                        "stored transcript admits a worker before its hello: " + workerId);
            }
            session.admitted = frame.getAdmission().getAdmitted();
            return;
        }
        String taskId = frame.getTaskId();
        if (frame.hasOffer()) {
            TaskOffer offer = frame.getOffer();
            TaskRuntime task = tasks.computeIfAbsent(taskId, TaskRuntime::new);
            task.workerId = workerId;
            task.attempt = offer.getAttempt();
            task.offer = offer;
            task.phase = DelegationReducer.Phase.OFFERED;
            task.expiry = toInstant(offer.getExpiresAt());
            task.leaseGeneration++;
            return;
        }
        TaskRuntime task = requireRestoredTask(taskId, workerId);
        switch (frame.getPayloadCase()) {
            case RENEWAL -> {
                task.expiry = toInstant(frame.getRenewal().getExpiresAt());
                task.leaseGeneration++;
            }
            case EXPIRED -> {
                task.phase = DelegationReducer.Phase.EXPIRED;
                task.leaseGeneration++;
            }
            case CANCELLATION -> {
                task.phase = DelegationReducer.Phase.CANCELLED;
                task.leaseGeneration++;
            }
            case REVISION_REQUESTED -> task.phase = DelegationReducer.Phase.LEASED;
            case ACCEPTED -> {
                task.phase = DelegationReducer.Phase.ACCEPTED;
                task.leaseGeneration++;
            }
            case TASK_MESSAGE -> {
                // Non-transitioning on the live path, so nothing to reconstruct:
                // the restored entry is already in the transcript and event feed.
            }
            case ADMISSION, OFFER, PAYLOAD_NOT_SET -> throw new IllegalArgumentException(
                    "stored transcript contains an unexpected coordinator payload");
        }
    }

    private TaskRuntime requireRestoredTask(String taskId, String workerId) {
        TaskRuntime task = tasks.get(taskId);
        if (task == null || !workerId.equals(task.workerId)) {
            throw new IllegalArgumentException(
                    "stored transcript references a task before its offer: " + taskId);
        }
        return task;
    }

    private void restoreCoordinatorSequences(Session session) {
        for (TranscriptEntry entry : entries) {
            if (entry.getLane() == Lane.LANE_COORDINATOR
                    && entry.getWorkerId().equals(session.workerId)) {
                DelegateResponse frame = entry.getCoordinatorFrame();
                session.restoreCoordinatorSeq(frame.getTaskId(), attemptOf(frame),
                        frame.getSeq());
            }
        }
    }

    private void resumeRestoredLeases(Session session) {
        for (TaskRuntime task : tasks.values()) {
            if (!task.workerId.equals(session.workerId)
                    || (task.phase != DelegationReducer.Phase.LEASED
                    && task.phase != DelegationReducer.Phase.CANDIDATE)) {
                continue;
            }
            if (!task.expiry.isAfter(clock.instant())) {
                expire(task, "lease deadline elapsed while the coordinator was offline");
            } else {
                scheduleExpiry(task.taskId, task.attempt, task.leaseGeneration, task.expiry);
            }
        }
    }

    /** The attempt a worker frame belongs to, for sequencing; mirrors the reducer. */
    private static int workerAttemptOf(DelegateRequest frame) {
        return switch (frame.getPayloadCase()) {
            case ACCEPT -> frame.getAccept().getAttempt();
            case REJECT -> frame.getReject().getAttempt();
            case HEARTBEAT -> frame.getHeartbeat().getAttempt();
            case PROGRESS -> frame.getProgress().getAttempt();
            case CHECKPOINT -> frame.getCheckpoint().getAttempt();
            case BLOCKED -> frame.getBlocked().getAttempt();
            case FAILED -> frame.getFailed().getAttempt();
            case CANCELLED -> frame.getCancelled().getAttempt();
            case COMPLETION -> frame.getCompletion().getAttempt();
            // Hellos sequence on the session scope; task messages carry no
            // attempt and sequence in the task's attempt-0 scope.
            case HELLO, TASK_MESSAGE -> 0;
            default -> 0;
        };
    }

    private static int attemptOf(DelegateResponse frame) {
        return switch (frame.getPayloadCase()) {
            case OFFER -> frame.getOffer().getAttempt();
            case RENEWAL -> frame.getRenewal().getAttempt();
            case EXPIRED -> frame.getExpired().getAttempt();
            case CANCELLATION -> frame.getCancellation().getAttempt();
            case REVISION_REQUESTED -> frame.getRevisionRequested().getAttempt();
            case ACCEPTED -> frame.getAccepted().getAttempt();
            // Task messages carry no attempt; they sequence in the task's
            // attempt-0 scope, exactly as on the live path.
            case TASK_MESSAGE -> 0;
            case ADMISSION, PAYLOAD_NOT_SET -> 0;
        };
    }

    private Optional<Event> firstEvent(String taskId, long afterCursor) {
        return events.stream()
                .filter(event -> event.cursor > afterCursor)
                .filter(event -> taskId == null || taskId.isEmpty()
                        || event.taskId().equals(taskId))
                .findFirst();
    }

    private Session requireAdmittedSession(String workerId) {
        Session session = sessions.get(workerId);
        if (session == null || !session.admitted || !session.connected) {
            throw new IllegalStateException("worker is not admitted and connected: " + workerId);
        }
        return session;
    }

    private TaskRuntime requireTask(String taskId) {
        TaskRuntime task = tasks.get(taskId);
        if (task == null) {
            throw new IllegalArgumentException("unknown task: " + taskId);
        }
        return task;
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("coordinator is closed");
        }
    }

    private void markDisconnected(Session session) {
        if (session == null) {
            return;
        }
        synchronized (lock) {
            session.connected = false;
        }
    }

    private Timestamp nowTimestamp() {
        return toTimestamp(clock.instant());
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamps.fromMillis(instant.toEpochMilli());
    }

    private static Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    private static Duration toProtoDuration(java.time.Duration duration) {
        return Durations.fromNanos(duration.toNanos());
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

    private static final class Session {
        private final String workerId;
        private final WorkerHello hello;
        private final StreamObserver<DelegateResponse> responses;
        private final Map<String, Long> coordinatorSequences = new HashMap<>();
        private boolean admitted;
        private boolean connected = true;
        private AdmissionDecision pendingAdmission;

        private Session(WorkerHello hello, StreamObserver<DelegateResponse> responses) {
            this.workerId = hello.getWorkerId();
            this.hello = hello;
            this.responses = responses;
        }

        private long peekNextCoordinatorSeq(String taskId, int attempt) {
            String key = taskId + "\n" + attempt;
            return coordinatorSequences.getOrDefault(key, 0L) + 1L;
        }

        private void commitCoordinatorSeq(String taskId, int attempt, long seq) {
            String key = taskId + "\n" + attempt;
            coordinatorSequences.put(key, seq);
        }

        private void restoreCoordinatorSeq(String taskId, int attempt, long seq) {
            String key = taskId + "\n" + attempt;
            coordinatorSequences.merge(key, seq, Math::max);
        }
    }

    private static final class TaskRuntime {
        private final String taskId;
        private String workerId;
        private int attempt;
        private TaskOffer offer;
        private DelegationReducer.Phase phase;
        private Instant expiry;
        private long leaseGeneration;
        private CompletionCandidate candidate;
        private Exception reviewFailure;

        private TaskRuntime(String taskId) {
            this.taskId = taskId;
        }

        private boolean attemptTerminal() {
            return phase == DelegationReducer.Phase.REJECTED
                    || phase == DelegationReducer.Phase.BLOCKED
                    || phase == DelegationReducer.Phase.FAILED
                    || phase == DelegationReducer.Phase.CANCELLED
                    || phase == DelegationReducer.Phase.EXPIRED;
        }
    }
}
