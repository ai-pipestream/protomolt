package ai.pipestream.proto.delegation;

import ai.pipestream.proto.delegation.v1.AdmissionDecision;
import ai.pipestream.proto.delegation.v1.Checkpoint;
import ai.pipestream.proto.delegation.v1.CheckpointReference;
import ai.pipestream.proto.delegation.v1.CompletionCandidate;
import ai.pipestream.proto.delegation.v1.DelegateRequest;
import ai.pipestream.proto.delegation.v1.DelegateResponse;
import ai.pipestream.proto.delegation.v1.ProgressEvent;
import ai.pipestream.proto.delegation.v1.TaskAccept;
import ai.pipestream.proto.delegation.v1.TaskMessage;
import ai.pipestream.proto.delegation.v1.TaskMessageKind;
import ai.pipestream.proto.delegation.v1.TaskOffer;
import ai.pipestream.proto.delegation.v1.TaskSpec;
import ai.pipestream.proto.delegation.v1.WorkerHello;
import ai.pipestream.proto.grpc.workflow.v1.ArtifactReference;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The session-owning adapter between MCP-style request/response callers and one
 * {@link InProcessDelegationCoordinator}. Coordinator-side operations (offer, review,
 * cancel, message, watch, transcript) pass straight through to the coordinator.
 * Worker-side operations are carried on real delegation streams the bridge opens and
 * keeps: a worker registered here stays admitted regardless of how many MCP sessions
 * come and go, and every worker frame is validated before it touches the stream so a
 * malformed tool call cannot tear the session down.
 *
 * <p>The bridge holds no lifecycle logic. Sequencing mirrors the wire contract (one
 * counter per (task, attempt) scope, task messages in the attempt-0 scope) and every
 * admission, transition, and review decision remains the coordinator's and the
 * reducer's. When a worker's stream fails or the server restarts over a durable
 * transcript, a replacement registration seeds its counters from the recorded
 * transcript's high-water marks, so the re-hello resumes the worker's sequence
 * scopes instead of rewinding them.</p>
 */
public final class DelegationBridge implements AutoCloseable {

    /** The outcome of one worker registration. */
    public record WorkerRegistration(String workerId, boolean admitted, String sessionId,
                                     String reason) {
        public WorkerRegistration {
            Objects.requireNonNull(workerId, "workerId");
            Objects.requireNonNull(sessionId, "sessionId");
            Objects.requireNonNull(reason, "reason");
        }
    }

    private final Object lock = new Object();
    private final InProcessDelegationCoordinator coordinator;
    private final Map<String, WorkerStream> streams = new LinkedHashMap<>();
    private boolean closed;

    /** Creates a bridge over one coordinator. */
    public DelegationBridge(InProcessDelegationCoordinator coordinator) {
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
    }

    /** The coordinator this bridge adapts; lifecycle decisions live there. */
    public InProcessDelegationCoordinator coordinator() {
        return coordinator;
    }

    /**
     * Opens a worker session: a real delegation stream, a validated hello, and the
     * coordinator's admission decision. One live stream per worker id: a second
     * registration while the current stream is still open fails fast, because two
     * live senders would race the transcript's sequence scopes.
     *
     * <p>A worker whose previous stream failed or completed re-registers as a
     * replacement: the new stream seeds its per-scope frame, progress, and
     * checkpoint counters from {@link InProcessDelegationCoordinator#workerResumption},
     * the high-water marks of the recorded transcript, so the coordinator admits the
     * re-hello as a continuation instead of rejecting a rewind. A worker the
     * transcript has never seen starts every scope at 1.</p>
     */
    public WorkerRegistration registerWorker(WorkerHello hello) {
        Objects.requireNonNull(hello, "hello");
        DelegationValidation.validate(hello);
        WorkerStream stream = new WorkerStream(hello.getWorkerId());
        synchronized (lock) {
            requireOpen();
            WorkerStream previous = streams.get(hello.getWorkerId());
            if (previous != null && previous.open && previous.failure == null) {
                throw new IllegalStateException(
                        "worker is already registered: " + hello.getWorkerId());
            }
            if (previous != null) {
                previous.complete();
            }
            stream.seed(coordinator.workerResumption(hello.getWorkerId()));
            streams.put(hello.getWorkerId(), stream);
        }
        CountDownLatch admission = new CountDownLatch(1);
        stream.responses = new StreamObserver<>() {
            @Override
            public void onNext(DelegateResponse frame) {
                if (frame.hasAdmission()) {
                    AdmissionDecision decision = frame.getAdmission();
                    stream.admitted = decision.getAdmitted();
                    stream.sessionId = decision.getSessionId();
                    stream.admissionReason = decision.getReason();
                    admission.countDown();
                }
                // Every other coordinator frame is already on the coordinator's
                // cursor-addressable event feed; watch reads it from there.
            }

            @Override
            public void onError(Throwable throwable) {
                stream.failure = throwable;
                stream.open = false;
                admission.countDown();
            }

            @Override
            public void onCompleted() {
                stream.open = false;
                admission.countDown();
            }
        };
        stream.requests = coordinator.delegate(stream.responses);
        stream.send("", 0, DelegateRequest.newBuilder().setHello(hello));
        try {
            if (!admission.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException(
                        "admission did not arrive for worker " + hello.getWorkerId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "interrupted awaiting admission for worker " + hello.getWorkerId(), e);
        }
        if (stream.failure != null) {
            throw new IllegalStateException(
                    "worker stream failed during admission: " + failureMessage(stream.failure));
        }
        return new WorkerRegistration(stream.workerId, stream.admitted, stream.sessionId,
                stream.admissionReason);
    }

    /** The worker takes the open offer for the task's current attempt. */
    public void accept(String workerId, String taskId, int attempt) {
        WorkerStream stream = requireStream(workerId);
        stream.sendChecked(taskId, attempt, DelegateRequest.newBuilder()
                .setAccept(TaskAccept.newBuilder().setAttempt(attempt)));
    }

    /**
     * One monotonic progress note from the worker.
     *
     * @return the assigned progress sequence inside the attempt
     */
    public int progress(String workerId, String taskId, int attempt, String message) {
        Objects.requireNonNull(message, "message");
        WorkerStream stream = requireStream(workerId);
        return stream.sendProgress(taskId, attempt, message);
    }

    /**
     * One resumable checkpoint of worker state.
     *
     * @return the assigned checkpoint sequence inside the attempt
     */
    public int checkpoint(String workerId, String taskId, int attempt, String resumeToken,
                          String note, ArtifactReference state) {
        Objects.requireNonNull(resumeToken, "resumeToken");
        WorkerStream stream = requireStream(workerId);
        return stream.sendCheckpoint(taskId, attempt, resumeToken, note, state);
    }

    /** Submits one revision of completion evidence for review. */
    public void submitCandidate(String workerId, String taskId, CompletionCandidate candidate) {
        Objects.requireNonNull(candidate, "candidate");
        WorkerStream stream = requireStream(workerId);
        stream.sendChecked(taskId, candidate.getAttempt(),
                DelegateRequest.newBuilder().setCompletion(candidate));
    }

    /**
     * Sends a non-transitioning task message from the worker to the coordinator.
     *
     * @return the emitted message, with its generated id and timestamp
     */
    public TaskMessage sendWorkerMessage(String workerId, String taskId,
                                         TaskMessageKind kind, String text, String replyTo,
                                         List<ArtifactReference> artifacts) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(text, "text");
        Objects.requireNonNull(artifacts, "artifacts");
        WorkerStream stream = requireStream(workerId);
        TaskMessage message = TaskMessage.newBuilder()
                .setMessageId(UUID.randomUUID().toString())
                .setSender(workerId)
                .setRecipient(DelegationValidation.COORDINATOR)
                .setTaskId(taskId)
                .setKind(kind)
                .setReplyTo(replyTo == null ? "" : replyTo)
                .setText(text)
                .addAllArtifacts(artifacts)
                .setSentAt(nowTimestamp())
                .build();
        // Messages sequence in the task's attempt-0 scope on both lanes.
        stream.sendChecked(taskId, 0, DelegateRequest.newBuilder().setTaskMessage(message));
        return message;
    }

    /** Offers a new task attempt to an admitted worker; see the coordinator. */
    public TaskOffer offer(String workerId, String taskId, TaskSpec spec,
                           Duration leaseDuration, CheckpointReference resumeFrom) {
        return coordinator.offer(workerId, taskId, spec, leaseDuration, resumeFrom);
    }

    /** Applies an external review decision; see the coordinator. */
    public void review(String taskId, CandidateReviewer.ReviewDecision decision) {
        coordinator.review(taskId, decision);
    }

    /** Cancels the current offer or lease; see the coordinator. */
    public void cancel(String taskId, String reason) {
        coordinator.cancel(taskId, reason);
    }

    /** Sends a coordinator task message to one worker; see the coordinator. */
    public TaskMessage sendCoordinatorMessage(String workerId, String taskId,
                                              TaskMessageKind kind, String text,
                                              String replyTo,
                                              List<ArtifactReference> artifacts) {
        return coordinator.sendMessage(workerId, taskId, kind, text, replyTo, artifacts);
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            streams.values().forEach(WorkerStream::complete);
            streams.clear();
        }
    }

    private WorkerStream requireStream(String workerId) {
        Objects.requireNonNull(workerId, "workerId");
        synchronized (lock) {
            WorkerStream stream = streams.get(workerId);
            if (stream == null) {
                throw new IllegalArgumentException(
                        "worker is not registered on this bridge: " + workerId);
            }
            return stream;
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("delegation bridge is closed");
        }
    }

    private static String failureMessage(Throwable failure) {
        String message = failure.getMessage();
        return message == null ? failure.getClass().getSimpleName() : message;
    }

    private static Timestamp nowTimestamp() {
        return Timestamps.fromMillis(Instant.now().toEpochMilli());
    }

    /** One open worker stream plus its per-scope sequence counters. */
    private static final class WorkerStream {
        private final String workerId;
        private final Map<String, Long> sequences = new HashMap<>();
        private final Map<String, Integer> progressSequences = new HashMap<>();
        private final Map<String, Integer> checkpointSequences = new HashMap<>();
        private volatile StreamObserver<DelegateRequest> requests;
        private volatile StreamObserver<DelegateResponse> responses;
        private volatile boolean admitted;
        private volatile String sessionId = "";
        private volatile String admissionReason = "";
        private volatile Throwable failure;
        private volatile boolean open = true;

        private WorkerStream(String workerId) {
            this.workerId = workerId;
        }

        /**
         * Seeds the per-scope counters from the transcript's high-water marks before
         * the stream sends its first frame, so a replacement stream continues the
         * recorded scopes instead of rewinding them.
         */
        private synchronized void seed(InProcessDelegationCoordinator.WorkerResumption resumption) {
            resumption.sequences().forEach((scope, last) ->
                    sequences.put(scope(scope.taskId(), scope.attempt()), last));
            resumption.progressSequences().forEach((scope, last) ->
                    progressSequences.put(scope(scope.taskId(), scope.attempt()), last));
            resumption.checkpointSequences().forEach((scope, last) ->
                    checkpointSequences.put(scope(scope.taskId(), scope.attempt()), last));
        }

        private synchronized int sendProgress(String taskId, int attempt, String message) {
            int progressSeq = progressSequences.getOrDefault(scope(taskId, attempt), 0) + 1;
            send(taskId, attempt, DelegateRequest.newBuilder()
                    .setProgress(ProgressEvent.newBuilder()
                            .setAttempt(attempt)
                            .setProgressSeq(progressSeq)
                            .setMessage(message)));
            progressSequences.put(scope(taskId, attempt), progressSeq);
            return progressSeq;
        }

        private synchronized int sendCheckpoint(String taskId, int attempt, String resumeToken,
                                                String note, ArtifactReference state) {
            int checkpointSeq = checkpointSequences.getOrDefault(scope(taskId, attempt), 0) + 1;
            Checkpoint.Builder checkpoint = Checkpoint.newBuilder()
                    .setAttempt(attempt)
                    .setCheckpointSeq(checkpointSeq)
                    .setResumeToken(resumeToken)
                    .setNote(note == null ? "" : note);
            if (state != null) {
                checkpoint.setState(state);
            }
            send(taskId, attempt, DelegateRequest.newBuilder().setCheckpoint(checkpoint));
            checkpointSequences.put(scope(taskId, attempt), checkpointSeq);
            return checkpointSeq;
        }

        private synchronized void sendChecked(String taskId, int attempt,
                                              DelegateRequest.Builder payload) {
            send(taskId, attempt, payload);
        }

        private synchronized void send(String taskId, int attempt,
                                       DelegateRequest.Builder payload) {
            if (!open) {
                throw new IllegalStateException("worker stream is closed: " + workerId);
            }
            if (failure != null) {
                throw new IllegalStateException("worker stream failed for " + workerId
                        + ": " + failureMessage(failure));
            }
            // Validation runs before the sequence commits: a malformed tool call fails
            // the call and leaves the scope's next sequence untouched.
            long seq = sequences.getOrDefault(scope(taskId, attempt), 0L) + 1;
            DelegateRequest frame = payload
                    .setFrameId(UUID.randomUUID().toString())
                    .setTaskId(taskId)
                    .setSeq(seq)
                    .setSentAt(nowTimestamp())
                    .build();
            DelegationValidation.validate(frame);
            sequences.put(scope(taskId, attempt), seq);
            requests.onNext(frame);
            if (failure != null) {
                throw new IllegalStateException("worker stream failed for " + workerId
                        + ": " + failureMessage(failure));
            }
            if (!open) {
                throw new IllegalStateException("worker stream is closed: " + workerId);
            }
        }

        private synchronized void complete() {
            open = false;
            if (requests != null) {
                requests.onCompleted();
            }
        }

        private static String scope(String taskId, int attempt) {
            return taskId + "\n" + attempt;
        }
    }
}
