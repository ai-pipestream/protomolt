package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.AgentDelegationServiceGrpc;
import ai.protomolt.proto.delegation.v1.CancelledNotice;
import ai.protomolt.proto.delegation.v1.Checkpoint;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.DelegateRequest;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.FailureReport;
import ai.protomolt.proto.delegation.v1.Heartbeat;
import ai.protomolt.proto.delegation.v1.ProgressEvent;
import ai.protomolt.proto.delegation.v1.RevisionRequested;
import ai.protomolt.proto.delegation.v1.TaskAccept;
import ai.protomolt.proto.delegation.v1.TaskMessage;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Connects a provider-neutral {@link WorkerRunner} to the delegation bidi stream. */
public final class DelegationWorker implements AutoCloseable {

    /** Maximum coordinator task messages buffered for the runner's host to drain. */
    private static final int MAX_BUFFERED_MESSAGES = 256;

    private final Object lock = new Object();
    private final AgentDelegationServiceGrpc.AgentDelegationServiceStub stub;
    private final WorkerRunner runner;
    private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<String, ActiveTask> active = new HashMap<>();
    private final Map<String, Long> sequences = new HashMap<>();
    private final java.util.concurrent.ConcurrentLinkedQueue<TaskMessage> messages =
            new java.util.concurrent.ConcurrentLinkedQueue<>();
    private volatile CountDownLatch admission = new CountDownLatch(1);
    private volatile StreamObserver<DelegateRequest> requests;
    private volatile boolean admitted;
    private volatile Throwable streamFailure;
    private volatile boolean streamTerminated;
    private volatile boolean closed;

    /** Creates a worker bridge for one async delegation stub. */
    public DelegationWorker(AgentDelegationServiceGrpc.AgentDelegationServiceStub stub,
                            WorkerRunner runner) {
        this.stub = Objects.requireNonNull(stub, "stub");
        this.runner = Objects.requireNonNull(runner, "runner");
    }

    /**
     * Opens the stream and sends the runner's hello.
     *
     * <p>After the stream terminates (transport failure or coordinator shutdown) a
     * worker may {@code start()} again: the replacement stream re-sends the hello and
     * this instance's per-scope sequence counters carry over, so the re-hello and every
     * later frame continue the transcript's scopes instead of rewinding them. The
     * coordinator must still hold the worker's recorded transcript (a durable
     * repository, or no restart at all); a coordinator that lost the transcript
     * rejects the continued hello as a sequence gap, which is the honest answer when
     * neither side can prove continuity.</p>
     */
    public void start() {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("worker is closed");
            }
            if (requests != null && !streamTerminated) {
                throw new IllegalStateException("worker is already started");
            }
            requests = stub.delegate(new CoordinatorObserver());
            streamTerminated = false;
            streamFailure = null;
            admitted = false;
            admission = new CountDownLatch(1);
        }
        send("", 0, DelegateRequest.newBuilder().setHello(runner.hello()));
    }

    /** Waits for the coordinator's admission decision. */
    public boolean awaitAdmission(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        admission.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
        return admitted;
    }

    /** Returns the terminal stream failure, when one occurred. */
    public Optional<Throwable> streamFailure() {
        return Optional.ofNullable(streamFailure);
    }

    /**
     * Returns whether the delegation stream is currently open. A host that runs
     * reconnect loops polls this after a failure or coordinator shutdown before
     * calling {@link #start()} again.
     */
    public boolean streamOpen() {
        return requests != null && !streamTerminated;
    }

    /**
     * Drains the buffered coordinator task messages in arrival order. Messages are
     * bounded at {@value MAX_BUFFERED_MESSAGES}; when the buffer is full the oldest
     * message is dropped, so a host that never drains cannot grow memory.
     */
    public java.util.List<TaskMessage> drainTaskMessages() {
        java.util.List<TaskMessage> drained = new java.util.ArrayList<>();
        TaskMessage message;
        while ((message = messages.poll()) != null) {
            drained.add(message);
        }
        return drained;
    }

    /**
     * Sends a non-transitioning task message to the coordinator. The message is
     * recorded and sequenced like any other frame but never moves the lifecycle.
     *
     * @return the emitted message, with its generated id and timestamp
     */
    public TaskMessage sendMessage(String taskId,
                                   ai.protomolt.proto.delegation.v1.TaskMessageKind kind,
                                   String text, String replyTo,
                                   java.util.List<ai.protomolt.proto.grpc.workflow.v1.ArtifactReference> artifacts) {
        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(text, "text");
        Objects.requireNonNull(artifacts, "artifacts");
        TaskMessage message = TaskMessage.newBuilder()
                .setMessageId(UUID.randomUUID().toString())
                .setSender(runner.hello().getWorkerId())
                .setRecipient(DelegationValidation.COORDINATOR)
                .setTaskId(taskId)
                .setKind(kind)
                .setReplyTo(replyTo == null ? "" : replyTo)
                .setText(text)
                .addAllArtifacts(artifacts)
                .setSentAt(nowTimestamp())
                .build();
        // Messages sequence in the task's attempt-0 scope on both lanes.
        send(taskId, 0, DelegateRequest.newBuilder().setTaskMessage(message));
        return message;
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            active.values().forEach(task -> task.cancelled.set(true));
            if (requests != null) {
                requests.onCompleted();
            }
        }
        tasks.shutdownNow();
    }

    private void handle(DelegateResponse frame) {
        DelegationValidation.validate(frame);
        switch (frame.getPayloadCase()) {
            case ADMISSION -> {
                admitted = frame.getAdmission().getAdmitted();
                admission.countDown();
            }
            case OFFER -> acceptAndRun(frame.getTaskId(), frame.getOffer());
            case RENEWAL -> {
                // The active runner continues under the advanced expiry.
            }
            case EXPIRED -> stop(frame.getTaskId());
            case CANCELLATION -> cancel(frame.getTaskId(), frame.getCancellation().getReason());
            case REVISION_REQUESTED -> revise(frame.getTaskId(), frame.getRevisionRequested());
            case ACCEPTED -> {
                synchronized (lock) {
                    active.remove(frame.getTaskId());
                }
            }
            case TASK_MESSAGE -> {
                // Non-transitioning guidance for the runner's host; buffered, never
                // delivered into the synchronous run loop.
                messages.offer(frame.getTaskMessage());
                while (messages.size() > MAX_BUFFERED_MESSAGES) {
                    messages.poll();
                }
            }
            default -> throw new IllegalArgumentException("unexpected coordinator payload");
        }
    }

    private void acceptAndRun(String taskId, TaskOffer offer) {
        ActiveTask task = new ActiveTask(taskId, offer);
        synchronized (lock) {
            if (active.putIfAbsent(taskId, task) != null) {
                throw new IllegalStateException("worker already has task " + taskId);
            }
        }
        send(taskId, offer.getAttempt(), DelegateRequest.newBuilder()
                .setAccept(TaskAccept.newBuilder().setAttempt(offer.getAttempt())));
        execute(task, Optional.empty());
    }

    private void revise(String taskId, RevisionRequested revision) {
        ActiveTask task;
        synchronized (lock) {
            task = active.get(taskId);
            if (task == null || task.offer.getAttempt() != revision.getAttempt()) {
                throw new IllegalStateException("revision names no active task");
            }
        }
        execute(task, Optional.of(revision));
    }

    private void execute(ActiveTask task, Optional<RevisionRequested> revision) {
        tasks.submit(() -> {
            try {
                WorkerRunner.WorkerTask invocation = new WorkerRunner.WorkerTask(
                        task.taskId, task.offer, revision);
                CompletionCandidate candidate = runner.run(invocation,
                        new EventSink(task));
                if (task.cancelled.get()) {
                    return;
                }
                if (candidate.getAttempt() != task.offer.getAttempt()
                        || candidate.getRevision() != invocation.expectedRevision()) {
                    throw new IllegalArgumentException(
                            "runner returned the wrong attempt or revision");
                }
                DelegationValidation.validate(candidate);
                send(task.taskId, task.offer.getAttempt(),
                        DelegateRequest.newBuilder().setCompletion(candidate));
            } catch (Exception e) {
                if (!task.cancelled.get()) {
                    String message = e.getMessage() == null
                            ? e.getClass().getSimpleName() : e.getMessage();
                    if (message.length() > 8_192) {
                        message = message.substring(0, 8_192);
                    }
                    synchronized (lock) {
                        active.remove(task.taskId, task);
                    }
                    send(task.taskId, task.offer.getAttempt(),
                            DelegateRequest.newBuilder().setFailed(
                                    FailureReport.newBuilder()
                                            .setAttempt(task.offer.getAttempt())
                                            .setError(message)));
                }
            }
        });
    }

    private void cancel(String taskId, String note) {
        ActiveTask task;
        synchronized (lock) {
            task = active.remove(taskId);
            if (task == null) {
                return;
            }
            task.cancelled.set(true);
        }
        send(taskId, task.offer.getAttempt(), DelegateRequest.newBuilder()
                .setCancelled(CancelledNotice.newBuilder()
                        .setAttempt(task.offer.getAttempt())
                        .setNote(note.length() <= 1_024
                                ? note : note.substring(0, 1_024))));
    }

    private void stop(String taskId) {
        synchronized (lock) {
            ActiveTask task = active.remove(taskId);
            if (task != null) {
                task.cancelled.set(true);
            }
        }
    }

    private void send(String taskId, int attempt, DelegateRequest.Builder payload) {
        DelegateRequest frame;
        StreamObserver<DelegateRequest> target;
        synchronized (lock) {
            if (closed) {
                return;
            }
            if (streamTerminated) {
                // Fail fast without consuming the scope's next sequence: a frame
                // that cannot be sent must not leave a gap the reducer later
                // reports against the replacement stream.
                throw new IllegalStateException(
                        "worker stream is terminated; start() again to reconnect");
            }
            long seq = nextSequence(taskId, attempt);
            frame = payload
                    .setFrameId(UUID.randomUUID().toString())
                    .setTaskId(taskId)
                    .setSeq(seq)
                    .setSentAt(nowTimestamp())
                    .build();
            target = requests;
        }
        DelegationValidation.validate(frame);
        target.onNext(frame);
    }

    private long nextSequence(String taskId, int attempt) {
        String key = taskId + "\n" + attempt;
        long next = sequences.getOrDefault(key, 1L);
        sequences.put(key, next + 1);
        return next;
    }

    private static Timestamp nowTimestamp() {
        return Timestamps.fromMillis(Instant.now().toEpochMilli());
    }

    private final class CoordinatorObserver implements StreamObserver<DelegateResponse> {
        @Override
        public void onNext(DelegateResponse frame) {
            try {
                handle(frame);
            } catch (RuntimeException e) {
                streamFailure = e;
                admission.countDown();
                requests.onError(e);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            synchronized (lock) {
                streamTerminated = true;
            }
            streamFailure = throwable;
            admission.countDown();
        }

        @Override
        public void onCompleted() {
            synchronized (lock) {
                streamTerminated = true;
            }
            admission.countDown();
        }
    }

    private final class EventSink implements WorkerRunner.WorkerEvents {
        private final ActiveTask task;

        private EventSink(ActiveTask task) {
            this.task = task;
        }

        @Override
        public void progress(String message) {
            int progressSeq = task.progress.incrementAndGet();
            send(task.taskId, task.offer.getAttempt(), DelegateRequest.newBuilder()
                    .setProgress(ProgressEvent.newBuilder()
                            .setAttempt(task.offer.getAttempt())
                            .setProgressSeq(progressSeq)
                            .setMessage(message)));
        }

        @Override
        public void checkpoint(Checkpoint checkpoint) {
            Objects.requireNonNull(checkpoint, "checkpoint");
            int checkpointSeq = task.checkpoints.incrementAndGet();
            Checkpoint normalized = checkpoint.toBuilder()
                    .setAttempt(task.offer.getAttempt())
                    .setCheckpointSeq(checkpointSeq)
                    .build();
            send(task.taskId, task.offer.getAttempt(), DelegateRequest.newBuilder()
                    .setCheckpoint(normalized));
        }

        @Override
        public void heartbeat(String note) {
            send(task.taskId, task.offer.getAttempt(), DelegateRequest.newBuilder()
                    .setHeartbeat(Heartbeat.newBuilder()
                            .setAttempt(task.offer.getAttempt())
                            .setNote(note)));
        }

        @Override
        public boolean cancelled() {
            return task.cancelled.get();
        }
    }

    private static final class ActiveTask {
        private final String taskId;
        private final TaskOffer offer;
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicInteger progress = new AtomicInteger();
        private final AtomicInteger checkpoints = new AtomicInteger();

        private ActiveTask(String taskId, TaskOffer offer) {
            this.taskId = taskId;
            this.offer = offer;
        }
    }
}
