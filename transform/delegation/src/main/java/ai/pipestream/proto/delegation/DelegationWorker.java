package ai.pipestream.proto.delegation;

import ai.pipestream.proto.delegation.v1.AgentDelegationServiceGrpc;
import ai.pipestream.proto.delegation.v1.CancelledNotice;
import ai.pipestream.proto.delegation.v1.Checkpoint;
import ai.pipestream.proto.delegation.v1.CompletionCandidate;
import ai.pipestream.proto.delegation.v1.DelegateRequest;
import ai.pipestream.proto.delegation.v1.DelegateResponse;
import ai.pipestream.proto.delegation.v1.FailureReport;
import ai.pipestream.proto.delegation.v1.Heartbeat;
import ai.pipestream.proto.delegation.v1.ProgressEvent;
import ai.pipestream.proto.delegation.v1.RevisionRequested;
import ai.pipestream.proto.delegation.v1.TaskAccept;
import ai.pipestream.proto.delegation.v1.TaskOffer;
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

    private final Object lock = new Object();
    private final AgentDelegationServiceGrpc.AgentDelegationServiceStub stub;
    private final WorkerRunner runner;
    private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<String, ActiveTask> active = new HashMap<>();
    private final Map<String, Long> sequences = new HashMap<>();
    private final CountDownLatch admission = new CountDownLatch(1);
    private volatile StreamObserver<DelegateRequest> requests;
    private volatile boolean admitted;
    private volatile Throwable streamFailure;
    private volatile boolean closed;

    /** Creates a worker bridge for one async delegation stub. */
    public DelegationWorker(AgentDelegationServiceGrpc.AgentDelegationServiceStub stub,
                            WorkerRunner runner) {
        this.stub = Objects.requireNonNull(stub, "stub");
        this.runner = Objects.requireNonNull(runner, "runner");
    }

    /** Opens the stream and sends the runner's hello. */
    public void start() {
        synchronized (lock) {
            if (requests != null) {
                throw new IllegalStateException("worker is already started");
            }
            requests = stub.delegate(new CoordinatorObserver());
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
            streamFailure = throwable;
            admission.countDown();
        }

        @Override
        public void onCompleted() {
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
