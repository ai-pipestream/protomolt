package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.v1.CoordinatorFrame;
import ai.protomolt.proto.mesh.runtime.v1.DeliveryClaim;
import ai.protomolt.proto.mesh.runtime.v1.DemandProcessorServiceGrpc;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorCompletion;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorFailure;
import ai.protomolt.proto.mesh.runtime.v1.WorkerAdmission;
import ai.protomolt.proto.mesh.runtime.v1.WorkerFrame;
import ai.protomolt.proto.mesh.runtime.v1.WorkerHello;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Worker-initiated, credit-driven gRPC coordinator over one durable channel. */
public final class DemandProcessorCoordinator
        extends DemandProcessorServiceGrpc.DemandProcessorServiceImplBase
        implements AutoCloseable {

    private static final int MAX_DEMAND = 100_000;

    private final DescriptorRegistry descriptors;
    private final DurableProcessorChannel channel;
    private final RemoteWorkerAdmission admission;
    private final Clock clock;
    private final Duration leaseDuration;
    private final ScheduledExecutorService maintenance;
    private final Map<String, Session> sessions = new LinkedHashMap<>();
    private RuntimeException maintenanceFailure;
    private boolean closed;

    public DemandProcessorCoordinator(
            DescriptorRegistry descriptors,
            DurableProcessorChannel channel,
            Duration leaseDuration) {
        this(descriptors, channel, RemoteWorkerAdmission.allowAll(),
                Clock.systemUTC(), leaseDuration);
    }

    public DemandProcessorCoordinator(
            DescriptorRegistry descriptors,
            DurableProcessorChannel channel,
            RemoteWorkerAdmission admission,
            Clock clock,
            Duration leaseDuration) {
        this.descriptors = Objects.requireNonNull(descriptors, "descriptors");
        this.channel = Objects.requireNonNull(channel, "channel");
        this.admission = Objects.requireNonNull(admission, "admission");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.leaseDuration = Objects.requireNonNull(leaseDuration, "leaseDuration");
        if (leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
        long intervalMillis = maintenanceIntervalMillis(leaseDuration);
        maintenance = Executors.newSingleThreadScheduledExecutor(task ->
                Thread.ofPlatform()
                        .daemon(true)
                        .name("protomolt-processor-channel-maintenance")
                        .unstarted(task));
        maintenance.scheduleWithFixedDelay(
                this::maintain, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public StreamObserver<WorkerFrame> connect(
            StreamObserver<CoordinatorFrame> responseObserver) {
        Objects.requireNonNull(responseObserver, "responseObserver");
        return new StreamObserver<>() {
            private Session session;
            private long expectedSequence = 1;
            private WorkerFrame lastFrame;
            private boolean ended;

            @Override
            public void onNext(WorkerFrame frame) {
                if (ended) {
                    return;
                }
                try {
                    synchronized (DemandProcessorCoordinator.this) {
                        requireOpen();
                        RemoteValidation.annotations(frame);
                        RemoteValidation.uuid(frame.getFrameId(), "frame_id");
                        if (frame.getSequence() == expectedSequence - 1
                                && lastFrame != null
                                && lastFrame.getFrameId().equals(frame.getFrameId())) {
                            if (lastFrame.equals(frame)) {
                                return;
                            }
                            throw new IllegalArgumentException(
                                    "conflicting worker frame_id " + frame.getFrameId());
                        }
                        if (frame.getSequence() != expectedSequence) {
                            throw new IllegalArgumentException("worker frame sequence must be "
                                    + expectedSequence + " but was " + frame.getSequence());
                        }
                        expectedSequence++;
                        lastFrame = frame;
                        if (session == null) {
                            if (!frame.hasHello()) {
                                throw new IllegalArgumentException(
                                        "first worker frame must be hello");
                            }
                            session = open(frame.getHello(), responseObserver);
                            if (!session.admitted) {
                                ended = true;
                                responseObserver.onCompleted();
                            }
                            return;
                        }
                        if (frame.hasHello()) {
                            throw new IllegalArgumentException(
                                    "hello may only be the first worker frame");
                        }
                        handle(session, frame);
                    }
                } catch (IllegalArgumentException e) {
                    ended = true;
                    disconnect(session, "worker stream refused: " + e.getMessage());
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription(e.getMessage()).asRuntimeException());
                } catch (IllegalStateException e) {
                    ended = true;
                    disconnect(session, "worker stream state refused: " + e.getMessage());
                    responseObserver.onError(Status.FAILED_PRECONDITION
                            .withDescription(e.getMessage()).asRuntimeException());
                } catch (RuntimeException e) {
                    ended = true;
                    disconnect(session, "worker stream failed");
                    responseObserver.onError(Status.INTERNAL
                            .withDescription("demand processor coordinator failed")
                            .withCause(e).asRuntimeException());
                }
            }

            @Override
            public void onError(Throwable throwable) {
                ended = true;
                disconnect(session, "worker stream disconnected");
            }

            @Override
            public void onCompleted() {
                ended = true;
                disconnect(session, "worker stream completed");
                responseObserver.onCompleted();
            }
        };
    }

    /** Attempts dispatch after new work is durably appended. */
    public synchronized void workAvailable() {
        requireOpen();
        dispatch();
    }

    public synchronized int connectedWorkers() {
        return sessions.size();
    }

    /** Returns a terminal background maintenance failure, if one occurred. */
    public synchronized Optional<RuntimeException> maintenanceFailure() {
        return Optional.ofNullable(maintenanceFailure);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        maintenance.shutdownNow();
        for (Session session : java.util.List.copyOf(sessions.values())) {
            channel.releaseWorker(session.workerId,
                    "processor coordinator closed", clock.instant());
            try {
                session.responses.onCompleted();
            } catch (RuntimeException ignored) {
                // Closing continues for every session.
            }
        }
        sessions.clear();
    }

    private Session open(
            WorkerHello hello, StreamObserver<CoordinatorFrame> responses) {
        RemoteValidation.hello(hello, descriptors);
        RemoteWorkerAdmission.Decision decision = admission.admit(hello);
        boolean duplicate = sessions.containsKey(hello.getWorkerId());
        boolean admitted = decision.admitted() && !duplicate;
        String reason = duplicate ? "worker_id already has a connected stream"
                : decision.reason();
        Session session = new Session(
                hello.getWorkerId(), hello, responses, admitted);
        send(session, CoordinatorFrame.newBuilder()
                .setAdmission(WorkerAdmission.newBuilder()
                        .setAdmitted(admitted)
                        .setReason(reason)));
        if (admitted) {
            sessions.put(session.workerId, session);
        }
        return session;
    }

    private void handle(Session session, WorkerFrame frame) {
        switch (frame.getPayloadCase()) {
            case DEMAND -> {
                int permits = frame.getDemand().getPermits();
                if (permits < 1 || permits > MAX_DEMAND) {
                    throw new IllegalArgumentException(
                            "worker demand permits must be between 1 and " + MAX_DEMAND);
                }
                if (session.demand > MAX_DEMAND - permits) {
                    throw new IllegalArgumentException(
                            "worker outstanding demand exceeds " + MAX_DEMAND);
                }
                session.demand += permits;
                dispatch();
            }
            case COMPLETED -> completed(session, frame.getCompleted());
            case FAILED -> failed(session, frame.getFailed());
            case HELLO -> throw new IllegalArgumentException(
                    "hello may only be the first worker frame");
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException(
                    "worker frame requires a payload");
        }
    }

    private void completed(Session session, ProcessorCompletion completion) {
        String token = session.claims.get(completion.getDeliveryId());
        if (token == null || !token.equals(completion.getLeaseToken())) {
            throw new IllegalArgumentException("worker completion has no matching live claim");
        }
        channel.complete(session.workerId, completion, clock.instant());
        session.claims.remove(completion.getDeliveryId());
    }

    private void failed(Session session, ProcessorFailure failure) {
        String token = session.claims.get(failure.getDeliveryId());
        if (token == null || !token.equals(failure.getLeaseToken())) {
            throw new IllegalArgumentException("worker failure has no matching live claim");
        }
        channel.fail(session.workerId, failure, clock.instant());
        session.claims.remove(failure.getDeliveryId());
        dispatch();
    }

    private void dispatch() {
        channel.expire(clock.instant());
        for (Session session : java.util.List.copyOf(sessions.values())) {
            if (!session.admitted || session.demand == 0) {
                continue;
            }
            var claims = channel.claim(
                    session.workerId,
                    session.hello.getContractsList(),
                    session.demand,
                    leaseDuration,
                    clock.instant());
            for (DeliveryClaim claim : claims) {
                session.demand--;
                session.claims.put(
                        claim.getWork().getDeliveryId(), claim.getLeaseToken());
                try {
                    send(session, CoordinatorFrame.newBuilder().setClaim(claim));
                } catch (RuntimeException e) {
                    session.claims.remove(claim.getWork().getDeliveryId());
                    channel.release(claim.getWork().getDeliveryId(), claim.getLeaseToken(),
                            "coordinator could not deliver claim", clock.instant());
                    disconnect(session, "coordinator response failed");
                    break;
                }
            }
        }
    }

    private void send(Session session, CoordinatorFrame.Builder frame) {
        session.responses.onNext(frame.setSequence(++session.sequence).build());
    }

    private synchronized void disconnect(Session session, String reason) {
        if (session == null || session.closed) {
            return;
        }
        session.closed = true;
        sessions.remove(session.workerId, session);
        channel.releaseWorker(session.workerId, reason, clock.instant());
        session.claims.clear();
        if (!closed) {
            dispatch();
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("demand processor coordinator is closed");
        }
        if (maintenanceFailure != null) {
            throw new IllegalStateException(
                    "demand processor coordinator maintenance failed",
                    maintenanceFailure);
        }
    }

    private synchronized void maintain() {
        if (closed || maintenanceFailure != null) {
            return;
        }
        try {
            dispatch();
        } catch (RuntimeException failure) {
            maintenanceFailure = failure;
            for (Session session : java.util.List.copyOf(sessions.values())) {
                session.closed = true;
                try {
                    session.responses.onError(Status.UNAVAILABLE
                            .withDescription("processor channel maintenance failed")
                            .withCause(failure)
                            .asRuntimeException());
                } catch (RuntimeException ignored) {
                    // Every connected worker is notified as far as its stream permits.
                }
            }
            sessions.clear();
        }
    }

    private static long maintenanceIntervalMillis(Duration leaseDuration) {
        long leaseMillis;
        try {
            leaseMillis = leaseDuration.toMillis();
        } catch (ArithmeticException ignored) {
            leaseMillis = Long.MAX_VALUE;
        }
        return Math.max(10L, Math.min(1_000L, Math.max(1L, leaseMillis / 4L)));
    }

    private static final class Session {
        private final String workerId;
        private final WorkerHello hello;
        private final StreamObserver<CoordinatorFrame> responses;
        private final boolean admitted;
        private final Map<String, String> claims = new LinkedHashMap<>();
        private long sequence;
        private int demand;
        private boolean closed;

        private Session(
                String workerId,
                WorkerHello hello,
                StreamObserver<CoordinatorFrame> responses,
                boolean admitted) {
            this.workerId = workerId;
            this.hello = hello;
            this.responses = responses;
            this.admitted = admitted;
        }
    }
}
