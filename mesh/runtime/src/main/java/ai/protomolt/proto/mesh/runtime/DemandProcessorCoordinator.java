package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.v1.ClaimCancellation;
import ai.protomolt.proto.mesh.runtime.v1.CoordinatorFrame;
import ai.protomolt.proto.mesh.runtime.v1.DeliveryClaim;
import ai.protomolt.proto.mesh.runtime.v1.DemandProcessorServiceGrpc;
import ai.protomolt.proto.mesh.runtime.v1.DirectoryRevisionAcknowledgement;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorCompletion;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorFailure;
import ai.protomolt.proto.mesh.runtime.v1.WorkerAdmission;
import ai.protomolt.proto.mesh.runtime.v1.WorkerCancellationAcknowledgement;
import ai.protomolt.proto.mesh.runtime.v1.WorkerCapacity;
import ai.protomolt.proto.mesh.runtime.v1.WorkerDrainRequest;
import ai.protomolt.proto.mesh.runtime.v1.WorkerFrame;
import ai.protomolt.proto.mesh.runtime.v1.WorkerHello;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
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
    private final DirectoryProcessorResolver directoryResolver;
    private final WorkerDirectoryControl directoryControl;
    private final WorkerSessionRegistry sessionRegistry;
    private final WorkerCapacityController capacityController;
    private final Clock clock;
    private final Duration leaseDuration;
    private final ScheduledExecutorService maintenance;
    /** Connected sessions only, keyed by the server-issued session fence. */
    private final Map<String, Session> sessions = new LinkedHashMap<>();
    private RuntimeException maintenanceFailure;
    private boolean closed;

    public DemandProcessorCoordinator(
            DescriptorRegistry descriptors,
            DurableProcessorChannel channel,
            Duration leaseDuration) {
        this(descriptors, channel, RemoteWorkerAdmission.allowAll(), null,
                WorkerDirectoryControl.none(), new WorkerSessionRegistry(),
                new WorkerCapacityController(MAX_DEMAND), Clock.systemUTC(), leaseDuration);
    }

    public DemandProcessorCoordinator(
            DescriptorRegistry descriptors,
            DurableProcessorChannel channel,
            RemoteWorkerAdmission admission,
            Clock clock,
            Duration leaseDuration) {
        this(descriptors, channel, admission, null,
                WorkerDirectoryControl.none(), new WorkerSessionRegistry(),
                new WorkerCapacityController(MAX_DEMAND), clock, leaseDuration);
    }

    /** Creates a directory-backed coordinator with capacity and session fencing enabled. */
    public DemandProcessorCoordinator(
            DescriptorRegistry descriptors,
            DurableProcessorChannel channel,
            RemoteWorkerAdmission admission,
            DirectoryProcessorResolver directoryResolver,
            WorkerDirectoryControl directoryControl,
            WorkerSessionRegistry sessionRegistry,
            WorkerCapacityController capacityController,
            Clock clock,
            Duration leaseDuration) {
        this.descriptors = Objects.requireNonNull(descriptors, "descriptors");
        this.channel = Objects.requireNonNull(channel, "channel");
        this.admission = Objects.requireNonNull(admission, "admission");
        this.directoryResolver = directoryResolver;
        this.directoryControl = Objects.requireNonNull(directoryControl, "directoryControl");
        this.sessionRegistry = Objects.requireNonNull(sessionRegistry, "sessionRegistry");
        this.capacityController = Objects.requireNonNull(capacityController,
                "capacityController");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.leaseDuration = Objects.requireNonNull(leaseDuration, "leaseDuration");
        if (leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
        long intervalMillis = maintenanceIntervalMillis(leaseDuration);
        maintenance = Executors.newSingleThreadScheduledExecutor(task ->
                Thread.ofPlatform().daemon(true)
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
        return sessionRegistry.connectedCount();
    }

    /** Cooperatively cancels the worker currently holding one delivery claim. */
    public synchronized boolean cancelDelivery(String deliveryId, String reason) {
        requireOpen();
        RemoteValidation.uuid(deliveryId, "delivery_id");
        if (reason == null || reason.isBlank() || reason.length() > 2_048) {
            throw new IllegalArgumentException(
                    "cancellation reason must contain 1 to 2048 characters");
        }
        var route = sessionRegistry.routeCancellation(deliveryId);
        if (route.isEmpty()) {
            return false;
        }
        Session session = sessions.get(route.orElseThrow().sessionId());
        if (session == null || session.closed) {
            return false;
        }
        var claim = route.orElseThrow().claim();
        session.cancellations.put(deliveryId, claim.leaseToken());
        send(session, CoordinatorFrame.newBuilder()
                .setClaimCancellation(ClaimCancellation.newBuilder()
                        .setDeliveryId(deliveryId)
                        .setLeaseToken(claim.leaseToken())
                        .setReason(reason)));
        return true;
    }

    /** Removes a worker from placement before asking it to drain live claims. */
    public synchronized boolean drainWorker(
            String workerId, String reason, Instant deadline) {
        requireOpen();
        RemoteValidation.workerId(workerId);
        Objects.requireNonNull(deadline, "deadline");
        if (reason == null || reason.isBlank() || reason.length() > 2_048) {
            throw new IllegalArgumentException(
                    "drain reason must contain 1 to 2048 characters");
        }
        Session session = sessions.values().stream()
                .filter(candidate -> candidate.workerId.equals(workerId))
                .findFirst().orElse(null);
        if (session == null || session.closed) {
            return false;
        }
        if (!deadline.isAfter(clock.instant())) {
            throw new IllegalArgumentException("drain deadline must be in the future");
        }
        session.draining = true;
        directoryControl.beginDrain(session.hello, reason);
        send(session, CoordinatorFrame.newBuilder()
                .setDrainRequest(WorkerDrainRequest.newBuilder()
                        .setReason(reason)
                        .setDeadline(RemoteValidation.timestamp(deadline))));
        return true;
    }

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
        for (Session session : List.copyOf(sessions.values())) {
            releaseClaims(sessionRegistry.disconnect(session.sessionId, clock.instant()),
                    "processor coordinator closed");
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
        WorkerSessionRegistry.OpenResult opened = decision.admitted()
                ? sessionRegistry.open(hello, decision.reconnectGrace(), clock.instant())
                : new WorkerSessionRegistry.OpenResult(false, decision.reason(), "", false,
                Map.of(), List.of());
        releaseClaims(opened.releases(), "worker session fence replaced or expired");
        boolean admitted = decision.admitted() && opened.admitted();
        String reason = decision.admitted() ? opened.reason() : decision.reason();
        Session session = new Session(hello.getWorkerId(), opened.sessionId(),
                hello, responses, admitted);
        send(session, CoordinatorFrame.newBuilder()
                .setAdmission(WorkerAdmission.newBuilder()
                        .setAdmitted(admitted)
                        .setReason(reason)
                        .setSessionId(admitted ? opened.sessionId() : "")
                        .setDirectoryGeneration(decision.directoryGeneration())
                        .setDirectoryEventSequence(decision.directoryEventSequence())));
        if (admitted) {
            sessions.put(session.sessionId, session);
            opened.claims().forEach((deliveryId, claim) ->
                    session.claims.put(deliveryId, claim.leaseToken()));
            if (decision.directoryGeneration() > 0) {
                send(session, CoordinatorFrame.newBuilder()
                        .setDirectoryAcknowledgement(
                                DirectoryRevisionAcknowledgement.newBuilder()
                                        .setGeneration(decision.directoryGeneration())
                                        .setEventSequence(decision.directoryEventSequence())));
            }
            if (opened.resumed()) {
                opened.claims().values().forEach(claim ->
                        send(session, CoordinatorFrame.newBuilder()
                                .setClaim(claim.claim())));
            }
        }
        return session;
    }

    private void handle(Session session, WorkerFrame frame) {
        if (!frame.getSessionId().equals(session.sessionId)
                || frame.getNodeIncarnationEpoch()
                != session.hello.getNodeIncarnationEpoch()) {
            throw new IllegalArgumentException(
                    "worker frame carries a stale session or node incarnation");
        }
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
                if (session.draining) {
                    throw new IllegalStateException(
                            "draining worker cannot request more claims");
                }
                session.demand += permits;
                dispatch();
            }
            case COMPLETED -> completed(session, frame.getCompleted());
            case FAILED -> failed(session, frame.getFailed());
            case HEARTBEAT -> heartbeat(session, frame);
            case CAPACITY -> capacity(session, frame.getCapacity());
            case DRAIN_PROGRESS -> drainProgress(session, frame);
            case CANCELLATION_ACKNOWLEDGEMENT -> cancellationAcknowledged(
                    session, frame.getCancellationAcknowledgement());
            case HELLO -> throw new IllegalArgumentException(
                    "hello may only be the first worker frame");
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException(
                    "worker frame requires a payload");
        }
    }

    private void heartbeat(Session session, WorkerFrame frame) {
        long sequence = frame.getHeartbeat().getHeartbeatSequence();
        if (sequence <= session.heartbeatSequence) {
            throw new IllegalArgumentException("worker heartbeat sequence must increase");
        }
        session.heartbeatSequence = sequence;
        directoryControl.heartbeat(session.hello, frame.getHeartbeat());
    }

    private void capacity(Session session, WorkerCapacity capacity) {
        if (capacity.getMaxInFlight() > MAX_DEMAND
                || capacity.getInFlight() > capacity.getMaxInFlight()) {
            throw new IllegalArgumentException(
                    "worker capacity is outside its declared bound");
        }
        session.capacity = capacity;
        directoryControl.capacity(session.hello, capacity);
        dispatch();
    }

    private void drainProgress(Session session, WorkerFrame frame) {
        if (!session.draining) {
            throw new IllegalArgumentException(
                    "worker reported drain progress without a drain request");
        }
        var progress = frame.getDrainProgress();
        directoryControl.drainProgress(session.hello, progress);
        if (progress.getActiveClaims() < session.claims.size()) {
            throw new IllegalArgumentException(
                    "worker drain progress omits owned claims");
        }
        if (progress.getDrained()) {
            disconnect(session, "worker drained");
            session.responses.onCompleted();
        }
    }

    private void completed(Session session, ProcessorCompletion completion) {
        requireClaim(session, completion.getDeliveryId(), completion.getLeaseToken());
        channel.complete(session.workerId, completion, clock.instant());
        session.claims.remove(completion.getDeliveryId());
        sessionRegistry.finished(session.sessionId, completion.getDeliveryId(),
                completion.getLeaseToken());
    }

    private void failed(Session session, ProcessorFailure failure) {
        requireClaim(session, failure.getDeliveryId(), failure.getLeaseToken());
        channel.fail(session.workerId, failure, clock.instant());
        session.claims.remove(failure.getDeliveryId());
        sessionRegistry.finished(session.sessionId, failure.getDeliveryId(),
                failure.getLeaseToken());
        dispatch();
    }

    private void cancellationAcknowledged(
            Session session, WorkerCancellationAcknowledgement acknowledgement) {
        String expected = session.cancellations.get(acknowledgement.getDeliveryId());
        if (expected == null || !expected.equals(acknowledgement.getLeaseToken())) {
            throw new IllegalArgumentException(
                    "cancellation acknowledgement has no matching live request");
        }
        requireClaim(session, acknowledgement.getDeliveryId(),
                acknowledgement.getLeaseToken());
        session.cancellations.remove(acknowledgement.getDeliveryId());
    }

    private static void requireClaim(
            Session session, String deliveryId, String leaseToken) {
        String token = session.claims.get(deliveryId);
        if (token == null || !token.equals(leaseToken)) {
            throw new IllegalArgumentException(
                    "worker outcome has no matching live claim");
        }
    }

    private void dispatch() {
        channel.expire(clock.instant());
        sessionRegistry.retainClaims(this::claimStillLive);
        for (Session session : List.copyOf(sessions.values())) {
            if (!session.admitted || session.demand == 0 || session.draining) {
                continue;
            }
            Map<String, Integer> processorPermits = processorPermits(session);
            int directoryCapacity = saturatedSum(processorPermits.values());
            int reportedMax = session.capacity == null
                    ? (directoryResolver == null ? MAX_DEMAND : 0)
                    : session.capacity.getMaxInFlight();
            int reportedInFlight = session.capacity == null
                    ? session.claims.size()
                    : Math.max(session.capacity.getInFlight(), session.claims.size());
            int queueDepth = session.capacity == null
                    ? 0 : session.capacity.getLocalQueueDepth();
            int effective = capacityController.effectivePermits(
                    session.demand,
                    directoryResolver == null ? MAX_DEMAND : directoryCapacity,
                    reportedMax, reportedInFlight, queueDepth, false);
            if (effective == 0) {
                continue;
            }
            var claims = channel.claim(
                    session.workerId, session.hello.getContractsList(), processorPermits,
                    effective, leaseDuration, clock.instant());
            for (DeliveryClaim claim : claims) {
                session.demand--;
                session.claims.put(
                        claim.getWork().getDeliveryId(), claim.getLeaseToken());
                sessionRegistry.claimed(session.sessionId, claim);
                try {
                    send(session, CoordinatorFrame.newBuilder().setClaim(claim));
                } catch (RuntimeException e) {
                    session.claims.remove(claim.getWork().getDeliveryId());
                    sessionRegistry.finished(session.sessionId,
                            claim.getWork().getDeliveryId(), claim.getLeaseToken());
                    channel.release(claim.getWork().getDeliveryId(), claim.getLeaseToken(),
                            "coordinator could not deliver claim", clock.instant());
                    disconnect(session, "coordinator response failed");
                    break;
                }
            }
        }
    }

    private Map<String, Integer> processorPermits(Session session) {
        Map<String, Integer> result = new LinkedHashMap<>();
        for (var contract : session.hello.getContractsList()) {
            int available = MAX_DEMAND;
            if (directoryResolver != null) {
                available = directoryResolver.resolve(
                                contract, 0, session.hello.getNodeId()).stream()
                        .filter(instance -> instance.nodeIncarnationEpoch()
                                == session.hello.getNodeIncarnationEpoch())
                        .filter(instance -> session.hello.getProcessorLeasesList().stream()
                                .anyMatch(lease -> lease.getProcessorId()
                                        .equals(contract.getProcessorId())
                                        && lease.getLeaseEpoch()
                                        == instance.processorLeaseEpoch()))
                        .mapToInt(DirectoryProcessorResolver.Instance::availableCapacity)
                        .max().orElse(0);
            }
            result.put(contract.getProcessorId(), available);
        }
        return result;
    }

    private boolean claimStillLive(WorkerSessionRegistry.ClaimRef claim) {
        return channel.delivery(claim.deliveryId())
                .filter(view -> view.state()
                        == DurableProcessorChannel.DeliveryState.CLAIMED)
                .filter(view -> view.claim().getLeaseToken().equals(claim.leaseToken()))
                .isPresent();
    }

    private void send(Session session, CoordinatorFrame.Builder frame) {
        if (!session.sessionId.isBlank()) {
            frame.setSessionId(session.sessionId)
                    .setNodeIncarnationEpoch(
                            session.hello.getNodeIncarnationEpoch());
        }
        session.responses.onNext(frame.setSequence(++session.sequence).build());
    }

    private synchronized void disconnect(Session session, String reason) {
        if (session == null || session.closed) {
            return;
        }
        session.closed = true;
        sessions.remove(session.sessionId, session);
        releaseClaims(sessionRegistry.disconnect(session.sessionId, clock.instant()), reason);
        session.claims.clear();
        if (!closed) {
            dispatch();
        }
    }

    private void releaseClaims(
            List<WorkerSessionRegistry.ClaimRef> claims, String reason) {
        for (var claim : claims) {
            try {
                channel.release(claim.deliveryId(), claim.leaseToken(), reason,
                        clock.instant());
            } catch (IllegalArgumentException | IllegalStateException ignored) {
                // The exact channel fence may already be expired or terminal.
            }
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
            releaseClaims(sessionRegistry.expire(clock.instant()),
                    "worker reconnect grace expired");
            dispatch();
        } catch (RuntimeException failure) {
            maintenanceFailure = failure;
            for (Session session : List.copyOf(sessions.values())) {
                session.closed = true;
                try {
                    session.responses.onError(Status.UNAVAILABLE
                            .withDescription("processor channel maintenance failed")
                            .withCause(failure).asRuntimeException());
                } catch (RuntimeException ignored) {
                    // Every connected worker is notified as far as its stream permits.
                }
            }
            sessions.clear();
        }
    }

    private static int saturatedSum(Iterable<Integer> values) {
        long sum = 0;
        for (int value : values) {
            sum = Math.min(MAX_DEMAND, sum + Math.max(0, value));
        }
        return (int) sum;
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
        private final String sessionId;
        private final WorkerHello hello;
        private final StreamObserver<CoordinatorFrame> responses;
        private final boolean admitted;
        private final Map<String, String> claims = new LinkedHashMap<>();
        private final Map<String, String> cancellations = new LinkedHashMap<>();
        private long sequence;
        private long heartbeatSequence;
        private int demand;
        private WorkerCapacity capacity;
        private boolean draining;
        private boolean closed;

        private Session(
                String workerId,
                String sessionId,
                WorkerHello hello,
                StreamObserver<CoordinatorFrame> responses,
                boolean admitted) {
            this.workerId = workerId;
            this.sessionId = sessionId;
            this.hello = hello;
            this.responses = responses;
            this.admitted = admitted;
        }
    }
}
