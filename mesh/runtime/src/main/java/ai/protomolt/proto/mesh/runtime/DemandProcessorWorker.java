package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorIdentity;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.v1.CoordinatorFrame;
import ai.protomolt.proto.mesh.runtime.v1.DeliveryClaim;
import ai.protomolt.proto.mesh.runtime.v1.DemandProcessorServiceGrpc;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorCompletion;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorFailure;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorWork;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorLeaseBinding;
import ai.protomolt.proto.mesh.runtime.v1.WorkerCapacity;
import ai.protomolt.proto.mesh.runtime.v1.WorkerDemand;
import ai.protomolt.proto.mesh.runtime.v1.WorkerDrainProgress;
import ai.protomolt.proto.mesh.runtime.v1.WorkerFrame;
import ai.protomolt.proto.mesh.runtime.v1.WorkerHeartbeat;
import ai.protomolt.proto.mesh.runtime.v1.WorkerHello;
import com.google.protobuf.DynamicMessage;
import io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Runs locally registered processors for claims pulled over the demand stream. */
public final class DemandProcessorWorker implements AutoCloseable {

    private final String workerId;
    private final String nodeId;
    private final long nodeIncarnationEpoch;
    private final String endpointId;
    private final Map<String, Long> processorLeaseEpochs;
    private final String resumeSessionId;
    private final int maxInFlight;
    private final DescriptorRegistry descriptors;
    private final ProcessorRegistry processors;
    private final PayloadResolver payloads;
    private final DemandProcessorServiceGrpc.DemandProcessorServiceStub stub;
    private final RemoteFailurePolicy failurePolicy;
    private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<String, ActiveInvocation> active = new LinkedHashMap<>();
    private final CountDownLatch admission = new CountDownLatch(1);
    private StreamObserver<WorkerFrame> requests;
    private long sentSequence;
    private long receivedSequence;
    private long heartbeatSequence;
    private boolean admissionReceived;
    private String sessionId = "";
    private boolean admitted;
    private boolean terminated;
    private boolean draining;
    private boolean closed;
    private Throwable streamFailure;

    public DemandProcessorWorker(
            String workerId,
            DescriptorRegistry descriptors,
            ProcessorRegistry processors,
            DemandProcessorServiceGrpc.DemandProcessorServiceStub stub) {
        this(workerId, descriptors, processors, PayloadResolver.inlineOnly(descriptors),
                stub, RemoteFailurePolicy.retryAll());
    }

    public DemandProcessorWorker(
            String workerId,
            DescriptorRegistry descriptors,
            ProcessorRegistry processors,
            PayloadResolver payloads,
            DemandProcessorServiceGrpc.DemandProcessorServiceStub stub,
            RemoteFailurePolicy failurePolicy) {
        this(workerId, descriptors, processors, payloads, stub, failurePolicy,
                workerId, 1, "grpc-main", Map.of(), "", 100_000);
    }

    /** Creates a directory-bound worker with explicit incarnation and processor leases. */
    public DemandProcessorWorker(
            String workerId,
            DescriptorRegistry descriptors,
            ProcessorRegistry processors,
            PayloadResolver payloads,
            DemandProcessorServiceGrpc.DemandProcessorServiceStub stub,
            RemoteFailurePolicy failurePolicy,
            String nodeId,
            long nodeIncarnationEpoch,
            String endpointId,
            Map<String, Long> processorLeaseEpochs,
            String resumeSessionId) {
        this(workerId, descriptors, processors, payloads, stub, failurePolicy,
                nodeId, nodeIncarnationEpoch, endpointId, processorLeaseEpochs,
                resumeSessionId, 100_000);
    }

    /** Creates a directory-bound worker with an explicit local concurrency ceiling. */
    public DemandProcessorWorker(
            String workerId,
            DescriptorRegistry descriptors,
            ProcessorRegistry processors,
            PayloadResolver payloads,
            DemandProcessorServiceGrpc.DemandProcessorServiceStub stub,
            RemoteFailurePolicy failurePolicy,
            String nodeId,
            long nodeIncarnationEpoch,
            String endpointId,
            Map<String, Long> processorLeaseEpochs,
            String resumeSessionId,
            int maxInFlight) {
        RemoteValidation.workerId(workerId);
        this.workerId = workerId;
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        if (nodeIncarnationEpoch < 1) {
            throw new IllegalArgumentException("nodeIncarnationEpoch must be positive");
        }
        this.nodeIncarnationEpoch = nodeIncarnationEpoch;
        this.endpointId = Objects.requireNonNull(endpointId, "endpointId");
        this.processorLeaseEpochs = Map.copyOf(processorLeaseEpochs);
        this.resumeSessionId = Objects.requireNonNull(resumeSessionId, "resumeSessionId");
        if (maxInFlight < 1 || maxInFlight > 100_000) {
            throw new IllegalArgumentException(
                    "maxInFlight must be between 1 and 100000");
        }
        this.maxInFlight = maxInFlight;
        this.descriptors = Objects.requireNonNull(descriptors, "descriptors");
        this.processors = Objects.requireNonNull(processors, "processors");
        this.payloads = Objects.requireNonNull(payloads, "payloads");
        this.stub = Objects.requireNonNull(stub, "stub");
        this.failurePolicy = Objects.requireNonNull(failurePolicy, "failurePolicy");
    }

    /** Opens the worker-initiated stream and advertises every local contract. */
    public synchronized void start() {
        if (closed) {
            throw new IllegalStateException("demand processor worker is closed");
        }
        if (requests != null) {
            throw new IllegalStateException("demand processor worker is already started");
        }
        Map<String, ai.protomolt.proto.mesh.runtime.v1.ProcessorContract> contracts =
                processors.contracts();
        WorkerHello.Builder hello = WorkerHello.newBuilder()
                .setWorkerId(workerId)
                .setNodeId(nodeId)
                .setNodeIncarnationEpoch(nodeIncarnationEpoch)
                .setEndpointId(endpointId)
                .addAllContracts(contracts.values())
                .setResumeSessionId(resumeSessionId);
        contracts.values().forEach(contract -> hello.addProcessorLeases(
                ProcessorLeaseBinding.newBuilder()
                        .setProcessorId(contract.getProcessorId())
                        .setLeaseEpoch(processorLeaseEpochs.getOrDefault(
                                contract.getProcessorId(), 1L))
                        .setContractFingerprint(contract.getContractFingerprint())));
        WorkerHello built = hello.build();
        RemoteValidation.hello(built, descriptors);
        requests = stub.connect(new CoordinatorObserver());
        send(WorkerFrame.newBuilder().setHello(built));
    }

    /** Waits for the coordinator's admission response. */
    public boolean awaitAdmission(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }
        long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException ignored) {
            timeoutNanos = Long.MAX_VALUE;
        }
        admission.await(timeoutNanos, TimeUnit.NANOSECONDS);
        return admitted;
    }

    /** Grants new work permits. No claim arrives without this explicit demand. */
    public synchronized void request(int permits) {
        if (!admitted || terminated || closed) {
            throw new IllegalStateException("worker has no admitted open stream");
        }
        if (draining) {
            throw new IllegalStateException("worker is draining");
        }
        if (permits < 1 || permits > maxInFlight) {
            throw new IllegalArgumentException("permits must be between 1 and 100000");
        }
        publishCapacity();
        send(WorkerFrame.newBuilder()
                .setDemand(WorkerDemand.newBuilder().setPermits(permits)));
    }

    /** Publishes one session-fenced liveness heartbeat. */
    public synchronized void heartbeat() {
        if (!admitted || terminated || closed) {
            throw new IllegalStateException("worker has no admitted open stream");
        }
        send(WorkerFrame.newBuilder().setHeartbeat(WorkerHeartbeat.newBuilder()
                .setHeartbeatSequence(++heartbeatSequence)
                .setObservedAt(RemoteValidation.timestamp(Instant.now()))));
    }

    public synchronized String sessionId() {
        return sessionId;
    }

    public synchronized int activeInvocations() {
        return active.size();
    }

    public synchronized Optional<Throwable> streamFailure() {
        return Optional.ofNullable(streamFailure);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (requests != null && !terminated) {
            requests.onCompleted();
        }
        tasks.shutdownNow();
    }

    private void receive(CoordinatorFrame frame) {
        synchronized (this) {
            RemoteValidation.annotations(frame);
            if (frame.getSequence() != receivedSequence + 1) {
                throw new IllegalArgumentException("coordinator frame sequence must be "
                        + (receivedSequence + 1) + " but was " + frame.getSequence());
            }
            receivedSequence = frame.getSequence();
            switch (frame.getPayloadCase()) {
                case ADMISSION -> {
                    if (admissionReceived) {
                        throw new IllegalArgumentException(
                                "coordinator sent more than one admission frame");
                    }
                    admissionReceived = true;
                    admitted = frame.getAdmission().getAdmitted();
                    sessionId = frame.getAdmission().getSessionId();
                    if (admitted && sessionId.isBlank()) {
                        throw new IllegalArgumentException(
                                "admitted worker requires a session_id");
                    }
                    admission.countDown();
                    if (!admitted) {
                        terminated = true;
                    } else {
                        publishCapacity();
                    }
                }
                case CLAIM -> {
                    requireSession(frame);
                    accept(frame.getClaim());
                }
                case CLAIM_CANCELLATION -> {
                    requireSession(frame);
                    cancel(frame.getClaimCancellation());
                }
                case DRAIN_REQUEST -> {
                    requireSession(frame);
                    drain(frame.getDrainRequest());
                }
                case DIRECTORY_ACKNOWLEDGEMENT -> requireSession(frame);
                case PAYLOAD_NOT_SET -> throw new IllegalArgumentException(
                        "coordinator frame requires a payload");
            }
        }
    }

    private void accept(DeliveryClaim claim) {
        if (!admitted) {
            throw new IllegalArgumentException("claim arrived before worker admission");
        }
        RemoteValidation.claim(claim, descriptors);
        if (!claim.getWorkerId().equals(workerId)) {
            throw new IllegalArgumentException("claim is addressed to worker "
                    + claim.getWorkerId() + " instead of " + workerId);
        }
        ProcessorWork work = claim.getWork();
        if (!RemoteValidation.supports(
                processors.contracts().values(), work.getContract())) {
            throw new IllegalArgumentException("claim contract is not advertised by worker: "
                    + work.getContract().getProcessorId());
        }
        if (draining) {
            throw new IllegalStateException("draining worker cannot accept a new claim");
        }
        if (active.size() >= maxInFlight) {
            throw new IllegalStateException("worker received work beyond max_in_flight");
        }
        ActiveInvocation invocation = new ActiveInvocation(
                claim, new ProcessorCancellation());
        if (active.putIfAbsent(work.getDeliveryId(), invocation) != null) {
            throw new IllegalArgumentException("worker already holds delivery "
                    + work.getDeliveryId());
        }
        Future<?> future = tasks.submit(() -> execute(invocation));
        invocation.future = future;
        publishCapacity();
    }

    private void execute(ActiveInvocation invocation) {
        DeliveryClaim claim = invocation.claim;
        ProcessorWork work = claim.getWork();
        try {
            ProcessorInvoker invoker = processors.find(work.getContract().getProcessorId())
                    .orElseThrow(() -> new IllegalStateException(
                            "advertised processor disappeared: "
                                    + work.getContract().getProcessorId()));
            DynamicMessage input = Objects.requireNonNull(payloads.resolve(work.getInput()),
                    "payload resolver returned null");
            DescriptorIdentity expected = RuntimeSchemas.identity(work.getInput().getSchema());
            DescriptorIdentity actual = DescriptorIdentity.of(input.getDescriptorForType());
            if (!expected.equals(actual)) {
                throw new IllegalArgumentException("payload resolver returned " + actual
                        + " for remote work schema " + expected);
            }
            ProcessorContext context = new ProcessorContext(
                    work.getRunId(), work.getNodeId(), work.getInvocationId(),
                    work.getInvocationOrdinal(), RemoteValidation.instant(work.getDeadline()),
                    invocation.cancellation);
            invocation.cancellation.throwIfRequested();
            ProcessorInvocationResult result = invoker.invoke(
                    new ProcessorInvocation(context, work.getInput(), input));
            invocation.cancellation.throwIfRequested();
            if (!result.settlement().deliveryId().isEmpty()) {
                throw new IllegalArgumentException(
                        "a demand worker may execute only local processors");
            }
            result.settlement().settle();
            if (invocation.outcomeSent.compareAndSet(false, true)) {
                send(WorkerFrame.newBuilder()
                        .setCompleted(ProcessorCompletion.newBuilder()
                                .setDeliveryId(work.getDeliveryId())
                                .setLeaseToken(claim.getLeaseToken())
                                .setCompletionId(completionId(claim))
                                .addAllOutputs(result.outputs())));
            }
        } catch (Throwable failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            String message = failure.getMessage() == null
                    ? failure.getClass().getSimpleName() : failure.getMessage();
            if (message.length() > 8_192) {
                message = message.substring(0, 8_192);
            }
            if (invocation.outcomeSent.compareAndSet(false, true)) {
                try {
                    sendFailure(invocation, "processor-failed", message,
                            failurePolicy.retryable(failure));
                } catch (RuntimeException sendFailure) {
                    failure.addSuppressed(sendFailure);
                    failStream(failure);
                }
            }
        } finally {
            synchronized (this) {
                active.remove(work.getDeliveryId());
                if (admitted && !terminated && !closed) {
                    publishCapacity();
                    if (draining) {
                        publishDrainProgress();
                    } else {
                    send(WorkerFrame.newBuilder()
                            .setDemand(WorkerDemand.newBuilder().setPermits(1)));
                    }
                }
            }
        }
    }

    private synchronized void send(WorkerFrame.Builder frame) {
        if (requests == null || terminated || closed) {
            throw new IllegalStateException("worker stream is not open");
        }
        if (!sessionId.isBlank() && !frame.hasHello()) {
            frame.setSessionId(sessionId)
                    .setNodeIncarnationEpoch(nodeIncarnationEpoch);
        }
        requests.onNext(frame
                .setFrameId(UUID.randomUUID().toString())
                .setSequence(++sentSequence)
                .build());
    }

    private void requireSession(CoordinatorFrame frame) {
        if (!frame.getSessionId().equals(sessionId)
                || frame.getNodeIncarnationEpoch() != nodeIncarnationEpoch) {
            throw new IllegalArgumentException(
                    "coordinator frame carries a stale session or node incarnation");
        }
    }

    private void cancel(ai.protomolt.proto.mesh.runtime.v1.ClaimCancellation cancellation) {
        ActiveInvocation invocation = active.get(cancellation.getDeliveryId());
        if (invocation == null
                || !invocation.claim.getLeaseToken().equals(cancellation.getLeaseToken())) {
            throw new IllegalArgumentException(
                    "claim cancellation has no matching active delivery");
        }
        invocation.cancellation.request(cancellation.getReason());
        send(WorkerFrame.newBuilder().setCancellationAcknowledgement(
                ai.protomolt.proto.mesh.runtime.v1.WorkerCancellationAcknowledgement.newBuilder()
                        .setDeliveryId(cancellation.getDeliveryId())
                        .setLeaseToken(cancellation.getLeaseToken())
                        .setInterrupted(true)));
        if (invocation.outcomeSent.compareAndSet(false, true)) {
            sendFailure(invocation, "processor-cancelled",
                    cancellation.getReason(), false);
        }
        Future<?> future = invocation.future;
        if (future != null) {
            future.cancel(true);
        }
    }

    private void drain(ai.protomolt.proto.mesh.runtime.v1.WorkerDrainRequest request) {
        draining = true;
        publishCapacity();
        publishDrainProgress();
    }

    private void publishCapacity() {
        send(WorkerFrame.newBuilder().setCapacity(WorkerCapacity.newBuilder()
                .setMaxInFlight(maxInFlight)
                .setInFlight(active.size())
                .setLocalQueueDepth(0)
                .setDraining(draining)));
    }

    private void publishDrainProgress() {
        send(WorkerFrame.newBuilder().setDrainProgress(
                WorkerDrainProgress.newBuilder()
                        .setActiveClaims(active.size())
                        .setDrained(active.isEmpty())));
    }

    private void sendFailure(
            ActiveInvocation invocation,
            String code,
            String message,
            boolean retryable) {
        var work = invocation.claim.getWork();
        var outcome = "processor-cancelled".equals(code)
                ? ProcessorOutcomes.cancelled(message,
                work.getContract().getProcessorId(), invocation.claim.getAttempt())
                : retryable
                ? ProcessorOutcomes.retryable(code, message,
                work.getContract().getProcessorId(), invocation.claim.getAttempt(),
                work.getMaxAttempts())
                : ProcessorOutcomes.permanent(code, message,
                work.getContract().getProcessorId(), invocation.claim.getAttempt());
        send(WorkerFrame.newBuilder()
                .setFailed(ProcessorFailure.newBuilder()
                        .setDeliveryId(invocation.claim.getWork().getDeliveryId())
                        .setLeaseToken(invocation.claim.getLeaseToken())
                        .setCode(code)
                        .setMessage(message)
                        .setCompletionId(completionId(invocation.claim))
                        .setOutcome(outcome)));
    }

    private static String completionId(DeliveryClaim claim) {
        return EntityEnvelopes.stableUuid("worker-completion\0"
                + claim.getWork().getDeliveryId() + '\0' + claim.getLeaseToken());
    }

    private synchronized void failStream(Throwable failure) {
        streamFailure = failure;
        terminated = true;
        admission.countDown();
        tasks.shutdownNow();
        if (requests != null) {
            requests.onError(failure);
        }
    }

    private final class CoordinatorObserver implements StreamObserver<CoordinatorFrame> {
        @Override
        public void onNext(CoordinatorFrame frame) {
            try {
                receive(frame);
            } catch (RuntimeException e) {
                failStream(e);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            synchronized (DemandProcessorWorker.this) {
                streamFailure = throwable;
                terminated = true;
                admission.countDown();
                tasks.shutdownNow();
            }
        }

        @Override
        public void onCompleted() {
            synchronized (DemandProcessorWorker.this) {
                terminated = true;
                admission.countDown();
                tasks.shutdownNow();
            }
        }
    }

    private static final class ActiveInvocation {
        private final DeliveryClaim claim;
        private final ProcessorCancellation cancellation;
        private final AtomicBoolean outcomeSent = new AtomicBoolean();
        private volatile Future<?> future;

        private ActiveInvocation(
                DeliveryClaim claim, ProcessorCancellation cancellation) {
            this.claim = claim;
            this.cancellation = cancellation;
        }
    }
}
