package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorIdentity;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.v1.CoordinatorFrame;
import ai.protomolt.proto.mesh.runtime.v1.DeliveryClaim;
import ai.protomolt.proto.mesh.runtime.v1.DemandProcessorServiceGrpc;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorCompletion;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorFailure;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorWork;
import ai.protomolt.proto.mesh.runtime.v1.WorkerDemand;
import ai.protomolt.proto.mesh.runtime.v1.WorkerFrame;
import ai.protomolt.proto.mesh.runtime.v1.WorkerHello;
import com.google.protobuf.DynamicMessage;
import io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Runs locally registered processors for claims pulled over the demand stream. */
public final class DemandProcessorWorker implements AutoCloseable {

    private final String workerId;
    private final DescriptorRegistry descriptors;
    private final ProcessorRegistry processors;
    private final PayloadResolver payloads;
    private final DemandProcessorServiceGrpc.DemandProcessorServiceStub stub;
    private final RemoteFailurePolicy failurePolicy;
    private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
    private final Set<String> active = new HashSet<>();
    private final CountDownLatch admission = new CountDownLatch(1);
    private StreamObserver<WorkerFrame> requests;
    private long sentSequence;
    private long receivedSequence;
    private boolean admissionReceived;
    private boolean admitted;
    private boolean terminated;
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
        RemoteValidation.workerId(workerId);
        this.workerId = workerId;
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
        WorkerHello hello = WorkerHello.newBuilder()
                .setWorkerId(workerId)
                .addAllContracts(contracts.values())
                .build();
        RemoteValidation.hello(hello, descriptors);
        requests = stub.connect(new CoordinatorObserver());
        send(WorkerFrame.newBuilder().setHello(hello));
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
        if (permits < 1 || permits > 100_000) {
            throw new IllegalArgumentException("permits must be between 1 and 100000");
        }
        send(WorkerFrame.newBuilder()
                .setDemand(WorkerDemand.newBuilder().setPermits(permits)));
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
                    admission.countDown();
                    if (!admitted) {
                        terminated = true;
                    }
                }
                case CLAIM -> accept(frame.getClaim());
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
        if (!active.add(work.getDeliveryId())) {
            throw new IllegalArgumentException("worker already holds delivery "
                    + work.getDeliveryId());
        }
        tasks.submit(() -> execute(claim));
    }

    private void execute(DeliveryClaim claim) {
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
                    work.getInvocationOrdinal(), RemoteValidation.instant(work.getDeadline()));
            ProcessorInvocationResult result = invoker.invoke(
                    new ProcessorInvocation(context, work.getInput(), input));
            if (!result.settlement().deliveryId().isEmpty()) {
                throw new IllegalArgumentException(
                        "a demand worker may execute only local processors");
            }
            result.settlement().settle();
            send(WorkerFrame.newBuilder()
                    .setCompleted(ProcessorCompletion.newBuilder()
                            .setDeliveryId(work.getDeliveryId())
                            .setLeaseToken(claim.getLeaseToken())
                            .addAllOutputs(result.outputs())));
        } catch (Throwable failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            String message = failure.getMessage() == null
                    ? failure.getClass().getSimpleName() : failure.getMessage();
            if (message.length() > 8_192) {
                message = message.substring(0, 8_192);
            }
            try {
                send(WorkerFrame.newBuilder()
                        .setFailed(ProcessorFailure.newBuilder()
                                .setDeliveryId(work.getDeliveryId())
                                .setLeaseToken(claim.getLeaseToken())
                                .setCode("processor-failed")
                                .setMessage(message)
                                .setRetryable(failurePolicy.retryable(failure))));
            } catch (RuntimeException sendFailure) {
                failure.addSuppressed(sendFailure);
                failStream(failure);
            }
        } finally {
            synchronized (this) {
                active.remove(work.getDeliveryId());
                if (admitted && !terminated && !closed) {
                    send(WorkerFrame.newBuilder()
                            .setDemand(WorkerDemand.newBuilder().setPermits(1)));
                }
            }
        }
    }

    private synchronized void send(WorkerFrame.Builder frame) {
        if (requests == null || terminated || closed) {
            throw new IllegalStateException("worker stream is not open");
        }
        requests.onNext(frame
                .setFrameId(UUID.randomUUID().toString())
                .setSequence(++sentSequence)
                .build());
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
}
