package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.test.RawInput;
import ai.protomolt.proto.mesh.runtime.test.RuntimeTestProto;
import ai.protomolt.proto.mesh.runtime.test.Token;
import ai.protomolt.proto.mesh.runtime.v1.DeliveryClaim;
import ai.protomolt.proto.mesh.runtime.v1.CoordinatorFrame;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorCompletion;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorFailure;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorWork;
import ai.protomolt.proto.mesh.runtime.v1.WorkerDemand;
import ai.protomolt.proto.mesh.runtime.v1.WorkerFrame;
import ai.protomolt.proto.mesh.runtime.v1.WorkerHello;
import ai.protomolt.proto.mesh.v1.CompletionPolicy;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileDurableProcessorChannelTest {

    private static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");
    private static final String DELIVERY = "9c91bdf8-244b-4d31-860e-a0a0a93e33dd";
    private static final String RUN = "9b06a1fd-1eb4-4149-8f10-92a879510af2";
    private static final String INVOCATION = "480760f2-0d87-42dc-b46b-e42b83358f00";
    private static final String INPUT = "5cb5ad7d-a497-4bec-bb69-e33036ed8d66";
    private static final String SCOPE = "a2afcaf1-e96f-4519-a20d-49530ac905a5";

    @TempDir
    Path temporary;

    private DescriptorRegistry descriptors;
    private ProcessorContract contract;
    private ProcessorWork work;
    private Clock clock;

    @BeforeEach
    void setUp() {
        descriptors = DescriptorRegistry.create(false);
        descriptors.registerFile(RuntimeTestProto.getDescriptor());
        contract = ProcessorContract.newBuilder()
                .setProcessorId("remote-tokenizer")
                .setInputSchema(EntityEnvelopes.schemaOf(RawInput.getDefaultInstance()))
                .addOutputSchemas(EntityEnvelopes.schemaOf(Token.getDefaultInstance()))
                .setMaxOutputs(2)
                .build();
        var input = EntityEnvelopes.root(INPUT, SCOPE,
                RawInput.newBuilder().setText("durable").build(), NOW,
                NOW.plusSeconds(3600), CompletionPolicy.COMPLETION_POLICY_STRICT);
        work = ProcessorWork.newBuilder()
                .setDeliveryId(DELIVERY)
                .setRunId(RUN)
                .setNodeId("remote_node")
                .setInvocationId(INVOCATION)
                .setInvocationOrdinal(1)
                .setContract(contract)
                .setInput(input)
                .setDeadline(RemoteValidation.timestamp(NOW.plusSeconds(600)))
                .setMaxAttempts(3)
                .build();
        clock = Clock.fixed(NOW, ZoneOffset.UTC);
    }

    @Test
    void completedAndSettledDeliverySurvivesWalRestart() {
        Path path = temporary.resolve("processor.wal");
        String lease;
        try (var channel = new FileDurableProcessorChannel(path, descriptors, clock)) {
            channel.enqueue(work);
            DeliveryClaim claim = channel.claim("worker-a", List.of(contract), 1,
                    Duration.ofSeconds(60), NOW).getFirst();
            lease = claim.getLeaseToken();
            channel.complete("worker-a", completion(lease), NOW);
            assertThat(channel.delivery(DELIVERY).orElseThrow().state())
                    .isEqualTo(DurableProcessorChannel.DeliveryState.COMPLETED);
        }

        try (var recovered = new FileDurableProcessorChannel(path, descriptors, clock)) {
            assertThat(recovered.awaitCompletion(DELIVERY, NOW.plusSeconds(1))
                    .completion().getOutputsCount()).isEqualTo(1);
            recovered.settle(DELIVERY, lease, NOW);
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }

        try (var recovered = new FileDurableProcessorChannel(path, descriptors, clock)) {
            assertThat(recovered.delivery(DELIVERY).orElseThrow().state())
                    .isEqualTo(DurableProcessorChannel.DeliveryState.SETTLED);
            assertThat(recovered.records()).hasSize(4);
        }
    }

    @Test
    void downstreamReleaseRequeuesAndFencesTheOldLease() {
        Path path = temporary.resolve("processor.wal");
        try (var channel = new FileDurableProcessorChannel(path, descriptors, clock)) {
            channel.enqueue(work);
            DeliveryClaim first = channel.claim("worker-a", List.of(contract), 1,
                    Duration.ofSeconds(60), NOW).getFirst();
            channel.complete("worker-a", completion(first.getLeaseToken()), NOW);
            channel.release(DELIVERY, first.getLeaseToken(), "downstream refused", NOW);

            DeliveryClaim second = channel.claim("worker-b", List.of(contract), 1,
                    Duration.ofSeconds(60), NOW).getFirst();
            assertThat(second.getAttempt()).isEqualTo(2);
            assertThatThrownBy(() -> channel.complete(
                    "worker-a", completion(first.getLeaseToken()), NOW))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("stale lease_token");
        }
    }

    @Test
    void retryableFailureStopsAtTheDeclaredAttemptLimit() {
        Path path = temporary.resolve("processor.wal");
        ProcessorWork oneAttempt = work.toBuilder().setMaxAttempts(1).build();
        try (var channel = new FileDurableProcessorChannel(path, descriptors, clock)) {
            channel.enqueue(oneAttempt);
            DeliveryClaim claim = channel.claim("worker-a", List.of(contract), 1,
                    Duration.ofSeconds(60), NOW).getFirst();
            channel.fail("worker-a", ProcessorFailure.newBuilder()
                    .setDeliveryId(DELIVERY)
                    .setLeaseToken(claim.getLeaseToken())
                    .setCode("model-failed")
                    .setMessage("model refused input")
                    .setRetryable(true)
                    .build(), NOW);

            assertThat(channel.delivery(DELIVERY).orElseThrow().state())
                    .isEqualTo(DurableProcessorChannel.DeliveryState.FAILED);
            assertThatThrownBy(() -> channel.awaitCompletion(DELIVERY, NOW.plusSeconds(1)))
                    .isInstanceOf(RemoteProcessorException.class)
                    .hasMessageContaining("model refused input");
        }
    }

    @Test
    void conflictingIdempotencyKeyIsRefused() {
        Path path = temporary.resolve("processor.wal");
        try (var channel = new FileDurableProcessorChannel(path, descriptors, clock)) {
            channel.enqueue(work);
            assertThat(channel.enqueue(work).state())
                    .isEqualTo(DurableProcessorChannel.DeliveryState.PENDING);
            assertThatThrownBy(() -> channel.enqueue(work.toBuilder()
                    .setNodeId("another_node").build()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("conflicting work");
        }
    }

    @Test
    void completionWithAnUndeclaredExactSchemaIsRefused() {
        Path path = temporary.resolve("processor.wal");
        try (var channel = new FileDurableProcessorChannel(path, descriptors, clock)) {
            channel.enqueue(work);
            DeliveryClaim claim = channel.claim("worker-a", List.of(contract), 1,
                    Duration.ofSeconds(60), NOW).getFirst();
            ProcessorCompletion invalid = ProcessorCompletion.newBuilder()
                    .setDeliveryId(DELIVERY)
                    .setLeaseToken(claim.getLeaseToken())
                    .addOutputs(RuntimeSchemas.pack(
                            RawInput.newBuilder().setText("wrong schema").build()))
                    .build();

            assertThatThrownBy(() -> channel.complete("worker-a", invalid, NOW))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("undeclared schema");
            assertThat(channel.delivery(DELIVERY).orElseThrow().state())
                    .isEqualTo(DurableProcessorChannel.DeliveryState.CLAIMED);
        }
    }

    @Test
    void recoveryTruncatesOnlyAnIncompleteTail() throws Exception {
        Path path = temporary.resolve("processor.wal");
        long validSize;
        try (var channel = new FileDurableProcessorChannel(path, descriptors, clock)) {
            channel.enqueue(work);
        }
        validSize = Files.size(path);
        Files.write(path, new byte[]{0x01, 0x02}, StandardOpenOption.APPEND);

        try (var recovered = new FileDurableProcessorChannel(path, descriptors, clock)) {
            assertThat(recovered.records()).hasSize(1);
        }
        assertThat(Files.size(path)).isEqualTo(validSize);
    }

    @Test
    void recoveryRefusesAChecksummedFrameWhoseBytesWereCorrupted() throws Exception {
        Path path = temporary.resolve("processor.wal");
        try (var channel = new FileDurableProcessorChannel(path, descriptors, clock)) {
            channel.enqueue(work);
        }
        byte[] bytes = Files.readAllBytes(path);
        bytes[bytes.length - 5] ^= 0x01;
        Files.write(path, bytes);

        assertThatThrownBy(() -> new FileDurableProcessorChannel(path, descriptors, clock))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CRC mismatch");
    }

    @Test
    void aSecondWriterCannotOpenTheSameChannel() {
        Path path = temporary.resolve("processor.wal");
        try (var channel = new FileDurableProcessorChannel(path, descriptors, clock)) {
            assertThatThrownBy(() ->
                    new FileDurableProcessorChannel(path, descriptors, clock))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already open");
        }
    }

    @Test
    void coordinatorMaintenanceRedispatchesAnExpiredLeaseToWaitingDemand()
            throws Exception {
        Path path = temporary.resolve("maintenance.wal");
        MutableClock mutableClock = new MutableClock(NOW);
        try (var channel = new FileDurableProcessorChannel(
                path, descriptors, mutableClock);
             var coordinator = new DemandProcessorCoordinator(
                     descriptors, channel, RemoteWorkerAdmission.allowAll(),
                     mutableClock, Duration.ofMillis(40))) {
            channel.enqueue(work);
            channel.claim("worker-a", List.of(contract), 1,
                    Duration.ofMillis(10), NOW).getFirst();

            var responses = new LinkedBlockingQueue<CoordinatorFrame>();
            StreamObserver<WorkerFrame> requests = coordinator.connect(
                    new StreamObserver<>() {
                        @Override
                        public void onNext(CoordinatorFrame frame) {
                            responses.add(frame);
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            throw new AssertionError(throwable);
                        }

                        @Override
                        public void onCompleted() {
                        }
                    });
            requests.onNext(workerFrame(1).setHello(WorkerHello.newBuilder()
                    .setWorkerId("worker-b")
                    .addContracts(contract)).build());
            assertThat(responses.poll(1, TimeUnit.SECONDS)).satisfies(
                    frame -> assertThat(frame.getAdmission().getAdmitted()).isTrue());
            requests.onNext(workerFrame(2).setDemand(
                    WorkerDemand.newBuilder().setPermits(1)).build());
            assertThat(responses.poll(50, TimeUnit.MILLISECONDS)).isNull();

            mutableClock.advance(Duration.ofMillis(20));

            CoordinatorFrame reassigned = responses.poll(2, TimeUnit.SECONDS);
            assertThat(reassigned).isNotNull();
            assertThat(reassigned.getClaim().getWorkerId()).isEqualTo("worker-b");
            assertThat(reassigned.getClaim().getAttempt()).isEqualTo(2);
            assertThat(coordinator.maintenanceFailure()).isEmpty();
        }
    }

    private ProcessorCompletion completion(String leaseToken) {
        return ProcessorCompletion.newBuilder()
                .setDeliveryId(DELIVERY)
                .setLeaseToken(leaseToken)
                .addOutputs(RuntimeSchemas.pack(
                        Token.newBuilder().setText("durable-token").build()))
                .build();
    }

    private static WorkerFrame.Builder workerFrame(long sequence) {
        return WorkerFrame.newBuilder()
                .setFrameId(UUID.randomUUID().toString())
                .setSequence(sequence);
    }

    private static final class MutableClock extends Clock {
        private final AtomicReference<Instant> now;

        private MutableClock(Instant now) {
            this.now = new AtomicReference<>(now);
        }

        private void advance(Duration duration) {
            now.updateAndGet(value -> value.plus(duration));
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            if (!ZoneOffset.UTC.equals(zone)) {
                throw new IllegalArgumentException("test clock supports UTC only");
            }
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }
    }
}
