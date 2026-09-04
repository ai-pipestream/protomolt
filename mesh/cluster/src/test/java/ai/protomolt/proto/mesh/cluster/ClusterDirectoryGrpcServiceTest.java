package ai.protomolt.proto.mesh.cluster;

import ai.protomolt.proto.mesh.cluster.ClusterFixtures.MutableClock;
import ai.protomolt.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.ClusterDirectoryServiceGrpc;
import ai.protomolt.proto.mesh.cluster.v1.DirectoryCursor;
import ai.protomolt.proto.mesh.cluster.v1.DirectoryFrame;
import ai.protomolt.proto.mesh.cluster.v1.GetSnapshotRequest;
import ai.protomolt.proto.mesh.cluster.v1.HeartbeatRequest;
import ai.protomolt.proto.mesh.cluster.v1.PresenceState;
import ai.protomolt.proto.mesh.cluster.v1.ProcessorReadinessOverlay;
import ai.protomolt.proto.mesh.cluster.v1.RegisterNodeRequest;
import ai.protomolt.proto.mesh.cluster.v1.RegisterProcessorRequest;
import ai.protomolt.proto.mesh.cluster.v1.SweepRequest;
import ai.protomolt.proto.mesh.cluster.v1.UpdateCapacityRequest;
import ai.protomolt.proto.mesh.cluster.v1.UpdateReadinessRequest;
import ai.protomolt.proto.mesh.cluster.v1.WatchDirectoryRequest;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterDirectoryGrpcServiceTest {

    private MutableClock clock;
    private PersistentClusterDirectory directory;
    private Server server;
    private ManagedChannel channel;
    private ClusterDirectoryServiceGrpc.ClusterDirectoryServiceBlockingStub blocking;
    private ClusterDirectoryServiceGrpc.ClusterDirectoryServiceStub async;

    @BeforeEach
    void start() throws Exception {
        clock = new MutableClock(ClusterFixtures.T0);
        directory = new PersistentClusterDirectory(ClusterFixtures.cluster(), clock,
                new InMemoryClusterEventRepository());
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new ClusterDirectoryGrpcService(directory)).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        blocking = ClusterDirectoryServiceGrpc.newBlockingStub(channel);
        async = ClusterDirectoryServiceGrpc.newStub(channel);
    }

    @AfterEach
    void stop() throws Exception {
        channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
    }

    @Test
    void everyUnaryRpcReducesAgainstTheSamePersistentDirectory() {
        var nodeCommit = blocking.registerNode(RegisterNodeRequest.newBuilder()
                .setAdvertisement(ClusterFixtures.node("node-1")).build()).getCommit();
        assertThat(nodeCommit.getSnapshotSeq()).isEqualTo(1);

        var heartbeatCommit = blocking.heartbeat(HeartbeatRequest.newBuilder()
                .setPresence(ClusterFixtures.presenceBuilder("node-1", 2).build()).build())
                .getCommit();
        assertThat(heartbeatCommit.getSnapshotSeq()).isEqualTo(1);

        var processor = ClusterFixtures.processorBuilder("proc-1", "node-1").build();
        blocking.registerProcessor(RegisterProcessorRequest.newBuilder()
                .setAdvertisement(processor).build());
        CapacityAdvertisement capacity = ClusterFixtures.capacityBuilder("node-1", 1)
                .setProcessorId("proc-1").build();
        blocking.updateCapacity(UpdateCapacityRequest.newBuilder()
                .setCapacity(capacity).build());
        ProcessorReadinessOverlay readiness = ProcessorReadinessOverlay.newBuilder()
                .setProcessorId("proc-1")
                .setNodeId("node-1")
                .setNodeEpoch(1)
                .setProcessorLeaseEpoch(1)
                .setReady(false)
                .setReason("maintenance")
                .setRevision(1)
                .setUpdatedAt(ClusterFixtures.ts(clock.instant()))
                .build();
        blocking.updateReadiness(UpdateReadinessRequest.newBuilder()
                .setReadiness(readiness).build());

        var snapshot = blocking.getSnapshot(GetSnapshotRequest.getDefaultInstance())
                .getSnapshot();
        assertThat(snapshot).isEqualTo(directory.snapshot());
        assertThat(snapshot.getReadinessOverlaysList()).containsExactly(readiness);
        assertThat(directory.eligibleProcessors("", "", "")).isEmpty();

        clock.advance(Duration.ofSeconds(61));
        assertThat(blocking.sweep(SweepRequest.getDefaultInstance()).getEventsCount())
                .isEqualTo(2);
    }

    @Test
    void staleFenceIsAbortedAndMalformedContractIsInvalidArgument() {
        blocking.registerNode(RegisterNodeRequest.newBuilder()
                .setAdvertisement(ClusterFixtures.node("node-1")).build());

        assertThatThrownBy(() -> blocking.registerNode(RegisterNodeRequest.newBuilder()
                .setAdvertisement(ClusterFixtures.nodeBuilder("node-1", 1, 1)
                        .addCapabilities("changed-at-stale-position").build()).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, error ->
                        assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.ABORTED));

        assertThatThrownBy(() -> blocking.registerProcessor(RegisterProcessorRequest.newBuilder()
                .setAdvertisement(ClusterFixtures.processorBuilder("proc-1", "node-1")
                        .clearContract().build()).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, error ->
                        assertThat(error.getStatus().getCode())
                                .isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    @Test
    void watchStartsWithSnapshotAndContinuesInExactEventOrder() throws Exception {
        RecordingObserver observer = new RecordingObserver();
        async.watchDirectory(WatchDirectoryRequest.getDefaultInstance(), observer);

        DirectoryFrame initial = observer.next();
        assertThat(initial.hasSnapshot()).isTrue();
        assertThat(initial.getSnapshot().getCursor().getGeneration()).isEqualTo(1);
        assertThat(initial.getSnapshot().getUnfilteredSnapshotSequence()).isZero();

        blocking.registerNode(RegisterNodeRequest.newBuilder()
                .setAdvertisement(ClusterFixtures.node("node-1")).build());
        blocking.registerProcessor(RegisterProcessorRequest.newBuilder()
                .setAdvertisement(ClusterFixtures.processorBuilder("proc-1", "node-1").build())
                .build());
        blocking.heartbeat(HeartbeatRequest.newBuilder()
                .setPresence(ClusterFixtures.presenceBuilder("node-1", 2)
                        .setState(PresenceState.PRESENCE_STATE_DRAINING).build()).build());

        DirectoryFrame first = observer.next();
        DirectoryFrame second = observer.next();
        DirectoryFrame third = observer.next();
        assertThat(first.getEvent().getCursor().getEventSequence()).isEqualTo(1);
        assertThat(second.getEvent().getCursor().getEventSequence()).isEqualTo(2);
        assertThat(third.getEvent().getCursor().getEventSequence()).isEqualTo(3);
        assertThat(third.getEvent().getEvent().getPresence().getState())
                .isEqualTo(PresenceState.PRESENCE_STATE_DRAINING);
    }

    @Test
    void reconnectFromCursorHasNoDuplicateOrGap() throws Exception {
        blocking.registerNode(RegisterNodeRequest.newBuilder()
                .setAdvertisement(ClusterFixtures.node("node-1")).build());
        RecordingObserver observer = new RecordingObserver();
        async.watchDirectory(WatchDirectoryRequest.newBuilder()
                .setAfter(DirectoryCursor.newBuilder().setGeneration(1).setEventSequence(0))
                .build(), observer);
        assertThat(observer.next().getEvent().getCursor().getEventSequence()).isEqualTo(1);

        blocking.registerProcessor(RegisterProcessorRequest.newBuilder()
                .setAdvertisement(ClusterFixtures.processorBuilder("proc-1", "node-1").build())
                .build());
        assertThat(observer.next().getEvent().getCursor().getEventSequence()).isEqualTo(2);
    }

    @Test
    void compactedCursorGetsNamedResyncInsteadOfPartialTail() throws Exception {
        var events = new InMemoryClusterEventRepository();
        var compacting = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), clock, events, 1);
        compacting.register(ClusterFixtures.node("node-1"));
        compacting.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1").build());
        String name = InProcessServerBuilder.generateName();
        Server compactServer = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new ClusterDirectoryGrpcService(compacting)).build().start();
        ManagedChannel compactChannel = InProcessChannelBuilder.forName(name)
                .directExecutor().build();
        try {
            RecordingObserver observer = new RecordingObserver();
            ClusterDirectoryServiceGrpc.newStub(compactChannel).watchDirectory(
                    WatchDirectoryRequest.newBuilder()
                            .setAfter(DirectoryCursor.newBuilder()
                                    .setGeneration(1).setEventSequence(1))
                            .build(), observer);
            DirectoryFrame frame = observer.next();
            assertThat(frame.hasResyncRequired()).isTrue();
            assertThat(frame.getResyncRequired().getOldestAvailable().getGeneration())
                    .isEqualTo(2);
            assertThat(frame.getResyncRequired().getOldestAvailable().getEventSequence())
                    .isEqualTo(2);
        } finally {
            compactChannel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
            compactServer.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void slowWatcherIsEvictedAtItsBoundWithoutBlockingMutation() {
        ClusterDirectoryGrpcService service = new ClusterDirectoryGrpcService(directory, 2);
        NeverReadyObserver observer = new NeverReadyObserver();
        service.watchDirectory(WatchDirectoryRequest.getDefaultInstance(), observer);

        directory.register(ClusterFixtures.node("node-1"));
        directory.registerProcessor(
                ClusterFixtures.processorBuilder("proc-1", "node-1").build());

        assertThat(directory.snapshot().getSnapshotSeq()).isEqualTo(2);
        assertThat(observer.failure.get()).isInstanceOfSatisfying(
                StatusRuntimeException.class, failure ->
                        assertThat(failure.getStatus().getCode())
                                .isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(observer.received).isZero();
    }

    private static final class RecordingObserver implements StreamObserver<DirectoryFrame> {
        private final LinkedBlockingQueue<Object> received = new LinkedBlockingQueue<>();

        @Override
        public void onNext(DirectoryFrame value) {
            received.add(value);
        }

        @Override
        public void onError(Throwable throwable) {
            received.add(throwable);
        }

        @Override
        public void onCompleted() {
            received.add(Boolean.TRUE);
        }

        private DirectoryFrame next() throws Exception {
            Object value = received.poll(2, TimeUnit.SECONDS);
            assertThat(value).isInstanceOf(DirectoryFrame.class);
            return (DirectoryFrame) value;
        }
    }

    private static final class NeverReadyObserver
            extends ServerCallStreamObserver<DirectoryFrame> {
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private int received;
        private Runnable cancellation;
        private Runnable readiness;

        @Override
        public void onNext(DirectoryFrame value) {
            received++;
        }

        @Override
        public void onError(Throwable throwable) {
            failure.set(throwable);
        }

        @Override
        public void onCompleted() {
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setOnCancelHandler(Runnable handler) {
            cancellation = handler;
        }

        @Override
        public void setCompression(String compression) {
        }

        @Override
        public boolean isReady() {
            return false;
        }

        @Override
        public void setOnReadyHandler(Runnable handler) {
            readiness = handler;
        }

        @Override
        public void request(int count) {
        }

        @Override
        public void disableAutoInboundFlowControl() {
        }

        @Override
        public void setMessageCompression(boolean enabled) {
        }
    }
}
