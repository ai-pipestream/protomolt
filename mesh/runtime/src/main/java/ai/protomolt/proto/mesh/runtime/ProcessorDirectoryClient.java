package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.ClusterDirectoryServiceGrpc;
import ai.protomolt.proto.mesh.cluster.v1.ClusterEvent;
import ai.protomolt.proto.mesh.cluster.v1.DirectoryCursor;
import ai.protomolt.proto.mesh.cluster.v1.DirectoryFrame;
import ai.protomolt.proto.mesh.cluster.v1.NodePresence;
import ai.protomolt.proto.mesh.cluster.v1.NodeRecord;
import ai.protomolt.proto.mesh.cluster.v1.PresenceState;
import ai.protomolt.proto.mesh.cluster.v1.ProcessorAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.ProcessorReadinessOverlay;
import ai.protomolt.proto.mesh.cluster.v1.WatchDirectoryRequest;
import com.google.protobuf.util.Timestamps;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Snapshot/watch/resync client exposing one atomic immutable directory view. */
public final class ProcessorDirectoryClient implements AutoCloseable {

    private final ClusterDirectoryServiceGrpc.ClusterDirectoryServiceStub stub;
    private final Clock clock;
    private final AtomicReference<View> view = new AtomicReference<>(View.empty());
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicLong streamEpoch = new AtomicLong();
    private final CountDownLatch initial = new CountDownLatch(1);
    private final AtomicReference<Instant> lastFrameAt = new AtomicReference<>();
    private Context.CancellableContext watchContext;
    private volatile boolean closed;

    public ProcessorDirectoryClient(
            ClusterDirectoryServiceGrpc.ClusterDirectoryServiceStub stub) {
        this(stub, Clock.systemUTC());
    }

    public ProcessorDirectoryClient(
            ClusterDirectoryServiceGrpc.ClusterDirectoryServiceStub stub, Clock clock) {
        this.stub = Objects.requireNonNull(stub, "stub");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Starts one resumable watch. */
    public synchronized void start() {
        if (closed) {
            throw new IllegalStateException("processor directory client is closed");
        }
        if (streamEpoch.get() != 0) {
            throw new IllegalStateException("processor directory client is already started");
        }
        open(false);
    }

    /** Reconnects from the last committed cursor, or requests a fresh snapshot after resync. */
    public synchronized void reconnect(boolean forceSnapshot) {
        if (closed) {
            return;
        }
        open(forceSnapshot);
    }

    public boolean awaitInitial(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }
        return initial.await(timeout.toNanos(), TimeUnit.NANOSECONDS) && view.get().initialized();
    }

    public View view() {
        return view.get();
    }

    public Optional<Throwable> failure() {
        return Optional.ofNullable(failure.get());
    }

    public Optional<Duration> watchLag() {
        return Optional.ofNullable(lastFrameAt.get())
                .map(last -> Duration.between(last, clock.instant()))
                .map(lag -> lag.isNegative() ? Duration.ZERO : lag);
    }

    @Override
    public synchronized void close() {
        closed = true;
        streamEpoch.incrementAndGet();
        if (watchContext != null) {
            watchContext.cancel(null);
            watchContext = null;
        }
    }

    private void open(boolean forceSnapshot) {
        if (watchContext != null) {
            watchContext.cancel(null);
        }
        long epoch = streamEpoch.incrementAndGet();
        View current = view.get();
        WatchDirectoryRequest.Builder request = WatchDirectoryRequest.newBuilder();
        if (!forceSnapshot && current.initialized()) {
            request.setAfter(DirectoryCursor.newBuilder()
                    .setGeneration(current.generation())
                    .setEventSequence(current.eventSequence()));
        }
        Context.CancellableContext context = Context.ROOT.withCancellation();
        watchContext = context;
        context.run(() -> stub.watchDirectory(request.build(), new StreamObserver<>() {
            @Override
            public void onNext(DirectoryFrame frame) {
                if (closed || streamEpoch.get() != epoch) {
                    return;
                }
                try {
                    lastFrameAt.set(clock.instant());
                    if (frame.hasSnapshot()) {
                        install(View.from(frame.getSnapshot()));
                    } else if (frame.hasEvent()) {
                        apply(frame.getEvent().getCursor(), frame.getEvent().getEvent());
                    } else if (frame.hasResyncRequired()) {
                        reconnect(true);
                    } else {
                        throw new IllegalArgumentException(
                                "directory frame carries no payload");
                    }
                } catch (RuntimeException e) {
                    failure.set(e);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                if (!closed && streamEpoch.get() == epoch) {
                    failure.set(throwable);
                }
            }

            @Override
            public void onCompleted() {
                if (!closed && streamEpoch.get() == epoch) {
                    failure.compareAndSet(null,
                            new IllegalStateException("directory watch completed"));
                }
            }
        }));
    }

    private void install(View next) {
        view.set(next);
        failure.set(null);
        initial.countDown();
    }

    private void apply(DirectoryCursor cursor, ClusterEvent event) {
        while (true) {
            View current = view.get();
            if (!current.initialized()) {
                throw new IllegalStateException("directory event arrived before snapshot");
            }
            if (cursor.getGeneration() != current.generation()) {
                reconnect(true);
                return;
            }
            if (cursor.getEventSequence() <= current.eventSequence()) {
                return;
            }
            if (cursor.getEventSequence() != current.eventSequence() + 1) {
                reconnect(true);
                return;
            }
            View next = current.apply(event);
            if (view.compareAndSet(current, next)) {
                failure.set(null);
                return;
            }
        }
    }

    /** One immutable unfiltered directory projection. */
    public record View(
            boolean initialized,
            long generation,
            long eventSequence,
            String snapshotFingerprint,
            Map<String, NodeRecord> nodes,
            Map<String, ProcessorAdvertisement> processors,
            Map<String, CapacityAdvertisement> processorCapacity,
            Map<String, ProcessorReadinessOverlay> readiness) {

        public View {
            Objects.requireNonNull(snapshotFingerprint, "snapshotFingerprint");
            nodes = Map.copyOf(nodes);
            processors = Map.copyOf(processors);
            processorCapacity = Map.copyOf(processorCapacity);
            readiness = Map.copyOf(readiness);
        }

        public static View empty() {
            return new View(false, 0, 0, "", Map.of(), Map.of(), Map.of(), Map.of());
        }

        public static View from(ai.protomolt.proto.mesh.cluster.v1.DirectorySnapshotView snapshot) {
            Map<String, NodeRecord> nodes = new LinkedHashMap<>();
            snapshot.getNodesList().forEach(node -> nodes.put(
                    node.getAdvertisement().getNodeId(), node));
            Map<String, ProcessorAdvertisement> processors = new LinkedHashMap<>();
            snapshot.getProcessorsList().forEach(processor ->
                    processors.put(processor.getProcessorId(), processor));
            Map<String, CapacityAdvertisement> capacity = new LinkedHashMap<>();
            snapshot.getCapacitiesList().forEach(record ->
                    capacity.put(record.getProcessorId(), record));
            Map<String, ProcessorReadinessOverlay> readiness = new LinkedHashMap<>();
            snapshot.getReadinessOverlaysList().forEach(record ->
                    readiness.put(record.getProcessorId(), record));
            return new View(true, snapshot.getCursor().getGeneration(),
                    snapshot.getCursor().getEventSequence(),
                    snapshot.getUnfilteredSnapshotFingerprint(), nodes, processors,
                    capacity, readiness);
        }

        /** Adapts the coordinator's direct, unfiltered snapshot without a network hop. */
        public static View from(
                ai.protomolt.proto.mesh.cluster.v1.ClusterSnapshot snapshot,
                long generation) {
            Map<String, NodeRecord> nodes = new LinkedHashMap<>();
            snapshot.getNodesList().forEach(node -> nodes.put(
                    node.getAdvertisement().getNodeId(), node));
            Map<String, ProcessorAdvertisement> processors = new LinkedHashMap<>();
            snapshot.getProcessorsList().forEach(processor ->
                    processors.put(processor.getProcessorId(), processor));
            Map<String, CapacityAdvertisement> capacity = new LinkedHashMap<>();
            snapshot.getCapacitiesList().forEach(record ->
                    capacity.put(record.getProcessorId(), record));
            Map<String, ProcessorReadinessOverlay> readiness = new LinkedHashMap<>();
            snapshot.getReadinessOverlaysList().forEach(record ->
                    readiness.put(record.getProcessorId(), record));
            return new View(true, generation, snapshot.getSnapshotSeq(),
                    snapshot.getFingerprint(), nodes, processors, capacity, readiness);
        }

        private View apply(ClusterEvent event) {
            Map<String, NodeRecord> nextNodes = new LinkedHashMap<>(nodes);
            Map<String, ProcessorAdvertisement> nextProcessors =
                    new LinkedHashMap<>(processors);
            Map<String, CapacityAdvertisement> nextCapacity =
                    new LinkedHashMap<>(processorCapacity);
            Map<String, ProcessorReadinessOverlay> nextReadiness =
                    new LinkedHashMap<>(readiness);
            switch (event.getType()) {
                case CLUSTER_EVENT_TYPE_NODE_REGISTERED -> {
                    NodeRecord existing = nextNodes.get(event.getNodeId());
                    NodePresence presence = existing == null
                            || existing.getAdvertisement().getEpoch() != event.getNode().getEpoch()
                            ? initialPresence(event.getNode()) : existing.getPresence();
                    NodeRecord.Builder record = NodeRecord.newBuilder()
                            .setAdvertisement(event.getNode()).setPresence(presence);
                    if (existing != null && existing.hasCapacity()
                            && existing.getCapacity().getSourceEpoch()
                            == event.getNode().getEpoch()) {
                        record.setCapacity(existing.getCapacity());
                    }
                    nextNodes.put(event.getNodeId(), record.build());
                }
                case CLUSTER_EVENT_TYPE_NODE_EXPIRED -> {
                    nextNodes.remove(event.getNodeId());
                    nextProcessors.values().removeIf(
                            processor -> processor.getNodeId().equals(event.getNodeId()));
                    nextCapacity.values().removeIf(
                            capacity -> capacity.getNodeId().equals(event.getNodeId()));
                    nextReadiness.values().removeIf(
                            ready -> ready.getNodeId().equals(event.getNodeId()));
                }
                case CLUSTER_EVENT_TYPE_PROCESSOR_REGISTERED ->
                        nextProcessors.put(event.getProcessorId(), event.getProcessor());
                case CLUSTER_EVENT_TYPE_PROCESSOR_EXPIRED -> {
                    nextProcessors.remove(event.getProcessorId());
                    nextCapacity.remove(event.getProcessorId());
                    nextReadiness.remove(event.getProcessorId());
                }
                case CLUSTER_EVENT_TYPE_PRESENCE_UPDATED -> {
                    NodeRecord existing = nextNodes.get(event.getNodeId());
                    if (existing == null) {
                        throw new IllegalStateException(
                                "presence event names absent node " + event.getNodeId());
                    }
                    nextNodes.put(event.getNodeId(), existing.toBuilder()
                            .setPresence(event.getPresence()).build());
                }
                case CLUSTER_EVENT_TYPE_CAPACITY_UPDATED -> {
                    if (event.getProcessorId().isBlank()) {
                        NodeRecord existing = nextNodes.get(event.getNodeId());
                        if (existing == null) {
                            throw new IllegalStateException(
                                    "capacity event names absent node " + event.getNodeId());
                        }
                        nextNodes.put(event.getNodeId(), existing.toBuilder()
                                .setCapacity(event.getCapacity()).build());
                    } else {
                        nextCapacity.put(event.getProcessorId(), event.getCapacity());
                    }
                }
                case CLUSTER_EVENT_TYPE_READINESS_UPDATED ->
                        nextReadiness.put(event.getProcessorId(), event.getReadiness());
                case CLUSTER_EVENT_TYPE_UNSPECIFIED, UNRECOGNIZED ->
                        throw new IllegalArgumentException(
                                "unsupported directory event " + event.getType());
            }
            return new View(true, generation, event.getSeq(), snapshotFingerprint,
                    nextNodes, nextProcessors, nextCapacity, nextReadiness);
        }

        private static NodePresence initialPresence(
                ai.protomolt.proto.mesh.cluster.v1.NodeAdvertisement node) {
            return NodePresence.newBuilder()
                    .setNodeId(node.getNodeId())
                    .setClusterId(node.getClusterId())
                    .setState(PresenceState.PRESENCE_STATE_ACTIVE)
                    .setLastHeartbeatAt(node.getAdvertisedAt())
                    .setHeartbeatSeq(node.getSeq())
                    .setTtl(node.getTtl())
                    .setNodeEpoch(node.getEpoch())
                    .setExpiresAt(Timestamps.add(node.getAdvertisedAt(), node.getTtl()))
                    .build();
        }
    }
}
