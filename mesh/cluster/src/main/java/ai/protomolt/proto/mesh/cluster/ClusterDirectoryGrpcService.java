package ai.protomolt.proto.mesh.cluster;

import ai.protomolt.proto.mesh.cluster.v1.ApplyOutcome;
import ai.protomolt.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.ClusterDirectoryServiceGrpc;
import ai.protomolt.proto.mesh.cluster.v1.ClusterEvent;
import ai.protomolt.proto.mesh.cluster.v1.ClusterSnapshot;
import ai.protomolt.proto.mesh.cluster.v1.DirectoryCommit;
import ai.protomolt.proto.mesh.cluster.v1.DirectoryCursor;
import ai.protomolt.proto.mesh.cluster.v1.DirectoryEventFrame;
import ai.protomolt.proto.mesh.cluster.v1.DirectoryFilter;
import ai.protomolt.proto.mesh.cluster.v1.DirectoryFrame;
import ai.protomolt.proto.mesh.cluster.v1.DirectoryResyncRequired;
import ai.protomolt.proto.mesh.cluster.v1.DirectorySnapshotView;
import ai.protomolt.proto.mesh.cluster.v1.GetSnapshotRequest;
import ai.protomolt.proto.mesh.cluster.v1.GetSnapshotResponse;
import ai.protomolt.proto.mesh.cluster.v1.HeartbeatRequest;
import ai.protomolt.proto.mesh.cluster.v1.HeartbeatResponse;
import ai.protomolt.proto.mesh.cluster.v1.NodeRecord;
import ai.protomolt.proto.mesh.cluster.v1.ProcessorAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.RegisterNodeRequest;
import ai.protomolt.proto.mesh.cluster.v1.RegisterNodeResponse;
import ai.protomolt.proto.mesh.cluster.v1.RegisterProcessorRequest;
import ai.protomolt.proto.mesh.cluster.v1.RegisterProcessorResponse;
import ai.protomolt.proto.mesh.cluster.v1.SweepRequest;
import ai.protomolt.proto.mesh.cluster.v1.SweepResponse;
import ai.protomolt.proto.mesh.cluster.v1.UpdateCapacityRequest;
import ai.protomolt.proto.mesh.cluster.v1.UpdateCapacityResponse;
import ai.protomolt.proto.mesh.cluster.v1.UpdateReadinessRequest;
import ai.protomolt.proto.mesh.cluster.v1.UpdateReadinessResponse;
import ai.protomolt.proto.mesh.cluster.v1.WatchDirectoryRequest;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/** Protobuf-only gRPC surface over one persistent fenced cluster directory. */
public final class ClusterDirectoryGrpcService
        extends ClusterDirectoryServiceGrpc.ClusterDirectoryServiceImplBase {

    public static final int DEFAULT_WATCH_BUFFER_FRAMES = 256;

    private final PersistentClusterDirectory directory;
    private final int watchBufferFrames;

    public ClusterDirectoryGrpcService(PersistentClusterDirectory directory) {
        this(directory, DEFAULT_WATCH_BUFFER_FRAMES);
    }

    public ClusterDirectoryGrpcService(
            PersistentClusterDirectory directory, int watchBufferFrames) {
        this.directory = Objects.requireNonNull(directory, "directory");
        if (watchBufferFrames < 1 || watchBufferFrames > 65_536) {
            throw new IllegalArgumentException(
                    "watchBufferFrames must be between 1 and 65536");
        }
        this.watchBufferFrames = watchBufferFrames;
    }

    @Override
    public void registerNode(
            RegisterNodeRequest request, StreamObserver<RegisterNodeResponse> observer) {
        unary(observer, () -> RegisterNodeResponse.newBuilder()
                .setCommit(commit(directory.register(request.getAdvertisement()))).build());
    }

    @Override
    public void heartbeat(
            HeartbeatRequest request, StreamObserver<HeartbeatResponse> observer) {
        unary(observer, () -> HeartbeatResponse.newBuilder()
                .setCommit(commit(directory.heartbeat(request.getPresence()))).build());
    }

    @Override
    public void registerProcessor(RegisterProcessorRequest request,
            StreamObserver<RegisterProcessorResponse> observer) {
        unary(observer, () -> RegisterProcessorResponse.newBuilder()
                .setCommit(commit(directory.registerProcessor(request.getAdvertisement())))
                .build());
    }

    @Override
    public void updateCapacity(UpdateCapacityRequest request,
            StreamObserver<UpdateCapacityResponse> observer) {
        unary(observer, () -> UpdateCapacityResponse.newBuilder()
                .setCommit(commit(directory.updateCapacity(request.getCapacity()))).build());
    }

    @Override
    public void updateReadiness(UpdateReadinessRequest request,
            StreamObserver<UpdateReadinessResponse> observer) {
        unary(observer, () -> UpdateReadinessResponse.newBuilder()
                .setCommit(commit(directory.updateReadiness(request.getReadiness()))).build());
    }

    @Override
    public void getSnapshot(
            GetSnapshotRequest request, StreamObserver<GetSnapshotResponse> observer) {
        unary(observer, () -> GetSnapshotResponse.newBuilder()
                .setSnapshot(directory.snapshot()).build());
    }

    @Override
    public void sweep(SweepRequest request, StreamObserver<SweepResponse> observer) {
        unary(observer, () -> {
            List<ClusterEvent> events = directory.sweep();
            return SweepResponse.newBuilder()
                    .addAllEvents(events)
                    .setSnapshotSeq(directory.snapshot().getSnapshotSeq())
                    .build();
        });
    }

    @Override
    public void watchDirectory(
            WatchDirectoryRequest request, StreamObserver<DirectoryFrame> responseObserver) {
        if (!(responseObserver instanceof ServerCallStreamObserver<DirectoryFrame> observer)) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("directory watch requires server flow control")
                    .asRuntimeException());
            return;
        }
        DirectorySubscriber subscriber = new DirectorySubscriber(
                observer, request.hasFilter() ? request.getFilter()
                        : DirectoryFilter.getDefaultInstance());
        PersistentClusterDirectory.WatchRegistration registration =
                directory.watch(subscriber::onDirectoryChange);
        subscriber.start(registration, request.hasAfter() ? request.getAfter() : null);
    }

    private DirectoryCommit commit(ClusterDirectory.ApplyOutcome outcome) {
        ClusterSnapshot snapshot = directory.snapshot();
        return DirectoryCommit.newBuilder()
                .setOutcome(ApplyOutcome.valueOf("APPLY_OUTCOME_" + outcome.name()))
                .setSnapshotSeq(snapshot.getSnapshotSeq())
                .setSnapshotFingerprint(snapshot.getFingerprint())
                .build();
    }

    private static <T> void unary(StreamObserver<T> observer, Supplier<T> operation) {
        try {
            observer.onNext(operation.get());
            observer.onCompleted();
        } catch (IllegalArgumentException e) {
            Status status = isFence(e.getMessage()) ? Status.ABORTED : Status.INVALID_ARGUMENT;
            observer.onError(status.withDescription(e.getMessage()).withCause(e)
                    .asRuntimeException());
        } catch (UncheckedIOException e) {
            observer.onError(Status.UNAVAILABLE.withDescription(e.getMessage()).withCause(e)
                    .asRuntimeException());
        } catch (RuntimeException e) {
            observer.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e)
                    .asRuntimeException());
        }
    }

    private static boolean isFence(String message) {
        return message != null && (message.contains("stale")
                || message.contains("must advance")
                || message.contains("newer lease_epoch")
                || message.contains("newer position")
                || message.contains("conflicting update"));
    }

    private final class DirectorySubscriber {
        private final ServerCallStreamObserver<DirectoryFrame> observer;
        private final DirectoryFilter filter;
        private final ArrayDeque<DirectoryFrame> pending = new ArrayDeque<>();
        private PersistentClusterDirectory.WatchRegistration registration;
        private boolean closed;
        private boolean completeWhenDrained;

        private DirectorySubscriber(
                ServerCallStreamObserver<DirectoryFrame> observer, DirectoryFilter filter) {
            this.observer = observer;
            this.filter = filter;
        }

        private void start(PersistentClusterDirectory.WatchRegistration registration,
                DirectoryCursor after) {
            synchronized (this) {
                this.registration = registration;
                observer.setOnCancelHandler(this::close);
                observer.setOnReadyHandler(this::drain);
                var state = registration.state();
                if (after == null) {
                    offer(snapshotFrame(state, filter));
                } else if (after.getGeneration() != state.generation()
                        || after.getEventSequence() < state.firstRetainedSequence() - 1) {
                    offer(resyncFrame(state.generation(), state.firstRetainedSequence() - 1));
                    completeWhenDrained = true;
                } else if (after.getEventSequence() > state.snapshot().getSnapshotSeq()) {
                    fail(Status.INVALID_ARGUMENT.withDescription(
                            "directory cursor is ahead of the current snapshot"));
                    return;
                } else {
                    state.events().stream()
                            .filter(event -> event.getSeq() > after.getEventSequence())
                            .filter(event -> matches(event, filter))
                            .forEach(event -> offer(eventFrame(state.generation(), event)));
                }
            }
            drain();
        }

        private void onDirectoryChange(PersistentClusterDirectory.DirectoryChange change) {
            synchronized (this) {
                if (closed) {
                    return;
                }
                if (change.resyncRequired()) {
                    offer(resyncFrame(change.generation(), change.compactedThroughSequence()));
                    completeWhenDrained = true;
                } else {
                    change.events().stream()
                            .filter(event -> matches(event, filter))
                            .forEach(event -> offer(eventFrame(change.generation(), event)));
                }
            }
            drain();
        }

        private synchronized void offer(DirectoryFrame frame) {
            if (closed) {
                return;
            }
            if (pending.size() >= watchBufferFrames) {
                fail(Status.RESOURCE_EXHAUSTED.withDescription(
                        "directory watcher exceeded its bounded frame buffer"));
                return;
            }
            pending.addLast(frame);
        }

        private synchronized void drain() {
            if (closed) {
                return;
            }
            try {
                while (observer.isReady() && !pending.isEmpty()) {
                    observer.onNext(pending.removeFirst());
                }
                if (completeWhenDrained && pending.isEmpty()) {
                    closed = true;
                    detach();
                    observer.onCompleted();
                }
            } catch (RuntimeException e) {
                close();
            }
        }

        private synchronized void fail(Status status) {
            if (closed) {
                return;
            }
            closed = true;
            pending.clear();
            detach();
            observer.onError(status.asRuntimeException());
        }

        private synchronized void close() {
            if (closed) {
                return;
            }
            closed = true;
            pending.clear();
            detach();
        }

        private void detach() {
            if (registration != null) {
                registration.close();
                registration = null;
            }
        }
    }

    private DirectoryFrame snapshotFrame(
            PersistentClusterDirectory.WatchState state, DirectoryFilter filter) {
        ClusterSnapshot snapshot = state.snapshot();
        Set<String> matchedProcessors = new HashSet<>();
        DirectorySnapshotView.Builder view = DirectorySnapshotView.newBuilder()
                .setCursor(cursor(state.generation(), snapshot.getSnapshotSeq()))
                .setUnfilteredSnapshotFingerprint(snapshot.getFingerprint())
                .setUnfilteredSnapshotSequence(snapshot.getSnapshotSeq());
        for (ProcessorAdvertisement processor : snapshot.getProcessorsList()) {
            if (matches(processor, filter)) {
                matchedProcessors.add(processor.getProcessorId());
                view.addProcessors(processor);
            }
        }
        for (NodeRecord node : snapshot.getNodesList()) {
            String nodeId = node.getAdvertisement().getNodeId();
            boolean direct = filter.getNodeIdsCount() == 0
                    || filter.getNodeIdsList().contains(nodeId);
            boolean ownsMatch = snapshot.getProcessorsList().stream()
                    .anyMatch(p -> matchedProcessors.contains(p.getProcessorId())
                            && p.getNodeId().equals(nodeId));
            if (direct && (filter.getProcessorIdsCount() == 0 || ownsMatch)) {
                view.addNodes(node);
            }
        }
        snapshot.getCapacitiesList().stream()
                .filter(capacity -> matchedProcessors.contains(capacity.getProcessorId()))
                .forEach(view::addCapacities);
        snapshot.getReadinessOverlaysList().stream()
                .filter(readiness -> matchedProcessors.contains(readiness.getProcessorId()))
                .forEach(view::addReadinessOverlays);
        return DirectoryFrame.newBuilder().setSnapshot(view).build();
    }

    private static DirectoryFrame eventFrame(long generation, ClusterEvent event) {
        return DirectoryFrame.newBuilder().setEvent(DirectoryEventFrame.newBuilder()
                .setCursor(cursor(generation, event.getSeq()))
                .setEvent(event)).build();
    }

    private static DirectoryFrame resyncFrame(long generation, long sequence) {
        return DirectoryFrame.newBuilder().setResyncRequired(
                DirectoryResyncRequired.newBuilder()
                        .setOldestAvailable(cursor(generation, sequence))).build();
    }

    private static DirectoryCursor cursor(long generation, long sequence) {
        return DirectoryCursor.newBuilder()
                .setGeneration(generation)
                .setEventSequence(sequence)
                .build();
    }

    private boolean matches(ClusterEvent event, DirectoryFilter filter) {
        if (filter.getNodeIdsCount() > 0
                && !filter.getNodeIdsList().contains(event.getNodeId())) {
            return false;
        }
        if (filter.getProcessorIdsCount() > 0
                && !filter.getProcessorIdsList().contains(event.getProcessorId())) {
            return false;
        }
        return switch (event.getDetailCase()) {
            case PROCESSOR -> matches(event.getProcessor(), filter);
            case CAPACITY -> event.getProcessorId().isBlank()
                    || directory.processor(event.getProcessorId())
                    .map(processor -> matches(processor, filter)).orElse(false);
            case READINESS -> directory.processor(event.getProcessorId())
                    .map(processor -> matches(processor, filter)).orElse(false);
            case NODE, PRESENCE -> filter.getProcessorIdsCount() == 0
                    && filter.getInputTypeName().isBlank()
                    && filter.getCapability().isBlank();
            case DETAIL_NOT_SET -> false;
        };
    }

    private static boolean matches(
            ProcessorAdvertisement processor, DirectoryFilter filter) {
        if (filter.getProcessorIdsCount() > 0
                && !filter.getProcessorIdsList().contains(processor.getProcessorId())) {
            return false;
        }
        if (filter.getNodeIdsCount() > 0
                && !filter.getNodeIdsList().contains(processor.getNodeId())) {
            return false;
        }
        if (!filter.getCapability().isBlank()
                && !processor.getCapabilitiesList().contains(filter.getCapability())) {
            return false;
        }
        if (!filter.getInputTypeName().isBlank()
                && !processor.getContract().getInputSchema().getTypeName()
                .equals(filter.getInputTypeName())) {
            return false;
        }
        return filter.getInputDescriptorFingerprint().isBlank()
                || processor.getContract().getInputSchema().getDescriptorFingerprint()
                .equals(filter.getInputDescriptorFingerprint());
    }
}
