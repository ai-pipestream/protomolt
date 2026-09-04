package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.cluster.PersistentClusterDirectory;
import ai.protomolt.proto.mesh.runtime.v1.GetMeshRuntimeHealthRequest;
import ai.protomolt.proto.mesh.runtime.v1.GetMeshRuntimeHealthResponse;
import ai.protomolt.proto.mesh.runtime.v1.MeshRuntimeHealthServiceGrpc;
import ai.protomolt.proto.mesh.runtime.v1.RuntimeComponentHealth;
import ai.protomolt.proto.mesh.runtime.v1.RuntimeComponentHealthState;
import io.grpc.stub.StreamObserver;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/** Component-level health for directory persistence, watch freshness, workers, and channel. */
public final class MeshRuntimeHealthGrpcService
        extends MeshRuntimeHealthServiceGrpc.MeshRuntimeHealthServiceImplBase {

    private static final String HEALTH_PROBE_DELIVERY_ID =
            "00000000-0000-0000-0000-000000000000";

    private final PersistentClusterDirectory directory;
    private final DemandProcessorCoordinator workers;
    private final DurableProcessorChannel channel;
    private final Supplier<WatchHealth> watch;
    private final Clock clock;

    public MeshRuntimeHealthGrpcService(
            PersistentClusterDirectory directory,
            DemandProcessorCoordinator workers,
            DurableProcessorChannel channel,
            Supplier<WatchHealth> watch,
            Clock clock) {
        this.directory = Objects.requireNonNull(directory, "directory");
        this.workers = Objects.requireNonNull(workers, "workers");
        this.channel = Objects.requireNonNull(channel, "channel");
        this.watch = Objects.requireNonNull(watch, "watch");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public void getMeshRuntimeHealth(
            GetMeshRuntimeHealthRequest request,
            StreamObserver<GetMeshRuntimeHealthResponse> response) {
        try {
            response.onNext(GetMeshRuntimeHealthResponse.newBuilder()
                    .addComponents(directoryPersistence())
                    .addComponents(directoryWatch())
                    .addComponents(workerReadiness())
                    .addComponents(channelAvailability())
                    .setObservedAt(RemoteValidation.timestamp(clock.instant()))
                    .build());
            response.onCompleted();
        } catch (RuntimeException failure) {
            response.onError(io.grpc.Status.INTERNAL
                    .withDescription("mesh runtime health inspection failed")
                    .withCause(failure).asRuntimeException());
        }
    }

    private RuntimeComponentHealth directoryPersistence() {
        Optional<RuntimeException> failure = directory.persistenceFailure();
        return component("directory-persistence",
                failure.isEmpty() ? RuntimeComponentHealthState
                        .RUNTIME_COMPONENT_HEALTH_STATE_SERVING
                        : RuntimeComponentHealthState
                        .RUNTIME_COMPONENT_HEALTH_STATE_UNAVAILABLE,
                failure.map(Throwable::getMessage).orElseGet(() ->
                        "snapshot sequence " + directory.snapshot().getSnapshotSeq()
                                + " is durably installed"));
    }

    private RuntimeComponentHealth directoryWatch() {
        WatchHealth current = watch.get();
        return component("directory-watch", current.state(), current.detail());
    }

    private RuntimeComponentHealth workerReadiness() {
        Optional<RuntimeException> failure = workers.maintenanceFailure();
        if (failure.isPresent()) {
            return component("worker-readiness", RuntimeComponentHealthState
                            .RUNTIME_COMPONENT_HEALTH_STATE_UNAVAILABLE,
                    failure.orElseThrow().getMessage());
        }
        int connected = workers.connectedWorkers();
        return component("worker-readiness", connected == 0
                        ? RuntimeComponentHealthState
                        .RUNTIME_COMPONENT_HEALTH_STATE_DEGRADED
                        : RuntimeComponentHealthState
                        .RUNTIME_COMPONENT_HEALTH_STATE_SERVING,
                connected + " admitted workers connected");
    }

    private RuntimeComponentHealth channelAvailability() {
        try {
            channel.delivery(HEALTH_PROBE_DELIVERY_ID);
            return component("processor-channel", RuntimeComponentHealthState
                            .RUNTIME_COMPONENT_HEALTH_STATE_SERVING,
                    "bounded channel lookup succeeded");
        } catch (RuntimeException failure) {
            return component("processor-channel", RuntimeComponentHealthState
                            .RUNTIME_COMPONENT_HEALTH_STATE_UNAVAILABLE,
                    failure.getMessage());
        }
    }

    private static RuntimeComponentHealth component(
            String name, RuntimeComponentHealthState state, String detail) {
        String bounded = detail == null ? "no detail" : detail;
        return RuntimeComponentHealth.newBuilder()
                .setComponent(name)
                .setState(state)
                .setDetail(bounded.substring(0, Math.min(2_048, bounded.length())))
                .build();
    }

    public record WatchHealth(RuntimeComponentHealthState state, String detail) {
        public WatchHealth {
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(detail, "detail");
            if (state == RuntimeComponentHealthState
                    .RUNTIME_COMPONENT_HEALTH_STATE_UNSPECIFIED) {
                throw new IllegalArgumentException("watch health state must be explicit");
            }
        }

        public static WatchHealth localView() {
            return new WatchHealth(RuntimeComponentHealthState
                    .RUNTIME_COMPONENT_HEALTH_STATE_SERVING,
                    "coordinator uses the atomic local directory view");
        }

        public static WatchHealth remote(
                Optional<Throwable> failure, Optional<Duration> lag, Duration maximumLag) {
            if (failure.isPresent()) {
                return new WatchHealth(RuntimeComponentHealthState
                        .RUNTIME_COMPONENT_HEALTH_STATE_UNAVAILABLE,
                        "watch failed: " + failure.orElseThrow().getMessage());
            }
            if (lag.isEmpty()) {
                return new WatchHealth(RuntimeComponentHealthState
                        .RUNTIME_COMPONENT_HEALTH_STATE_DEGRADED,
                        "watch has not received its initial frame");
            }
            Duration actual = lag.orElseThrow();
            return new WatchHealth(actual.compareTo(maximumLag) > 0
                    ? RuntimeComponentHealthState
                    .RUNTIME_COMPONENT_HEALTH_STATE_DEGRADED
                    : RuntimeComponentHealthState
                    .RUNTIME_COMPONENT_HEALTH_STATE_SERVING,
                    "watch lag is " + actual.toMillis() + " ms");
        }
    }
}
