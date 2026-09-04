package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.mesh.cluster.ClusterActions;
import ai.protomolt.proto.mesh.runtime.MeshRuntimeHealthGrpcService;
import ai.protomolt.proto.mesh.runtime.v1.GetMeshRuntimeHealthRequest;
import ai.protomolt.proto.mesh.runtime.v1.GetMeshRuntimeHealthResponse;
import ai.protomolt.proto.mesh.runtime.v1.RuntimeComponentHealthState;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class MeshClusterRuntimeTest {

    @TempDir
    Path temporary;

    @Test
    void disabledMeshCreatesNoRuntime() {
        assertThat(MeshClusterRuntime.open(
                null, null, ActionContext.builder().build().registry())).isNull();
    }

    @Test
    void inMemoryRuntimeMountsTheClusterActions() {
        var options = new ProtoMoltServe.MeshClusterOptions(
                "protomolt", "ProtoMolt private mesh", "taild24b1c.ts.net",
                Instant.parse("2026-08-14T00:00:00Z"), temporary,
                Duration.ofMillis(50), Duration.ofSeconds(30), 3, 100);
        ActionContext context = ActionContext.builder().build();
        try (MeshClusterRuntime runtime = MeshClusterRuntime.open(
                options, null, context.registry())) {
            assertThat(runtime).isNotNull();
            assertThat(runtime.directory().snapshot().getCluster().getClusterId())
                    .isEqualTo("protomolt");
            ActionCatalog catalog = ClusterActions.register(
                    ActionCatalog.defaults(context),
                    runtime.directory());
            assertThat(catalog.names()).contains("mesh-node-register", "mesh-snapshot");
            assertThat(runtime.grpcServices()).extracting(service -> service.bindService()
                            .getServiceDescriptor().getName())
                    .anyMatch(name -> name.endsWith(".ClusterDirectoryService"))
                    .anyMatch(name -> name.endsWith(".DemandProcessorService"))
                    .anyMatch(name -> name.endsWith(".FlowLifecycleService"))
                    .anyMatch(name -> name.endsWith(".RecoveryService"))
                    .anyMatch(name -> name.endsWith(".MeshRuntimeHealthService"));

            MeshRuntimeHealthGrpcService health = runtime.grpcServices().stream()
                    .filter(MeshRuntimeHealthGrpcService.class::isInstance)
                    .map(MeshRuntimeHealthGrpcService.class::cast)
                    .findFirst()
                    .orElseThrow();
            AtomicReference<GetMeshRuntimeHealthResponse> response = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            health.getMeshRuntimeHealth(GetMeshRuntimeHealthRequest.getDefaultInstance(),
                    new StreamObserver<>() {
                        @Override
                        public void onNext(GetMeshRuntimeHealthResponse value) {
                            response.set(value);
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            failure.set(throwable);
                        }

                        @Override
                        public void onCompleted() {
                        }
                    });
            assertThat(failure.get()).isNull();
            assertThat(response.get().getComponentsList())
                    .extracting(component -> component.getComponent())
                    .containsExactly("directory-persistence", "directory-watch",
                            "worker-readiness", "processor-channel");
            assertThat(response.get().getComponentsList())
                    .extracting(component -> component.getState())
                    .containsExactly(
                            RuntimeComponentHealthState.RUNTIME_COMPONENT_HEALTH_STATE_SERVING,
                            RuntimeComponentHealthState.RUNTIME_COMPONENT_HEALTH_STATE_SERVING,
                            RuntimeComponentHealthState.RUNTIME_COMPONENT_HEALTH_STATE_DEGRADED,
                            RuntimeComponentHealthState.RUNTIME_COMPONENT_HEALTH_STATE_SERVING);
        }
    }
}
