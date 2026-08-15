package ai.pipestream.proto.serve;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.mesh.cluster.ClusterActions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class MeshClusterRuntimeTest {

    @Test
    void disabledMeshCreatesNoRuntime() {
        assertThat(MeshClusterRuntime.open(null, null)).isNull();
    }

    @Test
    void inMemoryRuntimeMountsTheClusterActions() {
        var options = new ProtoMoltServe.MeshClusterOptions(
                "protomolt", "ProtoMolt private mesh", "taild24b1c.ts.net",
                Instant.parse("2026-08-14T00:00:00Z"));
        try (MeshClusterRuntime runtime = MeshClusterRuntime.open(options, null)) {
            assertThat(runtime).isNotNull();
            assertThat(runtime.directory().snapshot().getCluster().getClusterId())
                    .isEqualTo("protomolt");
            ActionCatalog catalog = ClusterActions.register(
                    ActionCatalog.defaults(ActionContext.builder().build()),
                    runtime.directory());
            assertThat(catalog.names()).contains("mesh-node-register", "mesh-snapshot");
        }
    }
}
