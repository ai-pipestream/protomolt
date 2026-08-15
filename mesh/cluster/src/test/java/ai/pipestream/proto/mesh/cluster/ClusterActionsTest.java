package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.ProcessorAdvertisement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterActionsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ClusterFixtures.MutableClock clock;
    private PersistentClusterDirectory directory;
    private ActionCatalog catalog;

    @BeforeEach
    void setUp() {
        clock = new ClusterFixtures.MutableClock(ClusterFixtures.T0);
        directory = new PersistentClusterDirectory(ClusterFixtures.cluster(), clock,
                new InMemoryClusterEventRepository());
        catalog = ClusterActions.register(ActionCatalog.defaults(ActionContext.builder().build()),
                directory);
    }

    @Test
    void registersAllBoundedMeshActions() {
        assertThat(catalog.names()).contains("mesh-node-register", "mesh-node-heartbeat",
                "mesh-processor-register", "mesh-capacity-update", "mesh-snapshot",
                "mesh-sweep");
        catalog.list().findValues("inputSchema").forEach(schema -> {
            assertThat(schema.path("type").asText()).isEqualTo("object");
            assertThat(schema.path("additionalProperties").asBoolean()).isFalse();
        });
    }

    @Test
    void nodeProcessorAndCapacityBecomeVisibleInSnapshot() throws Exception {
        NodeAdvertisement node = ClusterFixtures.node("nano1");
        ProcessorAdvertisement processor = ClusterFixtures.processorBuilder(
                "nano1-tei", "nano1").build();
        CapacityAdvertisement capacity = ClusterFixtures.capacityBuilder("nano1", 1)
                .setProcessorId("nano1-tei")
                .build();

        assertThat(execute("mesh-node-register", "advertisement", node)
                .path("outcome").asText()).isEqualTo("REGISTERED");
        assertThat(execute("mesh-processor-register", "advertisement", processor)
                .path("outcome").asText()).isEqualTo("REGISTERED");
        assertThat(execute("mesh-capacity-update", "capacity", capacity)
                .path("outcome").asText()).isEqualTo("REGISTERED");

        JsonNode snapshot = catalog.execute("mesh-snapshot", MAPPER.createObjectNode())
                .path("snapshot");
        assertThat(snapshot.path("nodes").get(0).path("advertisement").path("nodeId").asText())
                .isEqualTo("nano1");
        assertThat(snapshot.path("processors").get(0).path("processorId").asText())
                .isEqualTo("nano1-tei");
        assertThat(snapshot.path("capacities").get(0).path("maxInFlight").asInt())
                .isEqualTo(16);
    }

    @Test
    void staleMutationsReturnStableRejectionCode() throws Exception {
        execute("mesh-node-register", "advertisement", ClusterFixtures.node("nano1"));
        NodeAdvertisement changedAtSamePosition = ClusterFixtures.nodeBuilder("nano1", 1, 1)
                .setTtl(com.google.protobuf.Duration.newBuilder().setSeconds(45))
                .build();

        assertThatThrownBy(() -> execute("mesh-node-register", "advertisement",
                changedAtSamePosition))
                .isInstanceOfSatisfying(ActionException.class, error -> {
                    assertThat(error.code()).isEqualTo("cluster-rejected");
                    assertThat(error.getMessage()).contains("does not advance");
                });
    }

    @Test
    void unknownProtoFieldsAreRejectedInsteadOfIgnored() throws Exception {
        ObjectNode advertisement = render(ClusterFixtures.node("nano1"));
        advertisement.put("credential", "must-not-cross-the-boundary");
        ObjectNode input = MAPPER.createObjectNode().set("advertisement", advertisement);

        assertThatThrownBy(() -> catalog.execute("mesh-node-register", input))
                .isInstanceOfSatisfying(ActionException.class, error -> {
                    assertThat(error.code()).isEqualTo("invalid-input");
                    assertThat(error.getMessage()).contains("credential");
                });
        assertThat(directory.snapshot().getNodesCount()).isZero();
    }

    @Test
    void sweepExpiresProcessorThenCascadesOfflineNode() throws Exception {
        execute("mesh-node-register", "advertisement", ClusterFixtures.node("nano1"));
        execute("mesh-processor-register", "advertisement",
                ClusterFixtures.processorBuilder("nano1-tei", "nano1").build());

        clock.advance(Duration.ofSeconds(61));
        ObjectNode result = catalog.execute("mesh-sweep", MAPPER.createObjectNode());

        assertThat(result.path("expiredCount").asInt()).isEqualTo(2);
        assertThat(result.path("events").findValuesAsText("type"))
                .containsExactly("CLUSTER_EVENT_TYPE_PROCESSOR_EXPIRED",
                        "CLUSTER_EVENT_TYPE_NODE_EXPIRED");
        assertThat(directory.snapshot().getNodesCount()).isZero();
        assertThat(directory.snapshot().getProcessorsCount()).isZero();
    }

    private ObjectNode execute(String action, String field, Message message) throws Exception {
        return catalog.execute(action, MAPPER.createObjectNode().set(field, render(message)));
    }

    private static ObjectNode render(Message message) throws Exception {
        return (ObjectNode) MAPPER.readTree(JsonFormat.printer().print(message));
    }
}
