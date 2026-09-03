package ai.protomolt.proto.mcp;

import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.v1.TaskMessageKind;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** The bounded delegation resources: workers, tasks, and per-task transcripts. */
class DelegationResourcesTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private InProcessDelegationCoordinator coordinator;
    private DelegationBridge bridge;
    private DelegationResources resources;

    @BeforeEach
    void setUp() {
        coordinator = new InProcessDelegationCoordinator();
        bridge = new DelegationBridge(coordinator);
        resources = new DelegationResources(bridge);
    }

    @AfterEach
    void tearDown() {
        bridge.close();
        coordinator.close();
    }

    @Test
    void listsTheTwoRootsAndTheTranscriptTemplate() {
        ArrayNode listed = resources.list(mapper);
        assertThat(listed.findValuesAsText("uri")).containsExactly(
                DelegationResources.WORKERS_URI, DelegationResources.TASKS_URI);
        assertThat(resources.templates(mapper).findValuesAsText("name"))
                .containsExactly("delegation-transcript");
    }

    @Test
    void servesWorkersTasksAndOneTaskTranscript() throws Exception {
        bridge.registerWorker(WorkerHello.newBuilder()
                .setWorkerId("resource-kimi")
                .setProtocolVersion(1)
                .setProvider("kimi")
                .build());
        String taskId = UUID.randomUUID().toString();
        TaskSpec spec = TaskSpec.newBuilder()
                .setObjective("prove the resource surface")
                .addRequiredChecks(AcceptanceCheck.newBuilder().setName("unit-tests"))
                .build();
        bridge.offer("resource-kimi", taskId, spec, Duration.ofSeconds(30), null);
        bridge.accept("resource-kimi", taskId, 1);
        bridge.sendCoordinatorMessage("resource-kimi", taskId,
                TaskMessageKind.TASK_MESSAGE_KIND_NOTE, "resource test note", "", List.of());

        JsonNode workers = read(DelegationResources.WORKERS_URI);
        assertThat(workers.path("workers").findValuesAsText("workerId"))
                .containsExactly("resource-kimi");
        assertThat(workers.path("truncated").asBoolean()).isFalse();

        JsonNode tasks = read(DelegationResources.TASKS_URI);
        JsonNode task = tasks.path("tasks").get(0);
        assertThat(task.path("taskId").asText()).isEqualTo(taskId);
        assertThat(task.path("phase").asText()).isEqualTo("LEASED");
        assertThat(task.path("holder").asText()).isEqualTo("resource-kimi");

        JsonNode transcript = read(DelegationResources.TASKS_URI + "/" + taskId
                + "/transcript");
        assertThat(transcript.path("entries").size()).isEqualTo(3);
        List<Long> cursors = new ArrayList<>();
        transcript.path("entries").forEach(entry -> cursors.add(entry.path("cursor").asLong()));
        assertThat(cursors).isSorted();
        assertThat(transcript.path("entries").get(0).path("lane").asText())
                .isEqualTo("LANE_COORDINATOR");
        assertThat(transcript.path("entries").get(1).path("lane").asText())
                .isEqualTo("LANE_WORKER");
    }

    @Test
    void boundsLongIndexes() throws Exception {
        for (int i = 0; i < DelegationResources.MAX_ROWS + 4; i++) {
            bridge.registerWorker(WorkerHello.newBuilder()
                    .setWorkerId("worker-" + i)
                    .setProtocolVersion(1)
                    .build());
        }

        JsonNode workers = read(DelegationResources.WORKERS_URI);

        assertThat(workers.path("workers").size()).isEqualTo(DelegationResources.MAX_ROWS);
        assertThat(workers.path("truncated").asBoolean()).isTrue();
    }

    @Test
    void ignoresUrisItDoesNotOwn() {
        assertThat(resources.read(mapper, "protomolt://registry/subjects")).isEmpty();
        assertThat(resources.read(mapper, DelegationResources.ROOT)).isEmpty();
        assertThat(resources.read(mapper,
                DelegationResources.TASKS_URI + "/not-a-task/elsewhere")).isEmpty();
    }

    private JsonNode read(String uri) throws Exception {
        Optional<com.fasterxml.jackson.databind.node.ObjectNode> contents =
                resources.read(mapper, uri);
        assertThat(contents).as(uri).isPresent();
        return mapper.readTree(contents.get().path("text").asText());
    }
}
