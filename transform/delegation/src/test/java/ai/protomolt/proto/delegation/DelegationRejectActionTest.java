package ai.protomolt.proto.delegation;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/** A worker rejection through the same catalog verb used by MCP and AgentHost. */
class DelegationRejectActionTest {
    private static final String WORKER = "fixture-worker";
    private static final ObjectMapper JSON = new ObjectMapper();
    private InProcessDelegationCoordinator coordinator;
    private DelegationBridge bridge;
    private ActionCatalog catalog;

    @BeforeEach void open() {
        coordinator = new InProcessDelegationCoordinator();
        bridge = new DelegationBridge(coordinator);
        catalog = DelegationActions.register(ActionCatalog.defaults(ActionContext.create()),
                bridge);
        bridge.registerWorker(WorkerHello.newBuilder().setWorkerId(WORKER)
                .setProtocolVersion(1).setProvider("fixture")
                .addCapabilities(WorkerCapability.newBuilder().setName("report"))
                .build());
    }

    @AfterEach void close() {
        bridge.close();
        coordinator.close();
    }

    @Test void rejectEndsOnlyTheAddressedOfferedAttemptAndRecordsTheReason() throws Exception {
        String task = UUID.randomUUID().toString();
        bridge.offer(WORKER, task, DelegationFixtures.spec("fixture-report-valid"),
                Duration.ofSeconds(30), null);

        ObjectNode response = catalog.execute("delegation-reject", request(task, 1));

        assertThat(response.path("ok").asBoolean()).isTrue();
        assertThat(response.path("taskId").asText()).isEqualTo(task);
        assertThat(response.path("attempt").asInt()).isEqualTo(1);
        assertThat(coordinator.state().tasks().get(task).phase())
                .isEqualTo(DelegationReducer.Phase.REJECTED);
        assertThat(coordinator.transcript().getEntriesList().stream()
                .filter(entry -> entry.hasWorkerFrame()
                        && entry.getWorkerFrame().hasReject())
                .map(entry -> entry.getWorkerFrame().getReject()).toList())
                .singleElement().satisfies(reject -> {
                    assertThat(reject.getReason()).isEqualTo("unsupported offer");
                    assertThat(reject.getRetryable()).isTrue();
                });
        assertThat(coordinator.state().tasks().get(task).attempt()).isEqualTo(1);
        assertThat(bridge.offer(WORKER, task,
                DelegationFixtures.spec("fixture-report-valid"),
                Duration.ofSeconds(30), null).getAttempt()).isEqualTo(2);
    }

    @Test void invalidAttemptIsRejectedBeforeTheTranscriptChanges() throws Exception {
        String task = UUID.randomUUID().toString();
        bridge.offer(WORKER, task, DelegationFixtures.spec("fixture-report-valid"),
                Duration.ofSeconds(30), null);
        int entries = coordinator.transcript().getEntriesCount();

        ActionException invalid = catchThrowableOfType(
                () -> catalog.execute("delegation-reject", request(task, 0)),
                ActionException.class);

        assertThat(invalid.code()).isEqualTo("invalid-input");
        assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(entries);
        assertThat(coordinator.state().tasks().get(task).phase())
                .isEqualTo(DelegationReducer.Phase.OFFERED);
    }

    @Test void staleOrFutureAttemptCannotDeclineTheOpenOffer() throws Exception {
        String task = UUID.randomUUID().toString();
        bridge.offer(WORKER, task, DelegationFixtures.spec("fixture-report-valid"),
                Duration.ofSeconds(30), null);
        int entries = coordinator.transcript().getEntriesCount();

        ActionException refusal = catchThrowableOfType(
                () -> catalog.execute("delegation-reject", request(task, 2)),
                ActionException.class);

        assertThat(refusal).isNotNull();
        assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(entries);
        assertThat(coordinator.state().tasks().get(task).phase())
                .isEqualTo(DelegationReducer.Phase.OFFERED);
    }

    private static ObjectNode request(String task, int attempt) {
        return JSON.createObjectNode().put("workerId", WORKER).put("taskId", task)
                .put("attempt", attempt).put("reason", "unsupported offer")
                .put("retryable", true);
    }
}
