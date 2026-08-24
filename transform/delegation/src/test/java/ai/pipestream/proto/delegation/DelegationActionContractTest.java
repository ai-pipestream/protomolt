package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.delegation.v1.WorkerCapability;
import ai.pipestream.proto.delegation.v1.WorkerHello;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The delegation verbs enforce their request messages' declared rules.
 *
 * <p>The catalog path does not sit behind the validating gRPC interceptor, so a rule the
 * request declares only reaches a caller if the verb runs the validator itself. These tests
 * pin that, and pin the two places where an omitted value has to keep meaning what it meant
 * before the envelopes became protobuf messages: proto3 delivers an absent scalar as its zero.
 */
class DelegationActionContractTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WORKER = "contract-kimi";

    private InProcessDelegationCoordinator coordinator;
    private DelegationBridge bridge;
    private ActionContext context;

    @BeforeEach
    void open() {
        coordinator = new InProcessDelegationCoordinator();
        bridge = new DelegationBridge(coordinator);
        context = ActionContext.create();
        bridge.registerWorker(WorkerHello.newBuilder()
                .setWorkerId(WORKER)
                .setProtocolVersion(1)
                .setProvider("kimi")
                .addCapabilities(WorkerCapability.newBuilder().setName("java-build"))
                .build());
    }

    @AfterEach
    void close() {
        bridge.close();
        coordinator.close();
    }

    private static ObjectNode envelope() {
        return MAPPER.createObjectNode();
    }

    /** An offer with no lease still runs on the coordinator's default, not on zero seconds. */
    @Test
    void anOmittedLeaseKeepsMeaningTheDefaultRatherThanZero() throws Exception {
        ObjectNode output = new DelegationOfferAction(bridge).execute(offerEnvelope(), context);
        assertThat(output.path("offer").path("leaseDuration").asText())
                .isEqualTo(DelegationOfferAction.DEFAULT_LEASE_SECONDS + "s");
    }

    /** An offer with no task id still opens a new task under a generated uuid. */
    @Test
    void anOmittedTaskIdStillOpensANewTask() throws Exception {
        ObjectNode output = new DelegationOfferAction(bridge).execute(offerEnvelope(), context);
        assertThat(UUID.fromString(output.path("taskId").asText())).isNotNull();
    }

    /** A lease outside the declared range is refused, naming the field and the rule. */
    @Test
    void aLeaseBeyondADayIsRefusedByTheDeclaredRule() {
        ObjectNode envelope = offerEnvelope().put("leaseSeconds", 86_401);
        ActionException refusal = catchThrowableOfType(
                () -> new DelegationOfferAction(bridge).execute(envelope, context),
                ActionException.class);
        assertThat(refusal.code()).isEqualTo("invalid-input");
        assertThat(refusal.details().orElseThrow().toString())
                .contains("lease_seconds").contains("int32.gte_lte");
    }

    /** Accepting a candidate without the verdict that accepted it is refused by the CEL rule. */
    @Test
    void anAcceptanceWithoutAVerdictIsRefusedByTheCrossFieldRule() {
        ObjectNode envelope = envelope()
                .put("taskId", UUID.randomUUID().toString())
                .put("decision", "REVIEW_DECISION_ACCEPT");
        ActionException refusal = catchThrowableOfType(
                () -> new DelegationReviewAction(bridge).execute(envelope, context),
                ActionException.class);
        assertThat(refusal.code()).isEqualTo("invalid-input");
        assertThat(refusal.details().orElseThrow().toString())
                .contains("acceptance-carries-verdict");
    }

    /** A coordinator message that names no worker is refused by the CEL rule. */
    @Test
    void aCoordinatorMessageWithoutARecipientIsRefusedByTheCrossFieldRule() {
        ObjectNode envelope = envelope()
                .put("taskId", UUID.randomUUID().toString())
                .put("sender", "coordinator")
                .put("kind", "TASK_MESSAGE_KIND_GUIDANCE")
                .put("text", "keep going");
        ActionException refusal = catchThrowableOfType(
                () -> new DelegationMessageAction(bridge).execute(envelope, context),
                ActionException.class);
        assertThat(refusal.details().orElseThrow().toString())
                .contains("coordinator-message-names-recipient");
    }

    /** A worker message addressed to another worker is refused by the CEL rule. */
    @Test
    void aWorkerMessageAddressedElsewhereIsRefusedByTheCrossFieldRule() {
        ObjectNode envelope = envelope()
                .put("taskId", UUID.randomUUID().toString())
                .put("sender", WORKER)
                .put("recipient", "other-worker")
                .put("kind", "TASK_MESSAGE_KIND_NOTE")
                .put("text", "sidebar");
        ActionException refusal = catchThrowableOfType(
                () -> new DelegationMessageAction(bridge).execute(envelope, context),
                ActionException.class);
        assertThat(refusal.details().orElseThrow().toString())
                .contains("worker-message-addresses-coordinator");
    }

    /** A member the request does not declare is reported, never quietly dropped. */
    @Test
    void anUndeclaredMemberIsRefusedRatherThanIgnored() {
        ObjectNode envelope = offerEnvelope().put("leaseSecs", 60);
        assertThatThrownBy(() -> new DelegationOfferAction(bridge).execute(envelope, context))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("leaseSecs");
    }

    private static ObjectNode offerEnvelope() {
        ObjectNode envelope = envelope().put("workerId", WORKER);
        try {
            envelope.set("spec", MAPPER.readTree(
                    JsonFormat.printer().print(DelegationFixtures.spec("unit-tests"))));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return envelope;
    }
}
