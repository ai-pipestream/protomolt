package ai.protomolt.proto.delegation;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Any;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The deliverable travels over the catalog path as proto3 JSON. An {@code Any} whose type
 * lives only in the offer's descriptor set cannot be parsed by the default registry, so the
 * delegation verbs hand the catalog a registry built from the contracts of the coordinator's
 * own tasks; without it the same document would be refused as unparseable.
 */
class DelegationDeliverableActionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WORKER = "deliverable-kimi";
    private static final String CHECK = "unit-tests";

    private InProcessDelegationCoordinator coordinator;
    private DelegationBridge bridge;
    private ActionCatalog catalog;
    private String taskId;

    @BeforeEach
    void open() throws Exception {
        coordinator = new InProcessDelegationCoordinator();
        bridge = new DelegationBridge(coordinator);
        catalog = DelegationActions.register(
                ActionCatalog.defaults(ActionContext.create()), bridge);
        bridge.registerWorker(WorkerHello.newBuilder()
                .setWorkerId(WORKER)
                .setProtocolVersion(1)
                .setProvider("kimi")
                .addCapabilities(WorkerCapability.newBuilder().setName("java-build"))
                .build());
        ObjectNode offered = catalog.execute("delegation-offer", offerEnvelope());
        taskId = offered.path("taskId").asText();
        catalog.execute("delegation-accept", MAPPER.createObjectNode()
                .put("workerId", WORKER)
                .put("taskId", taskId)
                .put("attempt", 1));
    }

    @AfterEach
    void close() {
        bridge.close();
        coordinator.close();
    }

    /** The offer a worker reads carries the schema of the deliverable it must produce. */
    @Test
    void theOfferedSpecPublishesTheRenderedDeliverableSchema() throws Exception {
        ObjectNode offered = catalog.execute("delegation-offer", offerEnvelope()
                .put("taskId", java.util.UUID.randomUUID().toString()));
        String schema = offered.path("offer").path("spec").path("contract")
                .path("jsonSchema").asText();
        assertThat(schema).contains("headline").contains("findings");
    }

    /** A contract naming a type absent from its own set is bad input, not a coordinator fault. */
    @Test
    void anOfferWhoseContractNamesAnAbsentTypeIsRefusedAsInvalidInput() throws Exception {
        ObjectNode envelope = MAPPER.createObjectNode().put("workerId", WORKER);
        envelope.set("spec", MAPPER.readTree(JsonFormat.printer().print(
                DelegationFixtures.spec(CHECK).toBuilder()
                        .setContract(DeliverableFixtures.contract("delivery.v1.NoSuchReport"))
                        .build())));

        ActionException refusal = catchThrowableOfType(
                () -> catalog.execute("delegation-offer", envelope), ActionException.class);

        assertThat(refusal.code()).isEqualTo("invalid-input");
        assertThat(refusal.getMessage()).contains("delivery.v1.NoSuchReport");
    }

    @Test
    void aCandidateCarryingItsResultAsJsonIsTakenUnderReview() throws Exception {
        ObjectNode envelope = candidateEnvelope(
                DeliverableFixtures.result("a headline long enough", 4));
        assertThat(envelope.path("candidate").path("result").path("@type").asText())
                .isEqualTo("type.googleapis.com/" + DeliverableFixtures.TYPE_NAME);

        ObjectNode output = catalog.execute("delegation-candidate", envelope);

        assertThat(output.path("ok").asBoolean()).isTrue();
        assertThat(output.path("revision").asInt()).isEqualTo(1);
    }

    @Test
    void aCandidateWhoseResultViolatesTheContractIsRefusedWithTheRule() {
        ActionException refusal = catchThrowableOfType(
                () -> catalog.execute("delegation-candidate",
                        candidateEnvelope(DeliverableFixtures.result("short", 4))),
                ActionException.class);

        assertThat(refusal.getMessage())
                .contains("contract")
                .contains(DeliverableFixtures.HEADLINE_RULE);
    }

    private ObjectNode candidateEnvelope(Any result) throws Exception {
        ObjectNode candidate = (ObjectNode) MAPPER.readTree(
                JsonFormat.printer()
                        .usingTypeRegistry(JsonFormat.TypeRegistry.newBuilder()
                                .add(DeliverableFixtures.reportType())
                                .build())
                        .print(ai.protomolt.proto.delegation.v1.CompletionCandidate.newBuilder()
                                .setAttempt(1)
                                .setRevision(1)
                                .setSummary("the review report is written")
                                .addEvidence(DelegationFixtures.evidence(CHECK))
                                .addCommits(DelegationFixtures.commit("deliverable"))
                                .setResult(result)
                                .build()));
        ObjectNode envelope = MAPPER.createObjectNode()
                .put("workerId", WORKER)
                .put("taskId", taskId);
        envelope.set("candidate", candidate);
        return envelope;
    }

    private static ObjectNode offerEnvelope() throws Exception {
        TaskSpec spec = DelegationFixtures.spec(CHECK).toBuilder()
                .setContract(DeliverableFixtures.contract())
                .build();
        ObjectNode envelope = MAPPER.createObjectNode().put("workerId", WORKER);
        envelope.set("spec", MAPPER.readTree(JsonFormat.printer().print(spec)));
        return envelope;
    }
}
