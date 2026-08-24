package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ProtoAction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The input schemas the delegation verbs publish.
 *
 * <p>These schemas are derived from the request messages rather than written by hand, so a
 * caller reading the tool manifest sees the same contract the verb enforces. The assertions
 * below check that the derivation actually carries the declared rules through, because a
 * schema that merely says "object" would still serve, still parse, and still pass every
 * behavioural test while telling every caller nothing.
 */
class DelegationActionSchemaTest {

    private InProcessDelegationCoordinator coordinator;
    private DelegationBridge bridge;

    @BeforeEach
    void open() {
        coordinator = new InProcessDelegationCoordinator();
        bridge = new DelegationBridge(coordinator);
    }

    @AfterEach
    void close() {
        bridge.close();
        coordinator.close();
    }

    private List<ProtoAction> verbs() {
        return List.of(
                new DelegationWorkerRegisterAction(bridge),
                new DelegationWorkerListAction(bridge),
                new DelegationOfferAction(bridge),
                new DelegationAcceptAction(bridge),
                new DelegationProgressAction(bridge),
                new DelegationCheckpointAction(bridge),
                new DelegationCandidateAction(bridge),
                new DelegationReviewAction(bridge),
                new DelegationCancelAction(bridge),
                new DelegationMessageAction(bridge),
                new DelegationWatchAction(bridge),
                new DelegationTranscriptAction(bridge));
    }

    private ObjectNode schemaOf(String verb) {
        ProtoAction action = verbs().stream()
                .filter(candidate -> candidate.name().equals(verb))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no verb named " + verb));
        return action.inputSchema();
    }

    /**
     * The request message's own definition, resolved through the document's root reference.
     * The generator emits a reference plus a definitions block rather than one inline object,
     * because a message that reaches the same nested type twice must describe it once.
     */
    private static JsonNode root(ObjectNode schema) {
        String ref = schema.path("$ref").asText();
        assertThat(ref).as("root reference").startsWith("#/$defs/");
        JsonNode definition = schema.path("$defs").path(ref.substring("#/$defs/".length()));
        assertThat(definition.isMissingNode()).as("definition for %s", ref).isFalse();
        return definition;
    }

    private static JsonNode property(ObjectNode schema, String field) {
        JsonNode property = root(schema).path("properties").path(field);
        assertThat(property.isMissingNode()).as("property %s", field).isFalse();
        return property;
    }

    /**
     * Every verb but the roster listing takes arguments, and each names its fields. The
     * listing takes none, which the schema states as an empty property set rather than by
     * leaving the shape open.
     */
    @Test
    void everyVerbDescribesItsFieldsRatherThanAcceptingAnyObject() {
        for (ProtoAction verb : verbs()) {
            JsonNode definition = root(verb.inputSchema());
            assertThat(definition.path("properties").isObject())
                    .as("%s properties", verb.name()).isTrue();
            if (!"delegation-worker-list".equals(verb.name())) {
                assertThat(definition.path("properties"))
                        .as("%s field count", verb.name()).isNotEmpty();
            }
        }
    }

    /**
     * A task spec used to be an opaque object in the offer schema. It is the whole substance
     * of the offer, so a caller has to be able to see its shape before writing one.
     */
    @Test
    void theTaskSpecIsDescribedRatherThanLeftOpaque() {
        ObjectNode schema = schemaOf("delegation-offer");
        JsonNode spec = property(schema, "spec");
        String ref = spec.path("$ref").asText();
        assertThat(ref).as("spec reference").isEqualTo(
                "#/$defs/ai.pipestream.proto.delegation.v1.TaskSpec");
        JsonNode definition = schema.path("$defs")
                .path("ai.pipestream.proto.delegation.v1.TaskSpec");
        assertThat(definition.path("properties").path("objective").path("maxLength").asInt())
                .isEqualTo(16_384);
        assertThat(definition.path("properties").path("requiredChecks").path("minItems").asInt())
                .isEqualTo(1);
    }

    /** The lease bound exists so no offer can hold a worker beyond a day. */
    @Test
    void theLeaseBoundReachesCallersThroughTheSchema() {
        JsonNode lease = property(schemaOf("delegation-offer"), "leaseSeconds");
        assertThat(lease.path("minimum").asInt()).isEqualTo(1);
        assertThat(lease.path("maximum").asInt()).isEqualTo(86_400);
    }

    /** An offer must name the worker it addresses, so the identity is declared required. */
    @Test
    void theOfferDeclaresItsRequiredIdentities() {
        ObjectNode schema = schemaOf("delegation-offer");
        JsonNode required = root(schema).path("required");
        assertThat(required.toString()).contains("workerId").contains("spec");
        assertThat(property(schema, "workerId").path("maxLength").asInt()).isEqualTo(128);
    }

    /** Every task-addressed verb keys on a uuid, and the schema says so. */
    @Test
    void taskIdentitiesPublishTheirUuidShape() {
        for (String verb : List.of("delegation-accept", "delegation-cancel",
                "delegation-candidate", "delegation-checkpoint", "delegation-progress",
                "delegation-review", "delegation-message")) {
            assertThat(property(schemaOf(verb), "taskId").path("format").asText())
                    .as("%s taskId format", verb).isEqualTo("uuid");
        }
    }

    /** Attempt numbers are bounded on every verb that names one. */
    @Test
    void attemptNumbersCarryTheSameBoundEverywhere() {
        for (String verb : List.of("delegation-accept", "delegation-checkpoint",
                "delegation-progress")) {
            JsonNode attempt = property(schemaOf(verb), "attempt");
            assertThat(attempt.path("minimum").asInt()).as("%s attempt minimum", verb)
                    .isEqualTo(1);
            assertThat(attempt.path("maximum").asInt()).as("%s attempt maximum", verb)
                    .isEqualTo(1_024);
        }
    }

    /** The review decision is an enum, so its legal values are visible rather than guessed. */
    @Test
    void theReviewDecisionPublishesItsLegalValues() {
        JsonNode decision = property(schemaOf("delegation-review"), "decision");
        assertThat(decision.toString())
                .contains("REVIEW_DECISION_ACCEPT")
                .contains("REVIEW_DECISION_REVISE");
    }

    /**
     * Which fields a review decision needs is a cross-field rule, expressed as message-level
     * CEL. JSON Schema cannot state it, so the generator carries it as documentation; a
     * caller that never sees it writes a request the verb refuses.
     */
    @Test
    void theReviewCrossFieldRulesReachCallersAsDocumentedRules() {
        String rules = root(schemaOf("delegation-review")).path("x-pipestream-cel").toString();
        assertThat(rules)
                .contains("acceptance-carries-verdict")
                .contains("revision-carries-feedback")
                .contains("failed-checks-belong-to-a-revision");
    }

    /** The message verb's direction rules are declared the same way. */
    @Test
    void theMessageDirectionRulesReachCallersAsDocumentedRules() {
        String rules = root(schemaOf("delegation-message")).path("x-pipestream-cel").toString();
        assertThat(rules)
                .contains("coordinator-message-names-recipient")
                .contains("worker-message-addresses-coordinator");
    }

    /** Batch sizes are bounded so one call cannot ask for the whole feed. */
    @Test
    void feedBatchBoundsReachCallersThroughTheSchema() {
        JsonNode maxEvents = property(schemaOf("delegation-watch"), "maxEvents");
        assertThat(maxEvents.path("minimum").asInt()).isEqualTo(1);
        assertThat(maxEvents.path("maximum").asInt()).isEqualTo(256);
        JsonNode timeout = property(schemaOf("delegation-watch"), "timeoutMs");
        assertThat(timeout.path("minimum").asInt()).isEqualTo(0);
        assertThat(timeout.path("maximum").asInt()).isEqualTo(30_000);
        JsonNode maxEntries = property(schemaOf("delegation-transcript"), "maxEntries");
        assertThat(maxEntries.path("minimum").asInt()).isEqualTo(1);
        assertThat(maxEntries.path("maximum").asInt()).isEqualTo(500);
    }

    /** Capability entries are bounded in count and in the shape of each entry. */
    @Test
    void workerRegistrationBoundsItsCapabilityList() {
        ObjectNode schema = schemaOf("delegation-worker-register");
        JsonNode capabilities = property(schema, "capabilities");
        assertThat(capabilities.path("type").asText()).isEqualTo("array");
        assertThat(capabilities.path("maxItems").asInt()).isEqualTo(64);
        JsonNode capability = schema.path("$defs")
                .path("ai.pipestream.proto.delegation.v1.WorkerCapability");
        assertThat(capability.path("properties").path("name").path("maxLength").asInt())
                .isEqualTo(128);
        assertThat(capability.path("required").toString()).contains("name");
    }
}
