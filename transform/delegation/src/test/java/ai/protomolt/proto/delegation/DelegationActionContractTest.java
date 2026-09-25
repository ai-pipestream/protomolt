package ai.protomolt.proto.delegation;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The delegation verbs enforce their request messages' declared rules.
 *
 * <p>The rules are checked where every surface converges, so these dispatch through the
 * catalog rather than calling a verb directly: a verb reached over gRPC and a verb reached as
 * a JSON tool call are held to the same contract, and calling one in isolation would test
 * neither. They also pin the two places where an omitted value has to keep meaning what it
 * meant before the envelopes became protobuf messages: proto3 delivers an absent scalar as
 * its zero.
 */
class DelegationActionContractTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WORKER = "contract-kimi";

    private InProcessDelegationCoordinator coordinator;
    private DelegationBridge bridge;
    private ActionCatalog catalog;

    @BeforeEach
    void open() {
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
        ObjectNode output = dispatch("delegation-offer", offerEnvelope());
        assertThat(output.path("offer").path("leaseDuration").asText())
                .isEqualTo(DelegationOfferAction.DEFAULT_LEASE_SECONDS + "s");
    }

    /** An offer with no task id still opens a new task under a generated uuid. */
    @Test
    void anOmittedTaskIdStillOpensANewTask() throws Exception {
        ObjectNode output = dispatch("delegation-offer", offerEnvelope());
        assertThat(UUID.fromString(output.path("taskId").asText())).isNotNull();
    }

    /** A lease outside the declared range is refused, naming the field and the rule. */
    @Test
    void aLeaseBeyondADayIsRefusedByTheDeclaredRule() {
        ObjectNode envelope = offerEnvelope().put("leaseSeconds", 86_401);
        ActionException refusal = catchThrowableOfType(
                () -> dispatch("delegation-offer", envelope),
                ActionException.class);
        assertThat(refusal.code()).isEqualTo("invalid-input");
        // The field is named as the caller wrote it, not by its proto path, so a pointer
        // into the envelope leads to the member that was refused.
        assertThat(refusal.details().orElseThrow().toString())
                .contains("leaseSeconds").contains("int32.gte_lte");
    }

    /** Accepting a candidate without the verdict that accepted it is refused by the CEL rule. */
    @Test
    void anAcceptanceWithoutAVerdictIsRefusedByTheCrossFieldRule() {
        ObjectNode envelope = envelope()
                .put("taskId", UUID.randomUUID().toString())
                .put("attempt", 1)
                .put("revision", 1)
                .put("decision", "REVIEW_DECISION_ACCEPT");
        ActionException refusal = catchThrowableOfType(
                () -> dispatch("delegation-review", envelope),
                ActionException.class);
        assertThat(refusal.code()).isEqualTo("invalid-input");
        assertThat(refusal.details().orElseThrow().toString())
                .contains("acceptance-carries-verdict");
    }

    /** A review must bind its verdict to one attempt and candidate revision. */
    @Test
    void aReviewWithoutCandidateSelectorsIsRefusedByTheDeclaredRules() {
        ObjectNode envelope = envelope()
                .put("taskId", UUID.randomUUID().toString())
                .put("decision", "REVIEW_DECISION_ACCEPT")
                .put("verdict", "verified");
        ActionException refusal = catchThrowableOfType(
                () -> dispatch("delegation-review", envelope),
                ActionException.class);
        assertThat(refusal.code()).isEqualTo("invalid-input");
        assertThat(refusal.details().orElseThrow().toString())
                .contains("attempt").contains("revision");
    }

    @ParameterizedTest
    @CsvSource({
            "attempt, 0", "attempt, -1", "attempt, 1025",
            "revision, 0", "revision, -1", "revision, 1025"
    })
    void reviewSelectorsOutsideTheRuntimeBoundAreRefusedBeforeLifecycleLookup(
            String selector, int value) {
        ObjectNode request = reviewEnvelope().put(selector, value);
        int entriesBefore = coordinator.transcript().getEntriesCount();

        ActionException refusal = catchThrowableOfType(
                () -> dispatch("delegation-review", request), ActionException.class);

        assertThat(refusal.code()).isEqualTo("invalid-input");
        assertThat(refusal.details().orElseThrow().toString())
                .contains(selector).contains("int32.gte_lte");
        assertThat(coordinator.transcript().getEntriesCount())
                .as("contract rejection must happen before any coordinator lookup or write")
                .isEqualTo(entriesBefore);
    }

    @ParameterizedTest
    @CsvSource({
            "attempt, 1", "attempt, 1024", "revision, 1", "revision, 1024"
    })
    void reviewSelectorsAtBothRuntimeBoundsPassValidationBeforeLifecycleLookup(
            String selector, int value) {
        ObjectNode request = reviewEnvelope().put(selector, value);
        int entriesBefore = coordinator.transcript().getEntriesCount();

        ActionException lifecycleRefusal = catchThrowableOfType(
                () -> dispatch("delegation-review", request), ActionException.class);

        // The task does not exist, so dispatch reaches coordinator state lookup. A contract
        // refusal here would mean the declared inclusive endpoint is not actually accepted.
        assertThat(lifecycleRefusal.code()).isEqualTo("delegation-rejected");
        assertThat(lifecycleRefusal.code()).isNotEqualTo("invalid-input");
        assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(entriesBefore);
    }

    private static ObjectNode reviewEnvelope() {
        return envelope()
                .put("taskId", UUID.randomUUID().toString())
                .put("decision", "REVIEW_DECISION_ACCEPT")
                .put("verdict", "verified")
                .put("attempt", 1)
                .put("revision", 1);
    }

    /** A delayed review for a different candidate is rejected without adding a verdict. */
    @Test
    void aReviewForTheWrongCandidateDoesNotMutateTheTranscript() throws Exception {
        String taskId = UUID.randomUUID().toString();
        bridge.offer(WORKER, taskId, DelegationFixtures.spec("unit-tests"),
                java.time.Duration.ofSeconds(30), null);
        bridge.accept(WORKER, taskId, 1);
        bridge.submitCandidate(WORKER, taskId, CompletionCandidate.newBuilder()
                .setAttempt(1)
                .setRevision(1)
                .setSummary("implemented and proven")
                .addEvidence(DelegationFixtures.evidence("unit-tests"))
                .addCommits(DelegationFixtures.commit("contract-output"))
                .build());
        int entriesBefore = coordinator.transcript().getEntriesCount();

        ActionException refusal = catchThrowableOfType(
                () -> dispatch("delegation-review", envelope()
                        .put("taskId", taskId)
                        .put("attempt", 1)
                        .put("revision", 2)
                        .put("decision", "REVIEW_DECISION_ACCEPT")
                        .put("verdict", "stale verdict")),
                ActionException.class);
        assertThat(refusal.code()).isEqualTo("delegation-rejected");
        assertThat(refusal.getMessage()).contains("does not match the open candidate");
        assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(entriesBefore);
        assertThat(coordinator.state().tasks().get(taskId).phase())
                .isEqualTo(DelegationReducer.Phase.CANDIDATE);

        ObjectNode accepted = dispatch("delegation-review", envelope()
                .put("taskId", taskId)
                .put("attempt", 1)
                .put("revision", 1)
                .put("decision", "REVIEW_DECISION_ACCEPT")
                .put("verdict", "verified"));
        assertThat(accepted.path("ok").asBoolean()).isTrue();
        assertThat(coordinator.state().tasks().get(taskId).phase())
                .isEqualTo(DelegationReducer.Phase.ACCEPTED);
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
                () -> dispatch("delegation-message", envelope),
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
                () -> dispatch("delegation-message", envelope),
                ActionException.class);
        assertThat(refusal.details().orElseThrow().toString())
                .contains("worker-message-addresses-coordinator");
    }

    /** A member the request does not declare is reported, never quietly dropped. */
    @Test
    void anUndeclaredMemberIsRefusedRatherThanIgnored() {
        ObjectNode envelope = offerEnvelope().put("leaseSecs", 60);
        assertThatThrownBy(() -> dispatch("delegation-offer", envelope))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("leaseSecs");
    }

    private ObjectNode dispatch(String verb, ObjectNode envelope) throws ActionException {
        return catalog.execute(verb, envelope);
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
