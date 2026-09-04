package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.google.protobuf.Any;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static ai.protomolt.proto.delegation.DelegationFixtures.TASK;
import static ai.protomolt.proto.delegation.DelegationFixtures.WORKER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The deliverable contract gate: a task whose spec names a deliverable type is judged on
 * the typed result the candidate carries, under the rules the contract's own descriptor
 * set declares, before any reviewer reads the summary.
 */
class DelegationDeliverableContractTest {

    private static final String CHECK = "unit-tests";

    private InProcessDelegationCoordinator coordinator;
    private DelegationBridge bridge;

    @AfterEach
    void closeCoordinator() {
        if (bridge != null) {
            bridge.close();
        }
        if (coordinator != null) {
            coordinator.close();
        }
    }

    @Test
    void aCandidateWithoutAResultIsRefusedWhenTheSpecDeclaresAContract() {
        List<DelegationReducer.Finding> findings = reduce(withContract(), candidate(null));

        assertThat(findings).singleElement()
                .satisfies(finding -> {
                    assertThat(finding.kind()).isEqualTo("contract");
                    assertThat(finding.error()).contains(DeliverableFixtures.TYPE_NAME);
                    assertThat(finding.error()).contains("carries no result");
                });
    }

    @Test
    void aResultOfTheWrongTypeIsRefused() {
        List<DelegationReducer.Finding> findings =
                reduce(withContract(), candidate(DeliverableFixtures.resultOfAnotherType()));

        assertThat(findings).singleElement()
                .satisfies(finding -> {
                    assertThat(finding.kind()).isEqualTo("contract");
                    assertThat(finding.error()).contains("google.protobuf.Timestamp");
                    assertThat(finding.error()).contains(DeliverableFixtures.TYPE_NAME);
                });
    }

    @Test
    void aResultThatViolatesADeclaredFieldRuleIsRefusedWithTheRule() {
        List<DelegationReducer.Finding> findings = reduce(withContract(),
                candidate(DeliverableFixtures.result("short", 3)));

        assertThat(findings).singleElement()
                .satisfies(finding -> {
                    assertThat(finding.kind()).isEqualTo("contract");
                    assertThat(finding.error())
                            .contains(DeliverableFixtures.HEADLINE_RULE);
                    assertThat(finding.error()).contains("result.headline");
                });
    }

    @Test
    void aResultThatViolatesTheMessageCelIsRefusedWithTheRuleMessage() {
        List<DelegationReducer.Finding> findings = reduce(withContract(),
                candidate(DeliverableFixtures.result("a headline long enough", 0)));

        assertThat(findings).singleElement()
                .satisfies(finding -> assertThat(finding.error())
                        .contains(DeliverableFixtures.CEL_MESSAGE));
    }

    @Test
    void aValidResultMovesTheTaskToCandidate() {
        Transcript transcript = transcript(withContract(),
                candidate(DeliverableFixtures.result("a headline long enough", 4)));
        DelegationReducer.Result result = new DelegationReducer().reduce(transcript);

        assertThat(result.findings()).isEmpty();
        assertThat(result.tasks().get(TASK).phase())
                .isEqualTo(DelegationReducer.Phase.CANDIDATE);
    }

    @Test
    void aResultWithoutADeclaredContractIsRefused() {
        List<DelegationReducer.Finding> findings = reduce(DelegationFixtures.spec(CHECK),
                candidate(DeliverableFixtures.result("a headline long enough", 4)));

        assertThat(findings).singleElement()
                .satisfies(finding -> {
                    assertThat(finding.kind()).isEqualTo("contract");
                    assertThat(finding.error()).contains("declares no deliverable contract");
                });
    }

    @Test
    void theOfferRendersTheContractSchemaWhenTheRequestLeavesItEmpty() {
        coordinator = new InProcessDelegationCoordinator();
        register();

        TaskOffer offer = coordinator.offer(WORKER, UUID.randomUUID().toString(),
                withContract(), Duration.ofSeconds(60));

        String schema = offer.getSpec().getContract().getJsonSchema();
        assertThat(schema).contains("headline").contains("findings");
        assertThat(schema).contains("minLength");
    }

    @Test
    void anOfferNamingATypeAbsentFromItsDescriptorSetIsRefused() {
        coordinator = new InProcessDelegationCoordinator();
        register();
        TaskSpec spec = DelegationFixtures.spec(CHECK).toBuilder()
                .setContract(DeliverableFixtures.contract("delivery.v1.NoSuchReport"))
                .build();

        assertThatThrownBy(() -> coordinator.offer(WORKER,
                UUID.randomUUID().toString(), spec, Duration.ofSeconds(60)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("delivery.v1.NoSuchReport");
    }

    private void register() {
        bridge = new DelegationBridge(coordinator);
        bridge.registerWorker(WorkerHello.newBuilder()
                .setWorkerId(WORKER)
                .setProtocolVersion(1)
                .setProvider("sol")
                .addCapabilities(WorkerCapability.newBuilder().setName("java-build"))
                .build());
    }

    private static TaskSpec withContract() {
        return DelegationFixtures.spec(CHECK).toBuilder()
                .setContract(DeliverableFixtures.contract())
                .build();
    }

    private static CompletionCandidate candidate(Any result) {
        CompletionCandidate.Builder candidate = CompletionCandidate.newBuilder()
                .setAttempt(1)
                .setRevision(1)
                .setSummary("the review report is written")
                .addEvidence(DelegationFixtures.evidence(CHECK))
                .addCommits(DelegationFixtures.commit("deliverable"));
        if (result != null) {
            candidate.setResult(result);
        }
        return candidate.build();
    }

    private static List<DelegationReducer.Finding> reduce(TaskSpec spec,
                                                          CompletionCandidate candidate) {
        return new DelegationReducer().reduce(transcript(spec, candidate)).findings();
    }

    private static Transcript transcript(TaskSpec spec, CompletionCandidate candidate) {
        return new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER)
                .admit(WORKER)
                .offer(TASK, WORKER, 1, spec)
                .accept(TASK, WORKER, 1)
                .candidateWith(TASK, WORKER, 1, candidate)
                .build();
    }
}
