package ai.protomolt.proto.grpc.workflow;

import ai.protomolt.proto.grpc.workflow.v1.BranchEvidence;
import ai.protomolt.proto.grpc.workflow.v1.BranchFailurePolicy;
import ai.protomolt.proto.grpc.workflow.v1.EdgeEvidence;
import ai.protomolt.proto.grpc.workflow.v1.FanOutSpec;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowStep;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.grpc.workflow.v1.StepEvidence;
import ai.protomolt.proto.grpc.workflow.v1.StepStatus;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.validate.ProtoValidator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The typed-edge contract: every new persisted field's bounds, patterns, and shape
 * rules are enforced both by the hand-checked {@link WorkflowValidation} path and by the
 * declared validate.v1 annotations through {@link ProtoValidator}.
 */
class EdgeWorkflowValidationTest {

    private static TypedEdge edge() {
        return TypedEdge.newBuilder()
                .addSources("input")
                .setProduceType("example.v1.Document")
                .addRules("text = input.body")
                .build();
    }

    private static FanOutSpec fanOut() {
        return FanOutSpec.newBuilder()
                .setItems("items")
                .setMaxItems(8)
                .setMaxConcurrency(2)
                .setFailurePolicy(BranchFailurePolicy.BRANCH_FAILURE_POLICY_CONTINUE)
                .setCollectType("example.v1.Batch")
                .setCollectInto("results")
                .build();
    }

    /** The base workflow with its first step replaced by an edge-carrying one. */
    private static Workflow edgeWorkflow(WorkflowStep.Builder step) {
        Workflow base = TestWorkflows.workflow();
        WorkflowStep built = step.setName("tokenize")
                .setDependency("nlp")
                .setMethod("example.v1.Tokenizer/Tokenize")
                .setCompletion(
                        ai.protomolt.proto.grpc.workflow.v1.StepCompletion
                                .STEP_COMPLETION_LIVE)
                .build();
        return base.toBuilder().setSteps(0, built).build();
    }

    private static StepEvidence.Builder edgeStepEvidence() {
        return TestWorkflows.evidence().getSteps(0).toBuilder()
                .setEdge(EdgeEvidence.newBuilder()
                        .setEdgeFingerprint("a".repeat(64))
                        .setValidationPassed(true)
                        .setSourceCount(1)
                        .build());
    }

    private static RunEvidence evidenceWith(StepEvidence.Builder step) {
        return TestWorkflows.evidence().toBuilder().setSteps(0, step.build()).build();
    }

    @Test
    void aWellFormedEdgeAndFanOutPass() {
        assertThatCode(() -> WorkflowValidation.validate(
                edgeWorkflow(WorkflowStep.newBuilder()
                        .setEdge(edge()).setFanOut(fanOut()))))
                .doesNotThrowAnyException();
        // The declared annotations agree: the same content validates clean through
        // the validate.v1 framework.
        assertThat(ProtoValidator.create().validate(edge()).valid()).isTrue();
        assertThat(ProtoValidator.create().validate(fanOut()).valid()).isTrue();
    }

    @Test
    void anEdgeOwnsTheStepsMappingRules() {
        Workflow workflow = edgeWorkflow(WorkflowStep.newBuilder()
                .setEdge(edge()).addRules("text = input.body"));
        assertThatThrownBy(() -> WorkflowValidation.validate(workflow))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("the edge owns request mapping");
    }

    @Test
    void edgeSourcesAreBoundedStepNameTokens() {
        // Through the workflow, the declared annotations refuse the empty list first;
        // the standalone contract keeps its own message for direct compiler callers.
        assertThatThrownBy(() -> WorkflowValidation.validate(
                edgeWorkflow(WorkflowStep.newBuilder()
                        .setEdge(TypedEdge.newBuilder()
                                .setProduceType("example.v1.Document")))))
                .hasMessageContaining("at least 1");
        assertThatThrownBy(() -> WorkflowValidation.validate(TypedEdge.newBuilder()
                .setProduceType("example.v1.Document").build()))
                .hasMessageContaining("edge.sources must not be empty");
        assertThatThrownBy(() -> WorkflowValidation.validate(
                edgeWorkflow(WorkflowStep.newBuilder()
                        .setEdge(edge().toBuilder().addSources("not a name")))))
                .hasMessageContaining("edge.sources");
        TypedEdge.Builder sixtyFive = TypedEdge.newBuilder()
                .setProduceType("example.v1.Document");
        for (int i = 0; i < 65; i++) {
            sixtyFive.addSources("s" + i);
        }
        assertThatThrownBy(() -> WorkflowValidation.validate(
                edgeWorkflow(WorkflowStep.newBuilder().setEdge(sixtyFive))))
                .hasMessageContaining("at most 64 items");
        // The declared annotations enforce the same bounds.
        assertThat(ProtoValidator.create().validate(sixtyFive.build()).valid())
                .isFalse();
        assertThat(ProtoValidator.create().validate(TypedEdge.newBuilder()
                .setProduceType("example.v1.Document").build()).valid()).isFalse();
    }

    @Test
    void edgeTypesMustBeFullyQualifiedNames() {
        assertThatThrownBy(() -> WorkflowValidation.validate(
                edgeWorkflow(WorkflowStep.newBuilder()
                        .setEdge(edge().toBuilder().setProduceType("not a type")))))
                .hasMessageContaining("produce_type");
        assertThatThrownBy(() -> WorkflowValidation.validate(
                edgeWorkflow(WorkflowStep.newBuilder()
                        .setEdge(edge().toBuilder().setProjectTo("bad type!")))))
                .hasMessageContaining("project_to");
        assertThat(ProtoValidator.create().validate(edge().toBuilder()
                .setProjectTo("bad type!").build()).valid()).isFalse();
    }

    @Test
    void fanOutRequiresAnEdgeAndBoundedFields() {
        assertThatThrownBy(() -> WorkflowValidation.validate(
                edgeWorkflow(WorkflowStep.newBuilder().setFanOut(fanOut()))))
                .hasMessageContaining("fan_out requires step.edge");

        WorkflowStep.Builder withEdge = WorkflowStep.newBuilder().setEdge(edge());
        assertThatThrownBy(() -> WorkflowValidation.validate(edgeWorkflow(withEdge
                .setFanOut(fanOut().toBuilder().setMaxItems(0)))))
                .hasMessageContaining("max_items");
        assertThatThrownBy(() -> WorkflowValidation.validate(edgeWorkflow(withEdge
                .setFanOut(fanOut().toBuilder().setMaxItems(1025)))))
                .hasMessageContaining("max_items");
        assertThatThrownBy(() -> WorkflowValidation.validate(edgeWorkflow(withEdge
                .setFanOut(fanOut().toBuilder().setMaxConcurrency(65)))))
                .hasMessageContaining("max_concurrency");
        assertThatThrownBy(() -> WorkflowValidation.validate(edgeWorkflow(withEdge
                .setFanOut(fanOut().toBuilder().setFailurePolicy(
                        BranchFailurePolicy.BRANCH_FAILURE_POLICY_UNSPECIFIED)))))
                .hasMessageContaining("failure_policy");
        assertThatThrownBy(() -> WorkflowValidation.validate(edgeWorkflow(withEdge
                .setFanOut(fanOut().toBuilder().setCollectInto("not a field")))))
                .hasMessageContaining("collect_into");
        assertThatThrownBy(() -> WorkflowValidation.validate(edgeWorkflow(withEdge
                .setFanOut(fanOut().toBuilder().setItems(" ")))))
                .hasMessageContaining("items");
        // The declared annotations enforce the same bounds.
        assertThat(ProtoValidator.create().validate(fanOut().toBuilder()
                .setMaxConcurrency(65).build()).valid()).isFalse();
        assertThat(ProtoValidator.create().validate(fanOut().toBuilder()
                .setFailurePolicy(BranchFailurePolicy.BRANCH_FAILURE_POLICY_UNSPECIFIED)
                .build()).valid()).isFalse();
    }

    @Test
    void edgeEvidenceCarriesAFingerprintAndBoundedCounts() {
        assertThatCode(() -> WorkflowValidation.validate(
                evidenceWith(edgeStepEvidence()))).doesNotThrowAnyException();

        assertThatThrownBy(() -> WorkflowValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(edgeStepEvidence().getEdge().toBuilder()
                        .setEdgeFingerprint("not-a-fingerprint")))))
                .hasMessageContaining("edge_fingerprint");
        // The declared annotation alone carries the fingerprint format: this pins
        // the pattern-to-sha256_hex conversion independently of the hand check.
        assertThat(ProtoValidator.create().validate(edgeStepEvidence().getEdge().toBuilder()
                .setEdgeFingerprint("not-a-fingerprint").build())
                .violations())
                .anyMatch(v -> v.ruleId().equals("string.sha256_hex"));
        assertThatThrownBy(() -> WorkflowValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(edgeStepEvidence().getEdge().toBuilder()
                        .setSourceCount(0)))))
                .hasMessageContaining("source_count");
        assertThatThrownBy(() -> WorkflowValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(edgeStepEvidence().getEdge().toBuilder()
                        .setItemCount(1025)))))
                .hasMessageContaining("item_count");
        // A succeeded step without branches records a clean verdict.
        assertThatThrownBy(() -> WorkflowValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(edgeStepEvidence().getEdge().toBuilder()
                        .setValidationPassed(false)))))
                .hasMessageContaining("validation_passed true");
    }

    @Test
    void branchEvidenceIsBoundedAndSelfConsistent() {
        EdgeEvidence.Builder twoBranches = edgeStepEvidence().getEdge().toBuilder()
                .setItemCount(2)
                .addBranches(BranchEvidence.newBuilder()
                        .setBranchId("tokenize#0")
                        .setStatus(StepStatus.STEP_STATUS_SUCCEEDED)
                        .setResponseArtifact(TestWorkflows.artifact("{}", true)))
                .addBranches(BranchEvidence.newBuilder()
                        .setBranchId("tokenize#1")
                        .setStatus(StepStatus.STEP_STATUS_FAILED)
                        .setSummary("rejected"));
        assertThatCode(() -> WorkflowValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(twoBranches)))).doesNotThrowAnyException();

        // Branches must cover every item.
        assertThatThrownBy(() -> WorkflowValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(twoBranches.clone().setItemCount(3)))))
                .hasMessageContaining("must cover every item");
        // No branches without fan-out items.
        assertThatThrownBy(() -> WorkflowValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(twoBranches.clone().setItemCount(0)))))
                .hasMessageContaining("must be empty when item_count is 0");
        // Branch identities name their own step.
        assertThatThrownBy(() -> WorkflowValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(twoBranches.clone().setBranches(0,
                        twoBranches.getBranches(0).toBuilder()
                                .setBranchId("other#0"))))))
                .hasMessageContaining("its own step");
        // The step-name half of a branch id follows the slug contract...
        assertThatThrownBy(() -> WorkflowValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(twoBranches.clone().setBranches(0,
                        twoBranches.getBranches(0).toBuilder()
                                .setBranchId("Tokenize#0"))))))
                .hasMessageContaining("'<step-name>#<index>'");
        // ...and the index half is at most four digits, refused by the branch_id
        // pattern annotation before the hand scan sees it.
        assertThatThrownBy(() -> WorkflowValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(twoBranches.clone().setBranches(0,
                        twoBranches.getBranches(0).toBuilder()
                                .setBranchId("tokenize#00000"))))))
                .hasMessageContaining("branch_id");
        // Branch status is SUCCEEDED or FAILED only.
        assertThatThrownBy(() -> WorkflowValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(twoBranches.clone().setBranches(0,
                        twoBranches.getBranches(0).toBuilder()
                                .setStatus(StepStatus.STEP_STATUS_SKIPPED))))))
                .hasMessageContaining("SUCCEEDED or FAILED");
        // The declared annotations enforce the branch-id shape.
        assertThat(ProtoValidator.create().validate(BranchEvidence.newBuilder()
                .setBranchId("no-hash")
                .setStatus(StepStatus.STEP_STATUS_SUCCEEDED)
                .build()).valid()).isFalse();
    }

    @Test
    void theEdgeFingerprintBindsEdgeAndFanOutContent() {
        WorkflowStep step = edgeWorkflow(WorkflowStep.newBuilder()
                .setEdge(edge()).setFanOut(fanOut())).getSteps(0);
        String fingerprint = WorkflowValidation.edgeFingerprint(step);
        assertThat(fingerprint).matches("[0-9a-f]{64}");
        // Deterministic, and sensitive to both the edge and the fan-out.
        assertThat(WorkflowValidation.edgeFingerprint(step)).isEqualTo(fingerprint);
        WorkflowStep altered = step.toBuilder()
                .setFanOut(step.getFanOut().toBuilder().setMaxItems(9).build())
                .build();
        assertThat(WorkflowValidation.edgeFingerprint(altered)).isNotEqualTo(fingerprint);
        WorkflowStep noFanOut = step.toBuilder().clearFanOut().build();
        assertThat(WorkflowValidation.edgeFingerprint(noFanOut)).isNotEqualTo(fingerprint);
    }
}
