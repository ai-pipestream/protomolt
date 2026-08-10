package ai.pipestream.proto.grpc.recipe;

import ai.pipestream.proto.grpc.recipe.v1.BranchEvidence;
import ai.pipestream.proto.grpc.recipe.v1.BranchFailurePolicy;
import ai.pipestream.proto.grpc.recipe.v1.EdgeEvidence;
import ai.pipestream.proto.grpc.recipe.v1.FanOutSpec;
import ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe;
import ai.pipestream.proto.grpc.recipe.v1.RecipeStep;
import ai.pipestream.proto.grpc.recipe.v1.RunEvidence;
import ai.pipestream.proto.grpc.recipe.v1.StepEvidence;
import ai.pipestream.proto.grpc.recipe.v1.StepStatus;
import ai.pipestream.proto.grpc.recipe.v1.TypedEdge;
import ai.pipestream.proto.validate.ProtoValidator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The typed-edge contract: every new persisted field's bounds, patterns, and shape
 * rules are enforced both by the hand-checked {@link RecipeValidation} path and by the
 * declared validate.v1 annotations through {@link ProtoValidator}.
 */
class EdgeRecipeValidationTest {

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

    /** The base recipe with its first step replaced by an edge-carrying one. */
    private static GrpcRecipe edgeRecipe(RecipeStep.Builder step) {
        GrpcRecipe base = TestRecipes.recipe();
        RecipeStep built = step.setName("tokenize")
                .setDependency("nlp")
                .setMethod("example.v1.Tokenizer/Tokenize")
                .setCompletion(
                        ai.pipestream.proto.grpc.recipe.v1.StepCompletion
                                .STEP_COMPLETION_LIVE)
                .build();
        return base.toBuilder().setSteps(0, built).build();
    }

    private static StepEvidence.Builder edgeStepEvidence() {
        return TestRecipes.evidence().getSteps(0).toBuilder()
                .setEdge(EdgeEvidence.newBuilder()
                        .setEdgeFingerprint("a".repeat(64))
                        .setValidationPassed(true)
                        .setSourceCount(1)
                        .build());
    }

    private static RunEvidence evidenceWith(StepEvidence.Builder step) {
        return TestRecipes.evidence().toBuilder().setSteps(0, step.build()).build();
    }

    @Test
    void aWellFormedEdgeAndFanOutPass() {
        assertThatCode(() -> RecipeValidation.validate(
                edgeRecipe(RecipeStep.newBuilder()
                        .setEdge(edge()).setFanOut(fanOut()))))
                .doesNotThrowAnyException();
        // The declared annotations agree: the same content validates clean through
        // the validate.v1 framework.
        assertThat(ProtoValidator.create().validate(edge()).valid()).isTrue();
        assertThat(ProtoValidator.create().validate(fanOut()).valid()).isTrue();
    }

    @Test
    void anEdgeOwnsTheStepsMappingRules() {
        GrpcRecipe recipe = edgeRecipe(RecipeStep.newBuilder()
                .setEdge(edge()).addRules("text = input.body"));
        assertThatThrownBy(() -> RecipeValidation.validate(recipe))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("the edge owns request mapping");
    }

    @Test
    void edgeSourcesAreBoundedStepNameTokens() {
        assertThatThrownBy(() -> RecipeValidation.validate(
                edgeRecipe(RecipeStep.newBuilder()
                        .setEdge(TypedEdge.newBuilder()
                                .setProduceType("example.v1.Document")))))
                .hasMessageContaining("edge.sources must not be empty");
        assertThatThrownBy(() -> RecipeValidation.validate(
                edgeRecipe(RecipeStep.newBuilder()
                        .setEdge(edge().toBuilder().addSources("not a name")))))
                .hasMessageContaining("step.edge.sources");
        TypedEdge.Builder sixtyFive = TypedEdge.newBuilder()
                .setProduceType("example.v1.Document");
        for (int i = 0; i < 65; i++) {
            sixtyFive.addSources("s" + i);
        }
        assertThatThrownBy(() -> RecipeValidation.validate(
                edgeRecipe(RecipeStep.newBuilder().setEdge(sixtyFive))))
                .hasMessageContaining("exceeds the maximum");
        // The declared annotations enforce the same bounds.
        assertThat(ProtoValidator.create().validate(sixtyFive.build()).valid())
                .isFalse();
        assertThat(ProtoValidator.create().validate(TypedEdge.newBuilder()
                .setProduceType("example.v1.Document").build()).valid()).isFalse();
    }

    @Test
    void edgeTypesMustBeFullyQualifiedNames() {
        assertThatThrownBy(() -> RecipeValidation.validate(
                edgeRecipe(RecipeStep.newBuilder()
                        .setEdge(edge().toBuilder().setProduceType("not a type")))))
                .hasMessageContaining("produce_type");
        assertThatThrownBy(() -> RecipeValidation.validate(
                edgeRecipe(RecipeStep.newBuilder()
                        .setEdge(edge().toBuilder().setProjectTo("bad type!")))))
                .hasMessageContaining("project_to");
        assertThat(ProtoValidator.create().validate(edge().toBuilder()
                .setProjectTo("bad type!").build()).valid()).isFalse();
    }

    @Test
    void fanOutRequiresAnEdgeAndBoundedFields() {
        assertThatThrownBy(() -> RecipeValidation.validate(
                edgeRecipe(RecipeStep.newBuilder().setFanOut(fanOut()))))
                .hasMessageContaining("fan_out requires step.edge");

        RecipeStep.Builder withEdge = RecipeStep.newBuilder().setEdge(edge());
        assertThatThrownBy(() -> RecipeValidation.validate(edgeRecipe(withEdge
                .setFanOut(fanOut().toBuilder().setMaxItems(0)))))
                .hasMessageContaining("max_items");
        assertThatThrownBy(() -> RecipeValidation.validate(edgeRecipe(withEdge
                .setFanOut(fanOut().toBuilder().setMaxItems(1025)))))
                .hasMessageContaining("max_items");
        assertThatThrownBy(() -> RecipeValidation.validate(edgeRecipe(withEdge
                .setFanOut(fanOut().toBuilder().setMaxConcurrency(65)))))
                .hasMessageContaining("max_concurrency");
        assertThatThrownBy(() -> RecipeValidation.validate(edgeRecipe(withEdge
                .setFanOut(fanOut().toBuilder().setFailurePolicy(
                        BranchFailurePolicy.BRANCH_FAILURE_POLICY_UNSPECIFIED)))))
                .hasMessageContaining("failure_policy");
        assertThatThrownBy(() -> RecipeValidation.validate(edgeRecipe(withEdge
                .setFanOut(fanOut().toBuilder().setCollectInto("not a field")))))
                .hasMessageContaining("collect_into");
        assertThatThrownBy(() -> RecipeValidation.validate(edgeRecipe(withEdge
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
        assertThatCode(() -> RecipeValidation.validate(
                evidenceWith(edgeStepEvidence()))).doesNotThrowAnyException();

        assertThatThrownBy(() -> RecipeValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(edgeStepEvidence().getEdge().toBuilder()
                        .setEdgeFingerprint("not-a-fingerprint")))))
                .hasMessageContaining("edge_fingerprint");
        assertThatThrownBy(() -> RecipeValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(edgeStepEvidence().getEdge().toBuilder()
                        .setSourceCount(0)))))
                .hasMessageContaining("source_count");
        assertThatThrownBy(() -> RecipeValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(edgeStepEvidence().getEdge().toBuilder()
                        .setItemCount(1025)))))
                .hasMessageContaining("item_count");
        // A succeeded step without branches records a clean verdict.
        assertThatThrownBy(() -> RecipeValidation.validate(evidenceWith(
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
                        .setResponseArtifact(TestRecipes.artifact("{}", true)))
                .addBranches(BranchEvidence.newBuilder()
                        .setBranchId("tokenize#1")
                        .setStatus(StepStatus.STEP_STATUS_FAILED)
                        .setSummary("rejected"));
        assertThatCode(() -> RecipeValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(twoBranches)))).doesNotThrowAnyException();

        // Branches must cover every item.
        assertThatThrownBy(() -> RecipeValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(twoBranches.clone().setItemCount(3)))))
                .hasMessageContaining("must cover every item");
        // No branches without fan-out items.
        assertThatThrownBy(() -> RecipeValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(twoBranches.clone().setItemCount(0)))))
                .hasMessageContaining("must be empty when item_count is 0");
        // Branch identities name their own step.
        assertThatThrownBy(() -> RecipeValidation.validate(evidenceWith(
                edgeStepEvidence().setEdge(twoBranches.clone().setBranches(0,
                        twoBranches.getBranches(0).toBuilder()
                                .setBranchId("other#0"))))))
                .hasMessageContaining("its own step");
        // Branch status is SUCCEEDED or FAILED only.
        assertThatThrownBy(() -> RecipeValidation.validate(evidenceWith(
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
        RecipeStep step = edgeRecipe(RecipeStep.newBuilder()
                .setEdge(edge()).setFanOut(fanOut())).getSteps(0);
        String fingerprint = RecipeValidation.edgeFingerprint(step);
        assertThat(fingerprint).matches("[0-9a-f]{64}");
        // Deterministic, and sensitive to both the edge and the fan-out.
        assertThat(RecipeValidation.edgeFingerprint(step)).isEqualTo(fingerprint);
        RecipeStep altered = step.toBuilder()
                .setFanOut(step.getFanOut().toBuilder().setMaxItems(9).build())
                .build();
        assertThat(RecipeValidation.edgeFingerprint(altered)).isNotEqualTo(fingerprint);
        RecipeStep noFanOut = step.toBuilder().clearFanOut().build();
        assertThat(RecipeValidation.edgeFingerprint(noFanOut)).isNotEqualTo(fingerprint);
    }
}
