package ai.protomolt.proto.inference.v1;

import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class EvaluationContractTest {
    private static final String UUID = "123e4567-e89b-12d3-a456-426614174000";
    private static final String SHA = "0123456789abcdef".repeat(4);

    private static ValidationResult validate(com.google.protobuf.Message message) {
        return ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
    }

    private static void valid(com.google.protobuf.Message message) {
        ValidationResult result = validate(message);
        assertThat(result.valid()).as("%s violations: %s", message.getDescriptorForType().getFullName(), result.violations()).isTrue();
    }

    private static void invalid(com.google.protobuf.Message message) {
        ValidationResult result = validate(message);
        assertThat(result.valid()).as("expected %s to fail", message.getDescriptorForType().getFullName()).isFalse();
        assertThat(result.violations()).isNotEmpty();
    }

    private static void invalidWithRule(com.google.protobuf.Message message, String ruleId) {
        ValidationResult result = validate(message);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations()).anySatisfy(v -> assertThat(v.ruleId()).isEqualTo(ruleId));
    }

    private static EvaluationBinding binding() {
        return EvaluationBinding.newBuilder()
                .setCandidate(EvaluationCandidate.newBuilder().setTaskId(UUID).setAttempt(1).setRevision(1))
                .setResultSha256(SHA).setDescriptorSha256(SHA).setResultType("example.Result")
                .setEvidenceSha256(SHA).setProjectionSha256(SHA).setRubricSha256(SHA).setPolicySha256(SHA).build();
    }

    private static EvaluationQuestion choiceQuestion() {
        return EvaluationQuestion.newBuilder().setId("choose").setInstruction("Choose").setChoice(
                ChoiceCriteria.newBuilder().addAllOptions(List.of("yes", "no"))).build();
    }

    private static EvaluationQuestion scoreQuestion() {
        return EvaluationQuestion.newBuilder().setId("score").setInstruction("Score").setScore(
                ScoreCriteria.newBuilder().addAllLevels(List.of("low", "high"))).build();
    }

    private static EvaluationQuestion probabilityQuestion() {
        return EvaluationQuestion.newBuilder().setId("probability").setInstruction("True?").setProbability(
                ProbabilityCriteria.getDefaultInstance()).build();
    }

    private static EvaluateRequest request() {
        return EvaluateRequest.newBuilder().setRequestId(UUID).setBinding(binding()).setEvidence(Any.pack(StringValue.of("projected evidence")))
                .setRubric(EvaluationRubric.newBuilder().addQuestions(choiceQuestion()).addQuestions(scoreQuestion()).addQuestions(probabilityQuestion()))
                .setEvaluatorProfile("local.default").build();
    }

    private static EvaluationProbability probability(String label, double p) {
        return EvaluationProbability.newBuilder().setLabel(label).setProbability(p).build();
    }

    @Test
    void validRequestAndAllThreeJudgmentKindsValidate() {
        valid(request());
        valid(EvaluationAnswer.newBuilder().setQuestionId("choose").setChoice(ChoiceJudgment.newBuilder()
                .setOption("yes").addDistribution(probability("yes", 1)).addDistribution(probability("no", 0))).build());
        valid(EvaluationAnswer.newBuilder().setQuestionId("score").setScore(ScoreJudgment.newBuilder()
                .setScore(0).addDistribution(probability("low", 1)).addDistribution(probability("high", 0))).build());
        valid(EvaluationAnswer.newBuilder().setQuestionId("probability").setProbability(
                ProbabilityJudgment.newBuilder().setProbability(0)).build());
        valid(ChoiceJudgment.newBuilder().setOption("yes").addDistribution(probability("yes", 1))
                .addDistribution(probability("no", 0)).setConfidence(0).build());
        valid(ScoreJudgment.newBuilder().setScore(1).addDistribution(probability("low", 0))
                .addDistribution(probability("high", 1)).setConfidence(0).build());
        valid(EvaluateResponse.newBuilder().setRequestId(UUID).setBinding(binding())
                .addAnswers(EvaluationAnswer.newBuilder().setQuestionId("choose").setChoice(ChoiceJudgment.newBuilder()
                        .setOption("yes").addDistribution(probability("yes", 1)).addDistribution(probability("no", 0))))
                .addAnswers(EvaluationAnswer.newBuilder().setQuestionId("score").setScore(ScoreJudgment.newBuilder()
                        .setScore(0).addDistribution(probability("low", 1)).addDistribution(probability("high", 0))))
                .addAnswers(EvaluationAnswer.newBuilder().setQuestionId("probability").setProbability(
                        ProbabilityJudgment.newBuilder().setProbability(0)))
                .setProvider("provider").setModel("model").build());
    }

    @Test
    void envelopeValidationDoesNotUnpackAny() {
        // Any presence is the schema-level contract. A handler must resolve the type URL,
        // reject absent or unknown type URLs, unpack the evidence, and validate that message.
        valid(request().toBuilder().setEvidence(Any.getDefaultInstance()).build());
    }

    @Test
    void requiredFieldsAndOneofsAreEnforced() {
        invalid(EvaluateRequest.getDefaultInstance());
        invalid(EvaluationBinding.newBuilder().setResultSha256(SHA).setDescriptorSha256(SHA).setResultType("example.Result")
                .setEvidenceSha256(SHA).setProjectionSha256(SHA).setRubricSha256(SHA).setPolicySha256(SHA).build());
        invalid(EvaluationQuestion.newBuilder().setId("q").setInstruction("instruction").build());
        invalid(EvaluationAnswer.newBuilder().setQuestionId("q").build());
        invalid(EvaluationProbability.newBuilder().setLabel("yes").build());
        invalid(ProbabilityJudgment.getDefaultInstance());
        invalid(ScoreJudgment.newBuilder().addDistribution(probability("low", 1)).addDistribution(probability("high", 0)).build());
        invalid(EvaluateResponse.newBuilder().setRequestId(UUID).addAnswers(EvaluationAnswer.newBuilder()
                .setQuestionId("q").setProbability(ProbabilityJudgment.newBuilder().setProbability(.5)))
                .setProvider("provider").setModel("model").build());
        invalid(EvaluateResponse.newBuilder().setRequestId(UUID).setBinding(binding())
                .addAnswers(EvaluationAnswer.newBuilder().setQuestionId("q").setProbability(ProbabilityJudgment.newBuilder().setProbability(.5)))
                .build());
        invalid(EvaluateResponse.newBuilder().setRequestId(UUID).setBinding(binding())
                .addAnswers(EvaluationAnswer.newBuilder().setQuestionId("q").setProbability(ProbabilityJudgment.newBuilder().setProbability(.5)))
                .setProvider("provider").build());
        invalid(EvaluateResponse.newBuilder().setRequestId(UUID).setBinding(binding())
                .addAnswers(EvaluationAnswer.newBuilder().setQuestionId("q").setProbability(ProbabilityJudgment.newBuilder().setProbability(.5)))
                .setModel("model").build());
        invalid(EvaluateResponse.newBuilder().setRequestId(UUID).setBinding(binding()).setProvider("p").setModel("m").build());
        invalid(EvaluationBinding.newBuilder(binding()).clearCandidate().setRunId("").build());
    }

    @Test
    void candidateBoundsAndDigestsAreValidated() {
        invalid(EvaluationCandidate.newBuilder().setTaskId(UUID).setAttempt(0).setRevision(1).build());
        invalid(EvaluationCandidate.newBuilder().setTaskId(UUID).setAttempt(1025).setRevision(1).build());
        invalid(EvaluationCandidate.newBuilder().setTaskId(UUID).setAttempt(1).setRevision(0).build());
        invalid(EvaluationBinding.newBuilder(binding()).setResultSha256("bad-digest").build());
        valid(EvaluationBinding.newBuilder(binding()).clearCandidate().setRunId("run-123").build());
    }

    @Test
    void numericRangesAndFinitenessAreValidated() {
        invalid(EvaluationProbability.newBuilder().setLabel("x").setProbability(Double.NaN).build());
        invalid(EvaluationProbability.newBuilder().setLabel("x").setProbability(Double.POSITIVE_INFINITY).build());
        invalid(EvaluationProbability.newBuilder().setLabel("x").setProbability(1.1).build());
        invalid(ProbabilityJudgment.newBuilder().setProbability(Double.NEGATIVE_INFINITY).build());
        invalid(ProbabilityJudgment.newBuilder().setProbability(-0.1).build());
        invalid(ChoiceJudgment.newBuilder().setOption("x").addDistribution(probability("x", 1))
                .addDistribution(probability("y", 0)).setConfidence(Double.NaN).build());
        invalid(ScoreJudgment.newBuilder().setScore(Double.POSITIVE_INFINITY).addDistribution(probability("a", 1))
                .addDistribution(probability("b", 0)).build());
        invalid(ScoreJudgment.newBuilder().setScore(32).addDistribution(probability("a", 1))
                .addDistribution(probability("b", 0)).build());
    }

    @Test
    void rubricAndDistributionLabelsAreUniqueAndBounded() {
        invalidWithRule(EvaluationRubric.newBuilder().addQuestions(choiceQuestion()).addQuestions(choiceQuestion()).build(), "unique-question-ids");
        invalid(ChoiceCriteria.newBuilder().addOptions("same").addOptions("same").build());
        invalid(ChoiceCriteria.newBuilder().addOptions("").addOptions("other").build());
        invalid(EvaluationProbability.newBuilder().setLabel("x".repeat(513)).setProbability(1).build());
        invalid(ScoreCriteria.newBuilder().addLevels("same").addLevels("same").build());
        invalid(ChoiceJudgment.newBuilder().setOption("x").addDistribution(probability("x", 1))
                .addDistribution(probability("x", 0)).build());
        invalid(ScoreJudgment.newBuilder().setScore(0).addDistribution(probability("a", .5))
                .addDistribution(probability("a", .5)).build());
        invalidWithRule(EvaluateResponse.newBuilder().setRequestId(UUID).setBinding(binding())
                .addAnswers(EvaluationAnswer.newBuilder().setQuestionId("duplicate").setProbability(ProbabilityJudgment.newBuilder().setProbability(.5)))
                .addAnswers(EvaluationAnswer.newBuilder().setQuestionId("duplicate").setProbability(ProbabilityJudgment.newBuilder().setProbability(.5)))
                .setProvider("p").setModel("m").build(), "unique-answer-ids");
    }

    @Test
    void judgmentMustFitItsDeclaredDistributionShape() {
        invalidWithRule(ChoiceJudgment.newBuilder().setOption("missing").addDistribution(probability("x", 1))
                .addDistribution(probability("y", 0)).build(), "chosen-option-present");
        invalidWithRule(ScoreJudgment.newBuilder().setScore(2).addDistribution(probability("low", 1))
                .addDistribution(probability("high", 0)).build(), "score-within-scale");
        // The schema does not assert a choice/score distribution sums to one or matches the request's labels;
        // those are explicitly host-side checks documented in evaluation.proto.
    }

    @Test
    void rubricAndOptionCollectionsEnforceTheirBounds() {
        invalid(EvaluationRubric.getDefaultInstance());
        EvaluationRubric.Builder tooManyQuestions = EvaluationRubric.newBuilder();
        for (int i = 0; i < 33; i++) {
            tooManyQuestions.addQuestions(EvaluationQuestion.newBuilder().setId("q-" + i)
                    .setInstruction("instruction").setProbability(ProbabilityCriteria.getDefaultInstance()));
        }
        invalid(tooManyQuestions.build());
        ChoiceCriteria.Builder tooManyOptions = ChoiceCriteria.newBuilder();
        for (int i = 0; i < 33; i++) {
            tooManyOptions.addOptions("option-" + i);
        }
        invalid(tooManyOptions.build());
    }

    @Test
    void everyEvaluationMessageDescriptorCanBuildAValidator() {
        for (Descriptors.Descriptor descriptor : EvaluationCandidate.getDescriptor().getFile().getMessageTypes()) {
            assertThatCode(() -> ProtoValidator.forMessageType(descriptor)
                    .validate(DynamicMessage.getDefaultInstance(descriptor)))
                    .as(descriptor.getFullName()).doesNotThrowAnyException();
        }
    }
}
