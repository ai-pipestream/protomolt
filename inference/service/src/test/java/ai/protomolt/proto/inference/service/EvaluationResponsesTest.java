package ai.protomolt.proto.inference.service;

import ai.protomolt.proto.inference.v1.ChoiceCriteria;
import ai.protomolt.proto.inference.v1.ChoiceJudgment;
import ai.protomolt.proto.inference.v1.EvaluateRequest;
import ai.protomolt.proto.inference.v1.EvaluateResponse;
import ai.protomolt.proto.inference.v1.EvaluationAnswer;
import ai.protomolt.proto.inference.v1.EvaluationBinding;
import ai.protomolt.proto.inference.v1.EvaluationCandidate;
import ai.protomolt.proto.inference.v1.EvaluationProbability;
import ai.protomolt.proto.inference.v1.EvaluationQuestion;
import ai.protomolt.proto.inference.v1.EvaluationRubric;
import ai.protomolt.proto.inference.v1.ProbabilityCriteria;
import ai.protomolt.proto.inference.v1.ProbabilityJudgment;
import ai.protomolt.proto.inference.v1.ScoreCriteria;
import ai.protomolt.proto.inference.v1.ScoreJudgment;
import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EvaluationResponsesTest {
    private static final String REQUEST_ID = "123e4567-e89b-12d3-a456-426614174000";
    private static final String SHA = "0123456789abcdef".repeat(4);

    private static EvaluationBinding binding() {
        return EvaluationBinding.newBuilder()
                .setCandidate(EvaluationCandidate.newBuilder()
                        .setTaskId(REQUEST_ID).setAttempt(1).setRevision(1))
                .setResultSha256(SHA).setDescriptorSha256(SHA).setResultType("example.Result")
                .setEvidenceSha256(SHA).setProjectionSha256(SHA)
                .setRubricSha256(SHA).setPolicySha256(SHA).build();
    }

    private static EvaluationQuestion choiceQuestion() {
        return EvaluationQuestion.newBuilder().setId("choice").setInstruction("Choose one")
                .setChoice(ChoiceCriteria.newBuilder().addAllOptions(List.of("yes", "no"))).build();
    }

    private static EvaluationQuestion scoreQuestion() {
        return EvaluationQuestion.newBuilder().setId("score").setInstruction("Rate it")
                .setScore(ScoreCriteria.newBuilder().addAllLevels(List.of("low", "middle", "high"))).build();
    }

    private static EvaluationQuestion probabilityQuestion() {
        return EvaluationQuestion.newBuilder().setId("probability").setInstruction("Is it true?")
                .setProbability(ProbabilityCriteria.getDefaultInstance()).build();
    }

    private static EvaluateRequest request() {
        return requestWith(choiceQuestion(), scoreQuestion(), probabilityQuestion());
    }

    private static EvaluateRequest requestWith(EvaluationQuestion... questions) {
        EvaluationRubric.Builder rubric = EvaluationRubric.newBuilder();
        for (EvaluationQuestion question : questions) {
            rubric.addQuestions(question);
        }
        return EvaluateRequest.newBuilder().setRequestId(REQUEST_ID).setBinding(binding())
                .setEvidence(Any.pack(StringValue.of("projected evidence"))).setRubric(rubric)
                .setEvaluatorProfile("local.default").build();
    }

    private static EvaluationProbability p(String label, double value) {
        return EvaluationProbability.newBuilder().setLabel(label).setProbability(value).build();
    }

    private static EvaluationAnswer choiceAnswer() {
        return EvaluationAnswer.newBuilder().setQuestionId("choice")
                .setChoice(ChoiceJudgment.newBuilder().setOption("yes")
                        .addDistribution(p("yes", .7)).addDistribution(p("no", .3))).build();
    }

    private static EvaluationAnswer scoreAnswer() {
        return EvaluationAnswer.newBuilder().setQuestionId("score")
                .setScore(ScoreJudgment.newBuilder().setScore(1.25)
                        .addDistribution(p("low", .25)).addDistribution(p("middle", .25))
                        .addDistribution(p("high", .5))).build();
    }

    private static EvaluationAnswer probabilityAnswer() {
        return EvaluationAnswer.newBuilder().setQuestionId("probability")
                .setProbability(ProbabilityJudgment.newBuilder().setProbability(.625)).build();
    }

    private static EvaluateResponse response() {
        return responseWith(choiceAnswer(), scoreAnswer(), probabilityAnswer());
    }

    private static EvaluateResponse responseWith(EvaluationAnswer... answers) {
        EvaluateResponse.Builder response = EvaluateResponse.newBuilder()
                .setRequestId(REQUEST_ID).setBinding(binding()).setProvider("provider").setModel("model-v1");
        for (EvaluationAnswer answer : answers) {
            response.addAnswers(answer);
        }
        return response.build();
    }

    private static void invalidRequest(EvaluateRequest request) {
        assertThatThrownBy(() -> EvaluationResponses.validate(request, response()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("contract_violation:");
    }

    private static void invalidResponse(EvaluateRequest request, EvaluateResponse response) {
        assertThatThrownBy(() -> EvaluationResponses.validate(request, response))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("evaluator_response_invalid:");
    }

    @Test
    void acceptsFullyValidCorrelatedAnswersOfAllThreeKinds() {
        assertThatCode(() -> EvaluationResponses.validate(request(), response()))
                .doesNotThrowAnyException();
    }

    @Test
    void validatesRequestAndResponseEnvelopesBeforeMatching() {
        invalidRequest(request().toBuilder().setRequestId("bad-id").build());
        invalidRequest(requestWith(choiceQuestion(), choiceQuestion()));

        invalidResponse(request(), response().toBuilder().setRequestId("123e4567-e89b-12d3-a456-426614174001").build());
        invalidResponse(request(), response().toBuilder().setBinding(binding().toBuilder()
                .setRunId("different-run").build()).build());
        invalidResponse(request(), response().toBuilder().setProvider("").build());
    }

    @Test
    void requiresExactlyOneAnswerForEveryKnownQuestion() {
        invalidResponse(request(), responseWith(choiceAnswer(), scoreAnswer()));
        invalidResponse(request(), responseWith(choiceAnswer(), scoreAnswer(), probabilityAnswer(),
                EvaluationAnswer.newBuilder().setQuestionId("extra")
                        .setProbability(ProbabilityJudgment.newBuilder().setProbability(.5)).build()));
        invalidResponse(request(), responseWith(choiceAnswer(), scoreAnswer(),
                EvaluationAnswer.newBuilder().setQuestionId("unknown")
                        .setProbability(ProbabilityJudgment.newBuilder().setProbability(.5)).build()));
        invalidResponse(request(), responseWith(choiceAnswer(), scoreAnswer(), probabilityAnswer(), probabilityAnswer()));
    }

    @Test
    void answerKindMustMatchTheQuestionKind() {
        invalidResponse(request(), responseWith(
                EvaluationAnswer.newBuilder().setQuestionId("choice")
                        .setProbability(ProbabilityJudgment.newBuilder().setProbability(.5)).build(),
                scoreAnswer(), probabilityAnswer()));
    }

    @Test
    void choiceRequiresMembershipAndACompleteUniqueNormalizedDistribution() {
        invalidResponse(request(), responseWith(
                EvaluationAnswer.newBuilder().setQuestionId("choice")
                        .setChoice(ChoiceJudgment.newBuilder().setOption("maybe")
                                .addDistribution(p("yes", .7)).addDistribution(p("no", .3))).build(),
                scoreAnswer(), probabilityAnswer()));
        invalidResponse(request(), responseWith(
                EvaluationAnswer.newBuilder().setQuestionId("choice")
                        .setChoice(ChoiceJudgment.newBuilder().setOption("yes")
                                .addDistribution(p("yes", .7)).addDistribution(p("extra", .3))).build(),
                scoreAnswer(), probabilityAnswer()));
        invalidResponse(request(), responseWith(
                EvaluationAnswer.newBuilder().setQuestionId("choice")
                        .setChoice(ChoiceJudgment.newBuilder().setOption("yes")
                                .addDistribution(p("yes", .7)).addDistribution(p("yes", .3))).build(),
                scoreAnswer(), probabilityAnswer()));
        invalidResponse(request(), responseWith(
                EvaluationAnswer.newBuilder().setQuestionId("choice")
                        .setChoice(ChoiceJudgment.newBuilder().setOption("yes")
                                .addDistribution(p("yes", .6)).addDistribution(p("no", .3))).build(),
                scoreAnswer(), probabilityAnswer()));
    }

    @Test
    void scoreRequiresOrderedLevelsAndTheMatchingWeightedValue() {
        invalidResponse(request(), responseWith(choiceAnswer(),
                EvaluationAnswer.newBuilder().setQuestionId("score")
                        .setScore(ScoreJudgment.newBuilder().setScore(1.25)
                                .addDistribution(p("middle", .25)).addDistribution(p("low", .25))
                                .addDistribution(p("high", .5))).build(), probabilityAnswer()));
        invalidResponse(request(), responseWith(choiceAnswer(),
                EvaluationAnswer.newBuilder().setQuestionId("score")
                        .setScore(ScoreJudgment.newBuilder().setScore(2)
                                .addDistribution(p("low", .25)).addDistribution(p("middle", .25))
                                .addDistribution(p("high", .5))).build(), probabilityAnswer()));
    }

    @Test
    void allowsToleranceAtOnePartPerMillionAndRejectsOutsideIt() {
        EvaluateResponse atTolerance = responseWith(
                EvaluationAnswer.newBuilder().setQuestionId("choice")
                        .setChoice(ChoiceJudgment.newBuilder().setOption("yes")
                                .addDistribution(p("yes", .5)).addDistribution(p("no", .5000005))).build(),
                scoreAnswer(), probabilityAnswer());
        assertThatCode(() -> EvaluationResponses.validate(request(), atTolerance)).doesNotThrowAnyException();

        invalidResponse(request(), responseWith(
                EvaluationAnswer.newBuilder().setQuestionId("choice")
                        .setChoice(ChoiceJudgment.newBuilder().setOption("yes")
                                .addDistribution(p("yes", .5)).addDistribution(p("no", .5000011))).build(),
                scoreAnswer(), probabilityAnswer()));
    }

    @Test
    void rejectsNonFiniteAndOutOfRangeProbabilities() {
        invalidResponse(request(), responseWith(choiceAnswer(), scoreAnswer(),
                EvaluationAnswer.newBuilder().setQuestionId("probability")
                        .setProbability(ProbabilityJudgment.newBuilder().setProbability(Double.NaN)).build()));
        invalidResponse(request(), responseWith(choiceAnswer(), scoreAnswer(),
                EvaluationAnswer.newBuilder().setQuestionId("probability")
                        .setProbability(ProbabilityJudgment.newBuilder().setProbability(1.01)).build()));
        invalidResponse(request(), responseWith(
                EvaluationAnswer.newBuilder().setQuestionId("choice")
                        .setChoice(ChoiceJudgment.newBuilder().setOption("yes")
                                .addDistribution(p("yes", Double.POSITIVE_INFINITY)).addDistribution(p("no", 0))).build(),
                scoreAnswer(), probabilityAnswer()));
    }

    @Test
    void rejectsUnknownFieldsRecursivelyButDoesNotUnpackAnyPayload() {
        UnknownFieldSet unknown = UnknownFieldSet.newBuilder().addField(99,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        EvaluateResponse withNestedUnknown = response().toBuilder()
                .setAnswers(0, choiceAnswer().toBuilder().setChoice(
                        choiceAnswer().getChoice().toBuilder().setUnknownFields(unknown)))
                .build();
        invalidResponse(request(), withNestedUnknown);

        EvaluateRequest arbitraryEvidence = request().toBuilder()
                .setEvidence(Any.pack(StringValue.of("opaque inner value"))).build();
        assertThatCode(() -> EvaluationResponses.validate(arbitraryEvidence, response()))
                .doesNotThrowAnyException();
    }
}
