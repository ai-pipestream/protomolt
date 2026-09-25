package ai.protomolt.proto.inference.service;

import ai.protomolt.proto.inference.v1.EvaluateRequest;
import ai.protomolt.proto.inference.v1.EvaluateResponse;
import ai.protomolt.proto.inference.v1.EvaluationAnswer;
import ai.protomolt.proto.inference.v1.EvaluationProbability;
import ai.protomolt.proto.inference.v1.EvaluationQuestion;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Checks a provider's successful evaluation against the exact rubric it answered. */
public final class EvaluationResponses {

    private static final int MAX_ENVELOPE_BYTES = 1024 * 1024;
    private static final double TOLERANCE = 1e-6;

    private EvaluationResponses() {
    }

    /**
     * Validates both contract envelopes and the response's agreement with the request.
     * Stored-artifact resolution and candidate validity belong to the caller, before
     * any provider invocation. This method does not make an acceptance decision.
     */
    public static void validate(EvaluateRequest request, EvaluateResponse response) {
        if (request == null) {
            throw invalidRequest("request is required");
        }
        checkEnvelope(request, "request", false);
        if (response == null) {
            throw invalidResponse("response is required");
        }
        checkEnvelope(response, "response", true);

        if (!request.getRequestId().equals(response.getRequestId())) {
            throw invalidResponse("request_id differs from the request");
        }
        if (!request.getBinding().equals(response.getBinding())) {
            throw invalidResponse("binding differs from the request");
        }

        Map<String, EvaluationQuestion> questions = new HashMap<>();
        for (EvaluationQuestion question : request.getRubric().getQuestionsList()) {
            if (questions.putIfAbsent(question.getId(), question) != null) {
                throw invalidRequest("duplicate rubric question " + question.getId());
            }
        }
        if (response.getAnswersCount() != questions.size()) {
            throw invalidResponse("answer count differs from the rubric");
        }
        Set<String> answered = new HashSet<>();
        for (EvaluationAnswer answer : response.getAnswersList()) {
            String id = answer.getQuestionId();
            if (!answered.add(id)) {
                throw invalidResponse("duplicate answer for question " + id);
            }
            EvaluationQuestion question = questions.get(id);
            if (question == null) {
                throw invalidResponse("unknown answer question " + id);
            }
            switch (question.getKindCase()) {
                case CHOICE -> checkChoice(id, question, answer);
                case SCORE -> checkScore(id, question, answer);
                case PROBABILITY -> checkProbability(id, answer);
                default -> throw invalidRequest("question " + id + " has no kind");
            }
        }
    }

    private static void checkChoice(String id, EvaluationQuestion question,
                                    EvaluationAnswer answer) {
        if (!answer.hasChoice()) {
            throw invalidResponse("question " + id + " requires a choice answer");
        }
        List<String> options = question.getChoice().getOptionsList();
        if (!options.contains(answer.getChoice().getOption())) {
            throw invalidResponse("question " + id + " selects an unknown option");
        }
        List<EvaluationProbability> distribution = answer.getChoice().getDistributionList();
        if (distribution.size() != options.size()) {
            throw invalidResponse("question " + id + " has incomplete choice distribution");
        }
        Set<String> labels = new HashSet<>();
        for (EvaluationProbability probability : distribution) {
            if (!options.contains(probability.getLabel()) || !labels.add(probability.getLabel())) {
                throw invalidResponse("question " + id + " has unexpected choice label");
            }
        }
        checkSum(id, distribution);
    }

    private static void checkScore(String id, EvaluationQuestion question,
                                   EvaluationAnswer answer) {
        if (!answer.hasScore()) {
            throw invalidResponse("question " + id + " requires a score answer");
        }
        List<String> levels = question.getScore().getLevelsList();
        List<EvaluationProbability> distribution = answer.getScore().getDistributionList();
        if (distribution.size() != levels.size()) {
            throw invalidResponse("question " + id + " has incomplete score distribution");
        }
        double weighted = 0;
        for (int i = 0; i < levels.size(); i++) {
            EvaluationProbability probability = distribution.get(i);
            if (!levels.get(i).equals(probability.getLabel())) {
                throw invalidResponse("question " + id + " changes score level order");
            }
            weighted += i * probability.getProbability();
        }
        checkSum(id, distribution);
        if (!Double.isFinite(weighted) || !answer.getScore().hasScore()
                || Math.abs(weighted - answer.getScore().getScore()) > TOLERANCE) {
            throw invalidResponse("question " + id + " score differs from weighted levels");
        }
    }

    private static void checkProbability(String id, EvaluationAnswer answer) {
        if (!answer.hasProbability() || !answer.getProbability().hasProbability()) {
            throw invalidResponse("question " + id + " requires a probability answer");
        }
        double probability = answer.getProbability().getProbability();
        if (!Double.isFinite(probability) || probability < 0 || probability > 1) {
            throw invalidResponse("question " + id + " has invalid probability");
        }
    }

    private static void checkSum(String id, List<EvaluationProbability> distribution) {
        double sum = 0;
        for (EvaluationProbability entry : distribution) {
            double probability = entry.getProbability();
            if (!entry.hasProbability() || !Double.isFinite(probability)
                    || probability < 0 || probability > 1) {
                throw invalidResponse("question " + id + " has invalid distribution probability");
            }
            sum += probability;
        }
        if (!Double.isFinite(sum) || Math.abs(sum - 1) > TOLERANCE) {
            throw invalidResponse("question " + id + " distribution does not sum to one");
        }
    }

    private static void checkEnvelope(Message message, String name, boolean response) {
        if (message.getSerializedSize() > MAX_ENVELOPE_BYTES) {
            throw failure(response, name + " exceeds 1 MiB");
        }
        checkUnknownFields(message, name, response);
        ValidationResult validation = ai.protomolt.proto.validate.ProtoValidator
                .forMessageType(message.getDescriptorForType()).validate(message);
        if (!validation.valid()) {
            ValidationResult.Violation first = validation.violations().getFirst();
            throw failure(response, name + " violates " + first.path() + " ("
                    + first.ruleId() + ")");
        }
    }

    private static void checkUnknownFields(Message message, String path, boolean response) {
        if (!message.getUnknownFields().asMap().isEmpty()) {
            throw failure(response, path + " contains unknown fields");
        }
        for (Map.Entry<FieldDescriptor, Object> field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            String childPath = path + "." + field.getKey().getName();
            if (field.getKey().isRepeated()) {
                int index = 0;
                for (Object child : (List<?>) field.getValue()) {
                    checkUnknownFields((Message) child, childPath + "[" + index++ + "]",
                            response);
                }
            } else {
                checkUnknownFields((Message) field.getValue(), childPath, response);
            }
        }
    }

    private static IllegalArgumentException failure(boolean response, String message) {
        return response ? invalidResponse(message) : invalidRequest(message);
    }

    private static IllegalArgumentException invalidRequest(String message) {
        return new IllegalArgumentException("contract_violation: " + message);
    }

    private static IllegalArgumentException invalidResponse(String message) {
        return new IllegalArgumentException("evaluator_response_invalid: " + message);
    }
}
