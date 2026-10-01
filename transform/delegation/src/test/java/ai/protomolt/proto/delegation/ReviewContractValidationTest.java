package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.CandidateReviewIdentity;
import ai.protomolt.proto.delegation.v1.ReviewDeferred;
import ai.protomolt.proto.delegation.v1.ReviewFailed;
import ai.protomolt.proto.delegation.v1.ReviewFailureCode;
import ai.protomolt.proto.delegation.v1.ReviewStarted;
import ai.protomolt.proto.delegation.v1.RetryCandidateReviewRequest;
import ai.protomolt.proto.delegation.v1.RetryCandidateReviewResponse;
import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReviewContractValidationTest {
    private static final String TASK = "49e553aa-df4c-4f19-8c27-51655ecc0b53";
    private static final String INVOCATION = "11111111-1111-4111-8111-111111111111";
    private static final String PRIOR_INVOCATION = "22222222-2222-4222-8222-222222222222";
    private static final String RETRY = "33333333-3333-4333-8333-333333333333";

    private static CandidateReviewIdentity identity() {
        return CandidateReviewIdentity.newBuilder()
                .setTaskId(TASK)
                .setWorkerId("worker-review-1")
                .setAttempt(1)
                .setRevision(1)
                .setInvocationId(INVOCATION)
                .setOfferEntrySha256("a".repeat(64))
                .setCandidateEntrySha256("b".repeat(64))
                .build();
    }

    private static ReviewStarted.Builder start() {
        return ReviewStarted.newBuilder()
                .setIdentity(identity())
                .setStartedAt(Timestamp.newBuilder().setSeconds(1_700_000_000L))
                .setDeadline(Timestamp.newBuilder().setSeconds(1_700_000_060L));
    }

    private static RetryCandidateReviewRequest.Builder retryRequest() {
        return RetryCandidateReviewRequest.newBuilder()
                .setTaskId(TASK)
                .setAttempt(1)
                .setRevision(1)
                .setExpectedInvocationId(PRIOR_INVOCATION)
                .setRetryId(RETRY);
    }

    private static ValidationResult validate(Message message) {
        return ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
    }

    private static ValidationResult validateAsDynamic(Message message) throws Exception {
        DynamicMessage dynamic = DynamicMessage.parseFrom(
                message.getDescriptorForType(), message.toByteArray());
        return ProtoValidator.forMessageType(dynamic.getDescriptorForType()).validate(dynamic);
    }

    private static void validBoth(Message message) throws Exception {
        ValidationResult generated = validate(message);
        ValidationResult dynamic = validateAsDynamic(message);
        assertThat(generated.valid()).as("generated violations: %s", generated.violations()).isTrue();
        assertThat(dynamic.valid()).as("dynamic violations: %s", dynamic.violations()).isTrue();
    }

    private static void invalidBoth(Message message) throws Exception {
        ValidationResult generated = validate(message);
        ValidationResult dynamic = validateAsDynamic(message);
        assertThat(generated.valid()).as("generated violations: %s", generated.violations()).isFalse();
        assertThat(dynamic.valid()).as("dynamic violations: %s", dynamic.violations()).isFalse();
        assertThat(dynamic.violations()).isNotEmpty();
    }

    private static void hasRule(ValidationResult result, String id) {
        assertThat(result.violations()).anySatisfy(v -> assertThat(v.ruleId()).isEqualTo(id));
    }

    @Test
    void validInitialAndRetryReviewStartsValidateInGeneratedAndDynamicModes() throws Exception {
        validBoth(start().build());
        validBoth(start()
                .setPreviousInvocationId(PRIOR_INVOCATION)
                .setRetryId(RETRY)
                .build());

        invalidBoth(start().clearIdentity().build());
        invalidBoth(start()
                .setPreviousInvocationId(PRIOR_INVOCATION)
                .build());
        invalidBoth(start().setRetryId(RETRY).build());
        invalidBoth(start().setPreviousInvocationId(INVOCATION).setRetryId(RETRY).build());

        ValidationResult pair = validate(start().setRetryId(RETRY).build());
        hasRule(pair, "review-retry-pair");
        ValidationResult invocation = validate(start()
                .setPreviousInvocationId(INVOCATION).setRetryId(RETRY).build());
        hasRule(invocation, "review-new-invocation");
    }

    @Test
    void reviewIdentityEnforcesUuidSlugDigestAndAttemptRevisionBounds() throws Exception {
        invalidBoth(start().setIdentity(identity().toBuilder().setTaskId("same-Name").build()).build());
        invalidBoth(start().setIdentity(identity().toBuilder().setWorkerId("bad worker").build()).build());
        invalidBoth(start().setIdentity(identity().toBuilder()
                .setInvocationId("AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA").build()).build());
        invalidBoth(start().setIdentity(identity().toBuilder().setOfferEntrySha256("g".repeat(64)).build()).build());
        invalidBoth(start().setIdentity(identity().toBuilder().setCandidateEntrySha256("A".repeat(64)).build()).build());

        for (int value : new int[] {0, 1025}) {
            invalidBoth(start().setIdentity(identity().toBuilder().setAttempt(value).build()).build());
            invalidBoth(start().setIdentity(identity().toBuilder().setRevision(value).build()).build());
        }
    }

    @Test
    void reviewStartRequiresStrictlyOrderedTimesAndCanonicalRetryIds() throws Exception {
        validBoth(start().setDeadline(Timestamp.newBuilder().setSeconds(1_700_003_600L)).build());
        invalidBoth(start().setDeadline(Timestamp.newBuilder()
                .setSeconds(1_700_003_600L).setNanos(1)).build());
        invalidBoth(start().setDeadline(Timestamp.newBuilder().setSeconds(1_700_000_000L)).build());
        invalidBoth(start().setDeadline(Timestamp.newBuilder().setSeconds(1_699_999_999L)).build());
        hasRule(validate(start().setDeadline(Timestamp.newBuilder().setSeconds(1_700_000_000L)).build()),
                "review-deadline-order");
        invalidBoth(start().setPreviousInvocationId("NOT-A-UUID").setRetryId(RETRY).build());
        invalidBoth(start().setPreviousInvocationId(PRIOR_INVOCATION).setRetryId("not-a-uuid").build());

        invalidBoth(ReviewDeferred.newBuilder().build());
        validBoth(ReviewDeferred.newBuilder().setIdentity(identity()).build());
        invalidBoth(ReviewFailed.newBuilder().setIdentity(identity())
                .setCode(ReviewFailureCode.REVIEW_FAILURE_CODE_UNSPECIFIED).build());
        invalidBoth(ReviewFailed.newBuilder().setIdentity(identity()).setCodeValue(73).build());
        validBoth(ReviewFailed.newBuilder().setIdentity(identity())
                .setCode(ReviewFailureCode.REVIEW_FAILURE_CODE_INFRASTRUCTURE).build());
    }

    @Test
    void retryRequestAndResponseBindTaskAttemptRevisionAndNewInvocation() throws Exception {
        validBoth(retryRequest().build());
        for (int value : new int[] {0, 1025}) {
            invalidBoth(retryRequest().setAttempt(value).build());
            invalidBoth(retryRequest().setRevision(value).build());
        }
        invalidBoth(retryRequest().setExpectedInvocationId("bad").build());
        invalidBoth(retryRequest().setRetryId("bad").build());
        invalidBoth(retryRequest().setTaskId("Bad-Task").build());

        CandidateReviewIdentity retried = identity().toBuilder().setInvocationId("44444444-4444-4444-8444-444444444444").build();
        RetryCandidateReviewResponse response = RetryCandidateReviewResponse.newBuilder()
                .setRequest(retryRequest())
                .setIdentity(retried)
                .build();
        validBoth(response);
        invalidBoth(response.toBuilder().setIdentity(retried.toBuilder().setTaskId("55555555-5555-4555-8555-555555555555")).build());
        invalidBoth(response.toBuilder().setIdentity(retried.toBuilder().setAttempt(2)).build());
        invalidBoth(response.toBuilder().setIdentity(retried.toBuilder().setRevision(2)).build());
        invalidBoth(response.toBuilder().setIdentity(retried.toBuilder().setInvocationId(PRIOR_INVOCATION)).build());
        invalidBoth(RetryCandidateReviewResponse.newBuilder().setRequest(retryRequest()).build());

        hasRule(validate(response.toBuilder()
                .setIdentity(retried.toBuilder().setAttempt(2)).build()), "review-retry-response-binding");
    }

    @Test
    void jsonSchemaPublishesStructuralBoundsAndMarksCelAsRuntimeValidation() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ProtoJsonSchemaGenerator generator = ProtoJsonSchemaGenerator.create();
        JsonNode identitySchema = mapper.valueToTree(generator.generateRooted(CandidateReviewIdentity.getDescriptor()));
        assertThat(identitySchema.at("/properties/taskId/format").asText()).isEqualTo("uuid");
        assertThat(identitySchema.at("/properties/attempt/minimum").asInt()).isEqualTo(1);
        assertThat(identitySchema.at("/properties/attempt/maximum").asInt()).isEqualTo(1024);
        assertThat(identitySchema.at("/properties/offerEntrySha256/type").asText()).isEqualTo("string");

        JsonNode startSchema = mapper.valueToTree(generator.generateRooted(ReviewStarted.getDescriptor()));
        assertThat(startSchema.path("x-protomolt-cel").toString())
                .contains("review-deadline-order", "review-retry-pair", "review-new-invocation");
        // This generator exposes structural shape and CEL identifiers; CEL and
        // validate.v1 formats remain runtime validation, exercised above through
        // ProtoValidator in both generated and DynamicMessage modes.
        JsonNode retrySchema = mapper.valueToTree(generator.generateRooted(RetryCandidateReviewResponse.getDescriptor()));
        assertThat(retrySchema.path("x-protomolt-cel").toString()).contains("review-retry-response-binding");
    }
}
