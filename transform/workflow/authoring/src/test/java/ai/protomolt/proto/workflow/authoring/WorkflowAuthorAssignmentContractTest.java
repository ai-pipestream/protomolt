package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorAssignmentsRequest;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorAssignmentsResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthorAssignment;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Generated and dynamic contract checks; persisted offer ownership and digest custody are handler checks. */
class WorkflowAuthorAssignmentContractTest {
    private static final String TASK_A = "00000000-0000-4000-8000-0000000000a1";
    private static final String TASK_B = "00000000-0000-4000-8000-0000000000b2";
    private static final String HASH_A = "a".repeat(64);
    private static final String HASH_B = "b".repeat(64);

    @Test
    void requestRequiresNonnegativeCursorAndBoundedAssignmentCount() throws Exception {
        var validator = ProtoValidator.forMessageType(ReadWorkflowAuthorAssignmentsRequest.getDescriptor());
        ReadWorkflowAuthorAssignmentsRequest minimum = request(0, 1);
        ReadWorkflowAuthorAssignmentsRequest maximum = request(Long.MAX_VALUE, 64);
        assertValidBoth(validator, minimum);
        assertValidBoth(validator, maximum);
        assertInvalidBoth(validator, ReadWorkflowAuthorAssignmentsRequest.getDefaultInstance());
        assertInvalidBoth(validator, request(-1, 1));
        assertInvalidBoth(validator, request(0, 0));
        assertInvalidBoth(validator, request(0, 65));
    }

    @Test
    void assignmentRequiresPositiveCursorUuidBoundedAttemptAndDigestShape() throws Exception {
        var validator = ProtoValidator.forMessageType(WorkflowAuthorAssignment.getDescriptor());
        WorkflowAuthorAssignment valid = assignment(1, TASK_A, 1, HASH_A);
        assertValidBoth(validator, valid);
        assertValidBoth(validator, assignment(Long.MAX_VALUE, TASK_B, 1024, HASH_B));
        assertInvalidBoth(validator, WorkflowAuthorAssignment.getDefaultInstance());
        assertInvalidBoth(validator, valid.toBuilder().setCursor(0).build());
        assertInvalidBoth(validator, valid.toBuilder().setTaskId("not-a-uuid").build());
        assertInvalidBoth(validator, valid.toBuilder().setAttempt(0).build());
        assertInvalidBoth(validator, valid.toBuilder().setAttempt(1025).build());
        assertInvalidBoth(validator, valid.toBuilder().clearOfferEntrySha256().build());
        assertInvalidBoth(validator, valid.toBuilder().setOfferEntrySha256("A".repeat(64)).build());
        assertInvalidBoth(validator, valid.toBuilder().setOfferEntrySha256("short").build());
    }

    @Test
    void responseCursorCelConstrainsEveryAssignmentToTheScanInterval() throws Exception {
        var validator = ProtoValidator.forMessageType(ReadWorkflowAuthorAssignmentsResponse.getDescriptor());
        ReadWorkflowAuthorAssignmentsResponse empty = response(7, 7);
        assertValidBoth(validator, empty);
        assertValidBoth(validator, empty.toBuilder().setCursor(12).build());

        var inRange = empty.toBuilder().setCursor(12)
                .addAssignments(assignment(8, TASK_A, 1, HASH_A))
                .addAssignments(assignment(12, TASK_B, 1024, HASH_B)).build();
        assertValidBoth(validator, inRange);
        assertRule(inRange.toBuilder().setCursor(6).build(), "author-assignments-cursor");
        assertRule(inRange.toBuilder().setAssignments(0, assignment(7, TASK_A, 1, HASH_A)).build(),
                "author-assignments-cursor");
        assertRule(inRange.toBuilder().setAssignments(1, assignment(13, TASK_B, 1, HASH_B)).build(),
                "author-assignments-cursor");
        assertInvalidBoth(validator, empty.toBuilder().clearWorkerId().build());
        assertInvalidBoth(validator, empty.toBuilder().setWorkerId("not a slug").build());
        assertInvalidBoth(validator, empty.toBuilder().setWorkerId("a".repeat(129)).build());
        assertInvalidBoth(validator, empty.toBuilder().setAfterCursor(-1).build());
        assertInvalidBoth(validator, empty.toBuilder().setCursor(-1).build());
    }

    @Test
    void repeatedResultsAreCappedAt64AndWellShapedHashesDoNotProveCustody() throws Exception {
        var validator = ProtoValidator.forMessageType(ReadWorkflowAuthorAssignmentsResponse.getDescriptor());
        var maximum = response(10, 100).toBuilder();
        for (int i = 0; i < 64; i++) {
            maximum.addAssignments(assignment(11 + i, TASK_A, 1, HASH_A));
        }
        assertValidBoth(validator, maximum.build());
        assertInvalidBoth(validator, maximum.addAssignments(assignment(75, TASK_B, 2, HASH_B)).build());

        WorkflowAuthorAssignment fabricated = assignment(11, TASK_A, 1, HASH_B);
        var shaped = response(10, 11).toBuilder().addAssignments(fabricated).build();
        assertValidBoth(validator, shaped);
        // Native rules can check only digest syntax and the response cursor window. The handler
        // must verify this digest against the deterministic bytes of a trusted original offer.
        assertThat(fabricated.getOfferEntrySha256()).isEqualTo(HASH_B);
    }

    @Test
    void unknownFieldsAtEitherMessageBoundaryFailRuntimeContractValidation() throws Exception {
        var valid = response(0, 1).toBuilder().addAssignments(assignment(1, TASK_A, 1, HASH_A)).build();
        assertThatThrownBy(() -> WorkflowLaunchValidation.validate(valid.toBuilder()
                .setUnknownFields(unknown()).build())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WorkflowLaunchValidation.validate(valid.toBuilder()
                .setAssignments(0, valid.getAssignments(0).toBuilder().setUnknownFields(unknown()))
                .build())).isInstanceOf(IllegalArgumentException.class);

        DynamicMessage dynamic = DynamicMessage.parseFrom(valid.getDescriptorForType(), valid.toByteArray());
        assertThatThrownBy(() -> WorkflowLaunchValidation.validate(dynamic.toBuilder()
                .setUnknownFields(unknown()).build())).isInstanceOf(IllegalArgumentException.class);
    }

    private static ReadWorkflowAuthorAssignmentsRequest request(long afterCursor, int maxAssignments) {
        return ReadWorkflowAuthorAssignmentsRequest.newBuilder().setAfterCursor(afterCursor)
                .setMaxAssignments(maxAssignments).build();
    }

    private static WorkflowAuthorAssignment assignment(long cursor, String taskId, int attempt, String hash) {
        return WorkflowAuthorAssignment.newBuilder().setCursor(cursor).setTaskId(taskId)
                .setAttempt(attempt).setOfferEntrySha256(hash).build();
    }

    private static ReadWorkflowAuthorAssignmentsResponse response(long afterCursor, long cursor) {
        return ReadWorkflowAuthorAssignmentsResponse.newBuilder().setWorkerId("workflow-author")
                .setAfterCursor(afterCursor).setCursor(cursor).build();
    }

    private static UnknownFieldSet unknown() {
        return UnknownFieldSet.newBuilder().addField(99,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
    }

    private static void assertRule(Message message, String ruleId) throws Exception {
        var validator = ProtoValidator.forMessageType(message.getDescriptorForType());
        assertThat(validator.validate(message).violations()).anySatisfy(
                violation -> assertThat(violation.ruleId()).isEqualTo(ruleId));
        DynamicMessage dynamic = DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
        assertThat(validator.validate(dynamic).violations()).anySatisfy(
                violation -> assertThat(violation.ruleId()).isEqualTo(ruleId));
    }

    private static void assertValidBoth(ProtoValidator validator, Message message) throws Exception {
        assertThat(validator.validate(message).valid())
                .as("generated violations: %s", validator.validate(message).violations()).isTrue();
        DynamicMessage dynamic = DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
        assertThat(validator.validate(dynamic).valid()).isTrue();
    }

    private static void assertInvalidBoth(ProtoValidator validator, Message message) throws Exception {
        assertThat(validator.validate(message).valid()).isFalse();
        DynamicMessage dynamic = DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
        assertThat(validator.validate(dynamic).valid()).isFalse();
    }
}
