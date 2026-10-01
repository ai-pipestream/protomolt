package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.delegation.v1.*;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.samples.starter.v1.WorkflowPermittedCall;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.authoring.v1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Duration;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Annotation fixtures; storage, authority and semantic checks are separate gates. */
class WorkflowAuthorContractTest {
    private static final String TASK = "00000000-0000-4000-8000-000000000001";
    private static final String OTHER = "00000000-0000-4000-8000-000000000002";

    @Test
    void requiredTaskAndAttemptPreventGlobalReads() throws Exception {
        var context = GetWorkflowAuthorContextRequest.newBuilder().setTaskId(TASK).setAttempt(1).build();
        check(context, true);
        check(context.toBuilder().setAttempt(1024).build(), true);
        check(context.toBuilder().clearTaskId().build(), false);
        check(context.toBuilder().setTaskId("wrong").build(), false);
        check(context.toBuilder().setAttempt(0).build(), false);
        check(context.toBuilder().setAttempt(1025).build(), false);
        var events = ReadWorkflowAuthorEventsRequest.newBuilder().setTaskId(TASK).setAttempt(1)
                .setMaxEvents(64).build();
        check(events, true);
        check(events.toBuilder().clearTaskId().build(), false);
        check(events.toBuilder().setAttempt(0).build(), false);
        check(events.toBuilder().setAfterCursor(-1).build(), false);
        check(events.toBuilder().setMaxEvents(0).build(), false);
        check(events.toBuilder().setMaxEvents(65).build(), false);
    }

    @Test
    void contextBindsOfferAndDescriptorMetadata() throws Exception {
        var value = context();
        check(value, true);
        check(value.toBuilder().setTaskId(OTHER).build(), false);
        check(value.toBuilder().setAttempt(2).build(), false);
        check(value.toBuilder().clearOfferEntry().build(), false);
        check(value.toBuilder().setOfferEntry(value.getOfferEntry().toBuilder()
                .setCoordinatorFrame(value.getOfferEntry().getCoordinatorFrame().toBuilder()
                        .clearOffer().setCancellation(Cancellation.newBuilder().setAttempt(1)
                                .setReason("cancelled")))).build(), false);
        check(value.toBuilder().clearPolicy().build(), false);
        check(value.toBuilder().clearDescriptorSet().build(), false);
        check(value.toBuilder().setDescriptors(value.getDescriptors().toBuilder().setSizeBytes(2)).build(), false);
        check(value.toBuilder().setDescriptors(value.getDescriptors().toBuilder().setRedacted(true)).build(), false);
        check(value.toBuilder().setDescriptors(value.getDescriptors().toBuilder().setMediaType("text/plain")).build(), false);
        check(value.toBuilder().clearPermittedCalls().build(), false);
        check(value.toBuilder().setOfferEntrySha256("wrong").build(), false);
    }

    @Test
    void byteAndCallBoundsDoNotProveSchemaOrHashCorrectness() throws Exception {
        var value = context();
        check(value.toBuilder().setDescriptorSet(ByteString.copyFrom(new byte[4_194_304]))
                .setDescriptors(value.getDescriptors().toBuilder().setSizeBytes(4_194_304)).build(), true);
        check(value.toBuilder().setDescriptorSet(ByteString.copyFrom(new byte[4_194_305]))
                .setDescriptors(value.getDescriptors().toBuilder().setSizeBytes(4_194_305)).build(), false);
        var builder = value.toBuilder().clearPermittedCalls();
        for (int i = 0; i < 128; i++) builder.addPermittedCalls(value.getPermittedCalls(0));
        check(builder.build(), true);
        check(builder.addPermittedCalls(value.getPermittedCalls(0)).build(), false);
        // Fabricated but well-shaped evidence passes annotations. Handlers must
        // hash bytes, resolve imports, authenticate ownership and verify policy.
        check(value.toBuilder().setOfferEntrySha256("b".repeat(64)).build(), true);
    }

    @Test
    void eventPagesEnforceTaskAndCursorWindow() throws Exception {
        var empty = ReadWorkflowAuthorEventsResponse.newBuilder().setTaskId(TASK).setAttempt(1)
                .setAfterCursor(4).setCursor(4).build();
        check(empty, true);
        check(empty.toBuilder().setCursor(5).build(), true);
        check(empty.toBuilder().setCursor(3).build(), false);
        var event = ObservedEvent.newBuilder().setCursor(5).setTaskId(TASK).setWorkerId("author")
                .setLane(Lane.LANE_COORDINATOR).setEntry(context().getOfferEntry()).build();
        var page = empty.toBuilder().setCursor(5).addEvents(event).build();
        check(page, true);
        check(page.toBuilder().setEvents(0, event.toBuilder().setTaskId(OTHER)).build(), false);
        check(page.toBuilder().setEvents(0, event.toBuilder().setCursor(4)).build(), false);
        check(page.toBuilder().setEvents(0, event.toBuilder().setCursor(6)).build(), false);
        // Attempt/owner checks require the validated original transcript.
        check(page.toBuilder().setAttempt(2).build(), true);
    }

    @Test
    void serviceReusesDelegationContractsWithoutCoordinatorMethods() {
        var service = WorkflowAuthorTaskServiceOuterClass.getDescriptor().findServiceByName("WorkflowAuthorTaskService");
        assertThat(service.findMethodByName("RegisterWorkflowAuthor").getInputType())
                .isEqualTo(RegisterWorkerRequest.getDescriptor());
        assertThat(service.findMethodByName("AcceptWorkflowTask").getInputType())
                .isEqualTo(AcceptTaskRequest.getDescriptor());
        assertThat(service.findMethodByName("SubmitWorkflowCandidate").getInputType())
                .isEqualTo(SubmitCandidateRequest.getDescriptor());
        assertThat(service.getMethods()).extracting(method -> method.getName()).containsExactly(
                "RegisterWorkflowAuthor", "AcceptWorkflowTask", "SubmitWorkflowCandidate",
                "GetWorkflowAuthorContext", "ReadWorkflowAuthorEvents");
    }

    private static GetWorkflowAuthorContextResponse context() {
        var offer = TaskOffer.newBuilder().setAttempt(1)
                .setSpec(TaskSpec.newBuilder().setObjective("Author a workflow")
                        .addRequiredChecks(AcceptanceCheck.newBuilder().setName("fixture-valid")))
                .setLeaseDuration(Duration.newBuilder().setSeconds(30))
                .setExpiresAt(Timestamp.newBuilder().setSeconds(100)).build();
        var entry = TranscriptEntry.newBuilder().setWorkerId("author").setLane(Lane.LANE_COORDINATOR)
                .setCoordinatorFrame(DelegateResponse.newBuilder().setFrameId(OTHER).setTaskId(TASK)
                        .setSeq(1).setSentAt(Timestamp.newBuilder().setSeconds(70)).setOffer(offer)).build();
        var reference = ArtifactReference.newBuilder().setSha256("a".repeat(64))
                .setMediaType("application/x-protobuf").setSizeBytes(1).build();
        return GetWorkflowAuthorContextResponse.newBuilder().setTaskId(TASK).setAttempt(1)
                .setOfferEntry(entry).setOfferEntrySha256("a".repeat(64)).setPolicy(reference)
                .setDescriptors(reference).setDescriptorSet(ByteString.copyFromUtf8("x"))
                .addPermittedCalls(WorkflowPermittedCall.newBuilder().setTarget("localhost:9090")
                        .setMethod("example.Service/Execute")).build();
    }

    private static void check(Message message, boolean expected) throws Exception {
        var validator = ProtoValidator.forMessageType(message.getDescriptorForType());
        assertThat(validator.validate(message).valid()).as("%s", validator.validate(message)).isEqualTo(expected);
        assertThat(validator.validate(DynamicMessage.parseFrom(message.getDescriptorForType(),
                message.toByteArray())).valid()).isEqualTo(expected);
    }
}
