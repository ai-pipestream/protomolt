package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.Lane;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowStep;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowDeliverable;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationBinding;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationFailure;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationFailureReason;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationIntent;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationRecord;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Validator contract for the durable preparation record; it does not prove trusted provenance. */
class WorkflowPreparationStateContractTest {
    private static final String TASK_ID = "00000000-0000-4000-8000-000000000001";
    private static final String PREPARATION_ID = "00000000-0000-4000-8000-000000000002";
    private static final String RUN_ID = "prepare-00000000-0000-4000-8000-000000000002";
    private static final String HASH_A = "a".repeat(64);
    private static final String HASH_B = "b".repeat(64);
    private static final byte[] POLICY_BYTES = new byte[] {0x08, 0x01, 0x10, 0x02};

    @Test
    void pendingCompletedAndFailedRecordsValidateAsGeneratedAndDynamicMessages() throws Exception {
        assertValidBoth(record().setPending(true).build());
        assertValidBoth(record().setCompleted(completed()).build());
        assertValidBoth(record().setFailed(failed(binding(),
                WorkflowPreparationFailureReason.WORKFLOW_PREPARATION_FAILURE_REASON_FIXTURE_REJECTED,
                RUN_ID)).build());
        assertValidBoth(record().setFailed(failed(binding(),
                WorkflowPreparationFailureReason.WORKFLOW_PREPARATION_FAILURE_REASON_RECORDED_RUN_FAILED,
                RUN_ID)).build());
        assertInvalidBoth(record().clearIntent().setPending(true).build());
        assertInvalidBoth(record().setIntent(intent().setVersion(2)).setPending(true).build());
    }

    @Test
    void stateOneofRequiresPresentTruePendingAndRejectsNoStateOrFalse() throws Exception {
        assertRule(record().build(), "preparation-state-required");
        assertInvalidBoth(record().setPending(false).build());
    }

    @Test
    void requestAndBindingMustMatchEveryReservedIdentityField() throws Exception {
        for (var mutatedBinding : List.of(
                binding().toBuilder().setTaskId("00000000-0000-4000-8000-000000000099").build(),
                binding().toBuilder().setAttempt(2).build(),
                binding().toBuilder().setRevision(2).build(),
                binding().toBuilder().setPreparationId("00000000-0000-4000-8000-000000000099").build())) {
            assertRule(record().setIntent(intent().setBinding(mutatedBinding).build())
                    .setPending(true).build(), "preparation-request-binding");
        }
    }

    @Test
    void runIdentityIsDerivedFromThePreparationId() throws Exception {
        assertRule(record().setIntent(intent().setRunId("other-preparation-run").build())
                .setPending(true).build(), "preparation-run-identity");
    }

    @Test
    void selectedOfferMustBeForHolderTaskAndAttemptAndMustActuallyBeAnOffer() throws Exception {
        assertRule(record().setIntent(intent().setHolder("other-worker").build())
                .setPending(true).build(), "preparation-offer-binding");
        assertRule(record().setIntent(intent().setSelectedOffer(offerEntry(
                "worker-two", TASK_ID, 1)).build()).setPending(true).build(), "preparation-offer-binding");
        assertRule(record().setIntent(intent().setSelectedOffer(offerEntry(
                "worker-one", "00000000-0000-4000-8000-000000000099", 1)).build())
                .setPending(true).build(), "preparation-offer-binding");
        assertRule(record().setIntent(intent().setSelectedOffer(offerEntry("worker-one", TASK_ID, 2)).build())
                .setPending(true).build(), "preparation-offer-binding");
        TranscriptEntry nonOffer = TranscriptEntry.newBuilder().setLane(Lane.LANE_COORDINATOR)
                .setWorkerId("worker-one").setCoordinatorFrame(DelegateResponse.newBuilder()
                        .setFrameId("00000000-0000-4000-8000-000000000010").setTaskId(TASK_ID)
                        .setSeq(1).setSentAt(Timestamp.newBuilder().setSeconds(1))).build();
        assertRule(record().setIntent(intent().setSelectedOffer(nonOffer).build())
                .setPending(true).build(), "preparation-offer-binding");
    }

    @Test
    void policySnapshotMustBeBoundedUnredactedProtobufWithExactStoredSize() throws Exception {
        var base = intent().build();
        assertRule(record().setIntent(base.toBuilder().setPolicy(base.getPolicy().toBuilder()
                .setMediaType("application/json")).build()).setPending(true).build(),
                "preparation-policy-snapshot");
        assertRule(record().setIntent(base.toBuilder().setPolicy(base.getPolicy().toBuilder().setRedacted(true).build())
                .build()).setPending(true).build(), "preparation-policy-snapshot");
        assertRule(record().setIntent(base.toBuilder().setPolicy(base.getPolicy().toBuilder()
                .setSizeBytes(POLICY_BYTES.length + 1).build()).build()).setPending(true).build(),
                "preparation-policy-snapshot");
        assertRule(record().setIntent(base.toBuilder().setPolicyProto(ByteString.EMPTY).build())
                .setPending(true).build(), "bytes.min_len");
        assertRule(record().setIntent(base.toBuilder().setPolicyProto(ByteString.copyFrom(new byte[4_194_305])).build())
                .setPending(true).build(), "bytes.max_len");
    }

    @Test
    void failedStateRequiresARecognizedNonzeroReasonAndBoundedIdentity() throws Exception {
        WorkflowPreparationFailure unspecified = failed(binding(),
                WorkflowPreparationFailureReason.WORKFLOW_PREPARATION_FAILURE_REASON_UNSPECIFIED, RUN_ID);
        assertThat(ProtoValidator.forMessageType(WorkflowPreparationFailure.getDescriptor())
                .validate(unspecified).valid()).isFalse();
        WorkflowPreparationFailure unknown = unspecified.toBuilder().setReasonValue(99).build();
        assertThat(ProtoValidator.forMessageType(WorkflowPreparationFailure.getDescriptor())
                .validate(unknown).valid()).isFalse();
        assertInvalidBoth(record().setFailed(unspecified).build());
        assertInvalidBoth(record().setFailed(unknown).build());
    }

    @Test
    void completedAndFailedTerminalIdentityMustMatchIntent() throws Exception {
        var differentTask = binding().toBuilder().setTaskId("00000000-0000-4000-8000-000000000099").build();
        assertRule(record().setCompleted(completed().toBuilder().setBinding(differentTask).build()).build(),
                "preparation-completed-binding");
        assertRule(record().setCompleted(completed().toBuilder()
                .setAuthored(authored("different-run")).build()).build(), "preparation-completed-binding");
        assertRule(record().setFailed(failed(differentTask,
                WorkflowPreparationFailureReason.WORKFLOW_PREPARATION_FAILURE_REASON_RECORDED_RUN_FAILED,
                RUN_ID)).build(), "preparation-failed-binding");
        assertRule(record().setFailed(failed(binding(),
                WorkflowPreparationFailureReason.WORKFLOW_PREPARATION_FAILURE_REASON_RECORDED_RUN_FAILED,
                "different-run")).build(), "preparation-failed-binding");
    }

    @Test
    void validHashesAndPolicyShapeDoNotProveTrustedBytes() throws Exception {
        WorkflowPreparationIntent forged = intent().setBinding(binding().toBuilder()
                        .setOfferEntrySha256(HASH_B).setSourceSha256(HASH_B).build())
                .setPolicyProto(ByteString.copyFromUtf8("not a serialized policy"))
                .setPolicy(artifact(HASH_A, "application/x-protobuf", false,
                        "not a serialized policy".getBytes().length)).build();
        WorkflowPreparationRecord shaped = record().setIntent(forged).setPending(true).build();

        assertValidBoth(shaped);
        // The contract checks hash syntax and snapshot dimensions. Recovery must independently
        // verify the offer hash, source hash, pinned policy digest, parse, and trusted transcript.
    }

    private static WorkflowPreparationRecord.Builder record() {
        return WorkflowPreparationRecord.newBuilder().setIntent(intent());
    }

    private static WorkflowPreparationIntent.Builder intent() {
        PrepareWorkflowCandidateRequest request = PrepareWorkflowCandidateRequest.newBuilder()
                .setTaskId(TASK_ID).setAttempt(1).setRevision(1).setPreparationId(PREPARATION_ID)
                .setExecutableSourceJson(ByteString.copyFromUtf8("{}"))
                .build();
        WorkflowPreparationBinding binding = binding();
        return WorkflowPreparationIntent.newBuilder().setVersion(1).setRequest(request).setBinding(binding)
                .setHolder("worker-one").setSelectedOffer(offerEntry("worker-one", TASK_ID, 1))
                .setPolicy(artifact(HASH_A, "application/x-protobuf", false, POLICY_BYTES.length))
                .setPolicyProto(ByteString.copyFrom(POLICY_BYTES)).setRunId(RUN_ID);
    }

    private static WorkflowPreparationBinding binding() {
        return WorkflowPreparationBinding.newBuilder().setTaskId(TASK_ID).setAttempt(1).setRevision(1)
                .setPreparationId(PREPARATION_ID).setOfferEntrySha256(HASH_A).setSourceSha256(HASH_B).build();
    }

    private static TranscriptEntry offerEntry(String worker, String task, int attempt) {
        return TranscriptEntry.newBuilder().setLane(Lane.LANE_COORDINATOR).setWorkerId(worker)
                .setCoordinatorFrame(DelegateResponse.newBuilder()
                        .setFrameId("00000000-0000-4000-8000-000000000010").setTaskId(task)
                        .setSeq(1).setSentAt(Timestamp.newBuilder().setSeconds(1))
                        .setOffer(TaskOffer.newBuilder().setAttempt(attempt)
                                .setSpec(TaskSpec.newBuilder().setObjective("prepare workflow")
                                        .addRequiredChecks(AcceptanceCheck.newBuilder()
                                                .setName("validate-workflow")
                                                .setDescription("validate prepared workflow")))
                                .setLeaseDuration(Duration.newBuilder().setSeconds(30))
                                .setExpiresAt(Timestamp.newBuilder().setSeconds(60))))
                .build();
    }

    private static PrepareWorkflowCandidateResponse completed() {
        return PrepareWorkflowCandidateResponse.newBuilder().setBinding(binding())
                .setAuthored(authored(RUN_ID)).build();
    }

    private static WorkflowPreparationFailure failed(WorkflowPreparationBinding binding,
            WorkflowPreparationFailureReason reason, String runId) {
        return WorkflowPreparationFailure.newBuilder().setBinding(binding).setReason(reason)
                .setRunId(runId).build();
    }

    private static WorkflowAuthoringDeliverable authored(String runId) {
        ArtifactReference evidence = artifact(HASH_A, "application/x-protobuf", false, 24);
        Workflow workflow = Workflow.newBuilder().setName("prepared-workflow")
                .setInputType("example.v1.Input").setValidateContract(true)
                .addDependencies(ServiceDependency.newBuilder().setAlias("echo")
                        .setServiceProfile("example.v1.Echo").setEndpoint("local")
                        .setDescriptorFingerprint(HASH_A))
                .addSteps(WorkflowStep.newBuilder().setName("call").setDependency("echo")
                        .setMethod("example.v1.Echo/Run")
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE))
                .setDeadline(Duration.newBuilder().setSeconds(30)).build();
        WorkflowDeliverable deliverable = WorkflowDeliverable.newBuilder().setWorkflow(workflow)
                .setWorkflowArtifact(evidence).setDescriptors(evidence).addFixtures(evidence)
                .addChecks(ai.protomolt.proto.delegation.v1.CheckEvidence.newBuilder()
                        .setCheckName("prepare").setVerdict(ai.protomolt.proto.delegation.v1.CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamp.newBuilder().setSeconds(1)).addArtifacts(evidence))
                .setRunId(runId).setReceipt(evidence).build();
        return WorkflowAuthoringDeliverable.newBuilder().setDeliverable(deliverable)
                .setExecutableSource(artifact(binding().getSourceSha256(), "application/json", false, 24))
                .addAcceptanceFixtures(WorkflowAcceptanceFixture.newBuilder()
                        .setName("default-fixture").setInput(evidence).setExpectedOutput(evidence)).build();
    }

    private static ArtifactReference artifact(String hash, String mediaType, boolean redacted, int size) {
        return ArtifactReference.newBuilder().setSha256(hash).setMediaType(mediaType)
                .setSizeBytes(size).setRedacted(redacted).build();
    }

    private static void assertRule(WorkflowPreparationRecord message, String ruleId) throws Exception {
        var result = ProtoValidator.forMessageType(WorkflowPreparationRecord.getDescriptor()).validate(message);
        assertThat(result.violations()).anySatisfy(v -> assertThat(v.ruleId()).isEqualTo(ruleId));
        assertInvalidDynamic(message);
    }

    private static void assertValidBoth(WorkflowPreparationRecord message) throws Exception {
        var validator = ProtoValidator.forMessageType(WorkflowPreparationRecord.getDescriptor());
        assertThat(validator.validate(message).valid())
                .as("generated violations: %s", validator.validate(message).violations()).isTrue();
        DynamicMessage dynamic = DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
        assertThat(validator.validate(dynamic).valid()).isTrue();
    }

    private static void assertInvalidBoth(WorkflowPreparationRecord message) throws Exception {
        var validator = ProtoValidator.forMessageType(WorkflowPreparationRecord.getDescriptor());
        assertThat(validator.validate(message).valid()).isFalse();
        assertInvalidDynamic(message);
    }

    private static void assertInvalidDynamic(WorkflowPreparationRecord message) throws Exception {
        DynamicMessage dynamic = DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
        assertThat(ProtoValidator.forMessageType(WorkflowPreparationRecord.getDescriptor())
                .validate(dynamic).valid()).isFalse();
    }
}
