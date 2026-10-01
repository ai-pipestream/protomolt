package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchResult;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.authoring.v1.GetAcceptedWorkflowRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringServiceGrpc;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringServiceOuterClass;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WorkflowAuthoringServiceContractTest {
    @Test
    void generatedServiceReusesTheReviewedLaunchAndIdentityContracts() {
        var service = WorkflowAuthoringServiceOuterClass.getDescriptor()
                .findServiceByName("WorkflowAuthoringService");
        var lookup = service.findMethodByName("GetAcceptedWorkflow");
        var launch = service.findMethodByName("LaunchAcceptedWorkflow");
        assertThat(lookup.getInputType()).isEqualTo(GetAcceptedWorkflowRequest.getDescriptor());
        assertThat(lookup.getOutputType()).isEqualTo(WorkflowAcceptedCandidate.getDescriptor());
        assertThat(launch.getInputType()).isEqualTo(WorkflowAuthoringLaunchRequest.getDescriptor());
        assertThat(launch.getOutputType()).isEqualTo(WorkflowAuthoringLaunchResult.getDescriptor());
        assertThat(WorkflowAuthoringServiceGrpc.getLaunchAcceptedWorkflowMethod().getFullMethodName())
                .isEqualTo(service.getFullName() + "/LaunchAcceptedWorkflow");
        assertThat(service.getMethods()).allSatisfy(method -> {
            assertThat(method.isClientStreaming()).isFalse();
            assertThat(method.isServerStreaming()).isFalse();
        });
    }

    @Test
    void lookupRequestsValidateThroughGeneratedAndDynamicDescriptors() throws Exception {
        var descriptor = GetAcceptedWorkflowRequest.getDescriptor();
        var validator = ProtoValidator.forMessageType(descriptor);
        assertThat(validator.validate(GetAcceptedWorkflowRequest.getDefaultInstance()).valid()).isFalse();
        assertThat(validator.validate(GetAcceptedWorkflowRequest.newBuilder()
                .setTaskId("not-a-task-uuid").build()).valid()).isFalse();
        var request = GetAcceptedWorkflowRequest.newBuilder()
                .setTaskId("dcff5265-1466-46c0-bbe0-a9dc39da0e82").build();
        assertThat(validator.validate(request).valid()).isTrue();
        assertThat(validator.validate(DynamicMessage.parseFrom(descriptor, request.toByteArray())).valid())
                .isTrue();
        // Shape validity is not transcript membership or permission to launch.
        assertThat(ProtoValidator.forMessageType(WorkflowAcceptedCandidate.getDescriptor())
                .validate(WorkflowAcceptedCandidate.getDefaultInstance()).valid()).isFalse();
    }
}
