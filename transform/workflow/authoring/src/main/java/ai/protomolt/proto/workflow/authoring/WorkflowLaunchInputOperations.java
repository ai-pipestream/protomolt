package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchInputContractRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchInputContractResponse;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputRequest;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputResponse;

/** Trusted input lookup and preparation; transport authorization belongs to the mount. */
public interface WorkflowLaunchInputOperations {
    GetWorkflowLaunchInputContractResponse contract(GetWorkflowLaunchInputContractRequest request)
            throws WorkflowLaunchInputException;

    PrepareWorkflowLaunchInputResponse prepare(PrepareWorkflowLaunchInputRequest request)
            throws WorkflowLaunchInputException;
}
