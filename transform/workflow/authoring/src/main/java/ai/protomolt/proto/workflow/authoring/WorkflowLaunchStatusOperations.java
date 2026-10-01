package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchStatusRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchStatusResponse;

/** Read-only observation of one caller-supplied, immutable launch intent. */
public interface WorkflowLaunchStatusOperations {
    GetWorkflowLaunchStatusResponse get(GetWorkflowLaunchStatusRequest request)
            throws WorkflowLaunchStatusException;
}
