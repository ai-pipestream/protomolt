package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchResult;

/** Trusted host binding behind the contributed authoring protocol. */
public interface WorkflowAuthoringOperations {
    /** Read the exact delegation task identity without fixture or launch effects. */
    WorkflowAcceptedCandidate acceptedCandidate(String taskId) throws Exception;

    /** Verify and launch through durable authorization; success means a matching job exists. */
    WorkflowAuthoringLaunchResult launch(WorkflowAuthoringLaunchRequest request) throws Exception;
}
