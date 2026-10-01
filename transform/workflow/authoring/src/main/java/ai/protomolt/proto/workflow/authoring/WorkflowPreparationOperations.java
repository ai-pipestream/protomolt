package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateResponse;

/** Trusted host operation; resolved identity is never taken from request data. */
@FunctionalInterface
public interface WorkflowPreparationOperations {
    PrepareWorkflowCandidateResponse prepare(PrepareWorkflowCandidateRequest request, Caller caller)
            throws WorkflowPreparationException;
}
