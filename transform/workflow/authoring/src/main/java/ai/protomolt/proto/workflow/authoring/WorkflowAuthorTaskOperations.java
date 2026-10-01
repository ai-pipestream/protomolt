package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.delegation.v1.AcceptTaskRequest;
import ai.protomolt.proto.delegation.v1.AcceptTaskResponse;
import ai.protomolt.proto.delegation.v1.RegisterWorkerRequest;
import ai.protomolt.proto.delegation.v1.RegisterWorkerResponse;
import ai.protomolt.proto.delegation.v1.SubmitCandidateRequest;
import ai.protomolt.proto.delegation.v1.SubmitCandidateResponse;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthorContextRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthorContextResponse;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorEventsRequest;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorEventsResponse;

/** Trusted task operations; implementations enforce transcript and artifact authority. */
public interface WorkflowAuthorTaskOperations {
    RegisterWorkerResponse register(RegisterWorkerRequest request, Caller caller) throws WorkflowPreparationException;
    AcceptTaskResponse accept(AcceptTaskRequest request, Caller caller) throws WorkflowPreparationException;
    SubmitCandidateResponse submit(SubmitCandidateRequest request, Caller caller) throws WorkflowPreparationException;
    GetWorkflowAuthorContextResponse context(GetWorkflowAuthorContextRequest request, Caller caller) throws WorkflowPreparationException;
    ReadWorkflowAuthorEventsResponse events(ReadWorkflowAuthorEventsRequest request, Caller caller) throws WorkflowPreparationException;
}
