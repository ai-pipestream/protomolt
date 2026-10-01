package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthoringTemplateRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthoringTemplateResponse;
import ai.protomolt.proto.workflow.authoring.v1.StartWorkflowAuthoringRequest;
import ai.protomolt.proto.workflow.authoring.v1.StartWorkflowAuthoringResponse;

/** Host-configured, create-once entry to the existing delegation coordinator. */
public interface WorkflowAuthoringEntryOperations {
    GetWorkflowAuthoringTemplateResponse template(GetWorkflowAuthoringTemplateRequest request);
    StartWorkflowAuthoringResponse start(StartWorkflowAuthoringRequest request);
}
