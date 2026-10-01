package ai.protomolt.proto.samples;

import ai.protomolt.proto.workflow.authoring.WorkflowCandidatePreparer;
import ai.protomolt.proto.workflow.authoring.WorkflowSourceTemplateProvider;

/** Explicitly selected by the starter coordinator's trusted deployment configuration. */
public final class AuthoringStarterTemplateProvider implements WorkflowSourceTemplateProvider {
    public static final String ID = "normalize-record-v1";

    @Override public String id() { return ID; }

    @Override public WorkflowCandidatePreparer.SourceTemplate template() {
        return (workflow, source, policy) -> AuthoringStarterTemplate.verify(workflow);
    }
}
