package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.grpc.workflow.ArtifactRepository;
import ai.pipestream.proto.grpc.workflow.WorkflowVersionRepository;
import ai.pipestream.proto.grpc.workflow.RunEvidenceRepository;

/** Registers the agent-operable workflow workbench actions with one shared host wiring. */
public final class WorkflowWorkbenchActions {

    private WorkflowWorkbenchActions() {
    }

    public static ActionCatalog register(ActionCatalog catalog, WorkflowRunner runner,
                                         ArtifactRepository artifacts,
                                         RunEvidenceRepository runs,
                                         WorkflowVersionRepository workflows) {
        return catalog.register(new CompileWorkflowAction())
                .register(new SuggestMappingsAction())
                .register(new RecordWorkflowRunAction(runner, artifacts, runs))
                .register(new ReplayWorkflowAction(artifacts, runs))
                .register(new PromoteWorkflowAction(workflows));
    }
}
