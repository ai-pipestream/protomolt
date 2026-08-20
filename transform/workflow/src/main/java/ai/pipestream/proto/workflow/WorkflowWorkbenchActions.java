package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.grpc.workflow.ArtifactRepository;
import ai.pipestream.proto.grpc.workflow.WorkflowVersionRepository;
import ai.pipestream.proto.grpc.workflow.RunEvidenceRepository;

/** Registers the agent-operable workflow workbench actions with one shared host wiring. */
public final class WorkflowWorkbenchActions {

    private WorkflowWorkbenchActions() {
    }

    /** Registers with the record-signing identity read from the environment. */
    public static ActionCatalog register(ActionCatalog catalog, WorkflowRunner runner,
                                         ArtifactRepository artifacts,
                                         RunEvidenceRepository runs,
                                         WorkflowVersionRepository workflows) {
        return register(catalog, runner, artifacts, runs, workflows,
                RecordSigning.fromEnvironment());
    }

    public static ActionCatalog register(ActionCatalog catalog, WorkflowRunner runner,
                                         ArtifactRepository artifacts,
                                         RunEvidenceRepository runs,
                                         WorkflowVersionRepository workflows,
                                         RecordSigning signing) {
        return catalog.register(new CompileWorkflowAction())
                .register(new SuggestMappingsAction())
                .register(new RecordWorkflowRunAction(runner, artifacts, runs))
                .register(new ReplayWorkflowAction(artifacts, runs))
                .register(new PromoteWorkflowAction(workflows))
                .register(new ExportWorkRecordAction(runs, signing))
                .register(new VerifyWorkRecordAction())
                .register(new EvaluateWorkRecordAction(artifacts, runs));
    }
}
