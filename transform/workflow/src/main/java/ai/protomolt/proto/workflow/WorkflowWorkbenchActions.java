package ai.protomolt.proto.workflow;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowVersionRepository;
import ai.protomolt.proto.grpc.workflow.RunEvidenceRepository;

/** Registers the agent-operable workflow workbench actions with one shared host wiring. */
public final class WorkflowWorkbenchActions {

    private WorkflowWorkbenchActions() {
    }

    /** Registers with the signing identity and trust pin read from the environment. */
    public static ActionCatalog register(ActionCatalog catalog, WorkflowRunner runner,
                                         ArtifactRepository artifacts,
                                         RunEvidenceRepository runs,
                                         WorkflowVersionRepository workflows) {
        return register(catalog, runner, artifacts, runs, workflows,
                RecordSigning.fromEnvironment());
    }

    /** Registers with an explicit signing identity; the trust pin still reads the environment. */
    public static ActionCatalog register(ActionCatalog catalog, WorkflowRunner runner,
                                         ArtifactRepository artifacts,
                                         RunEvidenceRepository runs,
                                         WorkflowVersionRepository workflows,
                                         RecordSigning signing) {
        return register(catalog, runner, artifacts, runs, workflows, signing,
                TrustPin.fromEnvironment());
    }

    public static ActionCatalog register(ActionCatalog catalog, WorkflowRunner runner,
                                         ArtifactRepository artifacts,
                                         RunEvidenceRepository runs,
                                         WorkflowVersionRepository workflows,
                                         RecordSigning signing, TrustPin trust) {
        TrustSnapshot pinned = trust == null ? null : trust.snapshot();
        return register(catalog, runner, artifacts, runs, workflows, signing,
                () -> pinned);
    }

    /**
     * Registers the workbench with a live trust source: the verifying verbs read the
     * server's custody per request instead of the value it held at registration, so a
     * snapshot arriving on the config lane applies without re-registering the catalog.
     * A request's own {@code trust} always wins, whatever the source answers.
     *
     * @param trust the server's current trust snapshot, or one answering null when it
     *        keeps none
     */
    public static ActionCatalog register(ActionCatalog catalog, WorkflowRunner runner,
                                         ArtifactRepository artifacts,
                                         RunEvidenceRepository runs,
                                         WorkflowVersionRepository workflows,
                                         RecordSigning signing,
                                         java.util.function.Supplier<TrustSnapshot> trust) {
        java.util.function.Supplier<TrustSnapshot> pinned =
                trust == null ? () -> null : trust;
        return catalog.register(new CompileWorkflowAction())
                .register(new SuggestMappingsAction())
                .register(new RecordWorkflowRunAction(runner, artifacts, runs))
                .register(new ReplayWorkflowAction(artifacts, runs))
                .register(new PromoteWorkflowAction(workflows))
                .register(new ExportWorkRecordAction(runs, signing))
                .register(new VerifyWorkRecordAction(pinned))
                .register(new EvaluateWorkRecordAction(artifacts, runs, pinned));
    }
}
