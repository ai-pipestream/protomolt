package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.delegation.DelegationActions;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.workflow.authoring.WorkflowAuthorTaskActions;
import ai.protomolt.proto.workflow.authoring.WorkflowAuthoringActions;
import ai.protomolt.proto.workflow.authoring.WorkflowAuthoringEntryActions;
import ai.protomolt.proto.workflow.authoring.WorkflowLaunchInputActions;
import ai.protomolt.proto.workflow.authoring.WorkflowLaunchStatusActions;
import ai.protomolt.proto.workflow.authoring.WorkflowPreparationActions;
import java.time.Clock;
import java.util.Map;

/**
 * The delegation family of the serve wiring: one coordinator per server, adapted to catalog
 * verbs and MCP resources through the bridge. Durable when a delegation repository endpoint is
 * configured, in-memory otherwise. When workflow authoring is mounted the runtime runs with
 * the authoring-selected reviewer, and the authoring, launch, preparation, and author-task
 * verbs register over the same transcripts.
 */
final class DelegationMount {

    private DelegationMount() {
    }

    /**
     * Opens the delegation runtime. With workflow authoring mounted, the authoring policy's
     * reviewer selects candidates; otherwise review stays manual.
     */
    static DelegationRuntime open(ProtoMoltServe.DelegationOptions options,
                                  WorkflowAuthoringMount.Prepared authoring) {
        return authoring == null
                ? DelegationRuntime.open(options)
                : DelegationRuntime.open(options, authoring.selectingReviewer());
    }

    /**
     * Registers the delegation verbs, then the authoring verbs, then the preparation verbs, in
     * that order, and returns the explicit REST bindings of the author-task service (empty when
     * preparation is not mounted).
     */
    static Map<String, Map<String, String>> registerAll(ActionCatalog catalog,
                                                        DelegationRuntime delegation,
                                                        WorkflowAuthoringMount.Prepared authoring,
                                                        WorkflowPreparationMount.Prepared preparation) {
        DelegationBridge bridge = delegation.bridge();
        DelegationActions.register(catalog, bridge);
        if (authoring != null) {
            WorkflowAuthoringActions.register(catalog,
                    authoring.operations(delegation.transcripts()));
            WorkflowLaunchInputActions.register(catalog,
                    authoring.inputOperations(delegation.transcripts()));
            WorkflowLaunchStatusActions.register(catalog,
                    authoring.statusOperations(delegation.transcripts()));
            WorkflowAuthoringEntryActions.register(catalog,
                    authoring.entryOperations(bridge));
        }
        if (preparation != null) {
            WorkflowPreparationActions.register(catalog,
                    preparation.operations(delegation.transcripts()));
            WorkflowAuthorTaskActions.register(catalog, WorkflowAuthorTaskMount.operations(
                    bridge, delegation.transcripts(), authoring, Clock.systemUTC()));
        }
        return preparation == null ? Map.of() : WorkflowAuthorTaskMount.bindings();
    }
}
