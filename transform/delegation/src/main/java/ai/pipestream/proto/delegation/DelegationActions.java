package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionCatalog;

import java.util.Objects;

/**
 * Registers the delegation verbs on an {@link ActionCatalog}: worker registration and
 * discovery, task offers, worker responses (accept, progress, checkpoint, candidate),
 * review, cancellation, non-transitioning task messages, the cursor-based long-poll
 * watch, and transcript inspection. Every verb is a thin adapter over the
 * {@link DelegationBridge}; the coordinator and the reducer own the lifecycle.
 */
public final class DelegationActions {

    private DelegationActions() {
    }

    /** Registers every delegation verb against one shared bridge. */
    public static ActionCatalog register(ActionCatalog catalog, DelegationBridge bridge) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(bridge, "bridge");
        return catalog
                .register(new DelegationWorkerRegisterAction(bridge))
                .register(new DelegationWorkerListAction(bridge))
                .register(new DelegationOfferAction(bridge))
                .register(new DelegationAcceptAction(bridge))
                .register(new DelegationProgressAction(bridge))
                .register(new DelegationCheckpointAction(bridge))
                .register(new DelegationCandidateAction(bridge))
                .register(new DelegationReviewAction(bridge))
                .register(new DelegationCancelAction(bridge))
                .register(new DelegationMessageAction(bridge))
                .register(new DelegationWatchAction(bridge))
                .register(new DelegationTranscriptAction(bridge));
    }
}
