package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchAuthorization;
import java.io.IOException;
import java.util.Optional;

/** Durable, keyed authority for one independently verified workflow launch. */
public interface WorkflowLaunchAuthorizationRepository {

    /** Returns the validated authorization stored under the UUID, accepting case aliases. */
    Optional<WorkflowAuthoringLaunchAuthorization> find(String launchId) throws IOException;

    /**
     * Atomically creates a binding, returns an identical one, or rejects changed content.
     * A case-only difference inside the serialized request is changed content at the same key.
     */
    WorkflowAuthoringLaunchAuthorization createOrMatch(WorkflowAuthoringLaunchAuthorization authorization)
            throws IOException;
}
