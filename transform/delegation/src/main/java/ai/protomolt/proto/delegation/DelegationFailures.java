package ai.protomolt.proto.delegation;

import ai.protomolt.proto.actions.ActionException;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** The stable error codes the delegation verbs report. */
final class DelegationFailures {

    private DelegationFailures() {
    }

    /** The named worker has no live bridge session. */
    static ActionException unknownWorker(String workerId) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("workerId", workerId);
        return new ActionException("unknown-worker",
                "Worker '" + workerId + "' is not registered; call delegation-worker-register",
                details);
    }

    /** The worker's delegation stream already failed; re-register to continue. */
    static ActionException streamFailed(String workerId, String detail) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("workerId", workerId);
        return new ActionException("worker-stream-failed",
                "Worker stream for '" + workerId + "' failed: " + detail, details);
    }

    /** The coordinator refused the operation (unknown task, stale phase, bad frame). */
    static ActionException rejected(String detail) {
        return new ActionException("delegation-rejected", detail);
    }
}
