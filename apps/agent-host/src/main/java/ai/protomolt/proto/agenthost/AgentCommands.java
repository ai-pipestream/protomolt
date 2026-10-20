package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.delegation.v1.AcceptTaskRequest;
import ai.protomolt.proto.delegation.v1.CancelTaskRequest;
import ai.protomolt.proto.delegation.v1.OfferTaskRequest;
import ai.protomolt.proto.delegation.v1.RecordCheckpointRequest;
import ai.protomolt.proto.delegation.v1.RejectTaskRequest;
import ai.protomolt.proto.delegation.v1.ReportProgressRequest;
import ai.protomolt.proto.delegation.v1.ReviewCandidateRequest;
import ai.protomolt.proto.delegation.v1.SendTaskMessageRequest;
import ai.protomolt.proto.delegation.v1.SubmitCandidateRequest;
import com.google.protobuf.Descriptors.Descriptor;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The command catalog the turn contract is built from: which delegation
 * request message each tool carries, which tools each role may issue, and
 * which members the host pins itself. The parser, the structured-output
 * schema and the prose contract all read this one statement, so none of
 * them can drift from the others.
 */
final class AgentCommands {

    private AgentCommands() {
    }

    /** The most commands one turn may carry. */
    static final int MAX_COMMANDS = 16;
    /** The command that records a decision to take no protocol action; host-local. */
    static final String ACK = "host-ack";
    /** The command whose sender and recipient the host pins on both sides. */
    static final String MESSAGE = "delegation-message";
    static final String CANDIDATE = "delegation-candidate";

    /** The delegation request message each tool carries, by tool name. */
    static final Map<String, Descriptor> REQUESTS = Map.of(
            "delegation-accept", AcceptTaskRequest.getDescriptor(),
            "delegation-reject", RejectTaskRequest.getDescriptor(),
            "delegation-progress", ReportProgressRequest.getDescriptor(),
            "delegation-checkpoint", RecordCheckpointRequest.getDescriptor(),
            "delegation-candidate", SubmitCandidateRequest.getDescriptor(),
            MESSAGE, SendTaskMessageRequest.getDescriptor(),
            "delegation-offer", OfferTaskRequest.getDescriptor(),
            "delegation-review", ReviewCandidateRequest.getDescriptor(),
            "delegation-cancel", CancelTaskRequest.getDescriptor());

    private static final List<String> WORKER_TOOLS = List.of(
            ACK, "delegation-accept", "delegation-reject", MESSAGE,
            "delegation-progress", "delegation-checkpoint", "delegation-candidate");
    private static final List<String> COORDINATOR_TOOLS = List.of(
            ACK, "delegation-offer", MESSAGE,
            "delegation-review", "delegation-cancel");

    /** The commands a role may issue, host-local acknowledgement first. */
    static List<String> tools(AgentRole role) {
        return role == AgentRole.WORKER ? WORKER_TOOLS : COORDINATOR_TOOLS;
    }

    /** The members the host sets itself, which the model is neither asked for nor allowed. */
    static Set<String> pinnedFields(AgentRole role, String tool) {
        if (ACK.equals(tool)) {
            return Set.of();
        }
        if (role == AgentRole.WORKER) {
            return MESSAGE.equals(tool) ? Set.of("sender", "recipient") : Set.of("workerId");
        }
        return MESSAGE.equals(tool) ? Set.of("sender") : Set.of();
    }
}
