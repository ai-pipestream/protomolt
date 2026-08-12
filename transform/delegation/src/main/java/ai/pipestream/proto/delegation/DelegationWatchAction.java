package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Long-polls the coordinator's event feed from a caller-owned cursor. The call blocks
 * (safely on a virtual thread) until at least one event appears after the cursor or the
 * timeout elapses, then returns a bounded batch and the resumption cursor. Reconnecting
 * MCP sessions resume from their last cursor with no lost or duplicated frames.
 */
final class DelegationWatchAction extends DelegationAction {

    /** The largest long-poll a single call may hold open. */
    static final int MAX_TIMEOUT_MS = 30_000;

    DelegationWatchAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-watch";
    }

    @Override
    public String description() {
        return "Long-polls the delegation event feed: blocks up to timeoutMs until an event "
                + "appears after 'afterCursor', then returns a bounded batch with the "
                + "resumption cursor. Every frame (offers, progress, checkpoints, messages, "
                + "review verdicts) is one cursor-addressable event; reconnecting sessions "
                + "resume from their last cursor with no lost or duplicated frames.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = DelegationActionJson.schema();
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("afterCursor")
                .put("type", "integer")
                .put("minimum", 0)
                .put("description", "Resume position; 0 reads from the beginning. Defaults "
                        + "to 0.");
        putString(properties, "taskId", "Optional task uuid restricting the watch.");
        putInteger(properties, "timeoutMs",
                "How long to wait for the first event, in milliseconds.", 0, MAX_TIMEOUT_MS);
        putInteger(properties, "maxEvents",
                "The largest batch one call returns.", 1, MAX_EVENTS_PER_CALL);
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        long afterCursor = DelegationActionJson.boundedLong(input, "afterCursor", 0,
                Long.MAX_VALUE);
        String taskId = DelegationActionJson.optionalUuid(input, "taskId");
        int timeoutMs = DelegationActionJson.boundedInt(input, "timeoutMs", 1_000, 0,
                MAX_TIMEOUT_MS);
        int maxEvents = DelegationActionJson.boundedInt(input, "maxEvents", 64, 1,
                MAX_EVENTS_PER_CALL);
        List<InProcessDelegationCoordinator.Event> events;
        try {
            Optional<InProcessDelegationCoordinator.Event> first = bridge.coordinator()
                    .waitForEvent(taskId, afterCursor, Duration.ofMillis(timeoutMs));
            if (first.isEmpty()) {
                ObjectNode output = context.objectMapper().createObjectNode();
                output.put("ok", true);
                output.putArray("events");
                output.put("cursor", afterCursor);
                output.put("timedOut", true);
                return output;
            }
            events = bridge.coordinator().eventsAfter(taskId, afterCursor);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ActionException("interrupted", "delegation-watch was interrupted");
        } catch (RuntimeException e) {
            throw failure(e);
        }
        boolean truncated = events.size() > maxEvents;
        if (truncated) {
            events = events.subList(0, maxEvents);
        }
        ObjectNode output = eventsJson(events, truncated, context);
        output.put("timedOut", false);
        return output;
    }
}
