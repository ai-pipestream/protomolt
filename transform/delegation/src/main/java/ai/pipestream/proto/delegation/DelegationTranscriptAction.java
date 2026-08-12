package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

/** Reads a bounded slice of the recorded delegation transcript from a cursor. */
final class DelegationTranscriptAction extends DelegationAction {

    /** The largest transcript slice one call returns. */
    static final int MAX_ENTRIES_PER_CALL = 500;

    DelegationTranscriptAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-transcript";
    }

    @Override
    public String description() {
        return "Reads the recorded delegation transcript from a cursor: every accepted frame "
                + "in recorded order, optionally restricted to one task. Bounded per call; "
                + "follow the returned cursor while 'truncated' is true.";
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
        putString(properties, "taskId", "Optional task uuid restricting the read.");
        putInteger(properties, "maxEntries",
                "The largest slice one call returns.", 1, MAX_ENTRIES_PER_CALL);
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        long afterCursor = DelegationActionJson.boundedLong(input, "afterCursor", 0,
                Long.MAX_VALUE);
        String taskId = DelegationActionJson.optionalUuid(input, "taskId");
        int maxEntries = DelegationActionJson.boundedInt(input, "maxEntries", 100, 1,
                MAX_ENTRIES_PER_CALL);
        List<InProcessDelegationCoordinator.Event> events;
        try {
            events = bridge.coordinator().eventsAfter(taskId, afterCursor);
        } catch (RuntimeException e) {
            throw failure(e);
        }
        boolean truncated = events.size() > maxEntries;
        if (truncated) {
            events = events.subList(0, maxEntries);
        }
        return eventsJson(events, truncated, context);
    }
}
