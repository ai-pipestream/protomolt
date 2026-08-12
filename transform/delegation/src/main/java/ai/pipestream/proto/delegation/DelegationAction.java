package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

/** Shared base for the delegation catalog actions: the bridge plus event rendering. */
abstract class DelegationAction implements ProtoAction {

    /** Upper bound on events one watch or transcript call returns. */
    static final int MAX_EVENTS_PER_CALL = 256;

    final DelegationBridge bridge;

    DelegationAction(DelegationBridge bridge) {
        this.bridge = Objects.requireNonNull(bridge, "bridge");
    }

    /** Renders one cursor-addressable transcript event. */
    static ObjectNode eventJson(InProcessDelegationCoordinator.Event event,
                                ActionContext context) throws ActionException {
        ObjectNode node = context.objectMapper().createObjectNode();
        node.put("cursor", event.cursor());
        node.put("workerId", event.workerId());
        node.put("taskId", event.taskId());
        node.put("lane", event.entry().getLane().name());
        node.set("entry", DelegationActionJson.render(event.entry(), context));
        return node;
    }

    /** Renders a bounded run of transcript events plus the resumption cursor. */
    static ObjectNode eventsJson(java.util.List<InProcessDelegationCoordinator.Event> events,
                                 boolean truncated, ActionContext context)
            throws ActionException {
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("ok", true);
        ArrayNode array = output.putArray("events");
        long cursor = 0;
        for (InProcessDelegationCoordinator.Event event : events) {
            array.add(eventJson(event, context));
            cursor = event.cursor();
        }
        if (!events.isEmpty()) {
            output.put("cursor", cursor);
        }
        output.put("truncated", truncated);
        return output;
    }

    /** Maps bridge and coordinator failures onto the stable delegation error codes. */
    static ActionException failure(String workerId, RuntimeException e) {
        String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        if (message.startsWith("worker is not registered")) {
            return DelegationActionJson.unknownWorker(workerId);
        }
        if (message.startsWith("worker stream")) {
            return DelegationActionJson.streamFailed(workerId, message);
        }
        return DelegationActionJson.rejected(message);
    }

    /** Maps coordinator-side failures onto the stable delegation error codes. */
    static ActionException failure(RuntimeException e) {
        return failure("", e);
    }

    static void putString(ObjectNode properties, String name, String description) {
        properties.putObject(name).put("type", "string").put("description", description);
    }

    static void putInteger(ObjectNode properties, String name, String description,
                           int min, int max) {
        properties.putObject(name).put("type", "integer")
                .put("minimum", min).put("maximum", max)
                .put("description", description);
    }

    static void require(ObjectNode schema, String... fields) {
        ArrayNode required = schema.putArray("required");
        for (String field : fields) {
            required.add(field);
        }
    }
}
